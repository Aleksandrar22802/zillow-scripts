#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import os
import random
import re
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Tuple, Union, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───────────────────────────── SIGINT: instant exit ───────────────────────────
def _sigint_immediate_exit(sig, frame):
    try:
        logging.error("Ctrl+C detected — exiting immediately.")
    except Exception:
        pass
    os._exit(130)

signal.signal(signal.SIGINT, _sigint_immediate_exit)

# ───────────────────────────── Logging helpers ─────────────────────────────
def mask_proxy(proxy_url: Optional[str]) -> str:
    if not proxy_url:
        return "DIRECT"
    try:
        p = urlparse(proxy_url)
        hostport = p.netloc.split("@")[-1]
        return f"{p.scheme}://{hostport}"
    except Exception:
        return proxy_url.split("@")[-1]

def configure_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s.%(msecs)03d %(levelname)-7s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )

# ───────────────────────────── Thread-local RNG ─────────────────────────────
_rng_tls = threading.local()
def thread_rng() -> random.Random:
    rng = getattr(_rng_tls, "rng", None)
    if rng is None:
        seed = (os.getpid() << 32) ^ threading.get_ident() ^ time.monotonic_ns()
        rng = random.Random(seed)
        _rng_tls.rng = rng
    return rng

# ───────────────────────────── HTML → text renderer ──────────────────────────
_trace_render_chars = 400  # set from CLI

def render_html_to_text(html: str, max_len: int) -> str:
    try:
        html = re.sub(r'(?is)<(script|style)[^>]*>.*?</\1>', '', html)
        html = re.sub(r'(?i)</?(br|p|div|li|ul|ol|h[1-6]|header|footer|section|article|tr|td|th)>', '\n', html)
        text = re.sub(r'(?s)<[^>]+>', '', html)
        text = unescape(text)
        text = re.sub(r'[ \t\r\f\v]+', ' ', text)
        text = re.sub(r'\n+', '\n', text).strip()
        if len(text) > max_len:
            text = text[:max_len].rstrip() + '…'
        return text or "(empty body)"
    except Exception:
        return "(unrenderable error body)"

# ───────────────────────────── Proxy health persistence ──────────────────────
class ProxyHealthStore:
    def __init__(self, path: Path, autosave_seconds: float = 5.0):
        self.path = path
        self.autosave_seconds = autosave_seconds
        self._lock = threading.Lock()
        self._data: Dict[str, Dict] = {}
        self._dirty = False
        self._last_save = 0.0
        try:
            if self.path.exists():
                txt = self.path.read_text(encoding="utf-8")
                self._data = json.loads(txt) if txt.strip() else {}
        except Exception as e:
            logging.warning(f"HealthStore: could not read {self.path}: {e}")

    def _maybe_save(self, force: bool = False):
        now = time.time()
        if not force and (not self._dirty or (now - self._last_save) < self.autosave_seconds):
            return
        try:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.parent.mkdir(parents=True, exist_ok=True)
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
                f.write("\n")
            os.replace(tmp, self.path)
            self._dirty = False
            self._last_save = now
            logging.debug(f"HealthStore: saved {self.path}")
        except Exception as e:
            logging.warning(f"HealthStore: failed to save {self.path}: {e}")

    def mark_good(self, proxy_url: str):
        with self._lock:
            rec = self._data.get(proxy_url, {})
            rec["status"] = "good"
            rec["last_check"] = datetime.now(timezone.utc).isoformat()
            rec["success_count"] = int(rec.get("success_count", 0)) + 1
            rec.pop("down_until", None)
            self._data[proxy_url] = rec
            self._dirty = True
            self._maybe_save()

    def mark_bad(self, proxy_url: str, cooldown_seconds: float, reason: str = ""):
        with self._lock:
            rec = self._data.get(proxy_url, {})
            rec["status"] = "bad"
            rec["last_check"] = datetime.now(timezone.utc).isoformat()
            rec["fail_count"] = int(rec.get("fail_count", 0)) + 1
            if reason:
                rec["reason"] = reason
            rec["down_until"] = time.time() + float(cooldown_seconds)
            self._data[proxy_url] = rec
            self._dirty = True
            self._maybe_save()

    def is_down(self, proxy_url: str) -> bool:
        with self._lock:
            rec = self._data.get(proxy_url)
            if not rec or rec.get("status") != "bad":
                return False
            return time.time() < float(rec.get("down_until", 0.0))

    def next_available_in(self, proxy_url: str) -> float:
        with self._lock:
            until = float(self._data.get(proxy_url, {}).get("down_until", 0.0))
            return max(0.0, until - time.time())

    def flush(self):
        with self._lock:
            self._maybe_save(force=True)

# ───────────────────────────── Rounds control exception ──────────────────────
class ProxyRoundsExhausted(Exception):
    """Raised by ProxyPool.acquire() when max rounds have been completed."""
    pass

# ───────────────────────────── Proxy pool (coordinated sleep) ────────────────
class ProxyPool:
    """
    Round-robin proxy pool with:
      • cooldown on failures (in-memory + persistent health)
      • unique-per-interval (each proxy ≤1× per round)
      • randomized order each round (per-thread RNG)
      • sleep after every N rounds; optional max rounds
      • Condition guard so exactly one thread sleeps/resets between rounds
    """
    def __init__(self, proxies: List[str], cooldown_seconds: float = 60.0,
                 health: Optional[ProxyHealthStore] = None,
                 unique_per_interval: bool = False,
                 interval_sleep_seconds: float = 7200.0,
                 max_rounds: Optional[int] = None,
                 sleep_every_rounds: int = 1):
        if not proxies:
            raise ValueError("Proxy list is empty")
        self.proxies = proxies[:]
        thread_rng().shuffle(self.proxies)
        self.cooldown = cooldown_seconds
        self.health = health
        self.unique_per_interval = unique_per_interval
        self.interval_sleep_seconds = interval_sleep_seconds
        self.max_rounds = max_rounds
        self.sleep_every_rounds = max(1, int(sleep_every_rounds))

        self._i = 0
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._round_resetting = False
        self._down_until: Dict[str, float] = {}
        self._used_in_window: set[str] = set()
        self._window_started_ts = time.time()
        self._rounds_done = 0  # completed rounds

    def _is_available_now(self, p: str) -> bool:
        now = time.time()
        if now < self._down_until.get(p, 0.0): return False
        if self.health and self.health.is_down(p): return False
        if self.unique_per_interval and p in self._used_in_window: return False
        return True

    def _complete_round_locked(self):
        # one thread (with lock) completes a round
        self._rounds_done += 1
        if self.max_rounds is not None and self._rounds_done > self.max_rounds:
            self._round_resetting = False
            self._cv.notify_all()
            raise ProxyRoundsExhausted()

        should_sleep = (
            self.interval_sleep_seconds > 0 and
            (self._rounds_done % self.sleep_every_rounds == 0)
        )

        if should_sleep:
            sleep_for = float(self.interval_sleep_seconds)
            logging.warning(
                f"ProxyPool: completed round {self._rounds_done}"
                + (f"/{self.max_rounds}" if self.max_rounds else "")
                + f"; sleeping {sleep_for/60:.1f} minutes…"
            )
            self._lock.release()
            try:
                time.sleep(sleep_for)
            finally:
                self._lock.acquire()
        else:
            logging.info(
                f"ProxyPool: completed round {self._rounds_done}"
                + (f"/{self.max_rounds}" if self.max_rounds else "")
                + " — batched rounds (no sleep)."
            )

        thread_rng().shuffle(self.proxies)
        self._used_in_window.clear()
        self._window_started_ts = time.time()
        self._i = 0
        self._round_resetting = False
        self._cv.notify_all()
        logging.info(
            "ProxyPool: starting round %d%s (randomized order, %d proxies).",
            self._rounds_done + 1, f"/{self.max_rounds}" if self.max_rounds else "", len(self.proxies)
        )

    def acquire(self) -> str:
        last_log = 0.0
        while True:
            with self._lock:
                while self._round_resetting:
                    self._cv.wait()

                n = len(self.proxies)
                for _ in range(n):
                    p = self.proxies[self._i]
                    self._i = (self._i + 1) % n
                    if self._is_available_now(p):
                        if self.unique_per_interval:
                            self._used_in_window.add(p)
                        logging.debug(f"ProxyPool.acquire -> {mask_proxy(p)}")
                        return p

                # If unique-per-interval & we used all currently usable proxies → complete round
                if self.unique_per_interval:
                    usable_count = 0
                    used_count = 0
                    now = time.time()
                    for px in self.proxies:
                        if now < self._down_until.get(px, 0.0):
                            continue
                        if self.health and self.health.is_down(px):
                            continue
                        usable_count += 1
                        if px in self._used_in_window:
                            used_count += 1
                    if usable_count > 0 and used_count >= usable_count:
                        if not self._round_resetting:
                            self._round_resetting = True
                            self._complete_round_locked()
                        else:
                            self._cv.wait()
                        continue

                # No immediately-usable proxies: compute earliest next-available wait
                now = time.time()
                soonest = float("inf")
                down = 0
                for px in self.proxies:
                    w = 0.0
                    if px in self._down_until:
                        w = max(w, self._down_until[px] - now)
                    if self.health and self.health.is_down(px):
                        w = max(w, self.health.next_available_in(px))
                    if w > 0:
                        down += 1
                        if w < soonest:
                            soonest = w

                # Decide wait time + occasional visibility log
                wait = 1.0
                if soonest != float("inf"):
                    wait = max(1.0, min(soonest, 60.0))  # cap to avoid oversleeping too long in-thread

                    if now - last_log > 30.0:  # throttle logs to once every 30s
                        mins, secs = divmod(int(soonest), 60)
                        logging.info(
                            "ProxyPool: all proxies cooling down; next available in %dm %02ds "
                            "(down %d/%d).", mins, secs, down, len(self.proxies)
                        )
                        last_log = now
            time.sleep(wait)

    def report_bad(self, proxy: str, reason: str = ""):
        with self._lock:
            self._down_until[proxy] = time.time() + self.cooldown
        if self.health: self.health.mark_bad(proxy, cooldown_seconds=self.cooldown, reason=reason)
        logging.debug(f"ProxyPool.report_bad {mask_proxy(proxy)} for {self.cooldown:.0f}s reason={reason}")

    def report_good(self, proxy: str):
        with self._lock:
            self._down_until.pop(proxy, None)
        if self.health: self.health.mark_good(proxy)
        logging.debug(f"ProxyPool.report_good {mask_proxy(proxy)} (clearing cooldown)")

# ─────────────────────── Requests session factory ──────────────────────
_tls = threading.local()
def get_session() -> requests.Session:
    s = getattr(_tls, "session", None)
    if s is None:
        s = requests.Session()
        retry = Retry(total=0, backoff_factor=0, status_forcelist=[], raise_on_status=False)
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _tls.session = s
        logging.debug("Created new requests.Session for thread")
    return s

# ─────────────────────────── GraphQL fetcher ────────────────────────────
def fetch_graphql(
    zpid: Union[int, str],
    proxy_url: Optional[str],
    timeout: float,
    user_agent: Optional[str] = None,
    trace: bool = False,
) -> Dict:
    """
    Zillow GraphQL GET with params (optionally via proxy). Returns cleaned dict data['property'].
    NOTE: Review robots.txt / ToS before crawling.
    """
    base_url = "https://www.zillow.com/graphql/"
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": "db712c032dc3f55dee0742b5f2b20db1025fd209bee693c54eb145d6c6c8fa5c"}}
    zpid_val = int(zpid) if isinstance(zpid, str) and str(zpid).isdigit() else zpid
    variables = {"zpid": zpid_val}
    params = {
        "extensions": json.dumps(extensions, separators=(",", ":")),
        "variables": json.dumps(variables, separators=(",", ":")),
    }
    headers = {
        "user-agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "client-id": "for-sale-sub-app-browser-client",
        "referer": f"https://www.zillow.com/homedetails/any-title/{zpid}_zpid/",
        "x-z-enable-oauth-conversion": "true",
        "x-enabled-listings-flag": "restricted",
    }
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

    s = get_session()
    t0 = time.time()
    r = s.get(base_url, headers=headers, params=params, proxies=proxies, timeout=timeout, allow_redirects=True)
    dt = time.time() - t0
    logging.debug(f"HTTP GET {mask_proxy(proxy_url)} zpid={zpid_val} -> {r.status_code} in {dt:.2f}s, bytes={len(r.content)}, url={r.request.url}")

    if not r.ok:
        if trace:
            rendered = render_html_to_text(r.text, _trace_render_chars)
            logging.debug(f"TRACE rendered(err) [{r.status_code}] {rendered}")
        raise requests.HTTPError(f"status={r.status_code}")

    try:
        data = r.json()
        if trace:
            logging.debug(f"TRACE body(json ok) {str(list(data.keys()))[:200]}")
        d = data["data"]["property"]
        for k in ["topNavJson", "staticMap", "responsivePhotosOriginalRatio", "responsivePhotos", "originalPhotos"]:
            d.pop(k, None)
        return d
    except Exception as e:
        if trace:
            rendered = render_html_to_text(r.text, _trace_render_chars)
            logging.debug(f"TRACE rendered(parse) {rendered}")
        raise ValueError(f"Invalid JSON structure: {e}")

def fetch_data(
    zpid: Union[int, str],
    proxy_url: Optional[str],
    timeout: float,
    user_agent: Optional[str] = None,
    trace: bool = False,
) -> Dict:
    return fetch_graphql(zpid, proxy_url=proxy_url, timeout=timeout, user_agent=user_agent, trace=trace)

# ───────────────────────── Filesystem helpers ──────────────────────────
def atomic_write_json(path: Path, obj: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")
    os.replace(tmp, path)
    logging.debug(f"Wrote file {path}")

def coerce_zpid(val: str) -> Union[int, str]:
    v = val.strip()
    return int(v) if v.isdigit() else v

def read_pairs_from_csv(csv_path: Path,
                        only_zip: List[str],
                        limit: Optional[int],
                        dedupe: bool,
                        has_header: Optional[bool]) -> List[Tuple[str, Union[int, str]]]:
    pairs: List[Tuple[str, Union[int, str]]] = []
    only = set(only_zip) if only_zip else None
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        first = True
        for row in reader:
            if not row or len(row) < 2:
                continue
            if first:
                first = False
                if has_header is None:
                    z0, z1 = row[0].strip(), row[1].strip()
                    looks_like_header = ((not z1.isdigit()) or any(c.isalpha() for c in z0))
                    if looks_like_header:
                        logging.debug(f"CSV: skipping header row {row}")
                        continue
                elif has_header:
                    logging.debug(f"CSV: skipping header row {row}")
                    continue
            zipcode = row[0].strip()
            zpid = coerce_zpid(row[1])
            if only and zipcode not in only:
                continue
            pairs.append((zipcode, zpid))
            if limit and len(pairs) >= limit:
                break
    if dedupe:
        before = len(pairs)
        pairs = list(dict.fromkeys(pairs))
        logging.debug(f"CSV: deduped {before} -> {len(pairs)}")
    logging.info(f"CSV: loaded {len(pairs)} pairs from {csv_path}")
    return pairs

# ─────────────────────────── Worker ───────────────────────────
def scrape_one(
    zipcode: str,
    zpid: Union[int, str],
    out_root: Path,
    proxies: Optional[ProxyPool],
    skip_if_exists: bool,
    overwrite: bool,
    max_retries: int,
    base_delay: float,
    jitter: float,
    timeout: float,
    ua_list: Optional[List[str]] = None,
    trace: bool = False,
    sample_keys: int = 0
) -> Tuple[int, str]:
    out_path = out_root / zipcode / f"{zpid}.json"

    if out_path.exists() and skip_if_exists:
        logging.debug(f"Skip existing (per flag) {out_path}")
        return 0, f"skip exists {out_path}"

    delay = base_delay
    last_err = ""
    rng = thread_rng()

    for attempt in range(1, max_retries + 1):
        proxy_url = None
        if proxies is not None:
            try:
                proxy_url = proxies.acquire()
            except ProxyRoundsExhausted:
                return 0, "rounds-exhausted"

        ua = rng.choice(ua_list) if (ua_list and len(ua_list) > 0) else None
        via = mask_proxy(proxy_url)
        logging.debug(
            f"Attempt {attempt}/{max_retries} zpid={zpid} zip={zipcode} via {via} UA={ua or 'default'} "
            f"overwrite={'yes' if overwrite else 'no'}"
        )
        try:
            data = fetch_data(zpid, proxy_url, timeout=timeout, user_agent=ua, trace=trace)
            if not data:
                last_err = "empty"
                raise ValueError("empty payload")

            if sample_keys > 0:
                try:
                    klist = list(data.keys())[:sample_keys]
                    logging.debug(f"Sample keys: {klist}")
                except Exception:
                    pass

            data.setdefault("zpid", zpid)
            data.setdefault("_fetchedAt", datetime.now(timezone.utc).isoformat())
            data.setdefault("_zipcodeHint", zipcode)
            data.setdefault("_fetchMethod", "graphql")
            data.setdefault("_proxyUsed", via)

            atomic_write_json(out_path, data)

            if proxies is not None and proxy_url:
                proxies.report_good(proxy_url)
            return 1, f"ok ({via})"

        except requests.HTTPError as e:
            msg = str(e)
            last_err = msg
            logging.debug(f"HTTP error zpid={zpid}: {msg}")
            if proxies is not None and proxy_url:
                proxies.report_bad(proxy_url, reason=msg)
        except (requests.ConnectionError, requests.Timeout) as e:
            last_err = f"net:{e}"
            logging.debug(f"Network error zpid={zpid}: {e}")
            if proxies is not None and proxy_url:
                proxies.report_bad(proxy_url, reason="network")
        except Exception as e:
            last_err = f"other:{e}"
            logging.debug(f"Other error zpid={zpid}: {e}")

        sleep_for = delay + rng.uniform(0, jitter)
        logging.debug(f"Backoff sleeping {sleep_for:.2f}s before retry")
        time.sleep(sleep_for)
        delay *= 2

    return 2, f"fail after {max_retries} attempts; last_err={last_err}"

# ──────────────────────────────── Main ─────────────────────────────────
def read_lines(path: Path) -> List[str]:
    return [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip() and not ln.strip().startswith("#")]

def main():
    ap = argparse.ArgumentParser(description="GraphQL-only Zillow scraper with robust proxy pool and rounds.")
    ap.add_argument("--zpid-csv-file", required=True, type=Path, help="CSV file with two columns: zipcode,zpid")
    ap.add_argument("--out-root", required=True, type=Path, help="Output tree: /OUT_ROOT/<zipcode>/<zpid>.json")

    # Proxy + health + rounds
    ap.add_argument("--proxies-file", type=Path, default=None, help="Text file with one proxy URL per line")
    ap.add_argument("--proxy-health-file", type=Path, default=None, help="JSON file to persist proxy health")
    ap.add_argument("--bad-cooldown-seconds", type=float, default=3600.0, help="Seconds to keep a failed proxy on cooldown")
    ap.add_argument("--health-autosave-seconds", type=float, default=5.0, help="How often to flush health updates")

    ap.add_argument("--unique-per-interval", action="store_true",
                    help="Use each proxy at most once per round; when all usable proxies are used, reset the round")
    ap.add_argument("--interval-sleep-seconds", type=float, default=7200.0,
                    help="Sleep this long when a sleep is triggered between rounds (default 2h)")
    ap.add_argument("--max-rounds", type=int, default=0,
                    help="Maximum number of rounds (0 = unlimited). Applies only with --unique-per-interval")
    ap.add_argument("--sleep-every-rounds", type=int, default=1,
                    help="Run this many rounds back-to-back, then sleep once (default 1). Applies only with --unique-per-interval")

    # Data selection
    ap.add_argument("--only-zip", nargs="*", default=[], help="Limit to specific zipcodes")
    ap.add_argument("--limit", type=int, default=None, help="Process at most N items")
    ap.add_argument("--dedupe", action="store_true", help="De-duplicate (zipcode,zpid) pairs from CSV")
    ap.add_argument("--csv-has-header", action="store_true", help="CSV has a header row (if not set, auto-detect)")

    # Overwrite policy
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    group.add_argument("--skip-if-exists", action="store_true", help="Skip when output already exists")

    # Execution behavior
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--max-retries", type=int, default=2)
    ap.add_argument("--base-delay", type=float, default=15)
    ap.add_argument("--jitter", type=float, default=12)
    ap.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds")
    ap.add_argument("--user-agents-file", type=Path, default=None, help="Optional file with one User-Agent per line")

    # Debug/logging
    ap.add_argument("--trace", action="store_true", help="Log rendered error snippets on failures")
    ap.add_argument("--trace-render-chars", type=int, default=400, help="Chars of rendered text to log on errors")
    ap.add_argument("--sample", type=int, default=0, help="Log first N top-level keys of JSON payload")
    ap.add_argument("--log-level", default="DEBUG", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = ap.parse_args()
    configure_logging(args.log_level)

    def _watchdog():
        while True:
            time.sleep(300)
            logging.info("Watchdog: alive; threads=%d", threading.active_count())
    threading.Thread(target=_watchdog, daemon=True).start()

    global _trace_render_chars
    _trace_render_chars = max(50, int(args.trace_render_chars))

    # Health store
    health_store = None
    if args.proxy_health_file:
        health_store = ProxyHealthStore(args.proxy_health_file, autosave_seconds=args.health_autosave_seconds)
        logging.info(f"Proxy health persistence: {args.proxy_health_file}")

    # Proxies
    pool: Optional[ProxyPool] = None
    proxies_list: List[str] = []
    if args.proxies_file and args.proxies_file.exists():
        try:
            proxies_list = read_lines(args.proxies_file)
        except Exception as e:
            logging.warning(f"Could not read proxies file: {e}")

    if proxies_list:
        if health_store:
            rng = thread_rng()
            rng.shuffle(proxies_list)
            alive, down = [], 0
            for p in proxies_list:
                if health_store.is_down(p):
                    down += 1
                else:
                    alive.append(p)
            if alive:
                logging.info(f"Loaded {len(proxies_list)} proxies ({down} cooling down, {len(alive)} usable now)")
                proxies_list = alive
            else:
                logging.info(f"All {len(proxies_list)} proxies cooling down per health file; keeping full list (pool will wait)")

        max_rounds = None if args.max_rounds <= 0 else args.max_rounds
        sleep_every_rounds = max(1, int(args.sleep_every_rounds))
        pool = ProxyPool(
            proxies_list,
            cooldown_seconds=args.bad_cooldown_seconds,
            health=health_store,
            unique_per_interval=args.unique_per_interval,
            interval_sleep_seconds=args.interval_sleep_seconds,
            max_rounds=max_rounds,
            sleep_every_rounds=sleep_every_rounds,
        )
    else:
        logging.info("No proxies provided — using DIRECT connections (not recommended for protected targets)")

    # UA list
    ua_list = None
    if args.user_agents_file and args.user_agents_file.exists():
        try:
            ua_list = read_lines(args.user_agents_file)
        except Exception as e:
            logging.warning(f"Could not read user agents file: {e}")

    # CSV pairs
    pairs = read_pairs_from_csv(
        csv_path=args.zpid_csv_file,
        only_zip=args.only_zip,
        limit=args.limit,
        dedupe=args.dedupe,
        has_header=True if args.csv_has_header else None
    )
    if not pairs:
        raise SystemExit("No pairs to process from CSV")

    logging.info("Pairs ready: %d from %s | mode=%s",
                 len(pairs), args.zpid_csv_file,
                 "SKIP-IF-EXISTS" if args.skip_if_exists else "OVERWRITE/REFRESH")

    ok = skip = fail = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers), thread_name_prefix="worker") as ex:
        future_to_pair = {
            ex.submit(
                scrape_one, zipcode, zpid, args.out_root, pool,
                args.skip_if_exists, args.overwrite,
                args.max_retries, args.base_delay, args.jitter, args.timeout,
                ua_list, args.trace, args.sample
            ): (zipcode, zpid) for zipcode, zpid in pairs
        }
        for fut in as_completed(future_to_pair):
            zipcode, zpid = future_to_pair[fut]
            try:
                status, msg = fut.result()
            except Exception as e:
                status, msg = 2, f"unhandled:{e}"
            if status == 1:
                ok += 1
                logging.info(f"OK {zipcode}/{zpid} {msg}")
            elif status == 0:
                skip += 1
                logging.info(f"SKIP {zipcode}/{zpid} {msg}")
            else:
                fail += 1
                logging.warning(f"FAIL {zipcode}/{zpid} {msg}")

    logging.info("Done. ok=%d skipped=%d failed=%d", ok, skip, fail)
    if health_store:
        health_store.flush()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.error("Interrupted — exiting.")
        os._exit(130)
