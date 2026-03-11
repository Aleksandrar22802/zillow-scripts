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

# ───────────────────────────── SIGINT immediate exit ─────────────────────────
def _sigint_immediate_exit(sig, frame):
    try:
        logging.error("Ctrl+C detected — exiting immediately.")
    except Exception:
        pass
    os._exit(130)

signal.signal(signal.SIGINT, _sigint_immediate_exit)

# ───────────────────────────── Logging helpers ───────────────────────────────
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

# ───────────────────────────── Thread-local RNG ──────────────────────────────
_rng_tls = threading.local()
def thread_rng() -> random.Random:
    rng = getattr(_rng_tls, "rng", None)
    if rng is None:
        seed = (os.getpid() << 32) ^ threading.get_ident() ^ time.monotonic_ns()
        rng = random.Random(seed)
        _rng_tls.rng = rng
    return rng

# ───────────────────────────── HTML → text renderer ──────────────────────────
_trace_render_chars = 400
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
        self._load()

    def _load(self):
        try:
            if self.path.exists():
                txt = self.path.read_text(encoding="utf-8")
                self._data = json.loads(txt) if txt.strip() else {}
            else:
                self._data = {}
        except Exception as e:
            logging.warning(f"HealthStore: could not read {self.path}: {e}")
            self._data = {}

    def _maybe_save(self, force: bool = False):
        now = time.time()
        if not force and not self._dirty:
            return
        if not force and (now - self._last_save) < self.autosave_seconds:
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
            until = float(rec.get("down_until", 0.0))
            return time.time() < until

    def next_available_in(self, proxy_url: str) -> float:
        with self._lock:
            rec = self._data.get(proxy_url, {})
            until = float(rec.get("down_until", 0.0))
            return max(0.0, until - time.time())

    def flush(self):
        with self._lock:
            self._maybe_save(force=True)

# ───────────────────────────── Rounds control exception ──────────────────────
class ProxyRoundsExhausted(Exception):
    """Raised by ProxyPool.acquire() when max rounds completed."""
    pass

# ───────────────────────────── Proxy pool (coordinated sleep) ────────────────
class ProxyPool:
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
        self._rounds_done = 0

    def _is_available_now(self, p: str) -> bool:
        now = time.time()
        if now < self._down_until.get(p, 0.0): return False
        if self.health and self.health.is_down(p): return False
        if self.unique_per_interval and p in self._used_in_window: return False
        return True

    def _complete_round_locked(self):
        # finalize just-finished round
        self._rounds_done += 1
        if self.max_rounds is not None and self._rounds_done >= self.max_rounds:
            self._round_resetting = False
            self._cv.notify_all()
            raise ProxyRoundsExhausted()

        should_sleep = (
            self.interval_sleep_seconds > 0 and
            (self._rounds_done % self.sleep_every_rounds == 0)
        )
        if should_sleep:
            sleep_for = float(self.interval_sleep_seconds)
            logging.warning(f"ProxyPool: completed round {self._rounds_done}; sleeping {sleep_for/60:.1f} minutes…")
            self._lock.release()
            try:
                time.sleep(sleep_for)
            finally:
                self._lock.acquire()
        else:
            logging.info(f"ProxyPool: completed round {self._rounds_done} (no sleep)")

        thread_rng().shuffle(self.proxies)
        self._used_in_window.clear()
        self._i = 0
        self._round_resetting = False
        self._cv.notify_all()

    def acquire(self) -> str:
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
                        return p

                # if unique-per-interval and all available proxies used
                if self.unique_per_interval and len(self._used_in_window) >= len(self.proxies):
                    if not self._round_resetting:
                        self._round_resetting = True
                        self._complete_round_locked()
                    else:
                        self._cv.wait()
                    continue

                # otherwise simple cooldown wait
                wait = 1.0
            time.sleep(wait)

    def report_bad(self, proxy: str, reason: str = ""):
        with self._lock:
            self._down_until[proxy] = time.time() + self.cooldown
        if self.health:
            self.health.mark_bad(proxy, cooldown_seconds=self.cooldown, reason=reason)
        logging.debug(f"Proxy bad: {mask_proxy(proxy)} ({reason})")

    def report_good(self, proxy: str):
        with self._lock:
            self._down_until.pop(proxy, None)
        if self.health:
            self.health.mark_good(proxy)
        logging.debug(f"Proxy good: {mask_proxy(proxy)})")

# ───────────────────────────── Session factory ───────────────────────────────
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
    return s

# ───────────────────────────── RapidAPI rate limit ───────────────────────────
_rate_lock = threading.Lock()
_next_allowed_ts = 0.0
def rapidapi_rate_limit(min_interval: float = 0.4):
    global _next_allowed_ts
    with _rate_lock:
        now = time.monotonic()
        if now < _next_allowed_ts:
            time.sleep(_next_allowed_ts - now)
            now = time.monotonic()
        _next_allowed_ts = now + min_interval

# ───────────────────────────── Fetchers ──────────────────────────────────────
def fetch_graphql(zpid: Union[int, str], proxy_url: Optional[str], timeout: float,
                  user_agent: Optional[str] = None, trace: bool = False) -> Dict:
    """
    Zillow GraphQL GET with params (optionally via proxy). Returns data['property'].
    Review robots.txt / ToS before crawling.
    """
    base_url = "https://www.zillow.com/graphql/"
    extensions = {"persistedQuery":{"version":1,"sha256Hash":"db712c032dc3f55dee0742b5f2b20db1025fd209bee693c54eb145d6c6c8fa5c"}}
    zpid_val = int(zpid) if isinstance(zpid, str) and str(zpid).isdigit() else zpid
    params = {"extensions": json.dumps(extensions, separators=(",",":")),
              "variables": json.dumps({"zpid": zpid_val}, separators=(",",":"))}
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
    r = s.get(base_url, headers=headers, params=params, proxies=proxies, timeout=timeout, allow_redirects=True)
    if not r.ok:
        if trace:
            logging.debug(f"TRACE rendered(err) [{r.status_code}] {render_html_to_text(r.text, _trace_render_chars)}")
        raise requests.HTTPError(f"status={r.status_code}")
    try:
        data = r.json()
        return data["data"]["property"]
    except Exception as e:
        if trace:
            logging.debug(f"TRACE rendered(parse) {render_html_to_text(r.text, _trace_render_chars)}")
        raise ValueError(f"Invalid JSON: {e}")

def fetch_rapidapi(zpid: Union[int, str], timeout: float,
                   user_agent: Optional[str] = None, trace: bool = False) -> Dict:
    base_url = "https://zillow-com1.p.rapidapi.com/property"
    key = os.getenv("RAPIDAPI_KEY")
    if not key:
        raise RuntimeError("RAPIDAPI_KEY environment variable is missing")
    headers = {"X-RapidAPI-Key": key, "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com", "accept": "application/json"}
    if user_agent:
        headers["user-agent"] = user_agent
    params = {"zpid": int(zpid) if isinstance(zpid, str) and str(zpid).isdigit() else zpid}
    rapidapi_rate_limit(0.4)
    s = get_session()
    r = s.get(base_url, headers=headers, params=params, timeout=timeout, allow_redirects=True)
    if not r.ok:
        if trace:
            logging.debug(f"TRACE rendered(err) [{r.status_code}] {render_html_to_text(r.text, _trace_render_chars)}")
        raise requests.HTTPError(f"status={r.status_code}")
    try:
        return r.json()
    except Exception as e:
        if trace:
            logging.debug(f"TRACE rendered(parse) {render_html_to_text(r.text, _trace_render_chars)}")
        raise ValueError(f"Invalid JSON (RapidAPI): {e}")

def fetch_data(method: str, zpid: Union[int, str], proxy_url: Optional[str],
               timeout: float, user_agent: Optional[str], trace: bool) -> Dict:
    if method == "rapidapi":
        return fetch_rapidapi(zpid, timeout, user_agent, trace)
    return fetch_graphql(zpid, proxy_url, timeout, user_agent, trace)

# ───────────────────────────── Helpers ───────────────────────────────────────
def atomic_write_json(path: Path, obj: Dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)
    logging.debug(f"Wrote {path}")

def read_pairs_from_csv(csv_path: Path,
                        only_zip: List[str] = None,
                        limit: Optional[int] = None,
                        dedupe: bool = False,
                        has_header: Optional[bool] = None) -> List[Tuple[str, Union[int, str]]]:
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
            zpid = int(row[1]) if str(row[1]).strip().isdigit() else row[1].strip()
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

def read_lines(path: Path) -> List[str]:
    return [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip() and not ln.strip().startswith("#")]

# ───────────────────────────── Worker ────────────────────────────────────────
def scrape_one(fetch_method: str, zipcode: str, zpid: Union[int, str], out_root: Path,
               pool: Optional[ProxyPool], overwrite: bool,
               max_retries: int, base_delay: float, jitter: float, timeout: float,
               ua_list: Optional[List[str]], trace: bool, sample_keys: int = 0) -> Tuple[int, str]:
    rng = thread_rng()
    out_path = out_root / zipcode / f"{zpid}.json"
    delay = base_delay
    last_err = ""
    for attempt in range(1, max_retries + 1):
        proxy_url = None
        if fetch_method == "graphql" and pool is not None:
            # Let ProxyRoundsExhausted bubble up
            proxy_url = pool.acquire()
        ua = rng.choice(ua_list) if (ua_list and len(ua_list) > 0) else None
        via = mask_proxy(proxy_url) if fetch_method == "graphql" else "DIRECT(RAPIDAPI)"
        try:
            data = fetch_data(fetch_method, zpid, proxy_url, timeout, ua, trace)
            if not data:
                last_err = "empty"
                raise ValueError("empty payload")
            if sample_keys > 0:
                try:
                    logging.debug(f"Sample keys: {list(data.keys())[:sample_keys]}")
                except Exception:
                    pass
            data.setdefault("zpid", zpid)
            data.setdefault("_fetchedAt", datetime.now(timezone.utc).isoformat())
            data.setdefault("_zipcodeHint", zipcode)
            data.setdefault("_fetchMethod", fetch_method)
            data.setdefault("_proxyUsed", via)
            atomic_write_json(out_path, data)
            if fetch_method == "graphql" and pool is not None and proxy_url:
                pool.report_good(proxy_url)
            return 1, f"ok ({via})"
        except ProxyRoundsExhausted:
            raise
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            last_err = str(e)
            logging.debug(f"HTTP/NET error zpid={zpid}: {e}")
            if fetch_method == "graphql" and pool is not None and proxy_url:
                pool.report_bad(proxy_url, reason=last_err)
        except Exception as e:
            last_err = f"other:{e}"
            logging.debug(f"Other error zpid={zpid}: {e}")
        sleep_for = delay + rng.uniform(0, jitter)
        delay *= 2
        if attempt < max_retries:
            logging.debug(f"Backoff sleeping {sleep_for:.2f}s before retry")
            time.sleep(sleep_for)

    return 2, f"fail after {max_retries} attempts; last_err={last_err}"

# ───────────────────────────── Main ─────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Dual-mode Zillow scraper (GraphQL with proxies + RapidAPI).")
    ap.add_argument("--zpid-csv-file", required=True, type=Path)
    ap.add_argument("--out-root", required=True, type=Path)
    ap.add_argument("--fetch-method", choices=["graphql", "rapidapi"], default="graphql")
    ap.add_argument("--proxies-file", type=Path, help="GraphQL only: one proxy URL per line")
    ap.add_argument("--proxy-health-file", type=Path)
    ap.add_argument("--bad-cooldown-seconds", type=float, default=3600.0)
    ap.add_argument("--health-autosave-seconds", type=float, default=5.0)

    ap.add_argument("--unique-per-interval", action="store_true")
    ap.add_argument("--interval-sleep-seconds", type=float, default=7200.0)
    ap.add_argument("--max-rounds", type=int, default=0, help="0 = unlimited")
    ap.add_argument("--sleep-every-rounds", type=int, default=1)

    ap.add_argument("--only-zip", nargs="*", default=[])
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dedupe", action="store_true")
    ap.add_argument("--csv-has-header", action="store_true")

    g = ap.add_mutually_exclusive_group()
    g.add_argument("--overwrite", action="store_true")
    g.add_argument("--skip-if-exists", action="store_true")

    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--max-retries", type=int, default=2)
    ap.add_argument("--base-delay", type=float, default=15)
    ap.add_argument("--jitter", type=float, default=12)
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--user-agents-file", type=Path)
    ap.add_argument("--trace", action="store_true")
    ap.add_argument("--trace-render-chars", type=int, default=400)
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = ap.parse_args()

    configure_logging(args.log_level)
    global _trace_render_chars
    _trace_render_chars = max(50, int(args.trace_render_chars))

    # Health store
    health_store = None
    if args.proxy_health_file:
        health_store = ProxyHealthStore(args.proxy_health_file, autosave_seconds=args.health_autosave_seconds)
        logging.info(f"Proxy health persistence: {args.proxy_health_file}")

    # Proxies (GraphQL only)
    pool: Optional[ProxyPool] = None
    if args.fetch_method == "graphql":
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
                    logging.info(f"All {len(proxies_list)} proxies cooling down; pool will wait.")
            pool = ProxyPool(
                proxies_list,
                cooldown_seconds=args.bad_cooldown_seconds,
                health=health_store,
                unique_per_interval=args.unique_per_interval,
                interval_sleep_seconds=args.interval_sleep_seconds,
                max_rounds=(None if args.max_rounds <= 0 else args.max_rounds),
                sleep_every_rounds=max(1, int(args.sleep_every_rounds)),
            )
        else:
            logging.info("GraphQL mode: no proxies provided — using DIRECT connections")
    else:
        logging.info("RapidAPI mode: proxies ignored; global rate limit enforced")

    # User agents
    ua_list = None
    if args.user_agents_file and args.user_agents_file.exists():
        try:
            ua_list = read_lines(args.user_agents_file)
        except Exception as e:
            logging.warning(f"Could not read user agents file: {e}")

    # CSV
    pairs = read_pairs_from_csv(
        csv_path=args.zpid_csv_file,
        only_zip=args.only_zip,
        limit=args.limit,
        dedupe=args.dedupe,
        has_header=True if args.csv_has_header else None
    )
    if not pairs:
        raise SystemExit("No pairs to process from CSV")

    # Pre-skip existing files (avoids spawning no-op tasks)
    if args.skip_if_exists:
        pre_filtered_pairs = []
        skipped_pairs = 0
        for zipcode, zpid in pairs:
            out_path = args.out_root / zipcode / f"{zpid}.json"
            if out_path.exists():
                skipped_pairs += 1
                logging.debug(f"Pre-skip existing file: {out_path}")
            else:
                pre_filtered_pairs.append((zipcode, zpid))
        if skipped_pairs:
            logging.info(f"Pre-skip: {skipped_pairs} existing items skipped before thread submission.")
        pairs = pre_filtered_pairs

    logging.info("Pairs ready: %d from %s | method=%s | mode=%s",
                 len(pairs), args.zpid_csv_file, args.fetch_method,
                 "SKIP-IF-EXISTS(pre)" if args.skip_if_exists else "OVERWRITE/REFRESH")

    ok = skip = fail = 0
    # Submit tasks
    with ThreadPoolExecutor(max_workers=max(1, args.workers), thread_name_prefix="worker") as ex:
        future_to_pair = {
            ex.submit(
                scrape_one, args.fetch_method, zipcode, zpid, args.out_root, pool,
                args.overwrite,
                args.max_retries, args.base_delay, args.jitter, args.timeout,
                ua_list, args.trace, args.sample
            ): (zipcode, zpid) for zipcode, zpid in pairs
        }
        try:
            for fut in as_completed(future_to_pair):
                zipcode, zpid = future_to_pair[fut]
                try:
                    status, msg = fut.result()
                except ProxyRoundsExhausted:
                    logging.warning("Max rounds reached — cancelling remaining tasks and exiting.")
                    # cancel everything else
                    for f in future_to_pair:
                        if f is not fut and not f.done():
                            f.cancel()
                    if health_store:
                        health_store.flush()
                    raise SystemExit(0)
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
        finally:
            pass

    logging.info("Done. ok=%d skipped=%d failed=%d", ok, skip, fail)
    if health_store:
        health_store.flush()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.error("Interrupted — exiting.")
        os._exit(130)
