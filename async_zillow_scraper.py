#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, heapq, json, random, time, csv, re, sys, logging
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Union
from html import unescape

import aiohttp
from aiohttp import (
    ClientConnectorError, ClientHttpProxyError, ClientProxyConnectionError,
    ClientResponseError, ServerTimeoutError, ClientTimeout
)

log = logging.getLogger("zillow_async")

# ===================== Utilities =====================
TRACE_MAX = 160

def render_html_to_text(html: str, max_len: int = TRACE_MAX) -> str:
    """Safe, short plain-text rendering for error logs (no raw HTML)."""
    try:
        html = re.sub(r'(?is)<(script|style)[^>]*>.*?</\1>', '', html)
        html = re.sub(r'(?i)</?(br|p|div|li|ul|ol|h[1-6]|header|footer|section|article|tr|td|th)>', '\n', html)
        text = re.sub(r'(?s)<[^>]+>', '', html)
        text = unescape(text)
        text = re.sub(r'[ \t\r\f\v]+', ' ', text)
        text = re.sub(r'\n+', '\n', text).strip()
        return (text[:max_len] + '…') if len(text) > max_len else (text or "(empty body)")
    except Exception:
        return "(unrenderable)"

def short_reason_from_exc(e: Exception) -> str:
    name = e.__class__.__name__
    msg = str(e)
    if len(msg) > 120:
        msg = msg[:120] + "…"
    return f"{name}: {msg}"

# ===================== Proxy health persistence =====================
class ProxyHealthFile:
    """
    Persist proxy next-available times & counters across runs.
    JSON shape:
    { "<proxy_url>": {"next_ts": 1734300000.0, "succ": 3, "fail": 7, "last_reason": "HTTP 429"} }
    """
    def __init__(self, path: Path, max_age_seconds: float = 3*24*3600):
        self.path = path
        self.max_age = max_age_seconds
        self.data: Dict[str, Dict[str, Union[int, float, str]]] = {}
        self._load()

    def _load(self):
        if not self.path.exists():
            self.data = {}
            log.debug("Health: no existing file at %s", self.path)
            return
        try:
            raw = self.path.read_text(encoding="utf-8")
            self.data = json.loads(raw) if raw.strip() else {}
            log.info("Health: loaded %d entries from %s", len(self.data), self.path)
        except Exception as e:
            log.warning("Health: failed to read %s: %s", self.path, e)
            self.data = {}
        now = time.time()
        before = len(self.data)
        self.data = {
            p: rec for p, rec in self.data.items()
            if isinstance(rec, dict) and float(rec.get("next_ts", 0)) > now - self.max_age
        }
        if len(self.data) != before:
            log.debug("Health: pruned %d stale entries", before - len(self.data))

    def _save(self):
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(self.data, ensure_ascii=False, separators=(",", ":"), indent=2), encoding="utf-8")
            tmp.replace(self.path)
            log.debug("Health: saved %d entries to %s", len(self.data), self.path)
        except Exception as e:
            log.warning("Health: failed to save %s: %s", self.path, e)

    def mark_bad(self, proxy: str, cooldown: float, reason: str = ""):
        now = time.time()
        rec = dict(self.data.get(proxy, {}))
        rec["fail"] = int(rec.get("fail", 0)) + 1
        if reason:
            rec["last_reason"] = reason
        rec["next_ts"] = now + float(cooldown)
        self.data[proxy] = rec
        self._save()
        log.debug("Health: BAD %-38s next=%s reason=%s", proxy, time.strftime("%H:%M:%S", time.localtime(rec["next_ts"])), reason)

    def mark_good(self, proxy: str):
        now = time.time()
        rec = dict(self.data.get(proxy, {}))
        rec["succ"] = int(rec.get("succ", 0)) + 1
        rec["next_ts"] = now
        self.data[proxy] = rec
        self._save()
        log.debug("Health: GOOD %-37s succ=%d", proxy, rec["succ"])

    def next_available(self, proxy: str) -> float:
        return float(self.data.get(proxy, {}).get("next_ts", 0.0))

# ===================== Async lock-free proxy pool =====================
class AsyncProxyPool:
    """
    Lock-free proxy scheduler:
      - Min-heap of (available_at_loop_time, proxy)
      - Per-round usage cap (uses_per_round)
      - Sleep round_sleep when all proxies hit the cap, then reset counters
      - Integrates with ProxyHealthFile to persist cooldowns across restarts
    """
    def __init__(self, proxies: List[str], cooldown: float = 300.0,
                 uses_per_round: int = 1, round_sleep: float = 900.0,
                 health_file: Optional[ProxyHealthFile] = None):
        if not proxies:
            raise ValueError("No proxies supplied.")
        self.cooldown = float(cooldown)
        self.uses_per_round = max(1, int(uses_per_round))
        self.round_sleep = max(0.0, float(round_sleep))
        self.health = health_file

        loop = asyncio.get_event_loop()
        now_loop = loop.time()
        random.shuffle(proxies)

        # heap of (available_at_loop_time, proxy)
        self._heap: List[Tuple[float, str]] = []
        for p in proxies:
            t0 = now_loop
            if self.health:
                delta = max(0.0, self.health.next_available(p) - time.time())
                t0 += delta
            heapq.heappush(self._heap, (t0, p))

        # per-round usage counters
        self._used: Dict[str, int] = {p: 0 for p in proxies}

        # simple round reset gate
        self._round_ready = asyncio.Event()
        self._round_ready.set()
        self._round_reset_task: Optional[asyncio.Task] = None

        log.info("Pool: %d proxies, uses/round=%d, base cooldown=%ss, round-sleep=%ss",
                 len(proxies), self.uses_per_round, self.cooldown, self.round_sleep)

    async def _begin_round_reset(self):
        if self._round_reset_task and not self._round_reset_task.done():
            return
        self._round_ready.clear()

        async def _reset():
            if self.round_sleep > 0:
                mins = int(self.round_sleep // 60)
                secs = int(self.round_sleep % 60)
                log.warning("Pool: round quota reached — sleeping %dm %02ds", mins, secs)
                await asyncio.sleep(self.round_sleep)
            # reset per-round counters
            for k in list(self._used.keys()):
                self._used[k] = 0
            log.info("Pool: new round started (counters reset).")
            self._round_ready.set()

        self._round_reset_task = asyncio.create_task(_reset())

    async def acquire(self) -> str:
        """Return next eligible proxy. Sleep (non-blocking) until ready."""
        loop = asyncio.get_event_loop()
        while True:
            await self._round_ready.wait()

            # pick earliest proxy that hasn't exhausted round quota
            now = loop.time()
            best_idx = None
            best_time = None
            for idx, (t, p) in enumerate(self._heap):
                if self._used.get(p, 0) < self.uses_per_round:
                    if best_time is None or t < best_time:
                        best_time, best_idx = t, idx

            if best_idx is not None:
                avail_at, proxy = self._heap[best_idx]
                if avail_at <= now:
                    # pop at index, then restore heap
                    self._heap.pop(best_idx)
                    if best_idx < len(self._heap):
                        heapq.heapify(self._heap)
                    log.debug("Pool.acquire -> %s (used=%d/%d in round)",
                              proxy, self._used.get(proxy, 0), self.uses_per_round)
                    return proxy
                # wait until earliest eligible becomes available
                sleep_for = min(60.0, max(0.05, avail_at - now))
                log.debug("Pool: next eligible in %.2fs (proxy=%s)", sleep_for, self._heap[best_idx][1])
                await asyncio.sleep(sleep_for)
                continue

            # no proxy with remaining quota → start round reset
            log.info("Pool: no proxies with remaining per-round quota.")
            await self._begin_round_reset()

    def _push(self, when: float, proxy: str):
        heapq.heappush(self._heap, (when, proxy))

    def report_good(self, proxy: str):
        now = asyncio.get_event_loop().time()
        self._used[proxy] = self._used.get(proxy, 0) + 1
        self._push(now, proxy)
        if self.health:
            self.health.mark_good(proxy)
        log.debug("Pool.report_good %s (count=%d)", proxy, self._used[proxy])

    def report_bad(self, proxy: str, cooldown: float, reason: str = ""):
        now = asyncio.get_event_loop().time()
        self._used[proxy] = self._used.get(proxy, 0) + 1
        self._push(now + float(cooldown), proxy)
        if self.health:
            self.health.mark_bad(proxy, cooldown=float(cooldown), reason=reason)
        log.debug("Pool.report_bad %s cooldown=%.1fs reason=%s (count=%d)",
                  proxy, cooldown, reason, self._used[proxy])

# ===================== Zillow fetch (GraphQL) =====================
PERSISTED_HASH = "db712c032dc3f55dee0742b5f2b20db1025fd209bee693c54eb145d6c6c8fa5c"

async def fetch_graphql(session: aiohttp.ClientSession,
                        proxy_url: Optional[str],
                        zpid: Union[int, str],
                        timeout: float,
                        user_agent: Optional[str]) -> Dict:
    params = {
        "extensions": json.dumps({"persistedQuery": {"version": 1, "sha256Hash": PERSISTED_HASH}}, separators=(",", ":")),
        "variables": json.dumps({"zpid": int(zpid)}, separators=(",", ":")),
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
    url = "https://www.zillow.com/graphql/"
    log.debug("HTTP GET %s zpid=%s via %s", url, zpid, proxy_url or "DIRECT")

    async with session.get(
        url, params=params, headers=headers,
        proxy=proxy_url, allow_redirects=True,
        timeout=ClientTimeout(total=timeout)
    ) as resp:
        status = resp.status
        if status != 200:
            txt = await resp.text()
            snippet = render_html_to_text(txt)
            log.debug("HTTP %s zpid=%s via %s :: %s", status, zpid, proxy_url, snippet)
            raise ClientResponseError(
                request_info=resp.request_info,
                history=resp.history,
                status=status,
                message=snippet,
                headers=resp.headers
            )
        data = await resp.json()
        log.debug("HTTP 200 zpid=%s via %s (ok)", zpid, proxy_url)
        return data["data"]["property"]

# ===================== Error-sensitive cooldown policy =====================
def cooldown_for_error(exc: Exception,
                       base_cooldown: float,
                       mult_429_403: float = 6.0,
                       mult_timeout_conn: float = 1.5) -> float:
    """
    Longer cooldown for 429/403 (rate limiting / forbidden), shorter for timeouts/connect errors.
    Fallback to base_cooldown for everything else.
    """
    # HTTP 429 / 403
    if isinstance(exc, ClientResponseError):
        if exc.status in (429, 403):
            cd = base_cooldown * mult_429_403
            log.debug("Cooldown policy: 429/403 -> %.1fs", cd)
            return cd
        # 5xx: short backoff (serverside hiccup)
        if 500 <= exc.status <= 599:
            cd = base_cooldown * mult_timeout_conn
            log.debug("Cooldown policy: 5xx -> %.1fs", cd)
            return cd

    # connection / proxy / timeout family → short
    if isinstance(exc, (ClientConnectorError, ClientProxyConnectionError, ClientHttpProxyError, ServerTimeoutError, asyncio.TimeoutError)):
        cd = base_cooldown * mult_timeout_conn
        log.debug("Cooldown policy: connect/timeout -> %.1fs", cd)
        return cd

    # default
    log.debug("Cooldown policy: default -> %.1fs", base_cooldown)
    return base_cooldown

# ===================== Worker =====================
async def worker(name: int,
                 pool: AsyncProxyPool,
                 jobs: asyncio.Queue,
                 out_root: Path,
                 timeout: float,
                 ua_list: Optional[List[str]],
                 max_retries: int,
                 base_cooldown: float):
    rnd = random.Random((name << 32) ^ time.time_ns())
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as sess:
        log.info("Worker-%d: started", name)
        while True:
            item = await jobs.get()
            if item is None:
                jobs.task_done()
                log.info("Worker-%d: exiting", name)
                return
            zipcode, zpid = item
            out_path = out_root / zipcode / f"{zpid}.json"
            tries = 0
            while tries < max_retries:
                tries += 1
                proxy = await pool.acquire()
                ua = rnd.choice(ua_list) if ua_list else None
                log.debug("Worker-%d attempt %d zpid=%s via=%s UA=%s",
                          name, tries, zpid, proxy, (ua or "default"))
                try:
                    data = await fetch_graphql(sess, proxy, zpid, timeout, ua)
                    data.setdefault("zpid", zpid)
                    data.setdefault("_proxyUsed", proxy)
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
                    pool.report_good(proxy)
                    log.info("OK %s/%s via=%s", zipcode, zpid, proxy)
                    break
                except Exception as e:
                    reason = short_reason_from_exc(e)
                    cd = cooldown_for_error(e, base_cooldown)
                    pool.report_bad(proxy, cooldown=cd, reason=reason)
                    log.warning("FAIL zpid=%s via=%s reason=%s (cooldown=%.1fs)", zpid, proxy, reason, cd)
                    await asyncio.sleep(rnd.uniform(0.3, 1.2))
            jobs.task_done()

# ===================== CSV → queue (streaming) =====================
async def stream_csv_to_queue(csv_file: Path,
                              out_root: Path,
                              jobs: asyncio.Queue,
                              only_zip: Optional[List[str]],
                              limit: Optional[int],
                              dedupe: bool,
                              has_header: Optional[bool],
                              skip_if_exists: bool):
    seen = set() if dedupe else None
    only = set(only_zip) if only_zip else None
    submitted = 0
    with csv_file.open("r", encoding="utf-8", newline="") as f:
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
                        log.debug("CSV: auto-skip header: %s", row)
                        continue
                elif has_header:
                    log.debug("CSV: skip header row (flag set): %s", row)
                    continue
            zipcode, zpid_raw = row[0].strip(), row[1].strip()
            if only and zipcode not in only:
                continue
            zpid = int(zpid_raw) if zpid_raw.isdigit() else zpid_raw
            if dedupe:
                key = (zipcode, zpid)
                if key in seen:
                    continue
                seen.add(key)
            if skip_if_exists and (out_root / zipcode / f"{zpid}.json").exists():
                continue
            await jobs.put((zipcode, zpid))  # bounded queue size → low memory
            submitted += 1
            if submitted % 100 == 0:
                log.info("CSV: queued %d items so far…", submitted)
            if limit and submitted >= limit:
                break
    log.info("CSV: queued total %d items", submitted)

# ===================== Main orchestrator =====================
async def main_async(csv_file: Path,
                     out_root: Path,
                     proxies_file: Path,
                     workers: int = 100,
                     cooldown: float = 300.0,
                     uses_per_round: int = 1,
                     round_sleep: float = 900.0,
                     timeout: float = 20.0,
                     user_agents_file: Optional[Path] = None,
                     skip_if_exists: bool = True,
                     only_zip: Optional[List[str]] = None,
                     limit: Optional[int] = None,
                     dedupe: bool = False,
                     has_header: Optional[bool] = None,
                     max_retries: int = 1,
                     proxy_health_file: Optional[Path] = None):
    # Proxies
    proxies = [ln.strip() for ln in proxies_file.read_text(encoding="utf-8").splitlines()
               if ln.strip() and not ln.strip().startswith("#")]
    log.info("Start: proxies=%d workers=%d cooldown=%ss uses/round=%d round-sleep=%ss",
             len(proxies), workers, cooldown, uses_per_round, round_sleep)

    # Health persistence
    health = ProxyHealthFile(proxy_health_file) if proxy_health_file else None

    # Pool
    pool = AsyncProxyPool(
        proxies,
        cooldown=cooldown,
        uses_per_round=uses_per_round,
        round_sleep=round_sleep,
        health_file=health
    )

    # User agents
    ua_list = None
    if user_agents_file and user_agents_file.exists():
        ua_list = [ln.strip() for ln in user_agents_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
        log.info("User-Agents: %d loaded", len(ua_list))

    # Quiet down aiohttp unless DEBUG is requested
    aiohttp_log = logging.getLogger("aiohttp.client")
    if log.level > logging.DEBUG:
        aiohttp_log.setLevel(logging.WARNING)

    # Bounded job queue
    jobs: asyncio.Queue = asyncio.Queue(maxsize=max(1, workers * 2))
    tasks = [asyncio.create_task(worker(i, pool, jobs, out_root, timeout, ua_list, max_retries, cooldown))
             for i in range(workers)]
    log.info("Workers started: %d", len(tasks))

    await stream_csv_to_queue(csv_file, out_root, jobs, only_zip, limit, dedupe, has_header, skip_if_exists)

    await jobs.join()
    for _ in tasks:
        await jobs.put(None)
    await asyncio.gather(*tasks)
    log.info("All workers finished.")

# ===================== CLI =====================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Async Zillow GraphQL scraper with per-round quotas & persistent proxy health (verbose).")
    parser.add_argument("--zpid-csv-file", type=Path, required=True)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument("--proxies-file", type=Path, required=True)
    parser.add_argument("--proxy-health-file", type=Path)

    parser.add_argument("--workers", type=int, default=120, help="concurrent workers (async tasks)")
    parser.add_argument("--cooldown-seconds", type=float, default=300.0, help="base cooldown for a failed proxy")
    parser.add_argument("--uses-per-round", type=int, default=1, help="max uses per proxy per round")
    parser.add_argument("--round-sleep-seconds", type=float, default=900.0, help="sleep when all proxies hit per-round cap")
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--user-agents-file", type=Path)
    parser.add_argument("--skip-if-exists", action="store_true")
    parser.add_argument("--only-zip", nargs="*", default=[])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dedupe", action="store_true")
    parser.add_argument("--csv-has-header", action="store_true")
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        asyncio.run(main_async(
            csv_file=args.zpid_csv_file,
            out_root=args.out_root,
            proxies_file=args.proxies_file,
            workers=args.workers,
            cooldown=args.cooldown_seconds,
            uses_per_round=args.uses_per_round,
            round_sleep=args.round_sleep_seconds,
            timeout=args.timeout,
            user_agents_file=args.user_agents_file,
            skip_if_exists=args.skip_if_exists,
            only_zip=(args.only_zip or None),
            limit=args.limit,
            dedupe=args.dedupe,
            has_header=True if args.csv_has_header else None,
            max_retries=args.max_retries,
            proxy_health_file=args.proxy_health_file,
        ))
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
