import os
import io
import csv
import json
import time
import math
import zipfile
import sqlite3
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Optional

import aiohttp
from aiohttp import web
import websockets
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# POLYBTC SNIPER LAB — PAPER ONLY
# Adaptation of the public polybtc stale-quote research thesis.
# Four independent $500 paper accounts, same market/Binance snapshot.
# ============================================================

VERSION = "1.0-polybtc-sniper-lab-paper"
HOST = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_SPOT_WS = os.getenv(
    "BINANCE_SPOT_WS",
    "wss://data-stream.binance.vision/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade",
)
BINANCE_PERP_WS = os.getenv(
    "BINANCE_PERP_WS",
    "wss://fstream.binance.com/market/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade",
)
BINANCE_DATA_API = os.getenv("BINANCE_DATA_API", "https://data-api.binance.vision")
BINANCE_SYMBOL = os.getenv("BINANCE_SYMBOL", "BTCUSDT").upper()

PORT = int(os.getenv("PORT", "8080"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    p = DATA_DIR / ".write_test"
    p.write_text("ok")
    p.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "polybtc_sniper_lab_v1.db"
REPORT_DIR = DATA_DIR / "sniper_lab_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

PAPER_START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "500"))
MIN_FREE_CASH = float(os.getenv("MIN_FREE_CASH", "5"))
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "1500"))
MAX_FEED_AGE_MS = int(os.getenv("MAX_FEED_AGE_MS", "1500"))
DECISION_INTERVAL = float(os.getenv("DECISION_INTERVAL", "0.25"))
PAPER_TAKER_LATENCY_MS = int(os.getenv("PAPER_TAKER_LATENCY_MS", "400"))
ATTEMPT_COOLDOWN_MS = int(os.getenv("ATTEMPT_COOLDOWN_MS", "1200"))
MEMORY_KEEP_RESOLVED_SEC = int(os.getenv("MEMORY_KEEP_RESOLVED_SEC", "1800"))

# Source-backed research knobs.
ROBUST_BETAS = tuple(float(x) for x in os.getenv("ROBUST_BETAS", "0.83,1.36").split(","))
LATE_MODEL_WEIGHT = float(os.getenv("LATE_MODEL_WEIGHT", "0.95"))
TAIL_WEIGHT = float(os.getenv("TAIL_WEIGHT", "0.15"))
TAIL_SCALE = float(os.getenv("TAIL_SCALE", "2.5"))
VOL_FAST_WINDOW_SEC = int(os.getenv("VOL_FAST_WINDOW_SEC", "60"))
VOL_SLOW_WINDOW_SEC = int(os.getenv("VOL_SLOW_WINDOW_SEC", "600"))
VOL_FAST_HALFLIFE_SEC = float(os.getenv("VOL_FAST_HALFLIFE_SEC", "30"))
VOL_SLOW_HALFLIFE_SEC = float(os.getenv("VOL_SLOW_HALFLIFE_SEC", "300"))
MIN_VOL_PER_SQRT_SEC = float(os.getenv("MIN_VOL_PER_SQRT_SEC", "0.00002"))
BASIS_WINDOW_SEC = int(os.getenv("BASIS_WINDOW_SEC", "60"))

# Four clean A/B/C/D experiments. Each has its own $500 account.
STRATEGIES = [
    {
        "name": "S5_R10",
        "short": "A / 5m robust edge>=10c",
        "kind": "5m",
        "leg": "snipe",
        "min_tau": 1.0,
        "max_tau": 60.0,
        "min_edge": 0.10,
        "max_edge": 0.20,
        "min_ask": 0.50,
        "max_ask": 0.70,
        "min_take_usd": 10.0,
        "max_take_usd": 25.0,
        "max_market_usd": 50.0,
    },
    {
        "name": "S5_R15",
        "short": "B / 5m robust edge>=15c",
        "kind": "5m",
        "leg": "snipe",
        "min_tau": 1.0,
        "max_tau": 60.0,
        "min_edge": 0.15,
        "max_edge": 0.20,
        "min_ask": 0.50,
        "max_ask": 0.70,
        "min_take_usd": 10.0,
        "max_take_usd": 25.0,
        "max_market_usd": 50.0,
    },
    {
        "name": "S15_R10",
        "short": "C / 15m robust edge>=10c",
        "kind": "15m",
        "leg": "snipe",
        "min_tau": 1.0,
        "max_tau": 60.0,
        "min_edge": 0.10,
        "max_edge": 0.20,
        "min_ask": 0.50,
        "max_ask": 0.70,
        "min_take_usd": 10.0,
        "max_take_usd": 25.0,
        "max_market_usd": 50.0,
    },
    {
        "name": "SCALP15",
        "short": "D / 15m end-window scalp",
        "kind": "15m",
        "leg": "scalp",
        "min_tau": 5.0,
        "max_tau": 30.0,
        "min_prob": 0.997,
        "min_ask": 0.90,
        "max_ask": 0.99,
        "min_take_usd": 10.0,
        "max_take_usd": 20.0,
        "max_market_usd": 20.0,
    },
]
STRATEGY_BY_NAME = {s["name"]: s for s in STRATEGIES}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("polybtc-sniper-lab")

session: Optional[aiohttp.ClientSession] = None

# Shared Polymarket state.
books = {}
markets = {}
subscribed_assets = set()
ws_send_queue: asyncio.Queue = asyncio.Queue()

# Binance spot/perp state.
spot_bid = None
spot_ask = None
spot_last = None
spot_last_ms = 0
perp_bid = None
perp_ask = None
perp_last = None
perp_last_ms = 0
spot_second_prices = deque(maxlen=1800)  # (sec, price)
basis_samples = deque(maxlen=600)       # (ms, perp_mid - spot_mid)
market_open_cache = {}

# Execution state.
inflight = set()  # (strategy, condition)
last_attempt_ms = defaultdict(int)
last_signature = {}
spent_cache = {}
settle_lock = asyncio.Lock()

# ============================================================
# Helpers
# ============================================================

def now_ts():
    return int(time.time())

def now_ms():
    return int(time.time() * 1000)

def utc_iso(ts=None):
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()

def sf(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d

def si(v, d=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return d

def jd(v):
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))

def parse_jsonish(v):
    if isinstance(v, list):
        return v
    if v is None:
        return []
    try:
        x = json.loads(v)
        return x if isinstance(x, list) else []
    except Exception:
        return []

def parse_iso(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")) if s else None
    except Exception:
        return None

def normal_cdf(x):
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))

def fee_per_share(price):
    p = float(price)
    return 0.07 * p * (1.0 - p)

def fee_usdc(shares, price):
    fee = float(shares) * fee_per_share(price)
    return round(fee, 5) if fee >= 0.000005 else 0.0

def cash_key(strategy_name):
    return f"paper_cash::{strategy_name}"

def initial_key(strategy_name):
    return f"paper_initial::{strategy_name}"

# ============================================================
# Database
# ============================================================

def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    return c

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS state(
          key TEXT PRIMARY KEY,
          value TEXT
        );

        CREATE TABLE IF NOT EXISTS markets(
          condition_id TEXT PRIMARY KEY,
          kind TEXT,
          duration_sec INTEGER,
          question TEXT,
          slug TEXT,
          start_ts INTEGER,
          end_ts INTEGER,
          up_asset TEXT,
          down_asset TEXT,
          btc_open REAL,
          resolved INTEGER DEFAULT 0,
          winning_asset TEXT,
          winning_outcome TEXT
        );

        CREATE TABLE IF NOT EXISTS signals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_ms INTEGER,
          strategy TEXT,
          condition_id TEXT,
          kind TEXT,
          outcome TEXT,
          asset TEXT,
          tau_sec REAL,
          ask_before REAL,
          market_mid REAL,
          market_up_mid REAL,
          model_up_lowbeta REAL,
          model_up_highbeta REAL,
          robust_side_prob REAL,
          net_edge REAL,
          fee_per_share REAL,
          btc_open REAL,
          spot_price REAL,
          perp_price REAL,
          effective_price REAL,
          effective_source TEXT,
          sigma_fast REAL,
          sigma_slow REAL,
          sigma_used REAL,
          accepted INTEGER,
          filled INTEGER,
          reason TEXT
        );

        CREATE TABLE IF NOT EXISTS trades(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          trade_ms INTEGER,
          mode TEXT,
          strategy TEXT,
          condition_id TEXT,
          kind TEXT,
          asset TEXT,
          outcome TEXT,
          signal_type TEXT,
          requested_usd REAL,
          filled_shares REAL,
          avg_price REAL,
          gross_cost REAL,
          fee REAL,
          total_cost REAL,
          cash_before REAL,
          cash_after REAL,
          tau_sec REAL,
          robust_prob REAL,
          net_edge REAL,
          effective_price REAL,
          effective_source TEXT,
          sigma_used REAL,
          latency_ms INTEGER,
          fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS results(
          condition_id TEXT,
          strategy TEXT,
          mode TEXT,
          kind TEXT,
          winning_asset TEXT,
          winning_outcome TEXT,
          total_cost REAL,
          payout REAL,
          pnl REAL,
          trades INTEGER,
          settled_ms INTEGER,
          PRIMARY KEY(condition_id,strategy,mode)
        );

        CREATE INDEX IF NOT EXISTS idx_signals_ms ON signals(signal_ms);
        CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy,signal_ms);
        CREATE INDEX IF NOT EXISTS idx_trades_ms ON trades(trade_ms);
        CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy,trade_ms);
        CREATE INDEX IF NOT EXISTS idx_results_strategy ON results(strategy,settled_ms);
        """)
        for s in STRATEGIES:
            name = s["name"]
            if c.execute("SELECT 1 FROM state WHERE key=?", (cash_key(name),)).fetchone() is None:
                c.execute("INSERT INTO state(key,value) VALUES(?,?)", (cash_key(name), str(PAPER_START_BALANCE)))
            if c.execute("SELECT 1 FROM state WHERE key=?", (initial_key(name),)).fetchone() is None:
                c.execute("INSERT INTO state(key,value) VALUES(?,?)", (initial_key(name), str(PAPER_START_BALANCE)))
        if c.execute("SELECT 1 FROM state WHERE key='trading_enabled'").fetchone() is None:
            c.execute("INSERT INTO state(key,value) VALUES('trading_enabled','0')")
        if c.execute("SELECT 1 FROM state WHERE key='last_report_end'").fetchone() is None:
            current_hour = (now_ts() // 3600) * 3600
            c.execute("INSERT INTO state(key,value) VALUES('last_report_end',?)", (str(current_hour),))
        c.commit()

def state_get(k, d=None):
    with db() as c:
        r = c.execute("SELECT value FROM state WHERE key=?", (k,)).fetchone()
        return r["value"] if r else d

def state_set(k, v):
    with db() as c:
        c.execute(
            "INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (k, str(v)),
        )
        c.commit()

def paper_cash(strategy):
    return sf(state_get(cash_key(strategy), PAPER_START_BALANCE))

def paper_initial(strategy):
    return sf(state_get(initial_key(strategy), PAPER_START_BALANCE))

def set_paper_cash(strategy, value):
    state_set(cash_key(strategy), value)

def trading_enabled():
    return state_get("trading_enabled", "0") == "1"

# ============================================================
# HTTP / market discovery
# ============================================================

async def get_json(url, params=None, timeout=12):
    for i in range(3):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                txt = await r.text()
                if r.status == 200:
                    return json.loads(txt)
                log.warning("HTTP %s %s -> %s", r.status, url, txt[:160])
        except Exception as e:
            log.warning("GET failed %s: %s", url, e)
        await asyncio.sleep(0.3 * (i + 1))
    return None

def slot_start_from_slug(slug):
    try:
        return int(str(slug).rstrip("/").split("-")[-1])
    except Exception:
        return None

async def fetch_event_by_slug(slug):
    for url, params in (
        (f"{GAMMA}/events/slug/{slug}", None),
        (f"{GAMMA}/events", {"slug": slug}),
    ):
        d = await get_json(url, params)
        if isinstance(d, dict):
            return d
        if isinstance(d, list) and d and isinstance(d[0], dict):
            return d[0]
    return None

def parse_market(raw, event, kind, duration):
    if not isinstance(raw, dict):
        return None
    cid = str(raw.get("conditionId") or raw.get("condition_id") or "")
    if not cid:
        return None
    title = str(raw.get("question") or raw.get("title") or event.get("title") or "")
    slug = str(raw.get("slug") or event.get("slug") or "")
    text = (title + " " + slug).lower()
    if "bitcoin" not in text and "btc" not in text:
        return None
    outcomes = [str(x).strip().upper() for x in parse_jsonish(raw.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(raw.get("clobTokenIds"))]
    if len(tokens) < 2:
        return None
    up = down = None
    for i, o in enumerate(outcomes):
        if i >= len(tokens):
            break
        if o in {"UP", "YES"}:
            up = tokens[i]
        elif o in {"DOWN", "NO"}:
            down = tokens[i]
    up = up or tokens[0]
    down = down or tokens[1]
    start = slot_start_from_slug(slug)
    if not start:
        dt = parse_iso(raw.get("startDate")) or parse_iso(event.get("startDate"))
        start = int(dt.timestamp()) if dt else None
    if not start:
        return None
    return {
        "condition_id": cid,
        "kind": kind,
        "duration_sec": duration,
        "question": title,
        "slug": slug,
        "start_ts": start,
        "end_ts": start + duration,
        "up_asset": up,
        "down_asset": down,
        "btc_open": None,
    }

async def discover_slot(kind, duration, slot):
    slug = f"btc-updown-{kind}-{slot}"
    ev = await fetch_event_by_slug(slug)
    if not ev or not isinstance(ev.get("markets"), list):
        return None
    for raw in ev["markets"]:
        m = parse_market(raw, ev, kind, duration)
        if m:
            return m
    return None

async def fetch_binance_open(start_ts):
    if start_ts in market_open_cache:
        return market_open_cache[start_ts]
    url = f"{BINANCE_DATA_API}/api/v3/klines"
    d = await get_json(
        url,
        {
            "symbol": BINANCE_SYMBOL,
            "interval": "1m",
            "startTime": int(start_ts) * 1000,
            "limit": 1,
        },
    )
    if isinstance(d, list) and d and isinstance(d[0], list) and len(d[0]) >= 2:
        px = sf(d[0][1])
        if px > 0:
            market_open_cache[start_ts] = px
            return px
    # Runtime fallback: closest sampled second within 3 seconds of open.
    best = None
    best_dt = None
    for sec, px in spot_second_prices:
        dt = abs(sec - int(start_ts))
        if dt <= 3 and (best_dt is None or dt < best_dt):
            best, best_dt = px, dt
    if best:
        market_open_cache[start_ts] = best
    return best

def persist_market(m):
    with db() as c:
        c.execute(
            """INSERT INTO markets(condition_id,kind,duration_sec,question,slug,start_ts,end_ts,up_asset,down_asset,btc_open)
               VALUES(?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(condition_id) DO UPDATE SET
                 kind=excluded.kind,duration_sec=excluded.duration_sec,question=excluded.question,
                 slug=excluded.slug,start_ts=excluded.start_ts,end_ts=excluded.end_ts,
                 up_asset=excluded.up_asset,down_asset=excluded.down_asset,
                 btc_open=COALESCE(markets.btc_open,excluded.btc_open)""",
            (
                m["condition_id"], m["kind"], m["duration_sec"], m["question"], m["slug"],
                m["start_ts"], m["end_ts"], m["up_asset"], m["down_asset"], m.get("btc_open"),
            ),
        )
        c.commit()

async def subscribe_asset(asset):
    if asset and asset not in subscribed_assets:
        subscribed_assets.add(asset)
        await ws_send_queue.put({"operation": "subscribe", "assets_ids": [asset]})

async def discovery_loop():
    specs = (("5m", 300), ("15m", 900))
    while True:
        try:
            n = now_ts()
            for kind, duration in specs:
                cur = (n // duration) * duration
                for slot in (cur - duration, cur, cur + duration):
                    m = await discover_slot(kind, duration, slot)
                    if not m:
                        continue
                    cid = m["condition_id"]
                    if cid not in markets:
                        m["btc_open"] = await fetch_binance_open(m["start_ts"])
                        markets[cid] = m
                        persist_market(m)
                        await subscribe_asset(m["up_asset"])
                        await subscribe_asset(m["down_asset"])
                        log.info(
                            "MARKET %-3s %s | open=%s | %s",
                            kind, m["slug"],
                            f"{m['btc_open']:.2f}" if m.get("btc_open") else "NONE",
                            utc_iso(m["start_ts"]),
                        )
                    elif not markets[cid].get("btc_open"):
                        px = await fetch_binance_open(m["start_ts"])
                        if px:
                            markets[cid]["btc_open"] = px
                            persist_market(markets[cid])
        except Exception:
            log.exception("Discovery failed")
        await asyncio.sleep(8)

# ============================================================
# Polymarket order book
# ============================================================

def level_map(rows):
    out = {}
    for x in rows or []:
        if isinstance(x, dict):
            p = sf(x.get("price"), math.nan)
            q = sf(x.get("size"), 0)
            if not math.isnan(p) and q > 0:
                out[p] = q
    return out

def apply_book(asset, payload, source="ws"):
    books[asset] = {
        "bids": level_map(payload.get("bids")),
        "asks": level_map(payload.get("asks")),
        "received_ms": now_ms(),
        "source": source,
    }

def apply_price_change(payload):
    recv = now_ms()
    for ch in payload.get("price_changes") or payload.get("priceChanges") or []:
        if not isinstance(ch, dict):
            continue
        asset = str(ch.get("asset_id") or ch.get("token_id") or ch.get("tokenId") or "")
        if not asset:
            continue
        b = books.setdefault(asset, {"bids": {}, "asks": {}, "received_ms": recv, "source": "delta"})
        p = sf(ch.get("price"), math.nan)
        q = sf(ch.get("size"), 0)
        side = str(ch.get("side", "")).upper()
        if math.isnan(p):
            continue
        target = b["bids"] if side == "BUY" else b["asks"]
        if q <= 0:
            target.pop(p, None)
        else:
            target[p] = q
        b["received_ms"] = recv

def best_ask(asset):
    b = books.get(asset)
    return min(b["asks"]) if b and b["asks"] else None

def best_bid(asset):
    b = books.get(asset)
    return max(b["bids"]) if b and b["bids"] else None

def book_mid(asset):
    bid = best_bid(asset)
    ask = best_ask(asset)
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    if ask is not None:
        return ask
    if bid is not None:
        return bid
    return None

async def refresh_book(asset):
    d = await get_json(f"{HOST}/book", {"token_id": asset})
    if isinstance(d, dict):
        apply_book(asset, d, "rest")
        return True
    return False

async def ensure_book(asset):
    b = books.get(asset)
    if b and b["asks"] and now_ms() - b["received_ms"] <= MAX_BOOK_AGE_MS:
        return now_ms() - b["received_ms"]
    await refresh_book(asset)
    b = books.get(asset)
    return now_ms() - b["received_ms"] if b else None

def parse_ws(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    if raw in ("", "PING", "PONG"):
        return []
    try:
        x = json.loads(raw)
        return x if isinstance(x, list) else [x]
    except Exception:
        return []

async def ws_sender(ws):
    while True:
        msg = await ws_send_queue.get()
        try:
            await ws.send(jd(msg))
        except Exception:
            await ws_send_queue.put(msg)
            return

async def ws_ping(ws):
    while True:
        try:
            await ws.send("PING")
        except Exception:
            return
        await asyncio.sleep(10)

async def poly_ws_loop():
    while True:
        try:
            if not subscribed_assets:
                await asyncio.sleep(1)
                continue
            async with websockets.connect(POLY_WS, ping_interval=None, close_timeout=5, max_size=20_000_000) as ws:
                await ws.send(jd({"assets_ids": list(subscribed_assets), "type": "market", "custom_feature_enabled": True}))
                log.info("POLY WS connected | assets=%d", len(subscribed_assets))
                sender = asyncio.create_task(ws_sender(ws))
                ping = asyncio.create_task(ws_ping(ws))
                try:
                    async for raw in ws:
                        for ev in parse_ws(raw):
                            if not isinstance(ev, dict):
                                continue
                            et = str(ev.get("event_type") or ev.get("type") or "")
                            p = ev.get("payload") if isinstance(ev.get("payload"), dict) else ev
                            if et == "book":
                                a = str(p.get("asset_id") or p.get("token_id") or "")
                                if a:
                                    apply_book(a, p)
                            elif et == "price_change":
                                apply_price_change(p)
                            elif et == "market_resolved":
                                await settle_from_ws(p)
                finally:
                    sender.cancel()
                    ping.cancel()
        except Exception as e:
            log.warning("POLY WS reconnect: %s", e)
            await asyncio.sleep(1)

# ============================================================
# Binance spot + perpetual fair-value state
# ============================================================

def _mid(bid, ask, last=None):
    if bid and ask and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return last if last and last > 0 else None

def current_spot_mid():
    return _mid(spot_bid, spot_ask, spot_last)

def current_perp_mid():
    return _mid(perp_bid, perp_ask, perp_last)

def current_basis():
    cutoff = now_ms() - BASIS_WINDOW_SEC * 1000
    vals = [v for ts, v in basis_samples if ts >= cutoff]
    if not vals:
        return 0.0
    vals.sort()
    n = len(vals)
    return vals[n // 2] if n % 2 else 0.5 * (vals[n // 2 - 1] + vals[n // 2])

def effective_btc_price():
    sm = current_spot_mid()
    pm = current_perp_mid()
    now = now_ms()
    sage = now - spot_last_ms if spot_last_ms else 999999
    page = now - perp_last_ms if perp_last_ms else 999999
    basis = current_basis()

    # Use the perpetual as a faster estimator only when it is fresher.
    if pm and page <= MAX_FEED_AGE_MS and (not sm or page + 20 < sage):
        return pm - basis, "perp_led", sm, pm, sage, page
    if sm and sage <= MAX_FEED_AGE_MS:
        return sm, "spot", sm, pm, sage, page
    if pm and page <= MAX_FEED_AGE_MS:
        return pm - basis, "perp_fallback", sm, pm, sage, page
    return None, "stale", sm, pm, sage, page

async def binance_spot_ws_loop():
    global spot_bid, spot_ask, spot_last, spot_last_ms
    while True:
        try:
            async with websockets.connect(
                BINANCE_SPOT_WS, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=10_000_000
            ) as ws:
                log.info("BINANCE SPOT WS connected")
                async for raw in ws:
                    recv = now_ms()
                    d = json.loads(raw)
                    p = d.get("data", d)
                    stream = str(d.get("stream", "")).lower()
                    if "bookticker" in stream or ("b" in p and "a" in p and "q" not in p):
                        b = sf(p.get("b"))
                        a = sf(p.get("a"))
                        if b > 0 and a > 0:
                            spot_bid, spot_ask, spot_last_ms = b, a, recv
                    elif "aggtrade" in stream or p.get("e") == "aggTrade":
                        px = sf(p.get("p"))
                        if px > 0:
                            spot_last, spot_last_ms = px, recv
        except Exception as e:
            log.warning("BINANCE SPOT reconnect: %s", e)
            await asyncio.sleep(1)

async def binance_perp_ws_loop():
    global perp_bid, perp_ask, perp_last, perp_last_ms
    while True:
        try:
            async with websockets.connect(
                BINANCE_PERP_WS, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=10_000_000
            ) as ws:
                log.info("BINANCE PERP WS connected")
                async for raw in ws:
                    recv = now_ms()
                    d = json.loads(raw)
                    p = d.get("data", d)
                    stream = str(d.get("stream", "")).lower()
                    if "bookticker" in stream or ("b" in p and "a" in p and "q" not in p):
                        b = sf(p.get("b"))
                        a = sf(p.get("a"))
                        if b > 0 and a > 0:
                            perp_bid, perp_ask, perp_last_ms = b, a, recv
                    elif "aggtrade" in stream or p.get("e") == "aggTrade":
                        px = sf(p.get("p"))
                        if px > 0:
                            perp_last, perp_last_ms = px, recv
        except Exception as e:
            log.warning("BINANCE PERP reconnect: %s", e)
            await asyncio.sleep(1)

async def bootstrap_binance_history():
    # 1-second spot closes give the dual-horizon vol estimator an immediate warmup.
    url = f"{BINANCE_DATA_API}/api/v3/klines"
    d = await get_json(url, {"symbol": BINANCE_SYMBOL, "interval": "1s", "limit": 1000})
    if not isinstance(d, list):
        log.warning("BINANCE history bootstrap unavailable; live sampler will warm up")
        return
    for row in d:
        if isinstance(row, list) and len(row) >= 5:
            sec = si(row[0]) // 1000
            px = sf(row[4])
            if sec > 0 and px > 0:
                if spot_second_prices and spot_second_prices[-1][0] == sec:
                    spot_second_prices[-1] = (sec, px)
                else:
                    spot_second_prices.append((sec, px))
    log.info("BINANCE history bootstrap | seconds=%d", len(spot_second_prices))

async def binance_sampler_loop():
    while True:
        try:
            sec = now_ts()
            sm = current_spot_mid()
            pm = current_perp_mid()
            px = sm or pm
            if px:
                if spot_second_prices and spot_second_prices[-1][0] == sec:
                    spot_second_prices[-1] = (sec, px)
                else:
                    spot_second_prices.append((sec, px))
            if sm and pm and now_ms() - spot_last_ms <= 2000 and now_ms() - perp_last_ms <= 2000:
                basis_samples.append((now_ms(), pm - sm))
        except Exception:
            log.exception("Binance sampler failed")
        await asyncio.sleep(1.0)

def _returns_for_window(window_sec):
    cutoff = now_ts() - int(window_sec)
    pts = [(s, p) for s, p in spot_second_prices if s >= cutoff and p > 0]
    if len(pts) < 3:
        return []
    rets = []
    for i in range(1, len(pts)):
        p0, p1 = pts[i - 1][1], pts[i][1]
        if p0 > 0 and p1 > 0:
            rets.append(math.log(p1 / p0))
    return rets

def ewma_std(values, half_life_samples):
    if len(values) < 2:
        return 0.0
    lam = math.exp(math.log(0.5) / max(1.0, float(half_life_samples)))
    mean = 0.0
    var = 0.0
    wsum = 0.0
    w = 1.0
    for x in reversed(values):
        wsum += w
        mean += w * x
        w *= lam
    if wsum <= 0:
        return 0.0
    mean /= wsum
    w = 1.0
    wsum2 = 0.0
    for x in reversed(values):
        var += w * (x - mean) ** 2
        wsum2 += w
        w *= lam
    return math.sqrt(max(0.0, var / max(wsum2, 1e-12)))

def vol_snapshot():
    fast_r = _returns_for_window(VOL_FAST_WINDOW_SEC)
    slow_r = _returns_for_window(VOL_SLOW_WINDOW_SEC)
    fast = ewma_std(fast_r, VOL_FAST_HALFLIFE_SEC)
    slow = ewma_std(slow_r, VOL_SLOW_HALFLIFE_SEC)
    used = max(fast, slow, MIN_VOL_PER_SQRT_SEC)
    ready = len(fast_r) >= 30 and len(slow_r) >= 60
    return fast, slow, used, ready

def mixture_prob(z):
    # Two-regime Gaussian mixture. Wider tail component prevents extreme overconfidence.
    core = normal_cdf(z)
    tail = normal_cdf(z / max(TAIL_SCALE, 1.0001))
    return (1.0 - TAIL_WEIGHT) * core + TAIL_WEIGHT * tail

def fair_up_bounds(effective_price, open_price, sigma, tau_sec, market_up_mid):
    if not effective_price or not open_price or effective_price <= 0 or open_price <= 0:
        return None
    tau = max(float(tau_sec), 0.25)
    denom = max(float(sigma) * math.sqrt(tau), 1e-12)
    d = math.log(float(effective_price) / float(open_price)) / denom
    weight = LATE_MODEL_WEIGHT
    out = []
    for beta in ROBUST_BETAS:
        model = mixture_prob(float(beta) * d)
        blended = weight * model + (1.0 - weight) * float(market_up_mid)
        out.append(max(0.0001, min(0.9999, blended)))
    return out

# ============================================================
# Strategy / paper FAK execution
# ============================================================

def market_spent(strategy_name, cid):
    key = (strategy_name, cid)
    if key not in spent_cache:
        with db() as c:
            row = c.execute(
                "SELECT COALESCE(SUM(total_cost),0) x FROM trades WHERE mode='PAPER' AND strategy=? AND condition_id=?",
                (strategy_name, cid),
            ).fetchone()
        spent_cache[key] = sf(row["x"] if row else 0)
    return spent_cache[key]

def add_market_spent(strategy_name, cid, amount):
    key = (strategy_name, cid)
    spent_cache[key] = market_spent(strategy_name, cid) + float(amount)

def simulate_buy_budget(asset, target_total, max_price):
    b = books.get(asset)
    if not b or not b.get("asks"):
        return [], 0.0, 0.0
    budget = max(0.0, float(target_total))
    fills = []
    total = 0.0
    shares = 0.0
    for p in sorted(b["asks"]):
        if p > max_price + 1e-12:
            break
        q = b["asks"][p]
        per = p + fee_per_share(p)
        if per <= 0:
            continue
        take = min(q, max(0.0, (budget - total) / per))
        if take <= 1e-9:
            break
        fills.append((p, take))
        total += p * take + fee_usdc(take, p)
        shares += take
        if total >= budget - 1e-8:
            break
    return fills, shares, total

def favorite_side(m):
    up_mid = book_mid(m["up_asset"])
    dn_mid = book_mid(m["down_asset"])
    if up_mid is None or dn_mid is None:
        return None
    if up_mid >= dn_mid:
        return m["up_asset"], "Up", up_mid, up_mid
    return m["down_asset"], "Down", dn_mid, up_mid

def store_signal(s, m, outcome, asset, tau, ask, market_mid, up_mid, bounds, robust_prob,
                 edge, fee_ps, open_px, spot_px, perp_px, eff_px, eff_source,
                 vf, vs, vu, accepted, filled, reason):
    low = bounds[0] if bounds else None
    high = bounds[1] if bounds and len(bounds) > 1 else low
    with db() as c:
        c.execute(
            """INSERT INTO signals(
              signal_ms,strategy,condition_id,kind,outcome,asset,tau_sec,ask_before,market_mid,market_up_mid,
              model_up_lowbeta,model_up_highbeta,robust_side_prob,net_edge,fee_per_share,btc_open,
              spot_price,perp_price,effective_price,effective_source,sigma_fast,sigma_slow,sigma_used,
              accepted,filled,reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                now_ms(), s["name"], m["condition_id"], m["kind"], outcome, asset, tau, ask, market_mid, up_mid,
                low, high, robust_prob, edge, fee_ps, open_px, spot_px, perp_px, eff_px, eff_source,
                vf, vs, vu, 1 if accepted else 0, 1 if filled else 0, reason,
            ),
        )
        c.commit()

def update_last_signal_fill(strategy_name, cid, filled, reason):
    with db() as c:
        row = c.execute(
            "SELECT id FROM signals WHERE strategy=? AND condition_id=? ORDER BY id DESC LIMIT 1",
            (strategy_name, cid),
        ).fetchone()
        if row:
            c.execute("UPDATE signals SET filled=?,reason=? WHERE id=?", (1 if filled else 0, reason, row["id"]))
            c.commit()

def account_can_trade(s, cid):
    cash = paper_cash(s["name"])
    free = max(0.0, cash - MIN_FREE_CASH)
    remaining_market = max(0.0, s["max_market_usd"] - market_spent(s["name"], cid))
    return cash, min(free, remaining_market)

def target_take_usd(s, edge):
    if s["leg"] == "snipe":
        scale = min(1.0, max(0.0, float(edge) / max(2.0 * s["min_edge"], 1e-9)))
        return s["max_take_usd"] * scale
    return s["max_take_usd"]

async def execute_candidate(s, m, asset, outcome, tau, ask0, market_mid, up_mid, bounds,
                            robust_prob, edge, open_px, snapshot):
    key = (s["name"], m["condition_id"])
    try:
        # Commit-to-fill latency model: wait, then revalidate the same FAK price.
        await asyncio.sleep(PAPER_TAKER_LATENCY_MS / 1000.0)
        await ensure_book(asset)
        ask1 = best_ask(asset)
        if ask1 is None:
            update_last_signal_fill(s["name"], m["condition_id"], False, "fak_no_book")
            return
        if ask1 > ask0 + 1e-12:
            update_last_signal_fill(s["name"], m["condition_id"], False, f"fak_quote_gone:{ask0:.3f}->{ask1:.3f}")
            return

        cash, budget_cap = account_can_trade(s, m["condition_id"])
        desired = target_take_usd(s, edge)
        budget = min(desired, budget_cap)
        if budget < s["min_take_usd"] - 1e-9:
            update_last_signal_fill(s["name"], m["condition_id"], False, "account_or_market_cap")
            return

        fills, shares, total = simulate_buy_budget(asset, budget, ask0)
        if shares <= 1e-8 or total < s["min_take_usd"] - 1e-6:
            update_last_signal_fill(s["name"], m["condition_id"], False, f"fak_insufficient_depth:${total:.2f}")
            return

        gross = sum(p * q for p, q in fills)
        fee = sum(fee_usdc(q, p) for p, q in fills)
        avg = gross / shares
        after = cash - total
        if after < -1e-7:
            update_last_signal_fill(s["name"], m["condition_id"], False, "cash_race")
            return

        with db() as c:
            c.execute(
                """INSERT INTO trades(
                  trade_ms,mode,strategy,condition_id,kind,asset,outcome,signal_type,requested_usd,
                  filled_shares,avg_price,gross_cost,fee,total_cost,cash_before,cash_after,tau_sec,
                  robust_prob,net_edge,effective_price,effective_source,sigma_used,latency_ms,fills_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    now_ms(), "PAPER", s["name"], m["condition_id"], m["kind"], asset, outcome,
                    "SNIPE" if s["leg"] == "snipe" else "SCALP", desired, shares, avg, gross, fee, total,
                    cash, after, tau, robust_prob, edge, snapshot["effective_price"], snapshot["effective_source"],
                    snapshot["sigma_used"], PAPER_TAKER_LATENCY_MS,
                    jd([{"price": p, "shares": q} for p, q in fills]),
                ),
            )
            c.execute(
                "INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (cash_key(s["name"]), str(after)),
            )
            c.commit()
        add_market_spent(s["name"], m["condition_id"], total)
        update_last_signal_fill(s["name"], m["condition_id"], True, f"filled_fak_after_{PAPER_TAKER_LATENCY_MS}ms")
        log.info(
            "PAPER %-9s %-4s %-5s | tau=%4.1fs edge=%+.3f fair=%.3f ask=%.3f | %.2fsh cost=$%.2f cash %.2f->%.2f",
            s["name"], m["kind"], outcome, tau, edge, robust_prob, avg, shares, total, cash, after,
        )
    except Exception:
        log.exception("Candidate execution failed | %s %s", s["name"], m["condition_id"][-6:])
    finally:
        inflight.discard(key)

async def evaluate_strategy(s, m, now):
    if s["kind"] != m["kind"]:
        return
    tau = m["end_ts"] - now
    if tau < s["min_tau"] or tau > s["max_tau"]:
        return

    key = (s["name"], m["condition_id"])
    if key in inflight:
        return
    if now_ms() - last_attempt_ms[key] < ATTEMPT_COOLDOWN_MS:
        return

    fav = favorite_side(m)
    if not fav:
        return
    asset, outcome, mkt_mid, up_mid = fav
    ask = best_ask(asset)
    if ask is None or ask < s["min_ask"] or ask > s["max_ask"]:
        return

    if not m.get("btc_open"):
        m["btc_open"] = await fetch_binance_open(m["start_ts"])
        if m.get("btc_open"):
            persist_market(m)
        else:
            return

    eff, eff_source, sm, pm, sage, page = effective_btc_price()
    vf, vs, vu, vol_ready = vol_snapshot()
    if not eff or not vol_ready:
        return

    bounds = fair_up_bounds(eff, m["btc_open"], vu, tau, up_mid)
    if not bounds:
        return
    if outcome == "Up":
        side_probs = bounds
    else:
        side_probs = [1.0 - x for x in bounds]
    robust_prob = min(side_probs)
    fee_ps = fee_per_share(ask)
    edge = robust_prob - ask - fee_ps

    qualifies = False
    reason = ""
    if s["leg"] == "snipe":
        qualifies = edge >= s["min_edge"] and edge <= s["max_edge"]
        reason = "qualifies" if qualifies else "edge_gate"
    else:
        qualifies = robust_prob >= s["min_prob"]
        reason = "qualifies" if qualifies else "prob_gate"

    # Log only near-opportunities / actual candidates, not every 250ms snapshot.
    near = qualifies or (s["leg"] == "snipe" and edge >= s["min_edge"] - 0.02) or (
        s["leg"] == "scalp" and robust_prob >= s["min_prob"] - 0.01
    )
    if not near:
        return

    sig = (asset, round(ask, 3), round(edge, 3), int(tau))
    if not qualifies:
        # Keep blocked diagnostics but deduplicate identical state.
        if last_signature.get(key) != sig:
            store_signal(
                s, m, outcome, asset, tau, ask, mkt_mid, up_mid, bounds, robust_prob, edge, fee_ps,
                m["btc_open"], sm, pm, eff, eff_source, vf, vs, vu, False, False, reason,
            )
            last_signature[key] = sig
        return

    cash, cap = account_can_trade(s, m["condition_id"])
    if cap < s["min_take_usd"]:
        return

    last_attempt_ms[key] = now_ms()
    last_signature[key] = sig
    inflight.add(key)
    snapshot = {
        "effective_price": eff,
        "effective_source": eff_source,
        "sigma_used": vu,
    }
    store_signal(
        s, m, outcome, asset, tau, ask, mkt_mid, up_mid, bounds, robust_prob, edge, fee_ps,
        m["btc_open"], sm, pm, eff, eff_source, vf, vs, vu, True, False, "fak_committed",
    )
    asyncio.create_task(
        execute_candidate(s, m, asset, outcome, tau, ask, mkt_mid, up_mid, bounds, robust_prob, edge, m["btc_open"], snapshot)
    )

async def strategy_loop():
    while True:
        started = time.monotonic()
        n = time.time()
        try:
            if trading_enabled():
                for cid, m in list(markets.items()):
                    if n < m["start_ts"] or n > m["end_ts"]:
                        continue
                    if best_ask(m["up_asset"]) is None or best_ask(m["down_asset"]) is None:
                        continue
                    for s in STRATEGIES:
                        await evaluate_strategy(s, m, n)
        except Exception:
            log.exception("Strategy loop failed")
        elapsed = time.monotonic() - started
        await asyncio.sleep(max(0.02, DECISION_INTERVAL - elapsed))

# ============================================================
# Settlement
# ============================================================

def resolve_winner(row):
    outcomes = [str(x) for x in parse_jsonish(row.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(row.get("clobTokenIds"))]
    prices = [sf(x, -1) for x in parse_jsonish(row.get("outcomePrices"))]
    if len(outcomes) >= 2 and len(tokens) >= 2 and len(prices) >= 2:
        i = max(range(len(prices)), key=lambda j: prices[j])
        others = [prices[j] for j in range(len(prices)) if j != i]
        if prices[i] >= 0.999 and max(others or [-1]) <= 0.001 and bool(
            row.get("closed", False) or row.get("resolved", False) or prices[i] >= 0.9999
        ):
            return tokens[i], outcomes[i]
    return None, None

async def settle_from_ws(ev):
    cid = str(ev.get("market") or ev.get("condition_id") or "")
    win = str(ev.get("winning_asset_id") or ev.get("winning_asset") or "")
    out = str(ev.get("winning_outcome") or "")
    if cid and win:
        await settle_market(cid, win, out)

async def settle_market(cid, win, out):
    async with settle_lock:
        m = markets.get(cid)
        with db() as c:
            mr = c.execute("SELECT * FROM markets WHERE condition_id=?", (cid,)).fetchone()
            kind = (m or {}).get("kind") or (mr["kind"] if mr else "")
            lines = []
            for s in STRATEGIES:
                name = s["name"]
                if c.execute(
                    "SELECT 1 FROM results WHERE condition_id=? AND strategy=? AND mode='PAPER'",
                    (cid, name),
                ).fetchone():
                    continue
                rows = c.execute(
                    "SELECT * FROM trades WHERE condition_id=? AND strategy=? AND mode='PAPER'",
                    (cid, name),
                ).fetchall()
                if not rows:
                    continue
                cost = sum(sf(r["total_cost"]) for r in rows)
                payout = sum(sf(r["filled_shares"]) for r in rows if str(r["asset"]) == win)
                pnl = payout - cost
                cash = paper_cash(name)
                after = cash + payout
                c.execute(
                    """INSERT INTO results(condition_id,strategy,mode,kind,winning_asset,winning_outcome,total_cost,payout,pnl,trades,settled_ms)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (cid, name, "PAPER", kind, win, out, cost, payout, pnl, len(rows), now_ms()),
                )
                c.execute(
                    "INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (cash_key(name), str(after)),
                )
                lines.append(f"{name}: {pnl:+.2f} -> ${after:.2f}")
            c.execute(
                "UPDATE markets SET resolved=1,winning_asset=?,winning_outcome=? WHERE condition_id=?",
                (win, out, cid),
            )
            c.commit()
        if lines:
            log.info("SETTLED %s %s winner=%s | %s", kind, cid[-6:], out, " | ".join(lines))
            await tg_send("✅ MARKET SETTLED\n" + f"{kind} | Winner: {out}\n" + "\n".join(lines))

async def resolution_loop():
    while True:
        try:
            cutoff = now_ts() - 8
            with db() as c:
                rows = c.execute(
                    "SELECT * FROM markets WHERE resolved=0 AND end_ts<? ORDER BY end_ts LIMIT 100",
                    (cutoff,),
                ).fetchall()
            for r in rows:
                ev = await fetch_event_by_slug(r["slug"])
                if not ev or not isinstance(ev.get("markets"), list):
                    continue
                raw = next((x for x in ev["markets"] if str(x.get("conditionId") or "") == r["condition_id"]), None)
                if raw is None and len(ev["markets"]) == 1:
                    raw = ev["markets"][0]
                if not raw:
                    continue
                win, out = resolve_winner(raw)
                if win:
                    await settle_market(r["condition_id"], win, out)
        except Exception:
            log.exception("Resolution fallback failed")
        await asyncio.sleep(10)

# ============================================================
# Stats / reports
# ============================================================

def account_stats(strategy_name):
    cash = paper_cash(strategy_name)
    initial = paper_initial(strategy_name)
    with db() as c:
        realized = sf(c.execute(
            "SELECT COALESCE(SUM(pnl),0) p FROM results WHERE strategy=? AND mode='PAPER'",
            (strategy_name,),
        ).fetchone()["p"])
        wins = c.execute(
            "SELECT COUNT(*) c FROM results WHERE strategy=? AND mode='PAPER' AND pnl>0",
            (strategy_name,),
        ).fetchone()["c"]
        losses = c.execute(
            "SELECT COUNT(*) c FROM results WHERE strategy=? AND mode='PAPER' AND pnl<0",
            (strategy_name,),
        ).fetchone()["c"]
        trades = c.execute(
            "SELECT COUNT(*) c FROM trades WHERE strategy=? AND mode='PAPER'",
            (strategy_name,),
        ).fetchone()["c"]
        fees = sf(c.execute(
            "SELECT COALESCE(SUM(fee),0) f FROM trades WHERE strategy=? AND mode='PAPER'",
            (strategy_name,),
        ).fetchone()["f"])
        open_cost = sf(c.execute(
            """SELECT COALESCE(SUM(t.total_cost),0) x FROM trades t
               LEFT JOIN results r ON r.condition_id=t.condition_id AND r.strategy=t.strategy AND r.mode=t.mode
               WHERE t.strategy=? AND t.mode='PAPER' AND r.condition_id IS NULL""",
            (strategy_name,),
        ).fetchone()["x"])
        attempts = c.execute(
            "SELECT COUNT(*) c FROM signals WHERE strategy=? AND accepted=1",
            (strategy_name,),
        ).fetchone()["c"]
        fills = c.execute(
            "SELECT COUNT(*) c FROM signals WHERE strategy=? AND accepted=1 AND filled=1",
            (strategy_name,),
        ).fetchone()["c"]
        cost = sf(c.execute(
            "SELECT COALESCE(SUM(total_cost),0) x FROM trades WHERE strategy=? AND mode='PAPER'",
            (strategy_name,),
        ).fetchone()["x"])
    equity = cash + open_cost
    return {
        "initial": initial, "cash": cash, "open_cost": open_cost, "equity": equity,
        "realized": realized, "wins": wins, "losses": losses, "trades": trades,
        "fees": fees, "attempts": attempts, "fills": fills, "cost": cost,
    }

def period_summary(start_ms, end_ms, strategy_name):
    with db() as c:
        tr = c.execute(
            "SELECT * FROM trades WHERE strategy=? AND trade_ms>=? AND trade_ms<?",
            (strategy_name, start_ms, end_ms),
        ).fetchall()
        rs = c.execute(
            "SELECT * FROM results WHERE strategy=? AND settled_ms>=? AND settled_ms<?",
            (strategy_name, start_ms, end_ms),
        ).fetchall()
        attempts = c.execute(
            "SELECT COUNT(*) c FROM signals WHERE strategy=? AND accepted=1 AND signal_ms>=? AND signal_ms<?",
            (strategy_name, start_ms, end_ms),
        ).fetchone()["c"]
        fills = c.execute(
            "SELECT COUNT(*) c FROM signals WHERE strategy=? AND accepted=1 AND filled=1 AND signal_ms>=? AND signal_ms<?",
            (strategy_name, start_ms, end_ms),
        ).fetchone()["c"]
    pnl = sum(sf(r["pnl"]) for r in rs)
    wins = sum(1 for r in rs if sf(r["pnl"]) > 0)
    losses = sum(1 for r in rs if sf(r["pnl"]) < 0)
    fees = sum(sf(r["fee"]) for r in tr)
    cost = sum(sf(r["total_cost"]) for r in tr)
    return {
        "pnl": pnl, "wins": wins, "losses": losses, "trades": len(tr), "fees": fees,
        "cost": cost, "attempts": attempts, "fills": fills,
    }

def rows_to_csv(rows):
    if not rows:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()

def make_hourly_report(start_ts, end_ts):
    start_ms, end_ms = start_ts * 1000, end_ts * 1000
    stamp_a = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d_%H-%M")
    stamp_b = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%H-%M")
    zip_path = REPORT_DIR / f"sniper_lab_{stamp_a}_{stamp_b}_UTC.zip"

    summary_rows = []
    for s in STRATEGIES:
        p = period_summary(start_ms, end_ms, s["name"])
        all_s = account_stats(s["name"])
        fill_rate = p["fills"] / p["attempts"] if p["attempts"] else 0.0
        roi = p["pnl"] / p["cost"] * 100 if p["cost"] else 0.0
        summary_rows.append({
            "strategy": s["name"], "label": s["short"],
            "hour_pnl": round(p["pnl"], 6), "hour_wins": p["wins"], "hour_losses": p["losses"],
            "hour_trades": p["trades"], "hour_fees": round(p["fees"], 6), "hour_cost": round(p["cost"], 6),
            "hour_roi_pct": round(roi, 4), "fak_attempts": p["attempts"], "fak_fills": p["fills"],
            "fak_fill_rate_pct": round(fill_rate * 100, 2),
            "cash": round(all_s["cash"], 6), "equity": round(all_s["equity"], 6),
            "realized_total": round(all_s["realized"], 6), "total_wins": all_s["wins"],
            "total_losses": all_s["losses"], "total_trades": all_s["trades"],
        })

    with db() as c:
        trades = [dict(r) for r in c.execute(
            "SELECT * FROM trades WHERE trade_ms>=? AND trade_ms<? ORDER BY trade_ms",
            (start_ms, end_ms),
        ).fetchall()]
        signals = [dict(r) for r in c.execute(
            "SELECT * FROM signals WHERE signal_ms>=? AND signal_ms<? ORDER BY signal_ms",
            (start_ms, end_ms),
        ).fetchall()]
        results = [dict(r) for r in c.execute(
            "SELECT * FROM results WHERE settled_ms>=? AND settled_ms<? ORDER BY settled_ms",
            (start_ms, end_ms),
        ).fetchall()]

    lines = [
        f"PolyBTC Sniper Lab hourly report",
        f"Period UTC: {utc_iso(start_ts)} -> {utc_iso(end_ts)}",
        f"Version: {VERSION}",
        "",
    ]
    for row in summary_rows:
        lines.append(
            f"{row['strategy']}: pnl={row['hour_pnl']:+.2f} W/L={row['hour_wins']}/{row['hour_losses']} "
            f"trades={row['hour_trades']} cost={row['hour_cost']:.2f} ROI={row['hour_roi_pct']:+.2f}% "
            f"FAK={row['fak_fills']}/{row['fak_attempts']} equity={row['equity']:.2f}"
        )

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("summary.csv", rows_to_csv(summary_rows))
        z.writestr("trades.csv", rows_to_csv(trades))
        z.writestr("signals.csv", rows_to_csv(signals))
        z.writestr("results.csv", rows_to_csv(results))
        z.writestr("report.txt", "\n".join(lines) + "\n")
    return zip_path, summary_rows

async def tg_send_document(path, caption=""):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", TELEGRAM_CHAT_ID)
        if caption:
            form.add_field("caption", caption[:1024])
        form.add_field("document", path.read_bytes(), filename=path.name, content_type="application/zip")
        await session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
            data=form,
            timeout=aiohttp.ClientTimeout(total=30),
        )
    except Exception:
        log.exception("Telegram document send failed")

async def report_loop():
    while True:
        try:
            await asyncio.sleep(15)
            end_ts = (now_ts() // 3600) * 3600
            last_end = si(state_get("last_report_end", end_ts))
            while last_end < end_ts:
                start_ts = last_end
                next_end = last_end + 3600
                path, rows = make_hourly_report(start_ts, next_end)
                caption = " | ".join(f"{r['strategy']} {r['hour_pnl']:+.2f}" for r in rows)
                await tg_send_document(path, f"📦 Sniper Lab {caption}")
                state_set("last_report_end", next_end)
                last_end = next_end
        except Exception:
            log.exception("Hourly report failed")
            await asyncio.sleep(30)

# ============================================================
# Telegram
# ============================================================

def keyboard():
    return {
        "keyboard": [
            [{"text": "▶️ START"}, {"text": "⏹ STOP"}],
            [{"text": "💰 BALANCE"}, {"text": "📊 STATISTICS"}],
            [{"text": "📈 POSITIONS"}, {"text": "📜 TRADES"}],
            [{"text": "📦 REPORT"}, {"text": "🚨 EMERGENCY STOP"}],
        ],
        "resize_keyboard": True,
    }

async def tg_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        await session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4096], "reply_markup": keyboard()},
            timeout=aiohttp.ClientTimeout(total=15),
        )
    except Exception:
        log.exception("Telegram send failed")

async def send_balances():
    blocks = []
    for s in STRATEGIES:
        st = account_stats(s["name"])
        blocks.append(
            f"{s['short']}\nCash ${st['cash']:.2f} | Open ${st['open_cost']:.2f} | Equity ${st['equity']:.2f}\n"
            f"Realized {st['realized']:+.2f}"
        )
    await tg_send("💰 FOUR INDEPENDENT $500 PAPER ACCOUNTS\n\n" + "\n\n".join(blocks))

async def send_statistics():
    blocks = []
    for s in STRATEGIES:
        st = account_stats(s["name"])
        denom = st["wins"] + st["losses"]
        wr = st["wins"] / denom * 100 if denom else 0
        fr = st["fills"] / st["attempts"] * 100 if st["attempts"] else 0
        roi = st["realized"] / st["cost"] * 100 if st["cost"] else 0
        blocks.append(
            f"{s['short']}\nW/L {st['wins']}/{st['losses']} ({wr:.1f}%) | Trades {st['trades']}\n"
            f"PnL {st['realized']:+.2f} | Fees ${st['fees']:.2f} | ROI/cost {roi:+.1f}%\n"
            f"FAK fills {st['fills']}/{st['attempts']} ({fr:.1f}%)"
        )
    await tg_send("📊 SNIPER LAB STATISTICS\n\n" + "\n\n".join(blocks))

async def send_positions():
    for s in STRATEGIES:
        with db() as c:
            rows = c.execute(
                """SELECT t.condition_id,t.kind,t.outcome,SUM(t.filled_shares) shares,SUM(t.total_cost) cost
                   FROM trades t LEFT JOIN results r
                     ON r.condition_id=t.condition_id AND r.strategy=t.strategy AND r.mode=t.mode
                   WHERE t.strategy=? AND t.mode='PAPER' AND r.condition_id IS NULL
                   GROUP BY t.condition_id,t.kind,t.outcome ORDER BY MAX(t.trade_ms) DESC LIMIT 15""",
                (s["name"],),
            ).fetchall()
        body = "\n".join(
            f"{r['kind']} {r['condition_id'][-6:]} {r['outcome']}: {r['shares']:.2f}sh ${r['cost']:.2f}"
            for r in rows
        ) if rows else "None"
        await tg_send(f"📈 {s['short']}\n{body}")

async def send_trades():
    for s in STRATEGIES:
        with db() as c:
            rows = c.execute(
                "SELECT * FROM trades WHERE strategy=? ORDER BY id DESC LIMIT 10", (s["name"],)
            ).fetchall()
        lines = []
        for r in rows:
            t = datetime.fromtimestamp(r["trade_ms"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
            lines.append(
                f"{t} {r['kind']} {r['outcome']} @{r['avg_price']:.3f} "
                f"edge={r['net_edge']:+.3f} ${r['total_cost']:.2f}"
            )
        await tg_send(f"📜 {s['short']} LAST TRADES\n" + ("\n".join(lines) if lines else "None"))

async def send_latest_report():
    end_ts = (now_ts() // 3600) * 3600
    start_ts = end_ts - 3600
    path, rows = make_hourly_report(start_ts, end_ts)
    caption = " | ".join(f"{r['strategy']} {r['hour_pnl']:+.2f}" for r in rows)
    await tg_send_document(path, f"📦 Last complete hour | {caption}")

async def handle_tg(text):
    t = text.strip().upper()
    if t in {"/START", "▶️ START", "START"}:
        state_set("trading_enabled", "1")
        await tg_send("▶️ Sniper Lab STARTED\nPAPER only | 4 independent $500 accounts")
    elif t in {"⏹ STOP", "STOP", "/STOP"}:
        state_set("trading_enabled", "0")
        await tg_send("⏹ New paper entries stopped. Open positions settle normally.")
    elif t in {"🚨 EMERGENCY STOP", "EMERGENCY STOP"}:
        state_set("trading_enabled", "0")
        await tg_send("🚨 EMERGENCY STOP. No new paper orders.")
    elif t in {"💰 BALANCE", "BALANCE", "/BALANCE"}:
        await send_balances()
    elif t in {"📊 STATISTICS", "STATISTICS", "/STATS"}:
        await send_statistics()
    elif t in {"📈 POSITIONS", "POSITIONS"}:
        await send_positions()
    elif t in {"📜 TRADES", "TRADES"}:
        await send_trades()
    elif t in {"📦 REPORT", "REPORT"}:
        await send_latest_report()
    elif t in {"LIVE", "🔴 LIVE"}:
        await tg_send("🔒 LIVE is deliberately disabled in this research build.")
    else:
        await tg_send(
            "PolyBTC Sniper Lab\n"
            "A S5_R10 | B S5_R15 | C S15_R10 | D SCALP15\n"
            "Each account starts at $500. PAPER only."
        )

async def telegram_loop():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured")
        return
    offset = 0
    await tg_send(
        f"🤖 {VERSION} online\nTrading: {'ON' if trading_enabled() else 'OFF'}\n"
        "A/B/C/D each has an independent $500 PAPER account."
    )
    while True:
        try:
            async with session.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"timeout": 25, "offset": offset},
                timeout=aiohttp.ClientTimeout(total=35),
            ) as r:
                d = await r.json()
            for u in d.get("result", []):
                offset = max(offset, si(u.get("update_id")) + 1)
                msg = u.get("message") or {}
                chat = str((msg.get("chat") or {}).get("id", ""))
                if chat != str(TELEGRAM_CHAT_ID):
                    continue
                text = msg.get("text")
                if text:
                    await handle_tg(text)
        except Exception as e:
            log.warning("Telegram polling: %s", e)
            await asyncio.sleep(2)

# ============================================================
# Cleanup / health
# ============================================================

def cleanup_old_runtime():
    cutoff = now_ts() - MEMORY_KEEP_RESOLVED_SEC
    old = []
    with db() as c:
        rows = c.execute(
            "SELECT condition_id,up_asset,down_asset FROM markets WHERE resolved=1 AND end_ts<?",
            (cutoff,),
        ).fetchall()
    for r in rows:
        cid = str(r["condition_id"])
        if cid in markets:
            old.append(cid)
    for cid in old:
        markets.pop(cid, None)
        for s in STRATEGIES:
            spent_cache.pop((s["name"], cid), None)
            last_attempt_ms.pop((s["name"], cid), None)
            last_signature.pop((s["name"], cid), None)
    keep_assets = set()
    for m in markets.values():
        keep_assets.add(m["up_asset"])
        keep_assets.add(m["down_asset"])
    for a in list(books):
        if a not in keep_assets:
            books.pop(a, None)
    subscribed_assets.intersection_update(keep_assets)
    return len(old)

async def cleanup_loop():
    while True:
        try:
            n = cleanup_old_runtime()
            if n:
                log.info("CLEANUP markets=%d", n)
        except Exception:
            log.exception("Cleanup failed")
        await asyncio.sleep(60)

async def health(request):
    eff, source, sm, pm, sage, page = effective_btc_price()
    vf, vs, vu, ready = vol_snapshot()
    return web.json_response({
        "ok": True,
        "version": VERSION,
        "paper_only": True,
        "trading_enabled": trading_enabled(),
        "markets": len(markets),
        "books": len(books),
        "spot": sm,
        "perp": pm,
        "effective": eff,
        "effective_source": source,
        "spot_age_ms": sage,
        "perp_age_ms": page,
        "vol_fast": vf,
        "vol_slow": vs,
        "vol_used": vu,
        "vol_ready": ready,
        "accounts": {s["name"]: account_stats(s["name"]) for s in STRATEGIES},
        "time_utc": utc_iso(),
    })

async def web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Health server :%d", PORT)

# ============================================================
# Main
# ============================================================

async def main():
    global session
    init_db()
    session = aiohttp.ClientSession(headers={"User-Agent": "PolyBTCSniperLab/1.0", "Accept": "application/json"})
    await bootstrap_binance_history()
    tasks = [asyncio.create_task(x()) for x in (
        web_server,
        discovery_loop,
        poly_ws_loop,
        binance_spot_ws_loop,
        binance_perp_ws_loop,
        binance_sampler_loop,
        strategy_loop,
        resolution_loop,
        telegram_loop,
        report_loop,
        cleanup_loop,
    )]
    balances = ", ".join(f"{s['name']}=${paper_cash(s['name']):.2f}" for s in STRATEGIES)
    log.info("%s started | PAPER ONLY | %s", VERSION, balances)
    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
        await session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
