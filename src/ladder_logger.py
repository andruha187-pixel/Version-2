"""
📒 Логгер стаканов «лестниц» — ничего не торгует, только записывает.

Зачем. Архив цен Polymarket для часовых лестниц «Bitcoin above ___ on <дата>,
<час> ET» оказался бесполезным: у дальних страйков почти нет торгов, и в
истории стоят условные цены (0.5, 0.025). Чтобы честно проверить идею
«продавать лотерейные билеты» (выставлять лимитную продажу Yes по 1–3¢ на
далёких страйках = покупку No по 97–99¢ и ждать, пока кто-то купит), нужны
настоящие стаканы и настоящие сделки. Их и пишем:

  books.csv    — каждые 30–60 с: лучший bid/ask Yes, объёмы, глубина 3 уровня,
                 цена BTC на Binance, минут до конца;
  trades.csv   — все сделки по каждому страйку (кто купил/продал, Yes/No,
                 цена, объём) из публичного data-api Polymarket;
  outcomes.csv — исход каждого страйка по минутной свече Binance в момент конца.

Раз в REPORT_INTERVAL_HOURS (и по кнопке) всё уходит в Telegram одним zip.

По этим данным потом считаем: как часто и почём покупают «лотерейки», что
было бы с нашими лимитными ордерами (исполнились ли, выиграли ли), и есть ли
после комиссии плюс. Только если плюс есть — думаем о реальных деньгах.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import os
import time
import zipfile
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

from config import settings
from src import runtime_state
from src.ladder_research import _MONTHS, hour_suffixes, markets_from_event, parse_iso

log = logging.getLogger(__name__)
_ET = ZoneInfo("America/New_York")
_SYMBOLS = {"bitcoin": "BTCUSDT", "ethereum": "ETHUSDT", "solana": "SOLUSDT", "xrp": "XRPUSDT"}

BOOKS_COLUMNS = ["ts", "slug", "kind", "end_ts", "minutes_left", "strike", "spot",
                 "yes_bid", "yes_bid_size", "yes_ask", "yes_ask_size", "bid3_size", "ask3_size", "tick"]
TRADES_COLUMNS = ["ts", "slug", "kind", "end_ts", "strike", "outcome", "side", "price", "size", "tx"]
OUTCOMES_COLUMNS = ["slug", "kind", "end_ts", "strike", "res_close", "outcome_yes"]

_tracked: dict[str, dict] = {}        # condition_id (или yes_token) -> рынок
_finalized: set[str] = set()
_seen_trades: dict[tuple, int] = {}   # ключ сделки -> время, когда увидели
_last_book_write: dict[str, float] = {}
_status = {"last_discovery": 0, "events": 0, "errors": 0, "last_error": ""}


# --------------------------------------------------------------- файлы ------

class _Sink:
    def __init__(self) -> None:
        self.files: dict = {}
        self.writers: dict = {}
        self.paths: dict = {}
        self.counts: dict = {}
        self.started = 0
        self._open()

    def _open(self) -> None:
        self.started = int(time.time())
        folder = os.path.join(settings.REPORTS_DIR, "ladder_tmp")
        os.makedirs(folder, exist_ok=True)
        for name, cols in (("books", BOOKS_COLUMNS), ("trades", TRADES_COLUMNS), ("outcomes", OUTCOMES_COLUMNS)):
            path = os.path.join(folder, f"ladder_{name}_{self.started}.csv")
            f = open(path, "w", newline="", encoding="utf-8")
            w = csv.writer(f)
            w.writerow(cols)
            f.flush()
            self.files[name], self.writers[name], self.paths[name], self.counts[name] = f, w, path, 0

    def write(self, name: str, rows: list[list]) -> None:
        if not rows:
            return
        self.writers[name].writerows(rows)
        self.files[name].flush()
        self.counts[name] += len(rows)

    def rotate(self) -> tuple[str | None, dict, int, int]:
        """Закрывает текущие файлы, пакует в zip и открывает новые."""
        for f in self.files.values():
            f.close()
        counts, started, ended = dict(self.counts), self.started, int(time.time())
        zip_path = None
        if any(counts.values()):
            label = f"{time.strftime('%Y%m%d-%H%M', time.gmtime(started))}_to_{time.strftime('%Y%m%d-%H%M', time.gmtime(ended))}"
            zip_path = os.path.join(settings.REPORTS_DIR, f"ladder_{label}.zip")
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
                for name, path in self.paths.items():
                    z.write(path, arcname=f"{name}.csv")
        for path in self.paths.values():
            try:
                os.remove(path)
            except OSError:
                pass
        self._open()
        return zip_path, counts, started, ended


_sink: _Sink | None = None


def _get_sink() -> _Sink:
    global _sink
    if _sink is None:
        _sink = _Sink()
    return _sink


# ----------------------------------------------------------------- сеть -----

async def _get_json(client: httpx.AsyncClient, url: str, params: dict | None = None):
    for i in range(3):
        try:
            r = await client.get(url, params=params)
            if r.status_code == 429:
                await asyncio.sleep(2 + 2 * i)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, ValueError):
            if i == 2:
                raise
            await asyncio.sleep(1 + i)
    return None


def _candidate_slugs(now: datetime) -> list[str]:
    """Слаги часовых событий на ближайшие часы и дневных — на сегодня/завтра (по ET)."""
    now_et = now.astimezone(_ET)
    slugs: list[str] = []
    hours_ahead = int(settings.LADDER_TRACK_HOURS_HOURLY) + 2
    first = now_et.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    suffixes = hour_suffixes()
    for name in settings.LADDER_ASSETS:
        for k in range(hours_ahead):
            end = first + timedelta(hours=k)
            base = f"{name}-above-on-{_MONTHS[end.month - 1]}-{end.day}-{end.year}"
            slugs.append(f"{base}-{suffixes[end.hour]}")
        for k in range(2):
            d = (now_et + timedelta(days=k)).date()
            slugs.append(f"{name}-above-on-{_MONTHS[d.month - 1]}-{d.day}-{d.year}")
    return slugs


async def _discover(client: httpx.AsyncClient) -> None:
    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    events = 0
    for slug in _candidate_slugs(now):
        try:
            data = await _get_json(client, f"{settings.GAMMA_HOST}/events", {"slug": slug})
        except (httpx.HTTPError, ValueError) as exc:
            _status["errors"] += 1
            _status["last_error"] = f"gamma {slug}: {type(exc).__name__}"
            continue
        if not data:
            continue
        for ev in (data if isinstance(data, list) else [data]):
            end_ts = parse_iso(ev.get("endDate"))
            if end_ts is None or end_ts <= now_ts:
                continue
            name = slug.split("-above-on-")[0]
            kind = "hourly" if slug.endswith("-et") else "daily"
            window = (settings.LADDER_TRACK_HOURS_HOURLY if kind == "hourly" else settings.LADDER_TRACK_HOURS_DAILY) * 3600
            if end_ts - now_ts > window:
                continue
            events += 1
            for m in markets_from_event(ev, name, ev.get("slug") or slug):
                key = m.get("condition_id") or m["yes_token"]
                if key in _tracked or key in _finalized or m.get("closed"):
                    continue
                m["symbol"] = _SYMBOLS.get(name, "BTCUSDT")
                _tracked[key] = m
    _status["last_discovery"] = now_ts
    _status["events"] = events


def _parse_book(book: dict) -> dict:
    def levels(side: str) -> list[tuple[float, float]]:
        out = []
        for lvl in book.get(side) or []:
            try:
                p, s = float(lvl["price"]), float(lvl["size"])
            except (KeyError, TypeError, ValueError):
                continue
            if s > 0:
                out.append((p, s))
        return out
    bids = sorted(levels("bids"), key=lambda x: -x[0])
    asks = sorted(levels("asks"), key=lambda x: x[0])
    return {
        "bid": bids[0][0] if bids else None, "bid_size": bids[0][1] if bids else None,
        "ask": asks[0][0] if asks else None, "ask_size": asks[0][1] if asks else None,
        "bid3": sum(s for _, s in bids[:3]) if bids else 0.0,
        "ask3": sum(s for _, s in asks[:3]) if asks else 0.0,
        "tick": book.get("tick_size") or "",
    }


async def _fetch_books(client: httpx.AsyncClient, tokens: list[str]) -> dict[str, dict]:
    books: dict[str, dict] = {}
    for i in range(0, len(tokens), 50):
        chunk = tokens[i:i + 50]
        try:
            r = await client.post(f"{settings.POLY_HOST}/books", json=[{"token_id": t} for t in chunk])
            r.raise_for_status()
            for b in r.json() or []:
                if b.get("asset_id"):
                    books[str(b["asset_id"])] = b
            continue
        except (httpx.HTTPError, ValueError):
            pass
        for t in chunk:  # запасной путь — по одному
            try:
                b = await _get_json(client, f"{settings.POLY_HOST}/book", {"token_id": t})
            except (httpx.HTTPError, ValueError):
                b = None
            if b:
                books[t] = b
    return books


async def _spot(client: httpx.AsyncClient, symbols: set[str]) -> dict[str, float]:
    out = {}
    for sym in symbols:
        try:
            data = await _get_json(client, f"{settings.BINANCE_BASE_URL}/api/v3/ticker/price", {"symbol": sym})
            out[sym] = float(data["price"])
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            continue
    return out


async def _poll_books(client: httpx.AsyncClient) -> None:
    if not _tracked:
        return
    now = time.time()
    due = []
    for key, m in _tracked.items():
        left_min = (m["end_ts"] - now) / 60
        if left_min <= 0:
            continue
        min_gap = settings.LADDER_POLL_SECONDS if left_min <= 60 else 2 * settings.LADDER_POLL_SECONDS
        if now - _last_book_write.get(key, 0) >= min_gap - 1:
            due.append((key, m))
    if not due:
        return
    books = await _fetch_books(client, [m["yes_token"] for _, m in due])
    spots = await _spot(client, {m["symbol"] for _, m in due})
    rows = []
    for key, m in due:
        b = books.get(m["yes_token"])
        if not b:
            continue
        p = _parse_book(b)
        rows.append([int(now), m["slug"], m["kind"], m["end_ts"], round((m["end_ts"] - now) / 60, 2), m["strike"],
                     spots.get(m["symbol"], ""), p["bid"], p["bid_size"], p["ask"], p["ask_size"],
                     round(p["bid3"], 2), round(p["ask3"], 2), p["tick"]])
        _last_book_write[key] = now
    _get_sink().write("books", rows)


async def _poll_trades(client: httpx.AsyncClient, markets: list[dict]) -> None:
    rows = []
    sem = asyncio.Semaphore(5)

    async def fetch(m: dict):
        cid = m.get("condition_id")
        if not cid:
            return m, None
        async with sem:
            try:
                return m, await _get_json(client, f"{settings.DATA_API_HOST}/trades",
                                          {"market": cid, "limit": 200, "takerOnly": "true"})
            except (httpx.HTTPError, ValueError):
                return m, None

    for m, data in await asyncio.gather(*(fetch(m) for m in markets)):
        if isinstance(data, dict):
            data = data.get("data") or data.get("trades") or []
        for t in data or []:
            try:
                key = (t.get("transactionHash"), str(t.get("asset")), float(t["price"]), float(t["size"]),
                       int(t["timestamp"]), t.get("side"))
            except (KeyError, TypeError, ValueError):
                continue
            if key in _seen_trades:
                continue
            _seen_trades[key] = int(time.time())
            rows.append([key[4], m["slug"], m["kind"], m["end_ts"], m["strike"], t.get("outcome") or "",
                         key[5] or "", key[2], key[3], key[0] or ""])
    _get_sink().write("trades", rows)
    # не держим в памяти сделки старше 6 часов
    cutoff = int(time.time()) - 6 * 3600
    for k in [k for k, seen in _seen_trades.items() if seen < cutoff]:
        _seen_trades.pop(k, None)


async def _resolution_close(client: httpx.AsyncClient, symbol: str, end_ts: int) -> float | None:
    data = await _get_json(client, f"{settings.BINANCE_BASE_URL}/api/v3/klines",
                           {"symbol": symbol, "interval": "1m", "startTime": end_ts * 1000, "limit": 1})
    try:
        k = data[0]
        if int(k[0]) // 1000 != end_ts - end_ts % 60:
            return None
        return float(k[4])
    except (IndexError, TypeError, ValueError):
        return None


async def _finalize(client: httpx.AsyncClient) -> None:
    """Через 2 минуты после конца: последние сделки + исход по свече Binance."""
    now = time.time()
    done = [(k, m) for k, m in _tracked.items() if m["end_ts"] <= now - 120]
    if not done:
        return
    await _poll_trades(client, [m for _, m in done])
    closes: dict[tuple[str, int], float | None] = {}
    rows = []
    for key, m in done:
        ck = (m["symbol"], m["end_ts"])
        if ck not in closes:
            try:
                closes[ck] = await _resolution_close(client, *ck)
            except (httpx.HTTPError, ValueError):
                closes[ck] = None
        close = closes[ck]
        if close is None and now - m["end_ts"] < 1800:
            continue  # свеча ещё не готова — попробуем на следующем круге
        rows.append([m["slug"], m["kind"], m["end_ts"], m["strike"], "" if close is None else close,
                     "" if close is None else int(close > m["strike"])])
        _tracked.pop(key, None)
        _last_book_write.pop(key, None)
        _finalized.add(key)
    _get_sink().write("outcomes", rows)
    if len(_finalized) > 5000:
        _finalized.clear()


# ---------------------------------------------------------- отчёт/статус ----

def status_text() -> str:
    sink = _get_sink()
    events = len({m["slug"] for m in _tracked.values()})
    nxt = ""
    if _next_report_at:
        nxt = f"\nСледующий zip через ~{max(0, int((_next_report_at - time.time()) / 60))} мин."
    err = f"\nОшибок связи: {_status['errors']} (последняя: {_status['last_error'][:120]})" if _status["errors"] else ""
    return (
        f"📒 Логгер «лестниц»: {'вкл' if runtime_state.get('ladder_logger_enabled') else 'выкл'}\n"
        f"Сейчас пишу: {len(_tracked)} страйков в {events} событиях "
        f"(часовые — последние {settings.LADDER_TRACK_HOURS_HOURLY:g} ч, дневные — {settings.LADDER_TRACK_HOURS_DAILY:g} ч)\n"
        f"С прошлого zip: стаканов {sink.counts['books']}, сделок {sink.counts['trades']}, исходов {sink.counts['outcomes']}"
        f"{nxt}{err}\n\nНичего не покупает — только собирает данные для проверки «продажи лотереек»."
    )


async def send_report() -> None:
    from src import telegram_notify  # локальный импорт: telegram_notify импортирует этот модуль
    zip_path, counts, started, ended = _get_sink().rotate()
    if not zip_path:
        return
    caption = (
        f"📒 Лестницы {time.strftime('%d.%m %H:%M', time.gmtime(started))}–{time.strftime('%H:%M', time.gmtime(ended))} UTC: "
        f"стаканов {counts['books']}, сделок {counts['trades']}, исходов {counts['outcomes']}. Перешли мне."
    )
    await telegram_notify.send_document(zip_path, caption)


_next_report_at = 0.0


async def run_forever() -> None:
    global _next_report_at
    if not settings.LADDER_LOGGER_ENABLED:
        return
    _next_report_at = time.time() + settings.REPORT_INTERVAL_HOURS * 3600
    last_discovery = last_trades = 0.0
    limits = httpx.Limits(max_connections=8, max_keepalive_connections=8)
    async with httpx.AsyncClient(timeout=20, limits=limits, headers={"User-Agent": "ladder-logger/1.0"}) as client:
        while True:
            try:
                if runtime_state.get("ladder_logger_enabled"):
                    now = time.time()
                    if now - last_discovery >= 300:
                        await _discover(client)
                        last_discovery = now
                    await _poll_books(client)
                    if now - last_trades >= settings.LADDER_TRADES_POLL_SECONDS:
                        await _poll_trades(client, list(_tracked.values()))
                        last_trades = now
                    await _finalize(client)
                if time.time() >= _next_report_at:
                    _next_report_at = time.time() + settings.REPORT_INTERVAL_HOURS * 3600
                    await send_report()
            except Exception as exc:  # noqa: BLE001 — исследование не должно ронять бота
                _status["errors"] += 1
                _status["last_error"] = f"{type(exc).__name__}: {exc}"
                log.warning("ladder_logger: %s", _status["last_error"])
            await asyncio.sleep(settings.LADDER_POLL_SECONDS)
