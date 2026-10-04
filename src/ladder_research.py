"""
🔬 Исследование «лестниц» Polymarket — ничего не торгует, только собирает данные.

Зачем. На 15-минутных рынках Up/Down мы проверили всё, что можно сделать нашей
скоростью: цена там почти честная, а сделки «по первому касанию» на живых
деньгах и в DRY RUN дали 20 побед из 31 (65%) при безубыточности ~81.5%.
Выигрывают там те, кто быстрее, — с ними мы не соревнуемся.

Другая гипотеза, где скорость не важна. В рынках «Bitcoin above ___ on <дата>»
(лестница страйков, исход — закрытие минутной свечи Binance BTC/USDT в 12:00 по
Нью-Йорку) цены дальних страйков, похоже, заложены с волатильностью уровня
опционов (~30% годовых), а реальная волатильность BTC чаще ниже (премия за риск
волатильности), плюс известный перекос в пользу «лотерейных билетов». Если это
так, систематическая покупка «надёжной» стороны дальних страйков даёт плюс.
Проверять на живых деньгах не нужно: Polymarket хранит историю цен, и за
несколько минут можно выгрузить месяцы данных с исходами.

Что делает run(days):
1) Gamma API: находит события по слагам bitcoin-above-on-<месяц>-<день>-<год>
   (и ethereum-…), плюс варианты с часом "-1pm-et" за последние дни; берёт страйки
   и токены Yes.
2) CLOB /prices-history: цена Yes каждого страйка за 3 суток до конца события.
3) Binance 1m свечи: цена в моменты выборки и свеча резолюции (по ней — исход).
4) Пишет CSV и присылает его в Telegram с короткой сводкой калибровки.
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone

import httpx

from config import settings

_MONTHS = ["january", "february", "march", "april", "may", "june", "july",
           "august", "september", "october", "november", "december"]
_ASSETS = {"btc": ("bitcoin", "BTCUSDT"), "eth": ("ethereum", "ETHUSDT")}
# За сколько часов до конца события берём цену страйка.
OFFSETS_H = [72, 48, 24, 12, 6, 3, 2, 1, 0.5, 0.25]
_HISTORY_SPAN_H = 73
_CONCURRENCY = 6

_running = False


def is_running() -> bool:
    return _running


# ----------------------------------------------------------- разбор данных --

def hour_suffixes() -> list[str]:
    out = []
    for h in range(24):
        h12 = 12 if h % 12 == 0 else h % 12
        out.append(f"{h12}{'am' if h < 12 else 'pm'}-et")
    return out


def slugs_for(name: str, d: date, hourly: bool) -> list[str]:
    base = f"{name}-above-on-{_MONTHS[d.month - 1]}-{d.day}"
    slugs = [f"{base}-{d.year}", base]
    if hourly:
        slugs += [f"{base}-{d.year}-{suf}" for suf in hour_suffixes()]
    return slugs


def parse_strike(*texts: str | None) -> float | None:
    """'84,000' / '↑ 90,000' / '$84k' / 'Will the price … above $74,000 on …' -> число."""
    for text in texts:
        if not text:
            continue
        m = re.search(r"\$?\s*([\d][\d,]*(?:\.\d+)?)\s*([kK])?", text)
        if not m:
            continue
        try:
            val = float(m.group(1).replace(",", ""))
        except ValueError:
            continue
        if m.group(2):
            val *= 1000
        if val > 0:
            return val
    return None


def parse_iso(ts: str | None) -> int | None:
    if not ts:
        return None
    try:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def markets_from_event(event: dict, asset: str, slug: str) -> list[dict]:
    end_ts = parse_iso(event.get("endDate"))
    if end_ts is None:
        return []
    kind = "hourly" if slug.endswith("-et") else "daily"
    out = []
    for m in event.get("markets") or []:
        strike = parse_strike(m.get("groupItemTitle"), m.get("question"))
        try:
            tokens = m.get("clobTokenIds")
            tokens = json.loads(tokens) if isinstance(tokens, str) else tokens
            outcomes = m.get("outcomes")
            outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
        except (TypeError, ValueError):
            continue
        if strike is None or not tokens or len(tokens) < 2:
            continue
        yes_idx = 0
        if outcomes and len(outcomes) == len(tokens):
            for i, o in enumerate(outcomes):
                if str(o).strip().lower() == "yes":
                    yes_idx = i
        m_end = parse_iso(m.get("endDate")) or end_ts
        out.append({
            "asset": asset, "kind": kind, "slug": event.get("slug") or slug,
            "end_ts": m_end, "strike": strike, "yes_token": str(tokens[yes_idx]),
            "no_token": str(tokens[1 - yes_idx]) if len(tokens) == 2 else "",
            "condition_id": m.get("conditionId") or "",
            "closed": bool(m.get("closed")),
            "volume": m.get("volumeNum") or m.get("volume"),
            "liquidity": m.get("liquidityNum") or m.get("liquidity"),
        })
    return out


def price_at(history: list[tuple[int, float]], target_ts: int, max_age_s: int) -> tuple[float | None, int | None]:
    """Последняя цена не позже target_ts и не старше max_age_s."""
    best = None
    for t, p in history:  # отсортировано по t
        if t <= target_ts:
            best = (t, p)
        else:
            break
    if best is None or target_ts - best[0] > max_age_s:
        return None, None
    return best[1], (target_ts - best[0]) // 60


# --------------------------------------------------------------- сеть -------

async def _get_json(client: httpx.AsyncClient, url: str, params: dict | None = None, tries: int = 4):
    for i in range(tries):
        try:
            r = await client.get(url, params=params)
            if r.status_code == 429:
                await asyncio.sleep(2 + 3 * i)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, ValueError):
            if i == tries - 1:
                return None
            await asyncio.sleep(1 + i)
    return None


async def _discover(client: httpx.AsyncClient, days: int, hourly_days: int) -> list[dict]:
    today = datetime.now(timezone.utc).date()
    sem = asyncio.Semaphore(_CONCURRENCY)
    jobs = []
    for back in range(1, days + 1):
        d = today - timedelta(days=back)
        for asset, (name, _sym) in _ASSETS.items():
            hourly = asset == "btc" and back <= hourly_days
            for slug in slugs_for(name, d, hourly):
                jobs.append((asset, slug, d))

    found: dict[str, list[dict]] = {}

    async def one(asset: str, slug: str, d: date):
        async with sem:
            data = await _get_json(client, f"{settings.GAMMA_HOST}/events", {"slug": slug})
        if not data:
            return
        events = data if isinstance(data, list) else [data]
        for ev in events:
            end_ts = parse_iso(ev.get("endDate"))
            if end_ts is None:
                continue
            # Слаг без года может указать на прошлогоднее событие — проверяем дату.
            if abs(datetime.fromtimestamp(end_ts, tz=timezone.utc).date() - d) > timedelta(days=1):
                continue
            key = ev.get("slug") or slug
            if key not in found:
                found[key] = markets_from_event(ev, asset, key)

    await asyncio.gather(*(one(*j) for j in jobs))
    now = int(time.time())
    # Только закончившиеся события (исход уже известен по свече Binance).
    return [m for ms in found.values() for m in ms if m["end_ts"] < now - 120]


def max_age_for(offset_h: float) -> int:
    """Насколько старой может быть последняя цена до момента выборки."""
    return max(900, min(3600, int(offset_h * 3600 / 4)))


async def _price_histories(client: httpx.AsyncClient, markets: list[dict]) -> None:
    """Для каждого страйка — цены Yes в моменты OFFSETS_H. Полную историю не
    храним (тысячи токенов × сотни точек съели бы память контейнера)."""
    sem = asyncio.Semaphore(_CONCURRENCY)

    async def one(m: dict):
        params = {"market": m["yes_token"], "startTs": m["end_ts"] - _HISTORY_SPAN_H * 3600,
                  "endTs": m["end_ts"], "fidelity": 5}
        def _empty(d) -> bool:
            return not d or not (d.get("history") if isinstance(d, dict) else d)

        async with sem:
            data = await _get_json(client, f"{settings.POLY_HOST}/prices-history", params)
            if _empty(data):
                params["fidelity"] = 60
                data = await _get_json(client, f"{settings.POLY_HOST}/prices-history", params)
            if _empty(data):
                # У части закрытых рынков диапазон startTs/endTs отдаёт пусто, а
                # interval=max — нет. Лишнее отрежет price_at().
                data = await _get_json(client, f"{settings.POLY_HOST}/prices-history",
                                       {"market": m["yes_token"], "interval": "max", "fidelity": 60})
        hist = data.get("history", []) if isinstance(data, dict) else (data or [])
        pts = []
        for h in hist:
            try:
                pts.append((int(h["t"]), float(h["p"])))
            except (KeyError, TypeError, ValueError):
                continue
        pts.sort()
        m["n_hist"] = len(pts)
        m["samples"] = {off: price_at(pts, int(m["end_ts"] - off * 3600), max_age_for(off)) for off in OFFSETS_H}

    await asyncio.gather(*(one(m) for m in markets))


async def _binance_closes(client: httpx.AsyncClient, symbol: str, start_ts: int, end_ts: int) -> dict[int, float]:
    """Закрытия 1m свечей: {время открытия свечи (сек): close}."""
    closes: dict[int, float] = {}
    sem = asyncio.Semaphore(4)
    chunk = 1000 * 60
    starts = list(range(start_ts - start_ts % 60, end_ts + 60, chunk))

    async def one(st: int):
        async with sem:
            data = await _get_json(client, f"{settings.BINANCE_BASE_URL}/api/v3/klines",
                                   {"symbol": symbol, "interval": "1m", "startTime": st * 1000, "limit": 1000})
        for k in data or []:
            try:
                closes[int(k[0]) // 1000] = float(k[4])
            except (IndexError, TypeError, ValueError):
                continue

    await asyncio.gather(*(one(s) for s in starts))
    return closes


# ------------------------------------------------------------ главное -------

def _summary(rows: list[dict]) -> str:
    lines = []
    for off in (24, 6, 1):
        sel = [r for r in rows if r["offset_h"] == off and r["p_yes"] != "" and r["outcome_yes"] != ""]
        if not sel:
            continue
        parts = []
        for lo, hi in ((0, 0.05), (0.05, 0.2), (0.2, 0.8), (0.8, 0.95), (0.95, 1.01)):
            b = [r for r in sel if lo <= float(r["p_yes"]) < hi]
            if len(b) >= 5:
                avg_p = sum(float(r["p_yes"]) for r in b) / len(b)
                win = sum(int(r["outcome_yes"]) for r in b) / len(b)
                parts.append(f"{lo:.2f}–{min(hi, 1):.2f}: цена {avg_p:.3f} → Yes {win:.3f} (n={len(b)})")
        if parts:
            lines.append(f"За {off} ч до конца:\n  " + "\n  ".join(parts))
    return "\n".join(lines)


async def run(days: int = 120, hourly_days: int = 10) -> None:
    global _running
    if _running:
        return
    _running = True
    from src import telegram_notify  # локальный импорт: telegram_notify сам импортирует этот модуль
    started = time.time()
    try:
        await telegram_notify.notify(f"🔬 Выгружаю историю «лестниц» за {days} дн. (часовые — {hourly_days} дн.)…")
        limits = httpx.Limits(max_connections=12, max_keepalive_connections=12)
        async with httpx.AsyncClient(timeout=25, limits=limits, headers={"User-Agent": "ladder-research/1.0"}) as client:
            markets = await _discover(client, days, hourly_days)
            if not markets:
                await telegram_notify.notify("🔬 Не нашёл ни одного события «… above ___ on …». Пришли мне этот ответ.")
                return
            n_events = len({m["slug"] for m in markets})
            await telegram_notify.notify(f"🔬 Нашёл {n_events} событий, {len(markets)} страйков. Качаю историю цен…")
            await _price_histories(client, markets)

            closes: dict[str, dict[int, float]] = {}
            for asset, (_name, symbol) in _ASSETS.items():
                ms = [m for m in markets if m["asset"] == asset]
                if ms:
                    closes[asset] = await _binance_closes(
                        client, symbol, min(m["end_ts"] for m in ms) - _HISTORY_SPAN_H * 3600,
                        max(m["end_ts"] for m in ms) + 120)

        rows = []
        for m in markets:
            cl = closes.get(m["asset"], {})
            res_close = cl.get(m["end_ts"] - m["end_ts"] % 60)
            outcome = "" if res_close is None else int(res_close > m["strike"])
            for off in OFFSETS_H:
                target = int(m["end_ts"] - off * 3600)
                p, age = m.get("samples", {}).get(off, (None, None))
                spot = cl.get(target - target % 60 - 60)
                rows.append({
                    "asset": m["asset"], "kind": m["kind"], "slug": m["slug"],
                    "end_utc": datetime.fromtimestamp(m["end_ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    "end_ts": m["end_ts"], "strike": m["strike"], "offset_h": off,
                    "p_yes": "" if p is None else round(p, 4), "p_age_min": "" if age is None else age,
                    "spot": "" if spot is None else spot, "res_close": "" if res_close is None else res_close,
                    "outcome_yes": outcome, "volume": m.get("volume") or "", "liquidity": m.get("liquidity") or "",
                    "n_hist": m.get("n_hist", 0),
                })

        os.makedirs(settings.REPORTS_DIR, exist_ok=True)
        path = os.path.join(settings.REPORTS_DIR, f"ladders_{datetime.now(timezone.utc):%Y%m%d-%H%M}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

        with_price = sum(1 for r in rows if r["p_yes"] != "")
        caption = (f"🔬 Лестницы: {n_events} событий, {len(markets)} страйков, строк с ценой {with_price}/{len(rows)}, "
                   f"{(time.time() - started) / 60:.1f} мин. Пришли этот файл мне.")
        await telegram_notify.send_document(path, caption[:1000])
        summary = _summary(rows)
        if summary:
            await telegram_notify.notify("🔬 Калибровка (цена Yes против доли исходов Yes):\n" + summary)
    except Exception as exc:  # noqa: BLE001 — исследование не должно ронять бота
        try:
            await telegram_notify.notify(f"🔬 Выгрузка лестниц упала: {type(exc).__name__}: {exc}")
        except Exception:  # noqa: BLE001
            pass
    finally:
        _running = False
