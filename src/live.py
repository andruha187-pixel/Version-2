"""
🎯 Кандидаты на открытых температурных рынках.

Раз в LIVE_POLL_SEC секунд берём открытые события (Gamma: bestBid/bestAsk каждой
корзины). Цена стороны — середина спреда (YES — корзины, NO — 1 − она), как в
истории, по которой считались пороги. Касание уровня — цена впервые стала ≥
уровня, побыв ниже (первый взгляд на рынок касанием не считается: что было до
запуска бота — неизвестно).

Касание → ищем проверенное правило (📊): город и окно местного времени, потом
город в любое время, потом все города (если не «только свои»). Нашлось —
кандидат: сообщение в Telegram с ценой покупки (ask), объёмом в стакане и
тестовая ставка PAPER_STAKE_USDC. По одному кандидату на сторону корзины.
Итог — по исходу корзины, когда событие закроется.

Раз в SNAPSHOT_EVERY_MIN минут цены всех корзин открытых событий пишутся в базу
(📤 «снимки») — для будущих расчётов по стакану.
"""
from __future__ import annotations
import asyncio
import logging
import time

from config import settings
from src import clob, gamma, http_api, runtime_state, storage, study, text, weather_parse

log = logging.getLogger("live")

LOOKBACK_SEC = 3 * 86400
LOOKAHEAD_SEC = 10 * 86400
SETTLE_EVERY_SEC = 600
SETTLE_AFTER_SEC = 26 * 3600         # итог спрашиваем не раньше чем через 2 ч после конца местных суток

_touch: dict[tuple, dict] = {}       # (event_id, idx, side) -> {уровень: "below" | "touched"}
_last_snap: dict[int, float] = {}
_stats = {"polls": 0, "open_events": 0, "candidates": 0, "last_poll": 0, "last_error": "", "settled": 0}
_last_settle = {"t": 0.0}


def reset() -> None:
    _touch.clear()
    _last_snap.clear()
    for k in _stats:
        _stats[k] = "" if k == "last_error" else 0
    _last_settle["t"] = 0.0


def stats() -> dict:
    return dict(_stats)


def side_prices(b: dict, side: str) -> tuple[float | None, float | None]:
    """(середина спреда, цена покупки) стороны корзины."""
    bid, ask = b.get("best_bid"), b.get("best_ask")
    if bid is None or ask is None or ask <= 0 or bid < 0 or bid > ask:
        return None, None
    mid = (bid + ask) / 2
    if side == "YES":
        return mid, ask
    return 1.0 - mid, 1.0 - bid


def crossings(key: tuple, value: float, levels) -> list[float]:
    """Уровни, которых цена впервые достигла сейчас (побыв ниже)."""
    st = _touch.setdefault(key, {})
    out = []
    for level in levels:
        s = st.get(level)
        if s is None:
            st[level] = "touched" if value >= level - 1e-9 else "below"
        elif s == "below" and value >= level - 1e-9:
            st[level] = "touched"
            out.append(level)
    return out


def _snapshot(ev: dict, now: float) -> None:
    if now - _last_snap.get(ev["id"], 0) < float(settings.SNAPSHOT_EVERY_MIN) * 60:
        return
    _last_snap[ev["id"]] = now
    storage.event_upsert(ev)
    storage.snapshot_add(ev["id"], int(now), [[b["idx"], b.get("best_bid"), b.get("best_ask"), b.get("last"),
                                               b.get("liquidity")] for b in ev["buckets"]])


async def _depth(token: str, max_price: float) -> float | None:
    try:
        bk = await clob.book(token)
    except http_api.ApiError:
        return None
    return clob.depth_usdc(bk["asks"], max_price)


async def _notify(msg: str) -> None:
    try:
        from src import telegram_notify
        await telegram_notify.notify(msg)
    except Exception:  # noqa: BLE001
        pass


async def check_event(ev: dict, now: float) -> int:
    """Касания уровней в одном открытом событии → кандидаты. Возвращает, сколько новых."""
    if not runtime_state.city_alerts_on(ev["city_key"]):
        return 0
    hour = (now - ev["day_start_ts"]) / 3600.0
    window = study.window_of(hour)
    levels = list(settings.STUDY_LEVELS)
    city_only = bool(runtime_state.get("alerts_city_only"))
    sides = {"yes": ("YES",), "no": ("NO",)}.get(str(runtime_state.get("alerts_sides")), ("YES", "NO"))
    made = []
    for b in ev["buckets"]:
        if b.get("closed") or b.get("accepting") is False:
            continue
        for side in sides:
            mid, buy = side_prices(b, side)
            if mid is None:
                continue
            crossed = crossings((ev["id"], b["idx"], side), mid, levels)
            best = None
            for level in crossed:
                r = study.find_rule(ev["city_key"], ev["kind"], side, level, window, city_only=city_only)
                if r and (best is None or r["ev_pct"] > best[1]["ev_pct"]):
                    best = (level, r)
            if best is None:
                continue
            level, r = best
            if buy is None or buy > level + float(settings.STUDY_MAX_JUMP) + float(settings.STUDY_SLIP) + 1e-9:
                continue                           # уже перепрыгнула — по правилу так не входили
            line = await _candidate(ev, b, side, level, mid, buy, hour, r, now)
            if line:
                made.append(line)
    if made and runtime_state.get("alerts_enabled"):
        head = (f"🎯 {ev['city']} · {study.KIND_RU.get(ev['kind'], ev['kind'])} · {ev['date'][8:10]}.{ev['date'][5:7]} "
                f"(местное {text.hhmm_local(hour)})")
        await _notify("\n\n".join([head] + made))
    return len(made)


async def _candidate(ev: dict, b: dict, side: str, level: float, mid: float, buy: float, hour: float,
                     r: dict, now: float) -> str | None:
    """Записать кандидата и тестовую ставку. Строка для сообщения или None (уже был)."""
    token = b["yes_token"] if side == "YES" else b["no_token"]
    depth = await _depth(token, min(0.999, buy + 0.01))
    stake = float(runtime_state.get("paper_stake"))
    entry = round(buy, 4)
    fee = round(stake * float(settings.FEE_RATE) * (1 - entry), 4)
    storage.event_upsert(ev)
    cid = storage.candidate_add(ts=int(now), event_id=ev["id"], idx=b["idx"], side=side, level=level,
                                price=round(mid, 4), entry=entry, hour=round(hour, 2), rule=study.rule_text(r, short=True),
                                rule_n=r["n"], rule_wins=r["wins"], rule_ev=r["ev_pct"], depth_usdc=depth,
                                stake=stake, shares=round(stake / entry, 4), fee=fee)
    if not cid:
        return None
    _stats["candidates"] += 1
    bucket = weather_parse.bucket_label(b["lo"], b["hi"], ev["unit"])
    return (f"«{bucket}» {side} по {entry:.3f}\n"
            f"Правило: {study.rule_text(r, short=True)}\n"
            + (f"В стакане до {min(0.999, entry + 0.01):.3f}: ${depth:,.0f}. " if depth is not None else "")
            + f"Тестовая ставка ${stake:g}.")


async def settle(now: float | None = None) -> int:
    """Итоги кандидатов по закрытым событиям."""
    now = time.time() if now is None else now
    n = 0
    open_c = storage.candidates("c.outcome IS NULL")
    by_event: dict[int, list] = {}
    for c in open_c:
        by_event.setdefault(c["event_id"], []).append(c)
    for eid, cs in by_event.items():
        ev = storage.event(eid)
        if ev and now - ev["day_start_ts"] < SETTLE_AFTER_SEC:
            continue
        bks = {b["idx"]: b for b in storage.buckets(eid)} if ev and ev["closed"] else {}
        if not bks or any(bks.get(c["idx"], {}).get("outcome") is None for c in cs):
            try:
                raw = await gamma.get_event(eid)
            except http_api.ApiError:
                raw = None
            parsed = weather_parse.parse_event(raw) if raw else None
            if parsed is None:
                continue
            storage.event_upsert(parsed)
            bks = {b["idx"]: b for b in parsed["buckets"]}
        for c in cs:
            b = bks.get(c["idx"])
            if not b or b.get("outcome") not in (0, 1):
                continue
            win = (b["outcome"] == 1) == (c["side"] == "YES")
            pnl = round((c["shares"] if win else 0.0) - c["stake"] - (c["fee"] or 0.0), 4)
            if storage.candidate_settle(c["id"], b["outcome"], pnl):
                n += 1
    _stats["settled"] += n
    return n


async def poll(now: float | None = None) -> int:
    """Один проход по открытым событиям. Возвращает, сколько новых кандидатов."""
    now = time.time() if now is None else now
    raws = await gamma.find_events(closed=False, t0=now - LOOKBACK_SEC, t1=now + LOOKAHEAD_SEC)
    made = 0
    n_open = 0
    for raw in raws:
        ev = weather_parse.parse_event(raw)
        if ev is None or ev["closed"]:
            continue
        n_open += 1
        _snapshot(ev, now)
        made += await check_event(ev, now)
    _stats.update(polls=_stats["polls"] + 1, open_events=n_open, last_poll=int(now))
    return made


def paper_stats(since_ts: int = 0) -> dict:
    cs = storage.candidates("c.ts >= ?", (since_ts,))
    settled = [c for c in cs if c["outcome"] is not None]
    wins = sum(1 for c in settled if (c["outcome"] == 1) == (c["side"] == "YES"))
    return {"n": len(cs), "settled": len(settled), "wins": wins, "pnl": sum(c["pnl"] or 0 for c in settled),
            "staked": sum(c["stake"] or 0 for c in settled)}


async def run_forever() -> None:
    await asyncio.sleep(10)
    while True:
        if settings.LIVE_ENABLED:
            try:
                await poll()
                if time.time() - _last_settle["t"] >= SETTLE_EVERY_SEC:
                    _last_settle["t"] = time.time()
                    await settle()
            except Exception as exc:  # noqa: BLE001
                _stats["last_error"] = str(exc)[:200]
                log.warning("Кандидаты: %s", exc)
        await asyncio.sleep(max(15.0, float(settings.LIVE_POLL_SEC)))
