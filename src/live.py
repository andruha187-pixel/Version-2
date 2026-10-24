"""
🎯 Кандидаты на открытых температурных рынках.

Раз в LIVE_POLL_SEC секунд берём открытые события (Gamma: bestBid/bestAsk каждой
корзины). Цена стороны — середина спреда (YES — корзины, NO — 1 − она), как в
истории, по которой считались пороги. Касание уровня — цена впервые стала ≥
уровня, побыв ниже (первый взгляд на рынок касанием не считается: что было до
запуска бота — неизвестно).

Касание → правило (📊 study.find_rule): для города и окна местного времени, только
если та же ячейка проверена и по всем городам. Доход считаем по цене, по которой
можно купить сейчас: сначала по лучшей цене (ask), потом — по стакану: средняя
цена на всю тестовую ставку PAPER_STAKE_USDC. В стакане не хватает до лучшей
цены + FILL_MAX_SLIP — «тонко», не кандидат. Доход ниже порога (кнопки 0…+5%) —
не кандидат. Кандидат — тестовая ставка по этой средней цене и строка в Telegram:
сразу или сводкой раз в час. По одному кандидату на сторону корзины. Итог — по
исходу корзины, когда событие закроется.

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
FILL_MAX_SLIP = 0.03                 # тестовая ставка: по стакану не дальше 3¢ от лучшей цены — иначе «тонко»
DIGEST_SEC = 3600                    # сводка кандидатов — раз в час
TG_CHUNK = 3800

_touch: dict[tuple, dict] = {}       # (event_id, idx, side) -> {уровень: "below" | "touched"}
_last_snap: dict[int, float] = {}
_stats = {"polls": 0, "open_events": 0, "candidates": 0, "last_poll": 0, "last_error": "", "settled": 0,
          "weak": 0,                 # касания с правилом, но по цене покупки доход ниже порога
          "thin": 0,                 # в стакане не хватает на тестовую ставку до лучшей цены + FILL_MAX_SLIP
          "book_err": 0}             # стакан не ответил — тестовая ставка по лучшей цене
_last_settle = {"t": 0.0}
_digest: list[str] = []              # кандидаты для сводки раз в час
_last_digest = {"t": 0.0}


def reset() -> None:
    _touch.clear()
    _last_snap.clear()
    for k in _stats:
        _stats[k] = "" if k == "last_error" else 0
    _last_settle["t"] = 0.0
    _digest.clear()
    _last_digest["t"] = 0.0


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


def ev_at(r: dict, price: float) -> float:
    """Ожидаемый доход на $1 при покупке по price, если сторона выигрывает так же часто, как в правиле.
    Правило «NO от 0.90» собрано из входов 0.90–0.95: по 0.99 те же 98% выигрышей — уже минус."""
    p = float(r["win_pct"]) / 100.0
    return p / price - 1.0 - float(settings.FEE_RATE) * (1.0 - price)


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


async def _book_fill(token: str, best: float, stake: float) -> tuple[str, float | None, float | None]:
    """Покупка на stake по стакану: ("ok", средняя цена, $ до цены + 1¢) / ("thin", None, $) / ("err", None, None)."""
    try:
        bk = await clob.book(token)
    except http_api.ApiError:
        return "err", None, None
    asks = bk["asks"]
    depth = clob.depth_usdc(asks, min(0.999, best + 0.01))
    price = clob.fill_price(asks, stake, min(0.999, best + FILL_MAX_SLIP))
    if price is None:
        return "thin", None, depth
    return "ok", price, depth


async def _notify(msg: str) -> None:
    try:
        from src import telegram_notify
        await telegram_notify.notify(msg)
    except Exception:  # noqa: BLE001
        pass


def _when(ev: dict) -> str:
    return f"{ev['date'][8:10]}.{ev['date'][5:7]}"


async def check_event(ev: dict, now: float) -> int:
    """Касания уровней в одном открытом событии → кандидаты. Возвращает, сколько новых."""
    if not runtime_state.city_alerts_on(ev["city_key"]):
        return 0
    hour = (now - ev["day_start_ts"]) / 3600.0
    window = study.window_of(hour)
    levels = list(settings.STUDY_LEVELS)
    city_only = bool(runtime_state.get("alerts_city_only"))
    sides = {"yes": ("YES",), "no": ("NO",)}.get(str(runtime_state.get("alerts_sides")), ("YES", "NO"))
    min_ev = float(runtime_state.get("alert_min_ev"))
    made, short = [], []
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
                if buy > level + float(settings.STUDY_MAX_JUMP) + float(settings.STUDY_SLIP) + 1e-9:
                    continue                       # этот уровень цена перепрыгнула — по нему так не входили
                r = study.find_rule(ev["city_key"], ev["kind"], side, level, window, city_only=city_only)
                if r is None:
                    continue
                e = ev_at(r, buy)
                if best is None or e > best[2] + 1e-12 or (abs(e - best[2]) <= 1e-12 and level > best[0]):
                    best = (level, r, e)           # при равном доходе — уровень ближе к цене
            if best is None:
                continue
            level, r, e = best
            if 100 * e < min_ev - 1e-9:
                _stats["weak"] += 1                # по лучшей цене правило не окупается (или доход мал)
                continue
            stake = float(runtime_state.get("paper_stake"))
            token = b["yes_token"] if side == "YES" else b["no_token"]
            status, fill, depth = await _book_fill(token, buy, stake)
            if status == "thin":
                _stats["thin"] += 1                # на ставку в стакане не хватает — так не войти
                continue
            if status == "err":
                _stats["book_err"] += 1
                fill = buy
            e = ev_at(r, fill)
            if 100 * e < min_ev - 1e-9:
                _stats["weak"] += 1                # по средней цене на всю ставку — уже ниже порога
                continue
            res = _candidate(ev, b, side, level, mid, buy, fill, depth, hour, window, r, now, e, stake)
            if res:
                made.append(res[0])
                short.append(res[1])
    if made and runtime_state.get("alerts_enabled"):
        if runtime_state.get("alerts_hourly"):
            _digest.extend(short)
        else:
            head = (f"🎯 {ev['city']} · {study.KIND_RU.get(ev['kind'], ev['kind'])} · {_when(ev)} "
                    f"(сейчас там {text.local_when(ev['date'], hour)})")
            await _notify("\n\n".join([head] + made))
    return len(made)


def _candidate(ev: dict, b: dict, side: str, level: float, mid: float, buy: float, fill: float,
               depth: float | None, hour: float, window: str, r: dict, now: float, e: float,
               stake: float) -> tuple[str, str] | None:
    """Записать кандидата и тестовую ставку. (строка для сообщения, строка для сводки) или None (уже был)."""
    entry = round(fill, 4)
    fee = round(stake * float(settings.FEE_RATE) * (1 - entry), 4)
    storage.event_upsert(ev)
    cid = storage.candidate_add(ts=int(now), event_id=ev["id"], idx=b["idx"], side=side, level=level,
                                price=round(mid, 4), entry=entry, best=round(buy, 4), window=window,
                                hour=round(hour, 2), rule=study.rule_text(r, short=True), rule_n=r["n"],
                                rule_wins=r["wins"], rule_ev=r["ev_pct"], depth_usdc=depth, stake=stake,
                                shares=round(stake / entry, 4), fee=fee, ev_now=round(100 * e, 2))
    if not cid:
        return None
    _stats["candidates"] += 1
    bucket = weather_parse.bucket_label(b["lo"], b["hi"], ev["unit"])
    price_txt = f"по {entry:.3f}" + (f" (лучшая {buy:.3f}, на ${stake:g} по стакану — {entry:.3f})"
                                     if entry > buy + 0.0005 else "")
    long = (f"«{bucket}» {side} {price_txt} — по этой цене ожидаемо {100 * e:+.1f}% на ставку\n"
            f"Правило: {study.rule_text(r, short=True)}\n"
            + (f"В стакане до {min(0.999, buy + 0.01):.3f}: ${depth:,.0f}. " if depth is not None else "")
            + f"Тестовая ставка ${stake:g}.")
    short = (f"{ev['city']} · {study.KIND_RU.get(ev['kind'], ev['kind'])} · {_when(ev)} «{bucket}» {side} "
             f"по {entry:.3f}, {100 * e:+.1f}%" + (f", в стакане ${depth:,.0f}" if depth is not None else "")
             + f" · {window}")
    return long, short


def digest_messages(lines: list[str], stake: float) -> list[str]:
    """Сводка кандидатов за час — кусками под лимит Telegram."""
    if not lines:
        return []
    head = (f"🎯 Кандидаты за час: {len(lines)} (тестовые ставки по ${stake:g}; цена — средняя по стакану на "
            "всю ставку, % — ожидаемый доход по ней):")
    out, cur = [], head
    for ln in lines:
        if len(cur) + 1 + len(ln) > TG_CHUNK:
            out.append(cur)
            cur = "🎯 (продолжение)"
        cur += "\n" + ln
    out.append(cur)
    return out


async def flush_digest(now: float | None = None, force: bool = False) -> int:
    """Отправить сводку, если прошёл час (или force). Возвращает, сколько кандидатов ушло."""
    now = time.time() if now is None else now
    if not _digest or (not force and now - _last_digest["t"] < DIGEST_SEC):
        return 0
    lines = list(_digest)
    _digest.clear()
    _last_digest["t"] = now
    for msg in digest_messages(lines, float(runtime_state.get("paper_stake"))):
        await _notify(msg)
    return len(lines)


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


def _summ(cs: list[dict]) -> dict:
    settled = [c for c in cs if c["outcome"] is not None]
    wins = sum(1 for c in settled if (c["outcome"] == 1) == (c["side"] == "YES"))
    return {"n": len(cs), "settled": len(settled), "wins": wins, "pnl": sum(c["pnl"] or 0 for c in settled),
            "staked": sum(c["stake"] or 0 for c in settled)}


def paper_stats(since_ts: int = 0) -> dict:
    return _summ(storage.candidates("c.ts >= ?", (since_ts,)))


def paper_by_window(since_ts: int = 0) -> list[tuple[str, dict]]:
    """Тестовые ставки по окнам времени (за 2+ дня, накануне, …) — в порядке окон."""
    groups: dict[str, list] = {}
    for c in storage.candidates("c.ts >= ?", (since_ts,)):
        w = c.get("window") or study.window_of(c["hour"] if c["hour"] is not None else 0.0)
        groups.setdefault(w, []).append(c)
    order = {w: i for i, w in enumerate(study.WINDOW_NAMES)}
    return [(w, _summ(cs)) for w, cs in sorted(groups.items(), key=lambda kv: order.get(kv[0], 99))]


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
            try:
                if not runtime_state.get("alerts_enabled"):
                    _digest.clear()
                elif not runtime_state.get("alerts_hourly"):
                    await flush_digest(force=True)      # переключили на «сразу» — остаток сводки отправить
                else:
                    await flush_digest()
            except Exception as exc:  # noqa: BLE001
                log.warning("Сводка кандидатов: %s", exc)
        await asyncio.sleep(max(15.0, float(settings.LIVE_POLL_SEC)))
