"""
🔁 Серии UP/DOWN — тест на Polymarket без денег (окна 15m и 5m).

Зачем. По году свечей Binance (📚) после 3–4+ одинаковых исходов подряд
следующее 15-минутное окно чаще разворачивается: 54–56%, устойчиво по всем
монетам и месяцам. Ставка на разворот окупается, если разворот чаще ~52.7%
(ask 0.51 + комиссия). Проверяем на самом Polymarket: настоящие исходы и
настоящие цены, в том числе «подождать, пока цена пойдёт вниз».

Что пишем по каждому окну (таблица series_windows):
  - ask и bid обеих сторон, когда впервые увидели рынок (в первую минуту);
  - путь ask обеих сторон каждые SERIES_PATH_STEP_SEC секунд;
  - минимум ask каждой стороны до минуты до конца — исполнилась бы лимитка;
  - исход: ранний (стакан в последние секунды, иначе Chainlink) и официальный.
Серию считаем по исходам прошлых окон (до запуска бота — официальные исходы
последних окон из Gamma API). Ставку на разворот считаем при подведении
итогов: на старте по ask (тейкер, с комиссией) и лимиткой 0.48/0.45/0.42/0.40
(мейкер, без комиссии). Ничего не покупает.

Рынки 5m берём у основного цикла (main._active_markets), 15m — ищем сами по
слагу {монета}-updown-15m-{начало} и подписываем их токены на живой стакан.
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field

from config import settings
from src import book_stream, chainlink_feed, market_discovery, runtime_state, storage

log = logging.getLogger("series_bot")

STEP = {"5m": 300, "15m": 900}
FIRST_OBS_MAX_SEC = 60         # цену старта берём, если увидели рынок в первую минуту окна
END_GUARD_SEC = 60             # лимитка «подождать дешевле» — до минуты до конца
BOOK_WINDOW_SEC = 12.0         # исход по стакану: читаем в последние 12 с…
BOOK_LAST_SEC = 3.0            # …и верим последнему чтению, если оно не раньше чем за 3 с до конца
BOOK_LO, BOOK_HI = 0.03, 0.97
EARLY_WAIT_SEC = 30            # ждём точку Chainlink после конца окна не дольше
SEED_WINDOWS = 12              # сколько прошлых окон подтянуть при запуске (официальные исходы)
SETTLE_EVERY_SEC = 60
SETTLE_LAG_SEC = 120           # официальный исход спрашиваем не раньше чем через 2 мин после конца
SETTLE_MAX_AGE_SEC = 2 * 86400
FLUSH_EVERY_SEC = 30
DISCOVERY_RETRY_SEC = 20
LIMITS = (0.48, 0.45, 0.42, 0.40)
KS = (3, 4)
REQUEST_PAUSE_SEC = 0.2


@dataclass
class _Tf:
    """Минимальный профиль для market_discovery.get_active_market."""
    label: str
    interval_minutes: int


@dataclass
class Win:
    asset: str
    interval: str
    start: int
    end: int
    slug: str
    up: str
    down: str
    own_tokens: bool = False
    first: bool = False
    path: list = field(default_factory=list)
    last_path_t: float = -1e9
    up_min: float | None = None
    up_min_t: float | None = None
    down_min: float | None = None
    down_min_t: float | None = None
    last_flush: float = 0.0
    book_res: str | None = None
    book_res_t: float = 0.0


_cur: dict[tuple[str, str], Win] = {}
_pending: list[Win] = []
_retry_at: dict[tuple[str, str, int], float] = {}
_outcomes: dict[tuple[str, str], dict[int, str]] = defaultdict(dict)   # (монета, интервал) -> {старт: исход}
_seeded: set = set()
_supported: set = set()                     # (монета, интервал), у которых рынок хоть раз нашёлся
_fails: dict[tuple[str, str], int] = defaultdict(int)          # окон подряд без рынка
_skip_until: dict[tuple[str, str], float] = {}
_last_start: dict[tuple[str, str], int] = {}
_tasks: set = set()
UNSUPPORTED_AFTER = 3          # рынка нет 3 окна подряд (с первого запуска) — монету пропускаем…
UNSUPPORTED_SKIP_SEC = 6 * 3600  # …на 6 часов
_stats = {"windows": 0, "priced": 0, "early": 0, "official": 0, "discovery_errors": 0, "last_error": ""}


def reset() -> None:
    _cur.clear()
    _pending.clear()
    _retry_at.clear()
    _outcomes.clear()
    _seeded.clear()
    _supported.clear()
    _fails.clear()
    _skip_until.clear()
    _last_start.clear()
    for k in _stats:
        _stats[k] = "" if k == "last_error" else 0


def enabled() -> bool:
    return bool(runtime_state._state.get("series_test_enabled", settings.SERIES_TEST_ENABLED))


def set_enabled(on: bool) -> None:
    runtime_state.set("series_test_enabled", bool(on))


def intervals() -> list[str]:
    out = []
    for iv in str(settings.SERIES_INTERVALS or "").split(","):
        iv = iv.strip().lower()
        if iv in STEP and iv not in out:
            out.append(iv)
    return out


def path_step() -> int:
    return max(1, int(settings.SERIES_PATH_STEP_SEC))


def fee_per_dollar(price: float) -> float:
    return float(settings.FEE_RATE) * (1 - price)


def slug_for(asset: str, interval: str, start: int) -> str:
    return f"{asset}-updown-{interval}-{start}"


def book_winner(up: str, down: str) -> str | None:
    """Победитель по стакану: у победителя нет продавцов дешевле 0.97, проигравшего продают по ≤ 0.03."""
    ua, da = book_stream.best_ask(up), book_stream.best_ask(down)
    if da is not None and da <= BOOK_LO and (ua is None or ua >= BOOK_HI):
        return "UP"
    if ua is not None and ua <= BOOK_LO and (da is None or da >= BOOK_HI):
        return "DOWN"
    return None


# ------------------------------------------------------------- окна ----

async def _open(asset: str, interval: str, start: int, active_markets: dict | None, now: float) -> Win | None:
    step = STEP[interval]
    if interval == "5m":
        m = (active_markets or {}).get(f"{asset}:5m")
        if m is None or int(m.start_time) != start:
            return None                      # основной цикл ещё не нашёл рынок
        own = False
    else:
        key = (asset, interval, start)
        if now < _retry_at.get(key, 0):
            return None
        try:
            m = await market_discovery.get_active_market(asset, _Tf(interval, step // 60))
        except Exception as exc:  # noqa: BLE001 — рынка нет или Gamma недоступна: попробуем позже
            _retry_at[key] = now + DISCOVERY_RETRY_SEC
            _stats["discovery_errors"] += 1
            _stats["last_error"] = f"{asset} {interval}: {exc}"[:200]
            return None
        if int(m.start_time) != start:
            _retry_at[key] = now + DISCOVERY_RETRY_SEC
            return None
        own = bool(settings.USE_LIVE_BOOK_STREAM)
        if own:
            book_stream.subscribe([m.up_token_id, m.down_token_id])
    w = Win(asset, interval, start, start + step, m.slug, m.up_token_id, m.down_token_id, own_tokens=own)
    storage.series_upsert(asset, interval, start, slug=m.slug)
    _supported.add((asset, interval))
    _fails[(asset, interval)] = 0
    _stats["windows"] += 1
    return w


def _flush(w: Win) -> None:
    storage.series_upsert(w.asset, w.interval, w.start, path=json.dumps(w.path, separators=(",", ":")),
                          up_min=w.up_min, up_min_t=w.up_min_t, down_min=w.down_min, down_min_t=w.down_min_t)


def _sample(w: Win, now: float) -> None:
    t = now - w.start
    ua, da = book_stream.best_ask(w.up), book_stream.best_ask(w.down)
    if not w.first and t <= FIRST_OBS_MAX_SEC and ua is not None and da is not None:
        w.first = True
        _stats["priced"] += 1
        storage.series_upsert(w.asset, w.interval, w.start, t_first=round(t, 1), up_ask0=ua, down_ask0=da,
                              up_bid0=book_stream.best_bid(w.up), down_bid0=book_stream.best_bid(w.down))
    if t - w.last_path_t >= path_step() and (ua is not None or da is not None):
        w.path.append([int(round(t)), ua, da])
        w.last_path_t = t
    if w.first and now <= w.end - END_GUARD_SEC:
        if ua is not None and (w.up_min is None or ua < w.up_min):
            w.up_min, w.up_min_t = ua, round(t, 1)
        if da is not None and (w.down_min is None or da < w.down_min):
            w.down_min, w.down_min_t = da, round(t, 1)
    if w.end - BOOK_WINDOW_SEC <= now < w.end:
        w.book_res, w.book_res_t = book_winner(w.up, w.down), now
    if now - w.last_flush >= FLUSH_EVERY_SEC:
        w.last_flush = now
        _flush(w)


def _close(w: Win) -> None:
    _flush(w)
    if w.own_tokens:
        book_stream.unsubscribe([w.up, w.down])
    _pending.append(w)


def resolve_pending(now: float | None = None) -> int:
    """Ранний исход закончившихся окон: стакан последних секунд, иначе Chainlink."""
    now = time.time() if now is None else now
    done = 0
    for w in list(_pending):
        if now < w.end + 2:
            continue
        cl_start = chainlink_feed.price_at(w.asset, w.start)
        cl_end = chainlink_feed.price_after(w.asset, w.end)
        book = w.book_res if w.book_res_t >= w.end - BOOK_LAST_SEC else None
        if book is None and cl_end is None and now < w.end + EARLY_WAIT_SEC:
            continue
        _pending.remove(w)
        early = book
        if early is None and cl_start and cl_end:
            early = "UP" if cl_end >= cl_start else "DOWN"
        storage.series_upsert(w.asset, w.interval, w.start, cl_start=cl_start, cl_end=cl_end, early_outcome=early)
        if early:
            _outcomes[(w.asset, w.interval)].setdefault(w.start, early)
            _stats["early"] += 1
            done += 1
    return done


async def _seed(asset: str, interval: str, start: int) -> None:
    """Официальные исходы окон до запуска — чтобы сразу знать текущую серию."""
    step = STEP[interval]
    for i in range(1, SEED_WINDOWS + 1):
        s = start - i * step
        slug = slug_for(asset, interval, s)
        try:
            res = await market_discovery.get_resolution(slug)
        except Exception:  # noqa: BLE001
            res = None
        if res in ("UP", "DOWN"):
            storage.series_seed(asset, interval, s, slug, res)
            _outcomes[(asset, interval)][s] = res
        await asyncio.sleep(REQUEST_PAUSE_SEC)


async def settle(now: float | None = None) -> int:
    """Официальные исходы закончившихся окон."""
    now = time.time() if now is None else now
    n = 0
    for asset, interval, start, slug in storage.series_unresolved(int(now - SETTLE_LAG_SEC),
                                                                 int(now - SETTLE_MAX_AGE_SEC)):
        try:
            res = await market_discovery.get_resolution(slug)
        except Exception:  # noqa: BLE001
            res = None
        if res in ("UP", "DOWN"):
            storage.series_upsert(asset, interval, start, outcome=res)
            _outcomes[(asset, interval)][start] = res
            _stats["official"] += 1
            n += 1
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return n


def assets_for(interval: str) -> list[str]:
    if interval == "5m":
        return [a for a in settings.ASSETS if runtime_state.is_asset_enabled(a)]
    return list(settings.ASSETS)


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


def _new_window(key: tuple[str, str], start: int, now: float) -> None:
    """Началось новое окно: прошлое без рынка — запомнить (исход спросим официально,
    чтобы не рвать серию), считать подряд идущие пропуски."""
    asset, iv = key
    prev = _last_start.get(key)
    _last_start[key] = start
    if prev is None or prev == start:
        return
    missed = _cur.get(key) is None or _cur[key].start != prev
    if not missed:
        return
    if key in _supported:
        storage.series_upsert(asset, iv, prev, slug=slug_for(asset, iv, prev))
    else:
        _fails[key] += 1
        if _fails[key] >= UNSUPPORTED_AFTER:
            _skip_until[key] = now + UNSUPPORTED_SKIP_SEC
            _fails[key] = 0


async def tick(active_markets: dict | None, now: float | None = None) -> None:
    now = time.time() if now is None else now
    for iv in intervals():
        step = STEP[iv]
        start = int(now // step) * step
        for asset in assets_for(iv):
            key = (asset, iv)
            if _last_start.get(key) != start:
                _new_window(key, start, now)
            if now < _skip_until.get(key, 0):
                continue
            w = _cur.get(key)
            if w is not None and w.start != start:
                _close(w)
                _cur.pop(key, None)
                w = None
            if w is None:
                if now - start > FIRST_OBS_MAX_SEC and iv != "5m":
                    continue                 # цену старта уже не застать — ждём следующее окно
                w = await _open(asset, iv, start, active_markets, now)
                if w is None:
                    continue
                _cur[key] = w
                if key not in _seeded:
                    _seeded.add(key)
                    _spawn(_seed(asset, iv, start))
            _sample(w, now)
    resolve_pending(now)


async def run_forever(active_markets: dict | None = None) -> None:
    last_settle = 0.0
    await asyncio.sleep(3)
    while True:
        try:
            if enabled() and settings.USE_LIVE_BOOK_STREAM:
                await tick(active_markets)
                if time.monotonic() - last_settle >= SETTLE_EVERY_SEC:
                    last_settle = time.monotonic()
                    await settle()
            else:
                for w in list(_cur.values()):
                    _close(w)
                _cur.clear()
        except Exception as exc:  # noqa: BLE001 — тест не должен мешать боту
            log.warning("series_bot: %s", exc)
        await asyncio.sleep(1.0)


# ------------------------------------------------------------- итоги ----

def streak_before(asset: str, interval: str, start: int) -> tuple[str | None, int]:
    """Серия одинаковых исходов перед окном start: (направление, длина)."""
    outs = _outcomes.get((asset, interval), {})
    step = STEP[interval]
    s = start - step
    d = outs.get(s)
    if d is None:
        return None, 0
    k = 0
    while outs.get(s) == d:
        k += 1
        s -= step
    return d, k


def bets(interval: str, since_ts: int = 0) -> list[dict]:
    """Ставки на разворот по записанным окнам: серия перед окном, цена разворотной
    стороны на старте и её минимум до минуты до конца, исход (официальный, иначе ранний)."""
    step = STEP[interval]
    rows = storage.series_rows(interval, since_ts - 40 * step)
    by_asset: dict[str, list] = defaultdict(list)
    for r in rows:
        by_asset[r["asset"]].append(r)
    out = []
    for a, rs in by_asset.items():
        prev, run_dir, run_len = None, None, 0
        for r in rs:
            o = r["outcome"] or r["early_outcome"]
            s = r["start_ts"]
            contiguous = prev is not None and s - prev == step
            if not contiguous:
                run_dir, run_len = None, 0
            if (s >= since_ts and not r["seeded"] and run_dir is not None and o in ("UP", "DOWN")
                    and r["up_ask0"] is not None and r["down_ask0"] is not None):
                rev = "DOWN" if run_dir == "UP" else "UP"
                out.append({"asset": a, "start": s, "k": run_len, "side": rev, "win": o == rev,
                            "ask0": r["down_ask0"] if rev == "DOWN" else r["up_ask0"],
                            "min": r["down_min"] if rev == "DOWN" else r["up_min"],
                            "official": r["outcome"] is not None})
            if o not in ("UP", "DOWN"):
                run_dir, run_len = None, 0
            elif contiguous and o == run_dir:
                run_len += 1
            else:
                run_dir, run_len = o, 1
            prev = s
    return out


def plan_stats(bs: list[dict], k_min: int) -> dict:
    sel = [b for b in bs if b["k"] >= k_min]
    res = {"n": len(sel), "wins": sum(1 for b in sel if b["win"]), "taker": None, "limits": {}}
    if sel:
        pnl = [(1 / b["ask0"] - 1 - fee_per_dollar(b["ask0"])) if b["win"] else (-1 - fee_per_dollar(b["ask0"]))
               for b in sel if b["ask0"] and b["ask0"] > 0]
        res["taker"] = (sum(pnl) / len(pnl) if pnl else None, sum(b["ask0"] for b in sel) / len(sel))
    for lim in LIMITS:
        filled = [b for b in sel if b["min"] is not None and b["min"] <= lim + 1e-9]
        if filled:
            pnl = [(1 / lim - 1) if b["win"] else -1.0 for b in filled]
            res["limits"][lim] = (len(filled), sum(1 for b in filled if b["win"]), sum(pnl) / len(pnl))
        else:
            res["limits"][lim] = (0, 0, None)
    return res


def _pct(x) -> str:
    return f"{x * 100:+.1f}%" if x is not None else "—"


def interval_lines(interval: str, since_ts: int = 0) -> list[str]:
    rows = [r for r in storage.series_rows(interval, since_ts) if not r["seeded"]]
    priced = [r for r in rows if r["up_ask0"] is not None and r["down_ask0"] is not None]
    known = [r for r in priced if r["outcome"] or r["early_outcome"]]
    if not rows:
        return [f"{interval}: окон пока нет"]
    per = defaultdict(int)
    for r in rows:
        per[r["asset"]] += 1
    lines = [f"{interval}: окон {len(rows)} (с ценой старта {len(priced)}, с исходом {len(known)}) · "
             + " · ".join(f"{a.upper()} {n}" for a, n in sorted(per.items()))]
    if priced:
        avg_ask = sum(min(r["up_ask0"], r["down_ask0"]) + max(r["up_ask0"], r["down_ask0"]) for r in priced) / len(priced)
        lines.append(f"  сумма ask сторон на старте в среднем {avg_ask:.3f}")
    bs = bets(interval, since_ts)
    for k in KS:
        st = plan_stats(bs, k)
        if not st["n"]:
            lines.append(f"  после {k}+ подряд: ставок пока нет")
            continue
        taker = st["taker"]
        lines.append(f"  после {k}+ подряд: {st['n']} окон, разворот {st['wins'] / st['n'] * 100:.1f}%; на старте "
                     f"по ask (сред. {taker[1]:.3f}): {_pct(taker[0])} на $1")
        parts = []
        for lim, (n, w, pnl) in st["limits"].items():
            parts.append(f"{lim:.2f}: {n}/{st['n']}" + (f", разворот {w / n * 100:.0f}%, {_pct(pnl)}" if n else ""))
        lines.append("    лимитка (без комиссии) — исполнилась / итог: " + " · ".join(parts))
    return lines


def current_lines(now: float | None = None) -> list[str]:
    """Текущие серии: где сейчас была бы ставка на разворот."""
    now = time.time() if now is None else now
    out = []
    for iv in intervals():
        step = STEP[iv]
        start = int(now // step) * step
        parts = []
        for a in assets_for(iv):
            d, k = streak_before(a, iv, start)
            if d and k >= min(KS):
                parts.append(f"{a.upper()} {k}×{d} → {'DOWN' if d == 'UP' else 'UP'}")
        if parts:
            out.append(f"Сейчас серии {iv}: " + " · ".join(parts))
    return out


def summary_lines(since_ts: int = 0) -> list[str]:
    lines = []
    for iv in intervals():
        lines += interval_lines(iv, since_ts)
    return lines


def menu_text() -> str:
    lines = [
        "🔁 Серии — тест на Polymarket " + ("(запись идёт" if enabled() else "(запись выключена") +
        ", ничего не покупает)",
        "",
        "По каждому окну пишем цены сторон на старте и каждые "
        f"{path_step()} с, исход. Ставка — на разворот после серии одинаковых исходов: на старте по ask "
        "(с комиссией) и лимиткой дешевле (без комиссии, если цена дошла до неё до минуты до конца).",
        "",
    ]
    lines += summary_lines()
    cur = current_lines()
    if cur:
        lines += [""] + cur
    lines += ["", "Для сравнения, год свечей Binance: после 4+ подряд разворот 15m — 55%, 5m — 53%; "
              "окупается от ~52.7% при ask 0.51."]
    if _stats["last_error"] and _stats["discovery_errors"]:
        lines.append(f"Поиск рынков: ошибок {_stats['discovery_errors']}, последняя: {_stats['last_error']}")
    return "\n".join(lines)


def report_caption(from_label: str, to_label: str, since_ts: int) -> str:
    return "\n".join([f"🔁 Серии — тест {from_label} → {to_label} (UTC)"] + summary_lines(since_ts))
