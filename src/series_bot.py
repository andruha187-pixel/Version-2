"""
🎲 Стратегия серий на 15-минутных рынках Up/Down Polymarket и 🔁 запись окон.

Идея. После 4+ одинаковых исходов подряд следующее 15-минутное окно чаще
разворачивается: по году свечей Binance 55%, по официальным исходам Polymarket
за 60 дней 54.5%. Вход (правило lead60, с 17.10): на 60-й секунде окна, если
сторона разворота уже ведёт (ask 0.52–0.80) — по цене не выше ask + 1¢. За
16 ч теста 16–17.10 так вышло 13 выигрышей из 14.

Ставки — по включённым монетам (🪙): тестовая всегда, реальная — если монета
🔴 и бот в 🔴 LIVE (общий выключатель), есть ключ и не достигнут дневной стоп.
Рядом пишутся виртуальные ставки по другим правилам входа (варианты) — для
сравнения на тех же сигналах.

Что пишем по каждому окну (таблица series_windows), по всем монетам:
  - ask и bid обеих сторон, когда впервые увидели рынок (в первую минуту);
  - путь ask обеих сторон каждые SERIES_PATH_STEP_SEC секунд; у окон стратегии
    ещё с 30 с до старта и первые 90 с — каждые SERIES_FINE_STEP_SEC секунд
    (точки до старта — с отрицательным временем);
  - снимки стакана (лучшие уровни asks обеих сторон) за 5 с до старта, на
    старте и на 15/30/45/60-й секунде — колонка depth;
  - минимум ask каждой стороны до минуты до конца — исполнилась бы лимитка;
  - исход: ранний (стакан в последние секунды, иначе Chainlink) и официальный.
Серию считаем по исходам прошлых окон (до запуска бота — официальные исходы
последних 12 окон из Gamma API).

Рынок окна ищем по точному слагу {монета}-updown-15m-{начало} — заранее, за
PREFETCH_SEC до начала, — и сразу подписываем его токены на живой стакан:
цена видна с нулевой секунды.
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
DISCOVERY_RETRY_SEC = 5        # рынок окна не нашёлся на старте — снова через 5 с
LATE_RETRY_SEC = 30            # …а после первой минуты (нужен только исход) — через 30 с
PREFETCH_SEC = 90              # рынок следующего окна 15m ищем за 1.5 мин до начала…
PREFETCH_RETRY_SEC = 10        # …и снова каждые 10 с, если его ещё нет
PRE_PATH_SEC = 30              # у окон стратегии путь цены пишем с 30 с до начала
FINE_SEC = 90                  # …и первые 90 с (вход lead60 — с 60-й по 90-ю) — каждые SERIES_FINE_STEP_SEC с
DEPTH_AT = (-5, 0, 15, 30, 45, 60)   # снимки стакана: секунды от начала окна…
DEPTH_TOL_SEC = 5              # …снимок — в первые 5 с после отметки, иначе пропуск
DEPTH_LEVELS = 8               # уровней asks каждой стороны в снимке
TRIM_EVERY_SEC = 3600          # раз в час стираем путь и снимки старых окон
LIMITS = (0.50, 0.49, 0.48, 0.45, 0.42, 0.40)
KS = (3, 4)
REQUEST_PAUSE_SEC = 0.2


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
    depth: list = field(default_factory=list)          # [[t, asks UP, asks DOWN], …]
    depth_done: set = field(default_factory=set)       # отметки DEPTH_AT, по которым снимок уже есть
    early_set: bool = False                            # ранний исход уже записан (по стакану)


_cur: dict[tuple[str, str], Win] = {}
_next: dict[tuple[str, str], Win] = {}      # рынок следующего окна 15m, найденный заранее
_prefetch_at: dict[tuple[str, str, int], float] = {}   # когда снова искать рынок следующего окна
_prefetching: set = set()
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
_stats = {"windows": 0, "priced": 0, "early": 0, "official": 0, "prefetched": 0, "discovery_errors": 0,
          "last_error": ""}


def reset() -> None:
    _qual.clear()
    _traded.clear()
    _vars.clear()
    _trade_note["stop_day"] = None
    _cur.clear()
    _next.clear()
    _prefetch_at.clear()
    _prefetching.clear()
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


def fine_step() -> int:
    """Шаг пути до старта и в первые FINE_SEC секунд у окон стратегии (0 — как остальное окно)."""
    return max(0, int(settings.SERIES_FINE_STEP_SEC))


def _fine(w: Win) -> bool:
    """Окно стратегии: подробный путь в начале и снимки стакана."""
    return bool(fine_step()) and w.interval in trade_intervals()


def _step_for(w: Win, t: float) -> float:
    return fine_step() if t < FINE_SEC and _fine(w) else path_step()


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

def _activate(w: Win) -> Win:
    """Окно стало текущим: строка в базе, монету считаем поддерживаемой."""
    storage.series_upsert(w.asset, w.interval, w.start, slug=w.slug)
    _supported.add((w.asset, w.interval))
    _fails[(w.asset, w.interval)] = 0
    _stats["windows"] += 1
    return w


def _prune(d: dict, now: float) -> None:
    """Не копить ключи (монета, интервал, старт) по старым окнам."""
    if len(d) > 2000:
        for old in [x for x in d if x[2] < now - 86400]:
            d.pop(old, None)


async def _fetch_win(asset: str, interval: str, start: int, now: float) -> Win | None:
    """Рынок окна по точному слагу + подписка его токенов на стакан. None — рынка (пока) нет."""
    try:
        toks = await market_discovery.get_window_tokens(asset, interval, start)
    except Exception as exc:  # noqa: BLE001 — Gamma недоступна или кривой ответ: попробуем позже
        _stats["discovery_errors"] += 1
        _stats["last_error"] = f"{asset} {interval}: {exc}"[:200]
        return None
    if not toks:
        return None
    up, down = toks
    own = bool(settings.USE_LIVE_BOOK_STREAM)
    if own and start > now:
        # окно ещё не началось — стакана может не быть до старта: сторож сокета не ждёт его раньше
        book_stream.subscribe([up, down], watch_after_ms=int(start * 1000))
    elif own:
        book_stream.subscribe([up, down])
    return Win(asset, interval, start, start + STEP[interval], slug_for(asset, interval, start), up, down,
               own_tokens=own)


async def _open(asset: str, interval: str, start: int, now: float) -> Win | None:
    """Открыть текущее окно, если заранее его рынок не нашёлся."""
    key = (asset, interval, start)
    if now < _retry_at.get(key, 0):
        return None
    w = await _fetch_win(asset, interval, start, now)
    if w is None:
        late = now - start > FIRST_OBS_MAX_SEC          # цену старта уже не застать — нужен только исход
        _retry_at[key] = now + (LATE_RETRY_SEC if late else DISCOVERY_RETRY_SEC)
        _prune(_retry_at, now)
        return None
    return _activate(w)


def _release(w: Win) -> None:
    """Отписать токены окна, которое так и не стало текущим (кроме нужных текущему окну)."""
    if not w.own_tokens:
        return
    cur = _cur.get((w.asset, w.interval))
    busy = {cur.up, cur.down} if cur is not None else set()
    toks = [t for t in (w.up, w.down) if t not in busy]
    if toks:
        book_stream.unsubscribe(toks)


def _maybe_prefetch(asset: str, interval: str, start: int, now: float) -> None:
    key = (asset, interval, start)
    if key in _prefetching or now < _prefetch_at.get(key, 0):
        return
    _prefetching.add(key)
    _prefetch_at[key] = now + PREFETCH_RETRY_SEC
    _prune(_prefetch_at, now)
    _spawn(_prefetch(asset, interval, start, now))


async def _prefetch(asset: str, interval: str, start: int, now: float) -> None:
    """Найти рынок окна start заранее и подписать его стакан (в фоне, не держит тик)."""
    try:
        w = await _fetch_win(asset, interval, start, now)
        if w is None:
            return
        key = (asset, interval)
        old, cur = _next.get(key), _cur.get(key)
        if (old is not None and old.start == start) or (cur is not None and cur.start == start):
            return                           # уже есть (подписка у токенов та же — не трогаем)
        if old is not None:
            _release(old)
        _next[key] = w
        _stats["prefetched"] += 1
    finally:
        _prefetching.discard((asset, interval, start))


def _flush(w: Win) -> None:
    fields = dict(slug=w.slug, path=json.dumps(w.path, separators=(",", ":")), up_min=w.up_min,
                  up_min_t=w.up_min_t, down_min=w.down_min, down_min_t=w.down_min_t)
    if w.depth:
        fields["depth"] = json.dumps(w.depth, separators=(",", ":"))
    storage.series_upsert(w.asset, w.interval, w.start, **fields)


def _levels(token: str) -> list:
    """Лучшие уровни asks: [[цена, акций], …]."""
    asks = (book_stream.get_book(token) or {}).get("asks") or {}
    return [[p, round(s, 2)] for p, s in sorted(asks.items())[:DEPTH_LEVELS]]


def _snap_depth(w: Win, t: float) -> None:
    """Снимок стакана у ближайшей прошедшей отметки DEPTH_AT (не больше одного за тик)."""
    for c in DEPTH_AT:
        if c in w.depth_done or t < c or t - c > DEPTH_TOL_SEC:
            continue
        up, down = _levels(w.up), _levels(w.down)
        if not up and not down:
            return                           # стакана ещё нет — попробуем на следующем тике
        w.depth.append([round(t, 1), up, down])
        w.depth_done.add(c)
        return


def _sample(w: Win, now: float) -> None:
    t = now - w.start                        # до начала окна (рынок найден заранее) — отрицательное
    ua, da = book_stream.best_ask(w.up), book_stream.best_ask(w.down)
    if not w.first and 0 <= t <= FIRST_OBS_MAX_SEC and ua is not None and da is not None:
        w.first = True
        _stats["priced"] += 1
        storage.series_upsert(w.asset, w.interval, w.start, t_first=round(t, 1), up_ask0=ua, down_ask0=da,
                              up_bid0=book_stream.best_bid(w.up), down_bid0=book_stream.best_bid(w.down))
    # тик раз в ~секунду: допуск полсекунды, чтобы шаг 1 с не превращался в 2 с
    if t - w.last_path_t >= _step_for(w, t) - 0.5 and (ua is not None or da is not None):
        w.path.append([round(t, 1), ua, da])
        w.last_path_t = t
    if _fine(w):
        _snap_depth(w, t)
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


def _set_early(w: Win, outcome: str, **fields) -> None:
    w.early_set = True
    storage.series_upsert(w.asset, w.interval, w.start, early_outcome=outcome, **fields)
    _outcomes[(w.asset, w.interval)].setdefault(w.start, outcome)
    _stats["early"] += 1


def resolve_pending(now: float | None = None) -> int:
    """Ранний исход закончившихся окон. По стакану последних секунд — сразу после
    конца окна (серия перед следующим окном известна с его первой секунды), иначе —
    по Chainlink, когда придёт точка после конца. Точки Chainlink пишем в обоих случаях."""
    now = time.time() if now is None else now
    done = 0
    for w in list(_pending):
        if now < w.end:
            continue
        book = w.book_res if w.book_res_t >= w.end - BOOK_LAST_SEC else None
        if book and not w.early_set:
            _set_early(w, book)
            done += 1
        cl_end = chainlink_feed.price_after(w.asset, w.end)
        if cl_end is None and now < w.end + EARLY_WAIT_SEC:
            continue                          # ждём точку Chainlink после конца окна
        _pending.remove(w)
        cl_start = chainlink_feed.price_at(w.asset, w.start)
        if w.early_set:
            storage.series_upsert(w.asset, w.interval, w.start, cl_start=cl_start, cl_end=cl_end)
        elif cl_start and cl_end:
            _set_early(w, "UP" if cl_end >= cl_start else "DOWN", cl_start=cl_start, cl_end=cl_end)
            done += 1
        else:
            storage.series_upsert(w.asset, w.interval, w.start, cl_start=cl_start, cl_end=cl_end)
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
    """Монеты, окна которых пишем: все (у выключенной тоже — чтобы знать серию, когда включат)."""
    return list(settings.ASSETS)


def trade_assets() -> list[str]:
    """Монеты, по которым ставит стратегия: включённые в 🪙 Монетах."""
    return [a for a in settings.ASSETS if runtime_state.is_asset_enabled(a)]


def coin_live(asset: str) -> bool:
    """Монета помечена 🔴 — по ней идут реальные ставки (если бот в 🔴 LIVE)."""
    return runtime_state.is_asset_live(asset)


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


async def _tick_current(key: tuple[str, str], start: int, now: float) -> None:
    asset, iv = key
    w = _cur.get(key)
    if w is not None and w.start != start:
        _close(w)
        _cur.pop(key, None)
        w = None
    if w is None:
        nw = _next.get(key)
        if nw is not None and nw.start == start:
            w = _activate(_next.pop(key))    # рынок нашли заранее — цена есть с нулевой секунды
        else:
            if nw is not None and nw.start < start:
                _release(_next.pop(key))     # заранее найденное окно так и не стало текущим
            # Окно, найденное позже первой минуты (бот только что запустился), тоже
            # открываем — без цены старта, но с исходом по стакану в конце: иначе
            # серия перед следующим окном неизвестна, и его сигнал после каждого
            # перезапуска терялся бы.
            w = await _open(asset, iv, start, now)
            if w is None:
                return
        _cur[key] = w
        if key not in _seeded:
            _seeded.add(key)
            _spawn(_seed(asset, iv, start))
        if (iv in trade_intervals() and trade_enabled() and runtime_state.is_asset_enabled(asset)
                and coin_live(asset) and live_blocked(now) is None):
            # реальные ставки включены — прогреть рынок в SDK заранее, чтобы ордер ушёл без лишнего запроса
            from src import polymarket_client
            _spawn(polymarket_client.prewarm_market([w.up, w.down]))
    _sample(w, now)


def _tick_next(key: tuple[str, str], nxt: int, now: float) -> None:
    """Окно, которое начнётся в nxt: найти рынок заранее и (у окон стратегии) писать цену до старта."""
    if nxt - now > PREFETCH_SEC:
        return
    nw = _next.get(key)
    if nw is None or nw.start != nxt:
        _maybe_prefetch(key[0], key[1], nxt, now)
        return
    if nxt - now <= PRE_PATH_SEC and _fine(nw):
        _sample(nw, now)


async def tick(now: float | None = None) -> None:
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
            await _tick_current(key, start, now)
            _tick_next(key, start + step, now)
    resolve_pending(now)
    await maybe_trade(now)
    track_variants(now)


# ---------------------------------------------------------- 🎲 стратегия ----
#
# После trade_k()+ одинаковых исходов подряд — ставка на разворот по правилу
# trade_rule(): lead60 — на 60-й секунде окна, если разворот уже ведёт (ask
# LEAD_LO…LEAD_HI), по цене не выше ask + 1¢; cheap — в первые
# SERIES_TRADE_ENTRY_SEC секунд, если ask не выше trade_cap(). Тестовая ставка
# пишется всегда (по стакану: средняя цена по уровням не дороже потолка,
# комиссия тейкера); реальная — если монета 🔴 и бот в 🔴 LIVE. Окна, где
# серия была, а ставки нет (цена не подошла, мало объёма), тоже пишутся — со
# статусом SKIP_*: видно, выиграли бы они или нет.

TRADE_K_PRESETS = (3, 4, 5)
TRADE_CAP_PRESETS = (0.50, 0.51, 0.52)
TRADE_USDC_PRESETS = (3.0, 5.0, 10.0, 20.0)
TRADE_STOP_PRESETS = (0.0, 15.0, 30.0, 60.0)
MIN_TRADE_USDC = 1.0
# Правило входа (и реальных ставок). lead60 — с v9 (17.10): за 16 ч теста v8 вход
# «на 60-й с, если разворот уже ведёт» — 13 из 14, а «дёшево на старте» — 4 из 12.
RULES = {"lead60": "на 60-й с, если разворот уже ведёт", "cheap": "дёшево в первую минуту"}
LEAD_AT_SEC, LEAD_MAX_SEC = 60, 90      # «на 60-й с» — первый тик с 60-й по 90-ю секунду
LEAD_LO, LEAD_HI = 0.52, 0.80           # разворот «ведёт»: ask стороны разворота в этих пределах
LEAD_SLIP = 0.01                        # потолок ордера — ask + 1 цент
MIN_ORDER_SHARES = 5.0                  # меньше 5 акций биржа ордер не берёт
LIVE_RETRY_SEC = 0.5                    # реальный ордер не исполнился — ещё раз через полсекунды
_qual: dict[tuple[str, str, int], dict] = {}        # окно с серией, где ставки ещё нет: мин. ask и т.п.
_traded: set = set()
_trade_note = {"stop_day": None}


def _rs(key: str, default):
    return runtime_state._state.get(key, default)


def trade_enabled() -> bool:
    return bool(_rs("series_trade_enabled", settings.SERIES_TRADE_ENABLED))


def trade_k() -> int:
    return max(1, int(_rs("series_trade_k", settings.SERIES_TRADE_K)))


def trade_cap() -> float:
    return float(_rs("series_trade_cap", settings.SERIES_TRADE_CAP))


def trade_usdc() -> float:
    return float(_rs("series_trade_usdc", settings.SERIES_TRADE_USDC))


def trade_stop() -> float:
    return float(_rs("series_trade_stop", settings.SERIES_TRADE_STOP_USDC))


def trade_rule() -> str:
    r = str(_rs("series_trade_rule", settings.SERIES_TRADE_RULE))
    return r if r in RULES else "lead60"


def lead_cap(ask: float) -> float:
    """Потолок цены ордера по правилу lead60: ask + 1 цент."""
    return round(min(ask, LEAD_HI) + LEAD_SLIP, 2)


def bet_usdc(cap: float) -> float:
    """Сумма ставки: выбранная, но не меньше 5 акций по потолку цены (иначе биржа отклонит)."""
    return max(trade_usdc(), round(MIN_ORDER_SHARES * cap + 0.01, 2))


def trade_intervals() -> list[str]:
    want = [iv.strip().lower() for iv in str(settings.SERIES_TRADE_INTERVALS or "").split(",")]
    return [iv for iv in intervals() if iv in want]


def dry_fill(token: str, cap: float, usdc: float) -> tuple[float | None, float]:
    """(средняя цена, USDC) покупки на usdc по уровням asks не дороже cap."""
    b = book_stream.get_book(token)
    spent = shares = 0.0
    for price, size in sorted((b or {}).get("asks", {}).items()):
        if price > cap + 1e-9 or spent >= usdc - 1e-9:
            break
        take = min(price * size, usdc - spent)
        spent += take
        shares += take / price
    return (round(spent / shares, 4), round(spent, 2)) if shares > 0 else (None, 0.0)


def today_live_pnl(now: float | None = None) -> float:
    now = time.time() if now is None else now
    day = int(now // 86400) * 86400
    return sum(t["pnl_usdc"] or 0.0 for t in storage.series_trades_since(day - 3600)
               if not t["dry_run"] and t["settled_ts"] and t["settled_ts"] >= day)


def live_blocked(now: float | None = None) -> str | None:
    """Почему реальная ставка сейчас нельзя (None — можно)."""
    if not settings.POLY_PRIVATE_KEY:
        return "нет ключа POLY_PRIVATE_KEY"
    if runtime_state.get("dry_run"):
        return "бот в DRY RUN"
    stop = trade_stop()
    if stop > 0 and today_live_pnl(now) <= -stop:
        return f"дневной стоп −${stop:g}"
    return None


async def _live_buy(token: str, cap: float, usdc: float) -> tuple[float, float, str, str] | None:
    """Реальная покупка FAK-ордером по цене не выше cap: (USDC, акций, id ордера, статус) или None."""
    from src import polymarket_client
    tick = book_stream.tick_size(token)
    cap = polymarket_client.round_price_for_buy(cap, tick)
    try:
        resp = await polymarket_client.place_buy_order(token, cap, usdc, tick, order_type="FAK")
    except Exception as exc:  # noqa: BLE001
        if polymarket_client.is_no_fill_error(exc):
            return None
        raise
    oid = polymarket_client.response_field(resp, "order_id") or polymarket_client.response_field(resp, "orderID")
    status = str(polymarket_client.response_field(resp, "status") or "matched")
    amounts = polymarket_client.filled_amounts(resp)
    if amounts is None or status.lower() == "delayed" or (amounts[0] <= 0 and status.lower() == "matched"):
        spent, shares = usdc, usdc / cap
    else:
        spent, shares = amounts
    if spent <= 0:
        return None
    return round(spent, 2), shares, str(oid or ""), status


async def _notify(text: str) -> None:
    try:
        from src import telegram_notify
        await telegram_notify.notify(text)
    except Exception:  # noqa: BLE001
        pass


def _log_skip(key, w: Win, d: str, k: int, side: str, status: str, ask: float | None,
              now: float | None = None) -> None:
    asset, iv, start = key
    storage.series_trade_log(ts=int(time.time() if now is None else now), asset=asset, interval=iv, start_ts=start,
                             slug=w.slug, streak_dir=d, k=k, side=side, token_id=w.down if side == "DOWN" else w.up,
                             ask=ask, dry_run=1, status=status, sec_after_start=round((now or time.time()) - start, 1))


def _mark_traded(key, now: float) -> None:
    _traded.add(key)
    if len(_traded) > 5000:
        for old in [x for x in _traded if x[2] < now - 2 * 86400]:
            _traded.discard(old)


async def _record_and_enter(w: Win, key, d: str, k: int, side: str, token: str, ask: float, price: float,
                            spent: float, cap: float, usdc: float, now: float) -> int:
    """Тестовая ставка (по стакану) и, если монета 🔴, реальная. 1 — тестовая записана."""
    asset, iv, start = key
    fee = round(spent * float(settings.FEE_RATE) * (1 - price), 4)
    tid = storage.series_trade_log(
        ts=int(now), asset=asset, interval=iv, start_ts=start, slug=w.slug, streak_dir=d, k=k, side=side,
        token_id=token, ask=ask, price=price, size_usdc=spent, shares=round(spent / price, 4), fee_usdc=fee,
        sec_after_start=round(now - start, 1), dry_run=1, status="DRY")
    # Реальная ставка — одна на окно: после перезапуска бота в том же окне тестовая
    # строка уже есть, реальную не дублируем
    if coin_live(asset) and not storage.series_trade_exists(asset, iv, start, dry_run=False):
        await _enter_live(w, key, d, k, side, token, ask, now, cap, usdc)
    return 1 if tid else 0


async def maybe_trade(now: float | None = None) -> int:
    """Ставки стратегии по выбранному правилу. Возвращает, сколько тестовых ставок записали."""
    now = time.time() if now is None else now
    if not trade_enabled():
        return 0
    if trade_rule() == "lead60":
        return await _trade_lead(now)
    return await _trade_cheap(now)


async def _trade_lead(now: float) -> int:
    """lead60: на первом тике с 60-й по 90-ю секунду окна — если сторона разворота уже
    ведёт (ask LEAD_LO…LEAD_HI), покупаем её по цене не выше ask + 1 цент; иначе пропуск."""
    done = 0
    for iv in trade_intervals():
        step = STEP[iv]
        start = int(now // step) * step
        if not LEAD_AT_SEC <= now - start <= LEAD_MAX_SEC:
            continue
        for asset in trade_assets():
            key = (asset, iv, start)
            if key in _traded:
                continue
            w = _cur.get((asset, iv))
            if w is None or w.start != start:
                continue
            d, k = streak_before(asset, iv, start)
            if d is None or k < trade_k():
                continue
            side = "DOWN" if d == "UP" else "UP"
            token = w.down if side == "DOWN" else w.up
            ask = book_stream.best_ask(token)
            if ask is None:
                continue                              # стакана нет — ждём до 90-й секунды
            _mark_traded(key, now)
            if not LEAD_LO - 1e-9 <= ask <= LEAD_HI + 1e-9:
                _log_skip(key, w, d, k, side, "SKIP_PRICE", ask, now)   # разворот не ведёт
                continue
            cap = lead_cap(ask)
            usdc = bet_usdc(cap)
            price, spent = dry_fill(token, cap, usdc)
            if price is None or spent < MIN_TRADE_USDC:
                _log_skip(key, w, d, k, side, "SKIP_BOOK", ask, now)
                continue
            done += await _record_and_enter(w, key, d, k, side, token, ask, price, spent, cap, usdc, now)
    return done


async def _trade_cheap(now: float) -> int:
    """cheap (до v9): первая цена не выше trade_cap() в первые SERIES_TRADE_ENTRY_SEC секунд окна."""
    done = 0
    entry_sec = max(5, int(settings.SERIES_TRADE_ENTRY_SEC))
    for key, q in list(_qual.items()):            # окно ушло без ставки — записать пропуск
        asset, iv, start = key
        if now - start > entry_sec and key not in _traded:
            _qual.pop(key, None)
            w = q["win"]
            status = "SKIP_PRICE" if q.get("min_ask") is not None else "SKIP_BOOK"
            _log_skip(key, w, q["dir"], q["k"], q["side"], status, q.get("min_ask"), now)
    for iv in trade_intervals():
        step = STEP[iv]
        start = int(now // step) * step
        if now - start > entry_sec:
            continue
        for asset in trade_assets():
            key = (asset, iv, start)
            if key in _traded:
                continue
            w = _cur.get((asset, iv))
            if w is None or w.start != start:
                continue
            d, k = streak_before(asset, iv, start)
            if d is None or k < trade_k():
                continue
            side = "DOWN" if d == "UP" else "UP"
            token = w.down if side == "DOWN" else w.up
            ask = book_stream.best_ask(token)
            q = _qual.setdefault(key, {"win": w, "dir": d, "k": k, "side": side, "min_ask": None})
            if ask is None:
                continue
            q["min_ask"] = ask if q["min_ask"] is None else min(q["min_ask"], ask)
            cap = trade_cap()
            if ask > cap + 1e-9:
                continue                          # дорого — ждём до конца окна входа
            usdc = bet_usdc(cap)
            price, spent = dry_fill(token, cap, usdc)
            if price is None or spent < MIN_TRADE_USDC:
                continue
            _mark_traded(key, now)
            _qual.pop(key, None)
            done += await _record_and_enter(w, key, d, k, side, token, ask, price, spent, cap, usdc, now)
    return done


def _retry_cap(token: str) -> float | None:
    """Потолок для повтора реального ордера по свежему стакану (None — цена ушла из правила)."""
    ask = book_stream.best_ask(token)
    if ask is None:
        return None
    if trade_rule() == "lead60":
        return lead_cap(ask) if LEAD_LO - 1e-9 <= ask <= LEAD_HI + 1e-9 else None
    return trade_cap() if ask <= trade_cap() + 1e-9 else None


async def _enter_live(w: Win, key, d: str, k: int, side: str, token: str, ask: float, now: float,
                      cap: float | None = None, usdc: float | None = None) -> None:
    asset, iv, start = key
    why = live_blocked(now)
    if why:
        day = int(now // 86400)
        if why.startswith("дневной стоп") and _trade_note["stop_day"] != day:
            _trade_note["stop_day"] = day
            await _notify(f"🎲 Серии: реальные ставки остановлены до конца дня — {why}. Тестовые идут дальше.")
        return
    cap = trade_cap() if cap is None else cap
    usdc = bet_usdc(cap) if usdc is None else usdc
    res = None
    for attempt in range(2):                      # не исполнился — ещё раз по свежему стакану
        try:
            res = await _live_buy(token, cap, usdc)
        except Exception as exc:  # noqa: BLE001
            await _notify(f"❌ 🎲 Серии {asset.upper()} {iv}: ошибка ордера — {exc}"[:400])
            return
        if res is not None or attempt:
            break
        await asyncio.sleep(LIVE_RETRY_SEC)
        new_cap = _retry_cap(token)
        if new_cap is None:
            break
        cap, usdc = new_cap, bet_usdc(new_cap)
    if res is None:
        await _notify(f"⚠️ 🎲 Серии {asset.upper()} {iv}: ордер на {side} по ≤{cap:.2f} не исполнился.")
        return
    spent, shares, oid, status = res
    price = round(spent / shares, 4) if shares > 0 else cap
    fee = round(spent * float(settings.FEE_RATE) * (1 - price), 4)
    storage.series_trade_log(ts=int(now), asset=asset, interval=iv, start_ts=start, slug=w.slug, streak_dir=d, k=k,
                             side=side, token_id=token, ask=ask, price=price, size_usdc=spent,
                             shares=round(shares, 4), fee_usdc=fee, sec_after_start=round(now - start, 1),
                             dry_run=0, status=status, order_id=oid)
    await _notify(f"✅ 🎲 Серии {asset.upper()} {iv}: {k}×{d} подряд → ставка {side} ${spent:.2f} по {price:.3f}")


async def _official(t: dict, now: float, cache: dict) -> str | None:
    """Официальный исход окна строки t: из series_windows, иначе — спросить Gamma
    (не раньше SETTLE_LAG_SEC после конца; одно окно — один запрос за проход)."""
    key = (t["asset"], t["interval"], t["start_ts"])
    if key in cache:
        return cache[key]
    o = storage.series_outcome(*key)
    if o is None and now - (t["start_ts"] + STEP.get(t["interval"], 900)) >= SETTLE_LAG_SEC:
        try:
            o = await market_discovery.get_resolution(t["slug"])
        except Exception:  # noqa: BLE001
            o = None
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    cache[key] = o
    return o


async def settle_trades(now: float | None = None) -> int:
    """Итог ставок стратегии по исходу окна (официальный, иначе — спросить Gamma)."""
    now = time.time() if now is None else now
    n = 0
    cache: dict = {}
    for t in storage.series_trades_open(int(now - 60)):
        if t["start_ts"] < now - SETTLE_MAX_AGE_SEC:
            continue                                  # старше 2 суток — не спрашиваем
        o = await _official(t, now, cache)
        if o not in ("UP", "DOWN"):
            continue
        win = o == t["side"]
        if t["status"] and str(t["status"]).startswith("SKIP"):
            pnl = 0.0
        else:
            # И тестовые, и реальные — с комиссией тейкера (у реальных — оценка по
            # FEE_RATE: суммы ордера могут её и не включать; точный итог — в кошельке)
            pnl = (t["shares"] if win else 0.0) - t["size_usdc"] - (t["fee_usdc"] or 0.0)
        if storage.series_trade_settle(t["id"], o, round(pnl, 4)):
            n += 1
            if not t["dry_run"]:
                await _notify(f"🎲 {'🟢' if win else '🔴'} Серии {t['asset'].upper()} {t['interval']}: исход {o}, "
                              f"ставка {t['side']} → {pnl:+.2f} USDC")
    return n


def trade_stats(since_ts: int = 0, live: bool = False) -> dict:
    rows = [t for t in storage.series_trades_since(since_ts) if bool(t["dry_run"]) != live]
    bets = [t for t in rows if not str(t["status"] or "").startswith("SKIP")]
    settled = [t for t in bets if t["outcome"]]
    skips = [t for t in rows if str(t["status"] or "").startswith("SKIP")]
    wins = sum(1 for t in settled if t["outcome"] == t["side"])
    return {
        "bets": len(bets), "settled": len(settled), "wins": wins,
        "pnl": sum(t["pnl_usdc"] or 0.0 for t in settled),
        "staked": sum(t["size_usdc"] or 0.0 for t in settled),
        "avg_price": (sum(t["price"] for t in bets if t["price"]) / len(bets)) if bets else None,
        "skip_price": sum(1 for t in skips if t["status"] == "SKIP_PRICE"),
        "skip_book": sum(1 for t in skips if t["status"] == "SKIP_BOOK"),
        "skip_won": sum(1 for t in skips if t["outcome"] and t["outcome"] == t["side"]),
        "skip_settled": sum(1 for t in skips if t["outcome"]),
    }


def _stats_line(name: str, st: dict) -> str:
    if not st["bets"] and not st["skip_price"] and not st["skip_book"]:
        return f"{name}: ставок пока нет"
    s = f"{name}: ставок {st['bets']}"
    if st["avg_price"]:
        s += f" (средняя цена {st['avg_price']:.3f})"
    if st["settled"]:
        s += (f", с итогом {st['settled']}: выигрышей {st['wins'] / st['settled'] * 100:.1f}%, "
              f"итог {st['pnl']:+.2f} USDC ({st['pnl'] / st['staked'] * 100:+.1f}% ставки)" if st["staked"] else "")
    if st["skip_price"] or st["skip_book"]:
        why = (f"разворот не ведёт (цена вне {LEAD_LO:.2f}–{LEAD_HI:.2f})" if trade_rule() == "lead60"
               else f"дороже {trade_cap():.2f}")
        s += f"; пропущено: {why} — {st['skip_price']}, нет стакана — {st['skip_book']}"
        if st["skip_settled"]:
            s += f" (из пропущенных выиграли бы {st['skip_won']}/{st['skip_settled']})"
    return s


def rule_text() -> str:
    """Правило входа словами (для меню и уведомлений)."""
    ivs = ", ".join(trade_intervals()) or "—"
    if trade_rule() == "lead60":
        return (f"на {LEAD_AT_SEC}-й секунде окна {ivs}, если разворот уже ведёт (цена {LEAD_LO:.2f}–{LEAD_HI:.2f}), "
                "по цене не выше ask + 1¢")
    return f"в первые {int(settings.SERIES_TRADE_ENTRY_SEC)} с окна {ivs} по цене не выше {trade_cap():.2f}"


def live_coins() -> list[str]:
    """Включённые монеты с 🔴 — по ним идут реальные ставки, когда бот в 🔴 LIVE."""
    return [a for a in trade_assets() if coin_live(a)]


def mode_text() -> str:
    """Режим реальных ставок словами."""
    coins = live_coins()
    if runtime_state.get("dry_run"):
        return "🧪 DRY RUN — реальных ордеров нет" + (
            f" (🔴 помечены {', '.join(a.upper() for a in coins)} — пойдут после «🔴 Включить LIVE»)" if coins else "")
    if not coins:
        return "🔴 LIVE, но ни одна включённая монета не 🔴 — реальных ставок нет"
    return f"🔴 LIVE — реальные ставки по {', '.join(a.upper() for a in coins)}"


def strategy_lines() -> list[str]:
    lines = [f"🎲 Стратегия: после {trade_k()}+ одинаковых исходов подряд — ставка на разворот {rule_text()}, "
             f"${trade_usdc():g} за ставку (не меньше 5 акций). "
             + ("Сейчас: ✅ включена" if trade_enabled() else "Сейчас: ⏸ выключена"),
             "  Режим: " + mode_text()]
    day = int(time.time() // 86400) * 86400
    lines.append("  " + _stats_line("Тест", trade_stats(0, live=False)))
    lines.append("  " + _stats_line("Тест сегодня (UTC)", trade_stats(day, live=False)))
    live = trade_stats(0, live=True)
    if live["bets"] or live_coins():
        lines.append("  " + _stats_line("Реальные", live))
        stop = trade_stop()
        lines.append(f"  Сегодня реальные: {today_live_pnl():+.2f} USDC"
                     + (f" (дневной стоп −${stop:g})" if stop > 0 else " (без дневного стопа)"))
        why = live_blocked() if live_coins() else None
        if why and why != "бот в DRY RUN":
            lines.append(f"  ⚠️ Реальные ставки сейчас не уходят: {why}")
    return lines


def _coin_stats(rows: list[dict]) -> str:
    bets = [t for t in rows if not str(t["status"] or "").startswith("SKIP")]
    settled = [t for t in bets if t["outcome"]]
    if not bets:
        return "—"
    wins = sum(1 for t in settled if t["outcome"] == t["side"])
    pnl = sum(t["pnl_usdc"] or 0.0 for t in settled)
    return f"{len(bets)} ставок" + (f", выигр. {wins}/{len(settled)}, {pnl:+.2f} $" if settled else "")


def coin_lines() -> list[str]:
    """По монете: вкл/выкл, 🧪/🔴 и итог тестовых и реальных ставок (за всё время в базе)."""
    rows = storage.series_trades_since(0)
    out = []
    for a in settings.ASSETS:
        on = runtime_state.is_asset_enabled(a)
        mine = [t for t in rows if t["asset"] == a]
        test = _coin_stats([t for t in mine if t["dry_run"]])
        real = _coin_stats([t for t in mine if not t["dry_run"]])
        out.append(f"{a.upper()} {'✅' if on else '⏸'} {'🔴' if coin_live(a) else '🧪'} — тест: {test}"
                   + (f"; реальные: {real}" if real != "—" or coin_live(a) else ""))
    return out


# --------------------------------------------------- 🎲 варианты входа ----
#
# По тем же сигналам (trade_k()+ одинаковых исходов перед окном) — виртуальные
# ставки по другим правилам входа, чтобы через несколько дней сравнить правила
# на одних и тех же окнах. Основное правило — сама стратегия (тестовые ставки).
# 16.10 пропущенные основным правилом окна (весь первую минуту дороже 0.51)
# выиграли 9 из 9, а вошли только туда, где цена шла против разворота, — 5 из 9.

VARIANTS = (
    ("min51", "≤0.51 в первую минуту (правило до v9)"),
    ("start51", "сразу на старте, если ≤0.51"),
    ("start60", "сразу на старте, если ≤0.60"),
    ("lead60", "на 60-й с, если разворот уже ведёт (0.52–0.80)"),
)
VAR_MIN_CAP, VAR_MIN_SEC = 0.51, 60   # min51 — первая цена ≤0.51 за первую минуту (как основное правило до v9)
VAR_START_CAP = {"start51": 0.51, "start60": 0.60}
VAR_START_MAX_SEC = 10         # «на старте» — первый взгляд после того, как серия известна, не позже 10-й с
VAR_AT_SEC, VAR_AT_MAX_SEC = LEAD_AT_SEC, LEAD_MAX_SEC   # lead60 — те же пороги, что у правила lead60
VAR_LEAD_LO, VAR_LEAD_HI = LEAD_LO, LEAD_HI
_vars: dict[tuple[str, str, int], dict] = {}        # окно с сигналом -> состояние вариантов


def _log_variant(key, st: dict, variant: str, status: str, now: float, ask: float | None = None,
                 price: float | None = None, spent: float = 0.0) -> bool:
    asset, iv, start = key
    f = dict(ts=int(now), asset=asset, interval=iv, start_ts=start, slug=st["win"].slug, variant=variant,
             streak_dir=st["dir"], k=st["k"], side=st["side"], ask=ask, status=status,
             sec_after_start=round(now - start, 1))
    if price is not None:
        f.update(price=price, size_usdc=spent, shares=round(spent / price, 4),
                 fee_usdc=round(spent * float(settings.FEE_RATE) * (1 - price), 4))
    st["done"].add(variant)
    return storage.series_variant_log(**f) is not None


def _variant_buy(key, st: dict, variant: str, cap: float, ask: float, now: float) -> bool:
    """Виртуальная покупка по стакану (средняя цена по уровням не дороже cap)."""
    price, spent = dry_fill(st["token"], cap, trade_usdc())
    if price is None or spent < MIN_TRADE_USDC:
        _log_variant(key, st, variant, "SKIP_BOOK", now, ask=ask)
        return False
    return _log_variant(key, st, variant, "BET", now, ask=ask, price=price, spent=spent)


def track_variants(now: float | None = None) -> int:
    """Решения вариантов по окнам с сигналом. Возвращает, сколько виртуальных ставок записали."""
    now = time.time() if now is None else now
    if not trade_enabled():
        return 0
    n = 0
    for iv in trade_intervals():
        step = STEP[iv]
        start = int(now // step) * step
        t = now - start
        for asset in trade_assets():
            key = (asset, iv, start)
            st = _vars.get(key)
            if st is None:
                w = _cur.get((asset, iv))
                if t > VAR_AT_MAX_SEC or w is None or w.start != start:
                    continue
                d, k = streak_before(asset, iv, start)
                if d is None or k < trade_k():
                    continue
                side = "DOWN" if d == "UP" else "UP"
                st = _vars[key] = {"win": w, "dir": d, "k": k, "side": side, "t0": t, "min": None,
                                   "token": w.down if side == "DOWN" else w.up, "done": set()}
            if len(st["done"]) == len(VARIANTS):
                continue
            ask = book_stream.best_ask(st["token"])
            if "min51" not in st["done"]:
                if t > VAR_MIN_SEC:
                    status = ("SKIP_LATE" if st["t0"] > VAR_MIN_SEC
                              else "SKIP_PRICE" if st["min"] is not None else "SKIP_BOOK")
                    _log_variant(key, st, "min51", status, now, ask=st["min"])
                elif ask is not None:
                    st["min"] = ask if st["min"] is None else min(st["min"], ask)
                    if ask <= VAR_MIN_CAP + 1e-9:
                        price, spent = dry_fill(st["token"], VAR_MIN_CAP, trade_usdc())
                        if price is not None and spent >= MIN_TRADE_USDC:   # объёма нет — ждём, как основное
                            n += _log_variant(key, st, "min51", "BET", now, ask=ask, price=price, spent=spent)
            for v, cap in VAR_START_CAP.items():
                if v in st["done"]:
                    continue
                if t > VAR_START_MAX_SEC:
                    _log_variant(key, st, v, "SKIP_LATE", now, ask=ask)
                elif ask is None:
                    continue                          # стакана ещё нет — ждём до 10-й секунды
                elif ask > cap + 1e-9:
                    _log_variant(key, st, v, "SKIP_PRICE", now, ask=ask)
                else:
                    n += _variant_buy(key, st, v, cap, ask, now)
            if "lead60" not in st["done"] and t >= VAR_AT_SEC:
                if t > VAR_AT_MAX_SEC:
                    _log_variant(key, st, "lead60", "SKIP_LATE", now, ask=ask)
                elif ask is None:
                    pass
                elif VAR_LEAD_LO - 1e-9 <= ask <= VAR_LEAD_HI + 1e-9:
                    n += _variant_buy(key, st, "lead60", VAR_LEAD_HI, ask, now)
                else:
                    _log_variant(key, st, "lead60", "SKIP_PRICE", now, ask=ask)
    # окна, ушедшие без решения (бот стоял, нет стакана) — записать пропуск и забыть
    for key, st in list(_vars.items()):
        if now - key[2] > VAR_AT_MAX_SEC:
            for v, _label in VARIANTS:
                if v not in st["done"]:
                    _log_variant(key, st, v, "SKIP_LATE", now)
            _vars.pop(key, None)
    return n


async def settle_variants(now: float | None = None) -> int:
    """Итог виртуальных ставок вариантов (как у тестовых: с комиссией тейкера)."""
    now = time.time() if now is None else now
    n = 0
    cache: dict = {}
    for v in storage.series_variants_open(int(now - 60)):
        if v["start_ts"] < now - SETTLE_MAX_AGE_SEC:
            continue
        o = await _official(v, now, cache)
        if o not in ("UP", "DOWN"):
            continue
        win = o == v["side"]
        pnl = ((v["shares"] if win else 0.0) - v["size_usdc"] - (v["fee_usdc"] or 0.0)
               if v["status"] == "BET" else 0.0)
        if storage.series_variant_settle(v["id"], o, round(pnl, 4)):
            n += 1
    return n


def variant_stats(since_ts: int = 0) -> dict[str, dict]:
    out = {v: {"bets": 0, "settled": 0, "wins": 0, "pnl": 0.0, "staked": 0.0, "price_sum": 0.0,
               "skips": 0, "skip_won": 0, "skip_settled": 0} for v, _label in VARIANTS}
    for r in storage.series_variants_since(since_ts):
        s = out.get(r["variant"])
        if s is None:
            continue
        if r["status"] == "BET":
            s["bets"] += 1
            s["price_sum"] += r["price"] or 0.0
            if r["outcome"]:
                s["settled"] += 1
                s["wins"] += r["outcome"] == r["side"]
                s["pnl"] += r["pnl_usdc"] or 0.0
                s["staked"] += r["size_usdc"] or 0.0
        else:
            s["skips"] += 1
            if r["outcome"]:
                s["skip_settled"] += 1
                s["skip_won"] += r["outcome"] == r["side"]
    return out


def _cmp_line(label: str, bets: int, settled: int, wins: int, pnl: float, staked: float, avg: float | None,
              skips: int, skip_won: int, skip_settled: int) -> str:
    s = f"{label}: ставок {bets}"
    if avg:
        s += f" (ср. цена {avg:.3f})"
    if settled and staked:
        s += f", выигр. {wins}/{settled} = {wins / settled * 100:.0f}%, {pnl:+.2f} $ ({pnl / staked * 100:+.1f}%)"
    if skips:
        s += f"; без ставки {skips}" + (f" (выиграли бы {skip_won}/{skip_settled})" if skip_settled else "")
    return s


def variant_lines() -> list[str]:
    """Сравнение правил входа на одних сигналах — с первой записи вариантов."""
    since = storage.series_variants_first_ts()
    if since is None:
        return ["Сравнение входов на тех же сигналах: пока пусто — появится с первым сигналом."]
    m = trade_stats(since, live=False)
    main = (f"основной — на {LEAD_AT_SEC}-й с, если разворот ведёт" if trade_rule() == "lead60"
            else f"основной — ≤{trade_cap():.2f} в первые {int(settings.SERIES_TRADE_ENTRY_SEC)} с")
    lines = [f"Сравнение входов на тех же сигналах (тест, с {time.strftime('%d.%m %H:%M', time.gmtime(since))} UTC):",
             "  · " + _cmp_line(main, m["bets"], m["settled"], m["wins"], m["pnl"], m["staked"], m["avg_price"],
                                m["skip_price"] + m["skip_book"], m["skip_won"], m["skip_settled"])]
    st = variant_stats(since)
    for v, label in VARIANTS:
        s = st[v]
        lines.append("  · " + _cmp_line(label, s["bets"], s["settled"], s["wins"], s["pnl"], s["staked"],
                                        s["price_sum"] / s["bets"] if s["bets"] else None,
                                        s["skips"], s["skip_won"], s["skip_settled"]))
    return lines


def trim_paths(now: float | None = None) -> int:
    """Стереть путь и снимки стакана у окон старше SERIES_PATH_KEEP_DAYS дней."""
    days = int(settings.SERIES_PATH_KEEP_DAYS)
    if days <= 0:
        return 0
    now = time.time() if now is None else now
    return storage.series_trim(int(now - days * 86400))


async def _sleep_to_next_second() -> None:
    """Тик — в начале каждой секунды: окно стартует на ровной секунде, и первый
    взгляд на рынок приходится на её первые ~0.05 с, а не на случайный момент."""
    await asyncio.sleep(1.0 - (time.time() % 1.0) + 0.05)


async def run_forever() -> None:
    last_settle = last_trim = 0.0
    await asyncio.sleep(3)
    while True:
        try:
            if (enabled() or trade_enabled()) and settings.USE_LIVE_BOOK_STREAM:
                await tick()
                if time.monotonic() - last_settle >= SETTLE_EVERY_SEC:
                    last_settle = time.monotonic()
                    await settle()
                    await settle_trades()
                    await settle_variants()
                if time.monotonic() - last_trim >= TRIM_EVERY_SEC:
                    last_trim = time.monotonic()
                    trim_paths()
            else:
                for w in list(_cur.values()):
                    _close(w)
                _cur.clear()
                for w in list(_next.values()):
                    _release(w)
                _next.clear()
        except Exception as exc:  # noqa: BLE001 — тест не должен мешать боту
            log.warning("series_bot: %s", exc)
        await _sleep_to_next_second()


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
        tf = sorted(r["t_first"] for r in priced if r["t_first"] is not None)
        seen = (f"; рынок видим через {tf[len(tf) // 2]:.1f} с после начала (медиана), в первые 2 с — "
                f"{sum(1 for x in tf if x <= 2)}/{len(tf)}") if tf else ""
        lines.append(f"  сумма ask сторон на старте в среднем {avg_ask:.3f}{seen}")
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


def recorder_line() -> str:
    every = "каждую секунду" if fine_step() == 1 else f"каждые {fine_step()} с"
    fine = (f" С {PRE_PATH_SEC} с до старта и первые {FINE_SEC} с — цена {every} и снимки стакана."
            if fine_step() and trade_intervals() else "")
    return ("🔁 Запись окон " + ("идёт" if enabled() or trade_enabled() else "выключена")
            + f" — по всем монетам: рынок находим за {PREFETCH_SEC / 60:g} мин до начала, цены сторон каждые "
            f"{path_step()} с, исход.{fine}")


def stats_text() -> str:
    """📊 Статистика: стратегия, сравнение входов, записанные окна, текущие серии."""
    lines = ["📊 Статистика", ""] + strategy_lines() + [""] + variant_lines() + ["", recorder_line(), ""]
    lines += summary_lines()
    cur = current_lines()
    if cur:
        lines += [""] + cur
    lines += ["", "База идеи: после 4+ подряд разворот 15m — 55% по году свечей Binance и 54.5% по исходам "
              "Polymarket за 60 дней. Вход lead60 по цене ~0.64 окупается от ~66% выигрышей."]
    if _stats["last_error"] and _stats["discovery_errors"]:
        lines.append(f"Поиск рынков: ошибок {_stats['discovery_errors']}, последняя: {_stats['last_error']}")
    return "\n".join(lines)


def report_caption(from_label: str, to_label: str, since_ts: int) -> str:
    return "\n".join([f"🔁 Окна {from_label} → {to_label} (UTC)"] + summary_lines(since_ts))
