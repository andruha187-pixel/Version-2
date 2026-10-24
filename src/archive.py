"""
📚 Архив закрытых температурных рынков.

1. Ищем закрытые события в Gamma окнами по 2 дня — от сегодня назад на
   ARCHIVE_DAYS (кнопками 60/120/240/365). Последние 4 дня просматриваем каждый
   раз (там закрываются новые события), более старые — один раз.
2. По каждому событию — корзины, исходы и история цены YES-токена каждой корзины
   (CLOB prices-history, шаг PATH_FIDELITY_MIN минут) от выставления рынка до
   исхода. Сначала новые события — пороги можно считать, не дожидаясь всего архива.
3. Скачали новое — пересчитываем 📊 пороги.

Синхронизация — раз в ARCHIVE_REFRESH_HOURS часов и по кнопке.
"""
from __future__ import annotations
import asyncio
import logging
import time

from config import settings
from src import clob, gamma, http_api, runtime_state, storage, weather_parse

log = logging.getLogger("archive")

WINDOW_SEC = 2 * 86400
RECENT_SEC = 4 * 86400
OLDEST_KEY = "archive_oldest_ts"           # до какого момента назад архив уже просмотрен
MAX_ERRORS_PER_EVENT = 3
CHUNK_EVENTS = 1000                         # после каждой 1000 скачанных событий — пересчёт порогов
# Версия разбора событий: поменялась (например, пояса городов США по штату) — один раз заново
# просматриваем все окна, чтобы обновить пояс, станцию и границы дня у уже скачанных событий
PARSE_VERSION = 2
PARSE_VERSION_KEY = "archive_parse_version"

_progress = {"phase": "idle", "found": 0, "new": 0, "paths_total": 0, "paths_done": 0, "events_done": 0,
             "errors": 0, "started": 0, "finished": 0, "last_error": "", "window": "", "paths_t0": 0.0}
_wake = None                                # asyncio.Event: «синхронизировать сейчас»
_running = {"on": False}
_error_counts: dict[int, int] = {}


def progress() -> dict:
    return dict(_progress)


def is_running() -> bool:
    return _running["on"]


def _windows(now: float, days: int) -> list[tuple[float, float]]:
    """Окна поиска: свежие 4 дня всегда, дальше — только ещё не просмотренные (новые к старым)."""
    lo = now - days * 86400
    raw = storage.get_setting(OLDEST_KEY)
    oldest = float(raw) if raw else now
    out, t1 = [], now + 86400              # +1 день: конец события может стоять на завтра
    while t1 > lo:
        t0 = max(lo, t1 - WINDOW_SEC)
        if t1 >= now - RECENT_SEC or t0 < oldest:
            out.append((t0, t1))
        t1 = t0
    return out


async def discover(now: float | None = None, days: int | None = None) -> int:
    """Найти закрытые события и записать их корзины и исходы. Возвращает, сколько новых."""
    now = time.time() if now is None else now
    days = int(days or runtime_state.get("archive_days"))
    new = 0
    oldest_done = None
    for t0, t1 in _windows(now, days):
        _progress["window"] = time.strftime("%d.%m", time.gmtime(t0)) + "–" + time.strftime("%d.%m", time.gmtime(t1))
        evs = await gamma.find_events(closed=True, t0=t0, t1=t1)
        for raw in evs:
            ev = weather_parse.parse_event(raw)
            if ev is None or not ev["closed"]:
                continue
            _progress["found"] += 1
            if storage.event_upsert(ev):
                new += 1
                _progress["new"] += 1
        oldest_done = t0 if oldest_done is None else min(oldest_done, t0)
        if t0 < now - RECENT_SEC:
            raw_oldest = storage.get_setting(OLDEST_KEY)
            if raw_oldest is None or t0 < float(raw_oldest):
                storage.set_setting(OLDEST_KEY, int(t0))
    return new


def _path_window(ev: dict, now: float) -> tuple[int, int]:
    day = ev["day_start_ts"]
    start = max(ev.get("listed_ts") or day - 7 * 86400, day - 10 * 86400) - 3600
    if ev.get("closed_ts"):
        end = ev["closed_ts"] + 3600                     # после исхода торгов нет
    else:
        end = max(ev.get("end_ts") or 0, day + 2 * 86400) + 3600
    end = min(end, int(now), start + 20 * 86400)
    return int(start), int(max(end, start + 3600))


def _todo() -> list[dict]:
    """Очередь на историю цены: сначала события с объёмом от порога расчёта (они и нужны для порогов),
    потом мелкие; внутри — от новых к старым."""
    vmin = float(runtime_state.get("min_event_volume"))
    todo = [e for e in storage.events("closed = 1 AND status IN ('new', 'error')", order="date DESC, id DESC")
            if _error_counts.get(e["id"], 0) < MAX_ERRORS_PER_EVENT]
    todo.sort(key=lambda e: 0 if (e["volume"] or 0) >= vmin else 1)        # сортировка устойчивая
    return todo


def pending_events() -> int:
    """Сколько закрытых событий ещё без истории цены."""
    return len(_todo())


def eta_sec() -> float | None:
    """Сколько ещё качать историю, секунд (по скорости этого прохода)."""
    p = _progress
    if p["phase"] != "paths" or p["paths_done"] < 50 or not p["paths_t0"]:
        return None
    rate = p["paths_done"] / max(1.0, time.monotonic() - p["paths_t0"])
    return max(0.0, (p["paths_total"] - p["paths_done"]) / rate) if rate > 0 else None


async def fetch_paths(now: float | None = None, limit: int | None = None) -> tuple[int, int]:
    """Скачать историю цены корзин закрытых событий без неё (крупные и новые сначала).
    Возвращает (сколько событий скачали, сколько было в очереди этого прохода)."""
    now = time.time() if now is None else now
    todo = _todo()
    if limit:
        todo = todo[:limit]
    done = 0
    for ev in todo:
        start, end = _path_window(ev, now)
        bks = storage.buckets(ev["id"])
        # корзины события — параллельно (темп всё равно держит http_api: не чаще HTTP_RPS в секунду)
        res = await asyncio.gather(*(clob.price_history(b["yes_token"], start, end) for b in bks),
                                   return_exceptions=True)
        bad = [r for r in res if isinstance(r, BaseException)]
        if bad:
            exc = bad[0]
            if not isinstance(exc, http_api.ApiError):
                raise exc
            _error_counts[ev["id"]] = _error_counts.get(ev["id"], 0) + 1
            storage.event_set_status(ev["id"], "error", str(exc)[:200])
            _progress["errors"] += 1
            _progress["last_error"] = f"{ev['city']} {ev['date']}: {exc}"[:200]
            continue
        for b, (fid, pts) in zip(bks, res):
            storage.bucket_set_path(ev["id"], b["idx"], fid, pts)
            _progress["paths_done"] += 1
        storage.event_set_status(ev["id"], "done")
        done += 1
        _progress["events_done"] += 1
    return done, len(todo)


async def sync(now: float | None = None) -> dict:
    """Полный проход: найти события и скачать историю. Возвращает итог."""
    if _running["on"]:
        return {"busy": True}
    _running["on"] = True
    for k in ("found", "new", "paths_total", "paths_done", "events_done", "errors"):
        _progress[k] = 0
    _progress.update(phase="discover", started=int(time.time()), last_error="")
    try:
        if storage.get_setting(PARSE_VERSION_KEY) != str(PARSE_VERSION):
            storage.set_setting(OLDEST_KEY, int(time.time() if now is None else now))   # всё заново, история остаётся
        new = await discover(now)
        storage.set_setting(PARSE_VERSION_KEY, PARSE_VERSION)
        _progress["phase"] = "paths"
        _progress["paths_total"] = sum(e["n_buckets"] or 0 for e in _todo())
        _progress["paths_done"] = 0
        _progress["paths_t0"] = time.monotonic()
        done = 0
        first = True
        while True:
            n, queued = await fetch_paths(now, limit=CHUNK_EVENTS)
            done += n
            if n and queued == CHUNK_EVENTS:
                await _study_quietly()            # большой архив: пороги считаем по ходу
                if first:
                    first = False
                    await _notify_first(done)
            if queued < CHUNK_EVENTS:
                break
        return {"new": new, "done": done, "errors": _progress["errors"]}
    finally:
        _progress.update(phase="idle", finished=int(time.time()), window="")
        _running["on"] = False


async def _notify_first(done: int) -> None:
    from src import study, telegram_notify
    sm = study.summary() or {}
    try:
        await telegram_notify.notify(f"📊 Первые пороги готовы — по {done} последним событиям, проверенных правил: "
                                     f"{sm.get('rules', 0)}. /menu → 📊 Пороги. Архив докачивается дальше.")
    except Exception:  # noqa: BLE001
        pass


async def _study_quietly() -> None:
    from src import study
    try:
        await asyncio.to_thread(study.run)
    except Exception as exc:  # noqa: BLE001
        log.warning("Расчёт порогов упал: %s", exc)


def request_sync() -> None:
    if _wake is not None:
        _wake.set()


async def _after_sync(res: dict) -> None:
    """Скачали новое — пересчитать пороги и сообщить."""
    if not res.get("done"):
        return
    from src import study, telegram_notify
    try:
        summary = await asyncio.to_thread(study.run)
    except Exception as exc:  # noqa: BLE001
        log.warning("Расчёт порогов упал: %s", exc)
        summary = None
    c = storage.archive_counts()
    text = (f"📚 Архив: +{res['done']} событий с историей цены, всего {c['status'].get('done', 0)} "
            f"({c['cities']} городов, {c['first']} … {c['last']}).")
    if summary:
        text += f"\n📊 Пороги пересчитаны: {summary['bets']} расчётных ставок, правил прошло проверку: {summary['rules']}."
    try:
        await telegram_notify.notify(text)
    except Exception:  # noqa: BLE001
        pass


async def run_forever() -> None:
    global _wake
    _wake = asyncio.Event()
    await asyncio.sleep(5)
    if storage.archive_counts()["status"].get("done"):
        await _study_quietly()                 # таблицы порогов — сразу после рестарта
    while True:
        if runtime_state.get("archive_enabled"):
            try:
                res = await sync()
                await _after_sync(res)
            except Exception as exc:  # noqa: BLE001
                _progress["last_error"] = str(exc)[:200]
                log.warning("Архив: %s", exc)
        try:
            await asyncio.wait_for(_wake.wait(), timeout=max(60.0, settings.ARCHIVE_REFRESH_HOURS * 3600))
        except asyncio.TimeoutError:
            pass
        _wake.clear()
