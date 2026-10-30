"""
📡 METAR — официальные сводки со станций аэропортов: та же температура, по которой
резолвятся рынки. Это ground truth, и источник публичный.

Зачем. Рынок «Highest/Lowest temperature …» резолвится по дневному макс/мин на станции.
Как только выходит новое наблюдение (а METAR публикуется раз в ~30 мин, плюс внеочередные
SPECI), исход части корзин становится известнее. Если стакан Polymarket реагирует на свежее
наблюдение с задержкой, это видно по данным. Этот модуль берёт METAR как можно быстрее из
публичных источников, ставит метку времени на каждое НОВОЕ наблюдение (obs_ts — время самой
сводки, recv_ts — когда бот её увидел) и просит live снять стакан сразу после — чтобы потом
измерить задержку. Ставок он не делает.

Источник. По умолчанию aviationweather.gov (data API, один запрос на все станции разом,
format=raw — разбираем сам текст METAR, не завися от формата JSON). Если он не ответил —
поштучно NOAA tgftp. Обе точки — публичные, без авторизации. Запросы идут отдельным HTTP-
клиентом (не через лимитер Polymarket).
"""
from __future__ import annotations
import asyncio
import logging
import re
import time
from datetime import datetime, timezone

import httpx

from config import settings
from src import cities, storage

log = logging.getLogger("metar")

# Станция (ICAO) в начале строки METAR
_STATION_RE = re.compile(r"\b([A-Z][A-Z0-9]{3})\b")
# Время наблюдения: DDHHMMZ
_TIME_RE = re.compile(r"\b(\d{2})(\d{2})(\d{2})Z\b")
# Температура/точка росы в теле: «05/M01», «M03/M07»
_TEMP_RE = re.compile(r"\b(M?\d{1,2})/(M?\d{1,2}|//)\b")
# Точная температура из RMK: Tsdddsddd (s: 0 плюс, 1 минус; ddd — десятые °C)
_TPRECISE_RE = re.compile(r"\bT([01])(\d{3})([01])(\d{3})\b")

_last_obs: dict[str, int] = {}          # станция -> последнее увиденное obs_ts
_pending: set[str] = set()              # city_key, где только что было новое наблюдение (live снимет стакан)
_loaded = {"on": False}
_client: httpx.AsyncClient | None = None
_stats = {"polls": 0, "new_obs": 0, "parsed": 0, "last_poll": 0, "last_error": "", "source": "", "stations": 0}


def reset() -> None:
    _last_obs.clear()
    _pending.clear()
    _loaded["on"] = False
    for k in _stats:
        _stats[k] = "" if k in ("last_error", "source") else 0


def stats() -> dict:
    return dict(_stats)


def stations() -> list[str]:
    """ICAO станций всех городов, которые знаем (по одной на станцию)."""
    return sorted({ic for ic in (cities.icao_for(c) for c in _city_names()) if ic})


def _city_names() -> list[str]:
    # города из ASSETS-подобного списка нет; берём все известные ключи CITY_ICAO обратно к названиям не нужно —
    # city_key самих ключей совпадает с ними, а icao_for принимает и ключ
    return list(cities.CITY_ICAO.keys())


def _station_city(station: str) -> str:
    """city_key по ICAO (первый город с этой станцией)."""
    for ck, ic in cities.CITY_ICAO.items():
        if ic == station:
            return ck
    return ""


def obs_time(day: int, hh: int, mm: int, now: datetime | None = None) -> int | None:
    """DDHHMMZ → Unix-время. Месяц/год — из текущей даты (наблюдение всегда в недавнем прошлом):
    если в этом месяце получается будущее больше чем на час, значит это прошлый месяц."""
    now = now or datetime.now(timezone.utc)
    for back in (0, 1):
        y, m = now.year, now.month - back
        if m < 1:
            y, m = y - 1, 12
        try:
            cand = datetime(y, m, day, hh, mm, tzinfo=timezone.utc)
        except ValueError:
            continue
        if cand.timestamp() <= now.timestamp() + 3600:
            return int(cand.timestamp())
    return None


def parse_metar(line: str, now: datetime | None = None) -> tuple[str, int, float | None] | None:
    """Строка METAR → (станция, obs_ts, temp_c) или None. temp_c — десятые (RMK Tttt), иначе целые °C."""
    line = line.strip()
    if not line or line.upper().startswith(("METAR", "SPECI")):
        line = re.sub(r"^(METAR|SPECI)\s+", "", line, flags=re.I)
    m_st = _STATION_RE.match(line)
    m_t = _TIME_RE.search(line)
    if not m_st or not m_t:
        return None
    station = m_st.group(1)
    ts = obs_time(int(m_t.group(1)), int(m_t.group(2)), int(m_t.group(3)), now)
    if ts is None:
        return None
    temp = None
    mp = _TPRECISE_RE.search(line)
    if mp:
        temp = int(mp.group(2)) / 10.0 * (-1 if mp.group(1) == "1" else 1)
    else:
        mt = _TEMP_RE.search(line[m_t.end():])         # после группы времени, чтобы не поймать «R06/...»
        if mt and mt.group(1) != "//":
            v = mt.group(1)
            temp = -int(v[1:]) if v.startswith("M") else int(v)
    return station, ts, temp


def _ingest(line: str, recv: int, now_dt: datetime) -> bool:
    """Разобрать строку и, если наблюдение новее прежнего для станции, записать. True — новое."""
    parsed = parse_metar(line, now_dt)
    if parsed is None:
        return False
    station, obs_ts, temp = parsed
    _stats["parsed"] += 1
    if obs_ts <= _last_obs.get(station, 0):
        return False
    _last_obs[station] = obs_ts
    city_key = _station_city(station)
    if storage.metar_add(station, obs_ts, recv, temp, line.strip()[:500], city_key):
        _stats["new_obs"] += 1
        if city_key:
            _pending.add(city_key)
        return True
    return False


def take_pending() -> set[str]:
    """Города с новым наблюдением с прошлого раза — live снимет по ним стакан. Забираем и очищаем."""
    out = set(_pending)
    _pending.clear()
    return out


async def _get_text(url: str, params: dict | None = None) -> str | None:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=20, headers={"User-Agent": "polymarket-weather-bot (METAR)"})
    try:
        resp = await _client.get(url, params=params)
    except Exception as exc:  # noqa: BLE001
        _stats["last_error"] = f"{type(exc).__name__}: {exc}"[:200]
        return None
    if resp.status_code != 200:
        _stats["last_error"] = f"HTTP {resp.status_code} {url}"[:200]
        return None
    return resp.text


async def _poll_aviationweather(ids: list[str], recv: int, now_dt: datetime) -> bool:
    """Все станции одним запросом (format=raw). True — источник ответил."""
    text = await _get_text(f"{settings.METAR_AVWX_URL}", {"ids": ",".join(ids), "format": "raw", "taf": "false"})
    if text is None:
        return False
    for line in text.splitlines():
        _ingest(line, recv, now_dt)
    _stats["source"] = "aviationweather"
    return True


async def _poll_tgftp(ids: list[str], recv: int, now_dt: datetime) -> bool:
    """Запасной источник: по одному файлу на станцию."""
    ok = False
    for st in ids:
        text = await _get_text(f"{settings.METAR_TGFTP_BASE}/{st}.TXT")
        if text is None:
            continue
        ok = True
        for line in text.splitlines():
            if _ingest(line, recv, now_dt):
                break
        await asyncio.sleep(0.1)
    if ok:
        _stats["source"] = "tgftp"
    return ok


async def poll_once(now: float | None = None) -> int:
    """Один опрос всех станций. Возвращает, сколько новых наблюдений записано."""
    if not _loaded["on"]:
        _last_obs.update(storage.metar_last_obs())      # после рестарта старое не считаем новым
        _loaded["on"] = True
    ids = stations()
    _stats["stations"] = len(ids)
    if not ids:
        return 0
    now = time.time() if now is None else now
    now_dt = datetime.fromtimestamp(now, tz=timezone.utc)
    recv = int(now)
    before = _stats["new_obs"]
    ok = await _poll_aviationweather(ids, recv, now_dt)
    if not ok:
        ok = await _poll_tgftp(ids, recv, now_dt)
    _stats.update(polls=_stats["polls"] + 1, last_poll=int(now))
    return _stats["new_obs"] - before


async def run_forever() -> None:
    await asyncio.sleep(8)
    while True:
        if settings.METAR_ENABLED:
            try:
                await poll_once()
            except Exception as exc:  # noqa: BLE001
                _stats["last_error"] = str(exc)[:200]
                log.warning("METAR: %s", exc)
        await asyncio.sleep(max(10.0, float(settings.METAR_POLL_SEC)))
