"""
Поиск температурных событий в Gamma API.

Как именно Polymarket помечает погодные события, со временем менялось (теги
temperature / weather / daily-temperature, у старых событий слаг без года),
поэтому ищем несколькими способами сразу и складываем без повторов:
  1. /events/keyset?title_search=temperature — поиск по названию;
  2. /events?tag_slug=<тег> — по каждому тегу из DISCOVERY_TAGS.
Берём только события с названием «Highest/Lowest temperature in … on …?».
Какой способ сколько нашёл — в 📚 Архиве (если один перестанет работать, видно сразу).
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone

from config import settings
from src import http_api, weather_parse

log = logging.getLogger("gamma")

PAGE = 100
MAX_PAGES = 30                     # на одно окно дат и один способ — страховка от бесконечного листания

_strategy_stats: dict[str, dict] = {}     # способ -> {"events": найдено температурных, "error": текст}


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def strategies() -> list[str]:
    tags = [t.strip() for t in (settings.DISCOVERY_TAGS or "").split(",") if t.strip()]
    return ["keyset:temperature"] + [f"tag:{t}" for t in tags]


def strategy_stats() -> dict:
    return {k: dict(v) for k, v in _strategy_stats.items()}


def _note(strategy: str, found: int = 0, error: str = "") -> None:
    st = _strategy_stats.setdefault(strategy, {"events": 0, "error": ""})
    st["events"] += found
    if error:
        st["error"] = error[:160]
    elif found:
        st["error"] = ""


def _events_from(data) -> tuple[list, str | None]:
    """Ответ Gamma: список событий или {"events"/"data": [...], "next_cursor": …}."""
    if isinstance(data, list):
        return data, None
    if isinstance(data, dict):
        evs = data.get("events")
        if evs is None:
            evs = data.get("data") or []
        return list(evs or []), data.get("next_cursor") or data.get("nextCursor")
    return [], None


def _closed_param(closed: bool | None) -> str | None:
    return None if closed is None else ("true" if closed else "false")


async def _keyset(query: str, closed: bool | None, t0: float, t1: float) -> list:
    out, cursor = [], None
    for _ in range(MAX_PAGES):
        params = {"title_search": query, "limit": PAGE, "end_date_min": _iso(t0), "end_date_max": _iso(t1)}
        if _closed_param(closed):
            params["closed"] = _closed_param(closed)
        if cursor:
            params["after_cursor"] = cursor
        evs, cursor = _events_from(await http_api.get_json(f"{settings.GAMMA_HOST}/events/keyset", params))
        out += evs
        if not cursor or len(evs) < PAGE:
            break
    return out


async def _by_tag(tag: str, closed: bool | None, t0: float, t1: float) -> list:
    out = []
    for page in range(MAX_PAGES):
        params = {"tag_slug": tag, "limit": PAGE, "offset": page * PAGE,
                  "end_date_min": _iso(t0), "end_date_max": _iso(t1)}
        if _closed_param(closed):
            params["closed"] = _closed_param(closed)
        evs, _ = _events_from(await http_api.get_json(f"{settings.GAMMA_HOST}/events", params))
        out += evs
        if len(evs) < PAGE:
            break
    return out


async def get_event(event_id) -> dict | None:
    data = await http_api.get_json(f"{settings.GAMMA_HOST}/events/{event_id}")
    if isinstance(data, list):
        data = data[0] if data else None
    return data if isinstance(data, dict) else None


async def find_events(closed: bool | None, t0: float, t1: float) -> list[dict]:
    """Температурные события (сырой JSON Gamma) с концом в [t0, t1)."""
    found: dict[str, dict] = {}
    errors = []
    for strategy in strategies():
        kind, _, arg = strategy.partition(":")
        try:
            evs = await (_keyset(arg, closed, t0, t1) if kind == "keyset" else _by_tag(arg, closed, t0, t1))
        except http_api.ApiError as exc:
            _note(strategy, error=str(exc))
            errors.append(f"{strategy}: {exc}")
            continue
        n = 0
        for ev in evs:
            if not isinstance(ev, dict) or not weather_parse.is_temperature_title(ev.get("title") or ""):
                continue
            key = str(ev.get("id"))
            if key not in found:
                n += 1
                found[key] = ev
            elif not found[key].get("markets") and ev.get("markets"):
                found[key] = ev
        _note(strategy, n)
    if errors and len(errors) == len(strategies()):
        raise http_api.ApiError("все способы поиска не сработали — " + "; ".join(errors)[:300])
    out = []
    for key, ev in found.items():
        if not ev.get("markets"):
            try:
                full = await get_event(key)
            except http_api.ApiError:
                full = None
            if full and full.get("markets"):
                ev = full
        out.append(ev)
    return out
