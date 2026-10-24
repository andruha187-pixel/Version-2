"""
Настройки, которые меняются кнопками в Telegram: архив (вкл/выкл, глубина),
фильтры расчёта порогов, кандидаты (вкл/выкл, города, только «свои» правила
города), тестовая ставка. Живут в памяти, каждое изменение пишется в SQLite
(bot_settings) и переживает рестарт, если база на постоянном томе.
"""
from __future__ import annotations

from config import settings
from src import storage


def _bool(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _csv(v) -> str:
    return ",".join(sorted({x.strip().lower() for x in str(v or "").split(",") if x.strip()}))


_DEFAULTS = {
    "archive_enabled": True,
    "archive_days": int(settings.ARCHIVE_DAYS),
    "min_event_volume": float(settings.MIN_EVENT_VOLUME),
    "rule_min_n": int(settings.RULE_MIN_N),
    "alerts_enabled": bool(settings.ALERTS_ENABLED),
    "alerts_city_only": False,          # только правила самого города (без «все города»)
    "cities_off": "",                   # города без кандидатов (ключи через запятую)
    "alerts_sides": "both",             # кандидаты по стороне: both / yes / no
    "paper_stake": float(settings.PAPER_STAKE_USDC),
}

_CASTERS = {
    "archive_enabled": _bool,
    "archive_days": lambda v: max(7, min(730, int(float(v)))),
    "min_event_volume": lambda v: max(0.0, float(v)),
    "rule_min_n": lambda v: max(5, min(500, int(float(v)))),
    "alerts_enabled": _bool,
    "alerts_city_only": _bool,
    "cities_off": _csv,
    "alerts_sides": lambda v: str(v) if str(v) in ("both", "yes", "no") else "both",
    "paper_stake": lambda v: max(1.0, min(10000.0, float(v))),
}

_state: dict = dict(_DEFAULTS)


def init_from_db() -> None:
    for key, raw in storage.get_all_settings().items():
        if key in _CASTERS:
            try:
                _state[key] = _CASTERS[key](raw)
            except (TypeError, ValueError):
                pass


def get(key: str):
    return _state.get(key)


def set(key: str, value) -> None:
    if key in _CASTERS:
        value = _CASTERS[key](value)
    _state[key] = value
    storage.set_setting(key, value)


def reset() -> None:
    _state.clear()
    _state.update(_DEFAULTS)


def city_alerts_on(city_key: str) -> bool:
    return city_key not in {x for x in str(_state.get("cities_off") or "").split(",") if x}


def toggle_city(city_key: str) -> bool:
    """Вкл/выкл кандидатов по городу. True — теперь включены."""
    off = {x for x in str(_state.get("cities_off") or "").split(",") if x}
    off.symmetric_difference_update({city_key})
    set("cities_off", ",".join(sorted(off)))
    return city_key not in off
