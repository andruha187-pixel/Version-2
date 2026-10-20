"""
Настройки, которые меняются на лету из Telegram: общий режим DRY RUN / LIVE,
монеты (вкл/выкл, 🧪 тест / 🔴 реально) и параметры 🎲 стратегии.

Живут в памяти, каждое изменение сразу пишется в SQLite (`bot_settings`) и
переживает рестарт — если база на постоянном томе (/app/data).
"""
from __future__ import annotations

from config import settings
from src import storage


def _coins_csv(raw: str, default_all: bool) -> str:
    coins = [a.strip().lower() for a in (raw or "").split(",") if a.strip()]
    if not coins and default_all:
        coins = list(settings.ASSETS)
    return ",".join(sorted(set(coins)))


def _bool(v) -> bool:
    return str(v).lower() == "true"


RULES = ("lead60", "cheap")


def _lead_lo(v: float) -> float:
    """Порог «разворот ведёт» — в разумных пределах (выше потолка 0.80 ставок не было бы вовсе)."""
    return round(min(0.75, max(0.50, float(v))), 2)


_DEFAULTS = {
    "dry_run": settings.DRY_RUN,
    # Ключи в базе — свои, не как у прежнего 5m-бота (enabled_assets/live_assets):
    # старая база с его 🔴-монетами не включит реальные ставки серий
    "series_coins_on": _coins_csv(settings.SERIES_COINS, default_all=True),
    "series_coins_live": _coins_csv(settings.SERIES_LIVE_COINS, default_all=False),
    # 🔁 запись окон и 🎲 стратегия серий (src/series_bot.py)
    "series_test_enabled": settings.SERIES_TEST_ENABLED,
    "series_trade_enabled": settings.SERIES_TRADE_ENABLED,
    "series_trade_k": max(1, settings.SERIES_TRADE_K),
    "series_trade_cap": settings.SERIES_TRADE_CAP,
    "series_trade_usdc": max(0.0, settings.SERIES_TRADE_USDC),
    "series_trade_stop": max(0.0, settings.SERIES_TRADE_STOP_USDC),
    "series_trade_rule": settings.SERIES_TRADE_RULE if settings.SERIES_TRADE_RULE in RULES else "lead60",
    "series_lead_lo": _lead_lo(settings.SERIES_LEAD_MIN),
    # с какого момента действуют нынешние правило и пороги (сравнение входов — с него)
    "series_rule_since": 0,
}

_CASTERS = {
    "dry_run": _bool,
    "series_coins_on": str,
    "series_coins_live": str,
    "series_test_enabled": _bool,
    "series_trade_enabled": _bool,
    "series_trade_k": lambda v: max(1, int(float(v))),
    "series_trade_cap": float,
    "series_trade_usdc": lambda v: max(0.0, float(v)),
    "series_trade_stop": lambda v: max(0.0, float(v)),
    "series_trade_rule": lambda v: str(v) if str(v) in RULES else "lead60",
    "series_lead_lo": lambda v: _lead_lo(float(v)),
    "series_rule_since": lambda v: int(float(v)),
}

_state: dict = dict(_DEFAULTS)


def init_from_db() -> None:
    """Вызывать один раз при старте, после storage.init_db()."""
    for key, raw in storage.get_all_settings().items():
        if key in _CASTERS:
            try:
                _state[key] = _CASTERS[key](raw)
            except (TypeError, ValueError):
                pass


def get(key: str):
    """Значение настройки; неизвестный ключ — None (модули, общие с прежним ботом,
    спрашивают и то, чего здесь больше нет)."""
    return _state.get(key)


def set(key: str, value) -> None:
    _state[key] = value
    storage.set_setting(key, value)


def snapshot() -> dict:
    return dict(_state)


# --- Монеты: включена ли и реальные ли ставки ---

def _csv_set(key: str) -> set[str]:
    return {a for a in str(_state.get(key) or "").split(",") if a}


def get_enabled_assets() -> set[str]:
    return _csv_set("series_coins_on")


def is_asset_enabled(asset: str) -> bool:
    return asset.lower() in get_enabled_assets()


def set_enabled_assets(assets: set[str]) -> None:
    set("series_coins_on", ",".join(sorted(assets)))


def toggle_asset(asset: str) -> bool:
    """Включить/выключить монету; возвращает новое состояние (True — включена)."""
    asset = asset.lower()
    enabled = get_enabled_assets()
    enabled.symmetric_difference_update({asset})
    set_enabled_assets(enabled)
    return asset in enabled


def get_live_assets() -> set[str]:
    return _csv_set("series_coins_live")


def is_asset_live(asset: str) -> bool:
    """Монета помечена 🔴 — реальные ставки по ней (сама по себе, без учёта общего режима)."""
    return asset.lower() in get_live_assets()


def set_asset_live(asset: str, live: bool) -> None:
    assets = get_live_assets()
    if live:
        assets.add(asset.lower())
    else:
        assets.discard(asset.lower())
    set("series_coins_live", ",".join(sorted(assets)))


def trade_is_dry(asset: str) -> bool:
    """Ставка по монете будет только тестовой: бот в DRY RUN или монета не 🔴."""
    return bool(get("dry_run")) or not is_asset_live(asset)


def asset_mode(asset: str) -> str:
    """'off' — выключена, 'live' — реальные ставки прямо сейчас, 'dry' — только тест."""
    if not is_asset_enabled(asset):
        return "off"
    return "dry" if trade_is_dry(asset) else "live"
