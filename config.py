"""
Конфигурация бота. Все настройки — из переменных окружения (.env), см.
.env.example. Ключи не хардкодим.

Бот один: 🎲 стратегия серий на 15-минутных рынках Up/Down Polymarket —
после нескольких одинаковых исходов подряд ставка на разворот (src/series_bot.py).
"""
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _get_float(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def _get_int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


def _coins(raw: str) -> list:
    return [a.strip().lower() for a in (raw or "").split(",") if a.strip()]


@dataclass
class Settings:
    # --- Монеты ---
    # Рынки 15m есть у этих шести. SERIES_COINS — какие включены при старте
    # (пусто — все), SERIES_LIVE_COINS — какие сразу 🔴 (реальные ставки при
    # общем 🔴 LIVE; по умолчанию ни одной). Дальше — кнопками в 🪙 Монетах.
    # Имена новые, не ENABLED_ASSETS/LIVE_ASSETS прежнего 5m-бота: оставшиеся
    # от него переменные не включат реальные ставки серий.
    ASSETS: list = field(default_factory=lambda: _coins(os.getenv("ASSETS", "btc,eth,sol,xrp,bnb,doge")))
    SERIES_COINS: str = os.getenv("SERIES_COINS", "")
    SERIES_LIVE_COINS: str = os.getenv("SERIES_LIVE_COINS", "")

    # --- Режим ---
    # true — ни одного реального ордера (общий выключатель, 🧪/🔴 в главном меню)
    DRY_RUN: bool = _get_bool("DRY_RUN", True)

    # --- Binance (только свечи для 📚 истории) ---
    BINANCE_BASE_URL: str = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
    BINANCE_FUTURES_URL: str = os.getenv("BINANCE_FUTURES_URL", "https://fapi.binance.com")

    # --- Цена Chainlink (src/chainlink_feed.py) — исход окна, если стакан не решил ---
    CHAINLINK_USE_POLYBOLT: bool = _get_bool("CHAINLINK_USE_POLYBOLT", True)
    CHAINLINK_POLYBOLT_URL: str = os.getenv("CHAINLINK_POLYBOLT_URL", "wss://ws-live-v2.polymarket.com/ws")
    CHAINLINK_USE_RTDS: bool = _get_bool("CHAINLINK_USE_RTDS", True)
    CHAINLINK_RTDS_URL: str = os.getenv("CHAINLINK_RTDS_URL", "wss://ws-live-data.polymarket.com")
    # API-ключи CLOB для PolyBolt. Не заданы — бот берёт их из SDK-клиента или
    # выводит из POLY_PRIVATE_KEY (только чтение существующих).
    POLY_API_KEY: str = os.getenv("POLY_API_KEY", "")
    POLY_API_SECRET: str = os.getenv("POLY_API_SECRET", "")
    POLY_API_PASSPHRASE: str = os.getenv("POLY_API_PASSPHRASE", "")

    # --- Polymarket / CLOB ---
    POLY_HOST: str = os.getenv("POLY_HOST", "https://clob.polymarket.com")
    GAMMA_HOST: str = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
    POLY_CHAIN_ID: int = _get_int("POLY_CHAIN_ID", 137)
    POLY_PRIVATE_KEY: str = os.getenv("POLY_PRIVATE_KEY", "")
    POLY_FUNDER_ADDRESS: str = os.getenv("POLY_FUNDER_ADDRESS", "")
    # Живой стакан по WebSocket — без него стратегия не работает
    USE_LIVE_BOOK_STREAM: bool = _get_bool("USE_LIVE_BOOK_STREAM", True)
    # Комиссия тейкера Polymarket: ставка × FEE_RATE × (1 − цена)
    FEE_RATE: float = _get_float("FEE_RATE", 0.07)

    # --- 📚 История: свечи Binance и официальные исходы Polymarket ---
    SERIES_HISTORY_DAYS: int = _get_int("SERIES_HISTORY_DAYS", 365)
    SERIES_ENTRY_PRICE: float = _get_float("SERIES_ENTRY_PRICE", 0.51)
    SERIES_OFFICIAL_DAYS: int = _get_int("SERIES_OFFICIAL_DAYS", 60)

    # --- 🔁 Запись окон (цены сторон, путь цены, стакан, исходы) ---
    SERIES_TEST_ENABLED: bool = _get_bool("SERIES_TEST_ENABLED", True)
    SERIES_INTERVALS: str = os.getenv("SERIES_INTERVALS", "15m")
    SERIES_PATH_STEP_SEC: int = _get_int("SERIES_PATH_STEP_SEC", 5)
    # с 30 с до старта и первые 90 с — цена каждые SERIES_FINE_STEP_SEC секунд
    # (0 — как остальное окно), плюс снимки стакана; старше SERIES_PATH_KEEP_DAYS
    # дней путь и снимки стираются (цены старта и исходы остаются; 0 — хранить всё)
    SERIES_FINE_STEP_SEC: int = _get_int("SERIES_FINE_STEP_SEC", 1)
    SERIES_PATH_KEEP_DAYS: int = _get_int("SERIES_PATH_KEEP_DAYS", 14)

    # --- 🎲 Стратегия: после SERIES_TRADE_K+ одинаковых исходов — ставка на разворот ---
    # Правило входа: lead60 — на 60-й секунде окна, если разворот уже ведёт
    # (ask от SERIES_LEAD_MIN до 0.80), по цене не выше ask + 1¢; cheap — в первые
    # SERIES_TRADE_ENTRY_SEC секунд по цене не выше SERIES_TRADE_CAP.
    # Ставка — SERIES_TRADE_USDC, но не меньше 5 акций. SERIES_TRADE_STOP_USDC —
    # дневной стоп реальных ставок (0 — выкл). Всё меняется кнопками в 🎲.
    SERIES_TRADE_RULE: str = os.getenv("SERIES_TRADE_RULE", "lead60").strip().lower()
    # Нижний порог «разворот ведёт» для lead60 (с v11; до этого 0.52): по 16–20.10
    # входы по 0.52–0.56 — 12 из 22, около нуля; от 0.60 — 49 из 62
    SERIES_LEAD_MIN: float = _get_float("SERIES_LEAD_MIN", 0.58)
    SERIES_TRADE_ENABLED: bool = _get_bool("SERIES_TRADE_ENABLED", True)
    SERIES_TRADE_INTERVALS: str = os.getenv("SERIES_TRADE_INTERVALS", "15m")
    SERIES_TRADE_K: int = _get_int("SERIES_TRADE_K", 4)
    SERIES_TRADE_CAP: float = _get_float("SERIES_TRADE_CAP", 0.51)
    SERIES_TRADE_USDC: float = _get_float("SERIES_TRADE_USDC", 3.0)
    SERIES_TRADE_STOP_USDC: float = _get_float("SERIES_TRADE_STOP_USDC", 30.0)
    SERIES_TRADE_ENTRY_SEC: int = _get_int("SERIES_TRADE_ENTRY_SEC", 60)

    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # --- База и отчёты ---
    DB_PATH: str = os.getenv("DB_PATH", "data/bot.db")
    REPORT_INTERVAL_HOURS: float = _get_float("REPORT_INTERVAL_HOURS", 4.0)
    REPORTS_DIR: str = os.getenv("REPORTS_DIR", "data/reports")


settings = Settings()
