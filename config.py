"""
Конфигурация погодного бота. Все настройки — из переменных окружения (.env),
см. .env.example. Ключей кошелька боту не нужно: он ничего не покупает.

Бот: 📚 архив температурных рынков Polymarket («Highest/Lowest temperature in
<город> on <дата>?»), 📊 поиск порогов цены, после которых сторона выигрывала,
по городам и времени суток, и 🎯 кандидаты на открытых рынках с тестовыми
ставками. Почти всё ниже — стартовые значения: дальше настройки меняются
кнопками в Telegram и хранятся в базе.
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


def _floats(raw: str) -> list:
    return sorted({round(float(x), 4) for x in (raw or "").replace(";", ",").split(",") if x.strip()})


@dataclass
class Settings:
    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # --- API Polymarket (публичные, ключ не нужен) ---
    GAMMA_HOST: str = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
    CLOB_HOST: str = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
    # Не чаще стольких запросов в секунду на все API вместе
    HTTP_RPS: float = _get_float("HTTP_RPS", 8.0)
    # Как искать температурные события: keyset-поиск по названию и по тегам Gamma
    DISCOVERY_TAGS: str = os.getenv("DISCOVERY_TAGS", "temperature,weather,daily-temperature")

    # --- 📚 Архив закрытых рынков ---
    ARCHIVE_DAYS: int = _get_int("ARCHIVE_DAYS", 120)           # глубина архива (кнопками 60/120/240/365)
    ARCHIVE_REFRESH_HOURS: float = _get_float("ARCHIVE_REFRESH_HOURS", 6.0)
    PATH_FIDELITY_MIN: int = _get_int("PATH_FIDELITY_MIN", 5)   # шаг истории цены, минут

    # --- 📊 Пороги ---
    # Уровни цены, на которых смотрим «цена впервые дошла до X → выиграла ли сторона»
    STUDY_LEVELS: list = field(default_factory=lambda: _floats(
        os.getenv("STUDY_LEVELS", "0.80,0.83,0.85,0.87,0.90,0.92,0.94,0.96")))
    # Цена входа = цена в истории + столько (ask выше середины/последней сделки)
    STUDY_SLIP: float = _get_float("STUDY_SLIP", 0.01)
    # Цена перепрыгнула уровень больше чем на столько — считаем, что по уровню было не войти
    STUDY_MAX_JUMP: float = _get_float("STUDY_MAX_JUMP", 0.05)
    # Комиссия тейкера погодных рынков: сумма × FEE_RATE × (1 − цена)
    FEE_RATE: float = _get_float("FEE_RATE", 0.05)
    # Правило «проверено»: всего ставок не меньше RULE_MIN_N; на старой части архива (первые 60% дней)
    # выигрышей значимо больше, чем нужно для окупаемости (биномиальный p ≤ RULE_MAX_P), и на новой
    # части — в плюсе; в каждой части не меньше RULE_MIN_PART_N ставок
    RULE_MIN_N: int = _get_int("RULE_MIN_N", 30)
    RULE_MIN_PART_N: int = _get_int("RULE_MIN_PART_N", 10)
    RULE_MAX_P: float = _get_float("RULE_MAX_P", 0.05)
    # События с меньшим объёмом торгов, $ — в расчёт не берём (тонкие рынки)
    MIN_EVENT_VOLUME: float = _get_float("MIN_EVENT_VOLUME", 5000.0)

    # --- 🎯 Кандидаты на открытых рынках ---
    LIVE_ENABLED: bool = _get_bool("LIVE_ENABLED", True)
    LIVE_POLL_SEC: float = _get_float("LIVE_POLL_SEC", 60.0)
    SNAPSHOT_EVERY_MIN: float = _get_float("SNAPSHOT_EVERY_MIN", 10.0)   # снимок цен открытых рынков в базу
    ALERTS_ENABLED: bool = _get_bool("ALERTS_ENABLED", True)
    PAPER_STAKE_USDC: float = _get_float("PAPER_STAKE_USDC", 10.0)       # тестовая ставка на кандидата

    # --- База и отчёты ---
    DB_PATH: str = os.getenv("DB_PATH", "data/weather.db")
    REPORTS_DIR: str = os.getenv("REPORTS_DIR", "data/reports")
    REPORT_HOUR_UTC: int = _get_int("REPORT_HOUR_UTC", 6)               # ежедневная сводка


settings = Settings()
