"""
Пауза на время выхода важной американской статистики (только для режима
«ранний импульс»).

Почему: стратегия покупает лидера в первые 2.5 минуты 15-минутного окна.
Если окно начинается ровно в момент выхода новости, наш вход приходится
точно на рывок цены от новости, а такие рывки часто откатывают обратно.
Пример — 02.10.2026, 12:30 UTC (08:30 Нью-Йорк, отчёт по рынку труда США):
BTC +$500 за полторы минуты, вход UP по 0.78, к концу окна полный разворот
и проигрыш. На истории 16.09–02.10 таких входов не было ни одного, то есть
преимущество стратегии в этом режиме просто не проверено. Пропуск стоит
~1 рынок в будний день.

Время задаётся по Нью-Йорку: переход на зимнее время учитывается сам
(летом 08:30 NY = 12:30 UTC, зимой = 13:30 UTC).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from config import settings

_ET = ZoneInfo("America/New_York")


def _parse_hm(text: str) -> tuple[int, int] | None:
    try:
        h, m = text.strip().split(":")
        return int(h), int(m)
    except (ValueError, AttributeError):
        return None


def _parse_dt(text: str) -> datetime | None:
    try:
        return datetime.strptime(text.strip(), "%Y-%m-%d %H:%M").replace(tzinfo=_ET)
    except (ValueError, AttributeError):
        return None


def describe_times() -> str:
    """"08:30 NY (сейчас 12:30 UTC)" — для меню в Telegram."""
    now_et = datetime.now(tz=_ET)
    parts = []
    for text in settings.NEWS_PAUSE_ET_TIMES:
        hm = _parse_hm(text)
        if hm is None:
            continue
        t_utc = now_et.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0).astimezone(timezone.utc)
        parts.append(f"{hm[0]:02d}:{hm[1]:02d} NY (сейчас {t_utc:%H:%M} UTC)")
    return ", ".join(parts) or "—"


def pause_reason(market_start_ts: int | None, market_end_ts: int | None = None) -> str | None:
    """Причина пропуска рынка или None, если новостей внутри окна нет.

    Рынок пропускается, если момент выхода новости попадает в его окно
    [начало, конец). Рынок, который ЗАКАНЧИВАЕТСЯ ровно в момент новости,
    не трогаем — его итог фиксируется до реакции цены."""
    if not market_start_ts:
        return None
    start_utc = datetime.fromtimestamp(market_start_ts, tz=timezone.utc)
    end_utc = (datetime.fromtimestamp(market_end_ts, tz=timezone.utc)
               if market_end_ts else start_utc + timedelta(minutes=15))
    start_et = start_utc.astimezone(_ET)

    # Регулярная статистика: по будним дням в заданное время (по умолчанию 08:30 NY).
    if start_et.weekday() < 5:
        for text in settings.NEWS_PAUSE_ET_TIMES:
            hm = _parse_hm(text)
            if hm is None:
                continue
            release = start_et.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
            if start_utc <= release.astimezone(timezone.utc) < end_utc:
                return (f"пауза на новости: в {hm[0]:02d}:{hm[1]:02d} по Нью-Йорку выходит "
                        f"статистика США")

    # Разовые события (решения ФРС и т.п.).
    for text in settings.NEWS_PAUSE_ET_DATETIMES:
        release = _parse_dt(text)
        if release is None:
            continue
        if start_utc <= release.astimezone(timezone.utc) < end_utc:
            return f"пауза на новости: {text.strip()} по Нью-Йорку (ФРС)"
    return None
