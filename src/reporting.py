"""
Отчёт раз в REPORT_INTERVAL_HOURS: три CSV-файла в Telegram —
  *_series.csv          — окна (цены сторон, путь цены, снимки стакана, исходы);
  *_series_trades.csv   — ставки стратегии (тестовые и реальные);
  *_series_variants.csv — виртуальные ставки вариантов входа.
В каждом — период и 30 минут прошлого (уже с исходами; повторы отличаются по id).
Момент последнего отчёта хранится в bot_settings — переживает рестарт.
"""
from __future__ import annotations
import asyncio
import csv
import logging
import os
import time

from config import settings
from src import series_bot, storage, telegram_notify

log = logging.getLogger("reporting")

_LAST_REPORT_KEY = "last_report_ts"
OVERLAP_SEC = 30 * 60
# Telegram отклоняет подпись к файлу длиннее 1024 символов (считает в UTF-16)
CAPTION_LIMIT = 1024


def _caption_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _fit_caption(text: str, limit: int = CAPTION_LIMIT) -> str:
    if _caption_len(text) <= limit:
        return text
    while text and _caption_len(text) > limit - 1:
        text = text[:-1]
    return text + "…"


def _write_csv(path: str, columns: list[str], rows: list[tuple]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        w.writerows(rows)


def _get_last_report_ts() -> int:
    raw = storage.get_setting(_LAST_REPORT_KEY)
    try:
        return int(float(raw)) if raw else int(time.time()) - int(settings.REPORT_INTERVAL_HOURS * 3600)
    except ValueError:
        return int(time.time()) - int(settings.REPORT_INTERVAL_HOURS * 3600)


def _set_last_report_ts(ts: int) -> None:
    storage.set_setting(_LAST_REPORT_KEY, ts)


async def _send_series(base: str, from_label: str, to_label: str, since_ts: int, rows_since: int) -> int:
    """Окна, ставки и варианты с rows_since — тремя файлами. Возвращает число окон."""
    rows = storage.series_export(rows_since)
    trades = storage.series_trades_export(rows_since)
    variants = storage.series_variants_export(rows_since)
    if not rows and not trades and not variants:
        return 0
    files = []
    if rows:
        files.append((f"{base}_series.csv", storage.SERIES_COLUMNS, rows,
                      series_bot.report_caption(from_label, to_label, since_ts)))
    if trades:
        files.append((f"{base}_series_trades.csv", storage.SERIES_TRADE_COLUMNS, trades,
                      "\n".join(series_bot.strategy_lines())))
    if variants:
        files.append((f"{base}_series_variants.csv", storage.SERIES_VARIANT_COLUMNS, variants,
                      "🎲 " + "\n".join(series_bot.variant_lines())))
    try:
        for path, columns, data, caption in files:
            _write_csv(path, columns, data)
            await telegram_notify.send_document(path, _fit_caption(caption))
    finally:
        for path, *_rest in files:
            try:
                os.remove(path)
            except OSError:
                pass
    return len(rows)


async def build_and_send_report() -> None:
    since_ts = _get_last_report_ts()
    now_ts = int(time.time())
    from_label = time.strftime("%Y%m%d-%H%M", time.gmtime(since_ts))
    to_label = time.strftime("%Y%m%d-%H%M", time.gmtime(now_ts))
    base = os.path.join(settings.REPORTS_DIR, f"{from_label}_to_{to_label}")
    await _send_series(base, from_label, to_label, since_ts, max(0, since_ts - OVERLAP_SEC))
    _set_last_report_ts(now_ts)


async def send_series_all() -> int:
    """Кнопка 📤: все окна, ставки и варианты из базы."""
    to_label = time.strftime("%Y%m%d-%H%M", time.gmtime())
    return await _send_series(os.path.join(settings.REPORTS_DIR, f"all_to_{to_label}"), "всё в базе", to_label, 0, 0)


async def report_loop() -> None:
    """Фоновая задача: отчёт раз в REPORT_INTERVAL_HOURS (момент последнего — в базе)."""
    interval = max(60.0, settings.REPORT_INTERVAL_HOURS * 3600)
    while True:
        wait = _get_last_report_ts() + interval - time.time()
        if wait > 0:
            await asyncio.sleep(min(wait, 300))
            continue
        try:
            await build_and_send_report()
        except Exception as exc:  # noqa: BLE001 — отчёт не должен ронять бота
            log.warning("Отчёт не ушёл: %s", exc)
            await asyncio.sleep(300)
