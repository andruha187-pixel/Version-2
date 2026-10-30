"""
Точка входа погодного бота.

Фоновые задачи:
  - archive   — 📚 докачка закрытых температурных событий и истории цены;
  - live      — 🎯 открытые события: касания уровней, кандидаты, тестовые ставки, снимки цен;
  - metar     — 📡 официальные сводки станций (ground truth) для измерения задержки стакана;
  - forecast  — 🔮 ансамблевый прогноз дневного макс/мин (Open-Meteo), вероятности корзин;
  - reporting — сводка раз в сутки;
  - Telegram  — меню и уведомления.
"""
from __future__ import annotations
import asyncio
import logging

from src import archive, forecast_feed, live, metar_feed, reporting, runtime_state, storage, telegram_notify

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket-weather-bot")


async def main():
    storage.init_db()
    runtime_state.init_from_db()
    app = telegram_notify.build_app()
    async with app:
        await app.start()
        await app.updater.start_polling()
        try:
            await telegram_notify.notify(telegram_notify.startup_text())
        except Exception as exc:  # noqa: BLE001
            log.warning("Стартовое сообщение не ушло: %s", exc)
        tasks = [asyncio.create_task(j) for j in (archive.run_forever(), live.run_forever(),
                                              metar_feed.run_forever(), forecast_feed.run_forever(),
                                              reporting.report_loop())]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await app.updater.stop()
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
