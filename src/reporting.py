"""
Файлы и сводки в Telegram.

📤 Файлы (кнопка):
  weather_events.zip     — события и корзины: город, макс/мин, дата, станция, объём, какая корзина выиграла;
  weather_study.zip      — study_bets.csv (каждая расчётная ставка: касание уровня, вход, время, итог)
                           и study_cells.csv (все ячейки: город × тип × сторона × уровень × окно);
  weather_candidates.csv — кандидаты и тестовые ставки;
  weather_snapshots.zip  — снимки цен открытых рынков за 7 дней.
🗂 Пути цены (кнопка): weather_paths_<месяц>.zip — история цены YES каждой корзины
  (t0 — начало, dts — шаги по времени в секундах, ps — цены в десятитысячных).

Раз в сутки (REPORT_HOUR_UTC) — сводка: архив, лучшие правила, кандидаты за сутки.
"""
from __future__ import annotations
import asyncio
import csv
import io
import json
import logging
import os
import time
import zipfile

from config import settings
from src import live, storage, study, telegram_notify, text

log = logging.getLogger("reporting")

CAPTION_LIMIT = 1024
ZIP_PART_LIMIT = 40 * 1024 * 1024
_LAST_DAILY_KEY = "last_daily_report"


def _caption_len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def _fit_caption(s: str, limit: int = CAPTION_LIMIT) -> str:
    if _caption_len(s) <= limit:
        return s
    while s and _caption_len(s) > limit - 1:
        s = s[:-1]
    return s + "…"


def _csv_bytes(columns, rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(columns)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _dir() -> str:
    os.makedirs(settings.REPORTS_DIR, exist_ok=True)
    return settings.REPORTS_DIR


async def _send(path: str, caption: str) -> None:
    try:
        await telegram_notify.send_document(path, _fit_caption(caption))
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def build_events_zip(path: str) -> int:
    rows = storage.events_export()
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("weather_events.csv", _csv_bytes(storage.EVENTS_EXPORT_COLUMNS, rows))
    return len(rows)


def build_study_zip(path: str) -> dict:
    """Пересчитать пороги с записью всех расчётных ставок и упаковать ставки и ячейки."""
    bets_csv = path + ".bets.csv"
    try:
        summary = study.run(bets_path=bets_csv)
        cells = study.cells() or []
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(bets_csv, "study_bets.csv")
            zf.writestr("study_cells.csv", _csv_bytes(study.CELL_COLUMNS, [[c.get(k) for k in study.CELL_COLUMNS]
                                                                            for c in cells]))
            zf.writestr("study_summary.json", json.dumps(summary, ensure_ascii=False, indent=1))
    finally:
        try:
            os.remove(bets_csv)
        except OSError:
            pass
    return summary


def build_candidates_csv(path: str) -> int:
    rows = storage.candidates()
    cols = storage.CANDIDATE_COLUMNS + ["city", "kind", "date", "unit", "bucket"]
    with open(path, "wb") as f:
        f.write(_csv_bytes(cols, [[r.get(c) for c in cols] for r in reversed(rows)]))
    return len(rows)


def build_snapshots_zip(path: str, since_ts: int) -> int:
    rows = storage.snapshots_export(since_ts)
    if not rows:
        return 0
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("weather_snapshots.csv", _csv_bytes(["event_id", "city", "kind", "date", "ts",
                                                         "data_idx_bid_ask_last_liquidity"], rows))
    return len(rows)


async def send_files() -> int:
    """Кнопка 📤: события, расчёт порогов со всеми ставками, кандидаты, снимки."""
    d = _dir()
    stamp = time.strftime("%Y%m%d-%H%M", time.gmtime())
    sent = 0
    c = storage.archive_counts()
    if c["events"]:
        p = os.path.join(d, f"weather_events_{stamp}.zip")
        n = await asyncio.to_thread(build_events_zip, p)
        await _send(p, f"📚 События и корзины: {text.n_events(c['events'])}, {n} корзин, "
                       f"{c['first']} … {c['last']}.")
        sent += 1
    if c["status"].get("done"):
        p = os.path.join(d, f"weather_study_{stamp}.zip")
        sm = await asyncio.to_thread(build_study_zip, p)
        await _send(p, f"📊 Расчёт порогов: {text.n_bets(sm['bets'])} по {text.n_events(sm['events'])}, "
                       f"проверенных правил {sm['rules']}. study_bets.csv — каждая ставка, study_cells.csv — ячейки.")
        sent += 1
    p = os.path.join(d, f"weather_candidates_{stamp}.csv")
    if await asyncio.to_thread(build_candidates_csv, p):
        st = live.paper_stats(0)
        await _send(p, f"🎯 Кандидаты: {st['n']}, с итогом {st['settled']}, выигр. {st['wins']}, {st['pnl']:+.2f} $.")
        sent += 1
    elif os.path.exists(p):
        os.remove(p)
    p = os.path.join(d, f"weather_snapshots_{stamp}.zip")
    if await asyncio.to_thread(build_snapshots_zip, p, int(time.time()) - 7 * 86400):
        await _send(p, "📈 Снимки цен открытых рынков за 7 дней: [корзина, bid, ask, последняя, ликвидность].")
        sent += 1
    if not sent:
        await telegram_notify.notify("📤 В базе пока пусто — нечего выгружать.")
    return sent


def build_paths_zips(month: str, base: str) -> list[str]:
    """Пути цены событий месяца — zip-частями до ZIP_PART_LIMIT."""
    paths, zf, part, rows = [], None, 0, storage.paths_export(month)
    cols = ["event_id", "idx", "t0", "dts", "ps"]
    try:
        for i in range(0, len(rows), 500):
            chunk = []
            for eid, idx, js in rows[i:i + 500]:
                pts = json.loads(js)
                if not pts:
                    continue
                t0 = pts[0][0]
                dts = [pts[0][0] - t0] + [b[0] - a[0] for a, b in zip(pts, pts[1:])]
                chunk.append([eid, idx, t0, json.dumps(dts, separators=(",", ":")),
                              json.dumps([int(round(p * 10000)) for _, p in pts], separators=(",", ":"))])
            if not chunk:
                continue
            if zf is None:
                part += 1
                path = f"{base}_{part}.zip" if part > 1 else f"{base}.zip"
                zf = zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED)
                paths.append(path)
            zf.writestr(f"paths_{month}_{i // 500:04d}.csv", _csv_bytes(cols, chunk))
            if sum(x.compress_size for x in zf.infolist()) > ZIP_PART_LIMIT:
                zf.close()
                zf = None
    finally:
        if zf is not None:
            zf.close()
    return paths


async def send_paths() -> int:
    d = _dir()
    sent = 0
    for month in storage.months():
        files = await asyncio.to_thread(build_paths_zips, month, os.path.join(d, f"weather_paths_{month}"))
        for i, p in enumerate(files, 1):
            await _send(p, f"🗂 Пути цены YES корзин, {month}" + (f", часть {i} из {len(files)}" if len(files) > 1 else "")
                        + ". t0 — начало, dts — шаги в секундах, ps — цены ×10000.")
            sent += 1
    if not sent:
        await telegram_notify.notify("🗂 Путей цены в архиве пока нет.")
    return sent


def daily_text(now: float | None = None) -> str:
    now = time.time() if now is None else now
    from src import telegram_notify as tn
    st = live.paper_stats(int(now) - 86400)
    lines = ["🌡 Сводка за сутки", tn.archive_line(), tn.study_line(),
             f"🎯 Кандидатов за сутки: {st['n']}" + (f", с итогом {st['settled']}: выигр. {st['wins']}, {st['pnl']:+.2f} $"
                                                   if st["settled"] else "")]
    allt = live.paper_stats(0)
    if allt["settled"]:
        lines.append(f"Тестовые ставки за всё время: {allt['settled']} с итогом, выигр. {allt['wins']} "
                     f"({100 * allt['wins'] / allt['settled']:.0f}%), {allt['pnl']:+.2f} $"
                     + (f" ({100 * allt['pnl'] / allt['staked']:+.1f}% на ставку)" if allt["staked"] else ""))
        for w, x in live.paper_by_window(0):
            if x["settled"]:
                lines.append(f"  {w}: {x['settled']}, выигр. {x['wins']}, {x['pnl']:+.2f} $")
    rules = study.top_rules(5, cities=True) or study.top_rules(5)
    if rules:
        lines += ["", "Лучшие правила:"] + [f"• {study.rule_text(r, short=True)}" for r in rules]
    return "\n".join(lines)


async def report_loop() -> None:
    while True:
        now = time.time()
        day = int(now // 86400)
        due = (now % 86400) >= settings.REPORT_HOUR_UTC * 3600
        last = storage.get_setting(_LAST_DAILY_KEY)
        if last is None:                       # первый запуск: первая сводка — завтра
            storage.set_setting(_LAST_DAILY_KEY, day)
        elif due and int(float(last)) < day:
            storage.set_setting(_LAST_DAILY_KEY, day)
            try:
                await telegram_notify.notify(daily_text(now))
            except Exception as exc:  # noqa: BLE001
                log.warning("Сводка не ушла: %s", exc)
        await asyncio.sleep(300)
