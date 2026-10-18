"""
SQLite-хранилище бота серий: окна рынков (series_windows), ставки стратегии
(series_trades), виртуальные ставки вариантов входа (series_variants) и
настройки из Telegram (bot_settings). Внешней БД не нужно.
"""
from __future__ import annotations
import os
import sqlite3
import time
from contextlib import contextmanager

from config import settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- 🔁 Запись окон (src/series_bot.py): одна строка — одно окно рынка 15m.
-- Цены сторон на старте и путь ask каждые несколько секунд, исход: ранний
-- (стакан в последние секунды / Chainlink) и официальный. seeded=1 — окно
-- до запуска бота: известен только официальный исход (для подсчёта серии).
CREATE TABLE IF NOT EXISTS series_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    interval TEXT NOT NULL,
    start_ts INTEGER NOT NULL,
    slug TEXT,
    seeded INTEGER DEFAULT 0,
    t_first REAL,
    up_ask0 REAL,
    down_ask0 REAL,
    up_bid0 REAL,
    down_bid0 REAL,
    up_min REAL,
    up_min_t REAL,
    down_min REAL,
    down_min_t REAL,
    path TEXT,
    cl_start REAL,
    cl_end REAL,
    early_outcome TEXT,
    outcome TEXT,
    depth TEXT,
    UNIQUE(asset, interval, start_ts)
);
CREATE INDEX IF NOT EXISTS idx_series_windows_start ON series_windows(start_ts);

-- 🎲 Варианты входа (с 16.10, v8): виртуальные ставки по тем же сигналам, что и
-- стратегия, но по другим правилам входа — чтобы сравнить правила на одних окнах.
CREATE TABLE IF NOT EXISTS series_variants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    asset TEXT,
    interval TEXT,
    start_ts INTEGER,
    slug TEXT,
    variant TEXT,
    streak_dir TEXT,
    k INTEGER,
    side TEXT,
    ask REAL,
    price REAL,
    size_usdc REAL,
    shares REAL,
    fee_usdc REAL,
    sec_after_start REAL,
    status TEXT,
    outcome TEXT,
    pnl_usdc REAL,
    settled_ts INTEGER,
    UNIQUE(asset, interval, start_ts, variant)
);
CREATE INDEX IF NOT EXISTS idx_series_variants_ts ON series_variants(ts);

-- 🎲 Стратегия серий (src/series_bot.py): ставка на разворот после серии.
-- Тестовые (dry_run=1) и реальные (dry_run=0) — отдельными строками, по одной на окно.
CREATE TABLE IF NOT EXISTS series_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    asset TEXT,
    interval TEXT,
    start_ts INTEGER,
    slug TEXT,
    streak_dir TEXT,
    k INTEGER,
    side TEXT,
    token_id TEXT,
    ask REAL,
    price REAL,
    size_usdc REAL,
    shares REAL,
    fee_usdc REAL,
    sec_after_start REAL,
    dry_run INTEGER,
    status TEXT,
    order_id TEXT,
    outcome TEXT,
    pnl_usdc REAL,
    settled_ts INTEGER,
    UNIQUE(asset, interval, start_ts, dry_run)
);
CREATE INDEX IF NOT EXISTS idx_series_trades_ts ON series_trades(ts);
"""


@contextmanager
def _conn():
    os.makedirs(os.path.dirname(settings.DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(settings.DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# True, если при этом запуске файла базы ещё не было (создана с нуля). При
# каждом деплое без постоянного тома на /app/data так и будет — тогда
# настройки из Telegram и статистика сбрасываются; main.py предупреждает.
DB_WAS_NEW = False


def init_db():
    global DB_WAS_NEW
    DB_WAS_NEW = not os.path.exists(settings.DB_PATH)
    with _conn() as conn:
        conn.executescript(_SCHEMA)
        existing_series = {row[1] for row in conn.execute("PRAGMA table_info(series_windows)")}
        for col, sql_type in _SERIES_MIGRATIONS:
            if col not in existing_series:
                conn.execute(f"ALTER TABLE series_windows ADD COLUMN {col} {sql_type}")


def get_all_settings() -> dict[str, str]:
    with _conn() as conn:
        cur = conn.execute("SELECT key, value FROM bot_settings")
        return {k: v for k, v in cur.fetchall()}


def get_setting(key: str) -> str | None:
    with _conn() as conn:
        row = conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_setting(key: str, value) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT INTO bot_settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )


SERIES_COLUMNS = [
    "id", "asset", "interval", "start_ts", "slug", "seeded", "t_first", "up_ask0", "down_ask0", "up_bid0",
    "down_bid0", "up_min", "up_min_t", "down_min", "down_min_t", "path", "cl_start", "cl_end", "early_outcome",
    "outcome", "depth",
]
_SERIES_FIELDS = set(SERIES_COLUMNS) - {"id", "asset", "interval", "start_ts"}
_SERIES_HEAVY = ("path", "depth")          # JSON-колонки: в сводки не тянем
# Колонки series_windows, добавленные после первой версии теста (v8, 16.10)
_SERIES_MIGRATIONS = [("depth", "TEXT")]


def series_upsert(asset: str, interval: str, start_ts: int, **fields) -> None:
    """Окно теста серий: создать, если нет, и обновить переданные поля."""
    bad = set(fields) - _SERIES_FIELDS
    if bad:
        raise ValueError(f"неизвестные поля: {bad}")
    with _conn() as conn:
        conn.execute("INSERT OR IGNORE INTO series_windows (asset, interval, start_ts) VALUES (?, ?, ?)",
                     (asset, interval, int(start_ts)))
        if fields:
            cols = sorted(fields)
            conn.execute(f"UPDATE series_windows SET {', '.join(f'{c} = ?' for c in cols)} "
                         "WHERE asset = ? AND interval = ? AND start_ts = ?",
                         [fields[c] for c in cols] + [asset, interval, int(start_ts)])


def series_rows(interval: str | None = None, since_ts: int = 0, with_path: bool = False) -> list[dict]:
    cols = [c for c in SERIES_COLUMNS if with_path or c not in _SERIES_HEAVY]
    q = f"SELECT {', '.join(cols)} FROM series_windows WHERE start_ts >= ?"
    args: list = [since_ts]
    if interval:
        q += " AND interval = ?"
        args.append(interval)
    q += " ORDER BY asset, interval, start_ts"
    with _conn() as conn:
        return [dict(zip(cols, r)) for r in conn.execute(q, args).fetchall()]


def series_seed(asset: str, interval: str, start_ts: int, slug: str, outcome: str) -> None:
    """Окно до запуска бота: только официальный исход (цены не трогаем, если окно уже есть)."""
    with _conn() as conn:
        conn.execute("INSERT OR IGNORE INTO series_windows (asset, interval, start_ts, slug, seeded) "
                     "VALUES (?, ?, ?, ?, 1)", (asset, interval, int(start_ts), slug))
        conn.execute("UPDATE series_windows SET outcome = ? WHERE asset = ? AND interval = ? AND start_ts = ? "
                     "AND outcome IS NULL", (outcome, asset, interval, int(start_ts)))


def series_outcome(asset: str, interval: str, start_ts: int) -> str | None:
    """Официальный исход окна из series_windows (None — ещё нет)."""
    with _conn() as conn:
        row = conn.execute("SELECT outcome FROM series_windows WHERE asset = ? AND interval = ? AND start_ts = ?",
                           (asset, interval, int(start_ts))).fetchone()
    return row[0] if row and row[0] else None


def series_trim(before_ts: int) -> int:
    """Стереть путь цены и снимки стакана у окон старше before_ts (цены старта и
    исходы остаются — по ним считаются серии и сводки)."""
    with _conn() as conn:
        return conn.execute("UPDATE series_windows SET path = NULL, depth = NULL WHERE start_ts < ? "
                            "AND (path IS NOT NULL OR depth IS NOT NULL)", (int(before_ts),)).rowcount


def series_unresolved(ended_before: int, since_ts: int = 0, limit: int = 20) -> list[tuple[str, str, int, str]]:
    """Окна без официального исхода, закончившиеся раньше ended_before: (монета, интервал, старт, слаг)."""
    with _conn() as conn:
        cur = conn.execute(
            "SELECT asset, interval, start_ts, slug FROM series_windows WHERE outcome IS NULL AND slug IS NOT NULL "
            "AND start_ts >= ? AND start_ts + CASE interval WHEN '15m' THEN 900 ELSE 300 END < ? "
            "ORDER BY start_ts DESC LIMIT ?",
            (since_ts, ended_before, limit))
        return cur.fetchall()


SERIES_TRADE_COLUMNS = [
    "id", "ts", "asset", "interval", "start_ts", "slug", "streak_dir", "k", "side", "token_id", "ask", "price",
    "size_usdc", "shares", "fee_usdc", "sec_after_start", "dry_run", "status", "order_id", "outcome", "pnl_usdc",
    "settled_ts",
]


def series_trade_log(**f) -> int | None:
    """Записать ставку стратегии серий. None — по этому окну ставка уже есть."""
    cols = [c for c in SERIES_TRADE_COLUMNS if c in f and c != "id"]
    with _conn() as conn:
        cur = conn.execute(f"INSERT OR IGNORE INTO series_trades ({', '.join(cols)}) "
                           f"VALUES ({', '.join('?' for _ in cols)})", [f[c] for c in cols])
        return cur.lastrowid if cur.rowcount else None


def series_trade_exists(asset: str, interval: str, start_ts: int, dry_run: bool | None = None) -> bool:
    """Есть ли строка стратегии по окну (dry_run=None — любая, иначе только тестовая/реальная)."""
    q = "SELECT 1 FROM series_trades WHERE asset = ? AND interval = ? AND start_ts = ?"
    args: list = [asset, interval, int(start_ts)]
    if dry_run is not None:
        q += " AND dry_run = ?"
        args.append(int(bool(dry_run)))
    with _conn() as conn:
        return conn.execute(q + " LIMIT 1", args).fetchone() is not None


def series_trades_open(ended_before: int) -> list[dict]:
    cols = SERIES_TRADE_COLUMNS
    with _conn() as conn:
        rows = conn.execute(
            f"SELECT {', '.join(cols)} FROM series_trades WHERE outcome IS NULL "
            "AND start_ts + CASE interval WHEN '15m' THEN 900 ELSE 300 END < ? ORDER BY start_ts",
            (ended_before,)).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def series_trade_settle(trade_id: int, outcome: str, pnl_usdc: float) -> bool:
    with _conn() as conn:
        cur = conn.execute("UPDATE series_trades SET outcome = ?, pnl_usdc = ?, settled_ts = ? "
                           "WHERE id = ? AND outcome IS NULL", (outcome, pnl_usdc, int(time.time()), trade_id))
        return cur.rowcount > 0


def series_trades_since(since_ts: int) -> list[dict]:
    cols = SERIES_TRADE_COLUMNS
    with _conn() as conn:
        rows = conn.execute(f"SELECT {', '.join(cols)} FROM series_trades WHERE ts >= ? ORDER BY ts",
                            (since_ts,)).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def series_trades_export(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        return conn.execute(f"SELECT {', '.join(SERIES_TRADE_COLUMNS)} FROM series_trades WHERE ts >= ? ORDER BY ts",
                            (since_ts,)).fetchall()


def series_export(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        return conn.execute(f"SELECT {', '.join(SERIES_COLUMNS)} FROM series_windows WHERE start_ts >= ? "
                            "ORDER BY start_ts, asset, interval", (since_ts,)).fetchall()


SERIES_VARIANT_COLUMNS = [
    "id", "ts", "asset", "interval", "start_ts", "slug", "variant", "streak_dir", "k", "side", "ask", "price",
    "size_usdc", "shares", "fee_usdc", "sec_after_start", "status", "outcome", "pnl_usdc", "settled_ts",
]


def series_variant_log(**f) -> int | None:
    """Записать виртуальную ставку варианта входа. None — по этому окну вариант уже записан."""
    cols = [c for c in SERIES_VARIANT_COLUMNS if c in f and c != "id"]
    with _conn() as conn:
        cur = conn.execute(f"INSERT OR IGNORE INTO series_variants ({', '.join(cols)}) "
                           f"VALUES ({', '.join('?' for _ in cols)})", [f[c] for c in cols])
        return cur.lastrowid if cur.rowcount else None


def series_variants_open(ended_before: int) -> list[dict]:
    cols = SERIES_VARIANT_COLUMNS
    with _conn() as conn:
        rows = conn.execute(
            f"SELECT {', '.join(cols)} FROM series_variants WHERE outcome IS NULL "
            "AND start_ts + CASE interval WHEN '15m' THEN 900 ELSE 300 END < ? ORDER BY start_ts",
            (ended_before,)).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def series_variant_settle(row_id: int, outcome: str, pnl_usdc: float) -> bool:
    with _conn() as conn:
        cur = conn.execute("UPDATE series_variants SET outcome = ?, pnl_usdc = ?, settled_ts = ? "
                           "WHERE id = ? AND outcome IS NULL", (outcome, pnl_usdc, int(time.time()), row_id))
        return cur.rowcount > 0


def series_variants_since(since_ts: int) -> list[dict]:
    cols = SERIES_VARIANT_COLUMNS
    with _conn() as conn:
        rows = conn.execute(f"SELECT {', '.join(cols)} FROM series_variants WHERE ts >= ? ORDER BY ts",
                            (since_ts,)).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def series_variants_export(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        return conn.execute(f"SELECT {', '.join(SERIES_VARIANT_COLUMNS)} FROM series_variants WHERE ts >= ? "
                            "ORDER BY ts, id", (since_ts,)).fetchall()


def series_variants_first_ts() -> int | None:
    """Время первой записи вариантов — с него сравниваем их с основным правилом."""
    with _conn() as conn:
        row = conn.execute("SELECT MIN(ts) FROM series_variants").fetchone()
    return int(row[0]) if row and row[0] is not None else None
