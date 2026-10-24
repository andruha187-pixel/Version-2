"""
SQLite: 📚 события и корзины с историей цены (путь YES-токена, сжатый), снимки
цен открытых рынков, 🎯 кандидаты с тестовыми ставками и настройки из Telegram.
"""
from __future__ import annotations
import json
import os
import sqlite3
import time
import zlib
from contextlib import contextmanager

from config import settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT);

-- Температурное событие: город, макс/мин (kind high/low), дата (местные сутки
-- города), станция Weather Underground, объём и какая корзина выиграла.
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    slug TEXT, title TEXT, kind TEXT, city TEXT, city_key TEXT, date TEXT, unit TEXT,
    station TEXT, country TEXT, tz TEXT, tz_ok INTEGER, day_start_ts INTEGER,
    listed_ts INTEGER, end_ts INTEGER, closed_ts INTEGER,
    volume REAL, liquidity REAL, n_buckets INTEGER, winner TEXT, closed INTEGER,
    status TEXT,                 -- new — без истории цены, done — история скачана, error — не вышло
    error TEXT, updated_ts INTEGER
);
CREATE INDEX IF NOT EXISTS idx_events_date ON events(date);
CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);

-- Корзина (рынок Yes/No): границы в градусах (lo/hi, NULL — открытая сторона),
-- исход (1 — выиграл YES, 0 — NO) и путь цены YES-токена: zlib(JSON [[t, p], …]).
CREATE TABLE IF NOT EXISTS buckets (
    event_id INTEGER, idx INTEGER, title TEXT, lo REAL, hi REAL, cond_id TEXT,
    yes_token TEXT, no_token TEXT, outcome INTEGER, volume REAL,
    fidelity INTEGER, n_points INTEGER, path BLOB,
    PRIMARY KEY (event_id, idx)
);

-- Снимки цен открытых событий: [[idx, bid, ask, last, liquidity], …] — для будущих расчётов по стакану.
CREATE TABLE IF NOT EXISTS snapshots (
    event_id INTEGER, ts INTEGER, data TEXT,
    PRIMARY KEY (event_id, ts)
);

-- 🎯 Кандидат: цена стороны впервые дошла до уровня, а правило для города/времени
-- прошло проверку на архиве. Тестовая ставка stake по entry, итог — по исходу.
CREATE TABLE IF NOT EXISTS candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER, event_id INTEGER, idx INTEGER, side TEXT, level REAL, price REAL, entry REAL,
    hour REAL, rule TEXT, rule_n INTEGER, rule_wins INTEGER, rule_ev REAL, depth_usdc REAL,
    stake REAL, shares REAL, fee REAL, outcome INTEGER, pnl REAL, settled_ts INTEGER,
    UNIQUE (event_id, idx, side)
);
CREATE INDEX IF NOT EXISTS idx_candidates_ts ON candidates(ts);
"""


@contextmanager
def _conn():
    os.makedirs(os.path.dirname(settings.DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(settings.DB_PATH, timeout=30)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


DB_WAS_NEW = False


def init_db() -> None:
    global DB_WAS_NEW
    DB_WAS_NEW = not os.path.exists(settings.DB_PATH)
    with _conn() as conn:
        conn.executescript(_SCHEMA)


# ------------------------------------------------------------ настройки ----

def get_all_settings() -> dict[str, str]:
    with _conn() as conn:
        return dict(conn.execute("SELECT key, value FROM bot_settings").fetchall())


def get_setting(key: str) -> str | None:
    with _conn() as conn:
        row = conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_setting(key: str, value) -> None:
    with _conn() as conn:
        conn.execute("INSERT INTO bot_settings (key, value) VALUES (?, ?) "
                     "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, str(value)))


# ---------------------------------------------------------------- путь ----

def pack_path(points: list) -> bytes:
    """[[t, p], …] → zlib: время — разницами, цена — в десятитысячных."""
    if not points:
        return b""
    t0 = int(points[0][0])
    dts, ps, prev = [], [], t0
    for t, p in points:
        dts.append(int(t) - prev)
        prev = int(t)
        ps.append(int(round(float(p) * 10000)))
    return zlib.compress(json.dumps([t0, dts, ps], separators=(",", ":")).encode(), 6)


def unpack_path(blob) -> list[tuple[int, float]]:
    if not blob:
        return []
    t0, dts, ps = json.loads(zlib.decompress(blob))
    out, t = [], t0
    for dt, p in zip(dts, ps):
        t += dt
        out.append((t, p / 10000))
    return out


# --------------------------------------------------------------- события ----

EVENT_COLUMNS = ["id", "slug", "title", "kind", "city", "city_key", "date", "unit", "station", "country", "tz",
                 "tz_ok", "day_start_ts", "listed_ts", "end_ts", "closed_ts", "volume", "liquidity", "n_buckets",
                 "winner", "closed", "status", "error", "updated_ts"]
BUCKET_COLUMNS = ["event_id", "idx", "title", "lo", "hi", "cond_id", "yes_token", "no_token", "outcome", "volume",
                  "fidelity", "n_points"]


def event_upsert(ev: dict) -> bool:
    """Записать событие и его корзины (путь цены не трогаем). True — событие новое."""
    with _conn() as conn:
        row = conn.execute("SELECT status, closed FROM events WHERE id = ?", (ev["id"],)).fetchone()
        new = row is None
        status = "new" if new else row[0]
        if row is not None and not row[1] and ev.get("closed"):
            status = "new"                       # закрылось — нужна полная история
        vals = dict(ev, n_buckets=len(ev["buckets"]), tz_ok=int(bool(ev.get("tz_ok"))), closed=int(bool(ev.get("closed"))),
                    status=status, error=None, updated_ts=int(time.time()))
        cols = [c for c in EVENT_COLUMNS if c in vals]
        conn.execute(f"INSERT INTO events ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)}) "
                     f"ON CONFLICT(id) DO UPDATE SET {', '.join(f'{c} = excluded.{c}' for c in cols if c != 'id')}",
                     [vals[c] for c in cols])
        for b in ev["buckets"]:
            conn.execute(
                "INSERT INTO buckets (event_id, idx, title, lo, hi, cond_id, yes_token, no_token, outcome, volume) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(event_id, idx) DO UPDATE SET "
                "title = excluded.title, lo = excluded.lo, hi = excluded.hi, cond_id = excluded.cond_id, "
                "yes_token = excluded.yes_token, no_token = excluded.no_token, outcome = excluded.outcome, "
                "volume = excluded.volume",
                (ev["id"], b["idx"], b["title"], b["lo"], b["hi"], b["cond_id"], b["yes_token"], b["no_token"],
                 b["outcome"], b["volume"]))
    return new


def event_set_status(event_id: int, status: str, error: str | None = None) -> None:
    with _conn() as conn:
        conn.execute("UPDATE events SET status = ?, error = ?, updated_ts = ? WHERE id = ?",
                     (status, error, int(time.time()), event_id))


def bucket_set_path(event_id: int, idx: int, fidelity: int | None, points: list) -> None:
    with _conn() as conn:
        conn.execute("UPDATE buckets SET fidelity = ?, n_points = ?, path = ? WHERE event_id = ? AND idx = ?",
                     (fidelity, len(points), pack_path(points), event_id, idx))


def events(where: str = "", args: tuple = (), order: str = "date DESC, id DESC", limit: int | None = None) -> list[dict]:
    q = f"SELECT {', '.join(EVENT_COLUMNS)} FROM events" + (f" WHERE {where}" if where else "") + f" ORDER BY {order}"
    if limit:
        q += f" LIMIT {int(limit)}"
    with _conn() as conn:
        rows = conn.execute(q, args).fetchall()
    return [dict(zip(EVENT_COLUMNS, r)) for r in rows]


def event(event_id: int) -> dict | None:
    rows = events("id = ?", (event_id,))
    return rows[0] if rows else None


def buckets(event_id: int, with_path: bool = False) -> list[dict]:
    cols = BUCKET_COLUMNS + (["path"] if with_path else [])
    with _conn() as conn:
        rows = conn.execute(f"SELECT {', '.join(cols)} FROM buckets WHERE event_id = ? ORDER BY idx",
                            (event_id,)).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def iter_done_events(min_volume: float = 0.0):
    """Закрытые события с историей цены — по одному, с корзинами и путями (для расчёта порогов)."""
    ids = [r[0] for r in _query("SELECT id FROM events WHERE status = 'done' AND closed = 1 AND COALESCE(volume, 0) >= ? "
                                "ORDER BY date, id", (min_volume,))]
    for eid in ids:
        ev = event(eid)
        if ev:
            yield ev, buckets(eid, with_path=True)


def _query(q: str, args: tuple = ()) -> list[tuple]:
    with _conn() as conn:
        return conn.execute(q, args).fetchall()


def archive_counts() -> dict:
    """Сколько событий в архиве: всего, по статусам, по типам, городов, даты."""
    total = _query("SELECT COUNT(*), COUNT(DISTINCT city_key), MIN(date), MAX(date) FROM events WHERE closed = 1")[0]
    by_status = dict(_query("SELECT status, COUNT(*) FROM events WHERE closed = 1 GROUP BY status"))
    by_kind = dict(_query("SELECT kind, COUNT(*) FROM events WHERE closed = 1 AND status = 'done' GROUP BY kind"))
    fid = dict(_query("SELECT fidelity, COUNT(*) FROM buckets WHERE fidelity IS NOT NULL GROUP BY fidelity"))
    no_path = _query("SELECT COUNT(*) FROM buckets b JOIN events e ON e.id = b.event_id "
                     "WHERE e.status = 'done' AND (b.n_points IS NULL OR b.n_points = 0)")[0][0]
    open_n = _query("SELECT COUNT(*) FROM events WHERE closed = 0")[0][0]
    return {"events": total[0] or 0, "cities": total[1] or 0, "first": total[2], "last": total[3],
            "status": by_status, "kinds": by_kind, "fidelity": fid, "no_path": no_path, "open": open_n}


def city_stats(min_volume: float = 0.0) -> list[dict]:
    """По городам: событий (макс/мин), средний объём, станция, пояс."""
    rows = _query("SELECT city_key, MAX(city), kind, COUNT(*), AVG(volume), MAX(station), MAX(tz), MIN(tz_ok) "
                  "FROM events WHERE closed = 1 AND status = 'done' GROUP BY city_key, kind")
    out: dict[str, dict] = {}
    for key, city, kind, n, vol, station, tz, tz_ok in rows:
        d = out.setdefault(key, {"city_key": key, "city": city, "high": 0, "low": 0, "vol_high": 0.0, "vol_low": 0.0,
                                 "station": station, "tz": tz, "tz_ok": bool(tz_ok)})
        d[kind] = n
        d[f"vol_{kind}"] = vol or 0.0
    big = {k for k, in _query("SELECT DISTINCT city_key FROM events WHERE closed = 1 AND status = 'done' "
                              "AND COALESCE(volume, 0) >= ?", (min_volume,))}
    for d in out.values():
        d["in_study"] = d["city_key"] in big
    return sorted(out.values(), key=lambda d: -(d["vol_high"] * d["high"] + d["vol_low"] * d["low"]))


def events_export(since_date: str = "") -> list[tuple]:
    return _query("SELECT e.id, e.date, e.city, e.kind, e.unit, e.station, e.tz, e.volume, e.winner, e.slug, "
                  "b.idx, b.title, b.lo, b.hi, b.outcome, b.volume, b.fidelity, b.n_points "
                  "FROM events e JOIN buckets b ON b.event_id = e.id WHERE e.closed = 1 AND e.date >= ? "
                  "ORDER BY e.date, e.city_key, e.kind, b.idx", (since_date,))


EVENTS_EXPORT_COLUMNS = ["event_id", "date", "city", "kind", "unit", "station", "tz", "event_volume", "winner", "slug",
                         "idx", "bucket", "lo", "hi", "outcome", "bucket_volume", "fidelity_min", "n_points"]


def paths_export(month: str) -> list[tuple]:
    """Пути цены событий месяца «2026-09»: (event_id, idx, JSON [[t, p], …])."""
    rows = _query("SELECT b.event_id, b.idx, b.path FROM buckets b JOIN events e ON e.id = b.event_id "
                  "WHERE e.closed = 1 AND e.status = 'done' AND substr(e.date, 1, 7) = ? ORDER BY b.event_id, b.idx",
                  (month,))
    return [(eid, idx, json.dumps([[t, round(p, 4)] for t, p in unpack_path(blob)], separators=(",", ":")))
            for eid, idx, blob in rows if blob]


def months() -> list[str]:
    return [r[0] for r in _query("SELECT DISTINCT substr(date, 1, 7) FROM events WHERE closed = 1 AND status = 'done' "
                                 "ORDER BY 1")]


# --------------------------------------------------------------- снимки ----

def snapshot_add(event_id: int, ts: int, data: list) -> None:
    with _conn() as conn:
        conn.execute("INSERT OR REPLACE INTO snapshots (event_id, ts, data) VALUES (?, ?, ?)",
                     (event_id, int(ts), json.dumps(data, separators=(",", ":"))))


def snapshots_count() -> int:
    return _query("SELECT COUNT(*) FROM snapshots")[0][0]


def snapshots_export(since_ts: int) -> list[tuple]:
    return _query("SELECT s.event_id, e.city, e.kind, e.date, s.ts, s.data FROM snapshots s "
                  "LEFT JOIN events e ON e.id = s.event_id WHERE s.ts >= ? ORDER BY s.ts", (since_ts,))


# ------------------------------------------------------------ кандидаты ----

CANDIDATE_COLUMNS = ["id", "ts", "event_id", "idx", "side", "level", "price", "entry", "hour", "rule", "rule_n",
                     "rule_wins", "rule_ev", "depth_usdc", "stake", "shares", "fee", "outcome", "pnl", "settled_ts"]


def candidate_add(**f) -> int | None:
    cols = [c for c in CANDIDATE_COLUMNS if c in f and c != "id"]
    with _conn() as conn:
        cur = conn.execute(f"INSERT OR IGNORE INTO candidates ({', '.join(cols)}) "
                           f"VALUES ({', '.join('?' for _ in cols)})", [f[c] for c in cols])
        return cur.lastrowid if cur.rowcount else None


def candidates(where: str = "", args: tuple = (), limit: int | None = None) -> list[dict]:
    q = f"SELECT {', '.join('c.' + c for c in CANDIDATE_COLUMNS)}, e.city, e.kind, e.date, e.unit, b.title " \
        "FROM candidates c LEFT JOIN events e ON e.id = c.event_id " \
        "LEFT JOIN buckets b ON b.event_id = c.event_id AND b.idx = c.idx" + (f" WHERE {where}" if where else "") + \
        " ORDER BY c.ts DESC"
    if limit:
        q += f" LIMIT {int(limit)}"
    rows = _query(q, args)
    return [dict(zip(CANDIDATE_COLUMNS + ["city", "kind", "date", "unit", "bucket"], r)) for r in rows]


def candidate_settle(cid: int, outcome: int, pnl: float) -> bool:
    with _conn() as conn:
        cur = conn.execute("UPDATE candidates SET outcome = ?, pnl = ?, settled_ts = ? WHERE id = ? AND outcome IS NULL",
                           (outcome, pnl, int(time.time()), cid))
        return cur.rowcount > 0
