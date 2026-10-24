"""
📊 Пороги: «цена стороны впервые дошла до X — выиграла ли эта сторона».

По каждой корзине закрытого события и каждой стороне (YES — цена корзины,
NO — 1 − цена) и каждому уровню X из STUDY_LEVELS:
  - касание — первая точка истории с ценой ≥ X после того, как цена была ниже X
    (рынок, который с самого начала стоял выше X, касанием не считается);
  - вход — цена касания + STUDY_SLIP (покупаем по ask, он выше середины);
    цена перепрыгнула X больше чем на STUDY_MAX_JUMP — по уровню было не
    войти, такую ставку не считаем;
  - итог на $1: выигрыш 1/вход − 1, проигрыш −1, минус комиссия тейкера
    FEE_RATE × (1 − вход) (так Polymarket берёт с погодных рынков);
  - время — местное в городе, часов от полуночи дня рынка: «накануне» (< 0),
    0–8, 8–12, 12–16, 16–20, 20–24, «после дня» (≥ 24 — день кончился, ждём данные).

Складываем по ячейкам (город или все города) × (макс/мин) × сторона × уровень ×
(окно времени или любое). Правило «проверено», если:
  - всего ставок не меньше RULE_MIN_N;
  - на старой части архива (первые 60% дней) выигрышей ЗНАЧИМО больше, чем нужно
    для окупаемости: вероятность получить столько выигрышей случайно, если рынок
    оценивает верно (выигрывает с вероятностью окупаемости), не больше RULE_MAX_P;
  - на новой части (оставшиеся 40%) правило в плюсе;
  - в каждой части не меньше RULE_MIN_PART_N ставок.
Зачем так строго: ячеек тысячи. При честной цене 0.91 серия «20 из 20» выпадает
в 15% случаев — то есть среди тысяч ячеек сотни «100%» появятся просто по везению.
Цена 0.92 окупается от ~92.4% выигрышей, и «все выигрыши» доказывают перевес
только с ~40 ставок; один проигрыш на 70 ставок по 0.92 — ещё перевес, два — уже нет.

События с объёмом меньше MIN_EVENT_VOLUME не берём: в тонких рынках цена
мало что значит. Внутри одного события корзины связаны (выигрывает одна) —
поэтому рядом со ставками пишем, из скольких событий они.
"""
from __future__ import annotations
import csv
import json
import logging
import math
import os
import threading
import time

import numpy as np

from config import settings
from src import runtime_state, storage, text

log = logging.getLogger("study")

WINDOWS = (("накануне", -1e9, 0.0), ("0–8 ч", 0.0, 8.0), ("8–12 ч", 8.0, 12.0), ("12–16 ч", 12.0, 16.0),
           ("16–20 ч", 16.0, 20.0), ("20–24 ч", 20.0, 24.0), ("после дня", 24.0, 1e9))
WINDOW_NAMES = [w[0] for w in WINDOWS]
ANY = "любое"
ALL = "*"
SPLIT_SHARE = 0.6                    # старая часть архива — первые 60% дат
Z90 = 1.2816                         # односторонняя нижняя граница 90%

BET_COLUMNS = ["event_id", "date", "city", "kind", "unit", "idx", "bucket", "side", "level", "ts", "hour", "window",
               "price", "entry", "win", "ret", "event_volume", "part"]
CELL_COLUMNS = ["city", "city_key", "kind", "side", "level", "window", "n", "events", "wins", "win_pct", "avg_entry",
                "breakeven_pct", "ev_pct", "lb90_pct", "n_old", "wins_old", "ev_old_pct", "p_old", "n_new", "wins_new",
                "ev_new_pct", "per_day", "passed"]

_lock = threading.Lock()
_state = {"summary": None, "rules": None, "cells": None, "running": False}


def window_of(hour: float) -> str:
    for name, a, b in WINDOWS:
        if a <= hour < b:
            return name
    return WINDOWS[-1][0]


def fee_per_usd(entry: float) -> float:
    return float(settings.FEE_RATE) * (1.0 - entry)


def bet_return(entry: float, win: bool) -> float:
    """Итог на $1 ставки с комиссией тейкера."""
    return (1.0 / entry - 1.0 if win else -1.0) - fee_per_usd(entry)


def breakeven(entry: float) -> float:
    """Доля выигрышей, при которой ставка по entry в ноль."""
    return entry * (1.0 + fee_per_usd(entry))


def wilson_lb(wins: int, n: int, z: float = Z90) -> float:
    if n <= 0:
        return 0.0
    p = wins / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    r = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, (c - r) / d)


def binom_tail(k: int, n: int, p: float) -> float:
    """P(X ≥ k) при X ~ Bin(n, p): насколько вероятно получить k и больше выигрышей случайно."""
    if k <= 0:
        return 1.0
    if k > n:
        return 0.0
    p = min(max(p, 1e-12), 1 - 1e-12)
    lp, lq = math.log(p), math.log1p(-p)
    lg = math.lgamma
    terms = [lg(n + 1) - lg(i + 1) - lg(n - i + 1) + i * lp + (n - i) * lq for i in range(k, n + 1)]
    m = max(terms)
    return min(1.0, math.exp(m) * sum(math.exp(t - m) for t in terms))


def touches(vals: np.ndarray, levels: np.ndarray) -> list[tuple[int, int]]:
    """[(номер уровня, номер точки касания)]: первая точка ≥ уровня после того, как цена была ниже."""
    out = []
    if vals.size < 2:
        return out
    below = vals[None, :] < (levels[:, None] - 1e-9)
    for li in range(levels.size):
        b = below[li]
        if not b.any():
            continue
        j = int(np.argmax(b))
        rest = ~b[j + 1:]
        if not rest.any():
            continue
        out.append((li, j + 1 + int(np.argmax(rest))))
    return out


def event_bets(ev: dict, bks: list[dict], levels=None, slip=None, max_jump=None) -> tuple[list[dict], int]:
    """Расчётные ставки по событию: (ставки, сколько касаний «перепрыгнули» уровень)."""
    levels = np.array(levels if levels is not None else settings.STUDY_LEVELS, dtype=float)
    slip = float(settings.STUDY_SLIP if slip is None else slip)
    max_jump = float(settings.STUDY_MAX_JUMP if max_jump is None else max_jump)
    out, jumped = [], 0
    for b in bks:
        if b.get("outcome") not in (0, 1) or not b.get("path"):
            continue
        pts = storage.unpack_path(b["path"])
        if len(pts) < 2:
            continue
        ts = np.array([t for t, _ in pts], dtype=np.int64)
        yes = np.array([p for _, p in pts], dtype=float)
        for side, vals in (("YES", yes), ("NO", 1.0 - yes)):
            win = (b["outcome"] == 1) == (side == "YES")
            for li, i in touches(vals, levels):
                level, price = float(levels[li]), float(vals[i])
                if price > level + max_jump + 1e-9:
                    jumped += 1
                    continue
                entry = min(0.999, round(price + slip, 4))
                hour = (int(ts[i]) - int(ev["day_start_ts"])) / 3600.0
                out.append({"event_id": ev["id"], "date": ev["date"], "city": ev["city"], "city_key": ev["city_key"],
                            "kind": ev["kind"], "unit": ev["unit"], "idx": b["idx"], "bucket": b["title"],
                            "side": side, "level": level, "ts": int(ts[i]), "hour": round(hour, 2),
                            "window": window_of(hour), "price": round(price, 4), "entry": entry, "win": int(win),
                            "ret": round(bet_return(entry, win), 5), "event_volume": ev.get("volume")})
    return out, jumped


class Cell:
    __slots__ = ("n", "wins", "s_entry", "s_ret", "s_be", "events", "n_old", "w_old", "r_old", "be_old", "n_new",
                 "w_new", "r_new", "first", "last")

    def __init__(self):
        self.n = self.wins = self.n_old = self.w_old = self.n_new = self.w_new = 0
        self.s_entry = self.s_ret = self.s_be = self.r_old = self.r_new = self.be_old = 0.0
        self.events = set()
        self.first = self.last = None

    def add(self, bet: dict, old: bool) -> None:
        self.n += 1
        self.wins += bet["win"]
        self.s_entry += bet["entry"]
        self.s_ret += bet["ret"]
        self.s_be += breakeven(bet["entry"])
        self.events.add(bet["event_id"])
        if old:
            self.n_old += 1
            self.w_old += bet["win"]
            self.r_old += bet["ret"]
            self.be_old += breakeven(bet["entry"])
        else:
            self.n_new += 1
            self.w_new += bet["win"]
            self.r_new += bet["ret"]
        d = bet["date"]
        self.first = d if self.first is None or d < self.first else self.first
        self.last = d if self.last is None or d > self.last else self.last


def _days(a: str | None, b: str | None) -> float:
    if not a or not b:
        return 1.0
    try:
        return max(1.0, (time.mktime(time.strptime(b, "%Y-%m-%d")) - time.mktime(time.strptime(a, "%Y-%m-%d")))
                   / 86400 + 1)
    except ValueError:
        return 1.0


def cell_row(key: tuple, c: Cell, names: dict, span_days: float, min_n: int, min_part: int,
             max_p: float | None = None) -> dict:
    city_key, kind, side, level, window = key
    max_p = float(settings.RULE_MAX_P if max_p is None else max_p)
    ev = c.s_ret / c.n if c.n else 0.0
    ev_old = c.r_old / c.n_old if c.n_old else None
    ev_new = c.r_new / c.n_new if c.n_new else None
    p_old = binom_tail(c.w_old, c.n_old, c.be_old / c.n_old) if c.n_old else None
    passed = (c.n >= min_n and c.n_old >= min_part and c.n_new >= min_part and ev > 0
              and p_old is not None and p_old <= max_p and ev_old > 0 and ev_new is not None and ev_new > 0)
    return {"city": names.get(city_key, "все города") if city_key != ALL else "все города", "city_key": city_key,
            "kind": kind, "side": side, "level": level, "window": window, "n": c.n, "events": len(c.events),
            "wins": c.wins, "win_pct": round(100 * c.wins / c.n, 2) if c.n else 0.0,
            "avg_entry": round(c.s_entry / c.n, 4) if c.n else None,
            "breakeven_pct": round(100 * c.s_be / c.n, 2) if c.n else None,
            "ev_pct": round(100 * ev, 2), "lb90_pct": round(100 * wilson_lb(c.wins, c.n), 2),
            "n_old": c.n_old, "wins_old": c.w_old, "ev_old_pct": round(100 * ev_old, 2) if ev_old is not None else None,
            "p_old": round(p_old, 4) if p_old is not None else None,
            "n_new": c.n_new, "wins_new": c.w_new, "ev_new_pct": round(100 * ev_new, 2) if ev_new is not None else None,
            "per_day": round(c.n / span_days, 3), "passed": int(passed)}


def _split_date(dates: list[str]) -> str:
    ds = sorted(set(dates))
    if not ds:
        return ""
    return ds[min(len(ds) - 1, int(len(ds) * SPLIT_SHARE))]


def compute(min_volume: float | None = None, bets_path: str | None = None) -> dict:
    """Пройти архив: ячейки, правила, сводка. bets_path — записать все расчётные ставки в CSV."""
    min_volume = float(runtime_state.get("min_event_volume") if min_volume is None else min_volume)
    min_n, min_part = int(runtime_state.get("rule_min_n")), int(settings.RULE_MIN_PART_N)
    evs = storage.events("status = 'done' AND closed = 1 AND COALESCE(volume, 0) >= ?", (min_volume,), order="date, id")
    split = _split_date([e["date"] for e in evs])
    cells: dict[tuple, Cell] = {}
    names: dict[str, str] = {}
    n_bets = jumped = n_events = 0
    first = last = None
    f = w = None
    if bets_path:
        os.makedirs(os.path.dirname(bets_path) or ".", exist_ok=True)
        f = open(bets_path, "w", newline="", encoding="utf-8")
        w = csv.writer(f)
        w.writerow(BET_COLUMNS)
    try:
        for ev in evs:
            bets, j = event_bets(ev, storage.buckets(ev["id"], with_path=True))
            jumped += j
            n_events += 1
            names[ev["city_key"]] = ev["city"]
            first = ev["date"] if first is None else min(first, ev["date"])
            last = ev["date"] if last is None else max(last, ev["date"])
            for bet in bets:
                old = bet["date"] < split
                bet["part"] = "old" if old else "new"
                n_bets += 1
                for ck in (bet["city_key"], ALL):
                    for win in (bet["window"], ANY):
                        key = (ck, bet["kind"], bet["side"], bet["level"], win)
                        c = cells.get(key)
                        if c is None:
                            c = cells[key] = Cell()
                        c.add(bet, old)
                if w is not None:
                    w.writerow([bet[c] for c in BET_COLUMNS])
    finally:
        if f is not None:
            f.close()
    span = _days(first, last)
    rows = [cell_row(k, c, names, span, min_n, min_part) for k, c in cells.items()]
    rows.sort(key=lambda r: (r["city_key"] != ALL, r["city"], r["kind"], r["side"], r["level"],
                             WINDOW_NAMES.index(r["window"]) if r["window"] in WINDOW_NAMES else -1))
    rules = [r for r in rows if r["passed"]]
    rules.sort(key=lambda r: -(r["ev_pct"] * r["per_day"]))
    summary = {"computed": int(time.time()), "events": n_events, "bets": n_bets, "jumped": jumped,
               "rules": len(rules), "first": first, "last": last, "split": split, "min_volume": min_volume,
               "min_n": min_n, "min_part": min_part, "max_p": float(settings.RULE_MAX_P), "cells": len(rows),
               "cities": len(names), "levels": list(settings.STUDY_LEVELS),
               "slip": float(settings.STUDY_SLIP), "fee_rate": float(settings.FEE_RATE)}
    return {"summary": summary, "cells": rows, "rules": rules}


def run(bets_path: str | None = None) -> dict:
    """Пересчитать и запомнить (вызывается в отдельном потоке). Возвращает сводку."""
    with _lock:
        _state["running"] = True
        try:
            res = compute(bets_path=bets_path)
        finally:
            _state["running"] = False
        _state.update(summary=res["summary"], cells=res["cells"], rules=res["rules"])
    storage.set_setting("study_summary", json.dumps(res["summary"]))
    storage.set_setting("study_rules", json.dumps(res["rules"], ensure_ascii=False))
    return res["summary"]


def is_running() -> bool:
    return bool(_state["running"])


def summary() -> dict | None:
    if _state["summary"] is None:
        raw = storage.get_setting("study_summary")
        if raw:
            try:
                _state["summary"] = json.loads(raw)
            except ValueError:
                pass
    return _state["summary"]


def rules() -> list[dict]:
    if _state["rules"] is None:
        raw = storage.get_setting("study_rules")
        try:
            _state["rules"] = json.loads(raw) if raw else []
        except ValueError:
            _state["rules"] = []
    return _state["rules"]


def cells() -> list[dict] | None:
    return _state["cells"]


def top_rules(limit: int = 10, cities: bool | None = None) -> list[dict]:
    """Лучшие проверенные правила без повторов: одни и те же ставки часто попадают в несколько
    ячеек (окно и «любое время», соседние уровни) — показываем одну. cities: True — только
    города, False — только «все города», None — всё."""
    out, seen = [], set()
    for r in rules():
        if cities is not None and (r["city_key"] != ALL) != cities:
            continue
        key = (r["city_key"], r["kind"], r["side"], r["n"], r["wins"], round(r["avg_entry"] or 0, 3))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
        if len(out) >= limit:
            break
    return out


def reset() -> None:
    _state.update(summary=None, rules=None, cells=None, running=False)


# ----------------------------------------------------------------- правила ----

def find_rule(city_key: str, kind: str, side: str, level: float, window: str, city_only: bool = False) -> dict | None:
    """Проверенное правило для касания: сначала город и окно времени, потом город в любое время,
    потом все города (если не city_only)."""
    idx = {}
    for r in rules():
        idx[(r["city_key"], r["kind"], r["side"], round(r["level"], 4), r["window"])] = r
    keys = [(city_key, window), (city_key, ANY)] + ([] if city_only else [(ALL, window), (ALL, ANY)])
    for ck, win in keys:
        r = idx.get((ck, kind, side, round(level, 4), win))
        if r:
            return r
    return None


# ------------------------------------------------------------------ тексты ----

KIND_RU = {"high": "макс", "low": "мин"}


def rule_text(r: dict, short: bool = False) -> str:
    who = r["city"] if r["city_key"] != ALL else "все города"
    when = "в любое время" if r["window"] == ANY else r["window"]
    head = f"{who} · {KIND_RU.get(r['kind'], r['kind'])} · {r['side']} от {r['level']:.2f} · {when}"
    stats = (f"{r['wins']}/{r['n']} ({r['win_pct']:.1f}%), вход ~{r['avg_entry']:.3f}, "
             f"{r['ev_pct']:+.1f}%" + ("" if not short else " на ставку"))
    if short:
        return f"{head}: {stats}"
    check = ""
    if r.get("n_old") and r.get("n_new"):
        p = f" (p={r['p_old']:.3f})" if r.get("p_old") is not None else ""
        check = f" · до/после: {r['wins_old']}/{r['n_old']}{p}, {r['wins_new']}/{r['n_new']}"
    return f"{head}: {stats}{check} · {r['events']} соб., ~{r['per_day']:.1f} в день"


def _line(r: dict) -> str:
    return (f"от {r['level']:.2f}: {text.n_bets(r['n'])} ({r['events']} соб.), выигр. {r['win_pct']:.1f}%, "
            f"вход {r['avg_entry']:.3f}, {r['ev_pct']:+.1f}%")


def overview_lines(rows: list[dict] | None = None) -> list[str]:
    """Все города, любое время: по уровням, для макс/мин и YES/NO."""
    rows = rows if rows is not None else (cells() or [])
    out = []
    for kind in ("high", "low"):
        for side in ("YES", "NO"):
            sel = [r for r in rows if r["city_key"] == ALL and r["kind"] == kind and r["side"] == side
                   and r["window"] == ANY]
            if not sel:
                continue
            out.append(f"Все города · {KIND_RU[kind]} · {side}:")
            out += ["  " + _line(r) for r in sorted(sel, key=lambda r: r["level"])]
    return out


def city_lines(city_key: str, rows: list[dict] | None = None) -> list[str]:
    """Город: по уровням в любое время и лучшее окно времени на каждом уровне."""
    rows = rows if rows is not None else (cells() or [])
    out = []
    for kind in ("high", "low"):
        for side in ("YES", "NO"):
            sel = [r for r in rows if r["city_key"] == city_key and r["kind"] == kind and r["side"] == side]
            anyw = sorted([r for r in sel if r["window"] == ANY], key=lambda r: r["level"])
            if not anyw:
                continue
            out.append(f"{KIND_RU[kind]} · {side}:")
            for r in anyw:
                line = "  " + _line(r)
                best = [x for x in sel if x["level"] == r["level"] and x["window"] != ANY and x["n"] >= 5]
                if best:
                    b = max(best, key=lambda x: x["ev_pct"])
                    line += f"; лучше всего {b['window']}: {b['wins']}/{b['n']}, {b['ev_pct']:+.1f}%"
                if r["passed"]:
                    line += " ✅"
                out.append(line)
    return out
