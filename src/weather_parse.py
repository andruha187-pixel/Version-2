"""
Разбор температурных событий Gamma API.

Событие: «Highest temperature in London on May 19?» (или «Lowest …»), внутри —
рынки-корзины, у каждой своё Yes/No:
  «Will the highest temperature in London be 12°C or below on May 19?»  → ≤12
  «… be 13°C on May 19?»                                              → 13
  «… be 22°C or higher on May 19?»                                    → ≥22
В США — по Фаренгейту и по два градуса: «42-43°F», «35°F or below», «54°F or higher».
Исход — по истории станции на Weather Underground (ссылка в правилах), целые
градусы; «день» — местные сутки города. Выигрывает ровно одна корзина.
"""
from __future__ import annotations
import json
import re
from datetime import datetime, timezone

from src import cities

TITLE_RE = re.compile(r"^\s*(highest|lowest)\s+temperature\s+in\s+(.+?)\s+on\s+([A-Za-z]+)\.?\s+(\d{1,2})"
                      r"(?:st|nd|rd|th)?(?:,?\s*(\d{4}))?\s*\??\s*$", re.I)
QUESTION_RE = re.compile(r"\bbe\s+(.+?)\s+on\s+[A-Za-z]+\.?\s+\d{1,2}", re.I)
WU_RE = re.compile(r"wunderground\.com/history/daily/([a-z]{2})/([^/\s?#]+)/([A-Za-z0-9]{3,5})", re.I)

MONTHS = {m: i + 1 for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
     "november", "december"])}
MONTHS.update({m[:3]: i for m, i in list(MONTHS.items())})
MONTHS["sept"] = 9

_NUM = r"(-?\d+(?:\.\d+)?)"
_DEG = r"\s*°?\s*([CF])?"


def is_temperature_title(title: str) -> bool:
    return bool(TITLE_RE.match(title or ""))


def parse_title(title: str):
    """(kind 'high'/'low', город, месяц, день, год или None) или None."""
    m = TITLE_RE.match(title or "")
    if not m:
        return None
    month = MONTHS.get(m.group(3).lower())
    if not month:
        return None
    return ("high" if m.group(1).lower() == "highest" else "low", m.group(2).strip(), month, int(m.group(4)),
            int(m.group(5)) if m.group(5) else None)


def _clean(s: str) -> str:
    s = (s or "").replace("−", "-").replace("–", "-").replace("—", "-").replace("º", "°").replace("˚", "°")
    return re.sub(r"\s+", " ", s).strip()


def parse_bucket(text: str):
    """«12°C or below» → (None, 12, 'C'); «13°C» → (13, 13, 'C'); «42-43°F» → (42, 43, 'F');
    «54°F or higher» → (54, None, 'F'). Не разобрали — None."""
    s = _clean(text).rstrip("?").strip()
    low = s.lower()
    unit = None
    mu = re.search(r"°\s*([CF])\b|\b\d\s*([CF])\b", s, re.I)
    if mu:
        unit = (mu.group(1) or mu.group(2)).upper()
    m = re.match(r"^(?:≤\s*)?" + _NUM + _DEG + r"\s*(?:or|and)\s*(?:below|lower|less|under|colder)\b", low, re.I)
    if m or re.match(r"^≤\s*" + _NUM, low):
        m = m or re.match(r"^≤\s*" + _NUM, low)
        return None, float(m.group(1)), unit
    m = re.match(r"^(?:≥\s*)?" + _NUM + _DEG + r"\s*(?:or|and)\s*(?:higher|above|more|over|warmer)\b", low, re.I)
    if m or re.match(r"^≥\s*" + _NUM, low):
        m = m or re.match(r"^≥\s*" + _NUM, low)
        return float(m.group(1)), None, unit
    m = re.match(r"^" + _NUM + _DEG + r"\s*(?:-|to)\s*" + _NUM + _DEG + r"$", low, re.I)
    if m:
        a, b = float(m.group(1)), float(m.group(3))
        return min(a, b), max(a, b), unit
    m = re.match(r"^" + _NUM + _DEG + r"$", low, re.I)
    if m:
        v = float(m.group(1))
        return v, v, unit
    return None


def bucket_text(market: dict) -> str:
    """Подпись корзины: groupItemTitle, иначе кусок вопроса между «be» и «on»."""
    t = (market.get("groupItemTitle") or "").strip()
    if t:
        return t
    m = QUESTION_RE.search(_clean(market.get("question") or ""))
    return m.group(1) if m else ""


def _json_list(v):
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v.strip():
        try:
            out = json.loads(v)
            return out if isinstance(out, list) else []
        except ValueError:
            return []
    return []


def _ts(v) -> int | None:
    """ISO-строка или unix-время (с/мс) → unix-секунды."""
    if v in (None, ""):
        return None
    try:
        x = float(v)
        return int(x / 1000) if x > 1e12 else int(x)
    except (TypeError, ValueError):
        pass
    s = str(v).strip().replace("Z", "+00:00")
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)
    if "T" in s:
        s = re.sub(r"([+-]\d\d)$", r"\1:00", s)          # «+00» → «+00:00» (Python 3.11)
    s = re.sub(r"(\.\d{6})\d+", r"\1", s)                # больше 6 знаков долей секунды
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _num(v) -> float | None:
    try:
        return None if v in (None, "") else float(v)
    except (TypeError, ValueError):
        return None


def market_outcome(market: dict) -> int | None:
    """1 — выиграл YES, 0 — NO, None — исхода ещё нет (или спорный)."""
    if not market.get("closed"):
        return None
    outs = [str(o).strip().lower() for o in _json_list(market.get("outcomes"))] or ["yes", "no"]
    prices = [_num(p) for p in _json_list(market.get("outcomePrices"))]
    if len(prices) < 2 or None in prices[:2]:
        return None
    yi = outs.index("yes") if "yes" in outs else 0
    py = prices[yi]
    if py >= 0.99:
        return 1
    if py <= 0.01:
        return 0
    return None


def _pick_date(month: int, day: int, year: int | None, ref_ts: int | None) -> str | None:
    if year:
        try:
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return None
    ref = datetime.fromtimestamp(ref_ts or 0, tz=timezone.utc) if ref_ts else datetime.now(timezone.utc)
    best = None
    for y in (ref.year - 1, ref.year, ref.year + 1):
        try:
            d = datetime(y, month, day, tzinfo=timezone.utc)
        except ValueError:
            continue
        if best is None or abs((d - ref).total_seconds()) < abs((best - ref).total_seconds()):
            best = d
    return best.strftime("%Y-%m-%d") if best else None


def parse_event(ev: dict) -> dict | None:
    """Событие Gamma → словарь для базы (None — не температурное событие)."""
    t = parse_title(ev.get("title") or "")
    if not t:
        return None
    kind, city, month, day, year = t
    end_ts = _ts(ev.get("endDate"))
    date = None
    ed = str(ev.get("eventDate") or "")[:10]
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", ed):
        try:
            if int(ed[5:7]) == month and int(ed[8:10]) == day:
                date = ed
        except ValueError:
            date = None
    if date is None:
        date = _pick_date(month, day, year, end_ts or _ts(ev.get("startDate")))
    if date is None:
        return None
    text = " ".join(str(x or "") for x in (ev.get("resolutionSource"), ev.get("description")))
    for mk in ev.get("markets") or []:
        if isinstance(mk, dict):
            text += " " + str(mk.get("resolutionSource") or "") + " " + str(mk.get("description") or "")[:400]
            break
    wu = WU_RE.search(text)
    country, station = (wu.group(1).lower(), wu.group(3).upper()) if wu else (None, None)
    tz, tz_ok = cities.tz_for(city, country)
    buckets = []
    unit = None
    for mk in ev.get("markets") or []:
        if not isinstance(mk, dict):
            continue
        label = bucket_text(mk)
        parsed = parse_bucket(label)
        if parsed is None:
            continue
        lo, hi, u = parsed
        unit = unit or u
        toks = _json_list(mk.get("clobTokenIds"))
        outs = [str(o).strip().lower() for o in _json_list(mk.get("outcomes"))] or ["yes", "no"]
        if len(toks) < 2:
            continue
        yi = outs.index("yes") if "yes" in outs else 0
        buckets.append({
            "title": label, "lo": lo, "hi": hi, "cond_id": mk.get("conditionId") or "",
            "yes_token": str(toks[yi]), "no_token": str(toks[1 - yi]),
            "outcome": market_outcome(mk),
            "volume": _num(mk.get("volumeNum")) if mk.get("volumeNum") is not None else _num(mk.get("volume")),
            "best_bid": _num(mk.get("bestBid")), "best_ask": _num(mk.get("bestAsk")),
            "last": _num(mk.get("lastTradePrice")), "liquidity": _num(mk.get("liquidityNum")),
            "closed": bool(mk.get("closed")), "market_id": str(mk.get("id") or ""),
            "accepting": mk.get("acceptingOrders") if isinstance(mk.get("acceptingOrders"), bool) else None,
        })
    if len(buckets) < 2:
        return None
    buckets.sort(key=lambda b: (b["lo"] if b["lo"] is not None else -1e9, b["hi"] if b["hi"] is not None else 1e9))
    for i, b in enumerate(buckets):
        b["idx"] = i
    winners = [b["title"] for b in buckets if b["outcome"] == 1]
    return {
        "id": int(ev.get("id")), "slug": ev.get("slug") or "", "title": ev.get("title") or "",
        "kind": kind, "city": city, "city_key": cities.city_key(city), "date": date, "unit": unit or "",
        "station": station or "", "country": country or "", "tz": tz, "tz_ok": tz_ok,
        "day_start_ts": cities.local_midnight_ts(date, tz),
        "listed_ts": _ts(ev.get("creationDate")) or _ts(ev.get("startDate")),
        "end_ts": end_ts, "closed_ts": _ts(ev.get("closedTime")),
        "volume": _num(ev.get("volume")) or sum(b["volume"] or 0 for b in buckets),
        "liquidity": _num(ev.get("liquidity")),
        "closed": bool(ev.get("closed")), "winner": winners[0] if len(winners) == 1 else None,
        "buckets": buckets,
    }


def bucket_label(lo, hi, unit: str = "") -> str:
    u = f"°{unit}" if unit else "°"
    f = (lambda v: f"{v:g}")
    if lo is None:
        return f"≤{f(hi)}{u}"
    if hi is None:
        return f"≥{f(lo)}{u}"
    return f"{f(lo)}{u}" if lo == hi else f"{f(lo)}–{f(hi)}{u}"
