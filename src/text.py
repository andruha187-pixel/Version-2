"""Мелочи для текстов: склонения, время, деньги."""
from __future__ import annotations
import time


def plural(n: int, one: str, few: str, many: str) -> str:
    """1 ставка, 2 ставки, 5 ставок."""
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def n_bets(n: int) -> str:
    return f"{n} {plural(n, 'ставка', 'ставки', 'ставок')}"


def n_events(n: int) -> str:
    return f"{n} {plural(n, 'событие', 'события', 'событий')}"


def n_cities(n: int) -> str:
    return f"{n} {plural(n, 'город', 'города', 'городов')}"


def thousands(v: float) -> str:
    return f"{v:,.0f}".replace(",", " ")


def usd(v: float) -> str:
    return f"${thousands(v)}" if abs(v) >= 1000 else f"${v:g}"


def utc(ts: float | None, fmt: str = "%d.%m %H:%M") -> str:
    return time.strftime(fmt, time.gmtime(ts)) if ts else "—"


def duration(sec: float) -> str:
    """5400 → «1 ч 30 мин», 600 → «10 мин»."""
    m = int(round(sec / 60))
    h, m = divmod(m, 60)
    return f"{h} ч {m} мин" if h else f"{max(1, m)} мин"


def local_when(date_iso: str, hour: float) -> str:
    """Местное время в городе: «14:09» в день рынка, иначе с датой — «24.10 14:09»."""
    import datetime as _dt
    day, rem = divmod(hour, 24.0)
    h, m = int(rem), int(round((rem - int(rem)) * 60))
    if m == 60:
        h, m = h + 1, 0
    if day == 0:
        return f"{h:02d}:{m:02d}"
    try:
        d = _dt.date.fromisoformat(date_iso[:10]) + _dt.timedelta(days=int(day))
    except ValueError:
        return hhmm_local(hour)
    return f"{d.day:02d}.{d.month:02d} {h:02d}:{m:02d}"


def hhmm_local(hour: float) -> str:
    """Час от местной полуночи дня рынка → «16:40», «накануне 22:10», «+1 д 03:00»."""
    day, rem = divmod(hour, 24.0)
    h, m = int(rem), int(round((rem - int(rem)) * 60))
    if m == 60:
        h, m = h + 1, 0
    s = f"{h:02d}:{m:02d}"
    if day < 0:
        return f"накануне {s}" if day == -1 else f"за {int(-day)} дн. {s}"
    if day >= 1:
        return f"+{int(day)} д {s}"
    return s
