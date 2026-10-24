"""
CLOB API: история цены токена (prices-history) и стакан (book).

История закрытых рынков: с interval=max и мелким шагом Polymarket отдаёт пусто,
поэтому всегда задаём startTs/endTs (кусками не длиннее 14 дней) и шаг
PATH_FIDELITY_MIN; пусто — пробуем шаг крупнее (15, 60, 180 минут).
"""
from __future__ import annotations

from config import settings
from src import http_api

CHUNK_SEC = 14 * 86400
FALLBACK_FIDELITIES = (15, 60, 180)


def _points(data) -> list[list]:
    hist = data.get("history") if isinstance(data, dict) else data
    out = []
    for h in hist or []:
        try:
            t, p = int(float(h["t"])), float(h["p"])
        except (TypeError, ValueError, KeyError):
            continue
        if t > 1e12:
            t //= 1000
        if 0.0 <= p <= 1.0:
            out.append([t, p])
    return out


async def price_history(token_id: str, start_ts: int, end_ts: int) -> tuple[int | None, list[list]]:
    """(шаг в минутах, [[unix-время, цена], …]) — цена YES/NO токена; пусто — (None, [])."""
    fids = [int(settings.PATH_FIDELITY_MIN)] + [f for f in FALLBACK_FIDELITIES if f > int(settings.PATH_FIDELITY_MIN)]
    for fid in fids:
        pts: list[list] = []
        t = int(start_ts)
        while t < end_ts:
            t2 = min(int(end_ts), t + CHUNK_SEC)
            data = await http_api.get_json(f"{settings.CLOB_HOST}/prices-history",
                                           {"market": token_id, "startTs": t, "endTs": t2, "fidelity": fid})
            pts += _points(data)
            t = t2
        if pts:
            pts.sort(key=lambda x: x[0])
            dedup = [pts[0]]
            for p in pts[1:]:
                if p[0] != dedup[-1][0]:
                    dedup.append(p)
            return fid, dedup
    return None, []


def _levels(levels) -> list[tuple[float, float]]:
    out = []
    for lvl in levels or []:
        try:
            p = float(lvl["price"] if isinstance(lvl, dict) else lvl[0])
            s = float(lvl["size"] if isinstance(lvl, dict) else lvl[1])
        except (TypeError, ValueError, KeyError, IndexError):
            continue
        if s > 0:
            out.append((p, s))
    return out


async def book(token_id: str) -> dict:
    """Стакан токена: {"bids": [(цена, акций)…] по убыванию, "asks": […] по возрастанию}."""
    data = await http_api.get_json(f"{settings.CLOB_HOST}/book", {"token_id": token_id})
    data = data if isinstance(data, dict) else {}
    return {"bids": sorted(_levels(data.get("bids")), reverse=True), "asks": sorted(_levels(data.get("asks")))}


def depth_usdc(asks: list[tuple[float, float]], max_price: float) -> float:
    """Сколько $ можно купить по asks не дороже max_price."""
    return round(sum(p * s for p, s in asks if p <= max_price + 1e-9), 2)


def fill_price(asks: list[tuple[float, float]], usdc: float, max_price: float) -> float | None:
    """Средняя цена покупки на usdc по asks (по возрастанию), не дороже max_price.
    Столько в стакане до max_price нет — None."""
    left, shares = float(usdc), 0.0
    for p, s in asks:
        if p > max_price + 1e-9:
            break
        take = min(left, p * s)
        shares += take / p
        left -= take
        if left <= 1e-9:
            return round(float(usdc) / shares, 4)
    return None
