"""
📚 Серии UP/DOWN: что было после k одинаковых исходов подряд и чем кончилась бы
ставка на разворот с удвоением после проигрыша — по свечам Binance за
SERIES_HISTORY_DAYS дней, окна 15m (5m — если попросить). Только расчёт, ничего не покупает.

Исход окна — как у Polymarket: UP, если цена в конце не ниже, чем в начале;
берём свечу Binance того же окна (закрытие ≥ открытия). С официальным исходом
(Chainlink) это совпадает примерно в 94% окон 15m (сверка 16.10):
расходятся окна, где цена почти не сдвинулась. На вопрос «зависит ли следующее
окно от серии» этого хватает.

Ставка — по ask на старте окна (SERIES_ENTRY_PRICE; по архиву в среднем
0.515), комиссия тейкера — ставка × FEE_RATE × (1 − цена). План (k, m): после
k одинаковых исходов ставим на разворот; проиграли — следующая ставка ×2, не
больше m ставок; проигранная серия ждёт, пока серия исходов прервётся.
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
from array import array

from config import settings
from src import binance_feed

log = logging.getLogger("history_study")

INTERVALS = {"5m": 300, "15m": 900}
KMAX = 8                       # k=8 — это «8 и больше»
SIM_K = (2, 3, 4, 5, 6, 7)
SIM_M = (1, 2, 3, 4, 5)
REQUEST_PAUSE_SEC = 0.15       # между запросами свечей (лимиты Binance)

_state = {"running": False, "started": 0.0, "finished": 0.0, "error": "", "progress": ""}
_tasks: set = set()            # ссылки на фоновые задачи, чтобы их не собрал сборщик мусора


def running() -> bool:
    return bool(_state["running"])


def entry_price() -> float:
    return float(settings.SERIES_ENTRY_PRICE)


def breakeven(price: float | None = None) -> float:
    """Доля выигрышей, при которой ставка по цене price с комиссией в ноль."""
    p = entry_price() if price is None else price
    return p + float(settings.FEE_RATE) * p * (1 - p)


async def fetch_returns(asset: str, interval: str, days: int) -> tuple[array, array]:
    """Время открытия окон (с) и доходность закрытие/открытие − 1."""
    symbol = binance_feed.symbol_for(asset)
    step = INTERVALS[interval]
    now = int(time.time())
    start_ms = (now - days * 86400) // step * step * 1000
    end_ms = now // step * step * 1000              # текущее, ещё не закрытое окно не берём
    times, rets = array("q"), array("d")
    while start_ms < end_ms:
        df = await binance_feed.get_klines(symbol, limit=1000, interval=interval, start_time_ms=start_ms)
        if df is None or df.empty:
            break
        last = None
        for ot, o, c in zip(df["open_time"], df["open"], df["close"]):
            ot = int(ot)
            if ot >= end_ms:
                break
            if o > 0:
                times.append(ot // 1000)
                rets.append(float(c) / float(o) - 1)
            last = ot
        if last is None or last + step * 1000 <= start_ms:
            break
        start_ms = last + step * 1000
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return times, rets


def runs(times, rets, step: int) -> list[tuple[int, int, bool, int]]:
    """Серии одинаковых исходов: (направление ±1, длина, прервана разворотом, время последнего окна).
    Разрыв в данных обрывает серию без разворота."""
    out = []
    cur, length, prev, last_t = None, 0, None, 0
    for t, r in zip(times, rets):
        d = 1 if r >= 0 else -1
        contiguous = prev is not None and t - prev == step
        if contiguous and d == cur:
            length += 1
        else:
            if cur is not None:
                out.append((cur, length, contiguous, last_t))
            cur, length = d, 1
        prev = last_t = t
    if cur is not None:
        out.append((cur, length, False, last_t))
    return out


def streak_table(rs, kmax: int = KMAX, t_from: int = 0, t_to: int = 1 << 62) -> tuple[list[int], list[int]]:
    """n[k], rev[k]: сколько раз после k одинаковых подряд было следующее окно и сколько из них — разворот."""
    n, rev = [0] * (kmax + 1), [0] * (kmax + 1)
    for _d, length, closed, t_end in rs:
        if not (t_from <= t_end < t_to):
            continue
        for k in range(1, length + 1):
            kk = min(k, kmax)
            if k < length:
                n[kk] += 1
            elif closed:
                n[kk] += 1
                rev[kk] += 1
    return n, rev


def simulate(rs, k: int, m: int, price: float | None = None) -> dict:
    """План (k, m) по сериям в порядке времени; ставки 1, 2, 4… (в единицах базовой ставки)."""
    price = entry_price() if price is None else price
    fee = float(settings.FEE_RATE) * (1 - price)            # на $1 ставки
    win_g, loss_g = 1 / price - 1 - fee, -1 - fee
    pnl = peak = mdd = staked = worst = 0.0
    series = wins = losses = bets = 0
    for _d, length, closed, _t in rs:
        if length < k:
            continue
        sp = 0.0
        for j in range(m):
            stake = 2.0 ** j
            if length > k + j:                 # серия продолжилась — ставка проиграла
                g = stake * loss_g
                if j == m - 1:
                    losses += 1
                    series += 1
            elif closed:                       # серия кончилась — ставка выиграла
                g = stake * win_g
                wins += 1
                series += 1
            else:                              # данные оборвались — исход ставки неизвестен
                break
            bets += 1
            staked += stake
            pnl += g
            sp += g
            peak = max(peak, pnl)
            mdd = max(mdd, peak - pnl)
            if length <= k + j or j == m - 1:
                worst = min(worst, sp)
                break
    return {"k": k, "m": m, "series": series, "wins": wins, "losses": losses, "bets": bets, "staked": staked,
            "pnl": pnl, "pct": pnl / staked * 100 if staked else 0.0, "mdd": mdd, "worst": worst}


def _rate(n, r) -> str:
    return f"{r / n * 100:.1f}%" if n else "—"


def _num(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def _ktag(k: int) -> str:
    return f"{k}+" if k == KMAX else str(k)


async def run_study(days: int | None = None, assets=None, intervals=("15m",), progress=None) -> dict:
    days = int(days or settings.SERIES_HISTORY_DAYS)
    assets = list(assets or settings.ASSETS)
    res = {"days": days, "assets": assets, "price": entry_price(), "intervals": {}, "raw": {}}
    for iv in intervals:
        step = INTERVALS[iv]
        per = {}
        for a in assets:
            if progress:
                progress(f"{iv} {a.upper()}")
            try:
                times, rets = await fetch_returns(a, iv, days)
            except Exception as exc:  # noqa: BLE001 — монеты нет / сеть: идём дальше
                log.warning("История %s %s: %s", a, iv, exc)
                continue
            if len(times) < 100:
                continue
            rs = runs(times, rets, step)
            mid = times[len(times) // 2]
            sims = {}
            for k in SIM_K:
                for m in SIM_M:
                    sims[(k, m)] = simulate(rs, k, m)
                    await asyncio.sleep(0)                 # не держим цикл бота
            per[a] = {"windows": len(times), "up": sum(1 for r in rets if r >= 0) / len(rets),
                      "from": times[0], "to": times[-1], "table": streak_table(rs),
                      "half1": streak_table(rs, t_to=mid), "half2": streak_table(rs, t_from=mid), "sims": sims}
            res["raw"][(iv, a)] = (times, rets)
        res["intervals"][iv] = per
    return res


def _pooled(per: dict, key: str) -> tuple[list[int], list[int]]:
    n, rev = [0] * (KMAX + 1), [0] * (KMAX + 1)
    for v in per.values():
        for k in range(1, KMAX + 1):
            n[k] += v[key][0][k]
            rev[k] += v[key][1][k]
    return n, rev


def summary_text(res: dict) -> str:
    be = breakeven(res["price"])
    lines = [f"📚 Серии UP/DOWN по свечам Binance за {res['days']} дн.",
             f"Ставка на разворот по ask {res['price']:.3f} с комиссией окупается, если разворот чаще "
             f"{be * 100:.1f}%. Исход по Binance совпадает с официальным примерно в 94% окон 15m."]
    for iv, per in res["intervals"].items():
        if not per:
            lines += ["", f"{iv}: данных нет"]
            continue
        total = sum(v["windows"] for v in per.values())
        up = sum(v["up"] * v["windows"] for v in per.values()) / total
        t0 = min(v["from"] for v in per.values())
        t1 = max(v["to"] for v in per.values())
        lines += ["", f"{iv} — {', '.join(a.upper() for a in per)}; окон {_num(total)} "
                  f"({time.strftime('%d.%m.%y', time.gmtime(t0))}–{time.strftime('%d.%m.%y', time.gmtime(t1))}), "
                  f"UP {up * 100:.1f}%"]
        n, rev = _pooled(per, "table")
        lines.append("Разворот после k одинаковых подряд, все монеты: " + " · ".join(
            f"k={_ktag(k)} {_rate(n[k], rev[k])}" for k in range(1, KMAX + 1)))
        nn, rr = sum(n[3:]), sum(rev[3:])
        n1, r1 = _pooled(per, "half1")
        n2, r2 = _pooled(per, "half2")
        lines.append(f"k≥3: {_rate(nn, rr)} из {_num(nn)} (1-я половина {_rate(sum(n1[3:]), sum(r1[3:]))}, "
                     f"2-я {_rate(sum(n2[3:]), sum(r2[3:]))})")
        if "btc" in per:
            bn, br = per["btc"]["table"]
            lines.append("BTC: " + " · ".join(f"k={_ktag(k)} {_rate(bn[k], br[k])}" for k in range(1, KMAX + 1)))
        plans = {}
        for v in per.values():
            for key, s in v["sims"].items():
                p = plans.setdefault(key, {"pnl": 0.0, "staked": 0.0})
                p["pnl"] += s["pnl"]
                p["staked"] += s["staked"]
        rated = sorted(((p["pnl"] / p["staked"] * 100 if p["staked"] else 0.0, key) for key, p in plans.items()),
                       reverse=True)
        plus = sum(1 for pct, _ in rated if pct > 0)
        best, worst = rated[0], rated[-1]
        lines.append(f"Удвоение (k — серия до входа, m — ставок в серии): в плюсе {plus} из {len(rated)} планов; "
                     f"лучший k={best[1][0]}, m={best[1][1]}: {best[0]:+.1f}% оборота; "
                     f"худший k={worst[1][0]}, m={worst[1][1]}: {worst[0]:+.1f}%")
    return "\n".join(lines)


def write_zip(res: dict, path: str) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["interval", "asset", "part", "k", "n", "reversals", "rate"])
        for iv, per in res["intervals"].items():
            for a, v in per.items():
                for part in ("table", "half1", "half2"):
                    n, rev = v[part]
                    for k in range(1, KMAX + 1):
                        w.writerow([iv, a, {"table": "all", "half1": "first_half", "half2": "second_half"}[part],
                                    _ktag(k), n[k], rev[k], round(rev[k] / n[k], 4) if n[k] else ""])
        zf.writestr("streaks.csv", buf.getvalue())
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["interval", "asset", "k", "m", "series", "wins", "losses", "bets", "staked_units",
                    "pnl_units", "pnl_pct", "max_drawdown_units", "worst_series_units", "entry_price"])
        for iv, per in res["intervals"].items():
            for a, v in per.items():
                for (k, m), s in v["sims"].items():
                    w.writerow([iv, a, k, m, s["series"], s["wins"], s["losses"], s["bets"], round(s["staked"], 2),
                                round(s["pnl"], 2), round(s["pct"], 2), round(s["mdd"], 2), round(s["worst"], 2),
                                res["price"]])
        zf.writestr("plans.csv", buf.getvalue())
        for (iv, a), (times, rets) in res["raw"].items():
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["open_time", "ret_bps"])
            for t, r in zip(times, rets):
                w.writerow([t, round(r * 1e4, 2)])
            zf.writestr(f"candles_{iv}_{a}.csv", buf.getvalue())
    return path


async def run_and_send(days: int | None = None) -> None:
    """Кнопка 📚: посчитать и прислать итог и файл. Повторный запуск во время расчёта — игнор."""
    from src import telegram_notify
    if _state["running"]:
        return
    _state.update(running=True, started=time.time(), error="", progress="")
    path = None
    try:
        res = await run_study(days, progress=lambda s: _state.update(progress=s))
        text = summary_text(res)
        path = write_zip(res, os.path.join(settings.REPORTS_DIR,
                                           time.strftime("series_%Y%m%d-%H%M.zip", time.gmtime())))
        await telegram_notify.notify(text[:4000])
        await telegram_notify.send_document(path, "📚 Серии UP/DOWN: streaks.csv — развороты по k, plans.csv — "
                                                  "планы с удвоением, candles_*.csv — доходности окон")
    except Exception as exc:  # noqa: BLE001
        _state["error"] = f"{type(exc).__name__}: {exc}"[:300]
        log.warning("История серий не посчиталась: %s", exc)
        try:
            await telegram_notify.notify(f"⚠️ 📚 История серий не посчиталась: {_state['error']}")
        except Exception:  # noqa: BLE001
            pass
    finally:
        _state.update(running=False, finished=time.time())
        if path:
            try:
                os.remove(path)
            except OSError:
                pass


def start(days: int | None = None) -> bool:
    """Запустить расчёт в фоне. False — уже идёт."""
    if _state["running"]:
        return False
    task = asyncio.create_task(run_and_send(days))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return True


def status_text() -> str:
    if _state["running"]:
        return (f"⏳ Считаю ({_state['progress'] or 'старт'}), начал "
                f"{int(time.time() - _state['started'])} с назад — пришлю итог и файл.")
    return ""


# ------------------------------------------- 🧾 официальные исходы Polymarket ----
#
# Свечи Binance совпадают с официальным исходом примерно в 87% окон. Чтобы
# проверить эффект серий на самих исходах Polymarket, выгружаем закрытые рынки
# {монета}-updown-{интервал}-{начало} за SERIES_OFFICIAL_DAYS дней: через серию
# событий (GET /events/keyset?series_id=…&closed=true, по 100 на страницу), а
# если так не вышло — пачками слагов (GET /events?slug=…&slug=…).

OFFICIAL_BATCH = 40
OFFICIAL_MAX_PAGES = 400


def _market_outcome(ev: dict) -> str | None:
    """UP/DOWN закрытого рынка по outcomePrices (у победителя цена 1)."""
    market = ev["markets"][0] if ev.get("markets") else ev
    if not market.get("closed"):
        return None
    outs = market.get("outcomes")
    outs = json.loads(outs) if isinstance(outs, str) else (outs or ["Up", "Down"])
    prices = market.get("outcomePrices")
    prices = json.loads(prices) if isinstance(prices, str) else prices
    try:
        vals = [float(p) for p in prices or []]
    except (TypeError, ValueError):
        return None
    if not vals:
        return None
    i = max(range(len(vals)), key=lambda j: vals[j])
    if vals[i] < 0.99 or i >= len(outs):
        return None
    o = str(outs[i]).upper()
    return o if o in ("UP", "DOWN") else None


def _slug_start(slug: str) -> int | None:
    try:
        return int(str(slug).rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


async def _gamma_get(path: str, params) -> object:
    import httpx
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(f"{settings.GAMMA_HOST}{path}", params=params)
        resp.raise_for_status()
        return resp.json()


async def _official_by_series(series_id, prefix: str, t_from: int, t_to: int) -> dict[int, str] | None:
    """Исходы через /events/keyset по серии. None — так не получилось (тогда слагами)."""
    iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
    params = {"series_id": series_id, "closed": "true", "limit": 100,
              "end_date_min": iso(t_from), "end_date_max": iso(t_to)}
    out: dict[int, str] = {}
    cursor = None
    for _ in range(OFFICIAL_MAX_PAGES):
        p = dict(params)
        if cursor:
            p["after_cursor"] = cursor
        try:
            data = await _gamma_get("/events/keyset", p)
        except Exception as exc:  # noqa: BLE001
            log.info("keyset по серии %s не вышел: %s", series_id, exc)
            return None if not out else out
        evs = data.get("events") if isinstance(data, dict) else None
        if evs is None:
            return None if not out else out
        for ev in evs:
            slug = str(ev.get("slug") or "")
            if not slug.startswith(prefix):
                continue
            s, o = _slug_start(slug), _market_outcome(ev)
            if s is not None and o and t_from <= s < t_to:
                out[s] = o
        cursor = data.get("next_cursor")
        if not cursor or not evs:
            break
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return out


async def _official_by_slugs(prefix: str, starts: list[int], progress=None) -> dict[int, str]:
    out: dict[int, str] = {}
    for i in range(0, len(starts), OFFICIAL_BATCH):
        batch = starts[i:i + OFFICIAL_BATCH]
        try:
            data = await _gamma_get("/events", [("slug", f"{prefix}{s}") for s in batch])
        except Exception as exc:  # noqa: BLE001
            log.info("слаги %s…: %s", prefix, exc)
            data = []
        for ev in data if isinstance(data, list) else []:
            s, o = _slug_start(ev.get("slug")), _market_outcome(ev)
            if s is not None and o:
                out[s] = o
        if progress and i % (OFFICIAL_BATCH * 25) == 0:
            progress(f"{prefix}… {i}/{len(starts)}")
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return out


async def fetch_official(asset: str, interval: str, days: int, progress=None) -> tuple[dict[int, str], str]:
    """Официальные исходы закрытых окон за days дней: ({начало: UP/DOWN}, способ)."""
    step = INTERVALS[interval]
    now = int(time.time())
    t_to = now // step * step - step                      # последнее закрытое окно
    t_from = (now - days * 86400) // step * step
    prefix = f"{asset}-updown-{interval}-"
    probe = None
    for back in (2, 3, 4):                                # недавнее закрытое окно — есть ли рынки и серия
        data = await _gamma_get("/events", {"slug": f"{prefix}{t_to - back * step}"})
        if isinstance(data, list) and data:
            probe = data[0]
            break
    if probe is None:
        return {}, "нет рынков"
    series = probe.get("series") or []
    series_id = series[0].get("id") if series and isinstance(series[0], dict) else None
    if series_id:
        got = await _official_by_series(series_id, prefix, t_from, t_to + step)
        expected = (t_to - t_from) // step + 1
        if got is not None and len(got) >= 0.5 * expected:
            return got, "серия"
    starts = list(range(t_from, t_to + step, step))
    return await _official_by_slugs(prefix, starts, progress), "слаги"


def _runs_from_map(outs: dict[int, str], step: int):
    ts = sorted(outs)
    return runs(ts, [1.0 if outs[t] == "UP" else -1.0 for t in ts], step)


def flat_plan(rs, k_min: int, price: float | None = None) -> dict:
    """Ровная ставка на разворот в каждом окне, пока серия ≥ k_min."""
    price = entry_price() if price is None else price
    fee = float(settings.FEE_RATE) * (1 - price)
    bets = wins = 0
    for _d, length, closed, _t in rs:
        if length < k_min:
            continue
        bets += length - k_min                 # серия продолжалась — ставки проиграли
        if closed:
            bets += 1
            wins += 1
    pnl = wins * (1 / price - 1 - fee) + (bets - wins) * (-1 - fee)
    return {"bets": bets, "wins": wins, "pct": pnl / bets * 100 if bets else None}


async def run_official(days: int | None = None, assets=None, interval: str = "15m", progress=None) -> dict:
    days = int(days or settings.SERIES_OFFICIAL_DAYS)
    step = INTERVALS[interval]
    res = {"days": days, "interval": interval, "price": entry_price(), "assets": {}}
    for a in list(assets or settings.ASSETS):
        if progress:
            progress(f"исходы {interval} {a.upper()}")
        try:
            outs, how = await fetch_official(a, interval, days, progress)
        except Exception as exc:  # noqa: BLE001
            log.warning("Официальные исходы %s: %s", a, exc)
            continue
        if not outs:
            res["assets"][a] = {"official": {}, "how": how}
            continue
        try:
            times, rets = await fetch_returns(a, interval, days + 1)
        except Exception as exc:  # noqa: BLE001
            log.warning("Свечи %s: %s", a, exc)
            times, rets = array("q"), array("d")
        bn = {int(t): ("UP" if r >= 0 else "DOWN") for t, r in zip(times, rets)}
        bn_ret = {int(t): r for t, r in zip(times, rets)}
        res["assets"][a] = {"official": outs, "how": how, "binance": bn, "binance_ret": bn_ret}
    return res


def _tbl(rs) -> str:
    n, rev = streak_table(rs)
    return " · ".join(f"k={_ktag(k)} {_rate(n[k], rev[k])}" for k in range(1, 7)) + \
        f" (k≥3: {_rate(sum(n[3:]), sum(rev[3:]))} из {_num(sum(n[3:]))})"


def official_summary(res: dict) -> str:
    iv, step = res["interval"], INTERVALS[res["interval"]]
    be = breakeven(res["price"])
    lines = [f"🧾 Официальные исходы Polymarket {iv} за {res['days']} дн. против свечей Binance",
             f"Окупается разворот чаще {be * 100:.1f}% (ask {res['price']:.2f} + комиссия)."]
    pooled_off, pooled_same = {}, {}
    agree = total = 0
    for a, v in res["assets"].items():
        outs = v["official"]
        if not outs:
            lines.append(f"{a.upper()}: {v['how']}")
            continue
        bn = v.get("binance") or {}
        same = {t: bn[t] for t in outs if t in bn}
        ag = sum(1 for t in same if same[t] == outs[t])
        agree += ag
        total += len(same)
        lines.append(f"{a.upper()}: окон {_num(len(outs))} ({v['how']}), совпадение с Binance "
                     f"{ag / len(same) * 100:.1f}%" if same else f"{a.upper()}: окон {_num(len(outs))} ({v['how']})")
        pooled_off[a] = _runs_from_map(outs, step)
        pooled_same[a] = _runs_from_map(same, step)
    if not pooled_off:
        return "\n".join(lines + ["Исходов не получили."])

    def pool(rsmap):
        out = []
        for rs in rsmap.values():
            out += rs
        return out
    ro, rb = pool(pooled_off), pool(pooled_same)
    lines += ["", "Разворот после k одинаковых подряд, все монеты:",
              "  Polymarket: " + _tbl(ro),
              "  Binance, те же окна: " + _tbl(rb)]
    for k in (3, 4):
        po, pb = flat_plan(ro, k), flat_plan(rb, k)
        lines.append(f"Ставка на разворот в каждом окне при серии ≥{k} (по {res['price']:.2f}): Polymarket "
                     f"{_num(po['bets'])} ставок, выигрышей {_rate(po['bets'], po['wins'])}, итог "
                     f"{po['pct']:+.1f}% · Binance {_rate(pb['bets'], pb['wins'])}, {pb['pct']:+.1f}%"
                     if po["bets"] and pb["bets"] else f"Серий ≥{k} мало.")
    if total:
        lines.append(f"Совпадение исходов Binance и Polymarket: {agree / total * 100:.1f}% из {_num(total)}")
    return "\n".join(lines)


def write_official_zip(res: dict, path: str) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["asset", "interval", "start", "official", "binance", "binance_ret_bps"])
    for a, v in res["assets"].items():
        bn, br = v.get("binance") or {}, v.get("binance_ret") or {}
        for t in sorted(v["official"]):
            r = br.get(t)
            w.writerow([a, res["interval"], t, v["official"][t], bn.get(t, ""), round(r * 1e4, 2) if r is not None else ""])
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("official.csv", buf.getvalue())
    return path


async def run_official_and_send(days: int | None = None) -> None:
    from src import telegram_notify
    if _state["running"]:
        return
    _state.update(running=True, started=time.time(), error="", progress="")
    path = None
    try:
        res = await run_official(days, progress=lambda s: _state.update(progress=s))
        text = official_summary(res)
        path = write_official_zip(res, os.path.join(settings.REPORTS_DIR,
                                                    time.strftime("official_%Y%m%d-%H%M.zip", time.gmtime())))
        await telegram_notify.notify(text[:4000])
        await telegram_notify.send_document(path, "🧾 official.csv — официальные исходы Polymarket и свечи Binance "
                                                  "по тем же окнам")
    except Exception as exc:  # noqa: BLE001
        _state["error"] = f"{type(exc).__name__}: {exc}"[:300]
        log.warning("Сверка с Polymarket не прошла: %s", exc)
        try:
            await telegram_notify.notify(f"⚠️ 🧾 Сверка с исходами Polymarket не прошла: {_state['error']}")
        except Exception:  # noqa: BLE001
            pass
    finally:
        _state.update(running=False, finished=time.time())
        if path:
            try:
                os.remove(path)
            except OSError:
                pass


def start_official(days: int | None = None) -> bool:
    if _state["running"]:
        return False
    task = asyncio.create_task(run_official_and_send(days))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return True
