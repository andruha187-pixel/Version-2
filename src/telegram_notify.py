"""
Телеграм: меню погодного бота и уведомления.

/menu:
  📊 Пороги     — лучшие проверенные правила; все города по уровням; пересчитать
  🏙 Города     — города архива: события, объём; у города — пороги и кандидаты вкл/выкл
  🎯 Кандидаты  — последние кандидаты и итог тестовых ставок; сообщения: сразу / раз в час / выкл
  📚 Архив      — что скачано, ход докачки, способы поиска; глубина; докачать сейчас
  📤 Файлы      — события, расчётные ставки, ячейки, кандидаты, снимки; пути цены
  ⚙️ Настройки  — объём события, ставок для правила, тестовая ставка

Управлять может только чат из TELEGRAM_CHAT_ID.
"""
from __future__ import annotations
import asyncio
import logging
import time

from telegram import Update, InlineKeyboardButton as Btn, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import settings
from src import archive, gamma, live, runtime_state, storage, study, text

log = logging.getLogger("telegram")

_app: Application | None = None
TG_TEXT_LIMIT = 4096
TOP_CITY_RULES = 8
TOP_ALL_RULES = 4
VOLUME_PRESETS = (0.0, 2000.0, 5000.0, 20000.0)
MIN_N_PRESETS = (20, 30, 50, 100)
STAKE_PRESETS = (5.0, 10.0, 20.0, 50.0)
DAYS_PRESETS = (60, 120, 240, 365)
MIN_EV_PRESETS = (0.0, 1.0, 2.0, 3.0, 5.0)
_tasks: set = set()


def _fit(s: str) -> str:
    return s if len(s) <= TG_TEXT_LIMIT else s[:TG_TEXT_LIMIT - 2].rstrip() + "\n…"


def _menu(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([list(r) for r in rows])


BACK = Btn("◀️ Назад", callback_data="m")


def _mark(ok: bool) -> str:
    return "✅ " if ok else ""


def _spawn(coro) -> None:
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


# ------------------------------------------------------------- строки-сводки ----

def archive_line() -> str:
    c = storage.archive_counts()
    done = c["status"].get("done", 0)
    s = (f"📚 Архив: {text.n_events(done)} с историей цены ({text.n_cities(c['cities'])}; "
         f"макс {c['kinds'].get('high', 0)}, мин {c['kinds'].get('low', 0)})")
    if c["first"]:
        s += f", {c['first'][8:10]}.{c['first'][5:7]} … {c['last'][8:10]}.{c['last'][5:7]}"
    p = archive.progress()
    if p["phase"] == "discover":
        s += f"; ищу события ({p['window']})"
    elif p["phase"] == "paths" and p["paths_total"]:
        s += f"; качаю историю: {text.thousands(p['paths_done'])}/{text.thousands(p['paths_total'])} корзин"
        eta = archive.eta_sec()
        if eta is not None:
            s += f", осталось ~{text.duration(eta)}"
    elif not runtime_state.get("archive_enabled"):
        s += "; докачка ⏸"
    return s


def study_line() -> str:
    sm = study.summary()
    if study.is_running():
        return "📊 Пороги: считаю…"
    if not sm:
        return "📊 Пороги: ещё не считались — нужен архив"
    return (f"📊 Пороги: {text.utc(sm['computed'])} UTC — {text.n_events(sm['events'])}, "
            f"расчётных ставок: {text.thousands(sm['bets'])}; проверенных правил: {sm['rules']}")


def candidates_line() -> str:
    day = int(time.time() // 86400) * 86400
    st, today = live.paper_stats(0), live.paper_stats(day)
    s = f"🎯 Кандидаты: сегодня {today['n']}, всего {st['n']}"
    if st["settled"]:
        s += f"; тестовые ставки: выигр. {st['wins']}/{st['settled']}, {st['pnl']:+.2f} $"
    if not runtime_state.get("alerts_enabled"):
        s += " (сообщения ⏸)"
    elif runtime_state.get("alerts_hourly"):
        s += " (сообщения — сводкой раз в час)"
    return s


# ------------------------------------------------------------------ экраны ----

def main_text() -> str:
    ls = live.stats()
    lines = ["🌡 Погодный бот Polymarket — архив температурных рынков, пороги цены и кандидаты", "",
             archive_line(), study_line(), candidates_line()]
    if ls["last_poll"]:
        lines.append(f"Открытых температурных событий: {ls['open_events']} (проверка {text.utc(ls['last_poll'], '%H:%M')} UTC)")
    if ls["last_error"]:
        lines.append(f"⚠️ Кандидаты: {ls['last_error']}")
    return "\n".join(lines)


def main_markup() -> InlineKeyboardMarkup:
    return _menu(
        (Btn("📊 Пороги", callback_data="th"), Btn("🏙 Города", callback_data="ci")),
        (Btn("🎯 Кандидаты", callback_data="ca"), Btn("📚 Архив", callback_data="ar")),
        (Btn("📤 Файлы", callback_data="fx"), Btn("⚙️ Настройки", callback_data="se")),
        (Btn("🔄 Обновить", callback_data="m"),),
    )


def thresholds_text(head: str = "") -> str:
    sm = study.summary()
    lines = ([head, ""] if head else []) + ["📊 Пороги"]
    if not sm:
        lines.append("Ещё не считались: архив качается — пороги посчитаются сами по ходу (или «♻️ Пересчитать»).")
        return "\n".join(lines)
    lines += [
        f"Расчёт {text.utc(sm['computed'])} UTC: {text.n_events(sm['events'])} ({text.n_cities(sm['cities'])}), "
        f"{sm['first']} … {sm['last']}, объём события от {text.usd(sm['min_volume'])}.",
        f"Проверено = не меньше {sm['min_n']} ставок; до {sm['split']} выигрышей значимо больше окупаемости "
        f"(случайно — с вероятностью ≤ {sm.get('max_p', 0.05):g}), после — в плюсе.",
        "«YES от 0.90» — цена корзины впервые дошла до 0.90 (побыв ниже); вход — цена "
        f"+{sm['slip']:g} (ask), комиссия {sm['fee_rate']:g}×(1−цена). Время — местное в городе.",
        ""]
    rules = study.rules()
    active = [r for r in rules if r.get("active")]
    if active:
        city_rules, all_rules = study.top_rules(TOP_CITY_RULES, cities=True), study.top_rules(TOP_ALL_RULES, cities=False)
        lines.append(f"Правил для кандидатов: {len(active)} из {len(rules)} проверенных — в окне времени, и только "
                     "если то же правило проверено и по всем городам (у одного города «перевес» там, где у всех "
                     "его нет, — скорее везение). Повторы скрыты. Лучшие по доходу в день:")
        if city_rules:
            lines += ["", "По городам:"] + [f"{i}. {study.rule_text(r)}" for i, r in enumerate(city_rules, 1)]
        if all_rules:
            lines += ["", "Все города вместе:"] + [f"{i}. {study.rule_text(r)}" for i, r in enumerate(all_rules, 1)]
    elif rules:
        lines.append(f"Проверенных ячеек: {len(rules)}, но правил для кандидатов нет: ни одно не подтверждено "
                     "по всем городам в своём окне времени.")
    else:
        lines.append("Проверенных правил пока нет — архив мал или перевеса нет.")
    return "\n".join(lines)


def thresholds_markup() -> InlineKeyboardMarkup:
    return _menu((Btn("📋 Все города по уровням", callback_data="tho"), Btn("🏙 По городам", callback_data="ci")),
                 (Btn("♻️ Пересчитать", callback_data="thr"), BACK))


def overview_text() -> str:
    if study.cells() is None:
        return "📋 Все города по уровням — таблица появится после расчёта в этом запуске (♻️ Пересчитать)."
    sm = study.summary() or {}
    head = ("📋 Все города — доход на ставку по времени касания: «за 2+ дня» — рынок только открылся "
            "(события выставляют за ~1.5–2 суток), «накануне», «в день» рынка (местное время). Вход — цена "
            f"касания + {sm.get('slip', settings.STUDY_SLIP):g}, с комиссией; в скобках — ставок; "
            "✅ — по этому окну ищутся кандидаты.")
    return "\n".join([head, ""] + study.overview_lines())


def cities_text() -> str:
    rows = storage.city_stats(float(runtime_state.get("min_event_volume")))
    if not rows:
        return "🏙 Города — архив ещё пуст."
    lines = ["🏙 Города архива (событий макс/мин, средний объём события). 🎯 — кандидаты по городу; "
             "⏸ — выключены; ▫️ — объём меньше порога, в расчёт не входит.", ""]
    for d in rows:
        mark = ("🎯" if runtime_state.city_alerts_on(d["city_key"]) else "⏸") + ("" if d["in_study"] else "▫️")
        vol = max(d["vol_high"], d["vol_low"])
        tz = "" if d["tz_ok"] else " · пояс? (UTC)"
        lines.append(f"{mark} {d['city']}: {d['high']}/{d['low']}, ~{text.usd(vol)}{tz}")
    return "\n".join(lines)


def cities_markup() -> InlineKeyboardMarkup:
    rows = storage.city_stats(float(runtime_state.get("min_event_volume")))
    btns = [Btn(d["city"], callback_data=f"cy:{d['city_key']}"[:64]) for d in rows[:60]]
    grid = [tuple(btns[i:i + 3]) for i in range(0, len(btns), 3)]
    return _menu(*grid, (Btn("🔄 Обновить", callback_data="ci"), BACK))


def city_text(key: str, head: str = "") -> str:
    rows = [d for d in storage.city_stats() if d["city_key"] == key]
    if not rows:
        return "Нет такого города в архиве."
    d = rows[0]
    lines = ([head, ""] if head else []) + [
        f"🏙 {d['city']} — станция {d['station'] or '?'}, пояс {d['tz']}" + ("" if d["tz_ok"] else " (не знаю пояс — UTC)"),
        "Событий: " + ", ".join(f"{name} {d[k]} (~{text.usd(d['vol_' + k])})" for k, name in
                                (("high", "макс"), ("low", "мин")) if d[k]) + ".",
        f"Кандидаты: {'🎯 вкл' if runtime_state.city_alerts_on(key) else '⏸ выкл'}.", ""]
    if study.cells() is None:
        lines.append("Пороги по городу появятся после расчёта в этом запуске (📊 → ♻️ Пересчитать).")
    else:
        cl = study.city_lines(key)
        lines += cl if cl else ["Расчётных ставок по городу нет (мало событий или объём меньше порога)."]
        lines.append("Доход на ставку: за 2+ дня / накануне / в день рынка; в скобках — ставок. ✅ — в этом окне "
                     "по городу ищутся кандидаты (своё правило или правило всех городов, если у города не минус).")
    return "\n".join(lines)


def city_markup(key: str) -> InlineKeyboardMarkup:
    on = runtime_state.city_alerts_on(key)
    return _menu((Btn("🎯 Кандидаты: ✅ вкл" if on else "🎯 Кандидаты: ⏸ выкл", callback_data=f"ct:{key}"[:64]),),
                 (Btn("◀️ Города", callback_data="ci"), BACK))


def candidates_text() -> str:
    st, week = live.paper_stats(0), live.paper_stats(int(time.time()) - 7 * 86400)
    mode = ("⏸ выкл" if not runtime_state.get("alerts_enabled")
            else ("🕐 сводкой раз в час" if runtime_state.get("alerts_hourly") else "⚡ сразу"))
    lines = ["🎯 Кандидаты — цена стороны впервые дошла до уровня из правила для этого окна времени.",
             f"Сообщения: {mode}; правила: "
             + ("только самого города" if runtime_state.get("alerts_city_only") else "города и «все города»")
             + "; стороны: " + {"yes": "только YES", "no": "только NO"}.get(str(runtime_state.get("alerts_sides")), "YES и NO")
             + f"; тестовая ставка ${runtime_state.get('paper_stake'):g}.",
             f"Кандидат — только если по цене покупки ожидаемо от {runtime_state.get('alert_min_ev'):+g}% на ставку "
             "(по проценту выигрышей правила: правило «NO от 0.90» собрано из входов около 0.92, и по 0.99 те же "
             "98% — уже минус). Цена покупки — средняя по стакану на всю тестовую ставку; в стакане не хватает до "
             f"лучшей цены + {live.FILL_MAX_SLIP:g} — не кандидат."
             + _skipped_text(), ""]
    for name, s in (("Всего", st), ("За 7 дней", week)):
        line = f"{name}: {s['n']} {text.plural(s['n'], 'кандидат', 'кандидата', 'кандидатов')}"
        if s["settled"]:
            line += (f", с итогом {s['settled']}: выигр. {s['wins']} ({100 * s['wins'] / s['settled']:.0f}%), "
                     f"{s['pnl']:+.2f} $" + (f" ({100 * s['pnl'] / s['staked']:+.1f}%)" if s["staked"] else ""))
        lines.append(line)
    by_win = [(w, x) for w, x in live.paper_by_window(int(time.time()) - 7 * 86400) if x["settled"]]
    if by_win:
        lines += ["", "За 7 дней по окнам времени (с итогом):"]
        for w, x in by_win:
            lines.append(f"  {w}: {x['settled']}, выигр. {x['wins']} ({100 * x['wins'] / x['settled']:.0f}%), "
                         f"{x['pnl']:+.2f} $" + (f" ({100 * x['pnl'] / x['staked']:+.1f}%)" if x["staked"] else ""))
    recent = storage.candidates(limit=10)
    if recent:
        lines += ["", "Последние:"]
        for c in recent:
            res = "⏳" if c["outcome"] is None else ("🟢" if (c["outcome"] == 1) == (c["side"] == "YES") else "🔴")
            pnl = f" {c['pnl']:+.2f} $" if c["pnl"] is not None else ""
            lines.append(f"{res} {text.utc(c['ts'])} {c['city'] or '?'} {study.KIND_RU.get(c['kind'], '')} "
                         f"{(c['date'] or '')[8:10]}.{(c['date'] or '')[5:7]} «{c['bucket'] or c['idx']}» {c['side']} "
                         f"по {c['entry']:.3f}{pnl}")
    return "\n".join(lines)


def _skipped_text() -> str:
    st = live.stats()
    parts = [f"слабых по цене {st['weak']}" if st["weak"] else "",
             f"тонкий стакан {st['thin']}" if st["thin"] else ""]
    parts = [p for p in parts if p]
    return (" Отсеяно с запуска: " + ", ".join(parts) + ".") if parts else ""


def candidates_markup() -> InlineKeyboardMarkup:
    on, hourly = bool(runtime_state.get("alerts_enabled")), bool(runtime_state.get("alerts_hourly"))
    return _menu(
        (Btn(_mark(on and not hourly) + "⚡ сразу", callback_data="cam:now"),
         Btn(_mark(on and hourly) + "🕐 раз в час", callback_data="cam:hour"),
         Btn(_mark(not on) + "⏸ выкл", callback_data="cam:off")),
        (Btn(_mark(not runtime_state.get("alerts_city_only")) + "город + все города", callback_data="cac:0"),
         Btn(_mark(bool(runtime_state.get("alerts_city_only"))) + "только город", callback_data="cac:1")),
        tuple(Btn(_mark(runtime_state.get("alerts_sides") == k) + name, callback_data=f"cas:{k}")
              for k, name in (("both", "YES и NO"), ("yes", "только YES"), ("no", "только NO"))),
        tuple(Btn(_mark(abs(float(runtime_state.get("alert_min_ev")) - v) < 1e-9) + f"от {v:+g}%", callback_data=f"cae:{v:g}")
              for v in MIN_EV_PRESETS),
        (Btn("🔄 Обновить", callback_data="ca"), BACK))


def archive_text(head: str = "") -> str:
    c = storage.archive_counts()
    p = archive.progress()
    lines = ([head, ""] if head else []) + [
        archive_line(),
        f"Глубина: {runtime_state.get('archive_days')} дн.; докачка {'✅ вкл' if runtime_state.get('archive_enabled') else '⏸ выкл'}, "
        f"раз в {settings.ARCHIVE_REFRESH_HOURS:g} ч.",
        f"Закрытых событий найдено: {c['events']} (без истории: {c['status'].get('new', 0)}, ошибок: "
        f"{c['status'].get('error', 0)}); открытых в базе: {c['open']}."]
    vmin = float(runtime_state.get("min_event_volume"))
    big, small = storage.pending_by_volume(vmin)
    if big or small:
        lines.append(f"В очереди: {text.n_events(big)} с объёмом от {text.usd(vmin)} (качаются первыми — они и идут "
                     "в расчёт)" + (f" и {small} мелких." if small else "."))
    if c["fidelity"]:
        tot = sum(c["fidelity"].values())
        parts = ", ".join(f"{k} мин — {100 * v / tot:.0f}%" for k, v in sorted(c["fidelity"].items()))
        lines.append(f"Шаг истории цены: {parts}; корзин без истории: {c['no_path']}.")
    if p["started"]:
        lines.append(f"Последний проход: начат {text.utc(p['started'])} UTC"
                     + (f", закончен {text.utc(p['finished'])}" if p["finished"] and p["phase"] == "idle" else "")
                     + f"; найдено {p['found']}, новых {p['new']}, скачано {p['events_done']}, ошибок {p['errors']}.")
    if p["last_error"]:
        lines.append(f"Последняя ошибка: {p['last_error']}")
    st = gamma.strategy_stats()
    if st:
        lines.append("Поиск событий: " + "; ".join(
            f"{k} — {v['events']}" + (f" (ошибка: {v['error'][:60]})" if v["error"] else "") for k, v in st.items()))
    from src import http_api
    hs = http_api.stats()
    lines.append(f"Запросов к API: {hs['requests']}, повторов {hs['retries']}, ошибок {hs['errors']}.")
    return "\n".join(lines)


def archive_markup() -> InlineKeyboardMarkup:
    days = int(runtime_state.get("archive_days"))
    return _menu(
        (Btn("🔄 Докачать сейчас", callback_data="ars"),
         Btn("⏸ Пауза докачки" if runtime_state.get("archive_enabled") else "▶️ Включить докачку", callback_data="are")),
        tuple(Btn(_mark(days == d) + f"{d} дн.", callback_data=f"ard:{d}") for d in DAYS_PRESETS),
        (Btn("🔄 Обновить", callback_data="ar"), BACK))


def settings_text(head: str = "") -> str:
    return "\n".join(([head, ""] if head else []) + [
        "⚙️ Настройки",
        f"• Объём события от {text.usd(runtime_state.get('min_event_volume'))} — события поменьше в расчёт порогов "
        "не входят (тонкий стакан, цена мало что значит).",
        f"• Правило проверено, если ставок не меньше {runtime_state.get('rule_min_n')}, на старой части архива "
        f"(60% дней) выигрышей значимо больше окупаемости (p ≤ {settings.RULE_MAX_P:g}), на новой — в плюсе.",
        f"• Тестовая ставка на кандидата: ${runtime_state.get('paper_stake'):g}.",
        "Смена объёма или числа ставок — пороги пересчитаются сразу."])


def settings_markup() -> InlineKeyboardMarkup:
    vol, mn, stake = (float(runtime_state.get("min_event_volume")), int(runtime_state.get("rule_min_n")),
                      float(runtime_state.get("paper_stake")))
    return _menu(
        tuple(Btn(_mark(abs(vol - v) < 1e-9) + ("любой объём" if not v else f"от {text.usd(v)}"),
                  callback_data=f"sv:{v:g}") for v in VOLUME_PRESETS),
        tuple(Btn(_mark(mn == v) + f"≥{v} ставок", callback_data=f"sn:{v}") for v in MIN_N_PRESETS),
        tuple(Btn(_mark(abs(stake - v) < 1e-9) + f"${v:g}", callback_data=f"sp:{v:g}") for v in STAKE_PRESETS),
        (Btn("🔄 Обновить", callback_data="se"), BACK))


# ----------------------------------------------------------------- действия ----

async def _recompute(reason: str) -> None:
    try:
        sm = await asyncio.to_thread(study.run)
        await notify(f"📊 Пороги пересчитаны ({reason}): {text.n_bets(sm['bets'])} по {text.n_events(sm['events'])}, "
                     f"проверенных правил: {sm['rules']}.")
    except Exception as exc:  # noqa: BLE001
        await notify(f"⚠️ Пороги не пересчитались: {exc}"[:300])


def _float(s: str) -> float | None:
    try:
        return float(s)
    except ValueError:
        return None


async def handle(data: str) -> tuple[str, InlineKeyboardMarkup]:
    """Нажатие кнопки → (текст, кнопки) экрана."""
    parts = data.split(":", 1)
    cmd, arg = parts[0], (parts[1] if len(parts) > 1 else "")
    if cmd == "th":
        return thresholds_text(), thresholds_markup()
    if cmd == "tho":
        return overview_text(), thresholds_markup()
    if cmd == "thr":
        if study.is_running():
            return thresholds_text("Уже считаю — пришлю итог."), thresholds_markup()
        _spawn(_recompute("по кнопке"))
        return thresholds_text("♻️ Пересчитываю — пришлю итог сообщением."), thresholds_markup()
    if cmd == "ci":
        return cities_text(), cities_markup()
    if cmd == "cy" and arg:
        return city_text(arg), city_markup(arg)
    if cmd == "ct" and arg:
        on = runtime_state.toggle_city(arg)
        return city_text(arg, "🎯 Кандидаты по городу " + ("✅ включены." if on else "⏸ выключены.")), city_markup(arg)
    if cmd == "ca":
        return candidates_text(), candidates_markup()
    if cmd == "cat":
        runtime_state.set("alerts_enabled", not runtime_state.get("alerts_enabled"))
        return candidates_text(), candidates_markup()
    if cmd == "cam" and arg in ("now", "hour", "off"):
        runtime_state.set("alerts_enabled", arg != "off")
        if arg != "off":
            runtime_state.set("alerts_hourly", arg == "hour")
        return candidates_text(), candidates_markup()
    if cmd == "cac" and arg in ("0", "1"):
        runtime_state.set("alerts_city_only", arg == "1")
        return candidates_text(), candidates_markup()
    if cmd == "cae":
        v = _float(arg)
        if v is not None and any(abs(v - x) < 1e-9 for x in MIN_EV_PRESETS):
            runtime_state.set("alert_min_ev", v)
        return candidates_text(), candidates_markup()
    if cmd == "cas" and arg in ("both", "yes", "no"):
        runtime_state.set("alerts_sides", arg)
        return candidates_text(), candidates_markup()
    if cmd == "ar":
        return archive_text(), archive_markup()
    if cmd == "ars":
        if archive.is_running():
            return archive_text("Докачка уже идёт."), archive_markup()
        if not runtime_state.get("archive_enabled"):
            runtime_state.set("archive_enabled", True)
        archive.request_sync()
        return archive_text("🔄 Запустил докачку."), archive_markup()
    if cmd == "are":
        runtime_state.set("archive_enabled", not runtime_state.get("archive_enabled"))
        if runtime_state.get("archive_enabled"):
            archive.request_sync()
        return archive_text(), archive_markup()
    if cmd == "ard" and arg.isdigit() and int(arg) in DAYS_PRESETS:
        head = ""
        if int(arg) != int(runtime_state.get("archive_days")):
            runtime_state.set("archive_days", int(arg))
            archive.request_sync()
            head = f"Глубина архива: {arg} дн. — докачиваю недостающее."
        return archive_text(head), archive_markup()
    if cmd == "se":
        return settings_text(), settings_markup()
    if cmd in ("sv", "sn", "sp"):
        v = _float(arg)
        head = ""
        if cmd == "sv" and v is not None and any(abs(v - x) < 1e-9 for x in VOLUME_PRESETS) \
                and abs(v - float(runtime_state.get("min_event_volume"))) > 1e-9:
            runtime_state.set("min_event_volume", v)
            _spawn(_recompute(f"объём от {text.usd(v)}"))
            head = "Пересчитываю пороги — пришлю итог."
        elif cmd == "sn" and v is not None and int(v) in MIN_N_PRESETS and int(v) != int(runtime_state.get("rule_min_n")):
            runtime_state.set("rule_min_n", int(v))
            _spawn(_recompute(f"не меньше {int(v)} ставок"))
            head = "Пересчитываю пороги — пришлю итог."
        elif cmd == "sp" and v is not None and any(abs(v - x) < 1e-9 for x in STAKE_PRESETS):
            runtime_state.set("paper_stake", v)
        return settings_text(head), settings_markup()
    if cmd == "fx":
        from src import reporting
        _spawn(reporting.send_files())
        return "📤 Собираю файлы — пришлю следом (расчётные ставки считаются минуту-другую).\n\n" + main_text(), \
            _menu((Btn("🗂 Пути цены по месяцам", callback_data="fxp"),), (BACK,))
    if cmd == "fxp":
        from src import reporting
        _spawn(reporting.send_paths())
        return "🗂 Собираю пути цены по месяцам — пришлю следом.\n\n" + main_text(), main_markup()
    return main_text(), main_markup()


def _owner_chat_id() -> int | None:
    try:
        return int(str(settings.TELEGRAM_CHAT_ID).strip())
    except (TypeError, ValueError):
        return None


def _is_owner(update: Update) -> bool:
    owner = _owner_chat_id()
    if owner is None:
        return True
    chat = getattr(update, "effective_chat", None)
    return chat is not None and getattr(chat, "id", None) == owner


async def _cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(_fit(main_text()), reply_markup=main_markup())


async def _cmd_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(_fit(thresholds_text()), reply_markup=thresholds_markup())


async def _on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not _is_owner(update):
        return
    body, markup = await handle(query.data or "m")
    try:
        await query.edit_message_text(_fit(body), reply_markup=markup)
    except Exception as exc:  # noqa: BLE001 — «message is not modified»
        if "not modified" not in str(exc).lower():
            raise


def build_app() -> Application:
    global _app
    _app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    for name in ("start", "menu", "status"):
        _app.add_handler(CommandHandler(name, _cmd_menu))
    _app.add_handler(CommandHandler("rules", _cmd_rules))
    _app.add_handler(CallbackQueryHandler(_on_callback))
    return _app


def startup_text() -> str:
    c = storage.archive_counts()
    lines = ["🤖 Погодный бот запущен — 📚 архив температурных рынков, 📊 пороги, 🎯 кандидаты (без реальных ставок).",
             archive_line()]
    if not c["status"].get("done"):
        lines.append(f"Качаю архив за {runtime_state.get('archive_days')} дн. — в первый раз это часы; пороги "
                     "посчитаются по ходу, пришлю итог.")
    if storage.DB_WAS_NEW:
        lines.append(f"⚠️ База создана заново ({settings.DB_PATH}). Без постоянного тома (Persistent Storage на "
                     "/app/data) архив будет качаться заново после каждого деплоя.")
    lines.append("/menu — управление.")
    return "\n".join(lines)


async def notify(msg: str) -> None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID or _app is None:
        return
    await _app.bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=_fit(msg))


async def send_document(path: str, caption: str | None) -> None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID or _app is None:
        return
    with open(path, "rb") as f:
        await _app.bot.send_document(chat_id=settings.TELEGRAM_CHAT_ID, document=f, caption=caption)
