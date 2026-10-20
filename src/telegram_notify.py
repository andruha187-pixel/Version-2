"""
Телеграм: кнопочное меню бота серий 15m и уведомления.

/menu:
  🎲 Стратегия — вкл/выкл, правило входа, серия от, порог «ведёт от» (для «на 60-й с»)
                 или «цена до» (для «дёшево»), ставка, дневной стоп
  🪙 Монеты    — у каждой: ✅ вкл / ⏸ выкл и 🧪 тест / 🔴 реально
  📊 Статистика · 📤 Файлы · 📚 История и сверка
  🔴 Включить LIVE (с подтверждением) / 🧪 Выключить LIVE

Реальная ставка уходит, только если монета включена и 🔴, а бот в 🔴 LIVE.
Изменения пишутся в runtime_state (SQLite) и переживают рестарт, если база
на постоянном томе. Управлять может только чат из TELEGRAM_CHAT_ID.
"""
from __future__ import annotations
import logging
import time

from telegram import Update, InlineKeyboardButton as Btn, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import settings
from src import runtime_state, series_bot, storage

log = logging.getLogger("telegram")

_app: Application | None = None
TG_TEXT_LIMIT = 4096


def _fit(text: str) -> str:
    """Не длиннее лимита Telegram (иначе экран не откроется вовсе)."""
    return text if len(text) <= TG_TEXT_LIMIT else text[:TG_TEXT_LIMIT - 2].rstrip() + "\n…"


def _menu(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([list(r) for r in rows])


BACK = Btn("◀️ Назад", callback_data="m")


# ------------------------------------------------------------ главное меню ----

def coins_line() -> str:
    return " · ".join(f"{a.upper()} {'✅' if runtime_state.is_asset_enabled(a) else '⏸'}"
                      f"{'🔴' if runtime_state.is_asset_live(a) else '🧪'}" for a in settings.ASSETS)


def _day_line() -> str:
    day = int(time.time() // 86400) * 86400

    def part(st: dict) -> str:
        if not st["bets"]:
            return "ставок нет"
        s = f"{st['bets']} ставок"
        if st["settled"]:
            s += f", выигр. {st['wins']}/{st['settled']}, {st['pnl']:+.2f} $"
        return s
    test, real = series_bot.trade_stats(day, live=False), series_bot.trade_stats(day, live=True)
    line = f"Сегодня (UTC): тест — {part(test)}"
    if real["bets"] or series_bot.live_coins():
        line += f"; реальные — {part(real)}"
    return line


def main_text() -> str:
    stop = series_bot.trade_stop()
    lines = [
        "🎲 Серии 15m — ставка на разворот после серии одинаковых исходов",
        "",
        "Режим: " + series_bot.mode_text(),
        f"Стратегия: {'✅ включена' if series_bot.trade_enabled() else '⏸ выключена'} — после "
        f"{series_bot.trade_k()}+ подряд, {series_bot.rule_short()}; "
        f"${series_bot.trade_usdc():g} (не меньше 5 акций)"
        + (f", дневной стоп −${stop:g}" if stop > 0 else ", без дневного стопа"),
        "Монеты: " + coins_line(),
        _day_line(),
    ]
    why = series_bot.live_blocked() if series_bot.live_coins() and not runtime_state.get("dry_run") else None
    if why:
        lines.append(f"⚠️ Реальные ставки сейчас не уходят: {why}")
    if not settings.USE_LIVE_BOOK_STREAM:
        lines.append("⚠️ USE_LIVE_BOOK_STREAM=false — без живого стакана стратегия не работает.")
    return "\n".join(lines)


def main_markup() -> InlineKeyboardMarkup:
    live_btn = (Btn("🔴 Включить LIVE", callback_data="lv") if runtime_state.get("dry_run")
                else Btn("🧪 Выключить LIVE (в DRY RUN)", callback_data="lv"))
    return _menu(
        (Btn("🎲 Стратегия", callback_data="st"), Btn("🪙 Монеты", callback_data="co")),
        (Btn("📊 Статистика", callback_data="sta"), Btn("📤 Файлы", callback_data="fx")),
        (Btn("📚 История и сверка", callback_data="hi"),),
        (live_btn,),
        (Btn("🔄 Обновить", callback_data="m"),),
    )


# ----------------------------------------------------------------- монеты ----

def coins_text() -> str:
    lines = ["🪙 Монеты",
             "✅ — стратегия ставит по монете, ⏸ — нет (окна всё равно пишутся, чтобы знать серию).",
             "🧪 — только тестовые ставки, 🔴 — ещё и реальные, когда бот в 🔴 LIVE.",
             ""] + series_bot.coin_lines() + ["", "Режим: " + series_bot.mode_text()]
    return "\n".join(lines)


def coins_markup() -> InlineKeyboardMarkup:
    rows = []
    for a in settings.ASSETS:
        on, live = runtime_state.is_asset_enabled(a), runtime_state.is_asset_live(a)
        rows.append((Btn(f"{a.upper()} {'✅ вкл' if on else '⏸ выкл'}", callback_data=f"c:on:{a}"),
                     Btn(f"{a.upper()} {'🔴 реально' if live else '🧪 тест'}", callback_data=f"c:lv:{a}")))
    rows.append((Btn("🔄 Обновить", callback_data="co"), BACK))
    return _menu(*rows)


# --------------------------------------------------------------- стратегия ----

def strategy_text(head: str = "") -> str:
    lo = series_bot.lead_lo()
    lines = ([head, ""] if head else []) + series_bot.strategy_lines() + [""] + series_bot.variant_lines() + [
        "",
        "Тестовая ставка пишется по каждому окну с серией у включённых монет — по стакану (средняя цена "
        "по уровням не дороже потолка, с комиссией тейкера). Реальная — FAK-ордер на ту же сумму (не меньше "
        "5 акций — меньше биржа не берёт) по монетам с 🔴 в 🪙 Монетах, когда бот в 🔴 LIVE; свой дневной стоп.",
        f"Правило «на 60-й с»: на 60-й секунде окна смотрим цену стороны разворота — {lo:.2f}–0.80 (разворот уже "
        "ведёт): покупаем по цене не выше ask + 1¢, иначе пропуск. Порог — кнопками «от …»: по 16–20.10 "
        "входы по 0.52–0.56 дали 12 из 22 (около нуля), от 0.60 — 49 из 62.",
        "«Дёшево»: первая цена не выше потолка (кнопки «до …») в первую минуту окна.",
        "Сравнение входов — виртуальные ставки по тем же сигналам, но по другим правилам; реальных ордеров "
        "по ним нет. Считается с последней смены правила, серии или порога.",
    ]
    return "\n".join(lines)


def strategy_markup() -> InlineKeyboardMarkup:
    sb = series_bot
    k, cap, usdc, stop, rule = sb.trade_k(), sb.trade_cap(), sb.trade_usdc(), sb.trade_stop(), sb.trade_rule()
    lo = sb.lead_lo()
    mark = lambda ok: "✅ " if ok else ""          # noqa: E731
    # порог цены — только для выбранного правила: «ведёт от …» (на 60-й с) или «до …» (дёшево)
    price_row = (tuple(Btn(mark(abs(lo - v) < 1e-9) + f"от {v:.2f}", callback_data=f"stl:{v:.2f}")
                       for v in sb.LEAD_LO_PRESETS) if rule == "lead60"
                 else tuple(Btn(mark(abs(cap - v) < 1e-9) + f"до {v:.2f}", callback_data=f"stc:{v:.2f}")
                            for v in sb.TRADE_CAP_PRESETS))
    return _menu(
        (Btn("🎲 ✅ включена" if sb.trade_enabled() else "🎲 ⏸ выключена", callback_data="stt"),),
        (Btn(mark(rule == "lead60") + "вход на 60-й с", callback_data="str:lead60"),
         Btn(mark(rule == "cheap") + "вход дёшево", callback_data="str:cheap")),
        tuple(Btn(mark(k == v) + f"серия от {v}", callback_data=f"stk:{v}") for v in sb.TRADE_K_PRESETS),
        price_row,
        tuple(Btn(mark(abs(usdc - v) < 1e-9) + f"${v:g}", callback_data=f"stu:{v:g}") for v in sb.TRADE_USDC_PRESETS),
        tuple(Btn(mark(abs(stop - v) < 1e-9) + (f"стоп ${v:g}" if v else "без стопа"), callback_data=f"sts:{v:g}")
              for v in sb.TRADE_STOP_PRESETS),
        (Btn("🔄 Обновить", callback_data="st"), BACK),
    )


# ----------------------------------------------------- история и запись окон ----

def history_text(head: str = "") -> str:
    from src import history_study
    coins = ", ".join(a.upper() for a in settings.ASSETS)
    lines = ([head, ""] if head else []) + [
        "📚 История и сверка — только расчёт, ничего не покупает; итог придёт сообщением с файлом.",
        f"🧾 Сверить с исходами Polymarket: официальные исходы 15m за {settings.SERIES_OFFICIAL_DAYS} дн. "
        f"({coins}) — разворот после серий на самих исходах Polymarket. 5–15 минут.",
        f"📚 Год свечей Binance: окна 15m за {settings.SERIES_HISTORY_DAYS} дн. — развороты после серий "
        "и планы с удвоением. Минут 5.",
        "",
        series_bot.recorder_line(),
        "Пока 🎲 стратегия включена, окна пишутся всегда (по ним считается серия); кнопка «🔁 Запись окон» — "
        "чтобы писать их и при выключенной стратегии.",
    ]
    status = history_study.status_text()
    if status:
        lines += ["", status]
    return "\n".join(lines)


def history_markup() -> InlineKeyboardMarkup:
    on = series_bot.enabled()
    return _menu(
        (Btn("🧾 Сверить с исходами Polymarket (15m)", callback_data="hio"),),
        (Btn("📚 Год свечей Binance (15m)", callback_data="hib"),),
        (Btn("🔁 Запись окон: ✅ вкл" if on else "🔁 Запись окон: ⏸ выкл", callback_data="rec"),),
        (Btn("🔄 Обновить", callback_data="hi"), BACK),
    )


# ---------------------------------------------------------------- режим LIVE ----

def confirm_live_text() -> str:
    sb = series_bot
    coins = sb.live_coins()
    lines = ["⚠️ Включить LIVE? Бот начнёт ставить реальные деньги на Polymarket.", ""]
    if coins:
        stop = sb.trade_stop()
        lines.append(f"Реальные ставки пойдут по: {', '.join(a.upper() for a in coins)} — ${sb.trade_usdc():g} "
                     f"(не меньше 5 акций), {sb.rule_text()}"
                     + (f", дневной стоп −${stop:g}." if stop > 0 else ", без дневного стопа."))
        test = [a.upper() for a in sb.trade_assets() if a not in coins]
        if test:
            lines.append(f"Остальные включённые монеты ({', '.join(test)}) — только тест.")
        if not sb.trade_enabled():
            lines.append("⚠️ Стратегия сейчас ⏸ выключена — ставок не будет, пока не включишь в 🎲.")
    else:
        lines.append("Ни одна включённая монета не помечена 🔴 — реальных ставок не будет, пока не пометишь "
                     "в 🪙 Монетах.")
    return "\n".join(lines)


def _confirm_live_markup() -> InlineKeyboardMarkup:
    return _menu((Btn("✅ Да, включить LIVE", callback_data="lvy"), Btn("❌ Отмена", callback_data="m")))


NO_KEY_TEXT = ("❌ Нельзя включить LIVE: POLY_PRIVATE_KEY не задан в переменных окружения. "
               "Добавь ключ и передеплой бота.")


# -------------------------------------------------------------- обработка ----

async def handle(data: str) -> tuple[str, InlineKeyboardMarkup]:
    """Нажатие кнопки → (текст, кнопки) экрана, который показать."""
    sb = series_bot
    parts = data.split(":")
    cmd = parts[0]
    if cmd == "co":
        return coins_text(), coins_markup()
    if cmd == "c" and len(parts) == 3 and parts[2] in settings.ASSETS:
        asset, head = parts[2], ""
        if parts[1] == "on":
            on = runtime_state.toggle_asset(asset)
            head = f"{asset.upper()}: {'✅ включена' if on else '⏸ выключена — ставок по ней не будет'}.\n\n"
        elif parts[1] == "lv":
            live = not runtime_state.is_asset_live(asset)
            runtime_state.set_asset_live(asset, live)
            if live:
                head = f"{asset.upper()}: 🔴 реальные ставки"
                if runtime_state.get("dry_run"):
                    head += " — пойдут после «🔴 Включить LIVE» в главном меню"
                if not runtime_state.is_asset_enabled(asset):
                    head += "; монета ⏸ выключена — сначала включи"
                head += ".\n\n"
            else:
                head = f"{asset.upper()}: 🧪 только тест.\n\n"
        return head + coins_text(), coins_markup()
    if cmd == "st":
        return strategy_text(), strategy_markup()
    if cmd == "stt":
        runtime_state.set("series_trade_enabled", not sb.trade_enabled())
        return strategy_text(), strategy_markup()
    if cmd == "str" and len(parts) == 2:
        head = ""
        if parts[1] in sb.RULES and parts[1] != sb.trade_rule():
            runtime_state.set("series_trade_rule", parts[1])
            sb.mark_rule_change()
            head = f"Правило входа: {sb.rule_short()}. Сравнение входов считается заново с этого момента."
        return strategy_text(head), strategy_markup()
    if cmd in ("stk", "stc", "stl", "stu", "sts") and len(parts) == 2:
        try:
            value = float(parts[1])
        except ValueError:
            return strategy_text(), strategy_markup()
        key, presets, current = {
            "stk": ("series_trade_k", sb.TRADE_K_PRESETS, sb.trade_k()),
            "stc": ("series_trade_cap", sb.TRADE_CAP_PRESETS, sb.trade_cap()),
            "stl": ("series_lead_lo", sb.LEAD_LO_PRESETS, sb.lead_lo()),
            "stu": ("series_trade_usdc", sb.TRADE_USDC_PRESETS, sb.trade_usdc()),
            "sts": ("series_trade_stop", sb.TRADE_STOP_PRESETS, sb.trade_stop())}[cmd]
        head = ""
        if any(abs(value - v) < 1e-9 for v in presets) and abs(value - current) > 1e-9:
            runtime_state.set(key, int(value) if cmd == "stk" else value)
            if cmd in ("stk", "stc", "stl"):        # меняют, в какие окна входим, — сравнение с нуля
                sb.mark_rule_change()
                head = ((f"Порог: разворот ведёт от {value:.2f}." if cmd == "stl"
                         else f"Серия от {int(value)}." if cmd == "stk" else f"Цена до {value:.2f}.")
                        + " Сравнение входов считается заново с этого момента.")
        return strategy_text(head), strategy_markup()
    if cmd == "sta":
        return sb.stats_text(), _menu((Btn("🔄 Обновить", callback_data="sta"), BACK))
    if cmd == "fx":
        from src import reporting
        try:
            n = await reporting.send_series_all()
            head = ("📤 Отправил файлы: окна, ставки и варианты входа — всё, что в базе." if n
                    else "📤 В базе пока пусто — нечего выгружать.")
        except Exception as exc:  # noqa: BLE001
            head = f"⚠️ Файлы не ушли: {exc}"[:300]
        return head + "\n\n" + main_text(), main_markup()
    if cmd == "hi":
        return history_text(), history_markup()
    if cmd in ("hio", "hib"):
        from src import history_study
        coins = ", ".join(a.upper() for a in settings.ASSETS)
        if cmd == "hio" and history_study.start_official():
            head = (f"🧾 Запустил: официальные исходы Polymarket 15m за {settings.SERIES_OFFICIAL_DAYS} дн. ({coins}) "
                    "и свечи Binance по тем же окнам. 5–15 минут — пришлю итог и файл.")
        elif cmd == "hib" and history_study.start():
            head = (f"📚 Запустил: свечи Binance 15m за {settings.SERIES_HISTORY_DAYS} дн. ({coins}). "
                    "Минут 5 — пришлю итог и файл. Бот в это время работает как обычно.")
        else:
            head = "Расчёт уже идёт."
        return history_text(head), history_markup()
    if cmd == "rec":
        sb.set_enabled(not sb.enabled())
        return history_text(), history_markup()
    if cmd == "lv":
        if not runtime_state.get("dry_run"):
            runtime_state.set("dry_run", True)
            return "🧪 Переключено в DRY RUN — реальные ставки остановлены.\n\n" + main_text(), main_markup()
        if not settings.POLY_PRIVATE_KEY:
            return NO_KEY_TEXT, main_markup()
        return confirm_live_text(), _confirm_live_markup()
    if cmd == "lvy":
        if not settings.POLY_PRIVATE_KEY:
            return NO_KEY_TEXT, main_markup()
        runtime_state.set("dry_run", False)
        return "🔴 LIVE включён.\n\n" + main_text(), main_markup()
    return main_text(), main_markup()


def _owner_chat_id() -> int | None:
    try:
        return int(str(settings.TELEGRAM_CHAT_ID).strip())
    except (TypeError, ValueError):
        return None


def _is_owner(update: Update) -> bool:
    """Управлять ботом может только чат из TELEGRAM_CHAT_ID (туда же идут уведомления).
    Не задан — без ограничения, чтобы не запереть владельца."""
    owner = _owner_chat_id()
    if owner is None:
        return True
    chat = getattr(update, "effective_chat", None)
    return chat is not None and getattr(chat, "id", None) == owner


async def _cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(_fit(main_text()), reply_markup=main_markup())


async def _cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(_fit(series_bot.stats_text()))


async def _on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not _is_owner(update):
        return
    text, markup = await handle(query.data or "m")
    try:
        await query.edit_message_text(_fit(text), reply_markup=markup)
    except Exception as exc:  # noqa: BLE001 — повторное нажатие той же кнопки: «message is not modified»
        if "not modified" not in str(exc).lower():
            raise


def build_app() -> Application:
    global _app
    _app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    for name in ("start", "menu", "status"):
        _app.add_handler(CommandHandler(name, _cmd_menu))
    _app.add_handler(CommandHandler("stats", _cmd_stats))
    _app.add_handler(CallbackQueryHandler(_on_callback))
    return _app


def startup_text(forced_dry: bool = False) -> str:
    lines = ["🤖 Бот запущен — 🎲 серии 15m (ставка на разворот после серии).", "Режим: " + series_bot.mode_text(),
             "Монеты: " + coins_line() + " (✅ вкл, 🧪 тест, 🔴 реально)",
             "Стратегия: " + ("✅ " if series_bot.trade_enabled() else "⏸ выключена, ")
             + f"после {series_bot.trade_k()}+ подряд — {series_bot.rule_text()}, ${series_bot.trade_usdc():g}."]
    if forced_dry:
        lines.append("⚠️ Был сохранён LIVE, но POLY_PRIVATE_KEY не задан — откатил в DRY RUN.")
    if storage.DB_WAS_NEW:
        lines.append(f"⚠️ База создана заново ({settings.DB_PATH}). Если бот уже работал — данные не сохраняются "
                     "между деплоями: подключи в Coolify постоянный том (Persistent Storage) на /app/data. "
                     "Сейчас режим, монеты и настройки — по умолчанию, статистика пустая.")
    if not settings.USE_LIVE_BOOK_STREAM:
        lines.append("⚠️ USE_LIVE_BOOK_STREAM=false — без живого стакана стратегия не работает.")
    lines.append("/menu — управление.")
    return "\n".join(lines)


async def notify(text: str) -> None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID or _app is None:
        return
    await _app.bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=_fit(text))


async def send_document(path: str, caption: str | None) -> None:
    """Файл (CSV-отчёт и т.п.) в чат."""
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID or _app is None:
        return
    with open(path, "rb") as f:
        await _app.bot.send_document(chat_id=settings.TELEGRAM_CHAT_ID, document=f, caption=caption)
