"""
tg_bot.py — Telegram control panel for Hibachi Market Maker Bot.

Единственная точка входа: /start (или /menu).
ВСЯ навигация — через inline-кнопки, команды не нужны.

Структура меню:
  ┌──────────────────────────────────┐
  │  🤖 Hibachi MM Bot               │
  │  Статус: 🟢 Запущен             │
  │  Пары: BTC/USDT-P, HYPE/USDT-P │
  ├──────────────────────────────────┤
  │  [📊 Мониторинг]                 │
  │  [📈 Позиции]                    │
  │  [⚙️ Торговые пары]              │
  │  [▶️ Старт] [⏹ Стоп] [🔄 Рестарт]│
  └──────────────────────────────────┘

Required .env variables:
  TELEGRAM_BOT_TOKEN      — token from @BotFather
  TELEGRAM_ALLOWED_USERS  — comma-separated Telegram user IDs (empty = allow all)
  TELEGRAM_AUTOSTART_BOT  — true/false: auto-start main.py on TG bot launch (default false)
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv, set_key
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from env_config import load_env_config
from hibachi_client import HibachiRest

log = logging.getLogger("tg_bot")

# ─── Global state ─────────────────────────────────────────────────────────────

_bot_process: Optional[subprocess.Popen] = None
_rest: Optional[HibachiRest] = None
_cfg: Dict = {}
_allowed_users: Set[int] = set()

# Keys for bot_data (per-application shared storage)
KEY_PENDING   = "tg_pending_symbols"   # Set[str] — toggled pairs not yet applied
KEY_CONTRACTS = "tg_contracts_cache"   # List[Dict] — cached contract list with prices

# ─── REST client ──────────────────────────────────────────────────────────────

def get_rest() -> HibachiRest:
    global _rest
    if _rest is None:
        api = _cfg["api"]
        _rest = HibachiRest(
            api_url=api["apiUrl"],
            data_api_url=api["dataApiUrl"],
            api_key=api["apiKey"],
            account_id=api["accountId"],
            private_key=api["privateKey"],
        )
    return _rest


# ─── Subprocess management ────────────────────────────────────────────────────

def _bot_alive() -> bool:
    return _bot_process is not None and _bot_process.poll() is None


def _start_bot() -> bool:
    global _bot_process
    if _bot_alive():
        return False
    env = os.environ.copy()
    _bot_process = subprocess.Popen(
        [sys.executable, "main.py"],
        cwd=str(Path(__file__).parent),
        env=env,
    )
    log.info("Main bot started (PID %s)", _bot_process.pid)
    return True


def _stop_bot() -> bool:
    global _bot_process
    if not _bot_alive():
        _bot_process = None
        return False
    _bot_process.terminate()
    try:
        _bot_process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        _bot_process.kill()
        _bot_process.wait()
    log.info("Main bot stopped")
    _bot_process = None
    return True


# ─── Authorization ────────────────────────────────────────────────────────────

def _is_authorized(update: Update) -> bool:
    if not _allowed_users:
        return True
    user = update.effective_user
    return user is not None and user.id in _allowed_users


def authorized(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not _is_authorized(update):
            if update.effective_message:
                await update.effective_message.reply_text("⛔ Доступ запрещён.")
            return
        return await func(update, ctx)
    wrapper.__name__ = func.__name__
    return wrapper


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _bot_status_line() -> str:
    return "🟢 Запущен" if _bot_alive() else "🔴 Остановлен"


def _active_symbols_str() -> str:
    syms = _cfg["bot"].get("symbols", [_cfg["bot"]["symbol"]])
    return ", ".join(syms)


def _format_pos(p: Dict) -> str:
    sym      = p.get("symbol", "?")
    size     = float(p.get("size") or p.get("quantity") or 0)
    entry    = float(p.get("entryPrice") or p.get("entry_price") or 0)
    mark     = float(p.get("markPrice") or p.get("mark_price") or 0)
    upnl     = float(p.get("unrealizedPnl") or p.get("unrealized_pnl") or 0)
    side     = "LONG" if size > 0 else "SHORT"
    notional = abs(size) * mark
    sign     = "+" if upnl >= 0 else ""
    return (
        f"• `{sym}` {side} {abs(size):.4f}\n"
        f"  Вход: ${entry:.2f} → Марк: ${mark:.2f}\n"
        f"  PnL: `{sign}${upnl:.2f}` | Объём: `${notional:,.0f}`"
    )


async def _run(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)


async def _safe_edit(msg: Message, text: str, markup=None, md: bool = True):
    """Edit message text, swallowing 'message not modified' errors."""
    try:
        kwargs: Dict = {"parse_mode": "Markdown"} if md else {}
        if markup:
            kwargs["reply_markup"] = markup
        await msg.edit_text(text, **kwargs)
    except Exception as e:
        if "not modified" not in str(e).lower():
            raise


# ─── Main menu ────────────────────────────────────────────────────────────────

def _main_menu_markup() -> InlineKeyboardMarkup:
    alive = _bot_alive()
    bot_row = []
    if not alive:
        bot_row.append(InlineKeyboardButton("▶️ Запустить",  callback_data="bot_start"))
    else:
        bot_row.append(InlineKeyboardButton("⏹ Остановить", callback_data="bot_stop"))
        bot_row.append(InlineKeyboardButton("🔄 Рестарт",   callback_data="bot_restart"))

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Мониторинг",    callback_data="show_status")],
        [InlineKeyboardButton("📈 Позиции",       callback_data="show_positions")],
        [InlineKeyboardButton("⚙️ Торговые пары", callback_data="show_pairs")],
        [InlineKeyboardButton("🏆 Лидерборд",     url="https://hibachi.xyz/leaderboard")],
        bot_row,
    ])


def _main_menu_text() -> str:
    return (
        "🤖 *Hibachi MM Bot*\n"
        f"Статус: {_bot_status_line()}\n"
        f"Пары: `{_active_symbols_str()}`"
    )


async def _show_menu(target: Message, edit: bool = False):
    text   = _main_menu_text()
    markup = _main_menu_markup()
    if edit:
        await _safe_edit(target, text, markup)
    else:
        await target.reply_text(text, parse_mode="Markdown", reply_markup=markup)


# ─── /start — единственная команда, открывает главное меню ───────────────────

@authorized
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _show_menu(update.message, edit=False)


# ─── Callback router ──────────────────────────────────────────────────────────

@authorized
async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data
    msg   = query.message

    # ── Главное меню ─────────────────────────────────────────────────────────
    if data == "menu":
        await _show_menu(msg, edit=True)

    # ── Мониторинг ───────────────────────────────────────────────────────────
    elif data in ("show_status", "refresh_status"):
        await _render_status(msg)

    # ── Позиции ──────────────────────────────────────────────────────────────
    elif data in ("show_positions", "refresh_positions"):
        await _render_positions(msg)

    elif data.startswith("close:"):
        await _do_close_position(msg, data[6:])

    elif data == "close_all":
        await _do_close_all(msg)

    # ── Пары ─────────────────────────────────────────────────────────────────
    elif data == "show_pairs":
        await _load_and_render_pairs(msg, ctx)

    elif data.startswith("toggle:"):
        sym = data[7:]
        pending: Set[str] = ctx.bot_data.get(KEY_PENDING, set())
        if sym in pending:
            pending.discard(sym)
            if not pending:
                pending.add(sym)   # не позволяем снять все галочки
        else:
            pending.add(sym)
        ctx.bot_data[KEY_PENDING] = pending
        await _render_pairs_menu(msg, ctx)

    elif data == "apply_pairs":
        await _do_apply_pairs(msg, ctx)

    # ── Управление ботом ─────────────────────────────────────────────────────
    elif data == "bot_start":
        await _cb_bot_start(msg)

    elif data == "bot_stop":
        await _cb_bot_stop(msg)

    elif data == "bot_restart":
        await _cb_bot_restart(msg)


# ─── Экран мониторинга ───────────────────────────────────────────────────────

async def _render_status(msg: Message):
    await _safe_edit(msg, "⏳ Получаю данные...")
    try:
        rest = get_rest()
        balance, account, positions, trades = await asyncio.gather(
            _run(rest.get_capital_balance),
            _run(rest.get_account_info),
            _run(rest.get_positions),
            _run(rest.get_account_trades),
        )

        unrealized_pnl = float(account.get("totalUnrealizedPnl") or account.get("totalUnrealizedTradingPnl") or 0)
        pnl_sign = "+" if unrealized_pnl >= 0 else ""

        recent_volume = sum(
            float(t.get("price", 0)) * abs(float(t.get("quantity", 0)))
            for t in trades
            if t.get("price") and t.get("quantity")
        )
        recent_pnl = sum(
            float(t.get("realizedPnl", 0))
            for t in trades
            if t.get("realizedPnl")
        )
        recent_pnl_sign = "+" if recent_pnl >= 0 else ""
        n_trades = len(trades)

        text = (
            f"📊 *Мониторинг*\n\n"
            f"Статус бота: {_bot_status_line()}\n"
            f"Пары: `{_active_symbols_str()}`\n\n"
            f"💰 Баланс: `${balance:,.2f}`\n"
            f"📉 Unrealized PnL: `{pnl_sign}${unrealized_pnl:,.2f}`\n"
            f"📦 Объём (≈{n_trades} тр.): `${recent_volume:,.0f}`\n"
            f"💹 Realized PnL (≈{n_trades} тр.): `{recent_pnl_sign}${recent_pnl:,.2f}`\n\n"
        )

        active = [p for p in positions if float(p.get("size") or p.get("quantity") or 0) != 0]
        if active:
            text += "📈 *Позиции*\n" + "\n".join(_format_pos(p) for p in active)
        else:
            text += "📈 *Позиции:* нет"

        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить", callback_data="refresh_status")],
            [InlineKeyboardButton("🏆 Лидерборд", url="https://hibachi.xyz/leaderboard")],
            [InlineKeyboardButton("← Меню",      callback_data="menu")],
        ])
        await _safe_edit(msg, text, markup)

    except Exception as e:
        log.error("Status error: %s", e, exc_info=True)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="menu")]])
        await _safe_edit(msg, f"❌ Ошибка: {e}", markup, md=False)


# ─── Экран позиций ───────────────────────────────────────────────────────────

async def _render_positions(msg: Message):
    await _safe_edit(msg, "⏳ Загружаю позиции...")
    try:
        positions = await _run(get_rest().get_positions)
        active = [p for p in positions if float(p.get("size") or p.get("quantity") or 0) != 0]

        keyboard = []
        if not active:
            text = "📈 *Позиции*\n\nОткрытых позиций нет."
        else:
            lines = ["📈 *Открытые позиции*\n"]
            for p in active:
                lines.append(_format_pos(p))
                sym  = p.get("symbol", "?")
                size = float(p.get("size") or p.get("quantity") or 0)
                side = "LONG" if size > 0 else "SHORT"
                keyboard.append([InlineKeyboardButton(
                    f"❌ Закрыть {sym} {side}", callback_data=f"close:{sym}"
                )])
            keyboard.append([InlineKeyboardButton("⚠️ Закрыть ВСЕ", callback_data="close_all")])
            text = "\n".join(lines)

        keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="refresh_positions")])
        keyboard.append([InlineKeyboardButton("← Меню",      callback_data="menu")])

        await _safe_edit(msg, text, InlineKeyboardMarkup(keyboard))

    except Exception as e:
        log.error("Positions error: %s", e)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="menu")]])
        await _safe_edit(msg, f"❌ Ошибка: {e}", markup, md=False)


# ─── Закрытие позиций ────────────────────────────────────────────────────────

async def _do_close_position(msg: Message, symbol: str):
    await _safe_edit(msg, f"⏳ Закрываю {symbol}...")
    back = InlineKeyboardMarkup([
        [InlineKeyboardButton("← Позиции", callback_data="show_positions")],
        [InlineKeyboardButton("← Меню",    callback_data="menu")],
    ])
    try:
        rest = get_rest()
        positions = await _run(rest.get_positions)
        pos = next((p for p in positions if p.get("symbol") == symbol), None)

        if not pos:
            await _safe_edit(msg, f"⚠️ Позиция `{symbol}` не найдена.", back)
            return

        size = float(pos.get("size") or pos.get("quantity") or 0)
        if size == 0:
            await _safe_edit(msg, f"⚠️ Позиция `{symbol}` уже закрыта.", back)
            return

        close_side = "SELL" if size > 0 else "BUY"
        result = await _run(lambda: rest.close_position(symbol=symbol, size=size))
        order_id = result.get("orderId", "?")
        await _safe_edit(
            msg,
            f"✅ Позиция `{symbol}` закрыта\n"
            f"{close_side} {abs(size):.4f} | Ордер: `{order_id}`",
            back,
        )
    except Exception as e:
        log.error("Close position %s failed: %s", symbol, e)
        await _safe_edit(msg, f"❌ Ошибка: {e}", back, md=False)


async def _do_close_all(msg: Message):
    await _safe_edit(msg, "⏳ Закрываю все позиции...")
    back = InlineKeyboardMarkup([
        [InlineKeyboardButton("← Позиции", callback_data="show_positions")],
        [InlineKeyboardButton("← Меню",    callback_data="menu")],
    ])
    try:
        rest = get_rest()
        positions = await _run(rest.get_positions)
        active = [p for p in positions if float(p.get("size") or p.get("quantity") or 0) != 0]

        if not active:
            await _safe_edit(msg, "✅ Открытых позиций нет.", back)
            return

        async def close_one(pos):
            sym  = pos.get("symbol", "?")
            size = float(pos.get("size") or pos.get("quantity") or 0)
            try:
                await _run(lambda s=sym, sz=size: rest.close_position(symbol=s, size=sz))
                return f"✅ {sym}: закрыто"
            except Exception as e:
                return f"❌ {sym}: {e}"

        results = await asyncio.gather(*[close_one(p) for p in active])
        await _safe_edit(
            msg,
            "Результат закрытия всех позиций:\n" + "\n".join(results),
            back,
            md=False,
        )
    except Exception as e:
        log.error("Close all error: %s", e)
        await _safe_edit(msg, f"❌ Ошибка: {e}", back, md=False)


# ─── Экран выбора пар ────────────────────────────────────────────────────────

async def _load_and_render_pairs(msg: Message, ctx: ContextTypes.DEFAULT_TYPE):
    await _safe_edit(msg, "⏳ Загружаю список пар...")
    ctx.bot_data[KEY_PENDING] = set(_cfg["bot"].get("symbols", [_cfg["bot"]["symbol"]]))

    try:
        rest = get_rest()
        symbols_with_prices, positions = await asyncio.gather(
            _run(rest.get_symbols_with_prices),
            _run(rest.get_positions),
        )

        if symbols_with_prices:
            symbols: List[str] = list(symbols_with_prices.keys())
            prices: Dict[str, Optional[float]] = symbols_with_prices
        else:
            symbols = ["BTC/USDT-P", "ETH/USDT-P", "SOL/USDT-P", "BNB/USDT-P", "HYPE/USDT-P"]
            prices = {s: None for s in symbols}

        notional_map: Dict[str, float] = {}
        for p in positions:
            sym  = p.get("symbol", "")
            size = float(p.get("size") or p.get("quantity") or 0)
            mark = float(p.get("markPrice") or p.get("mark_price") or 0)
            if sym and size and mark:
                notional_map[sym] = abs(size) * mark

        ctx.bot_data[KEY_CONTRACTS] = {
            "symbols": symbols,
            "prices": prices,
            "notional": notional_map,
        }

    except Exception as e:
        log.error("Pairs load error: %s", e)
        ctx.bot_data[KEY_CONTRACTS] = {"symbols": [], "prices": {}, "notional": {}}

    await _render_pairs_menu(msg, ctx)


async def _render_pairs_menu(msg: Message, ctx: ContextTypes.DEFAULT_TYPE):
    pending: Set[str]                  = ctx.bot_data.get(KEY_PENDING, set())
    cache: Dict                        = ctx.bot_data.get(KEY_CONTRACTS, {})
    symbols: List[str]                 = cache.get("symbols", [])
    prices: Dict[str, Optional[float]] = cache.get("prices", {})
    notional: Dict[str, float]         = cache.get("notional", {})

    if not symbols:
        back = InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="menu")]])
        await _safe_edit(msg, "⚠️ Список контрактов недоступен.", back)
        return

    keyboard = []
    for sym in symbols:
        tick      = "✅" if sym in pending else "☐"
        price     = prices.get(sym)
        price_str = f"  ${price:,.2f}" if price else ""
        vol_str   = f"  [${notional[sym]:,.0f}]" if sym in notional else ""
        keyboard.append([InlineKeyboardButton(
            f"{tick} {sym}{price_str}{vol_str}",
            callback_data=f"toggle:{sym}",
        )])

    selected_str = ", ".join(sorted(pending)) if pending else "—"
    keyboard.append([InlineKeyboardButton("💾 Применить и перезапустить", callback_data="apply_pairs")])
    keyboard.append([InlineKeyboardButton("← Меню", callback_data="menu")])

    text = (
        "⚙️ *Торговые пары*\n\n"
        f"Выбрано: `{selected_str}`\n\n"
        "Цена — текущий марк-прайс\n"
        "\\[Объём\\] — ваш открытый нотиональный объём\n\n"
        "Нажмите пару для выбора/снятия, затем *Применить*."
    )
    try:
        await msg.edit_text(text, parse_mode="MarkdownV2",
                            reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        if "not modified" not in str(e).lower():
            log.warning("pairs menu edit: %s", e)


async def _do_apply_pairs(msg: Message, ctx: ContextTypes.DEFAULT_TYPE):
    pending: Set[str] = ctx.bot_data.get(KEY_PENDING, set())
    if not pending:
        await _safe_edit(msg, "⚠️ Не выбрано ни одной пары.")
        return

    sym_str = ",".join(sorted(pending))
    set_key(str(Path(__file__).parent / ".env"), "HIBACHI_SYMBOLS", sym_str)
    _cfg["bot"]["symbols"] = sorted(pending)
    _cfg["bot"]["symbol"]  = sorted(pending)[0]

    was_running = _bot_alive()
    await _run(_stop_bot)
    if was_running:
        await asyncio.sleep(2)
        _start_bot()
        note = "Бот перезапущен с новыми парами."
    else:
        note = "Бот не запущен — нажмите ▶️ Запустить в меню."

    back = InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="menu")]])
    await _safe_edit(msg, f"✅ Пары сохранены: `{sym_str}`\n{note}", back)


# ─── Управление ботом ────────────────────────────────────────────────────────

async def _cb_bot_start(msg: Message):
    if _bot_alive():
        note = "⚠️ Бот уже запущен."
    else:
        _start_bot()
        note = "🟢 Бот запущен."
    back = InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="menu")]])
    await _safe_edit(msg, note, back, md=False)


async def _cb_bot_stop(msg: Message):
    stopped = await _run(_stop_bot)
    note = "🔴 Бот остановлен." if stopped else "⚠️ Бот не был запущен."
    back = InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="menu")]])
    await _safe_edit(msg, note, back, md=False)


async def _cb_bot_restart(msg: Message):
    await _safe_edit(msg, "🔄 Перезапускаю...", md=False)
    await _run(_stop_bot)
    await asyncio.sleep(2)
    _start_bot()
    back = InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="menu")]])
    await _safe_edit(msg, "🔄 Бот перезапущен.", back, md=False)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    global _cfg, _allowed_users

    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        sys.exit("ERROR: TELEGRAM_BOT_TOKEN не задан в .env")

    raw_users = os.getenv("TELEGRAM_ALLOWED_USERS", "")
    if raw_users:
        _allowed_users = {
            int(u.strip()) for u in raw_users.split(",") if u.strip().isdigit()
        }
        log.info("Authorized users: %s", _allowed_users)
    else:
        log.warning("TELEGRAM_ALLOWED_USERS не задан — бот доступен всем!")

    _cfg.update(load_env_config())

    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        level=logging.INFO,
    )

    if os.getenv("TELEGRAM_AUTOSTART_BOT", "false").lower() in ("1", "true", "yes"):
        log.info("Auto-starting main bot...")
        _start_bot()

    app = Application.builder().token(token).build()

    # Единственная команда — открывает главное меню с кнопками
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu",  cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))

    log.info("Telegram bot запущен. Ctrl+C для остановки.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

