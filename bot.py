import html
import asyncio
import logging
import os
import warnings
from datetime import datetime

from telegram.warnings import PTBUserWarning
warnings.filterwarnings("ignore", category=PTBUserWarning, message=".*per_message=False.*")

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    PicklePersistence,
    filters,
    ContextTypes,
)
from telegram.error import BadRequest

from config import BOT_TOKEN, CHAT_ID, TOPIC_ID, SPREADSHEET_ID
import sheets
from hunter_utils import format_hunter_card, build_hunter_keyboard as _build_hunter_kb

logger = logging.getLogger(__name__)

SPREADSHEET_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit"

# ConversationHandler states
PARTNER_DESC, PARTNER_NEXT, PARTNER_CONFIRM = range(3)
NOTE_INPUT = 10

STATUS_MAP = {
    "test":     "На тесте",
    "postpone": "Отложено",
    "reject":   "Отклонено",
}

STATUS_EMOJI = {
    "Новая":       "🆕",
    "На тесте":    "✅",
    "Отложено":    "📅",
    "Связались":   "📞",
    "Отклонено":   "❌",
    "В партнерах": "🤝",
}


def h(text) -> str:
    return html.escape(str(text)) if text is not None else ""


def build_lead_keyboard(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ На тест",     callback_data=f"st:{lead_id}:test"),
            InlineKeyboardButton("📅 Отложить",    callback_data=f"st:{lead_id}:postpone"),
        ],
        [
            InlineKeyboardButton("❌ Отклонить",   callback_data=f"st:{lead_id}:reject"),
            InlineKeyboardButton("🤝 → Партнёры", callback_data=f"pt:{lead_id}"),
        ],
        [
            InlineKeyboardButton("📝 Заметка",     callback_data=f"nt:{lead_id}"),
            InlineKeyboardButton("📊 Таблица",     url=SPREADSHEET_URL),
        ],
    ])


def format_lead_message(
    lead_id: str,
    name: str,
    contact: str,
    company,
    budget,
    message: str,
    date: str,
    status: str = "Новая",
    notes: str = "",
) -> str:
    emoji = STATUS_EMOJI.get(status, "📊")
    text = (
        f"🌿 <b>Заявка #{h(lead_id)}</b>\n\n"
        f"👤 <b>Имя:</b> {h(name)}\n"
        f"📧 <b>Контакт:</b> {h(contact)}\n"
        f"🏢 <b>Компания:</b> {h(company or '—')}\n"
        f"💰 <b>Бюджет:</b> {h(budget or 'не указан')}\n\n"
        f"📝 <b>Сообщение:</b>\n{h(message)}\n\n"
        f"⏰ {h(date)}\n"
        f"📊 <b>Статус:</b> {emoji} {h(status)}"
    )
    if notes:
        text += f"\n💬 <b>Заметки:</b>\n{h(notes)}"
    return text


def _format_lead_short(r: dict) -> str:
    emoji = STATUS_EMOJI.get(r["status"], "📊")
    return (
        f"{emoji} <b>#{h(r['id'])}</b> — {h(r['name'])}\n"
        f"   📧 {h(r['contact'])}\n"
        f"   🕐 {h(r['date'])}  · {h(r['status'])}"
    )


# ── Notifications ──────────────────────────────────────────────────────────

async def send_lead_notification(
    lead_id: str, name: str, contact: str, company, budget, message: str, date: str
) -> None:
    text = format_lead_message(lead_id, name, contact, company, budget, message, date)
    msg = await application.bot.send_message(
        chat_id=CHAT_ID,
        message_thread_id=TOPIC_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=build_lead_keyboard(lead_id),
    )
    application.bot_data.setdefault("leads", {})[lead_id] = {
        "lead_id":    lead_id,
        "name":       name,
        "contact":    contact,
        "company":    company,
        "budget":     budget,
        "message":    message,
        "date":       date,
        "message_id": msg.message_id,
        "status":     "Новая",
        "notes":      "",
    }
    await application.update_persistence()


async def send_startup_summary() -> None:
    recent = await asyncio.to_thread(sheets.get_recent_leads, 3)
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    if not recent:
        text = f"🤖 <b>Бот запущен</b> · {h(now)}\n\nЗаявок ещё нет."
    else:
        leads_text = "\n\n".join(_format_lead_short(r) for r in recent)
        text = (
            f"🤖 <b>Бот запущен</b> · {h(now)}\n\n"
            f"<b>Последние заявки:</b>\n\n"
            f"{leads_text}"
        )

    await application.bot.send_message(
        chat_id=CHAT_ID,
        message_thread_id=TOPIC_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Открыть таблицу", url=SPREADSHEET_URL),
        ]]),
    )


# ── Internal helpers ───────────────────────────────────────────────────────

async def _edit_lead_message(
    context: ContextTypes.DEFAULT_TYPE,
    lead_id: str,
    status: str | None = None,
    notes: str | None = None,
) -> None:
    lead = context.bot_data.get("leads", {}).get(lead_id)
    if not lead:
        return
    if status is not None:
        lead["status"] = status
    if notes is not None:
        lead["notes"] = notes
    text = format_lead_message(
        lead["lead_id"], lead["name"], lead["contact"],
        lead["company"], lead["budget"], lead["message"],
        lead["date"], lead["status"], lead["notes"],
    )
    try:
        await context.bot.edit_message_text(
            chat_id=CHAT_ID,
            message_id=lead["message_id"],
            text=text,
            parse_mode="HTML",
            reply_markup=build_lead_keyboard(lead_id),
        )
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            raise


# ── Error handler ──────────────────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception", exc_info=context.error)
    if isinstance(update, Update):
        if update.callback_query:
            try:
                await update.callback_query.answer(
                    "⚠️ Что-то пошло не так. Попробуй ещё раз.", show_alert=True
                )
            except Exception:
                pass
        elif update.effective_message:
            try:
                await update.effective_message.reply_text("⚠️ Произошла ошибка. Попробуй ещё раз.")
            except Exception:
                pass


# ── Status change handler ──────────────────────────────────────────────────

async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, lead_id, status_key = query.data.split(":")
    status_label = STATUS_MAP[status_key]
    await asyncio.to_thread(sheets.update_lead_status, lead_id, status_label)
    await _edit_lead_message(context, lead_id, status=status_label)


# ── Partner conversation ───────────────────────────────────────────────────

async def partner_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    lead_id = query.data.split(":")[1]
    context.user_data["p_lead_id"] = lead_id
    await query.message.reply_text(
        f"Добавляем партнёра из заявки <b>#{h(lead_id)}</b>\n\n"
        "Опиши коротко, что это за партнёр:",
        parse_mode="HTML",
    )
    return PARTNER_DESC


async def partner_got_desc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["p_desc"] = update.message.text
    await update.message.reply_text("Что нужно делать дальше?")
    return PARTNER_NEXT


async def partner_got_next(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["p_next"] = update.message.text
    await update.message.reply_text(
        f"<b>Подтверди добавление партнёра:</b>\n\n"
        f"📋 Описание: {h(context.user_data['p_desc'])}\n"
        f"✅ Что делать: {h(context.user_data['p_next'])}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Добавить", callback_data="pconf:yes"),
            InlineKeyboardButton("❌ Отмена",   callback_data="pconf:no"),
        ]]),
    )
    return PARTNER_CONFIRM


async def partner_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "pconf:no":
        await query.edit_message_text("Отменено.")
        context.user_data.clear()
        return ConversationHandler.END

    lead_id = context.user_data["p_lead_id"]
    lead = context.bot_data.get("leads", {}).get(lead_id, {})
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    partner_id = await asyncio.to_thread(
        sheets.add_partner,
        lead.get("name", ""),
        lead.get("contact", ""),
        context.user_data["p_desc"],
        context.user_data["p_next"],
        now,
    )
    await asyncio.to_thread(sheets.update_lead_status, lead_id, "В партнерах")
    await _edit_lead_message(context, lead_id, status="В партнерах")

    await query.edit_message_text(
        f"✅ Партнёр добавлен!\n\n"
        f"ID: <b>{h(partner_id)}</b>\n"
        f"Имя: {h(lead.get('name', ''))}\n"
        f"Контакт: {h(lead.get('contact', ''))}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Открыть таблицу", url=SPREADSHEET_URL),
        ]]),
    )
    context.user_data.clear()
    return ConversationHandler.END


# ── Note conversation ──────────────────────────────────────────────────────

async def note_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    lead_id = query.data.split(":")[1]
    context.user_data["n_lead_id"] = lead_id
    await query.message.reply_text(
        f"Введи заметку для заявки <b>#{h(lead_id)}</b>:",
        parse_mode="HTML",
    )
    return NOTE_INPUT


async def note_got_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    lead_id = context.user_data["n_lead_id"]
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    stamped_note = f"[{now}] {update.message.text}"
    new_notes = await asyncio.to_thread(sheets.add_note, lead_id, stamped_note)
    await _edit_lead_message(context, lead_id, notes=new_notes)
    await update.message.reply_text("✅ Заметка добавлена!")
    context.user_data.clear()
    return ConversationHandler.END


# ── Commands ───────────────────────────────────────────────────────────────

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


HELP_TEXT = (
    "📖 <b>Инструкция — Fikus CRM-бот</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"

    "🌿 <b>Как приходят заявки</b>\n"
    "Когда кто-то заполняет форму на сайте — бот присылает карточку с именем, контактом, бюджетом и сообщением. "
    "К каждой карточке прикреплены кнопки управления и ссылка на таблицу.\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🔘 <b>Кнопки на карточке заявки</b>\n\n"

    "✅ <b>На тест</b> — берём в работу, тестируем сотрудничество\n"
    "📅 <b>Отложить</b> — не сейчас, вернёмся позже\n"
    "❌ <b>Отклонить</b> — не подходит\n"
    "🤝 <b>→ Партнёры</b> — переводим в партнёры (запустится диалог)\n"
    "📝 <b>Заметка</b> — добавить комментарий к заявке\n"
    "📊 <b>Таблица</b> — открыть Google Sheets одним нажатием\n\n"

    "Статус меняется <b>мгновенно</b> — и в карточке, и в таблице.\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🤝 <b>Как добавить партнёра</b>\n\n"
    "1. Нажми <b>→ Партнёры</b> на нужной карточке\n"
    "2. Бот спросит: кратко опиши партнёра\n"
    "3. Потом: что нужно делать дальше?\n"
    "4. Подтверди — данные уйдут в лист «Партнёры»,\n"
    "   а статус заявки сменится на «В партнёрах»\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "📋 <b>Статусы заявок</b>\n\n"
    "🆕 Новая → только пришла\n"
    "✅ На тесте → в работе\n"
    "📅 Отложено → вернёмся позже\n"
    "❌ Отклонено → не подходит\n"
    "🤝 В партнёрах → переведена в партнёры\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "⌨️ <b>Команды</b>\n\n"
    "/pending — все заявки со статусом «Новая» и «Отложено»\n"
    "/partners — список активных партнёров\n"
    "/help — эта инструкция\n"
    "/cancel — выйти из текущего диалога\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "💡 <b>Советы</b>\n\n"
    "• Таблица обновляется в реальном времени — можно открыть и сразу увидеть изменения\n"
    "• Заметки накапливаются с датой и временем — история общения всегда под рукой\n"
    "• Бот запоминает все данные даже после перезапуска — кнопки не теряются\n"
    "• При каждом старте бот присылает последние 3 заявки — быстрый контекст"
)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        HELP_TEXT,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Открыть таблицу", url=SPREADSHEET_URL),
        ]]),
    )


async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    records = await asyncio.to_thread(sheets.get_pending_leads)
    if not records:
        await update.message.reply_text("✅ Нет ожидающих заявок.")
        return
    lines = [f"📋 <b>Ожидающие заявки ({len(records)})</b>\n"]
    for r in records:
        lines.append(_format_lead_short(r))
    await update.message.reply_text(
        "\n\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Открыть таблицу", url=SPREADSHEET_URL),
        ]]),
    )


async def cmd_partners(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    records = await asyncio.to_thread(sheets.get_active_partners)
    if not records:
        await update.message.reply_text("Нет активных партнёров.")
        return
    lines = [f"🤝 <b>Активные партнёры ({len(records)})</b>\n"]
    for r in records:
        lines.append(
            f"<b>#{h(r['id'])}</b> — {h(r['name'])}\n"
            f"   📧 {h(r['contact'])}\n"
            f"   📋 {h(r['next_steps'])}\n"
            f"   📅 {h(r['date_added'])}"
        )
    await update.message.reply_text(
        "\n\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Открыть таблицу", url=SPREADSHEET_URL),
        ]]),
    )


# ── Hunter callbacks ───────────────────────────────────────────────────────

HUNTER_STATUS_MAP = {
    "contact":  "Связались",
    "pipeline": "В pipeline",
    "skip30":   "Отложено",
    "reject":   "Не целевой",
}

IG_ENABLED = bool(os.getenv("IG_USERNAME") and os.getenv("IG_PASSWORD"))


async def handle_hunter_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик кнопок на Hunter-карточке (prefix hl:)."""
    query = update.callback_query
    await query.answer()

    # Формат: hl:H-001:contact
    parts = query.data.split(":")   # ['hl', 'H-001', 'contact']
    if len(parts) < 3:
        return

    hunter_id  = parts[1]           # H-001
    action     = parts[2]           # contact | pipeline | skip30 | reject
    new_status = HUNTER_STATUS_MAP.get(action, "Неизвестно")
    now        = datetime.now().strftime("%d.%m.%Y %H:%M")

    contacted_at = now if action == "contact" else ""
    await asyncio.to_thread(
        sheets.update_hunter_lead_status, hunter_id, new_status, contacted_at
    )

    lead = await asyncio.to_thread(sheets.get_hunter_lead, hunter_id)
    if not lead:
        await query.answer("Лид не найден в таблице", show_alert=True)
        return

    text = format_hunter_card(hunter_id, lead, status=new_status)
    keyboard = _build_hunter_kb(
        hunter_id,
        phone=lead.get("phone", ""),
        maps_link=lead.get("maps_link", ""),
        instagram=lead.get("instagram", ""),
        status=new_status,
    )

    try:
        await query.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            raise


# ── Instagram DM handler ──────────────────────────────────────────────────

async def handle_instagram_dm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ручная отправка Instagram DM по кнопке в карточке лида."""
    query = update.callback_query
    await query.answer()

    if not IG_ENABLED:
        await query.answer("IG_USERNAME/IG_PASSWORD не настроены в .env", show_alert=True)
        return

    hunter_id = query.data.split(":")[1]
    lead = await asyncio.to_thread(sheets.get_hunter_lead, hunter_id)
    if not lead:
        await query.answer("Лид не найден в таблице", show_alert=True)
        return

    ig_url = (lead.get("instagram") or "").strip()
    if not ig_url:
        await query.answer("У этого лида нет Instagram", show_alert=True)
        return

    await query.answer("Отправляю DM...")

    try:
        import instagram_dm
        username = await asyncio.to_thread(
            instagram_dm.send_dm,
            ig_url,
            lead.get("name", ""),
            lead.get("niche", ""),
            str(lead.get("rating", "")),
            str(lead.get("reviews_count", "")),
            lead.get("city", ""),
        )
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        await asyncio.to_thread(
            sheets.update_hunter_lead_status,
            hunter_id, "Написали в Instagram", now,
        )
        new_status = "Написали в Instagram"
        text = format_hunter_card(hunter_id, lead, status=new_status)
        keyboard = _build_hunter_kb(
            hunter_id,
            phone=lead.get("phone", ""),
            maps_link=lead.get("maps_link", ""),
            instagram=lead.get("instagram", ""),
            status=new_status,
        )
        try:
            await query.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise
        count = instagram_dm.today_count()
        await query.message.reply_text(
            f"📸 DM отправлен → <b>@{h(username)}</b>\n"
            f"Сегодня отправлено: {count}/{instagram_dm.DAILY_LIMIT}",
            parse_mode="HTML",
        )
    except RuntimeError as e:
        await query.message.reply_text(f"⚠️ {h(str(e))}", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Instagram DM ошибка: {e}", exc_info=True)
        await query.message.reply_text(f"❌ Ошибка отправки: {h(str(e))}", parse_mode="HTML")


# ── Application setup ──────────────────────────────────────────────────────

conv_handler = ConversationHandler(
    entry_points=[
        CallbackQueryHandler(partner_start, pattern="^pt:"),
        CallbackQueryHandler(note_start,    pattern="^nt:"),
    ],
    states={
        PARTNER_DESC:    [MessageHandler(filters.TEXT & ~filters.COMMAND, partner_got_desc)],
        PARTNER_NEXT:    [MessageHandler(filters.TEXT & ~filters.COMMAND, partner_got_next)],
        PARTNER_CONFIRM: [CallbackQueryHandler(partner_confirm, pattern="^pconf:")],
        NOTE_INPUT:      [MessageHandler(filters.TEXT & ~filters.COMMAND, note_got_text)],
    },
    fallbacks=[CommandHandler("cancel", cancel)],
    name="main_conv",
    persistent=True,
)

_persistence = PicklePersistence(filepath="bot_data.pkl")

application = (
    Application.builder()
    .token(BOT_TOKEN)
    .persistence(_persistence)
    .build()
)
application.add_handler(conv_handler)
application.add_handler(CallbackQueryHandler(handle_status,        pattern="^st:"))
application.add_handler(CallbackQueryHandler(handle_hunter_status, pattern="^hl:"))
application.add_handler(CallbackQueryHandler(handle_instagram_dm,  pattern="^ig:"))
application.add_handler(CommandHandler("pending",  cmd_pending))
application.add_handler(CommandHandler("partners", cmd_partners))
application.add_handler(CommandHandler("help",     cmd_help))
application.add_error_handler(error_handler)
