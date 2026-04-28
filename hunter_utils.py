"""Общие утилиты Hunter — форматирование карточек и клавиатур.
Импортируется и hunter.py и bot.py."""
import html
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

HUNTER_STATUS_EMOJI = {
    "Новый":                  "🆕",
    "Связались":              "📞",
    "В pipeline":             "✅",
    "Отложено":               "⏳",
    "Не целевой":             "❌",
    "Написали в Instagram":   "📸",
}


def h(text) -> str:
    return html.escape(str(text)) if text is not None else ""


def format_hunter_card(hunter_id: str, lead: dict, status: str | None = None) -> str:
    status = status or lead.get("status", "Новый")
    emoji  = HUNTER_STATUS_EMOJI.get(status, "📊")

    website   = (lead.get("website")   or "").strip()
    instagram = (lead.get("instagram") or "").strip()
    phone     = (lead.get("phone")     or "").strip()
    niche     = lead.get("niche", "Кафе / ресторан")

    site_line = f"🌐 <b>Сайт:</b> {h(website)}" if website else "🌐 <b>Сайт:</b> нет"
    insta_line = f"\n📸 <b>Instagram:</b> {h(instagram)}" if instagram else ""
    phone_line = f"📞 <b>Телефон:</b> {h(phone)}" if phone else "📞 <b>Телефон:</b> не найден"

    return (
        f"🎯 <b>Холодный лид — Тбилиси</b>  <code>#{h(hunter_id)}</code>\n\n"
        f"🏪 <b>{h(lead.get('name', ''))}</b>\n"
        f"🍽 {h(niche)}\n\n"
        f"⭐ <b>{h(lead.get('rating', ''))}</b>  ({h(lead.get('reviews_count', ''))} отзывов)\n"
        f"{site_line}"
        f"{insta_line}\n"
        f"{phone_line}\n\n"
        f"💡 <b>Почему лид:</b> {h(lead.get('why_cold', ''))}\n"
        f"💰 <b>Бюджет:</b> {h(lead.get('budget_est', ''))}\n\n"
        f"{emoji} <b>Статус:</b> {h(status)}"
    )


def build_hunter_keyboard(
    hunter_id: str,
    phone: str = "",
    maps_link: str = "",
    instagram: str = "",
    status: str = "Новый",
) -> InlineKeyboardMarkup | None:
    """Строит клавиатуру в зависимости от текущего статуса лида."""

    # Финальные статусы — минимальная клавиатура
    if status in ("Не целевой", "В pipeline"):
        rows = []
        if maps_link:
            rows.append([InlineKeyboardButton("📍 Открыть на Maps", url=maps_link)])
        return InlineKeyboardMarkup(rows) if rows else None

    rows = []

    # Строка 1: Связались + В pipeline
    row1 = []
    if status != "Связались":
        row1.append(InlineKeyboardButton("📞 Связались", callback_data=f"hl:{hunter_id}:contact"))
    row1.append(InlineKeyboardButton("✅ В pipeline", callback_data=f"hl:{hunter_id}:pipeline"))
    rows.append(row1)

    # Строка 2: Instagram DM (если есть аккаунт и ещё не писали)
    if instagram and status != "Написали в Instagram":
        rows.append([
            InlineKeyboardButton("📸 Написать в Instagram", callback_data=f"ig:{hunter_id}"),
        ])

    # Строка 3: Пропустить + Не целевой
    rows.append([
        InlineKeyboardButton("⏳ Пропустить 30д", callback_data=f"hl:{hunter_id}:skip30"),
        InlineKeyboardButton("❌ Не целевой",     callback_data=f"hl:{hunter_id}:reject"),
    ])

    # Строка 4: Maps
    if maps_link:
        rows.append([InlineKeyboardButton("📍 Открыть на Maps", url=maps_link)])

    return InlineKeyboardMarkup(rows)
