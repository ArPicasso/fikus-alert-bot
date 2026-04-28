"""
Справочник ниш. Каждая ниша — отдельная вкладка в Google Sheets.
"""

NICHES: dict[str, dict] = {
    # ── Еда и напитки ─────────────────────────────────────────────────────────
    "cafes":       {"sheet": "Кафе",          "query": "cafes",              "label": "Кафе",         "icon": "☕", "prefix": "CAF"},
    "restaurants": {"sheet": "Рестораны",     "query": "restaurants",        "label": "Ресторан",     "icon": "🍽", "prefix": "RST"},
    "coffee":      {"sheet": "Кофейни",       "query": "coffee shop",        "label": "Кофейня",      "icon": "☕", "prefix": "COF"},
    "winebar":     {"sheet": "Винные_бары",   "query": "wine bar",           "label": "Винный бар",   "icon": "🍷", "prefix": "WIN"},
    "bakery":      {"sheet": "Пекарни",       "query": "bakery",             "label": "Пекарня",      "icon": "🥐", "prefix": "BAK"},
    "bars":        {"sheet": "Бары",          "query": "bar lounge",         "label": "Бар",          "icon": "🍸", "prefix": "BAR"},
    # ── Жильё ─────────────────────────────────────────────────────────────────
    "hotels":      {"sheet": "Отели",         "query": "hotel",              "label": "Отель",        "icon": "🏨", "prefix": "HTL"},
    "guesthouses": {"sheet": "Гестхаусы",     "query": "guesthouse",         "label": "Гестхаус",     "icon": "🏠", "prefix": "GST"},
    "apartments":  {"sheet": "Апартаменты",   "query": "apartments rent",    "label": "Апартаменты",  "icon": "🏢", "prefix": "APT"},
    # ── Красота ───────────────────────────────────────────────────────────────
    "beauty":      {"sheet": "Красота",       "query": "beauty salon",       "label": "Красота",      "icon": "💅", "prefix": "BTY"},
    "nails":       {"sheet": "Маникюр",       "query": "nail salon",         "label": "Маникюр",      "icon": "💅", "prefix": "NAL"},
    "barbershop":  {"sheet": "Барбершопы",    "query": "barbershop",         "label": "Барбершоп",    "icon": "💈", "prefix": "BAB"},
    # ── Здоровье ──────────────────────────────────────────────────────────────
    "fitness":     {"sheet": "Фитнес",        "query": "gym fitness",        "label": "Фитнес",       "icon": "💪", "prefix": "FIT"},
    "yoga":        {"sheet": "Йога",          "query": "yoga studio",        "label": "Йога",         "icon": "🧘", "prefix": "YOG"},
    "dental":      {"sheet": "Стоматология",  "query": "dental clinic",      "label": "Стоматология", "icon": "🦷", "prefix": "DEN"},
    "medical":     {"sheet": "Клиники",       "query": "medical clinic",     "label": "Клиника",      "icon": "🏥", "prefix": "MED"},
    # ── Услуги ────────────────────────────────────────────────────────────────
    "auto":        {"sheet": "Авто",          "query": "car repair service", "label": "Авто",         "icon": "🚗", "prefix": "AUT"},
    "travel":      {"sheet": "Туризм",        "query": "travel agency",      "label": "Туризм",       "icon": "✈️", "prefix": "TRV"},
    "design":      {"sheet": "Дизайн",        "query": "interior design",    "label": "Дизайн",       "icon": "🎨", "prefix": "DES"},
    "photo":       {"sheet": "Фото",          "query": "photography studio", "label": "Фото",         "icon": "📸", "prefix": "PHO"},
    "wedding":     {"sheet": "Свадьбы",       "query": "wedding agency",     "label": "Свадьба",      "icon": "💍", "prefix": "WED"},
    "education":   {"sheet": "Образование",   "query": "language school",    "label": "Образование",  "icon": "📚", "prefix": "EDU"},
    "realestate":  {"sheet": "Недвижимость",  "query": "real estate agency", "label": "Недвижимость", "icon": "🏗",  "prefix": "REL"},
}
