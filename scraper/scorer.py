"""Оценка и фильтрация лидов из Google Maps."""

MIN_RATING  = 4.3
MIN_REVIEWS = 50

# Конструкторы сайтов — не считаем "нормальным" сайтом
WEAK_SITE_KW = [
    "wix.com", "tilda.ws", "tilda.cc", "ucoz.", "jimdo.",
    "weebly.", "sites.google.com", "blogger.com",
    "tumblr.com", "taplink.cc", "linktree.com",
]


def score_lead(place: dict) -> dict:
    """
    Возвращает dict:
      passes (bool)    — прошёл ли лид фильтр
      why_cold (str)   — объяснение для команды
      budget_est (str) — грубая оценка бюджета
    """
    rating   = _to_float(place.get("rating"))
    reviews  = _to_int(place.get("reviews_count"))
    website  = (place.get("website")   or "").strip().lower()
    instagram = (place.get("instagram") or "").strip()

    # Основной фильтр по рейтингу и количеству отзывов.
    # Если рейтинг = 0 (не удалось распарсить) — пропускаем через фильтр
    # только по отзывам, чтобы не терять данные из-за ошибки парсинга.
    if rating > 0 and rating < MIN_RATING:
        return {"passes": False, "why_cold": "", "budget_est": ""}
    if reviews > 0 and reviews < MIN_REVIEWS:
        return {"passes": False, "why_cold": "", "budget_est": ""}

    has_real_site  = bool(website) and not any(kw in website for kw in WEAK_SITE_KW)
    has_weak_site  = bool(website) and any(kw in website for kw in WEAK_SITE_KW)
    instagram_only = bool(instagram) and not website

    # Лид с нормальным сайтом нам не нужен
    if has_real_site:
        return {"passes": False, "why_cold": "", "budget_est": ""}

    # Почему лид
    if not website and not instagram:
        if rating >= 4.5 and reviews >= 200:
            why_cold = "Топовое заведение без сайта — высокий потенциал"
        elif rating >= 4.3 and reviews >= 100:
            why_cold = "Высокий рейтинг, но нет современного сайта"
        else:
            why_cold = "Нет сайта"
    elif instagram_only:
        why_cold = "Только Instagram — нет полноценного сайта"
    elif has_weak_site:
        why_cold = "Сайт на конструкторе — можно сделать лучше"
    else:
        why_cold = "Слабое онлайн-присутствие"

    return {
        "passes":     True,
        "why_cold":   why_cold,
        "budget_est": _estimate_budget(reviews),
    }


def _estimate_budget(reviews: int) -> str:
    if reviews >= 500:
        return "$2,000 – $4,000"
    if reviews >= 200:
        return "$1,500 – $3,000"
    if reviews >= 100:
        return "$1,000 – $2,000"
    return "$800 – $1,500"


def _to_float(val) -> float:
    try:
        return float(str(val).replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def _to_int(val) -> int:
    try:
        return int(str(val).replace(",", "").replace(" ", ""))
    except (TypeError, ValueError):
        return 0
