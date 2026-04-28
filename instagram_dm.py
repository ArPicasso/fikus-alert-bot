"""
Instagram DM через неофициальный API (instagrapi).

Внимание: нарушает ToS Instagram.
  - Используйте отдельный прогретый аккаунт
  - Не более IG_DAILY_LIMIT DM в день (по умолчанию 20)
  - Аккаунт не должен быть свежезарегистрированным
"""
import json
import logging
import os
import re
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

SESSION_FILE = "ig_session.json"
COUNTER_FILE = "ig_daily.json"
DAILY_LIMIT  = int(os.getenv("IG_DAILY_LIMIT", "20"))

_client = None


# ── Счётчик дневных DM ────────────────────────────────────────────────────────

def _load_counter() -> dict:
    try:
        data = json.loads(Path(COUNTER_FILE).read_text())
        if data.get("date") == date.today().isoformat():
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return {"date": date.today().isoformat(), "count": 0}


def _save_counter(counter: dict) -> None:
    Path(COUNTER_FILE).write_text(json.dumps(counter))


def today_count() -> int:
    return _load_counter()["count"]


def limit_reached() -> bool:
    return today_count() >= DAILY_LIMIT


# ── Instagram клиент ──────────────────────────────────────────────────────────

def _get_client():
    global _client
    if _client is not None:
        return _client

    from instagrapi import Client

    ig_user = os.getenv("IG_USERNAME", "")
    ig_pass = os.getenv("IG_PASSWORD", "")
    if not ig_user or not ig_pass:
        raise RuntimeError("IG_USERNAME и IG_PASSWORD не заданы в .env")

    cl = Client()
    cl.delay_range = [2, 5]

    session_path = Path(SESSION_FILE)
    if session_path.exists():
        try:
            cl.load_settings(SESSION_FILE)
            cl.login(ig_user, ig_pass)
            logger.info("Instagram: сессия восстановлена из файла")
        except Exception as e:
            logger.warning(f"Сессия устарела ({e}), перелогин...")
            session_path.unlink(missing_ok=True)
            cl.login(ig_user, ig_pass)
            cl.dump_settings(SESSION_FILE)
            logger.info("Instagram: новая сессия сохранена")
    else:
        cl.login(ig_user, ig_pass)
        cl.dump_settings(SESSION_FILE)
        logger.info("Instagram: сессия создана и сохранена")

    _client = cl
    return _client


# ── Утилиты ───────────────────────────────────────────────────────────────────

def extract_username(instagram_url: str) -> str:
    """Из URL или @handle вытащить чистый username."""
    m = re.search(r'instagram\.com/([^/?#\s]+)', instagram_url)
    if m:
        return m.group(1).rstrip("/")
    return instagram_url.lstrip("@").strip().rstrip("/")


def build_message(name: str, niche: str, rating: str, reviews: str, city: str) -> str:
    niche_lower = (niche or "заведение").lower()
    city_str = city or "Грузии"
    rating_str = f"{rating}⭐" if rating else ""
    reviews_str = f"({reviews} отзывов)" if reviews else ""
    stats = f" {rating_str} {reviews_str}".strip()

    return (
        f"Здравствуйте! 👋\n\n"
        f"Нашли вас на Google Maps — {name} в {city_str}{stats}.\n\n"
        f"Мы делаем сайты для заведений в Грузии. Заметили, что у вас нет сайта — "
        f"а значит клиенты, которые ищут {niche_lower} в Google, вас не находят.\n\n"
        f"Покажем примеры и расскажем подробнее — бесплатно, без обязательств. Интересно? 🙂"
    )


# ── Основная функция ──────────────────────────────────────────────────────────

def send_dm(
    instagram_url: str,
    name: str,
    niche: str = "",
    rating: str = "",
    reviews: str = "",
    city: str = "",
) -> str:
    """
    Отправляет DM в Instagram.

    Возвращает username получателя.
    Бросает RuntimeError при исчерпании лимита или ошибке отправки.
    """
    counter = _load_counter()
    if counter["count"] >= DAILY_LIMIT:
        raise RuntimeError(f"Дневной лимит {DAILY_LIMIT} DM исчерпан")

    username = extract_username(instagram_url)
    if not username:
        raise ValueError(f"Не удалось извлечь username из: {instagram_url!r}")

    message = build_message(name, niche, rating, reviews, city)

    cl = _get_client()
    user_id = cl.user_id_from_username(username)
    cl.direct_send(message, [user_id])

    counter["count"] += 1
    _save_counter(counter)
    logger.info(f"DM → @{username} ({counter['count']}/{DAILY_LIMIT} сегодня)")
    return username
