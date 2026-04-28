"""
Google Maps scraper — кафе/рестораны по всей Грузии.

Стратегия (v4):
  Для каждого города × каждого запроса:
    1. Открываем поиск с координатами города → только локальные результаты.
    2. Собираем ссылки из списка (фаза 1).
    3. Открываем страницу каждого места и парсим детали (фаза 2).
    4. Вызываем on_place_done(current, total, name) для прогресс-бара.
"""
import re
import time
import random
import logging
from typing import Callable

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

# ── Города по странам (название, lat, lng, zoom) ──────────────────────────────
# zoom 13 ≈ 5 km, zoom 14 ≈ 2.5 km (для маленьких городов)
COUNTRIES: dict[str, list[tuple]] = {
    "Georgia": [
        ("Тбилиси",       41.6938,  44.8015,  13),
        ("Батуми",        41.6417,  41.6367,  13),
        ("Кутаиси",       42.2679,  42.7181,  13),
        ("Телави",        41.9191,  45.4766,  14),
        ("Боржоми",       41.8402,  43.4052,  14),
        ("Степанцминда",  42.6591,  44.6356,  14),
        ("Сигнахи",       41.6102,  45.9220,  14),
        ("Гори",          41.9812,  44.1066,  14),
        ("Мцхета",        41.8432,  44.7183,  14),
        ("Зугдиди",       42.5090,  41.8707,  14),
        ("Рустави",       41.5490,  45.0126,  14),
        ("Поти",          42.1517,  41.6716,  14),
    ],
    "Uzbekistan": [
        ("Ташкент",    41.2995,  69.2401,  13),
        ("Самарканд",  39.6542,  66.9597,  14),
        ("Бухара",     39.7747,  64.4286,  14),
        ("Наманган",   41.0045,  71.6709,  14),
        ("Андижан",    40.7821,  72.3442,  14),
        ("Фергана",    40.3864,  71.7864,  14),
        ("Нукус",      42.4600,  59.6021,  14),
        ("Карши",      38.8600,  65.7908,  14),
        ("Термез",     37.2241,  67.2783,  14),
    ],
    # "Armenia":    [("Ереван",  40.1872, 44.5152, 13)],
    # "Azerbaijan": [("Баку",    40.4093, 49.8671, 13)],
}

# Алиас для обратной совместимости
CITIES = COUNTRIES["Georgia"]

# Еда и напитки
FOOD_QUERIES = [
    ("cafes",       "Кафе"),
    ("restaurants", "Ресторан"),
    ("coffee shop", "Кофейня"),
    ("wine bar",    "Винный бар"),
    ("bakery",      "Пекарня"),
]

# Бизнес других ниш
BIZ_QUERIES = [
    ("hotel",              "Отель"),
    ("guesthouse",         "Гестхаус"),
    ("beauty salon",       "Красота"),
    ("nail salon",         "Маникюр"),
    ("gym fitness",        "Фитнес"),
    ("yoga studio",        "Йога"),
    ("dental clinic",      "Стоматология"),
    ("medical clinic",     "Клиника"),
    ("car repair service", "Авто"),
    ("travel agency",      "Туризм"),
    ("interior design",    "Дизайн"),
    ("photography studio", "Фото"),
]

# Алиас для обратной совместимости
SEARCH_QUERIES = FOOD_QUERIES

MAPS_BASE       = "https://www.google.com"
LINKS_PER_QUERY = 25

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def scrape_places(
    query: str,
    max_results: int = 200,
    on_place_done: Callable[[int, int, str], None] | None = None,
    country: str = "Georgia",
) -> list[dict]:
    """
    Скрапит одну нишу (query) по всем городам указанной страны.
    Используется в hunter_niche.py.
    """
    cities = COUNTRIES.get(country, COUNTRIES["Georgia"])
    all_results: list[dict] = []
    processed   = 0
    seen_names: set[str] = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage", "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1366,768",
            ],
        )
        ctx = browser.new_context(
            locale="en-US",
            timezone_id="Asia/Tbilisi",
            viewport={"width": 1366, "height": 768},
            user_agent=random.choice(USER_AGENTS),
        )
        page = ctx.new_page()

        try:
            from playwright_stealth import stealth_sync
            stealth_sync(page)
        except ImportError:
            pass

        try:
            for city_name, lat, lng, zoom in cities:
                if len(all_results) >= max_results:
                    break
                logger.info(f"  🏙 {city_name}")

                links = _collect_place_links(page, query, lat, lng, zoom, LINKS_PER_QUERY)
                logger.info(f"    Ссылок: {len(links)}")

                for pl in links:
                    if len(all_results) >= max_results:
                        break
                    name_key = pl.get("name", "").lower().strip()
                    if name_key in seen_names:
                        continue
                    seen_names.add(name_key)

                    logger.info(f"    Парсим: {pl['name']}")
                    details = _scrape_place_page(page, pl["maps_link"])
                    pl.update(details)
                    pl["country"] = country
                    pl["city"]    = city_name
                    all_results.append(pl)
                    processed += 1

                    if on_place_done:
                        on_place_done(processed, max_results, pl["name"])

                    time.sleep(random.uniform(2.0, 3.5))

                time.sleep(random.uniform(2, 4))

        finally:
            browser.close()

    logger.info(f"Итого: {len(all_results)} мест")
    return all_results[:max_results]


def scrape_tbilisi_cafes(
    max_results: int = 80,
    on_place_done: Callable[[int, int, str], None] | None = None,
    queries: list[tuple] | None = None,
) -> list[dict]:
    """Возвращает список мест с полными данными по всем городам."""
    active_queries = queries or FOOD_QUERIES
    all_results: list[dict] = []
    processed   = 0
    seen_names: set[str] = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage", "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1366,768",
            ],
        )
        ctx = browser.new_context(
            locale="en-US",
            timezone_id="Asia/Tbilisi",
            viewport={"width": 1366, "height": 768},
            user_agent=random.choice(USER_AGENTS),
        )
        page = ctx.new_page()

        try:
            from playwright_stealth import stealth_sync
            stealth_sync(page)
            logger.info("Stealth-режим активирован")
        except ImportError:
            logger.warning("playwright-stealth не найден")

        try:
            for city_name, lat, lng, zoom in CITIES:
                if len(all_results) >= max_results:
                    break
                logger.info(f"🏙 Город: {city_name}")

                for query, niche in active_queries:
                    if len(all_results) >= max_results:
                        break
                    logger.info(f"  ▶ Запрос: «{query}»")

                    links = _collect_place_links(page, query, lat, lng, zoom, LINKS_PER_QUERY)
                    logger.info(f"    Ссылок: {len(links)}")

                    for pl in links:
                        if len(all_results) >= max_results:
                            break

                        name_key = pl.get("name", "").lower().strip()
                        if name_key in seen_names:
                            continue
                        seen_names.add(name_key)

                        logger.info(f"    Парсим: {pl['name']}")
                        details = _scrape_place_page(page, pl["maps_link"])
                        pl.update(details)
                        pl["niche"] = niche
                        pl["city"]  = city_name
                        all_results.append(pl)
                        processed += 1

                        if on_place_done:
                            on_place_done(processed, max_results, pl["name"])

                        time.sleep(random.uniform(2.0, 3.5))

                    time.sleep(random.uniform(2, 4))

        finally:
            browser.close()

    logger.info(f"Итого: {len(all_results)} мест из {len(CITIES)} городов")
    return all_results[:max_results]


# ── Фаза 1: собрать ссылки из списка результатов ─────────────────────────────

def _collect_place_links(
    page, query: str,
    lat: float, lng: float, zoom: int,
    max_links: int,
) -> list[dict]:
    url = (
        f"{MAPS_BASE}/maps/search/{query.replace(' ', '+')}/"
        f"@{lat},{lng},{zoom}z?hl=en"
    )
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
    except Exception as e:
        logger.error(f"Не удалось загрузить поиск: {e}")
        return []

    time.sleep(random.uniform(2, 3))
    _accept_cookies(page)

    try:
        page.wait_for_selector('div[role="feed"]', timeout=20_000)
    except PWTimeout:
        logger.error("Фид результатов не загрузился")
        return []

    places: list[dict] = []
    seen: set[str] = set()
    scrolls = 0

    while len(places) < max_links and scrolls < 8:
        items = page.locator('div[role="feed"] a[href*="/maps/place"]').all()

        for item in items:
            try:
                href = item.get_attribute("href") or ""
                if not href or "/maps/place" not in href:
                    continue

                maps_link = (MAPS_BASE + href) if href.startswith("/") else href
                maps_link = re.sub(r'\?.*', '', maps_link)

                aria = item.get_attribute("aria-label") or ""
                name = aria.split("·")[0].strip() if "·" in aria else ""
                if not name:
                    try:
                        name = item.inner_text(timeout=500).strip().split("\n")[0]
                    except Exception:
                        pass
                if not name:
                    m = re.search(r"/maps/place/([^/]+)", href)
                    name = m.group(1).replace("+", " ") if m else ""

                if not name or name.lower() in seen:
                    continue
                seen.add(name.lower())
                places.append({"name": name, "maps_link": maps_link})

                if len(places) >= max_links:
                    break
            except Exception:
                continue

        if len(places) >= max_links:
            break

        try:
            page.locator('div[role="feed"]').evaluate("el => el.scrollBy(0, 1200)")
        except Exception:
            pass
        time.sleep(random.uniform(1.5, 2.5))
        scrolls += 1

    return places


# ── Фаза 2: открыть страницу места и собрать все данные ──────────────────────

def _scrape_place_page(page, maps_link: str) -> dict:
    data = {
        "rating": 0.0,
        "reviews_count": 0,
        "phone": "",
        "website": "",
        "instagram": "",
    }

    try:
        page.goto(maps_link + "?hl=en", wait_until="domcontentloaded", timeout=25_000)
        time.sleep(random.uniform(2, 3.5))

        # ── Рейтинг ──────────────────────────────────────────────────────────
        for sel in [
            'span[aria-label*="stars" i]',
            'div[aria-label*="stars" i]',
            'span[aria-label*=" out of 5"]',
        ]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=2000):
                    aria = el.get_attribute("aria-label") or ""
                    m = re.search(r"(\d+[.,]\d+)", aria)
                    if m:
                        data["rating"] = float(m.group(1).replace(",", "."))
                        break
            except Exception:
                continue

        if data["rating"] == 0.0:
            try:
                el = page.locator("div.F7nice").first
                if el.is_visible(timeout=1500):
                    txt = el.inner_text(timeout=1000)
                    m = re.search(r"(\d+[.,]\d+)", txt)
                    if m:
                        data["rating"] = float(m.group(1).replace(",", "."))
            except Exception:
                pass

        # ── Отзывы ───────────────────────────────────────────────────────────
        for sel in [
            'button[aria-label*="reviews" i]',
            'span[aria-label*="reviews" i]',
        ]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=2000):
                    aria = el.get_attribute("aria-label") or el.inner_text(timeout=1000)
                    m = re.search(r"([\d,]+)\s*reviews?", aria, re.IGNORECASE)
                    if m:
                        data["reviews_count"] = int(m.group(1).replace(",", ""))
                        break
            except Exception:
                continue

        if data["reviews_count"] == 0:
            try:
                txt = page.locator("div.F7nice").first.inner_text(timeout=1000)
                m = re.search(r"\(([\d,]+)\)", txt)
                if m:
                    data["reviews_count"] = int(m.group(1).replace(",", ""))
            except Exception:
                pass

        # ── Сайт ─────────────────────────────────────────────────────────────
        for sel in [
            'a[data-item-id="authority"]',
            'a[aria-label*="website" i]',
        ]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=3000):
                    href = el.get_attribute("href") or ""
                    if href:
                        if "instagram.com" in href:
                            data["instagram"] = href
                        else:
                            data["website"] = href
                        break
            except Exception:
                continue

        # ── Телефон ───────────────────────────────────────────────────────────
        try:
            el = page.locator('[data-item-id*="phone:tel"]').first
            if el.is_visible(timeout=3000):
                val = el.get_attribute("data-item-id") or ""
                data["phone"] = val.replace("phone:tel:", "").strip()
        except Exception:
            pass

        if not data["phone"]:
            for sel in [
                'button[aria-label*="Phone"]',
                '[aria-label*="phone number" i]',
                'a[href^="tel:"]',
            ]:
                try:
                    el = page.locator(sel).first
                    if el.is_visible(timeout=2000):
                        aria = el.get_attribute("aria-label") or el.get_attribute("href") or ""
                        m = re.search(r"\+?[\d\s\-\(\)]{7,}", aria.replace("tel:", ""))
                        if m:
                            data["phone"] = re.sub(r"\s+", " ", m.group()).strip()
                            break
                except Exception:
                    continue

        logger.info(
            f"    ⭐{data['rating']} ({data['reviews_count']} отзывов) | "
            f"📞{data['phone'] or '—'} | 🌐{data['website'] or data['instagram'] or '—'}"
        )

    except Exception as e:
        logger.warning(f"  Ошибка парсинга страницы: {e}")

    return data


def _accept_cookies(page) -> None:
    for sel in [
        'button[aria-label="Accept all"]',
        'button:has-text("Accept all")',
        'button:has-text("Agree")',
        'form[action*="consent"] button:first-child',
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=1500):
                btn.click()
                time.sleep(1)
                return
        except Exception:
            continue
