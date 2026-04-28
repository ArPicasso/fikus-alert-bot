#!/usr/bin/env python3
"""
Uzbekistan Lead Hunter — поиск лидов по всем нишам в Узбекистане.

Проходит по нишам из NICHES_UZ, для каждой скрапит все города Узбекистана,
фильтрует лидов без сайта и сохраняет в единую таблицу «Лиды».
Автоматически отправляет Instagram DM если настроены IG_USERNAME / IG_PASSWORD.

Запуск вручную:
    cd fikus-alert-bot && python hunter_uz.py

Управление через env:
    MAX_RESULTS_PER_NICHE — макс. мест на нишу (по умолчанию 150)
    NICHES_UZ             — через запятую, например: cafes,hotels,beauty
                            если не задано — запускаются все ниши из NICHES_UZ
    CLEAR_BEFORE_RUN      — yes/no
"""
import asyncio
import logging
import os
import random
import sys
import time as _time
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from telegram import Bot

from config import BOT_TOKEN, HUNTER_CHAT_ID, HUNTER_TOPIC_ID, HUNTER_ENABLED, SPREADSHEET_ID, SHEET_ALL
import sheets
from scraper import maps as maps_scraper
from scraper.niches import NICHES
from scraper import scorer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("hunter_uz")

COUNTRY = "Uzbekistan"

# Ниши по умолчанию для Узбекистана (ключи из NICHES)
DEFAULT_NICHES = [
    "cafes", "restaurants", "coffee",
    "hotels", "guesthouses",
    "beauty", "nails", "barbershop",
    "fitness", "dental",
    "travel", "photo", "wedding", "education",
]

MAX_RESULTS_PER_NICHE = int(os.getenv("MAX_RESULTS_PER_NICHE", "150"))
SUMMARY_MAX           = 15   # строк в итоговом сообщении


# ── Утилиты ───────────────────────────────────────────────────────────────────

def _bar(current: int, total: int, width: int = 12) -> str:
    if total <= 0:
        return f"[{'░' * width}]"
    filled = min(width, int(width * current / total))
    return f"[{'▓' * filled}{'░' * (width - filled)}] {current}/{total}"


async def _edit(bot: Bot, msg_id: int, text: str) -> None:
    try:
        await bot.edit_message_text(
            chat_id=HUNTER_CHAT_ID,
            message_id=msg_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        pass


async def _progress_loop(bot: Bot, msg_id: int, q: asyncio.Queue, icon: str, label: str) -> None:
    last_edit = 0.0
    latest: tuple = (0, 0, "")
    while True:
        try:
            while True:
                item = q.get_nowait()
                if item is None:
                    if latest[0] > 0:
                        c, t, name = latest
                        await _edit(bot, msg_id,
                            f"{icon} <b>{label} (UZ) — завершён</b>\n"
                            f"{_bar(c, t)}\n<i>Обработка...</i>")
                    return
                latest = item
        except asyncio.QueueEmpty:
            pass

        if latest[0] > 0:
            now = _time.monotonic()
            if now - last_edit >= 5:
                c, t, name = latest
                await _edit(bot, msg_id,
                    f"{icon} <b>{label} (UZ) — скрапинг...</b>\n"
                    f"{_bar(c, t)}\n<i>Сейчас: {name}</i>")
                last_edit = _time.monotonic()

        await asyncio.sleep(1)


def _lead_line(i: int, lead: dict, icon: str) -> str:
    name    = lead.get("name", "?")
    city    = lead.get("city", "")
    rating  = lead.get("rating") or "—"
    reviews = lead.get("reviews_count") or "—"
    website = lead.get("website", "")
    instagram = lead.get("instagram", "")

    if not website and not instagram:
        web = "без сайта"
    elif instagram and not website:
        web = "Instagram"
    else:
        web = "конструктор"

    loc = f" ({city})" if city else ""
    return f"{i}. {icon} <b>{name}</b>{loc} — ⭐{rating} ({reviews}) · {web}"


# ── Одна ниша ─────────────────────────────────────────────────────────────────

async def _run_niche(
    bot: Bot,
    niche_key: str,
    existing_global: set[str],
    ig_module,
) -> tuple[int, int, list[tuple[str, dict]]]:
    """
    Скрапит одну нишу по всем городам Узбекистана.
    Возвращает (saved_count, dm_count, [(lead_id, lead), ...]).
    """
    niche  = NICHES[niche_key]
    prefix = niche["prefix"]
    label  = niche["label"]
    icon   = niche["icon"]
    query  = niche["query"]

    logger.info(f"{icon} Ниша: {label}")

    # Прогресс-сообщение
    try:
        prog_msg = await bot.send_message(
            chat_id=HUNTER_CHAT_ID,
            message_thread_id=HUNTER_TOPIC_ID,
            text=f"{icon} <b>{label} (UZ) — запуск</b>",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение: {e}")
        return 0, 0, []

    loop = asyncio.get_running_loop()
    q: asyncio.Queue = asyncio.Queue()

    def on_place_done(current: int, total: int, name: str) -> None:
        loop.call_soon_threadsafe(q.put_nowait, (current, total, name))

    prog_task = asyncio.create_task(
        _progress_loop(bot, prog_msg.message_id, q, icon, label)
    )

    # Скрапинг
    try:
        raw_places = await asyncio.to_thread(
            maps_scraper.scrape_places, query, MAX_RESULTS_PER_NICHE, on_place_done, COUNTRY
        )
    except Exception as e:
        logger.error(f"Ошибка скрапинга {label}: {e}", exc_info=True)
        await q.put(None)
        await prog_task
        await _edit(bot, prog_msg.message_id, f"❌ {icon} {label}: ошибка скрапинга\n{e}")
        return 0, 0, []

    await q.put(None)
    await prog_task

    # Фильтрация
    qualified = []
    for place in raw_places:
        result = scorer.score_lead(place)
        if result["passes"]:
            qualified.append({**place, **result})

    new_leads = [
        p for p in qualified
        if p.get("name", "").lower().strip() not in existing_global
    ]

    # Сохранение
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    saved = 0
    saved_leads: list[tuple[str, dict]] = []

    for lead in new_leads:
        lead["niche"]   = label
        lead["country"] = COUNTRY
        try:
            lead_id = await asyncio.to_thread(sheets.next_all_id, f"UZ-{prefix}")
            await asyncio.to_thread(sheets.add_all_lead, lead_id, now, lead)
            saved_leads.append((lead_id, lead))
            existing_global.add(lead.get("name", "").lower().strip())
            saved += 1
            logger.info(f"  ✅ #{lead_id} — {lead.get('name', '')}")
        except Exception as e:
            logger.error(f"  Ошибка сохранения: {e}")
        await asyncio.sleep(0.4)

    # Instagram DM
    dm_sent = 0
    if ig_module:
        for lead_id, lead in saved_leads:
            ig_url = (lead.get("instagram") or "").strip()
            if not ig_url:
                continue
            try:
                username = await asyncio.to_thread(
                    ig_module.send_dm,
                    ig_url,
                    lead.get("name", ""),
                    lead.get("niche", ""),
                    str(lead.get("rating", "")),
                    str(lead.get("reviews_count", "")),
                    lead.get("city", ""),
                )
                dm_sent += 1
                logger.info(f"  DM → @{username}")
                await asyncio.sleep(random.uniform(60, 120))
            except RuntimeError as e:
                logger.warning(f"  DM лимит: {e}")
                break
            except Exception as e:
                logger.error(f"  DM ошибка: {e}")

    # Итог по нише
    sep = "━━━━━━━━━━━━━━━━━"
    lines = [
        f"✅ {icon} <b>{label} (UZ)</b> · {now}",
        sep,
        f"📍 Проверено: <b>{len(raw_places)}</b>  🎯 Новых: <b>{len(new_leads)}</b>",
    ]
    if dm_sent:
        lines.append(f"📸 DM: <b>{dm_sent}</b>")
    if new_leads:
        lines.append(sep)
        for i, lead in enumerate(new_leads[:SUMMARY_MAX], 1):
            lines.append(_lead_line(i, lead, icon))
        if len(new_leads) > SUMMARY_MAX:
            lines.append(f"  … и ещё {len(new_leads) - SUMMARY_MAX}")
    else:
        lines.append("Новых лидов не найдено.")

    try:
        await bot.edit_message_text(
            chat_id=HUNTER_CHAT_ID,
            message_id=prog_msg.message_id,
            text="\n".join(lines),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        pass

    return saved, dm_sent, saved_leads


# ── Главная логика ─────────────────────────────────────────────────────────────

async def main() -> None:
    if not HUNTER_ENABLED:
        logger.info("Hunter отключён. Выход.")
        return

    # Определяем список ниш
    niches_env = os.getenv("NICHES_UZ", "").strip()
    if niches_env:
        niche_keys = [k.strip() for k in niches_env.split(",") if k.strip() in NICHES]
    else:
        niche_keys = [k for k in DEFAULT_NICHES if k in NICHES]

    if not niche_keys:
        logger.error("Нет подходящих ниш для запуска")
        sys.exit(1)

    logger.info("═" * 50)
    logger.info(f"🇺🇿 Uzbekistan Hunter | {len(niche_keys)} ниш | {MAX_RESULTS_PER_NICHE} мест/нишу")
    logger.info("═" * 50)

    # Загружаем instagram_dm если настроено
    ig_module = None
    if os.getenv("IG_USERNAME") and os.getenv("IG_PASSWORD"):
        import instagram_dm as ig_module
        logger.info("📸 Instagram DM: включён")

    async with Bot(token=BOT_TOKEN) as bot:

        # Инициализация таблицы
        try:
            await asyncio.to_thread(sheets.init_all_sheet)
        except Exception as e:
            logger.error(f"Ошибка инициализации таблицы: {e}")
            return

        # Очистка (если нужно)
        if os.getenv("CLEAR_BEFORE_RUN", "no").lower() == "yes":
            try:
                deleted = await asyncio.to_thread(sheets.clear_all_leads)
                logger.info(f"🗑️ Очищено {deleted} строк")
            except Exception as e:
                logger.warning(f"Не удалось очистить: {e}")

        # Загружаем существующие имена один раз
        existing_global = await asyncio.to_thread(sheets.get_all_existing_names)
        logger.info(f"В базе уже: {len(existing_global)} записей")

        # Шапка-сводка в Telegram
        start_time = datetime.now().strftime("%d.%m.%Y %H:%M")
        try:
            header_msg = await bot.send_message(
                chat_id=HUNTER_CHAT_ID,
                message_thread_id=HUNTER_TOPIC_ID,
                text=(
                    f"🇺🇿 <b>Uzbekistan Hunter запущен</b> · {start_time}\n"
                    f"Ниши: {', '.join(NICHES[k]['label'] for k in niche_keys)}"
                ),
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Не удалось отправить шапку: {e}")
            return

        # Прогон по нишам
        total_saved = 0
        total_dm    = 0
        niche_stats: list[str] = []

        for niche_key in niche_keys:
            saved, dm_sent, _ = await _run_niche(bot, niche_key, existing_global, ig_module)
            total_saved += saved
            total_dm    += dm_sent
            n = NICHES[niche_key]
            niche_stats.append(f"{n['icon']} {n['label']}: {saved}")
            # Пауза между нишами чтобы не перегружать Maps
            await asyncio.sleep(random.uniform(10, 20))

        # Финальная сводка
        end_time = datetime.now().strftime("%d.%m.%Y %H:%M")
        sep = "━━━━━━━━━━━━━━━━━━━━━"
        summary_lines = [
            f"🇺🇿 <b>Uzbekistan Hunter завершён</b> · {end_time}",
            sep,
            f"🎯 Всего новых лидов: <b>{total_saved}</b>",
        ]
        if total_dm:
            summary_lines.append(f"📸 Instagram DM отправлено: <b>{total_dm}</b>")
        summary_lines += [sep] + niche_stats
        if SPREADSHEET_ID:
            summary_lines += [
                "",
                f'📊 <a href="https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}">Открыть Google Sheets</a>',
            ]

        try:
            await bot.edit_message_text(
                chat_id=HUNTER_CHAT_ID,
                message_id=header_msg.message_id,
                text="\n".join(summary_lines),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            try:
                await bot.send_message(
                    chat_id=HUNTER_CHAT_ID,
                    message_thread_id=HUNTER_TOPIC_ID,
                    text="\n".join(summary_lines),
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

    logger.info(f"🇺🇿 Готово. Лидов: {total_saved}, DM: {total_dm}")
    logger.info("═" * 50)


if __name__ == "__main__":
    asyncio.run(main())
