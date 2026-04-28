#!/usr/bin/env python3
"""
Outbound Biz Hunter — поиск лидов в бизнес-нишах по всей Грузии.

Ниши: отели, салоны красоты, фитнес, стоматологии, клиники,
      авто, туризм, дизайн, фото.

Запуск вручную:
    cd fikus-alert-bot && python hunter_biz.py

Автозапуск: .github/workflows/hunter_biz.yml
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

from config import (
    BOT_TOKEN, HUNTER_CHAT_ID, HUNTER_TOPIC_ID,
    HUNTER_ENABLED, SPREADSHEET_ID, SHEET_HUNTER_BIZ,
)
import sheets
from scraper import maps as maps_scraper
from scraper.maps import BIZ_QUERIES
from scraper import scorer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("hunter_biz")

MAX_LEADS_PER_RUN = 80
SUMMARY_LEADS_MAX = 20
ID_PREFIX         = "B"   # B-001, B-002, ...


# ── Прогресс-бар ───────────────────────────────────────────────────────────────

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


async def _progress_loop(bot: Bot, msg_id: int, q: asyncio.Queue) -> None:
    last_edit = 0.0
    latest: tuple = (0, 0, "")

    while True:
        try:
            while True:
                item = q.get_nowait()
                if item is None:
                    if latest[0] > 0:
                        current, total, name = latest
                        await _edit(bot, msg_id, (
                            f"🏢 <b>Скрапинг завершён</b>\n"
                            f"{_bar(current, total)}\n"
                            f"<i>Обработка результатов...</i>"
                        ))
                    return
                latest = item
        except asyncio.QueueEmpty:
            pass

        if latest[0] > 0:
            now = _time.monotonic()
            if now - last_edit >= 5:
                current, total, name = latest
                await _edit(bot, msg_id, (
                    f"🏢 <b>Скрапинг Google Maps...</b>\n"
                    f"{_bar(current, total)}\n"
                    f"<i>Сейчас: {name}</i>"
                ))
                last_edit = _time.monotonic()

        await asyncio.sleep(1)


# ── Итоговое сообщение ─────────────────────────────────────────────────────────

NICHE_ICONS = {
    "Отель": "🏨", "Гестхаус": "🏠", "Красота": "💅",
    "Маникюр": "💅", "Фитнес": "💪", "Йога": "🧘",
    "Стоматология": "🦷", "Клиника": "🏥", "Авто": "🚗",
    "Туризм": "✈️", "Дизайн": "🎨", "Фото": "📸",
}


def _lead_line(i: int, lead: dict) -> str:
    niche = lead.get("niche", "")
    icon = NICHE_ICONS.get(niche, "🏢")
    name = lead.get("name", "?")
    city = lead.get("city", "")
    rating = lead.get("rating") or "—"
    reviews = lead.get("reviews_count") or "—"
    website = lead.get("website", "")
    instagram = lead.get("instagram", "")

    if not website and not instagram:
        web_status = "без сайта"
    elif instagram and not website:
        web_status = "Instagram"
    else:
        web_status = "конструктор"

    city_tag = f" ({city})" if city else ""
    return f"{i}. {icon} <b>{name}</b>{city_tag} — ⭐{rating} ({reviews}) · {web_status}"


# ── Главная логика ─────────────────────────────────────────────────────────────

async def main() -> None:
    if not HUNTER_ENABLED:
        logger.info("Biz Hunter отключён. Выход.")
        return

    logger.info("═" * 50)
    logger.info("🏢 Запуск Outbound Biz Hunter")
    logger.info("═" * 50)

    async with Bot(token=BOT_TOKEN) as bot:

        # 1. Стартовое сообщение
        try:
            prog_msg = await bot.send_message(
                chat_id=HUNTER_CHAT_ID,
                message_thread_id=HUNTER_TOPIC_ID,
                text="🏢 <b>Biz Hunter запущен</b>\nИнициализация...",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Не удалось отправить стартовое сообщение: {e}")
            return

        # 2. Инициализация вкладки Hunter_Biz
        try:
            await asyncio.to_thread(sheets.init_hunter_sheet, SHEET_HUNTER_BIZ)
            logger.info("✅ Вкладка Hunter_Biz готова")
        except Exception as e:
            logger.error(f"Ошибка инициализации: {e}", exc_info=True)
            await _edit(bot, prog_msg.message_id, f"❌ Ошибка инициализации:\n{e}")
            return

        # 3. Очистка если запрошена
        if os.getenv("CLEAR_BEFORE_RUN", "no").lower() == "yes":
            try:
                deleted = await asyncio.to_thread(sheets.clear_hunter_leads, SHEET_HUNTER_BIZ)
                logger.info(f"🗑️ Очищено {deleted} строк")
            except Exception as e:
                logger.warning(f"Не удалось очистить: {e}")

        # 4. Существующие названия (дедупликация)
        existing_names = await asyncio.to_thread(
            sheets.get_hunter_existing_names, SHEET_HUNTER_BIZ
        )
        logger.info(f"В базе: {len(existing_names)} лидов")

        # 5. Прогресс-очередь
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()

        def on_place_done(current: int, total: int, name: str) -> None:
            loop.call_soon_threadsafe(q.put_nowait, (current, total, name))

        prog_task = asyncio.create_task(
            _progress_loop(bot, prog_msg.message_id, q)
        )

        # 6. Скрапинг с бизнес-запросами
        logger.info("🌐 Скрапим Google Maps (бизнес-ниши)...")
        try:
            raw_places = await asyncio.to_thread(
                maps_scraper.scrape_tbilisi_cafes,
                MAX_LEADS_PER_RUN,
                on_place_done,
                BIZ_QUERIES,
            )
        except Exception as e:
            logger.error(f"Ошибка скрапинга: {e}", exc_info=True)
            await q.put(None)
            await prog_task
            await _edit(bot, prog_msg.message_id, f"❌ Ошибка скрапинга:\n{e}")
            return

        await q.put(None)
        await prog_task

        logger.info(f"Собрано: {len(raw_places)}")

        # 7. Фильтрация
        qualified = []
        for place in raw_places:
            result = scorer.score_lead(place)
            if result["passes"]:
                qualified.append({**place, **result})

        new_leads = [
            p for p in qualified
            if p.get("name", "").lower().strip() not in existing_names
        ]
        logger.info(f"Новых лидов: {len(new_leads)} из {len(raw_places)}")

        # 8. Сохранение
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        saved = 0
        for lead in new_leads:
            try:
                biz_id = await asyncio.to_thread(
                    sheets.next_hunter_id, SHEET_HUNTER_BIZ, ID_PREFIX
                )
                await asyncio.to_thread(
                    sheets.add_hunter_lead, biz_id, now, lead, SHEET_HUNTER_BIZ
                )
                saved += 1
                logger.info(f"✅ #{biz_id} — {lead.get('name', '')}")
            except Exception as e:
                logger.error(f"Ошибка сохранения «{lead.get('name','')}»: {e}")
            await asyncio.sleep(0.4)

        # 9. Итоговое сообщение
        sep = "━━━━━━━━━━━━━━━━━━━━━"
        lines = [
            f"✅ <b>Biz Hunter завершён</b> · {now}",
            sep,
            f"📍 Проверено мест: <b>{len(raw_places)}</b>",
            f"🎯 Новых лидов: <b>{len(new_leads)}</b>",
        ]

        if new_leads:
            lines.append(sep)
            for i, lead in enumerate(new_leads[:SUMMARY_LEADS_MAX], 1):
                lines.append(_lead_line(i, lead))
            if len(new_leads) > SUMMARY_LEADS_MAX:
                lines.append(f"  … и ещё {len(new_leads) - SUMMARY_LEADS_MAX}")
            if SPREADSHEET_ID:
                lines.append("")
                lines.append(
                    f'📊 <a href="https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}">'
                    f'Открыть Google Sheets</a>'
                )
        else:
            lines.append("Новых лидов не найдено.")

        summary = "\n".join(lines)
        try:
            await bot.edit_message_text(
                chat_id=HUNTER_CHAT_ID,
                message_id=prog_msg.message_id,
                text=summary,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.warning(f"Не удалось обновить итог: {e}")
            try:
                await bot.send_message(
                    chat_id=HUNTER_CHAT_ID,
                    message_thread_id=HUNTER_TOPIC_ID,
                    text=summary,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

        logger.info(f"Готово. Сохранено: {saved}")
        logger.info("═" * 50)


if __name__ == "__main__":
    asyncio.run(main())
