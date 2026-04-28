#!/usr/bin/env python3
"""
Универсальный hunter для одной ниши.

Управляется через переменные окружения:
  NICHE_KEY     — ключ из scraper/niches.py (например: cafes, hotels, dental)
  COUNTRY       — страна (по умолчанию: Georgia)
  MAX_RESULTS   — макс. мест за запуск (по умолчанию: 200)
  CLEAR_BEFORE_RUN — yes/no, очистить вкладку перед запуском

Запуск вручную:
    cd fikus-alert-bot && NICHE_KEY=hotels python hunter_niche.py
"""
import asyncio
import logging
import os
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
logger = logging.getLogger("hunter_niche")


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
                        await _edit(bot, msg_id, (
                            f"{icon} <b>{label} — скрапинг завершён</b>\n"
                            f"{_bar(c, t)}\n<i>Обработка результатов...</i>"
                        ))
                    return
                latest = item
        except asyncio.QueueEmpty:
            pass

        if latest[0] > 0:
            now = _time.monotonic()
            if now - last_edit >= 5:
                c, t, name = latest
                await _edit(bot, msg_id, (
                    f"{icon} <b>{label} — скрапинг...</b>\n"
                    f"{_bar(c, t)}\n<i>Сейчас: {name}</i>"
                ))
                last_edit = _time.monotonic()

        await asyncio.sleep(1)


def _lead_line(i: int, lead: dict, icon: str) -> str:
    name    = lead.get("name", "?")
    city    = lead.get("city", "")
    country = lead.get("country", "")
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

    location = f"{city}, {country}" if city and country else city or country
    loc_tag = f" ({location})" if location else ""
    return f"{i}. {icon} <b>{name}</b>{loc_tag} — ⭐{rating} ({reviews}) · {web}"


async def main() -> None:
    if not HUNTER_ENABLED:
        logger.info("Hunter отключён. Выход.")
        return

    # ── Чтение конфига ────────────────────────────────────────────────────────
    niche_key  = os.getenv("NICHE_KEY", "").strip().lower()
    country    = os.getenv("COUNTRY", "Georgia").strip()
    max_res    = int(os.getenv("MAX_RESULTS", "200"))

    if niche_key not in NICHES:
        logger.error(f"NICHE_KEY='{niche_key}' не найден. Доступные: {', '.join(NICHES)}")
        sys.exit(1)

    niche  = NICHES[niche_key]
    prefix = niche["prefix"]
    label  = niche["label"]
    icon   = niche["icon"]
    query  = niche["query"]

    logger.info("═" * 50)
    logger.info(f"{icon} Hunter: {label} | {country} | вкладка: {SHEET_ALL}")
    logger.info("═" * 50)

    async with Bot(token=BOT_TOKEN) as bot:

        # 1. Стартовое сообщение
        try:
            prog_msg = await bot.send_message(
                chat_id=HUNTER_CHAT_ID,
                message_thread_id=HUNTER_TOPIC_ID,
                text=f"{icon} <b>{label} ({country}) — запуск</b>\nИнициализация...",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Не удалось отправить стартовое сообщение: {e}")
            return

        # 2. Инициализация единой вкладки «Лиды»
        try:
            await asyncio.to_thread(sheets.init_all_sheet)
            logger.info(f"✅ Вкладка «{SHEET_ALL}» готова")
        except Exception as e:
            await _edit(bot, prog_msg.message_id, f"❌ Ошибка инициализации:\n{e}")
            return

        # 3. Очистка (только данные этой ниши, если запрошено)
        if os.getenv("CLEAR_BEFORE_RUN", "no").lower() == "yes":
            logger.info("CLEAR_BEFORE_RUN=yes — очистка всей таблицы Лиды")
            try:
                deleted = await asyncio.to_thread(sheets.clear_all_leads)
                logger.info(f"🗑️ Очищено {deleted} строк")
            except Exception as e:
                logger.warning(f"Не удалось очистить: {e}")

        # 4. Существующие названия этой ниши (дедупликация)
        existing = await asyncio.to_thread(sheets.get_all_existing_names, label)
        logger.info(f"В базе по нише «{label}»: {len(existing)} записей")

        # 5. Прогресс
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()

        def on_place_done(current: int, total: int, name: str) -> None:
            loop.call_soon_threadsafe(q.put_nowait, (current, total, name))

        prog_task = asyncio.create_task(
            _progress_loop(bot, prog_msg.message_id, q, icon, label)
        )

        # 6. Скрапинг
        logger.info(f"🌐 Запрос: «{query}» | страна: {country} | макс: {max_res}")
        try:
            raw_places = await asyncio.to_thread(
                maps_scraper.scrape_places, query, max_res, on_place_done, country
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
            if p.get("name", "").lower().strip() not in existing
        ]
        logger.info(f"Новых лидов: {len(new_leads)} / {len(raw_places)}")

        # 8. Сохранение в единую таблицу
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        saved = 0
        for lead in new_leads:
            lead["niche"] = label   # добавляем метку ниши
            try:
                lead_id = await asyncio.to_thread(sheets.next_all_id, prefix)
                await asyncio.to_thread(sheets.add_all_lead, lead_id, now, lead)
                saved += 1
                logger.info(f"✅ #{lead_id} — {lead.get('name', '')}")
            except Exception as e:
                logger.error(f"Ошибка сохранения: {e}")
            await asyncio.sleep(0.4)

        # 9. Итог
        sep = "━━━━━━━━━━━━━━━━━━━━━"
        lines = [
            f"✅ <b>{icon} {label} ({country})</b> · {now}",
            sep,
            f"📍 Проверено: <b>{len(raw_places)}</b>",
            f"🎯 Новых лидов: <b>{len(new_leads)}</b>",
        ]
        if new_leads:
            lines.append(sep)
            for i, lead in enumerate(new_leads[:20], 1):
                lines.append(_lead_line(i, lead, icon))
            if len(new_leads) > 20:
                lines.append(f"  … и ещё {len(new_leads) - 20}")
            if SPREADSHEET_ID:
                lines += [
                    "",
                    f'📊 <a href="https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}">Google Sheets → {SHEET_ALL}</a>',
                ]
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
        except Exception:
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
