#!/usr/bin/env python3
"""
Outbound Lead Hunter — автоматический поиск холодных лидов по Google Maps.

Запуск вручную:
    cd fikus-alert-bot && python hunter.py

Автозапуск: .github/workflows/hunter.yml
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
    HUNTER_ENABLED, SPREADSHEET_ID,
)
import sheets
from scraper import maps as maps_scraper
from scraper import scorer

# ── Логирование ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("hunter")

# ── Константы ──────────────────────────────────────────────────────────────────

MAX_LEADS_PER_RUN = 80   # мест для проверки за запуск
SUMMARY_LEADS_MAX = 20   # сколько лидов показывать в итоговом сообщении


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
    """Читает обновления из очереди и редактирует progress-сообщение (≤1 раз/5 с)."""
    last_edit = 0.0
    latest: tuple = (0, 0, "")

    while True:
        # Дренируем очередь — оставляем только самое свежее состояние
        try:
            while True:
                item = q.get_nowait()
                if item is None:
                    # Sentinel: финальное редактирование и выход
                    if latest[0] > 0:
                        current, total, name = latest
                        await _edit(bot, msg_id, (
                            f"🔍 <b>Скрапинг завершён</b>\n"
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
                    f"🔍 <b>Скрапинг Google Maps...</b>\n"
                    f"{_bar(current, total)}\n"
                    f"<i>Сейчас: {name}</i>"
                ))
                last_edit = _time.monotonic()

        await asyncio.sleep(1)


# ── Формат итогового сообщения ─────────────────────────────────────────────────

def _lead_line(i: int, lead: dict) -> str:
    niche = lead.get("niche", "")
    icon = "☕" if niche in ("Кафе", "Кофейня") else "🍽"
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
        logger.info("Hunter отключён (HUNTER_ENABLED=false). Выход.")
        return

    logger.info("═" * 50)
    logger.info("🔍 Запуск Outbound Lead Hunter")
    logger.info("═" * 50)

    async with Bot(token=BOT_TOKEN) as bot:

        # 1. Отправляем одно сообщение, которое будем редактировать
        try:
            prog_msg = await bot.send_message(
                chat_id=HUNTER_CHAT_ID,
                message_thread_id=HUNTER_TOPIC_ID,
                text="🔍 <b>Hunter запущен</b>\nИнициализация...",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Не удалось отправить стартовое сообщение: {e}")
            return

        # 2. Инициализация вкладки Hunter_Leads
        try:
            await asyncio.to_thread(sheets.init_hunter_sheet)
            logger.info("✅ Вкладка Hunter_Leads готова")
        except Exception as e:
            logger.error(f"Ошибка инициализации таблицы: {e}", exc_info=True)
            await _edit(bot, prog_msg.message_id, f"❌ Ошибка инициализации:\n{e}")
            return

        # 3. Очистка если запрошена
        if os.getenv("CLEAR_BEFORE_RUN", "no").lower() == "yes":
            try:
                deleted = await asyncio.to_thread(sheets.clear_hunter_leads)
                logger.info(f"🗑️ Очищено {deleted} строк из Hunter_Leads")
            except Exception as e:
                logger.warning(f"Не удалось очистить таблицу: {e}")

        # 4. Загрузить существующие названия (дедупликация)
        existing_names = await asyncio.to_thread(sheets.get_hunter_existing_names)
        logger.info(f"В базе уже: {len(existing_names)} лидов")

        # 5. Прогресс-очередь + фоновая задача редактирования
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()

        def on_place_done(current: int, total: int, name: str) -> None:
            loop.call_soon_threadsafe(q.put_nowait, (current, total, name))

        prog_task = asyncio.create_task(
            _progress_loop(bot, prog_msg.message_id, q)
        )

        # 6. Скрапинг
        logger.info("🌐 Скрапим Google Maps...")
        try:
            raw_places = await asyncio.to_thread(
                maps_scraper.scrape_tbilisi_cafes, MAX_LEADS_PER_RUN, on_place_done
            )
        except Exception as e:
            logger.error(f"Критическая ошибка скрапинга: {e}", exc_info=True)
            await q.put(None)
            await prog_task
            await _edit(bot, prog_msg.message_id, f"❌ Ошибка скрапинга:\n{e}")
            return

        await q.put(None)   # сигнал остановки progress_loop
        await prog_task

        logger.info(f"Собрано мест: {len(raw_places)}")

        # 7. Оценка и фильтрация
        qualified: list[dict] = []
        for place in raw_places:
            result = scorer.score_lead(place)
            if result["passes"]:
                qualified.append({**place, **result})

        new_leads = [
            p for p in qualified
            if p.get("name", "").lower().strip() not in existing_names
        ]
        logger.info(f"Прошли фильтр: {len(qualified)}, новых: {len(new_leads)}")

        # 8. Сохранение в Google Sheets
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        saved = 0
        saved_leads: list[tuple[str, dict]] = []   # (hunter_id, lead)
        for lead in new_leads:
            try:
                hunter_id = await asyncio.to_thread(sheets.next_hunter_id)
                await asyncio.to_thread(sheets.add_hunter_lead, hunter_id, now, lead)
                saved_leads.append((hunter_id, lead))
                saved += 1
                logger.info(f"✅ #{hunter_id} — {lead.get('name', '')}")
            except Exception as e:
                logger.error(f"Ошибка сохранения «{lead.get('name','')}»: {e}")
            await asyncio.sleep(0.4)

        # 9. Instagram DM для лидов с аккаунтом
        dm_sent = 0
        dm_errors = 0
        if os.getenv("IG_USERNAME") and os.getenv("IG_PASSWORD"):
            import instagram_dm
            logger.info("📸 Отправка Instagram DM...")
            for hunter_id, lead in saved_leads:
                ig_url = (lead.get("instagram") or "").strip()
                if not ig_url:
                    continue
                try:
                    username = await asyncio.to_thread(
                        instagram_dm.send_dm,
                        ig_url,
                        lead.get("name", ""),
                        lead.get("niche", ""),
                        str(lead.get("rating", "")),
                        str(lead.get("reviews_count", "")),
                        lead.get("city", ""),
                    )
                    await asyncio.to_thread(
                        sheets.update_hunter_lead_status,
                        hunter_id, "Написали в Instagram", now,
                    )
                    dm_sent += 1
                    logger.info(f"  DM → @{username} ({hunter_id})")
                    await asyncio.sleep(random.uniform(60, 120))  # 1-2 мин между DM
                except RuntimeError as e:
                    logger.warning(f"DM лимит: {e}")
                    break
                except Exception as e:
                    logger.error(f"  DM ошибка для {lead.get('name')}: {e}")
                    dm_errors += 1
            logger.info(f"DM итого: отправлено {dm_sent}, ошибок {dm_errors}")

        # 10. Итоговое сообщение (редактируем прогресс-сообщение)
        sep = "━━━━━━━━━━━━━━━━━━━━━"
        lines = [
            f"✅ <b>Hunter завершён</b> · {now}",
            sep,
            f"📍 Проверено мест: <b>{len(raw_places)}</b>",
            f"🎯 Новых лидов: <b>{len(new_leads)}</b>",
        ]
        if dm_sent or dm_errors:
            lines.append(f"📸 Instagram DM: отправлено <b>{dm_sent}</b>"
                         + (f", ошибок {dm_errors}" if dm_errors else ""))

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
            logger.warning(f"Не удалось обновить итоговое сообщение: {e}")
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

        logger.info(f"Готово. Сохранено: {saved}, ошибок: {len(new_leads) - saved}")
        logger.info("═" * 50)


if __name__ == "__main__":
    asyncio.run(main())
