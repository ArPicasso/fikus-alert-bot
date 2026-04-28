#!/usr/bin/env python3
"""
Одноразовая миграция: переносит данные из нишевых вкладок в единую «Лиды»,
затем удаляет нишевые вкладки и legacy-вкладки Hunter_Leads / Hunter_Biz.

Запуск:
    cd fikus-alert-bot && python migrate_to_one_sheet.py
"""
import time
import logging
import sys

from dotenv import load_dotenv
load_dotenv()

import sheets
from sheets import _get_spreadsheet, ALL_HEADERS, SHEET_ALL
from scraper.niches import NICHES
from config import SHEET_HUNTER, SHEET_HUNTER_BIZ

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("migrate")

# Все вкладки которые нужно удалить после миграции
LEGACY_SHEETS = {SHEET_HUNTER, SHEET_HUNTER_BIZ}
NICHE_SHEETS  = {cfg["sheet"] for cfg in NICHES.values()}
TO_DELETE     = LEGACY_SHEETS | NICHE_SHEETS


def main():
    ss = _get_spreadsheet()
    existing = {ws.title: ws for ws in ss.worksheets()}

    # 1. Создать/проинициализировать единую вкладку «Лиды»
    logger.info(f"Инициализация вкладки «{SHEET_ALL}»...")
    sheets.init_all_sheet()
    time.sleep(2)

    # 2. Мигрировать данные из нишевых вкладок
    migrated = 0
    for niche_key, cfg in NICHES.items():
        tab = cfg["sheet"]
        if tab not in existing:
            continue

        ws = existing[tab]
        rows = ws.get_all_values()[1:]   # пропускаем заголовок
        if not rows:
            logger.info(f"  {tab}: пусто, пропускаем")
            continue

        logger.info(f"  {tab}: {len(rows)} строк → переносим в «{SHEET_ALL}»")
        all_ws = ss.worksheet(SHEET_ALL)

        for row in rows:
            def safe(i): return row[i] if len(row) > i else ""

            # Старая структура нишевой вкладки: ID|Дата|Страна|Город|Название|Maps|Сайт|IG|Тел|Рейт|Отз|Почему|Бюджет|Статус|ДатаК|Заметки
            # Новая структура: ID|Дата|Ниша|Страна|Город|Название|Maps|Сайт|IG|Тел|Рейт|Отз|Почему|Бюджет|Статус|ДатаК|Заметки
            new_row = [
                safe(0),            # ID
                safe(1),            # Дата скрапинга
                cfg["label"],       # Ниша (добавляем из конфига)
                safe(2),            # Страна
                safe(3),            # Город
                safe(4),            # Название
                safe(5),            # Maps link
                safe(6),            # Сайт
                safe(7),            # Instagram
                safe(8),            # Телефон
                safe(9),            # Рейтинг
                safe(10),           # Отзывов
                safe(11),           # Почему лид
                safe(12),           # Бюджет
                safe(13) or "Новый",# Статус
                safe(14),           # Дата контакта
                safe(15),           # Заметки
            ]
            all_ws.append_row(new_row)
            migrated += 1
            time.sleep(0.3)         # rate limit

    logger.info(f"Перенесено строк: {migrated}")

    # 3. Удалить нишевые и legacy вкладки
    # Нельзя удалить единственную вкладку — оставляем «Лиды» нетронутой
    to_remove = [ws for ws in ss.worksheets() if ws.title in TO_DELETE]
    for ws in to_remove:
        logger.info(f"Удаляем вкладку «{ws.title}»...")
        ss.del_worksheet(ws)
        time.sleep(1)

    logger.info("✅ Миграция завершена.")
    logger.info(f"Осталось вкладок: {[w.title for w in ss.worksheets()]}")


if __name__ == "__main__":
    main()
