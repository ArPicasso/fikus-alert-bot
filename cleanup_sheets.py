#!/usr/bin/env python3
"""Удаляет все вкладки кроме Лиды, Заявки, Партнеры."""
import time, logging
from dotenv import load_dotenv
load_dotenv()
from sheets import _get_spreadsheet
from config import SHEET_ALL, SHEET_LEADS, SHEET_PARTNERS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("cleanup")

KEEP = {SHEET_ALL, SHEET_LEADS, SHEET_PARTNERS}

def main():
    ss = _get_spreadsheet()
    all_ws = ss.worksheets()
    to_delete = [ws for ws in all_ws if ws.title not in KEEP]
    keep_ws   = [ws for ws in all_ws if ws.title in KEEP]

    logger.info(f"Оставляем: {[w.title for w in keep_ws]}")
    logger.info(f"Удаляем:   {[w.title for w in to_delete]}")

    for ws in to_delete:
        logger.info(f"  ❌ Удаляем «{ws.title}»")
        ss.del_worksheet(ws)
        time.sleep(0.8)

    logger.info("✅ Готово.")

if __name__ == "__main__":
    main()
