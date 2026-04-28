import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN  = os.getenv("BOT_TOKEN")
CHAT_ID    = os.getenv("CHAT_ID")
TOPIC_ID   = int(os.getenv("TOPIC_ID", "0")) or None  # None = главный чат
SECRET_KEY = os.getenv("SECRET_KEY", "")

SPREADSHEET_ID   = os.getenv("SPREADSHEET_ID", "")
CREDENTIALS_FILE = "credentials.json"
SHEET_LEADS   = "Заявки"
SHEET_PARTNERS = "Партнеры"
SHEET_HUNTER     = "Hunter_Leads"   # legacy
SHEET_HUNTER_BIZ = "Hunter_Biz"    # legacy
SHEET_ALL        = "Лиды"          # единая таблица всех лидов

# ── Hunter (Outbound Lead Hunter) ──────────────────────────────────────────
# Если не заданы — шлём в тот же чат/топик что и обычные заявки
HUNTER_CHAT_ID  = os.getenv("HUNTER_CHAT_ID") or CHAT_ID
HUNTER_TOPIC_ID = int(os.getenv("HUNTER_TOPIC_ID", "0")) or None
HUNTER_ENABLED  = os.getenv("HUNTER_ENABLED", "true").lower() == "true"
