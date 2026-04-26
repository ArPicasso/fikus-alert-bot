import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TOPIC_ID = int(os.getenv("TOPIC_ID", "0")) or None  # None = главный чат
SECRET_KEY = os.getenv("SECRET_KEY", "")

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
CREDENTIALS_FILE = "credentials.json"
SHEET_LEADS = "Заявки"
SHEET_PARTNERS = "Партнеры"
