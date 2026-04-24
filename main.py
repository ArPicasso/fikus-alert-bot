import os
import json
import httpx
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN          = os.getenv("BOT_TOKEN")
CHAT_ID            = os.getenv("CHAT_ID")
THREAD_ID          = os.getenv("THREAD_ID")
SECRET_KEY         = os.getenv("SECRET_KEY", "")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")  # JSON строка сервис-аккаунта
SHEET_ID           = os.getenv("SHEET_ID")             # ID таблицы из URL
SHEET_URL          = os.getenv("SHEET_URL", "")        # полная ссылка на таблицу

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["*"],
)


class Lead(BaseModel):
    name:    str
    email:   str
    company: str | None = None
    message: str
    budget:  str | None = None


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(lead: Lead, request: Request):
    if SECRET_KEY and request.headers.get("X-Secret-Key") != SECRET_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    # --- Google Sheets ---
    if GOOGLE_CREDENTIALS and SHEET_ID:
        try:
            _append_to_sheet(lead, now)
        except Exception as e:
            print(f"[sheets] error: {e}")

    # --- Telegram ---
    sheet_link = ""
    if SHEET_URL:
        sheet_link = f"\n\n📊 <a href=\"{SHEET_URL}\">Открыть Google Таблицу</a>"

    text = (
        "🌿 <b>Новая заявка с сайта Fikus</b>\n\n"
        f"👤 <b>Имя:</b> {_esc(lead.name)}\n"
        f"📧 <b>Email:</b> {_esc(lead.email)}\n"
        f"🏢 <b>Компания:</b> {_esc(lead.company or '—')}\n"
        f"💰 <b>Бюджет:</b> {_esc(lead.budget or 'не указан')}\n\n"
        f"📝 <b>Сообщение:</b>\n{_esc(lead.message)}\n\n"
        f"⏰ {now}"
        f"{sheet_link}"
    )

    payload: dict = {
        "chat_id":    CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
        "link_preview_options": {"is_disabled": True},
    }
    if THREAD_ID:
        payload["message_thread_id"] = int(THREAD_ID)

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json=payload,
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=resp.text)

    return {"ok": True}


def _append_to_sheet(lead: Lead, now: str) -> None:
    import gspread
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDENTIALS),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).sheet1

    if not sheet.get_all_values():
        sheet.append_row(["Дата", "Имя", "Email", "Компания", "Бюджет", "Сообщение", "Статус"])

    sheet.append_row([
        now,
        lead.name,
        lead.email,
        lead.company or "",
        lead.budget or "",
        lead.message,
        "Тест",
    ])


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
