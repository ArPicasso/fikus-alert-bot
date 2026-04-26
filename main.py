import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

from config import SECRET_KEY
import sheets
from bot import application, send_lead_notification, send_startup_summary

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await asyncio.to_thread(sheets.init_sheets)
        logger.info("Google Sheets инициализированы")
    except Exception as e:
        logger.error(
            "Не удалось подключиться к Google Sheets: %s\n"
            "Проверь credentials.json — возможно, нужно пересоздать ключ сервис-аккаунта "
            "в Google Cloud Console (IAM → Service Accounts → Keys → Add Key).\n"
            "Также убедись, что таблица расшарена с email сервис-аккаунта.",
            e,
        )
        raise
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    await send_startup_summary()
    yield
    await application.updater.stop()
    await application.stop()
    await application.shutdown()


app = FastAPI(lifespan=lifespan)

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
    lead_id = await asyncio.to_thread(sheets.next_lead_id)
    await asyncio.to_thread(sheets.add_lead, lead_id, now, lead.name, lead.email, lead.message)
    await send_lead_notification(lead_id, lead.name, lead.email, lead.company, lead.budget, lead.message, now)

    return {"ok": True, "lead_id": lead_id}
