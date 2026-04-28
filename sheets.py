import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from config import SPREADSHEET_ID, CREDENTIALS_FILE, SHEET_LEADS, SHEET_PARTNERS, SHEET_HUNTER, SHEET_HUNTER_BIZ, SHEET_ALL

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HUNTER_HEADERS = [
    "ID", "Дата скрапинга", "Ниша", "Город",
    "Название", "Ссылка Maps", "Сайт", "Instagram",
    "Телефон", "Рейтинг", "Отзывов", "Почему лид",
    "Бюджет", "Статус", "Дата контакта", "Заметки",
]

# Заголовки единой таблицы лидов (Ниша + Страна)
ALL_HEADERS = [
    "ID", "Дата скрапинга", "Ниша", "Страна", "Город",
    "Название", "Ссылка Maps", "Сайт", "Instagram",
    "Телефон", "Рейтинг", "Отзывов", "Почему лид",
    "Бюджет", "Статус", "Дата контакта", "Заметки",
]

# Заголовки для нишевых вкладок (Страна вместо Ниши) — legacy
NICHE_HEADERS = [
    "ID", "Дата скрапинга", "Страна", "Город",
    "Название", "Ссылка Maps", "Сайт", "Instagram",
    "Телефон", "Рейтинг", "Отзывов", "Почему лид",
    "Бюджет", "Статус", "Дата контакта", "Заметки",
]

LEAD_HEADERS = [
    "ID", "Дата поступления", "Имя", "Контакт",
    "Описание", "Статус", "Заметки", "Дата обновления",
]
PARTNER_HEADERS = [
    "ID", "Имя", "Контакт", "Описание",
    "Что делать", "Дата добавления", "Статус партнёра",
]

_client = None
_spreadsheet = None


def _get_spreadsheet():
    """Подключение к таблице с автоматическим retry при временных ошибках Google API."""
    global _client, _spreadsheet

    for attempt in range(4):
        try:
            if _client is None:
                creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
                if creds_json:
                    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
                else:
                    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
                _client = gspread.authorize(creds)
            if _spreadsheet is None:
                _spreadsheet = _client.open_by_key(SPREADSHEET_ID)
            return _spreadsheet
        except Exception as e:
            _spreadsheet = None  # сбросить чтобы следующий attempt переподключился
            if attempt < 3:
                wait = 2 ** attempt  # 1, 2, 4 секунды
                time.sleep(wait)
            else:
                raise


def _leads_ws():
    return _get_spreadsheet().worksheet(SHEET_LEADS)


def _partners_ws():
    return _get_spreadsheet().worksheet(SHEET_PARTNERS)


def init_sheets() -> None:
    ss = _get_spreadsheet()
    existing = {ws.title for ws in ss.worksheets()}

    if SHEET_LEADS not in existing:
        ws = ss.add_worksheet(title=SHEET_LEADS, rows=1000, cols=10)
        ws.append_row(LEAD_HEADERS)
    else:
        ws = ss.worksheet(SHEET_LEADS)
        if not ws.get_all_values():
            ws.append_row(LEAD_HEADERS)

    if SHEET_PARTNERS not in existing:
        ws = ss.add_worksheet(title=SHEET_PARTNERS, rows=1000, cols=10)
        ws.append_row(PARTNER_HEADERS)
    else:
        ws = ss.worksheet(SHEET_PARTNERS)
        if not ws.get_all_values():
            ws.append_row(PARTNER_HEADERS)


def next_lead_id() -> str:
    rows = _leads_ws().get_all_values()
    return f"L-{len(rows):03d}"  # header counts as row 1, so first lead = L-001


def add_lead(lead_id: str, date: str, name: str, contact: str, message: str) -> None:
    _leads_ws().append_row([lead_id, date, name, contact, message, "Новая", "", date])


def _find_lead_row(ws, lead_id: str):
    """Returns gspread Cell or None. gspread v6 returns None when not found."""
    return ws.find(lead_id, in_column=1)


def update_lead_status(lead_id: str, status: str) -> None:
    ws = _leads_ws()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    cell = _find_lead_row(ws, lead_id)
    if cell is None:
        return
    ws.batch_update([
        {"range": f"F{cell.row}", "values": [[status]]},
        {"range": f"H{cell.row}", "values": [[now]]},
    ])


def add_note(lead_id: str, note: str) -> str:
    ws = _leads_ws()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    cell = _find_lead_row(ws, lead_id)
    if cell is None:
        return ""
    current = ws.cell(cell.row, 7).value or ""
    new_notes = f"{current}\n{note}".strip() if current else note
    ws.batch_update([
        {"range": f"G{cell.row}", "values": [[new_notes]]},
        {"range": f"H{cell.row}", "values": [[now]]},
    ])
    return new_notes


def add_partner(name: str, contact: str, description: str, next_steps: str, date_added: str) -> str:
    ws = _partners_ws()
    rows = ws.get_all_values()
    partner_id = f"P-{len(rows):03d}"
    ws.append_row([partner_id, name, contact, description, next_steps, date_added, "Активный"])
    return partner_id


def get_recent_leads(n: int = 3) -> list[dict]:
    rows = _leads_ws().get_all_values()[1:]  # skip header
    recent = list(reversed(rows[-n:])) if rows else []
    return [
        {
            "id":      row[0],
            "date":    row[1] if len(row) > 1 else "",
            "name":    row[2] if len(row) > 2 else "",
            "contact": row[3] if len(row) > 3 else "",
            "message": row[4] if len(row) > 4 else "",
            "status":  row[5] if len(row) > 5 else "",
            "notes":   row[6] if len(row) > 6 else "",
        }
        for row in recent if row and row[0]
    ]


def get_pending_leads() -> list[dict]:
    rows = _leads_ws().get_all_values()[1:]  # skip header
    result = []
    for row in rows:
        if len(row) > 5 and row[5] in ("Новая", "Отложено"):
            result.append({
                "id":      row[0],
                "date":    row[1] if len(row) > 1 else "",
                "name":    row[2] if len(row) > 2 else "",
                "contact": row[3] if len(row) > 3 else "",
                "message": row[4] if len(row) > 4 else "",
                "status":  row[5],
                "notes":   row[6] if len(row) > 6 else "",
            })
    return result


def get_active_partners() -> list[dict]:
    rows = _partners_ws().get_all_values()[1:]  # skip header
    result = []
    for row in rows:
        if len(row) > 6 and row[6] == "Активный":
            result.append({
                "id":          row[0],
                "name":        row[1] if len(row) > 1 else "",
                "contact":     row[2] if len(row) > 2 else "",
                "description": row[3] if len(row) > 3 else "",
                "next_steps":  row[4] if len(row) > 4 else "",
                "date_added":  row[5] if len(row) > 5 else "",
                "status":      row[6],
            })
    return result


# ── Hunter (универсальные функции, работают с любой вкладкой) ────────────────

def _hunter_ws(sheet_name: str = SHEET_HUNTER):
    return _get_spreadsheet().worksheet(sheet_name)


def init_hunter_sheet(sheet_name: str = SHEET_HUNTER) -> None:
    ss = _get_spreadsheet()
    existing = {ws.title for ws in ss.worksheets()}
    if sheet_name not in existing:
        ws = ss.add_worksheet(title=sheet_name, rows=2000, cols=20)
        ws.append_row(HUNTER_HEADERS)
    else:
        ws = ss.worksheet(sheet_name)
        if not ws.get_all_values():
            ws.append_row(HUNTER_HEADERS)


def next_hunter_id(sheet_name: str = SHEET_HUNTER, prefix: str = "H") -> str:
    rows = _hunter_ws(sheet_name).get_all_values()
    return f"{prefix}-{len(rows):03d}"


def get_hunter_existing_names(sheet_name: str = SHEET_HUNTER) -> set[str]:
    rows = _hunter_ws(sheet_name).get_all_values()[1:]
    return {row[4].lower().strip() for row in rows if len(row) > 4 and row[4]}


def add_hunter_lead(
    hunter_id: str, scraped_at: str, lead: dict,
    sheet_name: str = SHEET_HUNTER,
) -> None:
    _hunter_ws(sheet_name).append_row([
        hunter_id,
        scraped_at,
        lead.get("niche", ""),
        lead.get("city", ""),
        lead.get("name", ""),
        lead.get("maps_link", ""),
        lead.get("website", ""),
        lead.get("instagram", ""),
        lead.get("phone", ""),
        str(lead.get("rating", "")),
        str(lead.get("reviews_count", "")),
        lead.get("why_cold", ""),
        lead.get("budget_est", ""),
        "Новый",
        "",
        "",
    ])


def get_hunter_lead(hunter_id: str, sheet_name: str = SHEET_HUNTER) -> dict | None:
    ws = _hunter_ws(sheet_name)
    cell = ws.find(hunter_id, in_column=1)
    if cell is None:
        return None
    return _row_to_hunter_lead(ws.row_values(cell.row))


def _row_to_hunter_lead(row: list) -> dict:
    def safe(i): return row[i] if len(row) > i else ""
    return {
        "id":            safe(0),
        "scraped_at":    safe(1),
        "niche":         safe(2),
        "city":          safe(3),
        "name":          safe(4),
        "maps_link":     safe(5),
        "website":       safe(6),
        "instagram":     safe(7),
        "phone":         safe(8),
        "rating":        safe(9),
        "reviews_count": safe(10),
        "why_cold":      safe(11),
        "budget_est":    safe(12),
        "status":        safe(13),
        "contacted_at":  safe(14),
        "notes":         safe(15),
    }


def update_hunter_lead_status(
    hunter_id: str, status: str, contacted_at: str = "",
    sheet_name: str = SHEET_HUNTER,
) -> None:
    ws = _hunter_ws(sheet_name)
    cell = ws.find(hunter_id, in_column=1)
    if cell is None:
        return
    updates = [{"range": f"N{cell.row}", "values": [[status]]}]
    if contacted_at:
        updates.append({"range": f"O{cell.row}", "values": [[contacted_at]]})
    ws.batch_update(updates)


def clear_hunter_leads(sheet_name: str = SHEET_HUNTER) -> int:
    ws = _hunter_ws(sheet_name)
    rows = ws.get_all_values()
    count = len(rows) - 1
    if count > 0:
        ws.delete_rows(2, len(rows))
    return count


def get_pending_hunter_leads(
    limit: int = 15, sheet_name: str = SHEET_HUNTER,
) -> list[dict]:
    rows = _hunter_ws(sheet_name).get_all_values()[1:]
    result = [
        _row_to_hunter_lead(row)
        for row in rows
        if len(row) > 13 and row[13] in ("Новый", "Отложено")
    ]
    return result[-limit:]


# ── Нишевые вкладки (Страна вместо Ниши, по одной вкладке на нишу) ───────────

def init_niche_sheet(sheet_name: str) -> None:
    ss = _get_spreadsheet()
    existing = {ws.title for ws in ss.worksheets()}
    if sheet_name not in existing:
        ws = ss.add_worksheet(title=sheet_name, rows=5000, cols=20)
        ws.append_row(NICHE_HEADERS)
    else:
        ws = ss.worksheet(sheet_name)
        if not ws.get_all_values():
            ws.append_row(NICHE_HEADERS)


def get_niche_existing_names(sheet_name: str) -> set[str]:
    rows = _get_spreadsheet().worksheet(sheet_name).get_all_values()[1:]
    return {row[4].lower().strip() for row in rows if len(row) > 4 and row[4]}


def next_niche_id(sheet_name: str, prefix: str) -> str:
    rows = _get_spreadsheet().worksheet(sheet_name).get_all_values()
    return f"{prefix}-{len(rows):03d}"


def add_niche_lead(lead_id: str, scraped_at: str, lead: dict, sheet_name: str) -> None:
    _get_spreadsheet().worksheet(sheet_name).append_row([
        lead_id,
        scraped_at,
        lead.get("country", "Georgia"),
        lead.get("city", ""),
        lead.get("name", ""),
        lead.get("maps_link", ""),
        lead.get("website", ""),
        lead.get("instagram", ""),
        lead.get("phone", ""),
        str(lead.get("rating", "")),
        str(lead.get("reviews_count", "")),
        lead.get("why_cold", ""),
        lead.get("budget_est", ""),
        "Новый",
        "",
        "",
    ])


def clear_niche_leads(sheet_name: str) -> int:
    ws = _get_spreadsheet().worksheet(sheet_name)
    rows = ws.get_all_values()
    count = len(rows) - 1
    if count > 0:
        ws.delete_rows(2, len(rows))
    return count


# ── Единая таблица «Лиды» ─────────────────────────────────────────────────────

def init_all_sheet() -> None:
    """Создаёт вкладку «Лиды» с форматированием, если её нет."""
    ss = _get_spreadsheet()
    existing = {ws.title for ws in ss.worksheets()}

    if SHEET_ALL not in existing:
        ws = ss.add_worksheet(title=SHEET_ALL, rows=10000, cols=20)
        ws.append_row(ALL_HEADERS)
    else:
        ws = ss.worksheet(SHEET_ALL)
        if not ws.get_all_values():
            ws.append_row(ALL_HEADERS)

    _format_all_sheet(ss.worksheet(SHEET_ALL))


def _format_all_sheet(ws) -> None:
    """Форматирование: тёмный заголовок, заморозка, ширины колонок, фильтр."""
    sheet_id = ws.id
    col_widths = [65, 110, 100, 90, 100, 210, 75, 155, 155, 130, 70, 80, 210, 110, 105, 110, 210]

    requests = [
        # Заморозить первую строку
        {"updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount",
        }},
        # Тёмный фон + белый жирный текст в заголовке
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.11, "green": 0.11, "blue": 0.11},
                "textFormat": {
                    "bold": True,
                    "fontSize": 10,
                    "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                },
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)",
        }},
        # Высота заголовка
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 36},
            "fields": "pixelSize",
        }},
        # Включить фильтр
        {"setBasicFilter": {"filter": {"range": {
            "sheetId": sheet_id,
            "startRowIndex": 0,
            "startColumnIndex": 0,
            "endColumnIndex": len(ALL_HEADERS),
        }}}},
    ]

    for i, width in enumerate(col_widths):
        requests.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": width},
            "fields": "pixelSize",
        }})

    ws.spreadsheet.batch_update({"requests": requests})


def get_all_existing_names(niche_label: str = "") -> set[str]:
    """Названия уже сохранённых мест (опционально фильтр по нише)."""
    rows = _get_spreadsheet().worksheet(SHEET_ALL).get_all_values()[1:]
    if niche_label:
        return {row[5].lower().strip() for row in rows if len(row) > 5 and row[2] == niche_label}
    return {row[5].lower().strip() for row in rows if len(row) > 5 and row[5]}


def next_all_id(prefix: str) -> str:
    rows = _get_spreadsheet().worksheet(SHEET_ALL).get_all_values()[1:]
    count = sum(1 for row in rows if row and row[0].startswith(f"{prefix}-"))
    return f"{prefix}-{count + 1:03d}"


def add_all_lead(lead_id: str, scraped_at: str, lead: dict) -> None:
    _get_spreadsheet().worksheet(SHEET_ALL).append_row([
        lead_id,
        scraped_at,
        lead.get("niche", ""),
        lead.get("country", "Georgia"),
        lead.get("city", ""),
        lead.get("name", ""),
        lead.get("maps_link", ""),
        lead.get("website", ""),
        lead.get("instagram", ""),
        lead.get("phone", ""),
        str(lead.get("rating", "")),
        str(lead.get("reviews_count", "")),
        lead.get("why_cold", ""),
        lead.get("budget_est", ""),
        "Новый",
        "",
        "",
    ])


def clear_all_leads() -> int:
    ws = _get_spreadsheet().worksheet(SHEET_ALL)
    rows = ws.get_all_values()
    count = len(rows) - 1
    if count > 0:
        ws.delete_rows(2, len(rows))
    return count
