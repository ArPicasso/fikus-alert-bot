import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from config import SPREADSHEET_ID, CREDENTIALS_FILE, SHEET_LEADS, SHEET_PARTNERS

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

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
    global _client, _spreadsheet
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
