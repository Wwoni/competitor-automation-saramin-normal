#!/usr/bin/env python3
import json
import os
import time
from typing import Any, Dict, List

import httplib2
from google.oauth2 import service_account
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build

CONFIG_PATH = os.environ.get("COMPETITOR_CONFIG", "config.json")
FREEZE_ONLY_TAB = os.environ.get("COMPETITOR_FREEZE_ONLY_TAB", "").strip()
FREEZE_LAST_COL = os.environ.get("COMPETITOR_FREEZE_LAST_COL", "").strip().upper()
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]
RETRY_COUNT = 3
RETRY_DELAY_SECONDS = 120
HTTP_TIMEOUT_SECONDS = 120
HTTP_RETRIES = 3


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_credentials() -> service_account.Credentials:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")
    info = json.loads(raw)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def exec_request(request):
    return request.execute(num_retries=HTTP_RETRIES)


def a1_to_col(a1: str) -> int:
    result = 0
    for ch in a1.upper():
        if not ("A" <= ch <= "Z"):
            break
        result = result * 26 + (ord(ch) - 64)
    return result


def col_to_a1(index: int) -> str:
    letters = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def get_sheet_properties_map(sheets_service, spreadsheet_id: str) -> Dict[str, Dict[str, int]]:
    print("[INFO] Sheet properties read start")
    response = exec_request(
        sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets.properties(title,sheetId,gridProperties.rowCount,gridProperties.columnCount)",
        )
    )
    print("[INFO] Sheet properties read done")
    result: Dict[str, Dict[str, int]] = {}
    for sheet in response.get("sheets", []):
        props = sheet.get("properties", {})
        title = props.get("title")
        if not title:
            continue
        result[title] = {
            "sheetId": props.get("sheetId", 0),
            "rowCount": props.get("gridProperties", {}).get("rowCount", 0),
            "columnCount": props.get("gridProperties", {}).get("columnCount", 0),
        }
    return result


def get_master_meta_from_rows(
    sheets_service,
    spreadsheet_id: str,
    meta_sheet: str,
    meta_rows: Dict[str, int],
) -> Dict[str, str]:
    print(f"[INFO] Master meta read start: {meta_sheet}")
    meta_map: Dict[str, str] = {}
    for tab_name, row_idx in meta_rows.items():
        cell = f"'{meta_sheet}'!B{row_idx}"
        response = exec_request(
            sheets_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=cell,
                valueRenderOption="UNFORMATTED_VALUE",
            )
        )
        values = response.get("values", [])
        if values and values[0]:
            meta_map[tab_name] = str(values[0][0]).strip().upper()
        print(f"[INFO] Master meta cell read: {cell} -> {meta_map.get(tab_name, '')}")
    print(f"[INFO] Master meta read done: {meta_sheet}")
    return meta_map


def freeze_column_values(
    sheets_service,
    spreadsheet_id: str,
    sheet_id: int,
    col_index_zero: int,
    start_row_index: int,
    end_row_index: int,
) -> None:
    print(f"[INFO] Freeze chunk start: rows {start_row_index+1}-{end_row_index}")
    exec_request(
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "copyPaste": {
                            "source": {
                                "sheetId": sheet_id,
                                "startRowIndex": start_row_index,
                                "endRowIndex": end_row_index,
                                "startColumnIndex": col_index_zero,
                                "endColumnIndex": col_index_zero + 1,
                            },
                            "destination": {
                                "sheetId": sheet_id,
                                "startRowIndex": start_row_index,
                                "endRowIndex": end_row_index,
                                "startColumnIndex": col_index_zero,
                                "endColumnIndex": col_index_zero + 1,
                            },
                            "pasteType": "PASTE_VALUES",
                            "pasteOrientation": "NORMAL",
                        }
                    }
                ]
            },
        )
    )
    print(f"[INFO] Freeze chunk done: rows {start_row_index+1}-{end_row_index}")


def run_freeze() -> None:
    config = load_config(CONFIG_PATH)
    creds = load_credentials()
    sheets_http = AuthorizedHttp(creds, http=httplib2.Http(timeout=HTTP_TIMEOUT_SECONDS))
    sheets_service = build("sheets", "v4", http=sheets_http, cache_discovery=False)

    target_sheet_id = config["target_sheet_id"]
    master_tabs = config.get("master_tabs", [])
    meta_sheet = config.get("master_meta_sheet", "Master_Meta")
    meta_rows = config.get("master_meta_rows", {})
    max_rows = int(os.environ.get("COMPETITOR_FREEZE_MAX_ROWS", config.get("master_max_rows", 5000)))
    chunk_size = int(os.environ.get("COMPETITOR_FREEZE_CHUNK_SIZE", config.get("master_freeze_chunk_size", 2000)))

    if not meta_rows:
        raise RuntimeError("master_meta_rows is required for freeze job")

    sheet_props = get_sheet_properties_map(sheets_service, target_sheet_id)
    tabs = master_tabs
    if FREEZE_ONLY_TAB:
        tabs = [FREEZE_ONLY_TAB]

    meta_map: Dict[str, str] = {}
    if FREEZE_LAST_COL:
        if len(tabs) != 1:
            raise RuntimeError("COMPETITOR_FREEZE_LAST_COL requires COMPETITOR_FREEZE_ONLY_TAB")
        meta_map[tabs[0]] = FREEZE_LAST_COL
    else:
        if FREEZE_ONLY_TAB:
            meta_map = get_master_meta_from_rows(
                sheets_service,
                target_sheet_id,
                meta_sheet,
                {FREEZE_ONLY_TAB: meta_rows[FREEZE_ONLY_TAB]},
            )
        else:
            meta_map = get_master_meta_from_rows(sheets_service, target_sheet_id, meta_sheet, meta_rows)

    for tab_name in tabs:
        props = sheet_props.get(tab_name)
        if not props:
            raise RuntimeError(f"Master tab not found: {tab_name}")

        last_col = meta_map.get(tab_name)
        if not last_col:
            raise RuntimeError(f"Master meta missing for tab: {tab_name}")

        last_col_index = a1_to_col(last_col)
        prev_col_index = last_col_index - 1
        if prev_col_index < 1:
            print(f"[SKIP] Invalid prev column for {tab_name}: {last_col}")
            continue

        effective_rows = min(props["rowCount"], max_rows) if max_rows > 0 else props["rowCount"]
        if effective_rows <= 1:
            print(f"[SKIP] No data rows to freeze: {tab_name}")
            continue

        prev_col_letter = col_to_a1(prev_col_index)
        print(f"[INFO] Freeze start: {tab_name} ({prev_col_letter}2:{prev_col_letter}{effective_rows})")

        row = 1
        chunk = max(1, chunk_size)
        while row < effective_rows:
            end_row = min(row + chunk, effective_rows)
            freeze_column_values(
                sheets_service,
                target_sheet_id,
                props["sheetId"],
                prev_col_index - 1,
                row,
                end_row,
            )
            print(f"[INFO] Freeze chunk done: {tab_name} rows {row+1}-{end_row}")
            row = end_row

        print(f"[SYNC] Freeze completed: {tab_name}")


def main() -> int:
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            run_freeze()
            return 0
        except Exception as exc:
            print(f"[ERROR] Attempt {attempt}/{RETRY_COUNT}: {exc}")
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
