#!/usr/bin/env python3
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import httplib2
from google_auth_httplib2 import AuthorizedHttp

CONFIG_PATH = os.environ.get("COMPETITOR_CONFIG", "config.json")
RUN_MODE = os.environ.get("COMPETITOR_RUN_MODE", "both").lower()
MASTER_ONLY_TAB = os.environ.get("COMPETITOR_MASTER_ONLY_TAB", "").strip()
MASTER_FREEZE_VALUES = os.environ.get("COMPETITOR_MASTER_FREEZE", "true").lower() != "false"
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]
RETRY_COUNT = 3
RETRY_DELAY_SECONDS = 120
HTTP_TIMEOUT_SECONDS = 300
HTTP_RETRIES = 3


class TabMissingError(RuntimeError):
    pass


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_credentials() -> service_account.Credentials:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON") from exc
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def exec_request(request):
    return request.execute(num_retries=HTTP_RETRIES)


def parse_rfc3339(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def parse_timestamp_from_name(name: str, prefix: str) -> Optional[datetime]:
    # Expect formats like: prefix_YYMMDD or prefix_YYMMDD_HHMMSS
    if not name.startswith(prefix):
        return None
    suffix = name[len(prefix) :]
    match = re.search(r"(\d{6})(?:_(\d{6}))?", suffix)
    if not match:
        return None
    yymmdd = match.group(1)
    hhmmss = match.group(2) or "000000"
    try:
        yy = int(yymmdd[0:2])
        year = 2000 + yy
        month = int(yymmdd[2:4])
        day = int(yymmdd[4:6])
        hour = int(hhmmss[0:2])
        minute = int(hhmmss[2:4])
        second = int(hhmmss[4:6])
        return datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None


def get_kst_today() -> datetime:
    try:
        from zoneinfo import ZoneInfo  # Python 3.9+

        return datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        return datetime.utcnow()


def get_week_monday_str() -> str:
    today = get_kst_today()
    monday = today - timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")


def a1_to_col(a1: str) -> int:
    result = 0
    for ch in a1.upper():
        if not ("A" <= ch <= "Z"):
            break
        result = result * 26 + (ord(ch) - 64)
    return result


def col_to_a1(index: int) -> str:
    # 1-based index to column letters
    letters = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def parse_date_header(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def list_spreadsheets_in_folder(
    drive_service,
    drive_id: str,
    folder_id: str,
) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    query = (
        f"'{folder_id}' in parents and "
        "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    )

    while True:
        response = exec_request(
            drive_service.files().list(
                q=query,
                corpora="drive",
                driveId=drive_id,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields="nextPageToken, files(id, name, modifiedTime, createdTime)",
                pageSize=1000,
                pageToken=page_token,
            )
        )
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return files


def list_child_folders(
    drive_service,
    drive_id: str,
    folder_id: str,
) -> List[Dict[str, Any]]:
    folders: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    query = (
        f"'{folder_id}' in parents and "
        "mimeType='application/vnd.google-apps.folder' and trashed=false"
    )

    while True:
        response = exec_request(
            drive_service.files().list(
                q=query,
                corpora="drive",
                driveId=drive_id,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields="nextPageToken, files(id, name)",
                pageSize=1000,
                pageToken=page_token,
            )
        )
        folders.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return folders


def list_spreadsheets_recursive(
    drive_service,
    drive_id: str,
    folder_id: str,
) -> List[Dict[str, Any]]:
    files = list_spreadsheets_in_folder(drive_service, drive_id, folder_id)
    for child in list_child_folders(drive_service, drive_id, folder_id):
        files.extend(list_spreadsheets_recursive(drive_service, drive_id, child["id"]))
    return files


def select_latest_file(
    files: List[Dict[str, Any]],
    prefix: str,
    exclude_prefixes: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    exclude_prefixes = exclude_prefixes or []
    candidates = []
    for f in files:
        name = f.get("name", "")
        if not name.startswith(prefix):
            continue
        if any(name.startswith(ex) for ex in exclude_prefixes):
            continue
        candidates.append(f)

    if not candidates:
        return None

    def sort_key(f: Dict[str, Any]) -> Tuple[datetime, datetime, datetime]:
        name = f.get("name", "")
        name_ts = parse_timestamp_from_name(name, prefix) or datetime.min
        modified_ts = parse_rfc3339(f.get("modifiedTime")) if f.get("modifiedTime") else datetime.min
        created_ts = parse_rfc3339(f.get("createdTime")) if f.get("createdTime") else datetime.min
        return (name_ts, modified_ts, created_ts)

    candidates.sort(key=sort_key, reverse=True)
    return candidates[0]


def get_selection_reason(file_info: Dict[str, Any], prefix: str) -> str:
    name = file_info.get("name", "")
    name_ts = parse_timestamp_from_name(name, prefix)
    if name_ts:
        return f"name_timestamp={name_ts.strftime('%Y-%m-%d %H:%M:%S')}"
    modified_ts = parse_rfc3339(file_info.get("modifiedTime")) if file_info.get("modifiedTime") else None
    if modified_ts:
        return f"modifiedTime={modified_ts.isoformat()}"
    created_ts = parse_rfc3339(file_info.get("createdTime")) if file_info.get("createdTime") else None
    if created_ts:
        return f"createdTime={created_ts.isoformat()}"
    return "no_timestamp"


def get_master_meta_map(
    sheets_service,
    spreadsheet_id: str,
    meta_sheet: str,
    meta_range: str,
    meta_rows: Dict[str, int],
) -> Dict[str, str]:
    print(f"[INFO] Master meta read start: {meta_sheet}")
    result: Dict[str, str] = {}
    if meta_rows:
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
                result[tab_name] = str(values[0][0]).strip().upper()
            print(f"[INFO] Master meta cell read: {cell} -> {result.get(tab_name, '')}")
        print(f"[INFO] Master meta read done: {meta_sheet}")
        return result

    response = exec_request(
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=f"'{meta_sheet}'!{meta_range}",
            valueRenderOption="FORMATTED_VALUE",
        )
    )
    print(f"[INFO] Master meta read done: {meta_sheet}")
    values = response.get("values", [])
    for row in values[1:]:
        if len(row) < 2:
            continue
        tab_name = str(row[0]).strip()
        last_col = str(row[1]).strip().upper()
        if tab_name and last_col:
            result[tab_name] = last_col
    return result


def update_master_meta(
    sheets_service,
    spreadsheet_id: str,
    meta_sheet: str,
    meta_range: str,
    meta_rows: Dict[str, int],
    tab_name: str,
    new_col: str,
) -> None:
    print(f"[INFO] Master meta update start: {meta_sheet} ({tab_name} -> {new_col})")
    if tab_name in meta_rows:
        row_idx = meta_rows[tab_name]
        exec_request(
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{meta_sheet}'!B{row_idx}",
                valueInputOption="RAW",
                body={"values": [[new_col]]},
            )
        )
        print(f"[INFO] Master meta update done: {meta_sheet} ({tab_name} -> {new_col})")
        return

    response = exec_request(
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=f"'{meta_sheet}'!{meta_range}",
            valueRenderOption="FORMATTED_VALUE",
        )
    )
    values = response.get("values", [])
    for idx, row in enumerate(values[1:], start=2):
        if row and str(row[0]).strip() == tab_name:
            exec_request(
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{meta_sheet}'!B{idx}",
                    valueInputOption="RAW",
                    body={"values": [[new_col]]},
                )
            )
            print(f"[INFO] Master meta update done: {meta_sheet} ({tab_name} -> {new_col})")
            return


def update_master_tab(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    last_date_col: str,
    sheet_id: int,
    row_count: int,
    column_count: int,
    max_rows: int,
    chunk_size: int,
) -> str:
    print(f"[INFO] Master tab start: {tab_name}")
    last_date_col_index = a1_to_col(last_date_col)
    last_date_value = None

    monday_str = get_week_monday_str()
    monday_date = parse_date_header(monday_str)
    if monday_date:
        last_date_value = monday_date - timedelta(days=7)

    new_col_index = last_date_col_index + 1
    effective_rows = min(row_count, max_rows) if max_rows > 0 else row_count
    if effective_rows <= 1:
        print(f"[SKIP] No data rows in master tab: {tab_name}")
        return last_date_col

    prev_col_index = last_date_col_index - 1
    new_col_index_zero = new_col_index - 1
    new_col_letter = col_to_a1(new_col_index)
    prev_col_letter = col_to_a1(prev_col_index + 1)
    data_start_row = 1
    data_end_row = effective_rows

    if new_col_index > column_count:
        print(f"[INFO] Master tab expand columns: {tab_name} ({column_count} -> {new_col_index})")
        exec_request(
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": sheet_id,
                                    "gridProperties": {"columnCount": new_col_index},
                                },
                                "fields": "gridProperties.columnCount",
                            }
                        }
                    ]
                },
            )
        )

    print(f"[INFO] Master tab header update: {tab_name}")
    exec_request(
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!{new_col_letter}1",
            valueInputOption="RAW",
            body={"values": [[monday_str]]},
        )
    )

    chunk = max(1, chunk_size)
    print(f"[INFO] Master tab chunked copy start: {tab_name} (rows: {effective_rows}, chunk: {chunk})")
    row = data_start_row
    chunk_idx = 1
    while row < data_end_row:
        chunk_end = min(row + chunk, data_end_row)
        start_row_num = row + 1
        end_row_num = chunk_end

        formula_range = f"'{tab_name}'!{prev_col_letter}{start_row_num}:{prev_col_letter}{end_row_num}"
        print(f"[INFO] Master tab chunk formula read start: {tab_name} ({start_row_num}-{end_row_num})")
        formulas = exec_request(
            sheets_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=formula_range,
                valueRenderOption="FORMULA",
            )
        ).get("values", [])
        print(f"[INFO] Master tab chunk formula read done: {tab_name} ({start_row_num}-{end_row_num})")

        if formulas:
            exec_request(
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{tab_name}'!{new_col_letter}{start_row_num}",
                    valueInputOption="USER_ENTERED",
                    body={"values": formulas},
                )
            )

        if MASTER_FREEZE_VALUES:
            print(f"[INFO] Master tab chunk value read start: {tab_name} ({start_row_num}-{end_row_num})")
            values = exec_request(
                sheets_service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=spreadsheet_id,
                    range=formula_range,
                    valueRenderOption="UNFORMATTED_VALUE",
                )
            ).get("values", [])
            print(f"[INFO] Master tab chunk value read done: {tab_name} ({start_row_num}-{end_row_num})")

            if values:
                exec_request(
                    sheets_service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"'{tab_name}'!{prev_col_letter}{start_row_num}",
                        valueInputOption="RAW",
                        body={"values": values},
                    )
                )

        print(f"[INFO] Master tab chunk done: {tab_name} ({chunk_idx}) rows {start_row_num}-{end_row_num}")
        row = chunk_end
        chunk_idx += 1

    print(f"[INFO] Master tab chunked copy done: {tab_name}")

    print(f"[SYNC] Master tab updated: {tab_name} -> {monday_str}")
    return col_to_a1(new_col_index)


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


def read_source_values(sheets_service, spreadsheet_id: str, sheet_name: str, source_range: str) -> List[List[Any]]:
    range_name = f"'{sheet_name}'!{source_range}"
    response = exec_request(
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
    )
    return response.get("values", [])


def clear_target_range(sheets_service, spreadsheet_id: str, tab_name: str, target_range: str) -> None:
    range_name = f"'{tab_name}'!{target_range}"
    exec_request(
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            body={},
        )
    )


def write_target_values(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    values: List[List[Any]],
) -> None:
    if not values:
        return
    range_name = f"'{tab_name}'!A1"
    exec_request(
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": values},
        )
    )


def run_update() -> None:
    config = load_config(CONFIG_PATH)
    creds = load_credentials()

    drive_http = AuthorizedHttp(creds, http=httplib2.Http(timeout=HTTP_TIMEOUT_SECONDS))
    sheets_http = AuthorizedHttp(creds, http=httplib2.Http(timeout=HTTP_TIMEOUT_SECONDS))
    drive_service = build("drive", "v3", http=drive_http, cache_discovery=False)
    sheets_service = build("sheets", "v4", http=sheets_http, cache_discovery=False)

    drive_id = config["drive_folder_id"]
    target_sheet_id = config["target_sheet_id"]
    source_sheet_name = config["source_sheet_name"]
    source_range = config["source_range"]
    target_range = config["target_range"]
    mappings = config["mappings"]
    master_tabs = config.get("master_tabs", [])
    master_meta_sheet = config.get("master_meta_sheet", "Master_Meta")
    master_meta_range = config.get("master_meta_range", "A1:B10")
    master_max_rows = int(os.environ.get("COMPETITOR_MASTER_MAX_ROWS", config.get("master_max_rows", 5000)))
    master_chunk_size = int(os.environ.get("COMPETITOR_MASTER_CHUNK_SIZE", config.get("master_chunk_size", 500)))
    master_meta_rows = config.get("master_meta_rows", {})
    master_meta_override = config.get("master_meta_override", {})

    files = list_spreadsheets_recursive(drive_service, drive_id, drive_id)
    print(f"Found {len(files)} spreadsheets in shared drive {drive_id} (recursive)")

    sheet_props = get_sheet_properties_map(sheets_service, target_sheet_id)
    target_tabs = set(sheet_props.keys())
    master_meta = {}
    if RUN_MODE in ("both", "master"):
        if master_meta_override:
            master_meta = master_meta_override
            print("[INFO] Master meta override in use")
        else:
            master_meta = get_master_meta_map(
                sheets_service,
                target_sheet_id,
                master_meta_sheet,
                master_meta_range,
                master_meta_rows,
            )

    if RUN_MODE in ("both", "extract"):
        for mapping in mappings:
            prefix = mapping["prefix"]
            target_tab = mapping["target_tab"]
            exclude_prefixes = mapping.get("exclude_prefixes", [])

            if target_tab not in target_tabs:
                raise TabMissingError(f"Target tab not found: {target_tab}")

            latest_file = select_latest_file(files, prefix, exclude_prefixes)
            if not latest_file:
                print(f"[SKIP] No file found for prefix: {prefix}")
                continue

            source_id = latest_file["id"]
            source_name = latest_file.get("name", "")
            reason = get_selection_reason(latest_file, prefix)
            print(f"[SYNC] {prefix} -> {target_tab} (source: {source_name}, reason: {reason})")

            try:
                print(f"[INFO] Read source start: {source_name}")
                values = read_source_values(sheets_service, source_id, source_sheet_name, source_range)
                print(f"[INFO] Read source done: {source_name} (rows: {len(values)})")
            except HttpError as exc:
                print(f"[SKIP] Failed to read source sheet for {prefix}: {exc}")
                continue

            print(f"[INFO] Clear target start: {target_tab}")
            clear_target_range(sheets_service, target_sheet_id, target_tab, target_range)
            print(f"[INFO] Clear target done: {target_tab}")
            print(f"[INFO] Write target start: {target_tab}")
            write_target_values(sheets_service, target_sheet_id, target_tab, values)
            print(f"[INFO] Write target done: {target_tab}")

    if RUN_MODE in ("both", "master"):
        for tab_name in master_tabs:
            if MASTER_ONLY_TAB and tab_name != MASTER_ONLY_TAB:
                continue
            props = sheet_props.get(tab_name)
            if not props:
                raise TabMissingError(f"Master tab not found: {tab_name}")
            last_date_col = master_meta.get(tab_name)
            if not last_date_col:
                raise TabMissingError(f"Master meta missing for tab: {tab_name}")
            new_col = update_master_tab(
                sheets_service,
                target_sheet_id,
                tab_name,
                last_date_col,
                props["sheetId"],
                props["rowCount"],
                props["columnCount"],
                master_max_rows,
                master_chunk_size,
            )
            if new_col != last_date_col:
                update_master_meta(
                    sheets_service,
                    target_sheet_id,
                    master_meta_sheet,
                    master_meta_range,
                    master_meta_rows,
                    tab_name,
                    new_col,
                )

    print("Update completed")


def main() -> int:
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            run_update()
            return 0
        except TabMissingError as exc:
            print(f"[FAIL] {exc}")
            return 2
        except Exception as exc:
            print(f"[ERROR] Attempt {attempt}/{RETRY_COUNT}: {exc}")
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            return 1

    return 1


if __name__ == "__main__":
    sys.exit(main())
