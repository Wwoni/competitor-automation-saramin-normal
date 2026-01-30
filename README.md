# Competitor Sheet Updater

Python automation that finds the latest competitor spreadsheets in a Drive folder and replaces matching tabs in `[사업운영팀]_경쟁사_히스토리`.

## What it does
- Scans a Drive folder for Google Sheets whose names start with fixed prefixes (date/time suffixes can change weekly)
- Reads `시트1` B~D from each source sheet
- Clears target tabs A~C and writes new values (replace 방식)
- Retries failures up to 3 times with a fixed 2-minute delay (탭 미존재는 즉시 실패)

## Setup
1. Create a Google service account JSON and store it as a GitHub Actions secret:
   - Secret name: `GOOGLE_SERVICE_ACCOUNT_JSON`
2. Share the following with the service account email:
   - Drive folder: `0AEhDsDMmTwieUk9PVA`
   - Target spreadsheet: `1cuCMDkpT-q0CWQU6bcwXQDYMFmOOgtNGKxKMWGOPhJQ`
3. Update `config.json` if needed.

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account", ... }'
python scripts/update_competitor_sheets.py
```

## Local weekly schedule (macOS LaunchAgent)
This project includes a weekly runner script:
- `scripts/run_weekly.sh`
- schedule: Mondays 14:10 (KST), logs to `logs/`

LaunchAgent file:
`~/Library/LaunchAgents/com.wanted.competitor-automation.plist`

Load / unload:
```bash
launchctl load ~/Library/LaunchAgents/com.wanted.competitor-automation.plist
launchctl unload ~/Library/LaunchAgents/com.wanted.competitor-automation.plist
```

## Remote weekly schedule (GitHub Actions)
Workflow: `.github/workflows/weekly-remote.yml`
- Runs Mondays 14:10 KST (05:10 UTC)
- Requires secret: `GOOGLE_SERVICE_ACCOUNT_JSON`

Note: `scripts/run_weekly.sh` uses `GOOGLE_SERVICE_ACCOUNT_JSON` if provided; otherwise it reads from `UTIL_JSON`.

If you keep the service account JSON in a shared util folder, you can load it like this:
```bash
export GOOGLE_SERVICE_ACCOUNT_JSON="$(cat /Users/wonheelee/Documents/Cursor/util/credentials/service-account.json)"
```

## Run mode (extract/master/both)
You can control which step runs by setting `COMPETITOR_RUN_MODE`:
- `extract`: update source → extract tabs only
- `master`: update Master tabs only
- `both` (default): run extract, then master

Postprocess (company name / bizno) runs after extract by default. To run it alone:
```bash
COMPETITOR_RUN_MODE=extract COMPETITOR_POSTPROCESS_ONLY_TAB="점핏_추출" python scripts/update_competitor_sheets.py
```

Tuning (optional):
- `COMPETITOR_POSTPROCESS_MAX_ROWS` (default: 10000)
- `COMPETITOR_POSTPROCESS_CHUNK_SIZE` (default: 1000)
- `COMPETITOR_POSTPROCESS_START_ROW` (default: 2)
- `COMPETITOR_POSTPROCESS_END_ROW` (default: 0 = until limit)
- `COMPETITOR_SKIP_EXTRACT` (default: false)
- `COMPETITOR_SKIP_POSTPROCESS` (default: false)

Example:
```bash
COMPETITOR_RUN_MODE=extract python scripts/update_competitor_sheets.py
COMPETITOR_RUN_MODE=master python scripts/update_competitor_sheets.py
```

## Freeze previous column values (post-master)
To convert the previous week's formula column into values (server-side copy/paste), run:
```bash
python scripts/freeze_master_values.py
```

Optional tuning:
- `COMPETITOR_FREEZE_MAX_ROWS` (default: `master_max_rows`)
- `COMPETITOR_FREEZE_CHUNK_SIZE` (default: 2000)

## GitHub Actions
Workflow: `.github/workflows/update-competitor-sheets.yml`
- Runs every Monday 00:00 UTC
- Can be triggered manually via `workflow_dispatch`

## Config
`config.json`
- `drive_folder_id`: source folder
- `target_sheet_id`: `[사업운영팀]_경쟁사_히스토리`
- `mappings`: prefix → target tab mapping
- `master_meta_sheet`: Master 메타 시트 탭명 (`Master_Meta`)
- `master_meta_rows`: Master 메타 시트의 row index 매핑 (tab → row)
- `master_freeze_chunk_size`: 값 고정 작업의 chunk 크기

## Notes
- One competitor is not yet crawled; add a new mapping when available.
- Source sheet name is fixed as `시트1` and range `B:D`.
- Target range is `A:C` and fully replaced each run.
- Master 탭 날짜 컬럼 갱신은 `Master_Meta`의 `last_date_col`을 기준으로 진행됨.
- 값 고정(FREEZE)은 현재 수동 처리로 운영 (자동화 시 API 타임아웃 가능성 존재).
- 추출 탭 후처리: A↔F 비교 및 C↔H 보정 규칙 적용 (아래 “Postprocess Rules” 참고).

## Test Log
- 2026-01-30: Full cycle verified (extract → postprocess chunked → master). Manual freeze remains required.

---

# 경쟁사 시트 업데이트 (한국어)

Drive 폴더에서 접두어(prefix)로 최신 스프레드시트를 찾아 `[사업운영팀]_경쟁사_히스토리`의 탭을 교체 업데이트합니다.

## 기능 요약
- Drive 폴더 내 스프레드시트를 스캔하고 접두어별 최신 파일 선택
- 각 소스의 `시트1` B~D → 대상 탭 A~C로 교체(replace)
- 일반 실패는 3회 재시도(2분 고정 간격), 탭 미존재는 즉시 실패

## 수동 실행 방법 (GitHub Actions 없이)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account", ... }'
python scripts/update_competitor_sheets.py
```

## 실행 모드 (추출/마스터/전체)
`COMPETITOR_RUN_MODE`로 실행 단계를 제어할 수 있습니다.
- `extract`: 추출 탭만 업데이트
- `master`: Master 탭만 업데이트
- `both` (기본값): 추출 → Master 순서로 실행

```bash
COMPETITOR_RUN_MODE=extract python scripts/update_competitor_sheets.py
COMPETITOR_RUN_MODE=master python scripts/update_competitor_sheets.py
```

## Postprocess Rules (추출 탭 보정)
After extract updates, the script normalizes values using columns A/E/F/H:
- If `A != F` and `E == "-"` and `F not in ("-", "미가입", "")` → set `A = F`
- If `C is empty` and `H not in ("-", "")` → set `C = H`

Target tabs are configured via `postprocess_tabs` in `config.json`.

### 운영 가이드 (분할 실행 예시)
행이 많은 탭은 아래처럼 범위를 나눠서 실행합니다.

예: 사람인(일반)_추출 (총 5,835행)
```bash
COMPETITOR_RUN_MODE=extract COMPETITOR_SKIP_EXTRACT=true COMPETITOR_POSTPROCESS_ONLY_TAB='사람인(일반)_추출' \\
COMPETITOR_POSTPROCESS_START_ROW=2 COMPETITOR_POSTPROCESS_END_ROW=2002 COMPETITOR_POSTPROCESS_CHUNK_SIZE=250 \\
python scripts/update_competitor_sheets.py

COMPETITOR_RUN_MODE=extract COMPETITOR_SKIP_EXTRACT=true COMPETITOR_POSTPROCESS_ONLY_TAB='사람인(일반)_추출' \\
COMPETITOR_POSTPROCESS_START_ROW=2002 COMPETITOR_POSTPROCESS_END_ROW=4002 COMPETITOR_POSTPROCESS_CHUNK_SIZE=250 \\
python scripts/update_competitor_sheets.py

COMPETITOR_RUN_MODE=extract COMPETITOR_SKIP_EXTRACT=true COMPETITOR_POSTPROCESS_ONLY_TAB='사람인(일반)_추출' \\
COMPETITOR_POSTPROCESS_START_ROW=4002 COMPETITOR_POSTPROCESS_END_ROW=6000 COMPETITOR_POSTPROCESS_CHUNK_SIZE=250 \\
python scripts/update_competitor_sheets.py
```

## 이전 주 컬럼 값 고정 (Master 이후)
수식이 들어있는 “이전 주 컬럼”을 값으로 변환하려면:
```bash
python scripts/freeze_master_values.py
```

환경 변수로 조정 가능:
- `COMPETITOR_FREEZE_MAX_ROWS` (기본값: `master_max_rows`)
- `COMPETITOR_FREEZE_CHUNK_SIZE` (기본값: 2000)

현재 운영 기준:
- Master 컬럼 생성/수식 복사는 자동화
- 이전 주 컬럼 값 고정은 수동으로 처리

## 준비 사항
1. 서비스 계정 JSON을 준비하고 로컬 환경 변수로 주입\n
2. 서비스 계정 이메일에 아래 항목 공유 권한 부여\n
   - Drive 폴더: `0AEhDsDMmTwieUk9PVA`\n
   - 대상 시트: `1cuCMDkpT-q0CWQU6bcwXQDYMFmOOgtNGKxKMWGOPhJQ`\n
3. 필요 시 `config.json` 수정

## 구성 파일
- `config.json`: Drive 폴더/대상 시트/매핑/범위 설정
- `config.json`의 `master_meta_sheet`: Master 메타 시트 탭명
- `config.json`의 `master_meta_rows`: Master 메타 시트 row index 매핑
- `scripts/update_competitor_sheets.py`: 업데이트 스크립트
- `scripts/freeze_master_values.py`: 값 고정 스크립트
- `.github/workflows/update-competitor-sheets.yml`: Actions 워크플로우

## Master_Meta 시트
`Master_Meta` 탭에 아래 형식으로 입력합니다.
```
tab_name | last_date_col
점핏 현황_Master | H
원픽 현황_Master | H
스마트리쿠르터 현황_Master | H
리멤버 현황_Master | H
```
스크립트 실행 시 작업이 완료되면 `last_date_col` 값이 자동으로 다음 컬럼(예: H → I)으로 갱신됩니다.
