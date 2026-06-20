# CLAUDE.md — DMT LPR Transaction Process

Batch worker (Python) that fills in missing license plates on toll transactions:
SQL Server → download lane images → ALPR API → UPDATE back. Runs in an infinite
loop, sleeping `RETRY_INTERVAL` (default 3600s) between batches. Current: v6.0.1.

## ALPR API (v6.0, since 2026-06-04)

- Endpoint: `http://localhost:8000/api/v1/alpr` (single default TARGET_URL).
- Request payload is `{"Trx_Datetime": "<DMTPX_TRX_DATETIME>", "alpr_image":
  "<plain base64, no data-URI prefix>"}` — only these two fields (old hw_id,
  user_id, date_time, … removed). `call_alpr(image_bytes, transaction_datetime)`;
  `Trx_Datetime` is formatted `YYYY-MM-DD HH:MM:SS.mmm` (3-digit ms) by
  `_format_trx_datetime()`.
- Response top-level: status, LicensePlate, Province, ProvinceCode,
  ProvinceID, PlateImageUrl, msg. Confidence is NOT top-level — it lives in
  `ealpr_recognition[].results[].confidence`; `_extract_alpr_confidence()`
  takes the max (falls back to top-level `confidence` if present).
- **DMTPX_PROVINCEID now stores numeric ProvinceID (e.g. 10)** — user-chosen,
  changed from Thai province name in v5.0. Downstream readers must cope.
- **DMTPX_PROMOTIONID now stores the PlateImageUrl filename only** (basename,
  e.g. `66010e37_0.jpg`, not the full URL) — `call_alpr` strips the path via
  `rsplit('/', 1)[-1]`. It returns a 5-tuple (status, lp, province_id, conf,
  plate_url) and the update function param is `plate_image_url`.
- no_plate handled both ways: status=ok with empty LicensePlate, OR status
  normalizing to nolicenseplate/noplate. status starting with "error" trips
  the circuit breaker; other statuses → unknown_error (no breaker).
- Deployed config JSONs are NOT auto-updated — operators must edit
  `TARGET_URL` in `Trasation_Process_config.json` manually.

## Files

- `Trasation_Procese.py` — everything: config defaults, SQL, HTTP pooling, circuit
  breaker, transaction processing, DB failover layer, main loop. v5.0,
  `Program_Name = "Trasation_Process"`.
- `LogLibrary.py` — shared helpers: `Load_Config` (JSON config, auto-created as
  `<Program_Name>_config.json` next to script/exe) and `Loguru_Logging`
  (console + rotating zipped file sink under `logs/`).
- No requirements.txt / tests / git. Deps: pyodbc, requests, loguru.
  Deployed on Windows (PyInstaller-aware via `sys.frozen`).

## Do-not-touch (user-confirmed 2026-06-04)

- Hard-coded date floor `DMTPX_TRX_DATETIME >= '2025-07-25'` in SQL —
  intentional, DO NOT change / move to config.
- Image column mapping `IMAGE_FILE_02`→Image1 (tried first),
  `IMAGE_FILE_01`→Image2, `IMAGE_FILE_03`→Image3 — intentional, DO NOT change.
- `DMTPX_PROMOTIONID` is repurposed (not a real promotion ID): since v6.0 it
  stores `PlateImageUrl` from the ALPR response (was `IMAGE_FILE_0x` source
  tag in v5.0) — user-directed change, keep as-is.

## Critical gotchas

- Name is misspelled ("Trasation" / "Procese") **intentionally everywhere** —
  config filename, log filename, folder. Do not "fix" the spelling.
- SQL is built with f-strings from config values (now wrapped in `int()`).
- Unknown `Lane_Type` falls back to ETC (original behavior, now logged).
- Comments and log/code notes are mixed Thai/English; match that style.

## Refactor history (2026-06-04)

Backup of pre-refactor code: `Trasation_Procese.py.bak`. Changes made:
- Fixed real bug in `_get_or_reconnect_db`: broken-connection health-check
  path used to return `None` instead of reconnecting.
- Removed dead code (~200 lines): original `get_db_connection` /
  `process_transactions` / `update_transaction_result` + the monkey-patch
  block. `__main__` and `db_update_worker` now call `*_failover_ready` directly.
- Deduplicated 3 SQL variants into one query + `_PAYMENT_METHODS_BY_LANE` dict.
- Removed unused `dir_path`, `import os/sys`, stale `base`/`force` vars.
- Aligned `_build_conn_str_for` driver default 18→17 to match default_config.

## Change history (2026-06-11)

- Added `Trx_Datetime` back to the ALPR payload (now 2 fields:
  `Trx_Datetime` + `alpr_image`). New `_format_trx_datetime()` formats
  `DMTPX_TRX_DATETIME` as `YYYY-MM-DD HH:MM:SS.mmm` (3-digit ms); `call_alpr`
  signature is now `call_alpr(image_bytes, transaction_datetime=None)`.

## Architecture quick map

- **SQL_QUERY** built at import time from one template +
  `_PAYMENT_METHODS_BY_LANE[Lane_Type]` (ALL/MTC/ETC). Selects rows with empty
  `DMTPX_LICENCEPLATE` from `DMT_PASSING_TRANSACTION`.
- **Per batch**: ThreadPool of tx workers (count = len(TARGET_URL) unless
  `Enable_Fix_Workers`) → each tx downloads 3 images in parallel
  (`IMG_DL_WORKERS`) → calls ALPR per image → best-confidence wins →
  result pushed to a `queue.Queue` consumed by a single `db-update` thread
  (serializes writes on the one global connection).
- **Outcome writes**: best plate+province; all downloads failed → "No Image";
  all ALPR said no-plate → "No Plate"; mixed/error → skip (retried next cycle);
  empty lp+prov path writes "No LPR".
- **Circuit breaker** on ALPR `TARGET_URL`s: `URL_TIMEOUT_THRESHOLD` failures
  → disabled for `URL_DISABLE_DURATION_SECONDS`; round-robin via
  `itertools.cycle`; blocks waiting if all tripped.
- **Image servers** (`base_urls`): round-robin start, then try every base in
  order; distinguishes timeout vs not_found.
- **DB failover layer**: single global `_DB_CONN` + lock; connects only to
  PRIMARY (checks `sys.fn_hadr_is_primary_replica`), skips SECONDARY/UNKNOWN
  endpoints from `DB_SERVER_LIST` (falls back to `DB_SERVER`); sleeps
  `DB_SECONDARY_SLEEP_SEC` if only secondaries; `SELECT 1` health check;
  rate-limited reconnects + jitter; transient-error classification by SQLSTATE
  (`08S01,HYT00,HYT01,IMC06,40001`) and message hints; exponential backoff on
  SELECT/UPDATE retries; `MultiSubnetFailover` auto-disabled for raw IPs;
  `_DB_METRICS` summary logged per batch.
- **HTTP**: per-thread `requests.Session` (`threading.local`) with pooled
  HTTPAdapter + urllib3 Retry on 502/503/504; (connect, read) timeout tuples.

## User context

- User (chayanon) communicates in Thai — reply in Thai.
- `Project.md` in repo root is the human-readable Thai doc of this structure;
  keep it in sync with significant code changes.
