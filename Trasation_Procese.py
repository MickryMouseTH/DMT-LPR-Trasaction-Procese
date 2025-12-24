import pyodbc
import requests
import base64
from datetime import datetime, timedelta
import time
import os
from typing import Optional, Tuple, List
from LogLibrary import Load_Config, Loguru_Logging
import concurrent.futures
import itertools
import threading
import random
import queue
import sys
import re

# new: สำหรับ HTTP connection pool/retry
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except Exception:
    HTTPAdapter = None
    Retry = None

# ----------------------- Configuration Values -----------------------
Program_Name = "Trasation_Process"
Program_Version = "5.0"

# กำหนด path ทำงานเท่าเดิม
if getattr(sys, 'frozen', False):
    dir_path = os.path.dirname(sys.executable)
else:
    dir_path = os.path.dirname(os.path.abspath(__file__))

# ค่าตั้งต้น
default_config = {
    "DB_SERVER": "",
    "DB_PORT": 1433,
    "DB_DRIVER": "ODBC Driver 17 for SQL Server",
    "DB_DATABASE": "",
    "DB_USERNAME": "",
    "DB_PASSWORD": "",

    # --- NEW: รองรับ 2 เครื่องคนละ IP (ถ้าใส่ list นี้ จะถูกใช้แทน DB_SERVER) ---
    "DB_SERVER_LIST": [],  # ตัวอย่าง: ["10.10.10.11", "10.10.20.11"]

    "QUERY_LIMIT": 100,
    "Back_date": 7,
    "Back_Time": 10,

    "base_urls": [
        "http://image-server-1.com/images",
        "http://image-server-2.com/images"
    ],

    "RETRY_INTERVAL": 3600,

    "Enable_Fix_Workers": 0,
    "Workers": 4,

    "TARGET_URL": [
        "http://localhost:8000/api/dmt/index.php",
        "http://localhost:8001/api/dmt/index.php"
    ],
    "URL_TIMEOUT_THRESHOLD": 5,
    "URL_DISABLE_DURATION_SECONDS": 3600,

    "Lane_Type": "ALL",

    "API_TIMEOUT": 10,

    # ---------- HTTP/Networking Tunables ----------
    "HTTP_POOL_MAXSIZE": 64,
    "HTTP_POOL_RETRIES": 1,
    "HTTP_BACKOFF_FACTOR": 0.2,
    "HTTP_CONNECT_TIMEOUT": 5,
    "HTTP_READ_TIMEOUT": 15,
    "IMAGE_DOWNLOAD_TIMEOUT": 12,
    "IMG_DL_WORKERS": 3,

    # ---------- DB Tunables ----------
    "DB_TIMEOUT": 30,
    "DB_AUTOCOMMIT": 1,

    # ---------- NEW: DB Failover/Resiliency Tunables ----------
    "DB_MULTISUBNET_FAILOVER": 1,
    "DB_CONNECT_RETRY_COUNT": 3,
    "DB_CONNECT_RETRY_INTERVAL": 3,
    "DB_LOGIN_TIMEOUT": 15,
    "DB_MULTISUBNET_FAILOVER_ON_IP": 0,    # << สำคัญ: ต่อ IP ตรง ๆ ให้ปิด
    "DB_SECONDARY_SLEEP_SEC": 15,          # ถ้าเจอแต่ secondary ให้พักก่อนค่อยลองใหม่


    "DB_CONNECT_MAX_ATTEMPTS": 6,
    "DB_SELECT_MAX_ATTEMPTS": 5,
    "DB_UPDATE_MAX_ATTEMPTS": 6,

    # ---------- NEW: rate-limit retry ตอน failover ----------
    "DB_RECONNECT_MIN_INTERVAL_SEC": 5,
    "DB_RETRY_JITTER_MAX_SEC": 0.5,

    # Logging
    "log_Level": "DEBUG",
    "Log_Console": 1,
    "log_Backup": 90,
    "Log_Size": "10 MB",
}

config = Load_Config(default_config, Program_Name)
logger = Loguru_Logging(config, Program_Name, Program_Version)
logger.debug("Loaded configuration: {}", config)

# ----------------------- Circuit Breaker Configuration -----------------------
TARGET_URLS = config.get('TARGET_URL', [])
URL_TIMEOUT_THRESHOLD = config.get('URL_TIMEOUT_THRESHOLD', 5)
URL_DISABLE_DURATION_SECONDS = config.get('URL_DISABLE_DURATION_SECONDS', 3600)

URL_STATUS = {url: {"failures": 0, "disabled_until": None} for url in TARGET_URLS}
URL_STATUS_LOCK = threading.Lock()
target_url_cycler = itertools.cycle(TARGET_URLS) if TARGET_URLS else None

# ----------------------- SQL Query ตาม Lane_Type -----------------------
Lane_Type = config.get('Lane_Type', 'ALL')
if Lane_Type == 'ALL':
    SQL_QUERY = f"""
        SELECT TOP {config.get('QUERY_LIMIT', 100)}
            dpt.DMTPX_ID,
            dpt.DMTPX_PLAZA_ID,
            dpt.DMTPX_LANE_ID,
            dpt.DMTPX_NTRX_NO,
            dpt.DMTPX_TRX_DATETIME,
            dpt.DMTPX_TC_PAYMENTMETHOD_ID,
            dpt.DMTPX_SIGNAL_CODE,
            dpt.DMTPX_LICENCEPLATE,
            dpt.DMTPX_PROVINCEID,
            dpt.DMTPX_IMAGE_FILE_02 as Image1,
            dpt.DMTPX_IMAGE_FILE_01 as Image2,
            dpt.DMTPX_IMAGE_FILE_03 as Image3
        FROM DMT_PASSING_TRANSACTION dpt
        WHERE dpt.DMTPX_TRX_DATETIME
            BETWEEN DATEADD(DAY,-{config.get('Back_date',7)},CONCAT(CONVERT(DATE,GETDATE()),' 00:00:00.000'))
                AND DATEADD(minute,-{config.get('Back_Time',10)},GETDATE())
        AND dpt.DMTPX_TRX_DATETIME >= '2025-07-25 00:00:00.000'
        AND dpt.DMTPX_TC_PAYMENTMETHOD_ID IN (1,2,3,4,5,6,9,17,18,19,20)
        AND dpt.DMTPX_SIGNAL_CODE IN (1,2,10,19,82,20,22,23,24,26)
        AND (dpt.DMTPX_LICENCEPLATE IS NULL OR dpt.DMTPX_LICENCEPLATE = '')
        ORDER BY dpt.DMTPX_TRX_DATETIME;
    """
elif Lane_Type == 'MTC':
    SQL_QUERY = f"""
        SELECT TOP {config.get('QUERY_LIMIT', 100)}
            dpt.DMTPX_ID,
            dpt.DMTPX_PLAZA_ID,
            dpt.DMTPX_LANE_ID,
            dpt.DMTPX_NTRX_NO,
            dpt.DMTPX_TRX_DATETIME,
            dpt.DMTPX_TC_PAYMENTMETHOD_ID,
            dpt.DMTPX_SIGNAL_CODE,
            dpt.DMTPX_LICENCEPLATE,
            dpt.DMTPX_PROVINCEID,
            dpt.DMTPX_IMAGE_FILE_02 as Image1,
            dpt.DMTPX_IMAGE_FILE_01 as Image2,
            dpt.DMTPX_IMAGE_FILE_03 as Image3
        FROM DMT_PASSING_TRANSACTION dpt
        WHERE dpt.DMTPX_TRX_DATETIME
            BETWEEN DATEADD(DAY,-{config.get('Back_date',7)},CONCAT(CONVERT(DATE,GETDATE()),' 00:00:00.000'))
                AND DATEADD(minute,-{config.get('Back_Time',10)},GETDATE())
        AND dpt.DMTPX_TRX_DATETIME >= '2025-07-25 00:00:00.000'
        AND dpt.DMTPX_TC_PAYMENTMETHOD_ID IN (1,2,3,4,17,18,19,20)
        AND dpt.DMTPX_SIGNAL_CODE IN (1,2,10,19,82,20,22,23,24,26)
        AND (dpt.DMTPX_LICENCEPLATE IS NULL OR dpt.DMTPX_LICENCEPLATE = '')
        ORDER BY dpt.DMTPX_TRX_DATETIME;
    """
else:  # ETC
    SQL_QUERY = f"""
        SELECT TOP {config.get('QUERY_LIMIT', 100)}
            dpt.DMTPX_ID,
            dpt.DMTPX_PLAZA_ID,
            dpt.DMTPX_LANE_ID,
            dpt.DMTPX_NTRX_NO,
            dpt.DMTPX_TRX_DATETIME,
            dpt.DMTPX_TC_PAYMENTMETHOD_ID,
            dpt.DMTPX_SIGNAL_CODE,
            dpt.DMTPX_LICENCEPLATE,
            dpt.DMTPX_PROVINCEID,
            dpt.DMTPX_IMAGE_FILE_02 as Image1,
            dpt.DMTPX_IMAGE_FILE_01 as Image2,
            dpt.DMTPX_IMAGE_FILE_03 as Image3
        FROM DMT_PASSING_TRANSACTION dpt
        WHERE dpt.DMTPX_TRX_DATETIME
            BETWEEN DATEADD(DAY,-{config.get('Back_date',7)},CONCAT(CONVERT(DATE,GETDATE()),' 00:00:00.000'))
                AND DATEADD(minute,-{config.get('Back_Time',10)},GETDATE())
        AND dpt.DMTPX_TRX_DATETIME >= '2025-07-25 00:00:00.000'
        AND dpt.DMTPX_TC_PAYMENTMETHOD_ID IN (5,6,9)
        AND dpt.DMTPX_SIGNAL_CODE IN (1,2,10,19,82,20,22,23,24,26)
        AND (dpt.DMTPX_LICENCEPLATE IS NULL OR dpt.DMTPX_LICENCEPLATE = '')
        ORDER BY dpt.DMTPX_TRX_DATETIME;
    """

# ----------------------- HTTP Session Pool (ต่อเธรด) -----------------------
_SESSION_LOCAL = threading.local()

def _build_http_session() -> requests.Session:
    s = requests.Session()
    pool = int(config.get("HTTP_POOL_MAXSIZE", 64))
    retries = int(config.get("HTTP_POOL_RETRIES", 1))
    backoff = float(config.get("HTTP_BACKOFF_FACTOR", 0.2))

    if HTTPAdapter is not None and Retry is not None:
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff,
            status_forcelist=(502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"])
        )
        adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool, max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
    return s

def get_http_session() -> requests.Session:
    s = getattr(_SESSION_LOCAL, "session", None)
    if s is None:
        s = _build_http_session()
        _SESSION_LOCAL.session = s
        logger.debug("HTTP session constructed with pooled adapters for current thread.")
    return s

# ----------------------- DB Connection (เดิม) -----------------------
def get_db_connection():
    try:
        conn_str = (
            f"DRIVER={config.get('DB_DRIVER', 'ODBC Driver 17 for SQL Server')};"
            f"SERVER={config.get('DB_SERVER')};"
            f"PORT={config.get('DB_PORT', 1433)};"
            f"DATABASE={config.get('DB_DATABASE')};"
            f"UID={config.get('DB_USERNAME')};"
            f"PWD={config.get('DB_PASSWORD')};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"MARS_Connection=yes;"
        )
        autocommit = bool(int(config.get("DB_AUTOCOMMIT", 1)))
        conn = pyodbc.connect(conn_str, autocommit=autocommit)
        try:
            conn.timeout = int(config.get("DB_TIMEOUT", 30))
        except Exception:
            pass
        logger.info("Database connection established successfully (autocommit={} ).", autocommit)
        return conn
    except Exception as e:
        logger.error("Failed to connect to database: {}", e)
        return None

# ----------------------- Image base_urls Round-robin -----------------------
base_urls = config.get('base_urls', [])
if base_urls:
    url_cycler = itertools.cycle(base_urls)
    logger.info("Initialized round-robin cycler for base image URLs.")
else:
    url_cycler = None
    logger.warning("base_urls is empty. Image downloading will fail.")

# ----------------------- Circuit Breaker helpers -----------------------
def handle_api_failure(url: str):
    with URL_STATUS_LOCK:
        status = URL_STATUS[url]
        status["failures"] += 1
        logger.warning("API failure for {}. Failure count={}.", url, status["failures"])
        if status["failures"] >= URL_TIMEOUT_THRESHOLD:
            status["disabled_until"] = datetime.now() + timedelta(seconds=URL_DISABLE_DURATION_SECONDS)
            logger.critical("Circuit Breaker tripped for {}. Disabled until {}.", url, status["disabled_until"])

def handle_api_success(url: str):
    with URL_STATUS_LOCK:
        status = URL_STATUS[url]
        if status["failures"] > 0 or status["disabled_until"] is not None:
            logger.info("API {} is healthy. Resetting failure counter and enabling.", url)
        status["failures"] = 0
        status["disabled_until"] = None

def get_available_target_url() -> Optional[str]:
    if not target_url_cycler:
        return None

    with URL_STATUS_LOCK:
        for _ in range(len(TARGET_URLS)):
            url = next(target_url_cycler)
            status = URL_STATUS[url]
            if status["disabled_until"]:
                if datetime.now() >= status["disabled_until"]:
                    logger.info("Re-enabling TARGET_URL: {}", url)
                    status["failures"] = 0
                    status["disabled_until"] = None
                    return url
                else:
                    continue
            else:
                return url
    return None

def get_soonest_reenable_eta() -> Optional[datetime]:
    with URL_STATUS_LOCK:
        times = [v["disabled_until"] for v in URL_STATUS.values() if v["disabled_until"]]
    return min(times) if times else None

def wait_for_available_target_url(max_sleep_chunk: int = 30):
    while True:
        url = get_available_target_url()
        if url:
            return url
        eta = get_soonest_reenable_eta()
        now = datetime.now()
        if eta and eta > now:
            sleep_secs = max(1, int((eta - now).total_seconds()))
            chunk = min(sleep_secs, max_sleep_chunk)
            logger.warning("All TARGET_URLs are in breaker. Waiting ~{}s (sleep {}s) until {}.",
                           sleep_secs, chunk, eta)
            time.sleep(chunk)
        else:
            logger.warning("All TARGET_URLs are in breaker with no ETA. Retrying in 10s...")
            time.sleep(10)

# ----------------------- Helpers for download & ALPR -----------------------
def download_image(image_path: Optional[str], transaction_id: int) -> Tuple[Optional[bytes], str]:
    if not url_cycler:
        logger.error("Image base_urls are not configured. Cannot download image.")
        return None, 'no_base_url'

    if not image_path:
        logger.error("image_path is None/empty for transaction_id={}", transaction_id)
        return None, 'no_image'

    image_path = image_path.lstrip('/')

    start_url = next(url_cycler)
    start_index = base_urls.index(start_url)
    ordered_urls_to_try = base_urls[start_index:] + base_urls[:start_index]
    logger.debug("[download_image] tx={} URL order: {}", transaction_id, ordered_urls_to_try)

    connect_to = float(config.get("HTTP_CONNECT_TIMEOUT", 5))
    read_to = float(config.get("IMAGE_DOWNLOAD_TIMEOUT", config.get("HTTP_READ_TIMEOUT", 15)))
    session = get_http_session()

    saw_timeout = False

    for base_url in ordered_urls_to_try:
        url = base_url.rstrip('/') + '/' + image_path
        try:
            logger.debug("[download_image] GET {}", url)
            resp = session.get(url, timeout=(connect_to, read_to))
            resp.raise_for_status()
            logger.info("[download_image] Success: {}", url)
            return resp.content, 'ok'
        except requests.exceptions.Timeout as e:
            saw_timeout = True
            logger.warning("[download_image] Timeout at {} ({}) — will try next base.", url, e)
            continue
        except requests.exceptions.RequestException as e:
            logger.debug("[download_image] Non-timeout HTTP error at {} ({}). Trying next...", url, e)
            continue

    reason = 'timeout' if saw_timeout else 'not_found'
    logger.error("[download_image] All base_urls tried, image not retrieved for tx={} (reason={})",
                 transaction_id, reason)
    return None, reason

def call_alpr(image_bytes: bytes, transaction_datetime: datetime):
    if not image_bytes:
        return 'error', None, None, None

    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    payload = {
        "data_type": "alpr_recognition",
        "hw_id": "a1027724-70dd-4b92-85ad-cdb0984ddd62",
        "user_id": "001",
        "os": "Win32NT",
        "date_time": transaction_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        "license_plate_rec": "true",
        "alpr_image": base64_image,
        "latitude": "",
        "longitude": "",
        "country": "th",
        "Place": ""
    }

    target_url = get_available_target_url()
    if not target_url:
        logger.warning("[call_alpr] All TARGET_URLs are disabled. Waiting for re-enable...")
        target_url = wait_for_available_target_url()

    read_to = float(config.get('API_TIMEOUT', 10))
    connect_to = float(config.get("HTTP_CONNECT_TIMEOUT", 5))
    session = get_http_session()

    try:
        logger.info("[call_alpr] POST {}", target_url)
        r = session.post(target_url, json=payload, timeout=(connect_to, read_to))
        r.raise_for_status()

        data = r.json() if r.content else {}
        status = (data.get("status") or "").strip()
        lp = (data.get("LicensePlate") or "").strip()
        prov = (data.get("Province") or "").strip()
        conf_raw = data.get("confidence", None)

        conf = None
        if conf_raw not in (None, ""):
            try:
                conf = float(conf_raw)
            except Exception:
                conf = None

        status_lower = status.lower()
        if status_lower == 'ok':
            if not lp:
                handle_api_success(target_url)
                return 'no_plate', "", "", conf
            handle_api_success(target_url)
            return 'ok', lp, prov, conf

        if status_lower in ('nolicenseplate', 'no license plate'):
            handle_api_success(target_url)
            return 'no_plate', "", "", conf

        if status in {'ErrorSeverFail', 'ErrorRequestFail', 'ErrorHttpFail', 'ErrorInternalFail'}:
            logger.warning("[call_alpr] ALPR error status: {}", status)
            handle_api_failure(target_url)
            return 'error', None, None, None

        logger.warning("[call_alpr] Unexpected status from ALPR: {}", status)
        return 'unknown_error', None, None, None

    except requests.exceptions.Timeout as e:
        logger.error("[call_alpr] Timeout calling {}: {}", target_url, e)
        handle_api_failure(target_url)
        return 'error', None, None, None
    except requests.exceptions.RequestException as e:
        logger.error("[call_alpr] HTTP error calling {}: {}", target_url, e)
        handle_api_failure(target_url)
        return 'error', None, None, None
    except Exception as e:
        logger.error("[call_alpr] Unexpected exception: {}", e, exc_info=True)
        return 'unknown_error', None, None, None

# ----------------------- Core: Process one row -----------------------
def process_single_transaction(row, update_queue):
    try:
        logger.debug("[process_single_transaction] Start row compact (id, time, img1..3): {}, {}, {}, {}, {}",
                     row[0], row[4], row[9], row[10], row[11])

        transaction_id = row[0]
        plaza_id = row[1]
        lane_id = row[2]
        ntrx_no = row[3]
        transaction_datetime = row[4]
        image_paths = [row[9], row[10], row[11]]

        download_results: List[Tuple[int, Optional[bytes], str]] = []
        max_img_workers = max(1, int(config.get("IMG_DL_WORKERS", 3)))
        logger.debug("[process_single_transaction] tx={} downloading images with {} workers", transaction_id, max_img_workers)

        def _dl(idx_path):
            idx, p = idx_path
            img_bytes, reason = download_image(p, transaction_id)
            return (idx, img_bytes, reason)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_img_workers, thread_name_prefix="imgdl") as ex:
            futs = [ex.submit(_dl, (i, p)) for i, p in enumerate(image_paths, start=1)]
            for f in concurrent.futures.as_completed(futs):
                idx, img_bytes, reason = f.result()
                download_results.append((idx, img_bytes, reason))
                logger.debug("[process_single_transaction] tx={} image idx={} result={} bytes={}",
                             transaction_id, idx, reason, len(img_bytes) if img_bytes else 0)

        all_failed = all(img_bytes is None for _, img_bytes, _ in download_results)
        if all_failed:
            logger.warning("[process_single_transaction] tx={} All images missing -> Update DB: 'No Image'", transaction_id)
            update_queue.put((transaction_id, "No Image", "", transaction_datetime, plaza_id, lane_id, ntrx_no, None))
            return

        best = {"conf": float("-inf"), "lp": None, "prov": None, "source": None}
        no_plate_count = 0
        alpr_called = 0

        for idx, img_bytes, reason in sorted(download_results, key=lambda t: t[0]):
            if not img_bytes:
                continue

            status_tag, lp, prov, conf = call_alpr(img_bytes, transaction_datetime)
            alpr_called += 1
            logger.debug("[process_single_transaction] tx={} ALPR idx={} status={} lp='{}' prov='{}' conf={}",
                         transaction_id, idx, status_tag, lp, prov, conf)

            if status_tag == 'ok':
                c = conf if conf is not None else -1.0
                if c > best["conf"]:
                    best.update({
                        "conf": c,
                        "lp": lp or "",
                        "prov": prov or "",
                        "source": f"IMAGE_FILE_0{idx}"
                    })
            elif status_tag == 'no_plate':
                no_plate_count += 1

        if best["lp"] is not None and best["lp"] != "" and best["conf"] != float("-inf"):
            logger.info("[process_single_transaction] tx={} Update with BEST (conf={}) src={}", transaction_id, best['conf'], best['source'])
            update_queue.put((
                transaction_id, best["lp"], best["prov"],
                transaction_datetime, plaza_id, lane_id, ntrx_no, best["source"]
            ))
            return

        if alpr_called > 0 and no_plate_count == alpr_called:
            logger.info("[process_single_transaction] tx={} All ALPR results = NoLicensePlate -> Update DB: 'No Plate'", transaction_id)
            update_queue.put((transaction_id, "No Plate", "", transaction_datetime, plaza_id, lane_id, ntrx_no, None))
            return

        logger.warning("[process_single_transaction] tx={} No definitive result (errors/mixed). Skip DB update.", transaction_id)
        return

    except Exception as e:
        logger.error("Exception in process_single_transaction: {}", e, exc_info=True)

# ----------------------- Process batch (เดิม) -----------------------
def process_transactions():
    conn = get_db_connection()
    if not conn:
        logger.error("Could not establish a database connection. Aborting this run.")
        return

    try:
        cursor = conn.cursor()
        try:
            cursor.timeout = int(config.get("DB_TIMEOUT", 30))
        except Exception:
            pass

        cursor.execute(SQL_QUERY)
        rows = cursor.fetchall()
        logger.info("Fetched {} records to process.", len(rows))
        if rows:
            logger.debug("Sample row (first id/time): {}, {}", rows[0][0], rows[0][4])
        if not rows:
            conn.close()
            logger.info("No records. Database connection closed.")
            return

        update_queue: "queue.Queue" = queue.Queue()

        def db_update_worker():
            logger.debug("[db_update_worker] Started.")
            while True:
                item = update_queue.get()
                logger.debug("[db_update_worker] Got item. Queue size after get: {}", update_queue.qsize())
                if item is None:
                    logger.debug("[db_update_worker] Received shutdown signal.")
                    update_queue.task_done()
                    break
                try:
                    update_transaction_result(conn, *item)
                except Exception as e:
                    logger.error("DB update worker error: {}", e)
                finally:
                    update_queue.task_done()

        db_thread = threading.Thread(target=db_update_worker, name="db-update")
        db_thread.start()

        if config.get('Enable_Fix_Workers', 0) >= 1:
            workers = max(1, int(config.get('Workers', 4)))
            logger.info("Using fixed {} worker threads.", workers)
        else:
            workers = max(1, len(TARGET_URLS))
            if workers == 0:
                logger.error("No TARGET_URL configured. Cannot process.")
                update_queue.put(None)
                db_thread.join()
                conn.close()
                return
            logger.info("Using {} worker threads based on number of TARGET_URLs.", workers)

        if not get_available_target_url():
            logger.critical("All TARGET_URLs are currently disabled by the circuit breaker. Waiting until available...")
            wait_for_available_target_url()

        logger.debug("[process_transactions] Starting ThreadPoolExecutor with {} workers.", workers)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tx") as executor:
            futures = [executor.submit(process_single_transaction, row, update_queue) for row in rows]
            concurrent.futures.wait(futures)
            logger.info("All processing tasks for this batch have been completed.")

        logger.debug("[process_transactions] Waiting for update_queue to become empty...")
        update_queue.join()
        logger.debug("[process_transactions] Queue empty. Sending shutdown signal to db_update_worker.")
        update_queue.put(None)
        db_thread.join()
        logger.info("All DB updates completed.")

        conn.close()
        logger.info("Database connection closed.")

    except Exception as e:
        logger.critical("An unhandled exception occurred in process_transactions: {}", e, exc_info=True)
        try:
            conn.close()
        except Exception:
            pass

# ----------------------- DB Update (เดิม) -----------------------
def update_transaction_result(conn, transaction_id, license_plate, province, transaction_datetime, plaza_id, lane_id, ntrx_no, image_source):
    try:
        with conn.cursor() as cursor:
            try:
                cursor.timeout = int(config.get("DB_TIMEOUT", 30))
            except Exception:
                pass

            if license_plate == "" and province == "":
                logger.debug("DB UPDATE (No LPR): id={} src={} ntrx={}", transaction_id, image_source, ntrx_no)
                cursor.execute(
                    "UPDATE DMT_PASSING_TRANSACTION SET DMTPX_LICENCEPLATE = ?, DMTPX_PROMOTIONID = ? "
                    "WHERE DMTPX_TRX_DATETIME = ? and DMTPX_ID = ? and DMTPX_PLAZA_ID = ? and DMTPX_LANE_ID = ? and DMTPX_NTRX_NO = ?",
                    ("No LPR", image_source, transaction_datetime, transaction_id, plaza_id, lane_id, ntrx_no)
                )
            else:
                logger.debug("DB UPDATE (LPR): id={} lp='{}' prov='{}' src={} ntrx={}",
                             transaction_id, license_plate, province, image_source, ntrx_no)
                cursor.execute(
                    "UPDATE DMT_PASSING_TRANSACTION SET DMTPX_LICENCEPLATE = ?, DMTPX_PROVINCEID = ?, DMTPX_PROMOTIONID = ? "
                    "WHERE DMTPX_TRX_DATETIME = ? and DMTPX_ID = ? and DMTPX_PLAZA_ID = ? and DMTPX_LANE_ID = ? and DMTPX_NTRX_NO = ?",
                    (license_plate, province, image_source, transaction_datetime, transaction_id, plaza_id, lane_id, ntrx_no)
                )

            if not getattr(conn, "autocommit", False):
                conn.commit()
        logger.info("Database updated successfully (transaction_id: {}).", transaction_id)
    except pyodbc.Error as e:
        logger.error("Database update failed for transaction_id {}: {}", transaction_id, e)
        try:
            if not getattr(conn, "autocommit", False):
                conn.rollback()
                logger.warning("Transaction rolled back for transaction_id {}.", transaction_id)
        except pyodbc.Error as rb_e:
            logger.error("Failed to rollback transaction: {}", rb_e)

# ============================================================================================
# FAILOVER + PRIMARY/SECONDARY LOG + METRICS + RATE LIMIT PATCH
# เงื่อนไข: ไม่แก้ body ฟังก์ชันเดิม -> ใช้วิธี monkey patch แทน
# ============================================================================================
_DB_CONN_LOCK = threading.Lock()
_DB_CONN = None
_DB_LAST_RECONNECT_TS = 0.0

_DB_METRICS = {
    "connect_attempts": 0,
    "connect_success": 0,
    "reconnect_count": 0,
    "select_retries": 0,
    "update_retries": 0,
    "readonly_hits": 0,
    "primary_connects": 0,
    "secondary_connects": 0,
}

_TRANSIENT_SQLSTATES = {"08S01", "HYT00", "HYT01", "IMC06", "40001"}

def _close_global_db():
    global _DB_CONN
    with _DB_CONN_LOCK:
        if _DB_CONN is not None:
            try:
                _DB_CONN.close()
            except Exception:
                pass
        _DB_CONN = None


class _PrimaryNotAvailable(Exception):
    pass

def _is_ip(host: str) -> bool:
    # IPv4 แบบง่าย ๆ พอใช้งานจริง
    return bool(re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", (host or "").strip()))

def _metrics_reset():
    for k in _DB_METRICS:
        _DB_METRICS[k] = 0

def _metrics_log_summary():
    logger.info(
        "[DB METRICS] connect_attempts={} connect_success={} reconnects={} "
        "select_retries={} update_retries={} readonly_hits={} primary_connects={} secondary_connects={}",
        _DB_METRICS["connect_attempts"], _DB_METRICS["connect_success"], _DB_METRICS["reconnect_count"],
        _DB_METRICS["select_retries"], _DB_METRICS["update_retries"], _DB_METRICS["readonly_hits"],
        _DB_METRICS["primary_connects"], _DB_METRICS["secondary_connects"],
    )

def _is_readonly_error(e: Exception) -> bool:
    s = str(e).lower()
    return ("read-only" in s) or ("3906" in s) or ("sqlstate=25000" in s) or ("25000" in s)

def _is_transient_pyodbc_error(e: Exception) -> bool:
    try:
        if hasattr(e, "args") and e.args:
            for a in e.args:
                s = str(a)
                for st in _TRANSIENT_SQLSTATES:
                    if st in s:
                        return True
        msg = str(e).lower()
        hints = (
            "communication link failure",
            "transport-level error",
            "timeout",
            "login timeout",
            "connection reset",
            "network-related",
            "could not connect",
            "server was not found",
            "connection is broken",
            "recovery is not possible",
            "tcp provider",
            "imc06",
            "08s01",
            "hyt00",
        )
        return any(h in msg for h in hints)
    except Exception:
        return False

def _rate_limit_reconnect():
    global _DB_LAST_RECONNECT_TS
    min_int = float(config.get("DB_RECONNECT_MIN_INTERVAL_SEC", 5))
    jitter = float(config.get("DB_RETRY_JITTER_MAX_SEC", 0.5))

    now = time.time()
    wait = (_DB_LAST_RECONNECT_TS + min_int) - now
    if wait > 0:
        sleep = wait + random.random() * jitter
        logger.warning("[DB RATE LIMIT] waiting {:.2f}s before reconnect (min_interval={}s)", sleep, min_int)
        time.sleep(sleep)

    _DB_LAST_RECONNECT_TS = time.time()

def _server_candidates() -> List[str]:
    lst = config.get("DB_SERVER_LIST", None)
    if isinstance(lst, list) and len(lst) > 0:
        return [str(x).strip() for x in lst if str(x).strip()]
    s = str(config.get("DB_SERVER", "") or "").strip()
    return [s] if s else []

def _build_conn_str_for(server: str) -> str:
    driver = config.get("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    port = int(config.get("DB_PORT", 1433))
    db = config.get("DB_DATABASE")
    uid = config.get("DB_USERNAME")
    pwd = config.get("DB_PASSWORD")

    # ✅ เงื่อนไขสำคัญ: ถ้าเป็น IP และ DB_MULTISUBNET_FAILOVER_ON_IP=0 => ไม่ใส่ MultiSubnetFailover
    msf_cfg = int(config.get("DB_MULTISUBNET_FAILOVER", 1))
    msf_on_ip = int(config.get("DB_MULTISUBNET_FAILOVER_ON_IP", 0))
    use_msf = (msf_cfg == 1) and (not _is_ip(server) or msf_on_ip == 1)
    msf = "Yes" if use_msf else "No"

    retry_count = int(config.get("DB_CONNECT_RETRY_COUNT", 3))
    retry_interval = int(config.get("DB_CONNECT_RETRY_INTERVAL", 3))
    login_timeout = int(config.get("DB_LOGIN_TIMEOUT", 15))
    app_name = f"{Program_Name}/{Program_Version}"

    server_part = f"tcp:{server},{port}" if server else ""

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server_part};"
        f"DATABASE={db};"
        f"UID={uid};PWD={pwd};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"MARS_Connection=yes;"
        f"MultiSubnetFailover={msf};"
        f"ConnectRetryCount={retry_count};"
        f"ConnectRetryInterval={retry_interval};"
        f"LoginTimeout={login_timeout};"
        f"APP={app_name};"
    )

def _conn_role_info(conn) -> Tuple[str, str]:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT CAST(SERVERPROPERTY('ServerName') AS nvarchar(256))")
            server_name = cur.fetchone()[0] or "UNKNOWN_SERVER"
        role = "UNKNOWN"
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT sys.fn_hadr_is_primary_replica(DB_NAME())")
                v = cur.fetchone()[0]
            if v is None:
                role = "UNKNOWN"
            elif int(v) == 1:
                role = "PRIMARY"
            else:
                role = "SECONDARY"
        except Exception:
            role = "UNKNOWN"
        return server_name, role
    except Exception:
        return "UNKNOWN_SERVER", "UNKNOWN"

def _connect_new_any_endpoint():
    """
    ✅ เปลี่ยน logic:
    - “พยายามหา PRIMARY เท่านั้น”
    - ถ้า connect ได้แต่เป็น SECONDARY -> ปิด conn แล้วลอง endpoint ถัดไป
    - ถ้าไม่มี PRIMARY เลย -> โยน _PrimaryNotAvailable เพื่อให้ caller ไปพักแล้วค่อยลองใหม่
    """
    autocommit = bool(int(config.get("DB_AUTOCOMMIT", 1)))
    candidates = _server_candidates()
    if not candidates:
        raise RuntimeError("No DB_SERVER/DB_SERVER_LIST configured")

    last_exc = None
    saw_secondary = False

    for endpoint in candidates:
        try:
            _DB_METRICS["connect_attempts"] += 1
            conn = pyodbc.connect(_build_conn_str_for(endpoint), autocommit=autocommit)
            try:
                conn.timeout = int(config.get("DB_TIMEOUT", 30))
            except Exception:
                pass

            server_name, role = _conn_role_info(conn)

            if role == "PRIMARY":
                _DB_METRICS["connect_success"] += 1
                _DB_METRICS["primary_connects"] += 1
                logger.info("[DB CONNECT] endpoint={} -> connected server='{}' role=PRIMARY", endpoint, server_name)
                return conn

            if role == "SECONDARY":
                saw_secondary = True
                _DB_METRICS["connect_success"] += 1
                _DB_METRICS["secondary_connects"] += 1
                logger.warning("[DB CONNECT] endpoint={} -> connected server='{}' role=SECONDARY (skip & try next)", endpoint, server_name)
                try:
                    conn.close()
                except Exception:
                    pass
                continue

            # role UNKNOWN -> ปิดและลองตัวถัดไป (กัน update พลาด)
            logger.warning("[DB CONNECT] endpoint={} -> connected server='{}' role=UNKNOWN (skip & try next)", endpoint, server_name)
            try:
                conn.close()
            except Exception:
                pass

        except Exception as e:
            last_exc = e
            logger.warning("[DB CONNECT] endpoint={} failed: {}", endpoint, e)

    if saw_secondary:
        raise _PrimaryNotAvailable("Only SECONDARY available (PRIMARY not reachable)")
    raise last_exc if last_exc else RuntimeError("DB connect failed all endpoints")

def _get_or_reconnect_db(force: bool = False):
    global _DB_CONN

    with _DB_CONN_LOCK:
        if force or _DB_CONN is None:
            _rate_limit_reconnect()
            _DB_METRICS["reconnect_count"] += 1
            try:
                if _DB_CONN is not None:
                    try:
                        _DB_CONN.close()
                    except Exception:
                        pass
            finally:
                _DB_CONN = None

        if _DB_CONN is None:
            max_attempts = int(config.get("DB_CONNECT_MAX_ATTEMPTS", 6))
            base = 0.5
            last = None
            for attempt in range(1, max_attempts + 1):
                try:
                    _DB_CONN = _connect_new_any_endpoint()
                    return _DB_CONN
                except _PrimaryNotAvailable as e:
                    # ✅ เจอแต่ secondary -> พักยาวขึ้นแล้วค่อยลองใหม่ (กัน loop 3906)
                    sleep_sec = int(config.get("DB_SECONDARY_SLEEP_SEC", 15))
                    logger.warning("[DB FAILOVER] {} -> sleep {}s then retry", e, sleep_sec)
                    time.sleep(sleep_sec)
                    last = e
            raise last

        try:
            cur = _DB_CONN.cursor()
            try:
                cur.timeout = int(config.get("DB_TIMEOUT", 30))  # กัน SELECT 1 ค้าง
            except Exception:
                pass
            cur.execute("SELECT 1")
            cur.fetchone()
        except Exception as e:
            logger.warning("DB connection seems broken ({}). Forcing reconnect...", e)
            # ไม่เรียกตัวเองซ้ำใน lock -> ปลอด deadlock
            force = True
            # ทำการปิด conn เดิมแล้วสร้างใหม่ต่อด้านล่าง
            try:
                _DB_CONN.close()
            except Exception:
                pass
            _DB_CONN = None

        return _DB_CONN

def _is_primary_replica(conn) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sys.fn_hadr_is_primary_replica(DB_NAME())")
            v = cur.fetchone()[0]
        if v is None:
            return True
        return int(v) == 1
    except Exception:
        return True

def get_db_connection_failover_ready():
    try:
        conn = _get_or_reconnect_db(force=False)
        server_name, role = _conn_role_info(conn)
        logger.info("[DB STATE] connected server='{}' role={}", server_name, role)
        return conn  # ✅ คืน conn เสมอถ้าเชื่อมได้
    except Exception as e:
        logger.error("Failed to connect to database (failover-ready): {}", e)
        return None


def update_transaction_result_failover_ready(conn, transaction_id, license_plate, province,
                                             transaction_datetime, plaza_id, lane_id, ntrx_no, image_source):
    max_attempts = int(config.get("DB_UPDATE_MAX_ATTEMPTS", 6))
    base = 0.5
    last_exc = None

    for attempt in range(1, max_attempts + 1):
        try:
            conn = _get_or_reconnect_db(force=False)

            if not _is_primary_replica(conn):
                _DB_METRICS["readonly_hits"] += 1
                raise pyodbc.Error("3906: database is read-only (secondary during failover)")

            with conn.cursor() as cursor:
                try:
                    cursor.timeout = int(config.get("DB_TIMEOUT", 30))
                except Exception:
                    pass

                if license_plate == "" and province == "":
                    cursor.execute(
                        "UPDATE DMT_PASSING_TRANSACTION "
                        "SET DMTPX_LICENCEPLATE = ?, DMTPX_PROMOTIONID = ? "
                        "WHERE DMTPX_TRX_DATETIME = ? and DMTPX_ID = ? and DMTPX_PLAZA_ID = ? and DMTPX_LANE_ID = ? and DMTPX_NTRX_NO = ?",
                        ("No LPR", image_source, transaction_datetime, transaction_id, plaza_id, lane_id, ntrx_no)
                    )
                else:
                    cursor.execute(
                        "UPDATE DMT_PASSING_TRANSACTION "
                        "SET DMTPX_LICENCEPLATE = ?, DMTPX_PROVINCEID = ?, DMTPX_PROMOTIONID = ? "
                        "WHERE DMTPX_TRX_DATETIME = ? and DMTPX_ID = ? and DMTPX_PLAZA_ID = ? and DMTPX_LANE_ID = ? and DMTPX_NTRX_NO = ?",
                        (license_plate, province, image_source, transaction_datetime, transaction_id, plaza_id, lane_id, ntrx_no)
                    )

                if not getattr(conn, "autocommit", False):
                    conn.commit()

            logger.info("Database updated successfully (transaction_id: {}).", transaction_id)
            return

        except pyodbc.Error as e:
            last_exc = e
            transient = _is_transient_pyodbc_error(e) or _is_readonly_error(e)

            if (attempt == max_attempts) or (not transient):
                logger.error("Database update failed (non-transient or maxed) tx={} : {}", transaction_id, e)
                try:
                    if conn and not getattr(conn, "autocommit", False):
                        conn.rollback()
                except Exception:
                    pass
                raise

            _DB_METRICS["update_retries"] += 1
            logger.warning("Transient DB error. tx={} attempt {}/{}: {}", transaction_id, attempt, max_attempts, e)

            _get_or_reconnect_db(force=True)

            sleep = min(10.0, base * (2 ** (attempt - 1))) + random.random() * float(config.get("DB_RETRY_JITTER_MAX_SEC", 0.5))
            time.sleep(sleep)

    raise last_exc

def process_transactions_failover_ready():
    _metrics_reset()

    conn = get_db_connection_failover_ready()
    if not conn:
        logger.error("Could not establish a database connection. Aborting this run.")
        _metrics_log_summary()
        return

    max_attempts = int(config.get("DB_SELECT_MAX_ATTEMPTS", 5))
    base = 0.5
    rows = None
    last_exc = None

    for attempt in range(1, max_attempts + 1):
        try:
            cursor = conn.cursor()
            try:
                cursor.timeout = int(config.get("DB_TIMEOUT", 30))
            except Exception:
                pass

            cursor.execute(SQL_QUERY)
            rows = cursor.fetchall()
            break
        except pyodbc.Error as e:
            last_exc = e
            if attempt == max_attempts or not _is_transient_pyodbc_error(e):
                logger.critical("SELECT failed (non-transient or maxed): {}", e, exc_info=True)
                _metrics_log_summary()
                raise

            _DB_METRICS["select_retries"] += 1
            logger.warning("SELECT transient error attempt {}/{}: {} -> reconnect", attempt, max_attempts, e)
            conn = _get_or_reconnect_db(force=True)
            sleep = min(10.0, base * (2 ** (attempt - 1))) + random.random() * float(config.get("DB_RETRY_JITTER_MAX_SEC", 0.5))
            time.sleep(sleep)

    if rows is None:
        logger.critical("SELECT exhausted retries. last_error={}", last_exc)
        _metrics_log_summary()
        return

    logger.info("Fetched {} records to process.", len(rows))
    if not rows:
        try:
            conn.close()
        except Exception:
            pass
        logger.info("No records. Database connection closed.")
        _metrics_log_summary()
        return

    update_queue: "queue.Queue" = queue.Queue()

    def db_update_worker():
        while True:
            item = update_queue.get()
            if item is None:
                update_queue.task_done()
                break
            try:
                update_transaction_result(conn, *item)
            except Exception as e:
                logger.error("DB update worker error: {}", e, exc_info=True)
            finally:
                update_queue.task_done()

    db_thread = threading.Thread(target=db_update_worker, name="db-update")
    db_thread.start()

    if config.get('Enable_Fix_Workers', 0) >= 1:
        workers = max(1, int(config.get('Workers', 4)))
        logger.info("Using fixed {} worker threads.", workers)
    else:
        workers = max(1, len(TARGET_URLS))
        if workers == 0:
            logger.error("No TARGET_URL configured. Cannot process.")
            update_queue.put(None)
            db_thread.join()
            _metrics_log_summary()
            return
        logger.info("Using {} worker threads based on number of TARGET_URLs.", workers)

    if not get_available_target_url():
        logger.critical("All TARGET_URLs are currently disabled by the circuit breaker. Waiting until available...")
        wait_for_available_target_url()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tx") as executor:
        futures = [executor.submit(process_single_transaction, row, update_queue) for row in rows]
        concurrent.futures.wait(futures)
        logger.info("All processing tasks for this batch have been completed.")

    update_queue.join()
    update_queue.put(None)
    db_thread.join()

    _close_global_db()
    _metrics_log_summary()
    logger.info("Database connection closed (global cleared).")


# --- Monkey patch (สลับให้ใช้ failover-ready โดยไม่แก้ฟังก์ชันเดิม) ---
get_db_connection = get_db_connection_failover_ready
update_transaction_result = update_transaction_result_failover_ready
process_transactions = process_transactions_failover_ready

# ----------------------- Main Loop -----------------------
if __name__ == "__main__":
    logger.info("Starting {} v{}", Program_Name, Program_Version)
    while True:
        try:
            process_transactions()
            logger.info("Run complete. Waiting for {} seconds before the next run.", config.get('RETRY_INTERVAL', 3600))
            time.sleep(config.get('RETRY_INTERVAL', 3600))
        except KeyboardInterrupt:
            logger.info("Processing interrupted by user. Shutting down.")
            break
        except Exception as e:
            logger.critical("An unhandled exception occurred in the main loop: {}", e, exc_info=True)
            time.sleep(60)
