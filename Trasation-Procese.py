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
import queue
import sys

# new: สำหรับ HTTP connection pool/retry
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except Exception:
    HTTPAdapter = None
    Retry = None

# ----------------------- Configuration Values -----------------------
Program_Name = "Trasation_Process"
Program_Version = "4.2"  # Keep same version label, but internals optimized

# กำหนด path ทำงานเท่าเดิม
if getattr(sys, 'frozen', False):
    dir_path = os.path.dirname(sys.executable)
else:
    dir_path = os.path.dirname(os.path.abspath(__file__))

# ค่าตั้งต้น (เพิ่มพารามิเตอร์สำหรับ performance tuning)
default_config = {
    "DB_SERVER": "",
    "DB_PORT": 1433,
    "DB_DRIVER": "ODBC Driver 17 for SQL Server",
    "DB_DATABASE": "",
    "DB_USERNAME": "",
    "DB_PASSWORD": "",

    "QUERY_LIMIT": 100,
    "Back_date": 7,
    "Back_Time": 10,

    # base url สำหรับโหลดภาพ (หมุนเวียน)
    "base_urls": [
        "http://image-server-1.com/images",
        "http://image-server-2.com/images"
    ],

    # รอบการรัน
    "RETRY_INTERVAL": 3600,

    # โหมดจำนวน worker
    "Enable_Fix_Workers": 0,
    "Workers": 4,

    # ALPR endpoints (มี circuit breaker)
    "TARGET_URL": [
        "http://localhost:8000/api/dmt/index.php",
        "http://localhost:8001/api/dmt/index.php"
    ],
    "URL_TIMEOUT_THRESHOLD": 5,
    "URL_DISABLE_DURATION_SECONDS": 3600,

    "Lane_Type": "ALL",

    # Timeout เรียก API ALPR (read timeout); connect timeout แยกกำหนดด้านล่าง
    "API_TIMEOUT": 10,

    # ---------- NEW: HTTP/Networking Tunables ----------
    # ขนาด connection pool ต่อ thread (Session) — กำหนดมากหน่อยเพื่อรองรับ I/O พร้อม ๆ กัน
    "HTTP_POOL_MAXSIZE": 64,
    # จำนวน retry เบา ๆ สำหรับสถานะ/ข้อผิดพลาดชั่วคราว
    "HTTP_POOL_RETRIES": 1,
    "HTTP_BACKOFF_FACTOR": 0.2,
    # เวลา timeout แยกส่วน connect/read สำหรับภาพ
    "HTTP_CONNECT_TIMEOUT": 5,
    "HTTP_READ_TIMEOUT": 15,
    "IMAGE_DOWNLOAD_TIMEOUT": 12,  # หากกำหนดจะ override HTTP_READ_TIMEOUT สำหรับภาพ
    # ดาวน์โหลดรูปต่อรายการธุรกรรมแบบขนานสูงสุด (3 = รูปละ 1 เธรด)
    "IMG_DL_WORKERS": 3,

    # ---------- NEW: DB Tunables ----------
    # กำหนด operation timeout (วินาที) ระดับ connection/cursor
    "DB_TIMEOUT": 30,
    # เปิด autocommit (ลด overhead commit ต่อครั้ง; semantics ใกล้เคียงเดิม)
    "DB_AUTOCOMMIT": 1,

    # Logging
    "log_Level": "DEBUG",
    "Log_Console": 1,
    "log_Backup": 90,
    "Log_Size": "10 MB",
}

config = Load_Config(default_config, Program_Name, dir_path)
logger = Loguru_Logging(config, Program_Name, Program_Version, dir_path)
logger.debug("Loaded configuration: {}", config)

# ----------------------- Circuit Breaker Configuration -----------------------
TARGET_URLS = config.get('TARGET_URL', [])
URL_TIMEOUT_THRESHOLD = config.get('URL_TIMEOUT_THRESHOLD', 5)
URL_DISABLE_DURATION_SECONDS = config.get('URL_DISABLE_DURATION_SECONDS', 3600)

# โครงสร้างสถานะของ URL (ใช้ lock ป้องกัน race)
URL_STATUS = {url: {"failures": 0, "disabled_until": None} for url in TARGET_URLS}
URL_STATUS_LOCK = threading.Lock()

# ตัวหมุนเวียนสำหรับ TARGET_URLS
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
# อธิบาย: Session ไม่ thread-safe → สร้างเก็บใน thread-local เพื่อ reuse connection/pool ในเธรดนั้น
_SESSION_LOCAL = threading.local()

def _build_http_session() -> requests.Session:
    """สร้าง Session พร้อม HTTPAdapter/Retry เพื่อเพิ่มประสิทธิภาพและความทนทาน"""
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
    """คืนค่า Session ของเธรดปัจจุบัน (สร้างครั้งแรกแล้ว reuse)"""
    s = getattr(_SESSION_LOCAL, "session", None)
    if s is None:
        s = _build_http_session()
        _SESSION_LOCAL.session = s
        logger.debug("HTTP session constructed with pooled adapters for current thread.")
    return s

# ----------------------- DB Connection -----------------------
def get_db_connection():
    """
    สร้างและคืนค่า connection ของฐานข้อมูล
    - เปิด option autocommit (ตาม config) เพื่อลด overhead การ commit ทีละคำสั่ง
    - ตั้งค่า operation timeout
    """
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
        # ตั้งค่า timeout ระดับ connection (หน่วยวินาที)
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
    """เพิ่มตัวนับความล้มเหลว และถ้าถึง threshold จะสั่งพัก URL ชั่วคราว"""
    with URL_STATUS_LOCK:
        status = URL_STATUS[url]
        status["failures"] += 1
        logger.warning("API failure for {}. Failure count={}.", url, status["failures"])
        if status["failures"] >= URL_TIMEOUT_THRESHOLD:
            status["disabled_until"] = datetime.now() + timedelta(seconds=URL_DISABLE_DURATION_SECONDS)
            logger.critical("Circuit Breaker tripped for {}. Disabled until {}.", url, status["disabled_until"])

def handle_api_success(url: str):
    """เรียกเมื่อ API ตอบปกติ เพื่อล้างตัวนับล้มเหลวและเปิดใช้งาน URL"""
    with URL_STATUS_LOCK:
        status = URL_STATUS[url]
        if status["failures"] > 0 or status["disabled_until"] is not None:
            logger.info("API {} is healthy. Resetting failure counter and enabling.", url)
        status["failures"] = 0
        status["disabled_until"] = None

def get_available_target_url() -> Optional[str]:
    """
    เลือก URL ที่ "เปิดใช้งาน" อยู่ตามลำดับหมุนเวียน
    - ถ้า URL ใกล้ re-enable และถึงเวลาแล้ว ให้เปิดและคืนค่านั้น
    """
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
    """คืนเวลาที่ URL จะ re-enable ใกล้สุด (ถ้ามี)"""
    with URL_STATUS_LOCK:
        times = [v["disabled_until"] for v in URL_STATUS.values() if v["disabled_until"]]
    return min(times) if times else None

def wait_for_available_target_url(max_sleep_chunk: int = 30):
    """
    บล็อครอจนกว่าจะมี TARGET_URL หลุดจาก breaker
    - sleep เป็นช่วงสั้น ๆ เพื่อไม่ spam log และ responsive ต่อสัญญาณ
    """
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

# ----------------------- NEW: Helpers for download & ALPR -----------------------
def download_image(image_path: Optional[str], transaction_id: int) -> Tuple[Optional[bytes], str]:
    """
    พยายามโหลดรูปจาก base_urls (round-robin ต่อรายการ)
    Return: (image_bytes | None, reason_str)
      reason_str ∈ {'ok','no_image','timeout','not_found','no_base_url'}

    ปรับปรุง:
    - ใช้ HTTP session (pooled) ต่อเธรด → เร็วขึ้นมาก
    - ถ้า timeout ณ base หนึ่ง จะ 'ลองฐานถัดไปต่อ' แทนการเลิกทันที
    """
    if not url_cycler:
        logger.error("Image base_urls are not configured. Cannot download image.")
        return None, 'no_base_url'

    if not image_path:
        logger.error("image_path is None/empty for transaction_id={}", transaction_id)
        return None, 'no_image'

    # ตัด '/' นำหน้าเพื่อประกอบ URL ให้สะอาด
    image_path = image_path.lstrip('/')

    # จัดลำดับลอง base_urls เริ่มจากตัวที่จะถึงคิว
    start_url = next(url_cycler)
    start_index = base_urls.index(start_url)
    ordered_urls_to_try = base_urls[start_index:] + base_urls[:start_index]
    logger.debug("[download_image] tx={} URL order: {}", transaction_id, ordered_urls_to_try)

    # เตรียม timeout แยกส่วน
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

    # สรุปผลล้มเหลว
    reason = 'timeout' if saw_timeout else 'not_found'
    logger.error("[download_image] All base_urls tried, image not retrieved for tx={} (reason={})",
                 transaction_id, reason)
    return None, reason


def call_alpr(image_bytes: bytes, transaction_datetime: datetime):
    """
    ส่งรูปเดียวไปที่ TARGET_URL (เคารพ circuit breaker)
    Return: (status_tag, license_plate, province, confidence)
      status_tag ∈ {'ok','no_plate','error','unknown_error'}

    ปรับปรุง:
    - ใช้ HTTP session (pooled) และ timeout แยก connect/read
    """
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

    # แยก connect/read timeout
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

        # แปลง confidence เป็น float ถ้าเป็นไปได้
        conf = None
        if conf_raw not in (None, ""):
            try:
                conf = float(conf_raw)
            except Exception:
                conf = None

        # สถานะตอบกลับ (กันตัวพิมพ์เล็กใหญ่)
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
        # ไม่ถือว่า success/failure → ไม่ reset counter
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
    """
    ประมวลผลธุรกรรมหนึ่งรายการ (ตามสเปกเดิม):
    - โหลดรูปทั้ง 3 (ถ้าโหลดไม่ได้ทุกใบ → Update DB = "No Image")
    - ถ้าโหลดได้ ส่ง ALPR ทีละรูป
      * พบ 'ok' อย่างน้อย 1 → เลือกค่า confidence สูงสุดแล้วอัปเดต
      * ถ้าไม่มี 'ok' แต่รูปที่เรียก ALPR ทั้งหมดตอบ 'NoLicensePlate' → Update DB = "No Plate"
      * กรณีอื่น (error/ผสม error) → ไม่อัปเดต
    ปรับปรุง:
    - โหลดรูปทั้ง 3 แบบขนาน (สูงสุด 3 เธรด/แถว) ลดเวลารวม
    """
    try:
        logger.debug("[process_single_transaction] Start row compact (id, time, img1..3): {}, {}, {}, {}, {}",
                     row[0], row[4], row[9], row[10], row[11])

        transaction_id = row[0]
        plaza_id = row[1]
        lane_id = row[2]
        ntrx_no = row[3]
        transaction_datetime = row[4]
        image_paths = [row[9], row[10], row[11]]  # Image1, Image2, Image3

        # 1) โหลดรูปทั้ง 3 (แบบขนาน)
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

        # 2) ถ้าทุกรูปโหลดไม่ได้ -> Update DB: No Image
        all_failed = all(img_bytes is None for _, img_bytes, _ in download_results)
        if all_failed:
            logger.warning("[process_single_transaction] tx={} All images missing -> Update DB: 'No Image'", transaction_id)
            update_queue.put((transaction_id, "No Image", "", transaction_datetime, plaza_id, lane_id, ntrx_no, None))
            return

        # 3) ส่ง ALPR ทีละรูป (เฉพาะที่โหลดได้)
        best = {"conf": float("-inf"), "lp": None, "prov": None, "source": None}
        no_plate_count = 0
        alpr_called = 0

        # คงไว้เป็น 'ทีละรูป' ตามสเปกเดิม เพื่อไม่ถล่ม ALPR endpoint
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
            else:
                # 'error' / 'unknown_error' => ข้าม ไม่ update
                pass

        # 4) ตัดสินใจ update DB
        if best["lp"] is not None and best["lp"] != "" and best["conf"] != float("-inf"):
            logger.info("[process_single_transaction] tx={} Update with BEST (conf={}) src={}", transaction_id, best['conf'], best['source'])
            update_queue.put((
                transaction_id,
                best["lp"],
                best["prov"],
                transaction_datetime,
                plaza_id,
                lane_id,
                ntrx_no,
                best["source"]
            ))
            return

        if alpr_called > 0 and no_plate_count == alpr_called:
            logger.info("[process_single_transaction] tx={} All ALPR results = NoLicensePlate -> Update DB: 'No Plate'", transaction_id)
            update_queue.put((
                transaction_id,
                "No Plate",
                "",
                transaction_datetime,
                plaza_id,
                lane_id,
                ntrx_no,
                None
            ))
            return

        logger.warning("[process_single_transaction] tx={} No definitive result (errors/mixed). Skip DB update.", transaction_id)
        return

    except Exception as e:
        logger.error("Exception in process_single_transaction: {}", e, exc_info=True)

# ----------------------- Process batch -----------------------
def process_transactions():
    """
    ประมวลผลด้วย multi-threading สำหรับ ALPR calls
    และ single-threaded queue สำหรับ DB updates (ตามสถาปัตยกรรมเดิม)
    ปรับปรุง:
    - ลบ execute ซ้ำซ้อน
    - บังคับจำนวน worker ขั้นต่ำ 1
    - ปรับ logging ให้ชัดเจน
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Could not establish a database connection. Aborting this run.")
        return

    try:
        # ตั้งค่า timeout ระดับ cursor (ถ้ารองรับ)
        cursor = conn.cursor()
        try:
            cursor.timeout = int(config.get("DB_TIMEOUT", 30))
        except Exception:
            pass

        # ดึงข้อมูลรอบเดียว (ตัด execute ซ้ำ)
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

        # Worker สำหรับอัปเดต DB
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
                    # (transaction_id, license_plate, province, transaction_datetime, plaza_id, lane_id, ntrx_no, image_source)
                    update_transaction_result(conn, *item)
                except Exception as e:
                    logger.error("DB update worker error: {}", e)
                finally:
                    update_queue.task_done()

        db_thread = threading.Thread(target=db_update_worker, name="db-update")
        db_thread.start()

        # จำนวนเธรดประมวลผล
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

        # ถ้า URL ทั้งหมดยังอยู่ใน breaker ให้รอ
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

# ----------------------- DB Update -----------------------
def update_transaction_result(conn, transaction_id, license_plate, province, transaction_datetime, plaza_id, lane_id, ntrx_no, image_source):
    """
    Update transaction result ใน DB (ตรรกะเหมือนเดิม):
      - ถ้า license_plate/province เป็นค่าว่างทั้งคู่ → อัปเดต DMTPX_LICENCEPLATE='No LPR'
      - กรณีปกติ → อัปเดต DMTPX_LICENCEPLATE, DMTPX_PROVINCEID และ DMTPX_PROMOTIONID (เก็บ IMAGE_FILE_0X)
    ปรับปรุง:
      - ถ้าเปิด autocommit จะไม่เรียก commit() เพื่อลด overhead
    """
    try:
        with conn.cursor() as cursor:
            try:
                cursor.timeout = int(config.get("DB_TIMEOUT", 30))
            except Exception:
                pass

            if license_plate == "" and province == "":
                logger.debug("DB UPDATE (No LPR): id={} src={} ntrx={}", transaction_id, image_source, ntrx_no)
                cursor.execute(
                    "UPDATE DMT_PASSING_TRANSACTION SET DMTPX_LICENCEPLATE = ?, DMTPX_PROMOTIONID = ? WHERE DMTPX_TRX_DATETIME = ? and DMTPX_ID = ? and DMTPX_PLAZA_ID = ? and DMTPX_LANE_ID = ? and DMTPX_NTRX_NO = ?",
                    ("No LPR", image_source, transaction_datetime, transaction_id, plaza_id, lane_id, ntrx_no)
                )
            else:
                logger.debug("DB UPDATE (LPR): id={} lp='{}' prov='{}' src={} ntrx={}",
                             transaction_id, license_plate, province, image_source, ntrx_no)
                cursor.execute(
                    "UPDATE DMT_PASSING_TRANSACTION SET DMTPX_LICENCEPLATE = ?, DMTPX_PROVINCEID = ?, DMTPX_PROMOTIONID = ? WHERE DMTPX_TRX_DATETIME = ? and DMTPX_ID = ? and DMTPX_PLAZA_ID = ? and DMTPX_LANE_ID = ? and DMTPX_NTRX_NO = ?",
                    (license_plate, province, image_source, transaction_datetime, transaction_id, plaza_id, lane_id, ntrx_no)
                )
            # commit เฉพาะเมื่อ autocommit ปิด
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
