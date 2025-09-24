import pyodbc
import requests
import base64
from datetime import datetime, timedelta # 1. เพิ่ม import timedelta
import time
import os
from typing import Optional
from LogLibrary import Load_Config, Loguru_Logging
import concurrent.futures
import itertools
import threading # 2. เพิ่ม import threading
import queue
import sys

# ... (ส่วน Configuration ของคุณเหมือนเดิม) ...

# ----------------------- Configuration Values -----------------------
Program_Name = "Trasation-Process"
Program_Version = "3.5"  # Updated version with Circuit Breaker
# ---------------------------------------------------------------------

if getattr(sys, 'frozen', False):
    dir_path = os.path.dirname(sys.executable)
else:
    dir_path = os.path.dirname(os.path.abspath(__file__))

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
    "log_Level": "DEBUG",
    "Log_Console": 1,
    "log_Backup": 90,
    "Log_Size": "10 MB",
}

config = Load_Config(default_config, Program_Name, dir_path)
logger = Loguru_Logging(config, Program_Name, Program_Version, dir_path)
logger.debug("Loaded configuration: {}", config)

# ----------------------- Circuit Breaker Configuration -----------------------
# 3. เพิ่มการตั้งค่าสำหรับ Circuit Breaker
TARGET_URLS = config.get('TARGET_URL', [])
URL_TIMEOUT_THRESHOLD = config.get('URL_TIMEOUT_THRESHOLD', 5)  # จำนวนครั้งที่ Timeout ก่อนจะพักการใช้งาน
URL_DISABLE_DURATION_SECONDS = config.get('URL_DISABLE_DURATION_SECONDS', 3600)  # ระยะเวลาที่พักการใช้งาน (1 ชั่วโมง)

# 4. สร้าง State ของ URL และ Lock เพื่อความปลอดภัยของ Thread
#   - failures: นับจำนวนครั้งที่ล้มเหลวติดต่อกัน
#   - disabled_until: เก็บเวลาที่จะกลับมาใช้งานได้อีกครั้ง
URL_STATUS = {url: {"failures": 0, "disabled_until": None} for url in TARGET_URLS}
URL_STATUS_LOCK = threading.Lock()
# สร้างตัวหมุนเวียนสำหรับ TARGET_URLS เพื่อกระจายการใช้งาน
target_url_cycler = itertools.cycle(TARGET_URLS) if TARGET_URLS else None
# -----------------------------------------------------------------------------

# ... (ส่วน SQL_QUERY และ get_db_connection() เหมือนเดิม) ...
Lane_Type = config.get('Lane_Type', 'ALL')
if Lane_Type == 'ALL': 
    SQL_QUERY = f"""
        SELECT TOP {config.get('QUERY_LIMIT', 100)}
            dpt.DMTPX_ID,
            dpt.DMTPX_PLAZA_ID ,
            dpt.DMTPX_LANE_ID ,
            dpt.DMTPX_NTRX_NO ,
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
if Lane_Type == 'MTC': 
    SQL_QUERY = f"""
        SELECT TOP {config.get('QUERY_LIMIT', 100)}
            dpt.DMTPX_ID,
            dpt.DMTPX_PLAZA_ID ,
            dpt.DMTPX_LANE_ID ,
            dpt.DMTPX_NTRX_NO ,
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
if Lane_Type == 'ETC': 
    SQL_QUERY = f"""
        SELECT TOP {config.get('QUERY_LIMIT', 100)}
            dpt.DMTPX_ID,
            dpt.DMTPX_PLAZA_ID ,
            dpt.DMTPX_LANE_ID ,
            dpt.DMTPX_NTRX_NO ,
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
def get_db_connection():
    """Create and return a database connection object."""
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
        conn = pyodbc.connect(conn_str)
        logger.info("Database connection established successfully!")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

# สร้างตัวหมุนเวียน URL สำหรับ Load Balance ของ Image Server
base_urls = config.get('base_urls', [])
if base_urls:
    url_cycler = itertools.cycle(base_urls)
    logger.info("Initialized round-robin cycler for base_urls.")
else:
    url_cycler = None
    logger.warning("base_urls is empty. Image downloading will fail.")

# ฟังก์ชันใหม่สำหรับจัดการสถานะ Circuit Breaker
def handle_api_failure(url: str):
    with URL_STATUS_LOCK:
        status = URL_STATUS[url]
        status["failures"] += 1 # เพิ่มตัวนับ
        logger.warning(f"API failure for {url}. Failure count: {status['failures']}.")

        # ตรวจสอบว่าถึงเกณฑ์หรือไม่
        if status["failures"] >= URL_TIMEOUT_THRESHOLD: # URL_TIMEOUT_THRESHOLD คือ 5
            # ถ้าใช่, ให้พักการใช้งาน URL นี้ 1 ชั่วโมง
            status["disabled_until"] = datetime.now() + timedelta(seconds=URL_DISABLE_DURATION_SECONDS) # URL_DISABLE_DURATION_SECONDS คือ 3600
            logger.critical(f"Circuit Breaker tripped for {url}. Disabled until {status['disabled_until']}.")

def handle_api_success(url: str):
    """รีเซ็ตสถานะเมื่อ API call สำเร็จ"""
    with URL_STATUS_LOCK:
        status = URL_STATUS[url]
        if status["failures"] > 0:
            logger.info(f"API for {url} is healthy again. Resetting failure count.")
            status["failures"] = 0
        # ถ้าเคยถูก disable แต่ตอนนี้กลับมาทำงานได้, ไม่ต้องทำอะไรเพิ่มเติม
        # เพราะการ re-enable จะเกิดขึ้นใน get_available_target_url
        status["disabled_until"] = None

def get_available_target_url() -> Optional[str]:
    """หา URL ที่พร้อมใช้งานจาก Pool โดยเคารพสถานะของ Circuit Breaker"""
    if not target_url_cycler:
        return None
        
    with URL_STATUS_LOCK:
        # วนหา URL ที่พร้อมใช้งาน
        # ใช้ for loop ที่มี range เพื่อป้องกันการวนไม่รู้จบถ้าทุก URL ถูก disable
        for _ in range(len(TARGET_URLS)):
            url = next(target_url_cycler)
            status = URL_STATUS[url]
            
            # ตรวจสอบว่า URL ถูก disable หรือไม่
            if status["disabled_until"]:
                # ถ้าถึงเวลา re-enable แล้ว ให้เปิดใช้งานอีกครั้ง
                if datetime.now() >= status["disabled_until"]:
                    logger.info(f"Re-enabling TARGET_URL: {url}")
                    status["failures"] = 0
                    status["disabled_until"] = None
                    return url
                else:
                    # ยังไม่ถึงเวลา, ข้ามไป URL ถัดไป
                    continue
            else:
                # URL ไม่ได้ถูก disable, สามารถใช้งานได้
                return url
                
    # ถ้าวนจนครบแล้วยังหา URL ที่ใช้งานไม่ได้เลย
    logger.warning("All TARGET_URLs are currently disabled by the circuit breaker.")
    return None

def update_transaction_result(conn, transaction_id, license_plate, province, transaction_datetime, plaza_id, lane_id, ntrx_no, image_source):
    """
    Update transaction result in the database using a provided connection.
    If license_plate and province are empty, set DMTPX_LICENCEPLATE = 'No LPR'.
    Otherwise, update DMTPX_LICENCEPLATE and DMTPX_PROVINCEID.
    Commit after each update.
    """
    try:
        # Each thread should use its own cursor from the shared connection
        with conn.cursor() as cursor:
            if license_plate == "" and province == "":
                logger.debug(f"Updating DB: DMTPX_ID={transaction_id}, DMTPX_PLAZA_ID={plaza_id}, DMTPX_LANE_ID={lane_id}, DMTPX_NTRX_NO={ntrx_no}, TRXDate={transaction_datetime}, DMTPX_LICENCEPLATE='No LPR', DMTPX_PROMOTIONID={image_source}")
                cursor.execute(
                    "UPDATE DMT_PASSING_TRANSACTION SET DMTPX_LICENCEPLATE = ?, DMTPX_PROMOTIONID = ? WHERE DMTPX_TRX_DATETIME = ? and DMTPX_ID = ? and DMTPX_PLAZA_ID = ? and DMTPX_LANE_ID = ? and DMTPX_NTRX_NO = ?",
                    ("No LPR", image_source, transaction_datetime, transaction_id, plaza_id, lane_id, ntrx_no)
                )
            else:
                logger.debug(f"Updating DB: DMTPX_ID={transaction_id}, DMTPX_PLAZA_ID={plaza_id}, DMTPX_LANE_ID={lane_id}, DMTPX_NTRX_NO={ntrx_no}, TRXDate={transaction_datetime}, DMTPX_LICENCEPLATE='{license_plate}', DMTPX_PROVINCEID='{province}', DMTPX_PROMOTIONID={image_source}")
                cursor.execute(
                    "UPDATE DMT_PASSING_TRANSACTION SET DMTPX_LICENCEPLATE = ?, DMTPX_PROVINCEID = ?, DMTPX_PROMOTIONID = ? WHERE DMTPX_TRX_DATETIME = ? and DMTPX_ID = ? and DMTPX_PLAZA_ID = ? and DMTPX_LANE_ID = ? and DMTPX_NTRX_NO = ?",
                    (license_plate, province, image_source, transaction_datetime, transaction_id, plaza_id, lane_id, ntrx_no)
                )
            conn.commit()
        logger.info(f"Database updated successfully (transaction_id: {transaction_id})")
    except pyodbc.Error as e:
        logger.error(f"Database update failed for transaction_id {transaction_id}: {e}")
        try:
            conn.rollback()
            logger.warning(f"Transaction rolled back for transaction_id {transaction_id}.")
        except pyodbc.Error as rb_e:
            logger.error(f"Failed to rollback transaction: {rb_e}")

def process_transactions():

    conn = get_db_connection()
    if not conn:
        logger.error("Could not establish a database connection. Aborting this run.")
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
    
    except Exception as e:
        logger.error(f"Failed to execute SQL query: {e}")
        if conn:
            conn.close()
        return

if __name__ == "__main__":
    logger.info(f"Starting {Program_Name} v{Program_Version}")
    while True:
        try:
            process_transactions()
            logger.info(f"Run complete. Waiting for {config.get('RETRY_INTERVAL', 3600)} seconds before the next run.")
            time.sleep(config.get('RETRY_INTERVAL', 3600))
        except KeyboardInterrupt:
            logger.info("Processing interrupted by user. Shutting down.")
            break
        except Exception as e:
            logger.critical(f"An unhandled exception occurred in the main loop: {e}", exc_info=True)
            time.sleep(60)