# DMT LPR Transaction Process — เอกสารโครงสร้างโปรแกรม

> **โปรแกรม:** `Trasation_Process` เวอร์ชัน **6.0.2**
> **ภาษา:** Python (ใช้ pyodbc, requests, loguru)
> **หน้าที่:** ดึงรายการธุรกรรมผ่านด่าน (DMT Passing Transaction) ที่ยังไม่มีเลขทะเบียนรถจาก SQL Server → ดาวน์โหลดภาพรถจาก Image Server → ส่งภาพไปอ่านป้ายทะเบียนด้วย ALPR API → อัปเดตผลกลับลงฐานข้อมูล ทำงานวนซ้ำเป็นรอบ (loop) ตลอดเวลา

---

## 1. ไฟล์ในโปรเจกต์

| ไฟล์ | ขนาด | หน้าที่ |
|---|---|---|
| `Trasation_Procese.py` | ~920 บรรทัด | โปรแกรมหลัก — ดึงข้อมูล, ดาวน์โหลดภาพ, เรียก ALPR, อัปเดต DB, จัดการ failover |
| `LogLibrary.py` | ~120 บรรทัด | ไลบรารีกลาง — โหลด/สร้าง config JSON และตั้งค่า Loguru logging |
| `Trasation_Procese.py.bak` | — | สำเนาไฟล์ก่อน refactor รอบ 2026-06-04 (เผื่อย้อนกลับ) |

ไฟล์ที่ถูกสร้างอัตโนมัติตอนรัน (อยู่ข้างไฟล์โปรแกรม หรือข้าง .exe ถ้า build ด้วย PyInstaller):
- `Trasation_Process_config.json` — ไฟล์ config (สร้างจากค่า default ถ้ายังไม่มี)
- `logs/Trasation_Process_5.0.log` — log file (rotate ตามขนาด, เก็บย้อนหลังตามวัน, บีบอัด zip)

---

## 2. ภาพรวมการไหลของข้อมูล (Data Flow)

```
┌─────────────────────────────────────────────────────────────────┐
│ Main Loop (รันวนทุก RETRY_INTERVAL วินาที, default 3600)        │
└────────────┬────────────────────────────────────────────────────┘
             ▼
  ① process_transactions()  ← (ถูก monkey-patch เป็นเวอร์ชัน failover-ready)
             │
             ▼
  ② SELECT จาก SQL Server (ตาราง DMT_PASSING_TRANSACTION)
     - เลือกรายการที่ DMTPX_LICENCEPLATE ว่าง/NULL
     - กรองตาม Lane_Type (ALL / MTC / ETC), ช่วงเวลา Back_date/Back_Time
     - จำกัดจำนวน QUERY_LIMIT รายการต่อรอบ
             │
             ▼
  ③ ThreadPoolExecutor (workers = จำนวน TARGET_URL หรือค่า Workers)
     แต่ละ transaction → process_single_transaction()
             │
             ├─ ④ ดาวน์โหลดภาพ 3 รูป (Image1/2/3) แบบขนาน
             │    - Round-robin สลับ base_urls + ลองทุก server ถ้าพลาด
             │
             ├─ ⑤ เรียก ALPR API ทีละภาพ (POST base64 JSON)
             │    - Round-robin TARGET_URL + Circuit Breaker
             │    - เก็บ LP จากภาพ lp_conf สูงสุด, Province จากภาพ prov_conf สูงสุด (แยกกัน)
             │
             └─ ⑥ ใส่ผลลง update_queue
             ▼
  ⑦ db_update_worker (thread เดียว) ดึงจาก queue
     → UPDATE DMTPX_LICENCEPLATE / DMTPX_PROVINCEID / DMTPX_PROMOTIONID
```

---

## 3. การตัดสินผลลัพธ์ต่อ 1 ธุรกรรม

| สถานการณ์ | ค่าที่อัปเดตลง DB |
|---|---|
| อ่านป้ายได้อย่างน้อย 1 ภาพ | `DMTPX_LICENCEPLATE` = LP ของภาพ **`lp_conf` สูงสุด**, `DMTPX_PROVINCEID` = `ProvinceID` ของภาพ **`prov_conf` สูงสุด** (แยกภาพได้), `DMTPX_PROMOTIONID` = `PlateImageUrl` ของภาพ LP |
| ดาวน์โหลดภาพไม่ได้เลยทั้ง 3 รูป | `DMTPX_LICENCEPLATE = "No Image"` |
| ALPR ตอบ NoLicensePlate ทุกภาพ | `DMTPX_LICENCEPLATE = "No Plate"` |
| ผลก้ำกึ่ง / error ปน | **ข้าม** ไม่อัปเดต (จะถูกหยิบมาทำใหม่รอบหน้า) |
| กรณีพิเศษ (lp="" prov="") | `DMTPX_LICENCEPLATE = "No LPR"` |

**เงื่อนไข WHERE ตอน UPDATE** ใช้ครบ 5 คีย์: `DMTPX_TRX_DATETIME, DMTPX_ID, DMTPX_PLAZA_ID, DMTPX_LANE_ID, DMTPX_NTRX_NO`

---

## 4. SQL Query ตาม Lane_Type

Query ทั้ง 3 แบบเหมือนกัน ต่างกันที่ `DMTPX_TC_PAYMENTMETHOD_ID`:

| Lane_Type | Payment Method IDs |
|---|---|
| `ALL` (default) | 1,2,3,4,5,6,9,17,18,19,20 |
| `MTC` (เงินสด/บัตร) | 1,2,3,4,17,18,19,20 |
| `ETC` (อิเล็กทรอนิกส์) | 5,6,9 |

เงื่อนไขร่วม:
- `DMTPX_SIGNAL_CODE IN (1,2,10,19,82,20,22,23,24,26)`
- ช่วงเวลา: ย้อนหลัง `Back_date` วัน (เริ่มเที่ยงคืน) จนถึง `Back_Time` นาทีก่อนปัจจุบัน
- **Hard-coded floor:** `DMTPX_TRX_DATETIME >= '2025-07-25'` (จะไม่ประมวลผลข้อมูลก่อนวันนี้)
- เรียงตาม `DMTPX_TRX_DATETIME` เก่าสุดก่อน

---

## 5. กลไกความทนทาน (Resiliency)

### 5.1 Circuit Breaker สำหรับ ALPR API (`TARGET_URL`)
- นับ failure ต่อ URL — ถ้าครบ `URL_TIMEOUT_THRESHOLD` (default 5) ครั้ง → **ปิด URL นั้น** เป็นเวลา `URL_DISABLE_DURATION_SECONDS` (default 1 ชม.)
- เรียกสำเร็จ 1 ครั้ง → reset ตัวนับ
- ถ้าปิดหมดทุก URL → รอจนถึงเวลา re-enable ที่ใกล้สุด (sleep เป็นช่วง ๆ ละไม่เกิน 30 วิ)
- ใช้ round-robin (`itertools.cycle`) กระจายโหลดระหว่าง URL ที่ยังเปิดอยู่

### 5.2 Image Server Failover (`base_urls`)
- Round-robin เลือก server เริ่มต้น แล้วถ้าโหลดไม่ได้จะ**ไล่ลองทุก server** ตามลำดับ
- แยกเหตุผลความล้มเหลว: `timeout` / `not_found` / `no_image` / `no_base_url`

### 5.3 DB Failover (SQL Server AlwaysOn / Primary-Secondary)
ใช้ฟังก์ชันชุด `*_failover_ready` โดยตรง (`get_db_connection_failover_ready`, `update_transaction_result_failover_ready`, `process_transactions_failover_ready`) — เดิมเป็น monkey patch ทับฟังก์ชันเก่า แต่ refactor แล้ว (ลบ dead code และเรียกตรง):

- **หา PRIMARY เท่านั้น:** ไล่ต่อทุก endpoint ใน `DB_SERVER_LIST` (หรือ `DB_SERVER` เดี่ยว) เช็ก role ด้วย `sys.fn_hadr_is_primary_replica()` — ถ้าเจอ SECONDARY/UNKNOWN จะปิดทิ้งและลองตัวถัดไป
- ถ้าเจอแต่ SECONDARY → โยน `_PrimaryNotAvailable` → พัก `DB_SECONDARY_SLEEP_SEC` (15 วิ) แล้วลองใหม่ สูงสุด `DB_CONNECT_MAX_ATTEMPTS` (6) ครั้ง
- **Global connection เดียว** (`_DB_CONN`) ป้องกันด้วย lock + health check ด้วย `SELECT 1` ก่อนใช้ซ้ำ
- **Rate-limit การ reconnect:** ห่างกันอย่างน้อย `DB_RECONNECT_MIN_INTERVAL_SEC` (5 วิ) + random jitter
- **Retry แบบ exponential backoff** (base 0.5s, สูงสุด 10s ต่อครั้ง):
  - SELECT: สูงสุด `DB_SELECT_MAX_ATTEMPTS` (5) ครั้ง
  - UPDATE: สูงสุด `DB_UPDATE_MAX_ATTEMPTS` (6) ครั้ง — เช็กก่อนทุกครั้งว่าเป็น PRIMARY ไม่งั้นถือเป็น error 3906 (read-only)
- **จำแนก transient error** จาก SQLSTATE (`08S01, HYT00, HYT01, IMC06, 40001`) และข้อความ เช่น "communication link failure", "tcp provider" ฯลฯ
- **Connection string พิเศษ:** `tcp:<server>,<port>`, `MultiSubnetFailover` (ปิดอัตโนมัติเมื่อต่อด้วย IP ตรง ๆ เว้นแต่เปิด `DB_MULTISUBNET_FAILOVER_ON_IP`), `ConnectRetryCount/Interval`, `LoginTimeout`, `APP=Trasation_Process/5.0`
- **Metrics:** นับ connect attempts/success, reconnects, select/update retries, readonly hits, primary/secondary connects — log สรุปท้ายทุกรอบ (`[DB METRICS]`)

### 5.4 HTTP Connection Pool
- `requests.Session` แยกต่อ thread (`threading.local`) + `HTTPAdapter` pool (`HTTP_POOL_MAXSIZE` 64)
- urllib3 `Retry` อัตโนมัติสำหรับ status 502/503/504 (`HTTP_POOL_RETRIES` 1, backoff 0.2)
- Timeout แยก connect (`HTTP_CONNECT_TIMEOUT` 5s) / read (`HTTP_READ_TIMEOUT` 15s, ภาพ `IMAGE_DOWNLOAD_TIMEOUT` 12s, ALPR `API_TIMEOUT` 10s)

---

## 6. โครงสร้าง Threading

```
Main thread
 ├─ db-update (1 thread) ── ดึงจาก update_queue → UPDATE DB ทีละรายการ
 └─ tx-N (N threads) ────── ประมวลผลธุรกรรม (N = len(TARGET_URL) หรือ Workers ถ้าเปิด Enable_Fix_Workers)
      └─ imgdl-M (3 threads ต่อ transaction) ── ดาวน์โหลดภาพ 3 รูปขนานกัน
```

- ผล ALPR ถูกส่งผ่าน `queue.Queue` ไปให้ db-update thread เดียว → การเขียน DB เป็น serial (ปลอดภัยต่อ connection เดียว)
- จบ batch: รอ `update_queue.join()` → ส่ง `None` (sentinel) ปิด worker → ปิด/เคลียร์ global connection

---

## 7. ALPR API Contract (v6.0 — `/api/v1/alpr`)

**Request** — `POST <TARGET_URL>` (default `http://localhost:8000/api/v1/alpr`) JSON:
```json
{
  "Trx_Datetime": "2026-06-11 23:34:50.324",
  "alpr_image": "<base64 ล้วน ไม่มี data URI prefix>"
}
```

**Response:**
```json
{
  "status": "ok",
  "LicensePlate": "7กบ3572",
  "LicensePlateConfidence": 97.6,
  "Province": "กรุงเทพมหานคร",
  "ProvinceCode": "BKK",
  "ProvinceID": 10,
  "ProvinceConfidence": 97.9,
  "PlateImageUrl": "http://.../plates/...jpg",
  "msg": "ok",
  "ealpr_recognition": [
    { "results": [ { "plate": "7กบ3572", "confidence": 97.6, "...": "..." } ] }
  ]
}
```

**การตีความ response:**
| เงื่อนไข | ผล |
|---|---|
| `status=ok` + มี `LicensePlate` | ok → ใช้ `LicensePlate` + **`ProvinceID` (ตัวเลข)** เขียนลง `DMTPX_PROVINCEID` |
| `status=ok` + `LicensePlate` ว่าง | no_plate |
| `status` = NoLicensePlate / NoPlate (normalize แล้ว) | no_plate |
| `status` ขึ้นต้นด้วย `error` | error → นับเป็น failure ของ circuit breaker |
| `status` อื่น ๆ | unknown_error (ไม่นับ breaker) |

**confidence (แยก 2 ค่า):** ฟังก์ชัน `_extract_alpr_confidences()` คืน `(lp_conf, prov_conf)` แยกกัน
- `lp_conf` = `LicensePlateConfidence` (บนสุด) → fallback `ealpr_recognition[].results[].confidence` (สูงสุด)
- `prov_conf` = `ProvinceConfidence` (บนสุด) → fallback `ealpr_recognition[].results[].region_confidence` (สูงสุด)

**การเลือกข้ามภาพ (v6.0.2):** ยิง ALPR ครบทุกภาพแล้วเลือก**แยก field** — `LicensePlate` เอาจากภาพที่ `lp_conf` สูงสุด, `ProvinceID` เอาจากภาพที่ `prov_conf` สูงสุด (มาจากคนละภาพกันได้) ส่วน `DMTPX_PROMOTIONID` (`PlateImageUrl`) ผูกกับภาพของ `LicensePlate`

**หมายเหตุภาพ:** คอลัมน์ DB ถูก map สลับลำดับ — `IMAGE_FILE_02` → Image1 (ลองก่อน), `IMAGE_FILE_01` → Image2, `IMAGE_FILE_03` → Image3

---

## 8. Configuration ทั้งหมด (`Trasation_Process_config.json`)

### Database
| Key | Default | ความหมาย |
|---|---|---|
| `DB_SERVER` | `""` | DB server เดี่ยว (ใช้เมื่อ DB_SERVER_LIST ว่าง) |
| `DB_SERVER_LIST` | `[]` | รายการ IP/host หลายตัวสำหรับ failover (มาก่อน DB_SERVER) |
| `DB_PORT` | `1433` | พอร์ต SQL Server |
| `DB_DRIVER` | `ODBC Driver 17 for SQL Server` | ODBC driver |
| `DB_DATABASE` / `DB_USERNAME` / `DB_PASSWORD` | `""` | ข้อมูลเชื่อมต่อ |
| `DB_TIMEOUT` | `30` | Query timeout (วินาที) |
| `DB_AUTOCOMMIT` | `1` | autocommit (1=เปิด) |

### DB Failover
| Key | Default | ความหมาย |
|---|---|---|
| `DB_MULTISUBNET_FAILOVER` | `1` | เปิด MultiSubnetFailover ใน conn string |
| `DB_MULTISUBNET_FAILOVER_ON_IP` | `0` | ถ้าต่อด้วย IP ตรง ๆ ให้ปิด MSF (สำคัญ) |
| `DB_CONNECT_RETRY_COUNT` / `DB_CONNECT_RETRY_INTERVAL` | `3` / `3` | retry ระดับ driver |
| `DB_LOGIN_TIMEOUT` | `15` | login timeout |
| `DB_SECONDARY_SLEEP_SEC` | `15` | พักเมื่อเจอแต่ secondary |
| `DB_CONNECT_MAX_ATTEMPTS` | `6` | จำนวนรอบหา primary |
| `DB_SELECT_MAX_ATTEMPTS` / `DB_UPDATE_MAX_ATTEMPTS` | `5` / `6` | retry ระดับ query |
| `DB_RECONNECT_MIN_INTERVAL_SEC` | `5` | ระยะห่างขั้นต่ำระหว่าง reconnect |
| `DB_RETRY_JITTER_MAX_SEC` | `0.5` | jitter สุ่มเพิ่มตอน retry |

### Query / Batch
| Key | Default | ความหมาย |
|---|---|---|
| `QUERY_LIMIT` | `100` | จำนวนรายการต่อรอบ (SELECT TOP) |
| `Back_date` | `7` | ดูข้อมูลย้อนหลังกี่วัน |
| `Back_Time` | `10` | เว้นข้อมูลล่าสุดกี่นาที (กันข้อมูลยังเขียนไม่เสร็จ) |
| `Lane_Type` | `ALL` | `ALL` / `MTC` / `ETC` |
| `RETRY_INTERVAL` | `3600` | พักกี่วินาทีระหว่างรอบ |

### Workers
| Key | Default | ความหมาย |
|---|---|---|
| `Enable_Fix_Workers` | `0` | 1 = ใช้จำนวน `Workers` คงที่, 0 = ใช้ตามจำนวน TARGET_URL |
| `Workers` | `4` | จำนวน thread ประมวลผล (เมื่อเปิด fix) |
| `IMG_DL_WORKERS` | `3` | thread ดาวน์โหลดภาพต่อ 1 transaction |

### API / Network
| Key | Default | ความหมาย |
|---|---|---|
| `base_urls` | 2 ตัวอย่าง | Image server URLs (round-robin + failover) |
| `TARGET_URL` | `["http://localhost:8000/api/v1/alpr"]` | ALPR API endpoints (round-robin + circuit breaker) |
| `URL_TIMEOUT_THRESHOLD` | `5` | failure กี่ครั้งถึงตัด URL ออก |
| `URL_DISABLE_DURATION_SECONDS` | `3600` | ตัดออกนานกี่วินาที |
| `API_TIMEOUT` | `10` | read timeout เรียก ALPR |
| `HTTP_CONNECT_TIMEOUT` / `HTTP_READ_TIMEOUT` | `5` / `15` | timeout ทั่วไป |
| `IMAGE_DOWNLOAD_TIMEOUT` | `12` | read timeout โหลดภาพ |
| `HTTP_POOL_MAXSIZE` / `HTTP_POOL_RETRIES` / `HTTP_BACKOFF_FACTOR` | `64` / `1` / `0.2` | connection pool |

### Logging
| Key | Default | ความหมาย |
|---|---|---|
| `log_Level` | `DEBUG` | ระดับ log |
| `Log_Console` | `1` | 1 = แสดง log บนหน้าจอด้วย |
| `log_Backup` | `90` | เก็บ log ย้อนหลังกี่วัน |
| `Log_Size` | `10 MB` | ขนาดไฟล์ก่อน rotate |

---

## 9. LogLibrary.py

- **`Load_Config(default_config, Program_Name)`** — ถ้าไม่มีไฟล์ `<Program_Name>_config.json` จะสร้างจาก default แล้วอ่านคืนเป็น dict (ผู้ดูแลระบบแก้ไฟล์ JSON ได้เลย)
- **`Loguru_Logging(config, Program_Name, Program_Version)`** — ตั้งค่า Loguru 2 sink: console (เปิด/ปิดได้) และไฟล์ `logs/<Name>_<Version>.log` (rotate ตาม `Log_Size`, retention ตาม `log_Backup` วัน, บีบอัด zip)
- รองรับ PyInstaller: ถ้า `sys.frozen` จะใช้โฟลเดอร์ของ .exe เป็นที่เก็บ config/logs

---

## 10. Dependencies

```
pyodbc      # SQL Server ผ่าน ODBC (ต้องติดตั้ง ODBC Driver 17/18 for SQL Server)
requests    # HTTP client
loguru      # logging
urllib3     # (มากับ requests) ใช้ Retry
```

## 11. ข้อสังเกต / จุดที่ควรรู้

1. **วันที่ floor hard-coded** `2025-07-25` ฝังอยู่ใน SQL — **คงไว้ตามนโยบาย** (ยืนยัน 2026-06-04 ว่าไม่แก้)
2. **`DMTPX_PROMOTIONID` ถูกใช้เก็บ `PlateImageUrl`** (URL ภาพป้ายที่ crop จาก ALPR) ไม่ใช่ promotion จริง — เปลี่ยนจาก v5.0 ที่เก็บแหล่งภาพ (`IMAGE_FILE_02`)
3. **ลำดับภาพ** `IMAGE_FILE_02` → Image1 (ลองก่อน) — **คงไว้ตามนโยบาย**
4. ค่า `confidence` ที่ ALPR ไม่ส่งมา จะถูกตีเป็น `-1.0` (ยังถือว่า "อ่านได้" แต่แพ้ทุกภาพที่มี confidence จริง)
5. ชื่อไฟล์/โปรแกรมสะกด "Trasation" (ตกตัว n) — เป็นชื่อที่ใช้จริงทั้งระบบ รวมถึงชื่อไฟล์ config และ log จึง**ไม่ควรเปลี่ยน**โดยไม่วางแผน
6. `Lane_Type` ที่ไม่ใช่ `ALL`/`MTC`/`ETC` จะถูกตีเป็น `ETC` (พฤติกรรมเดิม) แต่ตอนนี้มี log เตือนแล้ว

## 12. ประวัติการ Refactor (2026-06-04)

แก้ไขโดยคงพฤติกรรมเดิมทั้งหมด (มี `Trasation_Procese.py.bak` สำรองไว้):

1. **แก้บั๊ก `_get_or_reconnect_db`** — เดิมเมื่อ health check (`SELECT 1`) พบ connection พัง จะปิด connection แล้ว **return `None`** ทำให้ update รายการนั้นล้มเหลวแทนที่จะ reconnect → restructure ให้ reconnect จริง
2. **ลบ dead code ~200 บรรทัด** — ฟังก์ชัน `get_db_connection` / `process_transactions` / `update_transaction_result` เวอร์ชันเดิมที่ถูก monkey patch ทับ และลบ monkey patch แล้วเรียก `*_failover_ready` ตรง ๆ
3. **รวม SQL 3 ชุดเป็นชุดเดียว** — ใช้ dict `_PAYMENT_METHODS_BY_LANE` แยกเฉพาะ payment method ตาม Lane_Type
4. **บังคับ `int()`** กับ `QUERY_LIMIT`, `Back_date`, `Back_Time` ก่อนฝังใน SQL
5. **ลบของไม่ได้ใช้** — `dir_path`, `import os/sys`, ตัวแปร `base` ค้าง, `force = True` ที่ไม่มีผล
6. **default driver ใน `_build_conn_str_for`** แก้จาก 18 → 17 ให้ตรง `default_config`
7. **เพิ่ม log เตือน** เมื่อ `Lane_Type` ไม่รู้จัก

## 13. v6.0 — เปลี่ยน ALPR API (2026-06-04)

1. **Endpoint ใหม่:** `http://localhost:8000/api/v1/alpr` (default `TARGET_URL` เหลือตัวเดียว)
2. **Payload ใหม่:** ส่งแค่ `{"Trx_Datetime": "<DMTPX_TRX_DATETIME>", "alpr_image": "<base64>"}` — ตัด `hw_id`, `user_id`, `date_time` ฯลฯ ออกทั้งหมด (`call_alpr(image_bytes, transaction_datetime)`; `Trx_Datetime` รูปแบบ `YYYY-MM-DD HH:MM:SS.mmm`)
3. **เขียน `ProvinceID` (ตัวเลข เช่น 10)** ลง `DMTPX_PROVINCEID` แทนชื่อจังหวัดภาษาไทย ⚠️ **รูปแบบข้อมูลในคอลัมน์เปลี่ยน** — ระบบปลายน้ำที่อ่านคอลัมน์นี้ต้องรองรับ
4. **confidence** ย้ายไปอยู่ใน `ealpr_recognition[].results[]` — เพิ่ม `_extract_alpr_confidence()` ดึงค่าสูงสุด
5. **no_plate รองรับ 2 แบบ:** `status=ok` + ป้ายว่าง หรือ status เป็น NoLicensePlate/NoPlate
6. **เขียน `PlateImageUrl`** ลง `DMTPX_PROMOTIONID` แทนแหล่งภาพ (`IMAGE_FILE_0x`) ⚠️ ตรวจสอบความยาวคอลัมน์ให้พอเก็บ URL (~60+ ตัวอักษร)
7. ⚠️ **ไฟล์ config เดิมไม่ถูกอัปเดตอัตโนมัติ** — เครื่องที่ deploy แล้วต้องแก้ `TARGET_URL` ใน `Trasation_Process_config.json` เอง

## 14. เพิ่มฟิลด์ `Trx_Datetime` ใน payload (2026-06-11)

- **Payload ส่ง 2 ฟิลด์:** `{"Trx_Datetime": "<DMTPX_TRX_DATETIME>", "alpr_image": "<base64>"}`
- เพิ่มฟังก์ชัน `_format_trx_datetime()` แปลง `DMTPX_TRX_DATETIME` (row[4]) เป็นสตริงรูปแบบ `YYYY-MM-DD HH:MM:SS.mmm` (มิลลิวินาที 3 หลัก) — รองรับทั้ง datetime object และสตริง
- `call_alpr` รับพารามิเตอร์เพิ่ม: `call_alpr(image_bytes, transaction_datetime=None)` และ `process_single_transaction` ส่ง `transaction_datetime` เข้าไปด้วย

## 15. v6.0.1 — เปลี่ยนวิธีคำนวณ confidence เป็นค่าเฉลี่ย (2026-06-20)

- API เวอร์ชันใหม่ส่ง `LicensePlateConfidence` และ `ProvinceConfidence` มาที่ระดับบนสุดของ response
- `_extract_alpr_confidence()` เปลี่ยนมาใช้**ค่าเฉลี่ยของ `LicensePlateConfidence` กับ `ProvinceConfidence`** เป็นหลัก (เช่น `(97.6 + 97.9) / 2 = 97.75`) — ถ้ามีแค่ตัวใดตัวหนึ่งใช้ตัวนั้น
- ยังคง fallback เดิมไว้: fallback 1 = `confidence` ระดับบนสุด, fallback 2 = `ealpr_recognition[].results[].confidence` (ค่าสูงสุด)
- ไม่กระทบ logic อื่น — ค่า confidence ยังใช้เลือกภาพที่ดีที่สุด (best-confidence wins) เหมือนเดิม
- ⚠️ **ถูกแทนที่ด้วย v6.0.2** ซึ่งเลิกใช้ค่าเฉลี่ยและแยก `lp_conf`/`prov_conf` (ดูหัวข้อ 16)

## 16. v6.0.2 — เลือก LP / Province แยกภาพตาม confidence ของแต่ละ field (2026-06-21)

- เปลี่ยนจากการเลือก "ภาพที่ดีที่สุดภาพเดียว" มาเป็นเลือก**แยก field**: ในเมื่อยิง ALPR ครบทุกภาพอยู่แล้ว ก็เอา `LicensePlate` จากภาพที่ `LicensePlateConfidence` สูงสุด และ `ProvinceID` จากภาพที่ `ProvinceConfidence` สูงสุด (มาจากคนละภาพกันได้)
- แทน `_extract_alpr_confidence()` (ค่าเฉลี่ย) ด้วย `_extract_alpr_confidences()` ที่คืน `(lp_conf, prov_conf)` แยกกัน — `prov_conf` fallback ใช้ `region_confidence` ใน results
- `call_alpr` คืน 6-tuple: `(status_tag, lp, province_id, lp_conf, prov_conf, plate_image_url)` (เดิม 5-tuple)
- `process_single_transaction` เก็บ `best_lp` (ตาม `lp_conf`) และ `best_prov` (ตาม `prov_conf`) แยกกัน; `DMTPX_PROMOTIONID` (`PlateImageUrl`) ผูกกับภาพของ LP
- เงื่อนไขอัปเดต DB: อัปเดตเมื่อมี LP (ภาพ `lp_conf` สูงสุด); ถ้าไม่มีภาพไหนเจอจังหวัดเลย → `DMTPX_PROVINCEID = ""`. ส่วน "No Image" / "No Plate" / skip เหมือนเดิม
