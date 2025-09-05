#!/usr/bin/env python3
import os
import re
import shutil
import subprocess
import pywintypes  # type: ignore
import win32file   # type: ignore
import win32con    # type: ignore
import gspread
import datetime
from datetime import datetime
from gspread.cell import Cell
from oauth2client.service_account import ServiceAccountCredentials
from mutagen.mp4 import MP4, MP4Tags

# ----------------------------- 設定 -----------------------------
ut_dir_path      = r"C:\Users\chen8\OneDrive\文件\ControllerDriver\Cooked\uT"
qb_dir_path      = r"C:\Users\chen8\OneDrive\文件\ControllerDriver\qbCooking"
sheet_url        = "https://docs.google.com/spreadsheets/d/1cizSVrySFHKYfngBhkCCNVRXRJiMYH_2ltts9YAdbEo/edit?usp=sharing"
mkvpropedit_path = r"C:\Program Files\MKVToolNix\mkvpropedit.exe"

# ----------------------------- credentials.json 路徑 -----------------------------
CREDENTIAL_PATH = os.path.join(os.path.dirname(__file__), 'credentials.json')

# ----------------------------- Google Sheet 連線 -----------------------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]
creds  = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, scope)
client = gspread.authorize(creds)
sheet  = client.open_by_url(sheet_url).worksheet("Status")

# 讀取 Status 表格的識別碼、演員、發行日期、狀態
all_data = sheet.get_all_values()
identifiers = []
identifier_status_map = {}
identifier_actor_map  = {}
identifier_date_map   = {}

for idx, row in enumerate(all_data, start=1):
    if not row or not row[0].strip():
        continue
    ident = row[0].strip()
    identifiers.append(ident)
    status  = row[14].strip() if len(row) >= 15 else ""
    actor   = row[6].strip()  if len(row) >= 7  else ""
    pubdate = row[1].strip()  if len(row) >= 2  else ""
    identifier_status_map[ident] = {"row": idx, "status": status}
    identifier_actor_map[ident]  = actor
    identifier_date_map[ident]   = pubdate

# ----------------------------- 提取辨識碼 -----------------------------
def extract_identifier_from_filename(filename, identifiers):
    main = re.split(r'[\.\[\]@]', filename)[0]
    cleaned = main.replace('_', '-').lower()
    for ident in identifiers:
        if cleaned == ident.lower():
            return ident
    norm = filename.lower().replace('-', '').replace('_', '')
    for ident in identifiers:
        if ident.lower() in filename.lower() or ident.lower().replace('-','') in norm:
            return ident
    return None

# ----------------------------- 寫入演員 metadata -----------------------------
def write_actor_metadata(video_path, actor_name):
    if not actor_name:
        return
    ext = os.path.splitext(video_path)[1].lower()
    if ext in ('.mp4', '.mov'):
        try:
            mp4 = MP4(video_path)
            mp4.tags = mp4.tags or MP4Tags()
            mp4.tags["\xa9cmt"] = [f"參與演出者={actor_name}"]
            mp4.tags["\xa9ART"] = [actor_name]
            mp4.save()
            print(f"[Step2] MP4 演員寫入：{actor_name} → {os.path.basename(video_path)}")
        except Exception as e:
            print(f"[Step2] MP4 寫入失敗：{video_path}：{e}")
    elif ext == '.mkv':
        xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Tags>
  <Tag>
    <Targets><TargetTypeValue>50</TargetTypeValue></Targets>
    <Simple><Name>Comment</Name><String>參與演出者={actor_name}</String></Simple>
    <Simple><Name>Artist</Name><String>{actor_name}</String></Simple>
  </Tag>
</Tags>'''
        xml_path = video_path + ".tags.xml"
        with open(xml_path, 'w', encoding='utf-8') as f:
            f.write(xml)
        cmd = [mkvpropedit_path, video_path, "--tags", f"all:{xml_path}"]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(f"[Step2] MKV 演員寫入：{actor_name} → {os.path.basename(video_path)}")
        except Exception as e:
            print(f"[Step2] MKV 寫入失敗：{video_path}：{e}")
        finally:
            if os.path.exists(xml_path):
                os.remove(xml_path)

# ----------------------------- 同時設定三個時間戳 & 前後 log -----------------------------
def set_all_file_times(path, date_str):
    try:
        dt_obj = datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        print(f"[Step2] 發行日期解析失敗：{date_str}")
        return
    wintime = pywintypes.Time(dt_obj)

    # 讀取並列印前次時間
    before_ctime = os.path.getctime(path)
    before_mtime = os.path.getmtime(path)
    print(f"[Step2] 變更前 ctime={before_ctime}, mtime={before_mtime}")

    # 打開 handle
    hfile = win32file.CreateFile(
        path,
        win32con.GENERIC_WRITE,
        win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
        None,
        win32con.OPEN_EXISTING,
        0, None
    )
    # 同時設定 Created / Modified / Accessed
    win32file.SetFileTime(hfile, wintime, wintime, wintime)
    hfile.Close()

    # 讀取並列印後次時間
    after_ctime = os.path.getctime(path)
    after_mtime = os.path.getmtime(path)
    print(f"[Step2] 變更後 ctime={after_ctime}, mtime={after_mtime}")

# -----------------------------------
# Step1：處理 qbCooking - 搬移+重命名+刪資料夾+更新狀態
# -----------------------------------
print("Step 1: 處理 qbCooking...")
allowed_exts = ('.mp4', '.mkv', '.avi', '.mov')
qb_dirs      = [d for d in os.listdir(qb_dir_path) if os.path.isdir(os.path.join(qb_dir_path, d))]

downloaded = set()
pending    = set()
dirs_seen  = set()

for sub in qb_dirs:
    folder = os.path.join(qb_dir_path, sub)
    ident  = extract_identifier_from_filename(sub, identifiers)
    if not ident:
        print(f"[Step1] 忽略資料夾：{sub}")
        continue
    dirs_seen.add(sub)

    # 有 .!qb 就是未完成下載
    if any(fname.lower().endswith('.!qb') for _,_,fs in os.walk(folder) for fname in fs):
        pending.add(ident)
        print(f"[Step1] {ident}: still downloading")
        continue

    # 已下載 → 搬移 & 重命名
    count = 0
    for root, _, files in os.walk(folder):
        for fn in files:
            ext = os.path.splitext(fn)[1].lower()
            if ext in allowed_exts:
                src = os.path.join(root, fn)
                suffix = "" if count == 0 else f"_duplicated_{count}"
                new_fn = f"{ident}{suffix}{ext}"
                dst = os.path.join(ut_dir_path, new_fn)
                if not os.path.exists(dst):
                    shutil.move(src, dst)
                    print(f"[Step1] {ident}: moved → {new_fn}")
                else:
                    print(f"[Step1] {ident}: 已存在 → {new_fn}")
                count += 1

    # 搬完就刪除整個子資料夾
    try:
        shutil.rmtree(folder)
        print(f"[Step1] 刪除原資料夾：{sub}")
    except Exception as e:
        print(f"[Step1] 刪除 {sub} 失敗：{e}")

    downloaded.add(ident)

# debug：漏掉哪些子資料夾
print(f"[Debug] qbCooking 共 {len(qb_dirs)} 個子資料夾，處理 {len(dirs_seen)}，漏掉 {len(qb_dirs)-len(dirs_seen)}：")
for sub in qb_dirs:
    if sub not in dirs_seen:
        print(f" - {sub}")

# 更新 qbCooking 狀態（column O=15）
updates = []
for ident in downloaded:
    rec = identifier_status_map.get(ident)
    if rec and rec["status"] not in ("已閱", "下載完成"):
        updates.append(Cell(rec["row"], 15, "下載完成"))
for ident in pending:
    rec = identifier_status_map.get(ident)
    if rec and rec["status"] not in ("已閱", "下載中"):
        updates.append(Cell(rec["row"], 15, "下載中"))
if updates:
    sheet.update_cells(updates)
    print("Step1: qbCooking 狀態已更新")

# -----------------------------------
# Step2：處理 uT 資料夾 - metadata 與時間設定
# -----------------------------------
print("Step 2: 處理 uT 資料夾...")
ut_files = [f for f in os.listdir(ut_dir_path) if os.path.isfile(os.path.join(ut_dir_path, f))]
groups   = {}

for fn in ut_files:
    ident = extract_identifier_from_filename(fn, identifiers)
    if ident:
        groups.setdefault(ident, []).append(fn)
    else:
        print(f"[Step2] 跳過：{fn}")

for ident, files in groups.items():
    actor   = identifier_actor_map.get(ident, "")
    pubdate = identifier_date_map.get(ident, "")
    for fn in files:
        path = os.path.join(ut_dir_path, fn)
        print(f"[Step2] 處理 {fn} → 寫演員 & 三時間戳")
        write_actor_metadata(path, actor)
        set_all_file_times(path, pubdate)

# -----------------------------------
# Step3：更新 uT 狀態為「下載完成」
# -----------------------------------
print("Step 3: 更新 uT 狀態...")
updates = []
for ident in groups:
    rec = identifier_status_map.get(ident)
    if not rec:
        print(f"[Step3] {ident}: 無記錄")
        continue
    current = rec["status"]
    updates.append(Cell(rec["row"], 15, "下載完成"))
    print(f"[Step3] {ident}: {current} → 下載完成")

if updates:
    sheet.update_cells(updates)
    print("Step3: uT 狀態已更新")
