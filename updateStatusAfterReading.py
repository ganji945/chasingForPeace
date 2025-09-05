#!/usr/bin/env python3
import os
import re
import gspread
from datetime import datetime
from gspread.cell import Cell
from oauth2client.service_account import ServiceAccountCredentials

# ----------------------------- 設定 -----------------------------
# 支援多個 qb 資料夾路徑
qb_dir_paths = [
    r"E:\uT",
    r"C:\Users\chen8\OneDrive\文件\ControllerDriver\Cooked\uT"
]
sheet_url        = "https://docs.google.com/spreadsheets/d/1cizSVrySFHKYfngBhkCCNVRXRJiMYH_2ltts9YAdbEo/edit?usp=sharing"

# === 播放清單設置 ===
playlist_configs = [
    {
        "video_folder": r"C:\Users\chen8\OneDrive\文件\ControllerDriver\Cooked\uT",
        "output_path": r"C:\Users\chen8\OneDrive\文件\ControllerDriver\Cooked\playCooking.dpl"
    },
    {
        "video_folder": r"E:\uT",
        "output_path": r"C:\Users\chen8\OneDrive\文件\ControllerDriver\Cooked\playCooked.dpl"
    }
]

# -----------------------------
# 統一設定 credentials.json 路徑
# -----------------------------
CREDENTIAL_PATH = r"C:\Users\chen8\OneDrive\文件\pythonHouse\utCooking\credentials.json"

# ----------------------------- 連線 Google Sheet -----------------------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]
creds        = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, scope)
client       = gspread.authorize(creds)
spreadsheet  = client.open_by_url(sheet_url)
sheet_status = spreadsheet.worksheet("Status")
sheet_rating = spreadsheet.worksheet("Rating")

# ----------------------------- 讀取 Status 工作表 -----------------------------
all_rows = sheet_status.get_all_values()
# 建立識別碼 → 列索引映射
identifier_row_map = {
    row[0].strip(): idx
    for idx, row in enumerate(all_rows, start=1)
    if row and row[0].strip()
}
identifiers = set(identifier_row_map.keys())

# ----------------------------- 輔助函式：從檔名提取辨識碼 -----------------------------
def extract_identifier_from_filename(filename, identifiers):
    low  = filename.lower()
    norm = re.sub(r'[-_]', '', low)
    for ident in identifiers:
        i_low = ident.lower()
        if (i_low in low
                or i_low.replace('-', '_') in low
                or re.sub(r'[-_]', '', i_low) in norm):
            return ident
    return None

# ----------------------------- 建立 qb 預計掃描的辨識碼集合 -----------------------------
qb_identifiers = set()
qb_files       = []
for qb_dir_path in qb_dir_paths:
    for root, _, files in os.walk(qb_dir_path):
        for fn in files:
            qb_files.append(fn)
            ident = extract_identifier_from_filename(fn, identifiers)
            if ident:
                qb_identifiers.add(ident)

# -----------------------------------------------
# Step1：更新「下載完成」卻不在 qbCooking 的 → 已閱
# -----------------------------------------------
updates_read = []
for ident, idx in identifier_row_map.items():
    row = all_rows[idx-1]
    if len(row) >= 15 and row[14].strip() == "下載完成":
        if ident not in qb_identifiers:
            updates_read.append(Cell(idx, 15, "已閱"))
            print(f"{ident}: not in qbCooking → set to 已閱")
        else:
            print(f"{ident}: still in qbCooking → skip")

if updates_read:
    sheet_status.update_cells(updates_read)
    print("Status sheet: 已閱 更新完成")
else:
    print("Status sheet: no 已閱 updates needed")

# -----------------------------------------------
# Step2：直接從 Rating 表抓取所有評級 = Failed 的演員，並更新 Status
# （僅針對原狀態為「尚無 4K 資源」或「等待下載」的列）
# -----------------------------------------------
rating_rows = sheet_rating.get_all_values()
failed_actors = {
    row[0].strip()
    for row in rating_rows
    if len(row) >= 8 and row[7].strip().lower() == "failed" and row[0].strip()
}

updates_skip = []
for ident, idx in identifier_row_map.items():
    row = all_rows[idx-1]
    if len(row) >= 15 and row[14].strip() in ("尚無 4K 資源", "等待下載"):
        actor = row[6].strip() if len(row) >= 7 else ""
        if actor in failed_actors:
            updates_skip.append(Cell(idx, 15, "跳過"))
            print(f"{ident}: actor {actor} failed → set to 跳過")

if updates_skip:
    sheet_status.update_cells(updates_skip)
    print("Status sheet: 跳過 更新完成")
else:
    print("Status sheet: no 跳過 updates needed")

# -----------------------------------------------
# Step3：qb_dir_paths 中所有仍存在的辨識碼 → 下載完成
# -----------------------------------------------
updates_complete = []
for ident in qb_identifiers:
    idx = identifier_row_map.get(ident)
    if not idx:
        continue
    row = all_rows[idx-1]
    current = row[14].strip() if len(row) >= 15 else ""
    if current != "下載完成":
        updates_complete.append(Cell(idx, 15, "下載完成"))
        print(f"{ident}: set to 下載完成")

if updates_complete:
    sheet_status.update_cells(updates_complete)
    print("Status sheet: 下載完成 更新完成")
else:
    print("Status sheet: no 下載完成 updates needed")

# -----------------------------------------------
# Step4：列出 qb_dir_paths 中檔案但不在 Status 工作表的識別碼
# -----------------------------------------------
missing = set()
for fn in qb_files:
    ident = extract_identifier_from_filename(fn, identifiers)
    if not ident:
        m = re.search(r'[A-Za-z]+-\d+', fn)
        ident = m.group(0) if m else None
    if ident and ident not in identifier_row_map:
        missing.add(ident)

if missing:
    print("Warning: the following identifiers exist in qb_dir_paths but not in Status sheet:")
    for ident in sorted(missing):
        print(f"  - {ident}")
else:
    print("Step4: no missing identifiers.")


# ====更新播放清單=====

# 支援的影片副檔名
video_extensions = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv"}

def generate_playlist(video_folder: str, output_path: str):
    """
    掃描指定資料夾內的不遞迴影片檔案，依建立日期 (舊→新) 排序，
    最後輸出成 PotPlayer 可讀的 .dpl 播放清單。
    """
    print(f"\n==== 處理資料夾：{video_folder} ====")
    videos = []
    skipped_files = []

    # 1) 掃描電影檔
    for filename in os.listdir(video_folder):
        filepath = os.path.join(video_folder, filename)
        ext = os.path.splitext(filename)[1].lower()
        if os.path.isfile(filepath) and ext in video_extensions:
            try:
                created_time = os.path.getctime(filepath)
                videos.append((filename, created_time))
            except Exception as e:
                print(f"⚠️ 無法取得建立時間：{filename}，錯誤：{e}")
                skipped_files.append(filename)

    print(f"✅ 偵測到 {len(videos)} 個可用影片檔案")
    if skipped_files:
        print("⚠️ 下列檔案因錯誤被略過：")
        for f in skipped_files:
            print(f"    - {f}")

    # 2) 依建立日期排序（從舊到新）
    videos.sort(key=lambda x: x[1])

    # 3) 組成 .dpl 內容
    dpl_lines = [
        "DAUMPLAYLIST",
        f"playname={os.path.basename(output_path)}",
        "topindex=0",
        "saveplaypos=0"
    ]
    for index, (filename, _) in enumerate(videos, start=1):
        full_path = os.path.join(video_folder, filename)
        dpl_lines.append(f"{index}*file*{full_path}")
        dpl_lines.append(f"{index}*title*{filename}")
        dpl_lines.append(f"{index}*played*0")

    # 4) 寫入 .dpl 檔案
    try:
        # 確保輸出路徑所在資料夾存在
        out_dir = os.path.dirname(output_path)
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(dpl_lines))
        print(f"🎉 播放清單已成功更新：{output_path}")
    except Exception as e:
        print(f"❌ 播放清單寫入失敗：{e}")


if __name__ == "__main__":
    # 依序處理每一組設定
    for config in playlist_configs:
        video_folder = config["video_folder"]
        output_path = config["output_path"]

        # 檢查資料夾是否存在
        if not os.path.isdir(video_folder):
            print(f"❌ 資料夾不存在：{video_folder}，跳過這組設定。")
            continue

        generate_playlist(video_folder, output_path)
