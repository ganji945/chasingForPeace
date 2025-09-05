#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
from contextlib import contextmanager
from datetime import datetime

import gspread
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# -----------------------------
# 配置：請修改為你自己的路徑與參數
# -----------------------------
CREDENTIAL_PATH  = r"C:\Users\chen8\OneDrive\文件\pythonHouse\utCooking\credentials.json"
EDGE_DRIVER_PATH = r"C:\Users\chen8\OneDrive\文件\pythonHouse\edgedriver_win64\msedgedriver.exe"
SPREADSHEET_ID   = "1cizSVrySFHKYfngBhkCCNVRXRJiMYH_2ltts9YAdbEo"
TAB_NAME         = "checkList_t66y"

# 篩選上傳時間早於此日期會停止；留空不篩選
target_date_str = "2025-09-01"
target_date     = datetime.strptime(target_date_str, "%Y-%m-%d").date() if target_date_str else None

# 最大抓取筆數
search_qty = 1000

# -----------------------------
# 抑制 Chromium stderr 日誌
# -----------------------------
@contextmanager
def suppress_logs():
    old = os.dup(2)
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 2)
    try:
        yield
    finally:
        os.dup2(old, 2)
        os.close(devnull)
        os.close(old)

# -----------------------------
# 上傳到 Google Sheet（避免重複、加新欄位&公式）
# -----------------------------
def upload_to_google_sheet(data, sheet_id, tab_name):
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, scope)
    client = gspread.authorize(creds)
    ss     = client.open_by_key(sheet_id)

    header = [
        "識別碼","標題","影片大小","影片大小數值",
        "上傳時間","URL","磁力連結",
        "狀態","演員","評級",
        "長度","長度數值","GB/小時"
    ]
    try:
        ws = ss.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=tab_name, rows="1", cols=str(len(header)))
        ws.append_row(header, value_input_option="USER_ENTERED")

    existing_codes = ws.col_values(1)[1:]
    existing_count = len(existing_codes)

    new_rows = []
    for code, title, size, upload_time, url, magnet in data:
        if code in existing_codes:
            continue
        # 提取 size 數值
        m = re.search(r"(\d+(?:\.\d+)?)", size)
        size_val = m.group(1) if m else ""
        row_idx = existing_count + len(new_rows) + 2

        # 欄位公式
        f_status     = f"=vlookup(A{row_idx},Status!A:P,15,0)"
        f_actor      = f"=vlookup(A{row_idx},Status!A:P,7,0)"
        f_rating     = f"=vlookup(A{row_idx},Status!A:P,16,0)"
        f_length     = f"=vlookup(A{row_idx},Status!A:P,3,0)"
        f_length_val = f'=VALUE(REGEXREPLACE(K{row_idx},"[^0-9.]",""))'
        f_gbph       = f"=D{row_idx}/(L{row_idx}/60)"

        new_rows.append([
            code, title, size, size_val,
            upload_time, url, magnet,
            f_status, f_actor, f_rating,
            f_length, f_length_val, f_gbph
        ])

    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    print(f"✅ 已新增 {len(new_rows)} 筆，跳過 {len(data)-len(new_rows)} 筆重複資料。")

# -----------------------------
# 主流程：抓列表、解析 hash、組合 magnet、上傳
# -----------------------------
assert os.path.isfile(EDGE_DRIVER_PATH), f"找不到 driver：{EDGE_DRIVER_PATH}"

options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_experimental_option("excludeSwitches", ["enable-logging"])

with suppress_logs():
    service = Service(executable_path=EDGE_DRIVER_PATH, log_path=os.devnull)
    driver = webdriver.Edge(service=service, options=options)

wait    = WebDriverWait(driver, 10)
results = []
page    = 1
stop    = False

print("🔍 開始抓取含「4K」的文章…")
while len(results) < search_qty and not stop:
    list_url = f"https://t66y.com/thread0806.php?fid=15&search=&page={page}"
    print(f"  第 {page} 頁：{list_url}")
    driver.get(list_url)
    try:
        wait.until(EC.presence_of_element_located((By.ID, "ajaxtable")))
    except:
        break

    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.select("tbody#tbody tr.tr3")
    if not rows:
        break

    for row in rows:
        if len(results) >= search_qty or stop:
            break

        a = row.select_one("h3 a")
        title = a.get_text(strip=True)
        if "4K" not in title:
            continue

        # 1. 辨識碼 & 影片大小
        m_code = re.search(r"([A-Za-z0-9]+-\d+)", title)
        code   = m_code.group(1) if m_code else ""
        m_size = re.search(r"(\d+(?:\.\d+)?\s?(?:G|GB))", title, re.IGNORECASE)
        size   = m_size.group(1) if m_size else ""

        # 2. 上傳時間
        raw = row.select("td")[2].select_one("span")\
                 .get("title", row.select("td")[2].get_text(strip=True))
        date_m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        time_m = re.search(r"(\d{1,2}:\d{2}(?::\d{2})?)", raw)
        if date_m and time_m:
            time_str = f"{date_m.group(1)} {time_m.group(1)}"
        else:
            time_str = raw.strip()
        fmt = "%Y-%m-%d %H:%M:%S" if time_str.count(":") == 2 else "%Y-%m-%d %H:%M"
        dt = datetime.strptime(time_str, fmt).date()
        if target_date and dt < target_date:
            stop = True
            break
        upload_time = time_str

        # 3. URL
        href = a["href"]
        url  = f"https://t66y.com{href}"

        # 4. 解析 rmdown hash
        driver.get(url)
        vid_soup = BeautifulSoup(driver.page_source, "html.parser")
        link     = vid_soup.find("a", href=re.compile(r"rmdown\.com/link\.php\?hash="))
        m_hash   = re.search(r"hash=([0-9a-fA-F]+)", link["href"]) if link else None
        hash_val = m_hash.group(1) if m_hash else ""

        # 5. 組 magnet
        if hash_val and code:
            actual_hash = hash_val[3:] if len(hash_val) > 36 else hash_val
            magnet = f"magnet:?xt=urn:btih:{actual_hash}&dn={code}".split("&tr")[0]
        else:
            magnet = ""

        results.append([code, title, size, upload_time, url, magnet])
        print(f"  + {code} | 上傳時間: {upload_time} | 磁力: {magnet}")

    page += 1

driver.quit()

print(f"🎯 共擷取 {len(results)} 筆資料，開始上傳…")
upload_to_google_sheet(results, SPREADSHEET_ID, TAB_NAME)
print("✅ 完成")
