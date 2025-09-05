#!/usr/bin/env python3
import os
import time
from contextlib import contextmanager
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# -----------------------------
# 統一設定 credentials.json 路徑
# -----------------------------
CREDENTIAL_PATH = r"C:\Users\chen8\OneDrive\文件\pythonHouse\utCooking\credentials.json"

# -----------------------------
# 上傳到 Google Sheet 函式
# -----------------------------
def upload_to_google_sheet(data, sheet_id, tab_name):
    print(f"\n🔄 上傳結果到 Google Sheet：{tab_name}")
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    # 刪除已存在的工作表
    try:
        ws = spreadsheet.worksheet(tab_name)
        spreadsheet.del_worksheet(ws)
        print(f"🗑️ 已刪除原本存在的工作表：{tab_name}")
    except gspread.exceptions.WorksheetNotFound:
        pass

    # 建立新工作表，並把「標籤」欄位的標頭改為當下日期戳記
    header = ["識別碼", datetime.now().strftime("%Y/%m/%d")]
    ws = spreadsheet.add_worksheet(title=tab_name,
                                   rows=str(len(data) + 1),
                                   cols=str(len(header)))

    # 一次性上傳（包含標頭）
    ws.update("A1", [header] + data)
    print(f"✅ 已成功上傳 {len(data)} 筆資料到 Google Sheet")

# -----------------------------
# 抑制 Chromium stderr 訊息的 context manager
# -----------------------------
@contextmanager
def suppress_chromium_logs():
    old_stderr = os.dup(2)
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 2)
    try:
        yield
    finally:
        os.dup2(old_stderr, 2)
        os.close(devnull)
        os.close(old_stderr)

# -----------------------------
# 主流程：擷取「辨識碼」+「標籤」+ 進度回報 + 上傳
# -----------------------------
# Edge driver 路徑
edge_driver_path = r"C:\Users\chen8\OneDrive\文件\pythonHouse\edgedriver_win64\msedgedriver.exe"
# 確認 driver 檔案存在
assert os.path.isfile(edge_driver_path), f"找不到 driver：{edge_driver_path}"

# Selenium headless 設定
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-extensions")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
options.add_experimental_option("prefs", {
    "profile.managed_default_content_settings.images": 2,
    "profile.default_content_setting_values.notifications": 2,
    "profile.default_content_setting_values.geolocation": 2,
})
options.add_experimental_option("excludeSwitches", ["enable-logging"])

# 抑制 Chromium 日誌、啟動瀏覽器
with suppress_chromium_logs():
    service = Service(executable_path=edge_driver_path, log_path=os.devnull)
    driver = webdriver.Edge(service=service, options=options)

wait = WebDriverWait(driver, 10)

# 目標標籤設定
target_tags = {
    "今日新種", "昨日新種", "前日新種",
    "3天前新種", "4天前新種", "5天前新種",
    "6天前新種", "7天前新種"
}
results = []
start_time = time.time()

print("🔍 開始查找所有『新種』影片...")
page = 1
while True:
    url = "https://www.javbus.com/" if page == 1 else f"https://www.javbus.com/page/{page}"
    print(f"🌐 開啟第 {page} 頁：{url}")
    driver.get(url)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div#waterfall")))
    except:
        break

    soup = BeautifulSoup(driver.page_source, "html.parser")
    items = soup.select("div#waterfall .item")
    page_hits = []
    for it in items:
        link = it.select_one("a")
        if not link or not link.get("href"):
            continue
        code = os.path.basename(link["href"])
        tags = [b.text.strip() for b in it.select(".item-tag button")]
        for t in tags:
            if t in target_tags:
                page_hits.append([code, t])
    if not page_hits:
        print(f"🛑 第 {page} 頁沒有找到新種影片，停止。")
        break

    print(f"📄 第 {page} 頁找到 {len(page_hits)} 筆")
    results.extend(page_hits)
    page += 1

# 關閉瀏覽器
driver.quit()
print(f"\n🎯 共找到 {len(results)} 部影片")

# 回報進度
for idx, (code, tag) in enumerate(results, start=1):
    print(f"🔎 進度：{idx}/{len(results)} {code} / {tag}")

# 上傳到 Google Sheet（B1 現在會顯示執行程式當下的日期戳記）
spreadsheet_id = "1cizSVrySFHKYfngBhkCCNVRXRJiMYH_2ltts9YAdbEo"
upload_to_google_sheet(results, spreadsheet_id, "checkList")

print(f"⏱️ 總耗時：{round(time.time() - start_time, 2)} 秒")
