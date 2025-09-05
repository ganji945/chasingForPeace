import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import time
import pandas as pd
import requests
from urllib3.exceptions import ProtocolError
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2 import service_account

# 如果要用 Selenium 抓新的 mglinks，設為 True；如果直接用現有的 mglinks_checkList，設為 False
FETCH_NEW_MGLINKS = True

# -----------------------------
# 常數設定
# -----------------------------
CREDENTIAL_PATH  = r"C:\Users\chen8\OneDrive\文件\pythonHouse\utCooking\credentials.json"
SPREADSHEET_ID   = "1cizSVrySFHKYfngBhkCCNVRXRJiMYH_2ltts9YAdbEo"
CHECKLIST_TAB    = "checkList"
MGLINKS_TAB      = "mglinks_checkList"
STATUS_TAB       = "Status"
RATING_TAB       = "Rating"

# -----------------------------
# safe_api_call：重試機制
# -----------------------------
def safe_api_call(func, *args, **kwargs):
    max_retries = 5
    delay = 1
    for attempt in range(1, max_retries+1):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            print(f"⚠️ Google APIError ({e}), 等待 {delay}s 重試 ({attempt}/{max_retries})")
        except (requests.exceptions.ConnectionError, ProtocolError) as e:
            print(f"⚠️ 連線中斷 ({e.__class__.__name__}), 等待 {delay}s 重試 ({attempt}/{max_retries})")
        time.sleep(delay)
        delay *= 2
    raise Exception("❌ safe_api_call: 超過最大重試次數，仍然失敗。")

# -----------------------------
# 將可能的 numpy/pandas 型別轉成 Python 原生型別
# -----------------------------
def to_native(x):
    try:
        # numpy scalar, pandas scalar
        if hasattr(x, "item"):
            return x.item()
        # pandas NA
        if pd.isna(x):
            return ""
        return x
    except:
        return x

def to_native_list(lst):
    return [to_native(x) for x in lst]

# -----------------------------
# init_google_sheet：新增 mglinks worksheet
# -----------------------------
def init_google_sheet(sheet_id, tab_name, columns):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, scope)
    client = gspread.authorize(creds)
    ss = client.open_by_key(sheet_id)
    try:
        ws = ss.worksheet(tab_name)
        safe_api_call(ss.del_worksheet, ws)
    except gspread.exceptions.WorksheetNotFound:
        pass
    ws = safe_api_call(ss.add_worksheet, title=tab_name, rows="2000", cols=str(len(columns)))
    safe_api_call(ws.append_row, columns)
    return ws

# -----------------------------
# 批次寫入 mglinks
# -----------------------------
def append_rows_to_sheet_batch(ws, buffer, batch_size=20):
    while len(buffer) >= batch_size:
        batch = buffer[:batch_size]
        safe_api_call(ws.append_rows, batch)
        print(f"✅ 寫入 {batch_size} 筆 mglinks")
        del buffer[:batch_size]
        time.sleep(1)

# -----------------------------
# apply_conditional_formatting：4K 條件式格式
# -----------------------------
def apply_conditional_formatting(spreadsheet_id, tab_name):
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIAL_PATH, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    svc = build('sheets', 'v4', credentials=creds)
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    gid = next(
        (s['properties']['sheetId'] for s in meta['sheets'] if s['properties']['title'] == tab_name),
        None
    )
    if gid is None:
        print(f"❌ 找不到工作表 {tab_name}")
        return
    body = {
        "requests": [
            {"addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": gid, "startRowIndex": 0, "startColumnIndex": 0}],
                    "booleanRule": {
                        "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": "=$L1=TRUE"}]},
                        "format": {"backgroundColor": {"red": 0.34, "green": 0.733, "blue": 0.541}}
                    }
                },
                "index": 0
            }}
        ]
    }
    req = svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body)
    safe_api_call(req.execute)
    print("🎨 已套用 4K 條件式格式")

# -----------------------------
# fetch_and_parse：Selenium 抓新資料
#    - 當完全沒有磁力列時，也回傳一筆基本欄位資訊，並將磁力相關欄位留空
# -----------------------------
def fetch_and_parse(identifier, driver):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup

    url = f"https://www.javbus.com/{identifier}"
    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#magnet-table")))
    except:
        pass
    soup = BeautifulSoup(driver.page_source, "html.parser")

    def safe_text(sel, method="text", default="無資訊"):
        el = soup.select_one(sel)
        if not el:
            return default
        if method == "text":
            return el.text.strip()
        if method == "next_sibling":
            return el.next_sibling.strip()
        if method == "find_next":
            nxt = el.find_next("a")
            return nxt.text.strip() if nxt else default
        return default

    # 基本欄位：發行日期、長度、製作商、發行商
    rd = safe_text("span:-soup-contains('發行日期:')", "next_sibling")
    ln = safe_text("span:-soup-contains('長度:')", "next_sibling")
    st = safe_text("span:-soup-contains('製作商:')", "find_next")
    lb = safe_text("span:-soup-contains('發行商:')", "find_next")

    # 類別
    cats = []
    toggle = soup.select_one("#genre-toggle")
    if toggle:
        p = toggle.find_parent("p").find_next_sibling("p")
        if p:
            for a in p.select("label>a"):
                t = a.text.strip()
                if t:
                    cats.append(t)
    cat = " ; ".join(cats) if cats else "無資訊"

    # 演員
    actors = [a.text.strip() for a in soup.select("div.star-name a")]
    actor = " ; ".join(actors) if actors else "無資訊"

    # 嘗試抓取所有磁力列
    rows = soup.select("#magnet-table tr")[1:]
    out = []
    try:
        mins = int(ln.replace("分鐘", ""))
    except:
        mins = 0
    hrs = mins / 60 if mins > 0 else 1
    thr = 4.0

    for r in rows:
        cols = r.find_all("a", href=True)
        if len(cols) < 3:
            continue
        name      = cols[0].text.strip()
        size_text = cols[1].text.strip()
        # 轉成純數值 GB，MB 除以 1000
        if "GB" in size_text.upper():
            try:
                size_val = float(size_text.upper().replace("GB", "").strip())
            except:
                size_val = 0.0
        elif "MB" in size_text.upper():
            try:
                size_val = float(size_text.upper().replace("MB", "").strip()) / 1000
            except:
                size_val = 0.0
        else:
            try:
                size_val = float(size_text)
            except:
                size_val = 0.0

        date = cols[2].text.strip()
        urlm = cols[0]["href"]
        tags = [t.text.strip() for t in cols[0].parent.find_all("a", class_="btn") if t.text.strip()]
        tagstr = ", ".join(tags)

        per = round(size_val / hrs, 2) if hrs > 0 else 0
        is4k = per > thr

        out.append([
            identifier, rd, ln, st, lb, cat, actor,
            name, size_val, date, urlm,
            per, is4k, tagstr
        ])

    # 如果完全沒抓到任何一筆磁力列，回傳一筆只有「基本欄位」的空資料
    if not out:
        out.append([
            identifier,       # 識別碼
            rd,               # 發行日期
            ln,               # 長度
            st,               # 製作商
            lb,               # 發行商
            cat,              # 類別
            actor,            # 演員
            "",               # 磁力名稱（空）
            "",               # 檔案大小（空）
            "",               # 分享日期（空）
            "",               # Magnet 連結（空）
            0.0,              # 每小時檔案大小 (GB/hr)（用 0 填充）
            False,            # 是否為 4K 資源（False）
            ""                # tag（空）
        ])

    return out

# -----------------------------
# update_status_sheet：共用更新 logic（新增 4k60fps 標籤，並將 Numpy -> Python native）
# -----------------------------
def update_status_sheet(sheet_id, mglinks_tab, columns, ws_out):
    df = pd.DataFrame(ws_out.get_all_records())
    df["識別碼"] = df["識別碼"].str.upper()
    selected = []
    for ident, grp in df.groupby("識別碼"):
        grp = grp.copy()
        grp["tag"] = grp["tag"].fillna("")
        # 新優先級邏輯： >8GB/hr → >4GB/hr → <=4 + 字幕 → 其他
        def pr(r):
            per = float(r["每小時檔案大小 (GB/hr)"])
            has_sub = "字幕" in r["tag"]
            if per > 8:
                return 1
            elif per > 4:
                return 2
            elif has_sub:
                return 3
            else:
                return 4

        grp["priority"] = grp.apply(pr, axis=1)
        grp = grp.sort_values(by=["priority", "分享日期"], ascending=[True, False])

        # 取排序後的第一筆
        raw_row = grp.iloc[0].tolist()
        # 先將所有可能的 numpy/pandas 型別轉成 Python 原生
        row = to_native_list(raw_row)
        row[0] = row[0].upper()

        # 如果 per_hr > 8，且 tag 沒有 4k60fps，就加上
        try:
            per_hr = float(row[11])
        except:
            per_hr = 0.0

        if per_hr > 8:
            orig_tag = row[13] or ""
            if "4k60fps" not in orig_tag:
                row[13] = orig_tag + (", 4k60fps" if orig_tag else "4k60fps")

        selected.append(row)

    # 連線 Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, scope)
    client = gspread.authorize(creds)
    ss = client.open_by_key(sheet_id)

    # Rating map
    try:
        rd = pd.DataFrame(ss.worksheet(RATING_TAB).get_all_records())
        rd["演員"] = rd["演員"].astype(str).str.strip()
        rd["評級"] = rd["評級"].astype(str).str.strip()
        rating_map = dict(zip(rd["演員"], rd["評級"]))
    except:
        rating_map = {}

    # Status 工作表
    try:
        ws_s = ss.worksheet(STATUS_TAB)
    except gspread.exceptions.WorksheetNotFound:
        ws_s = safe_api_call(ss.add_worksheet, title=STATUS_TAB, rows="2000", cols=str(len(columns)+2))
        headers = columns + ["狀態", "評級"]
        safe_api_call(ws_s.append_row, headers)
    status_df = pd.DataFrame(ws_s.get_all_records())
    if not status_df.empty:
        status_df["識別碼"] = status_df["識別碼"].str.upper()

    skip_states = ["已閱", "跳過", "下載完成", "下載中"]
    inserts = []
    vrs = []
    sh = ws_s.title

    for row in selected:
        ident = row[0]
        desired = (
            "跳過" if any(tag in row[5] for tag in ["ハイクオリティVR", "VR専用", "8KVR"])
            else "跳過" if rating_map.get(row[6].strip()) == "Failed"
            else ("等待下載" if str(row[12]).strip().upper() == "TRUE" else "尚無 4K 資源")
        )
        base = [to_native(x) for x in row[0:14]] + [desired]

        if ident not in status_df["識別碼"].values:
            r = len(status_df) + len(inserts) + 2
            formula = f'=IFERROR(VLOOKUP(G{r},Rating!A:H,8,0),"Multiple")'
            inserts.append(base + [formula])
            continue

        idx = status_df[status_df["識別碼"] == ident].index[0]
        r = idx + 2
        cur = status_df.at[idx, "狀態"]

        # 更新 A~G
        exA = status_df.loc[idx, ["識別碼","發行日期","長度","製作商","發行商","類別","演員"]].tolist()
        exA = to_native_list(exA)
        nA = base[0:7]
        if exA != nA:
            vrs.append({"range": f"{sh}!A{r}:G{r}", "values":[nA]})

        # 更新 H~N（只有當前狀態不在 skip_states 時）
        if cur not in skip_states:
            exH = status_df.loc[idx, ["磁力名稱","檔案大小","分享日期","Magnet 連結","每小時檔案大小 (GB/hr)","是否為 4K 資源","tag"]].tolist()
            exH = to_native_list(exH)
            nH = base[7:14]
            if exH != nH:
                vrs.append({"range": f"{sh}!H{r}:N{r}", "values":[nH]})

        # 更新 狀態（只有當前狀態不在 skip_states 且與 desired 不同）
        if cur not in skip_states and cur != desired:
            vrs.append({"range": f"{sh}!O{r}", "values":[[desired]]})

    # 批次更新
    if vrs:
        body = {"valueInputOption":"RAW","data":vrs}
        safe_api_call(ss.values_batch_update, body)
    # 批次新增
    if inserts:
        safe_api_call(ws_s.append_rows, inserts, value_input_option="USER_ENTERED")

# -----------------------------
# update_rating_sheet：更新 Rating sheet 中的演員
# -----------------------------
def update_rating_sheet(sheet_id, ws_mglinks):
    df = pd.DataFrame(ws_mglinks.get_all_records())
    actors_series = df["演員"].dropna().astype(str)
    unique_actors = set()
    for cell in actors_series:
        for actor in cell.split(" ; "):
            a = actor.strip()
            if a:
                unique_actors.add(a)

    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, scope)
    client = gspread.authorize(creds)
    ss = client.open_by_key(sheet_id)
    try:
        ws_r = ss.worksheet(RATING_TAB)
    except gspread.exceptions.WorksheetNotFound:
        ws_r = safe_api_call(ss.add_worksheet, title=RATING_TAB, rows="2000", cols="9")
        safe_api_call(ws_r.append_row, ["演員","總番數","尚無 4K 資源","等待下載","下載中","下載完成","已閱","評級","備註"])

    existing = pd.DataFrame(ws_r.get_all_records())
    existing_names = set(existing["演員"].astype(str).str.strip().tolist())
    to_add = sorted(unique_actors - existing_names)

    if to_add:
        start_row = len(existing) + 2
        rows = []
        for i,name in enumerate(to_add):
            r = start_row + i
            total_fn = f'=COUNTIFS(Status!$G:$G,$A{r},Status!$O:$O,"<>")'
            no4k_fn  = f'=COUNTIFS(Status!$G:$G,$A{r},Status!$O:$O,C$1)'
            wait_fn  = f'=COUNTIFS(Status!$G:$G,$A{r},Status!$O:$O,D$1)'
            dlng_fn  = f'=COUNTIFS(Status!$G:$G,$A{r},Status!$O:$O,E$1)'
            dlok_fn  = f'=COUNTIFS(Status!$G:$G,$A{r},Status!$O:$O,F$1)'
            read_fn  = f'=COUNTIFS(Status!$G:$G,$A{r},Status!$O:$O,G$1)'
            rows.append([name,total_fn,no4k_fn,wait_fn,dlng_fn,dlok_fn,read_fn,"",""])
        safe_api_call(ws_r.append_rows, rows, value_input_option="USER_ENTERED")
        print(f"✅ 新增 {len(rows)} 位演員到 Rating: {', '.join(to_add)}")

# -----------------------------
# 主流程：切換模式 & 更新
# -----------------------------
if __name__ == "__main__":
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIAL_PATH, scope)
    client = gspread.authorize(creds)
    ss = client.open_by_key(SPREADSHEET_ID)

    if FETCH_NEW_MGLINKS:
        ws_out = init_google_sheet(
            SPREADSHEET_ID, MGLINKS_TAB,
            ["識別碼","發行日期","長度","製作商","發行商","類別","演員",
             "磁力名稱","檔案大小","分享日期","Magnet 連結",
             "每小時檔案大小 (GB/hr)","是否為 4K 資源","tag"]
        )
        from selenium import webdriver
        from selenium.webdriver.edge.service import Service as EdgeService
        from selenium.webdriver.edge.options import Options

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_experimental_option("excludeSwitches",["enable-logging"])
        options.add_argument("--disable-logging")
        options.add_experimental_option("prefs", {
            "profile.managed_default_content_settings.images":2,
            "profile.default_content_setting_values.notifications":2,
            "profile.default_content_setting_values.geolocation":2
        })
        driver = webdriver.Edge(
            service=EdgeService(r"C:\Users\chen8\OneDrive\文件\pythonHouse\edgedriver_win64\msedgedriver.exe"),
            options=options
        )

        ws_in = ss.worksheet(CHECKLIST_TAB)
        codes = ws_in.col_values(1)[1:]
        buffer = []
        for code in codes:
            buffer.extend(fetch_and_parse(code.strip(), driver))
            append_rows_to_sheet_batch(ws_out, buffer, batch_size=20)
        if buffer:
            safe_api_call(ws_out.append_rows, buffer)
        apply_conditional_formatting(SPREADSHEET_ID, MGLINKS_TAB)
        driver.quit()
    else:
        ws_out = ss.worksheet(MGLINKS_TAB)

    update_rating_sheet(SPREADSHEET_ID, ws_out)
    update_status_sheet(
        SPREADSHEET_ID, MGLINKS_TAB,
        ["識別碼","發行日期","長度","製作商","發行商","類別","演員",
         "磁力名稱","檔案大小","分享日期","Magnet 連結",
         "每小時檔案大小 (GB/hr)","是否為 4K 資源","tag"
        ],
        ws_out
    )

    print("✅ 全部更新完成")
