"""
東京湾釣果スクレイパー
各船宿の公式サイト・釣りビジョンから直近データを収集して
data/comments.json に保存する
"""
import json
import re
import time
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))
TODAY = datetime.now(JST)
WEEK_AGO = TODAY - timedelta(days=7)
CUTOFF = WEEK_AGO.strftime("%Y-%m-%d")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TokyoBayFishingTracker/1.0)"
}

POINT_COORDS = {
    "川崎沖":       [35.488, 139.762],
    "本牧沖":       [35.410, 139.672],
    "本牧":         [35.410, 139.672],
    "本牧根":       [35.410, 139.672],
    "八景沖":       [35.335, 139.640],
    "横浜沖":       [35.440, 139.712],
    "小柴沖":       [35.348, 139.642],
    "富岡沖":       [35.338, 139.652],
    "海堡北側":     [35.328, 139.808],
    "第2海堡":      [35.316, 139.827],
    "猿島沖":       [35.316, 139.785],
    "観音崎":       [35.283, 139.716],
    "観音沖":       [35.278, 139.740],
    "観音崎沖":     [35.278, 139.745],
    "走水沖":       [35.308, 139.748],
    "走水":         [35.317, 139.726],
    "大津沖":       [35.350, 139.658],
    "大貫沖":       [35.302, 139.855],
    "富津沖":       [35.290, 139.855],
    "竹岡沖":       [35.268, 139.875],
    "久里浜沖":     [35.218, 139.738],
    "木更津沖":     [35.390, 139.875],
    "富浦沖":       [34.975, 139.880],
}

def extract_point(text):
    keys = sorted(POINT_COORDS.keys(), key=len, reverse=True)
    for k in keys:
        if k in text:
            return {"name": k, "latlng": POINT_COORDS[k]}
    return None

def extract_depth(text):
    m = re.search(r'水深(\d+)(?:〜(\d+))?m|(\d+)m前後', text)
    if not m:
        return None
    if m.group(1) and m.group(2):
        return f"{m.group(1)}〜{m.group(2)}m"
    elif m.group(1):
        return f"{m.group(1)}m"
    elif m.group(3):
        return f"{m.group(3)}m前後"
    return None

def safe_get(url, delay=1.5):
    try:
        time.sleep(delay)
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except Exception as e:
        print(f"  [WARN] {url}: {e}")
        return None

def scrape_nakayamamaru():
    results = []
    html = safe_get("https://www.nakayamamaru.com/category/Choka/")
    if not html:
        return results
    soup = BeautifulSoup(html, "lxml")
    content = soup.get_text(separator="\n")
    lines = [l.strip() for l in content.split("\n") if l.strip()]

    current_date = None
    current_fish = None
    current_text_lines = []
    current_catch = ""

    for line in lines:
        dm = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', line)
        if dm:
            if current_date and current_text_lines and current_date >= CUTOFF:
                results.append({
                    "ship": "中山丸",
                    "date": current_date,
                    "platform": "web",
                    "text": " ".join(current_text_lines)[:300],
                    "fish": current_fish or "不明",
                    "catch": current_catch,
                    "source_url": "https://www.nakayamamaru.com/category/Choka/"
                })
            current_date = f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
            current_fish = None
            current_text_lines = []
            current_catch = ""
            continue

        if current_date:
            fish_words = ["タチウオ","アジ","マゴチ","トラフグ","マダイ","シロギス","カワハギ","アナゴ","サバ","サワラ"]
            if len(line) < 30 and any(f in line for f in fish_words):
                current_fish = line

            catch_m = re.search(r'(\d+)[〜~](\d+)\s*(?:本|匹|尾)', line)
            if catch_m and not current_catch:
                current_catch = f"{catch_m.group(1)}〜{catch_m.group(2)}"

            if len(line) > 15:
                current_text_lines.append(line)

    if current_date and current_text_lines and current_date >= CUTOFF:
        results.append({
            "ship": "中山丸",
            "date": current_date,
            "platform": "web",
            "text": " ".join(current_text_lines[:5])[:300],
            "fish": current_fish or "不明",
            "catch": current_catch,
            "source_url": "https://www.nakayamamaru.com/category/Choka/"
        })
    return results

def scrape_yoshinoya():
    results = []
    html = safe_get("https://www.team-yoshinoya.com/tsuri/diary/")
    if not html:
        return results
    soup = BeautifulSoup(html, "lxml")
    content = soup.get_text(separator="\n")
    lines = [l.strip() for l in content.split("\n") if l.strip()]

    current_date = None
    current_fish = None
    current_text_lines = []
    current_catch = ""

    for line in lines:
        dm = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', line)
        if dm:
            if current_date and current_text_lines and current_date >= CUTOFF:
                results.append({
                    "ship": "深川吉野屋",
                    "date": current_date,
                    "platform": "web",
                    "text": " ".join(current_text_lines[:5])[:300],
                    "fish": current_fish or "不明",
                    "catch": current_catch,
                    "source_url": "https://www.team-yoshinoya.com/tsuri/diary/"
                })
            current_date = f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
            current_fish = None
            current_text_lines = []
            current_catch = ""
            continue

        # 日のみの行（例: "03(金)"）
        dm2 = re.match(r'^(\d{1,2})\([月火水木金土日]\)$', line)
        if dm2:
            day = int(dm2.group(1))
            year = TODAY.year
            month = TODAY.month
            current_date = f"{year}-{month:02d}-{day:02d}"
            current_fish = None
            current_text_lines = []
            current_catch = ""
            continue

        if current_date:
            for fish in ["マゴチ","タチウオ","アジ","サワラ","シロギス","マダイ","マダコ","フグ"]:
                if f"【{fish}" in line:
                    current_fish = fish
                    break
            catch_m = re.search(r'(\d+)[〜~](\d+)\s*(?:匹|本|尾)', line)
            if catch_m and not current_catch:
                current_catch = f"{catch_m.group(1)}〜{catch_m.group(2)}"
            skip_words = ["copyright","Copyright","HOME","menu","©"]
            if len(line) > 15 and not any(x in line for x in skip_words):
                current_text_lines.append(line)

    if current_date and current_text_lines and current_date >= CUTOFF:
        results.append({
            "ship": "深川吉野屋",
            "date": current_date,
            "platform": "web",
            "text": " ".join(current_text_lines[:5])[:300],
            "fish": current_fish or "不明",
            "catch": current_catch,
            "source_url": "https://www.team-yoshinoya.com/tsuri/diary/"
        })
    return results

def scrape_fishing_v_text(ship_id, ship_name):
    results = []
    url = f"https://www.fishing-v.jp/choka/choka_detail.php?s={ship_id}&pageID=1"
    html = safe_get(url)
    if not html:
        return results
    soup = BeautifulSoup(html, "lxml")

    current_date = None
    by_date = {}

    for tag in soup.find_all(["h2","h3","li","p","td","tr"]):
        text = tag.get_text(strip=True)
        dm = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', text)
        if dm:
            current_date = f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
            continue
        if current_date and current_date >= CUTOFF and len(text) > 5:
            by_date.setdefault(current_date, []).append(text)

    for date, texts in by_date.items():
        combined = " ".join(texts[:10])
        fish_m = re.search(r'(タチウオ|アジ|マゴチ|マダイ|サワラ|シロギス|カワハギ|マダコ|フグ|トラフグ)', combined)
        catch_m = re.search(r'(\d+)[〜~](\d+)\s*(?:匹|本|尾)', combined)
        results.append({
            "ship": ship_name,
            "date": date,
            "platform": "web",
            "text": combined[:300],
            "fish": fish_m.group(1) if fish_m else "不明",
            "catch": f"{catch_m.group(1)}〜{catch_m.group(2)}" if catch_m else "",
            "source_url": url
        })
    return results

def main():
    print(f"スクレイピング開始: {TODAY.strftime('%Y-%m-%d %H:%M JST')}")
    print(f"取得対象: {CUTOFF} 以降\n")

    all_comments = []
    errors = []

    tasks = [
        ("中山丸",     scrape_nakayamamaru,   []),
        ("深川吉野屋", scrape_yoshinoya,       []),
        ("一之瀬丸",   scrape_ichinosemaru,    []),
        ("弁天屋",     scrape_chowari,         ["00300", "弁天屋"]),
        ("忠彦丸",     scrape_tadahikomaru,    []),
        # ("小川丸",     scrape_fishing_v_text,  [1158, "小川丸"])  # ID要確認,
        # ("荒川屋",     scrape_fishing_v_text,  [1192, "荒川屋"])  # ID要確認,
        # chowariから追加取得
        ("荒川屋",     scrape_chowari,         ["00007", "荒川屋"]),
        ("小川丸",     scrape_chowari,         ["00458", "小川丸"]),
        ("忠彦丸(chowari)", scrape_chowari,    ["00703", "忠彦丸"]),
        ("一之瀬丸(chowari)", scrape_chowari,  ["00307", "一之瀬丸"]),
    ]

    for ship_name, func, args in tasks:
        print(f"  取得中: {ship_name}...")
        try:
            entries = func(*args) if args else func()
            for e in entries:
                e["point"] = extract_point(e["text"])
                e["depth"] = extract_depth(e["text"])
            all_comments.extend(entries)
            print(f"    → {len(entries)}件")
        except Exception as ex:
            print(f"    [ERROR] {ex}")
            errors.append({"ship": ship_name, "error": str(ex)})

    # 重複除去
    seen = set()
    unique = []
    for c in all_comments:
        key = f"{c['ship']}_{c['date']}_{c['fish']}"
        if key not in seen:
            seen.add(key)
            unique.append(c)

    unique.sort(key=lambda x: x["date"], reverse=True)

    output = {
        "updated_at": TODAY.strftime("%Y-%m-%d %H:%M JST"),
        "cutoff": CUTOFF,
        "count": len(unique),
        "errors": errors,
        "comments": unique
    }

    import os
    os.makedirs("data", exist_ok=True)
    with open("data/comments.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n完了: {len(unique)}件 → data/comments.json")
    if errors:
        print(f"エラー: {len(errors)}件")

if __name__ == "__main__":
    main()
