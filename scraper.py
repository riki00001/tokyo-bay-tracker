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
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TokyoBayFishingTracker/1.0)"}

POINT_COORDS = {
    "川崎沖": [35.488, 139.762], "本牧沖": [35.410, 139.672],
    "本牧": [35.410, 139.672], "本牧根": [35.410, 139.672],
    "八景沖": [35.335, 139.640], "横浜沖": [35.440, 139.712],
    "小柴沖": [35.348, 139.642], "海堡北側": [35.328, 139.808],
    "第2海堡": [35.316, 139.827], "猿島沖": [35.316, 139.785],
    "観音崎": [35.283, 139.716], "観音沖": [35.278, 139.740],
    "走水沖": [35.308, 139.748], "走水": [35.317, 139.726],
    "大津沖": [35.350, 139.658], "大貫沖": [35.302, 139.855],
    "富津沖": [35.290, 139.855], "木更津沖": [35.390, 139.875],
    "富浦沖": [34.975, 139.880],
}

def extract_point(text):
    for k in sorted(POINT_COORDS.keys(), key=len, reverse=True):
        if k in text:
            return {"name": k, "latlng": POINT_COORDS[k]}
    return None

def extract_depth(text):
    m = re.search(r'水深(\d+)(?:〜(\d+))?m', text)
    if not m:
        return None
    return f"{m.group(1)}〜{m.group(2)}m" if m.group(2) else f"{m.group(1)}m"

def safe_get(url):
    try:
        time.sleep(1.5)
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except Exception as e:
        print(f"  [WARN] {url}: {e}")
        return None

def parse_lines(lines, ship_name, source_url):
    results = []
    current_date = None
    current_fish = None
    current_text = []
    current_catch = ""
    FISH_WORDS = ["タチウオ","アジ","マゴチ","トラフグ","マダイ","シロギス","カワハギ","アナゴ","サバ","サワラ","マダコ"]
    SKIP_WORDS = ["copyright","Copyright","HOME","menu","©","http"]

    for line in lines:
        dm = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', line)
        if dm:
            if current_date and current_text and current_date >= CUTOFF:
                results.append({
                    "ship": ship_name, "date": current_date, "platform": "web",
                    "text": " ".join(current_text[:6])[:300],
                    "fish": current_fish or "不明", "catch": current_catch,
                    "source_url": source_url
                })
            current_date = f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
            current_fish = None
            current_text = []
            current_catch = ""
            continue

        if not current_date:
            continue

        if len(line) < 30 and any(f in line for f in FISH_WORDS):
            current_fish = line.strip("【】　 ")

        m = re.search(r'(\d+)[〜~](\d+)\s*(?:本|匹|尾)', line)
        if m and not current_catch:
            current_catch = f"{m.group(1)}〜{m.group(2)}"

        if len(line) > 15 and not any(x in line for x in SKIP_WORDS):
            current_text.append(line)

    if current_date and current_text and current_date >= CUTOFF:
        results.append({
            "ship": ship_name, "date": current_date, "platform": "web",
            "text": " ".join(current_text[:6])[:300],
            "fish": current_fish or "不明", "catch": current_catch,
            "source_url": source_url
        })
    return results

def scrape_nakayamamaru():
    html = safe_get("https://www.nakayamamaru.com/category/Choka/")
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, "中山丸", "https://www.nakayamamaru.com/category/Choka/")

def scrape_yoshinoya():
    html = safe_get("https://www.team-yoshinoya.com/tsuri/diary/")
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, "深川吉野屋", "https://www.team-yoshinoya.com/tsuri/diary/")

def scrape_fishing_v(ship_id, ship_name):
    url = f"https://www.fishing-v.jp/choka/choka_detail.php?s={ship_id}&pageID=1"
    html = safe_get(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, ship_name, url)

def main():
    print(f"開始: {TODAY.strftime('%Y-%m-%d %H:%M JST')} / 対象: {CUTOFF}以降")
    all_comments = []
    errors = []

    tasks = [
        ("中山丸",     scrape_nakayamamaru, []),
        ("深川吉野屋", scrape_yoshinoya,    []),
        ("一之瀬丸",   scrape_fishing_v,   [186, "一之瀬丸"]),
        ("弁天屋",     scrape_fishing_v,   [190, "弁天屋"]),
        ("忠彦丸",     scrape_fishing_v,   [196, "忠彦丸"]),
        ("小川丸",     scrape_fishing_v,   [1158, "小川丸"]),
        ("荒川屋",     scrape_fishing_v,   [1192, "荒川屋"]),
    ]

    for ship_name, func, args in tasks:
        print(f"  {ship_name}...")
        try:
            entries = func(*args) if args else func()
            for e in entries:
                e["point"] = extract_point(e["text"])
                e["depth"] = extract_depth(e["text"])
            all_comments.extend(entries)
            print(f"    {len(entries)}件")
        except Exception as ex:
            print(f"    [ERROR] {ex}")
            errors.append({"ship": ship_name, "error": str(ex)})

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

    with open("data/comments.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n完了: {len(unique)}件保存")

if __name__ == "__main__":
    main()
