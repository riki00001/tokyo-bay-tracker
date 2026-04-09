import json
import os
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
    "富浦沖": [34.975, 139.880], "竹岡沖": [35.268, 139.875],
    "久里浜沖": [35.218, 139.738], "中ノ瀬": [35.430, 139.780],
    "扇島": [35.505, 139.758], "羽田沖": [35.540, 139.770],
    "浦賀水道": [35.218, 139.782], "航路内": [35.420, 139.760],
}

def extract_point(text):
    for k in sorted(POINT_COORDS.keys(), key=len, reverse=True):
        if k in text:
            return {"name": k, "latlng": POINT_COORDS[k]}
    return None

def extract_depth(text):
    m = re.search(r'水深(\d+)(?:〜(\d+))?m|(\d+)[Mm]前後', text)
    if not m:
        return None
    if m.group(1) and m.group(2):
        return f"{m.group(1)}〜{m.group(2)}m"
    elif m.group(1):
        return f"{m.group(1)}m"
    return None

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

FISH_WORDS = ["タチウオ","アジ","マゴチ","トラフグ","マダイ","シロギス",
              "カワハギ","アナゴ","サバ","サワラ","マダコ","イカ","メバル",
              "ハゼ","シーバス","スズキ","カレイ","イシモチ","カサゴ"]
SKIP_WORDS = ["copyright","Copyright","©","HOME","ログイン","会員登録",
              "お気に入り","釣り船詳細を見る","戻る（","予約プランを見る",
              "最安値でネット予約","続きを表示","Amazon Pay","クレジットカード"]

def parse_lines(lines, ship_name, source_url):
    results = []
    current_date = None
    current_fish = None
    current_text = []
    current_catch = ""
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
        if len(line) < 40 and any(f in line for f in FISH_WORDS):
            current_fish = next((f for f in FISH_WORDS if f in line), None)
        m = re.search(r'(\d+)[〜~～](\d+)\s*(?:本|匹|尾)', line)
        if m and not current_catch:
            current_catch = f"{m.group(1)}〜{m.group(2)}"
        if len(line) > 10 and not any(x in line for x in SKIP_WORDS):
            current_text.append(line)
    if current_date and current_text and current_date >= CUTOFF:
        results.append({
            "ship": ship_name, "date": current_date, "platform": "web",
            "text": " ".join(current_text[:6])[:300],
            "fish": current_fish or "不明", "catch": current_catch,
            "source_url": source_url
        })
    return results

# ── 公式サイト直接スクレイパー ──────────────────────

def scrape_nakayamamaru():
    html = safe_get("https://www.nakayamamaru.com/category/Choka/")
    if not html: return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, "中山丸", "https://www.nakayamamaru.com/category/Choka/")

def scrape_yoshinoya():
    html = safe_get("https://www.team-yoshinoya.com/tsuri/diary/")
    if not html: return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, "深川吉野屋", "https://www.team-yoshinoya.com/tsuri/diary/")

def scrape_ichinosemaru():
    html = safe_get("https://www.ichinosemaru.net/")
    if not html: return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, "一之瀬丸", "https://www.ichinosemaru.net/")

def scrape_tadahikomaru():
    for url in ["https://tadahikomaru.com/", "https://www.tadahikomaru.jp/"]:
        html = safe_get(url)
        if not html: continue
        soup = BeautifulSoup(html, "lxml")
        lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
        entries = parse_lines(lines, "忠彦丸", url)
        if entries: return entries
    return []

def scrape_yoshikyu():
    html = safe_get("https://www.yoshikyu.com/")
    if not html: return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, "吉久（浦安）", "https://www.yoshikyu.com/")

# ── chowari 共通スクレイパー ─────────────────────────

def scrape_chowari(ship_id, ship_name):
    results = []
    url = f"https://www.chowari.jp/ship/{ship_id}/catch/"
    html = safe_get(url)
    if not html: return results
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    current_date = None
    current_fish = None
    current_text = []
    current_catch = ""
    for line in lines:
        dm = re.search(r'釣行日[：:]\s*(\d{4})年(\d{1,2})月(\d{1,2})日', line)
        if not dm:
            dm = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', line)
        if dm:
            if current_date and current_text and current_date >= CUTOFF:
                results.append({
                    "ship": ship_name, "date": current_date, "platform": "web",
                    "text": " ".join(current_text[:6])[:300],
                    "fish": current_fish or "不明", "catch": current_catch,
                    "source_url": url
                })
            current_date = f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
            current_fish = None
            current_text = []
            current_catch = ""
            continue
        if not current_date: continue
        if len(line) < 40 and any(f in line for f in FISH_WORDS):
            current_fish = next((f for f in FISH_WORDS if f in line), None)
        m = re.search(r'(\d+)[〜~～](\d+)\s*(?:本|匹|尾)', line)
        if m and not current_catch:
            current_catch = f"{m.group(1)}〜{m.group(2)}"
        if len(line) > 10 and not any(x in line for x in SKIP_WORDS):
            current_text.append(line)
    if current_date and current_text and current_date >= CUTOFF:
        results.append({
            "ship": ship_name, "date": current_date, "platform": "web",
            "text": " ".join(current_text[:6])[:300],
            "fish": current_fish or "不明", "catch": current_catch,
            "source_url": url
        })
    return results


def scrape_esamasa():
    """えさ政釣船店 (esamasa.com) 公式釣果ページ - 中山丸と同じシステム"""
    html = safe_get("https://www.esamasa.com/category/Choka/")
    if not html: return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, "えさ政（羽田）", "https://www.esamasa.com/category/Choka/")

def scrape_tsurikou():
    """つり幸 - 公式サイト(釣果) + 釣りビジョン ID:684"""
    results = []
    # 公式サイトのトップに釣果あり
    html = safe_get("https://www.tsurikou.com/")
    if html:
        soup = BeautifulSoup(html, "lxml")
        lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
        results.extend(parse_lines(lines, "つり幸（川崎）", "https://www.tsurikou.com/"))
    # 釣りビジョンからも補完
    results.extend(scrape_fishing_v(684, "つり幸（川崎）"))
    return results

def scrape_yoshikyu():
    """吉久（浦安）公式サイト"""
    html = safe_get("https://www.yoshikyu.com/")
    if not html: return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, "吉久（浦安）", "https://www.yoshikyu.com/")

def scrape_fishing_v(ship_id, ship_name):
    url = f"https://www.fishing-v.jp/choka/choka_detail.php?s={ship_id}&pageID=1"
    html = safe_get(url)
    if not html: return []
    soup = BeautifulSoup(html, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
    return parse_lines(lines, ship_name, url)

# ── メイン ───────────────────────────────────────────

def main():
    print(f"スクレイピング開始: {TODAY.strftime('%Y-%m-%d %H:%M JST')}")
    print(f"取得対象: {CUTOFF} 以降\n")

    all_comments = []
    errors = []

    tasks = [
        # ── 金沢八景エリア ──
        ("一之瀬丸",       scrape_ichinosemaru, []),
        ("一之瀬丸",       scrape_chowari,      ["00307", "一之瀬丸"]),
        ("忠彦丸",         scrape_tadahikomaru, []),
        ("忠彦丸",         scrape_chowari,      ["00703", "忠彦丸"]),
        ("弁天屋",         scrape_chowari,      ["00300", "弁天屋"]),
        ("弁天屋",         scrape_fishing_v,    [190, "弁天屋"]),
        ("小川丸",         scrape_chowari,      ["00458", "小川丸"]),
        ("荒川屋",         scrape_chowari,      ["00007", "荒川屋"]),
        # ── 川崎エリア ──
        ("中山丸",         scrape_nakayamamaru, []),
        ("つり幸（川崎）", scrape_tsurikou,     []),
        # ── 深川エリア ──
        ("深川吉野屋",     scrape_yoshinoya,    []),
        # ── 浦安エリア ──
        ("吉野屋（浦安）", scrape_fishing_v,    [146, "吉野屋（浦安）"]),
        ("吉久（浦安）",   scrape_yoshikyu,     []),
        ("岩田屋（浦安）", scrape_chowari,      ["00847", "岩田屋（浦安）"]),
        # ── 羽田・品川エリア ──
        ("えさ政（羽田）", scrape_esamasa,      []),
        ("えさ政（羽田）", scrape_chowari,      ["00322", "えさ政（羽田）"]),
        ("船宿いわた",     scrape_fishing_v,    [1169, "船宿いわた"]),
        ("船宿いわた",     scrape_chowari,      ["01322", "船宿いわた"]),
        ("釣り船鶴",       scrape_chowari,      ["01678", "釣り船鶴"]),
        # ── 市川・江戸川エリア ──
        ("林遊船",         scrape_chowari,      ["00880", "林遊船"]),
        ("教至丸",         scrape_chowari,      ["00845", "教至丸"]),
    ]

    for ship_name, func, args in tasks:
        print(f"  {ship_name}...")
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

    os.makedirs("data", exist_ok=True)
    output = {
        "updated_at": TODAY.strftime("%Y-%m-%d %H:%M JST"),
        "cutoff": CUTOFF,
        "count": len(unique),
        "errors": errors,
        "comments": unique
    }
    with open("data/comments.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n完了: {len(unique)}件 → data/comments.json")
    if errors:
        print(f"エラー: {len(errors)}件")

if __name__ == "__main__":
    main()
