import os
import sys
import json
import datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo
from html import escape
from typing import Dict, List, Optional, Any
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ====== Telegram ======
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

TZ = ZoneInfo("Europe/Helsinki")
DATE_FMT = "%d %b %Y"

USER_AGENT = (
    "NHL-Standings-Bot/1.0 "
    "(+https://site.web.api.espn.com/apis/v2/; +https://site.api.espn.com/apis/v2/)"
)

# ====== Русские названия команд (ESPN аббревиатуры) ======
RU_BY_ABBR: Dict[str, str] = {
    # Atlantic
    "BOS": "Бостон Брюинз",
    "BUF": "Баффало Сэйбрз",
    "DET": "Детройт Ред Уингз",
    "FLA": "Флорида Пантерз",
    "MTL": "Монреаль Канадиенс",
    "OTT": "Оттава Сенаторз",
    "TBL": "Тампа-Бэй Лайтнинг",
    "TOR": "Торонто Мэйпл Лифс",
    # Metropolitan
    "CAR": "Каролина Харрикейнз",
    "CBJ": "Коламбус Блю Джекетс",
    "NJD": "Нью-Джерси Девилз",
    "NYI": "Нью-Йорк Айлендерс",
    "NYR": "Нью-Йорк Рейнджерс",
    "PHI": "Филадельфия Флайерз",
    "PIT": "Питтсбург Пингвинз",
    "WSH": "Вашингтон Кэпиталз",
    # Central
    "CHI": "Чикаго Блэкхокс",
    "COL": "Колорадо Эвеланш",
    "DAL": "Даллас Старз",
    "MIN": "Миннесота Уайлд",
    "NSH": "Нэшвилл Предаторз",
    "STL": "Сент-Луис Блюз",
    "WPG": "Виннипег Джетс",
    # Utah (разные варианты у ESPN встречаются)
    "UTH": "Юта Маммотс",
    "UTA": "Юта Маммотс",
    "UTAH": "Юта Маммотс",
    "UHC": "Юта Маммотс",
    # Pacific
    "ANA": "Анахайм Дакс",
    "CGY": "Калгари Флэймз",
    "EDM": "Эдмонтон Ойлерз",
    "LAK": "Лос-Анджелес Кингз",
    "SEA": "Сиэтл Кракен",
    "SJS": "Сан-Хосе Шаркс",
    "VAN": "Ванкувер Кэнакс",
    "VGK": "Вегас Голден Найтс",
}

# иногда ESPN может прислать короткие обозначения
VARIANT_TO_ESPN_ABBR = {
    "TB": "TBL",
    "LA": "LAK",
}

DIV_RU = {
    "Atlantic": "Атлантический дивизион",
    "Metropolitan": "Столичный дивизион",
    "Central": "Центральный дивизион",
    "Pacific": "Тихоокеанский дивизион",
}

CONF_DIV_ORDER = {
    "east": ["Atlantic", "Metropolitan"],
    "west": ["Central", "Pacific"],
}

# ====== путь для хранения «вчерашних» позиций ======
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
PREV_FILE = DATA_DIR / "nhl_prev_positions.json"

# ====== HTTP с ретраями ======
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": USER_AGENT})
    return s

SESSION = make_session()

# ====== утилиты ======
def normalize_abbr(abbr: str) -> str:
    a = (abbr or "").upper()
    return VARIANT_TO_ESPN_ABBR.get(a, a)

def arrow(delta_places: Optional[int]) -> str:
    if delta_places is None:
        return "⚪︎="
    if delta_places > 0:
        return f"🟢▲+{delta_places}"
    if delta_places < 0:
        return f"🔴▼{abs(delta_places)}"
    return "⚪︎="

def _get_json(url: str, params: dict | None = None) -> dict:
    try:
        r = SESSION.get(url, params=params or {}, timeout=30)
        if r.status_code != 200:
            return {}
        return r.json()
    except Exception:
        return {}

def load_prev_positions() -> Dict[str, Dict[str, Dict[str, int]]]:
    """
    {"date":"YYYY-MM-DD",
     "divisions": {
        "Atlantic": {"BOS":1,...},
        "Metropolitan": {...},
        "Central": {...},
        "Pacific": {...}
     }}
    """
    if not PREV_FILE.exists():
        return {"date": "", "divisions": {}}
    try:
        with PREV_FILE.open("r", encoding="utf-8") as f:
            j = json.load(f)
        return {
            "date": j.get("date") or "",
            "divisions": j.get("divisions") or {}
        }
    except Exception:
        return {"date": "", "divisions": {}}

def save_current_as_prev(today: dt.date, by_division: Dict[str, List[Dict]]) -> None:
    """
    by_division: {"Atlantic":[{abbr,rank,...}], ...}
    """
    div_map: Dict[str, Dict[str, int]] = {}
    for div_name, rows in by_division.items():
        div_map[div_name] = {r["abbr"]: r["rank"] for r in rows}
    payload = {
        "date": today.isoformat(),
        "divisions": div_map
    }
    with PREV_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# ====== ESPN NHL standings (JSON) ======
def _gather_standings_nodes(node: Any, out: List[dict]) -> None:
    if isinstance(node, dict):
        st = node.get("standings")
        if isinstance(st, dict) and isinstance(st.get("entries"), list) and st["entries"]:
            out.append(node)
        for v in node.values():
            _gather_standings_nodes(v, out)
    elif isinstance(node, list):
        for v in node:
            _gather_standings_nodes(v, out)

def _stats_to_map(stats_list: List[dict]) -> Dict[str, Any]:
    m: Dict[str, Any] = {}
    for s in stats_list or []:
        name = s.get("name") or s.get("abbreviation") or s.get("shortDisplayName")
        if not name:
            continue
        m[name] = s.get("value", s.get("displayValue"))
    return m

def _entries_to_rows(entries: List[dict]) -> List[Dict]:
    rows: List[Dict] = []
    for e in entries:
        team = e.get("team") or {}
        display = team.get("displayName") or team.get("name") or ""
        abbr = normalize_abbr(team.get("abbreviation") or team.get("shortDisplayName") or display)

        stats = _stats_to_map(e.get("stats") or [])
        gp = int(stats.get("gamesPlayed") or stats.get("gp") or 0)
        w  = int(stats.get("wins") or 0)
        l  = int(stats.get("losses") or 0)
        ot = int(stats.get("otLosses") or stats.get("otl") or 0)
        pts = int(stats.get("points") or stats.get("pts") or (w*2 + ot))

        rows.append({"team": display, "abbr": abbr, "gp": gp, "w": w, "l": l, "ot": ot, "pts": pts})

    # сортируем по очкам, затем по победам
    rows.sort(key=lambda x: (-x["pts"], -x["w"], x["team"]))
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows

def fetch_nhl_standings_by_division() -> Dict[str, Dict[str, List[Dict]]]:
    """
    Возвращает:
      {
        "east": {"Atlantic":[...], "Metropolitan":[...]},
        "west": {"Central":[...], "Pacific":[...]}
      }
    """
    candidates = [
        "https://site.web.api.espn.com/apis/v2/sports/hockey/nhl/standings?region=us&lang=en&contentorigin=espn",
        "https://site.api.espn.com/apis/v2/sports/hockey/nhl/standings?region=us&lang=en",
    ]
    data = {}
    for u in candidates:
        data = _get_json(u)
        if data:
            break
    if not data:
        return {"east": {}, "west": {}}

    nodes: List[dict] = []
    _gather_standings_nodes(data, nodes)

    divisions: Dict[str, List[Dict]] = {}  # "Atlantic":[rows], ...
    def push_div(name: str, entries: List[dict]):
        lname = (name or "").lower()
        key = None
        if "atlantic" in lname:
            key = "Atlantic"
        elif "metropolitan" in lname:
            key = "Metropolitan"
        elif "central" in lname:
            key = "Central"
        elif "pacific" in lname:
            key = "Pacific"
        if key:
            divisions[key] = _entries_to_rows(entries)

    # соберём из всех узлов
    for n in nodes:
        name = n.get("name") or n.get("shortName") or n.get("abbreviation") or ""
        st = n.get("standings") or {}
        entries = st.get("entries") or []
        if entries:
            push_div(name, entries)

    # расклад по конференциям
    east = {k: divisions.get(k, []) for k in ("Atlantic", "Metropolitan")}
    west = {k: divisions.get(k, []) for k in ("Central", "Pacific")}
    return {"east": east, "west": west}

# ====== тренд внутри дивизионов ======
def attach_trend_div(rows: List[Dict], y_positions: Dict[str, int]) -> List[Dict]:
    ranked = sorted(rows, key=lambda x: (-x["pts"], -x["w"], x["team"]))
    for i, r in enumerate(ranked, 1):
        r["rank"] = i
        y = y_positions.get(r["abbr"])
        r["delta_places"] = None if y is None else (y - i)
    return ranked

# ====== форматирование ======
_TAG_RE = re.compile(r"<[^>]+>")

def fmt_division(title: str, rows: List[Dict]) -> str:
    """
    Пример строки:
      1  🟢▲+1  Бостон Брюинз  6  4-1-1  9
    После 3-го места — разделитель '-------'
    """
    out = [f"<b>{escape(title)}</b>"]
    for r in rows:
        line = (
            f"{r['rank']:>2} {arrow(r.get('delta_places')):>4}  "
            f"{escape(RU_BY_ABBR.get(r['abbr'], r['team']))}  "
            f"{r['gp']:>2}  {r['w']}-{r['l']}-{r['ot']}  {r['pts']}"
        )
        out.append(line)
        if r["rank"] == 3:
            out.append("-------")
    return "\n".join(out)

# ====== сообщение и отправка ======
def build_message() -> str:
    today = dt.datetime.now(tz=TZ).date()

    cur = fetch_nhl_standings_by_division()
    prev = load_prev_positions()

    # тренд по каждому дивизиону
    east_divs = {}
    for d in CONF_DIV_ORDER["east"]:
        east_divs[d] = attach_trend_div(cur["east"].get(d, []), (prev["divisions"].get(d) or {}))
    west_divs = {}
    for d in CONF_DIV_ORDER["west"]:
        west_divs[d] = attach_trend_div(cur["west"].get(d, []), (prev["divisions"].get(d) or {}))

    # сохранение «сегодня» как «вчера» на следующий запуск
    all_divs = {**east_divs, **west_divs}
    save_current_as_prev(today, all_divs)

    head = f"<b>НХЛ · Таблица по дивизионам</b> — {today.strftime(DATE_FMT)}"
    info = "ℹ️ Источник: ESPN JSON. Сравнение — с предыдущего поста (локальный файл)."

    east_block = "\n\n".join([
        fmt_division(f"Восток — {DIV_RU['Atlantic']}", east_divs["Atlantic"]),
        fmt_division(f"Восток — {DIV_RU['Metropolitan']}", east_divs["Metropolitan"]),
    ])
    west_block = "\n\n".join([
        fmt_division(f"Запад — {DIV_RU['Central']}", west_divs["Central"]),
        fmt_division(f"Запад — {DIV_RU['Pacific']}", west_divs["Pacific"]),
    ])

    return "\n\n".join([head, east_block, "", west_block, "", info])

def send_telegram(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        print("No TELEGRAM_BOT_TOKEN/CHAT_ID in env", file=sys.stderr)
        return
    r = SESSION.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
        timeout=25
    )
    r.raise_for_status()

if __name__ == "__main__":
    try:
        msg = build_message()
        send_telegram(msg)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
