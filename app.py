import logging
from logging.handlers import TimedRotatingFileHandler
import threading, tempfile, time
from datetime import datetime
import os
import re
import json
import base64
import markdown
import smtplib
import requests
from requests.exceptions import HTTPError
from urllib.parse import quote
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from flask import Flask, request
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Set up logging
log_directory = 'A:/notifierr/log'
log_filename = os.path.join(log_directory, 'jellyfin_telegram-notifier.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure the log directory exists
os.makedirs(log_directory, exist_ok=True)

# Create a handler for rotating log files daily
rotating_handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=7)
rotating_handler.setLevel(logging.INFO)
rotating_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add the rotating handler to the logger
logging.getLogger().addHandler(rotating_handler)

# Базовая директория для JSON-состояний (рядом с логами/уведомлениями)
state_directory = 'A:/notifierr'
os.makedirs(state_directory, exist_ok=True)

# Полный путь к season_counts.json (задаётся в коде, без переменных среды)
SEASON_COUNTS_FILE = os.path.join(state_directory, 'season_counts.json')


# Constants
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
JELLYFIN_BASE_URL = os.environ["JELLYFIN_BASE_URL"]
JELLYFIN_API_KEY = os.environ["JELLYFIN_API_KEY"]
MDBLIST_API_KEY = os.environ["MDBLIST_API_KEY"]
TMDB_API_KEY = os.environ["TMDB_API_KEY"]
TMDB_V3_BASE = "https://api.themoviedb.org/3"
TMDB_TRAILER_LANG = os.getenv("TMDB_TRAILER_LANG", "en-US")  # пример: ru-RU, sv-SE, en-US
INCLUDE_MEDIA_TECH_INFO = os.getenv("INCLUDE_MEDIA_TECH_INFO", "true").strip().lower() in ("1","true","yes","y","on")
EPISODE_MSG_MIN_GAP_SEC = int(os.getenv("EPISODE_MSG_MIN_GAP_SEC", "0"))  # анти-спам: минимум N секунд между сообщениями по сезону
JELLYFIN_USER_ID = os.getenv("JELLYFIN_USER_ID")  # опционально; если не задан, определим автоматически по токену
LANGUAGE = os.getenv("LANGUAGE", "ru").lower()

# --- RADARR quality-upgrade tracking ---
RADARR_ENABLED = os.getenv("RADARR_ENABLED", "1").lower() in ("1","true","yes","on")
RADARR_WEBHOOK_SECRET = os.getenv("RADARR_WEBHOOK_SECRET", "").strip()  # опционально; передаём ?secret=...
RADARR_PENDING_FILE = os.getenv("RADARR_PENDING_FILE", os.path.join(state_directory, "radarr_pending.json"))
RADARR_RECHECK_AFTER_SEC = int(os.getenv("RADARR_RECHECK_AFTER_SEC", "120"))  # через сколько проверить (по умолчанию 5 мин)
RADARR_SCAN_PERIOD_SEC = int(os.getenv("RADARR_SCAN_PERIOD_SEC", "60"))      # период тика воркера

RADARR_JSON_LOCK = threading.Lock()
# Если вдруг у Radarr нет tmdbId, можно разрешить фолбэк на IMDb:
RADARR_USE_IMDB_FALLBACK = os.getenv("RADARR_USE_IMDB_FALLBACK", "1").lower() in ("1","true","yes","on")

# --- SONARR quality-upgrade tracking ---
SONARR_ENABLED = os.getenv("SONARR_ENABLED", "1").lower() in ("1","true","yes","on")
SONARR_WEBHOOK_SECRET = os.getenv("SONARR_WEBHOOK_SECRET", "").strip()  # опционально (?secret=...)
SONARR_PENDING_FILE = os.getenv("SONARR_PENDING_FILE", os.path.join(state_directory, "sonarr_pending.json"))
SONARR_RECHECK_AFTER_SEC = int(os.getenv("SONARR_RECHECK_AFTER_SEC", "300"))  # интервал переопроса
SONARR_SCAN_PERIOD_SEC  = int(os.getenv("SONARR_SCAN_PERIOD_SEC",  "15"))    # период тика воркера

# — Автозаполнение season_counts.json при старте —
SEASON_COUNTS_PRIME_ON_START = os.getenv("SEASON_COUNTS_PRIME_ON_START", "1").lower() in ("1","true","yes","on")
SEASON_COUNTS_PRIME_PAGE_SIZE = int(os.getenv("SEASON_COUNTS_PRIME_PAGE_SIZE", "100"))  # пачка сериалов за проход
SEASON_COUNTS_PRIME_SAVE_SEC  = float(os.getenv("SEASON_COUNTS_PRIME_SAVE_SEC", "3"))   # как часто сохранять json в процессе




# ----- Multi-messenger (optional) -----
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
SLACK_BOT_TOKEN     = os.getenv("SLACK_BOT_TOKEN", "").strip()
SLACK_CHANNEL_ID    = os.getenv("SLACK_CHANNEL_ID", "").strip()
GOTIFY_URL          = os.getenv("GOTIFY_URL", "").strip()
GOTIFY_TOKEN        = os.getenv("GOTIFY_TOKEN", "").strip()

# ----- Email (optional) -----
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
SMTP_FROM = os.getenv("SMTP_FROM", "").strip()
SMTP_TO   = os.getenv("SMTP_TO", "").strip()  # comma/space separated list
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "1").lower() not in ("0", "false", "")
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "0").lower() in ("1", "true")
SMTP_SUBJECT = "Новый релиз в Jellyfin"

# --- Reddit ---
REDDIT_ENABLED     = os.getenv("REDDIT_ENABLED", "1").lower() in ("1","true","yes","on")
REDDIT_APP_ID      = os.getenv("REDDIT_APP_ID", "")
REDDIT_APP_SECRET  = os.getenv("REDDIT_APP_SECRET", "")
REDDIT_USERNAME    = os.getenv("REDDIT_USERNAME", "")
REDDIT_PASSWORD    = os.getenv("REDDIT_PASSWORD", "")
REDDIT_SUBREDDIT   = os.getenv("REDDIT_SUBREDDIT", "MySubJellynotify")     # без /r/
REDDIT_USER_AGENT  = os.getenv("REDDIT_USER_AGENT", "jellyfin-bot/1.0 (by u/your_username)")
# опционально
REDDIT_SEND_REPLIES = os.getenv("REDDIT_SEND_REPLIES", "1").lower() in ("1","true","yes","on")
REDDIT_SPOILER      = os.getenv("REDDIT_SPOILER", "0").lower() in ("1","true","yes","on")
REDDIT_NSFW         = os.getenv("REDDIT_NSFW", "0").lower() in ("1","true","yes","on")
# --- Reddit post mode ---
# 1 = как сейчас: пост-ссылка (картинка), а описание — отдельным комментарием
# 0 = старый вариант: self-post, сверху ссылка на постер, ниже описание в том же посте
REDDIT_SPLIT_TO_COMMENT = os.getenv("REDDIT_SPLIT_TO_COMMENT", "1").lower() in ("1","true","yes","on")

# Whatsapp
WHATSAPP_API_URL = os.environ.get("WHATSAPP_API_URL", "").rstrip("/")
WHATSAPP_NUMBER = os.environ.get("WHATSAPP_NUMBER", "")
WHATSAPP_JID = os.environ.get("WHATSAPP_JID", "")
WHATSAPP_GROUP_JID = os.environ.get("WHATSAPP_GROUP_JID", "")
WHATSAPP_API_USERNAME = os.environ.get("WHATSAPP_API_USERNAME", "")
WHATSAPP_API_PWD = os.environ.get("WHATSAPP_API_PWD", "")
WHATSAPP_IMAGE_RETRY_ATTEMPTS = int(os.getenv("WHATSAPP_IMAGE_RETRY_ATTEMPTS", "3"))
WHATSAPP_IMAGE_RETRY_DELAY_SEC = int(os.getenv("WHATSAPP_IMAGE_RETRY_DELAY_SEC", "2"))

#Signal
SIGNAL_URL = os.environ.get("SIGNAL_URL", "").rstrip("/")
SIGNAL_NUMBER = os.environ.get("SIGNAL_NUMBER", "")
SIGNAL_RECIPIENTS = os.environ.get("SIGNAL_RECIPIENTS", "")

# --- Pushover ---
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY", "")  # ваш user/group key
PUSHOVER_TOKEN    = os.getenv("PUSHOVER_TOKEN", "")     # ваш app token
PUSHOVER_SOUND    = os.getenv("PUSHOVER_SOUND", "")     # опц.: имя звука (см. API sounds)
PUSHOVER_DEVICE   = os.getenv("PUSHOVER_DEVICE", "")    # опц.: конкретное устройство
PUSHOVER_PRIORITY = int(os.getenv("PUSHOVER_PRIORITY", "0"))  # -2..2
PUSHOVER_HTML     = os.getenv("PUSHOVER_HTML", "0").lower() in ("1","true","yes","on")

# если будете использовать экстренный приоритет (2)
PUSHOVER_EMERGENCY_RETRY  = int(os.getenv("PUSHOVER_EMERGENCY_RETRY",  "60"))   # >= 30 сек
PUSHOVER_EMERGENCY_EXPIRE = int(os.getenv("PUSHOVER_EMERGENCY_EXPIRE", "600"))  # сек
# --- Pushover retry/timing ---
PUSHOVER_TIMEOUT_SEC        = float(os.getenv("PUSHOVER_TIMEOUT_SEC", "10"))   # таймаут одного запроса
PUSHOVER_RETRIES            = int(os.getenv("PUSHOVER_RETRIES", "3"))          # сколько попыток всего
PUSHOVER_RETRY_BASE_DELAY   = float(os.getenv("PUSHOVER_RETRY_BASE_DELAY", "0.7"))  # стартовая пауза, сек
PUSHOVER_RETRY_BACKOFF      = float(os.getenv("PUSHOVER_RETRY_BACKOFF", "1.8"))     # множитель экспоненты

#matrix
MATRIX_URL = os.environ.get("MATRIX_URL", "").rstrip("/")
MATRIX_ACCESS_TOKEN = os.environ.get("MATRIX_ACCESS_TOKEN", "")
MATRIX_ROOM_ID = os.environ.get("MATRIX_ROOM_ID", "")

# --- Jellyfin: In-App сообщения (в клиент) ---
JELLYFIN_INAPP_ENABLED = os.getenv("JELLYFIN_INAPP_ENABLED", "0") == "1"
JELLYFIN_INAPP_TIMEOUT_MS = int(os.getenv("JELLYFIN_INAPP_TIMEOUT_MS", "800"))      # сколько висит поп-ап
JELLYFIN_INAPP_ACTIVE_WITHIN_SEC = int(os.getenv("JELLYFIN_INAPP_ACTIVE_WITHIN_SEC", "900"))  # «активность» сессии
JELLYFIN_INAPP_TITLE = os.getenv("JELLYFIN_INAPP_TITLE", "Jellyfin")
JELLYFIN_INAPP_FORCE_MODAL = os.getenv("JELLYFIN_INAPP_FORCE_MODAL", "1").lower() in ("1","true","yes","on")

# --- Home Assistant notifications ---
HA_BASE_URL = os.getenv("HA_BASE_URL", "").rstrip("/")          # например: http://192.168.1.10:8123
HA_TOKEN    = os.getenv("HA_TOKEN", "")                         # Long-Lived Access Token из профиля HA
HA_VERIFY_SSL = os.getenv("HA_VERIFY_SSL", "1").lower() in ("1","true","yes","on")
# Куда слать по умолчанию:
# для мобильного приложения указывайте notify/<имя_сервиса>, напр. "notify/mobile_app_m2007j20cg"
# для встроенной «постоянной» нотификации укажите "persistent_notification/create"
HA_DEFAULT_SERVICE = os.getenv("HA_DEFAULT_SERVICE", "persistent_notification/create")
# Показывать ссылку на постер в persistent_notification
HA_PN_IMAGE_LINK = os.getenv("HA_PN_IMAGE_LINK", "1").lower() in ("1","true","yes","on")
HA_PN_IMAGE_LABEL = os.getenv("HA_PN_IMAGE_LABEL", "Poster")  # Заголовок перед ссылкой

# --- Synology Chat ---
SYNOCHAT_ENABLED       = os.getenv("SYNOCHAT_ENABLED", "1").lower() in ("1","true","yes","on")
SYNOCHAT_WEBHOOK_URL   = os.getenv("SYNOCHAT_WEBHOOK_URL", "")   # полный URL из Incoming Webhook
SYNOCHAT_TIMEOUT_SEC   = float(os.getenv("SYNOCHAT_TIMEOUT_SEC", "8"))
SYNOCHAT_VERIFY_SSL    = os.getenv("SYNOCHAT_VERIFY_SSL", "1").lower() in ("1","true","yes","on")
SYNOCHAT_INCLUDE_POSTER = os.getenv("SYNOCHAT_INCLUDE_POSTER", "1").lower() in ("1","true","yes","on")
SYNOCHAT_CA_BUNDLE = os.getenv("SYNOCHAT_CA_BUNDLE", "").strip()  # путь к .pem (опционально)
SYNOCHAT_RETRIES = int(os.getenv("SYNOCHAT_RETRIES", "3"))
SYNOCHAT_RETRY_BASE_DELAY = float(os.getenv("SYNOCHAT_RETRY_BASE_DELAY", "0.8"))
SYNOCHAT_RETRY_BACKOFF = float(os.getenv("SYNOCHAT_RETRY_BACKOFF", "1.7"))

# ----- External image host (optional) -----
IMGBB_API_KEY = os.getenv("IMGBB_API_KEY", "").strip()
imgbb_upload_done = threading.Event()   # Сигнал о завершении загрузки
uploaded_image_url = None               # Здесь хранится ссылка после удачной загрузки



def fetch_mdblist_ratings(content_type: str, tmdb_id: str) -> str:
    """
    Запрос к https://api.mdblist.com/tmdb/{type}/{tmdbId}
    и формирование текста с найденными рейтингами.
    Возвращает строку вида:
      "- IMDb: 7.8\n- Rotten Tomatoes: 84%\n…"
    или пустую строку при ошибке/отсутствии данных.
    """
    url = f"https://api.mdblist.com/tmdb/{content_type}/{tmdb_id}?apikey={MDBLIST_API_KEY}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        ratings = data.get("ratings")
        if not isinstance(ratings, list):
            return ""

        lines = []
        for r in ratings:
            source = r.get("source")
            value = r.get("value")
            if source is None or value is None:
                continue
            lines.append(f"- {source}: {value}")

        return "\n".join(lines)
    except requests.RequestException as e:
        app.logger.warning(f"MDblist API error for {content_type}/{tmdb_id}: {e}")
        return ""

def send_telegram_photo(photo_id, caption):
    base_photo_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"

    # 1) Пытаемся скачать картинку у Jellyfin c api_key и таймаутом
    try:
        image_response = requests.get(
            base_photo_url,
            params={"api_key": JELLYFIN_API_KEY},
            timeout=10
        )
    except requests.RequestException as e:
        app.logger.warning(f"Failed to fetch JF image: {e}")
        image_response = None

    # 2) Если картинка есть — шлём фото, иначе — текстом
    tg_base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    if image_response is not None and image_response.ok:
        url = f"{tg_base}/sendPhoto"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "caption": caption,
            "parse_mode": "Markdown",
        }
        files = {"photo": ("photo.jpg", image_response.content, "image/jpeg")}
        response = requests.post(url, data=data, files=files, timeout=15)
    else:
        app.logger.warning("JF image not available, sending text-only message")
        url = f"{tg_base}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": caption,
            "parse_mode": "Markdown",
        }
        response = requests.post(url, data=data, timeout=15)

    return response

def send_telegram_text(text: str):
    tg_base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    return requests.post(f"{tg_base}/sendMessage", data={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }, timeout=15)

def send_telegram_photo_only(item_id: str):
    tg_base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    url = f"{JELLYFIN_BASE_URL}/Items/{item_id}/Images/Primary"
    try:
        r = requests.get(url, params={"api_key": JELLYFIN_API_KEY}, timeout=10)
        if not r.ok:
            return None
        return requests.post(f"{tg_base}/sendPhoto",
                             data={"chat_id": TELEGRAM_CHAT_ID},
                             files={"photo": ("photo.jpg", r.content, "image/jpeg")},
                             timeout=15)
    except Exception:
        return None



def get_item_details(item_id):
    headers = {'accept': 'application/json'}
    params = {'api_key': JELLYFIN_API_KEY}
    # Добавили ProviderIds и ExternalUrls — здесь будет TMDb ID
    url = (
        f"{JELLYFIN_BASE_URL}/emby/Items"
        f"?Recursive=true&Fields=DateCreated,Overview,ProviderIds,ExternalUrls,MediaStreams,MediaSources&Ids={item_id}"
    )
    response = requests.get(url, headers=headers, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def extract_tmdb_id_from_jellyfin_details(details) -> str | None:
    """
    Принимает json от get_item_details(..) и пытается вернуть TMDb ID как строку.
    Ищем в ProviderIds.Tmdb, затем пробуем извлечь из ExternalUrls (TheMovieDb/TMDB).
    """
    try:
        items = details.get("Items") or []
        if not items:
            return None
        item = items[0]

        provider_ids = item.get("ProviderIds") or {}
        # Наиболее типичный ключ для фильмов и сериалов — "Tmdb"
        for k in ("Tmdb", "TmdbShow", "TmdbId", "TmdbCollection"):
            val = provider_ids.get(k)
            if val:
                return str(val)

        # Фолбэк: иногда есть ExternalUrls → TheMovieDb
        for ext in (item.get("ExternalUrls") or []):
            name = (ext.get("Name") or "").lower()
            if "themoviedb" in name or "tmdb" in name:
                url = ext.get("Url") or ""
                # Берём последнюю числовую часть из URL
                import re
                m = re.search(r"/(\d+)(?:\D*$)", url)
                if m:
                    return m.group(1)

        return None
    except Exception as e:
        logging.warning(f"Failed to extract TMDb ID from Jellyfin details: {e}")
        return None

#Поиск трейлеров на tmdb
def _iso639_1(lang_code: str) -> str:
    """Из 'ru-RU' -> 'ru', из 'sv-SE' -> 'sv', из 'en' -> 'en'."""
    return (lang_code or "en").split("-")[0].lower()


def _pick_best_tmdb_video(results: list, preferred_iso: str | None = None) -> str | None:
    """
    Отдаём лучшую ссылку на трейлер (YouTube приоритет).
    Приоритет: YouTube → type=Trailer → official=True → совпадение языка → самый новый.
    Возвращает https://www.youtube.com/watch?v=KEY или None.
    """
    if not results:
        return None
    preferred_iso = (preferred_iso or "en").lower()

    def score(v: dict) -> tuple:
        site = (v.get("site") or "").lower()
        vtype = (v.get("type") or "").lower()
        official = bool(v.get("official"))
        lang = (v.get("iso_639_1") or "").lower()
        # published_at может отсутствовать
        published = v.get("published_at") or v.get("publishedAt") or ""
        # Чем больше — тем лучше
        return (
            1 if site == "youtube" else 0,
            2 if vtype == "trailer" else (1 if vtype == "teaser" else 0),
            1 if official else 0,
            1 if lang == preferred_iso else 0,
            published   # строковое сравнение по ISO-датам работает адекватно
        )

    # Сортируем по приоритету и берём лучший
    best = sorted(results, key=score, reverse=True)[0]
    if (best.get("site") or "").lower() == "youtube" and best.get("key"):
        return f"https://www.youtube.com/watch?v={best['key']}"
    return None


def get_tmdb_trailer_url(media_type: str, tmdb_id: str | int, preferred_lang: str | None = None) -> str | None:
    """
    Возвращает URL трейлера с TMDB для movie/tv c фолбэком языка:
    1) preferred_lang (+ include_video_language=iso,en,null)
    2) en-US (+ include_video_language=en,null)
    3) без фильтра языка (любой доступный)
    """
    if not tmdb_id:
        return None

    media = "movie" if str(media_type).lower() == "movie" else "tv"
    url = f"{TMDB_V3_BASE}/{media}/{tmdb_id}/videos"
    pref = preferred_lang or "en-US"
    pref_iso = _iso639_1(pref)

    tries = [
        {"language": pref,   "include_video_language": f"{pref_iso},en,null"},
        {"language": "en-US","include_video_language": "en,null"},
        {}  # последний запрос — без language (возьмём всё, что есть)
    ]

    all_results = []
    for params in tries:
        params = {**params, "api_key": TMDB_API_KEY}
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json() or {}
            results = data.get("results") or []
            all_results.extend(results)
            # если именно на этом шаге уже есть «лучший» — можно вернуть сразу
            best_here = _pick_best_tmdb_video(results, preferred_iso=pref_iso)
            if best_here:
                return best_here
        except requests.RequestException as e:
            logging.warning(f"TMDB videos fetch failed ({media}/{tmdb_id}, {params}): {e}")

    # Фолбэк: попробуем выбрать лучший из суммарного списка
    return _pick_best_tmdb_video(all_results, preferred_iso=pref_iso)

# Добавление технической информации в сообщение о новом фильме
def _channels_to_layout(channels: int | None) -> str:
    if not channels:
        return "?"
    # Желаемое человекочитаемое: 2 -> 2.0, 6 -> 5.1, 8 -> 7.1
    if channels == 6:  return "5.1"
    if channels == 8:  return "7.1"
    if channels == 2:  return "2.0"
    return str(channels)

def _normalize_codec(codec: str | None) -> str:
    if not codec:
        return "?"
    c = codec.lower()
    if c in ("hevc","h265","x265"): return "HEVC (H.265)"
    if c in ("h264","avc","x264"):  return "AVC (H.264)"
    if c in ("av1",):               return "AV1"
    if c in ("vp9",):               return "VP9"
    return codec.upper()

def _sanitize_audio_display_title(title: str) -> str:
    """
    Удаляет языковые префиксы в начале строки: 'ru:', 'rus:', 'eng:', '[RU]:', 'RU -', 'ru/' и т.п.
    Оставляет остальную часть названия без изменений.
    """
    if not title:
        return ""
    import re
    t = title.strip()

    # 1) [RU]:  | (RU)  | RU:  | RU -  | RU/  | RU|
    # а также короткие/длинные коды: ru, rus, en, eng, uk, ukr, de, ger, es, spa, fr, fre, it, ita, jp, jpn, zh, chi, pt, por, pl, pol
    langs = r"(?:ru|rus|en|eng|uk|ukr|de|ger|es|spa|fr|fre|it|ita|jp|jpn|zh|chi|pt|por|pl|pol)"
    # варианты с квадратными/круглыми скобками или без, затем разделитель ':' '/' '-' '|' и пробелы
    t = re.sub(rf"^\s*(?:\[\s*{langs}\s*\]|\(\s*{langs}\s*\)|{langs})\s*[:/\-\|]\s*", "", t, flags=re.IGNORECASE)
    # случай: просто '(RU) ' в начале без разделителя
    t = re.sub(rf"^\s*\(\s*{langs}\s*\)\s*", "", t, flags=re.IGNORECASE)

    # убрать лишние пробелы
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t


def _detect_image_profile(vs: dict) -> str:
    """
    Детектирует сразу ВСЕ профили изображения из полей видео-потока:
    Dolby Vision (с указанием Profile N), HDR10+, HDR10, HLG, общий HDR или SDR.
    Возвращает строку вида: "Dolby Vision Profile 8, HDR10".
    """
    import re

    # Соберём диагностический текст из наиболее информативных полей Jellyfin
    txt_parts = []
    for k in (
        "ColorTransfer", "VideoRange", "VideoRangeType", "ColorPrimaries", "ColorSpace",
        "Hdr", "Hdr10Plus", "DolbyVision", "DoVi", "VideoDoViProfile", "DvProfile",
        "DolbyVisionProfile", "Profile"  # бывает, но осторожно используем
    ):
        v = vs.get(k)
        if isinstance(v, bool):
            v = "1" if v else "0"
        if v is not None:
            txt_parts.append(str(v))
    txt = " ".join(txt_parts).upper()

    profiles: list[str] = []

    def add(label: str):
        if label and label not in profiles:
            profiles.append(label)

    # Dolby Vision + номер профиля, если удастся извлечь
    dv_present = ("DOLBY VISION" in txt) or ("DOVI" in txt) or (re.search(r"\bDV\b", txt) is not None)
    if dv_present:
        dv_profile_num = None
        # Ищем конкретные поля с номером DV-профиля, чтобы не перепутать с AVC/HEVC Profile
        for k, v in (vs or {}).items():
            lk = k.lower()
            if ("dovi" in lk or "dolby" in lk) and "profile" in lk:
                m = re.search(r"\d+", str(v))
                if m:
                    dv_profile_num = m.group(0)
                    break
        if dv_profile_num:
            add(f"Dolby Vision Profile {dv_profile_num}")
        else:
            add("Dolby Vision")

    # Остальные HDR-профили
    if ("HDR10+" in txt) or ("HDR10PLUS" in txt) or ("HDR10 PLUS" in txt):
        add("HDR10+")
    if "HDR10" in txt:
        add("HDR10")
    if "HLG" in txt:
        add("HLG")

    # Общий HDR (если нет более конкретных и есть намёки на HDR/PQ/BT2020)
    if ("HDR" in txt or "PQ" in txt or "BT2020" in txt) and not any(
        p in profiles for p in ("HDR10+", "HDR10", "HLG") + tuple([p for p in profiles if p.startswith("Dolby Vision")])
    ):
        add("HDR")

    if not profiles:
        add("SDR")

    # Стабильный приоритет отображения
    def order_key(label: str) -> int:
        if label.startswith("Dolby Vision"):
            return 0
        return {"HDR10+": 1, "HDR10": 2, "HLG": 3, "HDR": 4, "SDR": 5}.get(label, 99)

    profiles.sort(key=order_key)
    return ", ".join(profiles)


def build_movie_media_tech_text(details_json: dict) -> str:
    """
    Собирает блок:
      *Quality:*
      - Resolution: 4K (3840×1600)
      - Video codec: HEVC (H.265)
      - Image profiles: Dolby Vision
      *Audio tracks:*
      - EAC3 5.1 (Atmos)
      - DTS-HD MA 7.1 (en)
    """
    try:
        items = details_json.get("Items") or []
        if not items:
            return ""
        item = items[0]

        # потоки могут быть прямо в Item.MediaStreams или внутри MediaSources[].MediaStreams
        streams = (item.get("MediaStreams") or [])
        if not streams:
            for ms in (item.get("MediaSources") or []):
                if ms.get("MediaStreams"):
                    streams = ms["MediaStreams"]
                    break
        if not streams:
            return ""

        # ---- Видео ----
        video_streams = [s for s in streams if (s.get("Type") or "").lower() == "video"]
        vs = video_streams[0] if video_streams else {}
        width  = vs.get("Width")
        height = vs.get("Height")

        res_label = _resolution_label(width, height)
        vcodec = _normalize_codec(vs.get("Codec"))
        img_profile = _detect_image_profile(vs)

        L = _labels()
        quality_block = (
            f"*{L['quality']}:*\n"
            f"- {L['resolution']}: {res_label}\n"
            f"- {L['video_codec']}: {vcodec}\n"
            f"- {L['image_profiles']}: {img_profile}"
        )

        # ---- Аудио ----
        audio_streams = [s for s in streams if (s.get("Type") or "").lower() == "audio"]
        if audio_streams:
            audio_lines = []
            for a in audio_streams:
                # jellyfin часто уже даёт «DisplayTitle» вида "DTS-HD MA 7.1 (eng)" и т.п.
                raw_disp = (a.get("DisplayTitle") or "").strip()
                disp = _sanitize_audio_display_title(raw_disp)
                is_atmos = a.get("IsAtmos") or ("ATMOS" in raw_disp.upper()) or ("ATMOS" in disp.upper())

                if disp:
                    line = disp
                    if is_atmos and "atmos" not in disp.lower():
                        line += " (Atmos)"
                else:
                    base = _normalize_codec(a.get("Codec"))
                    ch = _channels_to_layout(a.get("Channels"))
                    lang = a.get("Language") or "und"
                    line = f"{base} {ch} ({lang})"
                    if is_atmos:
                        line += " (Atmos)"

                audio_lines.append(f"- {line}")

            audio_block = f"*{L['audio_tracks']}:*\n" + "\n".join(
                audio_lines) if audio_streams else f"*{L['audio_tracks']}:*\n- n/a"

        return f"\n\n{quality_block}\n\n{audio_block}"
    except Exception as e:
        logging.warning(f"Failed to build media tech text: {e}")
        return ""


def _resolution_label(width: int | None, height: int | None) -> str:
    """
    Возвращает человекочитаемую метку разрешения с учётом широкоформатных кадров и
    небольших отклонений от стандартов. Примеры:
      3840x1600 -> 4K (3840×1600)
      1920x800  -> 1080p (1920×800)
      7680x4320 -> 8K (7680×4320)
    """
    if not width or not height:
        return "?"

    w, h = int(width), int(height)
    # Толеранс по «старшему» измерению ~2%
    # Используем оба измерения, чтобы корректно ловить широкоформат (3840×1600 и т.п.)
    def label():
        if w >= 7600 or h >= 4300:
            return "8K"
        # (опционально можно оставить «5K», но обычно достаточно 4K)
        if w >= 3800 or h >= 2100:
            return "4K"
        # 2K DCI (2048×1080) часто встречается; пометим отдельно
        if (2000 <= w < 2560) and (1000 <= h < 1440):
            return "2K"
        if w >= 2500 or h >= 1400:
            return "1440p"
        if w >= 1900 or h >= 1060:
            return "1080p"
        if w >= 1200 or h >= 700:
            return "720p"
        # SD варианты
        if h >= 560:
            return "576p"
        if h >= 470:
            return "480p"
        return f"{h}p"

    # знак умножения × — аккуратнее, чем "x"
    return f"{label()} ({w}×{h})"

#Добавление информации о колличестве добавлений серий (колличество из планируемых)
_season_counts_lock = threading.Lock()

def get_jellyfin_user_id() -> str | None:
    """Определяем Id пользователя для api_key (кешируем в глобальной JELLYFIN_USER_ID)."""
    global JELLYFIN_USER_ID
    if JELLYFIN_USER_ID:
        return JELLYFIN_USER_ID
    try:
        url = f"{JELLYFIN_BASE_URL}/Users/Me"
        resp = requests.get(url, params={"api_key": JELLYFIN_API_KEY}, timeout=10)
        if resp.ok:
            JELLYFIN_USER_ID = (resp.json() or {}).get("Id")
            return JELLYFIN_USER_ID
    except requests.RequestException:
        pass
    return None

def _atomic_json_write(path: str, data: dict):
    """Безопасная запись json."""
    tmp = None
    try:
        d = json.dumps(data, ensure_ascii=False, indent=2)
        fd, tmp = tempfile.mkstemp(prefix=".tmp_", suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(d)
        os.replace(tmp, path)
    finally:
        try:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def load_season_counts() -> dict:
    try:
        with open(SEASON_COUNTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_season_counts(data: dict) -> None:
    try:
        _atomic_json_write(SEASON_COUNTS_FILE, data)
    except Exception as e:
        logging.warning(f"Failed to save {SEASON_COUNTS_FILE}: {e}")

# Глобальное состояние
season_counts = load_season_counts()


def _episode_has_file(ep: dict) -> bool:
    # Признаки наличия реального файла
    if (ep.get("LocationType") or "").lower() == "filesystem":
        return True
    if ep.get("Path"):
        return True
    if ep.get("MediaSources"):
        return True
    return False

def get_season_episode_count(series_id: str, season_id: str) -> int:
    """
    Фактическое число эпизодов (только с файлами) для сезона.
    Используем серверный фильтр isMissing=false и userId, плюс локальная фильтрация.
    """
    headers = {"accept": "application/json"}
    params = {
        "api_key": JELLYFIN_API_KEY,
        "seasonId": season_id,
        "isMissing": "false",                         # просим сервер не отдавать missing
        "Fields": "Path,LocationType,MediaSources",   # чтобы можно было локально отсечь «виртуальные»
        "limit": 10000,
    }
    uid = get_jellyfin_user_id()
    if uid:
        params["userId"] = uid                        # помогает фильтрации missing на сервере
    url = f"{JELLYFIN_BASE_URL}/Shows/{series_id}/Episodes"

    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        data = r.json() or {}
        items = data.get("Items") or []
        actual = [ep for ep in items if _episode_has_file(ep)]
        return len(actual)
    except requests.RequestException as e:
        logging.warning(f"Failed to fetch episodes for season {season_id}: {e}")
        return 0



def get_tmdb_season_total_episodes(tv_tmdb_id: str | int, season_number: int, preferred_lang: str | None = None) -> int | None:
    """
    Возвращает ожидаемое общее число эпизодов в сезоне по TMDb.
    Логика фолбэка: preferred_lang → en-US → без language.
    """
    if not tv_tmdb_id or season_number is None:
        return None

    tries = []
    if preferred_lang:
        tries.append({"language": preferred_lang})
    tries.append({"language": "en-US"})
    tries.append({})  # без языка

    for params in tries:
        p = {"api_key": TMDB_API_KEY}
        p.update(params)
        url = f"{TMDB_V3_BASE}/tv/{tv_tmdb_id}/season/{int(season_number)}"
        try:
            r = requests.get(url, params=p, timeout=10)
            r.raise_for_status()
            data = r.json() or {}
            # Обычно в ответе есть массив episodes — его длина и есть «плановое» количество.
            episodes = data.get("episodes") or []
            if episodes:
                return len(episodes)
            # На некоторых ответах встречается episode_count — используем его.
            if "episode_count" in data and isinstance(data["episode_count"], int):
                return data["episode_count"]
        except requests.RequestException as e:
            logging.warning(f"TMDb season fetch failed (tv_id={tv_tmdb_id}, S{season_number}, {params}): {e}")

    return None

def extract_season_number_from_details(season_details: dict) -> int | None:
    try:
        items = season_details.get("Items") or []
        if not items:
            return None
        season_item = items[0]
        num = season_item.get("IndexNumber")
        if isinstance(num, int):
            return num
        # Фолбэк: попытка вытащить число из имени ("Season 2", "Сезон 2", "S02")
        import re
        name = (season_item.get("Name") or "")
        m = re.search(r'(\d+)', name)
        return int(m.group(1)) if m else None
    except Exception:
        return None

#Добавление технической информации для сезонов
def get_season_episodes_with_files(series_id: str, season_id: str) -> list[dict]:
    """
    Возвращает список эпизодов сезона, у которых реально есть файл,
    c включёнными MediaStreams для анализа кодеков/дорожек.
    """
    headers = {"accept": "application/json"}
    params = {
        "api_key": JELLYFIN_API_KEY,
        "seasonId": season_id,
        "isMissing": "false",
        "Fields": "Path,LocationType,MediaSources,MediaStreams,IndexNumber,PremiereDate,SortName",
        "limit": 10000,
    }
    uid = get_jellyfin_user_id()
    if uid:
        params["userId"] = uid

    url = f"{JELLYFIN_BASE_URL}/Shows/{series_id}/Episodes"
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        items = (r.json() or {}).get("Items") or []
        # оставляем только те, у кого реально есть файл
        return [ep for ep in items if _episode_has_file(ep)]
    except requests.RequestException as e:
        logging.warning(f"Failed to fetch season episodes with files: {e}")
        return []

def _audio_label_from_stream(a: dict) -> str:
    """
    Единый формат для фильмов и сериалов:
      - сначала пробуем DisplayTitle (без языкового префикса), добавим (Atmos) если нужно;
      - если DisplayTitle пустой — используем "<CODEC> <channels> (lang)" [+ (Atmos)].
    """
    raw_disp = (a.get("DisplayTitle") or "").strip()
    disp = _sanitize_audio_display_title(raw_disp)
    is_atmos = a.get("IsAtmos") or ("ATMOS" in raw_disp.upper()) or ("ATMOS" in disp.upper())

    if disp:
        if is_atmos and "atmos" not in disp.lower():
            disp += " (Atmos)"
        return disp

    base = _normalize_codec(a.get("Codec"))
    ch   = _channels_to_layout(a.get("Channels"))
    lang = a.get("Language") or "und"
    label = f"{base} {ch} ({lang})"
    if is_atmos:
        label += " (Atmos)"
    return label


def build_season_media_tech_text(series_id: str, season_id: str) -> str:
    """
    Формирует блок для сообщения эпизодов:
      *Quality (from episode 1):*
      - Resolution: 4K (3840×1600)
      - Video codec: HEVC (H.265)
      - Image profiles: Dolby Vision

      *Audio tracks (season-wide):*
      - EAC3 5.1 (ru) — 6 episodes
      - DTS-HD MA 7.1 (en) — 4 episodes
    """
    try:
        eps = get_season_episodes_with_files(series_id, season_id)
        if not eps:
            return ""

        # ----- КАЧЕСТВО ИЗ ПЕРВОЙ СЕРИИ -----
        # сортируем по номеру эпизода (IndexNumber), затем по дате
        def _ep_key(e):
            idx = e.get("IndexNumber") or 10**9
            dt  = (e.get("PremiereDate") or "9999-12-31")
            return (idx, dt)
        eps_sorted = sorted(eps, key=_ep_key)
        first = eps_sorted[0]

        # берём MediaStreams (из Episode прямо)
        streams = (first.get("MediaStreams") or [])
        if not streams:
            # фолбэк к MediaSources[].MediaStreams
            for ms in (first.get("MediaSources") or []):
                if ms.get("MediaStreams"):
                    streams = ms["MediaStreams"]
                    break

        vs = {}
        for s in streams:
            if (s.get("Type") or "").lower() == "video":
                vs = s
                break

        quality_block = ""
        if vs:
            width  = vs.get("Width")
            height = vs.get("Height")
            res_label = _resolution_label(width, height)
            vcodec = _normalize_codec(vs.get("Codec"))
            img_profile = _detect_image_profile(vs)
            L = _labels()
            quality_block = (
                f"*{L['quality']}:*\n"
                f"- {L['resolution']}: {res_label}\n"
                f"- {L['video_codec']}: {vcodec}\n"
                f"- {L['image_profiles']}: {img_profile}"
            )

        # ----- АУДИО СВОДКА ПО СЕЗОНУ -----
        # считаем, в скольких эпизодах встречается каждая уникальная дорожка (по имени),
        # при этом объединяем варианты, отличающиеся только регистром и/или лишними пробелами
        counters: dict[str, dict] = {}  # key -> {"count": int, "display": str}
        for e in eps:
            # уникальные дорожки в рамках ОДНОГО эпизода:
            ep_keys = set()
            s_all = (e.get("MediaStreams") or [])
            if not s_all:
                for ms in (e.get("MediaSources") or []):
                    if ms.get("MediaStreams"):
                        s_all = ms["MediaStreams"]
                        break
            for a in s_all:
                if (a.get("Type") or "").lower() != "audio":
                    continue
                label = _audio_label_from_stream(a)  # уже без 'ru:' и т.п.
                key = _label_key(label)
                if not key:
                    continue
                if key in ep_keys:
                    continue  # в пределах серии считаем дорожку один раз
                ep_keys.add(key)

                if key not in counters:
                    counters[key] = {"count": 1, "display": label}
                else:
                    counters[key]["count"] += 1

        audio_block = ""
        if counters:
            # сортируем по убыванию встречаемости, затем по «красивому» названию (без учёта регистра)
            items = sorted(
                counters.values(),
                key=lambda v: (-v["count"], v["display"].casefold())
            )
            lines = [f"- {v['display']} — {v['count']} {L['episodes_word']}" for v in items]
            audio_block = f"*{L['audio_tracks']}:*\n" + "\n".join(lines)


        # собрать общий текст
        parts = []
        if quality_block:
            parts.append(quality_block)
        if audio_block:
            parts.append(audio_block)
        if not parts:
            return ""
        return "\n\n" + "\n\n".join(parts)
    except Exception as e:
        logging.warning(f"Failed to build season media tech text: {e}")
        return ""

def _label_key(s: str) -> str:
    """
    Нормализует название для сравнения:
    - убирает лишние пробелы
    - приводит к casefold() (лучше, чем lower(), для Юникода)
    """
    if not s:
        return ""
    import re
    return re.sub(r"\s+", " ", s).strip().casefold()

#Хелперы для очистки текста

def markdown_to_pushover_html(text: str) -> str:
    """
    Конвертирует «упрощённый Markdown» ваших уведомлений в HTML,
    совместимый с Pushover (поддерживаются: <b>, <i>, <u>, <a>).
    - Ссылки [текст](url) -> <a href="url">текст</a>
    - Жирный: **…** и строка формата *…* на отдельной строке -> <b>…</b>
    - Курсив: *…* и _…_ -> <i>…</i>
    - Заголовки '# ' в начале строки -> <b>…</b>
    - Маркеры списков "- " / "* " -> "• "
    - Бэктики `…` — убираются (содержимое оставляем как есть, уже экранировано)
    - Переходы строк: \n (теги <br> Pushover не поддерживает)
    Весь неразмеченный текст HTML-экранируется.
    """
    if not text:
        return ""

    s = text.replace("\r\n", "\n").replace("\r", "\n")

    def _esc(t: str) -> str:
        return (t.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
                 .replace('"', "&quot;"))

    # 0) Экранируем всё (чтобы не ломать HTML), дальше вставляем ТОЛЬКО наши теги
    s = _esc(s)

    import re

    # 1) Ссылки: [text](https://url)
    def _link_repl(m: re.Match) -> str:
        txt = m.group(1)
        url = m.group(2)
        # эскейп для href
        url = url.replace("&", "&amp;").replace('"', "&quot;").strip()
        return f'<a href="{url}">{txt}</a>'
    s = re.sub(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", _link_repl, s)

    # 2) Жирный: **…**
    s = re.sub(r"\*\*(.+?)\*\*", lambda m: f"<b>{m.group(1)}</b>", s)

    # 3) Жирная «цельная строка» в стиле ваших заголовков: *…* на отдельной строке
    s = re.sub(r"(?m)^\*\s*(.+?)\s*\*$", lambda m: f"<b>{m.group(1)}</b>", s)

    # 4) Жирный альтернативный: __…__
    s = re.sub(r"__(.+?)__", lambda m: f"<b>{m.group(1)}</b>", s)

    # 5) Курсив: *…* (внутри строки) — после обработки «цельной строки»
    s = re.sub(r"\*(.+?)\*", lambda m: f"<i>{m.group(1)}</i>", s)

    # 6) Курсив: _…_
    s = re.sub(r"_(.+?)_", lambda m: f"<i>{m.group(1)}</i>", s)

    # 7) Заголовки: '# ' в начале строки -> <b>…</b>
    s = re.sub(r"(?m)^#\s+(.*)$", lambda m: f"<b>{m.group(1)}</b>", s)

    # 8) Маркеры списков -> буллет
    s = re.sub(r"(?m)^\s*[-*]\s+", "• ", s)

    # 9) Убрать инлайн-кодовые бэктики (содержимое уже экранировано на шаге 0)
    s = re.sub(r"`(.+?)`", r"\1", s)

    # 10) Схлопываем лишние тройные переводы в двойные (аккуратнее выглядит)
    s = re.sub(r"\n{3,}", "\n\n", s)

    return s


def clean_markdown_for_apprise(text):
    """
    Упрощает markdown-подобные уведомления для plain text и приводит ссылки к единому виду:
    - [текст](url) -> url
    - Убирает повторяющиеся подряд одинаковые url
    - Добавляет префикс '🎥 <перевод new_trailer>:' перед каждой ссылкой (без дублирования)
    - Очищает лишние пробелы по краям строк
    """
    if not text:
        return text

    # 0) Получаем локализованную метку для "Трейлер"
    try:
        trailer_label = t("new_trailer")
    except Exception:
        trailer_label = MESSAGES.get(LANGUAGE, {}).get("new_trailer", "Trailer")
    if not trailer_label:
        trailer_label = "Trailer"
    # 1) [текст](url) -> url
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\s)]+)\)', r'\2', text)

    # 2) Убираем подряд идущие повторы одного и того же URL
    text = re.sub(r'(https?://\S+)(\s*\1)+', r'\1', text)

    # 3) Сначала убираем уже проставленные префиксы, чтобы не получить дубликаты,
    #    затем добавим их единообразно
    prefix_pattern = rf'🎥\s*{re.escape(trailer_label)}[:]?\s*'
    text = re.sub(rf'{prefix_pattern}(https?://\S+)', r'\1', text)

    # 4) Префиксуем ТОЛЬКО не-musicbrainz ссылки (через колбэк)
    def _prefix_non_mb(m):
        url = m.group(1)
        if re.search(r'https?://(?:[^/\s)]+\.)*musicbrainz\.org(?=[/\s)]|$)', url, re.IGNORECASE):
            return url
        return f'🎥 {trailer_label}: {url}'

    text = re.sub(r'(https?://\S+)', _prefix_non_mb, text)
    # 5) Чистим лишние пробелы по краям строк (сохраняем переносы)
    text = '\n'.join(line.strip() for line in text.split('\n'))

    # Убрать *жирный* и _курсив_
    text = re.sub(r'(\*|_){1,3}(.+?)\1{1,3}', r'\2', text)

    return text

def sanitize_whatsapp_text(text: str) -> str:
    if not text:
        return text

    # Берём язык из переменной окружения
    trailer_label = MESSAGES.get(LANGUAGE, {}).get("new_trailer")

    # 1) Превращаем [любой текст](https://...) в просто https://...
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\)]+)\)', r'\2', text)

    # 2) Убираем подряд идущие повторы одного и того же URL
    text = re.sub(r'(https?://\S+)(\s*\1)+', r'\1', text)


    # 3) Сносим уже поставленные префиксы (на всякий)
    prefix_re = rf'🎥\s*{re.escape(trailer_label)}:?[\s]*'
    text = re.sub(rf'{prefix_re}(https?://\S+)', r'\1', text)

    # 4) Префиксуем ТОЛЬКО не-musicbrainz ссылки (через колбэк)
    def _prefix_non_mb(m):
        url = m.group(1)
        if re.search(r'https?://(?:[^/\s)]+\.)*musicbrainz\.org(?=[/\s)]|$)', url, re.IGNORECASE):
            return url
        return f'🎥 {trailer_label} {url}'

    text = re.sub(r'(https?://\S+)', _prefix_non_mb, text)

    # 5) Чистим лишние пробелы
    text = re.sub(r'[ \t]+', ' ', text).strip()

    return text

def _split_caption_for_reddit(caption: str) -> tuple[str, str]:
    """
    Возвращает (title, body_md) для Reddit:
      - title: первая жирная строка (*...*) — «шапка» (например, New Movie Added)
      - body_md: caption БЕЗ «шапки». Начинается с второй жирной строки (название), затем текст.
    Если «шапки» нет — title='Jellyfin', body=исходный caption.
    """
    import re
    caption = (caption or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = caption.split("\n")

    # найти первую жирную строку (*...*)
    header = None
    hdr_idx = None
    for i, ln in enumerate(lines):
        m = re.fullmatch(r"\*\s*(.+?)\s*\*", ln.strip())
        if m:
            header = m.group(1).strip()
            hdr_idx = i
            break

    if header is None:
        return "Jellyfin", caption

    # тело = всё, кроме первой жирной строки (шапки)
    body = "\n".join(lines[:hdr_idx] + lines[hdr_idx+1:])
    # подчистим ведущие пустые строки
    while body.startswith("\n"):
        body = body[1:]
    while body.startswith("\n\n"):
        body = body[2:]
    return header or "Jellyfin", body.strip()

def jellyfin_image_exists(item_id: str, timeout: float = 5.0) -> bool:
    """Проверяем наличие Primary-постера в Jellyfin (упрощённо)."""
    try:
        url = f"{JELLYFIN_BASE_URL}/Items/{item_id}/Images/Primary"
        r = requests.get(url, params={"api_key": JELLYFIN_API_KEY}, timeout=timeout)
        return r.ok and (r.content is not None) and len(r.content) > 0
    except Exception:
        return False

def _extract_bold_line(line: str) -> str | None:
    m = re.fullmatch(r"\*\s*(.+?)\s*\*", (line or "").strip())
    return m.group(1).strip() if m else None

def make_jf_inapp_payload_from_caption(caption: str) -> tuple[str, str]:
    """
    Из Markdown-сообщения собирает:
      header -> первая жирная строка (*...*)
      title  -> вторая жирная строка (*...*)
      overview -> все строки после title до следующей жирной секции/конца
    Возвращает (header, text) где text = "title\\n\\noverview" (без Markdown).
    Если чего-то нет — gracefully деградируем.
    """
    caption = caption or ""
    lines = caption.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    # 1) найти header
    i = 0
    while i < len(lines) and not lines[i].strip():
        i += 1
    header = _extract_bold_line(lines[i]) if i < len(lines) else None
    if header is None:
        # нет жирной строки — берём первый непустой как "title", а header — дефолт
        first_non_empty = next((ln for ln in lines if ln.strip()), "")
        title_plain = clean_markdown_for_apprise(first_non_empty)
        header_plain = "Jellyfin"
        return header_plain, title_plain

    i += 1
    while i < len(lines) and not lines[i].strip():
        i += 1

    # 2) найти title (вторая жирная строка)
    title_md = _extract_bold_line(lines[i]) if i < len(lines) else None
    i += 1 if title_md is not None else 0

    # 3) собрать overview до следующей жирной секции
    overview_parts = []
    while i < len(lines):
        ln = lines[i]
        if _extract_bold_line(ln) is not None:
            break  # началась следующая секция (*...*)
        overview_parts.append(ln)
        i += 1

    # 4) очистить Markdown → plain
    header_plain = clean_markdown_for_apprise(header)
    title_plain  = clean_markdown_for_apprise(title_md) if title_md else ""
    overview_plain = clean_markdown_for_apprise("\n".join(overview_parts)).strip()

    # Итоговый текст для Jellyfin: только название и описание
    text = title_plain if title_plain else ""
    if overview_plain:
        text = (text + ("\n\n" if text else "")) + overview_plain

    # Fallback, если вдруг всё пусто
    if not text:
        text = clean_markdown_for_apprise(caption)[:500]

    return header_plain or "Jellyfin", text

#Загрузка изображения imgbb

def upload_image_to_imgbb(image_bytes):
    """
    Загружает изображение на imgbb.com (до 3 попыток) и устанавливает событие по завершении.
    """
    global uploaded_image_url
    uploaded_image_url = None
    imgbb_upload_done.clear()  # Сброс события

    # Проверка наличия ключа API
    if not IMGBB_API_KEY:
        logging.debug("IMGBB_API_KEY не задан — пропускаем загрузку на imgbb.")
        imgbb_upload_done.set()  # Сигнал о завершении (пропуск загрузки)
        return None

    url = "https://api.imgbb.com/1/upload"
    payload = {
        "key": IMGBB_API_KEY,
        "image": base64.b64encode(image_bytes).decode('utf-8')
    }

    for attempt in range(1, 4):
        try:
            logging.info(f"Попытка загрузки на imgbb #{attempt}")
            response = requests.post(url, data=payload, timeout=20)
            response.raise_for_status()
            data = response.json()
            uploaded_image_url = data['data']['url']
            logging.info(f"Изображение успешно загружено на imgbb: {uploaded_image_url}")
            break
        except Exception as ex:
            logging.warning(f"Ошибка загрузки на imgbb (попытка {attempt}): {ex}")
            if attempt < 3:
                time.sleep(2)  # Пауза между попытками

    imgbb_upload_done.set()  # Сигнал, что загрузка завершена (успешно или нет)
    return uploaded_image_url

def wait_for_imgbb_upload(timeout: float | None = 10.0):
    """
    Ждать завершения загрузки на imgbb ограниченное время.
    Возвращает URL или None по таймауту/ошибке.
    """
    signaled = imgbb_upload_done.wait(timeout=timeout if timeout is not None else None)
    if not signaled:
        logging.warning("IMGBB wait timed out; continue without image.")
    return uploaded_image_url


def get_jellyfin_image_and_upload_imgbb(photo_id):
    jellyfin_image_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    try:
        resp = requests.get(jellyfin_image_url, timeout=10)
        resp.raise_for_status()
        return upload_image_to_imgbb(resp.content)
    except Exception as ex:
        logging.warning(f"Ошибка скачивания из Jellyfin: {ex}")
        # ВАЖНО: разблокировать потенциальных ожидателей imgbb
        try:
            imgbb_upload_done.set()
        except Exception:
            pass
        return None

#Discord
def send_discord_message(photo_id, message, title="Jellyfin", uploaded_url=None):
    """
    Отправляет уведомление в Discord через Webhook.
    Картинку берём НАПРЯМУЮ из Jellyfin и прикрепляем как файл.
    Embed ссылается на неё через attachment://filename.
    """
    if not DISCORD_WEBHOOK_URL:
        logging.warning("DISCORD_WEBHOOK_URL not set, skipping Discord notification.")
        return None

    # 1) тянем постер из Jellyfin
    jellyfin_image_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    image_bytes = None
    filename = "poster.jpg"
    mimetype = "image/jpeg"
    try:
        r = requests.get(jellyfin_image_url, timeout=30)
        r.raise_for_status()
        image_bytes = r.content
        ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip().lower()
        if "png" in ct:
            filename, mimetype = "poster.png", "image/png"
        elif "webp" in ct:
            filename, mimetype = "poster.webp", "image/webp"
    except Exception as ex:
        logging.warning(f"Discord: failed to fetch image from Jellyfin: {ex}")

    # 2) готовим payload
    payload = {
        "username": title,
        "content": message
    }

    # если есть картинка — добавим embed, указывающий на attachment
    if image_bytes:
        payload["embeds"] = [{
            "image": {"url": f"attachment://{filename}"}
        }]

    try:
        if image_bytes:
            # multipart: payload_json + файл
            files = {
                "file": (filename, image_bytes, mimetype)
            }
            resp = requests.post(
                DISCORD_WEBHOOK_URL,
                data={"payload_json": json.dumps(payload, ensure_ascii=False)},
                files=files,
                timeout=30
            )
        else:
            # без картинки — обычный JSON
            resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=30)

        resp.raise_for_status()
        logging.info("Discord notification sent successfully")
        return resp
    except Exception as ex:
        logging.warning(f"Error sending to Discord: {ex}")
        return None

#Slack
def _slack_try_join_channel(channel_id: str) -> bool:
    """
    Пытается добавить бота в PUBLIC-канал (требует scope channels:join).
    Для приватных каналов не сработает — нужно вручную /invite в Slack.
    """
    if not (SLACK_BOT_TOKEN and channel_id):
        return False
    try:
        resp = requests.post(
            "https://slack.com/api/conversations.join",
            headers={
                "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"channel": channel_id},
            timeout=15,
        )
        data = resp.json()
        if not data.get("ok"):
            logging.debug(f"Slack join failed/ignored: {data.get('error')}")
            return False
        return True
    except Exception as ex:
        logging.debug(f"Slack join error: {ex}")
        return False

def send_slack_text_only(message_markdown: str) -> bool:
    """
    Фоллбэк на чат без файла. Использует chat.postMessage.
    """
    if not (SLACK_BOT_TOKEN and SLACK_CHANNEL_ID):
        logging.debug("Slack disabled/misconfigured; skip text.")
        return False

    url = "https://slack.com/api/chat.postMessage"
    # Slack понимает mrkdwn (не совсем Markdown). Можно слегка «очистить» текст:
    text_plain = sanitize_whatsapp_text(message_markdown) or ""

    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {
        "channel": SLACK_CHANNEL_ID,
        "text": text_plain,
        "mrkdwn": True,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            logging.warning(f"Slack chat.postMessage error: {data}")
            return False
        logging.info("Slack text message sent successfully")
        return True
    except Exception as ex:
        logging.warning(f"Slack text send failed: {ex}")
        return False


def send_slack_message_with_image_from_jellyfin(photo_id: str, caption_markdown: str) -> bool:
    """
    Slack: загрузка файла по новому потоку:
      1) files.getUploadURLExternal (получаем upload_url и file_id)
      2) POST байтов картинки на upload_url
      3) files.completeUploadExternal (channel_id + initial_comment)
    Фоллбэк: отправляем просто текст через chat.postMessage.
    """
    if not (SLACK_BOT_TOKEN and SLACK_CHANNEL_ID):
        logging.debug("Slack disabled/misconfigured; skip.")
        return False

    # 1) достаём картинку из Jellyfin
    img_bytes = None
    filename = "poster.jpg"
    mimetype = "image/jpeg"
    try:
        if "_fetch_jellyfin_primary" in globals():
            b, mt, fn = _fetch_jellyfin_primary(photo_id)
            img_bytes, mimetype, filename = b, mt, fn
        else:
            jf_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
            r = requests.get(jf_url, timeout=30)
            r.raise_for_status()
            img_bytes = r.content
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip().lower()
            if "png" in ct:
                filename, mimetype = "poster.png", "image/png"
            elif "webp" in ct:
                filename, mimetype = "poster.webp", "image/webp"
    except Exception as ex:
        logging.warning(f"Slack: failed to fetch image from Jellyfin: {ex}")

    if not img_bytes:
        # нет картинки — отправим текст
        return send_slack_text_only(caption_markdown)

    # 2) files.getUploadURLExternal
    auth_h = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    try:
        resp = requests.post(
            "https://slack.com/api/files.getUploadURLExternal",
            headers=auth_h,
            data={"filename": filename, "length": str(len(img_bytes))},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            logging.warning(f"Slack getUploadURLExternal error: {data}")
            return send_slack_text_only(caption_markdown)
        upload_url = data["upload_url"]
        file_id    = data["file_id"]
    except Exception as ex:
        logging.warning(f"Slack getUploadURLExternal failed: {ex}")
        return send_slack_text_only(caption_markdown)

    # 3) POST файла на upload_url
    try:
        # можно сырыми байтами:
        up_headers = {"Content-Type": mimetype}
        up = requests.post(upload_url, data=img_bytes, headers=up_headers, timeout=60)
        # альтернативно: multipart (иногда помогает при прокси):
        # up = requests.post(upload_url, files={"filename": (filename, img_bytes, mimetype)}, timeout=60)
        if up.status_code != 200:
            logging.warning(f"Slack upload_url returned {up.status_code}: {up.text[:200]}")
            return send_slack_text_only(caption_markdown)
    except Exception as ex:
        logging.warning(f"Slack raw upload failed: {ex}")
        return send_slack_text_only(caption_markdown)

    # 4) files.completeUploadExternal (шарим файл в канал + комментарий)
    def _complete_upload():
        comp_payload = {
            "files": [{"id": file_id, "title": filename}],
            "channel_id": SLACK_CHANNEL_ID,
            "initial_comment": sanitize_whatsapp_text(caption_markdown) or "",
        }
        return requests.post(
            "https://slack.com/api/files.completeUploadExternal",
            headers={**auth_h, "Content-Type": "application/json; charset=utf-8"},
            json=comp_payload,
            timeout=30,
        )

    # попытка заранее присоединиться (на случай публичного канала)
    _slack_try_join_channel(SLACK_CHANNEL_ID)

    try:
        comp = _complete_upload()
        comp.raise_for_status()
        comp_data = comp.json()
        if not comp_data.get("ok"):
            if comp_data.get("error") == "not_in_channel":
                # пробуем присоединиться и повторить один раз
                if _slack_try_join_channel(SLACK_CHANNEL_ID):
                    comp = _complete_upload()
                    comp.raise_for_status()
                    comp_data = comp.json()
                    if comp_data.get("ok"):
                        logging.info("Slack image sent successfully (after join).")
                        return True
                logging.warning("Slack: bot is not in the channel. Invite the app (/invite @Bot) and retry.")
            else:
                logging.warning(f"Slack completeUploadExternal error: {comp_data}")
            return send_slack_text_only(caption_markdown)

        logging.info("Slack image (external upload flow) sent successfully")
        return True

    except Exception as ex:
        logging.warning(f"Slack completeUploadExternal failed: {ex}")
        return send_slack_text_only(caption_markdown)

#Email
def send_email_with_image_jellyfin(item_id: str, subject: str, body_markdown: str):
    """
    Отправляет email с:
      - text/plain (plain-версия текста)
      - text/html (Markdown → HTML)
      - inline-изображением из Jellyfin (через CID)
    Возвращает True/False.
    """
    if not (SMTP_HOST and SMTP_FROM and SMTP_TO):
        logging.debug("Email disabled or misconfigured; skip.")
        return False

    # plain-версия (без форматирования) — используем ваш очиститель
    body_plain = clean_markdown_for_apprise(body_markdown or "")

    # HTML-версия — рендерим из Markdown
    # extensions для более приятных списков/переносов
    body_html_rendered = markdown.markdown(
        body_markdown or "",
        extensions=["extra", "sane_lists", "nl2br"]
    )

    # Тянем картинку из Jellyfin (с повторами)
    img_bytes = None
    img_subtype = "jpeg"
    try:
        img_bytes = _fetch_jellyfin_image_with_retries(item_id, attempts=3, timeout=10, delay=1.5)
        # subtype подберём осторожно (если есть headers в ретрае — можно хранить вместе)
        # здесь предполагаем jpeg; при желании можно расширить определение
    except Exception as ex:
        logging.warning(f"Email: failed to fetch Jellyfin image: {ex}")

    msg = EmailMessage()
    msg["Subject"] = subject or SMTP_SUBJECT
    msg["From"]    = SMTP_FROM
    recipients = [x.strip() for x in re.split(r"[,\s]+", SMTP_TO) if x.strip()]
    msg["To"]     = ", ".join(recipients)
    msg["Date"]   = formatdate(localtime=True)

    # 1) text/plain
    msg.set_content(body_plain or "")

    # 2) text/html (+ inline image при наличии)
    if img_bytes:
        cid = make_msgid()  # вида <...@domain>
        html_part = f"""\
<html>
  <body>
    <div>{body_html_rendered}</div>
    <p><img src="cid:{cid[1:-1]}" alt="poster"></p>
  </body>
</html>"""
        msg.add_alternative(html_part, subtype="html")
        try:
            # прикрепляем картинку к HTML-части как related
            msg.get_payload()[1].add_related(img_bytes, maintype="image", subtype=img_subtype, cid=cid)
        except Exception as ex:
            logging.warning(f"Email: cannot embed inline image (fallback as attachment): {ex}")
            msg.add_attachment(img_bytes, maintype="image", subtype=img_subtype, filename="poster.jpg")
    else:
        # нет картинки — просто HTML без тега <img>
        msg.add_alternative(f"<html><body>{body_html_rendered}</body></html>", subtype="html")

    # Отправка
    try:
        if SMTP_USE_SSL or SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_USER:
                    s.login(SMTP_USER, SMTP_PASS)
                s.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_USE_TLS:
                    s.starttls()
                if SMTP_USER:
                    s.login(SMTP_USER, SMTP_PASS)
                s.send_message(msg)
        logging.info("Email notification (Markdown->HTML) sent successfully")
        return True
    except Exception as ex:
        logging.warning(f"Email send failed: {ex}")
        return False

#Gotify
def send_gotify_message(item_id: str, message, title="Jellyfin", priority=5, uploaded_url=None):
    """
    Отправка в Gotify. Если картинка не готова — шлём текст без изображения.
    """
    if not GOTIFY_URL or not GOTIFY_TOKEN:
        logging.warning("GOTIFY_URL or GOTIFY_TOKEN not set, skipping Gotify notification.")
        return None

    # Если URL ещё не известен — подождём чуть-чуть, но не блокируемся надолго.
    if uploaded_url is None:
        uploaded_url = wait_for_imgbb_upload(timeout=0.5)

    if uploaded_url:
        message = f"![Poster]({uploaded_url})\n\n{message}"
        big_image_url = uploaded_url
    else:
        big_image_url = None
        logging.debug("IMGBB URL missing — sending Gotify text-only.")

    gotify_url = GOTIFY_URL.rstrip('/')
    url = f"{gotify_url}/message?token={GOTIFY_TOKEN}"

    data = {
        "title": title,
        "message": message,
        "priority": priority,
        "extras": {
            "client::display": {"contentType": "text/markdown"}
        }
    }
    if big_image_url:
        data["extras"]["client::notification"] = {"bigImageUrl": big_image_url}
    headers = {"X-Gotify-Format": "markdown"}

    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        logging.info("Gotify notification sent successfully")
        return response
    except Exception as ex:
        logging.warning(f"Error sending to Gotify: {ex}")
        return None

#Reddit
#Отправка в reddit
_reddit_oauth_cache = {"token": None, "exp": 0}

def _reddit_get_token() -> str | None:
    """
    Получить (и кэшировать) bearer-токен через password grant для script-app.
    Нужен скоуп 'submit'.
    """
    try:
        import time
        now = int(time.time())
        if _reddit_oauth_cache["token"] and now < _reddit_oauth_cache["exp"] - 20:
            return _reddit_oauth_cache["token"]

        if not all([REDDIT_APP_ID, REDDIT_APP_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD]):
            return None

        data = {
            "grant_type": "password",
            "username": REDDIT_USERNAME,
            "password": REDDIT_PASSWORD,
        }
        # Basic-авторизация client_id:client_secret + обязательный User-Agent
        r = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            data=data,
            auth=(REDDIT_APP_ID, REDDIT_APP_SECRET),
            headers={"User-Agent": REDDIT_USER_AGENT},
            timeout=12
        )
        r.raise_for_status()
        j = r.json()
        tok = j.get("access_token")
        exp = now + int(j.get("expires_in", 3600))
        if tok:
            _reddit_oauth_cache.update({"token": tok, "exp": exp})
        return tok
    except Exception as ex:
        logging.warning(f"Reddit OAuth failed: {ex}")
        return None


def send_reddit_post(title: str, body_markdown: str, external_image_url: str | None = None) -> bool:
    """
    Публикует self-post в Reddit. Если передан external_image_url,
    ставим его первой строкой (без Markdown) — Reddit обычно покажет превью.
    """
    try:
        if not (REDDIT_ENABLED and REDDIT_SUBREDDIT):
            return False

        token = _reddit_get_token()
        if not token:
            return False

        headers = {"Authorization": f"bearer {token}", "User-Agent": REDDIT_USER_AGENT}

        text = body_markdown or ""
        if external_image_url:
            url = external_image_url.strip()
            link_line = f"[Poster]({url})"
            # чтобы не дублировать, если уже вставлено
            if not (text.startswith(link_line) or text.startswith(url)):
                text = link_line + ("\n\n" if text else "") + text

        data = {
            "sr": REDDIT_SUBREDDIT,
            "kind": "self",
            "title": (title or "")[:300],
            "text": text,
            "resubmit": "true",
            "sendreplies": "true" if REDDIT_SEND_REPLIES else "false",
            "spoiler": "true" if REDDIT_SPOILER else "false",
            "nsfw": "true" if REDDIT_NSFW else "false",
            "api_type": "json",
        }

        r = requests.post("https://oauth.reddit.com/api/submit", headers=headers, data=data, timeout=20)
        if r.status_code != 200:
            logging.warning(f"Reddit submit HTTP {r.status_code}: {r.text[:300]}")
            return False

        jr = r.json().get("json", {})
        errs = jr.get("errors") or []
        if errs:
            logging.warning(f"Reddit submit errors: {errs}")
            return False

        logging.info("Reddit post submitted successfully")
        return True

    except Exception as ex:
        logging.warning(f"Reddit submit failed: {ex}")
        return False

def send_reddit_link_post_with_comment(title: str, url: str, body_markdown: str | None = None) -> bool:
    """
    Делает ссылочный пост (kind=link) с изображением-URL.
    Reddit отрисует превью/картинку. Затем добавляем комментарий с текстом.
    """
    try:
        if not (REDDIT_ENABLED and REDDIT_SUBREDDIT and url):
            return False

        token = _reddit_get_token()
        if not token:
            return False

        headers = {"Authorization": f"bearer {token}", "User-Agent": REDDIT_USER_AGENT}

        submit_data = {
            "sr": REDDIT_SUBREDDIT,
            "kind": "link",
            "title": (title or "")[:300],
            "url": url.strip(),
            "resubmit": "true",
            "sendreplies": "true" if REDDIT_SEND_REPLIES else "false",
            "spoiler": "true" if REDDIT_SPOILER else "false",
            "nsfw": "true" if REDDIT_NSFW else "false",
            "api_type": "json",
        }
        r = requests.post("https://oauth.reddit.com/api/submit", headers=headers, data=submit_data, timeout=20)
        if r.status_code != 200:
            logging.warning(f"Reddit link submit HTTP {r.status_code}: {r.text[:300]}")
            return False

        jr = r.json().get("json", {})
        errs = jr.get("errors") or []
        if errs:
            logging.warning(f"Reddit link submit errors: {errs}")
            return False

        data = jr.get("data") or {}
        thing_id = data.get("name") or (f"t3_{data.get('id')}" if data.get('id') else None)

        if thing_id and body_markdown:
            cdata = {"thing_id": thing_id, "text": body_markdown, "api_type": "json"}
            cr = requests.post("https://oauth.reddit.com/api/comment", headers=headers, data=cdata, timeout=20)
            if cr.status_code != 200:
                logging.warning(f"Reddit comment HTTP {cr.status_code}: {cr.text[:300]}")
            else:
                ce = (cr.json().get("json") or {}).get("errors") or []
                if ce:
                    logging.warning(f"Reddit comment errors: {ce}")

        logging.info("Reddit link post submitted successfully")
        return True

    except Exception as ex:
        logging.warning(f"Reddit link submit failed: {ex}")
        return False

#Whatapp
def send_whatsapp_image_via_rest(
    caption: str,
    phone_jid: str = None,
    image_url: str = None,
    view_once: bool = False,
    compress: bool = False,
    duration: int = 0,
    is_forwarded: bool = False,
):
    img_url = wait_for_imgbb_upload()
    if not img_url:
        logging.warning("Изображение не загружено — пропускаем отправку в WhatsApp.")
        return
    if not WHATSAPP_API_URL:
        logging.warning("WHATSAPP_API_URL not set, skipping WhatsApp image.")
        return None

    phone_jid = phone_jid or _wa_get_jid_from_env()
    if not phone_jid:
        logging.warning("WhatsApp JID is empty, skip sending image.")
        return None

    url = f"{WHATSAPP_API_URL.rstrip('/')}/send/image"
    auth = (WHATSAPP_API_USERNAME, WHATSAPP_API_PWD)

    form = {
        "phone": phone_jid,
        "caption": sanitize_whatsapp_text(caption or ""),
        "view_once": str(bool(view_once)).lower(),
        "compress": str(bool(compress)).lower(),
        "duration": str(int(duration)),
        "is_forwarded": str(bool(is_forwarded)).lower(),
    }

    files = None
    jellyfin_used = False

    if image_url:
        form["image_url"] = image_url
    else:
        logging.warning("WhatsApp image: image_url не задан, пропускаем отправку изображения.")
        return None

    try:
        resp = requests.post(url, data=form, files=files, auth=auth, timeout=30)
        resp.raise_for_status()
        logging.info("WhatsApp image sent successfully")
        return resp
    except requests.exceptions.RequestException as e:
        logging.warning(f"Error sending WhatsApp image: {e}")
        return None

def send_whatsapp_text_via_rest(message: str, phone_jid: str | None = None):
    """
    Шлёт ТОЛЬКО текст. Сначала /send/text, при 404 — /send/message.
    Возвращает response или None.
    """
    if not WHATSAPP_API_URL:
        logging.debug("WhatsApp API URL not set; skip text.")
        return None

    phone_jid = phone_jid or _wa_get_jid_from_env()
    if not phone_jid:
        logging.debug("WhatsApp JID empty; skip text.")
        return None

    base = WHATSAPP_API_URL.rstrip("/")
    url_text = f"{base}/send/text"
    url_msg  = f"{base}/send/message"
    auth = (WHATSAPP_API_USERNAME, WHATSAPP_API_PWD) if (WHATSAPP_API_USERNAME or WHATSAPP_API_PWD) else None

    form = {
        "phone": phone_jid,
        "message": sanitize_whatsapp_text(message or "")
    }

    try:
        r = requests.post(url_text, data=form, auth=auth, timeout=20)
        if r.status_code == 404:
            r = requests.post(url_msg, data=form, auth=auth, timeout=20)
        r.raise_for_status()
        logging.info("WhatsApp text sent successfully")
        return r
    except Exception as ex:
        logging.warning(f"WhatsApp text send failed: {ex}")
        return None

def send_whatsapp_image_with_retries(
    caption: str,
    phone_jid: str | None,
    image_url: str | None = None
) -> bool:
    """
    Пытается отправить изображение с подписью несколько раз.
    True при успехе, False если все попытки провалились.
    """
    attempts = max(1, WHATSAPP_IMAGE_RETRY_ATTEMPTS)
    delay = max(0, WHATSAPP_IMAGE_RETRY_DELAY_SEC)

    for i in range(1, attempts + 1):
        try:
            resp = send_whatsapp_image_via_rest(
                caption=caption,
                phone_jid=phone_jid,
                image_url=image_url
            )
            ok = (resp is not None) and (getattr(resp, "ok", True))
            if ok:
                logging.info(f"WhatsApp image sent on attempt {i}")
                return True
            else:
                logging.warning(f"WhatsApp image attempt {i} failed (no/negative response)")
        except Exception as ex:
            logging.warning(f"WhatsApp image attempt {i} exception: {ex}")
        if i < attempts:
            time.sleep(delay)
    return False

#Signal
def send_signal_message_with_image(photo_id, message, SIGNAL_NUMBER, SIGNAL_RECIPIENTS, api_url=SIGNAL_URL):
    """
    Отправляет текст и изображение из Jellyfin в Signal через base64_attachments.
    """
    # Скачиваем изображение из Jellyfin
    jellyfin_image_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    try:
        image_resp = requests.get(jellyfin_image_url)
        image_resp.raise_for_status()
        image_bytes = image_resp.content
        # Кодируем в base64
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")

        data = {
            "message": message,
            "number": SIGNAL_NUMBER,
            "recipients": SIGNAL_RECIPIENTS if isinstance(SIGNAL_RECIPIENTS, list) else [SIGNAL_RECIPIENTS],
            "base64_attachments": [image_b64],
        }

        resp = requests.post(api_url, json=data)
        resp.raise_for_status()
        logging.info("Signal image message sent successfully")
        return resp
    except Exception as ex:
        logging.warning(f"Error sending Signal image message: {ex}")
        return None

#pushover
def send_pushover_message(message: str,
                          title: str | None = None,
                          image_url: str | None = None,
                          image_bytes: bytes | None = None,
                          *,
                          sound: str | None = None,
                          priority: int | None = None,
                          device: str | None = None,
                          html: bool = False) -> bool:
    """
    Отправка уведомления в Pushover с ретраями на временные ошибки/таймауты.
    - Ретрай при: requests.Timeout/ConnectionError, HTTP 5xx, HTTP 429.
    - Пауза: экспоненциальная (base * backoff^(attempt-1)).
    """
    try:
        if not (PUSHOVER_USER_KEY and PUSHOVER_TOKEN):
            return False

        endpoint = "https://api.pushover.net/1/messages.json"
        data = {
            "token":   PUSHOVER_TOKEN,
            "user":    PUSHOVER_USER_KEY,
            "message": (message or "")[:1024],
        }
        if title:
            data["title"] = title[:250]
        if device:
            data["device"] = device
        if sound:
            data["sound"] = sound
        if priority is not None:
            data["priority"] = str(priority)
            if int(priority) == 2:
                data["retry"]  = str(max(30, int(PUSHOVER_EMERGENCY_RETRY)))
                data["expire"] = str(max(1,  int(PUSHOVER_EMERGENCY_EXPIRE)))
        if html:
            data["html"] = "1"

        files = None
        # используем уже подготовленные байты; fallback на скачивание по URL оставляем коротким
        if image_bytes:
            files = {"attachment": ("poster.jpg", image_bytes, "image/jpeg")}
        elif image_url:
            try:
                ir = requests.get(image_url, timeout=6)
                ir.raise_for_status()
                content = ir.content
                if len(content) <= 5242880:
                    mime = ir.headers.get("Content-Type") or "image/jpeg"
                    files = {"attachment": ("poster.jpg", content, mime)}
                else:
                    logging.warning("Pushover: image > 5MB, sending without attachment.")
            except Exception as ex:
                logging.warning(f"Pushover: image fetch failed: {ex}")

        # --- Ретраи на отправку ---
        import time
        from requests.exceptions import Timeout, ConnectionError

        attempts = max(1, PUSHOVER_RETRIES)
        delay = max(0.0, PUSHOVER_RETRY_BASE_DELAY)
        for attempt in range(1, attempts + 1):
            try:
                resp = requests.post(
                    endpoint,
                    data=data,
                    files=files,
                    timeout=PUSHOVER_TIMEOUT_SEC,
                    allow_redirects=True
                )
                # успех
                if resp.status_code == 200:
                    logging.info("Pushover notification sent")
                    return True

                # решаем, нужно ли повторять
                retryable_http = resp.status_code in (429, 500, 502, 503, 504)
                if not retryable_http or attempt == attempts:
                    logging.warning(f"Pushover failed {resp.status_code}: {resp.text[:300]}")
                    return False

                logging.warning(f"Pushover HTTP {resp.status_code}, retry {attempt}/{attempts}...")
            except (Timeout, ConnectionError) as ex:
                if attempt == attempts:
                    logging.warning(f"Pushover notify error: {ex}")
                    return False
                logging.warning(f"Pushover network error, retry {attempt}/{attempts}: {ex}")
            except Exception as ex:
                # прочее — не ретраим
                logging.warning(f"Pushover notify error: {ex}")
                return False

            # пауза перед следующей попыткой
            time.sleep(delay)
            delay *= max(1.0, PUSHOVER_RETRY_BACKOFF)

        return False  # теоретически не дойдём

    except Exception as ex:
        logging.warning(f"Pushover notify error: {ex}")
        return False

#MAtrix
def send_matrix_image_then_text_from_jellyfin(photo_id: str, caption_markdown: str) -> bool:
    """
    1) Тянем постер из Jellyfin
    2) Загружаем в Matrix (media repo) -> mxc://
    3) Отправляем m.image (body = имя файла)
    4) Отдельным сообщением отправляем текст (m.text)
    """
    if not (MATRIX_URL and MATRIX_ACCESS_TOKEN and MATRIX_ROOM_ID):
        logging.debug("Matrix not configured; skip.")
        return False

    # 1) картинка из Jellyfin
    try:
        img_bytes, mimetype, filename = _fetch_jellyfin_primary(photo_id)
    except Exception as ex:
        logging.warning(f"Matrix(JF): cannot fetch image from Jellyfin: {ex}")
        # хотя бы текст отправим
        resp_txt = send_matrix_text_rest(caption_markdown)
        return bool(resp_txt and resp_txt.ok)

    # 2) upload -> mxc://
    mxc_uri = matrix_upload_image_rest(img_bytes, filename, mimetype)
    if not mxc_uri:
        logging.warning("Matrix(JF): media upload failed; sending text only.")
        resp_txt = send_matrix_text_rest(caption_markdown)
        return bool(resp_txt and resp_txt.ok)

    # 3) m.image (ВАЖНО: body — имя файла)
    content_img = {
        "msgtype": "m.image",
        "body": filename,
        "url": mxc_uri,
        "info": {
            "mimetype": mimetype,
            "size": len(img_bytes),
        },
    }
    resp_img = _matrix_send_event_rest(MATRIX_ROOM_ID, "m.room.message", content_img)
    img_ok = bool(resp_img and resp_img.ok)

    # 4) затем текст отдельным сообщением
    resp_txt = send_matrix_text_rest(caption_markdown)
    txt_ok = bool(resp_txt and resp_txt.ok)

    if img_ok and txt_ok:
        logging.info("Matrix(JF): image then text sent successfully.")
    else:
        logging.warning("Matrix(JF): image+text flow partially/fully failed.")
    return img_ok and txt_ok

def send_matrix_text_rest(message_markdown: str):
    """
    Отправляет ТОЛЬКО текст в Matrix через REST (v3).
    1) Пытается правильный PUT по спецификации.
    2) Если прокси блокирует PUT (405) — делает POST фоллбэк на тот же путь.
    Возвращает объект response при успехе, иначе None.
    """
    if not (MATRIX_URL and MATRIX_ACCESS_TOKEN and MATRIX_ROOM_ID):
        logging.debug("Matrix not configured; skip.")
        return None

    try:
        # room_id вида "!MNddurK...:example.org" нужно URL-энкодить полностью
        room_enc = quote(MATRIX_ROOM_ID, safe="")
        base = f"{MATRIX_URL.rstrip('/')}/_matrix/client/v3/rooms/{room_enc}/send/m.room.message"

        headers = {
            "Authorization": f"Bearer {MATRIX_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        }

        # Чистим Markdown для plain-текста (Matrix клиенты корректно покажут)
        body_plain = clean_markdown_for_apprise(message_markdown) or ""
        payload = {"msgtype": "m.text", "body": body_plain}

        # Уникальный txnId (в миллисекундах)
        txn_id = f"{int(time.time() * 1000)}txt"
        url = f"{base}/{txn_id}"

        # 1) Правильный путь: PUT (спецификация)
        try:
            resp = requests.put(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            logging.info("Matrix text sent successfully via PUT v3")
            return resp
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 405:
                # 2) Фоллбэк: POST тем же урлом (некоторые reverse-proxy режут PUT)
                logging.warning("Matrix PUT blocked (405). Trying POST fallback…")
                resp2 = requests.post(url, headers=headers, json=payload, timeout=30)
                resp2.raise_for_status()
                logging.info("Matrix text sent successfully via POST fallback")
                return resp2
            else:
                logging.warning(f"Matrix text send failed via PUT: {e}")
                return None

    except Exception as ex:
        logging.warning(f"Matrix text send failed: {ex}")
        return None

def matrix_upload_image_rest(image_bytes: bytes, filename: str, mimetype: str = "image/jpeg") -> str | None:
    """
    Загружает картинку в Matrix content repo и возвращает mxc:// URI.
    Пробуем v3, при 404/405/501 — фоллбэк на r0.
    """
    if not (MATRIX_URL and MATRIX_ACCESS_TOKEN):
        logging.debug("Matrix not configured for media upload; skip.")
        return None

    headers = {"Authorization": f"Bearer {MATRIX_ACCESS_TOKEN}", "Content-Type": mimetype}
    base = MATRIX_URL.rstrip("/")
    url_v3 = f"{base}/_matrix/media/v3/upload?filename={quote(filename)}"

    try:
        r = requests.post(url_v3, headers=headers, data=image_bytes, timeout=30)
        r.raise_for_status()
        return r.json().get("content_uri")
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        if code in (404, 405, 501):
            logging.warning(f"media/v3/upload returned {code}, trying r0…")
            try:
                url_r0 = f"{base}/_matrix/media/r0/upload?filename={quote(filename)}"
                r2 = requests.post(url_r0, headers=headers, data=image_bytes, timeout=30)
                r2.raise_for_status()
                return r2.json().get("content_uri")
            except Exception as ex2:
                logging.warning(f"Matrix r0 upload failed: {ex2}")
                return None
        logging.warning(f"Matrix v3 upload failed: {e}")
        return None
    except Exception as ex:
        logging.warning(f"Matrix upload failed: {ex}")
        return None


def _matrix_send_event_rest(room_id: str, event_type: str, content: dict):
    """
    Отправляет событие в комнату:
      PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}
    При 405 — POST на тот же путь.
    Возвращает response или None.
    """
    if not (MATRIX_URL and MATRIX_ACCESS_TOKEN and room_id):
        return None

    room_enc = quote(room_id, safe="")
    base = f"{MATRIX_URL.rstrip('/')}/_matrix/client/v3/rooms/{room_enc}/send/{event_type}"
    txn_id = f"{int(time.time()*1000)}evt"
    url = f"{base}/{txn_id}"
    headers = {"Authorization": f"Bearer {MATRIX_ACCESS_TOKEN}", "Content-Type": "application/json"}

    try:
        resp = requests.put(url, headers=headers, json=content, timeout=30)
        resp.raise_for_status()
        return resp
    except requests.exceptions.HTTPError as e:
        if getattr(e.response, "status_code", None) == 405:
            logging.warning("PUT blocked (405). Trying POST fallback…")
            try:
                resp2 = requests.post(url, headers=headers, json=content, timeout=30)
                resp2.raise_for_status()
                return resp2
            except Exception as ex2:
                logging.warning(f"Matrix POST fallback failed: {ex2}")
                return None
        logging.warning(f"Matrix send event failed via PUT: {e}")
        return None
    except Exception as ex:
        logging.warning(f"Matrix send event failed: {ex}")
        return None

#отправка сообщения в jellyfin
def _jf_list_active_sessions(active_within_sec: int) -> list:
    """Возвращает список активных сессий Jellyfin за N секунд."""
    try:
        params = {
            "api_key": JELLYFIN_API_KEY,
            "ActiveWithinSeconds": str(active_within_sec)
        }
        r = requests.get(f"{JELLYFIN_BASE_URL}/Sessions", params=params, timeout=10)
        r.raise_for_status()
        return r.json() or []
    except Exception as ex:
        logging.warning(f"JF sessions fetch failed: {ex}")
        return []

def _jf_send_session_message(session_id: str, header: str, text: str, timeout_ms: int) -> bool:
    try:
        url = f"{JELLYFIN_BASE_URL}/Sessions/{session_id}/Message"
        headers = {"X-MediaBrowser-Token": JELLYFIN_API_KEY}
        payload = {"Header": header or "", "Text": text or ""}

        # Добавляем TimeoutMs только если явно хотим «toast»
        # Если включён форс-модалки или timeout_ms <= 0 — НЕ добавляем поле вовсе
        if not JELLYFIN_INAPP_FORCE_MODAL and (timeout_ms is not None) and (int(timeout_ms) > 0):
            payload["TimeoutMs"] = int(timeout_ms)

        r = requests.post(url, headers=headers, json=payload, timeout=8)
        if r.status_code not in (200, 204):
            logging.warning(f"JF message {session_id} failed {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as ex:
        logging.warning(f"JF session message error {session_id}: {ex}")
        return False

def send_jellyfin_inapp_message(message: str, title: str | None = None) -> bool:
    """Отправляет сообщение во ВСЕ активные сессии (за заданный период)."""
    if not (JELLYFIN_INAPP_ENABLED and JELLYFIN_BASE_URL and JELLYFIN_API_KEY):
        return False
    header = (title or JELLYFIN_INAPP_TITLE or "Jellyfin")[:120]
    sessions = _jf_list_active_sessions(JELLYFIN_INAPP_ACTIVE_WITHIN_SEC)
    if not sessions:
        logging.info("Jellyfin in-app: нет активных сессий — сообщение пропущено")
        return False

    ok_any = False
    for s in sessions:
        sid = s.get("Id") or s.get("SessionId") or s.get("Id")
        if not sid:
            continue
        if _jf_send_session_message(sid, header, message, JELLYFIN_INAPP_TIMEOUT_MS):
            ok_any = True

    if ok_any:
        logging.info(f"Jellyfin in-app: отправлено в {len(sessions)} сесс.")
    else:
        logging.warning("Jellyfin in-app: все попытки доставки неуспешны")
    return ok_any

#Отправка сообщения в HA
def send_homeassistant_message(message: str,
                               title: str | None = None,
                               service_path: str | None = None,
                               notification_id: str | None = None,
                               image_url: str | None = None) -> bool:
    """
    Универсальная отправка сервиса Home Assistant.
    По умолчанию используется persistent_notification/create.
    - Для persistent_notification: поддерживаются message, title, notification_id.
      Картинки не поддерживаются — можем (опционально) добавить ссылку в текст.
    - Для прочих сервисов, если они умеют поле 'image', передадим его в 'data.image'.
    """
    try:
        if not HA_BASE_URL or not HA_TOKEN:
            return False

        service_path = (service_path or HA_DEFAULT_SERVICE).strip().strip("/")
        domain, _, service = service_path.partition("/")
        if not domain or not service:
            logging.warning(f"Home Assistant: invalid service_path '{service_path}'")
            return False

        url = f"{HA_BASE_URL}/api/services/{domain}/{service}"
        headers = {
            "Authorization": f"Bearer {HA_TOKEN}",
            "Content-Type": "application/json",
        }

        # Базовый payload
        final_message = message

        # Если это persistent_notification — добавим ссылку на картинку (если включено)
        if domain == "persistent_notification" and image_url and HA_PN_IMAGE_LINK:
            final_message = f"{message}\n\n{HA_PN_IMAGE_LABEL}: {image_url}"

        payload = {"message": final_message}
        if title:
            payload["title"] = title
        if domain == "persistent_notification" and notification_id:
            payload["notification_id"] = notification_id

        # Для других доменов попробуем вложить картинку стандартным образом
        if domain != "persistent_notification" and image_url:
            payload["data"] = {"image": image_url}

        resp = requests.post(url, headers=headers, json=payload, timeout=8, verify=HA_VERIFY_SSL)
        if resp.status_code != 200:
            logging.warning(f"Home Assistant notify failed {resp.status_code}: {resp.text[:300]}")
            return False

        logging.info(f"Home Assistant notification sent via {domain}/{service}")
        return True

    except Exception as ex:
        logging.warning(f"Home Assistant notify error: {ex}")
        return False

#Отправка в synochat
def send_synology_chat_message(text: str, file_url: str | None = None) -> bool:
    """
    Synology Chat Incoming Webhook.
    1) Не отправляем пустой payload: если text пуст — достраиваем из caption.
    2) Попытка №1: form (payload=<json>), №2: JSON body.
    3) Ретраим 117/411/429/5xx.
    """
    try:
        if not (SYNOCHAT_ENABLED and SYNOCHAT_WEBHOOK_URL):
            return False

        # verify: True / False / CA bundle
        verify_param = True
        if not SYNOCHAT_VERIFY_SSL:
            try:
                import urllib3
                from urllib3.exceptions import InsecureRequestWarning
                urllib3.disable_warnings(InsecureRequestWarning)
            except Exception:
                pass
            verify_param = False
        elif SYNOCHAT_CA_BUNDLE:
            verify_param = SYNOCHAT_CA_BUNDLE

        # --- Страховка от пустого текста ---
        safe_text = (text or "").strip()
        if not safe_text:
            # Попробуем извлечь «заголовок + описание» из последнего caption-стиля
            # (первая жирная строка — header, вторая — title; дальше overview)
            try:
                hdr, body = make_jf_inapp_payload_from_caption(text or "")
                safe_text = (body or hdr or "Notification").strip()
            except Exception:
                safe_text = "Notification"

        # Если после этого и poster не включён — не шлём вовсе
        if not safe_text and not file_url:
            logging.debug("Synology Chat: empty payload suppressed")
            return False

        payload = {"text": safe_text}
        if file_url:
            payload["file_url"] = file_url

        import time
        attempts = max(1, SYNOCHAT_RETRIES)
        delay = max(0.0, SYNOCHAT_RETRY_BASE_DELAY)

        for attempt in range(1, attempts + 1):
            # --- Попытка №1: form ---
            r1 = requests.post(
                SYNOCHAT_WEBHOOK_URL,
                data={"payload": json.dumps(payload, ensure_ascii=False)},
                timeout=SYNOCHAT_TIMEOUT_SEC,
                verify=verify_param,
            )
            ok, detail, code = _synochat_resp_ok(r1)
            if ok:
                logging.info("Synology Chat notification sent")
                return True

            # --- Попытка №2: JSON body ---
            r2 = requests.post(
                SYNOCHAT_WEBHOOK_URL,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=SYNOCHAT_TIMEOUT_SEC,
                verify=verify_param,
            )
            ok2, detail2, code2 = _synochat_resp_ok(r2)
            if ok2:
                logging.info("Synology Chat notification sent (json)")
                return True

            # Решаем, ретраить ли
            retry_code = code2 if code2 is not None else code
            # 117 = busy/network; 411 = rate-limit "create post too fast"; 429/5xx уже будут как HTTP в detail
            should_retry = (retry_code in (117, 407, 411)) or ("HTTP 5" in str(detail) or "HTTP 429" in str(detail2))

            if not should_retry or attempt == attempts:
                logging.warning(f"Synology Chat failed: {detail} | {detail2}")
                return False

            logging.warning(f"Synology Chat temporary error (code={retry_code}), retry {attempt}/{attempts}...")
            time.sleep(delay)
            delay *= max(1.0, SYNOCHAT_RETRY_BACKOFF)

        return False

    except Exception as ex:
        logging.warning(f"Synology Chat error: {ex}")
        return False

def _synochat_resp_ok(resp) -> tuple[bool, str]:
    """Проверяем, что Synology Chat реально принял сообщение."""
    if resp is None:
        return False, "no response"
    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
    # Попытка разобрать JSON
    try:
        j = resp.json()
        if isinstance(j, dict) and j.get("success") is True:
            return True, ""
        # Иногда возвращают {"success":false,"error":{...}}
        return False, f"API: {j}"
    except Exception:
        # Бывают «простые» ответы (редко)
        t = (resp.text or "").strip()
        if '"success":true' in t.lower() or t.upper() == "OK":
            return True, ""
        return False, f"Body: {t[:200]}"

def _synochat_resp_ok(resp):
    if resp is None:
        return False, "no response", None
    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code}: {resp.text[:200]}", None
    try:
        j = resp.json()
        if isinstance(j, dict):
            if j.get("success") is True:
                return True, "", None
            # иногда: {"success":false,"error":{"code":...,"errors": "..."}}
            code = (j.get("error") or {}).get("code")
            return False, f"API: {j}", code
    except Exception:
        pass
    t = (resp.text or "").strip().lower()
    if '"success":true' in t or t == "ok":
        return True, "", None
    return False, f"Body: {resp.text[:200]}", None




def send_notification(item_id: str, caption_markdown: str):
    """
    1) Всегда пытаемся отправить в Telegram (фото+подпись) с фолбэком на (фото отдельно + текст отдельно).
    2) Параллельно/последовательно пытаемся Discord, Slack, Email, Gotify (если настроено).
    """
    # Telegram (с фолбэком)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        tg_response = send_telegram_photo(item_id, caption_markdown)
        if tg_response and tg_response.ok:
            logging.info("Notification sent via Telegram")
        else:
            # ФОЛБЭК: разбиваем на два сообщения (фото -> текст)
            logging.warning("Telegram (photo+caption) failed; trying split: photo-only then text…")
            ok_photo = send_telegram_photo_only(item_id)
            ok_text  = send_telegram_text(caption_markdown)
            if ok_photo and ok_text:
                logging.info("Telegram split (photo then text) sent successfully")
            else:
                logging.warning("Telegram split fallback failed")

    # Для сервисов, которым нужен внешний URL на картинку
    uploaded_url = get_jellyfin_image_and_upload_imgbb(item_id)

    # Discord
    if DISCORD_WEBHOOK_URL:
        discord_response = send_discord_message(item_id, caption_markdown, uploaded_url=uploaded_url)
        if discord_response and discord_response.ok:
            logging.info("Notification sent via Discord")
        else:
            logging.warning("Notification failed via Discord")

    # ======= SLACK: файл-изображение с комментарием =======
    try:
        if SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
            ok = send_slack_message_with_image_from_jellyfin(item_id, caption_markdown)
            if ok:
                logging.info("Notification sent via Slack")
            else:
                logging.warning("Notification failed via Slack")
        else:
            logging.debug("Slack disabled or not configured; skip.")
    except Exception as sl_ex:
        logging.warning(f"Slack send failed: {sl_ex}")
    # =====================================================

    # Email
    # ======= EMAIL: письмо с inline-картинкой из Jellyfin =======
    try:
        if SMTP_TO and SMTP_HOST:
            email_ok = send_email_with_image_jellyfin(item_id, subject=SMTP_SUBJECT, body_markdown=caption_markdown)
            if email_ok:
                logging.info("Notification sent via Email")
            else:
                logging.warning("Notification failed via Email")
    except Exception as em_ex:
        logging.warning(f"Email send failed: {em_ex}")

    # Gotify
    if GOTIFY_URL and GOTIFY_TOKEN:
        gotify_response = send_gotify_message(item_id, caption_markdown, uploaded_url=uploaded_url)
        if gotify_response and gotify_response.ok:
            logging.info("Notification sent via Gotify")
        else:
            logging.warning("Notification failed via Gotify")

    # ======= MATRIX (REST): СНАЧАЛА изображение из Jellyfin, затем текст =======
    try:
        if MATRIX_URL and MATRIX_ACCESS_TOKEN and MATRIX_ROOM_ID:
            ok = send_matrix_image_then_text_from_jellyfin(item_id, caption_markdown)
            if ok:
                logging.info("Notification sent via Matrix (REST, image from Jellyfin then text)")
            else:
                logging.warning("Matrix (REST, Jellyfin): image+text flow failed; trying text-only fallback")
                send_matrix_text_rest(caption_markdown)
        else:
            logging.debug("Matrix disabled or not configured; skip.")
    except Exception as m_ex:
        logging.warning(f"Matrix send failed: {m_ex}")

#reddit
    try:
        if REDDIT_ENABLED:
            # Заголовок = «шапка» (первая жирная строка), тело = caption БЕЗ «шапки»
            post_title, body_md = _split_caption_for_reddit(caption_markdown or "")
            external_url = uploaded_url or None  # прямой URL на постер (если есть)

            if REDDIT_SPLIT_TO_COMMENT and external_url:
                # Режим 1: пост-ссылка (картинка), описание — комментарием
                send_reddit_link_post_with_comment(
                    title=post_title,
                    url=external_url,
                    body_markdown=body_md
                )
            else:
                # Режим 0: обычный self-post; если есть URL — поставим его первой строкой в самом посте
                send_reddit_post(
                    title=post_title,
                    body_markdown=body_md,
                    external_image_url=external_url  # может быть None — тогда просто текст
                )
    except Exception as ex:
        logging.warning(f"Reddit wrapper failed: {ex}")

    # ======= WHATSAPP: сначала картинка с подписью (с ретраями), при провале — текст =======
    try:
        wa_jid = _wa_get_jid_from_env()
        if WHATSAPP_API_URL and wa_jid:
            ok_img = send_whatsapp_image_with_retries(
                caption=caption_markdown,
                phone_jid=wa_jid,
                image_url=uploaded_url
            )
            if not ok_img:
                logging.warning("WhatsApp image failed after retries; sending text-only fallback")
                send_whatsapp_text_via_rest(caption_markdown, phone_jid=wa_jid)
        else:
            logging.debug("WhatsApp disabled or no JID; skip WhatsApp send.")
    except Exception as wa_ex:
        logging.warning(f"WhatsApp send block failed: {wa_ex}")

    # --- ОТПРАВКА В SIGNAL ---
    # Plain text для Signal (без Markdown)
    if SIGNAL_URL and SIGNAL_NUMBER:
        signal_resp = send_signal_message_with_image(
            item_id,
            clean_markdown_for_apprise(caption_markdown),
            SIGNAL_NUMBER,
            SIGNAL_RECIPIENTS
        )
        if signal_resp and signal_resp.ok:
            logging.info("Notification sent via Signal")
        else:
            logging.warning("Notification failed via Signal")

#Отправка в pushover
    try:
        if PUSHOVER_USER_KEY and PUSHOVER_TOKEN:
            _title = "Jellyfin"
            # опционально: вытащим заголовок из первой жирной строки сообщения
            img_bytes = _safe_fetch_jellyfin_image_bytes(item_id)  # <— напрямую из Jellyfin
            # uploaded_url — ваш уже известный URL постера (если есть)
            html_msg = markdown_to_pushover_html(caption_markdown or "")
            send_pushover_message(
                message=html_msg,
                title=_title,
                image_bytes=img_bytes,  # <— передаём байты, никаких i.ibb.co
                sound=(PUSHOVER_SOUND or None),
                priority=PUSHOVER_PRIORITY,
                device=(PUSHOVER_DEVICE or None),
                html=True
            )
    except Exception as ex:
        logging.warning(f"Pushover wrapper failed: {ex}")

#отправка в jellyfin
    try:
        if JELLYFIN_INAPP_ENABLED:
            # Для клиентов Jellyfin лучше plain text без Markdown
            jf_header, jf_text = make_jf_inapp_payload_from_caption(caption_markdown or "")
            send_jellyfin_inapp_message(
                message=jf_text,
                title=jf_header
            )
    except Exception as ex:
        logging.warning(f"Jellyfin in-app notify failed: {ex}")

#Отправка в home assistant
    try:
        if HA_BASE_URL and HA_TOKEN:
            _title = "Jellyfin"
            # Можно красиво вытащить заголовок из первой жирной строки, если хотите:
            # m = re.match(r"\*\s*(.+?)\s*\*", caption); _title = (m.group(1)[:120] if m else _title)

            # uploaded_url — это ваш URL постера (если он есть)
            send_homeassistant_message(
                message=caption_markdown,
                title=_title,
                service_path=None,  # берётся из HA_DEFAULT_SERVICE
                notification_id="jellyfin",  # опционально для persistent_notification
                image_url=uploaded_url  # <-- вот тут передаём картинку
            )
    except Exception as ex:
        logging.warning(f"Home Assistant notify wrapper failed: {ex}")

    # ======= Synology Chat =======
    try:
        if SYNOCHAT_ENABLED and SYNOCHAT_WEBHOOK_URL:
            # plain-текст (Chat не рендерит Markdown как Telegram)
            caption_plain = clean_markdown_for_apprise(caption_markdown or "")
            file_url = uploaded_url if (SYNOCHAT_INCLUDE_POSTER and uploaded_url) else None
            send_synology_chat_message(caption_plain, file_url=file_url)
    except Exception as ex:
        logging.warning(f"Synology Chat wrapper failed: {ex}")
    # =============================



#Прочее
def _fetch_jellyfin_image_with_retries(photo_id: str, attempts: int = 3, timeout: int = 10, delay: float = 1.5):
    """
    Пытается скачать Primary-постер из Jellyfin с повторами.
    Возвращает bytes или None.
    """
    url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    last_err = None
    for i in range(1, attempts + 1):
        try:
            # Быстрая проверка доступности (необязательно, но полезно)
            head = requests.head(url, timeout=timeout)
            if head.ok:
                resp = requests.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp.content
            else:
                last_err = f"HTTP {head.status_code}"
        except Exception as ex:
            last_err = ex
        logging.warning(f"Jellyfin image try {i}/{attempts} failed: {last_err}")
        if i < attempts:
            time.sleep(delay)
    return None

def _fetch_jellyfin_primary(photo_id: str):
    """
    Возвращает (bytes, mimetype, filename) для Primary-постера из Jellyfin.
    """
    url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    mimetype = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip().lower()
    ext = ".jpg"
    if "png" in mimetype:
        ext = ".png"
    elif "webp" in mimetype:
        ext = ".webp"
    filename = f"poster{ext}"
    return resp.content, mimetype, filename

def _wa_get_jid_from_env():
    """
    Возвращает JID из окружения.
    Если задана группа — возвращаем группу.
    Иначе личный чат из WHATSAPP_JID или WHATSAPP_NUMBER.
    """
    group_jid = WHATSAPP_GROUP_JID.strip()
    if group_jid:
        if not group_jid.endswith("@g.us"):
            # допустим, передали только id без @g.us
            group_jid = re.sub(r"[^\w\-]", "", group_jid) + "@g.us"
        return group_jid

    # Личный
    raw = (WHATSAPP_JID or WHATSAPP_NUMBER).strip()
    if not raw:
        return None
    if raw.endswith("@s.whatsapp.net"):
        return raw
    # очищаем до цифр и добавляем домен
    local = re.sub(r"\D", "", raw)
    return f"{local}@s.whatsapp.net" if local else None

def _safe_fetch_jellyfin_image_bytes(item_id: str) -> bytes | None:
    """
    Скачивает постер напрямую из Jellyfin, возвращает bytes либо None.
    """
    try:
        url = f"{JELLYFIN_BASE_URL}/Items/{item_id}/Images/Primary"
        # если требуется ключ в query, раскомментируй следующую строку:
        # url = f"{url}?api_key={JELLYFIN_API_KEY}"
        r = requests.get(url, timeout=6)
        r.raise_for_status()
        return r.content
    except Exception as ex:
        logging.debug(f"Pushover: Jellyfin image fetch failed for {item_id}: {ex}")
        return None


#Перевод
MESSAGES = {
    "en": {
        "new_movie_title": "🍿New Movie Added🍿",
        "new_season_title": "📺New Season Added📺",
        "new_episode_title": "📺New Episode Added📺",
        "new_album_title": "🎵New Album Added🎵",
        "new_runtime": "🕒Runtime🕒",
        "new_ratings_movie": "⭐Ratings movie⭐",
        "new_ratings_show": "⭐Ratings show⭐",
        "new_trailer": "Trailer",
        "season_added_progress": "Added {added} of {total} episodes",
        "season_added_count_only": "Added {added} episodes",
        "audio_tracks": "Audio tracks",
        "image_profiles": "Image profiles",
        "quality_updated": "🔼Quality updated🔼",
        "updated": "Updated",
    },
    "ru": {
        "new_movie_title": "🍿Добавлен новый фильм🍿",
        "new_season_title": "📺Добавлен новый сезон📺",
        "new_episode_title": "📺Добавлены новые эпизоды📺",
        "new_album_title": "🎵Добавлен новый альбом🎵",
        "new_runtime": "🕒Продолжительность🕒",
        "new_ratings_movie": "⭐Рейтинги фильма⭐",
        "new_ratings_show": "⭐Рейтинги сериала⭐",
        "new_trailer": "Трейлер",
        "season_added_progress": "Добавлено {added} из {total} эпизодов",
        "season_added_count_only": "Добавлено {added} эпизодов",
        "audio_tracks": "Аудиодорожки",
        "image_profiles": "Профили изображения",
        "quality_updated": "🔼Обновлено качество🔼",
        "updated": "Обновлено",
    },
}

def t(key: str) -> str:
    lang = LANGUAGE if LANGUAGE in MESSAGES else "en"
    return MESSAGES[lang].get(key, key)

def _labels() -> dict:
    L = {
        "quality":       {"en": "Quality",        "ru": "Качество"},
        "resolution":    {"en": "Resolution",     "ru": "Разрешение"},
        "video_codec":   {"en": "Video codec",    "ru": "Видеокодек"},
        "image_profiles":{"en": "Image profiles", "ru": "Профили изображения"},
        "audio_tracks":  {"en": "Audio tracks",   "ru": "Аудиодорожки"},
        "episodes_word": {"en": "episodes",       "ru": "эпизодов"},
    }
    lang = LANGUAGE if LANGUAGE in ("ru", "en") else "en"
    return {k: v[lang] for k, v in L.items()}

def _format_quality_diff_for_message(old_snap: dict, new_snap: dict) -> str:
    """
    *Quality:*
    - Resolution: 1080p → 2160p
    - Video codec: HEVC → AV1
    - Image profiles: Dolby Vision, HDR10 → Dolby Vision, HDR10+
    (Без аудио — аудио идёт отдельным блоком ниже)
    """
    L = _labels() if ('_labels' in globals() or '_labels' in dir()) else {
        "quality":"Quality","resolution":"Resolution","video_codec":"Video codec","image_profiles":"Image profiles"
    }

    def _profiles_to_list(pf_field) -> list[str]:
        import re
        pool = []
        if isinstance(pf_field, list):
            for entry in pf_field:
                pool += [x.strip() for x in re.split(r"\s*,\s*", str(entry)) if x.strip()]
        elif isinstance(pf_field, str):
            pool = [x.strip() for x in re.split(r"\s*,\s*", pf_field) if x.strip()]

        def norm(lbl: str) -> str:
            s = lbl.strip()
            if not s: return ""
            u = s.upper()
            if u.startswith("DOLBY VISION PROFILE"):
                return "Dolby Vision Profile " + "".join(ch for ch in s if ch.isdigit())
            if "DOLBY VISION" in u:
                return "Dolby Vision"
            if u in ("HDR10+", "HDR10", "HLG", "HDR", "SDR"):
                return u
            return s

        out = []
        for x in map(norm, pool):
            if x and x not in out:
                out.append(x)
        if not out:
            out = ["SDR"]

        def key_order(x: str) -> tuple:
            if x.startswith("Dolby Vision"): return (0, x)
            return ({"HDR10+":1,"HDR10":2,"HLG":3,"HDR":4,"SDR":5}.get(x, 99), x)
        out.sort(key=key_order)
        return out

    old_res = old_snap.get("res_label") or "?"
    new_res = new_snap.get("res_label") or "?"
    old_vc  = old_snap.get("vcodec") or "?"
    new_vc  = new_snap.get("vcodec") or "?"

    old_pf = ", ".join(_profiles_to_list(old_snap.get("profiles")))
    new_pf = ", ".join(_profiles_to_list(new_snap.get("profiles")))

    lines = [
        f"- {L['resolution']}: {old_res} → {new_res}",
        f"- {L['video_codec']}: {old_vc} → {new_vc}",
        f"- {L['image_profiles']}: {old_pf} → {new_pf}",
    ]
    return f"*{L['quality']}:*\n" + "\n".join(lines)


#Проба отслеживания качества
# ---------- Radarr helpers: JSON state & snapshots ----------

def _load_json(path: str) -> dict:
    with RADARR_JSON_LOCK:
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f) or {}
        except Exception as ex:
            logging.warning(f"Radarr: load_json failed: {ex}")
        return {}

def _store_json(path: str, data: dict) -> None:
    with RADARR_JSON_LOCK:
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, path)
        except Exception as ex:
            logging.warning(f"Radarr: store_json failed: {ex}")

def _now_ts() -> float:
    import time as _t
    return float(_t.time())

def _snap_signature(s: dict) -> str:
    profs = s.get("profiles");  profs = ",".join(profs) if isinstance(profs, list) else (profs or "")
    a_sig = s.get("audio_sig"); a_sig = ",".join(a_sig) if isinstance(a_sig, list) else (a_sig or "")
    return f"{s.get('width')}x{s.get('height')}|{(s.get('vcodec') or '').lower()}|{profs}|{a_sig}"

def _format_snap_for_text(s: dict) -> str:
    res_label = s.get("res_label") or "?"
    vcodec = s.get("vcodec") or "?"
    profs = s.get("profiles") or ["SDR"];  profs = ", ".join(profs) if isinstance(profs, list) else profs
    ab = s.get("audio_best") or {}
    ac = ab.get("codec") or "?"
    ach = ab.get("channels") or "?"
    atmos = " (Atmos)" if ab.get("atmos") else ""
    return f"{res_label}, {vcodec} ({profs}) | {ac} {ach}{atmos}"



def _provider_imdb_equals(item: dict, imdb_id: str) -> bool:
    p = (item.get("ProviderIds") or {})
    imdb_id = (imdb_id or "").strip().lower()
    for k in ("Imdb", "ImdbId"):
        v = p.get(k)
        if v and str(v).strip().lower() == imdb_id:
            return True
    return False

def _jf_find_movie_by_imdb(imdb_id: str, expected_title: str | None = None, expected_year: int | None = None):
    imdb_id = (imdb_id or "").strip()
    if not imdb_id:
        return None
    # 1) Прямой запрос AnyProviderIdEquals
    try:
        params = {
            "api_key": JELLYFIN_API_KEY,
            "IncludeItemTypes": "Movie",
            "Recursive": "true",
            "AnyProviderIdEquals": imdb_id,
            "Fields": "ProviderIds,ProductionYear,Name,DateCreated"
        }
        url = f"{JELLYFIN_BASE_URL}/emby/Items"
        r = requests.get(url, params=params, timeout=10)
        if r.ok:
            items = (r.json() or {}).get("Items") or []
            cands = [it for it in items if _provider_imdb_equals(it, imdb_id)]
            if cands:
                # Выбор лучшего: совпадение по году → по «похожести» имени → по дате создания
                def score(it):
                    s = 0
                    y = it.get("ProductionYear")
                    if expected_year and y and int(y) == int(expected_year): s += 100
                    if expected_title:
                        from difflib import SequenceMatcher
                        s += int(SequenceMatcher(None,
                            (expected_title or "").strip().casefold(),
                            (it.get("Name") or "").strip().casefold()).ratio() * 50)
                    # более свежие — чутка предпочтительнее
                    s += 1
                    return s
                best = sorted(cands, key=score, reverse=True)[0]
                return best.get("Id"), best.get("Name"), best.get("ProductionYear")
    except Exception as ex:
        logging.debug(f"_jf_find_movie_by_imdb direct failed: {ex}")

    # 2) Фолбэк: большой список + явная проверка ProviderIds.Imdb
    try:
        params = {
            "api_key": JELLYFIN_API_KEY,
            "IncludeItemTypes": "Movie",
            "Recursive": "true",
            "Fields": "ProviderIds,ProductionYear,Name",
            "StartIndex": 0, "Limit": 10000
        }
        url = f"{JELLYFIN_BASE_URL}/emby/Items"
        r = requests.get(url, params=params, timeout=20)
        if r.ok:
            items = (r.json() or {}).get("Items") or []
            cands = [it for it in items if _provider_imdb_equals(it, imdb_id)]
            if cands:
                # тот же скоринг
                def score(it):
                    s = 0
                    y = it.get("ProductionYear")
                    if expected_year and y and int(y) == int(expected_year): s += 100
                    if expected_title:
                        from difflib import SequenceMatcher
                        s += int(SequenceMatcher(None,
                            (expected_title or "").strip().casefold(),
                            (it.get("Name") or "").strip().casefold()).ratio() * 50)
                    return s
                best = sorted(cands, key=score, reverse=True)[0]
                return best.get("Id"), best.get("Name"), best.get("ProductionYear")
    except Exception as ex:
        logging.warning(f"_jf_find_movie_by_imdb fallback failed: {ex}")
    return None

def _provider_tmdb_equals(item: dict, tmdb_id: str | int) -> bool:
    """Жёсткая проверка ProviderIds на TMDb ID (с учётом разных ключей)."""
    p = (item.get("ProviderIds") or {})
    s = str(tmdb_id).strip().lower()
    for k in ("Tmdb", "TmdbId", "TheMovieDb"):
        v = p.get(k)
        if v is not None and str(v).strip().lower() == s:
            return True
    return False

def _jf_find_movie_by_tmdb(tmdb_id: str | int, expected_title: str | None = None, expected_year: int | None = None):
    """Ищем Movie по TMDb ID с валидацией ProviderIds.Tmdb и приоритизацией по году/названию."""
    tid = str(tmdb_id).strip()
    if not tid:
        return None

    def _score(it: dict) -> int:
        s = 0
        y = it.get("ProductionYear")
        if expected_year and y and int(y) == int(expected_year):
            s += 100
        if expected_title:
            from difflib import SequenceMatcher
            s += int(
                SequenceMatcher(None,
                                (expected_title or "").strip().casefold(),
                                (it.get("Name") or "").strip().casefold()
                ).ratio() * 50
            )
        return s

    # 1) Прямой запрос + фильтрация по ProviderIds
    try:
        params = {
            "api_key": JELLYFIN_API_KEY,
            "IncludeItemTypes": "Movie",
            "Recursive": "true",
            "AnyProviderIdEquals": tid,
            "Fields": "ProviderIds,ProductionYear,Name,DateCreated"
        }
        url = f"{JELLYFIN_BASE_URL}/emby/Items"
        r = requests.get(url, params=params, timeout=10)
        if r.ok:
            items = (r.json() or {}).get("Items") or []
            cands = [it for it in items if _provider_tmdb_equals(it, tid)]
            if cands:
                best = sorted(cands, key=_score, reverse=True)[0]
                return best.get("Id"), best.get("Name"), best.get("ProductionYear")
    except Exception as ex:
        logging.debug(f"_jf_find_movie_by_tmdb direct failed: {ex}")

    # 2) Фолбэк: большой лист + явная проверка ProviderIds.Tmdb
    try:
        params = {
            "api_key": JELLYFIN_API_KEY,
            "IncludeItemTypes": "Movie",
            "Recursive": "true",
            "Fields": "ProviderIds,ProductionYear,Name",
            "StartIndex": 0, "Limit": 10000
        }
        url = f"{JELLYFIN_BASE_URL}/emby/Items"
        r = requests.get(url, params=params, timeout=20)
        if r.ok:
            items = (r.json() or {}).get("Items") or []
            cands = [it for it in items if _provider_tmdb_equals(it, tid)]
            if cands:
                best = sorted(cands, key=_score, reverse=True)[0]
                return best.get("Id"), best.get("Name"), best.get("ProductionYear")
    except Exception as ex:
        logging.warning(f"_jf_find_movie_by_tmdb fallback failed: {ex}")

    return None


def _channels_to_float(ch) -> float:
    # "7.1(side)" -> 7.1; 6 -> 5.1; "stereo" -> 2.0; "mono" -> 1.0
    try:
        import re
        s = str(ch)
        m = re.search(r"(\d+(?:\.\d+)?)", s)
        if m: return float(m.group(1))
        s = s.lower()
        if "mono" in s: return 1.0
        if "stereo" in s or "2ch" in s: return 2.0
    except Exception:
        pass
    return 0.0

def _audio_codec_rank(acodec: str) -> int:
    s = (acodec or "").lower()
    if "atmos" in s and ("truehd" in s or "e-ac3" in s or "dd+" in s): return 7
    if "truehd" in s: return 6
    if "dts-hd" in s or "dts/hd" in s or "ma" in s: return 5
    if "flac" in s: return 5
    if "e-ac3" in s or "dd+" in s: return 4
    if "ac3" in s or "dolby digital" in s: return 3
    if "aac" in s: return 2
    if "mp3" in s: return 1
    return 0

def _build_video_snapshot_from_details(details: dict) -> dict | None:
    try:
        item = (details.get("Items") or [None])[0] or {}
        # MediaStreams чаще всего в MediaSources[0].MediaStreams
        ms = None
        for src in (item.get("MediaSources") or []):
            if src.get("MediaStreams"):
                ms = src; break
        if not ms:
            return None
        streams = ms.get("MediaStreams") or []
        vs = next((s for s in streams if (s.get("Type") or "").lower()=="video"), None)
        if not vs:
            return None

        width  = vs.get("Width"); height = vs.get("Height")
        vcodec = _normalize_codec(vs.get("Codec"))

        # ВСЕ профили изображения (DV/HDR10/HDR10+/HLG/HDR/SDR)
        profiles_set = set()
        base = _detect_image_profile(vs) or ""  # например: "Dolby Vision Profile 8, HDR10"
        for tok in [x.strip() for x in re.split(r"\s*,\s*", base) if x.strip()]:
            u = tok.upper()
            if u.startswith("DOLBY VISION PROFILE"):
                # сохраним с номером профиля
                num = "".join(ch for ch in tok if ch.isdigit())
                profiles_set.add(f"Dolby Vision Profile {num}" if num else "Dolby Vision")
            elif "DOLBY VISION" in u:
                profiles_set.add("Dolby Vision")
            elif u in ("HDR10+", "HDR10", "HLG", "HDR", "SDR"):
                profiles_set.add(u)

        # 2) Доберём из VideoRange / VideoRangeType, но не дублируем
        rng = (vs.get("VideoRange") or "").upper()
        rtype = (vs.get("VideoRangeType") or "").upper()
        if "HDR10+" in rng or "HDR10+" in rtype: profiles_set.add("HDR10+")
        if "HDR10" in rng or "HDR10" in rtype: profiles_set.add("HDR10")
        if "HLG" in rng or "HLG" in rtype: profiles_set.add("HLG")
        if ("DOVI" in rtype or "DOLBY" in rng) and not any(p.startswith("Dolby Vision") for p in profiles_set):
            profiles_set.add("Dolby Vision")

        if not profiles_set:
            profiles_set.add("SDR")

        profiles = sorted(profiles_set, key=lambda x: (0, x) if x.startswith("Dolby Vision") else
        ({"HDR10+": 1, "HDR10": 2, "HLG": 3, "HDR": 4, "SDR": 5}.get(x, 99), x))

        # Аудио — лучший трек и «сигнатура» всех треков (codec+channels)
        audios = [s for s in streams if (s.get("Type") or "").lower()=="audio"]
        best_audio = None; best_score = -999
        audio_sig_set = set()
        for a in audios:
            c = _normalize_codec(a.get("Codec"))
            ch_val = a.get("Channels")
            ch = ch_val if isinstance(ch_val, str) else (f"{ch_val}" if ch_val else "")
            # нормализуем в человекочитаемый формат (2->2.0, 6->5.1, 8->7.1)
            ch_label = _channels_to_layout(a.get("Channels"))
            audio_sig_set.add(f"{c} {ch_label}")
            score = _audio_codec_rank(c) * 10 + _channels_to_float(ch or ch_label)
            # лёгкая эвристика на Atmos
            disp = (a.get("DisplayTitle") or "")
            is_atmos = bool(a.get("IsAtmos") or ("ATMOS" in str(disp).upper()))
            if is_atmos:
                score += 1
            if score > best_score:
                best_score = score
                best_audio = {"codec": c, "channels": ch_label, "atmos": is_atmos}

        res_label = _resolution_label(width, height)  # уже есть в коде :contentReference[oaicite:1]{index=1}
        snap = {
            "width": width, "height": height,
            "res_label": res_label,
            "vcodec": vcodec,
            "profiles": sorted(profiles, key=lambda x: x.upper()),
            "audio_best": best_audio or {"codec": "?", "channels": "?", "atmos": False},
            "audio_sig": sorted(audio_sig_set)  # для точного сравнения
        }
        return snap
    except Exception as ex:
        logging.warning(f"snapshot build failed: {ex}")
        return None

def _extract_overview_and_runtime(details: dict) -> tuple[str, str]:
    """Достаём overview и форматируем runtime из RunTimeTicks как HH:MM:SS (например, 00:50:42)."""
    item0 = (details.get("Items") or [{}])[0]
    overview = (item0.get("Overview") or "").strip()
    ticks = item0.get("RunTimeTicks")
    # 10_000_000 ticks = 1 секунда
    try:
        total_sec = int(ticks) // 10_000_000 if ticks else 0
    except Exception:
        total_sec = 0
    if total_sec <= 0:
        return overview, ""  # нет длительности — вернём пустую строку, чтобы шаблон мог пропустить блок

    h = total_sec // 3600
    m = (total_sec % 3600) // 60
    s = total_sec % 60
    runtime_label = f"{h:02d}:{m:02d}:{s:02d}"
    return overview, runtime_label



def _build_audio_tracks_block_from_details(details: dict) -> str:
    """Возвращает markdown-блок '*Audio tracks:*\\n- ...' как в шаблоне для фильма."""
    L = _labels() if ('_labels' in globals() or '_labels' in dir()) else {
        "audio_tracks": "Audio tracks"
    }
    item0 = (details.get("Items") or [{}])[0]
    # забираем MediaStreams (обычно в MediaSources[0].MediaStreams)
    streams = item0.get("MediaStreams") or []
    if not streams:
        for ms in (item0.get("MediaSources") or []):
            if ms.get("MediaStreams"):
                streams = ms["MediaStreams"]; break
    audios = [s for s in streams if (s.get("Type") or "").lower() == "audio"]
    if not audios:
        return f"*{L['audio_tracks']}:*\n- n/a"
    lines = []
    for a in audios:
        disp = (a.get("DisplayTitle") or "").strip()
        if not disp:
            # резерв: Codec + Channels + Language
            codec = (a.get("Codec") or "").upper()
            ch = a.get("Channels")
            lang = (a.get("Language") or "").upper()
            ch_label = _channels_to_layout(ch) if ' _channels_to_layout' in globals() or '_channels_to_layout' in dir() else (f"{ch}" if ch else "?")
            disp = " ".join(x for x in [codec, ch_label, lang] if x)
        lines.append(f"- {disp}")
    return f"*{L['audio_tracks']}:*\n" + "\n".join(lines)

@app.route("/radarr/webhook", methods=["POST"])
def radarr_webhook():
    if not RADARR_ENABLED:
        return "radarr disabled", 200

    # Опциональная защита секретом: /radarr/webhook?secret=XXX
    secret = (request.args.get("secret") or request.headers.get("X-Radarr-Secret") or "").strip()
    if RADARR_WEBHOOK_SECRET and secret != RADARR_WEBHOOK_SECRET:
        logging.warning("Radarr webhook: bad secret")
        return "forbidden", 403

    data = request.get_json(silent=True) or {}
    event = (data.get("eventType") or data.get("event") or "").lower()
    movie = data.get("movie") or {}
    tmdb = movie.get("tmdbId")  # может быть int
    title = (movie.get("title") or "").strip()
    year  = movie.get("year")

    # Нас интересуют события: grab/download/upgrade/import/added/rename/delete-for-upgrade
    interesting = {"grab", "download", "imported", "movieadded",
                   "moviefiledelete", "moviefiledeleted", "moviefiledeleteforupgrade",
                   "moviefilerenamed", "moviefileupgraded", "moviefileupdated"}
    if event and event not in interesting:
        logging.info(f"Radarr webhook: ignore event {event}")
        return "ignored", 200

    if not tmdb:
        if RADARR_USE_IMDB_FALLBACK:
            imdb = (movie.get("imdbId") or "").strip()
            logging.info("Radarr webhook: tmdbId missing; trying imdb fallback")
            jf = _jf_find_movie_by_imdb(imdb, expected_title=title, expected_year=year) if imdb else None
        else:
            logging.info("Radarr webhook: no tmdbId in payload")
            return "no tmdbId", 200
    else:
        jf = _jf_find_movie_by_tmdb(tmdb, expected_title=title, expected_year=year)

    if not jf:
        logging.info(f"Radarr webhook: TMDb {tmdb} not found in Jellyfin (yet)")
        return "not in jellyfin", 200

    item_id, jf_name, jf_year = jf
    details = get_item_details(item_id)
    try:
        item0 = (details.get("Items") or [{}])[0]
        pids = item0.get("ProviderIds") or {}
        got_tmdb = str(pids.get("Tmdb") or pids.get("TmdbId") or pids.get("TheMovieDb") or "").strip()
        if tmdb and got_tmdb and str(tmdb).strip() != got_tmdb:
            logging.warning(f"Radarr webhook: ProviderIds.Tmdb mismatch: expected {tmdb}, got {got_tmdb} — skip store")
            return "tmdb mismatch", 200
    except Exception:
        pass

    snap = _build_video_snapshot_from_details(details)
    if not snap:
        logging.info(f"Radarr webhook: cannot build snapshot for tmdb:{tmdb}")
        return "no snapshot", 200

    pend = _load_json(RADARR_PENDING_FILE)
    key = f"tmdb:{tmdb}" if tmdb else (f"imdb:{imdb}" if RADARR_USE_IMDB_FALLBACK else None)
    if not key:
        return "no key", 200

    pend[key] = {
        "tmdb": str(tmdb) if tmdb else None,
        "imdb": (movie.get("imdbId") or "").strip() if movie.get("imdbId") else None,
        "next_check_ts": _now_ts() + RADARR_RECHECK_AFTER_SEC,
        "movie_name": jf_name or title,
        "year": jf_year or year,
        "snapshot": snap,
        "last_item_id": item_id,  # опционально: только для логов/диагностики
    }
    _store_json(RADARR_PENDING_FILE, pend)
    logging.info(f"Radarr webhook: stored snapshot for {key} ({jf_name or title})")
    return "ok", 200

def _format_before_after(old: dict, new: dict) -> str:
    return f"*{t('was')}:* { _format_snap_for_text(old) }\n*{t('now')}:* { _format_snap_for_text(new) }"

def _radarr_worker_loop():
    """
    Background loop that checks Radarr pending entries and sends a full
    'quality updated' message once Jellyfin has switched to the new file
    and the quality snapshot actually differs (video + audio).
    """
    import time, os

    while True:
        try:
            pend = _load_json(RADARR_PENDING_FILE)
            if pend:
                now = _now_ts()
                changed = False
                to_delete = []

                for k, entry in list(pend.items()):
                    # Wait until it's time to re-check
                    next_ts = float(entry.get("next_check_ts") or 0.0)
                    if now < next_ts:
                        continue

                    old_snap = entry.get("snapshot") or {}

                    # Resolve current Jellyfin item by TMDb (with optional IMDb fallback)
                    item_id, name_now, year_now = _resolve_current_item_id(entry)
                    if not item_id:
                        # Jellyfin hasn't indexed/linked it yet — try again later
                        entry["next_check_ts"] = now + RADARR_RECHECK_AFTER_SEC
                        pend[k] = entry
                        changed = True
                        continue

                    # Pull fresh details
                    details = get_item_details(item_id)

                    # If Radarr told us the expected new file path,
                    # wait until Jellyfin points to a file with the same basename
                    expected_path = (entry.get("new_path") or "").strip()
                    if expected_path:
                        cur_path = (_jf_main_file_path_from_details(details) or "").strip()
                        if cur_path:
                            if os.path.basename(cur_path).lower() != os.path.basename(expected_path).lower():
                                # Jellyfin not yet switched to the new file — postpone
                                entry["next_check_ts"] = now + RADARR_RECHECK_AFTER_SEC
                                pend[k] = entry
                                changed = True
                                continue

                    # Build a new quality snapshot and compare with the stored one
                    new_snap = _build_video_snapshot_from_details(details)

                    if new_snap and _snap_signature(old_snap) != _snap_signature(new_snap):
                        # ================== Build the full "movie-like" message ==================
                        name = entry.get("movie_name") or name_now or "Movie"
                        year = entry.get("year") or year_now or ""

                        overview, runtime_label = _extract_overview_and_runtime(details)

                        msg = f"*{t('quality_updated')}*\n\n*{name}* *({year})*"
                        if overview:
                            msg += f"\n\n{overview}"
                        msg += f"\n\n*{t('new_runtime')}*\n{runtime_label}"

                        # Quality diff (was → now)
                        msg += "\n\n" + _format_quality_diff_for_message(old_snap, new_snap)

                        # Audio tracks list (as in the new-movie template)
                        msg += "\n\n" + _build_audio_tracks_block_from_details(details)

                        # Ratings (MDBList via TMDb)
                        tmdb_id = None
                        try:
                            item0 = (details.get("Items") or [{}])[0]
                            pids = item0.get("ProviderIds") or {}
                            tmdb_id = pids.get("Tmdb") or pids.get("TmdbId") or pids.get("TheMovieDb")
                            if tmdb_id:
                                ratings_text = fetch_mdblist_ratings("movie", tmdb_id)
                                if ratings_text:
                                    msg += f"\n\n*{t('new_ratings_movie')}:*\n{ratings_text}"
                        except Exception as ex:
                            logging.debug(f"ratings for upgrade failed: {ex}")

                        # Trailer (same helper as for Movie, if present)
                        # Trailer (как в шаблоне Movie)
                        try:
                            trailer_url = None
                            if tmdb_id:
                                trailer_url = get_tmdb_trailer_url("movie", str(tmdb_id), TMDB_TRAILER_LANG)
                            if trailer_url:
                                msg += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"
                        except NameError:
                            logging.debug("get_tmdb_trailer_url is not defined")
                        except Exception as ex:
                            logging.debug(f"trailer for upgrade failed: {ex}")

                        # Send and remove the entry only after success
                        try:
                            send_notification(item_id, msg)
                        except Exception as ex:
                            logging.warning(f"Radarr worker: send_notification failed: {ex}")

                        to_delete.append(k)

                    else:
                        # No changes yet — schedule next check
                        entry["next_check_ts"] = now + RADARR_RECHECK_AFTER_SEC
                        pend[k] = entry
                        changed = True

                # Persist changes (updated next_check_ts / deletions)
                if to_delete:
                    for key in to_delete:
                        pend.pop(key, None)
                    changed = True

                if changed:
                    _store_json(RADARR_PENDING_FILE, pend)

        except Exception as ex:
            logging.warning(f"Radarr worker loop error: {ex}")

        time.sleep(RADARR_SCAN_PERIOD_SEC)

def _jf_main_file_path_from_details(details: dict) -> str | None:
    try:
        item0 = (details.get("Items") or [{}])[0]
        # приоритетно из MediaSources
        for ms in (item0.get("MediaSources") or []):
            p = ms.get("Path")
            if p:
                return str(p)
        # иногда путь есть и на самом Item
        p = item0.get("Path")
        return str(p) if p else None
    except Exception:
        return None


def _resolve_current_item_id(entry: dict) -> tuple[str|None, str|None, int|None]:
    """Возвращает (item_id, name, year) по данным записи ожидалки."""
    title = entry.get("movie_name")
    year  = entry.get("year")
    tmdb  = entry.get("tmdb")
    imdb  = entry.get("imdb")

    jf = None
    if tmdb:
        jf = _jf_find_movie_by_tmdb(tmdb, expected_title=title, expected_year=year)
    if not jf and imdb and RADARR_USE_IMDB_FALLBACK:
        jf = _jf_find_movie_by_imdb(imdb, expected_title=title, expected_year=year)
    if jf:
        return jf[0], jf[1], jf[2]
    return None, None, None

#Пробуем sonarr

def _provider_tvdb_equals(item: dict, tvdb_id: str | int) -> bool:
    p = (item.get("ProviderIds") or {})
    s = str(tvdb_id).strip().lower()
    for k in ("Tvdb", "TvdbId", "TheTVDB"):
        v = p.get(k)
        if v and str(v).strip().lower() == s:
            return True
    return False

def _provider_tmdb_equals_series(item: dict, tmdb_id: str | int) -> bool:
    p = (item.get("ProviderIds") or {})
    s = str(tmdb_id).strip().lower()
    for k in ("Tmdb", "TmdbId", "TheMovieDb"):
        v = p.get(k)
        if v is not None and str(v).strip().lower() == s:
            return True
    return False

def _provider_tvmaze_equals(item: dict, tvmaze_id: str | int) -> bool:
    p = (item.get("ProviderIds") or {})
    s = str(tvmaze_id).strip().lower()
    for k in ("TvMaze", "Tvmaze", "TVMazeId"):
        v = p.get(k)
        if v and str(v).strip().lower() == s:
            return True
    return False

def _provider_imdb_equals_series(item: dict, imdb_id: str | int) -> bool:
    p = (item.get("ProviderIds") or {})
    s = str(imdb_id).strip().lower()
    for k in ("Imdb", "ImdbId"):
        v = p.get(k)
        if v and str(v).strip().lower() == s:
            return True
    return False

def _jf_find_series_candidates(fields_extra: str = "ProviderIds,ProductionYear,Name"):
    return {
        "api_key": JELLYFIN_API_KEY,
        "IncludeItemTypes": "Series",
        "Recursive": "true",
        "Fields": fields_extra,
        "StartIndex": 0, "Limit": 10000
    }

def _jf_find_series_by_ids(tvdb=None, tmdb=None, tvmaze=None, imdb=None, expected_title=None, expected_year=None):
    """Возвращает (series_id, name, year) по любому из ID (приоритет: TVDB → TMDB → TVMaze → IMDb)."""
    try:
        url = f"{JELLYFIN_BASE_URL}/emby/Items"
        r = requests.get(url, params=_jf_find_series_candidates(), timeout=20)
        r.raise_for_status()
        items = (r.json() or {}).get("Items") or []
    except Exception:
        items = []

    def score(it):
        s = 0
        if expected_year and int(it.get("ProductionYear") or 0) == int(expected_year): s += 3
        if expected_title:
            from difflib import SequenceMatcher
            s += int(SequenceMatcher(None,
                                     (expected_title or "").strip().casefold(),
                                     (it.get("Name") or "").strip().casefold()).ratio() * 2)
        return -s

    # фильтрации по провайдерам
    cands = []
    if tvdb:  cands = [x for x in items if _provider_tvdb_equals(x, tvdb)]
    if not cands and tmdb:   cands = [x for x in items if _provider_tmdb_equals_series(x, tmdb)]
    if not cands and tvmaze: cands = [x for x in items if _provider_tvmaze_equals(x, tvmaze)]
    if not cands and imdb:   cands = [x for x in items if _provider_imdb_equals_series(x, imdb)]
    if not cands:
        return None
    cands.sort(key=score)
    top = cands[0]
    return top.get("Id"), top.get("Name"), top.get("ProductionYear")

def _jf_find_season_by_index(series_id: str, season_number: int) -> tuple[str|None, str|None]:
    try:
        params = {
            "api_key": JELLYFIN_API_KEY,
            "ParentId": series_id,
            "IncludeItemTypes": "Season",
            "Recursive": "false",
            "Fields": "IndexNumber,Name",
            "Limit": 200,
        }
        url = f"{JELLYFIN_BASE_URL}/emby/Items"
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        for it in (r.json() or {}).get("Items") or []:
            if int(it.get("IndexNumber") or -1) == int(season_number):
                return it.get("Id"), it.get("Name") or f"Season {season_number}"
        return None, None
    except Exception:
        return None, None

def _collect_season_episode_signatures(series_id: str, season_id: str, only_epnums: set[int] | None = None) -> tuple[dict, int]:
    """
    Возвращает (sig_by_epnum, present_count),
    где sig_by_epnum = {episodeNumber -> signature} только для epnums (если заданы).
    present_count — сколько эпизодов с файлами в сезоне (для сравнения с season_counts.json).
    """
    sigs = {}
    present_count = 0
    try:
        url = f"{JELLYFIN_BASE_URL}/emby/Shows/{series_id}/Episodes"
        params = {
            "api_key": JELLYFIN_API_KEY,
            "seasonId": season_id,
            "IsMissing": "false",
            "Fields": "IndexNumber,MediaStreams,MediaSources"
        }
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        items = (r.json() or {}).get("Items") or []
        for it in items:
            epnum = it.get("IndexNumber")
            if epnum is None:
                continue
            present_count += 1
            epnum = int(epnum)
            if only_epnums and epnum not in only_epnums:
                continue
            details = {"Items": [it]}
            snap = _build_video_snapshot_from_details(details)
            sig = _snap_signature(snap or {})
            sigs[epnum] = sig
    except Exception as ex:
        logging.warning(f"_collect_season_episode_signatures failed: {ex}")
    return sigs, present_count


@app.route("/sonarr/webhook", methods=["POST"])
def sonarr_webhook():
    if not SONARR_ENABLED:
        return "sonarr disabled", 200
    if SONARR_WEBHOOK_SECRET and (request.args.get("secret") or "") != SONARR_WEBHOOK_SECRET:
        return "forbidden", 403

    p = request.get_json(silent=True, force=True) or {}

    # Разрешаем только grab-события (разные варианты поля/формата)
    event = (p.get("eventType") or p.get("event") or "").strip().lower()
    # В Sonarr это обычно "Grab", но на всякий случай ловим подстроку.
    if not event or ("grab" not in event):
        return "ignored (not grab)", 200
    episodes = p.get("episodes") or []
    series = p.get("series") or {}
    if not series or not episodes:
        return "no series/episodes", 200

    title = (series.get("title") or series.get("titleSlug") or "").strip()
    year  = series.get("year")
    tvdb  = series.get("tvdbId")
    tmdb  = series.get("tmdbId") or series.get("tmdbid")
    tvmz  = series.get("tvMazeId") or series.get("tvmazeId")
    imdb  = series.get("imdbId")

    # Собираем сезоны -> список эпизодов
    by_season = {}
    for e in episodes:
        sn = e.get("seasonNumber")
        en = e.get("episodeNumber")
        if sn is None or en is None:
            continue
        d = by_season.setdefault(int(sn), set())
        d.add(int(en))

    if not by_season:
        return "no season numbers", 200

    pend = _load_json(SONARR_PENDING_FILE)
    now  = _now_ts()
    touched = 0

    for season_number, ep_set in by_season.items():
        epnums = sorted(ep_set)
        key = f"tvdb:{tvdb}:S{season_number}" if tvdb else f"title:{title}:S{season_number}"
        pend[key] = {
            "tvdb": str(tvdb) if tvdb else None,
            "tmdb": str(tmdb) if tmdb else None,
            "tvmaze": str(tvmz) if tvmz else None,
            "imdb": str(imdb) if imdb else None,
            "series_title": title,
            "release_year": year,
            "season_number": int(season_number),
            "epnums": epnums,                     # <— какие серии Sonarr схватил
            "incoming_count": int(len(epnums)),   # <— сколько
            # baseline пока не знаем: возьмём позже, когда файлы появятся в JF
            "baseline_sigs": None,
            "baseline_present": None,
            "next_check_ts": now + SONARR_RECHECK_AFTER_SEC,
            "event": "grab",
        }
        touched += 1

    if touched:
        _store_json(SONARR_PENDING_FILE, pend)
        logging.info(f"Sonarr webhook (grab): stored {touched} season(s) for '{title}'")
    return "ok", 200


def _resolve_series_from_entry(entry: dict):
    return _jf_find_series_by_ids(
        tvdb=entry.get("tvdb"),
        tmdb=entry.get("tmdb"),
        tvmaze=entry.get("tvmaze"),
        imdb=entry.get("imdb"),
        expected_title=entry.get("series_title"),
        expected_year=entry.get("release_year"),
    )

def _sonarr_worker_loop():
    while True:
        try:
            pend = _load_json(SONARR_PENDING_FILE)
            if not pend:
                time.sleep(SONARR_SCAN_PERIOD_SEC); continue

            now = _now_ts()
            changed = False
            to_delete = []

            for key, entry in list(pend.items()):
                next_ts = float(entry.get("next_check_ts") or 0.0)
                if now < next_ts:
                    continue

                season_number = entry.get("season_number")
                epnums = entry.get("epnums") or []
                incoming_count = int(entry.get("incoming_count") or 0)
                if season_number is None or not epnums:
                    to_delete.append(key); continue

                # 1) Резолв сериала/сезона
                found = _resolve_series_from_entry(entry)
                if not found:
                    entry["next_check_ts"] = now + SONARR_RECHECK_AFTER_SEC
                    pend[key] = entry; changed = True; continue
                series_id, series_name, release_year = found

                sid, sname = _jf_find_season_by_index(series_id, int(season_number))
                if not sid:
                    entry["next_check_ts"] = now + SONARR_RECHECK_AFTER_SEC
                    pend[key] = entry; changed = True; continue
                season_id, season_name = sid, sname or f"Season {season_number}"

                # 2) Сколько реальных эпизодов с файлами сейчас
                _, present_count_all = _collect_season_episode_signatures(series_id, season_id, None)

                # 3) Эталон из season_counts.json
                with _season_counts_lock:
                    st = season_counts.get(season_id)
                last_count = int((st or {}).get("last_count") or 0)

                # Если Sonarr тянет > чем было — это новые эпизоды, не апгрейд
                if incoming_count > last_count:
                    to_delete.append(key)
                    continue

                # 4) Собираем сигнатуры ТОЛЬКО по тем сериям, которые Sonarr захватил
                want = set(int(x) for x in epnums)
                cur_sigs, _ = _collect_season_episode_signatures(series_id, season_id, only_epnums=want)

                baseline = entry.get("baseline_sigs")
                if not baseline:
                    # первая фиксация baseline — ждём изменений
                    entry["baseline_sigs"] = cur_sigs
                    entry["baseline_present"] = present_count_all
                    entry["series_id"] = series_id
                    entry["season_id"] = season_id
                    entry["season_name"] = season_name
                    entry["series_title"] = series_name or entry.get("series_title")
                    entry["release_year"] = release_year or entry.get("release_year")
                    entry["next_check_ts"] = now + SONARR_RECHECK_AFTER_SEC
                    pend[key] = entry; changed = True
                    continue

                # 5) Ищем первый сдвиг
                changed_eps = []
                for ep in want:
                    old = baseline.get(str(ep)) if isinstance(baseline, dict) else None
                    if old is None:
                        old = baseline.get(ep) if isinstance(baseline, dict) else None
                    new = cur_sigs.get(ep)
                    if new and old and new != old:
                        changed_eps.append(ep)

                if not changed_eps:
                    entry["next_check_ts"] = now + SONARR_RECHECK_AFTER_SEC
                    pend[key] = entry; changed = True
                    continue

                # ==== СЛАЕМ УВЕДОМЛЕНИЕ (шаблон как "новые серии", но с заголовком обновления) ====
                # planned_total (опционально)
                series_tmdb_id = entry.get("tmdb")
                planned_total = None
                try:
                    if series_tmdb_id and 'get_tmdb_season_total_episodes' in globals():
                        planned_total = get_tmdb_season_total_episodes(series_tmdb_id, int(season_number), TMDB_TRAILER_LANG)
                except Exception as ex:
                    logging.debug(f"sonarr planned_total failed: {ex}")

                series_details = get_item_details(series_id)
                season_details = get_item_details(season_id)
                season_item = (season_details.get("Items") or [{}])[0]
                series_item = (series_details.get("Items") or [{}])[0]
                overview_to_use = (season_item.get("Overview") or series_item.get("Overview") or "").strip()

                # Какие серии изменились (напр., E01, E02…)
                changed_eps_str = ", ".join(f"E{int(x):02d}" for x in sorted(changed_eps))

                msg = (
                    f"*{t('quality_updated')}*\n\n"
                    f"*{series_name or entry.get('series_title') or 'Series'}* *({release_year or entry.get('release_year') or ''})*\n\n"
                    f"*{season_name}*\n\n"
                    f"{overview_to_use}\n\n"
                    f"\n\n*{t('updated')}*: {changed_eps_str}" if changed_eps_str else
                    f"*{t('quality_updated')}*\n\n"
                    f"*{series_name or entry.get('series_title') or 'Series'}* *({release_year or entry.get('release_year') or ''})*\n\n"
                    f"*{season_name}*\n\n"
                    f"{overview_to_use}\n\n"
                )

                if INCLUDE_MEDIA_TECH_INFO:
                    try:
                        season_tech = build_season_media_tech_text(series_id, season_id)
                        if season_tech:
                            msg += season_tech
                    except Exception as ex:
                        logging.warning(f"Sonarr worker: season tech failed: {ex}")

                # Рейтинги/трейлер по сериалу
                try:
                    if series_tmdb_id:
                        ratings_text = fetch_mdblist_ratings("show", series_tmdb_id)
                        if ratings_text:
                            msg += f"\n\n*{t('new_ratings_show')}:*\n{ratings_text}"
                        if 'get_tmdb_trailer_url' in globals():
                            trailer_url = get_tmdb_trailer_url("tv", str(series_tmdb_id), TMDB_TRAILER_LANG)
                            if trailer_url:
                                msg += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"
                except Exception as ex:
                    logging.debug(f"Sonarr worker: ratings/trailer failed: {ex}")

                # Куда слать постер
                try:
                    target_image_id = season_id if jellyfin_image_exists(season_id) else series_id
                except Exception:
                    target_image_id = series_id

                try:
                    send_notification(target_image_id, msg)
                    to_delete.append(key)   # чистим ТОЛЬКО после отправки
                except Exception as ex:
                    logging.warning(f"Sonarr worker: send_notification failed: {ex}")
                    entry["next_check_ts"] = now + SONARR_RECHECK_AFTER_SEC
                    pend[key] = entry; changed = True

            if to_delete:
                for k in to_delete:
                    pend.pop(k, None)
                changed = True
            if changed:
                _store_json(SONARR_PENDING_FILE, pend)

        except Exception as ex:
            logging.warning(f"Sonarr worker loop error: {ex}")

        time.sleep(SONARR_SCAN_PERIOD_SEC)


#Пробуем заполнять информацию о сезонах
def _prime_season_counts_once():
    """
    Проходит все сериалы и их сезоны в Jellyfin и дополняет season_counts.json
    начальными значениями last_count (кол-во эпизодов с файлами).
    Существующие записи не перетирает.
    """
    import time

    try:
        start_index = 0
        limit = SEASON_COUNTS_PRIME_PAGE_SIZE
        last_save_ts = time.time()

        while True:
            # 1) Берём страницу сериалов
            params = {
                "api_key": JELLYFIN_API_KEY,
                "IncludeItemTypes": "Series",
                "Recursive": "true",
                "Fields": "ProviderIds,ProductionYear,Name",
                "StartIndex": start_index,
                "Limit": limit,
            }
            url = f"{JELLYFIN_BASE_URL}/emby/Items"
            try:
                r = requests.get(url, params=params, timeout=20)
                r.raise_for_status()
                data = r.json() or {}
                series_items = data.get("Items") or []
                if not series_items:
                    break
            except Exception as ex:
                logging.warning(f"Prime season_counts: series page failed at {start_index}: {ex}")
                break

            for series in series_items:
                series_id = series.get("Id")
                if not series_id:
                    continue

                # 2) Берём сезоны сериала (не рекурсивно)
                try:
                    p2 = {
                        "api_key": JELLYFIN_API_KEY,
                        "ParentId": series_id,
                        "IncludeItemTypes": "Season",
                        "Recursive": "false",
                        "Fields": "IndexNumber,Name",
                        "Limit": 500,
                    }
                    r2 = requests.get(f"{JELLYFIN_BASE_URL}/emby/Items", params=p2, timeout=15)
                    r2.raise_for_status()
                    seasons = (r2.json() or {}).get("Items") or []
                except Exception as ex:
                    logging.debug(f"Prime season_counts: list seasons failed: {ex}")
                    continue

                for season in seasons:
                    season_id = season.get("Id")
                    if not season_id:
                        continue
                    # 3) Считаем фактическое число эпизодов с файлами
                    try:
                        present = get_season_episode_count(series_id, season_id)
                    except Exception as ex:
                        logging.debug(f"Prime season_counts: count episodes failed: {ex}")
                        continue

                    # 4) Заполняем только пустые записи
                    with _season_counts_lock:
                        st = season_counts.get(season_id)
                        if not st:
                            season_counts[season_id] = {"last_count": int(present), "last_sent_ts": 0}
                        elif "last_count" not in st:
                            st["last_count"] = int(present)
                            season_counts[season_id] = st

                # Периодически сохраняем на диск
                if (time.time() - last_save_ts) >= SEASON_COUNTS_PRIME_SAVE_SEC:
                    with _season_counts_lock:
                        save_season_counts(season_counts)
                    last_save_ts = time.time()

            start_index += limit

        # Финальное сохранение
        with _season_counts_lock:
            save_season_counts(season_counts)
        logging.info("Prime season_counts: completed.")
    except Exception as ex:
        logging.warning(f"Prime season_counts error: {ex}")




@app.route("/webhook", methods=["POST"])
def announce_new_releases_from_jellyfin():
    try:
        payload = json.loads(request.data)
        item_type = payload.get("ItemType")
        tmdb_id = payload.get("Provider_tmdb")
        item_name = payload.get("Name")
        release_year = payload.get("Year")
        series_name = payload.get("SeriesName")
        season_epi = payload.get("EpisodeNumber00")
        season_num = payload.get("SeasonNumber00")

        if item_type == "Movie":
                movie_id = payload.get("ItemId")
                overview = payload.get("Overview")
                runtime = payload.get("RunTime")
                # Remove release_year from movie_name if present
                movie_name = item_name
                movie_name_cleaned = movie_name.replace(f" ({release_year})", "").strip()

                trailer_url = get_tmdb_trailer_url("movie", tmdb_id, TMDB_TRAILER_LANG)

                notification_message = (
                    f"*{t('new_movie_title')}*\n\n"
                    f"*{movie_name_cleaned}* *({release_year})*\n\n"
                    f"{overview}\n\n"
                    f"*{t('new_runtime')}*\n{runtime}"
                )

                # Добавляем блок качества/аудио (опционально, по умолчанию включено)
                if INCLUDE_MEDIA_TECH_INFO:
                    try:
                        movie_details = get_item_details(movie_id)
                        tech_text = build_movie_media_tech_text(movie_details)
                        if tech_text:
                            notification_message += tech_text
                    except Exception as e:
                        logging.warning(f"Could not append media tech info: {e}")

                if tmdb_id:
                    # приводим тип к тому, что ждёт MDblist: movie или series
                    mdblist_type = item_type.lower()
                    ratings_text = fetch_mdblist_ratings(mdblist_type, tmdb_id)
                    if ratings_text:
                        notification_message += f"\n\n*{t('new_ratings_movie')}:*\n{ratings_text}"

                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

                send_notification(movie_id, notification_message)
                logging.info(f"(Movie) {movie_name} {release_year} notification was sent.")
                return "Movie notification was sent"

        if item_type == "Season":
                season_id = payload.get("ItemId")
                season = item_name
                season_details = get_item_details(season_id)
                series_id = season_details["Items"][0].get("SeriesId")
                series_details = get_item_details(series_id)
                # Remove release_year from series_name if present
                series_name_cleaned = series_name.replace(f" ({release_year})", "").strip()

                try:
                    series_tmdb_id = extract_tmdb_id_from_jellyfin_details(series_details)
                except NameError:
                    # если helper ещё не добавлен — используем то, что пришло из вебхука
                    series_tmdb_id = payload.get("Provider_tmdb")

                trailer_url = get_tmdb_trailer_url("tv", series_tmdb_id, TMDB_TRAILER_LANG)

                # Get TMDb ID via external API
                tmdb_id = extract_tmdb_id_from_jellyfin_details(series_details)

                # **Новые строки**: получаем рейтинги для сериала
                ratings_text = fetch_mdblist_ratings("show", tmdb_id) if tmdb_id else ""
                # Если есть рейтинги — добавляем пустую строку после них
                ratings_section = f"{ratings_text}\n\n" if ratings_text else ""

                # Get series overview if season overview is empty
                overview_to_use = payload.get("Overview") if payload.get("Overview") else series_details["Items"][0].get(
                    "Overview")

                notification_message = (
                    f"*{t('new_season_title')}*\n\n"
                    f"*{series_name_cleaned}* *({release_year})*\n\n"
                    f"*{season}*\n\n"
                    f"{overview_to_use}"
                )

                if ratings_text:
                    notification_message += f"\n\n*{t('new_ratings_show')}:*\n{ratings_text}"

                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

                target_id = season_id if jellyfin_image_exists(season_id) else series_id
                if target_id == series_id:
                    logging.warning(
                        f"{series_name_cleaned} {season} image does not exist, falling back to series image")

                send_notification(target_id, notification_message)
                logging.info(f"(Season) {series_name_cleaned} {season} notification was sent.")
                return "Season notification was sent"

        if item_type == "Episode":
            # 1) Базовые ID
            episode_id = payload.get("ItemId")
            file_details = get_item_details(episode_id)
            item0 = (file_details.get("Items") or [{}])[0]
            season_id = item0.get("SeasonId")
            series_id = item0.get("SeriesId")

            if not season_id or not series_id:
                logging.warning("Episode payload missing SeasonId/SeriesId; skipping.")
                return "Skipped: missing SeasonId/SeriesId", 200

            # 2) Детали сезона и сериала
            season_details = get_item_details(season_id)
            series_details = get_item_details(series_id)
            season_item = (season_details.get("Items") or [{}])[0]
            series_item = (series_details.get("Items") or [{}])[0]

            series_name = series_item.get("Name") or payload.get("SeriesName") or "Unknown series"
            season_name = season_item.get("Name") or "Season"
            release_year = series_item.get("ProductionYear") or payload.get("Year") or ""

            # 3) Фактическое число серий сейчас (Jellyfin) + план (TMDb)
            present_count = get_season_episode_count(series_id, season_id)

            try:
                series_tmdb_id = extract_tmdb_id_from_jellyfin_details(series_details)
            except NameError:
                series_tmdb_id = None

            season_number = extract_season_number_from_details(season_details)
            planned_total = (
                get_tmdb_season_total_episodes(series_tmdb_id, season_number, TMDB_TRAILER_LANG)
                if series_tmdb_id and season_number is not None else None
            )

            # 4) Анти-спам на основе состояния
            now_ts = time.time()
            with _season_counts_lock:
                st = season_counts.get(season_id) or {}
                last_sent = float(st.get("last_sent_ts") or 0)
                last_count = int(st.get("last_count") or 0)

                should_send = False
                # отправляем, если увеличилось число эпизодов...
                if present_count > last_count:
                    # ...и прошло не меньше заданного окна (или сезон добит до планового числа)
                    quiet_enough = (now_ts - last_sent) >= EPISODE_MSG_MIN_GAP_SEC
                    completed = planned_total and present_count >= planned_total
                    should_send = bool(quiet_enough or completed)

                # обновляем «наблюдаемое» состояние (чтобы при следующем вебхуке знали актуальный счётчик)
                st["last_count"] = present_count
                # но метку отправки перепишем только если реально пошлём
                season_counts[season_id] = st
                if not should_send:
                    save_season_counts(season_counts)
                    logging.info(
                        f"(Episode batch) Suppressed by anti-spam: {series_name}/{season_name} now {present_count}"
                        + (f" of {planned_total}" if planned_total else ""))
                    return "Suppressed by anti-spam window", 200

            # 5) Доп. данные: рейтинги + трейлер по сериалу
            ratings_text = fetch_mdblist_ratings("show", series_tmdb_id) if series_tmdb_id else ""
            trailer_url = get_tmdb_trailer_url("tv", series_tmdb_id, TMDB_TRAILER_LANG) if series_tmdb_id else None

            overview_to_use = (
                    season_item.get("Overview")
                    or series_item.get("Overview")
                    or payload.get("Overview")
                    or ""
            )
            # 6) Сообщение: «добавлено N из M»
            added_line = (
                t('season_added_progress').format(added=present_count, total=planned_total)
                if planned_total else
                t('season_added_count_only').format(added=present_count)
            )
            notification_message = (
                f"*{t('new_episode_title')}*\n\n"
                f"*{series_name}* *({release_year})*\n\n"
                f"*{season_name}*\n\n"
                f"{overview_to_use}\n\n"
                f"{added_line}"
            )

            # Блок техники по сезону (по умолчанию включён через INCLUDE_MEDIA_TECH_INFO)
            if INCLUDE_MEDIA_TECH_INFO:
                try:
                    season_tech = build_season_media_tech_text(series_id, season_id)
                    if season_tech:
                        notification_message += f"{season_tech}"
                except Exception as e:
                    logging.warning(f"Could not append season tech info: {e}")

            if ratings_text:
                notification_message += f"\n\n*{t('new_ratings_show')}:*\n{ratings_text}"
            if trailer_url:
                notification_message += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

            # 7) Отправка (постер сезона → фолбэк на сериал)
            target_id = season_id if jellyfin_image_exists(season_id) else series_id
            if target_id == series_id:
                logging.warning("(Episode batch) Season image missing; fallback to series image.")
            send_notification(target_id, notification_message)

            # 8) Зафиксировать момент отправки
            with _season_counts_lock:
                season_counts[season_id]["last_sent_ts"] = now_ts
                save_season_counts(season_counts)

            logging.info(
                f"(Episode batch) {series_name}/{season_name}: sent {present_count}"
                + (f" of {planned_total}" if planned_total else "")
            )
            return "Episode batch notification was sent to telegram", 200


        if item_type == "MusicAlbum":
                album_id = payload.get("ItemId")
                album_name = payload.get("Name")
                artist = payload.get("Artist")
                year = payload.get("Year")
                overview = payload.get("Overview")
                runtime = payload.get("RunTime")
                musicbrainzalbum_id = payload.get("Provider_musicbrainzalbum")

                # Формируем ссылку на MusicBrainz, если есть ID
                mb_link = f"https://musicbrainz.org/release/{musicbrainzalbum_id}" if musicbrainzalbum_id else ""

                # Шаблон уведомления
                notification_message = (
                    f"*{t('new_album_title')}*\n\n"
                    f"*{artist}*\n\n"
                    f"*{album_name} ({year})*\n\n"
                    f"{(overview + '\n\n') if overview else ''}"
                    f"*{t('new_runtime')}*\n{runtime}\n\n"
                    f"{f'[MusicBrainz]({mb_link})' if mb_link else ''}\n"
                )

                # Отправляем обложку альбома, если есть, иначе ничего страшного
                target_id = album_id if jellyfin_image_exists(album_id) else None
                if target_id is None:
                    logging.warning(f"Album cover not found for {album_name}, sending text-only.")
                    # Вызовем отправку без картинки: используем send_notification на тексте
                    send_notification(album_id, notification_message)  # он сам отправит текст, если картинки нет
                else:
                    send_notification(target_id, notification_message)

                logging.info(f"(Album) {artist} – {album_name} ({year}) notification sent.")
                return "Album notification was sent"

        if item_type == "Movie":
            logging.info(f"(Movie) {item_name} Notification Was Already Sent")
        elif item_type == "Season":
            logging.info(f"(Season) {series_name} {item_name} Notification Was Already Sent")
        elif item_type == "Episode":
            logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} Notification Was Already Sent")
        else:
            logging.error('Item type not supported')
        return "Item type not supported."

    # Handle specific HTTP errors
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        return str(http_err)

    # Handle generic exceptions
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return f"Error: {str(e)}"

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200

#if __name__ == "__main__":
#    app.run(host="0.0.0.0", port=5000)

if __name__ == "__main__":
    if RADARR_ENABLED:
        threading.Thread(target=_radarr_worker_loop, name="radarr-qual-worker", daemon=True).start()
    if SONARR_ENABLED:
        threading.Thread(target=_sonarr_worker_loop, name="sonarr-qual-worker", daemon=True).start()
    if SEASON_COUNTS_PRIME_ON_START:
        threading.Thread(target=_prime_season_counts_once, name="season-counts-prime", daemon=True).start()
    app.run(host="0.0.0.0", port=5000)

