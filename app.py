import logging
from logging.handlers import TimedRotatingFileHandler
import threading, tempfile, time
from datetime import datetime
import os
import json
import requests
from requests.exceptions import HTTPError
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
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
JELLYFIN_BASE_URL = os.environ["JELLYFIN_BASE_URL"]
JELLYFIN_API_KEY = os.environ["JELLYFIN_API_KEY"]
MDBLIST_API_KEY = os.environ["MDBLIST_API_KEY"]
TMDB_API_KEY = os.environ["TMDB_API_KEY"]
TMDB_V3_BASE = "https://api.themoviedb.org/3"
TMDB_TRAILER_LANG = os.getenv("TMDB_TRAILER_LANG", "en-US")  # пример: ru-RU, sv-SE, en-US
INCLUDE_MEDIA_TECH_INFO = os.getenv("INCLUDE_MEDIA_TECH_INFO", "true").strip().lower() in ("1","true","yes","y","on")
EPISODE_MSG_MIN_GAP_SEC = int(os.getenv("EPISODE_MSG_MIN_GAP_SEC", "0"))  # анти-спам: минимум N секунд между сообщениями по сезону
JELLYFIN_USER_ID = os.getenv("JELLYFIN_USER_ID")  # опционально; если не задан, определим автоматически по токену


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

def _detect_image_profile(vs: dict) -> str:
    """
    Пытаемся красиво отобразить профиль изображения: Dolby Vision / HDR10 / HDR10+ / HLG / SDR.
    Jellyfin обычно даёт поля VideoRange/VideoRangeType; если есть профиль DV — подцепим.
    """
    rng = (vs.get("VideoRange") or "").upper()      # например: HDR10, SDR, DOLBY VISION
    rtype = (vs.get("VideoRangeType") or "").upper()  # например: DOVI, HDR10, HLG
    profile_hint = ""
    # иногда профиль DV встречается в полях типа 'DolbyVisionProfile', 'DvProfile', 'VideoDoViProfile'…
    for k, v in (vs or {}).items():
        if "profile" in k.lower() and isinstance(v, (str, int)):
            profile_hint = f" Profile {v}"
            break

    if "DOVI" in rtype or "DOLBY" in rng:
        return f"Dolby Vision{profile_hint}"
    if "HDR10+" in rng or "HDR10+" in rtype:
        return "HDR10+"
    if "HDR10" in rng or "HDR10" in rtype:
        return "HDR10"
    if "HLG" in rng or "HLG" in rtype:
        return "HLG"
    # если ничего явного — считаем SDR
    return "SDR"

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

        quality_block = (
            "*Quality:*\n"
            f"- Resolution: {res_label}\n"
            f"- Video codec: {vcodec}\n"
            f"- Image profiles: {img_profile}"
        )

        # ---- Аудио ----
        audio_streams = [s for s in streams if (s.get("Type") or "").lower() == "audio"]
        if audio_streams:
            audio_lines = []
            for a in audio_streams:
                # jellyfin часто уже даёт «DisplayTitle» вида "DTS-HD MA 7.1 (eng)" и т.п.
                acodec_disp = (a.get("DisplayTitle") or "").strip()
                is_atmos = a.get("IsAtmos") or ("ATMOS" in acodec_disp.upper())

                if acodec_disp:
                    # НЕ добавляем префикс "ru:"/"rus:" — как просили
                    line = acodec_disp
                    if is_atmos and "atmos" not in acodec_disp.lower():
                        line += " (Atmos)"
                else:
                    base = _normalize_codec(a.get("Codec"))
                    ch   = _channels_to_layout(a.get("Channels"))
                    lang = a.get("Language") or "und"
                    line = f"{base} {ch} ({lang})"
                    if is_atmos:
                        line += " (Atmos)"

                audio_lines.append(f"- {line}")

            audio_block = "*Audio tracks:*\n" + "\n".join(audio_lines)
        else:
            audio_block = "*Audio tracks:*\n- n/a"

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
                    f"*🍿New Movie Added🍿*\n\n*{movie_name_cleaned}* *({release_year})*\n\n{overview}\n\n"
                    f"Runtime\n{runtime}")

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
                        notification_message += f"\n\n*⭐Ratings movie⭐:*\n{ratings_text}"

                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[Trailer]({trailer_url})"

                send_telegram_photo(movie_id, notification_message)
                logging.info(f"(Movie) {movie_name} {release_year} "
                             f"notification was sent to telegram.")
                return "Movie notification was sent to telegram"

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
                    f"*New Season Added*\n\n*{series_name_cleaned}* *({release_year})*\n\n"
                    f"*{season}*\n\n{overview_to_use}")

                if ratings_text:
                    notification_message += f"\n\n*⭐Ratings show⭐:*\n{ratings_text}"

                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[Trailer]({trailer_url})"

                response = send_telegram_photo(season_id, notification_message)

                if response.status_code == 200:
                    logging.info(f"(Season) {series_name_cleaned} {season} "
                                 f"notification was sent to telegram.")
                    return "Season notification was sent to telegram"
                else:
                    send_telegram_photo(series_id, notification_message)
                    logging.warning(f"{series_name_cleaned} {season} image does not exists, falling back to series image")
                    logging.info(f"(Season) {series_name_cleaned} {season} notification was sent to telegram")
                    return "Season notification was sent to telegram"

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
            added_line = f"*Episodes added*: {present_count}" + (f" of {planned_total}" if planned_total else "")
            notification_message = (
                f"*📺 New Episodes Added*\n\n"
                f"*{series_name}* *({release_year})*\n\n"
                f"*{season_name}*\n\n"
                f"{overview_to_use}\n\n"
                f"{added_line}"
            )
            if ratings_text:
                notification_message += f"\n\n*⭐Ratings show⭐:*\n{ratings_text}"
            if trailer_url:
                notification_message += f"\n\n[🎥]({trailer_url})[Trailer]({trailer_url})"

            # 7) Отправка (постер сезона → фолбэк на сериал)
            response = send_telegram_photo(season_id, notification_message)
            if response.status_code != 200:
                send_telegram_photo(series_id, notification_message)
                logging.warning(f"(Episode batch) Season image missing; fallback to series image.")

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
                    "* 🎵 New Album Added 🎵 *\n\n"
                    f"*{artist}*\n\n"
                    f"*{album_name} ({year})*\n\n"
                    f"{overview and overview + '\n\n' or ''}"
                    f"Runtime\n{runtime}\n\n"
                    f"{f'[MusicBrainz]({mb_link})' if mb_link else ''}\n"
                )

                # Отправляем обложку альбома, если есть, иначе ничего страшного
                response = send_telegram_photo(album_id, notification_message)


                if response.status_code == 200:
                    logging.info(f"(Album) {artist} – {album_name} ({year}) notification sent.")
                    return "Album notification was sent to telegram"
                else:
                    # можно при падении картинки просто залогировать и вернуть успех, чтобы не спамить
                    logging.warning(f"Album cover not found for {album_name}, sent text-only message.")
                    return "Album notification was sent to telegram"

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
