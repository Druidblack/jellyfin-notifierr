import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta
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


# Constants
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
JELLYFIN_BASE_URL = os.environ["JELLYFIN_BASE_URL"]
JELLYFIN_API_KEY = os.environ["JELLYFIN_API_KEY"]
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]
MDBLIST_API_KEY = os.environ["MDBLIST_API_KEY"]
TMDB_API_KEY = os.environ["TMDB_API_KEY"]
TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/tv"
EPISODE_PREMIERED_WITHIN_X_DAYS = int(os.environ["EPISODE_PREMIERED_WITHIN_X_DAYS"])
SEASON_ADDED_WITHIN_X_DAYS = int(os.environ["SEASON_ADDED_WITHIN_X_DAYS"])
#выключить логику пропуска по датам
#DEBUG_DISABLE_DATE_CHECKS = True

# Path for the JSON file to store notified items
notified_items_file = 'A:/notifierr/notified_items.json'

# Убедимся, что папка /app/data существует
os.makedirs(os.path.dirname(notified_items_file), exist_ok=True)

# Function to load notified items from the JSON file
def load_notified_items():
    # Если файл есть — читаем
    if os.path.exists(notified_items_file):
        with open(notified_items_file, 'r', encoding='utf-8') as file:
            return json.load(file)
    # Иначе — создаём пустой JSON и возвращаем пустой словарь
    with open(notified_items_file, 'w', encoding='utf-8') as file:
        json.dump({}, file, ensure_ascii=False, indent=2)
    return {}

# Function to save notified items to the JSON file
def save_notified_items(notified_items_to_save):
    with open(notified_items_file, 'w', encoding='utf-8') as file:
        json.dump(notified_items_to_save, file, ensure_ascii=False, indent=2)


notified_items = load_notified_items()


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
        f"?Recursive=true&Fields=DateCreated,Overview,ProviderIds,ExternalUrls&Ids={item_id}"
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



def _parse_date_any(date_str: str):
    """
    Принимает 'YYYY-MM-DD' или 'YYYY-MM-DDTHH:MM:SS[Z]'.
    Возвращает datetime или None, если распарсить не удалось.
    """
    if not date_str:
        return None
    try:
        # Отрежем время, если прилетело '...T...'
        if "T" in date_str:
            date_str = date_str.split("T")[0]
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None


def is_within_last_x_days(date_str: str, x: int) -> bool:
    dt = _parse_date_any(date_str)
    if not dt:
        return False
    return dt >= (datetime.now() - timedelta(days=x))


def is_not_within_last_x_days(date_str: str, x: int) -> bool:
    dt = _parse_date_any(date_str)
    if not dt:
        return True
    return dt < (datetime.now() - timedelta(days=x))


def get_youtube_trailer_url(query):
    base_search_url = "https://www.googleapis.com/youtube/v3/search"
    if not YOUTUBE_API_KEY:
        return None
    api_key = YOUTUBE_API_KEY

    params = {
        'part': 'snippet',
        'q': query,
        'type': 'video',
        'key': api_key
    }

    response = requests.get(base_search_url, params=params, timeout=10)
    response.raise_for_status()  # Check for HTTP errors before processing the data
    response_data = response.json()
    video_id = response_data.get("items", [{}])[0].get('id', {}).get('videoId')

    return f"https://www.youtube.com/watch?v={video_id}" if video_id else "Video not found!"


def item_already_notified(item_type, item_name, release_year):
    key = f"{item_type}:{item_name}:{release_year}"
    return key in notified_items


def mark_item_as_notified(item_type, item_name, release_year, max_entries=100):
    key = f"{item_type}:{item_name}:{release_year}"
    notified_items[key] = True

    # Если превысили лимит — удаляем самый старый ключ (порядок вставки у dict сохраняется в Py3.7+)
    if len(notified_items) > max_entries:
        try:
            oldest_key = next(iter(notified_items))
            del notified_items[oldest_key]
            logging.info(f"Key '{oldest_key}' has been deleted from notified_items")
        except StopIteration:
            pass
    # Save the updated notified items to the JSON file
    save_notified_items(notified_items)


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
            if not item_already_notified(item_type, item_name, release_year):
                movie_id = payload.get("ItemId")
                overview = payload.get("Overview")
                runtime = payload.get("RunTime")
                # Remove release_year from movie_name if present
                movie_name = item_name
                movie_name_cleaned = movie_name.replace(f" ({release_year})", "").strip()

                trailer_url = get_youtube_trailer_url(f"{movie_name_cleaned} Trailer {release_year}")

                notification_message = (
                    f"*🍿New Movie Added🍿*\n\n*{movie_name_cleaned}* *({release_year})*\n\n{overview}\n\n"
                    f"Runtime\n{runtime}")

                if tmdb_id:
                    # приводим тип к тому, что ждёт MDblist: movie или series
                    mdblist_type = item_type.lower()
                    ratings_text = fetch_mdblist_ratings(mdblist_type, tmdb_id)
                    if ratings_text:
                        notification_message += f"\n\n*⭐Ratings movie⭐:*\n{ratings_text}"

                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[Trailer]({trailer_url})"

                send_telegram_photo(movie_id, notification_message)
                mark_item_as_notified(item_type, item_name, release_year)
                logging.info(f"(Movie) {movie_name} {release_year} "
                             f"notification was sent to telegram.")
                return "Movie notification was sent to telegram"

        if item_type == "Season":
            if not item_already_notified(item_type, item_name, release_year):
                season_id = payload.get("ItemId")
                season = item_name
                season_details = get_item_details(season_id)
                series_id = season_details["Items"][0].get("SeriesId")
                series_details = get_item_details(series_id)
                # Remove release_year from series_name if present
                series_name_cleaned = series_name.replace(f" ({release_year})", "").strip()

                trailer_url = get_youtube_trailer_url(f"{series_name_cleaned} Trailer {release_year}")

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
                    mark_item_as_notified(item_type, item_name, release_year)
                    logging.info(f"(Season) {series_name_cleaned} {season} "
                                 f"notification was sent to telegram.")
                    return "Season notification was sent to telegram"
                else:
                    send_telegram_photo(series_id, notification_message)
                    mark_item_as_notified(item_type, item_name, release_year)
                    logging.warning(f"{series_name_cleaned} {season} image does not exists, falling back to series image")
                    logging.info(f"(Season) {series_name_cleaned} {season} notification was sent to telegram")
                    return "Season notification was sent to telegram"

        if item_type == "Episode":
            if not item_already_notified(item_type, item_name, release_year):
                item_id = payload.get("ItemId")
                file_details = get_item_details(item_id)
                season_id = file_details["Items"][0].get("SeasonId")
                episode_premiere_date = file_details["Items"][0].get("PremiereDate", "0000-00-00T").split("T")[0]
                season_details = get_item_details(season_id)
                series_id = season_details["Items"][0].get("SeriesId")
                season_date_created = season_details["Items"][0].get("DateCreated", "0000-00-00T").split("T")[0]
                epi_name = item_name
                overview = payload.get("Overview")

#                if not DEBUG_DISABLE_DATE_CHECKS:
                if not is_not_within_last_x_days(season_date_created, SEASON_ADDED_WITHIN_X_DAYS):
                    logging.info(f"(Episode) {series_name} Season {season_num} "
                                 f"was added within the last {SEASON_ADDED_WITHIN_X_DAYS} "
                                 f"days. Not sending notification.")
                    return (f"Season was added within the last {SEASON_ADDED_WITHIN_X_DAYS} "
                            f"days. Not sending notification.")

                if episode_premiere_date and is_within_last_x_days(episode_premiere_date,
                                                                   EPISODE_PREMIERED_WITHIN_X_DAYS):

                    notification_message = (
                        f"*New Episode Added*\n\n*Release Date*: {episode_premiere_date}\n\n*Series*: {series_name} *S*"
                        f"{season_num}*E*{season_epi}\n*Episode Title*: {epi_name}\n\n{overview}\n\n"
                    )
                    response = send_telegram_photo(season_id, notification_message)

                    if response.status_code == 200:
                        mark_item_as_notified(item_type, item_name, release_year)
                        logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} notification sent to Telegram!")
                        return "Notification sent to Telegram!"
                    else:
                        send_telegram_photo(series_id, notification_message)
                        logging.warning(f"(Episode) {series_name} season image does not exists, "
                                        f"falling back to series image")
                        mark_item_as_notified(item_type, item_name, release_year)
                        logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} notification sent to Telegram!")
                        return "Notification sent to Telegram!"

                else:
                    logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} "
                                 f"was premiered more than {EPISODE_PREMIERED_WITHIN_X_DAYS} "
                                 f"days ago. Not sending notification.")
                    return (f"Episode was added more than {EPISODE_PREMIERED_WITHIN_X_DAYS} "
                            f"days ago. Not sending notification.")

        if item_type == "MusicAlbum":
            if not item_already_notified(item_type, item_name, release_year):
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

                # Фиксируем уведомление как отправленное
                mark_item_as_notified(item_type, item_name, release_year)

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
