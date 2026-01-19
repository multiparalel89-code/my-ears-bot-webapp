import asyncio
import sqlite3
import logging
import io
import json
import aiohttp
import urllib.parse
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, \
    BufferedInputFile
from aiogram.exceptions import TelegramBadRequest
from PIL import Image, ImageDraw, ImageFont, ImageOps

# --- НАСТРОЙКИ ---
API_TOKEN = '8482829925:AAHOyK-aOIuCcs3AZJ4bceh7lZ_OuMsZfWY'  # ВАЖНО: Замени на свой токен!
ADMIN_ID = 7483204058  # ВАЖНО: Замени на свой ID администратора!
CHANNEL_ID = "@metakreo"  # Если нужен канал
CHANNEL_URL = "https://t.me/metakreo"

# --- BASE URL для GitHub Pages. Убедись, что он указывает на корневую директорию твоего репозитория! ---
# Например: "https://yourusername.github.io/your-repo-name/"
WEBAPP_BASE_URL = "https://cain.github.io/my-ears-bot-webapp/"

# !!! ЗАМЕНИ ЭТО !!!

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# --- БАЗА ДАННЫХ ---
DB_NAME = 'ears_pro.db'


def get_db():
    """Возвращает подключение к БД."""
    return sqlite3.connect(DB_NAME)


def init_db():
    """Инициализирует БД, если она не существует."""
    conn = get_db()
    c = conn.cursor()
    # Таблица фильмов
    c.execute('''CREATE TABLE IF NOT EXISTS movies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    director TEXT NOT NULL,
                    cover_url TEXT,
                    year INTEGER, -- Добавим год для сортировки
                    genres TEXT  -- Добавим жанры (можно хранить как JSON строку)
                )''')
    # Таблица пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT, -- Для удобства
                    rank TEXT DEFAULT 'СТАЖЕР',
                    is_banned INTEGER DEFAULT 0
                )''')
    # Таблица оценок (RZT стиль)
    c.execute('''CREATE TABLE IF NOT EXISTS ratings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    movie_id INTEGER NOT NULL,
                    score_total REAL, -- Общий балл
                    c1 INTEGER, -- Сюжет
                    c2 INTEGER, -- Актеры
                    c3 INTEGER, -- Визуал
                    c4 INTEGER, -- Атмосфера
                    comment TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (movie_id) REFERENCES movies(id)
                )''')
    # Таблица для списка "Хочу посмотреть" (watchlist)
    c.execute('''CREATE TABLE IF NOT EXISTS watchlist (
                    user_id INTEGER NOT NULL,
                    movie_id INTEGER NOT NULL,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, movie_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (movie_id) REFERENCES movies(id)
                )''')
    conn.commit()
    conn.close()
    logging.info("Database initialized.")


init_db()


# --- СОСТОЯНИЯ FSM ---
class MyStates(StatesGroup):
    search = State()
    broadcast = State()
    last_msg = State()  # Для хранения ID последнего сообщения бота для удаления


class AdminSt(StatesGroup):
    add_movie_title = State()
    add_movie_director = State()
    add_movie_cover = State()
    add_movie_year = State()
    add_movie_genres = State()


# --- УТИЛИТЫ ---
async def clear_chat(state: FSMContext, chat_id: int):
    """Удаляет последнее сообщение бота, если оно сохранено в состоянии."""
    data = await state.get_data()
    last_msg_id = data.get("last_msg")
    if last_msg_id:
        try:
            await bot.delete_message(chat_id, last_msg_id)
        except TelegramBadRequest:
            logging.warning(
                f"Failed to delete message {last_msg_id} in chat {chat_id}. It might be too old or already deleted.")
        finally:
            await state.update_data(last_msg=None)


# !!! ИЗМЕНЕНА ФУНКЦИЯ get_user_data !!!
async def get_user_data(user_id: int, username: str | None = None, first_name: str | None = None):
    """
    Получает данные пользователя из БД. Если пользователь не найден, добавляет его.
    Принимает username и first_name для первичного создания записи,
    чтобы избежать лишних API-запросов к Telegram.
    """
    conn = get_db()
    c = conn.cursor()

    # Проверяем, есть ли пользователь
    c.execute("SELECT username, first_name, rank FROM users WHERE user_id = ?", (user_id,))
    user_info = c.fetchone()

    if not user_info:
        # Если пользователя нет, добавляем его с предоставленными данными
        username_to_insert = username if username else "Unknown"
        first_name_to_insert = first_name if first_name else "Unknown"
        c.execute("INSERT INTO users (user_id, username, first_name, rank) VALUES (?, ?, ?, 'СТАЖЕР')",
                  (user_id, username_to_insert, first_name_to_insert))
        conn.commit()
        user_info = (username_to_insert, first_name_to_insert, 'СТАЖЕР')  # Возвращаем данные для новой записи

    review_count = c.execute("SELECT COUNT(*) FROM ratings WHERE user_id = ?", (user_id,)).fetchone()[0]
    watchlist_count = c.execute("SELECT COUNT(*) FROM watchlist WHERE user_id = ?", (user_id,)).fetchone()[0]
    conn.close()

    return {
        "username": user_info[0],
        "first_name": user_info[1],
        "rank": user_info[2],
        "review_count": review_count,
        "watchlist_count": watchlist_count
    }


# !!! ИЗМЕНЕНА ФУНКЦИЯ get_user_avatar_url !!!
async def get_user_avatar_url(user_id: int) -> str | None:
    """Получает URL аватара пользователя."""
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file_info = await bot.get_file(photos.photos[0][-1].file_id)
            return f"https://api.telegram.org/file/bot{API_TOKEN}/{file_info.file_path}"
    except Exception as e:
        # aiogram.Bot does not have get_user method, but get_user_profile_photos is still valid.
        # However, it might fail if user has no public photos.
        logging.error(f"Could not get avatar for user {user_id}: {e}")
        return None


def get_rank_data(count):
    """Определяет ранг и иконку пользователя по количеству обзоров."""
    if count < 5: return "СТАЖЕР-ОБОЗРЕВАТЕЛЬ", "🔰"
    if count < 15: return "АККРЕДИТОВАННЫЙ КРИТИК", "🎙"
    return "КИНОМАН-ЭКСПЕРТ", "🏛"


def url_encode_params(params: dict) -> str:
    """Кодирует параметры словаря для URL."""
    return "&".join([f"{key}={urllib.parse.quote_plus(str(value))}" for key, value in params.items()])


# --- ГЕНЕРАЦИЯ ГРАФИКИ (для ответов бота) ---
async def gen_start_img(uid, name, count):
    """Генерирует стартовую картинку для профиля пользователя."""
    rank, icon = get_rank_data(count)
    img = Image.new('RGBA', (1280, 720), (0, 0, 0, 255))
    draw = ImageDraw.Draw(img)

    try:
        photos = await bot.get_user_profile_photos(uid, limit=1)
        if photos.total_count > 0:
            file = await bot.get_file(photos.photos[0][-1].file_id)
            p_data = await bot.download_file(file.file_path)
            avatar = Image.open(p_data).convert("RGBA").resize((300, 300))
            mask = Image.new("L", (300, 300), 0)
            ImageDraw.Draw(mask).ellipse((0, 0, 300, 300), fill=255)
            avatar.putalpha(mask)
            img.paste(avatar, (100, 210), avatar)
            draw.ellipse((95, 205, 405, 515), outline=(229, 9, 20), width=10)
    except Exception as e:
        logging.error(f"Error generating avatar for {uid}: {e}")
        draw.ellipse((100, 210, 400, 510), fill=(30, 30, 30), outline=(255, 255, 255), width=5)

    try:
        # Убедись, что файл шрифта 'arial.ttf' доступен или используй другой
        f_huge = ImageFont.truetype("arial.ttf", 80)
        f_mid = ImageFont.truetype("arial.ttf", 50)
    except IOError:
        logging.warning("Arial font not found. Using default fonts.")
        f_huge = ImageFont.load_default()
        f_mid = ImageFont.load_default()

    draw.text((480, 230), f"{icon} {name.upper()}", font=f_huge, fill=(255, 255, 255))
    draw.text((480, 340), f"STATUS: {rank}", font=f_mid, fill=(229, 9, 20))
    draw.text((480, 420), f"REVIEWS: {count}", font=f_mid, fill=(150, 150, 150))

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format='JPEG', quality=95)
    return buf.getvalue()


# --- ОСНОВНЫЕ ХЕНДЛЕРЫ ---

@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    """Обрабатывает команду /start, инициализирует пользователя и выводит стартовое сообщение."""
    uid = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    # Передаем username и first_name, чтобы избежать get_chat() при первом создании
    user_data = await get_user_data(uid, username, first_name)

    img_data = await gen_start_img(uid, user_data["first_name"] or "КРИТИК", user_data["review_count"])

    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎬 КАТАЛОГ"), KeyboardButton(text="🔍 ПОИСК")],
        [KeyboardButton(text="👤 ПРОФИЛЬ"), KeyboardButton(text="❤️ СПИСОК")]
    ], resize_keyboard=True)

    await clear_chat(state, message.chat.id)
    caption = f"🎬 **ДОБРО ПОЖАЛОВАТЬ В METAKREO**\n──────────────\nВаш аккаунт синхронизирован. Выберите раздел:"
    msg = await message.answer_photo(photo=BufferedInputFile(img_data, filename="s.jpg"), caption=caption,
                                     reply_markup=kb,
                                     parse_mode="Markdown")
    await state.update_data(last_msg=msg.message_id)


@dp.message(F.text == "👤 ПРОФИЛЬ")
async def open_profile_webapp(message: types.Message, state: FSMContext):
    """Открывает профиль пользователя в WebApp."""
    uid = message.from_user.id
    # Эти данные уже есть, но get_user_data также убедится, что пользователь в БД
    user_data = await get_user_data(uid, message.from_user.username, message.from_user.first_name)
    avatar_url = await get_user_avatar_url(uid)

    profile_params = {
        "first_name": user_data["first_name"] or '',
        "username": user_data["username"] or '',
        "rank": user_data["rank"] or 'СТАЖЕР',
        "review_count": user_data["review_count"],
        "watchlist_count": user_data["watchlist_count"],
        "avatar_url": avatar_url or ''
    }
    profile_webapp_url = f"{WEBAPP_BASE_URL}profile.html?{url_encode_params(profile_params)}"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой Профиль в WebApp", web_app=types.WebAppInfo(url=profile_webapp_url))]
    ])

    await clear_chat(state, message.chat.id)
    msg = await message.answer(
        "👤 **ВАШ ПРОФИЛЬ**\n──────────────\n"
        "Нажмите кнопку, чтобы просмотреть свою карточку критика!.",
        reply_markup=kb, parse_mode="Markdown"
    )
    await state.update_data(last_msg=msg.message_id)


@dp.message(F.text == "🎬 КАТАЛОГ")
async def catalog(message: types.Message, state: FSMContext):
    """Показывает последние добавленные фильмы."""
    conn = get_db()
    movies = conn.execute(
        "SELECT id, title, director, cover_url, year, genres FROM movies ORDER BY id DESC LIMIT 10").fetchall()
    conn.close()

    await clear_chat(state, message.chat.id)
    if not movies:
        msg = await message.answer("🍿 **КАТАЛОГ ПУСТ**\nВоспользуйтесь поиском для нахождения других лент.",
                                   parse_mode="Markdown")
        return await state.update_data(last_msg=msg.message_id)

    btns = []
    for m in movies:
        movie_id, title, director, cover, year, genres = m
        btn_text = f"▫️ {title.upper()} ({year if year else 'N/A'})" if year else f"▫️ {title.upper()}"
        btns.append([InlineKeyboardButton(text=btn_text, callback_data=f"movie_{movie_id}")])

    kb = InlineKeyboardMarkup(inline_keyboard=btns)
    msg = await message.answer("🎬 **ПОСЛЕДНИЕ ПОСТУПЛЕНИЯ:**\nВыбери фильм из списка ниже:", reply_markup=kb)
    await state.update_data(last_msg=msg.message_id)


@dp.callback_query(F.data.startswith("movie_"))
async def movie_card_webapp(cb: types.CallbackQuery, state: FSMContext):
    """Обрабатывает нажатие на фильм в каталоге/поиске, открывает карточку фильма в WebApp."""
    mid = cb.data.split("_")[1]
    user_id = cb.from_user.id
    conn = get_db()
    c = conn.cursor()

    movie_data = c.execute("""
        SELECT m.id, m.title, m.director, m.cover_url, m.year, m.genres, AVG(r.score_total)
        FROM movies m
        LEFT JOIN ratings r ON m.id = r.movie_id
        WHERE m.id = ?
        GROUP BY m.id
    """, (mid,)).fetchone()

    # Проверяем, есть ли фильм в списке желаний пользователя
    in_watchlist = c.execute("SELECT 1 FROM watchlist WHERE user_id = ? AND movie_id = ?",
                             (user_id, mid)).fetchone() is not None

    conn.close()

    if not movie_data:
        await cb.answer("Фильм не найден.")
        return

    movie_id, title, director, cover_url, year, genres_str, avg_rating = movie_data
    avg_rating = round(avg_rating, 1) if avg_rating is not None else 0.0

    movie_params = {
        "mid": movie_id,
        "title": title,
        "director": director,
        "cover_url": cover_url or '',
        "year": year or '',
        "genres": genres_str or '',
        "avg_rating": f"{avg_rating:.1f}",
        "in_watchlist": "true" if in_watchlist else "false"  # Передаем статус в WebApp
    }
    movie_webapp_url = f"{WEBAPP_BASE_URL}movie_display.html?{url_encode_params(movie_params)}"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎬 ОТКРЫТЬ КАРТОЧКУ: {title.upper()}",
                              web_app=types.WebAppInfo(url=movie_webapp_url))]
    ])

    await clear_chat(state, cb.message.chat.id)
    msg = await cb.message.answer(
        f"🎬 **{title.upper()}**\n"
        f"Режиссер: `{director}`\n"
        f"Год: {year if year else 'N/A'}\n"
        f"Общий рейтинг: ⭐ `{avg_rating:.1f}/10`\n\n"
        f"Нажмите кнопку, чтобы посмотреть полную карточку фильма в WebApp.",
        reply_markup=kb, parse_mode="Markdown"
    )
    await state.update_data(last_msg=msg.message_id)


@dp.callback_query(F.data.startswith("dir_"))
async def dir_card_webapp(cb: types.CallbackQuery, state: FSMContext):
    """Обрабатывает нажатие на режиссера, открывает карточку режиссера в WebApp."""
    director_name = cb.data.split("_")[1]
    conn = get_db()
    movies_by_director_raw = conn.execute("""
        SELECT m.id, m.title, m.cover_url, m.director, AVG(r.score_total) as avg_rating
        FROM movies m
        LEFT JOIN ratings r ON m.id = r.movie_id
        WHERE m.director = ?
        GROUP BY m.id
    """, (director_name,)).fetchall()
    conn.close()

    if not movies_by_director_raw:
        await cb.answer("Режиссер не найден или у него нет оцененных фильмов.")
        return

    movies_for_json = []
    total_avg_rating = 0.0
    valid_ratings_count = 0

    for item in movies_by_director_raw:
        movie_id, title, cover_url, director, avg_rating_for_movie = item  # Добавил director
        movies_for_json.append({"id": movie_id, "title": title, "cover_url": cover_url or '', "director": director})
        if avg_rating_for_movie is not None:
            total_avg_rating += avg_rating_for_movie
            valid_ratings_count += 1

    avg_dir_rating = round(total_avg_rating / valid_ratings_count, 1) if valid_ratings_count > 0 else 0.0
    movies_json_str = json.dumps(movies_for_json, ensure_ascii=False)

    director_params = {
        "director_name": director_name,
        "avg_rating_dir": f"{avg_dir_rating:.1f}",
        "movies_json": movies_json_str
    }
    director_webapp_url = f"{WEBAPP_BASE_URL}director_display.html?{url_encode_params(director_params)}"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎥 КАРТОЧКА РЕЖИССЕРА: {director_name.upper()}",
                              web_app=types.WebAppInfo(url=director_webapp_url))]
    ])

    await clear_chat(state, cb.message.chat.id)
    msg = await cb.message.answer(
        f"🎥 **ДОСЬЕ РЕЖИССЕРА: {director_name.upper()}**\n"
        f"Средний рейтинг работ: ⭐ `{avg_dir_rating:.1f}/10`\n\n"
        f"Нажмите кнопку, чтобы просмотреть полную карточку режиссера со всеми его фильмами!.",
        reply_markup=kb, parse_mode="Markdown"
    )
    await state.update_data(last_msg=msg.message_id)


# --- СПИСОК ЖЕЛАНИЙ ---
@dp.message(F.text == "❤️ СПИСОК")
async def open_watchlist_webapp(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    user_data = await get_user_data(uid, message.from_user.username, message.from_user.first_name)

    conn = get_db()
    # Получаем все фильмы из списка желаний пользователя
    watchlist_movies_raw = conn.execute("""
        SELECT m.id, m.title, m.cover_url, m.director
        FROM watchlist wl
        JOIN movies m ON wl.movie_id = m.id
        WHERE wl.user_id = ?
        ORDER BY wl.added_at DESC
    """, (uid,)).fetchall()
    conn.close()

    movies_for_json = []
    for item in watchlist_movies_raw:
        movie_id, title, cover_url, director = item
        movies_for_json.append({"id": movie_id, "title": title, "cover_url": cover_url or '',
                                "director": director or 'Неизвестный режиссер'})

    movies_json_str = json.dumps(movies_for_json, ensure_ascii=False)

    watchlist_params = {
        "user_id": uid,
        "username": user_data["username"] or '',
        "first_name": user_data["first_name"] or '',
        "watchlist_count": user_data["watchlist_count"],
        "movies_json": movies_json_str  # Передаем список фильмов
    }
    watchlist_url = f"{WEBAPP_BASE_URL}watchlist.html?{url_encode_params(watchlist_params)}"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой список желаний!", web_app=types.WebAppInfo(url=watchlist_url))]
    ])

    await clear_chat(state, message.chat.id)
    msg = await message.answer(
        "❤️ **ВАШ СПИСОК ЖЕЛАНИЙ**\n──────────────\n"
        "Нажмите кнопку, чтобы просмотреть и управлять своим списком!.",
        reply_markup=kb, parse_mode="Markdown"
    )
    await state.update_data(last_msg=msg.message_id)


# --- ПОИСК ---
@dp.message(F.text == "🔍 ПОИСК")
async def start_search(message: types.Message, state: FSMContext):
    """Переводит в режим поиска."""
    await state.set_state(MyStates.search)
    await clear_chat(state, message.chat.id)
    msg = await message.answer("🔍 **РЕЖИМ ПОИСКА**\n──────────────\nВведите название фильма или имя режиссера:",
                               parse_mode="Markdown")
    await state.update_data(last_msg=msg.message_id)


@dp.message(MyStates.search)
async def do_search(message: types.Message, state: FSMContext):
    """Выполняет поиск фильмов/режиссеров."""
    q = f"%{message.text}%"
    conn = get_db()
    res = conn.execute("""
        SELECT id, title, director, year, genres
        FROM movies
        WHERE title LIKE ? OR director LIKE ? OR year LIKE ? OR genres LIKE ?
        LIMIT 15
    """, (q, q, q, q)).fetchall()
    conn.close()

    await clear_chat(state, message.chat.id)
    if not res:
        msg = await message.answer("❌ **НИЧЕГО НЕ НАЙДЕНО**\nПопробуйте другой запрос.", parse_mode="Markdown")
    else:
        btns = []
        for m in res:
            movie_id, title, director, year, genres_str = m
            year_info = f" ({year})" if year else ""
            genres_info = f" [{genres_str}]" if genres_str else ""
            btns.append([InlineKeyboardButton(text=f"{title.upper()}{year_info}{genres_info}",
                                              callback_data=f"movie_{movie_id}")])

        kb = InlineKeyboardMarkup(inline_keyboard=btns)
        msg = await message.answer(f"🔍 **РЕЗУЛЬТАТЫ ПОИСКА ({len(res)}):**", reply_markup=kb, parse_mode="Markdown")

    await state.update_data(last_msg=msg.message_id)
    await state.set_state(None)


# --- ОБРАБОТКА ДАННЫХ ИЗ WEBAPP (RZT ОЦЕНКА И WATCHLIST) ---
@dp.message(F.content_type == types.ContentType.WEB_APP_DATA)
async def web_app_receive(message: types.Message):
    """Принимает данные из WebApp: оценки, управление watchlist."""
    try:
        data = json.loads(message.web_app_data.data)
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON from WebApp data: {message.web_app_data.data}")
        await message.answer("Произошла ошибка при обработке ваших данных. Попробуйте еще раз.")
        return

    action = data.get("action")
    user_id = message.from_user.id

    if action == "watchlist":
        movie_id = data.get("movie_id")
        action_type = data.get("action_type")
        if not movie_id or not action_type:
            await message.answer("Некорректные данные для списка желаний.")
            return

        conn = get_db()
        c = conn.cursor()
        movie_title = c.execute("SELECT title FROM movies WHERE id = ?", (movie_id,)).fetchone()
        if not movie_title:
            await message.answer("Фильм не найден.")
            conn.close()
            return
        movie_title = movie_title[0]

        try:
            if action_type == "add":
                c.execute("INSERT OR IGNORE INTO watchlist (user_id, movie_id) VALUES (?, ?)", (user_id, movie_id))
                await message.answer(f"✅ Фильм '{movie_title}' добавлен в ваш список желаний!")
            elif action_type == "remove":
                c.execute("DELETE FROM watchlist WHERE user_id = ? AND movie_id = ?", (user_id, movie_id))
                await message.answer(f"🗑️ Фильм '{movie_title}' удален из вашего списка желаний.")
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Database error managing watchlist: {e}")
            await message.answer("Произошла ошибка при обновлении списка желаний. Попробуйте позже.")
            conn.rollback()
        finally:
            conn.close()

    elif action is None:  # Если action не указан, считаем это оценкой фильма
        mid = data.get("mid")
        c1, c2, c3, c4 = data.get('c1'), data.get('c2'), data.get('c3'), data.get('c4')
        comment = data.get('comm', '')

        if not mid or not all([isinstance(val, int) for val in [c1, c2, c3, c4]]):  # Проверяем, что это int
            logging.warning(f"Incomplete or non-numeric data received from WebApp for rating: {data}")
            await message.answer(
                "Не удалось получить полные или корректные данные оценки. Пожалуйста, попробуйте еще раз.")
            return

        try:
            avg_score = (c1 + c2 + c3 + c4) / 4
        except TypeError:
            logging.error(f"Non-numeric score values received: c1={c1}, c2={c2}, c3={c3}, c4={c4}")
            await message.answer("Получены некорректные числовые значения для оценки. Попробуйте еще раз.")
            return

        conn = get_db()
        c = conn.cursor()
        try:
            c.execute(
                "INSERT INTO ratings (user_id, movie_id, score_total, c1, c2, c3, c4, comment) VALUES (?,?,?,?,?,?,?,?)",
                (user_id, mid, avg_score, c1, c2, c3, c4, comment))

            m_title_data = c.execute("SELECT title FROM movies WHERE id = ?", (mid,)).fetchone()
            m_title = m_title_data[0] if m_title_data else "Неизвестный фильм"

            review_count = c.execute("SELECT COUNT(*) FROM ratings WHERE user_id = ?", (user_id,)).fetchone()[0]
            new_rank, _ = get_rank_data(review_count)
            current_rank_data = c.execute("SELECT rank FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if current_rank_data and current_rank_data[0] != new_rank:
                c.execute("UPDATE users SET rank = ? WHERE user_id = ?", (new_rank, user_id))
                await bot.send_message(message.chat.id, f"🎉 Поздравляем! Ваш ранг изменен на: {new_rank}")

            conn.commit()

            text = (f"✅ **ОЦЕНКА ПРИНЯТА!**\n\n"
                    f"🎬 Фильм: *{m_title}*\n"
                    f"📊 Средний балл: `{avg_score:.1f}/10`\n"
                    f"───\n"
                    f"🔸 Сюжет: {c1} | Актеры: {c2}\n"
                    f"🔸 Визуал: {c3} | Атмосфера: {c4}\n"
                    f"💬 Отзыв: _{comment}_\n\n"
                    f"Спасибо за ваш вклад в кинокритику EARS!")
            await message.answer(text, parse_mode="Markdown")

        except sqlite3.Error as e:
            logging.error(f"Database error while processing rating: {e}")
            await message.answer("Произошла ошибка при сохранении вашей оценки. Попробуйте позже.")
            conn.rollback()
        finally:
            conn.close()
    else:
        await message.answer("Неизвестное действие.")


# --- ДОБАВЛЕНИЕ ФИЛЬМОВ (Admin) ---
@dp.message(Command("admin"), F.from_user.id == ADMIN_ID)
async def admin_menu(message: types.Message):
    """Показывает админ-меню."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ ДОБАВИТЬ ФИЛЬМ", callback_data="add_movie")],
        # Можно добавить другие кнопки админки
    ])
    await message.answer("🛠 **ADMIN PANEL**", reply_markup=kb)


@dp.callback_query(F.data == "add_movie")
async def add_movie_start(cb: types.CallbackQuery, state: FSMContext):
    """Начинает процесс добавления фильма."""
    await state.set_state(AdminSt.add_movie_title)
    await cb.message.answer("Введите название фильма:")
    await cb.answer()


@dp.message(AdminSt.add_movie_title)
async def add_movie_title(m: types.Message, state: FSMContext):
    await state.update_data(title=m.text)
    await state.set_state(AdminSt.add_movie_director)
    await m.answer("Введите имя режиссера:")


@dp.message(AdminSt.add_movie_director)
async def add_movie_director(m: types.Message, state: FSMContext):
    await state.update_data(director=m.text)
    await state.set_state(AdminSt.add_movie_cover)
    await m.answer("Введите URL обложки фильма:")


@dp.message(AdminSt.add_movie_cover)
async def add_movie_cover(m: types.Message, state: FSMContext):
    await state.update_data(cover_url=m.text)
    await state.set_state(AdminSt.add_movie_year)
    await m.answer("Введите год выпуска (числом, если есть):")


@dp.message(AdminSt.add_movie_year)
async def add_movie_year(m: types.Message, state: FSMContext):
    year = int(m.text) if m.text and m.text.isdigit() else None  # Добавил проверку m.text.isdigit()
    await state.update_data(year=year)
    await state.set_state(AdminSt.add_movie_genres)
    await m.answer("Введите жанры через запятую (например: Боевик, Фантастика):")


@dp.message(AdminSt.add_movie_genres)
async def add_movie_genres(m: types.Message, state: FSMContext):
    data = await state.get_data()
    genres = m.text.strip() if m.text else None

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO movies (title, director, cover_url, year, genres) VALUES (?, ?, ?, ?, ?)",
                  (data['title'], data['director'], data.get('cover_url'), data.get('year'), genres))
        conn.commit()
        await m.answer(f"✅ Фильм '{data['title']}' успешно добавлен!")
    except sqlite3.Error as e:
        logging.error(f"Database error adding movie: {e}")
        await m.answer("❌ Ошибка при добавлении фильма в базу данных.")
    finally:
        conn.close()
    await state.clear()


# --- РАССЫЛКА (Admin) ---
@dp.message(Command("broadcast"), F.from_user.id == ADMIN_ID)
async def broadcast_start(message: types.Message, state: FSMContext):
    """Начинает рассылку."""
    await state.set_state(MyStates.broadcast)
    await message.answer("📝 Введите текст для рассылки:")


@dp.message(MyStates.broadcast)
async def broadcast_send(message: types.Message, state: FSMContext):
    """Отправляет сообщение всем пользователям."""
    text_to_send = message.text
    conn = get_db()
    users = conn.execute("SELECT user_id FROM users WHERE is_banned = 0").fetchall()
    conn.close()

    success_count = 0
    failure_count = 0
    for user_id, in users:
        try:
            await bot.send_message(user_id, text_to_send, parse_mode="Markdown")
            success_count += 1
            await asyncio.sleep(0.05)  # Небольшая пауза, чтобы не улететь в бан Telegram
        except Exception as e:
            logging.warning(f"Failed to send broadcast to {user_id}: {e}")
            failure_count += 1
            if "bot was blocked by the user" in str(e):  # Если пользователь заблокировал бота
                conn = get_db()
                conn.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (user_id,))
                conn.commit()
                conn.close()

    await message.answer(
        f"✅ Рассылка завершена.\nУспешно отправлено: {success_count}\nНе удалось отправить: {failure_count}")
    await state.clear()


# --- SPAWN (for web apps) ---
@dp.message(Command("spawn"))
async def spawn_webapp(message: types.Message, state: FSMContext):
    """Отправляет сообщение с кнопкой для открытия WebApp."""
    uid = message.from_user.id
    user_data = await get_user_data(uid, message.from_user.username,
                                    message.from_user.first_name)  # Ensure user is in DB

    # Пример: открываем список желаний пользователя
    watchlist_params = {
        "user_id": uid,
        "username": user_data["username"] or '',
        "first_name": user_data["first_name"] or '',
        "watchlist_count": user_data["watchlist_count"]
    }

    conn = get_db()
    watchlist_movies_raw = conn.execute("""
        SELECT m.id, m.title, m.cover_url, m.director
        FROM watchlist wl
        JOIN movies m ON wl.movie_id = m.id
        WHERE wl.user_id = ?
        ORDER BY wl.added_at DESC
    """, (uid,)).fetchall()
    conn.close()

    movies_for_json = []
    for item in watchlist_movies_raw:
        movie_id, title, cover_url, director = item
        movies_for_json.append({"id": movie_id, "title": title, "cover_url": cover_url or '',
                                "director": director or 'Неизвестный режиссер'})

    watchlist_params["movies_json"] = json.dumps(movies_for_json, ensure_ascii=False)

    watchlist_url = f"{WEBAPP_BASE_URL}watchlist.html?{url_encode_params(watchlist_params)}"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть страницу оценки (пример)",
                              web_app=types.WebAppInfo(url=f"{WEBAPP_BASE_URL}index.html?mid=1"))],
        [InlineKeyboardButton(text="Открыть мой список желаний", web_app=types.WebAppInfo(url=watchlist_url))]
    ])
    await message.answer("Нажми кнопку, чтобы открыть WebApp:", reply_markup=kb)


async def main():
    logging.info("Bot starting...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
