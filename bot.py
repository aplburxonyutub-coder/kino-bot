"""Kino bot — aiogram 3 + aiosqlite. Railway uchun tayyor.

Yangi imkoniyatlar:
  1) Kino NOMI bo'yicha matnli qidiruv (katta-kichik harfga va apostrof turiga bog'liq emas).
  2) AI (Gemini Vision) orqali kinoni aniqlash: Instagram Reels havolasi, video yoki skrinshot.
     Oddiy foydalanuvchilar uchun kunlik limit (standart: 3 ta).
"""
import asyncio
import html
import json
import logging
import os
import re
import shutil
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandObject, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaVideo,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    TelegramObject,
)
from pydantic import BaseModel

try:  # lokal ishlatishda .env faylini o'qish (Railway'da shart emas)
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger("kino_bot")

# =====================================================================
# 1. SOZLAMALAR (muhit o'zgaruvchilaridan o'qiladi)
# =====================================================================


def _int_list(value: str) -> List[int]:
    return [int(x) for x in value.replace(" ", "").split(",") if x.lstrip("-").isdigit()]


def _str_list(value: str) -> List[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = _int_list(os.getenv("ADMIN_IDS", ""))
DB_PATH = os.getenv("DB_PATH", "kino_bot.db")  # Railway'da: /data/kino_bot.db (Volume)
VIP_REFERRAL_COUNT = int(os.getenv("VIP_REFERRAL_COUNT", "3"))
THROTTLE_MESSAGE = float(os.getenv("THROTTLE_MESSAGE", "1.0"))
THROTTLE_CALLBACK = float(os.getenv("THROTTLE_CALLBACK", "0.5"))
PROTECT_CONTENT = os.getenv("PROTECT_CONTENT", "0") == "1"  # videoni uzatish/saqlashni taqiqlash
# 1 bo'lsa adminlar ham majburiy obunadan o'tadi (obunani o'zingiz sinab ko'rish uchun)
ADMIN_CHECK_SUB = os.getenv("ADMIN_CHECK_SUB", "0") == "1"

# --- AI qidiruv sozlamalari ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
# Eslatma: gemini-1.5-* modellari Google tomonidan o'chirilgan. Model nomini Railway Variables'da
# GEMINI_MODEL orqali almashtirish mumkin (kod o'zgarmaydi). Birinchisi ishlamasa, keyingilari sinab ko'riladi.
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.8-flash").strip()
GEMINI_FALLBACK_MODELS = _str_list(os.getenv("GEMINI_FALLBACK_MODELS", "gemini-3.5-flash-lite,gemini-2.5-flash-lite"))
AI_DAILY_LIMIT_FREE = int(os.getenv("AI_DAILY_LIMIT_FREE", "3"))  # oddiy foydalanuvchi, kuniga
AI_DAILY_LIMIT_VIP = int(os.getenv("AI_DAILY_LIMIT_VIP", "20"))  # VIP (referal) foydalanuvchi; 0 = cheksiz
AI_MAX_PARALLEL = int(os.getenv("AI_MAX_PARALLEL", "2"))  # bir vaqtda nechta yuklash/kadr kesish jarayoni
MAX_REEL_MB = int(os.getenv("MAX_REEL_MB", "50"))  # Instagram videosi uchun maksimal hajm
# Instagram ko'p hollarda serverlardan kirishni bloklaydi. Kerak bo'lsa cookies.txt (Netscape format) yo'li:
INSTAGRAM_COOKIES_FILE = os.getenv("INSTAGRAM_COOKIES_FILE", "").strip()
MAX_TG_DOWNLOAD = 20 * 1024 * 1024  # Telegram Bot API faqat 20 MB gacha faylni yuklab berishga ruxsat beradi
ALL_MOVIES_PAGE_SIZE = 20  # "Barcha kinolar" ro'yxatida bir sahifadagi kinolar soni
SEARCH_RESULTS_LIMIT = 8  # nom bo'yicha qidiruvda ko'rsatiladigan maksimal tugmalar soni

# Kunlik limit O'zbekiston vaqti (UTC+5) bilan yangilanadi (Railway serveri UTC'da ishlaydi)
UZ_TZ = timezone(timedelta(hours=5))

# Kino kodi: faqat lotin harf, raqam, _ va - (1..32 belgi)
CODE_PATTERN = r"^[A-Za-z0-9_\-]{1,32}$"
# Instagram havolasi (reel / post / tv)
IG_URL_RE = re.compile(
    r"https?://(?:www\.|m\.)?(?:instagram\.com|instagr\.am)/(?:[A-Za-z0-9_.]+/)?(?:reels?|p|tv)/[A-Za-z0-9_\-]+[^\s]*",
    re.IGNORECASE,
)

NOT_FOUND_TEXT = (
    "Kechirasiz, bu nomdagi kino topilmadi. Kino kodini kiritib ko'ring yoki nomini to'g'ri yozganingizni tekshiring."
)

esc = html.escape


def _today_uz() -> str:
    return datetime.now(UZ_TZ).date().isoformat()


def _first_line(text: str, limit: int = 100) -> str:
    line = (text or "").strip().split("\n")[0]
    return line if len(line) <= limit else line[: limit - 1] + "…"


def _clip(text: str, limit: int = 800) -> str:
    """Telegram sarlavhasi 1024 belgidan oshmasligi uchun uzun matnni qisqartiradi."""
    text = text or ""
    return text if len(text) <= limit else text[: limit - 1] + "…"


def normalize_text(text: Optional[str]) -> str:
    """Qidiruv uchun matnni bir xil ko'rinishga keltiradi:
    kichik harf (kirill/lotin hammasi uchun), apostroflar olib tashlanadi (o', o‘, oʻ -> o),
    tinish belgilari bo'sh joyga aylanadi. SQLite'ning LIKE'i ASCII'dan boshqa harflarda katta-kichikni
    ajratadi, shuning uchun qidiruv shu normallashtirilgan ustunda olib boriladi."""
    text = (text or "").casefold()
    for ch in "'`’‘ʻʼ´ʹ":
        text = text.replace(ch, "")
    text = re.sub(r"[\W_]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def build_search_text(title: str, original_title: Optional[str]) -> str:
    """Qidiriladigan matn: sarlavhaning 1-qatori (kino nomi) + original nomi."""
    return normalize_text(f"{_first_line(title, 200)} {original_title or ''}")


# =====================================================================
# 2. MA'LUMOTLAR BAZASI (bitta umumiy ulanish + WAL + lock)
# =====================================================================

_db: Optional[aiosqlite.Connection] = None
_db_lock = asyncio.Lock()

MOVIE_COLS = "code, title, file_id, is_vip, views, likes, dislikes"
Movie = Tuple[str, str, str, int, int, int, int]


async def _columns(table: str) -> set:
    cur = await _db.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in await cur.fetchall()}


async def init_db() -> None:
    global _db
    folder = os.path.dirname(DB_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)

    _db = await aiosqlite.connect(DB_PATH, timeout=30)
    await _db.execute("PRAGMA journal_mode=WAL")
    await _db.execute("PRAGMA synchronous=NORMAL")
    await _db.execute("PRAGMA busy_timeout=30000")

    await _db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            joined_date TEXT,
            invited_by INTEGER DEFAULT NULL,
            referrals_count INTEGER DEFAULT 0,
            referral_counted INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
    """)
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            code TEXT PRIMARY KEY,
            title TEXT,
            file_id TEXT,
            is_vip INTEGER DEFAULT 0,
            views INTEGER DEFAULT 0,
            likes INTEGER DEFAULT 0,
            dislikes INTEGER DEFAULT 0,
            poster_id TEXT,
            original_title TEXT,
            search_text TEXT
        )
    """)
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            user_id INTEGER,
            movie_code TEXT,
            vote_type TEXT,
            PRIMARY KEY (user_id, movie_code)
        )
    """)
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            title TEXT,
            invite_link TEXT
        )
    """)
    # Serial qismlari: 1-qism movies.file_id da, 2-qismdan boshlab shu jadvalda
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS episodes (
            movie_code TEXT,
            part INTEGER,
            file_id TEXT,
            PRIMARY KEY (movie_code, part)
        )
    """)
    # AI qidiruvning kunlik hisobi
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS ai_usage (
            user_id INTEGER,
            day TEXT,
            used INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, day)
        )
    """)

    # Eski bazani yangilash (migratsiya)
    cols = await _columns("users")
    if "referral_counted" not in cols:
        await _db.execute("ALTER TABLE users ADD COLUMN referral_counted INTEGER DEFAULT 0")
        # eski referallar allaqachon hisoblangan — qayta sanalmasin
        await _db.execute("UPDATE users SET referral_counted = 1 WHERE invited_by IS NOT NULL")
    if "is_active" not in cols:
        await _db.execute("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1")

    mcols = await _columns("movies")
    if "poster_id" not in mcols:
        await _db.execute("ALTER TABLE movies ADD COLUMN poster_id TEXT")
    if "original_title" not in mcols:
        await _db.execute("ALTER TABLE movies ADD COLUMN original_title TEXT")
    if "search_text" not in mcols:
        await _db.execute("ALTER TABLE movies ADD COLUMN search_text TEXT")

    # Eski kinolar uchun qidiruv matnini to'ldirish
    cur = await _db.execute("SELECT code, title, original_title FROM movies WHERE search_text IS NULL")
    rows = await cur.fetchall()
    if rows:
        await _db.executemany(
            "UPDATE movies SET search_text = ? WHERE code = ?",
            [(build_search_text(title, orig), code) for code, title, orig in rows],
        )

    # 14 kundan eski AI hisoblarini tozalash
    old_day = (datetime.now(UZ_TZ).date() - timedelta(days=14)).isoformat()
    await _db.execute("DELETE FROM ai_usage WHERE day < ?", (old_day,))

    await _db.commit()


async def close_db() -> None:
    if _db is not None:
        await _db.close()


async def add_user(user_id: int, invited_by: Optional[int] = None) -> bool:
    """Yangi foydalanuvchi qo'shadi. Referal bu yerda HISOBLANMAYDI —
    obuna tasdiqlangandan keyin confirm_referral() hisoblaydi."""
    async with _db_lock:
        cur = await _db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        if await cur.fetchone():
            await _db.execute("UPDATE users SET is_active = 1 WHERE user_id = ?", (user_id,))
            await _db.commit()
            return False

        valid_inviter = None
        if invited_by and invited_by != user_id:
            cur = await _db.execute("SELECT 1 FROM users WHERE user_id = ?", (invited_by,))
            if await cur.fetchone():
                valid_inviter = invited_by

        await _db.execute(
            "INSERT INTO users (user_id, joined_date, invited_by) VALUES (?, ?, ?)",
            (user_id, date.today().isoformat(), valid_inviter),
        )
        await _db.commit()
        return True


async def confirm_referral(user_id: int) -> Optional[Tuple[int, int]]:
    """Obuna tasdiqlangach referalni hisoblaydi. (taklif qilgan odam, uning jami soni) yoki None."""
    async with _db_lock:
        cur = await _db.execute(
            "SELECT invited_by, referral_counted FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cur.fetchone()
        if not row or not row[0] or row[1]:
            return None
        inviter = row[0]
        await _db.execute("UPDATE users SET referral_counted = 1 WHERE user_id = ?", (user_id,))
        await _db.execute(
            "UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id = ?", (inviter,)
        )
        await _db.commit()
        cur = await _db.execute("SELECT referrals_count FROM users WHERE user_id = ?", (inviter,))
        count = (await cur.fetchone())[0]
        return inviter, count


async def get_user_referrals(user_id: int) -> int:
    cur = await _db.execute("SELECT referrals_count FROM users WHERE user_id = ?", (user_id,))
    row = await cur.fetchone()
    return row[0] if row else 0


async def get_all_users() -> List[int]:
    cur = await _db.execute("SELECT user_id FROM users WHERE is_active = 1")
    return [row[0] for row in await cur.fetchall()]


async def mark_inactive(user_ids: List[int]) -> None:
    if not user_ids:
        return
    async with _db_lock:
        await _db.executemany(
            "UPDATE users SET is_active = 0 WHERE user_id = ?", [(uid,) for uid in user_ids]
        )
        await _db.commit()


async def get_statistics() -> Tuple[int, int, int, int]:
    """(jami, faol, bugungi yangilar, jami kinolar)"""
    today = date.today().isoformat()
    cur = await _db.execute("SELECT COUNT(*) FROM users")
    total = (await cur.fetchone())[0]
    cur = await _db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
    active = (await cur.fetchone())[0]
    cur = await _db.execute("SELECT COUNT(*) FROM users WHERE joined_date = ?", (today,))
    today_users = (await cur.fetchone())[0]
    cur = await _db.execute("SELECT COUNT(*) FROM movies")
    movies = (await cur.fetchone())[0]
    return total, active, today_users, movies


async def add_movie(
    code: str, title: str, file_id: str, is_vip: int, original_title: Optional[str] = None
) -> None:
    async with _db_lock:
        await _db.execute("DELETE FROM episodes WHERE movie_code = ?", (code,))
        await _db.execute(
            "INSERT OR REPLACE INTO movies "
            "(code, title, file_id, is_vip, views, likes, dislikes, original_title, search_text) "
            "VALUES (?, ?, ?, ?, 0, 0, 0, ?, ?)",
            (code, title, file_id, is_vip, original_title, build_search_text(title, original_title)),
        )
        await _db.commit()


async def set_original_title(code: str, original_title: str) -> bool:
    """Mavjud kinoning original nomini o'rnatadi va qidiruv matnini yangilaydi."""
    async with _db_lock:
        cur = await _db.execute("SELECT title FROM movies WHERE code = ?", (code,))
        row = await cur.fetchone()
        if not row:
            return False
        await _db.execute(
            "UPDATE movies SET original_title = ?, search_text = ? WHERE code = ?",
            (original_title, build_search_text(row[0], original_title), code),
        )
        await _db.commit()
        return True


async def get_movie(code: str) -> Optional[Movie]:
    cur = await _db.execute(f"SELECT {MOVIE_COLS} FROM movies WHERE code = ?", (code,))
    return await cur.fetchone()


async def get_random_movie() -> Optional[Movie]:
    cur = await _db.execute(f"SELECT {MOVIE_COLS} FROM movies ORDER BY RANDOM() LIMIT 1")
    return await cur.fetchone()


async def search_movies(query: str, limit: int = SEARCH_RESULTS_LIMIT) -> List[Movie]:
    """Nom bo'yicha qidiruv (LIKE %so'z%). Barcha so'zlar bo'lishi kerak (tartib muhim emas);
    butun ibora to'liq mos kelganlar va ko'p ko'rilganlar yuqoriga chiqadi."""
    norm = normalize_text(query)
    if len(norm) < 2:
        return []
    words = norm.split()[:6]
    where = " AND ".join("search_text LIKE ?" for _ in words)
    params: List[Any] = [f"%{w}%" for w in words]
    cur = await _db.execute(
        f"SELECT {MOVIE_COLS} FROM movies WHERE {where} "
        f"ORDER BY (search_text LIKE ?) DESC, views DESC LIMIT ?",
        (*params, f"%{norm}%", limit),
    )
    return await cur.fetchall()


async def search_movies_multi(queries: List[str], limit: int = SEARCH_RESULTS_LIMIT) -> List[Movie]:
    """Bir nechta nom variantlari (original / o'zbekcha) bo'yicha qidirib, takrorlarsiz birlashtiradi."""
    seen: set = set()
    out: List[Movie] = []
    for q in queries:
        for movie in await search_movies(q, limit):
            if movie[0] not in seen:
                seen.add(movie[0])
                out.append(movie)
    return out[:limit]


async def count_movies() -> int:
    cur = await _db.execute("SELECT COUNT(*) FROM movies")
    return (await cur.fetchone())[0]


async def get_movies_page(offset: int, limit: int) -> List[tuple]:
    """(kod, sarlavha, VIP) — eng ko'p ko'rilganlar yuqorida."""
    cur = await _db.execute(
        "SELECT code, title, is_vip FROM movies ORDER BY views DESC, code LIMIT ? OFFSET ?",
        (limit, offset),
    )
    return await cur.fetchall()


async def set_poster(code: str, poster_id: Optional[str]) -> None:
    async with _db_lock:
        await _db.execute("UPDATE movies SET poster_id = ? WHERE code = ?", (poster_id, code))
        await _db.commit()


async def get_poster(code: str) -> Optional[str]:
    cur = await _db.execute("SELECT poster_id FROM movies WHERE code = ?", (code,))
    row = await cur.fetchone()
    return row[0] if row and row[0] else None


async def count_parts(code: str) -> int:
    """Kinoning jami qismlari soni (oddiy kino uchun 1)."""
    cur = await _db.execute("SELECT COUNT(*) FROM episodes WHERE movie_code = ?", (code,))
    return 1 + (await cur.fetchone())[0]


async def get_part_file(code: str, part: int) -> Optional[str]:
    """Berilgan qismning file_id si (yo'q bo'lsa None)."""
    if part < 1:
        return None
    if part == 1:
        cur = await _db.execute("SELECT file_id FROM movies WHERE code = ?", (code,))
    else:
        cur = await _db.execute(
            "SELECT file_id FROM episodes WHERE movie_code = ? AND part = ?", (code, part)
        )
    row = await cur.fetchone()
    return row[0] if row else None


async def add_episode(code: str, file_id: str) -> int:
    """Kinoga keyingi qismni qo'shadi va uning raqamini qaytaradi."""
    async with _db_lock:
        cur = await _db.execute(
            "SELECT COALESCE(MAX(part), 1) FROM episodes WHERE movie_code = ?", (code,)
        )
        part = (await cur.fetchone())[0] + 1
        await _db.execute(
            "INSERT INTO episodes (movie_code, part, file_id) VALUES (?, ?, ?)",
            (code, part, file_id),
        )
        await _db.commit()
        return part


async def increment_views(code: str) -> None:
    async with _db_lock:
        await _db.execute("UPDATE movies SET views = views + 1 WHERE code = ?", (code,))
        await _db.commit()


async def delete_movie(code: str) -> bool:
    async with _db_lock:
        await _db.execute("DELETE FROM votes WHERE movie_code = ?", (code,))
        await _db.execute("DELETE FROM episodes WHERE movie_code = ?", (code,))
        cur = await _db.execute("DELETE FROM movies WHERE code = ?", (code,))
        deleted = cur.rowcount > 0
        await _db.commit()
        return deleted


async def get_top_movies(limit: int = 5):
    cur = await _db.execute(
        "SELECT code, title, views, is_vip FROM movies ORDER BY views DESC LIMIT ?", (limit,)
    )
    top_viewed = await cur.fetchall()
    cur = await _db.execute(
        "SELECT code, title, likes, is_vip FROM movies ORDER BY likes DESC LIMIT ?", (limit,)
    )
    top_liked = await cur.fetchall()
    return top_viewed, top_liked


async def vote_movie(user_id: int, code: str, vote_type: str) -> Tuple[str, int, int]:
    """Natija: ('ok'|'same'|'missing', likes, dislikes)"""
    async with _db_lock:
        cur = await _db.execute("SELECT likes, dislikes FROM movies WHERE code = ?", (code,))
        if not await cur.fetchone():
            return "missing", 0, 0

        cur = await _db.execute(
            "SELECT vote_type FROM votes WHERE user_id = ? AND movie_code = ?", (user_id, code)
        )
        existing = await cur.fetchone()

        if existing and existing[0] == vote_type:
            return "same", 0, 0

        if existing:
            await _db.execute(
                "UPDATE votes SET vote_type = ? WHERE user_id = ? AND movie_code = ?",
                (vote_type, user_id, code),
            )
            if vote_type == "like":
                await _db.execute(
                    "UPDATE movies SET likes = likes + 1, dislikes = MAX(dislikes - 1, 0) WHERE code = ?",
                    (code,),
                )
            else:
                await _db.execute(
                    "UPDATE movies SET dislikes = dislikes + 1, likes = MAX(likes - 1, 0) WHERE code = ?",
                    (code,),
                )
        else:
            await _db.execute(
                "INSERT INTO votes (user_id, movie_code, vote_type) VALUES (?, ?, ?)",
                (user_id, code, vote_type),
            )
            column = "likes" if vote_type == "like" else "dislikes"
            await _db.execute(f"UPDATE movies SET {column} = {column} + 1 WHERE code = ?", (code,))

        await _db.commit()
        cur = await _db.execute("SELECT likes, dislikes FROM movies WHERE code = ?", (code,))
        likes, dislikes = await cur.fetchone()
        return "ok", likes, dislikes


async def add_channel(channel_id: str, title: str, invite_link: str) -> None:
    async with _db_lock:
        await _db.execute(
            "INSERT OR REPLACE INTO channels (channel_id, title, invite_link) VALUES (?, ?, ?)",
            (channel_id, title, invite_link),
        )
        await _db.commit()


async def get_channels() -> List[tuple]:
    cur = await _db.execute("SELECT channel_id, title, invite_link FROM channels")
    return await cur.fetchall()


async def delete_channel(channel_id: str) -> bool:
    async with _db_lock:
        cur = await _db.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
        await _db.commit()
        return cur.rowcount > 0


# --- AI qidiruv limiti ---


async def try_consume_ai(user_id: int, limit: int) -> Tuple[bool, int]:
    """Bitta AI urinishini ATOMIK ravishda band qiladi (bir vaqtda ko'p so'rov yuborib limitni
    aylanib o'tib bo'lmasligi uchun). limit <= 0 — cheksiz.
    Qaytaradi: (ruxsat berildimi, bugungi jami ishlatilgan soni)."""
    day = _today_uz()
    async with _db_lock:
        cur = await _db.execute(
            "SELECT used FROM ai_usage WHERE user_id = ? AND day = ?", (user_id, day)
        )
        row = await cur.fetchone()
        used = row[0] if row else 0
        if limit > 0 and used >= limit:
            return False, used
        await _db.execute(
            "INSERT INTO ai_usage (user_id, day, used) VALUES (?, ?, 1) "
            "ON CONFLICT(user_id, day) DO UPDATE SET used = used + 1",
            (user_id, day),
        )
        await _db.commit()
        return True, used + 1


async def refund_ai(user_id: int) -> None:
    """Texnik xatolik (yuklab bo'lmadi, Gemini javob bermadi) bo'lsa, urinishni qaytarib beradi."""
    async with _db_lock:
        await _db.execute(
            "UPDATE ai_usage SET used = MAX(used - 1, 0) WHERE user_id = ? AND day = ?",
            (user_id, _today_uz()),
        )
        await _db.commit()


# =====================================================================
# 3. FSM HOLATLARI
# =====================================================================


class UserStates(StatesGroup):
    waiting_for_code = State()


class AdminStates(StatesGroup):
    add_movie_code = State()
    add_movie_title = State()
    add_movie_original = State()
    add_movie_type = State()
    add_movie_poster = State()
    add_movie_video = State()
    add_movie_more = State()
    add_episode_code = State()
    add_episode_video = State()
    add_poster_code = State()
    add_poster_photo = State()

    delete_movie_code = State()

    broadcast_type = State()
    broadcast_message = State()

    add_channel_id = State()
    add_channel_title = State()
    add_channel_link = State()


# =====================================================================
# 4. TUGMALAR
# =====================================================================


def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎬 Kino qidirish"), KeyboardButton(text="🎲 Tasodifiy kino")],
            [KeyboardButton(text="👑 VIP holat"), KeyboardButton(text="🏆 Top kinolar")],
            [KeyboardButton(text="📚 Barcha kinolar")],
        ],
        resize_keyboard=True,
    )


def admin_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Statistika"), KeyboardButton(text="📢 Broadcast")],
            [KeyboardButton(text="🎬 Kino qo'shish"), KeyboardButton(text="➕ Qism qo'shish")],
            [KeyboardButton(text="🖼 Rasm qo'shish"), KeyboardButton(text="🗑 Kino o'chirish")],
            [KeyboardButton(text="🔗 Kanal qo'shish"), KeyboardButton(text="❌ Kanal o'chirish")],
            [KeyboardButton(text="🏠 Foydalanuvchi menyusi")],
        ],
        resize_keyboard=True,
    )


def cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Bekor qilish")]], resize_keyboard=True
    )


def sub_channels_kb(channels: List[tuple]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"➕ {title}", url=link)] for _, title, link in channels]
    rows.append([InlineKeyboardButton(text="✅ A'zo bo'ldim", callback_data="check_subs")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def movie_action_kb(
    code: str, likes: int, dislikes: int, total: int = 1, current: int = 1
) -> InlineKeyboardMarkup:
    rows = []
    if total > 1:
        buttons = [
            InlineKeyboardButton(
                text=(f"▶️ {n}" if n == current else str(n)),
                callback_data=f"part:{code}:{n}",
            )
            for n in range(1, total + 1)
        ]
        for i in range(0, len(buttons), 5):
            rows.append(buttons[i:i + 5])
    rows.append([
        InlineKeyboardButton(text=f"👍 {likes}", callback_data=f"vote:like:{code}:{current}"),
        InlineKeyboardButton(text=f"👎 {dislikes}", callback_data=f"vote:dislike:{code}:{current}"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def search_results_kb(movies: List[Movie]) -> InlineKeyboardMarkup:
    """Nom bo'yicha topilgan bir nechta kinodan birini tanlash tugmalari."""
    rows = []
    for code, title, _file_id, is_vip, _views, _likes, _dislikes in movies:
        label = ("👑 " if is_vip else "🎬 ") + _first_line(title, 50)
        rows.append([InlineKeyboardButton(text=label, callback_data=f"movie:{code}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def all_movies_kb(page: int, pages: int) -> Optional[InlineKeyboardMarkup]:
    if pages <= 1:
        return None
    row = []
    if page > 1:
        row.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"list:{page - 1}"))
    row.append(InlineKeyboardButton(text=f"{page} / {pages}", callback_data="list:noop"))
    if page < pages:
        row.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"list:{page + 1}"))
    return InlineKeyboardMarkup(inline_keyboard=[row])


def more_parts_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="➕ Yana qism qo'shish", callback_data="epadd:more"),
            InlineKeyboardButton(text="✅ Tugatish", callback_data="epadd:done"),
        ]]
    )


def poster_skip_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⏭ O'tkazib yuborish", callback_data="poster:skip")]]
    )


def original_skip_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⏭ O'tkazib yuborish", callback_data="orig:skip")]]
    )


def movie_type_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="🟢 Oddiy kino", callback_data="mtype:0"),
            InlineKeyboardButton(text="👑 VIP kino", callback_data="mtype:1"),
        ]]
    )


def broadcast_type_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="📝 Oddiy xabar (Copy)", callback_data="bctype:copy"),
            InlineKeyboardButton(text="↪️ Forward xabar", callback_data="bctype:forward"),
        ]]
    )


def delete_channels_kb(channels: List[tuple]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"🗑 {title} ({cid})", callback_data=f"delchan:{cid}")]
        for cid, title, _ in channels
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =====================================================================
# 5. ANTI-SPAM (THROTTLING)
# =====================================================================


class ThrottlingMiddleware(BaseMiddleware):
    """Xabarlar va tugmalar uchun alohida limit. Adminlarga cheklov yo'q."""

    def __init__(self, limit: float):
        self.limit = limit
        self.last: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user is None or user.id in ADMIN_IDS:
            return await handler(event, data)

        now = time.monotonic()
        if now - self.last.get(user.id, 0.0) < self.limit:
            if isinstance(event, CallbackQuery):
                await event.answer("⏳ Biroz sekinroq!")
            return None  # xabarlar jim tashlab yuboriladi (spamga javob yozilmaydi)

        if len(self.last) > 20000:  # xotira to'lib ketmasligi uchun tozalash
            cutoff = now - 60
            self.last = {k: v for k, v in self.last.items() if v > cutoff}

        self.last[user.id] = now
        return await handler(event, data)


# =====================================================================
# 6. XATOLARNI USHLASH
# =====================================================================

error_router = Router()
_last_error_notice = 0.0


@error_router.error()
async def on_error(event: ErrorEvent, bot: Bot):
    global _last_error_notice
    logger.error("Kutilmagan xatolik", exc_info=event.exception)

    now = time.monotonic()
    if now - _last_error_notice > 60:  # adminlarni spam qilmaslik
        _last_error_notice = now
        text = f"⚠️ <b>Botda xatolik:</b>\n<code>{esc(repr(event.exception)[:400])}</code>"
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, text)
            except Exception:
                pass
    return True


# =====================================================================
# 7. ADMIN PANEL
# =====================================================================

admin_router = Router()
admin_router.message.filter(F.from_user.id.in_(ADMIN_IDS))
admin_router.callback_query.filter(F.from_user.id.in_(ADMIN_IDS))

_bg_tasks: set = set()


def _chat_ref(raw: str):
    """'-100123' -> int, 'kanal' -> '@kanal', '@kanal' -> '@kanal'"""
    raw = raw.strip()
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw if raw.startswith("@") else f"@{raw}"


@admin_router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👨‍💻 <b>Admin panelga xush kelibsiz!</b>\nKerakli bo'limni tanlang:",
        reply_markup=admin_menu_kb(),
    )


@admin_router.message(Command("orig"))
async def cmd_set_original(message: Message, command: CommandObject):
    """/orig KOD Original nomi — mavjud kinoga original nom qo'shish (AI qidiruv aniqroq ishlashi uchun)."""
    parts = (command.args or "").split(maxsplit=1)
    if len(parts) != 2:
        await message.answer(
            "ℹ️ Foydalanish: <code>/orig KOD Original nomi</code>\n"
            "Masalan: <code>/orig 105 Avengers: Endgame</code>"
        )
        return
    code, original = parts[0], parts[1].strip()
    if await set_original_title(code, original):
        await message.answer(f"✅ <code>{esc(code)}</code> kinosining original nomi saqlandi: <b>{esc(original)}</b>")
    else:
        await message.answer(f"❌ <code>{esc(code)}</code> kodli kino topilmadi.")


@admin_router.message(F.text == "🏠 Foydalanuvchi menyusi")
async def back_to_user_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🏠 Asosiy menyuga qaytdingiz:", reply_markup=main_menu_kb())


@admin_router.message(StateFilter("*"), F.text == "❌ Bekor qilish")
async def cancel_admin_action(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Jarayon bekor qilindi.", reply_markup=admin_menu_kb())


@admin_router.message(F.text == "📊 Statistika")
async def show_stats(message: Message):
    total, active, today_users, movies = await get_statistics()
    await message.answer(
        "📊 <b>Bot Statistikasi:</b>\n\n"
        f"👥 Jami foydalanuvchilar: <b>{total} ta</b>\n"
        f"✅ Faol (botni bloklamagan): <b>{active} ta</b>\n"
        f"🆕 Bugun qo'shilganlar: <b>{today_users} ta</b>\n"
        f"🎬 Bazadagi jami kinolar: <b>{movies} ta</b>"
    )


# --- Kino qo'shish ---
@admin_router.message(F.text == "🎬 Kino qo'shish")
async def start_add_movie(message: Message, state: FSMContext):
    await state.set_state(AdminStates.add_movie_code)
    await message.answer(
        "🔢 <b>Kino uchun kod kiriting</b> (lotin harf/raqam, masalan: <code>105</code>):",
        reply_markup=cancel_kb(),
    )


@admin_router.message(AdminStates.add_movie_code, F.text)
async def process_movie_code(message: Message, state: FSMContext):
    code = message.text.strip()
    if not re.match(CODE_PATTERN, code):
        await message.answer(
            "⚠️ <b>Kod noto'g'ri!</b> Faqat lotin harflari, raqam, <code>_</code> va <code>-</code> "
            "(1–32 belgi, probelsiz). Qaytadan kiriting:"
        )
        return
    if await get_movie(code):
        await message.answer("⚠️ <b>Bu kodda kino mavjud!</b> Boshqa kod kiriting:")
        return

    await state.update_data(code=code)
    await state.set_state(AdminStates.add_movie_title)
    await message.answer(
        "📝 <b>Kino nomini va qisqacha ma'lumotini kiriting:</b>\n"
        "<i>Kino nomi birinchi qatorda bo'lsin — nom bo'yicha qidiruv shu qatordan qidiradi.</i>"
    )


@admin_router.message(AdminStates.add_movie_title, F.text)
async def process_movie_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(AdminStates.add_movie_original)
    await message.answer(
        "🌐 <b>Kinoning original (asl) nomini kiriting</b> (masalan: <code>Avengers: Endgame</code>).\n"
        "AI qidiruv kinoni aniqroq topishi uchun tavsiya etiladi, o'tkazib yuborsangiz ham bo'ladi:",
        reply_markup=original_skip_kb(),
    )


@admin_router.message(AdminStates.add_movie_original, F.text)
async def process_movie_original(message: Message, state: FSMContext):
    await state.update_data(original_title=message.text.strip())
    await state.set_state(AdminStates.add_movie_type)
    await message.answer("👑 <b>Kino turini tanlang:</b>", reply_markup=movie_type_kb())


@admin_router.callback_query(AdminStates.add_movie_original, F.data == "orig:skip")
async def skip_movie_original(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.add_movie_type)
    await callback.message.edit_text("👑 <b>Kino turini tanlang:</b>", reply_markup=movie_type_kb())
    await callback.answer()


@admin_router.callback_query(AdminStates.add_movie_type, F.data.startswith("mtype:"))
async def process_movie_type(callback: CallbackQuery, state: FSMContext):
    await state.update_data(is_vip=int(callback.data.split(":")[1]))
    await state.set_state(AdminStates.add_movie_poster)
    await callback.message.edit_text(
        "🖼 <b>Video ustida turadigan rasmni yuboring</b> (muqova) yoki o'tkazib yuboring:",
        reply_markup=poster_skip_kb(),
    )
    await callback.answer()


@admin_router.message(AdminStates.add_movie_poster, F.photo)
async def process_movie_poster(message: Message, state: FSMContext):
    await state.update_data(poster_id=message.photo[-1].file_id)
    await state.set_state(AdminStates.add_movie_video)
    await message.answer("✅ Rasm saqlandi.\n📹 <b>Endi kino videosini yuboring:</b>")


@admin_router.callback_query(AdminStates.add_movie_poster, F.data == "poster:skip")
async def skip_movie_poster(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.add_movie_video)
    await callback.message.edit_text("📹 <b>Endi kino videosini yuboring:</b>")
    await callback.answer()


@admin_router.message(AdminStates.add_movie_poster)
async def process_movie_poster_invalid(message: Message):
    await message.answer(
        "⚠️ <b>Iltimos, rasm yuboring</b> (fayl emas, oddiy rasm) yoki «O'tkazib yuborish» tugmasini bosing."
    )


@admin_router.message(AdminStates.add_movie_video, F.video)
async def process_movie_video(message: Message, state: FSMContext):
    data = await state.get_data()
    await add_movie(
        data["code"], data["title"], message.video.file_id, data["is_vip"], data.get("original_title")
    )
    if data.get("poster_id"):
        await set_poster(data["code"], data["poster_id"])
    await state.set_state(AdminStates.add_movie_more)  # ma'lumotlar (kod va h.k.) saqlanib qoladi
    vip_text = "👑 VIP" if data["is_vip"] == 1 else "🟢 Oddiy"
    poster_text = "bor" if data.get("poster_id") else "yo'q"
    original_line = f"🌐 Original nomi: {esc(data['original_title'])}\n" if data.get("original_title") else ""
    await message.answer(
        f"✅ <b>Kino qo'shildi (1-qism saqlandi)!</b>\n\n"
        f"🔢 Kod: <code>{esc(data['code'])}</code>\n"
        f"🎬 Nomi: {esc(data['title'])}\n"
        f"{original_line}"
        f"🔖 Holati: {vip_text}\n"
        f"🖼 Video ustidagi rasm: {poster_text}\n\n"
        "📺 Serial bo'lsa, yana qism qo'shing. Bitta kino bo'lsa «✅ Tugatish» ni bosing:",
        reply_markup=more_parts_kb(),
    )


@admin_router.callback_query(AdminStates.add_movie_more, F.data == "epadd:more")
async def episode_more(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    next_part = await count_parts(data["code"]) + 1
    await state.set_state(AdminStates.add_episode_video)
    await callback.message.edit_text(f"📹 <b>{next_part}-qism videosini yuboring:</b>")
    await callback.answer()


@admin_router.callback_query(AdminStates.add_movie_more, F.data == "epadd:done")
async def episode_done(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    total = await count_parts(data["code"])
    await state.clear()
    await callback.message.edit_text(
        f"✅ <b>Tayyor!</b> <code>{esc(data['code'])}</code> kodli kino — jami <b>{total} qism</b>."
    )
    await callback.message.answer("👨‍💻 Admin menyu:", reply_markup=admin_menu_kb())
    await callback.answer()


@admin_router.message(AdminStates.add_episode_video, F.video)
async def process_episode_video(message: Message, state: FSMContext):
    data = await state.get_data()
    part = await add_episode(data["code"], message.video.file_id)
    await state.set_state(AdminStates.add_movie_more)
    await message.answer(
        f"✅ <b>{part}-qism saqlandi!</b> Yana qism qo'shasizmi?", reply_markup=more_parts_kb()
    )


@admin_router.message(AdminStates.add_episode_video)
async def process_episode_video_invalid(message: Message):
    await message.answer("⚠️ <b>Iltimos, faqat video fayl yuboring!</b>")


# --- Mavjud kinoga keyinroq qism qo'shish ---
@admin_router.message(F.text == "➕ Qism qo'shish")
async def start_add_episode(message: Message, state: FSMContext):
    await state.set_state(AdminStates.add_episode_code)
    await message.answer(
        "🔢 <b>Qaysi kinoga qism qo'shmoqchisiz? Kino kodini kiriting:</b>", reply_markup=cancel_kb()
    )


@admin_router.message(AdminStates.add_episode_code, F.text)
async def process_episode_code(message: Message, state: FSMContext):
    code = message.text.strip()
    movie = await get_movie(code)
    if not movie:
        await message.answer(f"❌ <code>{esc(code)}</code> kodli kino topilmadi. Qaytadan kiriting:")
        return
    total = await count_parts(code)
    await state.update_data(code=code)
    await state.set_state(AdminStates.add_episode_video)
    await message.answer(
        f"🎬 <b>{esc(movie[1])}</b> hozir <b>{total} qism</b>dan iborat.\n"
        f"📹 <b>{total + 1}-qism videosini yuboring:</b>"
    )


# --- Mavjud kinoning ustiga rasm qo'shish / almashtirish ---
@admin_router.message(F.text == "🖼 Rasm qo'shish")
async def start_add_poster(message: Message, state: FSMContext):
    await state.set_state(AdminStates.add_poster_code)
    await message.answer(
        "🔢 <b>Qaysi kinoning ustiga rasm qo'ymoqchisiz? Kino kodini kiriting:</b>", reply_markup=cancel_kb()
    )


@admin_router.message(AdminStates.add_poster_code, F.text)
async def process_poster_code(message: Message, state: FSMContext):
    code = message.text.strip()
    movie = await get_movie(code)
    if not movie:
        await message.answer(f"❌ <code>{esc(code)}</code> kodli kino topilmadi. Qaytadan kiriting:")
        return
    await state.update_data(code=code)
    await state.set_state(AdminStates.add_poster_photo)
    await message.answer(f"🖼 <b>{esc(_first_line(movie[1], 80))}</b> uchun rasmni yuboring:")


@admin_router.message(AdminStates.add_poster_photo, F.photo)
async def process_poster_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    await set_poster(data["code"], message.photo[-1].file_id)
    await state.clear()
    await message.answer("✅ <b>Rasm saqlandi!</b>", reply_markup=admin_menu_kb())


@admin_router.message(AdminStates.add_poster_photo)
async def process_poster_photo_invalid(message: Message):
    await message.answer("⚠️ <b>Iltimos, rasm yuboring</b> (fayl emas, oddiy rasm).")


@admin_router.message(AdminStates.add_movie_video)
async def process_movie_video_invalid(message: Message):
    await message.answer("⚠️ <b>Iltimos, faqat video fayl yuboring!</b>")


# --- Kino o'chirish ---
@admin_router.message(F.text == "🗑 Kino o'chirish")
async def start_delete_movie(message: Message, state: FSMContext):
    await state.set_state(AdminStates.delete_movie_code)
    await message.answer(
        "🗑 <b>O'chirmoqchi bo'lgan kino kodini kiriting:</b>", reply_markup=cancel_kb()
    )


@admin_router.message(AdminStates.delete_movie_code, F.text)
async def process_delete_movie(message: Message, state: FSMContext):
    code = message.text.strip()
    deleted = await delete_movie(code)
    await state.clear()
    if deleted:
        await message.answer(
            f"✅ <code>{esc(code)}</code> kodli kino o'chirildi!", reply_markup=admin_menu_kb()
        )
    else:
        await message.answer(
            f"❌ <code>{esc(code)}</code> kodli kino topilmadi.", reply_markup=admin_menu_kb()
        )


# --- Broadcast (fon vazifasi sifatida) ---
async def _send_one(bot: Bot, user_id: int, from_chat_id: int, message_id: int, bc_type: str):
    if bc_type == "forward":
        await bot.forward_message(user_id, from_chat_id, message_id)
    else:
        await bot.copy_message(user_id, from_chat_id, message_id)


async def run_broadcast(bot: Bot, admin_chat_id: int, from_chat_id: int, message_id: int, bc_type: str):
    sent = failed = 0
    blocked: List[int] = []
    try:
        users = await get_all_users()
        for user_id in users:
            try:
                await _send_one(bot, user_id, from_chat_id, message_id, bc_type)
                sent += 1
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
                try:
                    await _send_one(bot, user_id, from_chat_id, message_id, bc_type)
                    sent += 1
                except TelegramForbiddenError:
                    blocked.append(user_id)
                    failed += 1
                except Exception:
                    failed += 1
            except TelegramForbiddenError:
                blocked.append(user_id)
                failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)  # Telegram limiti (~30 xabar/soniya) ichida qolish
        await mark_inactive(blocked)
        await bot.send_message(
            admin_chat_id,
            f"✅ <b>Broadcast yakunlandi!</b>\n\n"
            f"📤 Yetkazildi: <b>{sent} ta</b>\n"
            f"🚫 Yetkazilmadi: <b>{failed} ta</b>\n"
            f"🔕 Botni bloklaganlar (faolsiz deb belgilandi): <b>{len(blocked)} ta</b>",
            reply_markup=admin_menu_kb(),
        )
    except Exception:
        logger.exception("Broadcast xatosi")
        try:
            await bot.send_message(admin_chat_id, "⚠️ Broadcast vaqtida xatolik yuz berdi. Loglarni tekshiring.")
        except Exception:
            pass


@admin_router.message(F.text == "📢 Broadcast")
async def start_broadcast(message: Message, state: FSMContext):
    await state.set_state(AdminStates.broadcast_type)
    await message.answer("📢 <b>Xabar tarqatish usulini tanlang:</b>", reply_markup=broadcast_type_kb())


@admin_router.callback_query(AdminStates.broadcast_type, F.data.startswith("bctype:"))
async def select_broadcast_type(callback: CallbackQuery, state: FSMContext):
    await state.update_data(bc_type=callback.data.split(":")[1])
    await state.set_state(AdminStates.broadcast_message)
    await callback.message.delete()
    await callback.message.answer(
        "✉️ <b>Tarqatiladigan xabarni yuboring</b> (matn, rasm, video yoki post):",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@admin_router.message(AdminStates.broadcast_message)
async def execute_broadcast(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    bc_type = data.get("bc_type", "copy")
    await state.clear()

    total = len(await get_all_users())
    await message.answer(
        f"⏳ Xabar tarqatish fonda boshlandi (jami: {total} ta). Tugagach hisobot keladi — "
        "bot shu vaqt ichida ishlashda davom etadi.",
        reply_markup=admin_menu_kb(),
    )
    task = asyncio.create_task(
        run_broadcast(bot, message.chat.id, message.chat.id, message.message_id, bc_type)
    )
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


# --- Kanal qo'shish / o'chirish ---
@admin_router.message(F.text == "🔗 Kanal qo'shish")
async def start_add_channel(message: Message, state: FSMContext):
    await state.set_state(AdminStates.add_channel_id)
    await message.answer(
        "📢 <b>Kanal ID raqamini yoki username'ini kiriting:</b>\n"
        "Masalan: <code>-100192837465</code> yoki <code>@mening_kanalim</code>\n\n"
        "<i>⚠️ Botni ushbu kanalga Admin qilgan bo'lishingiz shart!</i>",
        reply_markup=cancel_kb(),
    )


@admin_router.message(AdminStates.add_channel_id, F.text)
async def process_channel_id(message: Message, state: FSMContext, bot: Bot):
    try:
        chat = await bot.get_chat(_chat_ref(message.text))
        me = await bot.me()
        member = await bot.get_chat_member(chat.id, me.id)
    except Exception:
        await message.answer(
            "⚠️ <b>Kanal topilmadi yoki bot kanalga qo'shilmagan.</b>\n"
            "ID/username'ni tekshirib, botni kanalga admin qiling va qaytadan yuboring:"
        )
        return

    if member.status not in ("administrator", "creator"):
        await message.answer(
            "⚠️ <b>Bot bu kanalda admin emas!</b> Botni kanalga admin qiling va qaytadan yuboring:"
        )
        return

    await state.update_data(channel_id=str(chat.id))
    await state.set_state(AdminStates.add_channel_title)
    await message.answer("📝 <b>Tugmada ko'rinadigan kanal nomini kiriting:</b>")


@admin_router.message(AdminStates.add_channel_title, F.text)
async def process_channel_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(AdminStates.add_channel_link)
    await message.answer("🔗 <b>Kanalga ulanish havolasini (link) yuboring:</b> (<code>https://t.me/...</code>)")


@admin_router.message(AdminStates.add_channel_link, F.text)
async def process_channel_link(message: Message, state: FSMContext):
    link = message.text.strip()
    if not (link.startswith("https://t.me/") or link.startswith("http://t.me/")):
        await message.answer("⚠️ <b>Havola https://t.me/ bilan boshlanishi kerak!</b> Qaytadan yuboring:")
        return

    data = await state.get_data()
    await add_channel(data["channel_id"], data["title"], link)
    await state.clear()
    await message.answer(
        f"✅ <b>Kanal majburiy obunaga qo'shildi!</b>\n\n"
        f"🆔 ID: <code>{esc(data['channel_id'])}</code>\n"
        f"📌 Nomi: {esc(data['title'])}\n"
        f"🔗 Havola: {esc(link)}",
        reply_markup=admin_menu_kb(),
    )


@admin_router.message(
    StateFilter(
        AdminStates.add_movie_code,
        AdminStates.add_movie_title,
        AdminStates.add_movie_original,
        AdminStates.add_episode_code,
        AdminStates.add_poster_code,
        AdminStates.delete_movie_code,
        AdminStates.add_channel_id,
        AdminStates.add_channel_title,
        AdminStates.add_channel_link,
    ),
    ~F.text,
)
async def admin_expect_text(message: Message):
    await message.answer("⚠️ <b>Iltimos, matn yuboring!</b> (rasm, sticker va h.k. qabul qilinmaydi)")


@admin_router.message(F.text == "❌ Kanal o'chirish")
async def show_channels_to_delete(message: Message):
    channels = await get_channels()
    if not channels:
        await message.answer("📭 Hozircha majburiy obuna kanallari yo'q.")
        return
    await message.answer(
        "🗑 <b>O'chirmoqchi bo'lgan kanalingiz ustiga bosing:</b>",
        reply_markup=delete_channels_kb(channels),
    )


@admin_router.callback_query(F.data.startswith("delchan:"))
async def process_delete_channel(callback: CallbackQuery):
    await delete_channel(callback.data.split(":", 1)[1])
    channels = await get_channels()
    try:
        if channels:
            await callback.message.edit_reply_markup(reply_markup=delete_channels_kb(channels))
        else:
            await callback.message.edit_text("✅ Barcha majburiy obuna kanallari o'chirildi.")
    except TelegramBadRequest:
        pass
    await callback.answer("Kanal o'chirildi!")


# =====================================================================
# 8. FOYDALANUVCHI HANDLERLARI
# =====================================================================

user_router = Router()


async def _check_one(bot: Bot, channel: tuple, user_id: int) -> Optional[tuple]:
    channel_id, _title, _link = channel
    try:
        member = await bot.get_chat_member(chat_id=_chat_ref(channel_id), user_id=user_id)
    except Exception as e:
        # Bot kanalda admin emas yoki ID noto'g'ri — foydalanuvchini to'smaymiz, lekin logga yozamiz
        logger.warning("Obuna tekshirib bo'lmadi (kanal=%s, user=%s): %s", channel_id, user_id, e)
        return None

    if member.status in ("left", "kicked"):
        return channel
    if member.status == "restricted" and not getattr(member, "is_member", True):
        return channel
    return None


async def get_unsubscribed_channels(bot: Bot, user_id: int) -> List[tuple]:
    if user_id in ADMIN_IDS and not ADMIN_CHECK_SUB:
        return []
    channels = await get_channels()
    if not channels:
        return []
    results = await asyncio.gather(*(_check_one(bot, ch, user_id) for ch in channels))
    return [ch for ch in results if ch]


async def require_subscription(message: Message, bot: Bot, text: str = "❗️ Oldin kanallarga obuna bo'ling:") -> bool:
    """True — obuna to'liq. False — obuna so'raldi."""
    unsubscribed = await get_unsubscribed_channels(bot, message.from_user.id)
    if unsubscribed:
        await message.answer(text, reply_markup=sub_channels_kb(unsubscribed))
        return False
    return True


async def notify_referral(bot: Bot, user_id: int) -> None:
    """Obuna tasdiqlangach referalni hisoblab, taklif qilganga xabar beradi."""
    result = await confirm_referral(user_id)
    if not result:
        return
    inviter, count = result
    text = f"🎉 <b>Yangi referal!</b> Do'stingiz botga qo'shildi.\n👥 Jami takliflaringiz: <b>{count} ta</b>"
    if count == VIP_REFERRAL_COUNT:
        text += "\n\n👑 <b>Tabriklaymiz! VIP kinolar siz uchun ochildi.</b>"
    try:
        await bot.send_message(inviter, text)
    except Exception:
        pass


async def _vip_locked(user_id: int, is_vip: int) -> Optional[int]:
    """VIP yopiq bo'lsa foydalanuvchining referallar sonini, ochiq bo'lsa None qaytaradi."""
    if is_vip != 1 or user_id in ADMIN_IDS:
        return None
    refs = await get_user_referrals(user_id)
    return refs if refs < VIP_REFERRAL_COUNT else None


def build_caption(movie: Movie, bot_username: str, part: int, total: int) -> str:
    code, title, _file_id, is_vip, views, _likes, _dislikes = movie
    stats_line = f"👁 Ko'rishlar: <b>{views}</b>"
    if is_vip == 1:
        stats_line += " | 👑 VIP"
    part_line = f"📺 Qism: <b>{part} / {total}</b>\n" if total > 1 else ""
    return (
        f"🎬 <b>{esc(_clip(title))}</b>\n\n"
        f"🔢 Kino kodi: <code>{esc(code)}</code>\n"
        f"{part_line}"
        f"{stats_line}\n\n"
        f"🤖 Bizning bot: @{bot_username}"
    )


async def send_movie_helper(message: Message, movie: Movie, bot: Bot, user_id: int) -> None:
    code, title, file_id, is_vip, views, likes, dislikes = movie
    bot_info = await bot.me()

    refs = await _vip_locked(user_id, is_vip)
    if refs is not None:
        ref_link = f"https://t.me/{bot_info.username}?start={user_id}"
        await message.answer(
            f"👑 <b>«{esc(_first_line(title))}» — VIP kino hisoblanadi!</b>\n\n"
            f"VIP kinolarni ko'rish uchun botga <b>{VIP_REFERRAL_COUNT} ta do'stingizni</b> taklif qilishingiz kerak.\n\n"
            f"👥 Siz taklif qilganlar: <b>{refs} / {VIP_REFERRAL_COUNT} ta</b>\n"
            f"🔗 Sizning referal havolangiz:\n<code>{ref_link}</code>"
        )
        return  # VIP yopiq bo'lsa ko'rishlar soni oshmaydi

    await increment_views(code)  # ko'rish faqat kino ochilganda sanaladi (qism almashtirishda emas)
    movie = (code, title, file_id, is_vip, views + 1, likes, dislikes)
    total = await count_parts(code)

    kwargs = dict(
        video=file_id,
        caption=build_caption(movie, bot_info.username, 1, total),
        reply_markup=movie_action_kb(code, likes, dislikes, total, 1),
        protect_content=PROTECT_CONTENT,
    )
    cover = await get_poster(code)  # videoning ustida turadigan rasm (muqova)
    if cover:
        try:
            await message.answer_video(cover=cover, **kwargs)
            return
        except TelegramBadRequest:
            logger.warning("Muqova bilan yuborib bo'lmadi (kino=%s), muqovasiz yuboriladi", code, exc_info=True)
    await message.answer_video(**kwargs)


@user_router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject, bot: Bot, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id

    invited_by = int(command.args) if (command.args and command.args.isdigit()) else None
    await add_user(user_id, invited_by)

    unsubscribed = await get_unsubscribed_channels(bot, user_id)
    if unsubscribed:
        await message.answer(
            "👋 <b>Xush kelibsiz!</b>\n\n"
            "Botdan to'liq foydalanish uchun quyidagi kanallarimizga obuna bo'ling va "
            "<b>«✅ A'zo bo'ldim»</b> tugmasini bosing:",
            reply_markup=sub_channels_kb(unsubscribed),
        )
        return

    await notify_referral(bot, user_id)
    await message.answer(
        f"👋 Assalomu alaykum, <b>{esc(message.from_user.full_name)}</b>!\n\n"
        "🎬 Kino topish uchun <b>kino kodini</b> yoki <b>nomini</b> yuboring.\n"
        "🤖 Kino nomini bilmasangiz — Instagram <b>Reels havolasi</b>, <b>video</b> yoki "
        "<b>skrinshot</b> yuboring, AI kinoni aniqlab beradi.",
        reply_markup=main_menu_kb(),
    )


@user_router.callback_query(F.data == "check_subs")
async def callback_check_subs(callback: CallbackQuery, bot: Bot):
    unsubscribed = await get_unsubscribed_channels(bot, callback.from_user.id)
    if unsubscribed:
        await callback.answer("❌ Siz hali barcha kanallarga obuna bo'lmadingiz!", show_alert=True)
        try:
            await callback.message.edit_reply_markup(reply_markup=sub_channels_kb(unsubscribed))
        except TelegramBadRequest:
            pass  # ro'yxat o'zgarmagan bo'lsa Telegram "message is not modified" deydi
        return

    await notify_referral(bot, callback.from_user.id)
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass
    await callback.message.answer(
        "✅ <b>Obunangiz tasdiqlandi!</b>\nKino kodini yoki nomini yuborishingiz yoki menyudan foydalanishingiz mumkin:",
        reply_markup=main_menu_kb(),
    )
    await callback.answer()


@user_router.message(F.text == "🎬 Kino qidirish")
async def menu_search_movie(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await require_subscription(message, bot):
        return
    await state.set_state(UserStates.waiting_for_code)
    await message.answer(
        "🔢 <b>Marhamat, kino kodini yoki nomini yuboring.</b>\n"
        "🤖 Yoki Instagram Reels havolasi / video / skrinshot yuboring — AI aniqlaydi."
    )


@user_router.message(F.text == "🎲 Tasodifiy kino")
async def menu_random_movie(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await require_subscription(message, bot):
        return
    movie = await get_random_movie()
    if not movie:
        await message.answer("📭 Hozircha bazada kinolar mavjud emas.")
        return
    await send_movie_helper(message, movie, bot, message.from_user.id)


@user_router.message(F.text == "👑 VIP holat")
async def menu_vip_status(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    user_id = message.from_user.id
    refs = await get_user_referrals(user_id)
    bot_info = await bot.me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"
    status_text = "✅ <b>Faol (VIP kinolar ochiq)</b>" if refs >= VIP_REFERRAL_COUNT else "🔒 <b>Faol emas</b>"

    await message.answer(
        f"👑 <b>Sizning VIP kabinetingiz</b>\n\n"
        f"📊 Holat: {status_text}\n"
        f"👥 Taklif qilgan do'stlaringiz: <b>{refs} / {VIP_REFERRAL_COUNT} ta</b>\n\n"
        f"💡 <i>VIP kinolarni bepul ko'rish uchun quyidagi shaxsiy havolangiz orqali "
        f"{VIP_REFERRAL_COUNT} ta do'stingizni botga taklif qiling (do'stingiz kanallarga "
        f"obuna bo'lgach hisoblanadi):</i>\n\n"
        f"🔗 <code>{ref_link}</code>"
    )


@user_router.message(F.text == "🏆 Top kinolar")
async def menu_top_movies(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await require_subscription(message, bot):
        return

    top_viewed, top_liked = await get_top_movies(limit=5)
    if not top_viewed and not top_liked:
        await message.answer("📭 Hozircha bazada kinolar mavjud emas.")
        return

    text = "🏆 <b>TOP 5 ENG KO'P KO'RILGAN KINOLAR:</b>\n"
    for idx, (code, title, views, is_vip) in enumerate(top_viewed, 1):
        vip_icon = "👑 " if is_vip else ""
        text += f"{idx}. {vip_icon}<b>{esc(title)}</b> — 👁 {views} | Kod: <code>{esc(code)}</code>\n"

    text += "\n❤️ <b>TOP 5 ENG KO'P LIKE OLGANLAR:</b>\n"
    for idx, (code, title, likes, is_vip) in enumerate(top_liked, 1):
        vip_icon = "👑 " if is_vip else ""
        text += f"{idx}. {vip_icon}<b>{esc(title)}</b> — 👍 {likes} | Kod: <code>{esc(code)}</code>\n"

    text += "\n💡 <i>Kino kodini nusxalab botga yuboring!</i>"
    await message.answer(text)


async def _all_movies_page(page: int) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    total = await count_movies()
    if total == 0:
        return "📭 Hozircha bazada kinolar mavjud emas.", None
    pages = max(1, -(-total // ALL_MOVIES_PAGE_SIZE))
    page = min(max(page, 1), pages)
    rows = await get_movies_page((page - 1) * ALL_MOVIES_PAGE_SIZE, ALL_MOVIES_PAGE_SIZE)
    lines = [f"📚 <b>Barcha kinolar</b> (jami: {total} ta)\n"]
    start = (page - 1) * ALL_MOVIES_PAGE_SIZE
    for i, (code, title, is_vip) in enumerate(rows, start + 1):
        vip = "👑 " if is_vip else ""
        lines.append(f"{i}. {vip}<b>{esc(_first_line(title, 50))}</b> — Kod: <code>{esc(code)}</code>")
    lines.append("\n💡 <i>Kino kodini nusxalab botga yuboring!</i>")
    return "\n".join(lines), all_movies_kb(page, pages)


@user_router.message(F.text == "📚 Barcha kinolar")
async def menu_all_movies(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await require_subscription(message, bot):
        return
    text, kb = await _all_movies_page(1)
    await message.answer(text, reply_markup=kb)


@user_router.callback_query(F.data.startswith("list:"))
async def handle_all_movies_page(callback: CallbackQuery, bot: Bot):
    arg = callback.data.split(":", 1)[1]
    if not arg.isdigit():  # "noop" — sahifa raqami tugmasi
        await callback.answer()
        return
    if await get_unsubscribed_channels(bot, callback.from_user.id):
        await callback.answer("❗️ Avval kanallarga obuna bo'ling va /start bosing.", show_alert=True)
        return
    text, kb = await _all_movies_page(int(arg))
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass  # "message is not modified"
    await callback.answer()


@user_router.callback_query(F.data.startswith("part:"))
async def handle_part_switch(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await callback.answer()
        return
    code, part = parts[1], int(parts[2])

    movie = await get_movie(code)
    if not movie:
        await callback.answer("❌ Bu kino endi mavjud emas.", show_alert=True)
        return
    if await get_unsubscribed_channels(bot, callback.from_user.id):
        await callback.answer("❗️ Avval kanallarga obuna bo'ling va /start bosing.", show_alert=True)
        return
    if await _vip_locked(callback.from_user.id, movie[3]) is not None:
        await callback.answer("👑 Bu VIP kino. Ko'rish uchun do'stlaringizni taklif qiling.", show_alert=True)
        return

    total = await count_parts(code)
    file_id = await get_part_file(code, part)
    if not file_id or part > total:
        await callback.answer("❌ Bunday qism topilmadi.", show_alert=True)
        return

    bot_info = await bot.me()
    cover = await get_poster(code)
    caption = build_caption(movie, bot_info.username, part, total)
    markup = movie_action_kb(code, movie[5], movie[6], total, part)

    async def _edit(with_cover: bool) -> None:
        extra = {"cover": cover} if (cover and with_cover) else {}
        await callback.message.edit_media(
            media=InputMediaVideo(media=file_id, caption=caption, **extra),
            reply_markup=markup,
        )

    try:
        await _edit(True)
    except TelegramBadRequest as e:
        # shu qismning o'zi bosilsa Telegram "message is not modified" deydi — bu xato emas
        if cover and "not modified" not in str(e).lower():
            try:
                await _edit(False)  # muqova o'tmasa, muqovasiz almashtiramiz
            except TelegramBadRequest:
                pass
    await callback.answer()


@user_router.callback_query(F.data.startswith("vote:"))
async def handle_movie_vote(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) not in (3, 4) or parts[1] not in ("like", "dislike"):
        await callback.answer()
        return
    vote_type, code = parts[1], parts[2]
    current = int(parts[3]) if len(parts) == 4 and parts[3].isdigit() else 1

    status, likes, dislikes = await vote_movie(callback.from_user.id, code, vote_type)
    if status == "missing":
        await callback.answer("❌ Bu kino endi mavjud emas.", show_alert=True)
        return
    if status == "same":
        await callback.answer("⚠️ Siz allaqachon ushbu ovozni bergansiz!")
        return

    total = await count_parts(code)
    try:
        await callback.message.edit_reply_markup(
            reply_markup=movie_action_kb(code, likes, dislikes, total, current)
        )
    except TelegramBadRequest:
        pass
    await callback.answer("✅ Ovozingiz qabul qilindi!")


@user_router.callback_query(F.data.startswith("movie:"))
async def handle_movie_pick(callback: CallbackQuery, bot: Bot):
    """Nom bo'yicha qidiruv (yoki AI) natijalari ro'yxatidan kino tanlash."""
    code = callback.data.split(":", 1)[1]
    movie = await get_movie(code)
    if not movie:
        await callback.answer("❌ Bu kino endi mavjud emas.", show_alert=True)
        return
    if await get_unsubscribed_channels(bot, callback.from_user.id):
        await callback.answer("❗️ Avval kanallarga obuna bo'ling va /start bosing.", show_alert=True)
        return
    await callback.answer()
    await send_movie_helper(callback.message, movie, bot, callback.from_user.id)


# ---------------------------------------------------------------------
# 8.1  NOM / KOD BO'YICHA QIDIRUV
# ---------------------------------------------------------------------


async def _search_by_name(
    message: Message, bot: Bot, query: str, not_found_text: str = NOT_FOUND_TEXT
) -> None:
    """Nom bo'yicha qidiradi: 1 ta topilsa — darhol yuboradi, ko'p bo'lsa — tugmalar, yo'q bo'lsa — xabar."""
    results = await search_movies(query)
    if not results:
        await message.answer(not_found_text)
        return
    if len(results) == 1:
        await send_movie_helper(message, results[0], bot, message.from_user.id)
        return
    await message.answer(
        f"🔎 «<b>{esc(_clip(query, 40))}</b>» bo'yicha <b>{len(results)} ta</b> kino topildi.\n"
        "Keraklisini tanlang:",
        reply_markup=search_results_kb(results),
    )


async def _search_text(message: Message, state: FSMContext, bot: Bot) -> None:
    """Oddiy matn: avval kino KODI sifatida, topilmasa — kino NOMI sifatida qidiriladi.
    (Bir so'zli nomlar — masalan «titanic» — kod andozasiga ham mos keladi, shuning uchun
    kod topilmasa albatta nom bo'yicha ham qidiriladi.)"""
    await state.clear()
    text = (message.text or "").strip()

    if text.startswith("/"):
        await message.answer(
            "🤔 Noma'lum buyruq. Kino kodini yoki nomini yuboring yoki menyudan foydalaning.",
            reply_markup=main_menu_kb(),
        )
        return

    if not await require_subscription(message, bot, "❗️ Kino olishdan oldin kanallarga obuna bo'ling:"):
        return

    is_code_like = bool(re.match(CODE_PATTERN, text))
    if is_code_like:
        movie = await get_movie(text)
        if movie:
            await send_movie_helper(message, movie, bot, message.from_user.id)
            return

    if text.isdigit():
        miss_text = (
            f"❌ <code>{esc(text)}</code> kodli kino topilmadi.\n"
            "Kodni to'g'ri yozganingizga ishonch hosil qiling!"
        )
    else:
        miss_text = NOT_FOUND_TEXT
    await _search_by_name(message, bot, text, miss_text)


# ---------------------------------------------------------------------
# 8.2  AI QIDIRUV (Instagram Reels / video / skrinshot -> Gemini Vision)
# ---------------------------------------------------------------------


class AIError(Exception):
    """Foydalanuvchiga ko'rsatiladigan xabarli xatolik."""

    def __init__(self, user_message: str):
        super().__init__(user_message)
        self.user_message = user_message


class MovieGuess(BaseModel):
    """Gemini javobining tuzilmasi (JSON)."""

    original_title: Optional[str] = None
    uzbek_title: Optional[str] = None
    release_year: Optional[int] = None
    # "high" | "medium" | "low" — model bilmasa to'qib chiqarmasligi uchun
    confidence: str = "low"


GEMINI_PROMPT = (
    "You are a film expert. The attached images are frames taken from a video clip (or a single screenshot) "
    "of a movie, TV series or animated film. Identify which movie or series it is.\n"
    "Use every contextual clue: on-screen text, subtitles, logos, watermarks, distinctive scenes, "
    "costumes, props, settings and the characters.\n"
    "Respond ONLY with JSON in exactly this shape:\n"
    '{"original_title": string|null, "uzbek_title": string|null, "release_year": number|null, '
    '"confidence": "high"|"medium"|"low"}\n'
    "Rules:\n"
    "- original_title: the official original title (usually English).\n"
    "- uzbek_title: the title as it is commonly known in Uzbekistan (Latin script); if you do not know it, "
    "repeat the original title.\n"
    "- release_year: release year of the movie/series, or null.\n"
    '- confidence: "high" if you are sure, "medium" if it is probable, "low" if you are guessing.\n'
    "- If you cannot identify it, set both titles and release_year to null and confidence to \"low\". "
    "Never invent a title."
)

_ai_busy: set = set()  # hozir AI so'rovi bajarilayotgan foydalanuvchilar (bittadan ortiq parallel so'rov yo'q)
_ai_sem = asyncio.Semaphore(AI_MAX_PARALLEL)  # yuklash + kadr kesish jarayonlarini cheklaydi (xotira uchun)
_gemini_client = None


def cleanup_stale_tmp() -> None:
    """Bot qulab tushganda qolib ketgan eski vaqtinchalik papkalarni o'chiradi."""
    base = tempfile.gettempdir()
    try:
        for name in os.listdir(base):
            if name.startswith("kinobot_"):
                shutil.rmtree(os.path.join(base, name), ignore_errors=True)
    except OSError:
        pass


# --- Og'ir (sinxron) funksiyalar: faqat asyncio.to_thread orqali chaqiriladi ---


def _download_reel_sync(url: str, workdir: str) -> str:
    """Instagram havolasidan videoni workdir ichiga yuklaydi va fayl yo'lini qaytaradi."""
    try:
        import yt_dlp
    except ImportError:
        raise AIError("⚠️ AI qidiruv hozircha sozlanmagan (yt-dlp o'rnatilmagan).")

    opts: Dict[str, Any] = {
        "outtmpl": os.path.join(workdir, "reel.%(ext)s"),
        # Bitta tayyor mp4 fayl (ffmpeg kerak emas), hajmi chegaradan oshmasin
        "format": f"b[ext=mp4][filesize<?{MAX_REEL_MB}M]/b[ext=mp4]/b",
        "max_filesize": MAX_REEL_MB * 1024 * 1024,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "socket_timeout": 20,
        "retries": 2,
    }
    if INSTAGRAM_COOKIES_FILE and os.path.isfile(INSTAGRAM_COOKIES_FILE):
        opts["cookiefile"] = INSTAGRAM_COOKIES_FILE

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
    except Exception as e:
        logger.warning("Instagramdan yuklab bo'lmadi (%s): %s", url, e)
        raise AIError(
            "⚠️ Instagram havolasidan videoni yuklab bo'lmadi (havola yopiq bo'lishi yoki Instagram "
            "cheklovi bo'lishi mumkin).\nVideoni to'g'ridan-to'g'ri botga yuboring yoki skrinshot tashlang."
        )

    files = [f for f in os.listdir(workdir) if f.startswith("reel.") and not f.endswith((".part", ".ytdl"))]
    if not files:
        raise AIError("⚠️ Videoni yuklab bo'lmadi yoki hajmi juda katta. Skrinshot yuborib ko'ring.")
    return os.path.join(workdir, files[0])


def _encode_frame(frame) -> bytes:
    """Kadrni (cv2 massivi) uzun tomoni 1024 px dan oshmaydigan JPEG baytlarga aylantiradi."""
    import cv2

    h, w = frame.shape[:2]
    scale = 1024 / max(h, w)
    if scale < 1:
        frame = cv2.resize(frame, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)
    ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
    if not ok:
        raise AIError("⚠️ Kadrni qayta ishlab bo'lmadi.")
    return buf.tobytes()


def _grab_clean_frame(cap, target: int, total: int) -> Optional[bytes]:
    """target kadrni o'qiydi; qora/bir xil rangli bo'lsa, yaqin kadrlardan toza birini qidiradi."""
    import cv2

    fallback: Optional[bytes] = None
    for offset in (0, 6, -6, 15, -15, 30, -30):
        idx = target + offset
        if idx < 0 or idx >= total:
            continue
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        if not ok or frame is None:
            continue
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        if gray.mean() >= 12 and gray.std() >= 6:  # qora yoki bir tekis kadr emas
            return _encode_frame(frame)
        if fallback is None:
            fallback = _encode_frame(frame)
    return fallback


def _extract_frames_sync(path: str, points: Tuple[float, ...] = (0.2, 0.5, 0.8)) -> List[bytes]:
    """Videoning 20%, 50% va 80% nuqtalaridan toza kadrlarni JPEG baytlar sifatida qaytaradi."""
    try:
        import cv2
    except ImportError:
        raise AIError("⚠️ AI qidiruv hozircha sozlanmagan (opencv o'rnatilmagan).")

    cap = cv2.VideoCapture(path)
    try:
        if not cap.isOpened():
            raise AIError("⚠️ Videoni o'qib bo'lmadi. Boshqa video yoki skrinshot yuborib ko'ring.")
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        if total <= 0:  # ba'zi fayllarda kadrlar soni ko'rsatilmaydi — qo'lda sanaymiz
            while cap.grab():
                total += 1
        if total <= 0:
            return []

        frames: List[bytes] = []
        for p in points:
            target = min(int(total * p), total - 1)
            jpeg = _grab_clean_frame(cap, target, total)
            if jpeg:
                frames.append(jpeg)
        return frames
    finally:
        cap.release()


def _read_image_sync(path: str) -> List[bytes]:
    """Rasmni o'qib, kichraytirilgan JPEG baytlarga aylantiradi."""
    try:
        import cv2
    except ImportError:
        raise AIError("⚠️ AI qidiruv hozircha sozlanmagan (opencv o'rnatilmagan).")
    img = cv2.imread(path)
    if img is None:
        return []
    return [_encode_frame(img)]


# --- Telegramdan yuklab olish ---


async def _download_tg_file(bot: Bot, file_obj: Any, dest: str) -> None:
    try:
        await bot.download(file_obj, destination=dest, timeout=120)
    except TelegramBadRequest as e:
        if "too big" in str(e).lower():
            raise AIError(
                "⚠️ Fayl hajmi 20 MB dan oshmasligi kerak. Qisqaroq video yuboring, skrinshot tashlang "
                "yoki Instagram havolasini yuboring."
            )
        logger.warning("Telegram faylini yuklab bo'lmadi: %s", e)
        raise AIError("⚠️ Faylni yuklab olib bo'lmadi. Qaytadan urinib ko'ring.")
    except Exception as e:
        logger.warning("Telegram faylini yuklab bo'lmadi: %s", e)
        raise AIError("⚠️ Faylni yuklab olib bo'lmadi. Qaytadan urinib ko'ring.")


# --- Gemini ---


def _parse_guess_json(text: Optional[str]) -> Optional[MovieGuess]:
    if not text:
        return None
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.MULTILINE).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        return MovieGuess(**json.loads(match.group(0)))
    except Exception:
        return None


async def identify_with_gemini(frames: List[bytes]) -> MovieGuess:
    """Kadrlarni Gemini Vision'ga yuborib, kino nomini JSON ko'rinishida oladi."""
    global _gemini_client
    try:
        from google import genai
        from google.genai import errors, types
    except ImportError:
        raise AIError("⚠️ AI qidiruv hozircha sozlanmagan (google-genai o'rnatilmagan).")

    if _gemini_client is None:
        _gemini_client = genai.Client(api_key=GEMINI_API_KEY)

    contents: List[Any] = [types.Part.from_bytes(data=b, mime_type="image/jpeg") for b in frames]
    contents.append(GEMINI_PROMPT)
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=MovieGuess,
        temperature=0.1,
    )

    last_error: Optional[BaseException] = None
    for model in [GEMINI_MODEL, *GEMINI_FALLBACK_MODELS]:
        try:
            response = await asyncio.wait_for(
                _gemini_client.aio.models.generate_content(model=model, contents=contents, config=config),
                timeout=90,
            )
        except (errors.APIError, asyncio.TimeoutError) as e:
            # model topilmadi (404), kvota (429), server xatosi yoki vaqt tugashi — keyingi modelni sinaymiz
            logger.warning("Gemini (%s) xatosi: %r", model, e)
            last_error = e
            continue

        parsed = getattr(response, "parsed", None)
        if isinstance(parsed, MovieGuess):
            return parsed
        try:
            text = response.text
        except Exception:  # xavfsizlik filtri bloklagan bo'lsa
            text = None
        guess = _parse_guess_json(text)
        if guess is not None:
            return guess
        logger.warning("Gemini (%s) javobini o'qib bo'lmadi: %r", model, text)

    logger.error("Gemini barcha modellarda xato berdi: %r", last_error)
    raise AIError("⚠️ AI xizmati hozir javob bermayapti. Birozdan keyin qayta urinib ko'ring (urinishingiz qaytarildi).")


# --- Limit va natija ---


async def ai_limit_for(user_id: int) -> Tuple[int, bool]:
    """(kunlik limit, VIP-mi). Limit 0 bo'lsa — cheksiz. Adminlarga limit yo'q."""
    if user_id in ADMIN_IDS:
        return 0, True
    is_vip = await get_user_referrals(user_id) >= VIP_REFERRAL_COUNT
    return (AI_DAILY_LIMIT_VIP if is_vip else AI_DAILY_LIMIT_FREE), is_vip


async def _limit_reached_text(bot: Bot, user_id: int, limit: int, is_vip: bool) -> str:
    text = (
        f"⛔️ <b>Bugungi AI qidiruv limitingiz tugadi</b> ({limit} ta).\n"
        "Limit har kuni Toshkent vaqti bilan yarim tunda yangilanadi.\n\n"
    )
    if not is_vip:
        me = await bot.me()
        ref_link = f"https://t.me/{me.username}?start={user_id}"
        vip_limit = "cheksiz" if AI_DAILY_LIMIT_VIP <= 0 else f"kuniga {AI_DAILY_LIMIT_VIP} ta"
        text += (
            f"👑 <b>VIP</b> bo'lish uchun {VIP_REFERRAL_COUNT} ta do'stingizni taklif qiling — "
            f"VIP foydalanuvchilarga AI qidiruv {vip_limit} beriladi va VIP kinolar ochiladi.\n"
            f"🔗 Sizning havolangiz:\n<code>{ref_link}</code>\n\n"
        )
    text += "Hozircha kinoni <b>kodi</b> yoki <b>nomi</b> bilan qidirishingiz mumkin."
    return text


def _guess_titles(guess: MovieGuess) -> List[str]:
    """Bo'sh bo'lmagan, takrorlanmagan nomlar ro'yxati (original, keyin o'zbekcha)."""
    titles: List[str] = []
    for t in (guess.original_title, guess.uzbek_title):
        t = (t or "").strip()
        if t and t.casefold() not in [x.casefold() for x in titles]:
            titles.append(t)
    return titles


async def _deliver_ai_result(message: Message, bot: Bot, guess: MovieGuess, remaining_line: str) -> None:
    titles = _guess_titles(guess)
    confidence = (guess.confidence or "low").strip().lower()

    if not titles or confidence not in ("high", "medium"):
        await message.answer(
            "🤔 <b>Kinoni aniqlab bo'lmadi.</b>\n"
            "Yuzi va yozuvlari aniq ko'ringan boshqa kadr/skrinshot yuboring yoki kino kodini/nomini kiriting."
            f"{remaining_line}"
        )
        return

    year = guess.release_year
    year_part = f" ({year})" if isinstance(year, int) and 1888 <= year <= datetime.now().year + 2 else ""
    label = "Aniqlandi" if confidence == "high" else "Taxminan aniqlandi"
    header = f"🎬 <b>{label}:</b> {esc(' / '.join(titles))}{year_part}"

    found = await search_movies_multi(titles)
    if not found:
        await message.answer(
            f"{header}\nℹ️ Ushbu film hozircha bazamizda mavjud emas, lekin adminlar tez orada qo'shishadi!"
            f"{remaining_line}"
        )
    elif len(found) == 1:
        await message.answer(f"{header}{remaining_line}")
        await send_movie_helper(message, found[0], bot, message.from_user.id)
    else:
        await message.answer(
            f"{header}\n\n🔎 Bazadan o'xshash kinolar topildi, keraklisini tanlang:{remaining_line}",
            reply_markup=search_results_kb(found),
        )


async def run_ai_search(message: Message, bot: Bot, kind: str, source: Any) -> None:
    """kind: 'reel' (source — havola matni) | 'video' | 'photo' (source — Telegram fayl obyekti)."""
    user_id = message.from_user.id

    if not GEMINI_API_KEY:
        await message.answer("⚠️ AI qidiruv hozircha sozlanmagan. Kino kodini yoki nomini yuboring.")
        return
    if not await require_subscription(message, bot, "❗️ AI qidiruvdan oldin kanallarga obuna bo'ling:"):
        return

    if kind in ("video", "photo"):
        size = getattr(source, "file_size", None)
        if size and size > MAX_TG_DOWNLOAD:
            await message.answer(
                "⚠️ Fayl hajmi 20 MB dan oshmasligi kerak. Qisqaroq video yuboring, skrinshot tashlang "
                "yoki Instagram havolasini yuboring."
            )
            return

    # tekshirish va qo'shish orasida await yo'q — parallel so'rovlar o'tib ketmaydi
    if user_id in _ai_busy:
        await message.answer("⏳ Oldingi so'rovingiz hali ishlanmoqda, bir oz kuting.")
        return
    _ai_busy.add(user_id)

    workdir: Optional[str] = None
    status: Optional[Message] = None
    refundable = False  # Gemini javob bermaguncha texnik xato bo'lsa, urinish qaytariladi
    try:
        limit, is_vip = await ai_limit_for(user_id)
        allowed, used = await try_consume_ai(user_id, limit)
        if not allowed:
            await message.answer(await _limit_reached_text(bot, user_id, limit, is_vip))
            return
        refundable = True

        status = await message.answer("🔎 Tahlil qilinmoqda, bir necha soniya kuting...")
        workdir = await asyncio.to_thread(tempfile.mkdtemp, prefix="kinobot_")

        # Og'ir ishlar (yuklash, kadr kesish) — bir vaqtda AI_MAX_PARALLEL tadan ko'p emas
        async with _ai_sem:
            if kind == "reel":
                path = await asyncio.to_thread(_download_reel_sync, source, workdir)
                frames = await asyncio.to_thread(_extract_frames_sync, path)
            elif kind == "video":
                path = os.path.join(workdir, "input.mp4")
                await _download_tg_file(bot, source, path)
                frames = await asyncio.to_thread(_extract_frames_sync, path)
            else:
                path = os.path.join(workdir, "input.jpg")
                await _download_tg_file(bot, source, path)
                frames = await asyncio.to_thread(_read_image_sync, path)

        if not frames:
            raise AIError("⚠️ Videodan kadrlarni ajratib bo'lmadi. Boshqa video yoki skrinshot yuborib ko'ring.")

        guess = await identify_with_gemini(frames)
        refundable = False  # Gemini javob berdi — urinish sarflandi

        remaining_line = ""
        if limit > 0:
            remaining_line = f"\n\n🤖 Bugungi qolgan AI qidiruv: <b>{max(limit - used, 0)} / {limit}</b>"
        await _deliver_ai_result(message, bot, guess, remaining_line)

    except AIError as e:
        if refundable:
            await refund_ai(user_id)
        await message.answer(e.user_message)
    except Exception:
        logger.exception("AI qidiruvda kutilmagan xatolik")
        if refundable:
            await refund_ai(user_id)
        try:
            await message.answer("⚠️ Kutilmagan xatolik yuz berdi. Birozdan keyin qayta urinib ko'ring.")
        except Exception:
            pass
    finally:
        _ai_busy.discard(user_id)
        # Muvaffaqiyatli yoki xato — vaqtinchalik fayllar har doim diskdan o'chiriladi
        if workdir:
            await asyncio.to_thread(shutil.rmtree, workdir, True)
        if status:
            try:
                await status.delete()
            except Exception:
                pass


def _is_ig_link(message: Message) -> bool:
    return bool(message.text and IG_URL_RE.search(message.text))


# Foydalanuvchi oddiy holatda yoki "kod kutilmoqda" holatida bo'lsa ishlaydi
# (admin FSM jarayonlariga xalaqit bermaslik uchun).
_USER_STATES = StateFilter(None, UserStates.waiting_for_code)


@user_router.message(_USER_STATES, _is_ig_link)
async def ai_from_reel(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    url = IG_URL_RE.search(message.text).group(0)
    await run_ai_search(message, bot, "reel", url)


@user_router.message(_USER_STATES, F.video | F.video_note | F.animation)
async def ai_from_video(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    media = message.video or message.video_note or message.animation
    await run_ai_search(message, bot, "video", media)


@user_router.message(_USER_STATES, F.photo)
async def ai_from_photo(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    await run_ai_search(message, bot, "photo", message.photo[-1])


# Tartib muhim: yuqoridagi menyu tugmalari va AI handlerlari avval tekshiriladi, bu esa eng oxirida.
@user_router.message(_USER_STATES, F.text)
async def search_by_text(message: Message, state: FSMContext, bot: Bot):
    await _search_text(message, state, bot)


# =====================================================================
# 9. ISHGA TUSHIRISH
# =====================================================================


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN topilmadi! Railway > Variables bo'limiga BOT_TOKEN qo'shing.")
    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS bo'sh — admin panel hech kimga ochilmaydi!")
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY yo'q — AI qidiruv o'chirilgan (nom/kod bo'yicha qidiruv ishlaydi).")

    cleanup_stale_tmp()
    await init_db()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    dp.message.middleware(ThrottlingMiddleware(THROTTLE_MESSAGE))
    dp.callback_query.middleware(ThrottlingMiddleware(THROTTLE_CALLBACK))

    dp.include_router(error_router)
    dp.include_router(admin_router)
    dp.include_router(user_router)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("🚀 Kino bot muvaffaqiyatli ishga tushdi!")
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        await close_db()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Bot to'xtatildi!")
