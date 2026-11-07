"""Kino bot — aiogram 3 + aiosqlite. Railway uchun tayyor."""
import asyncio
import html
import logging
import os
import re
import time
from datetime import date
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
 
 
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = _int_list(os.getenv("ADMIN_IDS", ""))
DB_PATH = os.getenv("DB_PATH", "kino_bot.db")  # Railway'da: /data/kino_bot.db (Volume)
VIP_REFERRAL_COUNT = int(os.getenv("VIP_REFERRAL_COUNT", "3"))
THROTTLE_MESSAGE = float(os.getenv("THROTTLE_MESSAGE", "1.0"))
THROTTLE_CALLBACK = float(os.getenv("THROTTLE_CALLBACK", "0.5"))
PROTECT_CONTENT = os.getenv("PROTECT_CONTENT", "0") == "1"  # videoni uzatish/saqlashni taqiqlash
# 1 bo'lsa adminlar ham majburiy obunadan o'tadi (obunani o'zingiz sinab ko'rish uchun)
ADMIN_CHECK_SUB = os.getenv("ADMIN_CHECK_SUB", "0") == "1"
 
# Kino kodi: faqat lotin harf, raqam, _ va - (1..32 belgi)
CODE_PATTERN = r"^[A-Za-z0-9_\-]{1,32}$"
 
esc = html.escape
 
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
            dislikes INTEGER DEFAULT 0
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
 
    # Eski bazani yangilash (migratsiya)
    cols = await _columns("users")
    if "referral_counted" not in cols:
        await _db.execute("ALTER TABLE users ADD COLUMN referral_counted INTEGER DEFAULT 0")
        # eski referallar allaqachon hisoblangan — qayta sanalmasin
        await _db.execute("UPDATE users SET referral_counted = 1 WHERE invited_by IS NOT NULL")
    if "is_active" not in cols:
        await _db.execute("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1")
 
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
 
 
async def add_movie(code: str, title: str, file_id: str, is_vip: int) -> None:
    async with _db_lock:
        await _db.execute("DELETE FROM episodes WHERE movie_code = ?", (code,))
        await _db.execute(
            "INSERT OR REPLACE INTO movies (code, title, file_id, is_vip, views, likes, dislikes) "
            "VALUES (?, ?, ?, ?, 0, 0, 0)",
            (code, title, file_id, is_vip),
        )
        await _db.commit()
 
 
async def get_movie(code: str) -> Optional[Movie]:
    cur = await _db.execute(f"SELECT {MOVIE_COLS} FROM movies WHERE code = ?", (code,))
    return await cur.fetchone()
 
 
async def get_random_movie() -> Optional[Movie]:
    cur = await _db.execute(f"SELECT {MOVIE_COLS} FROM movies ORDER BY RANDOM() LIMIT 1")
    return await cur.fetchone()
 
 
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
 
 
# =====================================================================
# 3. FSM HOLATLARI
# =====================================================================
 
 
class UserStates(StatesGroup):
    waiting_for_code = State()
 
 
class AdminStates(StatesGroup):
    add_movie_code = State()
    add_movie_title = State()
    add_movie_type = State()
    add_movie_video = State()
    add_movie_more = State()
    add_episode_code = State()
    add_episode_video = State()
 
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
        ],
        resize_keyboard=True,
    )
 
 
def admin_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Statistika"), KeyboardButton(text="📢 Broadcast")],
            [KeyboardButton(text="🎬 Kino qo'shish"), KeyboardButton(text="➕ Qism qo'shish")],
            [KeyboardButton(text="🗑 Kino o'chirish"), KeyboardButton(text="🔗 Kanal qo'shish")],
            [KeyboardButton(text="❌ Kanal o'chirish"), KeyboardButton(text="🏠 Foydalanuvchi menyusi")],
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
 
 
def more_parts_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="➕ Yana qism qo'shish", callback_data="epadd:more"),
            InlineKeyboardButton(text="✅ Tugatish", callback_data="epadd:done"),
        ]]
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
    await message.answer("📝 <b>Kino nomini va qisqacha ma'lumotini kiriting:</b>")
 
 
@admin_router.message(AdminStates.add_movie_title, F.text)
async def process_movie_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(AdminStates.add_movie_type)
    await message.answer("👑 <b>Kino turini tanlang:</b>", reply_markup=movie_type_kb())
 
 
@admin_router.callback_query(AdminStates.add_movie_type, F.data.startswith("mtype:"))
async def process_movie_type(callback: CallbackQuery, state: FSMContext):
    await state.update_data(is_vip=int(callback.data.split(":")[1]))
    await state.set_state(AdminStates.add_movie_video)
    await callback.message.edit_text("📹 <b>Endi kino videosini yuboring:</b>")
    await callback.answer()
 
 
@admin_router.message(AdminStates.add_movie_video, F.video)
async def process_movie_video(message: Message, state: FSMContext):
    data = await state.get_data()
    await add_movie(data["code"], data["title"], message.video.file_id, data["is_vip"])
    await state.set_state(AdminStates.add_movie_more)  # ma'lumotlar (kod va h.k.) saqlanib qoladi
    vip_text = "👑 VIP" if data["is_vip"] == 1 else "🟢 Oddiy"
    await message.answer(
        f"✅ <b>Kino qo'shildi (1-qism saqlandi)!</b>\n\n"
        f"🔢 Kod: <code>{esc(data['code'])}</code>\n"
        f"🎬 Nomi: {esc(data['title'])}\n"
        f"🔖 Holati: {vip_text}\n\n"
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
        AdminStates.add_episode_code,
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
        f"🎬 <b>{esc(title)}</b>\n\n"
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
            f"👑 <b>«{esc(title)}» — VIP kino hisoblanadi!</b>\n\n"
            f"VIP kinolarni ko'rish uchun botga <b>{VIP_REFERRAL_COUNT} ta do'stingizni</b> taklif qilishingiz kerak.\n\n"
            f"👥 Siz taklif qilganlar: <b>{refs} / {VIP_REFERRAL_COUNT} ta</b>\n"
            f"🔗 Sizning referal havolangiz:\n<code>{ref_link}</code>"
        )
        return  # VIP yopiq bo'lsa ko'rishlar soni oshmaydi
 
    await increment_views(code)  # ko'rish faqat kino ochilganda sanaladi (qism almashtirishda emas)
    movie = (code, title, file_id, is_vip, views + 1, likes, dislikes)
    total = await count_parts(code)
    await message.answer_video(
        video=file_id,
        caption=build_caption(movie, bot_info.username, 1, total),
        reply_markup=movie_action_kb(code, likes, dislikes, total, 1),
        protect_content=PROTECT_CONTENT,
    )
 
 
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
        "🎬 Kino topish uchun kino kodini yuboring yoki quyidagi menyudan foydalaning:",
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
        "✅ <b>Obunangiz tasdiqlandi!</b>\nKino kodini yuborishingiz yoki menyudan foydalanishingiz mumkin:",
        reply_markup=main_menu_kb(),
    )
    await callback.answer()
 
 
@user_router.message(F.text == "🎬 Kino qidirish")
async def menu_search_movie(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await require_subscription(message, bot):
        return
    await state.set_state(UserStates.waiting_for_code)
    await message.answer("🔢 <b>Marhamat, kino kodini yuboring:</b>")
 
 
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
    try:
        await callback.message.edit_media(
            media=InputMediaVideo(
                media=file_id, caption=build_caption(movie, bot_info.username, part, total)
            ),
            reply_markup=movie_action_kb(code, movie[5], movie[6], total, part),
        )
    except TelegramBadRequest:
        pass  # shu qismning o'zi bosilganda Telegram "message is not modified" deydi
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
 
 
async def _search_by_code(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await require_subscription(message, bot, "❗️ Kino olishdan oldin kanallarga obuna bo'ling:"):
        return
 
    code = message.text.strip()
    movie = await get_movie(code)
    if not movie:
        await message.answer(
            f"❌ <code>{esc(code)}</code> kodli kino topilmadi.\n"
            "Kodni to'g'ri yozganingizga ishonch hosil qiling!"
        )
        return
    await send_movie_helper(message, movie, bot, message.from_user.id)
 
 
# Tartib muhim: yuqoridagi menyu tugmalari avval tekshiriladi, bular esa oxirida.
@user_router.message(UserStates.waiting_for_code, F.text)
async def search_in_waiting_state(message: Message, state: FSMContext, bot: Bot):
    await _search_by_code(message, state, bot)
 
 
@user_router.message(F.text.regexp(CODE_PATTERN))
async def search_movie_by_code(message: Message, state: FSMContext, bot: Bot):
    await _search_by_code(message, state, bot)
 
 
@user_router.message(F.text)
async def unknown_text(message: Message):
    await message.answer(
        "🤔 Tushunmadim. Kino kodini yuboring (masalan: <code>105</code>) yoki menyudan foydalaning.",
        reply_markup=main_menu_kb(),
    )
 
 
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
