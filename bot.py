import asyncio
import os
import re
import calendar
import shutil
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, FSInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage


# ================== CONFIG ==================
TZ = ZoneInfo("Europe/Kyiv")

DATA_DIR = os.getenv("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "lashes_bot.sqlite3")

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Add BOT_TOKEN in Railway Variables.")

# Admins by username (WITHOUT @)
ADMIN_USERNAMES = {"roman2696", "Ekaterinahorbatiuk"}

# Where to send admin notifications (comma-separated chat ids)
# Example: ADMIN_CHAT_IDS="123456789,987654321"
ADMIN_CHAT_IDS_RAW = (os.getenv("ADMIN_CHAT_IDS") or "").strip()

# Optional: for "write admin" button
ADMIN_CONTACT_USERNAME = (os.getenv("ADMIN_CONTACT_USERNAME") or "").strip().lstrip("@")

# Schedule
DEFAULT_WEEKS = 12  # ✅ 12 weeks пачкою
WORKING_DAYS = {1, 2, 3, 4, 5}  # Tue-Sat (Mon=0..Sun=6)

DEFAULT_TIMES = ["09:30", "11:30", "13:30"]      # Tue–Fri
SATURDAY_TIMES = ["11:00", "13:00", "15:00"]     # ✅ Sat

# Services
SERV_LAMI = "Ламінування"
SERV_EXT = "Нарощування"
EXT_TYPES = ["Класика", "2D", "3D"]

# Reminders
REMINDER_LOOP_SECONDS = 30
REMIND_DAY_DELTA = timedelta(days=1)
REMIND_HOUR_DELTA = timedelta(hours=1)

# Auto-clean (hide past dates)
CLEANUP_EVERY_HOURS = 6

VERSION = "FULL ALL-IN-ONE v12weeks + admin buttons fixed"


# ================== HELPERS ==================
def is_admin_username(msg_or_cq) -> bool:
    u = msg_or_cq.from_user
    username = (u.username or "").lstrip("@")
    return username in ADMIN_USERNAMES


def admin_chat_ids() -> list[int]:
    if not ADMIN_CHAT_IDS_RAW:
        return []
    out: list[int] = []
    for x in ADMIN_CHAT_IDS_RAW.split(","):
        x = x.strip()
        if re.fullmatch(r"-?\d+", x):
            out.append(int(x))
    return out


def digits_count(s: str) -> int:
    return len(re.sub(r"\D", "", s or ""))


def norm_date_admin(s: str) -> str | None:
    """Accept YYYY-MM-DD or DD.MM.YYYY -> returns ISO YYYY-MM-DD"""
    s = (s or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", s):
        try:
            dt = datetime.strptime(s, "%d.%m.%Y").date()
            return dt.isoformat()
        except Exception:
            return None
    return None


def norm_time(s: str) -> str | None:
    s = (s or "").strip()
    if re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", s):
        return s
    return None


def fmt_date_ua(d_iso: str) -> str:
    try:
        dt = datetime.strptime(d_iso, "%Y-%m-%d").date()
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return d_iso


def times_for_date(d: date) -> list[str]:
    # Saturday = 5
    if d.weekday() == 5:
        return SATURDAY_TIMES
    return DEFAULT_TIMES


def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"


def parse_month_key(key: str) -> tuple[int, int]:
    y, m = key.split("-")
    return int(y), int(m)


def shift_month(y: int, m: int, delta: int) -> tuple[int, int]:
    mm = m + delta
    yy = y
    while mm > 12:
        yy += 1
        mm -= 12
    while mm < 1:
        yy -= 1
        mm += 12
    return yy, mm


def parse_dt_from_callback(call_data: str) -> tuple[str, str]:
    """
    Finds date and time from callback.
    Supports time encoded as HH:MM or HH:MM split by ':'.
    """
    parts = (call_data or "").split(":")
    # try patterns ending with YYYY-MM-DD:HH:MM
    for i in range(len(parts) - 1, -1, -1):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[i]):
            # YYYY-MM-DD : HH : MM
            if i + 2 < len(parts):
                hh, mm = parts[i + 1], parts[i + 2]
                if re.fullmatch(r"[0-2]\d", hh) and re.fullmatch(r"[0-5]\d", mm):
                    return parts[i], f"{hh}:{mm}"
            # YYYY-MM-DD : HH:MM
            if i + 1 < len(parts) and re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", parts[i + 1]):
                return parts[i], parts[i + 1]
    return "", ""


def booking_dt_local(d_iso: str, t_hhmm: str) -> datetime | None:
    try:
        y, m, dd = map(int, d_iso.split("-"))
        hh, mm = map(int, t_hhmm.split(":"))
        return datetime(y, m, dd, hh, mm, tzinfo=TZ)
    except Exception:
        return None


def first_day_current_month() -> date:
    today = datetime.now(TZ).date()
    return date(today.year, today.month, 1)


# ================== DB ==================
async def ensure_schema():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            d TEXT NOT NULL,
            t TEXT NOT NULL,
            is_open INTEGER NOT NULL DEFAULT 1
        )
        """)
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_slots_dt ON slots(d, t)")

        await db.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            client_name TEXT,
            phone TEXT,
            service TEXT,
            ext_type TEXT,
            d TEXT,
            t TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT
        )
        """)

        # reminders log (avoid duplicates)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            booking_id INTEGER NOT NULL,
            kind TEXT NOT NULL,  -- 'day' or 'hour'
            sent_at TEXT NOT NULL
        )
        """)
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_reminders_unique ON reminders(booking_id, kind)")

        await db.commit()


async def cleanup_past_slots():
    cutoff = first_day_current_month().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM slots WHERE d < ?", (cutoff,))
        await db.commit()


async def bulk_add_default_slots(weeks: int = DEFAULT_WEEKS) -> tuple[int, int]:
    today = datetime.now(TZ).date()
    end = today + timedelta(days=weeks * 7)
    added, skipped = 0, 0

    async with aiosqlite.connect(DB_PATH) as db:
        cur_d = today
        while cur_d <= end:
            if cur_d.weekday() in WORKING_DAYS:
                d_str = cur_d.isoformat()
                for tm in times_for_date(cur_d):
                    try:
                        await db.execute("INSERT INTO slots(d,t,is_open) VALUES(?,?,1)", (d_str, tm))
                        added += 1
                    except aiosqlite.IntegrityError:
                        skipped += 1
            cur_d += timedelta(days=1)
        await db.commit()

    return added, skipped


async def add_slot(d: str, t: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute("INSERT INTO slots(d,t,is_open) VALUES(?,?,1)", (d, t))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def get_open_times(d: str) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT t FROM slots WHERE d=? AND is_open=1 ORDER BY t", (d,))
        rows = await cur.fetchall()
        return [r[0] for r in rows]


async def get_slots_day(d: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, t, is_open FROM slots WHERE d=? ORDER BY t", (d,))
        rows = await cur.fetchall()
    return [{"id": r[0], "t": r[1], "is_open": r[2]} for r in rows]


async def delete_open_slot(slot_id: int) -> bool:
    """delete only if open"""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT is_open FROM slots WHERE id=?", (slot_id,))
        row = await cur.fetchone()
        if not row or row[0] != 1:
            return False
        await db.execute("DELETE FROM slots WHERE id=?", (slot_id,))
        await db.commit()
        return True


async def book_slot(user_id: int, username: str, client_name: str, phone: str,
                    service: str, ext_type: str | None, d: str, t: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT is_open FROM slots WHERE d=? AND t=?", (d, t))
        row = await cur.fetchone()
        if not row or row[0] != 1:
            return False

        await db.execute("UPDATE slots SET is_open=0 WHERE d=? AND t=?", (d, t))
        await db.execute("""
            INSERT INTO bookings(user_id, username, client_name, phone, service, ext_type, d, t, status, created_at)
            VALUES(?,?,?,?,?,?,?,?, 'active', ?)
        """, (user_id, username, client_name, phone, service, ext_type, d, t,
              datetime.now(TZ).isoformat()))
        await db.commit()
        return True


async def get_booking(booking_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, user_id, username, client_name, phone, service, ext_type, d, t, status
            FROM bookings WHERE id=?
        """, (booking_id,))
        r = await cur.fetchone()
    if not r:
        return None
    return {
        "id": r[0], "user_id": r[1], "username": r[2], "client_name": r[3], "phone": r[4],
        "service": r[5], "ext_type": r[6], "d": r[7], "t": r[8], "status": r[9]
    }


async def get_day_bookings(d: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, user_id, username, client_name, phone, service, ext_type, t, status
            FROM bookings WHERE d=? ORDER BY t
        """, (d,))
        rows = await cur.fetchall()
    return [{
        "id": r[0], "user_id": r[1], "username": r[2], "client_name": r[3], "phone": r[4],
        "service": r[5], "ext_type": r[6], "t": r[7], "status": r[8]
    } for r in rows]


async def get_user_active_bookings(user_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, d, t, service, ext_type, client_name, phone
            FROM bookings
            WHERE user_id=? AND status='active'
            ORDER BY d, t
        """, (user_id,))
        rows = await cur.fetchall()
    return [{
        "id": r[0], "d": r[1], "t": r[2], "service": r[3],
        "ext_type": r[4], "client_name": r[5], "phone": r[6]
    } for r in rows]


async def cancel_booking(booking_id: int) -> tuple[bool, str, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT d,t,status FROM bookings WHERE id=?", (booking_id,))
        row = await cur.fetchone()
        if not row:
            return False, "", ""
        d, t, status = row
        if status == "canceled":
            return True, d, t

        await db.execute("UPDATE bookings SET status='canceled' WHERE id=?", (booking_id,))
        await db.execute("UPDATE slots SET is_open=1 WHERE d=? AND t=?", (d, t))
        await db.commit()
        return True, d, t


async def move_booking(booking_id: int, new_d: str, new_t: str) -> tuple[bool, str, str, str, str]:
    """returns ok, old_d, old_t, new_d, new_t"""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT d,t,status FROM bookings WHERE id=?", (booking_id,))
        row = await cur.fetchone()
        if not row:
            return False, "", "", "", ""
        old_d, old_t, status = row
        if status != "active":
            return False, old_d, old_t, "", ""

        cur2 = await db.execute("SELECT is_open FROM slots WHERE d=? AND t=?", (new_d, new_t))
        row2 = await cur2.fetchone()
        if not row2 or row2[0] != 1:
            return False, old_d, old_t, new_d, new_t

        await db.execute("UPDATE slots SET is_open=1 WHERE d=? AND t=?", (old_d, old_t))
        await db.execute("UPDATE slots SET is_open=0 WHERE d=? AND t=?", (new_d, new_t))
        await db.execute("UPDATE bookings SET d=?, t=? WHERE id=?", (new_d, new_t, booking_id))
        await db.commit()
        return True, old_d, old_t, new_d, new_t


async def reminder_sent(booking_id: int, kind: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM reminders WHERE booking_id=? AND kind=? LIMIT 1", (booking_id, kind))
        row = await cur.fetchone()
        return bool(row)


async def mark_reminder_sent(booking_id: int, kind: str):
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO reminders(booking_id, kind, sent_at) VALUES(?,?,?)",
                (booking_id, kind, datetime.now(TZ).isoformat())
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            pass


# ================== BOT / DISPATCHER ==================
bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ================== NOTIFY ==================
async def notify_admins(text: str):
    for cid in admin_chat_ids():
        try:
            await bot.send_message(cid, text)
        except Exception:
            pass


# ================== UI (Reply keyboard) ==================
def rk_main(is_admin: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🏠 Меню"), KeyboardButton(text="📝 Записатись")],
        [KeyboardButton(text="📋 Мої записи")],
    ]
    if is_admin:
        rows.append([KeyboardButton(text="🛠 Адмін")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


# ================== UI (Inline keyboards) ==================
def kb_start() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📝 Записатись", callback_data="u:start")],
        [InlineKeyboardButton(text="📋 Мої записи", callback_data="u:my")],
    ]
    if ADMIN_CONTACT_USERNAME:
        rows.append([InlineKeyboardButton(text="💬 Написати адміну", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")])
    rows.append([InlineKeyboardButton(text="🛠 Адмін", callback_data="a:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_services() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✨ {SERV_LAMI}", callback_data="u:serv:lami")],
        [InlineKeyboardButton(text=f"💫 {SERV_EXT}", callback_data="u:serv:ext")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="u:back:start")]
    ])


def kb_ext_types() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for x in EXT_TYPES:
        b.row(InlineKeyboardButton(text=x, callback_data=f"u:ext:{x}"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="u:back:services"))
    return b.as_markup()


def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Підтвердити", callback_data="u:confirm:yes"),
            InlineKeyboardButton(text="❌ Скасувати", callback_data="u:confirm:no")
        ]
    ])


def kb_restart_booking() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Записатись заново", callback_data="u:start")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="u:back:start")]
    ])


def kb_calendar(month: str, prefix: str) -> InlineKeyboardMarkup:
    y, m = parse_month_key(month)
    cal = calendar.monthcalendar(y, m)

    today = datetime.now(TZ).date()
    min_month = (today.year, today.month)

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text=f"📅 {calendar.month_name[m]} {y}", callback_data="noop"))

    wd = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
    b.row(*[InlineKeyboardButton(text=x, callback_data="noop") for x in wd])

    for week in cal:
        row_btns = []
        for day in week:
            if day == 0:
                row_btns.append(InlineKeyboardButton(text=" ", callback_data="noop"))
            else:
                d_obj = date(y, m, day)
                d_str = d_obj.isoformat()
                if d_obj < today:
                    row_btns.append(InlineKeyboardButton(text=str(day), callback_data="noop"))
                else:
                    row_btns.append(InlineKeyboardButton(text=str(day), callback_data=f"{prefix}:day:{d_str}"))
        b.row(*row_btns)

    prev_y, prev_m = shift_month(y, m, -1)
    next_y, next_m = shift_month(y, m, +1)

    if (prev_y, prev_m) < min_month:
        prev_btn = InlineKeyboardButton(text="⬅️", callback_data="noop")
    else:
        prev_btn = InlineKeyboardButton(text="⬅️", callback_data=f"{prefix}:month:{month_key(prev_y, prev_m)}")

    next_btn = InlineKeyboardButton(text="➡️", callback_data=f"{prefix}:month:{month_key(next_y, next_m)}")
    b.row(prev_btn, next_btn)
    return b.as_markup()


def kb_times(d: str, times: list[str], prefix: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for t in times:
        b.row(InlineKeyboardButton(text=t, callback_data=f"{prefix}:time:{d}:{t}"))
    b.row(InlineKeyboardButton(text="⬅️ Назад до календаря", callback_data=f"{prefix}:backcal"))
    return b.as_markup()


def kb_admin_menu() -> InlineKeyboardMarkup:
    # ✅ callback_data exactly matches handlers below
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📅 Додати графік пачкою ({DEFAULT_WEEKS} тижнів)", callback_data="a:bulk")],
        [InlineKeyboardButton(text="➕ Додати слот вручну", callback_data="a:addslot")],
        [InlineKeyboardButton(text="🗑 Видалити час у дні (слоти)", callback_data="a:del_slot_day")],
        [InlineKeyboardButton(text="📆 Переглянути записи по дню", callback_data="a:view_day")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="u:back:start")],
    ])


def kb_admin_day_actions(d: str, bookings: list[dict]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if bookings:
        for bk in bookings:
            extra = f" ({bk['ext_type']})" if bk["ext_type"] else ""
            label = f"{bk['t']} — {bk['client_name']} — {bk['service']}{extra}"
            if bk["status"] != "active":
                label = "🚫 " + label
            b.row(InlineKeyboardButton(text=f"❌ Скасувати #{bk['id']} • {label}", callback_data=f"a:cancel:{bk['id']}"))
            b.row(InlineKeyboardButton(text=f"🔁 Перенести #{bk['id']} • {label}", callback_data=f"a:move:{bk['id']}"))
    else:
        b.row(InlineKeyboardButton(text="(Нема записів)", callback_data="noop"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="a:menu"))
    return b.as_markup()


def kb_slots_delete_day(d: str, slots: list[dict]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if slots:
        for s in slots:
            if s["is_open"] == 1:
                b.row(InlineKeyboardButton(text=f"🗑 Видалити {s['t']}", callback_data=f"a:del_slot:{s['id']}:{d}"))
            else:
                b.row(InlineKeyboardButton(text=f"🔒 {s['t']} (зайнято)", callback_data="noop"))
    else:
        b.row(InlineKeyboardButton(text="(Нема слотів)", callback_data="noop"))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="a:menu"))
    return b.as_markup()


def kb_user_my(items: list[dict]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for it in items:
        extra = f" ({it['ext_type']})" if it.get("ext_type") else ""
        txt = f"❌ Скасувати {fmt_date_ua(it['d'])} {it['t']} — {it['service']}{extra}"
        b.row(InlineKeyboardButton(text=txt, callback_data=f"u:cancel:{it['id']}"))
    b.row(InlineKeyboardButton(text="🏠 Меню", callback_data="u:back:start"))
    return b.as_markup()


# ================== FSM ==================
class UserBooking(StatesGroup):
    service = State()
    ext_type = State()
    day = State()
    time = State()
    fullname = State()
    phone = State()
    confirm = State()


class AdminAddSlot(StatesGroup):
    d = State()
    t = State()


# ================== NAV (Reply) ==================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Меню 👇", reply_markup=rk_main(is_admin_username(message)))
    await message.answer("Оберіть дію:", reply_markup=kb_start())


@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Ваш chat_id: {message.chat.id}")


@dp.message(F.text == "🏠 Меню")
async def nav_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Оберіть дію:", reply_markup=kb_start())


@dp.message(F.text == "📝 Записатись")
async def nav_book(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(UserBooking.service)
    await message.answer("Оберіть послугу:", reply_markup=kb_services())


@dp.message(F.text == "📋 Мої записи")
async def nav_my(message: Message, state: FSMContext):
    await state.clear()
    items = await get_user_active_bookings(message.from_user.id)
    if not items:
        return await message.answer("У вас немає активних записів.")
    await message.answer("Ваші активні записи (можна скасувати):", reply_markup=kb_user_my(items))


@dp.message(F.text == "🛠 Адмін")
async def nav_admin(message: Message, state: FSMContext):
    await state.clear()
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    await message.answer("🛠 Адмін-панель:", reply_markup=kb_admin_menu())


# ================== USER FLOW (callbacks) ==================
@dp.callback_query(F.data == "u:start")
async def u_start(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(UserBooking.service)
    await call.message.answer("Оберіть послугу:", reply_markup=kb_services())
    await call.answer()


@dp.callback_query(F.data == "u:my")
async def u_my(call: CallbackQuery, state: FSMContext):
    await state.clear()
    items = await get_user_active_bookings(call.from_user.id)
    if not items:
        await call.message.answer("У вас немає активних записів.")
    else:
        await call.message.answer("Ваші активні записи (можна скасувати):", reply_markup=kb_user_my(items))
    await call.answer()


@dp.callback_query(F.data.startswith("u:back:"))
async def u_back(call: CallbackQuery, state: FSMContext):
    where = call.data.split(":")[-1]
    if where == "start":
        await state.clear()
        await call.message.answer("Оберіть дію:", reply_markup=kb_start())
    elif where == "services":
        await state.set_state(UserBooking.service)
        await call.message.answer("Оберіть послугу:", reply_markup=kb_services())
    await call.answer()


@dp.callback_query(F.data.startswith("u:serv:"))
async def u_service(call: CallbackQuery, state: FSMContext):
    serv = call.data.split(":")[-1]
    if serv == "lami":
        await state.update_data(service=SERV_LAMI, ext_type=None)
        await state.set_state(UserBooking.day)
        today = datetime.now(TZ).date()
        await call.message.answer("Оберіть дату (календар):",
                                reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
    else:
        await state.update_data(service=SERV_EXT)
        await state.set_state(UserBooking.ext_type)
        await call.message.answer("Оберіть тип нарощування:", reply_markup=kb_ext_types())
    await call.answer()


@dp.callback_query(F.data.startswith("u:ext:"))
async def u_ext(call: CallbackQuery, state: FSMContext):
    ext = call.data.split(":", 2)[-1]
    await state.update_data(ext_type=ext)
    await state.set_state(UserBooking.day)
    today = datetime.now(TZ).date()
    await call.message.answer("Оберіть дату (календар):",
                             reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
    await call.answer()


@dp.callback_query(F.data.startswith("u:month:"))
async def u_month(call: CallbackQuery):
    mk = call.data.split(":")[-1]
    await call.message.edit_reply_markup(reply_markup=kb_calendar(mk, "u"))
    await call.answer()


@dp.callback_query(F.data.startswith("u:day:"))
async def u_day(call: CallbackQuery, state: FSMContext):
    d = call.data.split(":")[-1]
    await state.update_data(day=d)
    times = await get_open_times(d)
    if not times:
        today = datetime.now(TZ).date()
        await call.message.answer("На цю дату немає вільних віконець. Оберіть іншу.")
        await call.message.answer("Календар:", reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
        await call.answer()
        return

    await state.set_state(UserBooking.time)
    await call.message.answer(f"Вільний час на {fmt_date_ua(d)}:", reply_markup=kb_times(d, times, "u"))
    await call.answer()


@dp.callback_query(F.data == "u:backcal")
async def u_backcal(call: CallbackQuery):
    today = datetime.now(TZ).date()
    await call.message.answer("Оберіть дату:",
                             reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
    await call.answer()


@dp.callback_query(F.data.startswith("u:time:"))
async def u_time(call: CallbackQuery, state: FSMContext):
    d, t = parse_dt_from_callback(call.data)
    await state.update_data(day=d, time=t)
    await state.set_state(UserBooking.fullname)
    await call.message.answer("Вкажіть Прізвище та Ім’я (2 слова), наприклад: Іваненко Марія")
    await call.answer()


@dp.message(UserBooking.fullname)
async def u_fullname(message: Message, state: FSMContext):
    fullname = (message.text or "").strip()
    if len(fullname.split()) < 2:
        return await message.answer("Напишіть Прізвище та Ім’я (2 слова).")
    await state.update_data(client_name=fullname)
    await state.set_state(UserBooking.phone)
    await message.answer("Тепер номер телефону (наприклад +380XXXXXXXXX):")


@dp.message(UserBooking.phone)
async def u_phone(message: Message, state: FSMContext):
    phone = (message.text or "").strip()
    if digits_count(phone) < 9:
        return await message.answer("Номер виглядає некоректно. Введіть ще раз.")

    await state.update_data(phone=phone)
    data = await state.get_data()

    required = ("service", "day", "time", "client_name", "phone")
    if any(not data.get(k) for k in required):
        await state.clear()
        return await message.answer(
            "⚠️ Сесія запису перервана (оновлення/перезапуск).\nПочніть запис заново 👇",
            reply_markup=kb_restart_booking()
        )

    text = (
        "Перевірте запис 👇\n\n"
        f"📅 Дата: {fmt_date_ua(data['day'])}\n"
        f"🕒 Час: {data['time']}\n"
        f"💅 Послуга: {data['service']}{f' ({data.get('ext_type')})' if data.get('ext_type') else ''}\n"
        f"👤 Клієнт: {data['client_name']}\n"
        f"📞 Телефон: {data['phone']}\n\n"
        "Підтверджуєте?"
    )
    await state.set_state(UserBooking.confirm)
    await message.answer(text, reply_markup=kb_confirm())


@dp.callback_query(F.data.startswith("u:confirm:"))
async def u_confirm(call: CallbackQuery, state: FSMContext):
    action = call.data.split(":")[-1]
    data = await state.get_data()

    if action == "no":
        await state.clear()
        await call.message.answer("❌ Скасовано. Меню 👇", reply_markup=kb_start())
        await call.answer()
        return

    required = ("service", "day", "time", "client_name", "phone")
    if any(not data.get(k) for k in required):
        await state.clear()
        await call.message.answer(
            "⚠️ Не можу підтвердити — сесія застаріла.\nЗробіть запис заново 👇",
            reply_markup=kb_restart_booking()
        )
        await call.answer()
        return

    ok = await book_slot(
        user_id=call.from_user.id,
        username=(call.from_user.username or ""),
        client_name=data["client_name"],
        phone=data["phone"],
        service=data["service"],
        ext_type=data.get("ext_type"),
        d=data["day"],
        t=data["time"],
    )

    if not ok:
        await call.message.answer("❌ На жаль цей час вже зайняли. Оберіть інший.")
        await state.set_state(UserBooking.day)
        today = datetime.now(TZ).date()
        await call.message.answer("Календар:", reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
        await call.answer()
        return

    await call.message.answer(
        "✅ Запис підтверджено!\n"
        f"Дата: {fmt_date_ua(data['day'])}\n"
        f"Час: {data['time']}\n"
        f"Послуга: {data['service']}{f' ({data.get('ext_type')})' if data.get('ext_type') else ''}"
    )
    await notify_admins(
        "📥 НОВИЙ ЗАПИС\n\n"
        f"📅 {fmt_date_ua(data['day'])}\n"
        f"🕒 {data['time']}\n"
        f"💅 {data['service']}{f' ({data.get('ext_type')})' if data.get('ext_type') else ''}\n"
        f"👤 {data['client_name']}\n"
        f"📞 {data['phone']}\n"
        f"🔗 @{call.from_user.username or '-'} | id:{call.from_user.id}"
    )

    await state.clear()
    await call.answer()


@dp.callback_query(F.data.startswith("u:cancel:"))
async def u_cancel_booking(call: CallbackQuery, state: FSMContext):
    bid = int(call.data.split(":")[-1])
    bk = await get_booking(bid)
    if not bk or bk["user_id"] != call.from_user.id:
        await call.answer("Не знайдено запис.", show_alert=True)
        return

    ok, d, t = await cancel_booking(bid)
    if ok:
        await call.message.answer(f"✅ Ваш запис скасовано: {fmt_date_ua(d)} {t}")
        await notify_admins(
            "📤 СКАСУВАННЯ КЛІЄНТОМ\n\n"
            f"Запис #{bid}\n"
            f"📅 {fmt_date_ua(d)}\n"
            f"🕒 {t}\n"
            f"👤 {bk['client_name']}\n"
            f"📞 {bk['phone']}\n"
            f"🔗 @{bk['username'] or '-'} | id:{bk['user_id']}"
        )
    else:
        await call.message.answer("❌ Не знайшов запис.")
    await call.answer()


# ================== ADMIN ==================
@dp.callback_query(F.data == "a:menu")
async def a_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    await call.message.answer("🛠 Адмін-панель:", reply_markup=kb_admin_menu())
    await call.answer()


@dp.callback_query(F.data == "a:bulk")
async def a_bulk(call: CallbackQuery):
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    added, skipped = await bulk_add_default_slots(DEFAULT_WEEKS)
    await call.message.answer(
        f"✅ Готово!\n"
        f"Додано слотів: {added}\n"
        f"Вже існували (пропущено): {skipped}\n\n"
        f"Період: {DEFAULT_WEEKS} тижнів\n"
        f"Вт–Пт: {', '.join(DEFAULT_TIMES)}\n"
        f"Сб: {', '.join(SATURDAY_TIMES)}"
    )
    await call.answer()


@dp.callback_query(F.data == "a:addslot")
async def a_addslot(call: CallbackQuery, state: FSMContext):
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    await state.clear()
    await state.set_state(AdminAddSlot.d)
    await call.message.answer("Введіть дату слоту у форматі DD.MM.YYYY або YYYY-MM-DD:")
    await call.answer()


@dp.message(AdminAddSlot.d)
async def a_addslot_d(message: Message, state: FSMContext):
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    d = norm_date_admin(message.text)
    if not d:
        return await message.answer("❌ Невірна дата. Формат: DD.MM.YYYY або YYYY-MM-DD.")
    await state.update_data(d=d)
    await state.set_state(AdminAddSlot.t)
    await message.answer("Введіть час HH:MM (наприклад 15:30):")


@dp.message(AdminAddSlot.t)
async def a_addslot_t(message: Message, state: FSMContext):
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    t = norm_time(message.text)
    if not t:
        return await message.answer("❌ Невірний формат часу. Введіть HH:MM.")
    data = await state.get_data()
    inserted = await add_slot(data["d"], t)
    await message.answer("✅ Слот додано." if inserted else "ℹ️ Такий слот вже існує.")
    await state.clear()


@dp.callback_query(F.data == "a:view_day")
async def a_view_day(call: CallbackQuery):
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    today = datetime.now(TZ).date()
    await call.message.answer("Оберіть дату (календар):",
                             reply_markup=kb_calendar(month_key(today.year, today.month), "a_view"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_view:month:"))
async def a_view_month(call: CallbackQuery):
    mk = call.data.split(":")[-1]
    await call.message.edit_reply_markup(reply_markup=kb_calendar(mk, "a_view"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_view:day:"))
async def a_view_day_show(call: CallbackQuery):
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    d = call.data.split(":")[-1]
    bookings = await get_day_bookings(d)

    lines = [f"📌 Записи на {fmt_date_ua(d)}:"]
    if not bookings:
        lines.append("— немає")
    else:
        for bk in bookings:
            st = "✅" if bk["status"] == "active" else "🚫"
            extra = f" ({bk['ext_type']})" if bk["ext_type"] else ""
            lines.append(f"{st} {bk['t']} — {bk['client_name']} {bk['phone']} — {bk['service']}{extra}")

    await call.message.answer("\n".join(lines), reply_markup=kb_admin_day_actions(d, bookings))
    await call.answer()


@dp.callback_query(F.data.startswith("a:cancel:"))
async def a_cancel(call: CallbackQuery):
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    bid = int(call.data.split(":")[-1])
    bk = await get_booking(bid)

    ok, d, t = await cancel_booking(bid)
    if not ok:
        await call.message.answer("❌ Не знайшов запис.")
        await call.answer()
        return

    await call.message.answer(f"✅ Запис #{bid} скасовано. Слот {fmt_date_ua(d)} {t} відкрито.")
    await notify_admins(f"🛠 АДМІН СКАСУВАВ ЗАПИС #{bid}\n📅 {fmt_date_ua(d)}\n🕒 {t}")

    # notify client
    if bk and bk["user_id"]:
        try:
            await bot.send_message(
                bk["user_id"],
                f"❌ Ваш запис скасовано адміністратором.\n📅 {fmt_date_ua(d)}\n🕒 {t}"
            )
        except Exception:
            pass

    await call.answer()


@dp.callback_query(F.data.startswith("a:move:"))
async def a_move_start(call: CallbackQuery):
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    bid = int(call.data.split(":")[-1])
    today = datetime.now(TZ).date()
    await call.message.answer(
        f"Оберіть НОВУ дату для переносу запису #{bid}:",
        reply_markup=kb_calendar(month_key(today.year, today.month), f"a_move:{bid}")
    )
    await call.answer()


@dp.callback_query(F.data.startswith("a_move:") & F.data.contains(":month:"))
async def a_move_month(call: CallbackQuery):
    parts = call.data.split(":")
    bid = parts[1]
    mk = parts[-1]
    await call.message.edit_reply_markup(reply_markup=kb_calendar(mk, f"a_move:{bid}"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_move:") & F.data.contains(":day:"))
async def a_move_day(call: CallbackQuery):
    parts = call.data.split(":")
    bid = parts[1]
    d = parts[-1]
    times = await get_open_times(d)
    if not times:
        await call.message.answer("На цю дату немає вільних слотів. Оберіть іншу дату.")
        await call.answer()
        return
    await call.message.answer(f"Оберіть НОВИЙ час для #{bid} на {fmt_date_ua(d)}:",
                             reply_markup=kb_times(d, times, f"a_move:{bid}"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_move:") & F.data.contains(":time:"))
async def a_move_time(call: CallbackQuery):
    parts = call.data.split(":")
    bid = int(parts[1])
    d, t = parse_dt_from_callback(call.data)
    bk = await get_booking(bid)

    ok, old_d, old_t, new_d, new_t = await move_booking(bid, d, t)
    if not ok:
        await call.message.answer("❌ Не вдалося перенести (слот зайнятий/запис неактивний).")
        await call.answer()
        return

    await call.message.answer(f"✅ Перенесено запис #{bid} на {fmt_date_ua(new_d)} {new_t}.")
    await notify_admins(
        f"🔁 ПЕРЕНЕСЕННЯ (адмін)\n"
        f"Запис #{bid}\n"
        f"Було: {fmt_date_ua(old_d)} {old_t}\n"
        f"Стало: {fmt_date_ua(new_d)} {new_t}"
    )

    # notify client
    if bk and bk["user_id"]:
        try:
            await bot.send_message(
                bk["user_id"],
                "🔁 Ваш запис перенесено адміністратором.\n"
                f"Було: {fmt_date_ua(old_d)} {old_t}\n"
                f"Стало: {fmt_date_ua(new_d)} {new_t}"
            )
        except Exception:
            pass

    await call.answer()


@dp.callback_query(F.data == "a:del_slot_day")
async def a_del_slot_day(call: CallbackQuery):
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    today = datetime.now(TZ).date()
    await call.message.answer("Оберіть дату (календар):",
                             reply_markup=kb_calendar(month_key(today.year, today.month), "a_delday"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_delday:month:"))
async def a_delday_month(call: CallbackQuery):
    mk = call.data.split(":")[-1]
    await call.message.edit_reply_markup(reply_markup=kb_calendar(mk, "a_delday"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_delday:day:"))
async def a_delday_show_slots(call: CallbackQuery):
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    d = call.data.split(":")[-1]
    slots = await get_slots_day(d)
    await call.message.answer(f"Слоти на {fmt_date_ua(d)} (можна видалити тільки ВІЛЬНІ):",
                             reply_markup=kb_slots_delete_day(d, slots))
    await call.answer()


@dp.callback_query(F.data.startswith("a:del_slot:"))
async def a_del_slot(call: CallbackQuery):
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    # a:del_slot:<slot_id>:<YYYY-MM-DD>
    parts = call.data.split(":")
    slot_id = int(parts[2])
    d = parts[3] if len(parts) > 3 else ""

    ok = await delete_open_slot(slot_id)
    if not ok:
        await call.answer("Не можна видалити (зайнято або не знайдено).", show_alert=True)
        return

    slots = await get_slots_day(d) if d else []
    await call.message.answer("✅ Слот видалено.", reply_markup=kb_slots_delete_day(d, slots))
    await call.answer()


# ================== BACKUP COMMAND ==================
@dp.message(Command("backup"))
async def cmd_backup(message: Message):
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    try:
        ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(DATA_DIR, f"backup_{ts}.sqlite3")
        shutil.copy2(DB_PATH, backup_path)
        await message.answer_document(FSInputFile(backup_path), caption="✅ Backup бази")
    except Exception as e:
        await message.answer(f"❌ Не вдалося зробити backup: {e}")


# ================== REMINDERS LOOP ==================
async def reminders_loop():
    while True:
        try:
            now = datetime.now(TZ)
            # check active bookings in next ~2 days
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("""
                    SELECT id, user_id, client_name, phone, service, ext_type, d, t
                    FROM bookings
                    WHERE status='active'
                """)
                rows = await cur.fetchall()

            for r in rows:
                bid, user_id, client_name, phone, service, ext_type, d_iso, t_hhmm = r
                dt = booking_dt_local(d_iso, t_hhmm)
                if not dt:
                    continue

                delta = dt - now
                if delta.total_seconds() <= 0:
                    continue  # already in past

                # day reminder
                if abs(delta - REMIND_DAY_DELTA) <= timedelta(minutes=2):
                    if not await reminder_sent(bid, "day"):
                        try:
                            extra = f" ({ext_type})" if ext_type else ""
                            await bot.send_message(
                                user_id,
                                "⏰ Нагадування: завтра у вас запис!\n"
                                f"📅 {fmt_date_ua(d_iso)}\n"
                                f"🕒 {t_hhmm}\n"
                                f"💅 {service}{extra}\n"
                                f"👤 {client_name}\n"
                                f"📞 {phone}"
                            )
                        except Exception:
                            pass
                        await mark_reminder_sent(bid, "day")

                # hour reminder
                if abs(delta - REMIND_HOUR_DELTA) <= timedelta(minutes=2):
                    if not await reminder_sent(bid, "hour"):
                        try:
                            extra = f" ({ext_type})" if ext_type else ""
                            await bot.send_message(
                                user_id,
                                "⏰ Нагадування: через 1 годину у вас запис!\n"
                                f"📅 {fmt_date_ua(d_iso)}\n"
                                f"🕒 {t_hhmm}\n"
                                f"💅 {service}{extra}\n"
                                f"👤 {client_name}\n"
                                f"📞 {phone}"
                            )
                        except Exception:
                            pass
                        await mark_reminder_sent(bid, "hour")

        except Exception:
            pass

        await asyncio.sleep(REMINDER_LOOP_SECONDS)


async def cleanup_loop():
    while True:
        try:
            await cleanup_past_slots()
        except Exception:
            pass
        await asyncio.sleep(CLEANUP_EVERY_HOURS * 3600)


# ================== NOOP ==================
@dp.callback_query(F.data == "noop")
async def noop(call: CallbackQuery):
    await call.answer()


# ================== MAIN ==================
async def main():
    await ensure_schema()
    await cleanup_past_slots()

    asyncio.create_task(reminders_loop())
    asyncio.create_task(cleanup_loop())

    print(f"DB_PATH = {DB_PATH}", flush=True)
    print(f"VERSION: {VERSION}", flush=True)
    print("=== BOT STARTED (polling) ===", flush=True)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
