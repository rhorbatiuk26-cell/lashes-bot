import asyncio
import os
import re
import calendar
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage


# ================== CONFIG ==================
DB_PATH = "lashes_bot.sqlite3"

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Add BOT_TOKEN in Railway Variables.")

# admins by username (WITHOUT @)
ADMIN_USERNAMES = {"roman2696", "Ekaterinahorbatiuk"}

# Optional: inline button "Write to admin"
ADMIN_CONTACT_USERNAME = (os.getenv("ADMIN_CONTACT_USERNAME") or "").strip().lstrip("@")

# Admin notification chats (comma-separated chat_ids)
# Railway Variables: ADMIN_CHAT_IDS="123,456"
def admin_chat_targets() -> list[int]:
    raw = (os.getenv("ADMIN_CHAT_IDS") or "").strip()
    if not raw:
        return []
    ids: list[int] = []
    for x in raw.split(","):
        x = x.strip()
        if x.isdigit():
            ids.append(int(x))
    return ids


# timezone
TZ = ZoneInfo("Europe/Kyiv")

# reminders
REMINDER_INTERVAL_SEC = 60
DAY_WINDOW_MIN = 10
HOUR_WINDOW_MIN = 10

# bulk schedule: Tue-Sat (Mon=0..Sun=6)
DEFAULT_TIMES = ["09:30", "11:30", "13:30"]
WORKING_DAYS = {1, 2, 3, 4, 5}  # Tue-Sat
DEFAULT_WEEKS = 4

SERV_LAMI = "Ламінування"
SERV_EXT = "Нарощування"
EXT_TYPES = ["Класика", "2D", "3D"]


# ================== HELPERS ==================
def is_admin_username(msg_or_cq) -> bool:
    u = msg_or_cq.from_user
    username = (u.username or "").lstrip("@")
    return username in ADMIN_USERNAMES


def digits_count(s: str) -> int:
    return len(re.sub(r"\D", "", s or ""))


def norm_date(s: str) -> str | None:
    s = (s or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    return None


def norm_date_admin(s: str) -> str | None:
    """
    Accepts:
      - YYYY-MM-DD
      - DD.MM.YYYY
    Returns ISO: YYYY-MM-DD
    """
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
    callback_data examples:
      u:time:YYYY-MM-DD:HH:MM
      u_move:123:time:YYYY-MM-DD:HH:MM
      a_move:123:time:YYYY-MM-DD:HH:MM
    returns ("YYYY-MM-DD", "HH:MM")
    """
    parts = (call_data or "").split(":")
    if len(parts) < 3:
        return "", ""

    for i in range(len(parts) - 1, -1, -1):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[i]):
            # HH:MM split into HH and MM
            if i + 2 < len(parts):
                hh = parts[i + 1]
                mm = parts[i + 2]
                if re.fullmatch(r"[0-2]\d", hh) and re.fullmatch(r"[0-5]\d", mm):
                    return parts[i], f"{hh}:{mm}"
            # HH:MM as one token
            if i + 1 < len(parts) and re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", parts[i + 1]):
                return parts[i], parts[i + 1]
    return "", ""


def appt_dt_local(d: str, t: str) -> datetime | None:
    try:
        dt_naive = datetime.strptime(f"{d} {t}", "%Y-%m-%d %H:%M")
        return dt_naive.replace(tzinfo=TZ)
    except Exception:
        return None


def fmt_date_iso_to_ua(d: str) -> str:
    try:
        dt = datetime.strptime(d, "%Y-%m-%d").date()
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return d


def fmt_dt(d: str, t: str) -> str:
    return f"{fmt_date_iso_to_ua(d)} {t}"


def tg_user_label(user_id: int, username: str | None) -> str:
    u = (username or "").strip()
    if u:
        return f"@{u} | id:{user_id}"
    return f"(без username) | id:{user_id}"


def split_time(t: str) -> tuple[str, str]:
    hh, mm = (t or "").split(":")
    return hh, mm


# ================== Reply Keyboard (always visible) ==================
def rk_main(is_admin: bool) -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="🏠 Меню"), KeyboardButton(text="📝 Записатись")],
        [KeyboardButton(text="📋 Мої записи")],
    ]
    if is_admin:
        keyboard.append([KeyboardButton(text="🛠 Адмін")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


# ================== DB ==================
async def _column_exists(db: aiosqlite.Connection, table: str, col: str) -> bool:
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    cols = {r[1] for r in rows}
    return col in cols


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

        # reminders flags
        if not await _column_exists(db, "bookings", "reminded_day"):
            await db.execute("ALTER TABLE bookings ADD COLUMN reminded_day INTEGER NOT NULL DEFAULT 0")
        if not await _column_exists(db, "bookings", "reminded_hour"):
            await db.execute("ALTER TABLE bookings ADD COLUMN reminded_hour INTEGER NOT NULL DEFAULT 0")

        await db.commit()


async def bulk_add_default_slots(weeks: int = DEFAULT_WEEKS) -> tuple[int, int]:
    today = date.today()
    end = today + timedelta(days=weeks * 7)
    added, skipped = 0, 0

    async with aiosqlite.connect(DB_PATH) as db:
        cur_d = today
        while cur_d <= end:
            if cur_d.weekday() in WORKING_DAYS:
                d_str = cur_d.isoformat()
                for tm in DEFAULT_TIMES:
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


async def book_slot(user_id: int, username: str, client_name: str, phone: str,
                    service: str, ext_type: str | None, d: str, t: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT is_open FROM slots WHERE d=? AND t=?", (d, t))
        row = await cur.fetchone()
        if not row or row[0] != 1:
            return False

        await db.execute("UPDATE slots SET is_open=0 WHERE d=? AND t=?", (d, t))
        await db.execute("""
            INSERT INTO bookings(user_id, username, client_name, phone, service, ext_type, d, t, status, created_at, reminded_day, reminded_hour)
            VALUES(?,?,?,?,?,?,?,?, 'active', ?, 0, 0)
        """, (user_id, username, client_name, phone, service, ext_type, d, t, datetime.utcnow().isoformat()))
        await db.commit()
        return True


async def get_user_active_bookings(user_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, d, t, service, ext_type, client_name, phone
            FROM bookings
            WHERE user_id=? AND status='active'
            ORDER BY d, t
        """, (user_id,))
        rows = await cur.fetchall()

    return [
        {"id": r[0], "d": r[1], "t": r[2], "service": r[3], "ext_type": r[4], "client_name": r[5], "phone": r[6]}
        for r in rows
    ]


async def get_booking_by_id(booking_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, user_id, username, client_name, phone, service, ext_type, d, t, status
            FROM bookings
            WHERE id=?
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
            FROM bookings
            WHERE d=?
            ORDER BY t
        """, (d,))
        rows = await cur.fetchall()

    return [
        {
            "id": r[0], "user_id": r[1], "username": r[2], "client_name": r[3],
            "phone": r[4], "service": r[5], "ext_type": r[6], "t": r[7], "status": r[8]
        }
        for r in rows
    ]


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


async def move_booking(booking_id: int, new_d: str, new_t: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT d,t,status FROM bookings WHERE id=?", (booking_id,))
        row = await cur.fetchone()
        if not row:
            return False
        old_d, old_t, status = row
        if status != "active":
            return False

        cur2 = await db.execute("SELECT is_open FROM slots WHERE d=? AND t=?", (new_d, new_t))
        row2 = await cur2.fetchone()
        if not row2 or row2[0] != 1:
            return False

        await db.execute("UPDATE slots SET is_open=1 WHERE d=? AND t=?", (old_d, old_t))
        await db.execute("UPDATE slots SET is_open=0 WHERE d=? AND t=?", (new_d, new_t))
        await db.execute("""
            UPDATE bookings
            SET d=?, t=?, reminded_day=0, reminded_hour=0
            WHERE id=?
        """, (new_d, new_t, booking_id))
        await db.commit()
        return True


async def delete_slots_all():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM slots")
        await db.commit()


async def delete_bookings_all():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM bookings")
        await db.commit()


async def delete_everything():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM bookings")
        await db.execute("DELETE FROM slots")
        await db.commit()


# ================== UI (Inline) ==================
def kb_calendar(month: str, prefix: str) -> InlineKeyboardMarkup:
    y, m = parse_month_key(month)
    cal = calendar.monthcalendar(y, m)

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
                d_str = f"{y:04d}-{m:02d}-{day:02d}"
                row_btns.append(InlineKeyboardButton(text=str(day), callback_data=f"{prefix}:day:{d_str}"))
        b.row(*row_btns)

    prev_y, prev_m = shift_month(y, m, -1)
    next_y, next_m = shift_month(y, m, +1)
    b.row(
        InlineKeyboardButton(text="⬅️", callback_data=f"{prefix}:month:{month_key(prev_y, prev_m)}"),
        InlineKeyboardButton(text="➡️", callback_data=f"{prefix}:month:{month_key(next_y, next_m)}"),
    )
    return b.as_markup()


def kb_times(d: str, times: list[str], prefix: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for t in times:
        b.row(InlineKeyboardButton(text=t, callback_data=f"{prefix}:time:{d}:{t}"))
    b.row(InlineKeyboardButton(text="⬅️ Назад до календаря", callback_data=f"{prefix}:backcal"))
    return b.as_markup()


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


def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📅 Додати графік пачкою ({DEFAULT_WEEKS} тижні)", callback_data="a:bulk")],
        [InlineKeyboardButton(text="➕ Додати слот вручну", callback_data="a:addslot")],
        [InlineKeyboardButton(text="📆 Переглянути записи по дню", callback_data="a:day")],
        [InlineKeyboardButton(text="🧹 Видалити слоти ВСІ", callback_data="a:del_slots_all")],
        [InlineKeyboardButton(text="🧹 Видалити записи ВСІ", callback_data="a:del_bookings_all")],
        [InlineKeyboardButton(text="🧨 Видалити ВСЕ", callback_data="a:del_all")],
    ])


def kb_confirm(prefix: str = "u:confirm") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Підтвердити", callback_data=f"{prefix}:yes"),
            InlineKeyboardButton(text="❌ Скасувати", callback_data=f"{prefix}:no"),
        ]
    ])


def kb_user_cancel_confirm(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Так, скасувати", callback_data=f"u:cancel_yes:{booking_id}"),
            InlineKeyboardButton(text="↩️ Ні", callback_data="u:my"),
        ]
    ])


def kb_user_move_confirm(booking_id: int, new_d: str, new_t: str) -> InlineKeyboardMarkup:
    hh, mm = split_time(new_t)
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Так, перенести", callback_data=f"u:move_yes:{booking_id}:{new_d}:{hh}:{mm}"),
            InlineKeyboardButton(text="↩️ Ні", callback_data="u:my"),
        ]
    ])


def kb_user_bookings_list(items: list[dict]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if not items:
        b.row(InlineKeyboardButton(text="(Нема активних записів)", callback_data="noop"))
    else:
        for it in items:
            extra = f" ({it['ext_type']})" if it.get("ext_type") else ""
            b.row(InlineKeyboardButton(
                text=f"❌ Скасувати • {fmt_dt(it['d'], it['t'])} • {it['service']}{extra}",
                callback_data=f"u:cancel_ask:{it['id']}"
            ))
            b.row(InlineKeyboardButton(
                text=f"🔁 Перенести • {fmt_dt(it['d'], it['t'])} • {it['service']}{extra}",
                callback_data=f"u:move_start:{it['id']}"
            ))
    b.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="u:back:start"))
    return b.as_markup()


def kb_admin_day_actions(d: str, bookings: list[dict]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if bookings:
        for bk in bookings:
            extra = f" ({bk['ext_type']})" if bk.get("ext_type") else ""
            st = "✅" if bk["status"] == "active" else "🚫"
            label = f"{st} {bk['t']} — {bk['client_name']} — {bk['service']}{extra} (id:{bk['id']})"
            if bk["status"] == "active":
                b.row(InlineKeyboardButton(text=f"❌ Скасувати #{bk['id']}", callback_data=f"a:cancel:{bk['id']}"))
                b.row(InlineKeyboardButton(text=f"🔁 Перенести #{bk['id']}", callback_data=f"a:move:{bk['id']}"))
            else:
                b.row(InlineKeyboardButton(text=label, callback_data="noop"))
    else:
        b.row(InlineKeyboardButton(text="(Нема записів)", callback_data="noop"))

    b.row(InlineKeyboardButton(text="⬅️ Назад до календаря", callback_data="a:day"))
    b.row(InlineKeyboardButton(text="🏠 Адмін-меню", callback_data="a:menu"))
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


bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ================== NOTIFY ADMINS ==================
async def notify_admins(text: str):
    for chat_id in admin_chat_targets():
        try:
            await bot.send_message(chat_id, text)
        except Exception:
            pass


async def notify_client(user_id: int | None, text: str):
    if not user_id:
        return
    try:
        await bot.send_message(user_id, text)
    except Exception:
        pass


# ================== REMINDERS ==================
async def _set_reminded_flag(booking_id: int, field: str):
    if field not in {"reminded_day", "reminded_hour"}:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE bookings SET {field}=1 WHERE id=?", (booking_id,))
        await db.commit()


async def reminder_worker():
    while True:
        try:
            now = datetime.now(TZ)
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("""
                    SELECT id, user_id, service, ext_type, d, t, reminded_day, reminded_hour
                    FROM bookings
                    WHERE status='active'
                """)
                rows = await cur.fetchall()

            for r in rows:
                bid, user_id, service, ext_type, d, t, reminded_day, reminded_hour = r
                if not user_id:
                    continue

                appt = appt_dt_local(d, t)
                if not appt:
                    continue

                delta_min = (appt - now).total_seconds() / 60.0
                if delta_min < -5:
                    continue

                if reminded_day == 0 and (24 * 60 - DAY_WINDOW_MIN) <= delta_min <= (24 * 60 + DAY_WINDOW_MIN):
                    msg = (
                        "🔔 Нагадування про запис на завтра\n\n"
                        f"📅 Дата: {fmt_date_iso_to_ua(d)}\n"
                        f"🕒 Час: {t}\n"
                        f"💅 Послуга: {service}{f' ({ext_type})' if ext_type else ''}\n\n"
                        "Якщо потрібно змінити/скасувати — напишіть, будь ласка, адміну."
                    )
                    try:
                        await bot.send_message(user_id, msg)
                        await _set_reminded_flag(bid, "reminded_day")
                    except Exception:
                        pass

                if reminded_hour == 0 and (60 - HOUR_WINDOW_MIN) <= delta_min <= (60 + HOUR_WINDOW_MIN):
                    msg = (
                        "🔔 Нагадування про запис через 1 годину\n\n"
                        f"📅 Дата: {fmt_date_iso_to_ua(d)}\n"
                        f"🕒 Час: {t}\n"
                        f"💅 Послуга: {service}{f' ({ext_type})' if ext_type else ''}\n\n"
                        "Чекаємо вас 💛"
                    )
                    try:
                        await bot.send_message(user_id, msg)
                        await _set_reminded_flag(bid, "reminded_hour")
                    except Exception:
                        pass

        except Exception as e:
            print(f"[REMINDER_WORKER_ERROR] {e}", flush=True)

        await asyncio.sleep(REMINDER_INTERVAL_SEC)


# ================== NAV BUTTONS (Reply keyboard) ==================
@dp.message(F.text == "🏠 Меню")
async def nav_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Меню 👇", reply_markup=kb_start())


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
        await message.answer("У вас немає активних записів.", reply_markup=kb_user_bookings_list(items))
    else:
        lines = ["📋 Ваші активні записи:"]
        for it in items:
            extra = f" ({it['ext_type']})" if it.get("ext_type") else ""
            lines.append(f"• {fmt_dt(it['d'], it['t'])} — {it['service']}{extra}")
        await message.answer("\n".join(lines), reply_markup=kb_user_bookings_list(items))


@dp.message(F.text == "🛠 Адмін")
async def nav_admin(message: Message, state: FSMContext):
    await state.clear()
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    await message.answer("🛠 Адмін-панель:", reply_markup=kb_admin())


# ================== START ==================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Вітаю! 👋\n"
        "• «Записатись» — зробити запис\n"
        "• «Мої записи» — переглянути/скасувати/перенести\n",
        reply_markup=rk_main(is_admin_username(message))
    )
    await message.answer("Меню 👇", reply_markup=kb_start())


@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Ваш chat_id: {message.chat.id}")


@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    await state.clear()
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    await message.answer("🛠 Адмін-панель:", reply_markup=kb_admin())


# ================== USER: My bookings ==================
@dp.callback_query(F.data == "u:my")
async def u_my(call: CallbackQuery, state: FSMContext):
    await state.clear()
    items = await get_user_active_bookings(call.from_user.id)
    if not items:
        await call.message.answer("У вас немає активних записів.", reply_markup=kb_user_bookings_list(items))
    else:
        lines = ["📋 Ваші активні записи:"]
        for it in items:
            extra = f" ({it['ext_type']})" if it.get("ext_type") else ""
            lines.append(f"• {fmt_dt(it['d'], it['t'])} — {it['service']}{extra}")
        await call.message.answer("\n".join(lines), reply_markup=kb_user_bookings_list(items))
    await call.answer()


@dp.callback_query(F.data.startswith("u:cancel_ask:"))
async def u_cancel_ask(call: CallbackQuery):
    booking_id = int(call.data.split(":")[-1])
    bk = await get_booking_by_id(booking_id)
    if not bk or bk["status"] != "active":
        await call.answer("Запис не знайдено або вже скасовано.", show_alert=True)
        return
    if bk["user_id"] != call.from_user.id:
        await call.answer("Це не ваш запис.", show_alert=True)
        return

    extra = f" ({bk['ext_type']})" if bk.get("ext_type") else ""
    text = (
        "❗ Ви точно хочете скасувати запис?\n\n"
        f"📅 {fmt_date_iso_to_ua(bk['d'])}\n"
        f"🕒 {bk['t']}\n"
        f"💅 {bk['service']}{extra}\n"
        f"👤 {bk['client_name']}\n"
    )
    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Так, скасувати", callback_data=f"u:cancel_yes:{booking_id}")],
        [InlineKeyboardButton(text="↩️ Ні", callback_data="u:my")],
    ]))
    await call.answer()


@dp.callback_query(F.data.startswith("u:cancel_yes:"))
async def u_cancel_yes(call: CallbackQuery):
    booking_id = int(call.data.split(":")[-1])
    bk = await get_booking_by_id(booking_id)
    if not bk or bk["status"] != "active":
        await call.answer("Запис не знайдено або вже скасовано.", show_alert=True)
        return
    if bk["user_id"] != call.from_user.id:
        await call.answer("Це не ваш запис.", show_alert=True)
        return

    ok, d, t = await cancel_booking(booking_id)
    if not ok:
        await call.message.answer("❌ Не вдалося скасувати запис.")
        await call.answer()
        return

    await call.message.answer(f"✅ Запис скасовано.\n📅 {fmt_date_iso_to_ua(d)}\n🕒 {t}")

    extra = f" ({bk['ext_type']})" if bk.get("ext_type") else ""
    admin_text = (
        "🚫 СКАСУВАННЯ ЗАПИСУ (клієнтом)\n\n"
        f"📅 {fmt_date_iso_to_ua(bk['d'])}\n"
        f"🕒 {bk['t']}\n"
        f"💅 {bk['service']}{extra}\n"
        f"👤 {bk['client_name']}\n"
        f"📞 {bk['phone']}\n"
        f"🔗 {tg_user_label(call.from_user.id, call.from_user.username)}\n"
        f"🆔 booking_id: {booking_id}"
    )
    await notify_admins(admin_text)
    await call.answer()


@dp.callback_query(F.data.startswith("u:move_start:"))
async def u_move_start(call: CallbackQuery):
    booking_id = int(call.data.split(":")[-1])
    bk = await get_booking_by_id(booking_id)
    if not bk or bk["status"] != "active":
        await call.answer("Запис не знайдено або вже неактивний.", show_alert=True)
        return
    if bk["user_id"] != call.from_user.id:
        await call.answer("Це не ваш запис.", show_alert=True)
        return

    today = date.today()
    mk = month_key(today.year, today.month)
    await call.message.answer(
        f"🔁 Перенесення запису #{booking_id}\nОберіть НОВУ дату:",
        reply_markup=kb_calendar(mk, f"u_move:{booking_id}")
    )
    await call.answer()


@dp.callback_query(F.data.startswith("u_move:") & F.data.contains(":month:"))
async def u_move_month(call: CallbackQuery):
    parts = call.data.split(":")
    booking_id = parts[1]
    mk = parts[-1]
    await call.message.edit_reply_markup(reply_markup=kb_calendar(mk, f"u_move:{booking_id}"))
    await call.answer()


@dp.callback_query(F.data.startswith("u_move:") & F.data.contains(":day:"))
async def u_move_day(call: CallbackQuery):
    parts = call.data.split(":")
    booking_id = int(parts[1])
    d = parts[-1]

    bk = await get_booking_by_id(booking_id)
    if not bk or bk["status"] != "active" or bk["user_id"] != call.from_user.id:
        await call.answer("Запис недоступний.", show_alert=True)
        return

    times = await get_open_times(d)
    if not times:
        await call.message.answer("На цю дату немає вільних слотів. Оберіть іншу дату.")
        await call.answer()
        return

    await call.message.answer(
        f"Оберіть НОВИЙ час для запису #{booking_id} на {fmt_date_iso_to_ua(d)}:",
        reply_markup=kb_times(d, times, f"u_move:{booking_id}")
    )
    await call.answer()


@dp.callback_query(F.data.startswith("u_move:") & F.data.contains(":time:"))
async def u_move_time(call: CallbackQuery):
    parts = call.data.split(":")
    booking_id = int(parts[1])

    bk = await get_booking_by_id(booking_id)
    if not bk or bk["status"] != "active" or bk["user_id"] != call.from_user.id:
        await call.answer("Запис недоступний.", show_alert=True)
        return

    new_d, new_t = parse_dt_from_callback(call.data)
    if not new_d or not new_t:
        await call.answer("Помилка дати/часу. Спробуйте ще раз.", show_alert=True)
        return

    extra = f" ({bk['ext_type']})" if bk.get("ext_type") else ""
    text = (
        "Підтвердіть перенесення 👇\n\n"
        f"Було: 📅 {fmt_date_iso_to_ua(bk['d'])} 🕒 {bk['t']}\n"
        f"Стало: 📅 {fmt_date_iso_to_ua(new_d)} 🕒 {new_t}\n\n"
        f"💅 {bk['service']}{extra}\n"
        f"👤 {bk['client_name']}"
    )
    await call.message.answer(text, reply_markup=kb_user_move_confirm(booking_id, new_d, new_t))
    await call.answer()


def kb_user_move_confirm(booking_id: int, new_d: str, new_t: str) -> InlineKeyboardMarkup:
    hh, mm = split_time(new_t)
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Так, перенести", callback_data=f"u:move_yes:{booking_id}:{new_d}:{hh}:{mm}"),
            InlineKeyboardButton(text="↩️ Ні", callback_data="u:my"),
        ]
    ])


@dp.callback_query(F.data.startswith("u:move_yes:"))
async def u_move_yes(call: CallbackQuery):
    parts = call.data.split(":")
    if len(parts) != 6:
        await call.answer("Помилка даних.", show_alert=True)
        return

    booking_id = int(parts[2])
    new_d = parts[3]
    hh = parts[4]
    mm = parts[5]
    new_t = f"{hh}:{mm}"

    bk = await get_booking_by_id(booking_id)
    if not bk or bk["status"] != "active":
        await call.answer("Запис не знайдено або вже неактивний.", show_alert=True)
        return
    if bk["user_id"] != call.from_user.id:
        await call.answer("Це не ваш запис.", show_alert=True)
        return
    if not norm_date(new_d) or not norm_time(new_t):
        await call.answer("Невірна дата/час.", show_alert=True)
        return

    old_d, old_t = bk["d"], bk["t"]

    ok = await move_booking(booking_id, new_d, new_t)
    if not ok:
        await call.message.answer("❌ Не вдалося перенести (слот зайнятий або недоступний).")
        await call.answer()
        return

    await call.message.answer(
        "✅ Запис перенесено!\n"
        f"Було: {fmt_date_iso_to_ua(old_d)} {old_t}\n"
        f"Стало: {fmt_date_iso_to_ua(new_d)} {new_t}"
    )

    extra = f" ({bk['ext_type']})" if bk.get("ext_type") else ""
    admin_text = (
        "🔁 ПЕРЕНЕСЕННЯ ЗАПИСУ (клієнтом)\n\n"
        f"Було: 📅 {fmt_date_iso_to_ua(old_d)} 🕒 {old_t}\n"
        f"Стало: 📅 {fmt_date_iso_to_ua(new_d)} 🕒 {new_t}\n\n"
        f"💅 {bk['service']}{extra}\n"
        f"👤 {bk['client_name']}\n"
        f"📞 {bk['phone']}\n"
        f"🔗 {tg_user_label(call.from_user.id, call.from_user.username)}\n"
        f"🆔 booking_id: {booking_id}"
    )
    await notify_admins(admin_text)
    await call.answer()


# ================== USER: booking flow ==================
@dp.callback_query(F.data == "u:start")
async def u_start(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(UserBooking.service)
    await call.message.answer("Оберіть послугу:", reply_markup=kb_services())
    await call.answer()


@dp.callback_query(F.data.startswith("u:back:"))
async def u_back(call: CallbackQuery, state: FSMContext):
    where = call.data.split(":")[-1]
    if where == "start":
        await state.clear()
        await call.message.answer("Меню 👇", reply_markup=kb_start())
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
        today = date.today()
        await call.message.answer("Оберіть дату (календар):", reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
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
    today = date.today()
    await call.message.answer("Оберіть дату (календар):", reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
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
        await call.message.answer("На цю дату немає вільних віконець. Оберіть іншу дату.")
        today = date.today()
        await call.message.answer("Календар:", reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
        await call.answer()
        return

    await state.set_state(UserBooking.time)
    await call.message.answer(f"Вільний час на {fmt_date_iso_to_ua(d)}:", reply_markup=kb_times(d, times, "u"))
    await call.answer()


@dp.callback_query(F.data == "u:backcal")
async def u_backcal(call: CallbackQuery):
    today = date.today()
    await call.message.answer("Оберіть дату:", reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
    await call.answer()


@dp.callback_query(F.data.startswith("u:time:"))
async def u_time(call: CallbackQuery, state: FSMContext):
    d, t = parse_dt_from_callback(call.data)
    if not d or not t:
        await call.answer("Помилка часу/дати. Оберіть знову.", show_alert=True)
        return

    await state.update_data(day=d, time=t)
    await state.set_state(UserBooking.fullname)
    await call.message.answer("Вкажіть Прізвище та Ім’я (наприклад: Іваненко Марія):")
    await call.answer()


@dp.message(UserBooking.fullname)
async def u_fullname(message: Message, state: FSMContext):
    fullname = (message.text or "").strip()
    if len(fullname.split()) < 2 or len(fullname) < 5:
        return await message.answer("Напишіть, будь ласка, Прізвище та Ім’я (2 слова).")

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
    service = data.get("service")
    d = data.get("day")
    t = data.get("time")
    client_name = data.get("client_name")
    ext_type = data.get("ext_type")

    if not service or not d or not t or not client_name:
        await state.clear()
        await message.answer("⚠️ Дані запису загубились. Натисніть «Записатись» і зробіть запис знову 👇", reply_markup=kb_start())
        return

    text = (
        "Перевірте запис 👇\n\n"
        f"📅 Дата: {fmt_date_iso_to_ua(d)}\n"
        f"🕒 Час: {t}\n"
        f"💅 Послуга: {service}{f' ({ext_type})' if ext_type else ''}\n"
        f"👤 Клієнт: {client_name}\n"
        f"📞 Телефон: {phone}\n\n"
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

    service = data.get("service")
    d = data.get("day")
    t = data.get("time")
    fullname = data.get("client_name")
    phone = data.get("phone")
    ext_type = data.get("ext_type")

    if not service or not d or not t or not fullname or not phone:
        await state.clear()
        await call.message.answer("⚠️ Дані запису загубились. Почніть запис заново.", reply_markup=kb_start())
        await call.answer()
        return

    ok = await book_slot(
        user_id=call.from_user.id,
        username=(call.from_user.username or ""),
        client_name=fullname,
        phone=phone,
        service=service,
        ext_type=ext_type,
        d=d,
        t=t
    )

    if not ok:
        await call.message.answer("❌ На жаль цей час вже зайняли. Оберіть інший.")
        await state.set_state(UserBooking.day)
        today = date.today()
        await call.message.answer("Календар:", reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
        await call.answer()
        return

    await call.message.answer(
        "✅ Запис підтверджено!\n"
        f"Дата: {fmt_date_iso_to_ua(d)}\n"
        f"Час: {t}\n"
        f"Послуга: {service}{f' ({ext_type})' if ext_type else ''}"
    )
    await state.clear()

    admin_text = (
        "📥 НОВИЙ ЗАПИС\n\n"
        f"📅 {fmt_date_iso_to_ua(d)}\n"
        f"🕒 {t}\n"
        f"💅 {service}{f' ({ext_type})' if ext_type else ''}\n"
        f"👤 {fullname}\n"
        f"📞 {phone}\n"
        f"🔗 {tg_user_label(call.from_user.id, call.from_user.username)}"
    )
    await notify_admins(admin_text)
    await call.answer()


# ================== ADMIN ==================
class AdminAddSlot(StatesGroup):
    d = State()
    t = State()


@dp.callback_query(F.data == "a:menu")
async def a_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    if not is_admin_username(call):
        await call.answer("Нема доступу", show_alert=True)
        return
    await call.message.answer("🛠 Адмін-панель:", reply_markup=kb_admin())
    await call.answer()


@dp.callback_query(F.data == "a:bulk")
async def a_bulk(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    added, skipped = await bulk_add_default_slots(DEFAULT_WEEKS)
    await call.message.answer(
        "✅ Готово!\n"
        f"Додано слотів: {added}\n"
        f"Вже існували (пропущено): {skipped}\n\n"
        f"Шаблон: Вт–Сб / {', '.join(DEFAULT_TIMES)}"
    )
    await call.answer()


@dp.callback_query(F.data == "a:addslot")
async def a_addslot(call: CallbackQuery, state: FSMContext):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    await state.clear()
    await state.set_state(AdminAddSlot.d)
    await call.message.answer("Введіть дату слоту (ДД.ММ.РРРР), наприклад 03.02.2026:")
    await call.answer()


@dp.message(AdminAddSlot.d)
async def a_addslot_d(message: Message, state: FSMContext):
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    d = norm_date_admin(message.text)
    if not d:
        return await message.answer("❌ Невірний формат. Введіть ДД.ММ.РРРР (наприклад 03.02.2026).")
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


# ---- Admin: view bookings by day ----
@dp.callback_query(F.data == "a:day")
async def a_day(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    today = date.today()
    await call.message.answer("Оберіть дату (календар):", reply_markup=kb_calendar(month_key(today.year, today.month), "a_day"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_day:month:"))
async def a_day_month(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    mk = call.data.split(":")[-1]
    await call.message.edit_reply_markup(reply_markup=kb_calendar(mk, "a_day"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_day:day:"))
async def a_day_show(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    d = call.data.split(":")[-1]
    bookings = await get_day_bookings(d)

    lines = [f"📌 Записи на {fmt_date_iso_to_ua(d)}:"]
    if not bookings:
        lines.append("— немає")
    else:
        for bk in bookings:
            st = "✅" if bk["status"] == "active" else "🚫"
            extra = f" ({bk['ext_type']})" if bk["ext_type"] else ""
            lines.append(f"{st} {bk['t']} — {bk['client_name']} {bk['phone']} — {bk['service']}{extra} (id:{bk['id']})")

    await call.message.answer("\n".join(lines), reply_markup=kb_admin_day_actions(d, bookings))
    await call.answer()


# ---- Admin: cancel booking button ----
@dp.callback_query(F.data.startswith("a:cancel:"))
async def a_cancel(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)

    booking_id = int(call.data.split(":")[-1])
    bk = await get_booking_by_id(booking_id)
    if not bk:
        await call.message.answer("❌ Не знайшов запис.")
        await call.answer()
        return

    ok, d, t = await cancel_booking(booking_id)
    if not ok:
        await call.message.answer("❌ Не вдалося скасувати.")
        await call.answer()
        return

    # notify client
    extra = f" ({bk['ext_type']})" if bk.get("ext_type") else ""
    await notify_client(
        bk["user_id"],
        "🚫 Ваш запис скасовано адміністратором.\n\n"
        f"📅 {fmt_date_iso_to_ua(d)}\n"
        f"🕒 {t}\n"
        f"💅 {bk['service']}{extra}\n\n"
        "Якщо треба — запишіться знову через бота."
    )

    # notify admins (who did it)
    actor = call.from_user.username or call.from_user.full_name
    admin_text = (
        "🚫 СКАСУВАНО (адмін)\n\n"
        f"Хто: @{actor}\n"
        f"Запис: 📅 {fmt_date_iso_to_ua(d)} 🕒 {t}\n"
        f"Клієнт: {bk['client_name']} • {bk['phone']}\n"
        f"Послуга: {bk['service']}{extra}\n"
        f"🆔 booking_id: {booking_id}"
    )
    await notify_admins(admin_text)

    await call.message.answer(f"✅ Запис #{booking_id} скасовано. Слот {fmt_date_iso_to_ua(d)} {t} відкрито.")
    await call.answer()


# ---- Admin: move booking start ----
@dp.callback_query(F.data.startswith("a:move:"))
async def a_move_start(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)

    booking_id = int(call.data.split(":")[-1])
    bk = await get_booking_by_id(booking_id)
    if not bk or bk["status"] != "active":
        await call.message.answer("❌ Запис не знайдено або неактивний.")
        await call.answer()
        return

    today = date.today()
    mk = month_key(today.year, today.month)
    await call.message.answer(
        f"🔁 Перенесення запису #{booking_id}\nОберіть НОВУ дату:",
        reply_markup=kb_calendar(mk, f"a_move:{booking_id}")
    )
    await call.answer()


@dp.callback_query(F.data.startswith("a_move:") & F.data.contains(":month:"))
async def a_move_month(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    parts = call.data.split(":")
    booking_id = parts[1]
    mk = parts[-1]
    await call.message.edit_reply_markup(reply_markup=kb_calendar(mk, f"a_move:{booking_id}"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_move:") & F.data.contains(":day:"))
async def a_move_day(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    parts = call.data.split(":")
    booking_id = int(parts[1])
    d = parts[-1]

    bk = await get_booking_by_id(booking_id)
    if not bk or bk["status"] != "active":
        await call.message.answer("❌ Запис не знайдено або неактивний.")
        await call.answer()
        return

    times = await get_open_times(d)
    if not times:
        await call.message.answer("На цю дату немає вільних слотів. Оберіть іншу дату.")
        await call.answer()
        return

    await call.message.answer(
        f"Оберіть НОВИЙ час для запису #{booking_id} на {fmt_date_iso_to_ua(d)}:",
        reply_markup=kb_times(d, times, f"a_move:{booking_id}")
    )
    await call.answer()


@dp.callback_query(F.data.startswith("a_move:") & F.data.contains(":time:"))
async def a_move_time(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)

    parts = call.data.split(":")
    booking_id = int(parts[1])

    bk = await get_booking_by_id(booking_id)
    if not bk or bk["status"] != "active":
        await call.message.answer("❌ Запис не знайдено або неактивний.")
        await call.answer()
        return

    new_d, new_t = parse_dt_from_callback(call.data)
    if not new_d or not new_t:
        await call.message.answer("❌ Помилка дати/часу.")
        await call.answer()
        return

    old_d, old_t = bk["d"], bk["t"]

    ok = await move_booking(booking_id, new_d, new_t)
    if not ok:
        await call.message.answer("❌ Не вдалося перенести (слот зайнятий/недоступний).")
        await call.answer()
        return

    # notify client
    extra = f" ({bk['ext_type']})" if bk.get("ext_type") else ""
    await notify_client(
        bk["user_id"],
        "🔁 Ваш запис перенесено адміністратором.\n\n"
        f"Було: 📅 {fmt_date_iso_to_ua(old_d)} 🕒 {old_t}\n"
        f"Стало: 📅 {fmt_date_iso_to_ua(new_d)} 🕒 {new_t}\n\n"
        f"💅 {bk['service']}{extra}"
    )

    # notify admins
    actor = call.from_user.username or call.from_user.full_name
    admin_text = (
        "🔁 ПЕРЕНЕСЕНО (адмін)\n\n"
        f"Хто: @{actor}\n"
        f"Було: 📅 {fmt_date_iso_to_ua(old_d)} 🕒 {old_t}\n"
        f"Стало: 📅 {fmt_date_iso_to_ua(new_d)} 🕒 {new_t}\n"
        f"Клієнт: {bk['client_name']} • {bk['phone']}\n"
        f"Послуга: {bk['service']}{extra}\n"
        f"🆔 booking_id: {booking_id}"
    )
    await notify_admins(admin_text)

    await call.message.answer(f"✅ Перенесено запис #{booking_id} на {fmt_date_iso_to_ua(new_d)} {new_t}.")
    await call.answer()


@dp.callback_query(F.data == "a:del_slots_all")
async def a_del_slots_all(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    await delete_slots_all()
    await call.message.answer("✅ Всі слоти (віконця) видалено.")
    await call.answer()


@dp.callback_query(F.data == "a:del_bookings_all")
async def a_del_bookings_all(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    await delete_bookings_all()
    await call.message.answer("✅ Всі записи видалено.")
    await call.answer()


@dp.callback_query(F.data == "a:del_all")
async def a_del_all(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    await delete_everything()
    await call.message.answer("✅ Видалено ВСЕ: слоти + записи.")
    await call.answer()


@dp.callback_query(F.data == "noop")
async def noop(call: CallbackQuery):
    await call.answer()


# ================== MAIN ==================
async def main():
    await ensure_schema()
    asyncio.create_task(reminder_worker())

    print("VERSION: FULL ALL-IN-ONE (admin day cancel/move + client notify)", flush=True)
    print("=== BOT STARTED (polling) ===", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
