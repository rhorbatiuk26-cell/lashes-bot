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
    InlineKeyboardMarkup, InlineKeyboardButton
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

# timezone for schedule/reminders
TZ = ZoneInfo("Europe/Kyiv")

# reminders loop interval (seconds)
REMINDER_INTERVAL_SEC = 60  # 1 minute

# windows: how close to exact time we allow sending
DAY_WINDOW_MIN = 10   # +/- 10 min around 24h
HOUR_WINDOW_MIN = 10  # +/- 10 min around 1h

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


def admin_chat_targets() -> list[int]:
    """
    Надійний спосіб для сповіщень:
    Railway Variables -> ADMIN_CHAT_IDS="123,456"
    """
    raw = (os.getenv("ADMIN_CHAT_IDS") or "").strip()
    if not raw:
        return []
    ids: list[int] = []
    for x in raw.split(","):
        x = x.strip()
        if x.isdigit():
            ids.append(int(x))
    return ids


def norm_date(s: str) -> str | None:
    s = (s or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
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


def digits_count(s: str) -> int:
    return len(re.sub(r"\D", "", s or ""))


def parse_dt_from_callback(call_data: str) -> tuple[str, str]:
    """
    callback_data:
      u:time:YYYY-MM-DD:HH:MM
      a_move:123:time:YYYY-MM-DD:HH:MM
    -> returns ("YYYY-MM-DD", "HH:MM")
    """
    parts = (call_data or "").split(":")
    if len(parts) < 3:
        return "", ""

    for i in range(len(parts) - 1, -1, -1):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[i]):
            if i + 2 < len(parts):
                hh = parts[i + 1]
                mm = parts[i + 2]
                if re.fullmatch(r"[0-2]\d", hh) and re.fullmatch(r"[0-5]\d", mm):
                    return parts[i], f"{hh}:{mm}"
            if i + 1 < len(parts) and re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", parts[i + 1]):
                return parts[i], parts[i + 1]
    return "", ""


def appt_dt_local(d: str, t: str) -> datetime | None:
    """
    Convert YYYY-MM-DD + HH:MM to timezone-aware datetime in Europe/Kyiv
    """
    try:
        dt_naive = datetime.strptime(f"{d} {t}", "%Y-%m-%d %H:%M")
        return dt_naive.replace(tzinfo=TZ)
    except Exception:
        return None


# ================== DB ==================
async def _column_exists(db: aiosqlite.Connection, table: str, col: str) -> bool:
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    cols = {r[1] for r in rows}  # r[1] = name
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

        # ✅ migrations for reminders (safe)
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
        cur = today
        while cur <= end:
            if cur.weekday() in WORKING_DAYS:
                d_str = cur.isoformat()
                for tm in DEFAULT_TIMES:
                    try:
                        await db.execute("INSERT INTO slots(d,t,is_open) VALUES(?,?,1)", (d_str, tm))
                        added += 1
                    except aiosqlite.IntegrityError:
                        skipped += 1
            cur += timedelta(days=1)
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


async def get_day_bookings(d: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, client_name, phone, service, ext_type, t, status
            FROM bookings
            WHERE d=?
            ORDER BY t
        """, (d,))
        rows = await cur.fetchall()

    return [
        {"id": r[0], "client_name": r[1], "phone": r[2], "service": r[3], "ext_type": r[4], "t": r[5], "status": r[6]}
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
        await db.execute("UPDATE bookings SET d=?, t=?, reminded_day=0, reminded_hour=0 WHERE id=?", (new_d, new_t, booking_id))
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


async def delete_slots_range(d_from: str, d_to: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM slots WHERE d>=? AND d<=?", (d_from, d_to))
        await db.commit()


async def delete_bookings_range(d_from: str, d_to: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM bookings WHERE d>=? AND d<=?", (d_from, d_to))
        await db.commit()


# ================== UI ==================
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
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Записатись", callback_data="u:start")],
        [InlineKeyboardButton(text="🛠 Адмін", callback_data="a:menu")],
    ])


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
        [InlineKeyboardButton(text="📆 Переглянути день (записи)", callback_data="a:day")],
        [InlineKeyboardButton(text="🧹 Видалити slоти ВСІ", callback_data="a:del_slots_all")],
        [InlineKeyboardButton(text="🧹 Видалити записи ВСІ", callback_data="a:del_bookings_all")],
        [InlineKeyboardButton(text="🧨 Видалити ВСЕ", callback_data="a:del_all")],
        [InlineKeyboardButton(text="ℹ️ Команди діапазону", callback_data="a:help_range")]
    ])


def kb_confirm(prefix: str = "u:confirm") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Підтвердити", callback_data=f"{prefix}:yes"),
            InlineKeyboardButton(text="❌ Скасувати", callback_data=f"{prefix}:no"),
        ]
    ])


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


# ================== REMINDERS ==================
async def _set_reminded_flag(booking_id: int, field: str):
    if field not in {"reminded_day", "reminded_hour"}:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE bookings SET {field}=1 WHERE id=?", (booking_id,))
        await db.commit()


async def reminder_worker():
    """
    Runs forever. Every minute checks upcoming appointments and sends:
    - 24h reminder (once)
    - 1h reminder (once)
    """
    while True:
        try:
            now = datetime.now(TZ)

            # fetch active bookings with reminders not fully sent
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("""
                    SELECT id, user_id, client_name, phone, service, ext_type, d, t, reminded_day, reminded_hour
                    FROM bookings
                    WHERE status='active'
                """)
                rows = await cur.fetchall()

            for r in rows:
                bid, user_id, client_name, phone, service, ext_type, d, t, reminded_day, reminded_hour = r
                if not user_id:
                    continue

                appt = appt_dt_local(d, t)
                if not appt:
                    continue

                delta_min = (appt - now).total_seconds() / 60.0

                # ignore past appointments
                if delta_min < -5:
                    continue

                # 24h reminder window
                if reminded_day == 0:
                    if (24 * 60 - DAY_WINDOW_MIN) <= delta_min <= (24 * 60 + DAY_WINDOW_MIN):
                        msg = (
                            "🔔 Нагадування про запис на завтра\n\n"
                            f"📅 Дата: {d}\n"
                            f"🕒 Час: {t}\n"
                            f"💅 Послуга: {service}{f' ({ext_type})' if ext_type else ''}\n\n"
                            "Якщо потрібно змінити/скасувати — напишіть, будь ласка, адміну."
                        )
                        try:
                            await bot.send_message(user_id, msg)
                            await _set_reminded_flag(bid, "reminded_day")
                        except Exception:
                            pass

                # 1h reminder window
                if reminded_hour == 0:
                    if (60 - HOUR_WINDOW_MIN) <= delta_min <= (60 + HOUR_WINDOW_MIN):
                        msg = (
                            "🔔 Нагадування про запис через 1 годину\n\n"
                            f"📅 Дата: {d}\n"
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


# ================== START ==================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Вітаю! 👋\nНатисніть «Записатись», щоб обрати послугу, дату і час.",
        reply_markup=kb_start()
    )


@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    await state.clear()
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    await message.answer("🛠 Адмін-панель:", reply_markup=kb_admin())


@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Ваш chat_id: {message.chat.id}")


# ================== USER ==================
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
        await call.message.answer("Натисніть «Записатись» 👇", reply_markup=kb_start())
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
        await call.message.answer(
            "Оберіть дату (календар):",
            reply_markup=kb_calendar(month_key(today.year, today.month), "u")
        )
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
    await call.message.answer(
        "Оберіть дату (календар):",
        reply_markup=kb_calendar(month_key(today.year, today.month), "u")
    )
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
    await call.message.answer(f"Вільний час на {d}:", reply_markup=kb_times(d, times, "u"))
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
        return await message.answer("Напишіть, будь ласка, *Прізвище та Ім’я* (2 слова).")

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

    if not service or not d or not t or not client_name:
        await state.clear()
        await message.answer(
            "⚠️ Дані запису загубились. Натисніть «Записатись» і зробіть запис знову 👇",
            reply_markup=kb_start()
        )
        return

    ext_type = data.get("ext_type")

    text = (
        "Перевірте запис 👇\n\n"
        f"📅 Дата: {d}\n"
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
        await call.message.answer("❌ Скасовано. Натисніть «Записатись», щоб почати знову.", reply_markup=kb_start())
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

    user_text = f"✅ Запис підтверджено!\nДата: {d}\nЧас: {t}\nПослуга: {service}"
    if ext_type:
        user_text += f" ({ext_type})"
    await call.message.answer(user_text)
    await state.clear()

    uname = (call.from_user.username or "").strip()
    uname_part = f"@{uname}" if uname else "(без username)"
    admin_text = (
        "📥 НОВИЙ ЗАПИС\n\n"
        f"📅 {d}\n"
        f"🕒 {t}\n"
        f"💅 {service}{f' ({ext_type})' if ext_type else ''}\n"
        f"👤 {fullname}\n"
        f"📞 {phone}\n"
        f"🔗 Telegram: {uname_part} | id: {call.from_user.id}"
    )
    await notify_admins(admin_text)

    await call.answer()


# ================== ADMIN ==================
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
        f"✅ Готово!\n"
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
    await call.message.answer("Введіть дату слоту YYYY-MM-DD (наприклад 2026-02-03):")
    await call.answer()


@dp.message(AdminAddSlot.d)
async def a_addslot_d(message: Message, state: FSMContext):
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    d = norm_date(message.text)
    if not d:
        return await message.answer("❌ Невірний формат. Введіть YYYY-MM-DD.")
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

    # start reminder loop
    asyncio.create_task(reminder_worker())

    print("VERSION: 2026-01-31 + reminders 24h/1h (Europe/Kyiv)", flush=True)
    print("=== BOT STARTED (polling) ===", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
