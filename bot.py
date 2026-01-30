import asyncio
import os
import re
import calendar
from datetime import datetime, date, timedelta

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

# admins by username (without @)
ADMIN_USERNAMES = {"roman2696", "Ekaterinahorbatiuk"}

# bulk schedule: Tue-Sat
DEFAULT_TIMES = ["09:00", "11:30", "13:30"]
WORKING_DAYS = {1, 2, 3, 4, 5}  # Tue-Sat (Mon=0..Sun=6)
DEFAULT_WEEKS = 4

SERV_LAMI = "Ламінування"
SERV_EXT = "Нарощування"
EXT_TYPES = ["Класика", "2D", "3D"]


# ================== HELPERS ==================
def is_admin_username(msg_or_cq) -> bool:
    u = msg_or_cq.from_user
    username = (u.username or "").lstrip("@")
    return username in ADMIN_USERNAMES


def norm_date(s: str) -> str | None:
    # YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s.strip()):
        return s.strip()
    return None


def norm_time(s: str) -> str | None:
    # HH:MM
    if re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", s.strip()):
        return s.strip()
    return None


def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"


def parse_month_key(key: str) -> tuple[int, int]:
    y, m = key.split("-")
    return int(y), int(m)


def shift_month(y: int, m: int, delta: int) -> tuple[int, int]:
    # delta = +/-1 etc
    mm = m + delta
    yy = y
    while mm > 12:
        yy += 1
        mm -= 12
    while mm < 1:
        yy -= 1
        mm += 12
    return yy, mm


# ================== DB ==================
async def ensure_schema():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            d TEXT NOT NULL,        -- YYYY-MM-DD
            t TEXT NOT NULL,        -- HH:MM
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
            status TEXT NOT NULL DEFAULT 'active',  -- active/canceled
            created_at TEXT
        )
        """)

        await db.commit()


async def bulk_add_default_slots(weeks: int = DEFAULT_WEEKS) -> tuple[int, int]:
    """Add Tue-Sat slots with DEFAULT_TIMES for N weeks. Return (added, skipped)."""
    today = date.today()
    end = today + timedelta(days=weeks * 7)

    added = 0
    skipped = 0
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
    """Return True if inserted, False if existed."""
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute("INSERT INTO slots(d,t,is_open) VALUES(?,?,1)", (d, t))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def set_slot_open(d: str, t: str, is_open: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE slots SET is_open=? WHERE d=? AND t=?", (is_open, d, t))
        await db.commit()


async def get_open_times(d: str) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT t FROM slots WHERE d=? AND is_open=1 ORDER BY t",
            (d,)
        )
        rows = await cur.fetchall()
        return [r[0] for r in rows]


async def book_slot(
    user_id: int,
    username: str,
    client_name: str,
    phone: str,
    service: str,
    ext_type: str | None,
    d: str,
    t: str
) -> bool:
    """Book if slot open. Close slot. Return success."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT is_open FROM slots WHERE d=? AND t=?",
            (d, t)
        )
        row = await cur.fetchone()
        if not row or row[0] != 1:
            return False

        await db.execute("UPDATE slots SET is_open=0 WHERE d=? AND t=?", (d, t))
        await db.execute("""
            INSERT INTO bookings(user_id, username, client_name, phone, service, ext_type, d, t, status, created_at)
            VALUES(?,?,?,?,?,?,?,?, 'active', ?)
        """, (
            user_id, username, client_name, phone, service, ext_type, d, t,
            datetime.utcnow().isoformat()
        ))
        await db.commit()
        return True


async def get_day_bookings(d: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, client_name, phone, service, ext_type, t, status, username
            FROM bookings
            WHERE d=?
            ORDER BY t
        """, (d,))
        rows = await cur.fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r[0],
                "client_name": r[1],
                "phone": r[2],
                "service": r[3],
                "ext_type": r[4],
                "t": r[5],
                "status": r[6],
                "username": r[7],
            })
        return out


async def cancel_booking(booking_id: int) -> tuple[bool, str, str]:
    """Cancel booking -> open slot back. Return (ok, d, t)."""
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
    """Move active booking to new open slot. Old slot opens, new closes."""
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
        await db.execute("UPDATE bookings SET d=?, t=? WHERE id=?", (new_d, new_t, booking_id))
        await db.commit()
        return True


# ================== CALENDAR UI ==================
def kb_calendar(month: str, prefix: str) -> InlineKeyboardMarkup:
    """
    prefix:
      "u" -> user select day
      "a_day" -> admin view day
      "a_move:<booking_id>" -> admin choose day for move
    """
    y, m = parse_month_key(month)
    cal = calendar.monthcalendar(y, m)

    b = InlineKeyboardBuilder()

    # header
    b.row(InlineKeyboardButton(text=f"📅 {calendar.month_name[m]} {y}", callback_data="noop"))

    # weekdays
    wd = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
    b.row(*[InlineKeyboardButton(text=x, callback_data="noop") for x in wd])

    for week in cal:
        row_btns = []
        for day in week:
            if day == 0:
                row_btns.append(InlineKeyboardButton(text=" ", callback_data="noop"))
            else:
                d_str = f"{y:04d}-{m:02d}-{day:02d}"
                row_btns.append(InlineKeyboardButton(
                    text=str(day),
                    callback_data=f"{prefix}:day:{d_str}"
                ))
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


# ================== FSM ==================
class UserBooking(StatesGroup):
    service = State()
    ext_type = State()
    day = State()
    time = State()
    name = State()
    phone = State()


class AdminAddSlot(StatesGroup):
    d = State()
    t = State()


class AdminMove(StatesGroup):
    booking_id = State()
    new_day = State()
    new_time = State()


# ================== BOT ==================
bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ================== MENUS ==================
def kb_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Записатись", callback_data="u:start")]
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
        [InlineKeyboardButton(text="📆 Переглянути день (записи/слоти)", callback_data="a:day")],
    ])


# ================== START ==================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Вітаю! 👋\nНатисніть «Записатись», щоб обрати дату і час.",
        reply_markup=kb_start()
    )


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin_username(message):
        return await message.answer("❌ Доступ лише для адмінів.")
    await message.answer("🛠 Адмін-панель:", reply_markup=kb_admin())


# ================== USER FLOW ==================
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
        mk = month_key(today.year, today.month)
        await call.message.answer("Оберіть дату (календар):", reply_markup=kb_calendar(mk, "u"))
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
    mk = month_key(today.year, today.month)
    await call.message.answer("Оберіть дату (календар):", reply_markup=kb_calendar(mk, "u"))
    await call.answer()


@dp.callback_query(F.data.startswith("u:month:"))
async def u_month(call: CallbackQuery, state: FSMContext):
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
async def u_backcal(call: CallbackQuery, state: FSMContext):
    today = date.today()
    await call.message.answer("Оберіть дату:", reply_markup=kb_calendar(month_key(today.year, today.month), "u"))
    await call.answer()


@dp.callback_query(F.data.startswith("u:time:"))
async def u_time(call: CallbackQuery, state: FSMContext):
    _, _, d, t = call.data.split(":")
    await state.update_data(day=d, time=t)
    await state.set_state(UserBooking.name)
    await call.message.answer("Вкажіть ваше імʼя:")
    await call.answer()


@dp.message(UserBooking.name)
async def u_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        return await message.answer("Імʼя закоротке. Спробуйте ще раз.")
    await state.update_data(client_name=name)
    await state.set_state(UserBooking.phone)
    await message.answer("Тепер номер телефону (наприклад +380XXXXXXXXX):")


@dp.message(UserBooking.phone)
async def u_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if len(re.sub(r"\D", "", phone)) < 9:
        return await message.answer("Номер виглядає некоректно. Введіть ще раз.")
    data = await state.get_data()
    d = data["day"]
    t = data["time"]
    service = data["service"]
    ext_type = data.get("ext_type")

    ok = await book_slot(
        user_id=message.from_user.id,
        username=(message.from_user.username or ""),
        client_name=data["client_name"],
        phone=phone,
        service=service,
        ext_type=ext_type,
        d=d,
        t=t
    )
    if not ok:
        await message.answer("❌ На жаль цей час уже зайняли. Оберіть інший.")
        await state.set_state(UserBooking.day)
        today = date.today()
        return await message.answer("Календар:", reply_markup=kb_calendar(month_key(today.year, today.month), "u"))

    text = f"✅ Запис підтверджено!\nДата: {d}\nЧас: {t}\nПослуга: {service}"
    if ext_type:
        text += f" ({ext_type})"
    await message.answer(text)

    # notify admins
    for a in ADMIN_USERNAMES:
        # we can't send by username reliably; admins should talk to bot once
        pass

    await state.clear()


# ================== ADMIN FLOW ==================
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
    await call.message.answer("Введіть дату слоту у форматі YYYY-MM-DD (наприклад 2026-02-03):")
    await call.answer()


@dp.message(AdminAddSlot.d)
async def a_addslot_d(message: Message, state: FSMContext):
    d = norm_date(message.text)
    if not d:
        return await message.answer("❌ Невірний формат. Введіть YYYY-MM-DD.")
    await state.update_data(d=d)
    await state.set_state(AdminAddSlot.t)
    await message.answer("Введіть час у форматі HH:MM (наприклад 15:30):")


@dp.message(AdminAddSlot.t)
async def a_addslot_t(message: Message, state: FSMContext):
    t = norm_time(message.text)
    if not t:
        return await message.answer("❌ Невірний формат часу. Введіть HH:MM.")
    data = await state.get_data()
    d = data["d"]
    inserted = await add_slot(d, t)
    await message.answer("✅ Слот додано." if inserted else "ℹ️ Такий слот вже існує.")
    await state.clear()


@dp.callback_query(F.data == "a:day")
async def a_day(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    today = date.today()
    await call.message.answer("Оберіть дату (календар):", reply_markup=kb_calendar(month_key(today.year, today.month), "a_day"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_day:month:"))
async def a_day_month(call: CallbackQuery):
    mk = call.data.split(":")[-1]
    await call.message.edit_reply_markup(reply_markup=kb_calendar(mk, "a_day"))
    await call.answer()


def kb_admin_day_actions(d: str, bookings: list[dict]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if bookings:
        for bk in bookings:
            label = f"{bk['t']} — {bk['client_name']} ({bk['service']}{' '+bk['ext_type'] if bk['ext_type'] else ''})"
            if bk["status"] != "active":
                label = "🚫 " + label
            b.row(InlineKeyboardButton(text=f"❌ Скасувати: {label}", callback_data=f"a:cancel:{bk['id']}"))
            b.row(InlineKeyboardButton(text=f"🔁 Перенести: {label}", callback_data=f"a:move:{bk['id']}"))
    else:
        b.row(InlineKeyboardButton(text="(Нема записів)", callback_data="noop"))

    b.row(InlineKeyboardButton(text="⬅️ Назад до календаря", callback_data="a:day"))
    return b.as_markup()


@dp.callback_query(F.data.startswith("a_day:day:"))
async def a_day_show(call: CallbackQuery):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    d = call.data.split(":")[-1]
    bookings = await get_day_bookings(d)

    lines = [f"📌 Записи на {d}:"]
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
        return await call.answer("Нема доступу", show_alert=True)
    bid = int(call.data.split(":")[-1])
    ok, d, t = await cancel_booking(bid)
    if ok:
        await call.message.answer(f"✅ Запис #{bid} скасовано. Слот {d} {t} знову відкрито.")
    else:
        await call.message.answer("❌ Не знайшов цей запис.")
    await call.answer()


@dp.callback_query(F.data.startswith("a:move:"))
async def a_move_start(call: CallbackQuery, state: FSMContext):
    if not is_admin_username(call):
        return await call.answer("Нема доступу", show_alert=True)
    bid = int(call.data.split(":")[-1])
    await state.clear()
    await state.set_state(AdminMove.booking_id)
    await state.update_data(booking_id=bid)

    today = date.today()
    mk = month_key(today.year, today.month)
    await call.message.answer(
        f"Оберіть НОВУ дату для переносу запису #{bid}:",
        reply_markup=kb_calendar(mk, f"a_move:{bid}")
    )
    await call.answer()


@dp.callback_query(F.data.startswith("a_move:") and F.data.contains(":month:"))
async def a_move_month(call: CallbackQuery):
    # a_move:<bid>:month:<mk>
    parts = call.data.split(":")
    bid = parts[1]
    mk = parts[-1]
    await call.message.edit_reply_markup(reply_markup=kb_calendar(mk, f"a_move:{bid}"))
    await call.answer()


@dp.callback_query(F.data.startswith("a_move:") and F.data.contains(":day:"))
async def a_move_day(call: CallbackQuery, state: FSMContext):
    parts = call.data.split(":")
    bid = int(parts[1])
    d = parts[-1]

    times = await get_open_times(d)
    if not times:
        await call.message.answer("На цю дату немає вільних слотів. Оберіть іншу дату.")
        await call.answer()
        return

    await state.update_data(booking_id=bid, new_day=d)
    await state.set_state(AdminMove.new_time)

    await call.message.answer(
        f"Оберіть НОВИЙ час для #{bid} на {d}:",
        reply_markup=kb_times(d, times, f"a_move:{bid}")
    )
    await call.answer()


@dp.callback_query(F.data.startswith("a_move:") and F.data.contains(":time:"))
async def a_move_time(call: CallbackQuery, state: FSMContext):
    # a_move:<bid>:time:<d>:<t>
    parts = call.data.split(":")
    bid = int(parts[1])
    d = parts[3]
    t = parts[4]

    ok = await move_booking(bid, d, t)
    if ok:
        await call.message.answer(f"✅ Перенесено запис #{bid} на {d} {t}.")
    else:
        await call.message.answer("❌ Не вдалося перенести (слот може бути зайнятий або запис неактивний).")

    await state.clear()
    await call.answer()


# ================== NOOP ==================
@dp.callback_query(F.data == "noop")
async def noop(call: CallbackQuery):
    await call.answer()


# ================== MAIN ==================
async def main():
    await ensure_schema()
    print("=== BOT STARTED (polling) ===", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
