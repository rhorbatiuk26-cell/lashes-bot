import asyncio
import os
import re
import calendar
from dataclasses import dataclass
from datetime import datetime, date

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ================== CONFIG ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in Railway Variables")

ADMIN_USERNAMES = {"roman2696", "Ekaterinahorbatiuk"}  # username без @
DB_PATH = "lashes_bot.sqlite3"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ================== SMART EDIT (FIX) ==================
async def smart_edit(cq: CallbackQuery, text: str, reply_markup=None):
    """
    Якщо повідомлення було текстове -> edit_text
    Якщо це фото/медіа -> edit_caption
    Якщо Telegram не дає редагувати -> надсилаємо нове повідомлення
    """
    try:
        if cq.message.text is not None:
            await cq.message.edit_text(text, reply_markup=reply_markup)
        else:
            await cq.message.edit_caption(caption=text, reply_markup=reply_markup)
    except Exception:
        await cq.message.answer(text, reply_markup=reply_markup)


# ================== HELPERS ==================
def is_admin(user) -> bool:
    u = (user.username or "").lstrip("@")
    return u in ADMIN_USERNAMES

def now_local():
    return datetime.now()

def clean_phone(s: str):
    s = s.strip()
    s2 = "".join(ch for ch in s if ch.isdigit() or ch == "+")
    digits = "".join(ch for ch in s2 if ch.isdigit())
    if len(digits) < 10 or len(digits) > 15:
        return None
    return s2

def is_time(s: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}", s))


# ================== DATABASE ==================
async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            is_open INTEGER NOT NULL DEFAULT 1
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            client_name TEXT,
            phone TEXT,
            service TEXT NOT NULL,
            subtype TEXT,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'booked',
            created_at TEXT NOT NULL
        )
        """)
        await db.commit()

async def db_add_slot(date_: str, time_: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id FROM slots WHERE date=? AND time=?", (date_, time_))
        row = await cur.fetchone()
        if row:
            await db.execute("UPDATE slots SET is_open=1 WHERE id=?", (row[0],))
        else:
            await db.execute("INSERT INTO slots(date, time, is_open) VALUES(?,?,1)", (date_, time_))
        await db.commit()

async def db_toggle_slot(date_: str, time_: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, is_open FROM slots WHERE date=? AND time=?", (date_, time_))
        row = await cur.fetchone()
        if row:
            slot_id, is_open = row
            new_val = 0 if is_open == 1 else 1
            await db.execute("UPDATE slots SET is_open=? WHERE id=?", (new_val, slot_id))
            await db.commit()
            return new_val
        else:
            await db.execute("INSERT INTO slots(date, time, is_open) VALUES(?,?,1)", (date_, time_))
            await db.commit()
            return 1

async def db_get_times_for_date(date_: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT time FROM slots WHERE date=? ORDER BY time", (date_,))
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def db_get_open_times_for_date(date_: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT time FROM slots WHERE date=? AND is_open=1 ORDER BY time", (date_,))
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def db_open_days_in_month(y: int, m: int) -> set[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT DISTINCT date FROM slots
            WHERE is_open=1 AND substr(date,1,7)=?
            ORDER BY date
        """, (f"{y:04d}-{m:02d}",))
        rows = await cur.fetchall()
        days = set()
        for (dstr,) in rows:
            try:
                days.add(int(dstr.split("-")[2]))
            except:
                pass
        return days

async def db_is_slot_free(date_: str, time_: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT COUNT(*) FROM appointments
            WHERE date=? AND time=? AND status='booked'
        """, (date_, time_))
        c = (await cur.fetchone())[0]
        return c == 0

async def db_create_appointment(st, user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            INSERT INTO appointments(user_id, username, client_name, phone, service, subtype, date, time, created_at)
            VALUES(?,?,?,?,?,?,?,?,?)
        """, (
            user_id,
            username or "",
            st.client_name or "",
            st.phone or "",
            st.service,
            st.subtype,
            st.date,
            st.time,
            now_local().isoformat(timespec="seconds")
        ))
        await db.commit()
        return cur.lastrowid

async def db_my_appointments(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, date, time, service, COALESCE(subtype,''), status
            FROM appointments
            WHERE user_id=?
            ORDER BY date, time
        """, (user_id,))
        return await cur.fetchall()


# ================== CALENDAR UI ==================
def ym_add(y: int, m: int, delta: int):
    m2 = m + delta
    y2 = y + (m2 - 1) // 12
    m2 = (m2 - 1) % 12 + 1
    return y2, m2

def build_month_kb(prefix: str, y: int, m: int, enabled_days: set[int]):
    kb = InlineKeyboardBuilder()

    prev_y, prev_m = ym_add(y, m, -1)
    next_y, next_m = ym_add(y, m, +1)

    kb.button(text="◀︎", callback_data=f"{prefix}:nav:{prev_y:04d}-{prev_m:02d}")
    kb.button(text=f"{calendar.month_name[m]} {y}", callback_data=f"{prefix}:noop")
    kb.button(text="▶︎", callback_data=f"{prefix}:nav:{next_y:04d}-{next_m:02d}")

    for wd in ["Пн","Вт","Ср","Чт","Пт","Сб","Нд"]:
        kb.button(text=wd, callback_data=f"{prefix}:noop")

    cal = calendar.Calendar(firstweekday=0)
    for week in cal.monthdayscalendar(y, m):
        for d in week:
            if d == 0:
                kb.button(text=" ", callback_data=f"{prefix}:noop")
            else:
                if d in enabled_days:
                    kb.button(text=str(d), callback_data=f"{prefix}:day:{y:04d}-{m:02d}-{d:02d}")
                else:
                    kb.button(text=f"·{d}", callback_data=f"{prefix}:noop")

    kb.adjust(3, 7, 7, 7, 7, 7, 7)
    return kb.as_markup()


# ================== UI (KEYBOARDS) ==================
def main_menu_kb(is_admin_flag: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Записатись", callback_data="menu:book")
    kb.button(text="📋 Мої записи", callback_data="menu:mine")
    if is_admin_flag:
        kb.button(text="🛠 Адмін", callback_data="menu:admin")
    kb.adjust(1)
    return kb.as_markup()

def kb_services():
    kb = InlineKeyboardBuilder()
    kb.button(text="✨ Ламінування", callback_data="bk:svc:Ламінування")
    kb.button(text="💎 Нарощування", callback_data="bk:svc:Нарощування")
    kb.button(text="⬅️ Назад", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def kb_ext_types():
    kb = InlineKeyboardBuilder()
    kb.button(text="Класика", callback_data="bk:sub:Класика")
    kb.button(text="2D", callback_data="bk:sub:2D")
    kb.button(text="3D", callback_data="bk:sub:3D")
    kb.button(text="⬅️ Назад", callback_data="bk:back:services")
    kb.adjust(2, 1, 1)
    return kb.as_markup()

def kb_times_booking(date_: str, times: list[str]):
    kb = InlineKeyboardBuilder()
    for t in times[:48]:
        kb.button(text=f"🕒 {t}", callback_data=f"bk:time:{date_}|{t}")
    kb.button(text="⬅️ Назад", callback_data="bk:back:calendar")
    kb.adjust(4, 4, 4, 4, 4, 1)
    return kb.as_markup()

def kb_confirm():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Підтвердити", callback_data="bk:confirm")
    kb.button(text="✏️ Змінити час", callback_data="bk:change:time")
    kb.button(text="❌ Скасувати", callback_data="bk:cancel")
    kb.adjust(1)
    return kb.as_markup()

def kb_admin():
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Календар слотів", callback_data="adm:calendar")
    kb.button(text="⬅️ Назад", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def kb_admin_times(date_: str, times: list[str], open_times: set[str]):
    kb = InlineKeyboardBuilder()
    for t in times[:60]:
        mark = "✅" if t in open_times else "❌"
        kb.button(text=f"{mark} {t}", callback_data=f"adm:toggle:{date_}|{t}")
    kb.button(text="➕ Додати час", callback_data=f"adm:addtime:{date_}")
    kb.button(text="⬅️ Назад", callback_data="adm:calendar")
    kb.adjust(4, 4, 4, 4, 4, 4, 1, 1)
    return kb.as_markup()


# ================== STATE ==================
@dataclass
class BookingState:
    service: str | None = None
    subtype: str | None = None
    date: str | None = None
    time: str | None = None
    client_name: str | None = None
    phone: str | None = None
    step: str | None = None  # "name" або "phone"

BOOKING: dict[int, BookingState] = {}
ADMIN_FLOW: dict[int, dict] = {}  # {admin_id: {"step": "time", "date": "YYYY-MM-DD"}}


# ================== START (PHOTO + TEXT) ==================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    photo = FSInputFile("assets/welcome.jpg")
    text = (
        "Lash Studio ✨\n\n"
        "Запис онлайн на процедури.\n"
        "Оберіть дію нижче 👇"
    )
    await message.answer_photo(
        photo=photo,
        caption=text,
        reply_markup=main_menu_kb(is_admin(message.from_user))
    )

@dp.callback_query(F.data == "menu:home")
async def menu_home(cq: CallbackQuery):
    text = (
        "Lash Studio ✨\n\n"
        "Запис онлайн на процедури.\n"
        "Оберіть дію нижче 👇"
    )
    await smart_edit(cq, text, reply_markup=main_menu_kb(is_admin(cq.from_user)))
    await cq.answer()


# ================== ADMIN (CALENDAR) ==================
@dp.callback_query(F.data == "menu:admin")
async def admin_menu(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    await smart_edit(cq, "🛠 Адмін\n\nКалендар слотів:", reply_markup=kb_admin())
    await cq.answer()

async def show_admin_calendar(cq: CallbackQuery, y: int | None = None, m: int | None = None):
    today = date.today()
    y = y or today.year
    m = m or today.month

    _, last_day = calendar.monthrange(y, m)
    enabled_days = set(range(1, last_day + 1))

    await smart_edit(
        cq,
        "📅 Оберіть дату (адмін):",
        reply_markup=build_month_kb("cala", y, m, enabled_days)
    )

@dp.callback_query(F.data == "adm:calendar")
async def admin_calendar_entry(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    await show_admin_calendar(cq)
    await cq.answer()

@dp.callback_query(F.data.startswith("cala:nav:"))
async def cala_nav(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    ym = cq.data.split(":", 2)[2]
    y, m = map(int, ym.split("-"))
    await show_admin_calendar(cq, y, m)
    await cq.answer()

@dp.callback_query(F.data.startswith("cala:day:"))
async def cala_pick_day(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    date_ = cq.data.split(":", 2)[2]

    times = await db_get_times_for_date(date_)
    open_times = set(await db_get_open_times_for_date(date_))

    await smart_edit(
        cq,
        f"📅 {date_}\n\nНатискай час: ✅ відкрито / ❌ закрито",
        reply_markup=kb_admin_times(date_, times, open_times)
    )
    await cq.answer()

@dp.callback_query(F.data.startswith("adm:addtime:"))
async def admin_add_time_start(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    date_ = cq.data.split(":", 2)[2]
    ADMIN_FLOW[cq.from_user.id] = {"step": "time", "date": date_}
    await smart_edit(cq, f"📅 {date_}\n\nВведіть час HH:MM (наприклад 14:30):")
    await cq.answer()

@dp.callback_query(F.data.startswith("adm:toggle:"))
async def admin_toggle_slot(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    payload = cq.data.split(":", 2)[2]
    date_, time_ = payload.split("|", 1)

    new_val = await db_toggle_slot(date_, time_)
    await cq.answer("✅ Відкрито" if new_val == 1 else "❌ Закрито")

    times = await db_get_times_for_date(date_)
    open_times = set(await db_get_open_times_for_date(date_))
    await smart_edit(
        cq,
        f"📅 {date_}\n\nНатискай час: ✅ відкрито / ❌ закрито",
        reply_markup=kb_admin_times(date_, times, open_times)
    )


# ================== BOOKING FLOW (WITH CALENDAR) ==================
@dp.callback_query(F.data == "menu:book")
async def start_booking(cq: CallbackQuery):
    BOOKING[cq.from_user.id] = BookingState()
    await smart_edit(cq, "Оберіть послугу 👇", reply_markup=kb_services())
    await cq.answer()

@dp.callback_query(F.data.startswith("bk:svc:"))
async def choose_service(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id) or BookingState()
    BOOKING[cq.from_user.id] = st

    st.service = cq.data.split(":", 2)[2]
    st.subtype = None
    st.date = None
    st.time = None
    st.client_name = None
    st.phone = None
    st.step = None

    if st.service == "Нарощування":
        await smart_edit(cq, "Оберіть тип нарощування 👇", reply_markup=kb_ext_types())
    else:
        await show_booking_calendar(cq)
    await cq.answer()

@dp.callback_query(F.data.startswith("bk:sub:"))
async def choose_subtype(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id)
    if not st:
        await cq.answer("Натисніть /start", show_alert=True)
        return
    st.subtype = cq.data.split(":", 2)[2]
    await show_booking_calendar(cq)
    await cq.answer()

async def show_booking_calendar(cq: CallbackQuery, y: int | None = None, m: int | None = None):
    today = date.today()
    y = y or today.year
    m = m or today.month

    enabled_days = await db_open_days_in_month(y, m)
    await smart_edit(
        cq,
        "Оберіть дату 📅\n(·дні — недоступні):",
        reply_markup=build_month_kb("calb", y, m, enabled_days)
    )

@dp.callback_query(F.data.startswith("calb:nav:"))
async def calb_nav(cq: CallbackQuery):
    ym = cq.data.split(":", 2)[2]
    y, m = map(int, ym.split("-"))
    await show_booking_calendar(cq, y, m)
    await cq.answer()

@dp.callback_query(F.data.startswith("calb:day:"))
async def calb_pick_day(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id)
    if not st:
        await cq.answer("Натисніть /start", show_alert=True)
        return

    date_ = cq.data.split(":", 2)[2]
    times = await db_get_open_times_for_date(date_)
    if not times:
        await cq.answer("На цю дату немає відкритих слотів", show_alert=True)
        return

    st.date = date_
    await smart_edit(cq, f"Оберіть час 🕒 ({date_})", reply_markup=kb_times_booking(date_, times))
    await cq.answer()

@dp.callback_query(F.data == "bk:back:services")
async def back_services(cq: CallbackQuery):
    await smart_edit(cq, "Оберіть послугу 👇", reply_markup=kb_services())
    await cq.answer()

@dp.callback_query(F.data == "bk:back:calendar")
async def back_calendar(cq: CallbackQuery):
    await show_booking_calendar(cq)
    await cq.answer()

@dp.callback_query(F.data.startswith("bk:time:"))
async def choose_time(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id)
    if not st:
        await cq.answer("Натисніть /start", show_alert=True)
        return

    payload = cq.data.split(":", 2)[2]
    date_, time_ = payload.split("|", 1)

    ok = await db_is_slot_free(date_, time_)
    if not ok:
        await cq.answer("Цей час вже зайнятий 😔", show_alert=True)
        return

    st.date = date_
    st.time = time_
    st.step = "name"

    await smart_edit(cq, "✍️ Вкажіть ваше імʼя:")
    await cq.answer()

@dp.callback_query(F.data == "bk:change:time")
async def change_time(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id)
    if not st or not st.date:
        await cq.answer("Немає вибраної дати", show_alert=True)
        return
    times = await db_get_open_times_for_date(st.date)
    await smart_edit(cq, f"Оберіть інший час 🕒 ({st.date})", reply_markup=kb_times_booking(st.date, times))
    await cq.answer()

@dp.callback_query(F.data == "bk:cancel")
async def book_cancel(cq: CallbackQuery):
    BOOKING.pop(cq.from_user.id, None)
    await smart_edit(cq, "Запис скасовано.", reply_markup=main_menu_kb(is_admin(cq.from_user)))
    await cq.answer()

@dp.callback_query(F.data == "bk:confirm")
async def book_confirm(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id)
    if not st or not (st.service and st.date and st.time and st.client_name and st.phone):
        await cq.answer("Дані неповні. Натисніть /start", show_alert=True)
        return

    ok = await db_is_slot_free(st.date, st.time)
    if not ok:
        await cq.answer("Час вже зайняли 😔 Оберіть інший.", show_alert=True)
        return

    app_id = await db_create_appointment(st, cq.from_user.id, cq.from_user.username or "")
    BOOKING.pop(cq.from_user.id, None)

    msg = (
        "✅ Запис підтверджено!\n\n"
        f"№ {app_id}\n"
        f"👤 {st.client_name}\n📞 {st.phone}\n"
        f"📌 {st.service}{' ('+st.subtype+')' if st.subtype else ''}\n"
        f"📅 {st.date}\n🕒 {st.time}\n\n"
        "До зустрічі ✨"
    )
    await smart_edit(cq, msg, reply_markup=main_menu_kb(is_admin(cq.from_user)))
    await cq.answer()


# ================== INPUT ROUTER (ADMIN time + BOOKING name/phone) ==================
@dp.message()
async def input_router(message: Message):
    text = (message.text or "").strip()

    # --- ADMIN: add time after calendar date selected ---
    if is_admin(message.from_user) and message.from_user.id in ADMIN_FLOW:
        flow = ADMIN_FLOW[message.from_user.id]
        if flow.get("step") == "time":
            date_ = flow.get("date")
            if not is_time(text):
                await message.answer("❌ Невірно. Формат: HH:MM (наприклад 14:30)")
                return
            await db_add_slot(date_, text)
            await message.answer(f"✅ Додано слот: {date_} {text}\nМожеш вводити наступний час або відкрий календар слотів ще раз.")
            return

    # --- BOOKING: name/phone ---
    st = BOOKING.get(message.from_user.id)
    if not st or not st.step:
        return

    if st.step == "name":
        if len(text) < 2:
            await message.answer("❌ Імʼя занадто коротке. Напишіть ще раз:")
            return
        st.client_name = text
        st.step = "phone"
        await message.answer("📞 Вкажіть номер телефону (наприклад +380XXXXXXXXX):")
        return

    if st.step == "phone":
        ph = clean_phone(text)
        if not ph:
            await message.answer("❌ Невірний номер. Спробуйте ще раз (наприклад +380XXXXXXXXX):")
            return
        st.phone = ph
        st.step = None

        title = "Підтвердіть запис:\n\n"
        title += f"👤 {st.client_name}\n📞 {st.phone}\n"
        title += f"📌 {st.service}"
        if st.subtype:
            title += f" ({st.subtype})"
        title += f"\n📅 {st.date}\n🕒 {st.time}"

        await message.answer(title, reply_markup=kb_confirm())


# ================== MY APPOINTMENTS ==================
@dp.callback_query(F.data == "menu:mine")
async def my_appointments(cq: CallbackQuery):
    rows = await db_my_appointments(cq.from_user.id)
    if not rows:
        await smart_edit(cq, "У вас поки немає записів.", reply_markup=main_menu_kb(is_admin(cq.from_user)))
        await cq.answer()
        return

    lines = ["📋 Ваші записи:\n"]
    for app_id, d, t, svc, sub, status in rows:
        s = f"{svc}" + (f" ({sub})" if sub else "")
        lines.append(f"— #{app_id} • {d} {t} • {s} • {status}")

    await smart_edit(cq, "\n".join(lines), reply_markup=main_menu_kb(is_admin(cq.from_user)))
    await cq.answer()


# ================== NOOP HANDLER ==================
@dp.callback_query(F.data.endswith(":noop"))
async def noop(cq: CallbackQuery):
    await cq.answer()


# ================== RUN ==================
async def main():
    await db_init()
    print("=== START POLLING ===", flush=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
