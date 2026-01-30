import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ================== CONFIG ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in Railway Variables")

# Адміни (username без @)
ADMIN_USERNAMES = {"roman2696", "Ekaterinahorbatiuk"}

DB_PATH = "lashes_bot.sqlite3"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


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

def is_date(s: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", s))

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

async def db_add_slot(date: str, time: str):
    async with aiosqlite.connect(DB_PATH) as db:
        # якщо такий слот вже є — просто відкриємо його
        cur = await db.execute("SELECT id FROM slots WHERE date=? AND time=?", (date, time))
        row = await cur.fetchone()
        if row:
            await db.execute("UPDATE slots SET is_open=1 WHERE id=?", (row[0],))
        else:
            await db.execute(
                "INSERT INTO slots(date, time, is_open) VALUES(?,?,1)",
                (date, time)
            )
        await db.commit()

async def db_toggle_slot(date: str, time: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, is_open FROM slots WHERE date=? AND time=?", (date, time))
        row = await cur.fetchone()
        if row:
            slot_id, is_open = row
            new_val = 0 if is_open == 1 else 1
            await db.execute("UPDATE slots SET is_open=? WHERE id=?", (new_val, slot_id))
            await db.commit()
            return new_val  # 1=open, 0=closed
        else:
            await db.execute("INSERT INTO slots(date, time, is_open) VALUES(?,?,1)", (date, time))
            await db.commit()
            return 1

async def db_get_all_dates():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT DISTINCT date FROM slots ORDER BY date")
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def db_get_dates_with_open_slots():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT DISTINCT date FROM slots
            WHERE is_open=1
            ORDER BY date
        """)
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def db_get_times_for_date(date: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT time FROM slots
            WHERE date=? 
            ORDER BY time
        """, (date,))
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def db_get_open_times_for_date(date: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT time FROM slots
            WHERE date=? AND is_open=1
            ORDER BY time
        """, (date,))
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def db_is_slot_free(date: str, time: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT COUNT(*) FROM appointments
            WHERE date=? AND time=? AND status='booked'
        """, (date, time))
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

def kb_dates(dates: list[str]):
    kb = InlineKeyboardBuilder()
    for d in dates[:20]:
        kb.button(text=f"📅 {d}", callback_data=f"bk:date:{d}")
    kb.button(text="⬅️ Назад", callback_data="bk:back:services")
    kb.adjust(2, 1)
    return kb.as_markup()

def kb_times(date: str, times: list[str]):
    kb = InlineKeyboardBuilder()
    for t in times[:48]:
        kb.button(text=f"🕒 {t}", callback_data=f"bk:time:{date}|{t}")
    kb.button(text="⬅️ Назад", callback_data=f"bk:back:dates:{date}")
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
    kb.button(text="📅 Слоти (кнопками)", callback_data="adm:slots")
    kb.button(text="➕ Додати слот (командою)", callback_data="adm:help_addslot")
    kb.button(text="⬅️ Назад", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def kb_admin_dates(dates: list[str]):
    kb = InlineKeyboardBuilder()
    for d in dates[:20]:
        kb.button(text=f"📅 {d}", callback_data=f"adm:date:{d}")
    kb.button(text="➕ Додати нову дату", callback_data="adm:newdate")
    kb.button(text="⬅️ Назад", callback_data="menu:admin")
    kb.adjust(2, 1, 1)
    return kb.as_markup()

def kb_admin_times(date: str, times: list[str], open_times: set[str]):
    kb = InlineKeyboardBuilder()
    for t in times[:48]:
        mark = "✅" if t in open_times else "❌"
        kb.button(text=f"{mark} {t}", callback_data=f"adm:toggle:{date}|{t}")
    kb.button(text="➕ Додати час", callback_data=f"adm:addtime:{date}")
    kb.button(text="⬅️ Назад", callback_data="adm:slots")
    kb.adjust(4, 4, 4, 4, 4, 1, 1)
    return kb.as_markup()


# ================== BOOKING STATE ==================
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

# Адмін-потік (ввід дати/часу текстом)
ADMIN_FLOW: dict[int, dict] = {}


# ================== START (PHOTO + TEXT) ==================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    # Фото має бути в assets/welcome.jpg
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
    await cq.message.edit_text(text, reply_markup=main_menu_kb(is_admin(cq.from_user)))
    await cq.answer()


# ================== ADMIN ==================
@dp.callback_query(F.data == "menu:admin")
async def admin_menu(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    await cq.message.edit_text(
        "🛠 Адмін\n\n"
        "Тут ти відкриваєш віконця для запису.\n"
        "Найзручніше: «Слоти (кнопками)».",
        reply_markup=kb_admin()
    )
    await cq.answer()

@dp.callback_query(F.data == "adm:help_addslot")
async def admin_help_addslot(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    await cq.message.edit_text(
        "➕ Додати слот (командою)\n\n"
        "Формат:\n"
        "`/addslot 2026-02-05 14:30`\n\n"
        "Але краще — «Слоти (кнопками)».",
        reply_markup=kb_admin(),
        parse_mode="Markdown"
    )
    await cq.answer()

@dp.message(Command("addslot"))
async def cmd_addslot(message: Message):
    if not is_admin(message.from_user):
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Формат: /addslot YYYY-MM-DD HH:MM\nПриклад: /addslot 2026-02-05 14:30")
        return
    d, t = parts[1], parts[2]
    if not is_date(d) or not is_time(t):
        await message.answer("Невірний формат. Приклад: /addslot 2026-02-05 14:30")
        return
    await db_add_slot(d, t)
    await message.answer(f"✅ Додано слот: {d} {t}")

# --- Admin slots by buttons ---
@dp.callback_query(F.data == "adm:slots")
async def admin_slots(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    dates = await db_get_all_dates()
    await cq.message.edit_text(
        "📅 Слоти\n\nОбери дату або додай нову:",
        reply_markup=kb_admin_dates(dates)
    )
    await cq.answer()

@dp.callback_query(F.data == "adm:newdate")
async def admin_newdate(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    ADMIN_FLOW[cq.from_user.id] = {"step": "date"}
    await cq.message.edit_text("Введи дату у форматі YYYY-MM-DD (наприклад 2026-02-05):")
    await cq.answer()

@dp.callback_query(F.data.startswith("adm:date:"))
async def admin_pick_date(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    date = cq.data.split(":", 2)[2]
    times = await db_get_times_for_date(date)
    open_times = set(await db_get_open_times_for_date(date))

    await cq.message.edit_text(
        f"📅 {date}\n\nНатискай на час — відкриє/закриє слот ✅/❌",
        reply_markup=kb_admin_times(date, times, open_times)
    )
    await cq.answer()

@dp.callback_query(F.data.startswith("adm:addtime:"))
async def admin_add_time_start(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return
    date = cq.data.split(":", 2)[2]
    ADMIN_FLOW[cq.from_user.id] = {"step": "time", "date": date}
    await cq.message.edit_text(
        f"📅 {date}\n\nВведи час у форматі HH:MM (наприклад 14:30):"
    )
    await cq.answer()

@dp.callback_query(F.data.startswith("adm:toggle:"))
async def admin_toggle_slot(cq: CallbackQuery):
    if not is_admin(cq.from_user):
        await cq.answer("Немає доступу", show_alert=True)
        return

    payload = cq.data.split(":", 2)[2]
    date, time = payload.split("|", 1)

    new_val = await db_toggle_slot(date, time)
    await cq.answer("✅ Відкрито" if new_val == 1 else "❌ Закрито")

    times = await db_get_times_for_date(date)
    open_times = set(await db_get_open_times_for_date(date))
    await cq.message.edit_text(
        f"📅 {date}\n\nНатискай на час — відкриє/закриє слот ✅/❌",
        reply_markup=kb_admin_times(date, times, open_times)
    )


# ================== BOOKING FLOW ==================
@dp.callback_query(F.data == "menu:book")
async def start_booking(cq: CallbackQuery):
    BOOKING[cq.from_user.id] = BookingState()
    await cq.message.edit_text("Оберіть послугу 👇", reply_markup=kb_services())
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
        await cq.message.edit_text("Оберіть тип нарощування 👇", reply_markup=kb_ext_types())
    else:
        await show_booking_dates(cq)
    await cq.answer()

@dp.callback_query(F.data.startswith("bk:sub:"))
async def choose_subtype(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id)
    if not st:
        await cq.answer("Натисніть /start", show_alert=True)
        return
    st.subtype = cq.data.split(":", 2)[2]
    await show_booking_dates(cq)
    await cq.answer()

async def show_booking_dates(cq: CallbackQuery):
    dates = await db_get_dates_with_open_slots()
    if not dates:
        await cq.message.edit_text(
            "Поки що немає відкритих віконець 😔\n\n"
            "Адміну треба відкрити слоти в адмінці.",
            reply_markup=kb_services()
        )
        return
    await cq.message.edit_text("Оберіть дату 📅", reply_markup=kb_dates(dates))

@dp.callback_query(F.data.startswith("bk:date:"))
async def choose_date(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id)
    if not st:
        await cq.answer("Натисніть /start", show_alert=True)
        return
    st.date = cq.data.split(":", 2)[2]
    times = await db_get_open_times_for_date(st.date)
    if not times:
        await cq.message.edit_text("На цю дату немає відкритих слотів.", reply_markup=kb_dates(await db_get_dates_with_open_slots()))
        return
    await cq.message.edit_text("Оберіть час 🕒", reply_markup=kb_times(st.date, times))
    await cq.answer()

@dp.callback_query(F.data.startswith("bk:time:"))
async def choose_time(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id)
    if not st:
        await cq.answer("Натисніть /start", show_alert=True)
        return

    payload = cq.data.split(":", 2)[2]
    date, time = payload.split("|", 1)

    ok = await db_is_slot_free(date, time)
    if not ok:
        await cq.answer("Цей час вже зайнятий 😔", show_alert=True)
        return

    st.date = date
    st.time = time

    st.step = "name"
    await cq.message.edit_text("✍️ Вкажіть ваше імʼя:")
    await cq.answer()

@dp.callback_query(F.data == "bk:back:services")
async def back_services(cq: CallbackQuery):
    await cq.message.edit_text("Оберіть послугу 👇", reply_markup=kb_services())
    await cq.answer()

@dp.callback_query(F.data.startswith("bk:back:dates:"))
async def back_dates(cq: CallbackQuery):
    await show_booking_dates(cq)
    await cq.answer()

@dp.callback_query(F.data == "bk:change:time")
async def change_time(cq: CallbackQuery):
    st = BOOKING.get(cq.from_user.id)
    if not st or not st.date:
        await cq.answer("Немає вибраної дати", show_alert=True)
        return
    times = await db_get_open_times_for_date(st.date)
    await cq.message.edit_text("Оберіть інший час 🕒", reply_markup=kb_times(st.date, times))
    await cq.answer()

@dp.callback_query(F.data == "bk:cancel")
async def book_cancel(cq: CallbackQuery):
    BOOKING.pop(cq.from_user.id, None)
    await cq.message.edit_text("Запис скасовано.", reply_markup=main_menu_kb(is_admin(cq.from_user)))
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

    await cq.message.edit_text(
        "✅ Запис підтверджено!\n\n"
        f"№ {app_id}\n"
        f"👤 {st.client_name}\n📞 {st.phone}\n"
        f"📌 {st.service}{' ('+st.subtype+')' if st.subtype else ''}\n"
        f"📅 {st.date}\n🕒 {st.time}\n\n"
        "До зустрічі ✨",
        reply_markup=main_menu_kb(is_admin(cq.from_user))
    )
    await cq.answer()


# ================== INPUT ROUTER (ADMIN + BOOKING) ==================
@dp.message()
async def input_router(message: Message):
    text = (message.text or "").strip()

    # --- ADMIN FLOW (дата/час) ---
    if is_admin(message.from_user) and message.from_user.id in ADMIN_FLOW:
        flow = ADMIN_FLOW[message.from_user.id]
        step = flow.get("step")

        if step == "date":
            if not is_date(text):
                await message.answer("❌ Невірно. Формат: YYYY-MM-DD (наприклад 2026-02-05)")
                return
            flow["date"] = text
            flow["step"] = "time"
            await message.answer(f"✅ Дата {text} збережена.\nТепер введи час HH:MM (наприклад 14:30):")
            return

        if step == "time":
            date = flow.get("date")
            if not is_time(text):
                await message.answer("❌ Невірно. Формат: HH:MM (наприклад 14:30)")
                return
            await db_add_slot(date, text)
            await message.answer(f"✅ Додано слот: {date} {text}\n\nМожеш вводити наступний час або натисни /start")
            return

    # --- BOOKING FLOW (ім'я/телефон) ---
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
        await cq.message.edit_text("У вас поки немає записів.", reply_markup=main_menu_kb(is_admin(cq.from_user)))
        await cq.answer()
        return

    lines = ["📋 Ваші записи:\n"]
    for app_id, d, t, svc, sub, status in rows:
        s = f"{svc}" + (f" ({sub})" if sub else "")
        lines.append(f"— #{app_id} • {d} {t} • {s} • {status}")
    await cq.message.edit_text("\n".join(lines), reply_markup=main_menu_kb(is_admin(cq.from_user)))
    await cq.answer()


# ================== RUN ==================
async def main():
    await db_init()
    print("=== START POLLING ===", flush=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())



