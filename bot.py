import asyncio
import os
import re
import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext


# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Add it in Railway Variables.")

DB_PATH = "lashes_bot.sqlite3"

# Адміни (без @)
ADMIN_USERNAMES = {"roman2696", "Ekaterinahorbatiuk"}

# Опційно: картинка на /start
# 1) Якщо є файл у репо: assets/start.jpg -> бот відправить як локальний файл
# 2) Або можна задати START_PHOTO_URL (https://...) у Railway Variables
START_PHOTO_LOCAL = "assets/start.jpg"
START_PHOTO_URL = os.getenv("START_PHOTO_URL", "").strip()

# Назви послуг
SERVICE_LAMI = "Ламінування"
SERVICE_EXT = "Нарощування"
EXT_TYPES = ["Класика", "2D", "3D"]


# =========================
# HELPERS
# =========================
def is_admin_username(obj: Message | CallbackQuery) -> bool:
    user = obj.from_user
    username = (user.username or "").lstrip("@")
    return username in ADMIN_USERNAMES


def norm_phone(s: str) -> Optional[str]:
    s = s.strip()
    s = re.sub(r"[^\d+]", "", s)
    # дозволимо: +380XXXXXXXXX або 0XXXXXXXXX або просто цифри (мін 9)
    digits = re.sub(r"\D", "", s)
    if len(digits) < 9:
        return None
    if s.startswith("+"):
        return s
    # якщо починається з 0 або 3 — ок
    return digits


def fmt_date(d: str) -> str:
    # d = YYYY-MM-DD -> DD.MM.YYYY
    try:
        dt = datetime.strptime(d, "%Y-%m-%d").date()
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return d


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def month_add(y: int, m: int, delta: int) -> Tuple[int, int]:
    # delta = +1 / -1 month
    nm = m + delta
    ny = y
    while nm > 12:
        nm -= 12
        ny += 1
    while nm < 1:
        nm += 12
        ny -= 1
    return ny, nm


def safe_edit_text_cq(cq: CallbackQuery, text: str, kb: Optional[InlineKeyboardMarkup] = None):
    # Telegram інколи кидає "there is no text in the message to edit" якщо це photo-message
    # Тому просто відправляємо нове повідомлення як fallback.
    async def _inner():
        try:
            if cq.message and cq.message.text:
                await cq.message.edit_text(text, reply_markup=kb)
            else:
                await cq.message.answer(text, reply_markup=kb)
        except Exception:
            await cq.message.answer(text, reply_markup=kb)
    return _inner()


# =========================
# DB
# =========================
async def db_exec(sql: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, params)
        await db.commit()


async def db_fetchall(sql: str, params: tuple = ()) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows


async def db_fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(sql, params)
        row = await cur.fetchone()
        await cur.close()
        return row


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # slots: часи, які адмін відкриває/закриває
        await db.execute("""
        CREATE TABLE IF NOT EXISTS slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            d TEXT NOT NULL,          -- YYYY-MM-DD
            t TEXT NOT NULL,          -- HH:MM
            is_open INTEGER NOT NULL DEFAULT 1
        );
        """)
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_slots_dt ON slots(d, t);")

        # appointments: записи клієнтів
        await db.execute("""
        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            service TEXT NOT NULL,
            subtype TEXT,
            d TEXT NOT NULL,
            t TEXT NOT NULL,
            slot_id INTEGER,
            status TEXT NOT NULL DEFAULT 'booked',
            reminded_24h INTEGER NOT NULL DEFAULT 0,
            reminded_2h INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        """)

        # clients: проста база клієнтів (останнє ім’я/телефон)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            user_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """)

        await db.commit()

    # Міграції (на випадок старих баз)
    await migrate_db()


async def column_exists(table: str, col: str) -> bool:
    rows = await db_fetchall(f"PRAGMA table_info({table});")
    cols = {r[1] for r in rows}
    return col in cols


async def migrate_db():
    # додати колонки, якщо їх нема (щоб не ловити no such column)
    if not await column_exists("slots", "is_open"):
        await db_exec("ALTER TABLE slots ADD COLUMN is_open INTEGER NOT NULL DEFAULT 1;")

    if not await column_exists("appointments", "reminded_24h"):
        await db_exec("ALTER TABLE appointments ADD COLUMN reminded_24h INTEGER NOT NULL DEFAULT 0;")

    if not await column_exists("appointments", "reminded_2h"):
        await db_exec("ALTER TABLE appointments ADD COLUMN reminded_2h INTEGER NOT NULL DEFAULT 0;")

    if not await column_exists("appointments", "status"):
        await db_exec("ALTER TABLE appointments ADD COLUMN status TEXT NOT NULL DEFAULT 'booked';")

    if not await column_exists("appointments", "created_at"):
        await db_exec("ALTER TABLE appointments ADD COLUMN created_at TEXT NOT NULL DEFAULT '';")

    if not await column_exists("appointments", "slot_id"):
        await db_exec("ALTER TABLE appointments ADD COLUMN slot_id INTEGER;")


# =========================
# CALENDAR UI
# =========================
def kb_month(y: int, m: int, prefix: str) -> InlineKeyboardMarkup:
    # prefix:
    #  - "u" для користувача
    #  - "a" для адміна
    # callback:
    #  - cal:{prefix}:{YYYY-MM}:{day}
    #  - calnav:{prefix}:{YYYY-MM}:{delta}
    cal = calendar.Calendar(firstweekday=0)
    month_weeks = cal.monthdayscalendar(y, m)

    title = f"{calendar.month_name[m]} {y}"
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=title, callback_data=f"noop")]
    ]

    # days row
    days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
    rows.append([InlineKeyboardButton(text=d, callback_data="noop") for d in days])

    ym = f"{y:04d}-{m:02d}"
    for week in month_weeks:
        line = []
        for day in week:
            if day == 0:
                line.append(InlineKeyboardButton(text=" ", callback_data="noop"))
            else:
                line.append(InlineKeyboardButton(text=str(day), callback_data=f"cal:{prefix}:{ym}:{day:02d}"))
        rows.append(line)

    py, pm = month_add(y, m, -1)
    ny, nm = month_add(y, m, +1)
    rows.append([
        InlineKeyboardButton(text="⬅️", callback_data=f"calnav:{prefix}:{ym}:-1"),
        InlineKeyboardButton(text="Сьогодні", callback_data=f"caltoday:{prefix}"),
        InlineKeyboardButton(text="➡️", callback_data=f"calnav:{prefix}:{ym}:1"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_user_services() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✨ Ламінування", callback_data="srv:lami")],
        [InlineKeyboardButton(text="💫 Нарощування", callback_data="srv:ext")],
        [InlineKeyboardButton(text="📅 Подивитись вільні слоти", callback_data="u:pick_date")],
    ])


def kb_ext_types() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=t, callback_data=f"ext:{t}")] for t in EXT_TYPES]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="u:back_services")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Додати/керувати слотами", callback_data="a:pick_date")],
        [InlineKeyboardButton(text="📋 Записи на дату", callback_data="a:view_apps_date")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="a:stats")],
    ])


# =========================
# FSM
# =========================
class Booking(StatesGroup):
    service = State()
    subtype = State()
    date = State()
    time = State()
    name = State()
    phone = State()
    confirm = State()


class AdminAddSlot(StatesGroup):
    date = State()
    time = State()


# =========================
# ROUTER
# =========================
router = Router()


# =========================
# START / HELP
# =========================
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()

    text = (
        "🤍 *Lashes Booking*\n\n"
        "Запис на процедуру в 2 кліки:\n"
        "• обери послугу\n"
        "• обери дату і час\n\n"
        "Після запису бот може нагадати за 24 год і за 2 год ✅"
    )

    # Спробуємо надіслати фото (локальне або URL)
    try:
        if START_PHOTO_URL:
            await message.answer_photo(START_PHOTO_URL, caption=text, parse_mode="Markdown", reply_markup=kb_user_services())
            return
        if os.path.exists(START_PHOTO_LOCAL):
            from aiogram.types import FSInputFile
            await message.answer_photo(FSInputFile(START_PHOTO_LOCAL), caption=text, parse_mode="Markdown", reply_markup=kb_user_services())
            return
    except Exception:
        pass

    await message.answer(text, parse_mode="Markdown", reply_markup=kb_user_services())


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin_username(message):
        await message.answer("⛔️ Доступ лише для адмінів.")
        return
    await message.answer("🔐 Адмін-панель", reply_markup=kb_admin_menu())


# =========================
# USER FLOW
# =========================
@router.callback_query(F.data == "u:back_services")
async def u_back_services(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_text_cq(cq, "Обери послугу:", kb_user_services())
    await cq.answer()


@router.callback_query(F.data.startswith("srv:"))
async def u_pick_service(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    key = cq.data.split(":")[1]
    if key == "lami":
        await state.update_data(service=SERVICE_LAMI, subtype=None)
        await state.set_state(Booking.date)
        today = date.today()
        await safe_edit_text_cq(cq, "📅 Обери дату запису:", kb_month(today.year, today.month, "u"))
    elif key == "ext":
        await state.update_data(service=SERVICE_EXT)
        await state.set_state(Booking.subtype)
        await safe_edit_text_cq(cq, "Оберіть тип нарощування:", kb_ext_types())


@router.callback_query(F.data.startswith("ext:"))
async def u_pick_ext_type(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    subtype = cq.data.split(":", 1)[1]
    await state.update_data(subtype=subtype)
    await state.set_state(Booking.date)
    today = date.today()
    await safe_edit_text_cq(cq, "📅 Обери дату запису:", kb_month(today.year, today.month, "u"))


@router.callback_query(F.data == "u:pick_date")
async def u_pick_date_only(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    # якщо просто дивитись слоти — service не обов'язково, але для запису потрібно
    data = await state.get_data()
    if not data.get("service"):
        # попросимо обрати послугу
        await safe_edit_text_cq(cq, "Спочатку обери послугу:", kb_user_services())
        return
    await state.set_state(Booking.date)
    today = date.today()
    await safe_edit_text_cq(cq, "📅 Обери дату запису:", kb_month(today.year, today.month, "u"))


@router.callback_query(F.data.startswith("calnav:"))
async def cal_nav(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    _, prefix, ym, delta = cq.data.split(":")
    y, m = map(int, ym.split("-"))
    dy = int(delta)
    ny, nm = month_add(y, m, dy)
    await safe_edit_text_cq(cq, "📅 Обери дату:", kb_month(ny, nm, prefix))


@router.callback_query(F.data.startswith("caltoday:"))
async def cal_today(cq: CallbackQuery):
    await cq.answer()
    _, prefix = cq.data.split(":")
    today = date.today()
    await safe_edit_text_cq(cq, "📅 Обери дату:", kb_month(today.year, today.month, prefix))


@router.callback_query(F.data.startswith("cal:"))
async def cal_pick_day(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    _, prefix, ym, day = cq.data.split(":")
    d = f"{ym}-{day}"
    if prefix == "u":
        await state.update_data(date=d)
        await state.set_state(Booking.time)
        await show_user_times(cq, state)
    else:
        # admin selected date
        await state.update_data(admin_date=d)
        await show_admin_day_menu(cq, d)


async def show_user_times(cq: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    d = data.get("date")
    if not d:
        await safe_edit_text_cq(cq, "Помилка: дата не вибрана. /start")
        return

    # Вільні слоти: is_open=1 і нема активного запису на цей час
    rows = await db_fetchall("""
        SELECT s.id, s.t
        FROM slots s
        LEFT JOIN appointments a
          ON a.d = s.d AND a.t = s.t AND a.status='booked'
        WHERE s.d = ? AND s.is_open = 1 AND a.id IS NULL
        ORDER BY s.t
    """, (d,))

    if not rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Інша дата", callback_data="u:pick_date")],
            [InlineKeyboardButton(text="🏠 Меню", callback_data="u:back_services")],
        ])
        await safe_edit_text_cq(cq, f"На {fmt_date(d)} немає вільних слотів 😔\nОбери іншу дату.", kb)
        return

    buttons = []
    for slot_id, t in rows:
        buttons.append([InlineKeyboardButton(text=f"🕒 {t}", callback_data=f"utime:{slot_id}")])

    buttons.append([InlineKeyboardButton(text="⬅️ Інша дата", callback_data="u:pick_date")])
    buttons.append([InlineKeyboardButton(text="🏠 Меню", callback_data="u:back_services")])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await safe_edit_text_cq(cq, f"✅ Вільний час на {fmt_date(d)}:", kb)


@router.callback_query(F.data.startswith("utime:"))
async def u_pick_time(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    slot_id = int(cq.data.split(":")[1])
    slot = await db_fetchone("SELECT d, t, is_open FROM slots WHERE id=?", (slot_id,))
    if not slot:
        await safe_edit_text_cq(cq, "Слот не знайдено. Спробуй ще раз.", kb_user_services())
        await state.clear()
        return

    d, t, is_open = slot
    if not is_open:
        await safe_edit_text_cq(cq, "Цей слот закритий. Обери інший.", None)
        await show_user_times(cq, state)
        return

    # перевір, що не зайнято
    exists = await db_fetchone("SELECT id FROM appointments WHERE d=? AND t=? AND status='booked' LIMIT 1", (d, t))
    if exists:
        await safe_edit_text_cq(cq, "Цей час вже зайнято. Обери інший.", None)
        await show_user_times(cq, state)
        return

    await state.update_data(slot_id=slot_id, time=t, date=d)

    # Спробуємо підтягнути клієнта з бази
    client = await db_fetchone("SELECT name, phone FROM clients WHERE user_id=?", (cq.from_user.id,))
    if client:
        name, phone = client
        await state.update_data(name=name, phone=phone)
        await state.set_state(Booking.confirm)
        await show_confirm(cq, state)
        return

    await state.set_state(Booking.name)
    await safe_edit_text_cq(cq, "Введи *ім'я* (як до тебе звертатись):", None)


@router.message(Booking.name)
async def u_enter_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("Напиши ім'я трохи довше 🙂")
        return
    await state.update_data(name=name)
    await state.set_state(Booking.phone)
    await message.answer("Тепер введи *номер телефону* (наприклад +380XXXXXXXXX):", parse_mode="Markdown")


@router.message(Booking.phone)
async def u_enter_phone(message: Message, state: FSMContext):
    phone = norm_phone(message.text or "")
    if not phone:
        await message.answer("Номер виглядає некоректно. Спробуй ще раз (наприклад +380XXXXXXXXX).")
        return
    await state.update_data(phone=phone)
    await state.set_state(Booking.confirm)
    await show_confirm(message, state)


async def show_confirm(obj: Message | CallbackQuery, state: FSMContext):
    data = await state.get_data()
    service = data.get("service")
    subtype = data.get("subtype")
    d = data.get("date")
    t = data.get("time")
    name = data.get("name")
    phone = data.get("phone")

    full_service = service if service != SERVICE_EXT else f"{service} • {subtype}"
    txt = (
        "✅ *Підтверди запис:*\n\n"
        f"👤 *Ім'я:* {name}\n"
        f"📞 *Телефон:* {phone}\n"
        f"✨ *Послуга:* {full_service}\n"
        f"📅 *Дата:* {fmt_date(d)}\n"
        f"🕒 *Час:* {t}\n"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Підтвердити", callback_data="u:confirm")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="u:cancel")],
    ])

    if isinstance(obj, Message):
        await obj.answer(txt, parse_mode="Markdown", reply_markup=kb)
    else:
        await safe_edit_text_cq(obj, txt, kb)


@router.callback_query(F.data == "u:cancel")
async def u_cancel(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    await state.clear()
    await safe_edit_text_cq(cq, "Скасовано. Обери послугу:", kb_user_services())


@router.callback_query(F.data == "u:confirm")
async def u_confirm(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    data = await state.get_data()
    user_id = cq.from_user.id
    name = data["name"]
    phone = data["phone"]
    service = data["service"]
    subtype = data.get("subtype")
    d = data["date"]
    t = data["time"]
    slot_id = data.get("slot_id")

    # перевір ще раз на зайнятість
    exists = await db_fetchone("SELECT id FROM appointments WHERE d=? AND t=? AND status='booked' LIMIT 1", (d, t))
    if exists:
        await safe_edit_text_cq(cq, "⛔️ Хтось вже зайняв цей час. Обери інший.", None)
        await state.set_state(Booking.time)
        await show_user_times(cq, state)
        return

    await db_exec("""
        INSERT INTO appointments (user_id, name, phone, service, subtype, d, t, slot_id, status, reminded_24h, reminded_2h, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'booked', 0, 0, ?)
    """, (user_id, name, phone, service, subtype, d, t, slot_id, now_str()))

    # оновимо clients
    await db_exec("""
        INSERT INTO clients (user_id, name, phone, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            name=excluded.name,
            phone=excluded.phone,
            updated_at=excluded.updated_at
    """, (user_id, name, phone, now_str()))

    full_service = service if service != SERVICE_EXT else f"{service} • {subtype}"
    await safe_edit_text_cq(
        cq,
        f"🎉 Запис підтверджено!\n\n✨ {full_service}\n📅 {fmt_date(d)} о {t}\n\n"
        "Нагадування прийде за 24 год та за 2 год ✅",
        kb_user_services()
    )
    await state.clear()


# =========================
# ADMIN FLOW
# =========================
async def show_admin_day_menu(cq: CallbackQuery, d: str):
    # меню дня: додати слот, показати слоти з toggle, показати записи
    rows = await db_fetchall("SELECT id, t, is_open FROM slots WHERE d=? ORDER BY t", (d,))
    apps = await db_fetchall("SELECT id, t, name, service, subtype FROM appointments WHERE d=? AND status='booked' ORDER BY t", (d,))

    text = f"🛠 Адмін • {fmt_date(d)}\n\n"
    text += f"Слотів: {len(rows)} | Записів: {len(apps)}\n"

    kb_rows: List[List[InlineKeyboardButton]] = []
    kb_rows.append([InlineKeyboardButton(text="➕ Додати слот (вручну)", callback_data=f"a:addslot:{d}")])
    kb_rows.append([InlineKeyboardButton(text="📋 Записи на цю дату", callback_data=f"a:apps:{d}")])

    if rows:
        kb_rows.append([InlineKeyboardButton(text="— СЛОТИ —", callback_data="noop")])
        for slot_id, t, is_open in rows:
            status = "🟢" if is_open else "🔴"
            kb_rows.append([
                InlineKeyboardButton(text=f"{status} {t}", callback_data=f"a:toggle:{slot_id}:{d}")
            ])
    else:
        text += "\nНемає слотів. Додай хоча б один."

    kb_rows.append([InlineKeyboardButton(text="⬅️ Інша дата", callback_data="a:pick_date")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Адмін-меню", callback_data="a:menu")])

    await safe_edit_text_cq(cq, text, InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data == "a:menu")
async def a_menu(cq: CallbackQuery):
    await cq.answer()
    if not is_admin_username(cq):
        await safe_edit_text_cq(cq, "⛔️ Доступ лише для адмінів.")
        return
    await safe_edit_text_cq(cq, "🔐 Адмін-панель", kb_admin_menu())


@router.callback_query(F.data == "a:pick_date")
async def a_pick_date(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    if not is_admin_username(cq):
        await safe_edit_text_cq(cq, "⛔️ Доступ лише для адмінів.")
        return
    today = date.today()
    await safe_edit_text_cq(cq, "📅 Обери дату (адмін):", kb_month(today.year, today.month, "a"))


@router.callback_query(F.data == "a:view_apps_date")
async def a_view_apps_date(cq: CallbackQuery):
    await cq.answer()
    if not is_admin_username(cq):
        await safe_edit_text_cq(cq, "⛔️ Доступ лише для адмінів.")
        return
    today = date.today()
    await safe_edit_text_cq(cq, "📅 Обери дату для перегляду записів:", kb_month(today.year, today.month, "a"))


@router.callback_query(F.data.startswith("a:apps:"))
async def a_apps_for_date(cq: CallbackQuery):
    await cq.answer()
    if not is_admin_username(cq):
        await safe_edit_text_cq(cq, "⛔️ Доступ лише для адмінів.")
        return

    d = cq.data.split(":", 2)[2]
    apps = await db_fetchall("""
        SELECT t, name, phone, service, COALESCE(subtype,'')
        FROM appointments
        WHERE d=? AND status='booked'
        ORDER BY t
    """, (d,))

    if not apps:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cal:a:{d[:7]}:{d[-2:]}")],
            [InlineKeyboardButton(text="🏠 Адмін-меню", callback_data="a:menu")]
        ])
        await safe_edit_text_cq(cq, f"📋 Записів на {fmt_date(d)} немає.", kb)
        return

    lines = [f"📋 Записи на {fmt_date(d)}:\n"]
    for t, name, phone, service, subtype in apps:
        srv = service if service != SERVICE_EXT else f"{service} • {subtype}"
        lines.append(f"• 🕒 {t} — {name} ({phone}) — {srv}")
    text = "\n".join(lines)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад до слотів", callback_data=f"a:day:{d}")],
        [InlineKeyboardButton(text="🏠 Адмін-меню", callback_data="a:menu")]
    ])
    await safe_edit_text_cq(cq, text, kb)


@router.callback_query(F.data.startswith("a:day:"))
async def a_day_back(cq: CallbackQuery):
    await cq.answer()
    d = cq.data.split(":", 2)[2]
    await show_admin_day_menu(cq, d)


@router.callback_query(F.data.startswith("a:toggle:"))
async def a_toggle_slot(cq: CallbackQuery):
    await cq.answer()
    if not is_admin_username(cq):
        await safe_edit_text_cq(cq, "⛔️ Доступ лише для адмінів.")
        return

    _, _, slot_id_s, d = cq.data.split(":")
    slot_id = int(slot_id_s)

    row = await db_fetchone("SELECT is_open, d, t FROM slots WHERE id=?", (slot_id,))
    if not row:
        await safe_edit_text_cq(cq, "Слот не знайдено.")
        return
    is_open, sd, st = row
    new_val = 0 if is_open else 1

    # якщо є запис — краще не закривати/відкривати бездумно, але дозволимо тільки закриття (не видаляємо запис)
    await db_exec("UPDATE slots SET is_open=? WHERE id=?", (new_val, slot_id))
    await show_admin_day_menu(cq, d)


@router.callback_query(F.data.startswith("a:addslot:"))
async def a_add_slot_start(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    if not is_admin_username(cq):
        await safe_edit_text_cq(cq, "⛔️ Доступ лише для адмінів.")
        return
    d = cq.data.split(":", 2)[2]
    await state.set_state(AdminAddSlot.time)
    await state.update_data(admin_slot_date=d)
    await safe_edit_text_cq(cq, f"Введи час слоту для {fmt_date(d)} у форматі *HH:MM* (наприклад 14:30):", None)


@router.message(AdminAddSlot.time)
async def a_add_slot_time(message: Message, state: FSMContext):
    if not is_admin_username(message):
        await message.answer("⛔️ Доступ лише для адмінів.")
        return

    t = (message.text or "").strip()
    if not re.fullmatch(r"\d{2}:\d{2}", t):
        await message.answer("Невірний формат. Приклад: 14:30")
        return

    hh, mm = map(int, t.split(":"))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        await message.answer("Невірний час. Приклад: 14:30")
        return

    data = await state.get_data()
    d = data.get("admin_slot_date")
    if not d:
        await message.answer("Помилка: дата не вибрана.")
        await state.clear()
        return

    # upsert слот
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO slots (d, t, is_open)
            VALUES (?, ?, 1)
            ON CONFLICT(d, t) DO UPDATE SET is_open=1
        """, (d, t))
        await db.commit()

    await message.answer(f"✅ Слот додано: {fmt_date(d)} {t}\n\nВведи наступний час (або напиши /admin щоб вийти).")


@router.callback_query(F.data == "a:stats")
async def a_stats(cq: CallbackQuery):
    await cq.answer()
    if not is_admin_username(cq):
        await safe_edit_text_cq(cq, "⛔️ Доступ лише для адмінів.")
        return

    # Загальна статистика по booked
    rows = await db_fetchall("""
        SELECT service, COALESCE(subtype,''), COUNT(*)
        FROM appointments
        WHERE status='booked'
        GROUP BY service, COALESCE(subtype,'')
        ORDER BY service, subtype
    """)
    if not rows:
        await safe_edit_text_cq(cq, "📊 Поки статистики немає (немає записів).", kb_admin_menu())
        return

    lines = ["📊 *Статистика (всі записи)*\n"]
    for service, subtype, cnt in rows:
        if service == SERVICE_EXT and subtype:
            lines.append(f"• {service} • {subtype}: *{cnt}*")
        else:
            lines.append(f"• {service}: *{cnt}*")
    await safe_edit_text_cq(cq, "\n".join(lines), kb_admin_menu())


# =========================
# NOOP
# =========================
@router.callback_query(F.data == "noop")
async def noop(cq: CallbackQuery):
    await cq.answer()


# =========================
# REMINDERS
# =========================
async def reminder_loop(bot: Bot):
    while True:
        try:
            now = datetime.now()

            rows = await db_fetchall("""
                SELECT id, user_id, service, COALESCE(subtype,''), d, t, reminded_24h, reminded_2h
                FROM appointments
                WHERE status='booked'
            """)

            for app_id, user_id, service, subtype, d, t, r24, r2 in rows:
                try:
                    app_dt = datetime.strptime(f"{d} {t}", "%Y-%m-%d %H:%M")
                except Exception:
                    continue

                diff = app_dt - now

                full_service = service if service != SERVICE_EXT else f"{service} • {subtype}"

                # 24 години (вікно 2 хв)
                if (timedelta(hours=23, minutes=59) <= diff <= timedelta(hours=24, minutes=1)) and (r24 == 0):
                    await bot.send_message(
                        user_id,
                        f"⏰ Нагадування\n\n"
                        f"У вас запис через 24 години:\n"
                        f"✨ {full_service}\n"
                        f"📅 {fmt_date(d)} о {t}"
                    )
                    await db_exec("UPDATE appointments SET reminded_24h=1 WHERE id=?", (app_id,))

                # 2 години (вікно 2 хв)
                if (timedelta(hours=1, minutes=59) <= diff <= timedelta(hours=2, minutes=1)) and (r2 == 0):
                    await bot.send_message(
                        user_id,
                        f"⏰ Нагадування\n\n"
                        f"У вас запис через 2 години:\n"
                        f"✨ {full_service}\n"
                        f"📅 {fmt_date(d)} о {t}"
                    )
                    await db_exec("UPDATE appointments SET reminded_2h=1 WHERE id=?", (app_id,))

        except Exception:
            # не валимо бота через нагадування
            pass

        await asyncio.sleep(60)


# =========================
# MAIN
# =========================
async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    await init_db()

    # Фонові нагадування
    asyncio.create_task(reminder_loop(bot))

    print("=== BOT STARTED (polling + reminders) ===", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
