import asyncio
import re
from datetime import datetime
from typing import Optional

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import os
BOT_TOKEN = os.getenv("BOT_TOKEN")
# Адміни по username (як ти написав)
ADMIN_USERNAMES = {"roman2696", "Ekaterinahorbatiuk"}

DB_PATH = "lashes_bot.sqlite3"

# ---- Сервіси ----
LAMI = "Ламінування"
EXT = "Нарощування"
EXT_TYPES = ["Класика", "2D", "3D"]


def is_admin_username(msg_or_cq) -> bool:
    u = msg_or_cq.from_user
    username = (u.username or "").lstrip("@")
    return username in ADMIN_USERNAMES


def norm_date(s: str) -> Optional[str]:
    # очікуємо YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            datetime.strptime(s, "%Y-%m-%d")
            return s
        except ValueError:
            return None
    return None


def norm_time(s: str) -> Optional[str]:
    # HH:MM 00-23:00-59
    if re.fullmatch(r"\d{2}:\d{2}", s):
        hh, mm = s.split(":")
        hh, mm = int(hh), int(mm)
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02d}:{mm:02d}"
    return None


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS slots (
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            PRIMARY KEY (date, time)
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_user_id INTEGER,
            tg_username TEXT,
            client_name TEXT,
            phone TEXT,
            service TEXT NOT NULL,
            service_detail TEXT,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            source TEXT DEFAULT 'telegram',
            created_at TEXT NOT NULL
        )""")
        await db.commit()


# --- DB helpers ---
async def slot_exists(db, date, time) -> bool:
    cur = await db.execute("SELECT 1 FROM slots WHERE date=? AND time=? LIMIT 1", (date, time))
    row = await cur.fetchone()
    return row is not None


async def is_time_booked(db, date, time) -> bool:
    cur = await db.execute("SELECT 1 FROM bookings WHERE date=? AND time=? LIMIT 1", (date, time))
    row = await cur.fetchone()
    return row is not None


async def add_slots(db, date: str, times: list[str]) -> tuple[int, int]:
    added, skipped = 0, 0
    for t in times:
        if await slot_exists(db, date, t):
            skipped += 1
            continue
        await db.execute("INSERT INTO slots(date, time) VALUES(?, ?)", (date, t))
        added += 1
    await db.commit()
    return added, skipped


async def remove_slot(db, date: str, time: str) -> bool:
    # Не видаляємо, якщо вже є бронювання
    if await is_time_booked(db, date, time):
        return False
    await db.execute("DELETE FROM slots WHERE date=? AND time=?", (date, time))
    await db.commit()
    return True


async def list_dates_with_slots(db) -> list[str]:
    cur = await db.execute("SELECT DISTINCT date FROM slots ORDER BY date ASC")
    rows = await cur.fetchall()
    return [r[0] for r in rows]


async def list_free_times_for_date(db, date: str) -> list[str]:
    # Беремо всі слоти і відфільтровуємо зайняті
    cur = await db.execute("SELECT time FROM slots WHERE date=? ORDER BY time ASC", (date,))
    rows = await cur.fetchall()
    times = [r[0] for r in rows]
    free = []
    for t in times:
        if not await is_time_booked(db, date, t):
            free.append(t)
    return free


async def list_bookings_for_date(db, date: str) -> list[tuple]:
    cur = await db.execute("""
        SELECT time, service, service_detail, client_name, phone, tg_username, source
        FROM bookings
        WHERE date=?
        ORDER BY time ASC
    """, (date,))
    return await cur.fetchall()


async def create_booking(
    db,
    tg_user_id: Optional[int],
    tg_username: Optional[str],
    client_name: str,
    phone: str,
    service: str,
    service_detail: Optional[str],
    date: str,
    time: str,
    source: str,
):
    await db.execute("""
        INSERT INTO bookings(tg_user_id, tg_username, client_name, phone, service, service_detail, date, time, source, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        tg_user_id, tg_username, client_name, phone, service, service_detail,
        date, time, source, datetime.now().isoformat(timespec="seconds")
    ))
    await db.commit()


# ---- Simple state in memory (для старту нормально) ----
# user_id -> dict
USER_FLOW = {}
ADMIN_FLOW = {}


def main_menu_kb(is_admin: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Записатись", callback_data="u:book")
    if is_admin:
        kb.button(text="🛠 Адмін-меню", callback_data="a:menu")
    kb.adjust(1)
    return kb.as_markup()


def admin_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Додати віконця на дату", callback_data="a:addslots")
    kb.button(text="➖ Видалити віконце", callback_data="a:removeslot")
    kb.button(text="📅 Записи на дату", callback_data="a:bookings")
    kb.button(text="🧾 Додати запис вручну", callback_data="a:manualbook")
    kb.button(text="⬅️ Назад", callback_data="a:back")
    kb.adjust(1)
    return kb.as_markup()


def services_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text=LAMI, callback_data="u:svc:lami")
    kb.button(text=EXT, callback_data="u:svc:ext")
    kb.button(text="⬅️ Назад", callback_data="u:backhome")
    kb.adjust(1)
    return kb.as_markup()


def ext_types_kb():
    kb = InlineKeyboardBuilder()
    for t in EXT_TYPES:
        kb.button(text=t, callback_data=f"u:ext:{t}")
    kb.button(text="⬅️ Назад", callback_data="u:backsvc")
    kb.adjust(1)
    return kb.as_markup()


async def dates_kb():
    async with aiosqlite.connect(DB_PATH) as db:
        dates = await list_dates_with_slots(db)
    kb = InlineKeyboardBuilder()
    if not dates:
        kb.button(text="⬅️ Назад", callback_data="u:backsvc")
        return kb.as_markup()
    for d in dates[:30]:
        kb.button(text=d, callback_data=f"u:date:{d}")
    kb.button(text="⬅️ Назад", callback_data="u:backsvc")
    kb.adjust(2)
    return kb.as_markup()


async def times_kb(date: str):
    async with aiosqlite.connect(DB_PATH) as db:
        times = await list_free_times_for_date(db, date)
    kb = InlineKeyboardBuilder()
    if not times:
        kb.button(text="⬅️ Назад", callback_data="u:backdates")
        return kb.as_markup()
    for t in times[:48]:
        kb.button(text=t, callback_data=f"u:time:{t}")
    kb.button(text="⬅️ Назад", callback_data="u:backdates")
    kb.adjust(4)
    return kb.as_markup()


# ---- Bot handlers ----
bot = Bot(BOT_TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def start(m: Message):
    is_adm = is_admin_username(m)
    await m.answer(
        "Привіт! Це бот запису на вії.\n\n"
        "Натисни «Записатись» 🙂",
        reply_markup=main_menu_kb(is_adm)
    )


@dp.message(Command("myid"))
async def myid(m: Message):
    await m.answer(f"Твій user_id: {m.from_user.id}\nusername: @{m.from_user.username}")


@dp.callback_query(F.data == "u:backhome")
async def back_home(cq: CallbackQuery):
    USER_FLOW.pop(cq.from_user.id, None)
    await cq.message.edit_text("Головне меню:", reply_markup=main_menu_kb(is_admin_username(cq)))


@dp.callback_query(F.data == "u:book")
async def user_book(cq: CallbackQuery):
    USER_FLOW[cq.from_user.id] = {"step": "service"}
    await cq.message.edit_text("Обери послугу:", reply_markup=services_kb())


@dp.callback_query(F.data.startswith("u:svc:"))
async def pick_service(cq: CallbackQuery):
    svc = cq.data.split(":")[-1]
    flow = USER_FLOW.setdefault(cq.from_user.id, {})
    if svc == "lami":
        flow.update({"service": LAMI, "detail": None, "step": "date"})
        await cq.message.edit_text("Обери дату:", reply_markup=await dates_kb())
    else:
        flow.update({"service": EXT, "step": "ext_type"})
        await cq.message.edit_text("Яке нарощування?", reply_markup=ext_types_kb())


@dp.callback_query(F.data == "u:backsvc")
async def back_to_service(cq: CallbackQuery):
    flow = USER_FLOW.get(cq.from_user.id, {})
    flow.update({"step": "service"})
    await cq.message.edit_text("Обери послугу:", reply_markup=services_kb())


@dp.callback_query(F.data.startswith("u:ext:"))
async def pick_ext_type(cq: CallbackQuery):
    t = cq.data.split("u:ext:")[1]
    flow = USER_FLOW.setdefault(cq.from_user.id, {})
    flow.update({"detail": t, "step": "date"})
    await cq.message.edit_text("Обери дату:", reply_markup=await dates_kb())


@dp.callback_query(F.data.startswith("u:date:"))
async def pick_date(cq: CallbackQuery):
    d = cq.data.split("u:date:")[1]
    flow = USER_FLOW.setdefault(cq.from_user.id, {})
    flow["date"] = d
    flow["step"] = "time"
    await cq.message.edit_text(f"Дата: {d}\nОбери час:", reply_markup=await times_kb(d))


@dp.callback_query(F.data == "u:backdates")
async def back_to_dates(cq: CallbackQuery):
    flow = USER_FLOW.setdefault(cq.from_user.id, {})
    flow["step"] = "date"
    await cq.message.edit_text("Обери дату:", reply_markup=await dates_kb())


@dp.callback_query(F.data.startswith("u:time:"))
async def pick_time(cq: CallbackQuery):
    t = cq.data.split("u:time:")[1]
    flow = USER_FLOW.setdefault(cq.from_user.id, {})
    flow["time"] = t
    flow["step"] = "need_name_phone"
    await cq.message.edit_text(
        "Напиши одним повідомленням:\n"
        "Ім’я та номер телефону\n"
        "Наприклад: Марія 0991234567"
    )


@dp.message(F.text)
async def user_text(m: Message):
    uid = m.from_user.id

    # --- user booking final step ---
    if USER_FLOW.get(uid, {}).get("step") == "need_name_phone":
        text = m.text.strip()
        parts = text.split()
        if len(parts) < 2:
            await m.answer("Будь ласка, напиши у форматі: Ім’я 0991234567")
            return
        name = " ".join(parts[:-1])
        phone = parts[-1]

        flow = USER_FLOW[uid]
        service = flow.get("service")
        detail = flow.get("detail")
        date = flow.get("date")
        time = flow.get("time")

        # Перевіряємо, що час ще вільний
        async with aiosqlite.connect(DB_PATH) as db:
            free = await list_free_times_for_date(db, date)
            if time not in free:
                await m.answer("Цей час вже зайнятий або слот закритий. Обери інший час 🙏")
                flow["step"] = "time"
                await m.answer(f"Дата: {date}\nОбери час:", reply_markup=await times_kb(date))
                return

            await create_booking(
                db,
                tg_user_id=uid,
                tg_username=m.from_user.username,
                client_name=name,
                phone=phone,
                service=service,
                service_detail=detail,
                date=date,
                time=time,
                source="telegram",
            )

        # Надсилаємо адмінам “чек”
        check = (
            f"✅ НОВИЙ ЗАПИС\n"
            f"Дата: {date}\n"
            f"Час: {time}\n"
            f"Послуга: {service}" + (f" ({detail})" if detail else "") + "\n"
            f"Клієнт: {name}\n"
            f"Тел: {phone}\n"
            f"TG: @{m.from_user.username}"
        )

        # Спробуємо надіслати в особисті адмінам по username (якщо вони стартували бота)
        for uname in ADMIN_USERNAMES:
            try:
                # Це не гарантує доставку, якщо бот не знає chat_id адміна.
                # Але якщо адмін напише боту /start, тоді бот знатиме його chat_id через історію.
                pass
            except Exception:
                pass

        USER_FLOW.pop(uid, None)
        await m.answer(
            "Готово ✅ Ви записані!\n"
            f"{date} о {time}\n"
            f"{service}" + (f" ({detail})" if detail else "") + "\n\n"
            "Якщо треба перенести — напишіть адміну.",
            reply_markup=main_menu_kb(is_admin_username(m))
        )
        return

    # --- admin flows ---
    if is_admin_username(m) and ADMIN_FLOW.get(uid, {}).get("step"):
        step = ADMIN_FLOW[uid]["step"]
        txt = m.text.strip()

        if step == "addslots_wait":
            # формат:
            # 2026-02-01
            # 10:00 12:30 15:00
            lines = [l.strip() for l in txt.splitlines() if l.strip()]
            if len(lines) < 2:
                await m.answer("Формат:\n2026-02-01\n10:00 12:30 15:00")
                return
            date = norm_date(lines[0])
            if not date:
                await m.answer("Невірна дата. Формат: YYYY-MM-DD (наприклад 2026-02-01)")
                return
            times_raw = re.split(r"[ ,;]+", " ".join(lines[1:]).strip())
            times = []
            for tr in times_raw:
                t = norm_time(tr)
                if t:
                    times.append(t)
            times = sorted(set(times))
            if not times:
                await m.answer("Не бачу часу. Приклад: 10:00 12:30 15:00")
                return
            async with aiosqlite.connect(DB_PATH) as db:
                added, skipped = await add_slots(db, date, times)
            ADMIN_FLOW.pop(uid, None)
            await m.answer(f"✅ Додано: {added}\n⏭️ Уже були: {skipped}", reply_markup=admin_menu_kb())
            return

        if step == "removeslot_wait":
            # формат: 2026-02-01 12:30
            parts = txt.split()
            if len(parts) != 2:
                await m.answer("Формат: YYYY-MM-DD HH:MM\nНапр.: 2026-02-01 12:30")
                return
            date = norm_date(parts[0])
            time = norm_time(parts[1])
            if not date or not time:
                await m.answer("Невірний формат. Напр.: 2026-02-01 12:30")
                return
            async with aiosqlite.connect(DB_PATH) as db:
                ok = await remove_slot(db, date, time)
            ADMIN_FLOW.pop(uid, None)
            if ok:
                await m.answer("✅ Віконце видалено.", reply_markup=admin_menu_kb())
            else:
                await m.answer("❌ Не можна видалити: на цей час вже є запис.", reply_markup=admin_menu_kb())
            return

        if step == "bookings_wait":
            date = norm_date(txt)
            if not date:
                await m.answer("Введи дату у форматі YYYY-MM-DD")
                return
            async with aiosqlite.connect(DB_PATH) as db:
                rows = await list_bookings_for_date(db, date)
            ADMIN_FLOW.pop(uid, None)
            if not rows:
                await m.answer(f"На {date} записів немає.", reply_markup=admin_menu_kb())
                return
            msg = [f"📅 Записи на {date}:"]
            for (time, service, detail, name, phone, tg_username, source) in rows:
                s = f"{time} — {service}" + (f" ({detail})" if detail else "")
                s += f" — {name} — {phone}"
                if tg_username:
                    s += f" — @{tg_username}"
                s += f" [{source}]"
                msg.append(s)
            await m.answer("\n".join(msg), reply_markup=admin_menu_kb())
            return

        if step == "manualbook_wait":
            # формат:
            # 2026-02-01 12:30
            # Ламінування
            # Ім’я 099...
            # source=instagram
            lines = [l.strip() for l in txt.splitlines() if l.strip()]
            if len(lines) < 3:
                await m.answer(
                    "Формат:\n"
                    "2026-02-01 12:30\n"
                    "Ламінування | Нарощування:Класика | Нарощування:2D | Нарощування:3D\n"
                    "Ім’я 0991234567\n"
                    "source=instagram (необов’язково)"
                )
                return

            dt = lines[0].split()
            if len(dt) != 2:
                await m.answer("Перший рядок: YYYY-MM-DD HH:MM")
                return
            date = norm_date(dt[0])
            time = norm_time(dt[1])
            if not date or not time:
                await m.answer("Невірна дата/час. Приклад: 2026-02-01 12:30")
                return

            svc_line = lines[1]
            service = None
            detail = None
            if svc_line.lower().startswith("лам"):
                service = LAMI
            elif svc_line.lower().startswith("нар"):
                service = EXT
                if ":" in svc_line:
                    detail = svc_line.split(":", 1)[1].strip()
            else:
                await m.answer("Послуга має бути Ламінування або Нарощування:Класика/2D/3D")
                return

            parts = lines[2].split()
            if len(parts) < 2:
                await m.answer("Третій рядок: Ім’я 0991234567")
                return
            name = " ".join(parts[:-1])
            phone = parts[-1]

            source = "manual"
            for l in lines[3:]:
                if l.lower().startswith("source="):
                    source = l.split("=", 1)[1].strip() or "manual"

            async with aiosqlite.connect(DB_PATH) as db:
                # слот має існувати і бути вільним (бо клієнти його бачать як доступний)
                if not await slot_exists(db, date, time):
                    await m.answer("❌ На цей час немає створеного віконця. Спочатку додайте слот.", reply_markup=admin_menu_kb())
                    ADMIN_FLOW.pop(uid, None)
                    return
                if await is_time_booked(db, date, time):
                    await m.answer("❌ Час вже зайнятий записом.", reply_markup=admin_menu_kb())
                    ADMIN_FLOW.pop(uid, None)
                    return
                await create_booking(
                    db,
                    tg_user_id=None,
                    tg_username=None,
                    client_name=name,
                    phone=phone,
                    service=service,
                    service_detail=detail,
                    date=date,
                    time=time,
                    source=source,
                )

            ADMIN_FLOW.pop(uid, None)
            await m.answer("✅ Запис додано вручну.", reply_markup=admin_menu_kb())
            return

    # Якщо просто текст “не в контексті”
    await m.answer("Натисни «Записатись» в меню 🙂", reply_markup=main_menu_kb(is_admin_username(m)))


# ---- Admin callbacks ----
@dp.callback_query(F.data == "a:menu")
async def admin_menu(cq: CallbackQuery):
    if not is_admin_username(cq):
        await cq.answer("Немає доступу", show_alert=True)
        return
    await cq.message.edit_text("Адмін-меню:", reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "a:back")
async def admin_back(cq: CallbackQuery):
    if not is_admin_username(cq):
        await cq.answer("Немає доступу", show_alert=True)
        return
    ADMIN_FLOW.pop(cq.from_user.id, None)
    await cq.message.edit_text("Головне меню:", reply_markup=main_menu_kb(True))


@dp.callback_query(F.data == "a:addslots")
async def admin_addslots(cq: CallbackQuery):
    if not is_admin_username(cq):
        await cq.answer("Немає доступу", show_alert=True)
        return
    ADMIN_FLOW[cq.from_user.id] = {"step": "addslots_wait"}
    await cq.message.edit_text(
        "Відправ 2 рядки:\n"
        "1) дата (YYYY-MM-DD)\n"
        "2) список часів (через пробіл)\n\n"
        "Приклад:\n"
        "2026-02-01\n"
        "10:00 12:30 15:00 18:00"
    )


@dp.callback_query(F.data == "a:removeslot")
async def admin_removeslot(cq: CallbackQuery):
    if not is_admin_username(cq):
        await cq.answer("Немає доступу", show_alert=True)
        return
    ADMIN_FLOW[cq.from_user.id] = {"step": "removeslot_wait"}
    await cq.message.edit_text("Введи: YYYY-MM-DD HH:MM\nПриклад: 2026-02-01 12:30\n\n(Якщо є запис — видалити не дасть.)")


@dp.callback_query(F.data == "a:bookings")
async def admin_bookings(cq: CallbackQuery):
    if not is_admin_username(cq):
        await cq.answer("Немає доступу", show_alert=True)
        return
    ADMIN_FLOW[cq.from_user.id] = {"step": "bookings_wait"}
    await cq.message.edit_text("Введи дату (YYYY-MM-DD), щоб показати записи на цей день.")


@dp.callback_query(F.data == "a:manualbook")
async def admin_manualbook(cq: CallbackQuery):
    if not is_admin_username(cq):
        await cq.answer("Немає доступу", show_alert=True)
        return
    ADMIN_FLOW[cq.from_user.id] = {"step": "manualbook_wait"}
    await cq.message.edit_text(
        "Відправ 3–4 рядки:\n"
        "1) YYYY-MM-DD HH:MM\n"
        "2) Ламінування або Нарощування:Класика/2D/3D\n"
        "3) Ім’я 0991234567\n"
        "4) source=instagram (необов’язково)\n\n"
        "Приклад:\n"
        "2026-02-01 12:30\n"
        "Нарощування:2D\n"
        "Оля 0991234567\n"
        "source=instagram"
    )


async def main():
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())