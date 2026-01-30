import asyncio
import os
import re
from typing import Optional
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

import aiosqlite

# ===== TOKEN =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ===== TEST /start =====
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("✅ Бот онлайн на Railway. /start працює!")

# ===== ADMINS =====
ADMIN_USERNAMES = {
    "roman2696",
    "Ekaterinahorbatiuk"
}

# ===== DATABASE =====
DB_PATH = "lashes_bot.sqlite3"

# ===== SERVICES =====
LAMI = "Ламінування"
EXT = "Нарощування"
EXT_TYPES = ["Класика", "2D", "3D"]


def is_admin_username(msg_or_cq) -> bool:
    user = msg_or_cq.from_user
    username = (user.username or "").lstrip("@")
    return username in ADMIN_USERNAMES


def norm_date(s: str) -> Optional[str]:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    return None


# ===== START BOT =====
async def main():
    print("=== START POLLING ===", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
