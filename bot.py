import asyncio
import re
import os
from datetime import datetime
from typing import Optional

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder


print("=== BOT STARTING ===", flush=True)

# --- TOKEN ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
print("BOT_TOKEN exists:", bool(BOT_TOKEN), flush=True)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Add it in Railway Variables.")

# --- BOT / DISPATCHER ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- ADMINS (usernames without @) ---
ADMIN_USERNAMES = {
    "roman2696",
    "Ekaterinahorbatiuk"
}

# --- DATABASE ---
DB_PATH = "lashes_bot.sqlite3"

# --- SERVICES ---
LAMI = "Ламінування"
EXT = "Нарощування"
EXT_TYPES = ["Класика", "2D", "3D"]


def is_admin_username(msg_or_cq) -> bool:
    user = msg_or_cq.from_user
    username = (user.username or "").lstrip("@")
    return username in ADMIN_USERNAMES


def norm_date(s: str) -> Optional[str]:
    # очікуємо YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    return None
