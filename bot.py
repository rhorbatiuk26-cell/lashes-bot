import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in Railway Variables")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

def main_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Записатись", callback_data="menu:book")
    kb.button(text="📋 Мої записи", callback_data="menu:mine")
    kb.adjust(1)
    return kb.as_markup()

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
        reply_markup=main_menu_kb()
    )

@dp.message(Command("whoami"))
async def cmd_whoami(message: Message):
    await message.answer(f"Ваш user_id: {message.from_user.id}")

async def main():
    print("=== START POLLING ===", flush=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

