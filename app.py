import os
import asyncio
import logging
import random

from aiohttp import web
import psycopg2
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(level=logging.INFO)

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
BASE_URL = os.getenv("BASE_URL", "https://your-service.onrender.com")

lock = asyncio.Lock()


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


async def init_db():
    # создаём таблицу участников, если её ещё нет
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS participants (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            number TEXT UNIQUE NOT NULL
        );
        """
    )
    conn.commit()
    cur.close()
    conn.close()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # стартовое сообщение пользователю
    await update.message.reply_text(
        "Welcome to the giveaway!\n"
        "Send your email to participate and get a unique number like 001, 002, 003."
    )


async def handle_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # обработка любого текста как email
    email = update.message.text.strip().lower()

    if "@" not in email:
        await update.message.reply_text("This doesn't look like a valid email. Please try again.")
        return

    async with lock:
        conn = get_db_connection()
        cur = conn.cursor()

        # если уже участвует — просто показываем ранее выданный номер
        cur.execute("SELECT number FROM participants WHERE email = %s", (email,))
        row = cur.fetchone()
        if row:
            number = row[0]
            cur.close()
            conn.close()
            await update.message.reply_text(
                f"You are already participating in the giveaway.\n"
                f"Your number: {number}"
            )
            return

        # берём максимальный id и даём следующий номер в формате 001, 002...
        cur.execute("SELECT MAX(id) FROM participants;")
        max_id = cur.fetchone()[0] or 0
        next_id = max_id + 1
        number = f"{next_id:03d}"  # 001, 002, 010, 123

        cur.execute(
            "INSERT INTO participants (email, number) VALUES (%s, %s)",
            (email, number),
        )
        conn.commit()
        cur.close()
        conn.close()

    await update.message.reply_text(
        f"Thank you! You are now in the giveaway.\n"
        f"Your unique number: {number}"
    )


async def raffle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # розыгрыш — доступен только админу
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("You are not allowed to run the raffle.")
        return

    async with lock:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT number FROM participants ORDER BY id;")
        numbers = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

    if not numbers:
        await update.message.reply_text("There are no participants yet.")
        return

    winner = random.choice(numbers)
    await update.message.reply_text(f"The winner is number {winner}!")


async def export_participants(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # экспорт списка участников — только для админа, текстом
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("This command is only available to the admin.")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT number, email FROM participants ORDER BY id;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        await update.message.reply_text("The participants list is empty.")
        return

    # текстовый вывод вида:
    # 001 - mail1
    # 002 - mail2
    lines = [f"{number} - {email}" for number, email in rows]
    text = "Participants list:\n" + "\n".join(lines)

    # если текст очень длинный — режем на части по ~4000 символов
    chunk_size = 4000
    for i in range(0, len(text), chunk_size):
        await update.message.reply_text(text[i : i + chunk_size])


async def handle_webhook(request: web.Request):
    # вебхук — приём апдейтов от Telegram
    from telegram import Update as TgUpdate

    data = await request.json()
    update = TgUpdate.de_json(data, request.app["bot"])
    await request.app["application"].process_update(update)
    return web.Response(text="OK")


async def on_startup(app_: web.Application):
    # инициализация БД и запуск Telegram-приложения
    await init_db()

    application = app_["application"]
    await application.initialize()
    await application.start()

    # настройка webhook
    webhook_url = f"{BASE_URL}/{TELEGRAM_TOKEN}"
    await application.bot.set_webhook(webhook_url)
    logging.info(f"Webhook set to {webhook_url}")


async def on_cleanup(app_: web.Application):
    # корректная остановка Telegram-приложения
    application = app_["application"]
    await application.stop()
    await application.shutdown()


def create_web_app() -> web.Application:
    # сборка Telegram Application и aiohttp-приложения
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_email)
    )
    application.add_handler(CommandHandler("raffle", raffle))
    application.add_handler(CommandHandler("export", export_participants))

    web_app = web.Application()
    web_app["application"] = application
    web_app["bot"] = application.bot

    web_app.router.add_post(f"/{TELEGRAM_TOKEN}", handle_webhook)
    web_app.on_startup.append(on_startup)
    web_app.on_cleanup.append(on_cleanup)

    return web_app


async def main():
    # запуск aiohttp-сервера на Render
    web_app = create_web_app()
    runner = web.AppRunner(web_app)
    await runner.setup()
    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Server started on port {port}")
    # держим процесс живым
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
