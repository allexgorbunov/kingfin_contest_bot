import os
import asyncio
import logging
import random
from aiohttp import web
import psycopg2
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO)
DATABASE_URL = os.getenv('DATABASE_URL')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID', 0))
BASE_URL = os.getenv('BASE_URL', 'https://your-service.onrender.com')

app = web.Application()
lock = asyncio.Lock()

async def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

async def init_db():
    conn = await get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS participants (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            number TEXT UNIQUE NOT NULL
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введите вашу email для участия. Получите уникальный номер!")

async def handle_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    email = update.message.text.strip().lower()
    if '@' not in email:
        await update.message.reply_text("Неверный email. Попробуйте снова.")
        return
    async with lock:
        conn = await get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM participants WHERE email = %s", (email,))
        if cur.fetchone():
            await update.message.reply_text("Вы уже участвуете! Ваш номер сохранён.")
            cur.close()
            conn.close()
            return
        max_id_res = cur.execute("SELECT MAX(id) FROM participants")
        max_id = cur.fetchone()[0] or 0
        number = f"USER{ max_id + 1:03d}"
        cur.execute("INSERT INTO participants (email, number) VALUES (%s, %s)", (email, number))
        conn.commit()
        cur.close()
        conn.close()
    await update.message.reply_text(f"Спасибо! Ваш уникальный номер: {number}\nЖдите результатов розыгрыша.")

async def raffle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Доступ запрещён.")
        return
    async with lock:
        conn = await get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT number FROM participants ORDER BY id")
        numbers = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    if not numbers:
        await update.message.reply_text("Нет участников.")
        return
    winner = random.choice(numbers)
    await update.message.reply_text(f"Победитель: {winner}!")

async def export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    conn = await get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT number, email FROM participants ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if rows:
        text = "\n".join([f"{num} - {email}" for num, email in rows])
        await update.message.reply_text(text[:4096])  # Telegram limit
    else:
        await update.message.reply_text("Список пуст.")

async def webhook(request):
    update = Update.de_json(await request.json(), app.bot)
    await app.process_update(update)
    return web.Response()

def create_app():
    global app
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_email))
    application.add_handler(CommandHandler("raffle", raffle))
    application.add_handler(CommandHandler("export", export))
    app.bot = application.bot
    app.application = application
    web_app = web.Application()
    web_app.router.add_post(f'/{TELEGRAM_TOKEN}', webhook)
    return web_app

async def main():
    await init_db()
    web_app = create_app()
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 10000)
    await site.start()
    print("Bot started!")

if __name__ == "__main__":
    asyncio.run(main())
