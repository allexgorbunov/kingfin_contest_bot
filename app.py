import os
import asyncio
import logging
import random
from difflib import SequenceMatcher

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
    # подключение к Postgres на Render
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
            number TEXT UNIQUE NOT NULL,
            chat_id BIGINT NOT NULL
        );
        """
    )
    # Добавляем колонку chat_id, если её ещё нет (для существующих БД)
    cur.execute(
        """
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='participants' AND column_name='chat_id'
            ) THEN
                ALTER TABLE participants ADD COLUMN chat_id BIGINT;
            END IF;
        END $$;
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


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # помощь — команды только для админа
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        # Обычные пользователи не видят команды админа
        await update.message.reply_text(
            "Welcome to the giveaway!\n"
            "Send your email to participate and get a unique number."
        )
        return

    # Список команд для админа
    help_text = (
        "🔧 Admin Commands:\n\n"
        "📋 Management:\n"
        "• /list - Show all participants with IDs\n"
        "• /export - Export participants list\n"
        "• /check_duplicates - Find suspicious duplicate emails\n"
        "• /remove <number> - Remove participant by number\n"
        "• /remove <email> - Remove participant by email\n\n"
        "🎲 Raffle:\n"
        "• /raffle - Run the giveaway and select a winner\n\n"
        "🔄 Reset:\n"
        "• /reset - Clear all participants and reset counter\n\n"
        "ℹ️ Info:\n"
        "• /start - Start message\n"
        "• /help - Show this help"
    )
    
    await update.message.reply_text(help_text)


async def handle_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # обработка любого текста как email
    email = update.message.text.strip().lower()

    if "@" not in email:
        await update.message.reply_text(
            "This doesn't look like a valid email. Please try again."
        )
        return

    async with lock:
        conn = get_db_connection()
        cur = conn.cursor()
        chat_id = update.effective_chat.id

        # если уже участвует — просто показываем ранее выданный номер
        cur.execute("SELECT number FROM participants WHERE email = %s", (email,))
        row = cur.fetchone()
        if row:
            number = row[0]
            # Обновляем chat_id на случай, если пользователь пишет из другого чата
            cur.execute(
                "UPDATE participants SET chat_id = %s WHERE email = %s",
                (chat_id, email),
            )
            conn.commit()
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
            "INSERT INTO participants (email, number, chat_id) VALUES (%s, %s, %s)",
            (email, number, chat_id),
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
        cur.execute("SELECT number, chat_id FROM participants ORDER BY id;")
        participants = cur.fetchall()
        cur.close()
        conn.close()

    if not participants:
        await update.message.reply_text("There are no participants yet.")
        return

    winner_data = random.choice(participants)
    winner_number = winner_data[0]
    winner_chat_id = winner_data[1]

    # Отправляем уведомление админу
    await update.message.reply_text(f"The winner is number {winner_number}!")

    # Отправляем уведомление победителю
    try:
        application = context.application
        await application.bot.send_message(
            chat_id=winner_chat_id,
            text=f"🎉 Congratulations! You won the giveaway!\n"
                 f"Your winning number: {winner_number}\n"
                 f"Please contact the administrator to claim your prize."
        )
    except Exception as e:
        logging.error(f"Failed to send message to winner {winner_chat_id}: {e}")
        await update.message.reply_text(
            f"Winner selected: {winner_number}, but failed to send notification. "
            f"Error: {str(e)}"
        )


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


async def reset_participants(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # полный сброс таблицы participants с обнулением ID
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("This command is only available to the admin.")
        return

    async with lock:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE participants RESTART IDENTITY;")
        conn.commit()
        cur.close()
        conn.close()

    await update.message.reply_text(
        "All participants have been removed.\n"
        "ID counter has been reset. New participants will start from 001 again."
    )


def similarity(a: str, b: str) -> float:
    """Вычисляет схожесть двух строк (0.0 - 1.0)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


async def check_duplicates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # проверка на похожие email-адреса (дубликаты)
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("This command is only available to the admin.")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, number, email FROM participants ORDER BY id;")
    participants = cur.fetchall()
    cur.close()
    conn.close()

    if len(participants) < 2:
        await update.message.reply_text("Not enough participants to check for duplicates.")
        return

    # Ищем похожие email-адреса
    duplicates_found = []
    threshold = 0.7  # порог схожести (70%)

    for i, (id1, num1, email1) in enumerate(participants):
        # Извлекаем локальную часть email (до @)
        local1 = email1.split("@")[0] if "@" in email1 else email1
        
        for j, (id2, num2, email2) in enumerate(participants[i + 1:], start=i + 1):
            local2 = email2.split("@")[0] if "@" in email2 else email2
            
            # Проверяем схожесть локальных частей
            sim = similarity(local1, local2)
            
            # Также проверяем одинаковый домен
            domain1 = email1.split("@")[1] if "@" in email1 else ""
            domain2 = email2.split("@")[1] if "@" in email2 else ""
            
            if sim >= threshold or (domain1 == domain2 and sim >= 0.5):
                duplicates_found.append({
                    "id1": id1, "num1": num1, "email1": email1,
                    "id2": id2, "num2": num2, "email2": email2,
                    "similarity": sim
                })

    if not duplicates_found:
        await update.message.reply_text("No suspicious duplicates found. ✅")
        return

    # Формируем сообщение с найденными дубликатами
    message = "⚠️ Suspicious duplicates found:\n\n"
    for dup in duplicates_found:
        message += (
            f"• {dup['num1']} ({dup['email1']})\n"
            f"  {dup['num2']} ({dup['email2']})\n"
            f"  Similarity: {dup['similarity']:.1%}\n\n"
        )
    
    message += f"Use /remove <number> to delete a participant."

    # Разбиваем на части, если сообщение слишком длинное
    chunk_size = 4000
    for i in range(0, len(message), chunk_size):
        await update.message.reply_text(message[i : i + chunk_size])


async def remove_participant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # удаление участника по номеру или email
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("This command is only available to the admin.")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: /remove <number> or /remove <email>\n"
            "Example: /remove 001 or /remove user@example.com"
        )
        return

    identifier = " ".join(context.args).strip().lower()

    async with lock:
        conn = get_db_connection()
        cur = conn.cursor()

        # Пытаемся найти по номеру или email
        cur.execute(
            "SELECT id, number, email FROM participants WHERE number = %s OR LOWER(email) = %s",
            (identifier, identifier)
        )
        participant = cur.fetchone()

        if not participant:
            cur.close()
            conn.close()
            await update.message.reply_text(
                f"Participant not found: {identifier}\n"
                f"Use /export to see the list of participants."
            )
            return

        part_id, part_number, part_email = participant

        # Удаляем участника
        cur.execute("DELETE FROM participants WHERE id = %s", (part_id,))
        conn.commit()
        cur.close()
        conn.close()

    await update.message.reply_text(
        f"✅ Participant removed:\n"
        f"Number: {part_number}\n"
        f"Email: {part_email}"
    )


async def list_participants(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # список участников с ID для удобства удаления
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("This command is only available to the admin.")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, number, email FROM participants ORDER BY id;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        await update.message.reply_text("The participants list is empty.")
        return

    # Вывод с ID для удобства
    lines = [f"ID:{id} | {number} - {email}" for id, number, email in rows]
    text = "Participants list (use /remove <number> to delete):\n\n" + "\n".join(lines)

    # Разбиваем на части, если текст очень длинный
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
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_email)
    )
    application.add_handler(CommandHandler("raffle", raffle))
    application.add_handler(CommandHandler("export", export_participants))
    application.add_handler(CommandHandler("list", list_participants))
    application.add_handler(CommandHandler("reset", reset_participants))
    application.add_handler(CommandHandler("check_duplicates", check_duplicates))
    application.add_handler(CommandHandler("remove", remove_participant))

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
