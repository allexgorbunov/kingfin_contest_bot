import os
import re
from flask import Flask, request
import telebot

# 1. Token и admin id из env
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

bot = telebot.TeleBot(TOKEN)
app = Flask(__name__)

# 2. In-memory storage
emails = []  # ["user@example.com", ...]

email_regex = re.compile(r"[^@ \t\r\n]+@[^@ \t\r\n]+\.[^@ \t\r\n]+")


# 3. Handlers
@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "Hi! Please send your email to participate.")


@bot.message_handler(func=lambda m: True, content_types=['text'])
def collect_email(message):
    text = message.text.strip()

    if not email_regex.fullmatch(text):
        bot.reply_to(
            message,
            "This does not look like an email address.\n"
            "Please send an email in the format: user@example.com"
        )
        return

    emails.append(text)
    bot.reply_to(message, "Your email has been received. Thank you!")


@bot.message_handler(commands=['all_emails'])
def send_all_emails(message):
    if message.from_user.id != ADMIN_ID:
        return  # only admin can use this command

    if not emails:
        bot.send_message(ADMIN_ID, "There are no emails yet.")
        return

    lines = [f"{i} - {mail}" for i, mail in enumerate(emails, start=1)]
    text = "\n".join(lines)

    MAX_LEN = 4000
    if len(text) <= MAX_LEN:
        bot.send_message(ADMIN_ID, text)
    else:
        for i in range(0, len(text), MAX_LEN):
            bot.send_message(ADMIN_ID, text[i:i + MAX_LEN])


@bot.message_handler(commands=['clear_emails'])
def clear_emails(message):
    if message.from_user.id != ADMIN_ID:
        return  # only admin can use this command

    global emails
    emails = []

    bot.send_message(ADMIN_ID, "All collected emails have been cleared.")


# 4. Flask routes
@app.route("/", methods=["GET"])
def index():
    return "Bot is running", 200


@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    json_str = request.get_data().decode("utf-8")
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "OK", 200


# 5. Local debug (не используется на Render, но не мешает)
if __name__ == "__main__":
    print("Bot is running in polling mode...")
    bot.infinity_polling()

@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    print("=== GOT UPDATE FROM TELEGRAM ===")
    json_str = request.get_data().decode("utf-8")
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "OK", 200
