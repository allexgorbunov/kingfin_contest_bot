import os
from flask import Flask, request
import telebot

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

bot = telebot.TeleBot(TOKEN)
app = Flask(__name__)

# simple test: in-memory emails
emails = []


@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "Test: bot received /start")


@bot.message_handler(func=lambda m: True, content_types=['text'])
def collect_email(message):
    text = message.text.strip()
    emails.append(text)
    bot.reply_to(message, f"Saved: {text}")


@app.route("/", methods=["GET"])
def index():
    return "Bot is running", 200


@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    print("=== GOT UPDATE FROM TELEGRAM ===")
    json_str = request.get_data().decode("utf-8")
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "OK", 200


if __name__ == "__main__":
    print("Bot is running in polling mode...")
    bot.infinity_polling()
