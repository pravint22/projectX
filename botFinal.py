import asyncio
import os
import json
import uuid
import logging
import random
from io import BytesIO
from datetime import datetime, timedelta

import aiohttp
import cloudscraper
import urllib3
import ssl
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from PIL import Image, ImageFilter
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject
from aiogram.utils.keyboard import InlineKeyboardBuilder
import redis.asyncio as redis
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", 0))
PUBLIC_CH = int(os.getenv("PUBLIC_CH", 0))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", 0))
BOT_USERNAME = os.getenv("BOT_USERNAME")
SHRINKME_API_KEY = os.getenv("SHRINKME_API_KEY")
ADMIN_IDS = [int(i.strip()) for i in os.getenv("ADMIN_IDS", "").split(",") if i.strip()]
BASE_URL = "https://viralkand.com/"
CHECK_INTERVAL = 3600 * 6

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Bot and Dispatcher
bot = Bot(token=TOKEN)
dp = Dispatcher()

# Redis setup
r_conn = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)

# Scraper instance with SSL fix
# Suppress "InsecureRequestWarning"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- SSL FIX ---
custom_context = ssl.create_default_context()
custom_context.check_hostname = False
custom_context.verify_mode = ssl.CERT_NONE

class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = custom_context
        return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)

scraper = cloudscraper.create_scraper()
scraper.mount("https://", SSLAdapter())

# --- REDIS HELPERS ---
async def init_redis():
    """Initialize Redis connection"""
    try:
        await r_conn.ping()
        logger.info("✅ Redis Connected Successfully")
    except Exception as e:
        logger.error(f"❌ Redis Connection Failed: {e}")

async def save_video(video_num, msg_id):
    await r_conn.set(f"video:{video_num}", msg_id)

async def get_msg_id(video_num):
    msg_id = await r_conn.get(f"video:{video_num}")
    return int(msg_id) if msg_id else None

async def get_history():
    return await r_conn.smembers("processed_posts")

async def save_to_history(post_id):
    await r_conn.sadd("processed_posts", post_id)

# --- USER MANAGEMENT ---
def get_seconds_until_midnight():
    now = datetime.now()
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int((midnight - now).total_seconds())

async def check_user_status(user_id):
    usage_key = f"user:{user_id}:usage"
    verify_key = f"user:{user_id}:verified"
    token_key = f"user:{user_id}:token"

    usage_count = await r_conn.get(usage_key) or 0
    is_verified = await r_conn.get(verify_key)
    token = await r_conn.get(token_key)

    return {
        "usage_count": int(usage_count),
        "is_verified": bool(is_verified),
        "verification_token": token
    }

async def update_usage(user_id):
    usage_key = f"user:{user_id}:usage"
    new_val = await r_conn.incr(usage_key)
    if new_val == 1:
        await r_conn.expire(usage_key, get_seconds_until_midnight())

async def set_verified(user_id):
    verify_key = f"user:{user_id}:verified"
    await r_conn.set(verify_key, 1)
    await r_conn.expire(verify_key, get_seconds_until_midnight())

async def save_verification_token(user_id, token):
    token_key = f"user:{user_id}:token"
    await r_conn.set(token_key, token)
    await r_conn.expire(token_key, get_seconds_until_midnight())

# --- CHANNEL MANAGEMENT ---
async def add_channel(channel_id, link, name):
    data = json.dumps({"link": link, "name": name})
    await r_conn.hset("channels_map", str(channel_id), data)

async def remove_channel(channel_id):
    await r_conn.hdel("channels_map", str(channel_id))

async def get_channels():
    all_channels = await r_conn.hgetall("channels_map")
    result = []
    for ch_id, ch_data in all_channels.items():
        data = json.loads(ch_data)
        result.append({"id": ch_id, "link": data["link"], "name": data["name"]})
    return result

# --- HELPERS ---
async def fetch_url(url, is_json=False):
    """Fetch URL using cloudscraper in a thread to bypass Cloudflare."""
    def _fetch():
        response = scraper.get(url, timeout=15, verify=False)
        return response.json() if is_json else response.text
    return await asyncio.to_thread(_fetch)

async def download_file_async(url):
    """Downloads a file to memory using cloudscraper in a thread."""
    def _download():
        try:
            response = scraper.get(url, timeout=45, verify=False, stream=True)
            if response.status_code == 200:
                return BytesIO(response.content)
            return None
        except Exception as e:
            logger.error(f"Download Error: {e}")
            return None
    return await asyncio.to_thread(_download)

async def shorten_url(destination_url):
    """Shortens a URL using ShrinkMe.io API."""
    api_url = f"https://shrinkme.io/api?api={SHRINKME_API_KEY}&url={destination_url}&alias={uuid.uuid4().hex[:8]}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(api_url) as resp:
                data = await resp.json()
                if data.get("status") == "success":
                    return data["shortenedUrl"]
        except Exception as e:
            logger.error(f"Error shortening URL: {e}")
    return destination_url

async def check_subscription(user_id):
    """Checks if user is subscribed to all forced channels."""
    channels = await get_channels()
    not_joined = []
    for ch in channels:
        try:
            member = await bot.get_chat_member(chat_id=ch["id"], user_id=user_id)
            if member.status in ['left', 'kicked']:
                not_joined.append(ch)
        except Exception as e:
            logger.error(f"Error checking sub for {ch['id']}: {e}")
            continue
    return not_joined

async def process_image_async(url):
    """Downloads and blurs the image in a separate thread."""
    def _process():
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            # Note: Using cloudscraper here too just in case the image is behind CF
            response = scraper.get(url, headers=headers, timeout=15, verify=False)
            img = Image.open(BytesIO(response.content)).convert("RGB")
            img = img.filter(ImageFilter.GaussianBlur(radius=25))
            
            width, height = img.size
            pixels = img.load()
            for _ in range(int(width * height * 0.15)):
                x, y = random.randint(0, width - 1), random.randint(0, height - 1)
                pixels[x, y] = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))

            bio = BytesIO()
            img.save(bio, 'JPEG', quality=80)
            bio.seek(0)
            return bio
        except Exception as e:
            logger.error(f"Blur Error: {e}")
            return None
    return await asyncio.to_thread(_process)

async def delete_after_delay(chat_id: int, message_id: int, delay: int = 1800):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass

# --- COMMAND HANDLERS ---
@dp.message(Command("addch"))
async def add_channel_cmd(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return
    if not command.args:
        return await message.answer("Usage: /addch <channel_id> <link> <name>")
    parts = command.args.split(' ', 2)
    if len(parts) < 3:
        return await message.answer("Usage: /addch <channel_id> <link> <name>")
    await add_channel(parts[0], parts[1], parts[2])
    await message.answer(f"Channel {parts[2]} added.")

@dp.message(Command("delch"))
async def del_channel_cmd(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return
    if not command.args:
        return await message.answer("Usage: /delch <channel_id>")
    await remove_channel(command.args)
    await message.answer("Channel removed.")

@dp.callback_query(F.data == "delete_msg")
async def delete_button_handler(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        await callback.answer("Message already deleted.")

@dp.callback_query(F.data.startswith("check_sub_"))
async def check_sub_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    original_args = callback.data.replace("check_sub_", "")
    not_joined = await check_subscription(user_id)
    if not not_joined:
        await callback.message.delete()
        await callback.answer("✅ Verified!")
        await callback.message.answer(f"✅ Verified! Click here: /start {original_args}")
    else:
        await callback.answer("❌ You haven't joined all channels yet!", show_alert=True)

@dp.message(Command("start"))
async def cmd_start(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    args = command.args

    try:
        await message.delete()
    except Exception:
        pass

    not_joined = await check_subscription(user_id)
    if not_joined:
        builder = InlineKeyboardBuilder()
        for ch in not_joined:
            builder.row(types.InlineKeyboardButton(text=f"Join {ch['name']}", url=ch['link']))
        builder.row(types.InlineKeyboardButton(text="Try Again 🔄", callback_data=f"check_sub_{args or ''}"))
        return await message.answer("⚠️ You must join our channels to access files.", reply_markup=builder.as_markup())

    if not args:
        return await message.answer("👋 Welcome! Use a link to get a video.")
    
    if args.startswith("verify_"):
        token = args.replace("verify_", "")
        user_data = await check_user_status(user_id)
        if user_data["verification_token"] == token:
            await set_verified(user_id)
            await message.answer("✅ Verified! You can now access unlimited videos for today.")
        else:
            await message.answer("❌ Invalid or expired verification link.")
        return

    # Handle Video Request
    video_num = args
    msg_id = await get_msg_id(video_num)
    
    if msg_id:
        user_data = await check_user_status(user_id)
        if user_data["usage_count"] < 3 or user_data["is_verified"]:
            builder = InlineKeyboardBuilder()
            start_link = f"https://t.me/{BOT_USERNAME}?start={video_num}"
            builder.row(types.InlineKeyboardButton(text="Share Video 🔗", url=f"https://t.me/share/url?url={start_link}"))
            builder.row(types.InlineKeyboardButton(text="Delete Now 🗑️", callback_data="delete_msg"))

            try:
                sent_video = await bot.copy_message(
                    chat_id=message.chat.id,
                    from_chat_id=TARGET_CHANNEL_ID,
                    message_id=msg_id,
                    caption=f"🎬 Video #{video_num}",
                    reply_markup=builder.as_markup()
                )
                await update_usage(user_id)
                logger.info(f"✅ Forwarded video #{video_num} to user {user_id}")
                
                log_data = {"event": "#user_request", "user_id": user_id, "requested_id": video_num, "status": "sent"}
                await bot.send_message(LOG_CHANNEL_ID, f"<code>{json.dumps(log_data, indent=2)}</code>", parse_mode="HTML")
                
                # Auto-delete after 15 mins
                asyncio.create_task(delete_after_delay(message.chat.id, sent_video.message_id, 900))

            except Exception as e:
                await message.answer(f"❌ Error sending video: {e}")
        else:
            new_token = uuid.uuid4().hex
            await save_verification_token(user_id, new_token)
            dest_link = f"https://t.me/{BOT_USERNAME}?start=verify_{new_token}"
            final_ad_link = await shorten_url(dest_link)
            await message.answer(
                f"⚠️ <b>Daily Limit Reached (3/3)</b>\n\nTo access more files today, please complete this verification:\n\n👉 {final_ad_link}\n\n<i>This helps keep our bot free!</i>",
                parse_mode="HTML"
            )
    else:
        await message.answer("❌ Video not found.")

# --- SCRAPING LOOP ---
async def scraping_loop():
    logger.info("Bot started monitoring...")
    while True:
        try:
            html = await fetch_url(BASE_URL)
            soup = BeautifulSoup(html, 'html.parser')
            video_blocks = soup.find_all('div', class_='video-block', limit=10)

            if not video_blocks:
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            processed_ids = await get_history()
            new_updates = 0

            for block in reversed(video_blocks):
                current_post_id = block.get('data-post-id')
                thumb_tag = block.find('a', class_='thumb')

                if not current_post_id or not thumb_tag or current_post_id in processed_ids:
                    continue

                video_page_url = thumb_tag['href']
                v_html = await fetch_url(video_page_url)
                v_soup = BeautifulSoup(v_html, 'html.parser')

                try:
                    name = v_soup.find("meta", {"itemprop": "name"})["content"]
                    thumb = v_soup.find("meta", {"itemprop": "thumbnailUrl"})["content"]
                    content_url = v_soup.find("meta", {"itemprop": "contentURL"})["content"]
                    duration = v_soup.find("meta", {"itemprop": "duration"})["content"]
                    date = v_soup.find("meta", {"itemprop": "uploadDate"})["content"]
                    video_num = content_url.split('/')[-1].replace('.mp4', '')

                    caption = (
                        f"video #{video_num}\n"
                        "{\n"
                        f'  "name": "{name}",\n'
                        f'  "thumbnail": "{thumb}",\n'
                        f'  "contentURL": "{content_url}",\n'
                        f'  "duration": "{duration}",\n'
                        f'  "uploadDate": "{date}"\n'
                        "}"
                    )

                    sent_msg = None
                    video_buffer = await download_file_async(content_url)
                    
                    if video_buffer:
                        sent_msg = await bot.send_video(
                            chat_id=TARGET_CHANNEL_ID,
                            video=types.BufferedInputFile(video_buffer.getvalue(), filename=f"{video_num}.mp4"),
                            caption=caption,
                            supports_streaming=True
                        )
                    else:
                        logger.error(f"Failed to download video #{video_num} for upload.")
                        continue
                    
                    if sent_msg:
                        await save_video(video_num, sent_msg.message_id)
                        await save_to_history(current_post_id)
                        new_updates += 1

                    processed_photo = await process_image_async(thumb)
                    if processed_photo:
                        banner_text = (
                            f"🎬 **{name}**\n\n"
                            f"⏳ **duration:-** {duration}\n"
                            f"📥 **download link:-** https://t.me/{BOT_USERNAME}?start={video_num}\n\n"
                            f"🚀 *Share us to support!*"
                        )
                        await bot.send_photo(
                            chat_id=PUBLIC_CH,
                            photo=types.BufferedInputFile(processed_photo.getvalue(), filename="banner.jpg"),
                            caption=banner_text,
                            parse_mode="Markdown"
                        )
                    
                    await asyncio.sleep(2)  # Flood limit guard

                except Exception as e:
                    logger.error(f"Error processing {current_post_id}: {e}")

            if new_updates > 0:
                logger.info(f"Status: Finished processing {new_updates} new posts.")

        except Exception as e:
            logger.error(f"Critical Error in main loop: {e}")

        await asyncio.sleep(CHECK_INTERVAL)

async def main():
    await init_redis()
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("✅ Starting polling mode...")
    await asyncio.gather(scraping_loop(), dp.start_polling(bot))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
