import os
import re
import time
import uuid
import logging
import asyncio
from pathlib import Path
from collections import deque
from datetime import datetime, timezone

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified

from health_check import start_health_server

# =========================
# LOAD ENV
# =========================
load_dotenv()

API_ID          = int(os.getenv("API_ID", "0"))
API_HASH        = os.getenv("API_HASH", "")
BOT_TOKEN       = os.getenv("BOT_TOKEN", "")
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://localhost:27017")
RENAME_USERNAME = os.getenv("RENAME_USERNAME", "Bot")
ADMIN_IDS       = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

BATCH_SIZE      = int(os.getenv("BATCH_SIZE", "10"))
POST_LINKS_CH   = os.getenv("POST_LINKS_CHANNEL", "")
POSTER_URL      = os.getenv("POSTER_URL", "")
WORKER_BASE_URL = os.getenv("WORKER_BASE_URL", "https://files.atozlinksbot.workers.dev")

DOWNLOAD_DIR      = "downloads"
PROGRESS_INTERVAL = 5
MAX_FILES_PER_CH  = 1000
MAX_RETRIES       = 3

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# =========================
# MONGODB
# =========================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db           = mongo_client["mediabot"]
col_files    = db["files"]
col_channels = db["channels"]
col_batches  = db["post_links"]

# =========================
# BOT CLIENT
# =========================
bot = Client(
    "MediaBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# =========================
# PER-USER QUEUE
# =========================
_queues:  dict[int, deque]        = {}
_workers: dict[int, asyncio.Task] = {}

def get_queue(user_id: int) -> deque:
    if user_id not in _queues:
        _queues[user_id] = deque()
    return _queues[user_id]

# =========================
# RATE LIMITER
# =========================
_user_last: dict[int, float] = {}
RATE_LIMIT_SECONDS = 5

# =========================
# ADMIN GUARD
# =========================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# =========================
# HELPERS
# =========================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def human_size(num: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} PB"


def progress_bar(current: int, total: int, length: int = 15) -> str:
    filled = int(length * current / total) if total else 0
    bar    = "█" * filled + "░" * (length - filled)
    pct    = (current / total * 100) if total else 0
    return f"[{bar}] {pct:.1f}%"


def rename_file(original_name: str) -> str:
    name = original_name.strip()
    name = re.sub(r"@[\w]+\s*", "", name).strip()
    name = re.sub(r"^\s*[Bb]y\s+", "", name).strip()
    name = re.sub(r"\s{2,}", " ", name)
    p    = Path(name)
    stem = p.stem.strip()
    ext  = p.suffix
    if not stem:
        stem = Path(original_name).stem
    return f"[{RENAME_USERNAME}] {stem}{ext}"


async def safe_edit(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value)
        try:
            await msg.edit_text(text)
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"safe_edit failed: {e}")


async def safe_delete(msg: Message):
    try:
        await msg.delete()
    except Exception as e:
        logger.warning(f"safe_delete failed: {e}")


def make_progress_callback(message: Message, action: str):
    state = {"last_update": 0.0}

    async def callback(current: int, total: int):
        now = time.time()
        if now - state["last_update"] < PROGRESS_INTERVAL and current < total:
            return
        state["last_update"] = now
        icon = "⬇️ Downloading" if action == "download" else "⬆️ Uploading"
        text = (
            f"{icon}...\n\n"
            f"{progress_bar(current, total)}\n"
            f"{human_size(current)} / {human_size(total)}"
        )
        await safe_edit(message, text)

    return callback


def _cleanup(path: str):
    try:
        if path and os.path.exists(path):
            os.remove(path)
            logger.info(f"Deleted local file: {path}")
    except Exception as e:
        logger.warning(f"Could not delete {path}: {e}")


def parse_tg_link(url: str) -> tuple[int | None, int | None]:
    m = re.match(r"https?://t\.me/c/(\d+)/(\d+)", url.strip())
    if m:
        return int(f"-100{m.group(1)}"), int(m.group(2))
    return None, None

# =========================
# DB HELPERS
# =========================

async def is_duplicate(file_unique_id: str) -> bool:
    return await col_files.find_one({"file_unique_id": file_unique_id}) is not None


async def save_file(
    file_unique_id: str,
    file_id: str,
    name: str,
    size: int,
    from_chat_id: int,
    message_id: int,
) -> str:
    batch_id = await get_or_create_open_batch()
    await col_files.insert_one({
        "file_unique_id": file_unique_id,
        "file_id":        file_id,
        "name":           name,
        "size":           size,
        "batch_id":       batch_id,
        "from_chat_id":   from_chat_id,
        "message_id":     message_id,
        "uploaded_at":    utcnow(),
    })
    await col_batches.update_one(
        {"batch_id": batch_id},
        {"$inc": {"file_count": 1}}
    )
    return batch_id


async def get_or_create_open_batch() -> str:
    doc = await col_batches.find_one({"status": "open"}, sort=[("created_at", 1)])
    if doc:
        return doc["batch_id"]
    batch_id = uuid.uuid4().hex[:8]
    await col_batches.insert_one({
        "batch_id":   batch_id,
        "file_count": 0,
        "status":     "open",
        "created_at": utcnow(),
        "link":       None
    })
    logger.info(f"Created new batch: {batch_id}")
    return batch_id


async def get_batch_file_count(batch_id: str) -> int:
    doc = await col_batches.find_one({"batch_id": batch_id})
    return doc["file_count"] if doc else 0


async def close_batch(batch_id: str, link: str):
    await col_batches.update_one(
        {"batch_id": batch_id},
        {"$set": {"status": "closed", "link": link, "closed_at": utcnow()}}
    )


async def get_active_channel() -> dict | None:
    return await col_channels.find_one(
        {"active": True, "file_count": {"$lt": MAX_FILES_PER_CH}},
        sort=[("added_at", 1)]
    )


async def increment_channel_count(channel_id: int):
    await col_channels.update_one(
        {"channel_id": channel_id},
        {"$inc": {"file_count": 1}}
    )


async def add_channel(channel_id: int, channel_name: str):
    if await col_channels.find_one({"channel_id": channel_id}):
        return False, "already_exists"
    await col_channels.insert_one({
        "channel_id":   channel_id,
        "channel_name": channel_name,
        "file_count":   0,
        "added_at":     utcnow(),
        "active":       True
    })
    return True, "added"


async def list_channels() -> list:
    return await col_channels.find({}, sort=[("added_at", 1)]).to_list(length=100)

# =========================
# BATCH PUBLISHER
# =========================

async def publish_batch_link(client: Client, batch_id: str):
    if not POST_LINKS_CH or not WORKER_BASE_URL:
        logger.warning("POST_LINKS_CHANNEL or WORKER_BASE_URL not set.")
        return

    worker_link = f"{WORKER_BASE_URL.rstrip('/')}/{batch_id}"
    try:
        post_channel_id = int(POST_LINKS_CH)
    except ValueError:
        post_channel_id = POST_LINKS_CH

    caption = (
        f"🎬 New Batch Ready!\n\n"
        f"Files : {BATCH_SIZE}\n"
        f"Link  : {worker_link}"
    )

    if POSTER_URL:
        chat_id, msg_id = parse_tg_link(POSTER_URL)
        if chat_id and msg_id:
            try:
                src = await client.get_messages(chat_id, msg_id)
                photo_fid = None
                if src and src.photo:
                    photo_fid = src.photo.file_id
                elif src and src.document:
                    photo_fid = src.document.file_id
                if photo_fid:
                    await client.send_photo(post_channel_id, photo=photo_fid, caption=caption)
                    await close_batch(batch_id, worker_link)
                    logger.info(f"Batch {batch_id} published with poster.")
                    return
            except Exception as e:
                logger.warning(f"Poster copy failed: {e}. Falling back to text.")

    try:
        await client.send_message(post_channel_id, caption)
        await close_batch(batch_id, worker_link)
        logger.info(f"Batch {batch_id} published (text only).")
    except Exception as e:
        logger.error(f"Failed to publish batch {batch_id}: {e}")

# =========================
# CORE: process one file
# =========================

async def process_one(client: Client, message: Message) -> bool:
    user_name = message.from_user.first_name

    if message.photo:
        media         = message.photo[-1]
        original_name = f"photo_{message.id}.jpg"
    elif message.video:
        media         = message.video
        original_name = media.file_name or f"video_{message.id}.mp4"
    elif message.audio:
        media         = message.audio
        original_name = media.file_name or f"audio_{message.id}.mp3"
    elif message.voice:
        media         = message.voice
        original_name = f"voice_{message.id}.ogg"
    elif message.video_note:
        media         = message.video_note
        original_name = f"videonote_{message.id}.mp4"
    elif message.animation:
        media         = message.animation
        original_name = media.file_name or f"animation_{message.id}.mp4"
    elif message.sticker:
        media         = message.sticker
        original_name = f"sticker_{message.id}.webp"
    elif message.document:
        media         = message.document
        original_name = media.file_name or f"document_{message.id}"
    else:
        await message.reply_text("❌ Unsupported media type.")
        return False

    file_unique_id = getattr(media, "file_unique_id", None)
    file_id        = getattr(media, "file_id", None)

    logger.info(f"Processing: {user_name} | {original_name}")

    if file_unique_id and await is_duplicate(file_unique_id):
        await message.reply_text(
            f"⚠️ Duplicate skipped!\nFile: {original_name}"
        )
        await safe_delete(message)
        return False

    active_ch = await get_active_channel()
    if not active_ch:
        await message.reply_text(
            "🚫 No active channel!\n"
            f"All channels reached {MAX_FILES_PER_CH} file limit.\n"
            "Admin: /add_channel <id>"
        )
        return False

    target_channel = active_ch["channel_id"]
    ch_name        = active_ch["channel_name"]
    ch_count       = active_ch["file_count"]

    Path(DOWNLOAD_DIR).mkdir(exist_ok=True)

    progress_msg = await message.reply_text(
        f"⬇️ Downloading...\n"
        f"Target: {ch_name} ({ch_count + 1}/{MAX_FILES_PER_CH})"
    )

    downloaded_path = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            downloaded_path = await client.download_media(
                message,
                file_name=os.path.join(DOWNLOAD_DIR, original_name),
                progress=make_progress_callback(progress_msg, "download")
            )
            break
        except FloodWait as e:
            await safe_edit(progress_msg, f"⏳ Flood wait {e.value}s... (dl attempt {attempt})")
            await asyncio.sleep(e.value)
        except Exception as e:
            if attempt == MAX_RETRIES:
                await safe_edit(progress_msg, f"❌ Download failed:\n{e}")
                logger.error(f"Download failed ({original_name}): {e}")
                return False
            await asyncio.sleep(2 * attempt)

    if not downloaded_path:
        await safe_edit(progress_msg, "❌ Download failed: unknown error.")
        return False

    await safe_delete(message)

    renamed = rename_file(original_name)
    ext     = Path(original_name).suffix
    if not renamed.endswith(ext):
        renamed = Path(renamed).stem + ext

    renamed_path = os.path.join(DOWNLOAD_DIR, renamed)
    try:
        os.rename(downloaded_path, renamed_path)
    except Exception as e:
        logger.error(f"Rename failed: {e}")
        renamed_path = downloaded_path
        renamed      = original_name

    file_size_raw = os.path.getsize(renamed_path)
    file_size     = human_size(file_size_raw)

    await safe_edit(
        progress_msg,
        f"✅ Downloaded: {renamed}\n"
        f"Size: {file_size}\n\n"
        f"⬆️ Uploading to {ch_name}..."
    )

    sent_msg  = None
    upload_ok = False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            up_cb = make_progress_callback(progress_msg, "upload")
            if message.video or message.animation:
                sent_msg = await client.send_video(
                    target_channel, video=renamed_path,
                    caption=f"File Name : {renamed}\nFile Size : {file_size}",
                    progress=up_cb,
                    supports_streaming=True
                )
            elif message.audio:
                sent_msg = await client.send_audio(
                    target_channel, audio=renamed_path,
                    caption=f"File Name : {renamed}\nFile Size : {file_size}",
                    progress=up_cb
                )
            elif message.voice:
                sent_msg = await client.send_voice(
                    target_channel, voice=renamed_path,
                    caption=f"File Name : {renamed}\nFile Size : {file_size}",
                    progress=up_cb
                )
            elif message.video_note:
                sent_msg = await client.send_video_note(
                    target_channel, video_note=renamed_path,
                    progress=up_cb
                )
            elif message.photo:
                sent_msg = await client.send_photo(
                    target_channel,
                    photo=file_id,
                    caption=f"File Name : {renamed}\nFile Size : {file_size}"
                )
            elif message.sticker:
                sent_msg = await client.send_sticker(
                    target_channel, sticker=renamed_path
                )
            else:
                sent_msg = await client.send_document(
                    target_channel, document=renamed_path,
                    caption=f"File Name : {renamed}\nFile Size : {file_size}",
                    progress=up_cb
                )
            upload_ok = True
            break
        except FloodWait as e:
            await safe_edit(progress_msg, f"⏳ Flood wait {e.value}s... (up attempt {attempt})")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error(f"Upload attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                await safe_edit(progress_msg, f"⚠️ Upload error (attempt {attempt}), retrying...\n{e}")
                await asyncio.sleep(3 * attempt)
            else:
                await safe_edit(progress_msg, f"❌ Upload failed after {MAX_RETRIES} attempts:\n{e}")

    _cleanup(renamed_path)

    if not upload_ok:
        return False

    batch_id = None
    if file_unique_id and sent_msg:
        batch_id = await save_file(
            file_unique_id,
            file_id or "",
            renamed,
            file_size_raw,
            from_chat_id=sent_msg.chat.id,
            message_id=sent_msg.id,
        )

    await increment_channel_count(target_channel)
    new_count = ch_count + 1

    batch_link_text = ""
    if batch_id and await get_batch_file_count(batch_id) >= BATCH_SIZE:
        await publish_batch_link(client, batch_id)
        worker_link     = f"{WORKER_BASE_URL.rstrip('/')}/{batch_id}"
        batch_link_text = f"\n\n🔗 Batch ready:\n{worker_link}"

    limit_warn = ""
    if new_count >= MAX_FILES_PER_CH:
        limit_warn = f"\n\n⚠️ {ch_name} is FULL! Admin: /add_channel <id>"
        logger.warning(f"Channel {ch_name} ({target_channel}) reached limit!")

    await safe_delete(progress_msg)

    done_msg = await message.reply_text(
        f"✅ {renamed}\n"
        f"{file_size} | {ch_name} ({new_count}/{MAX_FILES_PER_CH})"
        f"{batch_link_text}"
        f"{limit_warn}"
    )
    asyncio.get_event_loop().call_later(
        8, asyncio.ensure_future, safe_delete(done_msg)
    )

    logger.info(f"Done: {renamed} -> {ch_name} ({new_count}/{MAX_FILES_PER_CH})")
    return True


# =========================
# PER-USER WORKER
# =========================

async def user_worker(client: Client, user_id: int):
    queue = get_queue(user_id)
    while queue:
        message = queue.popleft()
        try:
            last = _user_last.get(user_id, 0)
            gap  = RATE_LIMIT_SECONDS - (time.time() - last)
            if gap > 0:
                await asyncio.sleep(gap)
            _user_last[user_id] = time.time()
            await process_one(client, message)
        except Exception as e:
            logger.error(f"Worker error for user {user_id}: {e}")
    _workers.pop(user_id, None)


# =========================
# MEDIA HANDLER
# =========================

@bot.on_message(filters.private & (
    filters.video | filters.document | filters.audio |
    filters.voice | filters.video_note | filters.photo |
    filters.animation | filters.sticker
))
async def media_handler(client: Client, message: Message):
    user_id = message.from_user.id
    queue   = get_queue(user_id)
    queue.append(message)
    pos = len(queue)

    if user_id not in _workers or _workers[user_id].done():
        task = asyncio.ensure_future(user_worker(client, user_id))
        _workers[user_id] = task
    else:
        if pos > 1:
            notice = await message.reply_text(
                f"📥 Queued (position {pos}). Processing sequentially..."
            )
            asyncio.get_event_loop().call_later(
                5, asyncio.ensure_future, safe_delete(notice)
            )

# =========================
# /start
# =========================

@bot.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    user = message.from_user.first_name
    await message.reply_text(
        f"👋 Hello {user}!\n\n"
        f"🚀 High-Speed Media Bot\n\n"
        f"Send multiple files — I process them one by one.\n\n"
        f"✅ Duplicate check (MongoDB)\n"
        f"✅ Sequential queue per user\n"
        f"✅ Download → Rename → Upload\n"
        f"✅ Delete original after download\n"
        f"✅ Progress message auto-deleted on success\n"
        f"✅ Batch link every {BATCH_SIZE} files\n"
        f"✅ Auto-rotate channel after {MAX_FILES_PER_CH} files\n\n"
        f"Powered by Pyrogram ⚡"
    )

# =========================
# /add_channel  (admin)
# =========================

@bot.on_message(filters.command("add_channel"))
async def add_channel_handler(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        await message.reply_text("❌ Not authorized.")
        return

    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.reply_text("Usage: /add_channel -1001234567890")
        return

    try:
        channel_id = int(parts[1])
    except ValueError:
        await message.reply_text("❌ Invalid channel ID.")
        return

    msg = await message.reply_text("🔍 Verifying...")
    try:
        chat = await client.get_chat(channel_id)
    except Exception as e:
        await msg.edit_text(f"❌ Cannot access channel.\nError: {e}")
        return

    channel_name = chat.title or str(channel_id)
    ok, _        = await add_channel(channel_id, channel_name)

    if not ok:
        await msg.edit_text(f"⚠️ Already exists: {channel_name} ({channel_id})")
        return

    await msg.edit_text(
        f"✅ Channel added!\n\n"
        f"Name  : {channel_name}\n"
        f"ID    : {channel_id}\n"
        f"Files : 0 / {MAX_FILES_PER_CH}"
    )

# =========================
# /channels  (admin)
# =========================

@bot.on_message(filters.command("channels"))
async def channels_handler(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        await message.reply_text("❌ Not authorized.")
        return

    channels = await list_channels()
    if not channels:
        await message.reply_text("📭 No channels. Use /add_channel <id>")
        return

    lines = ["📋 Channels:\n"]
    for ch in channels:
        full   = ch["file_count"] >= MAX_FILES_PER_CH
        status = "🔴 Full" if full else "🟢 Active"
        lines.append(f"{status} {ch['channel_name']}\n   {ch['channel_id']} | {ch['file_count']}/{MAX_FILES_PER_CH}\n")
    await message.reply_text("\n".join(lines))

# =========================
# /stats  (admin)
# =========================

@bot.on_message(filters.command("stats"))
async def stats_handler(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        await message.reply_text("❌ Not authorized.")
        return

    total_files   = await col_files.count_documents({})
    total_batches = await col_batches.count_documents({})
    open_batch    = await col_batches.find_one({"status": "open"})
    channels      = await list_channels()
    active_ch     = await get_active_channel()

    ch_info    = f"{active_ch['channel_name']} ({active_ch['file_count']}/{MAX_FILES_PER_CH})" if active_ch else "None"
    batch_info = f"{open_batch['batch_id']} ({open_batch['file_count']}/{BATCH_SIZE})" if open_batch else "None"

    await message.reply_text(
        f"📊 Stats\n\n"
        f"Files uploaded : {total_files}\n"
        f"Batches total  : {total_batches}\n"
        f"Open batch     : {batch_info}\n"
        f"Channels       : {len(channels)}\n"
        f"Active channel : {ch_info}"
    )

# =========================
# RUN
# =========================

async def main():
    await start_health_server()   # HTTP server + keep-alive pinger start
    await bot.start()
    logger.info("Bot is running...")
    await asyncio.Event().wait()  # forever chalao

if __name__ == "__main__":
    logger.info("Starting Bot...")
    asyncio.run(main())
