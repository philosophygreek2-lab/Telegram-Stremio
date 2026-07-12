from asyncio import Lock, Queue, create_task
from asyncio import sleep as asleep

from pyrogram import Client, filters
from pyrogram.enums.parse_mode import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import Message

import Backend
from Backend import db
from Backend.helper.encrypt import encode_string
from Backend.helper.pyro import get_readable_file_size
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER

file_queue = Queue()
db_lock = Lock()

def _is_supported_media(message: Message) -> bool:
    if message.video:
        return True
    if message.document:
        mime_type = message.document.mime_type or ""
        if mime_type.startswith("video/"):
            return True
    return False

def _is_manual_channel(chat_id) -> bool:
    target = str(chat_id).replace("-100", "")
    return any(str(c).strip().replace("-100", "") == target for c in SettingsManager.current().manual_channels)

def _extract_fields(message: Message):
    file = message.video or message.document
    title = file.file_name or f"video_{message.id}.mp4"
    channel = str(message.chat.id).replace("-100", "")
    return file, title, message.id, file.file_size, get_readable_file_size(file.file_size), channel

async def process_file():
    while True:
        metadata_info, channel, msg_id, size, raw_size, title = await file_queue.get()
        async with db_lock:
            updated_id = await db.insert_media(metadata_info, channel=channel, msg_id=msg_id, size=size, raw_size=raw_size, name=title)
            if updated_id:
                LOGGER.info(f"Video saved with ID: {updated_id}")
            else:
                LOGGER.info("Save failed.")
        file_queue.task_done()

create_task(process_file())

@Client.on_message(filters.channel & (filters.document | filters.video))
async def file_receive_handler(client: Client, message: Message):
    if _is_manual_channel(message.chat.id):
        return
    if str(message.chat.id) not in SettingsManager.current().auth_channels:
        await message.reply_text("> Channel is not in AUTH_CHANNEL")
        return
    try:
        if not _is_supported_media(message):
            await message.reply_text("> Not a supported video file")
            return

        file, title, msg_id, raw_size, size, channel = _extract_fields(message)

        # Strip extension for display title
        display_title = title.rsplit(".", 1)[0] if "." in title else title

        encoded_string = await encode_string({"chat_id": int(channel), "msg_id": msg_id})

        metadata_info = {
            "tmdb_id": msg_id,
            "imdb_id": f"pv{channel}_{msg_id}",
            "title": display_title,
            "genres": ["Personal"],
            "description": "",
            "rate": None,
            "year": None,
            "poster": "",
            "backdrop": "",
            "logo": "",
            "cast": [],
            "runtime": None,
            "media_type": "movie",
            "quality": "Personal",
            "encoded_string": encoded_string,
            "is_anime": False,
            "original_language": None,
            "origin_country": [],
            "group_key": None,
            "part_number": None,
        }

        await file_queue.put((metadata_info, int(channel), msg_id, size, raw_size, display_title))

    except FloodWait as e:
        LOGGER.info(f"Sleeping for {str(e.value)}s")
        await asleep(e.value)
    except Exception as e:
        LOGGER.error(f"Error handling file {message.id}: {e}")

@Client.on_deleted_messages(filters.channel)
async def file_deleted_handler(client: Client, messages: list[Message]):
    try:
        for message in messages:
            if not message.chat:
                continue
            if not (str(message.chat.id) in SettingsManager.current().auth_channels or _is_manual_channel(message.chat.id)):
                continue
            channel = str(message.chat.id).replace("-100", "")
            msg_id = message.id
            try:
                if await db.remove_media_part(int(channel), msg_id):
                    LOGGER.info(f"Purged deleted message {msg_id}")
            except Exception as ex:
                LOGGER.error(f"Failed to purge {msg_id}: {ex}")
    except Exception as e:
        LOGGER.error(f"Error handling deleted messages: {e}")
