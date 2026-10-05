import asyncio
import logging
import mimetypes
import os
import re
import shutil
import time
import uuid
from urllib.parse import quote

# Event loop must exist before Pyrogram is imported
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import aiohttp
from aiohttp import web

# HEIC/HEIF/TIFF can't be shown in browsers, so we convert them to JPEG on demand.
try:
    from PIL import Image, ImageOps
    import pillow_heif
    pillow_heif.register_heif_opener()
    HEIF_OK = True
except Exception:  # pip install pillow pillow-heif
    HEIF_OK = False

mimetypes.add_type("image/heic", ".heic")
mimetypes.add_type("image/heif", ".heif")
mimetypes.add_type("image/avif", ".avif")
from pyrogram import Client, filters, idle

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("awax")


# ───────────────────────── Config (Hardcoded) ─────────────────────────
API_ID = 37585567
API_HASH = "32dbd6f87b324ab3fce42c2f7f2622f4"
BOT_TOKEN = "8926737743:AAEJ2CLq3gM44V0M8e09h7_dNQq0BK0WRG4"
FIREBASE_SECRET = "7J71angnyl8fPjiXtKdSyr4ZskJ5uLd2BGsCjhHC"  # Realtime Database secret
FIREBASE_URL = "https://logingpage-e86f2-default-rtdb.firebaseio.com"
DEFAULT_CLOUD_CHANNEL_ID = -1004483191022

# Optional: verify the signed-in user with a Firebase ID token (recommended).
FIREBASE_API_KEY = "AIzaSyDdYRHmJ_sTbzgMMsEHa0_VLZAJxNR-d28"
REQUIRE_AUTH = False
MAX_UPLOAD_MB = 1024
MAX_CACHE_MB = 500  # disk cache for converted JPEGs

TEMP_ROOT = "temp_uploads"
CACHE_DIR = "cache"
os.makedirs(TEMP_ROOT, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

app = Client("my_cloud_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
HTTP: aiohttp.ClientSession = None  # created in main()

CHUNK = 1024 * 1024  # Pyrogram streams in 1 MiB chunks
FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,600}$")
UID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
SIZE_CACHE = {}  # file_id -> bytes actually served by Telegram
CONVERT_LOCKS = {}
CONVERT_SEM = asyncio.Semaphore(2)  # at most 2 conversions at once


# ───────────────────────────── Helpers ─────────────────────────────
def fb(path):
    return f"{FIREBASE_URL}/{path}.json?auth={FIREBASE_SECRET}"


def clean_text(value, limit=80):
    value = re.sub(r"[\r\n\t]+", " ", value or "").strip()
    return value[:limit] or "Unknown"


def clean_filename(name):
    name = os.path.basename((name or "file").replace("\\", "/"))
    name = re.sub(r"[^\w.\- ()]", "_", name).strip(" .")
    return name[:120] or "file"


def parse_channel(val):
    val = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060\ufeff\s]", "", val or "")
    val = re.sub(r"[\u2010-\u2015\u2212\ufe58\ufe63\uff0d]", "-", val)  # fancy dashes -> "-"
    if re.fullmatch(r"\d{12,14}", val) and val.startswith("100"):
        val = "-" + val
    if re.fullmatch(r"-?\d{5,20}", val):
        return int(val)
    if re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{3,31}", val):
        return val
    return None


async def verify_token(id_token):
    """Return the Firebase uid for a valid ID token, else None."""
    if not FIREBASE_API_KEY or not id_token:
        return None
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:lookup?key={FIREBASE_API_KEY}"
    try:
        async with HTTP.post(url, json={"idToken": id_token}) as r:
            if r.status != 200:
                return None
            users = (await r.json()).get("users") or []
            return users[0].get("localId") if users else None
    except Exception as e:
        log.warning("Token verification failed: %s", e)
        return None


async def index_put(file_id, nbytes, message_id=None):
    """Remember the exact byte size so /stream can answer Range and HEAD requests."""
    if not file_id or not nbytes:
        return
    SIZE_CACHE[file_id] = int(nbytes)
    if len(SIZE_CACHE) > 5000:
        SIZE_CACHE.pop(next(iter(SIZE_CACHE)))
    try:
        async with HTTP.put(fb(f"file_index/{file_id}"), json={"bytes": int(nbytes), "msg": message_id}) as r:
            if r.status >= 300:
                log.warning("file_index write failed: HTTP %s", r.status)
    except Exception as e:
        log.warning("file_index write failed: %s", e)


async def index_get(file_id):
    if file_id in SIZE_CACHE:
        return SIZE_CACHE[file_id]
    try:
        async with HTTP.get(fb(f"file_index/{file_id}/bytes")) as r:
            if r.status == 200:
                v = await r.json()
                if isinstance(v, (int, float)) and v > 0:
                    SIZE_CACHE[file_id] = int(v)
                    return int(v)
    except Exception as e:
        log.warning("file_index read failed: %s", e)
    return None


async def make_thumb(src, dst):
    for ss in ("1", "0"):
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-ss", ss, "-i", src, "-vframes", "1", "-vf", "scale=480:-2", dst,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
        except FileNotFoundError:
            log.warning("ffmpeg not installed, skipping video thumbnails")
            return False
        if os.path.exists(dst) and os.path.getsize(dst) > 0:
            return True
    return False


async def send_to_cloud(path, filename, caption):
    """Upload to the main channel. Returns (message, kind, file_id, thumb_id, duration)."""
    ctype = mimetypes.guess_type(filename)[0] or ""
    kind, thumb_id, duration = "document", "", 0

    if ctype.startswith("image"):
        kind = "image"
        try:
            msg = await app.send_photo(DEFAULT_CLOUD_CHANNEL_ID, photo=path, caption=caption)
            media = msg.photo
        except Exception as e:  # too large / odd dimensions: keep the original as a document
            log.info("send_photo failed (%s), sending as document", e)
            msg = await app.send_document(DEFAULT_CLOUD_CHANNEL_ID, document=path, caption=caption)
            media = msg.document
    elif ctype.startswith("video"):
        kind = "video"
        thumb_path = path + ".jpg"
        if await make_thumb(path, thumb_path):
            tmsg = await app.send_photo(
                DEFAULT_CLOUD_CHANNEL_ID, photo=thumb_path,
                caption=f"🖼 Thumbnail · {filename[:80]}", disable_notification=True,
            )
            thumb_id = tmsg.photo.file_id
            await index_put(thumb_id, tmsg.photo.file_size, tmsg.id)
        msg = await app.send_video(DEFAULT_CLOUD_CHANNEL_ID, video=path, supports_streaming=True, caption=caption)
        media = msg.video or msg.document
        duration = getattr(media, "duration", 0) or 0
    else:
        msg = await app.send_document(DEFAULT_CLOUD_CHANNEL_ID, document=path, caption=caption)
        media = msg.document

    file_id = media.file_id
    await index_put(file_id, getattr(media, "file_size", 0), msg.id)
    return msg, kind, file_id, thumb_id, duration


# ───────────────────────────── CORS ─────────────────────────────
async def add_cors(request, response):
    # Runs before headers are sent, so it also covers streamed responses
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Range, Authorization"
    response.headers["Access-Control-Expose-Headers"] = (
        "Content-Length, Content-Type, Content-Range, Accept-Ranges, Content-Disposition"
    )
    response.headers["Access-Control-Max-Age"] = "86400"


@web.middleware
async def error_middleware(request, handler):
    if request.method == "OPTIONS":
        return web.Response(status=204)
    try:
        return await handler(request)
    except web.HTTPException as ex:
        return ex
    except Exception as e:
        log.exception("Unhandled error")
        return web.json_response({"status": "error", "message": str(e)}, status=500)


# ───────────────────────────── Telegram bot ─────────────────────────────
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    await message.reply_text(
        "👋 Awax Cloud bot.\n\nTo back up your uploads to your own channel, add this bot as an admin "
        "of that channel, then paste the channel ID in the Awax app (Profile → Backup channel)."
    )


@app.on_message(filters.private & filters.forwarded & ~(filters.document | filters.video | filters.photo))
async def channel_id_helper(client, message):
    chat = getattr(message, "forward_from_chat", None) or getattr(getattr(message, "forward_origin", None), "chat", None)
    if chat is not None and getattr(chat, "id", None):
        await message.reply_text(
            f"📢 {chat.title or 'Channel'}\nChannel ID: `{chat.id}`\n\n"
            "Paste this ID in Awax (Profile → Backup channel) and make this bot an admin of the channel."
        )


@app.on_message((filters.document | filters.video | filters.photo) & filters.private)
async def upload_to_cloud(client, message):
    try:
        await message.forward(DEFAULT_CLOUD_CHANNEL_ID)
    except Exception as e:
        await message.reply_text(f"Error: {e}")


# ───────────────────────────── Web upload ─────────────────────────────
async def web_upload(request):
    uid, album, user_name = "public", "All Photos", "Unknown"
    id_token, user_channel = "", None
    display_name, work_dir, temp_path, file_size = "", None, "", 0
    verified = None

    try:
        reader = await request.multipart()
        async for field in reader:
            if field.name == "file":
                # Everything except the file is sent first, so we can authorise before reading it
                if id_token:
                    verified = await verify_token(id_token)
                    if not verified:
                        return web.json_response({"status": "error", "message": "Invalid or expired sign-in"}, status=401)
                    uid = verified
                elif REQUIRE_AUTH:
                    return web.json_response({"status": "error", "message": "Sign-in required"}, status=401)
                if not UID_RE.match(uid):
                    return web.json_response({"status": "error", "message": "Bad uid"}, status=400)

                display_name = clean_filename(field.filename)
                work_dir = os.path.join(TEMP_ROOT, uuid.uuid4().hex)
                os.makedirs(work_dir, exist_ok=True)
                temp_path = os.path.join(work_dir, display_name)
                limit = MAX_UPLOAD_MB * 1024 * 1024
                with open(temp_path, "wb") as f:
                    while True:
                        chunk = await field.read_chunk()
                        if not chunk:
                            break
                        file_size += len(chunk)
                        if file_size > limit:
                            return web.json_response({"status": "error", "message": f"File over {MAX_UPLOAD_MB} MB"}, status=413)
                        f.write(chunk)
            else:
                value = (await field.read()).decode("utf-8", "ignore").strip()
                if field.name == "uid":
                    uid = value
                elif field.name == "album":
                    album = clean_text(value, 60)
                elif field.name == "user_name":
                    user_name = clean_text(value, 60)
                elif field.name == "id_token":
                    id_token = value
                elif field.name == "channel_id":
                    user_channel = parse_channel(value)

        if not temp_path or file_size == 0:
            return web.json_response({"status": "error", "message": "No file"}, status=400)

        caption = f"👤 User: {user_name}\n📁 Collection: {album}\n📄 File: {display_name}"

        # A) main cloud channel
        msg, kind, file_id, thumb_id, duration = await send_to_cloud(temp_path, display_name, caption)

        # B) optional copy into the user's own backup channel
        backup, backup_error = "skipped", ""
        if user_channel is not None and user_channel != DEFAULT_CLOUD_CHANNEL_ID:
            try:
                try:
                    await app.get_chat(user_channel)  # lets Pyrogram resolve the channel before copying
                except Exception:
                    pass
                await app.copy_message(chat_id=user_channel, from_chat_id=DEFAULT_CLOUD_CHANNEL_ID, message_id=msg.id)
                backup = "ok"
            except Exception as e:
                backup, backup_error = "failed", str(e)[:200]
                log.warning("Backup copy to %s failed: %s", user_channel, e)

        # C) metadata in Firebase
        file_data = {
            "id": file_id,
            "name": display_name,
            "type": kind,
            "album": album,
            "thumb_id": thumb_id,
            "size": file_size,  # original size of the uploaded file
            "duration": duration,
            "timestamp": int(time.time() * 1000),
        }
        async with HTTP.post(fb(f"users/{uid}/files"), json=file_data) as r:
            if r.status >= 300:
                raise RuntimeError(f"Firebase write failed: HTTP {r.status}")

        return web.json_response({
            "status": "success", "file_id": file_id, "type": kind,
            "backup": backup, "backup_error": backup_error,
        })
    except Exception as e:
        log.exception("Upload failed")
        return web.json_response({"status": "error", "message": str(e)}, status=500)
    finally:
        if work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)


# ───────────────────────────── Streaming (Range + HEAD) ─────────────────────────────
def _convert_sync(src, dst, width):
    with Image.open(src) as im:
        im = ImageOps.exif_transpose(im)  # respect phone rotation
        if im.mode != "RGB":
            im = im.convert("RGB")
        if width and im.width > width:
            im = im.resize((width, max(1, round(im.height * width / im.width))), Image.LANCZOS)
        im.save(dst, "JPEG", quality=88 if width else 94, optimize=True)


def trim_cache():
    try:
        items = []
        for n in os.listdir(CACHE_DIR):
            p = os.path.join(CACHE_DIR, n)
            if n.endswith(".jpg"):
                items.append((os.path.getmtime(p), os.path.getsize(p), p))
        total, limit = sum(i[1] for i in items), MAX_CACHE_MB * 1024 * 1024
        for _, size, p in sorted(items):  # oldest first
            if total <= limit:
                break
            try:
                os.remove(p)
                total -= size
            except OSError:
                pass
    except Exception as e:
        log.warning("Cache trim failed: %s", e)


async def convert_to_jpeg(file_id, out, width):
    src, tmp = out + ".src", out + ".tmp.jpg"
    try:
        with open(src, "wb") as f:
            async for chunk in app.stream_media(file_id):
                f.write(chunk)
        await asyncio.get_running_loop().run_in_executor(None, _convert_sync, src, tmp, width)
        os.replace(tmp, out)
    finally:
        for p in (src, tmp):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
    trim_cache()


async def serve_jpeg(request, file_id):
    if not HEIF_OK:
        return web.json_response({"status": "error", "message": "Server needs: pip install pillow pillow-heif"}, status=501)
    try:
        width = max(0, min(int(request.query.get("w", "0")), 4096))
    except ValueError:
        width = 0
    out = os.path.join(CACHE_DIR, f"{file_id}_{width}.jpg")
    try:
        if not os.path.exists(out):
            lock = CONVERT_LOCKS.setdefault(out, asyncio.Lock())
            async with lock:
                if not os.path.exists(out):
                    async with CONVERT_SEM:
                        await convert_to_jpeg(file_id, out, width)
            CONVERT_LOCKS.pop(out, None)
        os.utime(out)  # keeps recently used files in the cache
    except Exception as e:
        log.warning("JPEG conversion failed for %s: %s", file_id, e)
        return web.json_response({"status": "error", "message": "Could not convert this image"}, status=415)
    return web.FileResponse(out, headers={"Content-Type": "image/jpeg", "Cache-Control": "private, max-age=86400"})


def parse_range(header, size):
    """Return (start, end), None for no/invalid header, or 'bad' for unsatisfiable."""
    m = re.fullmatch(r"bytes=(\d*)-(\d*)", (header or "").strip())
    if not m or (m.group(1) == "" and m.group(2) == ""):
        return None
    s, e = m.groups()
    if s == "":
        start, end = max(0, size - int(e)), size - 1
    else:
        start = int(s)
        end = min(int(e), size - 1) if e else size - 1
    if start >= size or start > end:
        return "bad"
    return start, end


async def iter_bytes(file_id, start, end):
    first = start // CHUNK
    skip = start - first * CHUNK
    remaining = end - start + 1
    limit = (skip + remaining + CHUNK - 1) // CHUNK
    async for chunk in app.stream_media(file_id, limit=limit, offset=first):
        if skip:
            chunk, skip = chunk[skip:], 0
        if len(chunk) > remaining:
            chunk = chunk[:remaining]
        if chunk:
            remaining -= len(chunk)
            yield chunk
        if remaining <= 0:
            break


async def stream_file(request):
    file_id = request.match_info.get("file_id", "")
    file_name = request.match_info.get("file_name", "file")
    if not FILE_ID_RE.match(file_id):
        return web.Response(text="Bad file id", status=400)
    if request.query.get("fmt") == "jpg":  # HEIC/HEIF/TIFF -> JPEG for browsers
        return await serve_jpeg(request, file_id)

    ctype = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
    disp = "attachment" if request.query.get("download") == "1" else "inline"
    ascii_name = re.sub(r"[^\w.\- ]", "_", file_name) or "file"
    headers = {
        "Content-Type": ctype,
        "Content-Disposition": f"{disp}; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(file_name)}",
        "Cache-Control": "private, max-age=86400",
    }

    size = await index_get(file_id)
    status, start, end = 200, 0, (size - 1 if size else None)

    if size:
        headers["Accept-Ranges"] = "bytes"
        rng = parse_range(request.headers.get("Range"), size)
        if rng == "bad":
            return web.Response(status=416, headers={"Content-Range": f"bytes */{size}"})
        if rng:
            status, (start, end) = 206, rng
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        headers["Content-Length"] = str(end - start + 1)
    else:
        headers["Accept-Ranges"] = "none"  # older uploads without a size record: plain stream

    response = web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)
    if request.method == "HEAD":
        return response

    try:
        if size:
            async for chunk in iter_bytes(file_id, start, end):
                await response.write(chunk)
        else:
            async for chunk in app.stream_media(file_id):
                await response.write(chunk)
        await response.write_eof()
    except (ConnectionResetError, asyncio.CancelledError):
        pass  # client closed the connection (seeking, navigating away)
    except Exception as e:
        log.warning("Stream error for %s: %s", file_id, e)
    return response


async def ping(request):
    return web.json_response({"status": "online", "message": "Backend is running!"})


# ───────────────────────────── Main ─────────────────────────────
async def main():
    global HTTP
    log.info("Starting bot and API server...")
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
    await app.start()
    # Fresh session (e.g. after a Render redeploy) has an empty peer cache, so resolve the channel up front
    try:
        chat = await app.get_chat(DEFAULT_CLOUD_CHANNEL_ID)
        log.info("Cloud channel OK: %s", getattr(chat, "title", chat.id))
    except Exception as e:
        log.error("Cannot access cloud channel %s: %s  -> make the bot an ADMIN of that channel", DEFAULT_CLOUD_CHANNEL_ID, e)

    server = web.Application(middlewares=[error_middleware], client_max_size=(MAX_UPLOAD_MB + 10) * 1024 * 1024)
    server.on_response_prepare.append(add_cors)
    server.router.add_get("/", ping)
    server.router.add_post("/upload", web_upload)
    server.router.add_get("/stream/{file_id}/{file_name}", stream_file)  # also answers HEAD

    runner = web.AppRunner(server)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("Backend API is live on port %s (auth required: %s)", port, REQUIRE_AUTH)

    await idle()
    await app.stop()
    await runner.cleanup()
    await HTTP.close()


if __name__ == "__main__":
    loop.run_until_complete(main())
