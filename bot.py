#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import print_function

import html
import copy
import json
import logging
import os
import threading
import random
import socket
import sqlite3
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from queue import Queue
except ImportError:
    from Queue import Queue


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_env(path):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_admin_ids(value):
    result = set()
    if not value:
        return result
    for item in value.split(","):
        item = item.strip()
        if item:
            result.add(int(item))
    return result


def abs_path(path):
    if os.path.isabs(path):
        return path
    return os.path.join(BASE_DIR, path)


load_env(os.path.join(BASE_DIR, ".env"))

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_IDS = parse_admin_ids(os.environ.get("ADMIN_IDS", ""))
BOT_USERNAME = os.environ.get("BOT_USERNAME", "").strip().lstrip("@")
DATABASE_PATH = abs_path(os.environ.get("DATABASE_PATH", "data/bot.sqlite3"))
LOG_FILE = abs_path(os.environ.get("LOG_FILE", "logs/bot.log"))
API_TIMEOUT = int(os.environ.get("API_TIMEOUT", "20") or 20)
POLL_TIMEOUT = int(os.environ.get("POLL_TIMEOUT", "20") or 20)
UPDATE_WORKERS = int(os.environ.get("UPDATE_WORKERS", "16") or 16)
MAX_INFLIGHT_UPDATES = int(os.environ.get("MAX_INFLIGHT_UPDATES", str(UPDATE_WORKERS * 4)) or (UPDATE_WORKERS * 4))
API_BASE = "https://api.telegram.org/bot{0}/".format(BOT_TOKEN)

ADMIN_STATES = {}
VERIFY_STATES = {}
CLEANUP_MESSAGES = {}
DELETE_QUEUE = Queue()
DELETE_WORKER_STARTED = False
USER_LOCKS = {}
USER_LOCKS_GUARD = threading.Lock()
INFLIGHT_SEMAPHORE = threading.BoundedSemaphore(MAX_INFLIGHT_UPDATES)
UPDATE_EXECUTOR = None
CAPTCHA_TTL_SECONDS = 120


def ensure_dirs():
    for path in (DATABASE_PATH, LOG_FILE):
        directory = os.path.dirname(path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)


def setup_logging():
    ensure_dirs()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def now_text():
    return datetime.utcnow().replace(microsecond=0).isoformat(" ")


def db_connect():
    conn = sqlite3.connect(DATABASE_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def ensure_column(conn, table, column, ddl):
    columns = [row["name"] for row in conn.execute("PRAGMA table_info({0})".format(table)).fetchall()]
    if column not in columns:
        conn.execute(ddl)


def init_db():
    with db_connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                batch_type TEXT NOT NULL,
                shared_code TEXT,
                usage_limit INTEGER,
                usage_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                required_group_id TEXT,
                required_group_messages INTEGER NOT NULL DEFAULT 0,
                required_channel_id TEXT,
                required_channel_link TEXT,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                CHECK (batch_type IN ('usage', 'unique')),
                CHECK (status IN ('active', 'disabled'))
            );

            CREATE TABLE IF NOT EXISTS batch_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'available',
                claimed_by INTEGER,
                claimed_at TEXT,
                created_at TEXT NOT NULL,
                UNIQUE (batch_id, code),
                CHECK (status IN ('available', 'claimed')),
                FOREIGN KEY (batch_id) REFERENCES batches(id)
            );

            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS claim_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                username TEXT,
                batch_id INTEGER,
                batch_token TEXT,
                code TEXT,
                status TEXT NOT NULL,
                captcha_passed INTEGER NOT NULL DEFAULT 0,
                reason TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS user_chat_stats (
                telegram_id INTEGER NOT NULL,
                chat_id TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (telegram_id, chat_id)
            );

            CREATE INDEX IF NOT EXISTS idx_batches_token ON batches(token);
            CREATE INDEX IF NOT EXISTS idx_batch_codes_batch_status ON batch_codes(batch_id, status, id);
            CREATE INDEX IF NOT EXISTS idx_claim_logs_user ON claim_logs(telegram_id);
            CREATE INDEX IF NOT EXISTS idx_claim_logs_batch_user_status
                ON claim_logs(batch_id, telegram_id, status);
            CREATE INDEX IF NOT EXISTS idx_user_chat_stats_chat ON user_chat_stats(chat_id, telegram_id);
            """
        )
        ensure_column(conn, "batches", "required_group_id", "ALTER TABLE batches ADD COLUMN required_group_id TEXT")
        ensure_column(
            conn,
            "batches",
            "required_group_messages",
            "ALTER TABLE batches ADD COLUMN required_group_messages INTEGER NOT NULL DEFAULT 0",
        )
        ensure_column(conn, "batches", "required_channel_id", "ALTER TABLE batches ADD COLUMN required_channel_id TEXT")
        ensure_column(conn, "batches", "required_channel_link", "ALTER TABLE batches ADD COLUMN required_channel_link TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_infos (
                chat_id TEXT PRIMARY KEY,
                title TEXT,
                username TEXT,
                chat_type TEXT,
                last_seen_at TEXT NOT NULL
            )
            """
        )


def api_call(method, data=None, timeout=None):
    if data is None:
        data = {}
    body = urlencode(data).encode("utf-8")
    request = Request(API_BASE + method, data=body)
    request_timeout = timeout if timeout is not None else API_TIMEOUT
    with urlopen(request, timeout=request_timeout) as response:
        payload = response.read().decode("utf-8")
    result = json.loads(payload)
    if not result.get("ok"):
        raise RuntimeError("Telegram API error: {0}".format(result))
    return result["result"]


def send_message(chat_id, text, reply_markup=None):
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    return api_call("sendMessage", data)


def delete_message_now(chat_id, message_id):
    if not chat_id or not message_id:
        return False
    try:
        api_call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
        return True
    except Exception as exc:
        logging.debug("deleteMessage ignored chat=%s message=%s error=%s", chat_id, message_id, exc)
        return False


def delete_worker():
    while True:
        chat_id, message_id = DELETE_QUEUE.get()
        try:
            delete_message_now(chat_id, message_id)
        except Exception as exc:
            logging.debug("delete worker ignored chat=%s message=%s error=%s", chat_id, message_id, exc)
        finally:
            DELETE_QUEUE.task_done()


def start_delete_worker():
    global DELETE_WORKER_STARTED
    if DELETE_WORKER_STARTED:
        return
    DELETE_WORKER_STARTED = True
    worker = threading.Thread(target=delete_worker, name="delete-message-worker")
    worker.daemon = True
    worker.start()


def delete_message(chat_id, message_id):
    if not chat_id or not message_id:
        return False
    try:
        DELETE_QUEUE.put_nowait((chat_id, message_id))
        return True
    except Exception as exc:
        logging.debug("deleteMessage enqueue ignored chat=%s message=%s error=%s", chat_id, message_id, exc)
        return False


def cleanup_key(chat_id, user_id=None):
    return "{0}:{1}".format(chat_id, user_id or chat_id)


def send_flow_message(chat_id, user_id, text, reply_markup=None, replace_previous=True):
    key = cleanup_key(chat_id, user_id)
    if replace_previous:
        delete_message(chat_id, CLEANUP_MESSAGES.pop(key, None))
    result = send_message(chat_id, text, reply_markup)
    message_id = result.get("message_id") if isinstance(result, dict) else None
    if message_id:
        CLEANUP_MESSAGES[key] = message_id
    return result


def clear_flow_message(chat_id, user_id):
    delete_message(chat_id, CLEANUP_MESSAGES.pop(cleanup_key(chat_id, user_id), None))


def answer_callback_query(callback_query_id, text=None, show_alert=False):
    data = {"callback_query_id": callback_query_id}
    if text:
        data["text"] = text
    if show_alert:
        data["show_alert"] = "true"
    return api_call("answerCallbackQuery", data)


def answer_inline_query(inline_query_id, results, cache_time=0, is_personal=True):
    data = {
        "inline_query_id": inline_query_id,
        "results": json.dumps(results, ensure_ascii=False),
        "cache_time": cache_time,
    }
    if is_personal:
        data["is_personal"] = "true"
    return api_call("answerInlineQuery", data)


def edit_message_text(chat_id, message_id, text, reply_markup=None):
    data = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    return api_call("editMessageText", data)


def safe_edit_message_text(chat_id, message_id, text, reply_markup=None):
    try:
        return edit_message_text(chat_id, message_id, text, reply_markup)
    except Exception as exc:
        logging.warning("editMessageText ignored chat=%s message=%s error=%s", chat_id, message_id, exc)
        return None


def get_chat_member(chat_id, user_id):
    try:
        return api_call("getChatMember", {"chat_id": chat_id, "user_id": user_id})
    except Exception as exc:
        logging.warning("getChatMember failed chat=%s user=%s error=%s", chat_id, user_id, exc)
        return None


def get_chat(chat_id):
    try:
        return api_call("getChat", {"chat_id": chat_id})
    except Exception as exc:
        logging.warning("getChat failed chat=%s error=%s", chat_id, exc)
        return None


def get_setting(conn, key, default=""):
    row = conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    return row["value"]


def set_setting(conn, key, value):
    conn.execute(
        "INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)",
        (key, value if value is not None else ""),
    )


def parse_nullable_text(value):
    value = (value or "").strip()
    if value in ("", "0", "-", "无", "none", "None"):
        return None
    return value


def is_invite_link(value):
    value = (value or "").strip()
    return value.startswith("https://t.me/+") or value.startswith("http://t.me/+") or "t.me/joinchat/" in value


def public_tme_username(value):
    value = (value or "").strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if value.startswith(prefix):
            slug = value[len(prefix):].strip("/")
            if slug and not slug.startswith("+") and not slug.startswith("joinchat/") and "/" not in slug:
                return slug
    return None


def default_subscription_link(target):
    if not target:
        return None
    if str(target).startswith("@"):
        return "https://t.me/{0}".format(str(target)[1:])
    return None


def normalize_chat_id_text(value):
    value = parse_nullable_text(value)
    if not value:
        return None
    if value.isdigit() and value.startswith("100") and len(value) > 10:
        return "-{0}".format(value)
    return value


def remember_chat_info(chat):
    if not chat or not chat.get("id"):
        return
    chat_id = str(chat.get("id"))
    title = chat.get("title") or chat.get("username") or chat_id
    current = now_text()
    with db_connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO chat_infos
                (chat_id, title, username, chat_type, last_seen_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (chat_id, title, chat.get("username"), chat.get("type"), current),
        )


def validate_group_chat_id(value):
    group_id = normalize_chat_id_text(value)
    if not group_id:
        return None
    if group_id.isdigit():
        raise ValueError(
            "这看起来是 Telegram 用户 ID，不是群聊 ID。\n\n"
            "群聊 ID 通常是负数，超级群一般是 <code>-100...</code>。\n"
            "请点击 <code>📊 已记录群</code> 复制正确的 Chat ID。"
        )
    if group_id.startswith("@"):
        raise ValueError(
            "群发言统计必须使用数字群 ID，不能使用 @用户名。\n\n"
            "请点击 <code>📊 已记录群</code> 复制 <code>-100...</code> 格式的 Chat ID。"
        )
    if not group_id.startswith("-") or not group_id[1:].isdigit():
        raise ValueError("群 ID 格式不正确，请填写 <code>-100...</code> 这样的数字群 ID。")

    chat = get_chat(group_id)
    if not chat:
        raise ValueError(
            "Bot 无法访问这个群 ID。\n\n"
            "请确认 Bot 已加入目标群，然后在群里发送一条普通消息，再回到私聊点击 <code>📊 已记录群</code> 复制 Chat ID。"
        )
    chat_type = chat.get("type")
    if chat_type not in ("group", "supergroup"):
        raise ValueError("这个 ID 不是群聊 ID，请填写目标群的 Chat ID。")
    remember_chat_info(chat)
    return str(chat.get("id"))


def data_value(data, key, default=None):
    try:
        return data.get(key, default)
    except AttributeError:
        try:
            return data[key]
        except Exception:
            return default


def parse_subscription_input(value):
    value = (value or "").strip()
    if parse_nullable_text(value) is None:
        return None, None

    parts = value.split()
    link = None
    target = None
    for part in parts:
        if part.startswith("https://") or part.startswith("http://") or part.startswith("t.me/"):
            link = part
            username = public_tme_username(part)
            if username and target is None:
                target = "@{0}".format(username)
        elif target is None:
            target = normalize_chat_id_text(part)

    if len(parts) == 1:
        single = parts[0]
        if is_invite_link(single):
            raise ValueError(
                "私密邀请链接不能单独用于检测订阅。\n\n"
                "请发送：<code>频道或群 chat_id 邀请链接</code>\n"
                "示例：<code>-1001234567890 https://t.me/+xxxx</code>"
            )
        username = public_tme_username(single)
        if username:
            target = "@{0}".format(username)
            link = single if single.startswith("http") else "https://t.me/{0}".format(username)
        else:
            target = normalize_chat_id_text(single)
            link = default_subscription_link(target)
    elif link and is_invite_link(link) and not target:
        raise ValueError("私密邀请链接需要同时填写可检测的频道或群 chat_id。")

    target = normalize_chat_id_text(target)
    link = parse_nullable_text(link)
    if target and (target.startswith("https://") or target.startswith("http://") or target.startswith("t.me/")):
        username = public_tme_username(target)
        if username:
            target = "@{0}".format(username)
            link = link or "https://t.me/{0}".format(username)
        else:
            raise ValueError("订阅检测目标不能只填写私密邀请链接，请同时填写 chat_id。")
    return target, link


def subscription_display_html(data):
    target = data_value(data, "required_channel_id")
    link = data_value(data, "required_channel_link") or default_subscription_link(target)
    if link:
        return '<a href="{0}">打开订阅入口</a>'.format(html.escape(str(link), quote=True))
    return "<b>{0}</b>".format(html.escape(str(target)))


def parse_nonnegative_int(value, field_name):
    try:
        number = int(str(value).strip())
    except Exception:
        raise ValueError("{0}必须是数字。".format(field_name))
    if number < 0:
        raise ValueError("{0}不能小于 0。".format(field_name))
    return number


def normalize_condition_data(data):
    group_messages = int(data.get("required_group_messages") or 0)
    group_id = normalize_chat_id_text(data.get("required_group_id"))
    if group_messages <= 0:
        group_id = None
        group_messages = 0
    elif not group_id:
        raise ValueError("启用群发言数条件时，必须填写群 ID。")
    channel_id = parse_nullable_text(data.get("required_channel_id"))
    channel_link = parse_nullable_text(data.get("required_channel_link"))
    if channel_id and (channel_id.startswith("http://") or channel_id.startswith("https://") or channel_id.startswith("t.me/")):
        try:
            channel_id, parsed_link = parse_subscription_input(channel_id)
            channel_link = channel_link or parsed_link
        except ValueError:
            channel_link = channel_link or channel_id
            channel_id = None
    if channel_id and not channel_link:
        channel_link = default_subscription_link(channel_id)
    data["required_group_id"] = group_id
    data["required_group_messages"] = group_messages
    data["required_channel_id"] = channel_id
    data["required_channel_link"] = channel_link
    return data


def load_default_conditions(conn):
    data = {
        "required_group_id": parse_nullable_text(get_setting(conn, "default_required_group_id", "")),
        "required_group_messages": int(get_setting(conn, "default_required_group_messages", "0") or 0),
        "required_channel_id": parse_nullable_text(get_setting(conn, "default_required_channel_id", "")),
        "required_channel_link": parse_nullable_text(get_setting(conn, "default_required_channel_link", "")),
    }
    try:
        return normalize_condition_data(data)
    except ValueError:
        return {
            "required_group_id": None,
            "required_group_messages": 0,
            "required_channel_id": None,
            "required_channel_link": None,
        }


def render_condition_lines(data):
    group_id = data.get("required_group_id")
    group_messages = int(data.get("required_group_messages") or 0)
    channel_id = data.get("required_channel_id")
    lines = []
    if group_id and group_messages > 0:
        lines.append("• <b>群发言数</b>：{0} 中普通发言 <b>至少 {1}</b>".format(html.escape(str(group_id)), group_messages))
    else:
        lines.append("• <b>群发言数</b>：<i>未启用</i>")
    if channel_id:
        lines.append("• <b>频道订阅</b>：必须订阅 {0}".format(subscription_display_html(data)))
        lines.append("  检测目标：<code>{0}</code>".format(html.escape(str(channel_id))))
    else:
        lines.append("• <b>频道订阅</b>：<i>未启用</i>")
    return lines


def render_condition_summary(data, title="领取条件"):
    return "<b>⚙️ {0}</b>\n{1}".format(title, "\n".join(render_condition_lines(data)))


def batch_type_label(batch_type):
    if batch_type == "usage":
        return "🔁 使用次数"
    if batch_type == "unique":
        return "🎁 领完为止"
    return html.escape(str(batch_type or "-"))


def status_label(status):
    if status == "active":
        return "✅ 启用中"
    if status == "disabled":
        return "⏸ 已停用"
    return html.escape(str(status or "-"))


def render_defaults_summary(data):
    return (
        "<b>⚙️ 默认领取条件</b>\n\n"
        + "\n".join(render_condition_lines(data))
        + "\n\n"
        "<blockquote>默认条件会自动带入新批次；已经创建的批次不会被影响。</blockquote>"
    )


def render_batch_summary(data):
    lines = [
        "<b>📦 批次预览</b>",
        "",
        "• <b>名称</b>：{0}".format(html.escape(data.get("name") or "-")),
        "• <b>类型</b>：{0}".format(batch_type_label(data.get("batch_type"))),
    ]
    if data.get("batch_type") == "usage":
        lines.append("• <b>可领取次数</b>：{0}".format(int(data.get("usage_limit") or 0)))
        if data.get("shared_code"):
            lines.append("• <b>兑换码</b>：<code>{0}</code>".format(html.escape(data.get("shared_code"))))
    else:
        codes = data.get("codes") or []
        lines.append("• <b>导入数量</b>：{0}".format(len(codes)))
        lines.append("• <b>重复跳过</b>：{0}".format(int(data.get("duplicated") or 0)))
    lines.extend(["", render_condition_summary(data)])
    return "\n".join(lines)


def show_defaults_screen(chat_id, user_id, message_id=None):
    state = ADMIN_STATES.get(user_id)
    data = state.get("data") if state else None
    if not data:
        with db_connect() as conn:
            data = load_default_conditions(conn)
    text = render_defaults_summary(data) + "\n\n<i>点击按钮修改默认值。</i>"
    if message_id:
        result = safe_edit_message_text(chat_id, message_id, text, defaults_keyboard())
        if result:
            CLEANUP_MESSAGES[cleanup_key(chat_id, user_id)] = message_id
        else:
            send_flow_message(chat_id, user_id, text, defaults_keyboard())
    else:
        send_flow_message(chat_id, user_id, text, defaults_keyboard())


def show_batch_condition_screen(chat_id, user_id, message_id=None):
    state = ADMIN_STATES.get(user_id)
    if not state:
        send_message(chat_id, "请先重新开始创建批次。", admin_keyboard())
        return
    text = (
        "<b>⚙️ 第 3 步 / 共 5 步</b>\n\n"
        "<b>当前领取条件</b>\n"
        + "\n".join(render_condition_lines(state["data"]))
        + "\n\n"
        "<blockquote>群发言条件使用“群 ID”；频道订阅条件使用频道/群的订阅检测 ID。两者可以不同，请分别设置。</blockquote>"
    )
    if message_id:
        result = safe_edit_message_text(chat_id, message_id, text, condition_edit_keyboard())
        if result:
            CLEANUP_MESSAGES[cleanup_key(chat_id, user_id)] = message_id
        else:
            send_flow_message(chat_id, user_id, text, condition_edit_keyboard())
    else:
        send_flow_message(chat_id, user_id, text, condition_edit_keyboard())


def show_batch_preview(chat_id, user_id, message_id=None):
    state = ADMIN_STATES.get(user_id)
    if not state:
        send_message(chat_id, "请先重新开始创建批次。", admin_keyboard())
        return
    text = (
        render_batch_summary(state["data"])
        + "\n\n"
        "<b>请确认以上配置</b>\n"
        "<i>确认后会写入库存并生成专属领取链接。</i>"
    )
    if message_id:
        result = safe_edit_message_text(chat_id, message_id, text, confirm_keyboard())
        if result:
            CLEANUP_MESSAGES[cleanup_key(chat_id, user_id)] = message_id
        else:
            send_flow_message(chat_id, user_id, text, confirm_keyboard())
    else:
        send_flow_message(chat_id, user_id, text, confirm_keyboard())


def admin_keyboard():
    return {
        "keyboard": [
            [{"text": "📦 创建批次"}, {"text": "📋 批次列表"}],
            [{"text": "⚙️ 默认条件"}, {"text": "🧭 核心流程"}],
            [{"text": "📊 已记录群"}, {"text": "🛡 接收状态"}],
            [{"text": "🗒 最近记录"}],
            [{"text": "⬅️ 取消"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def batch_type_keyboard():
    return {
        "keyboard": [
            [{"text": "🔁 使用次数"}, {"text": "🎁 领完为止"}],
            [{"text": "⬅️ 返回"}, {"text": "⬅️ 取消"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def condition_edit_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "💬 群发言条件", "callback_data": "draft:group_condition"},
                {"text": "📢 频道订阅", "callback_data": "draft:channel_id"},
            ],
            [
                {"text": "📥 载入默认", "callback_data": "draft:load_defaults"},
                {"text": "🧹 清空", "callback_data": "draft:clear_conditions"},
            ],
            [
                {"text": "✅ 继续", "callback_data": "draft:continue"},
                {"text": "⚙️ 默认值", "callback_data": "draft:open_defaults"},
            ],
            [
                {"text": "⬅️ 上一步", "callback_data": "draft:back_type"},
            ],
        ]
    }


def defaults_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "💬 群发言条件", "callback_data": "defaults:group_condition"},
                {"text": "📢 频道订阅", "callback_data": "defaults:channel_id"},
            ],
            [
                {"text": "🧹 清空", "callback_data": "defaults:clear"},
                {"text": "✅ 完成", "callback_data": "defaults:done"},
            ],
        ]
    }


def confirm_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "✅ 确认创建", "callback_data": "draft:confirm"},
                {"text": "✏️ 返回条件", "callback_data": "draft:edit_conditions"},
            ],
            [
                {"text": "✏️ 返回兑换码", "callback_data": "draft:edit_codes"},
                {"text": "⬅️ 返回类型", "callback_data": "draft:edit_type"},
            ],
        ]
    }


def admin_keyboard():
    return {
        "keyboard": [
            [{"text": "📦 创建批次"}, {"text": "📋 批次列表"}, {"text": "🗒 最近记录"}],
            [{"text": "⚙️ 默认条件"}, {"text": "🧩 核心流程"}, {"text": "📊 已记录群"}],
            [{"text": "🛡 接收状态"}, {"text": "⬅️ 取消"}, {"text": "👤 我的ID"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def batch_type_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "🔁 使用次数", "callback_data": "draft:type_usage"},
                {"text": "🎁 领完为止", "callback_data": "draft:type_unique"},
            ],
            [{"text": "❌ 取消", "callback_data": "draft:cancel"}],
        ]
    }


def draft_exit_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "❌ 取消", "callback_data": "draft:cancel"}],
        ]
    }


def draft_input_keyboard(back_data=None):
    rows = [[{"text": "❌ 取消", "callback_data": "draft:cancel"}]]
    if back_data:
        rows = [[
            {"text": "⬅️ 返回", "callback_data": back_data},
            {"text": "❌ 取消", "callback_data": "draft:cancel"},
        ]]
    return {"inline_keyboard": rows}


def condition_edit_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "💬 群发言条件", "callback_data": "draft:group_condition"},
                {"text": "📣 频道订阅", "callback_data": "draft:channel_id"},
            ],
            [
                {"text": "📥 载入默认", "callback_data": "draft:load_defaults"},
                {"text": "🧹 清空条件", "callback_data": "draft:clear_conditions"},
            ],
            [
                {"text": "✅ 继续", "callback_data": "draft:continue"},
                {"text": "⚙️ 默认值", "callback_data": "draft:open_defaults"},
            ],
            [
                {"text": "⬅️ 返回类型", "callback_data": "draft:back_type"},
                {"text": "❌ 退出创建", "callback_data": "draft:cancel"},
            ],
        ]
    }


def confirm_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "✅ 确认创建", "callback_data": "draft:confirm"},
                {"text": "✏️ 修改条件", "callback_data": "draft:edit_conditions"},
            ],
            [
                {"text": "✏️ 修改兑换码", "callback_data": "draft:edit_codes"},
                {"text": "⬅️ 返回类型", "callback_data": "draft:edit_type"},
            ],
            [{"text": "❌ 退出创建", "callback_data": "draft:cancel"}],
        ]
    }


def make_captcha():
    symbols = ["◆", "◇", "●", "○", "▲", "△", "■", "□", "★", "☆", "✚", "✦"]
    labels = random.sample(symbols, 9)
    target = random.choice(labels)
    choices = []
    answer_key = None
    for label in labels:
        choice_key = uuid.uuid4().hex[:8]
        if label == target:
            answer_key = choice_key
        choices.append({"label": label, "key": choice_key})
    random.shuffle(choices)
    return target, answer_key, choices


def verify_keyboard(token, choices):
    rows = []
    for index in range(0, len(choices), 3):
        row = []
        for choice in choices[index:index + 3]:
            row.append(
                {
                    "text": choice["label"],
                    "callback_data": "captcha:{0}:{1}".format(token, choice["key"]),
                }
            )
        rows.append(row)
    return {
        "inline_keyboard": rows
    }


def is_admin(user_id):
    return user_id in ADMIN_IDS


def is_private_chat(chat):
    return (chat or {}).get("type") == "private"


def upsert_user(conn, user):
    current = now_text()
    conn.execute(
        """
        INSERT OR IGNORE INTO users (telegram_id, username, first_name, created_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (user["id"], user.get("username"), user.get("first_name"), current, current),
    )
    conn.execute(
        """
        UPDATE users
        SET username = ?, first_name = ?, last_seen_at = ?
        WHERE telegram_id = ?
        """,
        (user.get("username"), user.get("first_name"), current, user["id"]),
    )


def create_batch_link(token):
    if not BOT_USERNAME:
        return "请先在 .env 配置 BOT_USERNAME 后重启 Bot。批次参数：{0}".format(token)
    return "https://t.me/{0}?start={1}".format(BOT_USERNAME, token)


def condition_lines(batch):
    lines = []
    if batch["required_group_id"] and (batch["required_group_messages"] or 0) > 0:
        lines.append(
            "• <b>群发言数</b>：{0} 中普通发言 <b>至少 {1}</b>".format(
                html.escape(str(batch["required_group_id"])),
                batch["required_group_messages"],
            )
        )
    if batch["required_channel_id"]:
        lines.append("• <b>频道订阅</b>：必须订阅 {0}".format(subscription_display_html(batch)))
    if not lines:
        lines.append("• <b>领取条件</b>：<i>无额外条件</i>")
    return ["<b>⚙️ 领取条件</b>"] + lines


def is_command_message(message):
    text = (message.get("text") or message.get("caption") or "").strip()
    return text.startswith("/")


def has_countable_group_content(message):
    if is_command_message(message):
        return False
    content_keys = (
        "text",
        "audio",
        "document",
        "animation",
        "game",
        "photo",
        "sticker",
        "video",
        "voice",
        "video_note",
        "caption",
        "contact",
        "dice",
        "poll",
        "venue",
        "location",
    )
    for key in content_keys:
        if message.get(key) is not None:
            return True
    return False


def record_group_message(message):
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    chat_type = chat.get("type")
    if chat_type not in ("group", "supergroup"):
        return
    if not user.get("id") or user.get("is_bot"):
        return
    current = now_text()
    chat_id = str(chat.get("id"))
    title = chat.get("title") or chat.get("username") or chat_id
    with db_connect() as conn:
        upsert_user(conn, user)
        conn.execute(
            """
            INSERT OR REPLACE INTO chat_infos
                (chat_id, title, username, chat_type, last_seen_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (chat_id, title, chat.get("username"), chat_type, current),
        )
        if not has_countable_group_content(message):
            return
        conn.execute(
            """
            INSERT OR IGNORE INTO user_chat_stats
                (telegram_id, chat_id, message_count, first_seen_at, last_seen_at)
            VALUES (?, ?, 0, ?, ?)
            """,
            (user["id"], chat_id, current, current),
        )
        conn.execute(
            """
            UPDATE user_chat_stats
            SET message_count = message_count + 1, last_seen_at = ?
            WHERE telegram_id = ? AND chat_id = ?
            """,
            (current, user["id"], chat_id),
        )


def member_is_joined(member):
    if not member:
        return False
    status = member.get("status")
    if status in ("creator", "administrator", "member"):
        return True
    if status == "restricted":
        return bool(member.get("is_member", True))
    return False


def check_claim_conditions(conn, user, batch):
    group_id = batch["required_group_id"]
    group_messages = batch["required_group_messages"] or 0
    if group_id and group_messages > 0:
        if str(group_id).isdigit():
            return (
                False,
                "group_id_invalid",
                "<b>领取失败</b>\n\n"
                "当前批次的群发言条件配置错误：<code>{0}</code> 是正数 ID，通常是用户 ID，不是群聊 ID。\n\n"
                "请管理员重新创建或修改批次，使用 <code>📊 已记录群</code> 里的负数 Chat ID。".format(
                    html.escape(str(group_id))
                ),
            )
        row = conn.execute(
            """
            SELECT message_count
            FROM user_chat_stats
            WHERE telegram_id = ? AND chat_id = ?
            """,
            (user["id"], str(group_id)),
        ).fetchone()
        message_count = row["message_count"] if row else 0
        if message_count < group_messages:
            info = conn.execute(
                "SELECT title, username FROM chat_infos WHERE chat_id = ?",
                (str(group_id),),
            ).fetchone()
            group_title = info["title"] if info and info["title"] else str(group_id)
            return (
                False,
                "group_messages_not_enough",
                "<b>领取失败</b>\n\n"
                "你在指定群聊 <b>{0}</b> 中的普通发言数是 <b>{1}</b>，需要至少 <b>{2}</b> 才能领取。\n\n"
                "如果这里一直是 0，请让管理员确认 BotFather 的 Privacy Mode 已关闭，并且 Bot 仍在目标群内。".format(
                    html.escape(group_title),
                    message_count,
                    group_messages,
                ),
            )

    channel_id = batch["required_channel_id"]
    if channel_id:
        if is_invite_link(str(channel_id)):
            return (
                False,
                "subscription_target_invalid",
                "<b>领取失败</b>\n\n当前频道订阅条件需要管理员重新配置：私密邀请链接不能单独作为检测目标。",
            )
        member = get_chat_member(channel_id, user["id"])
        if not member_is_joined(member):
            open_text = ""
            channel_link = data_value(batch, "required_channel_link") or default_subscription_link(channel_id)
            if channel_link:
                open_text = "\n\n订阅入口：{0}".format(subscription_display_html(batch))
            return (
                False,
                "channel_not_joined",
                "<b>领取失败</b>\n\n请先完成频道订阅，然后重新打开领取链接。{0}".format(open_text),
            )

    return True, None, None


def log_claim(conn, user, batch, code, status, captcha_passed, reason):
    conn.execute(
        """
        INSERT INTO claim_logs
            (telegram_id, username, batch_id, batch_token, code, status, captcha_passed, reason, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user["id"],
            user.get("username"),
            batch["id"] if batch else None,
            batch["token"] if batch else None,
            code,
            status,
            1 if captcha_passed else 0,
            reason,
            now_text(),
        ),
    )


def configure_bot_menu():
    user_commands = [
        {"command": "start", "description": "通过专属链接领取兑换码"},
        {"command": "whoami", "description": "查看自己的 Telegram User ID"},
    ]
    admin_commands = user_commands + [
        {"command": "admin", "description": "管理员面板"},
        {"command": "newbatch", "description": "创建兑换码批次"},
        {"command": "defaults", "description": "设置默认领取条件"},
        {"command": "batches", "description": "查看批次列表"},
        {"command": "batch", "description": "查看批次详情"},
        {"command": "records", "description": "查看最近领取记录"},
        {"command": "flow", "description": "查看核心流程"},
        {"command": "groups", "description": "查看已记录群 ID"},
        {"command": "botstatus", "description": "查看群消息接收状态"},
        {"command": "chatid", "description": "查看当前会话 ID"},
    ]
    api_call("deleteWebhook", {"drop_pending_updates": "false"})
    api_call("deleteMyCommands")
    api_call("deleteMyCommands", {"scope": json.dumps({"type": "all_group_chats"})})
    api_call("deleteMyCommands", {"scope": json.dumps({"type": "all_chat_administrators"})})
    api_call(
        "setMyCommands",
        {
            "scope": json.dumps({"type": "all_private_chats"}),
            "commands": json.dumps(user_commands, ensure_ascii=False),
        },
    )
    for admin_id in ADMIN_IDS:
        api_call(
            "setMyCommands",
            {
                "scope": json.dumps({"type": "chat", "chat_id": admin_id}),
                "commands": json.dumps(admin_commands, ensure_ascii=False),
            },
        )


def handle_admin(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员，无法使用管理功能。")
        return
    send_message(
        chat_id,
        "<b>🛠 管理员面板</b>\n\n"
        "<b>核心流程</b>\n"
        "创建批次 → 导入兑换码 → 设置条件 → 生成链接 → 分享链接 → 用户验证 → 自动发码 → 记录日志\n\n"
        "<i>请从下方按钮开始。</i>",
        admin_keyboard(),
    )


def handle_flow(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    send_message(
        chat_id,
        "<b>🧭 核心流程</b>\n\n"
        "1. 管理员点击 <code>📦 创建批次</code>\n"
        "2. 输入批次名称\n"
        "3. 选择批次类型\n"
        "4. 在条件面板里点选或修改默认值\n"
        "5. 导入兑换码并确认创建\n"
        "6. Bot 自动生成唯一领取链接\n"
        "7. 管理员复制链接发到频道、群聊或私聊\n"
        "8. 用户进入后先做九宫格点击验证\n"
        "9. 系统再校验领取条件并自动发码\n"
        "10. 全过程自动记录领取日志\n\n"
        "<i>普通用户没有领取入口，只能通过专属链接领取。</i>",
        admin_keyboard(),
    )


def list_batches(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    with db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, token, name, batch_type, usage_limit, usage_count, status, created_at
            FROM batches
            ORDER BY id DESC
            LIMIT 10
            """
        ).fetchall()
    if not rows:
        send_message(chat_id, "<b>📋 批次列表</b>\n\n<i>还没有批次。</i>", admin_keyboard())
        return
    lines = ["<b>📋 最近批次</b>"]
    for row in rows:
        if row["batch_type"] == "usage":
            stock = "已领 {0}/{1}".format(row["usage_count"], row["usage_limit"])
        else:
            with db_connect() as conn:
                stock_row = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END) AS available,
                        COUNT(*) AS total
                    FROM batch_codes
                    WHERE batch_id = ?
                    """,
                    (row["id"],),
                ).fetchone()
            stock = "剩余 {0}/{1}".format(stock_row["available"] or 0, stock_row["total"] or 0)
        lines.append(
            "\n<b>#{0} {1}</b>\n"
            "• 类型：{2}\n"
            "• 状态：{3}\n"
            "• 库存：{4}\n"
            "• 链接：<code>{5}</code>".format(
                row["id"],
                html.escape(row["name"]),
                batch_type_label(row["batch_type"]),
                status_label(row["status"]),
                stock,
                html.escape(create_batch_link(row["token"])),
            )
        )
    lines.append("\n<i>查看详情：发送 /batch 批次编号，例如 /batch 1</i>")
    send_message(chat_id, "\n".join(lines), admin_keyboard())


def batch_stock_text(conn, batch):
    if batch["batch_type"] == "usage":
        remaining = max((batch["usage_limit"] or 0) - (batch["usage_count"] or 0), 0)
        return "已领 {0}/{1}，剩余 {2}".format(
            batch["usage_count"] or 0,
            batch["usage_limit"] or 0,
            remaining,
        )
    stock_row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END) AS available,
            SUM(CASE WHEN status = 'claimed' THEN 1 ELSE 0 END) AS claimed,
            COUNT(*) AS total
        FROM batch_codes
        WHERE batch_id = ?
        """,
        (batch["id"],),
    ).fetchone()
    available = stock_row["available"] or 0
    claimed = stock_row["claimed"] or 0
    total = stock_row["total"] or 0
    return "已领 {0}/{1}，剩余 {2}".format(claimed, total, available)


def show_batch_detail(chat_id, user_id, text):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    parts = text.split()
    if len(parts) < 2:
        send_message(chat_id, "请发送：/batch 批次编号")
        return
    try:
        batch_id = int(parts[1])
    except ValueError:
        send_message(chat_id, "批次编号必须是数字。")
        return
    with db_connect() as conn:
        batch = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
        if not batch:
            send_message(chat_id, "没有找到这个批次。")
            return
        success_count = conn.execute(
            "SELECT COUNT(*) AS total FROM claim_logs WHERE batch_id = ? AND status = 'success'",
            (batch["id"],),
        ).fetchone()["total"]
        failed_count = conn.execute(
            "SELECT COUNT(*) AS total FROM claim_logs WHERE batch_id = ? AND status = 'failed'",
            (batch["id"],),
        ).fetchone()["total"]
        stock = batch_stock_text(conn, batch)
    lines = [
        "<b>📦 批次详情</b>",
        "",
        "• <b>编号</b>：#{0}".format(batch["id"]),
        "• <b>名称</b>：{0}".format(html.escape(batch["name"])),
        "• <b>类型</b>：{0}".format(batch_type_label(batch["batch_type"])),
        "• <b>状态</b>：{0}".format(status_label(batch["status"])),
        "• <b>库存</b>：{0}".format(html.escape(stock)),
        "",
        "\n".join(condition_lines(batch)),
        "",
        "<b>📊 领取统计</b>",
        "• 成功领取：{0}".format(success_count),
        "• 失败记录：{0}".format(failed_count),
        "• 创建时间：{0}".format(html.escape(batch["created_at"])),
        "",
        "<b>🔗 专属领取链接</b>",
        "<code>{0}</code>".format(html.escape(create_batch_link(batch["token"]))),
        "",
        "<i>把链接发到频道、群聊或私聊即可。用户进入后会先验证，再校验条件并自动发码。</i>",
    ]
    send_message(chat_id, "\n".join(lines), admin_keyboard())


def show_records(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    with db_connect() as conn:
        rows = conn.execute(
            """
            SELECT telegram_id, username, batch_token, code, status, captcha_passed, reason, created_at
            FROM claim_logs
            ORDER BY id DESC
            LIMIT 15
            """
        ).fetchall()
    if not rows:
        send_message(chat_id, "<b>🗒 最近领取记录</b>\n\n<i>暂无领取记录。</i>", admin_keyboard())
        return
    lines = ["<b>🗒 最近领取记录</b>"]
    for row in rows:
        username = "@{0}".format(row["username"]) if row["username"] else "-"
        result = row["code"] if row["status"] == "success" else row["reason"]
        lines.append(
            "\n• <b>{0}</b>｜{1}\n"
            "  用户：<code>{2}</code> {3}\n"
            "  结果：<code>{4}</code>".format(
                html.escape(row["created_at"]),
                html.escape(row["status"]),
                row["telegram_id"],
                html.escape(username),
                html.escape(result or "-"),
            )
        )
    send_message(chat_id, "\n".join(lines), admin_keyboard())


def show_seen_groups(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    with db_connect() as conn:
        rows = conn.execute(
            """
            SELECT
                c.chat_id,
                c.title,
                c.username,
                c.chat_type,
                c.last_seen_at,
                COUNT(s.telegram_id) AS user_count,
                COALESCE(SUM(s.message_count), 0) AS message_count
            FROM chat_infos c
            LEFT JOIN user_chat_stats s ON s.chat_id = c.chat_id
            GROUP BY c.chat_id, c.title, c.username, c.chat_type, c.last_seen_at
            ORDER BY c.last_seen_at DESC
            LIMIT 20
            """
        ).fetchall()
    if not rows:
        send_message(
            chat_id,
            "<b>📊 已记录群</b>\n\n"
            "<i>暂时没有记录到群聊消息。</i>\n\n"
            "请把 Bot 加入目标群。\n"
            "普通聊天统计需要在 BotFather 关闭 privacy mode；关闭后 Bot 会静默记录群内普通用户发言。",
            admin_keyboard(),
        )
        return
    lines = [
        "<b>📊 已记录群</b>",
        "<i>创建批次时，点击“群发言条件”后复制这里的 Chat ID。</i>",
        "<i>统计来源：Bot 收到的普通群消息；斜杠命令不会计入。</i>",
        "<i>如果这里没有目标群，请确认 Bot 在群内，且 Privacy Mode 已关闭后让群里发一条普通消息。</i>",
    ]
    for row in rows:
        username = "@{0}".format(row["username"]) if row["username"] else "-"
        lines.append(
            "\n<b>{0}</b>\n"
            "• Chat ID：<code>{1}</code>\n"
            "• 类型：{2}\n"
            "• 用户数：{3}\n"
            "• 已统计普通发言：{4}\n"
            "• 用户名：{5}\n"
            "• 最近记录：{6}".format(
                html.escape(row["title"] or row["chat_id"]),
                html.escape(row["chat_id"]),
                html.escape(row["chat_type"] or "-"),
                row["user_count"] or 0,
                row["message_count"] or 0,
                html.escape(username),
                html.escape(row["last_seen_at"] or "-"),
            )
        )
    send_message(chat_id, "\n".join(lines), admin_keyboard())


def show_bot_receive_status(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    try:
        bot_info = api_call("getMe")
    except Exception as exc:
        send_message(chat_id, "<b>🛡 接收状态</b>\n\n无法获取 Bot 状态：{0}".format(html.escape(str(exc))), admin_keyboard())
        return

    can_read = bot_info.get("can_read_all_group_messages")
    username = bot_info.get("username") or BOT_USERNAME or "-"
    if can_read is True:
        status_text = "✅ 已关闭 Privacy Mode，Bot 可以接收普通群消息。"
    elif can_read is False:
        status_text = "⚠️ Privacy Mode 仍开启，Bot 无法接收普通群消息。"
    else:
        status_text = "⚠️ Telegram 未返回明确状态，请以 BotFather 设置为准。"

    send_message(
        chat_id,
        "<b>🛡 群消息接收状态</b>\n\n"
        "• Bot：@{0}\n"
        "• 普通群消息接收：{1}\n\n"
        "<b>BotFather 设置路径</b>\n"
        "1. 打开 @BotFather\n"
        "2. 发送 <code>/mybots</code>\n"
        "3. 选择当前 Bot\n"
        "4. Bot Settings → Group Privacy\n"
        "5. 选择 <b>Turn off</b>\n\n"
        "<i>关闭后，把 Bot 留在目标群里。之后群内普通用户发言会被静默计数，Bot 不会回复。</i>".format(
            html.escape(username),
            status_text,
        ),
        admin_keyboard(),
    )


def forwarded_chat_from_message(message):
    chat = message.get("forward_from_chat")
    if chat:
        return chat
    origin = message.get("forward_origin") or {}
    if origin.get("chat"):
        return origin.get("chat")
    if origin.get("type") == "channel" and origin.get("chat"):
        return origin.get("chat")
    return None


def handle_forwarded_chat(chat_id, user_id, message):
    if not is_admin(user_id):
        return False
    forwarded_chat = forwarded_chat_from_message(message)
    if not forwarded_chat or not forwarded_chat.get("id"):
        return False
    target_id = str(forwarded_chat.get("id"))
    title = forwarded_chat.get("title") or forwarded_chat.get("username") or target_id
    username = forwarded_chat.get("username")
    current = now_text()
    with db_connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO chat_infos
                (chat_id, title, username, chat_type, last_seen_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (target_id, title, username, forwarded_chat.get("type"), current),
        )
    link = "https://t.me/{0}".format(username) if username else ""
    lines = [
        "<b>📌 已识别转发来源</b>",
        "",
        "• 名称：{0}".format(html.escape(title)),
        "• Chat ID：<code>{0}</code>".format(html.escape(target_id)),
    ]
    if link:
        lines.append("• 公开链接：<code>{0}</code>".format(html.escape(link)))
    lines.append("")
    lines.append("<i>私密邀请链接可按这个格式设置：</i>")
    lines.append("<code>{0} https://t.me/+xxxx</code>".format(html.escape(target_id)))
    send_message(chat_id, "\n".join(lines), admin_keyboard())
    return True


def find_batch(conn, token):
    return conn.execute("SELECT * FROM batches WHERE token = ?", (token,)).fetchone()


def begin_claim(chat_id, user, token):
    with db_connect() as conn:
        upsert_user(conn, user)
        batch = find_batch(conn, token)
        if not batch:
            log_claim(conn, user, None, None, "failed", 0, "link_not_found")
            send_message(chat_id, "<b>领取失败</b>\n\n领取链接不存在或已失效。")
            return
        if batch["status"] != "active":
            log_claim(conn, user, batch, None, "failed", 0, "batch_disabled")
            send_message(chat_id, "<b>领取失败</b>\n\n当前活动未开始、已结束或已失效。")
            return
        previous = conn.execute(
            """
            SELECT code
            FROM claim_logs
            WHERE batch_id = ? AND telegram_id = ? AND status = 'success'
            ORDER BY id DESC
            LIMIT 1
            """,
            (batch["id"], user["id"]),
        ).fetchone()
        if previous:
            send_message(
                chat_id,
                "<b>你已经领取过这个批次</b>\n\n兑换码：\n<code>{0}</code>".format(
                    html.escape(previous["code"] or "")
                ),
            )
            return

    verify_token = uuid.uuid4().hex[:16]
    target, answer_key, choices = make_captcha()
    VERIFY_STATES[user["id"]] = {
        "token": verify_token,
        "batch_token": token,
        "answer_key": answer_key,
        "expires_at": time.time() + CAPTCHA_TTL_SECONDS,
    }
    send_flow_message(
        chat_id,
        user["id"],
        "<b>🎁 准备领取</b>\n\n"
        "• 批次：<b>{0}</b>\n\n"
        "{1}\n\n"
        "<b>🛡 人机验证</b>\n"
        "请在下方九宫格中点击这个图案：<b>{2}</b>\n\n"
        "<i>验证码 2 分钟内有效，通过后会自动校验资格并发放兑换码。</i>".format(
            html.escape(batch["name"]),
            "\n".join(condition_lines(batch)),
            html.escape(target),
        ),
        verify_keyboard(verify_token, choices),
    )


def issue_code(chat_id, user, batch_token):
    conn = db_connect()
    try:
        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE")
        upsert_user(conn, user)
        batch = find_batch(conn, batch_token)
        if not batch:
            conn.execute("COMMIT")
            send_message(chat_id, "<b>领取失败</b>\n\n领取链接不存在或已失效。")
            return
        if batch["status"] != "active":
            log_claim(conn, user, batch, None, "failed", 1, "batch_disabled")
            conn.execute("COMMIT")
            send_message(chat_id, "<b>领取失败</b>\n\n当前活动未开始、已结束或已失效。")
            return
        previous = conn.execute(
            """
            SELECT code
            FROM claim_logs
            WHERE batch_id = ? AND telegram_id = ? AND status = 'success'
            ORDER BY id DESC
            LIMIT 1
            """,
            (batch["id"], user["id"]),
        ).fetchone()
        if previous:
            conn.execute("COMMIT")
            send_message(
                chat_id,
                "<b>你已经领取过这个批次</b>\n\n兑换码：\n<code>{0}</code>".format(
                    html.escape(previous["code"] or "")
                ),
            )
            return

        conditions_ok, reason, message = check_claim_conditions(conn, user, batch)
        if not conditions_ok:
            log_claim(conn, user, batch, None, "failed", 1, reason)
            conn.execute("COMMIT")
            send_message(chat_id, message)
            return

        if batch["batch_type"] == "usage":
            if batch["usage_count"] >= batch["usage_limit"]:
                log_claim(conn, user, batch, None, "failed", 1, "usage_limit_reached")
                conn.execute("COMMIT")
                send_message(chat_id, "<b>领取失败</b>\n\n当前兑换码已达到使用次数上限。")
                return
            conn.execute(
                "UPDATE batches SET usage_count = usage_count + 1 WHERE id = ?",
                (batch["id"],),
            )
            code = batch["shared_code"]
            log_claim(conn, user, batch, code, "success", 1, None)
            conn.execute("COMMIT")
            send_message(
                chat_id,
                "<b>✅ 领取成功</b>\n\n你的兑换码：\n<code>{0}</code>\n\n<i>领取记录已保存。</i>".format(html.escape(code)),
            )
            return

        code_row = conn.execute(
            """
            SELECT id, code
            FROM batch_codes
            WHERE batch_id = ? AND status = 'available'
            ORDER BY id ASC
            LIMIT 1
            """,
            (batch["id"],),
        ).fetchone()
        if not code_row:
            log_claim(conn, user, batch, None, "failed", 1, "sold_out")
            conn.execute("COMMIT")
            send_message(chat_id, "<b>领取失败</b>\n\n当前兑换码已领完。")
            return
        conn.execute(
            """
            UPDATE batch_codes
            SET status = 'claimed', claimed_by = ?, claimed_at = ?
            WHERE id = ? AND status = 'available'
            """,
            (user["id"], now_text(), code_row["id"]),
        )
        log_claim(conn, user, batch, code_row["code"], "success", 1, None)
        conn.execute("COMMIT")
        send_message(
            chat_id,
            "<b>✅ 领取成功</b>\n\n你的兑换码：\n<code>{0}</code>\n\n<i>领取记录已保存。</i>".format(
                html.escape(code_row["code"])
            ),
        )
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def issue_code_v2(chat_id, user, batch_token):
    response_text = None

    with db_connect() as conn:
        upsert_user(conn, user)

    with db_connect() as conn:
        batch = find_batch(conn, batch_token)
        if not batch:
            response_text = "<b>领取失败</b>\n\n领取链接不存在或已失效。"
        elif batch["status"] != "active":
            log_claim(conn, user, batch, None, "failed", 1, "batch_disabled")
            response_text = "<b>领取失败</b>\n\n当前活动未开始、已结束或已失效。"
        else:
            previous = conn.execute(
                """
                SELECT code
                FROM claim_logs
                WHERE batch_id = ? AND telegram_id = ? AND status = 'success'
                ORDER BY id DESC
                LIMIT 1
                """,
                (batch["id"], user["id"]),
            ).fetchone()
            if previous:
                response_text = "<b>你已经领取过这个批次</b>\n\n兑换码：\n<code>{0}</code>".format(
                    html.escape(previous["code"] or "")
                )
            else:
                conditions_ok, reason, message = check_claim_conditions(conn, user, batch)
                if not conditions_ok:
                    log_claim(conn, user, batch, None, "failed", 1, reason)
                    response_text = message

    if response_text:
        send_message(chat_id, response_text)
        return

    conn = db_connect()
    transaction_started = False
    try:
        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE")
        transaction_started = True
        upsert_user(conn, user)
        batch = find_batch(conn, batch_token)
        if not batch:
            conn.execute("COMMIT")
            transaction_started = False
            response_text = "<b>领取失败</b>\n\n领取链接不存在或已失效。"
        elif batch["status"] != "active":
            log_claim(conn, user, batch, None, "failed", 1, "batch_disabled")
            conn.execute("COMMIT")
            transaction_started = False
            response_text = "<b>领取失败</b>\n\n当前活动未开始、已结束或已失效。"
        else:
            previous = conn.execute(
                """
                SELECT code
                FROM claim_logs
                WHERE batch_id = ? AND telegram_id = ? AND status = 'success'
                ORDER BY id DESC
                LIMIT 1
                """,
                (batch["id"], user["id"]),
            ).fetchone()
            if previous:
                conn.execute("COMMIT")
                transaction_started = False
                response_text = "<b>你已经领取过这个批次</b>\n\n兑换码：\n<code>{0}</code>".format(
                    html.escape(previous["code"] or "")
                )
            elif batch["batch_type"] == "usage":
                if batch["usage_count"] >= batch["usage_limit"]:
                    log_claim(conn, user, batch, None, "failed", 1, "usage_limit_reached")
                    conn.execute("COMMIT")
                    transaction_started = False
                    response_text = "<b>领取失败</b>\n\n当前兑换码已达到使用次数上限。"
                else:
                    conn.execute(
                        "UPDATE batches SET usage_count = usage_count + 1 WHERE id = ?",
                        (batch["id"],),
                    )
                    code = batch["shared_code"]
                    log_claim(conn, user, batch, code, "success", 1, None)
                    conn.execute("COMMIT")
                    transaction_started = False
                    response_text = (
                        "<b>✅ 领取成功</b>\n\n"
                        "你的兑换码：\n<code>{0}</code>\n\n"
                        "<i>领取记录已保存。</i>"
                    ).format(html.escape(code or ""))
            else:
                code_row = conn.execute(
                    """
                    SELECT id, code
                    FROM batch_codes
                    WHERE batch_id = ? AND status = 'available'
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (batch["id"],),
                ).fetchone()
                if not code_row:
                    log_claim(conn, user, batch, None, "failed", 1, "sold_out")
                    conn.execute("COMMIT")
                    transaction_started = False
                    response_text = "<b>领取失败</b>\n\n当前兑换码已领完。"
                else:
                    conn.execute(
                        """
                        UPDATE batch_codes
                        SET status = 'claimed', claimed_by = ?, claimed_at = ?
                        WHERE id = ? AND status = 'available'
                        """,
                        (user["id"], now_text(), code_row["id"]),
                    )
                    log_claim(conn, user, batch, code_row["code"], "success", 1, None)
                    conn.execute("COMMIT")
                    transaction_started = False
                    response_text = (
                        "<b>✅ 领取成功</b>\n\n"
                        "你的兑换码：\n<code>{0}</code>\n\n"
                        "<i>领取记录已保存。</i>"
                    ).format(html.escape(code_row["code"] or ""))
    except Exception:
        if transaction_started:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    if response_text:
        send_message(chat_id, response_text)


def handle_callback(callback_query):
    callback_id = callback_query.get("id")
    user = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    user_id = user.get("id")
    data = callback_query.get("data") or ""
    if not callback_id or not chat_id or not user_id:
        return
    if not is_private_chat(chat):
        answer_callback_query(callback_id, "请私聊使用机器人。")
        return
    if data.startswith("draft:"):
        handle_draft_callback(callback_query)
        return
    if data.startswith("defaults:"):
        handle_defaults_callback(callback_query)
        return
    if not data.startswith("captcha:"):
        answer_callback_query(callback_id)
        return
    parts = data.split(":")
    if len(parts) != 3:
        answer_callback_query(callback_id, "验证数据异常，请重新打开领取链接。", show_alert=True)
        return
    token = parts[1]
    selected_key = parts[2]
    state = VERIFY_STATES.get(user_id)
    if not state or state.get("token") != token:
        answer_callback_query(callback_id, "验证已失效，请重新打开领取链接。", show_alert=True)
        return
    if time.time() > state.get("expires_at", 0):
        VERIFY_STATES.pop(user_id, None)
        clear_flow_message(chat_id, user_id)
        answer_callback_query(callback_id, "验证已过期，请重新打开领取链接。", show_alert=True)
        return
    if selected_key != state.get("answer_key"):
        VERIFY_STATES.pop(user_id, None)
        clear_flow_message(chat_id, user_id)
        with db_connect() as conn:
            upsert_user(conn, user)
            batch = find_batch(conn, state["batch_token"])
            if batch:
                log_claim(conn, user, batch, None, "failed", 0, "captcha_failed")
        answer_callback_query(callback_id, "验证失败，请重新打开领取链接。", show_alert=True)
        send_message(chat_id, "人机验证失败，本次领取已终止。请重新打开领取链接后再试。")
        return
    VERIFY_STATES.pop(user_id, None)
    clear_flow_message(chat_id, user_id)
    answer_callback_query(callback_id, "验证通过")
    issue_code_v2(chat_id, user, state["batch_token"])


def handle_start(chat_id, user, text):
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        if is_admin(user["id"]):
            show_admin_panel_v2(chat_id, user["id"])
        else:
            send_message(chat_id, "请通过管理员分享的专属领取链接进入。")
        return
    token = parts[1].strip()
    begin_claim(chat_id, user, token)


def begin_new_batch_v2(chat_id, user_id):
    with db_connect() as conn:
        defaults = load_default_conditions(conn)
    ADMIN_STATES[user_id] = {
        "action": "newbatch_v2",
        "step": "name",
        "data": defaults,
    }
    send_flow_message(
        chat_id,
        user_id,
        "<b>📦 创建批次</b>\n"
        "<b>第 1 步 / 共 5 步</b>\n\n"
        "请输入批次名称。\n"
        "<b>已载入默认条件</b>\n"
        + "\n".join(render_condition_lines(defaults))
        + "\n\n"
        "<i>例如：7 月新用户福利、频道活动兑换码、测试批次。</i>",
        admin_keyboard(),
    )


def begin_defaults_screen(chat_id, user_id, return_state=None):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    with db_connect() as conn:
        defaults = load_default_conditions(conn)
    ADMIN_STATES[user_id] = {
        "action": "defaults",
        "step": "menu",
        "data": defaults,
        "return_state": copy.deepcopy(return_state) if return_state else None,
    }
    send_flow_message(chat_id, user_id, render_defaults_summary(defaults) + "\n\n<i>点击按钮修改默认值。</i>", defaults_keyboard())


def show_admin_panel_v2(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员，无法使用管理功能。")
        return
    send_flow_message(
        chat_id,
        user_id,
        "<b>🛠 管理员面板</b>\n\n"
        "<b>本机状态</b>\n"
        "• 批次创建：按钮式配置 + 最终确认\n"
        "• 默认条件：可在 Bot 内随时修改\n"
        "• 领取逻辑：验证 + 条件校验 + 自动发码\n\n"
        "<i>请选择下方功能开始操作。</i>",
        admin_keyboard(),
    )


def show_flow_v2(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    send_flow_message(
        chat_id,
        user_id,
        "<b>🧭 核心流程</b>\n\n"
        "1. 点击 <code>📦 创建批次</code>\n"
        "2. 输入批次名称\n"
        "3. 选择批次类型\n"
        "4. 用按钮调整领取条件\n"
        "5. 导入兑换码并确认创建\n"
        "6. 分享专属领取链接\n"
        "7. 用户完成九宫格点击验证\n"
        "8. 系统校验群发言数 / 频道订阅条件\n"
        "9. 自动发放兑换码并记录日志\n\n"
        "<i>普通用户不会看到领取入口，只能通过专属链接领取。</i>",
        admin_keyboard(),
    )


def ask_condition_value(chat_id, field):
    prompt_map = {
        "required_channel_id": (
            "<b>📢 设置频道订阅</b>\n\n"
            "公开频道：发送 <code>@channel</code> 或 <code>https://t.me/channel</code>\n"
            "私密邀请链接：发送 <code>chat_id 邀请链接</code>\n"
            "示例：<code>-1001234567890 https://t.me/+xxxx</code>\n\n"
            "输入 <code>0</code> 表示不启用。"
        ),
    }
    return prompt_map.get(field, "请输入内容。")


def ask_group_condition_id():
    return (
        "<b>💬 设置群发言条件</b>\n\n"
        "<b>第 1 步 / 共 2 步</b>\n"
        "请发送要统计发言的<b>群 ID</b>。\n\n"
        "可点击 <code>📊 已记录群</code> 查看 Bot 已记录到的群 ID。\n"
        "群 ID 通常是负数，超级群一般是 <code>-100...</code>；你的个人 User ID 不能用于这里。\n"
        "<blockquote>群 ID 用于统计群内普通发言；频道订阅 ID 用于检测订阅，两者可以不同。</blockquote>\n"
        "输入 <code>0</code> 表示不启用群发言条件。"
    )


def ask_group_condition_messages(group_id):
    return (
        "<b>💬 设置群发言条件</b>\n\n"
        "<b>第 2 步 / 共 2 步</b>\n"
        "群 ID：<code>{0}</code>\n\n"
        "请输入最低普通发言数，用户累计普通发言必须 <b>至少达到</b> 这个数字才能领取。\n"
        "如果 Bot 收不到普通群消息，请先到 BotFather 关闭 Privacy Mode。\n"
        "<i>示例：</i> <code>5</code>"
    ).format(html.escape(str(group_id)))


def apply_condition_input(state, field, text):
    text = text.strip()
    if field == "required_group_id":
        state["data"]["required_group_id"] = validate_group_chat_id(text)
    elif field == "required_group_messages":
        state["data"]["required_group_messages"] = parse_nonnegative_int(text, "群发言数")
    elif field == "required_channel_id":
        target, link = parse_subscription_input(text)
        state["data"]["required_channel_id"] = target
        state["data"]["required_channel_link"] = link
    else:
        raise ValueError("未知条件字段。")
    normalize_condition_data(state["data"])


def normalize_batch_draft(state):
    data = state["data"]
    normalize_condition_data(data)
    if data.get("batch_type") == "usage":
        if "shared_code" not in data or data["shared_code"] is None:
            raise ValueError("请先填写兑换码。")
        data["shared_code"] = data["shared_code"].strip()
        limit = int(data.get("usage_limit") or 0)
        if limit <= 0:
            raise ValueError("可用次数必须大于 0。")
    else:
        codes = data.get("codes") or []
        if not codes:
            raise ValueError("请先导入兑换码。")


def create_batch_from_draft(chat_id, user_id):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "newbatch_v2":
        return
    data = state["data"]
    normalize_batch_draft(state)
    token = "batch_" + uuid.uuid4().hex[:16]
    current = now_text()
    with db_connect() as conn:
        if data["batch_type"] == "usage":
            conn.execute(
                """
                INSERT INTO batches
                    (
                        token, name, batch_type, shared_code, usage_limit, usage_count,
                        required_group_id, required_group_messages, required_channel_id, required_channel_link,
                        created_by, created_at
                    )
                VALUES (?, ?, 'usage', ?, ?, 0, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token,
                    data["name"],
                    data["shared_code"],
                    int(data["usage_limit"]),
                    data.get("required_group_id"),
                    int(data.get("required_group_messages") or 0),
                    data.get("required_channel_id"),
                    data.get("required_channel_link"),
                    user_id,
                    current,
                ),
            )
        else:
            cursor = conn.execute(
                """
                INSERT INTO batches
                    (
                        token, name, batch_type,
                        required_group_id, required_group_messages, required_channel_id, required_channel_link,
                        created_by, created_at
                    )
                VALUES (?, ?, 'unique', ?, ?, ?, ?, ?, ?)
                """,
                (
                    token,
                    data["name"],
                    data.get("required_group_id"),
                    int(data.get("required_group_messages") or 0),
                    data.get("required_channel_id"),
                    data.get("required_channel_link"),
                    user_id,
                    current,
                ),
            )
            batch_id = cursor.lastrowid
            for code in data.get("codes") or []:
                conn.execute(
                    "INSERT INTO batch_codes (batch_id, code, created_at) VALUES (?, ?, ?)",
                    (batch_id, code, current),
                )
    ADMIN_STATES.pop(user_id, None)
    clear_flow_message(chat_id, user_id)
    summary = render_batch_summary(data)
    send_message(
        chat_id,
        summary
        + "\n\n<b>✅ 创建成功</b>\n"
        "领取链接：\n<code>{0}</code>\n\n"
        "<i>复制后发到群聊、频道或私聊即可。</i>".format(create_batch_link(token)),
        admin_keyboard(),
    )


def handle_newbatch_v2_state(chat_id, user_id, text):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "newbatch_v2":
        return False

    if text in ("⬅️ 取消", "取消操作"):
        ADMIN_STATES.pop(user_id, None)
        clear_flow_message(chat_id, user_id)
        send_message(chat_id, "<b>已取消创建批次。</b>", admin_keyboard())
        return True

    if text in ("⬅️ 返回", "返回"):
        if state["step"] == "type":
            ADMIN_STATES.pop(user_id, None)
            begin_new_batch_v2(chat_id, user_id)
        elif state["step"] == "conditions":
            state["step"] = "type"
            send_flow_message(chat_id, user_id, "<b>🔁 选择批次类型</b>\n\n请点击一个选项。", batch_type_keyboard())
        elif state["step"] == "codes":
            state["step"] = "conditions"
            show_batch_condition_screen(chat_id, user_id)
        elif state["step"] == "usage_limit":
            state["step"] = "conditions"
            show_batch_condition_screen(chat_id, user_id)
        elif state["step"] == "usage_code":
            state["step"] = "usage_limit"
            send_flow_message(
                chat_id,
                user_id,
                "<b>🧾 第 4 步 / 共 5 步</b>\n\n请输入这个兑换码可被领取的次数。\n\n"
                "<i>示例：</i> <code>100</code>",
                admin_keyboard(),
            )
        return True

    if state["step"] == "name":
        if not text:
            send_flow_message(chat_id, user_id, "批次名称不能为空。")
            return True
        state["data"]["name"] = text
        state["step"] = "type"
        send_flow_message(chat_id, user_id, "<b>🔁 第 2 步 / 共 5 步</b>\n\n请选择批次类型。", batch_type_keyboard())
        return True

    if state["step"] == "type":
        if text in ("🔁 使用次数", "使用次数", "usage"):
            state["data"]["batch_type"] = "usage"
        elif text in ("🎁 领完为止", "领完为止", "unique"):
            state["data"]["batch_type"] = "unique"
        else:
            send_flow_message(chat_id, user_id, "请点击一个类型按钮。", batch_type_keyboard())
            return True
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "condition_input":
        field = state.get("pending_field")
        if field == "required_channel_id" and text.strip() != "0":
            send_flow_message(chat_id, user_id, ask_condition_value(chat_id, field), draft_input_keyboard("draft:edit_conditions"))
            return True
        try:
            apply_condition_input(state, field, text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc))
            return True
        state["step"] = "conditions"
        state["pending_field"] = None
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_id":
        try:
            group_id = validate_group_chat_id(text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc))
            return True
        if not group_id:
            state["data"]["required_group_id"] = None
            state["data"]["required_group_messages"] = 0
            state["step"] = "conditions"
            state["pending_group_id"] = None
            show_batch_condition_screen(chat_id, user_id)
            return True
        state["pending_group_id"] = group_id
        state["step"] = "group_condition_messages"
        send_flow_message(chat_id, user_id, ask_group_condition_messages(group_id), admin_keyboard())
        return True

    if state["step"] == "group_condition_messages":
        try:
            group_messages = parse_nonnegative_int(text, "群发言数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc))
            return True
        if group_messages <= 0:
            send_flow_message(chat_id, user_id, "群发言数必须大于 0；如需关闭条件，请重新点击群发言条件并输入 0。")
            return True
        state["data"]["required_group_id"] = state.get("pending_group_id")
        state["data"]["required_group_messages"] = group_messages
        state["pending_group_id"] = None
        state["step"] = "conditions"
        normalize_condition_data(state["data"])
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "usage_limit":
        try:
            usage_limit = parse_nonnegative_int(text, "可用次数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc))
            return True
        if usage_limit <= 0:
            send_flow_message(chat_id, user_id, "可用次数必须大于 0。")
            return True
        state["data"]["usage_limit"] = usage_limit
        state["step"] = "usage_code"
        send_flow_message(
            chat_id,
            user_id,
            "<b>🧾 第 5 步 / 共 5 步</b>\n\n请输入这一个兑换码。\n"
            "<i>示例：</i> <code>ABC-DEF-001</code>",
            admin_keyboard(),
        )
        return True

    if state["step"] == "usage_code":
        code = text.strip()
        if not code:
            send_flow_message(chat_id, user_id, "兑换码不能为空。")
            return True
        state["data"]["shared_code"] = code
        state["step"] = "confirm"
        show_batch_preview(chat_id, user_id)
        return True

    if state["step"] == "codes":
        raw_codes = [line.strip() for line in text.splitlines() if line.strip()]
        if not raw_codes:
            send_flow_message(chat_id, user_id, "请发送兑换码，每行一个。")
            return True
        seen = set()
        codes = []
        duplicated = 0
        for code in raw_codes:
            if code in seen:
                duplicated += 1
                continue
            seen.add(code)
            codes.append(code)
        state["data"]["codes"] = codes
        state["data"]["duplicated"] = duplicated
        state["step"] = "confirm"
        show_batch_preview(chat_id, user_id)
        return True

    if state["step"] == "confirm":
        send_flow_message(chat_id, user_id, "请点击下方按钮确认创建，或返回修改。", confirm_keyboard())
        return True

    return False


def handle_defaults_state(chat_id, user_id, text):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "defaults":
        return False

    if text in ("⬅️ 取消", "取消操作"):
        ADMIN_STATES.pop(user_id, None)
        clear_flow_message(chat_id, user_id)
        send_message(chat_id, "<b>已取消默认值设置。</b>", admin_keyboard())
        return True

    if text == "⬅️ 返回":
        ADMIN_STATES.pop(user_id, None)
        show_defaults_screen(chat_id, user_id)
        return True

    if state["step"] == "menu":
        send_flow_message(chat_id, user_id, "请点击下方按钮修改默认值。", defaults_keyboard())
        return True

    if state["step"] == "edit":
        field = state.get("pending_field")
        try:
            apply_condition_input(state, field, text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc))
            return True
        with db_connect() as conn:
            set_setting(conn, "default_required_group_id", state["data"].get("required_group_id") or "")
            set_setting(conn, "default_required_group_messages", str(state["data"].get("required_group_messages") or 0))
            set_setting(conn, "default_required_channel_id", state["data"].get("required_channel_id") or "")
            set_setting(conn, "default_required_channel_link", state["data"].get("required_channel_link") or "")
        state["step"] = "menu"
        state["pending_field"] = None
        show_defaults_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_id":
        try:
            group_id = validate_group_chat_id(text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc))
            return True
        if not group_id:
            state["data"]["required_group_id"] = None
            state["data"]["required_group_messages"] = 0
            with db_connect() as conn:
                set_setting(conn, "default_required_group_id", "")
                set_setting(conn, "default_required_group_messages", "0")
            state["step"] = "menu"
            state["pending_group_id"] = None
            show_defaults_screen(chat_id, user_id)
            return True
        state["pending_group_id"] = group_id
        state["step"] = "group_condition_messages"
        send_flow_message(chat_id, user_id, ask_group_condition_messages(group_id), admin_keyboard())
        return True

    if state["step"] == "group_condition_messages":
        try:
            group_messages = parse_nonnegative_int(text, "群发言数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc))
            return True
        if group_messages <= 0:
            send_flow_message(chat_id, user_id, "群发言数必须大于 0；如需关闭条件，请重新点击群发言条件并输入 0。")
            return True
        state["data"]["required_group_id"] = state.get("pending_group_id")
        state["data"]["required_group_messages"] = group_messages
        state["pending_group_id"] = None
        normalize_condition_data(state["data"])
        with db_connect() as conn:
            set_setting(conn, "default_required_group_id", state["data"].get("required_group_id") or "")
            set_setting(conn, "default_required_group_messages", str(state["data"].get("required_group_messages") or 0))
        state["step"] = "menu"
        show_defaults_screen(chat_id, user_id)
        return True

    return False


def handle_draft_callback(callback_query):
    user = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    user_id = user.get("id")
    data = (callback_query.get("data") or "").split(":", 1)[1]
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "newbatch_v2":
        answer_callback_query(callback_query.get("id"), "请先重新开始创建批次。", show_alert=True)
        return

    answer_callback_query(callback_query.get("id"))
    if data == "group_condition":
        state["step"] = "group_condition_id"
        state["pending_group_id"] = None
        send_flow_message(chat_id, user_id, ask_group_condition_id(), admin_keyboard())
    elif data == "channel_id":
        state["step"] = "condition_input"
        state["pending_field"] = "required_channel_id"
        send_flow_message(chat_id, user_id, ask_condition_value(chat_id, "required_channel_id"), admin_keyboard())
    elif data == "load_defaults":
        with db_connect() as conn:
            state["data"].update(load_default_conditions(conn))
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "clear_conditions":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        state["data"]["required_channel_link"] = None
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "continue":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_flow_message(
                chat_id,
                user_id,
                "<b>🧾 第 4 步 / 共 5 步</b>\n\n"
                "请输入这个兑换码可被领取的次数。\n\n"
                "<i>示例：</i> <code>100</code>",
                admin_keyboard(),
            )
        else:
            state["step"] = "codes"
            send_flow_message(
                chat_id,
                user_id,
                "<b>🎁 第 4 步 / 共 5 步</b>\n\n"
                "请批量发送兑换码，每行一个。\n\n"
                "<i>示例：</i>\n<code>CODE001</code>\n<code>CODE002</code>\n<code>CODE003</code>",
                admin_keyboard(),
            )
    elif data == "back_type":
        state["step"] = "type"
        send_flow_message(chat_id, user_id, "<b>🔁 第 2 步 / 共 5 步</b>\n\n请选择批次类型。", batch_type_keyboard())
    elif data == "open_defaults":
        begin_defaults_screen(chat_id, user_id, state)
    elif data == "edit_conditions":
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "edit_codes":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_flow_message(
                chat_id,
                user_id,
                "<b>🧾 第 4 步 / 共 5 步</b>\n\n"
                "请输入这个兑换码可被领取的次数。\n\n"
                "<i>示例：</i> <code>100</code>",
                admin_keyboard(),
            )
        else:
            state["step"] = "codes"
            send_flow_message(
                chat_id,
                user_id,
                "<b>🎁 第 4 步 / 共 5 步</b>\n\n"
                "请批量发送兑换码，每行一个。\n\n"
                "<i>示例：</i>\n<code>CODE001</code>\n<code>CODE002</code>\n<code>CODE003</code>",
                admin_keyboard(),
            )
    elif data == "edit_type":
        state["step"] = "type"
        send_flow_message(chat_id, user_id, "<b>🔁 第 2 步 / 共 5 步</b>\n\n请选择批次类型。", batch_type_keyboard())
    elif data == "confirm":
        create_batch_from_draft(chat_id, user_id)


def handle_defaults_callback(callback_query):
    user = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    user_id = user.get("id")
    data = (callback_query.get("data") or "").split(":", 1)[1]
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "defaults":
        answer_callback_query(callback_query.get("id"), "请先打开默认值面板。", show_alert=True)
        return

    answer_callback_query(callback_query.get("id"))
    if data == "group_condition":
        state["step"] = "group_condition_id"
        state["pending_group_id"] = None
        send_flow_message(chat_id, user_id, ask_group_condition_id(), admin_keyboard())
    elif data == "channel_id":
        state["step"] = "edit"
        state["pending_field"] = "required_channel_id"
        send_flow_message(chat_id, user_id, ask_condition_value(chat_id, state["pending_field"]), admin_keyboard())
    elif data == "clear":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        state["data"]["required_channel_link"] = None
        with db_connect() as conn:
            set_setting(conn, "default_required_group_id", "")
            set_setting(conn, "default_required_group_messages", "0")
            set_setting(conn, "default_required_channel_id", "")
            set_setting(conn, "default_required_channel_link", "")
        show_defaults_screen(chat_id, user_id, message_id)
    elif data == "done":
        return_state = state.get("return_state")
        if return_state:
            ADMIN_STATES[user_id] = return_state
            clear_flow_message(chat_id, user_id)
            send_message(chat_id, "<b>✅ 默认值已保存，已返回批次草稿。</b>", admin_keyboard())
            if return_state.get("step") == "conditions":
                show_batch_condition_screen(chat_id, user_id)
            elif return_state.get("step") == "confirm":
                show_batch_preview(chat_id, user_id)
        else:
            ADMIN_STATES.pop(user_id, None)
            clear_flow_message(chat_id, user_id)
            send_message(chat_id, "<b>✅ 默认值已保存。</b>", admin_keyboard())
def cancel_new_batch(chat_id, user_id):
    ADMIN_STATES.pop(user_id, None)
    clear_flow_message(chat_id, user_id)
    send_message(chat_id, "<b>已退出创建批次。</b>", admin_keyboard())


def send_batch_type_prompt(chat_id, user_id):
    send_flow_message(
        chat_id,
        user_id,
        "<b>🔁 第 2 步 / 共 5 步</b>\n\n请选择批次类型。",
        batch_type_keyboard(),
    )


def send_usage_limit_prompt(chat_id, user_id):
    send_flow_message(
        chat_id,
        user_id,
        "<b>🧾 第 4 步 / 共 5 步</b>\n\n请输入这个兑换码可被领取的次数。\n\n<i>示例：</i> <code>100</code>",
        draft_input_keyboard("draft:edit_conditions"),
    )


def send_usage_code_prompt(chat_id, user_id):
    send_flow_message(
        chat_id,
        user_id,
        "<b>🧾 第 5 步 / 共 5 步</b>\n\n请输入这一个兑换码。\n\n<i>示例：</i> <code>ABC-DEF-001</code>",
        draft_input_keyboard("draft:back_usage_limit"),
    )


def send_unique_codes_prompt(chat_id, user_id):
    send_flow_message(
        chat_id,
        user_id,
        "<b>🎁 第 4 步 / 共 5 步</b>\n\n请批量发送兑换码，每行一个。\n\n<i>示例：</i>\n<code>CODE001</code>\n<code>CODE002</code>\n<code>CODE003</code>",
        draft_input_keyboard("draft:edit_conditions"),
    )


def begin_new_batch_v2(chat_id, user_id):
    with db_connect() as conn:
        defaults = load_default_conditions(conn)
    ADMIN_STATES[user_id] = {
        "action": "newbatch_v2",
        "step": "name",
        "data": defaults,
    }
    send_flow_message(
        chat_id,
        user_id,
        "<b>📦 创建批次</b>\n"
        "<b>第 1 步 / 共 5 步</b>\n\n"
        "请输入批次名称。\n\n"
        "<b>已载入默认条件</b>\n"
        + "\n".join(render_condition_lines(defaults))
        + "\n\n"
        "<i>示例：7 月新用户福利、频道活动兑换码、测试批次。</i>",
        draft_exit_keyboard(),
    )


def handle_newbatch_v2_state(chat_id, user_id, text):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "newbatch_v2":
        return False

    if text in ("⬅️ 取消", "❌ 退出创建", "退出创建", "取消操作", "取消"):
        cancel_new_batch(chat_id, user_id)
        return True

    if state["step"] == "name":
        if not text:
            send_flow_message(chat_id, user_id, "批次名称不能为空。", draft_exit_keyboard())
            return True
        state["data"]["name"] = text
        state["step"] = "type"
        send_batch_type_prompt(chat_id, user_id)
        return True

    if state["step"] == "type":
        if text in ("🔁 使用次数", "使用次数", "usage"):
            state["data"]["batch_type"] = "usage"
        elif text in ("🎁 领完为止", "领完为止", "unique"):
            state["data"]["batch_type"] = "unique"
        else:
            send_flow_message(chat_id, user_id, "请点击下方按钮选择批次类型。", batch_type_keyboard())
            return True
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "condition_input":
        field = state.get("pending_field")
        if field == "required_channel_id" and text.strip() != "0":
            send_flow_message(chat_id, user_id, ask_condition_value(chat_id, field), draft_input_keyboard("draft:edit_conditions"))
            return True
        try:
            apply_condition_input(state, field, text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), draft_input_keyboard("draft:edit_conditions"))
            return True
        state["step"] = "conditions"
        state["pending_field"] = None
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_id":
        try:
            group_id = validate_group_chat_id(text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), draft_input_keyboard("draft:edit_conditions"))
            return True
        if not group_id:
            state["data"]["required_group_id"] = None
            state["data"]["required_group_messages"] = 0
            state["step"] = "conditions"
            state["pending_group_id"] = None
            show_batch_condition_screen(chat_id, user_id)
            return True
        state["pending_group_id"] = group_id
        state["step"] = "group_condition_messages"
        send_flow_message(chat_id, user_id, ask_group_condition_messages(group_id), draft_input_keyboard("draft:group_condition"))
        return True

    if state["step"] == "group_condition_messages":
        try:
            group_messages = parse_nonnegative_int(text, "群发言数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), draft_input_keyboard("draft:group_condition"))
            return True
        if group_messages <= 0:
            send_flow_message(chat_id, user_id, "群发言数必须大于 0；如需关闭条件，请重新点击群发言条件并输入 0。", draft_input_keyboard("draft:group_condition"))
            return True
        state["data"]["required_group_id"] = state.get("pending_group_id")
        state["data"]["required_group_messages"] = group_messages
        state["pending_group_id"] = None
        state["step"] = "conditions"
        normalize_condition_data(state["data"])
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "usage_limit":
        try:
            usage_limit = parse_nonnegative_int(text, "可用次数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), draft_input_keyboard("draft:edit_conditions"))
            return True
        if usage_limit <= 0:
            send_flow_message(chat_id, user_id, "可用次数必须大于 0。", draft_input_keyboard("draft:edit_conditions"))
            return True
        state["data"]["usage_limit"] = usage_limit
        state["step"] = "usage_code"
        send_usage_code_prompt(chat_id, user_id)
        return True

    if state["step"] == "usage_code":
        code = text.strip()
        if not code:
            send_flow_message(chat_id, user_id, "兑换码不能为空。", draft_input_keyboard("draft:back_usage_limit"))
            return True
        state["data"]["shared_code"] = code
        state["step"] = "confirm"
        show_batch_preview(chat_id, user_id)
        return True

    if state["step"] == "codes":
        raw_codes = [line.strip() for line in text.splitlines() if line.strip()]
        if not raw_codes:
            send_flow_message(chat_id, user_id, "请发送兑换码，每行一个。", draft_input_keyboard("draft:edit_conditions"))
            return True
        seen = set()
        codes = []
        duplicated = 0
        for code in raw_codes:
            if code in seen:
                duplicated += 1
                continue
            seen.add(code)
            codes.append(code)
        state["data"]["codes"] = codes
        state["data"]["duplicated"] = duplicated
        state["step"] = "confirm"
        show_batch_preview(chat_id, user_id)
        return True

    if state["step"] == "confirm":
        send_flow_message(chat_id, user_id, "请点击下方按钮确认创建，或返回修改。", confirm_keyboard())
        return True

    return False


def handle_draft_callback(callback_query):
    user = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    user_id = user.get("id")
    data = (callback_query.get("data") or "").split(":", 1)[1]
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "newbatch_v2":
        answer_callback_query(callback_query.get("id"), "请先重新开始创建批次。", show_alert=True)
        return

    answer_callback_query(callback_query.get("id"))
    if data == "cancel":
        cancel_new_batch(chat_id, user_id)
    elif data == "type_usage":
        state["data"]["batch_type"] = "usage"
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "type_unique":
        state["data"]["batch_type"] = "unique"
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "group_condition":
        state["step"] = "group_condition_id"
        state["pending_group_id"] = None
        send_flow_message(chat_id, user_id, ask_group_condition_id(), draft_input_keyboard("draft:edit_conditions"))
    elif data == "channel_id":
        state["step"] = "condition_input"
        state["pending_field"] = "required_channel_id"
        send_flow_message(chat_id, user_id, ask_condition_value(chat_id, "required_channel_id"), draft_input_keyboard("draft:edit_conditions"))
    elif data == "load_defaults":
        with db_connect() as conn:
            state["data"].update(load_default_conditions(conn))
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "clear_conditions":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        state["data"]["required_channel_link"] = None
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "continue":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_usage_limit_prompt(chat_id, user_id)
        else:
            state["step"] = "codes"
            send_unique_codes_prompt(chat_id, user_id)
    elif data == "back_type" or data == "edit_type":
        state["step"] = "type"
        send_batch_type_prompt(chat_id, user_id)
    elif data == "open_defaults":
        begin_defaults_screen(chat_id, user_id, state)
    elif data == "edit_conditions":
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "edit_codes":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_usage_limit_prompt(chat_id, user_id)
        else:
            state["step"] = "codes"
            send_unique_codes_prompt(chat_id, user_id)
    elif data == "back_usage_limit":
        state["step"] = "usage_limit"
        send_usage_limit_prompt(chat_id, user_id)
    elif data == "confirm":
        create_batch_from_draft(chat_id, user_id)


def admin_keyboard():
    return {
        "keyboard": [
            [{"text": "📦 创建批次"}, {"text": "📋 批次列表"}, {"text": "🗒 最近记录"}],
            [{"text": "⚙️ 默认条件"}, {"text": "🧩 核心流程"}, {"text": "📊 已记录群"}],
            [{"text": "👤 我的ID"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def defaults_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "💬 群发言条件", "callback_data": "defaults:group_condition"},
                {"text": "📢 频道订阅", "callback_data": "defaults:channel_id"},
            ],
            [
                {"text": "🧹 清空条件", "callback_data": "defaults:clear"},
                {"text": "✅ 完成", "callback_data": "defaults:done"},
            ],
        ]
    }


def defaults_input_keyboard(back_data="defaults:done"):
    return {
        "inline_keyboard": [
            [{"text": "⬅️ 返回默认条件", "callback_data": back_data}],
            [{"text": "✅ 完成", "callback_data": "defaults:done"}],
        ]
    }


def ask_condition_value(chat_id, field):
    if field == "required_channel_id":
        return (
            "<b>📢 设置频道订阅</b>\n\n"
            "请从目标频道转发任意一条消息到这里，Bot 会自动识别并绑定频道。\n\n"
            "<blockquote>绑定后我会检测 Bot 是否已在该频道内。"
            "如果检测不到，请把 Bot 拉进频道，并给它查看成员/管理员相关权限。</blockquote>\n\n"
            "如需关闭频道订阅条件，请发送 <code>0</code>。"
        )
    return "请输入内容。"


def normalize_main_menu_text(text):
    return {
        "📦 创建批次": "/newbatch",
        "📋 批次列表": "/batches",
        "🗒 最近记录": "/records",
        "⚙️ 默认条件": "/defaults",
        "🧩 核心流程": "/flow",
        "📊 已记录群": "/groups",
        "👤 我的ID": "/whoami",
    }.get(text, text)


def is_main_menu_command(text):
    return text in ("/newbatch", "/batches", "/records", "/defaults", "/flow", "/groups", "/whoami")


def bot_membership_note(target_id):
    try:
        bot_info = api_call("getMe")
        bot_id = bot_info.get("id")
    except Exception:
        bot_id = None
    member = get_chat_member(target_id, bot_id) if bot_id else None
    if member_is_joined(member):
        return "✅ 已检测到 Bot 在目标频道/群内。"
    return "⚠️ 暂未检测到 Bot 在目标频道/群内。请把 Bot 拉进去，并给它必要权限后再让用户领取。"


def bind_subscription_from_forward(chat_id, user_id, message):
    state = ADMIN_STATES.get(user_id)
    if not state:
        return False
    if state.get("action") == "newbatch_v2":
        pending = state.get("step") == "condition_input" and state.get("pending_field") == "required_channel_id"
    elif state.get("action") == "defaults":
        pending = state.get("step") == "edit" and state.get("pending_field") == "required_channel_id"
    else:
        pending = False
    if not pending:
        return False

    forwarded_chat = forwarded_chat_from_message(message)
    if not forwarded_chat or not forwarded_chat.get("id"):
        send_flow_message(chat_id, user_id, "没有识别到频道来源。请直接从目标频道转发一条消息过来。", defaults_input_keyboard() if state.get("action") == "defaults" else draft_input_keyboard("draft:edit_conditions"))
        return True

    target_id = str(forwarded_chat.get("id"))
    title = forwarded_chat.get("title") or forwarded_chat.get("username") or target_id
    username = forwarded_chat.get("username")
    link = "https://t.me/{0}".format(username) if username else None
    remember_chat_info(forwarded_chat)

    state["data"]["required_channel_id"] = target_id
    state["data"]["required_channel_link"] = link
    normalize_condition_data(state["data"])
    note = bot_membership_note(target_id)

    if state.get("action") == "defaults":
        with db_connect() as conn:
            set_setting(conn, "default_required_channel_id", state["data"].get("required_channel_id") or "")
            set_setting(conn, "default_required_channel_link", state["data"].get("required_channel_link") or "")
        state["step"] = "menu"
        state["pending_field"] = None
        send_flow_message(
            chat_id,
            user_id,
            "<b>✅ 频道订阅已绑定</b>\n\n频道：<b>{0}</b>\n检测 ID：<code>{1}</code>\n\n{2}".format(
                html.escape(title),
                html.escape(target_id),
                note,
            ),
            defaults_keyboard(),
        )
    else:
        state["step"] = "conditions"
        state["pending_field"] = None
        send_flow_message(
            chat_id,
            user_id,
            "<b>✅ 频道订阅已绑定</b>\n\n频道：<b>{0}</b>\n检测 ID：<code>{1}</code>\n\n{2}".format(
                html.escape(title),
                html.escape(target_id),
                note,
            ),
            condition_edit_keyboard(),
        )
    return True


def handle_forwarded_chat(chat_id, user_id, message):
    if not is_admin(user_id):
        return False
    if bind_subscription_from_forward(chat_id, user_id, message):
        return True
    forwarded_chat = forwarded_chat_from_message(message)
    if not forwarded_chat or not forwarded_chat.get("id"):
        return False
    target_id = str(forwarded_chat.get("id"))
    title = forwarded_chat.get("title") or forwarded_chat.get("username") or target_id
    username = forwarded_chat.get("username")
    current = now_text()
    with db_connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO chat_infos
                (chat_id, title, username, chat_type, last_seen_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (target_id, title, username, forwarded_chat.get("type"), current),
        )
    link = "https://t.me/{0}".format(username) if username else ""
    lines = [
        "<b>📌 已识别转发来源</b>",
        "",
        "• 名称：{0}".format(html.escape(title)),
        "• Chat ID：<code>{0}</code>".format(html.escape(target_id)),
    ]
    if link:
        lines.append("• 公开链接：<code>{0}</code>".format(html.escape(link)))
    send_message(chat_id, "\n".join(lines), admin_keyboard())
    return True


def show_seen_groups(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    with db_connect() as conn:
        rows = conn.execute(
            """
            SELECT
                c.chat_id,
                c.title,
                c.username,
                c.chat_type,
                c.last_seen_at,
                COUNT(s.telegram_id) AS user_count,
                COALESCE(SUM(s.message_count), 0) AS message_count
            FROM chat_infos c
            LEFT JOIN user_chat_stats s ON s.chat_id = c.chat_id
            GROUP BY c.chat_id, c.title, c.username, c.chat_type, c.last_seen_at
            ORDER BY c.last_seen_at DESC
            LIMIT 20
            """
        ).fetchall()
    if not rows:
        send_message(
            chat_id,
            "<b>📊 已记录群</b>\n\n"
            "<i>暂时没有记录到群消息。</i>\n\n"
            "请把 Bot 加入目标群，并确保 BotFather 的 Privacy Mode 已关闭。"
            "之后让群里发一条普通消息，这里就会出现可复制的群 ID。",
            admin_keyboard(),
        )
        return
    lines = [
        "<b>📊 已记录群</b>",
        "<i>创建批次时，群发言条件请复制这里的 Chat ID。</i>",
    ]
    for index, row in enumerate(rows, 1):
        username = "@{0}".format(row["username"]) if row["username"] else "-"
        lines.append(
            "\n<b>{0}. {1}</b>\n"
            "<blockquote>"
            "Chat ID：<code>{2}</code>\n"
            "类型：{3}\n"
            "统计用户：{4} 人\n"
            "累计发言：{5} 条\n"
            "用户名：{6}\n"
            "最近记录：{7}"
            "</blockquote>".format(
                index,
                html.escape(row["title"] or row["chat_id"]),
                html.escape(row["chat_id"]),
                html.escape(row["chat_type"] or "-"),
                row["user_count"] or 0,
                row["message_count"] or 0,
                html.escape(username),
                html.escape(row["last_seen_at"] or "-"),
            )
        )
    send_message(chat_id, "\n".join(lines), admin_keyboard())


def handle_defaults_callback(callback_query):
    user = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    user_id = user.get("id")
    data = (callback_query.get("data") or "").split(":", 1)[1]
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "defaults":
        answer_callback_query(callback_query.get("id"), "请先打开默认条件面板。", show_alert=True)
        return

    answer_callback_query(callback_query.get("id"))
    if data == "group_condition":
        state["step"] = "group_condition_id"
        state["pending_group_id"] = None
        send_flow_message(chat_id, user_id, ask_group_condition_id(), defaults_input_keyboard())
    elif data == "channel_id":
        state["step"] = "edit"
        state["pending_field"] = "required_channel_id"
        send_flow_message(chat_id, user_id, ask_condition_value(chat_id, "required_channel_id"), defaults_input_keyboard())
    elif data == "clear":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        state["data"]["required_channel_link"] = None
        with db_connect() as conn:
            set_setting(conn, "default_required_group_id", "")
            set_setting(conn, "default_required_group_messages", "0")
            set_setting(conn, "default_required_channel_id", "")
            set_setting(conn, "default_required_channel_link", "")
        show_defaults_screen(chat_id, user_id, message_id)
    elif data == "done":
        return_state = state.get("return_state")
        if return_state:
            ADMIN_STATES[user_id] = return_state
            clear_flow_message(chat_id, user_id)
            send_message(chat_id, "<b>✅ 默认条件已保存，已返回批次草稿。</b>", admin_keyboard())
            if return_state.get("step") == "conditions":
                show_batch_condition_screen(chat_id, user_id)
            elif return_state.get("step") == "confirm":
                show_batch_preview(chat_id, user_id)
        else:
            ADMIN_STATES.pop(user_id, None)
            clear_flow_message(chat_id, user_id)
            send_message(chat_id, "<b>✅ 默认条件已保存。</b>", admin_keyboard())


def handle_defaults_state(chat_id, user_id, text):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "defaults":
        return False

    if is_main_menu_command(text) and text != "/defaults":
        ADMIN_STATES.pop(user_id, None)
        clear_flow_message(chat_id, user_id)
        return False

    if state["step"] == "menu":
        send_flow_message(chat_id, user_id, "请点击面板里的按钮修改默认条件，或点击完成退出。", defaults_keyboard())
        return True

    if state["step"] == "group_chat_choose":
        token = state.get("group_bind_token") or uuid.uuid4().hex[:12]
        state["group_bind_token"] = token
        state["private_chat_id"] = chat_id
        send_flow_message(chat_id, user_id, ask_group_choose_prompt(), group_choose_keyboard("defaults", user_id, token))
        return True

    if state["step"] == "edit":
        field = state.get("pending_field")
        if field == "required_channel_id" and text.strip() != "0":
            send_flow_message(chat_id, user_id, ask_condition_value(chat_id, field), defaults_input_keyboard())
            return True
        try:
            apply_condition_input(state, field, text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), defaults_input_keyboard())
            return True
        with db_connect() as conn:
            set_setting(conn, "default_required_group_id", state["data"].get("required_group_id") or "")
            set_setting(conn, "default_required_group_messages", str(state["data"].get("required_group_messages") or 0))
            set_setting(conn, "default_required_channel_id", state["data"].get("required_channel_id") or "")
            set_setting(conn, "default_required_channel_link", state["data"].get("required_channel_link") or "")
        state["step"] = "menu"
        state["pending_field"] = None
        show_defaults_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_id":
        try:
            group_id = validate_group_chat_id(text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), defaults_input_keyboard())
            return True
        if not group_id:
            state["data"]["required_group_id"] = None
            state["data"]["required_group_messages"] = 0
            with db_connect() as conn:
                set_setting(conn, "default_required_group_id", "")
                set_setting(conn, "default_required_group_messages", "0")
            state["step"] = "menu"
            state["pending_group_id"] = None
            show_defaults_screen(chat_id, user_id)
            return True
        state["pending_group_id"] = group_id
        state["step"] = "group_condition_messages"
        send_flow_message(chat_id, user_id, ask_group_condition_messages(group_id), defaults_input_keyboard())
        return True

    if state["step"] == "group_condition_messages":
        try:
            group_messages = parse_nonnegative_int(text, "群发言数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), defaults_input_keyboard())
            return True
        if group_messages <= 0:
            send_flow_message(chat_id, user_id, "群发言数必须大于 0；如需关闭条件，请重新点击群发言条件并输入 0。", defaults_input_keyboard())
            return True
        state["data"]["required_group_id"] = state.get("pending_group_id")
        state["data"]["required_group_messages"] = group_messages
        state["pending_group_id"] = None
        normalize_condition_data(state["data"])
        with db_connect() as conn:
            set_setting(conn, "default_required_group_id", state["data"].get("required_group_id") or "")
            set_setting(conn, "default_required_group_messages", str(state["data"].get("required_group_messages") or 0))
        state["step"] = "menu"
        show_defaults_screen(chat_id, user_id)
        return True

    return False


def settings_status_lines(data):
    group_id = data.get("required_group_id")
    group_messages = int(data.get("required_group_messages") or 0)
    channel_id = data.get("required_channel_id")
    lines = []
    lines.append("群聊：<code>{0}</code>".format(html.escape(str(group_id))) if group_id else "群聊：<i>未绑定</i>")
    lines.append("发言数：<b>{0}</b>".format(group_messages) if group_messages > 0 else "发言数：<i>未设置</i>")
    lines.append("频道：{0}".format(subscription_display_html(data)) if channel_id else "频道：<i>未绑定</i>")
    return lines


def render_defaults_panel(data):
    return (
        "<b>⚙️ 默认条件设置</b>\n\n"
        + "\n".join("• " + line for line in settings_status_lines(data))
        + "\n\n"
        "<i>这些值只会带入新批次，已创建的批次不会被修改。</i>"
    )


def defaults_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "💬 绑定群聊", "callback_data": "defaults:group_chat"},
                {"text": "🔢 发言数量", "callback_data": "defaults:group_messages"},
            ],
            [
                {"text": "📢 绑定频道", "callback_data": "defaults:channel_id"},
                {"text": "🧹 清空条件", "callback_data": "defaults:clear"},
            ],
            [{"text": "✅ 完成", "callback_data": "defaults:done"}],
        ]
    }


def condition_edit_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "💬 绑定群聊", "callback_data": "draft:group_chat"},
                {"text": "🔢 发言数量", "callback_data": "draft:group_messages"},
            ],
            [
                {"text": "📢 绑定频道", "callback_data": "draft:channel_id"},
                {"text": "🧹 清空条件", "callback_data": "draft:clear_conditions"},
            ],
            [
                {"text": "✅ 继续", "callback_data": "draft:continue"},
                {"text": "⚙️ 默认值", "callback_data": "draft:open_defaults"},
            ],
            [
                {"text": "⬅️ 返回类型", "callback_data": "draft:back_type"},
                {"text": "❌ 退出创建", "callback_data": "draft:cancel"},
            ],
        ]
    }


def show_defaults_screen(chat_id, user_id, message_id=None):
    state = ADMIN_STATES.get(user_id)
    data = state.get("data") if state else None
    if not data:
        with db_connect() as conn:
            data = load_default_conditions(conn)
    text = render_defaults_panel(data)
    if message_id:
        result = safe_edit_message_text(chat_id, message_id, text, defaults_keyboard())
        if result:
            CLEANUP_MESSAGES[cleanup_key(chat_id, user_id)] = message_id
        else:
            send_flow_message(chat_id, user_id, text, defaults_keyboard())
    else:
        send_flow_message(chat_id, user_id, text, defaults_keyboard())


def begin_defaults_screen(chat_id, user_id, return_state=None):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    with db_connect() as conn:
        defaults = load_default_conditions(conn)
    ADMIN_STATES[user_id] = {
        "action": "defaults",
        "step": "menu",
        "data": defaults,
        "return_state": copy.deepcopy(return_state) if return_state else None,
    }
    show_defaults_screen(chat_id, user_id)


def ask_group_chat_prompt():
    return (
        "<b>💬 绑定群聊</b>\n\n"
        "请发送要统计发言的群聊 Chat ID。\n\n"
        "可在 <code>📊 已记录群</code> 中复制，通常是 <code>-100...</code> 这样的负数 ID。\n\n"
        "<i>这里只负责绑定群聊；最低发言数请回到面板后单独点击「🔢 发言数量」设置。</i>"
    )


def ask_group_messages_prompt(group_id):
    return (
        "<b>🔢 设置发言数量</b>\n\n"
        "当前群聊：<code>{0}</code>\n\n"
        "请输入用户在该群聊中至少需要累计发送多少条普通消息，才能领取兑换码。\n\n"
        "<i>示例：</i> <code>5</code>"
    ).format(html.escape(str(group_id)))


def save_default_conditions(data):
    with db_connect() as conn:
        set_setting(conn, "default_required_group_id", data.get("required_group_id") or "")
        set_setting(conn, "default_required_group_messages", str(data.get("required_group_messages") or 0))
        set_setting(conn, "default_required_channel_id", data.get("required_channel_id") or "")
        set_setting(conn, "default_required_channel_link", data.get("required_channel_link") or "")


def load_default_conditions(conn):
    group_messages = int(get_setting(conn, "default_required_group_messages", "0") or 0)
    group_id = normalize_chat_id_text(parse_nullable_text(get_setting(conn, "default_required_group_id", "")))
    data = {
        "required_group_id": group_id,
        "required_group_messages": group_messages,
        "required_channel_id": parse_nullable_text(get_setting(conn, "default_required_channel_id", "")),
        "required_channel_link": parse_nullable_text(get_setting(conn, "default_required_channel_link", "")),
    }
    if data["required_channel_id"] and not data["required_channel_link"]:
        data["required_channel_link"] = default_subscription_link(data["required_channel_id"])
    return data


def start_group_messages_input(chat_id, user_id, state, defaults_mode=False):
    group_id = state["data"].get("required_group_id")
    if not group_id:
        keyboard = defaults_keyboard() if defaults_mode else condition_edit_keyboard()
        send_flow_message(chat_id, user_id, "<b>请先绑定群聊。</b>\n\n绑定群聊后，再设置最低发言数量。", keyboard)
        return
    state["step"] = "group_condition_messages"
    keyboard = defaults_input_keyboard() if defaults_mode else draft_input_keyboard("draft:edit_conditions")
    send_flow_message(chat_id, user_id, ask_group_messages_prompt(group_id), keyboard)


def handle_defaults_callback(callback_query):
    user = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    user_id = user.get("id")
    data = (callback_query.get("data") or "").split(":", 1)[1]
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "defaults":
        answer_callback_query(callback_query.get("id"), "请先打开默认条件面板。", show_alert=True)
        return

    answer_callback_query(callback_query.get("id"))
    if data in ("group_condition", "group_chat"):
        state["step"] = "group_condition_id"
        state["pending_group_id"] = None
        send_flow_message(chat_id, user_id, ask_group_chat_prompt(), defaults_input_keyboard())
    elif data == "group_messages":
        start_group_messages_input(chat_id, user_id, state, defaults_mode=True)
    elif data == "channel_id":
        state["step"] = "edit"
        state["pending_field"] = "required_channel_id"
        send_flow_message(chat_id, user_id, ask_condition_value(chat_id, "required_channel_id"), defaults_input_keyboard())
    elif data == "clear":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        state["data"]["required_channel_link"] = None
        save_default_conditions(state["data"])
        show_defaults_screen(chat_id, user_id, message_id)
    elif data == "done":
        return_state = state.get("return_state")
        if return_state:
            ADMIN_STATES[user_id] = return_state
            clear_flow_message(chat_id, user_id)
            send_message(chat_id, "<b>✅ 默认条件已保存，已返回批次草稿。</b>", admin_keyboard())
            if return_state.get("step") == "conditions":
                show_batch_condition_screen(chat_id, user_id)
            elif return_state.get("step") == "confirm":
                show_batch_preview(chat_id, user_id)
        else:
            ADMIN_STATES.pop(user_id, None)
            clear_flow_message(chat_id, user_id)
            send_message(chat_id, "<b>✅ 默认条件已保存。</b>", admin_keyboard())


def handle_defaults_state(chat_id, user_id, text):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "defaults":
        return False

    if is_main_menu_command(text) and text != "/defaults":
        ADMIN_STATES.pop(user_id, None)
        clear_flow_message(chat_id, user_id)
        return False

    if state["step"] == "menu":
        send_flow_message(chat_id, user_id, "请点击面板里的按钮修改默认条件，或点击完成退出。", defaults_keyboard())
        return True

    if state["step"] == "edit":
        field = state.get("pending_field")
        if field == "required_channel_id" and text.strip() != "0":
            send_flow_message(chat_id, user_id, ask_condition_value(chat_id, field), defaults_input_keyboard())
            return True
        try:
            apply_condition_input(state, field, text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), defaults_input_keyboard())
            return True
        save_default_conditions(state["data"])
        state["step"] = "menu"
        state["pending_field"] = None
        show_defaults_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_id":
        try:
            group_id = validate_group_chat_id(text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), defaults_input_keyboard())
            return True
        if not group_id:
            state["data"]["required_group_id"] = None
            state["data"]["required_group_messages"] = 0
        else:
            state["data"]["required_group_id"] = group_id
        save_default_conditions(state["data"])
        state["step"] = "menu"
        state["pending_group_id"] = None
        show_defaults_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_messages":
        try:
            group_messages = parse_nonnegative_int(text, "群发言数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), defaults_input_keyboard())
            return True
        if group_messages <= 0:
            send_flow_message(chat_id, user_id, "群发言数必须大于 0。", defaults_input_keyboard())
            return True
        state["data"]["required_group_messages"] = group_messages
        normalize_condition_data(state["data"])
        save_default_conditions(state["data"])
        state["step"] = "menu"
        show_defaults_screen(chat_id, user_id)
        return True

    return False


def handle_draft_callback(callback_query):
    user = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    user_id = user.get("id")
    data = (callback_query.get("data") or "").split(":", 1)[1]
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "newbatch_v2":
        answer_callback_query(callback_query.get("id"), "请先重新开始创建批次。", show_alert=True)
        return

    answer_callback_query(callback_query.get("id"))
    if data == "cancel":
        cancel_new_batch(chat_id, user_id)
    elif data == "type_usage":
        state["data"]["batch_type"] = "usage"
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "type_unique":
        state["data"]["batch_type"] = "unique"
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data in ("group_condition", "group_chat"):
        state["step"] = "group_condition_id"
        state["pending_group_id"] = None
        send_flow_message(chat_id, user_id, ask_group_chat_prompt(), draft_input_keyboard("draft:edit_conditions"))
    elif data == "group_messages":
        start_group_messages_input(chat_id, user_id, state, defaults_mode=False)
    elif data == "channel_id":
        state["step"] = "condition_input"
        state["pending_field"] = "required_channel_id"
        send_flow_message(chat_id, user_id, ask_condition_value(chat_id, "required_channel_id"), draft_input_keyboard("draft:edit_conditions"))
    elif data == "load_defaults":
        with db_connect() as conn:
            state["data"].update(load_default_conditions(conn))
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "clear_conditions":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        state["data"]["required_channel_link"] = None
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "continue":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_usage_limit_prompt(chat_id, user_id)
        else:
            state["step"] = "codes"
            send_unique_codes_prompt(chat_id, user_id)
    elif data == "back_type" or data == "edit_type":
        state["step"] = "type"
        send_batch_type_prompt(chat_id, user_id)
    elif data == "open_defaults":
        begin_defaults_screen(chat_id, user_id, state)
    elif data == "edit_conditions":
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "edit_codes":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_usage_limit_prompt(chat_id, user_id)
        else:
            state["step"] = "codes"
            send_unique_codes_prompt(chat_id, user_id)
    elif data == "back_usage_limit":
        state["step"] = "usage_limit"
        send_usage_limit_prompt(chat_id, user_id)
    elif data == "confirm":
        create_batch_from_draft(chat_id, user_id)


def handle_newbatch_v2_state(chat_id, user_id, text):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "newbatch_v2":
        return False

    if text in ("⬅️ 取消", "❌ 退出创建", "退出创建", "取消操作", "取消"):
        cancel_new_batch(chat_id, user_id)
        return True

    if state["step"] == "name":
        if not text:
            send_flow_message(chat_id, user_id, "批次名称不能为空。", draft_exit_keyboard())
            return True
        state["data"]["name"] = text
        state["step"] = "type"
        send_batch_type_prompt(chat_id, user_id)
        return True

    if state["step"] == "type":
        send_flow_message(chat_id, user_id, "请点击下方按钮选择批次类型。", batch_type_keyboard())
        return True

    if state["step"] == "group_chat_choose":
        token = state.get("group_bind_token") or uuid.uuid4().hex[:12]
        state["group_bind_token"] = token
        state["private_chat_id"] = chat_id
        send_flow_message(chat_id, user_id, ask_group_choose_prompt(), group_choose_keyboard("draft", user_id, token))
        return True

    if state["step"] == "condition_input":
        field = state.get("pending_field")
        if field == "required_channel_id" and text.strip() != "0":
            send_flow_message(chat_id, user_id, ask_condition_value(chat_id, field), draft_input_keyboard("draft:edit_conditions"))
            return True
        try:
            apply_condition_input(state, field, text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), draft_input_keyboard("draft:edit_conditions"))
            return True
        state["step"] = "conditions"
        state["pending_field"] = None
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_id":
        try:
            group_id = validate_group_chat_id(text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), draft_input_keyboard("draft:edit_conditions"))
            return True
        if not group_id:
            state["data"]["required_group_id"] = None
            state["data"]["required_group_messages"] = 0
        else:
            state["data"]["required_group_id"] = group_id
        state["step"] = "conditions"
        state["pending_group_id"] = None
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_messages":
        try:
            group_messages = parse_nonnegative_int(text, "群发言数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), draft_input_keyboard("draft:edit_conditions"))
            return True
        if group_messages <= 0:
            send_flow_message(chat_id, user_id, "群发言数必须大于 0。", draft_input_keyboard("draft:edit_conditions"))
            return True
        state["data"]["required_group_messages"] = group_messages
        normalize_condition_data(state["data"])
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "usage_limit":
        try:
            usage_limit = parse_nonnegative_int(text, "可用次数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), draft_input_keyboard("draft:edit_conditions"))
            return True
        if usage_limit <= 0:
            send_flow_message(chat_id, user_id, "可用次数必须大于 0。", draft_input_keyboard("draft:edit_conditions"))
            return True
        state["data"]["usage_limit"] = usage_limit
        state["step"] = "usage_code"
        send_usage_code_prompt(chat_id, user_id)
        return True

    if state["step"] == "usage_code":
        code = text.strip()
        if not code:
            send_flow_message(chat_id, user_id, "兑换码不能为空。", draft_input_keyboard("draft:back_usage_limit"))
            return True
        state["data"]["shared_code"] = code
        state["step"] = "confirm"
        show_batch_preview(chat_id, user_id)
        return True

    if state["step"] == "codes":
        raw_codes = [line.strip() for line in text.splitlines() if line.strip()]
        if not raw_codes:
            send_flow_message(chat_id, user_id, "请发送兑换码，每行一个。", draft_input_keyboard("draft:edit_conditions"))
            return True
        seen = set()
        codes = []
        duplicated = 0
        for code in raw_codes:
            if code in seen:
                duplicated += 1
                continue
            seen.add(code)
            codes.append(code)
        state["data"]["codes"] = codes
        state["data"]["duplicated"] = duplicated
        state["step"] = "confirm"
        show_batch_preview(chat_id, user_id)
        return True

    if state["step"] == "confirm":
        send_flow_message(chat_id, user_id, "请点击下方按钮确认创建，或返回修改。", confirm_keyboard())
        return True

    return False


GROUP_BIND_QUERY_PREFIX = "bind_group"
GROUP_BIND_COMMAND = "/bindgroup"


def defaults_input_keyboard(back_data="defaults:back_menu"):
    return {
        "inline_keyboard": [
            [
                {"text": "⬅️ 返回", "callback_data": back_data},
                {"text": "✅ 完成", "callback_data": "defaults:done"},
            ],
        ]
    }


def render_condition_lines(data):
    group_id = data.get("required_group_id")
    group_messages = int(data.get("required_group_messages") or 0)
    channel_id = data.get("required_channel_id")
    lines = []
    if group_id:
        if group_messages > 0:
            lines.append(
                "• <b>群发言数</b>：<code>{0}</code> 中普通发言 <b>至少 {1}</b>".format(
                    html.escape(str(group_id)),
                    group_messages,
                )
            )
        else:
            lines.append(
                "• <b>绑定群聊</b>：<code>{0}</code>\n"
                "  发言数量：<i>未设置，暂不启用群发言限制</i>".format(html.escape(str(group_id)))
            )
    else:
        lines.append("• <b>绑定群聊</b>：<i>未绑定</i>")
        lines.append("• <b>发言数量</b>：<i>未设置</i>")
    if channel_id:
        lines.append("• <b>频道订阅</b>：必须订阅 {0}".format(subscription_display_html(data)))
        lines.append("  检测目标：<code>{0}</code>".format(html.escape(str(channel_id))))
    else:
        lines.append("• <b>频道订阅</b>：<i>未启用</i>")
    return lines


def ask_group_chat_prompt():
    return (
        "<b>💬 手动绑定群聊</b>\n\n"
        "请发送要统计发言的群聊 Chat ID。\n\n"
        "群聊 ID 通常是 <code>-100...</code> 这样的负数；个人 User ID 不能用于这里。\n\n"
        "<i>建议优先使用“选择群聊”按钮自动绑定。</i>"
    )


def ask_group_choose_prompt():
    return (
        "<b>💬 绑定群聊</b>\n\n"
        "点击下方 <b>选择群聊</b>，在 Telegram 弹出的列表里选择目标群聊，"
        "然后发送 Bot 提供的 <b>绑定这个群聊</b> 结果。\n\n"
        "<blockquote>发送后 Bot 会静默删除群里的绑定消息，并在这里回写绑定结果。</blockquote>\n\n"
        "<i>如果列表打不开，请先到 BotFather 开启 Inline Mode。</i>"
    )


def group_bind_query(user_id, token):
    return "{0}:{1}:{2}".format(GROUP_BIND_QUERY_PREFIX, user_id, token)


def parse_group_bind_query(query):
    parts = (query or "").strip().split(":")
    if len(parts) != 3 or parts[0] != GROUP_BIND_QUERY_PREFIX:
        return None, None
    try:
        return int(parts[1]), parts[2]
    except Exception:
        return None, None


def parse_group_bind_command(text):
    text = (text or "").strip()
    if not text:
        return None
    parts = text.split(None, 1)
    command = parts[0].split("@", 1)[0]
    if command != GROUP_BIND_COMMAND:
        return None
    return parts[1].strip() if len(parts) > 1 else None


def group_bind_command_text(query):
    command = GROUP_BIND_COMMAND
    if BOT_USERNAME:
        command = "{0}@{1}".format(command, BOT_USERNAME)
    return "{0} {1}".format(command, query)


def group_choose_keyboard(scope, user_id, token):
    query = group_bind_query(user_id, token)
    rows = [
        [
            {
                "text": "💬 选择群聊",
                "switch_inline_query_chosen_chat": {
                    "query": query,
                    "allow_user_chats": False,
                    "allow_bot_chats": False,
                    "allow_group_chats": True,
                    "allow_channel_chats": False,
                },
            }
        ],
        [{"text": "⌨️ 手动输入", "callback_data": "{0}:manual_group_id".format(scope)}],
    ]
    if scope == "defaults":
        rows.append([
            {"text": "⬅️ 返回", "callback_data": "defaults:back_menu"},
            {"text": "✅ 完成", "callback_data": "defaults:done"},
        ])
    else:
        rows.append([
            {"text": "⬅️ 返回", "callback_data": "draft:edit_conditions"},
            {"text": "❌ 取消", "callback_data": "draft:cancel"},
        ])
    return {"inline_keyboard": rows}


def start_group_chat_choose(chat_id, user_id, state, scope):
    token = uuid.uuid4().hex[:12]
    state["step"] = "group_chat_choose"
    state["pending_group_id"] = None
    state["group_bind_token"] = token
    state["private_chat_id"] = chat_id
    send_flow_message(chat_id, user_id, ask_group_choose_prompt(), group_choose_keyboard(scope, user_id, token))


def current_group_bind_scope(state):
    if not state:
        return None
    if state.get("action") == "defaults":
        return "defaults"
    if state.get("action") == "newbatch_v2":
        return "draft"
    return None


def group_bind_state_is_valid(user_id, token):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("step") != "group_chat_choose":
        return False
    if state.get("group_bind_token") != token:
        return False
    return current_group_bind_scope(state) is not None


def bind_group_to_state(user_id, chat):
    state = ADMIN_STATES.get(user_id)
    scope = current_group_bind_scope(state)
    if not state or not scope:
        return False
    chat_type = (chat or {}).get("type")
    private_chat_id = state.get("private_chat_id") or user_id
    if chat_type not in ("group", "supergroup"):
        send_flow_message(
            private_chat_id,
            user_id,
            "<b>⚠️ 请选择群聊</b>\n\n当前选择的不是普通群或超级群，请重新点击按钮选择目标群。",
            group_choose_keyboard(scope, user_id, state.get("group_bind_token") or uuid.uuid4().hex[:12]),
        )
        return True

    target_id = str(chat.get("id"))
    title = chat.get("title") or chat.get("username") or target_id
    remember_chat_info(chat)
    state["data"]["required_group_id"] = target_id
    state["pending_group_id"] = None
    state.pop("group_bind_token", None)
    note = bot_membership_note(target_id)
    text = (
        "<b>✅ 群聊已绑定</b>\n\n"
        "群聊：<b>{0}</b>\n"
        "检测 ID：<code>{1}</code>\n\n"
        "{2}\n\n"
        "<i>接下来可继续设置“🔢 发言数量”。</i>"
    ).format(html.escape(title), html.escape(target_id), note)

    if scope == "defaults":
        save_default_conditions(state["data"])
        state["step"] = "menu"
        send_flow_message(private_chat_id, user_id, text, defaults_keyboard())
    else:
        state["step"] = "conditions"
        send_flow_message(private_chat_id, user_id, text, condition_edit_keyboard())
    return True


def build_group_bind_inline_results(query):
    user_id, token = parse_group_bind_query(query)
    if not user_id or not token or not group_bind_state_is_valid(user_id, token):
        return [
            {
                "type": "article",
                "id": "group_bind_expired",
                "title": "绑定请求已过期",
                "description": "请回到 Bot 私聊，重新点击绑定群聊。",
                "input_message_content": {
                    "message_text": "绑定请求已过期，请回到 Bot 私聊重新点击绑定群聊。"
                },
            }
        ]
    return [
        {
            "type": "article",
            "id": "group_bind_{0}".format(token),
            "title": "绑定这个群聊",
            "description": "发送后 Bot 会自动检测并回到私聊面板。",
            "input_message_content": {
                "message_text": group_bind_command_text(query)
            },
        }
    ]


def handle_inline_query(inline_query):
    query = (inline_query.get("query") or "").strip()
    user = inline_query.get("from") or {}
    user_id = user.get("id")
    if not query.startswith(GROUP_BIND_QUERY_PREFIX + ":"):
        return
    if not is_admin(user_id):
        answer_inline_query(inline_query.get("id"), [], cache_time=0, is_personal=True)
        return
    answer_inline_query(
        inline_query.get("id"),
        build_group_bind_inline_results(query),
        cache_time=0,
        is_personal=True,
    )


def handle_chosen_inline_result(chosen_inline_result):
    query = (chosen_inline_result.get("query") or "").strip()
    if not query.startswith(GROUP_BIND_QUERY_PREFIX + ":"):
        return
    user = chosen_inline_result.get("from") or {}
    user_id = user.get("id")
    if not is_admin(user_id):
        return
    expected_user_id, token = parse_group_bind_query(query)
    if expected_user_id != user_id:
        return
    if not group_bind_state_is_valid(user_id, token):
        return

    chosen_chat = chosen_inline_result.get("chosen_chat")
    if chosen_chat and chosen_chat.get("id"):
        bind_group_to_state(user_id, chosen_chat)
        return

    state = ADMIN_STATES.get(user_id)
    scope = current_group_bind_scope(state)
    if state and scope:
        send_flow_message(
            state.get("private_chat_id") or user_id,
            user_id,
            "<b>⏳ 正在等待群聊回传</b>\n\n"
            "如果这里没有自动完成，请确认：\n"
            "• Bot 已加入目标群\n"
            "• BotFather 的 Privacy Mode 已关闭\n"
            "• 你已经在目标群发送了“绑定这个群聊”结果",
            group_choose_keyboard(scope, user_id, token),
        )


def handle_group_bind_message(message):
    query = parse_group_bind_command(message.get("text") or "")
    if not query:
        return False
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    user_id = user.get("id")
    deleted = delete_message_now(chat.get("id"), message.get("message_id"))
    expected_user_id, token = parse_group_bind_query(query)
    if not expected_user_id or expected_user_id != user_id:
        return True
    if not is_admin(user_id):
        return True
    if not group_bind_state_is_valid(user_id, token):
        send_message(user_id, "<b>⚠️ 群聊绑定请求已过期。</b>\n\n请回到 Bot 私聊重新点击“绑定群聊”。", admin_keyboard())
        return True
    bind_group_to_state(user_id, chat)
    if not deleted:
        send_message(
            user_id,
            "<b>⚠️ 临时绑定消息未能删除</b>\n\n"
            "群聊已绑定成功，但 Bot 没有及时删除群里的绑定消息。"
            "请把 Bot 设为该群管理员，并开启删除消息权限。",
            admin_keyboard(),
        )
    return True


def handle_defaults_state(chat_id, user_id, text):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "defaults":
        return False

    if is_main_menu_command(text) and text != "/defaults":
        ADMIN_STATES.pop(user_id, None)
        clear_flow_message(chat_id, user_id)
        return False

    if state["step"] == "menu":
        send_flow_message(chat_id, user_id, "请点击面板里的按钮修改默认条件，或点击完成退出。", defaults_keyboard())
        return True

    if state["step"] == "group_chat_choose":
        token = state.get("group_bind_token") or uuid.uuid4().hex[:12]
        state["group_bind_token"] = token
        state["private_chat_id"] = chat_id
        send_flow_message(chat_id, user_id, ask_group_choose_prompt(), group_choose_keyboard("defaults", user_id, token))
        return True

    if state["step"] == "edit":
        field = state.get("pending_field")
        if field == "required_channel_id" and text.strip() != "0":
            send_flow_message(chat_id, user_id, ask_condition_value(chat_id, field), defaults_input_keyboard())
            return True
        try:
            apply_condition_input(state, field, text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), defaults_input_keyboard())
            return True
        save_default_conditions(state["data"])
        state["step"] = "menu"
        state["pending_field"] = None
        show_defaults_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_id":
        try:
            group_id = validate_group_chat_id(text)
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), defaults_input_keyboard())
            return True
        if not group_id:
            state["data"]["required_group_id"] = None
            state["data"]["required_group_messages"] = 0
        else:
            state["data"]["required_group_id"] = group_id
        save_default_conditions(state["data"])
        state["step"] = "menu"
        state["pending_group_id"] = None
        show_defaults_screen(chat_id, user_id)
        return True

    if state["step"] == "group_condition_messages":
        try:
            group_messages = parse_nonnegative_int(text, "群发言数")
        except ValueError as exc:
            send_flow_message(chat_id, user_id, str(exc), defaults_input_keyboard())
            return True
        if group_messages <= 0:
            send_flow_message(chat_id, user_id, "群发言数必须大于 0。", defaults_input_keyboard())
            return True
        state["data"]["required_group_messages"] = group_messages
        normalize_condition_data(state["data"])
        save_default_conditions(state["data"])
        state["step"] = "menu"
        show_defaults_screen(chat_id, user_id)
        return True

    return False


def handle_defaults_callback(callback_query):
    user = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    user_id = user.get("id")
    data = (callback_query.get("data") or "").split(":", 1)[1]
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "defaults":
        answer_callback_query(callback_query.get("id"), "请先打开默认条件面板。", show_alert=True)
        return

    answer_callback_query(callback_query.get("id"))
    if data in ("group_condition", "group_chat"):
        start_group_chat_choose(chat_id, user_id, state, "defaults")
    elif data == "manual_group_id":
        state["step"] = "group_condition_id"
        state["pending_group_id"] = None
        send_flow_message(chat_id, user_id, ask_group_chat_prompt(), defaults_input_keyboard("defaults:back_menu"))
    elif data == "back_menu":
        state["step"] = "menu"
        state["pending_field"] = None
        state["pending_group_id"] = None
        show_defaults_screen(chat_id, user_id, message_id)
    elif data == "group_messages":
        start_group_messages_input(chat_id, user_id, state, defaults_mode=True)
    elif data == "channel_id":
        state["step"] = "edit"
        state["pending_field"] = "required_channel_id"
        send_flow_message(chat_id, user_id, ask_condition_value(chat_id, "required_channel_id"), defaults_input_keyboard("defaults:back_menu"))
    elif data == "clear":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        state["data"]["required_channel_link"] = None
        save_default_conditions(state["data"])
        show_defaults_screen(chat_id, user_id, message_id)
    elif data == "done":
        return_state = state.get("return_state")
        if return_state:
            ADMIN_STATES[user_id] = return_state
            clear_flow_message(chat_id, user_id)
            send_message(chat_id, "<b>✅ 默认条件已保存，已返回批次草稿。</b>", admin_keyboard())
            if return_state.get("step") == "conditions":
                show_batch_condition_screen(chat_id, user_id)
            elif return_state.get("step") == "confirm":
                show_batch_preview(chat_id, user_id)
        else:
            ADMIN_STATES.pop(user_id, None)
            clear_flow_message(chat_id, user_id)
            send_message(chat_id, "<b>✅ 默认条件已保存。</b>", admin_keyboard())


def handle_draft_callback(callback_query):
    user = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    user_id = user.get("id")
    data = (callback_query.get("data") or "").split(":", 1)[1]
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "newbatch_v2":
        answer_callback_query(callback_query.get("id"), "请先重新开始创建批次。", show_alert=True)
        return

    answer_callback_query(callback_query.get("id"))
    if data == "cancel":
        cancel_new_batch(chat_id, user_id)
    elif data == "type_usage":
        state["data"]["batch_type"] = "usage"
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "type_unique":
        state["data"]["batch_type"] = "unique"
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data in ("group_condition", "group_chat"):
        start_group_chat_choose(chat_id, user_id, state, "draft")
    elif data == "manual_group_id":
        state["step"] = "group_condition_id"
        state["pending_group_id"] = None
        send_flow_message(chat_id, user_id, ask_group_chat_prompt(), draft_input_keyboard("draft:edit_conditions"))
    elif data == "group_messages":
        start_group_messages_input(chat_id, user_id, state, defaults_mode=False)
    elif data == "channel_id":
        state["step"] = "condition_input"
        state["pending_field"] = "required_channel_id"
        send_flow_message(chat_id, user_id, ask_condition_value(chat_id, "required_channel_id"), draft_input_keyboard("draft:edit_conditions"))
    elif data == "load_defaults":
        with db_connect() as conn:
            state["data"].update(load_default_conditions(conn))
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "clear_conditions":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        state["data"]["required_channel_link"] = None
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "continue":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_usage_limit_prompt(chat_id, user_id)
        else:
            state["step"] = "codes"
            send_unique_codes_prompt(chat_id, user_id)
    elif data == "back_type" or data == "edit_type":
        state["step"] = "type"
        send_batch_type_prompt(chat_id, user_id)
    elif data == "open_defaults":
        begin_defaults_screen(chat_id, user_id, state)
    elif data == "edit_conditions":
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "edit_codes":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_usage_limit_prompt(chat_id, user_id)
        else:
            state["step"] = "codes"
            send_unique_codes_prompt(chat_id, user_id)
    elif data == "back_usage_limit":
        state["step"] = "usage_limit"
        send_usage_limit_prompt(chat_id, user_id)
    elif data == "confirm":
        create_batch_from_draft(chat_id, user_id)


def handle_chatid(chat_id, message):
    chat = message.get("chat") or {}
    title = chat.get("title") or chat.get("username") or "当前会话"
    send_message(
        chat_id,
        "<b>💬 {0}</b>\n\nChat ID：<code>{1}</code>\n\n"
        "<i>Bot 现在只在私聊响应；群聊中只会静默统计发言数。</i>".format(
            html.escape(title),
            html.escape(str(chat_id)),
        ),
    )


def handle_whoami(chat_id, user):
    username = user.get("username")
    username_text = "@{0}".format(username) if username else "-"
    send_message(
        chat_id,
        "你的 Telegram User ID：<code>{0}</code>\n用户名：{1}".format(
            user["id"],
            html.escape(username_text),
        ),
    )


def process_message(message):
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    text = (message.get("text") or "").strip()
    text = {
        "📦 创建批次": "/newbatch",
        "📋 批次列表": "/batches",
        "🗒 最近记录": "/records",
        "⚙️ 默认条件": "/defaults",
        "🧩 核心流程": "/flow",
        "📊 已记录群": "/groups",
        "🛡 接收状态": "/botstatus",
        "👤 我的ID": "/whoami",
        "⬅️ 取消": "取消操作",
    }.get(text, text)
    text = normalize_main_menu_text(text)
    chat_id = chat.get("id")
    user_id = user.get("id")
    if not chat_id or not user_id:
        return

    if not is_private_chat(chat):
        if handle_group_bind_message(message):
            return
        record_group_message(message)
        return

    record_group_message(message)

    if handle_forwarded_chat(chat_id, user_id, message):
        delete_message(chat_id, message.get("message_id"))
        return

    delete_message(chat_id, message.get("message_id"))

    if handle_newbatch_v2_state(chat_id, user_id, text):
        return
    if handle_defaults_state(chat_id, user_id, text):
        return

    if text in ("取消操作", "⬅️ 取消"):
        ADMIN_STATES.pop(user_id, None)
        send_message(chat_id, "<b>已取消当前操作。</b>", admin_keyboard())
    elif text.startswith("/start"):
        handle_start(chat_id, user, text)
    elif text.startswith("/admin"):
        show_admin_panel_v2(chat_id, user_id)
    elif text.startswith("/whoami"):
        handle_whoami(chat_id, user)
    elif text.startswith("/chatid"):
        handle_chatid(chat_id, message)
    elif text.startswith("/groups") or text in ("📊 已记录群", "已记录群"):
        show_seen_groups(chat_id, user_id)
    elif text.startswith("/botstatus") or text in ("🛡 接收状态", "接收状态"):
        send_message(chat_id, "<b>该功能已移除。</b>", admin_keyboard())
    elif text.startswith("/defaults") or text == "⚙️ 默认条件":
        begin_defaults_screen(chat_id, user_id)
    elif text.startswith("/newbatch") or text in ("创建批次", "创建兑换码批次", "📦 创建批次"):
        if is_admin(user_id):
            begin_new_batch_v2(chat_id, user_id)
        else:
            send_message(chat_id, "你不是管理员。")
    elif text.startswith("/batches") or text in ("批次列表", "📋 批次列表"):
        list_batches(chat_id, user_id)
    elif text.startswith("/batch"):
        show_batch_detail(chat_id, user_id, text)
    elif text.startswith("/records") or text in ("最近领取记录", "🗒 最近记录"):
        show_records(chat_id, user_id)
    elif text.startswith("/flow") or text in ("核心流程", "🧭 核心流程"):
        show_flow_v2(chat_id, user_id)
    else:
        if is_admin(user_id):
            send_message(chat_id, "<b>请选择管理员操作。</b>", admin_keyboard())
        else:
            send_message(chat_id, "请通过管理员分享的专属领取链接进入。")


def configure_bot_menu():
    user_commands = [
        {"command": "start", "description": "通过专属链接领取兑换码"},
        {"command": "whoami", "description": "查看自己的 Telegram User ID"},
    ]
    admin_commands = user_commands + [
        {"command": "admin", "description": "管理员面板"},
        {"command": "newbatch", "description": "创建兑换码批次"},
        {"command": "defaults", "description": "设置默认领取条件"},
        {"command": "batches", "description": "查看批次列表"},
        {"command": "batch", "description": "查看批次详情"},
        {"command": "records", "description": "查看最近领取记录"},
        {"command": "flow", "description": "查看核心流程"},
        {"command": "groups", "description": "查看已记录群 ID"},
        {"command": "chatid", "description": "查看当前会话 ID"},
    ]
    api_call("deleteWebhook", {"drop_pending_updates": "false"})
    api_call("deleteMyCommands")
    api_call("deleteMyCommands", {"scope": json.dumps({"type": "all_group_chats"})})
    api_call("deleteMyCommands", {"scope": json.dumps({"type": "all_chat_administrators"})})
    api_call(
        "setMyCommands",
        {
            "scope": json.dumps({"type": "all_private_chats"}),
            "commands": json.dumps(user_commands, ensure_ascii=False),
        },
    )
    for admin_id in ADMIN_IDS:
        api_call(
            "setMyCommands",
            {
                "scope": json.dumps({"type": "chat", "chat_id": admin_id}),
                "commands": json.dumps(admin_commands, ensure_ascii=False),
            },
        )


def get_update_user_id(update):
    if "message" in update:
        user = (update.get("message") or {}).get("from") or {}
        return user.get("id")
    if "callback_query" in update:
        user = (update.get("callback_query") or {}).get("from") or {}
        return user.get("id")
    if "inline_query" in update:
        user = (update.get("inline_query") or {}).get("from") or {}
        return user.get("id")
    if "chosen_inline_result" in update:
        user = (update.get("chosen_inline_result") or {}).get("from") or {}
        return user.get("id")
    return None


def get_user_lock(user_id):
    key = user_id if user_id is not None else "anonymous"
    with USER_LOCKS_GUARD:
        lock = USER_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            USER_LOCKS[key] = lock
        return lock


def process_update(update):
    if "message" in update:
        process_message(update["message"])
    elif "callback_query" in update:
        handle_callback(update["callback_query"])
    elif "inline_query" in update:
        handle_inline_query(update["inline_query"])
    elif "chosen_inline_result" in update:
        handle_chosen_inline_result(update["chosen_inline_result"])


def run_update(update):
    user_id = get_update_user_id(update)
    with get_user_lock(user_id):
        process_update(update)


def update_done(future):
    try:
        exc = future.exception()
        if exc:
            logging.error("Update worker error: %s", exc, exc_info=(type(exc), exc, exc.__traceback__))
    finally:
        INFLIGHT_SEMAPHORE.release()


def submit_update(update):
    INFLIGHT_SEMAPHORE.acquire()
    future = UPDATE_EXECUTOR.submit(run_update, update)
    future.add_done_callback(update_done)


def poll_loop():
    global UPDATE_EXECUTOR
    offset = None
    UPDATE_EXECUTOR = ThreadPoolExecutor(max_workers=UPDATE_WORKERS)
    try:
        while True:
            try:
                params = {
                    "timeout": POLL_TIMEOUT,
                    "allowed_updates": json.dumps(
                        ["message", "callback_query", "inline_query", "chosen_inline_result"]
                    ),
                }
                if offset is not None:
                    params["offset"] = offset
                updates = api_call("getUpdates", params, timeout=POLL_TIMEOUT + 10)
                for update in updates:
                    offset = update["update_id"] + 1
                    submit_update(update)
            except (HTTPError, URLError, RuntimeError, sqlite3.Error, socket.timeout, TimeoutError) as exc:
                logging.exception("Polling error: %s", exc)
                time.sleep(2)
            except KeyboardInterrupt:
                logging.info("Bot stopped")
                break
    finally:
        UPDATE_EXECUTOR.shutdown(wait=True)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is required in .env")
    ensure_dirs()
    setup_logging()
    init_db()
    start_delete_worker()
    configure_bot_menu()
    logging.info(
        "Bot started workers=%s max_inflight=%s api_timeout=%s poll_timeout=%s",
        UPDATE_WORKERS,
        MAX_INFLIGHT_UPDATES,
        API_TIMEOUT,
        POLL_TIMEOUT,
    )
    poll_loop()


if __name__ == "__main__":
    main()
