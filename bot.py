#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import print_function

import html
import copy
import json
import logging
import os
import random
import sqlite3
import sys
import time
import uuid
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


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
API_BASE = "https://api.telegram.org/bot{0}/".format(BOT_TOKEN)

ADMIN_STATES = {}
VERIFY_STATES = {}


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


def api_call(method, data=None):
    if data is None:
        data = {}
    body = urlencode(data).encode("utf-8")
    request = Request(API_BASE + method, data=body)
    with urlopen(request, timeout=60) as response:
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


def answer_callback_query(callback_query_id, text=None, show_alert=False):
    data = {"callback_query_id": callback_query_id}
    if text:
        data["text"] = text
    if show_alert:
        data["show_alert"] = "true"
    return api_call("answerCallbackQuery", data)


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


def get_chat_member(chat_id, user_id):
    try:
        return api_call("getChatMember", {"chat_id": chat_id, "user_id": user_id})
    except Exception as exc:
        logging.warning("getChatMember failed chat=%s user=%s error=%s", chat_id, user_id, exc)
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
    group_id = parse_nullable_text(data.get("required_group_id"))
    if group_messages <= 0:
        group_id = None
        group_messages = 0
    elif not group_id:
        raise ValueError("启用群发言数条件时，必须填写群 ID。")
    channel_id = parse_nullable_text(data.get("required_channel_id"))
    data["required_group_id"] = group_id
    data["required_group_messages"] = group_messages
    data["required_channel_id"] = channel_id
    return data


def load_default_conditions(conn):
    data = {
        "required_group_id": parse_nullable_text(get_setting(conn, "default_required_group_id", "")),
        "required_group_messages": int(get_setting(conn, "default_required_group_messages", "0") or 0),
        "required_channel_id": parse_nullable_text(get_setting(conn, "default_required_channel_id", "")),
    }
    try:
        return normalize_condition_data(data)
    except ValueError:
        return {
            "required_group_id": None,
            "required_group_messages": 0,
            "required_channel_id": None,
        }


def render_condition_lines(data):
    group_id = data.get("required_group_id")
    group_messages = int(data.get("required_group_messages") or 0)
    channel_id = data.get("required_channel_id")
    lines = []
    if group_id and group_messages > 0:
        lines.append("• <b>群发言数</b>：{0} 中累计发言 <b>大于 {1}</b>".format(html.escape(str(group_id)), group_messages))
    else:
        lines.append("• <b>群发言数</b>：<i>未启用</i>")
    if channel_id:
        lines.append("• <b>频道关注</b>：必须关注 <b>{0}</b>".format(html.escape(str(channel_id))))
    else:
        lines.append("• <b>频道关注</b>：<i>未启用</i>")
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
        edit_message_text(chat_id, message_id, text, defaults_keyboard())
    else:
        send_message(chat_id, text, defaults_keyboard())


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
        "<blockquote>需要群发言数时，请先设置群 ID，再设置发言数。频道可填写 @用户名或频道 ID。</blockquote>"
    )
    if message_id:
        edit_message_text(chat_id, message_id, text, condition_edit_keyboard())
    else:
        send_message(chat_id, text, condition_edit_keyboard())


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
        edit_message_text(chat_id, message_id, text, confirm_keyboard())
    else:
        send_message(chat_id, text, confirm_keyboard())


def admin_keyboard():
    return {
        "keyboard": [
            [{"text": "📦 创建批次"}, {"text": "📋 批次列表"}],
            [{"text": "⚙️ 默认条件"}, {"text": "🧭 核心流程"}],
            [{"text": "🗒 最近记录"}, {"text": "⬅️ 取消"}],
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
                {"text": "✏️ 群 ID", "callback_data": "draft:group_id"},
                {"text": "✏️ 发言数", "callback_data": "draft:group_messages"},
            ],
            [
                {"text": "📢 频道", "callback_data": "draft:channel_id"},
                {"text": "📥 载入默认", "callback_data": "draft:load_defaults"},
            ],
            [
                {"text": "🧹 清空", "callback_data": "draft:clear_conditions"},
                {"text": "✅ 继续", "callback_data": "draft:continue"},
            ],
            [
                {"text": "⬅️ 上一步", "callback_data": "draft:back_type"},
                {"text": "⚙️ 默认值", "callback_data": "draft:open_defaults"},
            ],
        ]
    }


def defaults_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "✏️ 群 ID", "callback_data": "defaults:group_id"},
                {"text": "✏️ 发言数", "callback_data": "defaults:group_messages"},
            ],
            [
                {"text": "📢 频道", "callback_data": "defaults:channel_id"},
                {"text": "🧹 清空", "callback_data": "defaults:clear"},
            ],
            [
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


def make_captcha():
    operator = random.choice(["+", "-"])
    left = random.randint(1, 9)
    right = random.randint(1, 9)
    if operator == "-" and left < right:
        left, right = right, left
    answer = left + right if operator == "+" else left - right
    question = "{0} {1} {2} = ?".format(left, operator, right)
    return question, answer


def verify_keyboard(token, answer):
    options = set([answer])
    while len(options) < 4:
        candidate = answer + random.randint(-3, 3)
        if 0 <= candidate <= 18:
            options.add(candidate)
    options = list(options)
    random.shuffle(options)
    return {
        "inline_keyboard": [
            [
                {
                    "text": str(option),
                    "callback_data": "captcha:{0}:{1}".format(token, option),
                }
                for option in options[:2]
            ],
            [
                {
                    "text": str(option),
                    "callback_data": "captcha:{0}:{1}".format(token, option),
                }
                for option in options[2:]
            ],
        ]
    }


def is_admin(user_id):
    return user_id in ADMIN_IDS


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
            "• <b>群发言数</b>：{0} 中累计发言 <b>大于 {1}</b>".format(
                html.escape(str(batch["required_group_id"])),
                batch["required_group_messages"],
            )
        )
    if batch["required_channel_id"]:
        lines.append("• <b>频道关注</b>：必须关注 <b>{0}</b>".format(html.escape(str(batch["required_channel_id"]))))
    if not lines:
        lines.append("• <b>领取条件</b>：<i>无额外条件</i>")
    return ["<b>⚙️ 领取条件</b>"] + lines


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
    with db_connect() as conn:
        upsert_user(conn, user)
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
        row = conn.execute(
            """
            SELECT message_count
            FROM user_chat_stats
            WHERE telegram_id = ? AND chat_id = ?
            """,
            (user["id"], str(group_id)),
        ).fetchone()
        message_count = row["message_count"] if row else 0
        if message_count <= group_messages:
            return (
                False,
                "group_messages_not_enough",
                "<b>领取失败</b>\n\n"
                "你在指定群聊中的累计发言数是 <b>{0}</b>，需要大于 <b>{1}</b> 才能领取。".format(
                    message_count,
                    group_messages,
                ),
            )

    channel_id = batch["required_channel_id"]
    if channel_id:
        member = get_chat_member(channel_id, user["id"])
        if not member_is_joined(member):
            return (
                False,
                "channel_not_joined",
                "<b>领取失败</b>\n\n请先关注指定频道，然后重新打开领取链接。",
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
        {"command": "chatid", "description": "查看当前群聊 ID"},
    ]
    api_call("deleteWebhook", {"drop_pending_updates": "false"})
    api_call("setMyCommands", {"commands": json.dumps(user_commands, ensure_ascii=False)})
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
        "8. 用户进入后先做加减法验证\n"
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
    question, answer = make_captcha()
    VERIFY_STATES[user["id"]] = {
        "token": verify_token,
        "batch_token": token,
        "answer": answer,
    }
    send_message(
        chat_id,
        "<b>🎁 准备领取</b>\n\n"
        "• 批次：<b>{0}</b>\n\n"
        "{1}\n\n"
        "<b>✅ 人机验证</b>\n"
        "请点击正确答案：<code>{2}</code>\n\n"
        "<i>验证通过后，系统会自动校验资格并发放兑换码。</i>".format(
            html.escape(batch["name"]),
            "\n".join(condition_lines(batch)),
            html.escape(question),
        ),
        verify_keyboard(verify_token, answer),
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
    try:
        selected_answer = int(parts[2])
    except ValueError:
        answer_callback_query(callback_id, "验证数据异常，请重新打开领取链接。", show_alert=True)
        return
    state = VERIFY_STATES.get(user_id)
    if not state or state.get("token") != token:
        answer_callback_query(callback_id, "验证已失效，请重新打开领取链接。", show_alert=True)
        return
    if selected_answer != state.get("answer"):
        VERIFY_STATES.pop(user_id, None)
        with db_connect() as conn:
            upsert_user(conn, user)
            batch = find_batch(conn, state["batch_token"])
            if batch:
                log_claim(conn, user, batch, None, "failed", 0, "captcha_failed")
        answer_callback_query(callback_id, "验证失败，请重新打开领取链接。", show_alert=True)
        send_message(chat_id, "人机验证失败，本次领取已终止。请重新打开领取链接后再试。")
        return
    VERIFY_STATES.pop(user_id, None)
    answer_callback_query(callback_id, "验证通过")
    issue_code(chat_id, user, state["batch_token"])


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
    send_message(
        chat_id,
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
    send_message(chat_id, render_defaults_summary(defaults) + "\n\n<i>点击按钮修改默认值。</i>", defaults_keyboard())


def show_admin_panel_v2(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员，无法使用管理功能。")
        return
    send_message(
        chat_id,
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
    send_message(
        chat_id,
        "<b>🧭 核心流程</b>\n\n"
        "1. 点击 <code>📦 创建批次</code>\n"
        "2. 输入批次名称\n"
        "3. 选择批次类型\n"
        "4. 用按钮调整领取条件\n"
        "5. 导入兑换码并确认创建\n"
        "6. 分享专属领取链接\n"
        "7. 用户完成加减法验证\n"
        "8. 系统校验群发言数 / 频道关注条件\n"
        "9. 自动发放兑换码并记录日志\n\n"
        "<i>普通用户不会看到领取入口，只能通过专属链接领取。</i>",
        admin_keyboard(),
    )


def ask_condition_value(chat_id, field):
    prompt_map = {
        "required_group_id": "<b>✏️ 设置群 ID</b>\n\n请输入群 ID，或输入 <code>0</code> 清空。",
        "required_group_messages": "<b>✏️ 设置群发言数</b>\n\n请输入阈值，用户累计发言数必须 <b>大于</b> 这个数字。\n输入 <code>0</code> 表示不启用。",
        "required_channel_id": "<b>📢 设置频道</b>\n\n请输入频道 <code>@用户名</code> 或频道 ID。\n输入 <code>0</code> 表示不启用。",
    }
    return prompt_map.get(field, "请输入内容。")


def apply_condition_input(state, field, text):
    text = text.strip()
    if field == "required_group_id":
        state["data"]["required_group_id"] = parse_nullable_text(text)
    elif field == "required_group_messages":
        state["data"]["required_group_messages"] = parse_nonnegative_int(text, "群发言数")
    elif field == "required_channel_id":
        state["data"]["required_channel_id"] = parse_nullable_text(text)
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
                        required_group_id, required_group_messages, required_channel_id,
                        created_by, created_at
                    )
                VALUES (?, ?, 'usage', ?, ?, 0, ?, ?, ?, ?, ?)
                """,
                (
                    token,
                    data["name"],
                    data["shared_code"],
                    int(data["usage_limit"]),
                    data.get("required_group_id"),
                    int(data.get("required_group_messages") or 0),
                    data.get("required_channel_id"),
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
                        required_group_id, required_group_messages, required_channel_id,
                        created_by, created_at
                    )
                VALUES (?, ?, 'unique', ?, ?, ?, ?, ?)
                """,
                (
                    token,
                    data["name"],
                    data.get("required_group_id"),
                    int(data.get("required_group_messages") or 0),
                    data.get("required_channel_id"),
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
        send_message(chat_id, "<b>已取消创建批次。</b>", admin_keyboard())
        return True

    if text in ("⬅️ 返回", "返回"):
        if state["step"] == "type":
            ADMIN_STATES.pop(user_id, None)
            begin_new_batch_v2(chat_id, user_id)
        elif state["step"] == "conditions":
            state["step"] = "type"
            send_message(chat_id, "<b>🔁 选择批次类型</b>\n\n请点击一个选项。", batch_type_keyboard())
        elif state["step"] == "codes":
            state["step"] = "conditions"
            show_batch_condition_screen(chat_id, user_id)
        elif state["step"] == "usage_limit":
            state["step"] = "conditions"
            show_batch_condition_screen(chat_id, user_id)
        elif state["step"] == "usage_code":
            state["step"] = "usage_limit"
            send_message(
                chat_id,
                "<b>🧾 第 4 步 / 共 5 步</b>\n\n请输入这个兑换码可被领取的次数。\n\n"
                "<i>示例：</i> <code>100</code>",
                admin_keyboard(),
            )
        return True

    if state["step"] == "name":
        if not text:
            send_message(chat_id, "批次名称不能为空。")
            return True
        state["data"]["name"] = text
        state["step"] = "type"
        send_message(chat_id, "<b>🔁 第 2 步 / 共 5 步</b>\n\n请选择批次类型。", batch_type_keyboard())
        return True

    if state["step"] == "type":
        if text in ("🔁 使用次数", "使用次数", "usage"):
            state["data"]["batch_type"] = "usage"
        elif text in ("🎁 领完为止", "领完为止", "unique"):
            state["data"]["batch_type"] = "unique"
        else:
            send_message(chat_id, "请点击一个类型按钮。", batch_type_keyboard())
            return True
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "condition_input":
        field = state.get("pending_field")
        try:
            apply_condition_input(state, field, text)
        except ValueError as exc:
            send_message(chat_id, str(exc))
            return True
        state["step"] = "conditions"
        state["pending_field"] = None
        show_batch_condition_screen(chat_id, user_id)
        return True

    if state["step"] == "usage_limit":
        try:
            usage_limit = parse_nonnegative_int(text, "可用次数")
        except ValueError as exc:
            send_message(chat_id, str(exc))
            return True
        if usage_limit <= 0:
            send_message(chat_id, "可用次数必须大于 0。")
            return True
        state["data"]["usage_limit"] = usage_limit
        state["step"] = "usage_code"
        send_message(
            chat_id,
            "<b>🧾 第 5 步 / 共 5 步</b>\n\n请输入这一个兑换码。\n"
            "<i>示例：</i> <code>ABC-DEF-001</code>",
            admin_keyboard(),
        )
        return True

    if state["step"] == "usage_code":
        code = text.strip()
        if not code:
            send_message(chat_id, "兑换码不能为空。")
            return True
        state["data"]["shared_code"] = code
        state["step"] = "confirm"
        show_batch_preview(chat_id, user_id)
        return True

    if state["step"] == "codes":
        raw_codes = [line.strip() for line in text.splitlines() if line.strip()]
        if not raw_codes:
            send_message(chat_id, "请发送兑换码，每行一个。")
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
        send_message(chat_id, "请点击下方按钮确认创建，或返回修改。", confirm_keyboard())
        return True

    return False


def handle_defaults_state(chat_id, user_id, text):
    state = ADMIN_STATES.get(user_id)
    if not state or state.get("action") != "defaults":
        return False

    if text in ("⬅️ 取消", "取消操作"):
        ADMIN_STATES.pop(user_id, None)
        send_message(chat_id, "<b>已取消默认值设置。</b>", admin_keyboard())
        return True

    if text == "⬅️ 返回":
        ADMIN_STATES.pop(user_id, None)
        show_defaults_screen(chat_id, user_id)
        return True

    if state["step"] == "menu":
        send_message(chat_id, "请点击下方按钮修改默认值。", defaults_keyboard())
        return True

    if state["step"] == "edit":
        field = state.get("pending_field")
        try:
            apply_condition_input(state, field, text)
        except ValueError as exc:
            send_message(chat_id, str(exc))
            return True
        with db_connect() as conn:
            set_setting(conn, "default_required_group_id", state["data"].get("required_group_id") or "")
            set_setting(conn, "default_required_group_messages", str(state["data"].get("required_group_messages") or 0))
            set_setting(conn, "default_required_channel_id", state["data"].get("required_channel_id") or "")
        state["step"] = "menu"
        state["pending_field"] = None
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
    if data == "group_id":
        state["step"] = "condition_input"
        state["pending_field"] = "required_group_id"
        send_message(chat_id, ask_condition_value(chat_id, "required_group_id"), admin_keyboard())
    elif data == "group_messages":
        state["step"] = "condition_input"
        state["pending_field"] = "required_group_messages"
        send_message(chat_id, ask_condition_value(chat_id, "required_group_messages"), admin_keyboard())
    elif data == "channel_id":
        state["step"] = "condition_input"
        state["pending_field"] = "required_channel_id"
        send_message(chat_id, ask_condition_value(chat_id, "required_channel_id"), admin_keyboard())
    elif data == "load_defaults":
        with db_connect() as conn:
            state["data"].update(load_default_conditions(conn))
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "clear_conditions":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "continue":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_message(
                chat_id,
                "<b>🧾 第 4 步 / 共 5 步</b>\n\n"
                "请输入这个兑换码可被领取的次数。\n\n"
                "<i>示例：</i> <code>100</code>",
                admin_keyboard(),
            )
        else:
            state["step"] = "codes"
            send_message(
                chat_id,
                "<b>🎁 第 4 步 / 共 5 步</b>\n\n"
                "请批量发送兑换码，每行一个。\n\n"
                "<i>示例：</i>\n<code>CODE001</code>\n<code>CODE002</code>\n<code>CODE003</code>",
                admin_keyboard(),
            )
    elif data == "back_type":
        state["step"] = "type"
        send_message(chat_id, "<b>🔁 第 2 步 / 共 5 步</b>\n\n请选择批次类型。", batch_type_keyboard())
    elif data == "open_defaults":
        begin_defaults_screen(chat_id, user_id, state)
    elif data == "edit_conditions":
        state["step"] = "conditions"
        show_batch_condition_screen(chat_id, user_id, message_id)
    elif data == "edit_codes":
        if state["data"]["batch_type"] == "usage":
            state["step"] = "usage_limit"
            send_message(
                chat_id,
                "<b>🧾 第 4 步 / 共 5 步</b>\n\n"
                "请输入这个兑换码可被领取的次数。\n\n"
                "<i>示例：</i> <code>100</code>",
                admin_keyboard(),
            )
        else:
            state["step"] = "codes"
            send_message(
                chat_id,
                "<b>🎁 第 4 步 / 共 5 步</b>\n\n"
                "请批量发送兑换码，每行一个。\n\n"
                "<i>示例：</i>\n<code>CODE001</code>\n<code>CODE002</code>\n<code>CODE003</code>",
                admin_keyboard(),
            )
    elif data == "edit_type":
        state["step"] = "type"
        send_message(chat_id, "<b>🔁 第 2 步 / 共 5 步</b>\n\n请选择批次类型。", batch_type_keyboard())
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
    if data in ("group_id", "group_messages", "channel_id"):
        state["step"] = "edit"
        state["pending_field"] = {
            "group_id": "required_group_id",
            "group_messages": "required_group_messages",
            "channel_id": "required_channel_id",
        }[data]
        send_message(chat_id, ask_condition_value(chat_id, state["pending_field"]), admin_keyboard())
    elif data == "clear":
        state["data"]["required_group_id"] = None
        state["data"]["required_group_messages"] = 0
        state["data"]["required_channel_id"] = None
        with db_connect() as conn:
            set_setting(conn, "default_required_group_id", "")
            set_setting(conn, "default_required_group_messages", "0")
            set_setting(conn, "default_required_channel_id", "")
        show_defaults_screen(chat_id, user_id, message_id)
    elif data == "done":
        return_state = state.get("return_state")
        if return_state:
            ADMIN_STATES[user_id] = return_state
            send_message(chat_id, "<b>✅ 默认值已保存，已返回批次草稿。</b>", admin_keyboard())
            if return_state.get("step") == "conditions":
                show_batch_condition_screen(chat_id, user_id)
            elif return_state.get("step") == "confirm":
                show_batch_preview(chat_id, user_id)
        else:
            ADMIN_STATES.pop(user_id, None)
            send_message(chat_id, "<b>✅ 默认值已保存。</b>", admin_keyboard())
def handle_chatid(chat_id, message):
    chat = message.get("chat") or {}
    title = chat.get("title") or chat.get("username") or "当前会话"
    send_message(
        chat_id,
        "<b>💬 {0}</b>\n\nChat ID：<code>{1}</code>\n\n"
        "<i>创建批次或设置默认条件时，点击“群 ID”按钮后填入这个值。</i>".format(
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
    chat_id = chat.get("id")
    user_id = user.get("id")
    if not chat_id or not user_id:
        return

    record_group_message(message)

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


def poll_loop():
    offset = None
    while True:
        try:
            params = {"timeout": 50}
            if offset is not None:
                params["offset"] = offset
            updates = api_call("getUpdates", params)
            for update in updates:
                offset = update["update_id"] + 1
                if "message" in update:
                    process_message(update["message"])
                elif "callback_query" in update:
                    handle_callback(update["callback_query"])
        except (HTTPError, URLError, RuntimeError, sqlite3.Error) as exc:
            logging.exception("Polling error: %s", exc)
            time.sleep(2)
        except KeyboardInterrupt:
            logging.info("Bot stopped")
            break


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is required in .env")
    ensure_dirs()
    setup_logging()
    init_db()
    configure_bot_menu()
    logging.info("Bot started")
    poll_loop()


if __name__ == "__main__":
    main()
