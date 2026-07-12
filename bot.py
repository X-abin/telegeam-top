#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import print_function

import html
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


def get_chat_member(chat_id, user_id):
    try:
        return api_call("getChatMember", {"chat_id": chat_id, "user_id": user_id})
    except Exception as exc:
        logging.warning("getChatMember failed chat=%s user=%s error=%s", chat_id, user_id, exc)
        return None


def admin_keyboard():
    return {
        "keyboard": [
            [{"text": "创建兑换码批次"}, {"text": "批次列表"}],
            [{"text": "核心流程"}, {"text": "最近领取记录"}],
            [{"text": "取消操作"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def batch_type_keyboard():
    return {
        "keyboard": [
            [{"text": "使用次数"}, {"text": "领完为止"}],
            [{"text": "取消操作"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
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


def empty_value(value):
    return value.strip().lower() in ("", "0", "无", "none", "no", "-")


def clean_condition_value(value):
    value = value.strip()
    if empty_value(value):
        return None
    return value


def parse_batch_conditions(text):
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    if len(lines) < 3:
        raise ValueError("请按 3 行发送领取条件。")
    group_id = clean_condition_value(lines[0])
    try:
        group_messages = int(lines[1])
    except ValueError:
        raise ValueError("群发言数必须是数字。")
    if group_messages < 0:
        raise ValueError("群发言数不能小于 0。")
    if group_messages > 0 and not group_id:
        raise ValueError("设置群发言数条件时，第一行必须填写群 ID。")
    channel_id = clean_condition_value(lines[2])
    return {
        "required_group_id": group_id,
        "required_group_messages": group_messages,
        "required_channel_id": channel_id,
    }


def condition_lines(batch):
    lines = []
    if batch["required_group_id"] and (batch["required_group_messages"] or 0) > 0:
        lines.append(
            "群发言数：在 {0} 中总发言数必须大于 {1}".format(
                batch["required_group_id"],
                batch["required_group_messages"],
            )
        )
    if batch["required_channel_id"]:
        lines.append("频道关注：必须关注 {0}".format(batch["required_channel_id"]))
    if not lines:
        return ["领取条件：无额外条件"]
    return ["领取条件："] + lines


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
                "领取失败：你在指定群聊中的累计发言数是 {0}，需要大于 {1} 才能领取。".format(
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
                "领取失败：请先关注指定频道，然后重新打开领取链接。",
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
        "管理员面板\n\n"
        "核心流程：创建批次 → 导入兑换码 → 生成领取链接 → 分享链接 → 用户验证 → 自动发码 → 记录日志\n\n"
        "请从下方按钮开始：",
        admin_keyboard(),
    )


def handle_flow(chat_id, user_id):
    if not is_admin(user_id):
        send_message(chat_id, "你不是管理员。")
        return
    send_message(
        chat_id,
        "核心流程\n\n"
        "1. 管理员点击「创建兑换码批次」\n"
        "2. 填写批次名称\n"
        "3. 选择兑换码类型：使用次数 / 领完为止\n"
        "4. 设置领取条件：群发言数 / 频道关注 / 无条件\n"
        "5. 按提示导入兑换码\n"
        "6. Bot 自动生成唯一领取链接\n"
        "7. 管理员复制链接，发到频道、群聊或私聊\n"
        "8. 用户只能通过这个链接进入领取\n"
        "9. 用户回答个位数加减法验证\n"
        "10. 系统校验领取条件并自动发放兑换码\n"
        "11. 系统记录领取日志，管理员可查看\n\n"
        "普通用户没有领取菜单，也不能靠命令主动领取。",
        admin_keyboard(),
    )


def begin_new_batch(chat_id, user_id):
    ADMIN_STATES[user_id] = {"action": "newbatch", "step": "name", "data": {}}
    send_message(
        chat_id,
        "创建批次：第 1 步 / 共 5 步\n\n"
        "请先输入批次名称。\n\n"
        "例如：7 月新用户福利、频道活动兑换码、测试批次",
        admin_keyboard(),
    )


def finish_usage_batch(chat_id, user_id, state, text):
    parts = [line.strip() for line in text.splitlines() if line.strip()]
    if len(parts) < 2:
        send_message(chat_id, "请按两行发送：\n第一行：可使用次数\n第二行：兑换码")
        return
    try:
        usage_limit = int(parts[0])
    except ValueError:
        send_message(chat_id, "可使用次数必须是数字。")
        return
    if usage_limit <= 0:
        send_message(chat_id, "可使用次数必须大于 0。")
        return
    code = parts[1]
    token = "batch_" + uuid.uuid4().hex[:16]
    current = now_text()
    with db_connect() as conn:
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
                state["data"]["name"],
                code,
                usage_limit,
                state["data"].get("required_group_id"),
                state["data"].get("required_group_messages", 0),
                state["data"].get("required_channel_id"),
                user_id,
                current,
            ),
        )
    ADMIN_STATES.pop(user_id, None)
    send_message(
        chat_id,
        "批次创建完成，专属领取链接已生成。\n\n"
        "批次名称：{0}\n"
        "类型：使用次数\n"
        "可用次数：{1}\n"
        "{2}\n"
        "领取链接：\n{3}\n\n"
        "下一步：复制这条链接发给用户。用户只能通过这条链接领取，领取前需要回答一道个位数加减法验证题。".format(
            html.escape(state["data"]["name"]),
            usage_limit,
            "\n".join(condition_lines(state["data"])),
            create_batch_link(token),
        ),
        admin_keyboard(),
    )


def finish_unique_batch(chat_id, user_id, state, text):
    raw_codes = [line.strip() for line in text.splitlines() if line.strip()]
    if not raw_codes:
        send_message(chat_id, "请发送兑换码，每行一个。")
        return
    seen = set()
    codes = []
    duplicated = 0
    for code in raw_codes:
        if code in seen:
            duplicated += 1
            continue
        seen.add(code)
        codes.append(code)
    token = "batch_" + uuid.uuid4().hex[:16]
    current = now_text()
    with db_connect() as conn:
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
                state["data"]["name"],
                state["data"].get("required_group_id"),
                state["data"].get("required_group_messages", 0),
                state["data"].get("required_channel_id"),
                user_id,
                current,
            ),
        )
        batch_id = cursor.lastrowid
        for code in codes:
            conn.execute(
                "INSERT INTO batch_codes (batch_id, code, created_at) VALUES (?, ?, ?)",
                (batch_id, code, current),
            )
    ADMIN_STATES.pop(user_id, None)
    send_message(
        chat_id,
        "批次创建完成，专属领取链接已生成。\n\n"
        "批次名称：{0}\n"
        "类型：领完为止\n"
        "导入数量：{1}\n"
        "重复跳过：{2}\n"
        "{3}\n"
        "领取链接：\n{4}\n\n"
        "下一步：复制这条链接发给用户。每个兑换码只会发放一次，领完后自动停止。".format(
            html.escape(state["data"]["name"]),
            len(codes),
            duplicated,
            "\n".join(condition_lines(state["data"])),
            create_batch_link(token),
        ),
        admin_keyboard(),
    )


def handle_new_batch_state(chat_id, user_id, text):
    state = ADMIN_STATES.get(user_id)
    if not state:
        return False
    if state["action"] != "newbatch":
        return False

    if text == "取消操作":
        ADMIN_STATES.pop(user_id, None)
        send_message(chat_id, "已取消创建批次。", admin_keyboard())
        return True

    step = state["step"]
    if step == "name":
        if not text:
            send_message(chat_id, "批次名称不能为空。")
            return True
        state["data"]["name"] = text
        state["step"] = "type"
        send_message(
            chat_id,
            "创建批次：第 2 步 / 共 5 步\n\n"
            "请选择兑换码类型：\n\n"
            "使用次数：只有一个兑换码，可被多个用户领取，达到次数上限后停止。\n"
            "领完为止：导入多个兑换码，每个兑换码只发给一个用户。",
            batch_type_keyboard(),
        )
        return True

    if step == "type":
        value = text.strip().lower()
        if text == "使用次数":
            value = "usage"
        elif text == "领完为止":
            value = "unique"
        if value not in ("usage", "unique"):
            send_message(chat_id, "请点击「使用次数」或「领完为止」，也可以发送 usage 或 unique。")
            return True
        state["data"]["batch_type"] = value
        state["step"] = "conditions"
        send_message(
            chat_id,
            "创建批次：第 3 步 / 共 5 步\n\n"
            "请设置领取条件，按 3 行发送：\n"
            "第一行：群 ID。没有群发言条件就填 0\n"
            "第二行：群发言数阈值。用户发言数必须大于这个数；没有就填 0\n"
            "第三行：频道用户名或频道 ID。没有频道关注条件就填 0\n\n"
            "示例 1，无额外条件：\n0\n0\n0\n\n"
            "示例 2，群发言数大于 5 且关注频道：\n-1001234567890\n5\n@your_channel\n\n"
            "提示：把 Bot 拉进群后，在群里发送 /chatid 可以获取群 ID。",
        )
        return True

    if step == "conditions":
        try:
            conditions = parse_batch_conditions(text)
        except ValueError as exc:
            send_message(chat_id, str(exc))
            return True
        state["data"].update(conditions)
        state["step"] = "codes"
        if state["data"]["batch_type"] == "usage":
            send_message(
                chat_id,
                "创建批次：第 4 步 / 共 5 步\n\n"
                "请按两行发送：\n"
                "第一行：可使用次数\n"
                "第二行：兑换码\n\n"
                "示例：\n100\nABC-DEF-001",
            )
        else:
            send_message(
                chat_id,
                "创建批次：第 4 步 / 共 5 步\n\n"
                "请批量发送兑换码，每行一个：\n\n"
                "CODE001\nCODE002\nCODE003",
            )
        return True

    if step == "codes":
        if state["data"]["batch_type"] == "usage":
            finish_usage_batch(chat_id, user_id, state, text)
        else:
            finish_unique_batch(chat_id, user_id, state, text)
        return True

    return False


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
        send_message(chat_id, "还没有批次。")
        return
    lines = ["最近批次："]
    for row in rows:
        if row["batch_type"] == "usage":
            stock = "{0}/{1}".format(row["usage_count"], row["usage_limit"])
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
            "#{0} | {1} | {2} | {3} | {4}".format(
                row["id"],
                html.escape(row["name"]),
                row["batch_type"],
                row["status"],
                stock,
            )
        )
        lines.append(create_batch_link(row["token"]))
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
        "批次详情：",
        "编号：#{0}".format(batch["id"]),
        "名称：{0}".format(html.escape(batch["name"])),
        "类型：{0}".format(batch["batch_type"]),
        "状态：{0}".format(batch["status"]),
        "库存：{0}".format(stock),
        "\n".join(condition_lines(batch)),
        "成功领取：{0}".format(success_count),
        "失败记录：{0}".format(failed_count),
        "创建时间：{0}".format(batch["created_at"]),
        "领取链接：",
        create_batch_link(batch["token"]),
        "",
        "分享方式：把上面的领取链接发到频道、群聊或私聊。用户点链接进入后，Bot 会先验证，再自动发码并记录日志。",
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
        send_message(chat_id, "暂无领取记录。")
        return
    lines = ["最近领取记录："]
    for row in rows:
        lines.append(
            "{0} | {1} | @{2} | {3} | {4}".format(
                row["created_at"],
                row["telegram_id"],
                row["username"] or "-",
                row["status"],
                html.escape(row["code"] or row["reason"] or "-"),
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
            send_message(chat_id, "领取链接不存在或已失效。")
            return
        if batch["status"] != "active":
            log_claim(conn, user, batch, None, "failed", 0, "batch_disabled")
            send_message(chat_id, "当前活动未开始、已结束或已失效。")
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
                "你已经领取过这个批次，兑换码是：\n\n<code>{0}</code>".format(
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
        "你正在领取：{0}\n\n"
        "{1}\n\n"
        "领取流程：完成验证 → 校验资格 → 自动发放兑换码。\n\n"
        "请完成下面的验证题：\n"
        "{2}".format(
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
            send_message(chat_id, "领取链接不存在或已失效。")
            return
        if batch["status"] != "active":
            log_claim(conn, user, batch, None, "failed", 1, "batch_disabled")
            conn.execute("COMMIT")
            send_message(chat_id, "当前活动未开始、已结束或已失效。")
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
                "你已经领取过这个批次，兑换码是：\n\n<code>{0}</code>".format(
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
                send_message(chat_id, "当前兑换码已达到使用次数上限。")
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
                "领取成功，你的兑换码是：\n\n<code>{0}</code>\n\n领取记录已保存。".format(html.escape(code)),
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
            send_message(chat_id, "当前兑换码已领完。")
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
            "领取成功，你的兑换码是：\n\n<code>{0}</code>\n\n领取记录已保存。".format(
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
            handle_admin(chat_id, user["id"])
        else:
            send_message(chat_id, "请通过管理员分享的专属领取链接进入。")
        return
    token = parts[1].strip()
    begin_claim(chat_id, user, token)


def handle_chatid(chat_id, message):
    chat = message.get("chat") or {}
    title = chat.get("title") or chat.get("username") or "当前会话"
    send_message(
        chat_id,
        "{0}\nChat ID：<code>{1}</code>\n\n"
        "如果要设置群发言数条件，请把这个 Chat ID 填到批次领取条件的第一行。".format(
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

    if handle_new_batch_state(chat_id, user_id, text):
        return

    if text == "取消操作":
        ADMIN_STATES.pop(user_id, None)
        send_message(chat_id, "已取消当前操作。", admin_keyboard())
    elif text.startswith("/start"):
        handle_start(chat_id, user, text)
    elif text.startswith("/admin"):
        handle_admin(chat_id, user_id)
    elif text.startswith("/whoami"):
        handle_whoami(chat_id, user)
    elif text.startswith("/chatid"):
        handle_chatid(chat_id, message)
    elif text.startswith("/newbatch") or text == "创建批次" or text == "创建兑换码批次":
        if is_admin(user_id):
            begin_new_batch(chat_id, user_id)
        else:
            send_message(chat_id, "你不是管理员。")
    elif text.startswith("/batches") or text == "批次列表":
        list_batches(chat_id, user_id)
    elif text.startswith("/batch"):
        show_batch_detail(chat_id, user_id, text)
    elif text.startswith("/records") or text == "最近领取记录":
        show_records(chat_id, user_id)
    elif text.startswith("/flow") or text == "核心流程":
        handle_flow(chat_id, user_id)
    else:
        if is_admin(user_id):
            send_message(chat_id, "请选择管理员操作。", admin_keyboard())
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
