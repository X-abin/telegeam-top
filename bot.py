#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import print_function

import html
import json
import logging
import os
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

            CREATE INDEX IF NOT EXISTS idx_batches_token ON batches(token);
            CREATE INDEX IF NOT EXISTS idx_batch_codes_batch_status ON batch_codes(batch_id, status, id);
            CREATE INDEX IF NOT EXISTS idx_claim_logs_user ON claim_logs(telegram_id);
            CREATE INDEX IF NOT EXISTS idx_claim_logs_batch_user_status
                ON claim_logs(batch_id, telegram_id, status);
            """
        )


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


def admin_keyboard():
    return {
        "keyboard": [
            [{"text": "创建批次"}, {"text": "批次列表"}],
            [{"text": "最近领取记录"}, {"text": "取消操作"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def verify_keyboard(token):
    return {
        "inline_keyboard": [
            [{"text": "点击完成验证", "callback_data": "claim:{0}".format(token)}],
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
    ]
    admin_commands = user_commands + [
        {"command": "admin", "description": "管理员面板"},
        {"command": "newbatch", "description": "创建兑换码批次"},
        {"command": "batches", "description": "查看批次列表"},
        {"command": "batch", "description": "查看批次详情"},
        {"command": "records", "description": "查看最近领取记录"},
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
        "管理员面板：\n\n"
        "创建批次：创建兑换码批次并生成专属领取链接\n"
        "批次列表：查看最近批次\n"
        "最近领取记录：查看领取日志",
        admin_keyboard(),
    )


def begin_new_batch(chat_id, user_id):
    ADMIN_STATES[user_id] = {"action": "newbatch", "step": "name", "data": {}}
    send_message(chat_id, "请输入批次名称：", admin_keyboard())


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
                (token, name, batch_type, shared_code, usage_limit, usage_count, created_by, created_at)
            VALUES (?, ?, 'usage', ?, ?, 0, ?, ?)
            """,
            (token, state["data"]["name"], code, usage_limit, user_id, current),
        )
    ADMIN_STATES.pop(user_id, None)
    send_message(
        chat_id,
        "批次创建完成。\n\n"
        "批次名称：{0}\n"
        "类型：使用次数\n"
        "可用次数：{1}\n"
        "领取链接：\n{2}".format(
            html.escape(state["data"]["name"]),
            usage_limit,
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
            INSERT INTO batches (token, name, batch_type, created_by, created_at)
            VALUES (?, ?, 'unique', ?, ?)
            """,
            (token, state["data"]["name"], user_id, current),
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
        "批次创建完成。\n\n"
        "批次名称：{0}\n"
        "类型：领完为止\n"
        "导入数量：{1}\n"
        "重复跳过：{2}\n"
        "领取链接：\n{3}".format(
            html.escape(state["data"]["name"]),
            len(codes),
            duplicated,
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
        send_message(chat_id, "请选择批次类型，发送：\nusage = 使用次数\nunique = 领完为止")
        return True

    if step == "type":
        value = text.strip().lower()
        if value not in ("usage", "unique"):
            send_message(chat_id, "类型只能是 usage 或 unique。")
            return True
        state["data"]["batch_type"] = value
        state["step"] = "codes"
        if value == "usage":
            send_message(chat_id, "请按两行发送：\n第一行：可使用次数\n第二行：兑换码\n\n示例：\n100\nABC-DEF-001")
        else:
            send_message(chat_id, "请批量发送兑换码，每行一个：\n\nCODE001\nCODE002\nCODE003")
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
        "成功领取：{0}".format(success_count),
        "失败记录：{0}".format(failed_count),
        "创建时间：{0}".format(batch["created_at"]),
        "领取链接：",
        create_batch_link(batch["token"]),
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
    VERIFY_STATES[user["id"]] = {"token": verify_token, "batch_token": token}
    send_message(chat_id, "请先完成人机验证，点击下方按钮继续领取。", verify_keyboard(verify_token))


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
            send_message(chat_id, "领取成功，你的兑换码是：\n\n<code>{0}</code>".format(html.escape(code)))
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
        send_message(chat_id, "领取成功，你的兑换码是：\n\n<code>{0}</code>".format(html.escape(code_row["code"])))
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
    if not data.startswith("claim:"):
        answer_callback_query(callback_id)
        return
    token = data.split(":", 1)[1]
    state = VERIFY_STATES.get(user_id)
    if not state or state.get("token") != token:
        answer_callback_query(callback_id, "验证已失效，请重新打开领取链接。", show_alert=True)
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


def process_message(message):
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    text = (message.get("text") or "").strip()
    chat_id = chat.get("id")
    user_id = user.get("id")
    if not chat_id or not user_id:
        return

    if handle_new_batch_state(chat_id, user_id, text):
        return

    if text == "取消操作":
        ADMIN_STATES.pop(user_id, None)
        send_message(chat_id, "已取消当前操作。", admin_keyboard())
    elif text.startswith("/start"):
        handle_start(chat_id, user, text)
    elif text.startswith("/admin"):
        handle_admin(chat_id, user_id)
    elif text.startswith("/newbatch") or text == "创建批次":
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
