# Telegram Bot 项目结构与性能重构说明

## 当前项目结构

- `bot.py`：主程序，包含 Telegram API 调用、数据库初始化、管理员流程、用户领取流程、群发言统计、人机验证、轮询入口。
- `data/bot.sqlite3`：SQLite 数据库，保存批次、库存、领取日志、用户信息、群发言统计和默认配置。
- `logs/bot.log`：运行日志。
- `.env`：生产环境配置，包括 Bot Token、管理员 ID、数据库路径、并发参数等。
- `.env.example`：配置模板。
- `requirements.txt`：依赖文件；当前程序仍保持 Python 3.6 标准库兼容。

## 核心业务逻辑

管理员在私聊中创建兑换码批次，设置批次名称、批次类型、领取条件和兑换码库存。系统为每个批次生成唯一领取链接。

用户只能通过专属领取链接进入 Bot。进入后，Bot 会先发起人机验证。验证通过后，系统检查用户是否重复领取、批次是否有效、群发言数是否达标、频道/群订阅是否满足，然后按批次类型发码。

批次类型分为两种：

- 使用次数型：所有用户领取同一个兑换码，每成功领取一次 `usage_count + 1`，达到上限后停止发放。
- 领完为止型：库存表中每个兑换码只能被领取一次，领取时把 `batch_codes.status` 从 `available` 改为 `claimed`。

领取日志写入 `claim_logs`，用于查询成功、失败、失败原因和领取记录。

群发言统计通过群消息 update 静默记录到 `user_chat_stats`，Bot 在群内不回复消息，避免刷屏。

## 原性能瓶颈

旧版本 `poll_loop()` 是串行处理：

1. `getUpdates` 拉到多条 update。
2. 第一条 update 处理完成后，才处理第二条。
3. 如果某个用户触发了 Telegram API 慢请求、频道订阅检测、SQLite 写事务或发送消息超时，后面的所有用户都会等待。

旧版本发码函数还存在一个明显问题：在 `BEGIN IMMEDIATE` 写事务中执行领取条件检查，而频道订阅检查需要访问 Telegram `getChatMember`。如果 Telegram API 慢，SQLite 写锁会被长时间占用，导致其他用户无法及时领取。

## 本次性能重构

本次改为“轮询单线程 + 更新处理线程池”的并发模型：

- `getUpdates` 只负责拉取 update。
- 每条 update 提交给 `ThreadPoolExecutor` 并发处理。
- 同一个 Telegram 用户使用独立锁串行处理，避免人机验证状态、创建批次状态被并发点击打乱。
- 不同用户可以同时领取、验证、查询和操作。
- 使用 `MAX_INFLIGHT_UPDATES` 做总积压上限，避免瞬时流量把服务器打满。
- Telegram API 超时从固定 60 秒改成可配置 `API_TIMEOUT`，默认 20 秒。

发码流程拆成两段：

1. 预检查阶段：检查用户、批次、重复领取、群发言数、频道订阅。这里不持有库存写事务。
2. 短事务发码阶段：只在 `BEGIN IMMEDIATE` 中完成库存扣减、领取日志写入和重复领取二次确认。

这样可以保证库存扣减仍然安全，同时大幅缩短 SQLite 写锁占用时间。

## 可调性能参数

`.env` 可配置：

```env
API_TIMEOUT=20
POLL_TIMEOUT=20
UPDATE_WORKERS=16
MAX_INFLIGHT_UPDATES=64
```

建议小服务器先保持默认值。如果领取人数明显增加，可以把 `UPDATE_WORKERS` 调到 24 或 32，同时观察 CPU、内存和日志中的 Telegram API 超时情况。
