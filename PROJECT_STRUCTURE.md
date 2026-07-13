# 项目结构整理计划

当前线上入口仍是 `bot.py`，为了保证 Bot 稳定运行，本次先修交互问题，不做大规模移动代码。

后续代码会按下面结构逐步拆分：

```text
telegram-bot/
  bot.py                  # 当前线上入口，后续逐步瘦身
  app/
    config/               # 环境变量、常量、运行参数
    telegram/             # Telegram API、消息发送、菜单命令
    storage/              # SQLite 连接、表结构、数据访问
    services/             # 发码、条件校验、库存扣减、群统计
    handlers/             # 管理员、默认条件、批次创建、用户领取
    ui/                   # 文案、InlineKeyboard、ReplyKeyboard
  data/
  logs/
```

## 拆分原则

- 先拆纯函数和 UI 键盘，不影响数据库。
- 再拆 Telegram API 调用层。
- 最后拆发码事务和领取条件校验。
- 每拆一层都先部署编译验证，避免线上 Bot 因重构中断。

## 本次已调整的交互边界

- ReplyKeyboard 只保留主功能入口。
- 功能内部的返回、退出、完成、绑定都使用 InlineKeyboard。
- 默认条件中，群聊绑定和发言数量拆成独立动作。
- 频道订阅改为通过转发频道消息自动绑定。
