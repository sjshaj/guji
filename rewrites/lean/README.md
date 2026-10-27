# GUJI AI 日报（精简版）

这个版本保留核心流程：**定时触发 → 分片抓取 → AI 中文精选 → Telegram 推送**。
它针对旧版的队列消息结构错误和异步阶段竞态做了修复，并移除了独立补推队列、向量聚类、逐篇正文增强和大量重复的诊断/打分逻辑。

## 这个版本保留什么

- 30 个源：AI 公司官方源、芯片/数据中心源、12 家公司的 SEC 重大事项申报、6 个核心播客、合并后的 arXiv RSS。每个源独立成一个队列任务，降低单次解析超出免费 CPU 限制时对其他来源的连带影响。
- 4 个队列用途：抓取、精选、推送、死信。旧的 `guji-retry` 不再被代码使用；上线稳定前不要删除它。
- 复用现有 KV namespace ID，并沿用旧版 `guji:v4:latest` 和 `guji:v4:seen` 键，尽量保留上次精选时间与 Anthropic 去重记录。
- `/api/health`、`/api/diag`、`/api/build`、`/api/push` 作为基础检查和手动测试入口。

## 部署前检查

1. 确认 `wrangler.toml` 里的 KV namespace ID 与当前 Worker 正在使用的 ID 一致。文件沿用上传项目中的 ID：`ca49d41ac8fa4987a0f818cee47237bf`；如果你真实环境不一样，先改成真实 ID。
2. 代码使用现有队列名称：`guji-fetch`、`guji-digest`、`guji-send`、`guji-dlq`。部署前在 Cloudflare Queues 页面确认它们存在且没有大量旧消息积压。不要为了部署先清空队列。
3. **必须配置 `SEC_USER_AGENT`，使用带你自己真实联系邮箱的字符串。** 例如：`GUJI-Digest/1.0 contact: you@example.com`。代码不会伪造联系邮箱；没有该配置时，SEC 源会记录明确的失败原因，但其他来源仍会继续处理。
4. 需要已有的 `TG_BOT_TOKEN`、`TG_CHAT_ID`；频道推送可选 `TG_CHANNEL`。`DIAG_KEY` 用于保护诊断和手动触发接口。`ZHIPU_KEY` 可选；不配置时使用 Cloudflare Workers AI。

## 安装与部署

在本目录执行：

```bash
npm ci
npx wrangler login
npx wrangler secret put SEC_USER_AGENT
# 如果这些密钥尚未存在，再分别设置：
# npx wrangler secret put TG_BOT_TOKEN
# npx wrangler secret put TG_CHAT_ID
# npx wrangler secret put TG_CHANNEL
# npx wrangler secret put DIAG_KEY
# npx wrangler secret put ZHIPU_KEY   # 可选
npx wrangler deploy
```

当 Wrangler 提示输入 `SEC_USER_AGENT` 时，填入你自己的联系邮箱，例如 `GUJI-Digest/1.0 contact: you@example.com`。不要把 Bot Token 或诊断密钥贴到公开聊天或公开仓库。

## 定时推送时间

Cron 在 UTC 时区配置：

- UTC 21:40 / 21:50 / 22:00 → 北京时间次日 05:40 抓取、05:50 精选、06:00 推送。
- UTC 13:40 / 13:50 / 14:00 → 北京时间当日 21:40 抓取、21:50 精选、22:00 推送。

## 部署后的验证顺序

1. 打开 `https://你的-worker域名/api/health`，确认 `feedCount` 和 `shardCount` 都为 30，且 `telegramConfigured`、`aiConfigured` 为 `true`。
2. 手动运行抓取与精选：`/api/build?key=你的DIAG_KEY`。等待返回的 `digestDelaySeconds` 所示时间，再看 `/api/diag?key=你的DIAG_KEY`。
3. 手动触发推送：`/api/push?key=你的DIAG_KEY&force=1`。只有你确认愿意发送当前最新结果时才使用 `force=1`。
4. 如果还是没有推送，先查看 Cloudflare Worker 的 Logs/Errors、四个队列的积压量、死信队列以及 `/api/diag`。`/api/health` 只证明 Worker 能响应 HTTP，不证明 Cron 和队列已正常执行。

## 运行测试

安装依赖后执行：

```bash
npm test
npm run check
```

测试覆盖队列批量消息包装、北京时间班次、RSS 解析、AI 精选结果映射，以及 Cron 抓取消息入队。它是冒烟测试，不替代真实 Cloudflare 部署验证。当前工作环境无法完成 npm 依赖下载，因此尚未运行通过完整测试；部署前请在可访问 npm registry 的环境运行 `npm ci && npm test && npm run check`。
