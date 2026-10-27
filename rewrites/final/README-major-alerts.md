# GUJI · 日报 + 重大事件提醒（Cloudflare Workers）

## 功能
- 保留北京时间 06:00、22:00 两次 AI 情报日报。
- 增加重大事件监测：每分钟只轮询一个来源，6 个来源约每 6 分钟复查一次。
- 只有新文章先通过规则筛选，再交给 AI 判断是否属于重大事件；普通更新不推送。
- 首次扫描只建立 RSS 基线，不会把历史文章当作突发事件。
- Telegram 推送失败不会推进该来源游标，下一轮可重新尝试。已成功发送的事件会记录去重账本。
- 不新增队列；复用已有 KV、Workers AI/智谱 Key 和 Telegram 配置。

## 部署前注意
1. 先备份线上 Worker 代码和 `wrangler.toml`。不要删除现有 Queue 或 KV。
2. 此包中的 `wrangler.toml` 基于当前 GUJI 配置，并增加第五个 Cron Trigger `* * * * *`。Cloudflare 每个账户的 Cron Trigger 数量有上限；如果账户已被其他 Worker 占满，部署前需先确认额度。
3. 保留现有 KV namespace ID 和 Queue 名称。不要新建一套 KV，否则日报和去重状态会断开。
4. 确认已设置 `TG_BOT_TOKEN`、`TG_CHAT_ID` 或 `TG_CHANNEL`，以及 `AI` 绑定或 `ZHIPU_KEY`。如果使用 SEC 数据，`SEC_USER_AGENT` 应使用真实联系邮箱。
5. 第一次轮询每个来源时只建立基线；约 6 分钟后，开始对基线之后出现的新内容进行提醒筛选。

## 部署
在该目录执行：

```bash
npm install
npx wrangler deploy
```

先检查 `wrangler.toml` 的 `name`、KV namespace ID、Queue 绑定与线上配置一致，再部署。

## 验证
- 打开 Worker 根路径，查看日报页面是否正常。
- 访问 `/api/health`，确认返回 `majorMonitor.enabled: true` 及 6 个监测来源。
- Cloudflare Dashboard → Workers & Pages → GUJI → Logs，搜索 `[major]`，可看到来源基线、候选数和提醒结果。
- Cron 监测目标是几分钟级，不是秒级实时；RSS 发布延迟、源站可用性及 Cloudflare 配额都会影响发现时间。

## 监测来源
OpenAI、Google DeepMind、NVIDIA Newsroom、Techmeme、TechCrunch AI、Microsoft AI Blog。日报仍保留代码中完整的源清单。

## 重要限制
- 重大事件判断是规则初筛 + 模型审核，不可能保证零漏报或零误报。
- 当前提醒发送到 `TG_CHAT_ID`（若设置），否则发送到 `TG_CHANNEL`；日报的既有推送逻辑不变。
- 本次已完成 JavaScript 语法检查；未连接你的 Cloudflare 账户，也未进行线上部署验证。
