# GUJI 全局重写版（部署前候选包）

## 目标
让 GUJI 更接近一个低噪声的 AI/芯片/算力产业判断系统，而不是简单的 RSS 转发器：
- 每天北京时间 06:00、22:00 推送早报/晚报；
- 每分钟轮询一个重大事件源，6 个源约每 6 分钟复查一次；
- 只有高影响关键词或多个独立信号的候选才调用模型审核，不再把每一条官方博客更新都交给模型；
- 重大事件提醒必须向私聊与频道分别投递；某个目标成功、另一个目标失败时，只重试失败目标；
- 队列拆分保留，避免把 35 个源抓取、AI 精选和 Telegram 推送塞进一次 Worker invocation。

## 重要的调度修正
只保留一个 Cron：`* * * * *`。Worker 内部严格按北京时间小时和分钟匹配：
- 05:40、21:40：抓取；
- 05:50、21:50：AI 精选；
- 06:00、22:00：推送。

不能只判断分钟数，否则每小时的 `:40`、`:50`、`:00` 都可能误触发日报阶段。此版本已加入小时条件和班次阶段去重门闩。

## 资源与密钥
本包沿用当前部署复盘记录的配置，不新增资源：
- Worker：`guji`
- KV binding：`GUJI_KV`，沿用现有 namespace ID
- Queues：`guji-fetch`、`guji-digest`、`guji-send`、`guji-retry`、`guji-dlq`
- Secrets：`ZHIPU_KEY`、`TG_BOT_TOKEN`、`TG_CHAT_ID`、`TG_CHANNEL`（若有）、`SEC_USER_AGENT`、`DIAG_KEY`

不要把任何 secret 写进文件或提交到 Git。不要删除/重建现有 KV 或队列。`delivery_delay` 对既有队列是否生效，要在 Cloudflare 控制台核对，不能仅依赖 TOML。

## 本地检查
```bash
npm install
npm test
npm run check
```

## 部署前必须确认
1. 备份线上 `guji` 的代码和配置。
2. 确认 `wrangler.toml` 的 KV ID、Queue 绑定与生产环境一致；本包只配置一个 Cron。
3. 检查 Cloudflare 账户中 `guji` 的 Cron 确实只有 `* * * * *`，且没有额外每日 Cron。
4. 本地测试通过后，先在非生产环境或可回滚版本验证；不要未经验证直接覆盖生产。
5. 部署后检查 `/api/health`、`/api/diag?key=...`、队列日志、Telegram 私聊和频道；特别确认 06:00/22:00 才推日报，以及每条重大提醒两处都收到。

## 诚实的验证边界
本包可以进行静态语法检查和本地 smoke tests；这些测试不等于真实 Cloudflare 队列、KV、智谱接口和 Telegram 端到端验证。当前没有替用户部署，也没有声称线上验证通过。
