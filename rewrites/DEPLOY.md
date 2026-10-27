# GUJI 精简重写 · 部署指南（DEPLOY）

> 目标：让其他人**照着就能把 `rewrites/global`（线上版 `guji-global-rewrite-v1`）部署起来**。
> 配套思考链见 `DEVLOG.md`，版本时间线见 `README.md`。
> 本指南以"从零新建一套"为主，也标注了"复用现有资源"的路径。

---

## 0. 先决条件

- 一个 Cloudflare 账号（免费套餐即可，但注意 §6 的硬约束）。
- 本地装好 [Node.js](https://nodejs.org/)（≥ 18）和 `wrangler`：
  ```bash
  npm install -g wrangler
  wrangler login        # 浏览器授权，会写入默认 account，toml 里就不必写 account_id
  ```
- 以下**密钥/服务**（都通过 `wrangler secret put` 注入，**不要写进任何文件**）：
  - `ZHIPU_KEY`：智谱 GLM-4-plus（中文精选主力模型）。不设置会降级到 CF Workers AI。
  - `TG_BOT_TOKEN` / `TG_CHAT_ID` / `TG_CHANNEL`：Telegram 推送（私聊 + 频道）。不设就不推送，其余照跑。
  - `SEC_USER_AGENT`：**必须设**，值形如 `GUJI AI Digest (you@example.com)`——SEC EDGAR 强制要求 UA 含联系邮箱，否则 12 个 SEC 源全挂（见 `DEVLOG.md` §8）。
  - `DIAG_KEY`：`/api/diag` 诊断端点鉴权（可选；不设则端点免鉴权开放）。

---

## 1. 取代码

线上版在 `rewrites/global/`：

```
rewrites/global/
├── index.js          # 全部线上代码（guji-global-rewrite-v1）
├── wrangler.toml     # Worker + KV + Queues + 单 cron 配置
├── README.md         # 该版说明
├── smoke-test.mjs    # 本地冒烟（mock 外部依赖，不需密钥/网络）
└── package.json
```

> 仓库根的 `src/index.js` 是 v4（另一套实现），不要混用。本目录三版共用同一 KV/队列名，但 KV 键前缀不同（v4 用 `guji:v4:*`，本目录用 `guji:v5:*`）。

---

## 2. 配置 wrangler.toml

提交里的 `wrangler.toml` **不含 `account_id`**（不提交账号信息）。两种处理方式：

- **推荐**：先 `wrangler login`，wrangler 会自动用默认账号，`toml` 里无需 `account_id`。
- 或：在 `toml` 顶部加 `account_id = "你的CF账号ID"`（从 CF 控制台 → 右侧账号 ID 复制）。

`wrangler.toml` 关键字段（已写好，按需改）：

```toml
name = "guji"                       # worker 名，可改
main = "index.js"
compatibility_date = "2026-10-01"

[[kv_namespaces]]
binding = "GUJI_KV"
id = "ca49d41ac8fa4987a0f818cee47237bf"   # ← 换成你自己的 KV namespace id（见 §3）

[ai]
binding = "AI"                       # Workers AI（智谱缺失时的 fallback）

# 队列：复用现有名 guji-fetch/digest/send/retry/dlq（见 §4）
[[queues.producers]] binding = "GUJI_FETCH_Q"  queue = "guji-fetch"
[[queues.producers]] binding = "GUJI_DIGEST_Q" queue = "guji-digest"
[[queues.producers]] binding = "GUJI_SEND_Q"   queue = "guji-send"
[[queues.producers]] binding = "GUJI_RETRY_Q"  queue = "guji-retry"
[[queues.consumers]] queue = "guji-fetch"  max_concurrency = 4 max_retries = 3 dead_letter_queue = "guji-dlq"
[[queues.consumers]] queue = "guji-digest" max_retries = 10 dead_letter_queue = "guji-dlq"
[[queues.consumers]] queue = "guji-send"   max_retries = 3  dead_letter_queue = "guji-dlq"
[[queues.consumers]] queue = "guji-retry"  max_retries = 3  dead_letter_queue = "guji-dlq"
[[queues.consumers]] queue = "guji-dlq"    max_batch_size = 10

[triggers]
crons = ["* * * * *"]                # 单 cron，由 worker 内 scheduledStage 自驱每日 6 时段（北京 05:40/05:50/06:00、21:40/21:50/22:00）
```

---

## 3. 创建 KV 命名空间

```bash
wrangler kv namespace create GUJI_KV
# 输出里拿到 id，回填到 toml 的 [[kv_namespaces]].id
# 注意：本目录代码用的 KV 键前缀是 guji:v5:*，与 v4 的 guji:v4:* 不冲突，可共存也可独立新建
```

---

## 4. 创建队列

队列名必须与 `wrangler.toml` 一致：`guji-fetch` / `guji-digest` / `guji-send` / `guji-retry` / `guji-dlq`。

```bash
for q in guji-fetch guji-digest guji-send guji-retry guji-dlq; do
  wrangler queues create "$q"
done
```

> **坑（见 DEVLOG §4）**：`wrangler.toml` 里 producer 的 `delivery_delay` 只对**新建**绑定生效，对已存在的绑定被静默忽略。本目录版本**不依赖** digest 的延迟投递（改成"抓取与精选同时入队 + 消费者内轮采等待"），所以不配 delay 也能跑。若你确实想要 digest 延迟 90s，用 API 显式设：
> ```bash
> curl -X PUT "https://api.cloudflare.com/client/v4/accounts/$CF_ACCOUNT/queues/$QUEUE_ID" \
>   -H "Authorization: Bearer $CF_TOKEN" \
>   -d '{"queue_name":"guji-digest","settings":{"delivery_delay":90,"message_retention_period":86400}}'
> ```

---

## 5. 注入密钥

```bash
cd rewrites/global
wrangler secret put ZHIPU_KEY
wrangler secret put TG_BOT_TOKEN
wrangler secret put TG_CHAT_ID
wrangler secret put TG_CHANNEL
wrangler secret put SEC_USER_AGENT     # 必须：SEC 源依赖它
wrangler secret put DIAG_KEY          # 可选
```

---

## 6. 部署

```bash
cd rewrites/global
wrangler deploy
```

> 首次部署若报 `10013`（队列绑定瞬时未对齐），**重部署一次**即可（见 DEVLOG §6）。
> 部署要求 worker 里为每个队列都声明了 consumer 处理函数，本目录代码已包含。

---

## 7. 验证

部署后等下一个班次（或手动触发）。健康检查与诊断：

```bash
# 健康检查（返回 feedCount / shardCount / 各配置是否就绪）
curl https://<你的worker>.<子域>.workers.dev/api/health
# 或绑了自定义域：
curl https://guji.dpdns.org/api/health

# 诊断（设了 DIAG_KEY 则带 ?key=）
curl "https://<你的worker>.<子域>.workers.dev/api/diag?health=1&key=$DIAG_KEY"
```

也可看 KV 里的 `guji:v5:latest`（`window` / `status` / `digest` 条数 / `failures`）确认日报生成与推送结果。

---

## 8. 常见坑速查（別重蹈覆辙）

| 现象 | 根因 | 处理 |
|---|---|---|
| SEC 12 源全挂，`failures` 高 | 没设 `SEC_USER_AGENT` | `wrangler secret put SEC_USER_AGENT`（带邮箱） |
| 智谱不生效、总结变弱 | 密钥名写成 `ZHIPU_API_KEY`，实际是 `ZHIPU_KEY` | 用 `env.ZHIPU_KEY` 对齐账号 secret 名 |
| 抓取/精选翻倍 | 多条 cron 同分钟重复入队（或 hourly 误触发） | 单 cron + worker 内 `scheduledStage` 门闩（本目录已修） |
| 重大提醒只发一处 | 用了 `TG_CHAT_ID \|\| TG_CHANNEL` 单发 | 改 `[...].filter(Boolean)` 循环双发（本目录已修） |
| digest 延迟不生效 | toml `delivery_delay` 对已有绑定无效 | 用 API `PUT /queues/{id}`；或像本目录不依赖它 |
| 部署报 10013 | 队列绑定瞬时未对齐 | 重部署一次 |
| 部署被拒 | worker 缺队列 consumer 入口 | 给每个队列补 consumer 处理函数 |

---

## 9. 与 v4 共存的注意事项

- 本目录三版与根 `src/` 的 v4 **共用同一组队列名**，若同一个账号同时部署 v4 和本目录，队列会被两套代码争用——**二选一部署**，不要同时跑。
- KV 键前缀不同（`v4:*` vs `v5:*`），数据不互相覆盖，但 `guji-dlq` / `guji-retry` 是同一个物理队列，两套都往里写。
- 想让 `global` 成为仓库正式 `src/index.js`，需同步更新根 `README`/`DEVLOG`/`CHANGELOG` 对源数（35）与 cron（单 cron）的描述。
