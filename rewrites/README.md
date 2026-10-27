# GUJI 精简重写 lineage（rewrites/）

> # ★ 线上版本：`rewrites/global` —— `guji-global-rewrite-v1`
> **这是当前 Cloudflare `guji` Worker 实际运行的代码。** 本目录另两版（`lean` / `final`）只是演进存档，**不要直接部署**。
> 想部署/看懂线上版，从 `global/` 开始；想看"怎么一步步走到这"，读下面的时间线与 `DEVLOG.md`。

---

## 这是什么

本目录存档 2026-10 期间对 GUJI 的**精简重写三版**（`lean` → `final` → `global`），加上：

- **`DEVLOG.md`** —— 完整思考链：写代码、部署、上线过程中发现的问题，怎么理解、怎么思考、最后怎么解决、为什么这么解决（按问题逐条展开）。
- **`DEPLOY.md`** —— 让其他人更容易部署的指南（前置条件、密钥、KV/队列、cron、命令、常见坑）。

它们与仓库根 `src/` 的 **v4 队列架构**（39 源、多 cron）是**两套独立实现**，但**共用同一个生产 KV 与队列**（仅 KV 键前缀从 `guji:v4:*` 改为 `guji:v5:*`），重写版部署后直接接管了 v4 的数据与推送链路。

---

## 时间线（演进）

| 日期（北京） | 版本 | 状态 | 关键事件 |
|---|---|---|---|
| 2026-10-26 凌晨 | `min` | 已弃用（无存档） | selfCall 状态机卡在 fetch 后写不进 `latest`，废弃 |
| 2026-10-26 夜 | `lean` | 存档 | 改用"队列消息驱动"；单 cron + 队列化（fetch/digest/send）；30 源；最小骨架 |
| 2026-10-27 上午 | `final` | 存档 | 在 lean 上修两处实锤 bug（cron 双倍触发、重大提醒单发→双发）；35 源 + 6 监测源 |
| **2026-10-27 下午** | **`global`** | **★ 线上** | 在 final 上收窄重大监测、加时段门闩消除每小时误触发、逐目标投递账本；即 `guji-global-rewrite-v1` |

---

## 与 v4 的区别（一句话）

- **共用**：KV 命名空间 `ca49d41ac8fa4987a0f818cee47237bf`、队列 `guji-fetch`/`guji-digest`/`guji-send`/`guji-retry`/`guji-dlq`。
- **不同**：重写版 KV 键用 `guji:v5:*`（reviewed / major-monitor / schedule / run），v4 用 `guji:v4:*`；重写版**单 cron + worker 内时段门闩**，v4 是多 cron + `delivery_delay=90`；重写版 35 源，v4 39 源。
- 根 `src/index.js`（v4）作为文档基线保留在 git 历史中。若把 `global` 提升为正式 `src/index.js`，需同步更新根 `README`/`DEVLOG`/`CHANGELOG` 对源数与 cron 的描述（见 `DEPLOY.md` §9）。

---

## 文档导航

| 你想做什么 | 去哪 |
|---|---|
| 部署线上版 | `DEPLOY.md`（前置 → 密钥 → KV/队列 → `wrangler deploy` → 验证） |
| 看完整思考链 / 踩坑复盘 | `DEVLOG.md`（§1–§13，按问题展开） |
| 看某版代码 | `lean/` `final/` `global/` 各自目录 |
| 看演进时间线 | 上表 / `DEVLOG.md` §0 |

---

## 密钥

所有 token（`ZHIPU_KEY` / `TG_BOT_TOKEN` / `TG_CHAT_ID` / `TG_CHANNEL` / `SEC_USER_AGENT` / `DIAG_KEY`）均通过 `wrangler secret put` 注入，**不在本目录任何文件中**。提交前已做密钥扫描，确认三版代码均无硬编码凭据（仓库 `.gitignore` 与根 `README` 安全须知也禁止提交真实 token）。
