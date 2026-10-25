# GUJI · 严格一手来源的 AI 产业日报

> **关于本文档的作者**：本仓库的全部代码、文档与开发日志（DEVLOG.md）均由
> **AI 编程助手（CodeBuddy）** 与项目所有者（用户，GitHub: `sjshaj`）协作完成——
> 用户提需求、做关键决策，AI 负责实现、排查、记录。DEVLOG.md 以 AI 第一人称
> 实时记录了四个版本「怎么写、错在哪、为何错、怎么改」的完整过程，
> 供后续接手的 AI 或人类开发者参考。

> 一个跑在 Cloudflare Workers 免费套餐上的自动化日报系统：每天两班（北京时间 06:00 / 22:00），
> 从 **39 个严格一手来源**抓取 AI 产业信息（海外大厂官方发布、SEC EDGAR filing、深度访谈播客、
> arXiv），自动去重合并，经 AI 精选生成中文总结，逐条推送到 Telegram 频道，并渲染成网页。

**线上地址**：https://guji.dpdns.org

**当前版本**：v4.0.2.1（Cloudflare Queues 队列化架构，2026-10-25 上线；
精选环节五项改进 10-26 凌晨部署；运维加固批次 10-26 凌晨部署，版本 c739fd45）

---

## 核心特性

- **严格一手来源**：模型实验室官方博客（OpenAI / Google DeepMind / Meta / Anthropic）、
  硬件与数据中心官方新闻稿（NVIDIA / Arm / AMD…）+ SEC EDGAR filing（覆盖 IR 站点被
  Cloudflare 防护挡住的硬件公司）、14 个深度访谈播客（Dwarkesh / Acquired / Latent Space /
  No Priors…）、arXiv。**二手聚合器一概不要**
- **多源合并去重**：同一条新闻被多个一手源报道时自动合并（页面上显示
  「DeepMind · Google AI · Meta」这样的合并署名）
- **先精选、后总结**：先由 AI 从全天条目中精选出最重要的，**只对精选做中文总结**，
  不做全文翻译（用户明确要求的流程）
- **队列化架构**：cron 只负责入队，抓取/精选/推送各自独立 invocation、独立 50 子请求预算，
  彻底消除单次调用超限问题；队列自带退避重试 + DLQ 兜底
- **DLQ 告警与每日巡检**：死信队列一有消息，班次结束时私聊推送运维告警（去重不刷屏）；
  GitHub Actions 每天早班后自动跑全套测试 + 线上 diag 检查，失败自动开 issue
- **全链路可观测**：消费者每个阶段写 KV 诊断记录，失败路径绝不静默；
  `/api/diag?health=1` 聚合近 3 天各源成功率

## 架构（v4）

```
                    ┌────────────────────────────────────────────┐
                    │  cron（免费档 4 个表达式覆盖 6 个触发点）      │
                    │  21:40/21:50/22:00 UTC = 北京 05:40/05:50/06:00
                    │  13:40/13:50/14:00 UTC = 北京 21:40/21:50/22:00
                    └───────┬──────────────┬──────────────┬───────┘
                      入队4条        入队1条          入队3条
                            ▼              ▼              ▼
                   [guji-fetch]    [guji-digest]    [guji-send]
                    消费者×4并行      90秒延迟         消费者串行(保序)
                    (分片隔离)        (等抓取就位)      (一条精选一条消息)
                        │                │              │
                        ▼                ▼              ▼
                    分片 KV          latest KV      Telegram
                   (window+序号      (中文精选)      (私聊+频道)
                    隔离,无共享写)        │
                        └──────┬─────────┘
                               ▼
                          [guji-dlq] ──▶ 记日志消费者（重试耗尽不静默丢失）
```

每个消费者都是独立 invocation，享有独立的 **50 子请求**预算（Cloudflare 免费套餐限制）：

| 阶段 | 子请求预算 | 说明 |
|---|---|---|
| 抓取分片 | ~13/50 | 10 源 fetch + seen KV + 诊断埋点 + 分片落库 |
| 精选 | ~19/50 | 读 4 分片 + enrich + AI 总结 + latest 落库 |
| 推送 | ≤27/50 | 单片 ≤9 条消息 × 至多 3 次尝试 |

## 快速开始

```bash
# 1. 克隆
git clone https://github.com/sjshaj/guji.git && cd guji

# 2. 安装依赖（wrangler）
npm install

# 3. 配置密钥（三选，按需）
npx wrangler secret put ZHIPU_API_KEY    # 智谱 GLM-4-plus（中文精选主力模型）
npx wrangler secret put TG_BOT_TOKEN     # Telegram bot token（推送）
npx wrangler secret put TG_CHAT_ID       # Telegram 私聊 chat id（推送目标）

# 4. 本地冒烟 + 端到端测试（mock 全部外部依赖，不需要密钥和网络）
node src/smoke-v4.mjs   # 100 项
node src/e2e-v4.mjs     # 39 项

# 5. 本地 dev（可选，注意沙箱限制见下）
cd src && npx wrangler dev --port 8787

# 6. 部署
cd src && npx wrangler deploy
```

**本地开发须知（沙箱/本机环境的已知限制，别浪费时间试）**：

- `api.telegram.org` 在沙箱 DNS 被污染（解析到 Facebook IP）——
  **TG 推送只能线上验证**，本地测不通是正常的
- `guji.2698553157.workers.dev` 子域在沙箱不可达——生产域名只有
  `https://guji.dpdns.org`
- 沙箱里 Node fetch 有波动（偶发 000/502），同一测试跑三遍再下结论
- 测试文件不需要 secret/网络：mock 了 KV/队列/AI/fetch，
  改完逻辑先跑它们（139 项）再部署
- **怎么加测试**：照 `smoke-v4.mjs` 的 `ok(cond, name)` 模式追加即可；
  纯函数（parsers/tgTruncate/分片键/预算记账）进 smoke，跨模块链路进 e2e

**部署后必做**（详见 `docs/GUJI-代码重构交接.md` §8）：

```bash
# 队列级延迟投递必须走 API 设置（wrangler.toml 的 producer delay 对已存在绑定不生效）
curl -X PUT "https://api.cloudflare.com/client/v4/accounts/$CF_ACCOUNT/queues/$QUEUE_ID" \
  -H "Authorization: Bearer $CF_TOKEN" \
  -d '{"queue_name":"guji-digest","settings":{"delivery_delay":90,"message_retention_period":86400}}'
```

## 密钥管理

| Secret | 用途 | 不设置的后果 |
|---|---|---|
| `ZHIPU_API_KEY` | 智谱 GLM-4-plus（中文精选主力模型） | 退化 CF Workers AI llama-3.1-8b |
| `TG_BOT_TOKEN` / `TG_CHAT_ID` | Telegram 推送 | 不推送（其余照跑） |
| `DIAG_KEY` | `/api/diag` 诊断端点鉴权（v4.0.2.1） | **端点免鉴权开放**（灰度期默认行为） |

```bash
# 设置 / 轮换任一 secret（wrangler 会提示交互输入，避免进 shell history）
cd src && npx wrangler secret put DIAG_KEY

# GitHub Actions 每日巡检（D3）另需在 repo 级配置同名 secret：
#   repo → Settings → Secrets and variables → Actions → New repository secret
#   Name: DIAG_KEY，Value: 与上面完全一致
```

> 轮换方法：直接 `wrangler secret put` 同名变量即覆盖旧值，无需改代码、无需重新部署，
> 下次请求即按新值鉴权。`DIAG_KEY` 一旦设置，`/api/diag` 不带正确 `?key=` 即返回 403。

## 目录结构

```
guji/
├── README.md                    # 本文件：项目门面
├── DEVLOG.md                    # ★ 从零到 v4 的完整开发日志（每版写法+错误+反思，932 行）
├── ROADMAP.md                   # 接下来的更新计划（续写接口）
├── CHANGELOG.md                 # 版本变更事实清单（按时间倒序）
├── LICENSE                      # MIT
├── docs/
│   ├── GUJI-代码重构交接.md      # 代码地图/硬约束/部署运维说明书（2457 行）
│   ├── 从零重建指南.md           # 换账号/换域名自建一套：基建命令顺序 + 预期差异清单
│   ├── 首次cron验收手册.md       # v4 首班无人值守运行的验收清单（按时间序）
│   ├── 源病历.md                # 39 源健康档案：谁被挡过/谁偶发故障/怎么排查
│   ├── 故障排查Runbook.md       # 症状 → 查什么 → 常见根因（含紧急恢复）
│   └── archive/                 # 历史快照（源清单副本、未引用资源，只作存档）
└── src/
    ├── index.js                 # 全部线上代码（2386 行，按 §1-§15 区分区）
    ├── wrangler.toml            # Worker + KV + Queues + cron 配置（注释即文档）
    ├── package.json
    ├── smoke-v4.mjs             # 100 项冒烟测试（mock KV/队列/AI/fetch）
    ├── e2e-v4.mjs               # 39 项端到端测试
    └── __fixtures__/            # 真实 feed 快照（D2 契约测试 + dry-run 对照基线）
```

## 文档索引（出问题先查哪份）

| 你的问题 | 去这里 |
|---|---|
| 这代码为什么这么写？/ 接手项目 | `DEVLOG.md` → `docs/GUJI-代码重构交接.md` |
| 想**自己建一套**（换账号/换域名） | `docs/从零重建指南.md` |
| 某源的条目不出现 | `docs/源病历.md` |
| 页面/推送/AI/队列出了问题 | `docs/故障排查Runbook.md` |
| 今晚（或每次）cron 跑完怎么验收 | `docs/首次cron验收手册.md` |
| 接下来做什么 / 我改完怎么记录 | `ROADMAP.md` |
| 这个版本改了什么 | `CHANGELOG.md` |

## 给接手者的阅读顺序

1. **本文 §架构**——建立数据流全貌
2. **`DEVLOG.md`**——理解每个设计决策背后的约束与踩坑（**强烈推荐通读**，
   特别是 §3.0 的 50 子请求预算模型和 §5.3 的 deliveryDelay 坑）
3. **`docs/GUJI-代码重构交接.md` §1-§2**——数据流图与硬约束
4. **`src/index.js`**——事实源；十五个区从 config 到 worker 入口依次读
5. 改任何逻辑前：`node src/smoke-v4.mjs && node src/e2e-v4.mjs` 必须全绿

## 当前状态与已知限制

- **已验证**：39 源 → ~160 条/班、精选 ~12 条、双班推送正常、延迟投递 t0+104s、
  全链路测试 139 项通过
- **已知取舍/开放项**（详见 `ROADMAP.md`）：
  - digest 消费者同 window 内二次 build 会读到上轮分片（低频操作；B3 新鲜度防线
    已把误用窗口从「整个班次」压到「10 分钟内」）
  - `/api/diag` 已上鉴权灰度——**但 `DIAG_KEY` secret 尚未设置**，当前实际仍免鉴权；
    设置方法见上文「密钥管理」，一经设置即生效
  - 「分片消费者成批消失」事件根因未定位（沙箱 wrangler tail 不可用），已加埋点观测
  - 单文件 2386 行，物理拆模块在计划中（ROADMAP D1）

## 安全须知

- **仓库中没有任何密钥**。所有 token 通过 `wrangler secret put` 注入
- push 前请跑 ROADMAP A4 的密钥扫描
- 本项目早期对话中明文出现过 CF API token——**已按泄露处理**，
  轮换与 2FA 事项见 `ROADMAP.md` §A

## 技术栈

Cloudflare Workers（cron + KV + Workers AI + Queues）· fast-xml-parser v5 ·
SEC EDGAR API · Telegram Bot API · 智谱 GLM-4-plus · Node.js（仅测试）

## License

MIT（如无特殊说明）
