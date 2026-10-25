# GUJI AI 情报日报 · 代码交接文档（v4 队列化版）

> **这份文档的用途**：把 GUJI 项目全部在用代码 + 完整设计上下文打包给你（后续接手的 AI / 维护者）。
> 当前状态：**v4 队列化架构已上线运行**（2026-10-25 部署，Version ID `bfce87d9-ae8f-4285-9c26-14cc5639510d`）。
> 任何改动前必须读第 2 节硬约束；改完必须跑第 8 节的验证全家桶。
>
> **版本注记（2026-10-25 深夜，v4.0.1）**：本文按 v4.0（commit `0482501`，1699 行、
> smoke 17 项）撰写，文中行号区间与「17 项/56 项」均为该版快照。当前线上为
> **v4.0.1**（1825 行、smoke 31 项、e2e 39 项 = 70 项，新增 guji-retry 补推队列），
> 变更清单见 `CHANGELOG.md` v4.0.1 节、设计决策见 `DEVLOG.md` §4.5。
> 具体行号以 `src/index.js` 实际为准，§1-§15 分区结构不变。

---

## 0. 文档定位与 v3→v4 演变总览

**这个项目经历过两轮大改，都在 2026-10-25 完成**：

| 阶段 | 形态 | 解决的问题 | 状态 |
|---|---|---|---|
| v3 及以前 | 单文件 885 行，cron 一个调用跑完「抓取 39 源 + 精选」 | —— | 已归档（备份 `index.js.bak-20251025-v3refactor-pre-queues`） |
| v3 重构 | 同架构下重排为「关注点分区」单文件 + budget 计数器 + 纯函数单测 | 可维护性、预算显式化 | 已并入 v4 |
| **v4（当前）** | **队列化：cron 只入队，抓取/精选/推送各自独立消费者调用** | **单次调用 47/50 子请求走钢丝、余量为 0；瞬时失败无自动重试** | **线上运行中** |

原 v3 时代文档里的「重构任务指令」（预算显式化 / 关注点分离 / 可验证）已全部执行完毕，结论并入本文档；后续维护者的纪律简化为三条：

1. **新增任何子请求前，先过 §6 的 budget 账**（每个消费者调用独立 50 上限，但纪律不变）
2. **改完跑 `node smoke-v4.mjs && node e2e-v4.mjs`**（17 + 39 项，全绿才准部署）
3. **部署后按 §8 验证**（build → 观察 run 记录 → push → diag），无人值守链路以「第二天推送结果」为准

---

## 1. 项目是什么

**GUJI**（线上地址：https://guji.dpdns.org ）是一个**严格一手来源的 AI 产业日报**：

- 每天两班（北京时间 05:40 抓取 / 05:50 精选 / 06:00 推送早报；21:40 / 21:50 / 22:00 晚报）自动抓取 39 个一手源，生成中文精选摘要，推送到 Telegram
- 39 个源全部是一手来源：海外大厂官方发布（OpenAI/Google/Meta/Anthropic/NVIDIA…）、SEC EDGAR 官方 filing、深度访谈播客（Acquired/Dwarkesh/Lex Fridman…）、arXiv 论文。**不做二手聚合**
- 产品形态三件套：
  1. **网页**（renderPage）：按分类展示今日全部条目 + 顶部「今日精选」中文摘要区
  2. **Telegram 频道** `@guji_ai_daily`（公开可订阅）+ 创建者私聊：早晚各收一次分批推送
  3. **管理接口**：`/api/build`（手动抓取+精选入队）、`/api/push`（手动推送入队，幂等）、`/api/diag`（诊断）

技术形态：**单个 Cloudflare Worker**（`guji`）+ **4 条队列**，无服务器、无数据库，状态全在 KV。

### 核心处理流程（v4 队列化数据流）

```
cron :40（UTC 21:40/13:40，北京时间 05:40/21:40）
  scheduled(:40) ──入队 4 条──▶ [guji-fetch 消费者]  conc=4，每片 10 源
                                     │ 写分片 KV（guji:v4:shard:{window}:{n}，2 天 TTL）
                                     │ 哪怕全失败也落库（digest 以「键存在」判就绪）
cron :50                                ▼
  scheduled(:50) ──入队 1 条──▶ [guji-digest 消费者] conc=1，队列级 delay=90s
                                     │ ① 等 4 分片就绪（轮询 ≤45s，预算护栏 40）
                                     │ ② finalizeItems 跨分片合并后处理（纯函数）
                                     │ ③ 早晚班 cutoff（lastDigestAt，宁薄勿空）
                                     │ ④ enrich 4 条 + 模型链（智谱 glm-4-plus 主力）
                                     │ ⑤ 落库 guji:v4:latest（budget 快照先于 put！）
                                     ▼
cron :00（UTC 22:00/14:00）        KV latest
  scheduled(:00) ──入队 3 条──▶ [guji-send 消费者]  conc=1 串行保序
                                     │ 读 latest → 3h 新鲜度闸门 → 窗口匹配检查
                                     │ 顺序分片（报头→精选条目，私聊→频道）+ 片内 300ms 限速
                                     ▼
                                   Telegram 私聊 + 频道（slice 0 完成写「本班已推」幂等标记）

任一消费者抛错 → 消息不 ack → 队列自动退避重试（fetch 3 次 / digest 10 次 / send 3 次）
重试耗尽 → [guji-dlq 消费者] console.error 记日志（不静默丢失）
```

**cron 时间复用技巧**：`40,50 21 * * *` 一个表达式覆盖 :40/:50 两次触发，handler 按北京时间分钟数分流（40 抓取 / 50 精选 / 00 推送）——4 个表达式覆盖 6 个触发点，仍在免费档 5 个 cron 上限内。

---

## 2. 硬约束（红线，违反任何一条都会导致线上故障）

### 2.1 Cloudflare Workers 免费套餐限制（最重要）

| 限制 | 值 | 影响 |
|---|---|---|
| **单次调用子请求上限** | **50** | v4 起**每个消费者调用独立计数**：cron 入队 4、抓取分片 16、精选 19、推送片 29。峰值 29/50，余量 21 |
| 每账号 cron 触发器 | 5 个 | 现用 4 个表达式（时间复用覆盖 6 个触发点） |
| **Queues 免费档** | **10,000 操作/天、消息保留 24h** | 每日三班共 8 次发送 + 8 次消费，远低于上限；24h 保留意味着「昨晚没跑成的班」今天补不回来，只能重跑 |
| Workers AI 免费额度 | 10,000 neurons/天 | 智谱主力时 CF AI 主要耗在 enrich 的 toMarkdown（每条约 1 次） |
| `[limits]` 配置 | 付费套餐才能改 | 曾尝试调大子请求上限被拒（code 100328） |

**子请求计数法**（fetch、KV 读写、AI 调用、队列发送都计入；第 51 个不抛异常、只在响应里报错——静默失败，本项目历史上最难排查的事故模式）。v4 各阶段预算账（§6 BUDGET_PLAN，静态表与运行时计数一一对应）：

```
cron 入队班   ：4 次队列发送                                    = 4
抓取分片班    ：10 源 fetch + Anthropic seen 2 KV + 埋点 2 KV
               + 分片落库 1 KV                                  = 16（实测 12-14）
精选班        ：4 分片读 + latest 读 1 + 增强 4×2 + 模型链 4
               + latest 落库 1 + run 落库 1                     = 19（实测 14-15）
推送片班      ：latest 读 1 + ≤9 条消息 × 至多 3 次尝试
               + 统计落库 1（slice 0 另 +1）                    = 29（最坏）
```

**队列专项约束**：

- **队列发送保守计 1 子请求**（Cloudflare 未明确是否计入 50 上限，按计入估——入队班最多 4 条，两种口径下都安全）
- **producer 绑定的 `delivery_delay`（wrangler.toml）不会被 Cloudflare 应用到已存在的绑定**：camelCase `deliveryDelay` 会被 wrangler 静默忽略（部署日志有 `Unexpected fields` 警告；snake_case 虽被 schema 接受并上传元数据，但实测对已注册绑定不生效——详见 §4 与 §8 的 delay 设置命令）。**队列级默认延迟必须走 API PUT 设置**，消费者内轮询是主保险
- **max_concurrency=4 的并行安全性前提**：抓取分片键按 `window+序号` 隔离、无共享写，才敢让 4 片并行（§4）
- **抛错 = 消息不 ack = 自动退避重试**：这是 v4 对瞬时失败的结构性修复，不要改成「吞掉错误照常 ack」
- **24h 消息保留**：消费者挂起超过保留期的消息会永久丢失，挂起类故障必须靠埋点记录发现（§5）

### 2.2 其他环境约束

- **KV 最终一致性**：写入后边缘节点最长约 60 秒不可见。诊断数据要走 Worker 内部路由（`/api/diag`）或 REST API 读，**CLI `wrangler kv` 命令只读本地 `.wrangler/state`**，看不到远程真实数据（踩过坑）
- **KV REST API 支持 DELETE**（`DELETE .../namespaces/{ns}/values/{key}`），运维清理用；CLI 无此能力
- **Error 1102**：Worker CPU/内存超限。并行大页面 toMarkdown 转换曾触发，所以 enrich 必须串行、HTML 截断 150KB
- **Telegram**：单条消息上限 4096 **UTF-8 字节**（中文 1 字 = 3 字节，`String.slice` 按 UTF-16 计数会超限，必须用 `tgTruncate` 按字节截断）；同一聊天约 1 条/秒，连发会 429（已有 300ms 条间退让）
- **沙箱网络**：开发沙箱 DNS 污染导致 `api.telegram.org` 不可达、wrangler tail 的 WebSocket 被拦（ECONNREFUSED）——所有 TG 操作与线上观测必须经 Worker / KV 运行记录完成

### 2.3 必须保留的行为特征

v3 时代六条（全部仍有效）：

1. 早晚班互补：晚报只精选「上次成功精选以来」的新条目（`lastDigestAt` cutoff，注意 ts 毫秒/秒单位）
2. 精选 12 条硬上限；模型输出容错解析（JSON 数组/换行分隔对象/代码块包裹/前言废话都要能兜住）
3. 推送班有 3 小时数据新鲜度闸门：抓取班失败时不推昨天的旧内容
4. AI 全挂时推送退回英文标题清单（宁薄勿空），不静默失败
5. 每源限额（tomshw/sth 各 8 条）防单一媒体刷屏；arXiv 限 8 条
6. EDGAR 源必须带含联系邮箱的 UA（`UA_SEC`），否则 SEC 拒绝

v4 新增：

7. **抓取分片必然落库**（哪怕全失败）：digest 以「分片键存在」判断抓取完成，单个源失败不卡班
8. **digest 等分片轮询 ≤45s + 预算护栏 40**：超限交队列重试（每次重试全新预算），不硬等
9. **budget 快照必须先于 KV.put 挂载**：否则 KV 副本永远没有 budget 字段（v3 遗留坑，权威完整账以 run 记录为准）
10. **推送幂等守卫**：同 window 10 分钟内推过则 `/api/push` 跳过，`?force=1` 强制
11. **窗口匹配检查**：latest 属于其他 window 时跳过推送（不推旧班数据），force 可 bypass
12. **诊断埋点留痕**：抓取分片每个阶段（start/fetched/ok/error）都写 run:fetch 记录；digest 的静默 throw 路径也写 run:digest——**不许再把失败路径改成静默**（15:14/15:25 挂起事件的教训，见 §8.4）

---

## 3. 代码结构现状（1699 行单文件，15 区）

单文件、按「关注点分区」组织，顺序即依赖顺序（前面的区不依赖后面的区）。拆模块是可选项（§9），分区已满足「改一处不需要通读全文件」。

```
index.js（1699 行）
├── §1  config    配置与源清单（48-177）        —— 改源/改窗口/改超时只看这里
│                    KV/队列/分片/班次键常量、FEEDS 39 源、AI 模型、cron 分钟分流常量
├── §2  text      纯文本工具（178-218）          —— esc/stripTags/normTitle/cleanMd/tgTruncate
├── §3  time      时间工具（219-265）            —— toMs/toSec 统一单位；currentWindow 三班次
├── §4  xml       XML 解析基础设施（266-307）     —— pickText/pickLink/pickDate
├── §5  parsers   源数据解析·纯函数（308-371）    —— parseXmlFeed / parseEdgar
├── §6  budget    子请求预算（372-473）           —— BUDGET_PLAN 静态表 + createBudget 计数器
├── §7  net       子请求网关（474-551）           —— ★ fetch/KV/AI/队列发送的唯一出口
├── §8  sources   单源抓取（552-654）             —— fetchFeedOne / fetchAnthropic（seen 单次读写）
├── §9  shard     分片抓取与合并后处理（655-749） —— fetchShardFeeds + finalizeItems（纯函数）
├── §10 digest    每日精选（750-1012）            —— 模型链/容错解析/enrich/提示词/makeDigest
├── §11 telegram  推送（1013-1136）               —— buildPushPairs 保序 + sendChunk 限速重试
├── §12 render    网页渲染（1137-1226）            —— renderPage（纯函数）
├── §13 queue     队列消费者（1227-1462）          —— 13.1 抓取分片 / 13.2 精选 / 13.3 推送分片 / 13.4 死信
├── §14 worker    入口（1463-1667）                —— scheduled 分钟分流入队 / queue 分发 / fetch 路由
└── §15 exports   对外导出（1668-1699）            —— 供单测与调试引用，Worker 只用 default 导出
```

配套文件：`wrangler.toml`（绑定 + 4 队列 + cron）、`package.json`（唯一依赖 fast-xml-parser）、`smoke-v4.mjs`（17 项冒烟）、`e2e-v4.mjs`（39 项端到端，mock KV/队列/AI/fetch 全链路）。原文均在 §6。

---

## 4. 设计决策史（这些「奇怪」的代码都是有用意的，别当废话删掉）

### 4.1 v3 时代决策（全部仍有效）

| 代码现象 | 原因 |
|---|---|
| `guji:v4:seen` 跨调用去重集合，单次读写 | 原每篇文章各 1 get + 1 put（最多 24 次子请求），合并后构建从 63 降到 40。分片化后只有含 anthropic 的分片碰 seen，天然无并发写竞争 |
| prompt 里 `<arg_key:opensource>` + 尾部 `/no_think` + payload `chat_template_kwargs:{enable_thinking:false}` | Qwen3 思考模式会把 max_tokens 耗在推理上返回空 content（曾导致精选恒 0）。三重开关是 Cloudflare 侧实例陆续失效后逐个补上的，**全部保留** |
| `max_tokens: 3072` | 思考模型关不干净时，给推理留够空间才有可能吐出正文 |
| 模型链 智谱 glm-4-plus → llama | Qwen3-30B 在 Cloudflare 侧持续返回空 content（已观测多日，非代码问题）；glm-4-plus 中文原生、7 秒/次；llama 兜底。Qwen 仍留在无 key 时的链首位，等 Cloudflare 恢复 |
| `tgTruncate` 按 UTF-8 字节截断 | `String.slice(3800)` 按 UTF-16，纯中文可达 5700 字节被 Telegram 拒 |
| KV 数据里存 `lastDigestAt`/`logs`/`aiTrace`/`digestErr` | 定时任务无人值守，故障只能靠这些字段事后诊断（`/api/diag` 读取） |
| enrich 串行 + HTML_MAX 150KB | 并行大页面转换触发过 Error 1102（CPU/内存超限） |
| parseDigest 三层容错解析 | 模型输出过：标准 JSON 数组、换行分隔 JSON 对象、```json 包裹、带前言废话。GLM 输出标准数组，llama 输出换行分隔对象 |
| `fetchViaToMarkdown` 的 90s 超时 + Promise.race | GPU 冷启动可能 90s+ |

### 4.2 v4 队列化决策（2026-10-25）

| 代码现象 | 原因 |
|---|---|
| cron 只入队、四个消费者干活 | v3 单次调用抓取 41 + 精选 ≈ 48~50/50，余量为 0——每加一个源都要重算钢丝。队列化后每次调用独立预算，峰值 29/50 |
| 39 源分 4 片（SHARD_SIZE=10），分片键 `guji:v4:shard:{window}:{n}` | 单片 16/50 预算；**键按 window+序号隔离、无共享写**，是 max_concurrency=4 敢并行的前提 |
| cron 表达式 `40,50 21 * * *` 时间复用 | 一个表达式覆盖 :40/:50 两次触发，handler 按北京时间分钟分流；4 个表达式覆盖 6 个触发点，守住免费档 5 个 cron 上限 |
| 推送分 3 片、max_concurrency=1 串行 | 保序（报头→精选条目，私聊→频道）；单片 ≤9 条消息 × 至多 3 次尝试 ≤ 27 子请求，TG 抽风也不越限（v3 单 invocation 26 条无重试余量的问题解除） |
| 消费者「抛错即重试」，不吞错误 | 05:50 那类瞬时失败（模型抖动/网络）的结构性根治：队列本身就是重试器，退避 + DLQ 兜底 |
| digest 队列 90 秒延迟投递（**队列级 API 设置**） | 手动 `/api/build` 把抓取与精选同时入队时的竞态保险：给抓取消费者留就位时间。cron 路径 :40→:50 有 10 分钟缓冲，通常首轮即就绪 |
| 消费者内分片轮询 ≤45s + 预算护栏 40 | 延迟投递的双保险（生产端延迟曾被证明不可靠，见下条）：抓取异常慢时在调用内等；轮询预算超 40 交队列重试（每次重试全新 50 预算，10 次 ≈ 7.5 分钟总覆盖） |
| budget 快照先于 `kvPut(latest)` | v3 遗留坑：快照在 put 之后挂载 → KV 副本永远没有 budget 字段。权威完整账以 `run:{window}:digest` 为准 |
| `/api/push` 幂等守卫（同 window 10 分钟 + `?force=1`） | 防手滑重复推送；push 标记由推送消费者 slice 0 完成时写 |
| window 班次标识 `YYYY-MM-DD-am/pm`，分片/运行记录/幂等全部按 window 隔离 | 早晚班互不覆盖，旧键靠 2 天 TTL 自动清扫 |
| 诊断埋点：fetch 分片每阶段写 run:fetch，digest 静默 throw 路径也写 run:digest | 2026-10-25 15:14/15:25 连续两轮「3/4 分片消费者无痕迹消失」（现象与推断见 §8.4）。没有埋点时，失败路径全静默，只能靠猜 |
| 同 window 旧分片会被 digest 误判「就绪」 | 已知取舍：digest 只检查分片键存在、不校验新鲜度。同一 window 内二次 `/api/build` 可能读到上一轮的分片（同班次数据，分钟级差异），可接受；跨 window 有键隔离，不会读到昨天的 |
| `deliveryDelay`（camelCase）是坑不是配置 | wrangler 只在 **dev 模式**绑定转换里映射 camelCase→snake_case（源码 `cli.js` dev 路径）；wrangler.toml 走 schema 校验直接警告 `Unexpected fields` 并忽略。正确字段名是 `delivery_delay`——且实测 snake_case 上传元数据后对**已存在**的生产者绑定仍不生效，最终靠队列级 API PUT 解决（§8.2） |

---

## 5. 技术债清单

### 5.1 已修复（v4 落地时清理）

| 原债务 | 处置 |
|---|---|
| 单文件 885 行无分区 | 15 区结构化（§3），行内注释即地图 |
| 无任何自动化测试 | `smoke-v4.mjs`（17 项）+ `e2e-v4.mjs`（39 项），mock 全链路，部署前必跑 |
| time 单位混用 | `toMs/toSec/nowSec` 显式转换，注释禁令「裸写 *1000」 |
| 模型链写死 if/else | `buildChain` 声明式数组 `[{name, attempts, call}]`，attempts 之和对应预算配额 |
| 无预算可见性 | `createBudget/createNet` 全网关记账，BUDGET_PLAN 静态表 + 运行时明细，单测断言 `PLAN_PEAK ≤ 50` |
| 死代码 `feeds-list.mjs`/`renderTg`/`LAST_DIGEST_KEY` | 已删（feeds-list.mjs 文件仍在磁盘但未被引用，可删） |
| budget 字段进不了 KV（v3 审查记录） | 快照先于 put，已修 |
| `/api/push` 无 force 参数 | 已加（手动补推旧内容） |
| makeDigest 早退边界（无 AI 绑定时直接放弃） | 已修：有 ZHIPU_KEY 时无 AI 绑定也应能跑 |

### 5.2 仍开放（不碍运行，按需处理）

1. **Qwen3-30B 空 content**：Cloudflare 侧实例问题，代码已容错（重试+降级），但无 key 路径下每班仍浪费 ~8 秒在两次空尝试上。可加「连续失败 N 次后临时熔断」
2. **llama 兜底慢**（47s）且不遵守「≤12 条」约束（靠 parseDigest 硬截断兜底）；glm-4-plus 无此问题
3. **enrich 结果无缓存**：同 URL 重复构建会重复转换。缓存要权衡 KV 写也占子请求
4. **`/api/diag` 无鉴权**：含实时 AI/智谱探针，任何人可打（消耗少量额度）。生产可加简单 token
5. **DLQ 无告警**：死信消费者只 console.error，沙箱内 wrangler tail 不可用（WebSocket 被拦），死信只能靠 `/api/diag` 对照 run 记录缺失来发现。可接 Tail Worker 或邮件告警
6. **fetch 消费者挂起只能靠重试+埋点兜底**：15:14/15:25 事件（§8.4）根因未完全定位（疑调用级挂起占住并发槽位），重新部署可恢复。埋点已保证留痕；若再发，按 run:fetch 的 stage 记录定位
7. **单文件 1699 行**：分区已缓解维护压力；wrangler 支持 ESM 多文件打包，想拆 `feeds.js`/`parsers.js`/`digest.js`/`telegram.js`/`render.js` 随时可做（测试文件按模块路径引用即可，改动小）
8. **`/api/build` 同窗口重复调用会读到旧分片**（见 §4.2 最后一条）：如觉得不妥，可在分片记录里加 `enqueuedAt` 让 digest 校验新鲜度，成本约 1 次 KV 写

---

## 6. 全部在用代码

以下代码为线上运行版本原文（2026-10-25 15:38 部署，Version ID `bfce87d9-ae8f-4285-9c26-14cc5639510d`），逐字粘贴，未做任何删改。

### 6.1 `index.js` —— Cloudflare Worker 主体（1699 行）

```js
// ============================================================
// GUJI v4 · AI 情报日报（Cloudflare Worker · 严格一手来源）
// 只收：官方发布（大厂官网/博客）、SEC EDGAR 官方 filing、
//       原创工程媒体、深度访谈播客、arXiv 论文
// 不收：二手聚合、编译转载、新闻通稿网站
// ============================================================
//
// ★ v4 架构变革：队列化（Queues）
//   v3 及以前：一个 cron 调用里跑完「抓取 39 源 + 精选」，子请求峰值 47~50/50，
//              余量为 0——每加一个源、每加一次 enrich 都要重算一遍钢丝。
//   v4 现在：  cron 只负责入队；抓取/精选/推送各自是独立的消费者调用，
//              每次调用享有独立的 50 子请求预算，峰值 29/50，余量 21。
//              瞬时失败由队列自动退避重试兜底（05:50 那类事故的根治）。
//
// 数据流：
//   cron :40 ──入队 4 条──▶ [guji-fetch 消费者]   每片 10 源抓取 → 写分片 KV
//   cron :50 ──入队 1 条──▶ [guji-digest 消费者]  读 4 分片 → 合并 → enrich → AI 精选 → 写 latest
//   cron :00 ──入队 3 条──▶ [guji-send 消费者]    读 latest → 顺序分片限速推送 TG
//   任一消费者重试耗尽 → [guji-dlq 消费者] 记日志（不静默丢失）
//
// 本文件按「关注点分区」组织，顺序即依赖顺序（前面的区不依赖后面的区）：
//
//   §1  config    配置与源清单        —— 改源/改窗口/改超时只看这里
//   §2  text      纯文本工具          —— esc/stripTags/normTitle/cleanMd/tgTruncate
//   §3  time      时间工具            —— 统一毫秒/秒单位，消除裸 *1000；currentWindow
//   §4  xml       XML 解析基础设施
//   §5  parsers   源数据解析（纯函数）
//   §6  budget    子请求预算（记账不改流；现为「每阶段独立预算」）
//   §7  net       子请求网关（★ 所有 fetch/KV/AI/队列发送的唯一出口）
//   §8  sources   单源抓取
//   §9  shard     分片抓取与合并后处理（纯函数 finalizeItems）
//   §10 digest    模型链与每日精选
//   §11 telegram  推送（顺序分片 + 单条重试）
//   §12 render    网页渲染
//   §13 queue     队列消费者（抓取分片 / 精选 / 推送分片 / 死信）
//   §14 worker    入口（cron 入队 + 路由）
//   §15 exports   对外导出（供单测/调试引用，Worker 只使用 default 导出）
//
// ⚠️ 硬约束（免费套餐）：单次 Worker 调用最多 50 个子请求（fetch / KV / AI /
//    队列发送都算，第 51 个不抛异常、只在响应里报错——静默失败）。
//    队列化后每个消费者调用独立预算（见 §6 BUDGET_PLAN，峰值 29/50），
//    但纪律不变：新增任何子请求前，先过一遍 §6 budget 的账。
//    除 §7 net 区外，不得出现绕过网关的裸子请求调用。

import { XMLParser } from 'fast-xml-parser';

// ============================================================
// §1 config · 配置与源清单
// ============================================================

// ---------------- KV ----------------
const KV_KEY = 'guji:v4:latest';       // 主状态：{items, failures, generatedAt, digest, lastDigestAt, window, logs, digestErr, aiTrace, budget}
const SEEN_SET_KEY = 'guji:v4:seen';   // 跨调用去重集合（Anthropic slug，单次读写）

// ---------------- 队列 ----------------
// 队列名须与 wrangler.toml [[queues.producers]] 一致；v4 部署时自动创建。
const Q_FETCH = 'guji-fetch';   // 抓取分片（4 条消息 × 10 源）
const Q_DIGEST = 'guji-digest'; // 精选编制（1 条消息）
const Q_SEND = 'guji-send';     // 限速推送（3 条顺序分片消息）
const Q_DLQ = 'guji-dlq';       // 死信（重试耗尽落地，消费者只记日志）

// ---------------- 分片参数 ----------------
const SHARD_SIZE = 10;          // 每个抓取分片的源数（39 源 → 4 片）
const SEND_SLICES = 3;          // 推送分片数（顺序发送，保序；单片 ≤9 条消息 ×3 次尝试 ≤ 27 子请求）
// 精选消费者等分片就绪的最长时间（手动 /api/build 把抓取与精选同时入队时的竞态保险；
// 生产 cron 路径 :40 抓取 → :50 精选有 10 分钟缓冲，通常首轮即就绪）
const SHARD_WAIT_MS = 45000;
const SHARD_POLL_MS = 5000;

// ---------------- 班次键 ----------------
// 分片/运行记录的 KV 键都带 window，早晚班互不覆盖，旧键靠 TTL 自动清扫。
const shardKey = (window, n) => `guji:v4:shard:${window}:${n}`;
const runKey = (window, stage) => `guji:v4:run:${window}:${stage}`;

// ---------------- 增强（薄摘要补全文）----------------
// v4 起预算充裕（digest 班 18/50），从 v3 的 2 条提高到 4 条：
//   4 分片读 4 + latest 读 1 + 增强 4×2 = 8 + 模型链 4 + 落库 2 = 19/50
// ⚠️ 若下调 AI 链或分片数，这里可以再放；改大前先过 §6 budget 的账。
const ENRICH_MAX = 4;
const HTML_MAX = 150000; // 转 Markdown 前的 HTML 字符上限（控内存/CPU）

// ---------------- 时间窗口（秒）----------------
const MAX_AGE = { '72h': 72 * 3600, '7d': 7 * 86400, '14d': 14 * 86400, '30d': 30 * 86400, '120d': 120 * 86400 };

// ---------------- AI 关键词过滤 ----------------
// 用于 filter:1 的源：只保留标题命中 AI 关键词的条目，否则官方博客会被人事/营销稿刷屏
const AI_KW = /\b(ai|a\.i\.|artificial intelligence|llm|large language model|gpt|chatgpt|claude|gemini|copilot|agent|agentic|machine learning|deep learning|neural|transformer|diffusion|inference|foundation model|gpu|tpu|npu|hbm|accelerator|compute|datacenter|data cent(er|re)|chip|chips|semiconductor|silicon|nvidia|amd|intel|tsmc|foundry|hpc|blackwell|hopper|rubin|robot|humanoid|autonomous|self-driving|lidar|openai|anthropic|deepmind|mistral|grok|llama|qwen|deepseek|sora|multimodal|rag|embedding|fine-tun|distillation|quantiz)\b/i;
function isAI(it) { return AI_KW.test(it.title); }

// ---------------- 源清单 ----------------
// kind: rss | edgar | html
// cat:  芯片算力 | 数据中心 | 模型 | 应用产品 | 资本 | 论文 | 访谈观点
// ⚠️ 源数量只影响分片数与抓取班墙钟时间，不再影响单次调用子请求预算（队列化红利）。
const FEEDS = [
  // ---- 模型 / 实验室官方 ----
  { id:'openai',    name:'OpenAI',           kind:'rss', cat:'模型', win:'7d',  url:'https://openai.com/news/rss.xml' },
  { id:'deepmind',  name:'Google DeepMind',  kind:'rss', cat:'模型', win:'7d',  filter:1, url:'https://deepmind.google/blog/rss.xml' },
  { id:'googleai',  name:'Google AI',        kind:'rss', cat:'模型', win:'7d',  url:'https://blog.google/technology/ai/rss/' },
  { id:'meta',      name:'Meta AI',          kind:'rss', cat:'模型', win:'7d',  filter:1, url:'https://about.fb.com/feed/' },
  { id:'hf',        name:'Hugging Face',     kind:'rss', cat:'模型', win:'7d',  url:'https://huggingface.co/blog/feed.xml' },
  { id:'anthropic', name:'Anthropic',        kind:'html',cat:'模型', win:'7d',  url:'https://www.anthropic.com/news' },

  // ---- 芯片 / 算力 / 数据中心：官方 + 原创工程媒体 ----
  { id:'nvdanews',  name:'NVIDIA Newsroom',  kind:'rss', cat:'芯片算力', win:'7d',  filter:1, url:'https://nvidianews.nvidia.com/releases.xml' },
  { id:'nvdablog',  name:'NVIDIA 博客',       kind:'rss', cat:'芯片算力', win:'7d',  filter:1, url:'https://blogs.nvidia.com/feed/' },
  { id:'arm',       name:'Arm Newsroom',     kind:'rss', cat:'芯片算力', win:'7d',  url:'https://newsroom.arm.com/rss' },
  { id:'tomshw',    name:"Tom's Hardware",   kind:'rss', cat:'芯片算力', win:'72h', filter:1, url:'https://www.tomshardware.com/rss.xml' },
  { id:'sth',       name:'ServeTheHome',     kind:'rss', cat:'数据中心', win:'72h', filter:1, url:'https://www.servethehome.com/feed/' },

  // ---- SEC EDGAR 官方 filing（IR 页被 Cloudflare 挡，这里永远畅通）----
  { id:'e-mrvl', name:'Marvell',   kind:'edgar', cat:'芯片算力', cik:'0001835632', type:'8-K' },
  { id:'e-avgo', name:'Broadcom',  kind:'edgar', cat:'芯片算力', cik:'0001730168', type:'8-K' },
  { id:'e-dell', name:'Dell',      kind:'edgar', cat:'数据中心', cik:'0001571996', type:'8-K' },
  { id:'e-smci', name:'Supermicro',kind:'edgar', cat:'数据中心', cik:'0001375365', type:'8-K' },
  { id:'e-anet', name:'Arista',    kind:'edgar', cat:'数据中心', cik:'0001596532', type:'8-K' },
  { id:'e-tsm',  name:'TSMC',      kind:'edgar', cat:'芯片算力', cik:'0001046179', type:'6-K' },
  { id:'e-intc', name:'Intel',     kind:'edgar', cat:'芯片算力', cik:'0000050863', type:'8-K' },
  { id:'e-nvda', name:'NVIDIA·SEC',kind:'edgar', cat:'芯片算力', cik:'0001045810', type:'8-K' },
  { id:'e-amd',  name:'AMD·SEC',   kind:'edgar', cat:'芯片算力', cik:'000002488',  type:'8-K' },
  { id:'e-arm',  name:'Arm·SEC',   kind:'edgar', cat:'芯片算力', cik:'0001973239', type:'6-K' },
  { id:'e-googl',name:'Alphabet·SEC',kind:'edgar',cat:'资本',    cik:'001652044', type:'8-K' },
  { id:'e-msft', name:'Microsoft·SEC',kind:'edgar',cat:'模型',   cik:'0000789019', type:'8-K' },

  // ---- 深度访谈 / 观点（月更-周更，窗口 30 天）----
  { id:'p-acquired', name:'Acquired',           kind:'rss', cat:'访谈观点', win:'30d', url:'https://feeds.transistor.fm/acquired' },
  { id:'p-dwarkesh', name:'Dwarkesh Podcast',   kind:'rss', cat:'访谈观点', win:'30d', url:'https://apple.dwarkesh-podcast.workers.dev/feed.rss' },
  { id:'p-lex',      name:'Lex Fridman',        kind:'rss', cat:'访谈观点', win:'30d', url:'https://lexfridman.com/feed/podcast/' },
  { id:'p-nopriors', name:'No Priors',          kind:'rss', cat:'访谈观点', win:'30d', url:'https://feeds.megaphone.fm/nopriors' },
  { id:'p-allin',    name:'All-In',             kind:'rss', cat:'访谈观点', win:'30d', url:'https://rss.libsyn.com/shows/254861/destinations/1928300.xml' },
  { id:'p-bg2',      name:'BG2',                kind:'rss', cat:'访谈观点', win:'30d', url:'https://anchor.fm/s/s06c2370/podcast/rss' },
  { id:'p-latent',   name:'Latent Space',       kind:'rss', cat:'访谈观点', win:'30d', url:'https://podcast.latent.space/feed.xml' },
  { id:'p-cogrev',   name:'Cognitive Revolution',kind:'rss',cat:'访谈观点', win:'30d', url:'https://feeds.megaphone.fm/RINTP3108857801' },
  { id:'p-iltb',     name:'Invest Like the Best',kind:'rss',cat:'访谈观点', win:'30d', url:'https://feeds.megaphone.fm/CLS2859450455' },
  { id:'p-20vc',     name:'20VC',               kind:'rss', cat:'访谈观点', win:'30d', url:'https://rss.libsyn.com/shows/61840/destinations/240976.xml' },
  { id:'p-a16z',     name:'AI + a16z',          kind:'rss', cat:'访谈观点', win:'30d', url:'https://feeds.simplecast.com/Hb_IuXOo' },
  { id:'p-training', name:'Sequoia Training Data',kind:'rss',cat:'访谈观点', win:'30d', url:'https://feeds.megaphone.fm/trainingdata' },
  { id:'p-eye',      name:'Eye on AI',          kind:'rss', cat:'访谈观点', win:'30d', url:'https://rss.libsyn.com/shows/123267/destinations/727317.xml' },
  { id:'p-stratechery', name:'Stratechery',     kind:'rss', cat:'访谈观点', win:'30d', url:'https://stratechery.com/feed/' },

  // ---- 投资圈：官方 13F ----
  { id:'e-brk', name:'Berkshire 13F', kind:'edgar', cat:'资本', win:'120d', cik:'0001067983', type:'13F-HR' },

  // ---- 论文 ----
  { id:'arxiv', name:'arXiv cs.CL', kind:'rss', cat:'论文', win:'72h', url:'https://rss.arxiv.org/rss/cs.CL' },
];

// 抓取分片总数：从 FEEDS 推导，勿手改（39 源 → 4 片）
const SHARD_COUNT = Math.ceil(FEEDS.length / SHARD_SIZE);

// ---------------- 抓取参数 ----------------
const UA_CHROME = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36';
export const UA_SEC = 'GUJI AI Digest guji-digest@example.com'; // SEC EDGAR 要求 UA 含联系邮箱
const FETCH_MS = 12000; // 单源超时（参考实现为 15s，这里略收紧）
const BATCH_MS = 15000; // 整批兜底超时（防 Node undici 下 abort 不 reject 导致挂起）
const CONCURRENCY = 4;  // 小批次并发，兼顾生产速度与稳定

// ---------------- AI 模型 ----------------
const AI_MODELS = ['@cf/qwen/qwen3-30b-a3b-fp8', '@cf/meta/llama-3.1-8b-instruct-fp8']; // 主力 + 备选
const AI_MS = 150000; // 首次调用 GPU 冷启动可能 90s+，放宽容忍；队列调用无 15 分钟墙钟限制但仍保守

// ---------------- cron 时间表（UTC，须与 wrangler.toml 一致）----------------
//   40,50 21 * * * → 北京 05:40 抓取入队 / 05:50 精选入队（早班）
//   0    22 * * * → 北京 06:00 推送入队（早报）
//   40,50 13 * * * → 北京 21:40 抓取入队 / 21:50 精选入队（晚班）
//   0    14 * * * → 北京 22:00 推送入队（晚报）
// scheduled  handler 按北京时间的分钟数分流：40 抓取 / 50 精选 / 0 推送。
const FETCH_MINUTE = 40;
const DIGEST_MINUTE = 50;
const SEND_MINUTE = 0;

// ---------------- 渲染 ----------------
const CAT_ORDER = [
  ['芯片算力', '🔩'], ['数据中心', '🏭'], ['模型', '🧠'],
  ['应用产品', '🚀'], ['资本', '💰'], ['访谈观点', '🎙️'], ['论文', '📚']
];

// ============================================================
// §2 text · 纯文本工具（无副作用，可单测）
// ============================================================

const esc = (s) => String(s || '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

const stripTags = (s) => String(s || '')
  .replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g,' ').replace(/&amp;/g,'&')
  .replace(/&#0*39;|&apos;/g, "'").replace(/&quot;/g,'"').replace(/&lt;/g,'<').replace(/&gt;/g,'>')
  .replace(/&#x([0-9a-f]+);/gi, (_, h) => { try { return String.fromCodePoint(parseInt(h, 16)); } catch (e) { return _; } })
  .replace(/&#(\d+);/g, (_, d) => { try { return String.fromCodePoint(parseInt(d, 10)); } catch (e) { return _; } })
  .replace(/\s+/g,' ').trim();

// 同题多源去重的键：小写化 → 只保留字母/数字/汉字 → 截断 50
// （去掉标点与空格，让 "GPT-6 发布" 与 "GPT6发布" 归一）
const normTitle = (s) => String(s||'').toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g,'').slice(0,50);

// markdown 轻量清理：去标题符号/链接语法/强调符，留纯文本给模型
const cleanMd = (s) => String(s || '')
  .replace(/^#{1,6}\s*/gm, '')
  .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
  .replace(/[*_`>|]/g, ' ')
  .replace(/\s+/g, ' ').trim();

/**
 * 按 UTF-8 字节长度截断（Telegram 单条消息上限 4096 字节）。
 *
 * ⚠️ 历史坑：不能用 String.slice —— 它按 UTF-16 计数，
 *    中文 1 字 = 2 单位 = 3 字节，slice(0,3800) 的纯中文实际可达 5700 字节，
 *    会被 Telegram 整条拒绝。必须走 TextEncoder。
 *
 * 注意既有语义：入参非字符串时原样返回（不会变成空串）。
 */
export function tgTruncate(s, maxBytes = 3600) {
  const enc = new TextEncoder();
  const bytes = enc.encode(s);
  if (bytes.length <= maxBytes) return s;
  const cut = bytes.slice(0, maxBytes);
  return new TextDecoder().decode(cut).replace(/\uFFFD+$/, '') + '\n…（已截断）';
}

// ============================================================
// §3 time · 时间工具
// ============================================================
//
// ⚠️ 单位约定（历史混用坑，务必遵守）：
//   - 条目时间戳 it.ts          → 毫秒（来自 Date.parse / Date.now）
//   - lastDigestAt / nowSec     → 秒
//   - MAX_AGE / expirationTtl   → 秒
// 跨单位比较一律走 toMs / toSec，禁止裸写 *1000 或 /1000。

/** 北京时间的 UTC 偏移（UTC+8），毫秒 */
const BJ_OFFSET_MS = 8 * 3600 * 1000;

const pad = (n) => String(n).padStart(2, '0');

/** 秒 → 毫秒 */
const toMs = (sec) => Math.floor(Number(sec) || 0) * 1000;

/** 毫秒 → 秒 */
const toSec = (ms) => Math.floor((Number(ms) || 0) / 1000);

/** 当前时间的秒级时间戳 */
const nowSec = () => Math.floor(Date.now() / 1000);

/** 毫秒时间戳 → 北京时间 MM-DD HH:mm（UTC+8） */
export function bjTime(ts) {
  const d = new Date((ts || Date.now()) + BJ_OFFSET_MS);
  return `${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

/** 毫秒时间戳 → 北京时间的 Date 对象（供需要取 UTC 字段做进一步判断的场合） */
const bjDate = (ts) => new Date((ts || Date.now()) + BJ_OFFSET_MS);

/** 条目（毫秒）是否比 lastAtSec（秒）更新 */
const isFresh = (tsMs, lastAtSec) => (Number(tsMs) || 0) > toMs(lastAtSec);

/**
 * 班次标识：如 2026-10-25-am / 2026-10-25-pm。
 * 抓取分片、运行记录、幂等守卫全部按 window 隔离，早晚班互不覆盖。
 * 入参传 cron 的 event.scheduledTime（名义触发时刻，而非 Date.now()），保证确定性。
 */
export function currentWindow(tsMs = Date.now()) {
  const d = bjDate(tsMs);
  const edition = d.getUTCHours() < 12 ? 'am' : 'pm';
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}-${edition}`;
}

// ============================================================
// §4 xml · 解析基础设施
// ============================================================

// RSS/Atom/RDF 统一解析器（fast-xml-parser v5：原生处理 CDATA、实体、属性）
// 模块级单例：无内部状态，可安全复用（避免每条 feed 重复构造）
const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  textNodeName: '#text',
  isArray: (name) => ['item', 'entry', 'category'].includes(name),
  processEntities: true,
});

const pickText = (node) => {
  if (node == null) return '';
  if (typeof node === 'string') return node.trim();
  if (typeof node === 'object' && node['#text'] != null) return String(node['#text']).trim();
  return '';
};

const pickLink = (node) => {
  if (node == null) return '';
  if (typeof node === 'string') return node.trim();
  if (Array.isArray(node)) {
    const alt = node.find(l => l && typeof l === 'object' && l['@_rel'] === 'alternate')
      || node.find(l => l && typeof l === 'object' && l['@_href']);
    return alt ? String(alt['@_href'] || '').trim() : '';
  }
  if (typeof node === 'object' && node['@_href']) return String(node['@_href']).trim();
  if (typeof node === 'object' && node['#text']) return String(node['#text']).trim();
  return '';
};

const pickDate = (...nodes) => {
  for (const n of nodes) {
    const s = pickText(n);
    if (s) { const t = Date.parse(s); if (!isNaN(t)) return t; }
  }
  return 0;
};

// ============================================================
// §5 parsers · 源数据解析（纯函数）
// ============================================================

// RSS / Atom / RDF 通用解析（fast-xml-parser，自动处理 CDATA 与实体）
export function parseXmlFeed(xml, source) {
  let items = [];
  try {
    const doc = xmlParser.parse(xml);
    if (doc.rss?.channel?.item) items = doc.rss.channel.item;
    else if (doc.feed?.entry) items = doc.feed.entry;
    else if (doc['rdf:RDF']?.item) items = doc['rdf:RDF'].item;
    else if (doc.channel?.item) items = doc.channel.item;
    else if (doc.item) items = doc.item;
  } catch (e) { return []; }
  const out = [];
  for (const it of (Array.isArray(items) ? items : []).slice(0, 40)) {
    if (!it || typeof it !== 'object') continue;
    const title = stripTags(pickText(it.title));
    if (!title) continue;
    const link = pickLink(it.link) || pickText(it.id) || pickText(it.guid) || source.url;
    const ts = pickDate(it.pubDate, it.published, it.updated, it['dc:date'], it.date);
    const desc = stripTags(pickText(it.description) || pickText(it.summary) || pickText(it['content:encoded']) || pickText(it.content));
    out.push({
      title: title.slice(0, 300),
      link: String(link).slice(0, 500),
      ts,
      desc: desc.slice(0, 600),
      source: source.name, srcId: source.id, cat: source.cat, kind: source.kind,
      win: source.win, filter: source.filter
    });
  }
  return out;
}

// SEC EDGAR atom：从 summary 提取 "Item X.XX: 主题"
export function parseEdgar(xml, source) {
  let entries = [];
  try {
    const doc = xmlParser.parse(xml);
    entries = doc.feed?.entry || [];
  } catch (e) { return []; }
  const out = [];
  for (const e of (Array.isArray(entries) ? entries : []).slice(0, 8)) {
    if (!e || typeof e !== 'object') continue;
    const link = pickLink(e.link);
    const ts = pickDate(e.updated, e.published);
    const summary = pickText(e.summary) || pickText(e.content);
    const items = [...summary.matchAll(/Item\s+[\d.]+:\s*([^<]+)/g)].map(m => m[1].trim()).filter(Boolean);
    const itemText = items.slice(0, 2).join(' · ');
    const ftype = source.type === '13F-HR' ? '13F 持仓' : source.type + ' 事件';
    const title = itemText ? `${source.name} · ${ftype}：${itemText}` : `${source.name} · ${ftype}`;
    out.push({
      title: title.slice(0, 300),
      link: String(link).slice(0, 500),
      ts,
      desc: stripTags(summary).slice(0, 300),
      source: source.name, srcId: source.id, cat: source.cat, kind: source.kind,
      win: source.win, filter: source.filter
    });
  }
  return out;
}

// ============================================================
// §6 budget · 子请求预算（记账不改流）
// ============================================================
//
// Cloudflare 免费套餐：单次 Worker 调用最多 50 个子请求（fetch / KV / AI /
// 队列发送都算）。⚠️ 第 51 个不抛异常、只在响应里报错——静默失败，
// 这是本项目历史上最难排查的事故模式（digest=0 的根因之一）。
//
// v4 队列化后的预算模型：每个消费者调用独立计数，峰值远低于上限。
//
// 设计原则：
//   1. 计数，但不限流。网关负责记账与告警，**绝不**因预算耗尽而跳过必要的 KV.put。
//      静默丢数据比超限报错危险得多。
//   2. 每次调用都留痕（seq / kind / label / total），明细随运行记录写进 KV，
//      供 /api/diag 在线排查。
//   3. 静态预算表 BUDGET_PLAN 是"应然"，运行时计数是"实然"，
//      二者按阶段一一对应——任何新增子请求都会立刻在账上看出来。

/** 免费套餐单次调用子请求硬上限 */
export const SUBREQUEST_LIMIT = 50;

/** 告警线（免费套餐无 [limits] 可调，只能提前预警） */
export const SUBREQUEST_WARN = 45;

/**
 * 静态预算表：v4 各阶段（每次调用独立预算）。
 * 改动这里的数字前，先确认 wrangler.toml 的 crons 与 FEEDS 数量是否同步。
 */
export const BUDGET_PLAN = [
  { stage: 'cron.enqueue', label: '入队 4 条抓取分片消息（队列发送保守计入）', quota: 4 },
  { stage: 'fetch.shard', label: '单分片：10 源 fetch + Anthropic seen 2 + 诊断埋点 2 + 分片落库 1', quota: 16 },
  { stage: 'digest', label: '读 4 分片 + latest 1 + 增强 4×2 + 模型链 4 + 落库 2（等分片轮询另有 40 预算护栏）', quota: 19 },
  { stage: 'send.slice', label: '单推送片：latest 1 + ≤9 条消息×至多 3 次尝试 + 统计 1', quota: 29 },
];

/** 静态预算峰值（单次调用最大消耗），单测断言它 ≤ SUBREQUEST_LIMIT */
export const PLAN_PEAK = BUDGET_PLAN.reduce((max, p) => Math.max(max, p.quota), 0);

/**
 * 创建一个预算计数器。每个消费者调用各一个实例。
 * @param {object} [opts]
 * @param {number} [opts.limit]   硬上限
 * @param {number} [opts.warnAt]  告警线
 * @param {boolean} [opts.silent] 静音（测试用）
 */
export function createBudget({ limit = SUBREQUEST_LIMIT, warnAt = SUBREQUEST_WARN, silent = false } = {}) {
  /** @type {{seq:number, kind:string, label:string, n:number, total:number}[]} */
  const entries = [];
  let total = 0;
  let seq = 0;
  let breached = false;

  return {
    /** 已消耗的子请求总数 */
    get total() { return total; },
    /** 硬上限 */
    get limit() { return limit; },
    /** 是否已越过硬上限 */
    get breached() { return breached; },

    /**
     * 记一次子请求消耗。
     * @param {number} n      消耗次数（一般为 1）
     * @param {'fetch'|'kv'|'ai'|'queue'} kind
     * @param {string} [label] 可读标签，用于日志与断言
     */
    spend(n = 1, kind = 'fetch', label = '') {
      total += n;
      seq += 1;
      const entry = { seq, kind, label, n, total };
      entries.push(entry);
      if (!silent && total >= warnAt) {
        console.error(`[budget] ${total}/${limit} ⚠️ ${kind}:${label}`);
      }
      if (total > limit && !breached) {
        breached = true;
        console.error(`[budget] ❌ 超出预算 ${total}/${limit} @ ${kind}:${label}`);
      }
      return entry;
    },

    /** 按类型汇总 */
    byKind() {
      const acc = { fetch: 0, kv: 0, ai: 0, queue: 0 };
      for (const e of entries) acc[e.kind] = (acc[e.kind] || 0) + e.n;
      return acc;
    },

    /** 完整明细，供 /api/diag 与单测断言 */
    report() {
      return {
        total,
        limit,
        warnAt,
        breached,
        byKind: this.byKind(),
        entries: entries.slice(),
      };
    },
  };
}

// ============================================================
// §7 net · 子请求网关（★ 所有 fetch / KV / AI / 队列发送的唯一出口）
// ============================================================
//
// 存在的意义：让「这次调用花了多少子请求」变成可数、可断言、可日志的事实，
// 而不是散落在注释里的数学题。
//
// 铁律：
//   - 除本区外，本文件其他区不得出现裸 fetch(、env.GUJI_KV.、env.AI.、
//     env.GUJI_*_Q.send( 调用
//   - 网关只记账，不拦截。预算超了照常执行并告警，绝不静默跳过写操作。

/**
 * 创建绑定在当前 env 与预算计数器上的网络出口。
 * @param {object} env    Worker 环境变量（含 GUJI_KV / AI / 队列绑定 / JINA_KEY / ZHIPU_KEY）
 * @param {object} budget createBudget() 返回的计数器
 */
export function createNet(env, budget) {
  const kv = env && env.GUJI_KV ? env.GUJI_KV : null;
  const ai = env && env.AI ? env.AI : null;

  return {
    /**
     * 发起一次 HTTP 请求（计 1 个子请求）。
     * 签名与全局 fetch 一致，返回原始 Response，由调用方决定如何消费。
     */
    async fetch(url, init = {}, label = '') {
      budget.spend(1, 'fetch', label || url);
      return fetch(url, init);
    },

    /** KV 读（计 1 个子请求）。未绑定 KV 时返回 null 且**不计数**——与基线行为一致。 */
    async kvGet(key, type = 'text', label = '') {
      if (!kv) return null;
      budget.spend(1, 'kv', label || `get:${key}`);
      return kv.get(key, type);
    },

    /** KV 写（计 1 个子请求）。未绑定 KV 时静默跳过且不计数。 */
    async kvPut(key, value, opts = {}, label = '') {
      if (!kv) return undefined;
      budget.spend(1, 'kv', label || `put:${key}`);
      return kv.put(key, value, opts);
    },

    /** Workers AI 推理（计 1 个子请求） */
    async aiRun(model, payload, label = '') {
      budget.spend(1, 'ai', label || model);
      if (!ai) throw new Error('AI 绑定缺失');
      return ai.run(model, payload);
    },

    /** Workers AI toMarkdown（计 1 个子请求）。不支持时返回 null 且**不计数**。 */
    async aiToMarkdown(files, label = '') {
      if (!ai || typeof ai.toMarkdown !== 'function') return null;
      budget.spend(1, 'ai', label || 'toMarkdown');
      return ai.toMarkdown(files);
    },

    /**
     * 队列发送（保守计 1 个子请求；Cloudflare 未明确队列发送是否计入
     * 50 上限，按计入估——cron 入队班最多 4 条，两种口径下都安全）。
     */
    async qSend(binding, body, label = '') {
      if (!binding) throw new Error('队列绑定缺失: ' + (label || '?'));
      budget.spend(1, 'queue', label || 'queue:send');
      return binding.send(body);
    },

    /** 是否已绑定 KV（供调用方做短路判断，避免无谓的分支） */
    get hasKv() { return !!kv; },

    /** 原始 env 只读引用：仅供读取特性开关（如 ZHIPU_KEY / JINA_KEY / TG_*），
     *  ⚠️ 不要绕过网关直接调用 env.GUJI_KV / env.AI / 队列 binding —— 那样会漏记预算。 */
    get env() { return env; },
  };
}

// ============================================================
// §8 sources · 单源抓取
// ============================================================
// 一个源固定 1 次 fetch，失败静默记入 failures，绝不让单个源拖垮整体。
// 参照 cloudflare-rss-aggregator 的 fetcher：AbortController 超时 + try/catch 兜底。

/**
 * 抓取单个源。
 * @param {object} f      FEEDS 中的源定义
 * @param {object} net    createNet() 出口
 * @param {Array}  failures 失败收集器（就地 push）
 * @returns {Promise<object[]>} 解析后的条目（失败时为空数组）
 */
async function fetchFeedOne(f, net, failures) {
  try {
    if (f.kind === 'html') return await fetchAnthropic(f, net);
    const url = f.kind === 'edgar'
      ? `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${f.cik}&type=${f.type}&dateb=&owner=include&count=10&output=atom`
      : f.url;
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), FETCH_MS);
    let xml;
    try {
      const r = await net.fetch(url, {
        signal: ac.signal,
        redirect: 'follow',
        headers: {
          'User-Agent': f.kind === 'edgar' ? UA_SEC : UA_CHROME,
          'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*',
        },
      }, `source:${f.id}`);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      xml = await r.text();
    } finally {
      clearTimeout(timer);
    }
    const parsed = f.kind === 'edgar' ? parseEdgar(xml, f) : parseXmlFeed(xml, f);
    console.error(`✓ ${f.id}: ${xml.length}B → ${parsed.length} 条`);
    return parsed;
  } catch (e) {
    const err = String(e && e.message || e).slice(0, 80);
    failures.push({ name: f.name, err });
    console.error(`✗ ${f.id}: ${err}`);
    return [];
  }
}

/**
 * Anthropic 官网 HTML 兜底（无 RSS）：抓 FeaturedGrid 文章链接 + 标题。
 * ⚠️ 子请求账：1 fetch + 1 KV get（无条件，只要有 KV 绑定）+ 1 KV put（仅当有新 slug）
 *    旧实现是每篇文章各 1 次 get + 1 次 put（最多 24 次），免费套餐装不下，故合并为单次读写。
 *    分片化后只有含 anthropic 的那一片会碰 seen 集合，天然无并发写竞争。
 */
export async function fetchAnthropic(source, net) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), FETCH_MS);
  let html;
  try {
    const r = await net.fetch(source.url, {
      signal: ac.signal,
      redirect: 'follow',
      headers: { 'User-Agent': UA_CHROME, 'Accept': 'text/html,application/xhtml+xml,*/*' },
    }, `source:${source.id}`);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    html = await r.text();
  } finally {
    clearTimeout(timer);
  }
  const out = [];
  const seen = new Set();
  const nowSecValue = nowSec();
  // 一次性读取跨调用去重集合（见上方预算说明）
  const seenMap = (await net.kvGet(SEEN_SET_KEY, 'json', 'seen:get').catch(() => null)) || {};
  const newSeen = {};
  // 栏目页黑名单（TopicLink 类）
  const NAV = new Set(['announcements','engineering','research','policy','news','company','public-benefit']);
  const re = /<a[^>]*class="[^"]*FeaturedGrid-module[^"]*tileLink[^"]*"[^>]*href="\/news\/([a-z0-9-]+)"[^>]*>([\s\S]*?)<\/a>/g;
  let m;
  while ((m = re.exec(html)) && out.length < 12) {
    const slug = m[1];
    if (NAV.has(slug) || seen.has(slug)) continue;
    const title = stripTags(m[2]);
    if (!title || title.length < 8) continue;
    seen.add(slug);
    if (seenMap[slug]) continue; // 之前已推送过，跳过
    newSeen[slug] = nowSecValue;
    out.push({
      title: `Anthropic：${title}`.slice(0, 300),
      link: 'https://www.anthropic.com/news/' + slug,
      ts: Date.now(), // 无日期，按“新出现”算
      desc: '', source: source.name, srcId: source.id, cat: source.cat, kind: source.kind,
      win: source.win, filter: source.filter
    });
  }
  // 一次性写回去重集合，并清理 60 天前的记录（避免无限膨胀）
  if (Object.keys(newSeen).length) {
    const merged = Object.assign({}, seenMap, newSeen);
    for (const k of Object.keys(merged)) if (nowSecValue - merged[k] > 60 * 86400) delete merged[k];
    await net.kvPut(SEEN_SET_KEY, JSON.stringify(merged), { expirationTtl: 90 * 86400 }, 'seen:put').catch(() => {});
  }
  return out;
}

// ============================================================
// §9 shard · 分片抓取与合并后处理
// ============================================================
// v4 起抓取按分片进行：cron 入队 SHARD_COUNT 条消息，消费者每次处理一片
// （10 源，约 13/50 子请求）。本区包含两个纯函数/准纯函数：
//   fetchShardFeeds —— 一片源的抓取（并发 + 兜底超时）
//   finalizeItems   —— 跨分片合并后的后处理（过滤/限额/去重/排序），可单测

// ---------------- 分类微调 ----------------
// 模型类里提到数据中心/能源/代工的，归到数据中心（避免「芯片算力」吃掉一切）
const DC_KW = /datacenter|data center|data-centre|energy|power grid|foundry|gigawatt|nuclear|reactor|grid|utility|utilities|semicapid/i;
function recat(item) {
  if (item.cat === '模型' && DC_KW.test(item.title)) item.cat = '数据中心';
  return item;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/** 每源限额：防止单一高产量媒体刷屏 */
const SRC_LIMIT = { tomshw: 8, sth: 8 };

/**
 * 抓取一个分片内的全部源（小批次并发 + 每批兜底超时 + 逐源 try/catch）。
 * @param {object[]} feeds   本分片的源定义（≤ SHARD_SIZE 个）
 * @param {object} net       createNet() 出口
 * @param {Array}  failures  失败收集器（就地 push）
 * @returns {Promise<object[]>} 解析后的条目（拼接，未过滤）
 */
export async function fetchShardFeeds(feeds, net, failures) {
  const results = [];
  for (let i = 0; i < feeds.length; i += CONCURRENCY) {
    const batch = feeds.slice(i, i + CONCURRENCY).map(f => fetchFeedOne(f, net, failures));
    // 兜底：某批整体不 settle（Node undici 下偶发）也不能挂死整个调用
    const settled = await Promise.race([
      Promise.all(batch),
      sleep(BATCH_MS).then(() => null),
    ]);
    if (settled) results.push(...settled);
    else failures.push({ name: `第 ${i / CONCURRENCY + 1} 批`, err: '批次超时' });
  }
  return results.flat();
}

/**
 * 跨分片合并后的后处理（纯函数）：时间窗过滤 → 无日期放行 → AI 关键词 →
 * 每源限额 → arXiv 截断 → 同题多源去重 → 按时间倒序。
 * ⚠️ 语义与 v3 collect() 的后半段完全一致（单测可直接比对）。
 * @param {object[]} all 全部分片拼接的原始条目
 * @returns {object[]} 可进 AI 精选的条目池
 */
export function finalizeItems(all) {
  const nowSecValue = nowSec();
  const kept = [];
  for (const it of all) {
    const win = it.win || (it.kind === 'edgar' ? '14d' : '72h');
    const maxAge = MAX_AGE[win] || MAX_AGE['72h'];
    // ① 超出时间窗口的丢弃（ts 为 0 表示无日期，不在此处判死）
    if (it.ts && nowSecValue - toSec(it.ts) > maxAge) continue;
    // ② 无日期的只有 anthropic 源放行（按「新出现」算）
    if (!it.ts && it.srcId !== 'anthropic') continue;
    // ③ filter:1 的源只保留 AI 相关
    if (it.filter && !isAI(it)) continue;
    kept.push(recat(it));
  }

  // 限额只对「其余源」生效；arXiv 单独在下面取最新 8 条
  const srcCount = {};
  const limited = [];
  for (const it of kept) {
    const lim = SRC_LIMIT[it.srcId];
    if (lim) {
      srcCount[it.srcId] = (srcCount[it.srcId] || 0) + 1;
      if (srcCount[it.srcId] > lim) continue;
    }
    limited.push(it);
  }

  const nonArxiv = limited.filter(i => i.srcId !== 'arxiv');
  const arxiv = kept.filter(i => i.srcId === 'arxiv').slice(0, 8);
  // 去重合并（同题多源）
  const byKey = new Map();
  for (const it of nonArxiv.concat(arxiv)) {
    const k = normTitle(it.title);
    if (!k) continue;
    if (byKey.has(k)) {
      const ex = byKey.get(k);
      if (!ex.also.includes(it.source)) ex.also.push(it.source);
    } else {
      it.also = [];
      byKey.set(k, it);
    }
  }
  return [...byKey.values()].sort((a, b) => (b.ts || 0) - (a.ts || 0));
}

// ============================================================
// §10 digest · 每日精选 · 中文总结
// ============================================================
//
// 流程严格按「先整理精选，再总结翻译」：
//   第一步，模型通读今日全部条目，挑出真正重要的（跳过营销/人事/无信息量）；
//   第二步，只对精选结果写中文标题 + 1-2 句中文摘要（基于原文，不编造）。
// 绝不做「一开始就把所有英文条目全文翻译」——那只会产出噪音。

/** 素材上限：先按新鲜度截断，模型再精选，双控输出长度 */
const MATERIAL_MAX = 24;

/** 精选条数硬上限（与提示词「不超过 12 条」对齐） */
export const DIGEST_MAX = 12;

// ---------------- AI 调用 ----------------

async function runAI(net, model, payload) {
  let timer;
  const timeout = new Promise((_, rej) => { timer = setTimeout(() => rej(new Error('AI 超时')), AI_MS); });
  try {
    return await Promise.race([net.aiRun(model, payload, model), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

// 兼容三种响应形态：string / [{content}] / {content}
export function aiText(res) {
  const r = res && (res.response !== undefined ? res.response : res.result);
  if (typeof r === 'string') return r;
  if (Array.isArray(r)) {
    return r.map(x => (x && typeof x === 'object' ? String(x.content ?? x.text ?? '') : String(x))).join('');
  }
  if (r && typeof r === 'object') return String(r.content ?? r.text ?? '');
  return '';
}

// ---------------- 容错解析 ----------------

/**
 * 解析模型输出。容错链：整体 JSON → 截取数组段 → 逐对象提取
 * （抗截断 / ```json 包裹 / 前言废话）。
 * @param {string} text     模型原始输出
 * @param {object[]} material 素材（idx 从 1 开始对应 material[0]）
 */
export function parseDigest(text, material) {
  let arr = null;
  const s = String(text || '').replace(/```(?:json)?/gi, '');
  try { arr = JSON.parse(s); } catch (e) {
    const m = s.match(/\[[\s\S]*\]/);
    if (m) { try { arr = JSON.parse(m[0]); } catch (e2) { arr = null; } }
  }
  if (!Array.isArray(arr)) {
    arr = [];
    const re = /\{[^{}]*\}/g;
    let m;
    while ((m = re.exec(s))) {
      try { arr.push(JSON.parse(m[0])); } catch (e3) { /* 跳过这个残段 */ }
    }
  }
  const out = [];
  const seen = new Set();
  for (const d of arr) {
    const idx = Number(d && d.idx);
    if (!Number.isInteger(idx) || idx < 1 || idx > material.length || seen.has(idx)) continue;
    const title = String(d.title || '').trim();
    const summary = String(d.summary || '').trim();
    if (!title || !summary) continue;
    seen.add(idx);
    const it = material[idx - 1];
    out.push({
      cat: it.cat, title: title.slice(0, 120), summary: summary.slice(0, 200),
      source: it.source, link: it.link, ts: it.ts
    });
    if (out.length >= DIGEST_MAX) break; // 模型不守约时硬截断
  }
  return out;
}

// ---------------- 薄摘要补全文 ----------------
// 两条通道：
//   1. 有 JINA_KEY → Jina Reader（快，免 GPU）
//   2. 否则 → Cloudflare 原生 Workers AI toMarkdown（复用 [ai] 绑定，零额外配置）
// 只补 desc 短的（EDGAR 条款名、官方短通稿），充分的条目不碰；失败静默回退原摘要。

async function enrichMaterial(material, net) {
  const thin = material.filter(it => (!it.desc || it.desc.length < 120) && /^https?:\/\//.test(it.link || ''));
  if (!thin.length) return material;
  const targets = thin.slice(0, ENRICH_MAX); // ⚠️ 上限 4 条：队列化后 digest 班 19/50，余量充足
  for (const it of targets) { // 串行：峰值内存可控（并行大页面易触发 Worker 资源上限 1102）
    try {
      const md = net.env && net.env.JINA_KEY ? await fetchViaJina(it.link, net) : await fetchViaToMarkdown(it.link, net);
      if (md && md.length > (it.desc || '').length + 40) it.desc = md.slice(0, 600); // 确实更丰富才采用
    } catch (e) { /* 静默：保留原摘要 */ }
  }
  return material;
}

async function fetchViaJina(url, net) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 8000);
  try {
    const r = await net.fetch('https://r.jina.ai/' + url, {
      signal: ac.signal, redirect: 'follow',
      headers: { 'Accept': 'text/plain', 'Authorization': 'Bearer ' + net.env.JINA_KEY }
    }, 'enrich:jina');
    if (!r.ok) return null;
    return cleanMd(await r.text()).slice(0, 600);
  } finally { clearTimeout(timer); }
}

// Cloudflare 原生方案（参照 doocs/anything-md）：抓 HTML → env.AI.toMarkdown → 纯文本
// 无需任何 key，走已有 AI 绑定；含整调用超时兜底（GPU 冷启动可能很慢）
async function fetchViaToMarkdown(url, net) {
  if (!net.env?.AI?.toMarkdown) return null;
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 10000);
  let html;
  try {
    const r = await net.fetch(url, {
      signal: ac.signal, redirect: 'follow',
      headers: { 'User-Agent': UA_CHROME, 'Accept': 'text/html,application/xhtml+xml,*/*' }
    }, 'enrich:page');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const ct = r.headers.get('content-type') || '';
    if (!/text\/html|application\/xhtml/i.test(ct)) throw new Error('CT=' + ct.slice(0, 40));
    html = await r.text();
  } finally { clearTimeout(timer); }
  if (!html || html.length < 200) throw new Error('html ' + (html ? html.length : 0) + 'B');
  if (html.length > HTML_MAX) html = html.slice(0, HTML_MAX); // 截断：防止超大页面吃爆内存
  const call = net.aiToMarkdown([{ name: 'page.html', blob: new Blob([html], { type: 'text/html' }) }], 'enrich:md');
  const timeout = new Promise(res => setTimeout(() => res(null), 90000)); // 冷启动容忍
  const results = await Promise.race([call, timeout]);
  const res = results && results[0];
  if (!res) throw new Error('toMarkdown 超时');
  if (res.format === 'error') throw new Error('toMarkdown: ' + String(res.error).slice(0, 80));
  let md = String(res.data || '');
  md = md.replace(/^---\n[\s\S]*?\n---\n*/, ''); // 去 YAML frontmatter
  return cleanMd(md).slice(0, 600);
}

// ---------------- 提示词 ----------------

/**
 * 构建精选提示词。
 * 开头的 <arg_key:opensource> 是 Qwen3 的「软开关」：关闭思考模式。
 * ⚠️ 历史坑：不关的话，推理模型会把 max_tokens 全耗在 reasoning 上，
 *    返回空 content（曾导致精选恒为 0 条）。
 * 结尾 /no_think 是双重保险。
 */
export function buildPrompt(material) {
  const list = material.map((it, i) =>
    `${i + 1}. [${it.cat}] ${it.title}（来源：${it.source}）${it.desc ? ' 摘要：' + it.desc.slice(0, 100) : ''}`
  ).join('\n');
  return `<arg_key:opensource>
你是「GUJI AI 情报日报」的主编。下面是今天从各官方一手来源抓到的条目（编号、分类、标题、来源、摘要片段）。
请分两步工作，不要跳步：
第一步【整理精选】：通读全部条目，挑出真正重要、有实质信息量的（如新模型/新产品/重大财报与订单/重要论文/深度观点），总量严格不超过 ${DIGEST_MAX} 条；跳过营销通稿、人事任命、无信息量的条目。
第二步【总结翻译】：只对精选出的条目，为每条写一个中文标题（25 字以内）和一句中文摘要（50 字以内）。必须基于原文信息概括，不得编造原文没有的内容，数字与名称保留原文。
输出严格的 JSON 数组（不要 markdown 代码块标记，不要任何其他文字），每项格式：
{"idx": 条目编号, "title": "中文标题", "summary": "中文摘要"}

今日条目：
${list}

/no_think`;
}

// 智谱 GLM（OpenAI 兼容 v4 接口）。选 glm-4-plus 的原因：
//   - 中文原生，摘要质量稳定高于 llama-3.1-8b
//   - 无思考模式（glm-4.5/4.6 是思考模型，会重蹈 Qwen「空 content」覆辙）
//   - 1-3 秒级响应，无 GPU 冷启动
// ZHIPU_KEY 不存在时整条链路自动跳过，行为退回纯 Cloudflare 模型。
export async function fetchViaZhipu(prompt, net) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 60000);
  try {
    const r = await net.fetch('https://open.bigmodel.cn/api/paas/v4/chat/completions', {
    method: 'POST', signal: ac.signal,
    headers: { 'Authorization': 'Bearer ' + net.env.ZHIPU_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'glm-4-plus',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 1536, temperature: 0.3
      })
    }, 'zhipu:glm-4-plus');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json().catch(() => null);
    const text = j && j.choices && j.choices[0] && j.choices[0].message
      ? String(j.choices[0].message.content || '') : '';
    if (!text.trim()) throw new Error('空 content');
    return text;
  } finally { clearTimeout(timer); }
}

// ---------------- 模型链 ----------------

/**
 * 构造模型链。声明式配置，attempts 之和直接对应 §6 budget 里的 digest 配额（4）。
 *
 * ⚠️ v3 时代「留给 AI 的只有 3 次」的约束随队列化解除（digest 班 19/50）；
 *    兜底模型 llama 给 2 次尝试，直接针对 05:50 那类瞬时失败。
 *
 * @param {object} net
 * @param {string} prompt
 */
export function buildChain(net, prompt) {
  // chat_template_kwargs.enable_thinking=false 是 Qwen3 关思考的「正开关」（GLM 侧忽略此参数）
  const cfPayload = {
    messages: [{ role: 'user', content: prompt }], max_tokens: 3072, temperature: 0.3,
    chat_template_kwargs: { enable_thinking: false },
  };
  const hasZhipu = !!(net.env && net.env.ZHIPU_KEY);
  return hasZhipu
    ? [
        { name: 'zhipu:glm-4-plus', attempts: 2, call: () => fetchViaZhipu(prompt, net) },
        { name: AI_MODELS[1], attempts: 2, call: async () => aiText(await runAI(net, AI_MODELS[1], cfPayload)) },
      ]
    : [
        { name: AI_MODELS[0], attempts: 2, call: async () => aiText(await runAI(net, AI_MODELS[0], cfPayload)) },
        { name: AI_MODELS[1], attempts: 2, call: async () => aiText(await runAI(net, AI_MODELS[1], cfPayload)) },
      ];
}

/**
 * 生成每日精选。
 * @param {object[]} items 已排序的条目池
 * @param {object} net     createNet() 出口
 * @returns {Promise<{digest: object[]|null, trace: object[]}>}
 */
export async function makeDigest(items, net) {
  // 注意：不再因「AI 绑定缺失」早退——有 ZHIPU_KEY 时纯走智谱链路，
  // 无 key 也无绑定时由 buildChain 自然失败并记入 trace（队列重试兜底）。
  if (!items.length) return { digest: null, trace: [] };
  const trace = []; // 逐模型诊断：供 /api/diag 排查「精选 0 条」
  const material = items.slice(0, MATERIAL_MAX);
  await enrichMaterial(material, net); // 薄摘要先补全文（Jina key 或原生 toMarkdown），模型看得更深
  const prompt = buildPrompt(material);
  const chain = buildChain(net, prompt);
  for (const m of chain) {
    for (let attempt = 1; attempt <= m.attempts; attempt++) {
      const t0 = Date.now();
      try {
        const text = await m.call();
        const digest = parseDigest(text, material);
        trace.push({ model: m.name, attempt, ms: Date.now() - t0, textLen: text.length, digest: digest.length, head: text.slice(0, 120) });
        if (digest.length) {
          console.error(`digest: ${m.name} → ${digest.length} 条精选`);
          return { digest, trace };
        }
        if (text.trim()) break; // 有输出但解析不出：换模型更有希望
        if (attempt < m.attempts) await new Promise(r => setTimeout(r, 2000));
      } catch (e) {
        trace.push({ model: m.name, attempt, ms: Date.now() - t0, err: String(e && e.message).slice(0, 200) });
        console.error(`digest: ${m.name} 失败：${e && e.message}`);
        if (attempt < m.attempts) await new Promise(r => setTimeout(r, 2000));
      }
    }
  }
  return { digest: null, trace };
}

// ============================================================
// §11 telegram · 推送
// ============================================================
// 顺序分片推送：cron 入队 SEND_SLICES 条消息，消费者 max_concurrency=1 串行
// 执行，片内 300ms 限速；单片 ≤9 条消息 × 至多 3 次尝试 = ≤27 个子请求，
// 即使 Telegram 抽风也不越 50 上限（v3 单 invocation 26 条无重试余量的问题解除）。
// 单条消息天然很短，不会像合并大段文本那样超出 4096 字节被整条拒绝。

const CAT_ICON = new Map(CAT_ORDER);

/** 报头：首条消息用（日期 + 班次 + 条数概览） */
export function renderTgHeader(data) {
  const d = bjDate(data.generatedAt || Date.now());
  const p = n => String(n).padStart(2, '0');
  const edition = d.getUTCHours() < 12 ? '☀️ 早报' : '🌆 晚报'; // 6:00 早班 / 22:00 晚班
  const n = (data.digest || []).length;
  return `🤖 GUJI AI 情报 · ${edition} · ${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}\n今日抓取 ${data.items.length} 条，精选 ${n} 条`;
}

/** 单条精选 = 一条消息：短、可读、各自带链接与来源 */
export function renderTgItem(it, idx, total) {
  const icon = CAT_ICON.get(it.cat) || '🔧';
  const when = it.ts ? bjTime(it.ts) : '近期';
  return `${icon} ${it.title}\n\n${it.summary}\n\n${it.source} · ${when} · ${total > 1 ? `(${idx}/${total}) ` : ''}${it.link}`;
}

/**
 * 生成待推送的消息列表。
 * 有精选 → 报头 + N 条精选；无精选（AI 不可用）→ 退回英文标题清单，合并为一条。
 */
export function buildTgMessages(data) {
  const { items, digest } = data;
  const out = [];
  if (digest && digest.length) {
    out.push(`${renderTgHeader(data)}\n\n🧾 今日精选 · 中文总结，共 ${digest.length} 条`);
    for (const [i, it] of digest.entries()) out.push(renderTgItem(it, i + 1, digest.length));
    return out;
  }
  // AI 不可用：退回英文标题清单（合并为一条）
  let msg = renderTgHeader(data) + '\n';
  for (const [cat, icon] of CAT_ORDER) {
    const list = items.filter(i => i.cat === cat).slice(0, 4);
    if (!list.length) continue;
    msg += `\n${icon} ${cat}\n`;
    for (const it of list) msg += `· ${it.title}\n  ${it.link}\n`;
  }
  out.push(msg);
  return out;
}

/**
 * 把「目标 × 消息」拍平成有序对列表（推送分片按这个顺序切）。
 * 目标顺序：TG_CHAT_ID（私聊）→ TG_CHANNEL（订阅频道）。
 */
export function buildPushPairs(env, data) {
  const targets = [env.TG_CHAT_ID, env.TG_CHANNEL].filter(Boolean);
  const msgs = buildTgMessages(data);
  const pairs = [];
  for (const chat of targets) {
    for (const [i, text] of msgs.entries()) pairs.push({ chat, i, text });
  }
  return pairs;
}

/** 消息间隔（同一聊天限速 ~1 条/秒，13 条连发会触发 429） */
const MSG_GAP_MS = 300;

/** 单条消息重试间隔（TG 瞬时 429/5xx） */
const MSG_RETRY_MS = 1000;

/** 单条消息最多尝试次数 */
const MSG_MAX_ATTEMPTS = 3;

/**
 * 发送一个推送分片（串行 + 片内限速 + 单条至多 3 次尝试）。
 * @param {object} net   createNet() 出口
 * @param {Array}  chunk buildPushPairs() 的有序子集
 * @returns {Promise<object[]>} 每条消息的推送结果
 */
async function sendChunk(net, chunk) {
  const token = net.env.TG_BOT_TOKEN;
  const results = [];
  for (const [j, p] of chunk.entries()) {
    if (j > 0) await sleep(MSG_GAP_MS);
    let ok = false, err = null, msgId = null;
    for (let attempt = 1; attempt <= MSG_MAX_ATTEMPTS && !ok; attempt++) {
      try {
        const r = await net.fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: p.chat, text: tgTruncate(p.text), disable_web_page_preview: p.i === 0 })
        }, `tg:${p.chat}#${p.i}`);
        const j2 = await r.json().catch(() => ({ ok: false, description: '响应非 JSON' }));
        if (j2.ok) { ok = true; msgId = j2.result && j2.result.message_id; }
        else {
          err = j2.description || ('HTTP ' + r.status);
          if (attempt < MSG_MAX_ATTEMPTS) await sleep(MSG_RETRY_MS);
        }
      } catch (e) {
        err = String(e && e.message || e);
        if (attempt < MSG_MAX_ATTEMPTS) await sleep(MSG_RETRY_MS);
      }
    }
    results.push({ chat: p.chat, i: p.i, ok, msgId, err: ok ? null : err });
    if (!ok) console.error(`TG 推送失败 (${p.chat} 第${p.i}条): ${err}`);
  }
  return results;
}

/**
 * 全量推送（测试/调试用；生产路径走 §13 的分片消费者）。
 * @returns {Promise<object[]>} 每条消息的推送结果
 */
export async function pushTelegram(net, data) {
  if (!net.env.TG_BOT_TOKEN) return [];
  const pairs = buildPushPairs(net.env, data);
  if (!pairs.length) return [];
  const results = [];
  const K = Math.ceil(pairs.length / SEND_SLICES);
  for (let s = 0; s < SEND_SLICES; s++) {
    results.push(...await sendChunk(net, pairs.slice(s * K, (s + 1) * K)));
  }
  return results;
}

// ============================================================
// §12 render · 网页渲染
// ============================================================
// 移动端优先的单页 HTML。纯函数（输出只依赖入参 data 与 FEEDS/CAT_ORDER）。

/** 每个分类在网页上最多展示条数（访谈观点更新慢，多给一些） */
const CAT_SHOW_LIMIT = (cat) => (cat === '访谈观点' ? 14 : 10);

export function renderPage(data) {
  const { items, failures, generatedAt, digest } = data;
  const total = items.length;
  // 今日精选 · 中文总结（先精选后总结的产物；AI 不可用时自动隐藏）
  let digestBlock = '';
  if (digest && digest.length) {
    const rows = digest.map(d => `<a class="item" href="${esc(d.link)}" target="_blank" rel="noopener">
      <div class="t">${esc(d.title)}</div>
      <div class="m"><span class="src">${esc(d.source)}</span><span class="time">${bjTime(d.ts)}</span></div>
      <div class="desc">${esc(d.summary)}</div>
    </a>`).join('');
    digestBlock = `<section class="digest"><h2>🧾 今日精选 · 中文总结<span class="cnt">${digest.length}</span></h2>${rows}</section>`;
  }
  const catBlocks = [];
  for (const [cat, icon] of CAT_ORDER) {
    const list = items.filter(i => i.cat === cat);
    if (!list.length) continue;
    const limit = CAT_SHOW_LIMIT(cat);
    const shown = list.slice(0, limit);
    const rows = shown.map(it => {
      const ago = it.ts ? bjTime(it.ts) : '近期';
      const multi = it.also && it.also.length ? `<span class="multi">×${it.also.length + 1} 源</span>` : '';
      const desc = it.desc ? `<div class="desc">${esc(it.desc.slice(0, 220))}${it.desc.length > 220 ? '…' : ''}</div>` : '';
      return `<a class="item" href="${esc(it.link)}" target="_blank" rel="noopener">
      <div class="t">${esc(it.title)}${multi}</div>
      <div class="m"><span class="src">${esc(it.source)}</span><span class="time">${ago}</span></div>
      ${desc}
    </a>`;
    }).join('');
    const more = list.length > shown.length ? `<div class="more">…还有 ${list.length - shown.length} 条</div>` : '';
    catBlocks.push(`<section><h2>${icon} ${cat}<span class="cnt">${list.length}</span></h2>${rows}${more}</section>`);
  }
  const failHtml = failures && failures.length
    ? `<details class="fails"><summary>⚠️ ${failures.length} 个源本次抓取失败</summary>${failures.map(f => `<div>${esc(f.name)}：${esc(f.err)}</div>`).join('')}</details>` : '';
  const srcList = FEEDS.map(f => f.name).join(' · ');
  return `<!DOCTYPE html>
<html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0b0d12">
<title>GUJI · AI 情报</title>
<style>
:root{--bg:#0b0d12;--card:#141822;--line:#232a3a;--txt:#e8ecf3;--sub:#8b94a7;--acc:#5b8cff;--multi:#f5a623}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Noto Sans SC",sans-serif;line-height:1.5;padding:14px 14px 40px;max-width:760px;margin:0 auto}
header{padding:10px 2px 16px;border-bottom:1px solid var(--line);margin-bottom:14px}
h1{font-size:20px;letter-spacing:.5px}
h1 .ai{color:var(--acc)}
.meta{color:var(--sub);font-size:12px;margin-top:6px}
section{margin-bottom:22px}
h2{font-size:15px;color:var(--sub);font-weight:600;margin-bottom:8px;display:flex;align-items:center;gap:6px}
.cnt{background:var(--line);border-radius:10px;padding:0 8px;font-size:11px;color:var(--txt)}
.item{display:block;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:11px 13px;margin-bottom:8px;text-decoration:none;color:var(--txt)}
.item:active{opacity:.75}
.t{font-size:14.5px;font-weight:550}
.m{display:flex;gap:10px;margin-top:5px;font-size:11.5px;color:var(--sub)}
.src{color:var(--acc)}
.desc{font-size:12.5px;color:var(--sub);margin-top:6px}
.multi{color:var(--multi);font-size:11px;margin-left:6px}
.more{color:var(--sub);font-size:12px;padding:4px 2px}
.digest .item{border-color:#2b3a55;background:#141a26}
.digest .t{color:#fff}
.fails{margin:18px 0 8px;color:var(--sub);font-size:12px}
.fails summary{cursor:pointer}
.fails div{padding:2px 0}
footer{margin-top:26px;color:#565f73;font-size:10.5px;line-height:1.7;border-top:1px solid var(--line);padding-top:12px}
.refresh{background:none;border:1px solid var(--line);color:var(--sub);border-radius:8px;font-size:11px;padding:3px 10px;margin-left:8px;cursor:pointer}
.tg-btn{display:inline-flex;align-items:center;gap:6px;margin-top:10px;background:#2AABEE;color:#fff;text-decoration:none;font-size:13px;font-weight:600;padding:8px 16px;border-radius:10px}
.tg-btn:active{opacity:.8}
@media(prefers-color-scheme:light){:root{--bg:#f6f7f9;--card:#fff;--line:#e3e6ec;--txt:#1a1d26;--sub:#6b7280;--acc:#2f6bff}}
</style></head><body>
<header>
  <h1>🤖 GUJI <span class="ai">· AI 情报</span><button class="refresh" onclick="location.reload()">刷新</button></h1>
  <div class="meta">${total} 条 · ${new Set(items.map(i=>i.source)).size} 个一手源 · 更新于 ${bjTime(generatedAt)}（北京时间）</div>
  <a class="tg-btn" href="https://t.me/guji_ai_daily" target="_blank" rel="noopener">📢 订阅 Telegram 频道 · 每日早晚推送</a>
</header>
${digestBlock}
${catBlocks.join('') || '<div class="meta">今日暂无新内容，10 秒后自动重试…</div><script>setTimeout(()=>location.reload(),10000)</script>'}
${failHtml}
<footer>来源全部为官方发布 / SEC EDGAR 官方 filing / 原创工程媒体 / 一手访谈播客，无二手聚合。<br>${esc(srcList)}</footer>
</body></html>`;
}

// ============================================================
// §13 queue · 队列消费者
// ============================================================
// 每个消费者调用：独立 budget 计数器 → 干活 → 落运行记录 → ack。
// 抛错 = 消息不 ack = 按 max_retries 退避重试，耗尽进 DLQ（§13.4 记日志）。
// 这就是 v4 对「瞬时失败」的结构性修复：05:50 那类模型/网络抖动
// 不再需要手写重试循环去兜，队列本身就是重试器。

// ---------------- 13.1 抓取分片消费者 ----------------

/**
 * guji-fetch 消费者：每条消息 = 一个分片（10 源）。
 * 子请求账：10 fetch + Anthropic seen 2（仅含 anthropic 的分片）+ 分片落库 1 ≈ 13/50。
 * 分片键带 window 且内容整体覆盖写，重试幂等；分片必然落库（哪怕全失败），
 * digest 消费者以「分片存在」判断抓取完成，不会被单个源的失败卡住。
 *
 * ⚠️ 诊断埋点（2026-10-25 追加）：曾出现「3/4 分片消费者无痕迹消失」
 *    （run:fetch 记录不更新、分片键不出现），因早期 throw 路径全部静默。
 *    现在每个阶段都写 run:fetch 记录（start → fetched → ok:true），
 *    失败路径写 ok:false + stage + err，重试前也能从 KV 看到死在哪一步。
 *    代价：+2 次 KV 写/分片（峰值 16/50，仍远低于上限）。
 */
async function consumeFetch(batch, env) {
  for (const msg of batch.messages) {
    const budget = createBudget();
    const net = createNet(env, budget);
    const body = msg.body;
    const { window, shard, feeds } = body || {};
    // 阶段记录：失败也留痕（ok:false + stage + err），供 KV 事后诊断
    const mark = (stage, extra = {}) => net.kvPut(runKey(window, `fetch:${shard}`), JSON.stringify({
      at: Date.now(), shard, stage, budget: budget.report(), ...extra
    }), { expirationTtl: 2 * 86400 }, `run:fetch:${stage}`).catch(() => {});
    try {
      if (!Array.isArray(feeds) || !feeds.length) {
        await mark('empty-feeds', { ok: false, bodyType: typeof body, bodyKeys: body && typeof body === 'object' ? Object.keys(body) : null });
        msg.ack();
        continue;
      }
      await mark('start', { ok: false, feeds: feeds.length });
      const failures = [];
      const items = await fetchShardFeeds(feeds, net, failures);
      await mark('fetched', { ok: false, items: items.length, failures: failures.length });
      // 写分片（2 天 TTL：覆盖下一班同 window 键前的保洁，异常键自动清扫）
      await net.kvPut(shardKey(window, shard), JSON.stringify({ items, failures, at: Date.now() }),
        { expirationTtl: 2 * 86400 }, `shard:${shard}`);
      await net.kvPut(runKey(window, `fetch:${shard}`), JSON.stringify({
        at: Date.now(), shard, ok: true, items: items.length, failures: failures.length, budget: budget.report()
      }), { expirationTtl: 2 * 86400 }, 'run:fetch').catch(() => {});
      msg.ack();
      console.error(`[fetch] ${window} 分片${shard}：${items.length} 条，失败 ${failures.length} 个源，预算 ${budget.total}/${budget.limit}`);
    } catch (e) {
      await mark('error', { ok: false, err: String(e && e.message || e).slice(0, 300), errName: e && e.name });
      throw e;
    }
  }
}

// ---------------- 13.2 精选消费者 ----------------

/**
 * guji-digest 消费者：读全部分片 → 合并后处理 → 早晚班 cutoff → enrich → AI 精选 → 落库。
 * 子请求账：4 分片读 + latest 1 + 增强 4×2 + 模型链 4 + 落库 2 ≈ 19/50。
 *
 * 关键语义：
 *   - 分片未就绪 → 调用内轮询等待（≤45s，预算护栏 40），仍缺则 throw（队列重试，
 *     每次重试都有全新预算与 45s 等待——生产者绑定另有 90s deliveryDelay 双保险）
 *   - items 为空 → 不碰 latest（保留上一班数据，页面不空屏），throw 重试
 *   - 精选彻底失败 → 仍落库（digest=[]，推送走英文清单兜底）→ 记 run → throw 重试
 */
async function consumeDigest(batch, env) {
  const budget = createBudget();
  const net = createNet(env, budget);
  const t0 = Date.now();
  const logs = [];
  const log = m => {
    const s = `${((Date.now() - t0) / 1000).toFixed(1)}s ${m}`;
    logs.push(s);
    console.error('[digest] ' + s);
  };
  const msg = batch.messages[0];
  const { window, full = false } = msg.body || {};
  log(`开始 window=${window} full=${full}`);

  // 1. 等分片就绪（生产者绑定配了 90s deliveryDelay，通常首轮即就绪；
  //    此处轮询是双保险：抓取异常慢时在调用内等，不烧队列重试次数）
  let shards = null;
  const deadline = Date.now() + SHARD_WAIT_MS;
  const markDigest = (extra) => net.kvPut(runKey(window, 'digest'), JSON.stringify({
    at: Date.now(), ok: false, budget: budget.report(), ...extra
  }), { expirationTtl: 2 * 86400 }, 'run:digest:err').catch(() => {});
  while (true) {
    const attempt = [];
    let ready = true;
    for (let n = 0; n < SHARD_COUNT; n++) {
      const s = await net.kvGet(shardKey(window, n), 'json', `shard:${n}`).catch(() => null);
      if (!s || !Array.isArray(s.items)) { ready = false; break; }
      attempt.push(s);
    }
    if (ready) { shards = attempt; break; }
    if (Date.now() >= deadline) {
      await markDigest({ stage: 'shard-wait-timeout', waitedMs: SHARD_WAIT_MS });
      throw new Error(`分片未就绪（等待 ${SHARD_WAIT_MS / 1000}s 仍缺失，window=${window}，抓取班失败？）`);
    }
    // 预算护栏：轮询消耗超过 40 就中止，交给队列重试（每次重试都有全新 50 预算，
    // 10 次重试 × 45s ≈ 7.5 分钟总覆盖，且任何单次调用都不越限）
    if (budget.total > 40) {
      await markDigest({ stage: 'shard-wait-budget', total: budget.total });
      throw new Error(`等分片期间预算已达 ${budget.total}/50，中止本轮等待（队列将重试）`);
    }
    log(`分片未就绪，${SHARD_POLL_MS / 1000}s 后重试…`);
    await sleep(SHARD_POLL_MS);
  }
  const rawItems = shards.flatMap(s => s.items);
  const failures = shards.flatMap(s => s.failures || []);
  log(`分片就绪：${rawItems.length} 条原始条目，${failures.length} 个源失败`);

  // 2. 合并后处理（纯函数，语义同 v3 collect 后半段）
  const items = finalizeItems(rawItems);
  log(`后处理完成：${items.length} 条`);
  if (!items.length) {
    await markDigest({ stage: 'no-items', raw: rawItems.length });
    throw new Error('本班无有效条目（全被时间窗/关键词过滤？）');
  }

  // 3. 早晚班机制：晚报只精选「上次成功精选以来」的新条目，避免与早报重复播报；
  //    一条新增也够起一期；完全没有新增才回退全窗口（宁薄勿空）
  let nowData = null;
  try { nowData = await net.kvGet(KV_KEY, 'json', 'state:latest'); } catch (e) {}
  const lastAt = (!full && nowData) ? Number(nowData.lastDigestAt) || 0 : 0;
  const fresh = lastAt ? items.filter(it => isFresh(it.ts, lastAt)) : [];
  const pool = fresh.length >= 1 ? fresh : items;
  log(`精选素材 ${pool.length} 条（新增 ${fresh.length} 条）`);

  // 4. AI 精选（enrich + 模型链）
  let digest = null, digestErr = null, aiTrace = null;
  try {
    const r = await makeDigest(pool, net);
    digest = r && r.digest;
    aiTrace = r && r.trace;
  } catch (e) { digestErr = String(e && e.message || e).slice(0, 200); }
  log(`精选完成：${digest ? digest.length : 0} 条${digestErr ? ' / 错误：' + digestErr : ''}`);

  // 5. 落库（items 总是保存；精选失败也保存，推送走英文清单兜底）
  const nowSecValue = nowSec();
  const data = {
    items, failures, generatedAt: Date.now(), digest: digest || [],
    lastDigestAt: (digest && digest.length) ? nowSecValue : (nowData ? nowData.lastDigestAt || 0 : 0),
    window, logs, digestErr, aiTrace,
  };
  // ⚠️ 快照必须先于 KV.put 挂载，否则 KV 副本永远没有 budget 字段（v3 遗留坑）。
  //    此快照不含随后的 2 次 put（latest + run），权威完整账以 run:{window}:digest 为准。
  data.budget = budget.report();
  await net.kvPut(KV_KEY, JSON.stringify(data), { expirationTtl: 14 * 86400 }, 'state:save');

  // 6. 精选彻底失败 → 记 run + throw（消息重试；items 已落库，页面与推送有兜底）
  if (!digest || !digest.length) {
    await net.kvPut(runKey(window, 'digest'), JSON.stringify({
      at: Date.now(), ok: false, digestErr, budget: budget.report()
    }), { expirationTtl: 2 * 86400 }, 'run:digest').catch(() => {});
    throw new Error('精选失败：' + (digestErr || '模型链全部失败'));
  }
  await net.kvPut(runKey(window, 'digest'), JSON.stringify({
    at: Date.now(), ok: true, digest: digest.length, budget: budget.report() // put 之后的完整账
  }), { expirationTtl: 2 * 86400 }, 'run:digest').catch(() => {});
  msg.ack();
  log(`完成，预算 ${budget.total}/${budget.limit}`);
}

// ---------------- 13.3 推送分片消费者 ----------------

/**
 * guji-send 消费者：每条消息 = 一个顺序分片（max_concurrency=1 串行执行保序）。
 * 子请求账：latest 1 + ≤9 条消息×至多 3 次尝试 + 统计 1 ≈ 29/50（最坏）。
 *
 * 幂等：slice 0 完成后写 run:{window}:push，/api/push 路由据此防重复入队；
 * 数据新鲜度闸门继承 v3（3 小时），force 可 bypass（手动补推旧内容）。
 */
async function consumeSend(batch, env) {
  const budget = createBudget();
  const net = createNet(env, budget);
  const msg = batch.messages[0];
  const { window, slice = 0, slices = SEND_SLICES, force = false } = msg.body || {};

  const data = await net.kvGet(KV_KEY, 'json', 'state:latest').catch(() => null);
  if (!data) throw new Error('KV 无数据（精选班可能未完成，重试）');

  // 新鲜度闸门：抓取/精选班失败时不推 3 小时前的旧内容
  const age = data.generatedAt ? Date.now() - data.generatedAt : Infinity;
  if (!force && age >= 3 * 3600 * 1000) {
    console.error(`[send] 跳过：数据已过期 ${(age / 3600000).toFixed(1)} 小时（抓取班可能失败）`);
    msg.ack();
    return;
  }
  if (data.window && window && data.window !== window && !force) {
    console.error(`[send] 跳过：latest 属于 ${data.window}，本班是 ${window}`);
    msg.ack();
    return;
  }

  const pairs = buildPushPairs(env, data);
  if (!pairs.length) {
    console.error('[send] 跳过：无推送目标（TG_BOT_TOKEN/TG_CHAT_ID/TG_CHANNEL 未配置）');
    msg.ack();
    return;
  }
  const K = Math.ceil(pairs.length / slices);
  const chunk = pairs.slice(slice * K, (slice + 1) * K);
  if (!chunk.length) { msg.ack(); return; }

  const results = await sendChunk(net, chunk);
  const okCount = results.filter(r => r.ok).length;
  const record = { at: Date.now(), slice, ok: okCount, total: results.length, budget: budget.report() };
  await net.kvPut(runKey(window, `send:${slice}`), JSON.stringify(record),
    { expirationTtl: 2 * 86400 }, `run:send:${slice}`).catch(() => {});
  // slice 0 完成即写「本班已推」标记（路由幂等守卫与 diag 用）
  if (slice === 0) {
    await net.kvPut(runKey(window, 'push'), JSON.stringify({ at: Date.now(), ok: okCount, total: results.length }),
      { expirationTtl: 2 * 86400 }, 'run:push').catch(() => {});
  }
  msg.ack();
  console.error(`[send] ${window} 分片${slice}：${okCount}/${results.length} 成功，预算 ${budget.total}/${budget.limit}`);
}

// ---------------- 13.4 死信消费者 ----------------

/**
 * guji-dlq 消费者：重试耗尽的消息在此落地。只记日志（不静默丢失），
 * 排查入口：wrangler tail 搜 [dlq]，或 /api/diag 看对应 run 记录缺失。
 */
async function consumeDlq(batch, env) {
  for (const msg of batch.messages) {
    console.error(`[dlq] 死信（重试耗尽）：${JSON.stringify(msg.body).slice(0, 500)}`);
    msg.ack();
  }
}

// ============================================================
// §14 worker · 入口（cron 入队 + 路由）
// ============================================================

// ---------------- 诊断辅助 ----------------

/** 实时探测 AI 绑定是否可用（排查「精选 0 条」） */
async function probeAI(net) {
  try {
    const t0 = Date.now();
    const res = await net.aiRun(AI_MODELS[0], {
      messages: [{ role: 'user', content: '只回复两个字：正常' }], max_tokens: 64,
      chat_template_kwargs: { enable_thinking: false }
    }, 'diag:ai');
    return { ok: true, ms: Date.now() - t0, text: aiText(res).slice(0, 40) };
  } catch (e) {
    return { ok: false, err: String(e && e.message).slice(0, 200) };
  }
}

/** 探测智谱 GLM 是否可用（仅当配置了 ZHIPU_KEY） */
async function probeZhipu(net) {
  try {
    const t0 = Date.now();
    const text = await fetchViaZhipu('只回复两个字：正常', net);
    return { ok: true, ms: Date.now() - t0, text: text.slice(0, 40) };
  } catch (e) {
    return { ok: false, err: String(e && e.message).slice(0, 200) };
  }
}

const JSON_HEADERS = { 'content-type': 'application/json', 'access-control-allow-origin': '*' };

function json(body, pretty = false) {
  return new Response(JSON.stringify(body, null, pretty ? 1 : 0), { headers: JSON_HEADERS });
}

const LOADING_PAGE = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>GUJI</title><style>body{background:#0b0d12;color:#e8ecf3;font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}</style></head><body><div>正在生成今日情报（队列处理中，约 1 分钟）…<script>setTimeout(()=>location.reload(),10000)</script></div></body></html>`;

// ---------------- 入队助手 ----------------

/**
 * 入队抓取分片（cron :40 与 /api/build 共用）。
 * @param {object} env
 * @param {object} net   createNet() 出口（用于记账）
 * @param {string} window 班次标识
 */
async function enqueueFetch(env, net, window) {
  for (let n = 0; n < SHARD_COUNT; n++) {
    const feeds = FEEDS.slice(n * SHARD_SIZE, (n + 1) * SHARD_SIZE);
    await net.qSend(env.GUJI_FETCH_Q, { kind: 'fetch', window, shard: n, feeds }, `fetch:shard${n}`);
  }
}

/**
 * 入队一次「抓取 + 精选」（/api/build 手动全量用）。
 * @param {object} env
 * @param {object} net   createNet() 出口（用于记账）
 * @param {string} window 班次标识
 * @param {boolean} full   忽略 lastDigestAt 重新全量精选
 */
async function enqueueBuild(env, net, window, full) {
  await enqueueFetch(env, net, window);
  await net.qSend(env.GUJI_DIGEST_Q, { kind: 'digest', window, full }, 'digest');
}

/** 入队一次「推送」（3 个顺序分片） */
async function enqueueSend(env, net, window, force) {
  for (let s = 0; s < SEND_SLICES; s++) {
    await net.qSend(env.GUJI_SEND_Q, { kind: 'send', window, slice: s, slices: SEND_SLICES, force }, `send:slice${s}`);
  }
}

// ---------------- 入口 ----------------

export default {
  /**
   * cron：只入队，不干活。
   * 按北京时间的分钟数分流：40 抓取 / 50 精选 / 00 推送。
   * 每次触发 ≤4 个子请求（队列发送，保守计入）。
   */
  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      const budget = createBudget();
      const net = createNet(env, budget);
      try {
        const at = event.scheduledTime || Date.now();
        const window = currentWindow(at);
        const minute = bjDate(at).getUTCMinutes();
        if (minute === FETCH_MINUTE) {
          // 只入队抓取；精选由 :50 的 cron 单独入队（10 分钟缓冲给抓取消费者）
          await enqueueFetch(env, net, window);
          console.error(`[cron] ${window} 抓取入队：${SHARD_COUNT} 分片，预算 ${budget.total}`);
        } else if (minute === DIGEST_MINUTE) {
          await net.qSend(env.GUJI_DIGEST_Q, { kind: 'digest', window, full: false }, 'digest');
          console.error(`[cron] ${window} 精选入队，预算 ${budget.total}`);
        } else if (minute === SEND_MINUTE) {
          await enqueueSend(env, net, window, false);
          console.error(`[cron] ${window} 推送入队：${SEND_SLICES} 片，预算 ${budget.total}`);
        } else {
          console.error(`[cron] 未识别的分钟数 ${minute}（cron=${event.cron}），忽略`);
        }
      } catch (e) {
        console.error(`[cron] 入队失败：${e && e.message}`);
      }
    })());
  },

  /**
   * 队列消费者路由：按 batch.queue 分发。
   * 抛错 → 未 ack 消息按 max_retries 重试，耗尽进 DLQ。
   */
  async queue(batch, env, ctx) {
    const q = batch.queue;
    try {
      if (q === Q_FETCH) await consumeFetch(batch, env);
      else if (q === Q_DIGEST) await consumeDigest(batch, env);
      else if (q === Q_SEND) await consumeSend(batch, env);
      else if (q === Q_DLQ) await consumeDlq(batch, env);
      else for (const m of batch.messages) m.ack(); // 未知队列：确认消费，不阻塞
    } catch (e) {
      console.error(`[queue] ${q} 消费失败（将按 max_retries 重试，耗尽进 DLQ）：${e && e.message}`);
      throw e;
    }
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 抓取班次：入队抓取分片 + 精选（异步处理，轮询 /api/diag 看结果）
    if (url.pathname === '/api/build') {
      const full = url.searchParams.get('full') === '1';
      const budget = createBudget();
      const net = createNet(env, budget);
      const window = currentWindow(Date.now());
      await enqueueBuild(env, net, window, full);
      return json({ ok: true, window, enqueued: { fetch: SHARD_COUNT, digest: 1 }, budget: budget.report() });
    }

    // 推送班次：入队 3 个顺序分片（幂等：本班 10 分钟内推过则跳过，?force=1 强制）
    if (url.pathname === '/api/push') {
      const force = url.searchParams.get('force') === '1';
      const budget = createBudget();
      const net = createNet(env, budget);
      const window = currentWindow(Date.now());
      if (!force) {
        let pushed = null;
        try { pushed = await net.kvGet(runKey(window, 'push'), 'json', 'guard:push'); } catch (e) {}
        if (pushed && Date.now() - pushed.at < 10 * 60 * 1000) {
          return json({ ok: true, skipped: `本班（${window}）已于 ${bjTime(pushed.at)} 推送过，加 ?force=1 可重推` });
        }
      }
      await enqueueSend(env, net, window, force);
      return json({ ok: true, window, enqueued: { send: SEND_SLICES }, force });
    }

    // 诊断：latest 状态 + 分片就绪情况 + 实时探测 AI 配额（排查「精选 0 条」）
    if (url.pathname === '/api/diag') {
      const budget = createBudget();
      const net = createNet(env, budget);
      let data = null;
      try { data = await net.kvGet(KV_KEY, 'json', 'state:latest'); } catch (e) {}
      const win = (data && data.window) || currentWindow(Date.now());
      const shards = [];
      for (let n = 0; n < SHARD_COUNT; n++) {
        let s = null;
        try { s = await net.kvGet(shardKey(win, n), 'json', `diag:shard:${n}`); } catch (e) {}
        shards.push(s ? { shard: n, items: s.items.length, failures: (s.failures || []).length, at: s.at } : null);
      }
      const aiProbe = await probeAI(net);
      const zhipuProbe = net.env.ZHIPU_KEY ? await probeZhipu(net) : null;
      return json({
        kvFound: !!data, window: win,
        generatedAt: data ? data.generatedAt : null,
        bjTime: data ? bjTime(data.generatedAt) : null,
        items: data ? (data.items || []).length : 0,
        digest: data ? (data.digest || []).length : 0,
        lastDigestAt: data ? data.lastDigestAt : null,
        digestErr: data ? data.digestErr : null,
        logs: data ? data.logs : null,
        aiTrace: data ? data.aiTrace : null,
        budget: data ? data.budget : null, // 上一班精选班的子请求明细
        failures: data ? (data.failures || []).map(f => f.name + ': ' + f.err) : null,
        shards, aiProbe, zhipuProbe
      }, true);
    }

    let data = null;
    try {
      data = await createNet(env, createBudget()).kvGet(KV_KEY, 'json', 'state:latest');
    } catch (e) {}
    if (!data) {
      // 首次打开：后台入队抓取 + 精选，10 秒后刷
      if (ctx?.waitUntil) {
        const budget = createBudget();
        ctx.waitUntil(enqueueBuild(env, createNet(env, budget), currentWindow(Date.now()), false).catch(e => console.error('[prime] ' + (e && e.message))));
      }
      return new Response(LOADING_PAGE, { headers: { 'content-type': 'text/html; charset=utf-8' } });
    }
    return new Response(renderPage(data), {
      headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'public, max-age=120' }
    });
  }
};

// ============================================================
// §15 对外导出
// ------------------------------------------------------------
// 作用域说明：
//   - Cloudflare Workers 只认 default 导出（上方的 scheduled / queue / fetch）。
//   - 其余符号在此集中导出，供单测与线上调试引用；
//     行为相关的函数（renderPage/pushTelegram/…）已在各自分区就地 export，
//     这里只补各区常量与纯工具，不重复导出。
//   - 新增导出请按分区归类，保持「§N 注释 = 一个关注点」的可读性。
// ============================================================

export {
  // §1 config
  KV_KEY, SEEN_SET_KEY,
  Q_FETCH, Q_DIGEST, Q_SEND, Q_DLQ,
  SHARD_SIZE, SHARD_COUNT, SEND_SLICES, SHARD_WAIT_MS, SHARD_POLL_MS, shardKey, runKey,
  ENRICH_MAX, HTML_MAX, MAX_AGE,
  AI_KW, isAI, FEEDS,
  UA_CHROME, FETCH_MS, BATCH_MS, CONCURRENCY,
  AI_MODELS, AI_MS,
  FETCH_MINUTE, DIGEST_MINUTE, SEND_MINUTE, CAT_ORDER,
  // §2 text（esc/stripTags/normTitle/cleanMd 为纯函数；tgTruncate 已就地导出）
  esc, stripTags, normTitle, cleanMd,
  // §3 time（bjTime/currentWindow 已就地导出）
  toMs, toSec, nowSec, bjDate, isFresh,
  // §6 budget（SUBREQUEST_LIMIT / SUBREQUEST_WARN / BUDGET_PLAN / PLAN_PEAK 已就地导出）
  // §9 shard（fetchShardFeeds/finalizeItems 已就地导出）
  DC_KW, recat, SRC_LIMIT,
  // §10 digest
  MATERIAL_MAX,
  // §11 telegram
  CAT_ICON, MSG_GAP_MS, MSG_MAX_ATTEMPTS,
};
```

### 6.2 `wrangler.toml` —— 绑定 / 队列 / cron（80 行）

```toml
name = "guji"
main = "index.js"
compatibility_date = "2026-10-01"

# 复用现有 KV（guji-free-kv），不新建
[[kv_namespaces]]
binding = "GUJI_KV"
id = "ca49d41ac8fa4987a0f818cee47237bf"

# Workers AI：每日精选的中文总结（先精选后总结，不做全文翻译）
[ai]
binding = "AI"

# ---------------- 队列（2026-02-04 起免费套餐可用）----------------
# v4 架构：cron 只入队，消费者干活。每个消费者调用享有独立的 50 子请求预算，
# 彻底消除 v3「单次调用 47/50 走钢丝、余量为 0」的问题。
#   抓取分片队列：4 条消息（39 源 ÷ 10），消费者每次 1 条 ≈ 13/50
#   精选队列：1 条消息，消费者 ≈ 19/50
#   推送队列：3 条顺序分片消息，每片 ≈ 29/50（最坏，含单条 3 次重试）
#   死信队列：重试耗尽的消息在此落地，消费者负责记日志（不静默丢失）
[[queues.producers]]
binding = "GUJI_FETCH_Q"
queue = "guji-fetch"

# 精选队列生产者：90 秒延迟投递——给抓取消费者留出就位时间。
#   cron 路径：:50 入队 → :51:30 前后消费，距 :00 推送班仍有 8 分钟缓冲；
#   手动 /api/build 路径：抓取与精选同时入队，延迟保证分片先就绪（双保险，
#   消费者内还有 ≤45s 轮采等待 + 预算护栏，见 index.js §13.2）。
# ⚠️ 字段名是 snake_case delivery_delay（秒）。曾误写为 camelCase deliveryDelay，
#    wrangler  silently 忽略（部署日志有 Unexpected fields 警告），队列实际 delay=0。
[[queues.producers]]
binding = "GUJI_DIGEST_Q"
queue = "guji-digest"
delivery_delay = 90

[[queues.producers]]
binding = "GUJI_SEND_Q"
queue = "guji-send"

# 抓取消费者：每批 1 条消息（= 1 个分片），4 片可并行（分片键按 window+序号隔离，无共享写）
[[queues.consumers]]
queue = "guji-fetch"
max_batch_size = 1
max_concurrency = 4
max_retries = 3
dead_letter_queue = "guji-dlq"

# 精选消费者：单条串行；重试次数给足（退避正好充当「等抓取班完成」的等待）
[[queues.consumers]]
queue = "guji-digest"
max_batch_size = 1
max_concurrency = 1
max_retries = 10
dead_letter_queue = "guji-dlq"

# 推送消费者：单条串行（保序：报头→精选条目，私聊→频道）
[[queues.consumers]]
queue = "guji-send"
max_batch_size = 1
max_concurrency = 1
max_retries = 3
dead_letter_queue = "guji-dlq"

# 死信消费者：只记日志
[[queues.consumers]]
queue = "guji-dlq"
max_batch_size = 10

# 每天三班 × 两个时段（列表语法：40,50 = 每小时第 40、50 分各触发一次）：
#   40 分 → 抓取班：入队 4 条分片消息（消费者抓取 + 落分片 KV）
#   50 分 → 精选班：入队 1 条精选消息（消费者读分片 → enrich → AI → 落 latest）
#   00 分 → 推送班：入队 3 条推送消息（消费者读 latest → 顺序分片限速推送）
# UTC：21:40/21:50 = 北京 05:40/05:50 早班；22:00 = 北京 06:00 早报推送
#      13:40/13:50 = 北京 21:40/21:50 晚班；14:00 = 北京 22:00 晚报推送
[triggers]
crons = ["40,50 21 * * *", "0 22 * * *", "40,50 13 * * *", "0 14 * * *"]

# 可选：Telegram 推送（不配就只是网站）
# npx wrangler secret put TG_BOT_TOKEN
# npx wrangler secret put TG_CHAT_ID
```

### 6.3 `package.json`

```json
{
  "dependencies": {
    "fast-xml-parser": "^5.11.2"
  }
}
```

### 6.4 `smoke-v4.mjs` —— 冒烟测试（17 项：纯函数 + 记账 + 容错解析）

```js
// v4 冒烟测试：纯函数 + 记账 + 消费者逻辑（mock KV/队列）
import * as m from './index.js';

let pass = 0, fail = 0;
const ok = (cond, name) => { if (cond) { pass++; console.log('✓', name); } else { fail++; console.error('✗', name); } };

// ---- currentWindow ----
ok(m.currentWindow(Date.UTC(2026, 9, 25, 21, 40)) === '2026-10-26-am', 'UTC21:40 → 北京次晨 am 班');
ok(m.currentWindow(Date.UTC(2026, 9, 25, 14, 0)) === '2026-10-25-pm', 'UTC14:00 → 北京当晚 pm 班');
ok(m.currentWindow(Date.UTC(2026, 9, 25, 13, 40)) === '2026-10-25-pm', 'UTC13:40 → 北京 21:40 pm 班');

// ---- 分片与预算 ----
ok(m.SHARD_COUNT === 4, '39 源 → 4 个分片');
ok(m.PLAN_PEAK === 29 && m.PLAN_PEAK <= m.SUBREQUEST_LIMIT, `预算峰值 ${m.PLAN_PEAK}/50 ≤ 上限`);
ok(m.shardKey('2026-10-25-am', 2) === 'guji:v4:shard:2026-10-25-am:2', '分片键格式');
ok(m.runKey('2026-10-25-am', 'fetch:0') === 'guji:v4:run:2026-10-25-am:fetch:0', '运行记录键格式');

// ---- finalizeItems ----
const now = Date.now();
const items = [
  { title: 'GPT-6 发布', link: 'https://a.com', ts: now, desc: 'x'.repeat(200), source: 'OpenAI', srcId: 'openai', cat: '模型', kind: 'rss', win: '7d' },
  { title: 'GPT6发布', link: 'https://b.com', ts: now, desc: 'y'.repeat(200), source: 'Meta AI', srcId: 'meta', cat: '模型', kind: 'rss', win: '7d' },
  { title: 'HR 人事任命', link: 'https://c.com', ts: now, desc: 'z'.repeat(200), source: 'DeepMind', srcId: 'deepmind', cat: '模型', kind: 'rss', win: '7d', filter: 1 },
  { title: '过期条目', link: 'https://d.com', ts: now - 40 * 86400000, desc: 'w'.repeat(200), source: 'Old', srcId: 'x', cat: '模型', kind: 'rss', win: '7d' },
];
const fin = m.finalizeItems(items);
ok(fin.length === 1, 'finalizeItems：同题合并 + 关键词过滤 + 时间窗过滤 → 1 条');
ok(fin[0] && fin[0].also && fin[0].also[0] === 'Meta AI', 'finalizeItems：also 记录多来源');
ok(m.finalizeItems([]).length === 0, 'finalizeItems：空输入');

// ---- budget / net 记账 ----
globalThis.fetch = async () => new Response('ok'); // stub：避免真实外网
const b = m.createBudget({ silent: true });
const net = m.createNet({ GUJI_KV: { get: async () => null, put: async () => {} } }, b);
await net.fetch('https://example.com', {}, 'test:fetch');
await net.kvGet('k', 'json', 'test:kv');
await net.kvPut('k', 'v', {}, 'test:kvput');
await net.qSend({ send: async () => {} }, { a: 1 }, 'test:queue');
const rep = b.report();
ok(rep.byKind.fetch === 1 && rep.byKind.kv === 2 && rep.byKind.queue === 1, 'net 记账：fetch/kv/queue 分类型计数');
ok(rep.total === 4 && !rep.breached, 'net 记账：总数 4，未越限');
const b2 = m.createBudget({ silent: true, limit: 2 });
const net2 = m.createNet({ GUJI_KV: null }, b2);
await net2.kvGet('k'); // 无 KV 绑定 → 不计数
await net2.fetch('https://x.com', {}, 'a');
await net2.fetch('https://x.com', {}, 'b');
await net2.fetch('https://x.com', {}, 'c'); // 第 3 次越限
ok(b2.total === 3 && b2.breached, '越限告警：超限不拦截照常执行，但打 breached 标记');

// ---- parseDigest 容错 ----
const d = m.parseDigest('前言废话 [{"idx":1,"title":"测试","summary":"摘要"}]', items);
ok(d.length === 1 && d[0].title === '测试', 'parseDigest：抗前言 + 数组截取');

// ---- buildPushPairs 顺序 ----
const pairs = m.buildPushPairs({ TG_CHAT_ID: '111', TG_CHANNEL: '@ch', TG_BOT_TOKEN: 't' }, { items, digest: [{ cat: '模型', title: 'a', summary: 's', link: 'l', ts: now }] });
ok(pairs.length === 4 && pairs[0].chat === '111' && pairs[2].chat === '@ch', 'buildPushPairs：私聊全部消息 → 频道全部消息（保序）');
ok(pairs.every(p => typeof p.text === 'string' && p.text.length > 0), 'buildPushPairs：消息文本非空');

// ---- tgTruncate 字节安全 ----
const long = '中'.repeat(2000);
ok(Buffer.byteLength(m.tgTruncate(long), 'utf8') <= 3600 + 20, 'tgTruncate：UTF-8 字节 ≤ 3600');

console.log(`\n结果：${pass} 通过，${fail} 失败`);
process.exit(fail ? 1 : 0);
```

### 6.5 `e2e-v4.mjs` —— 端到端测试（39 项：mock KV/队列/AI/fetch 全链路）

```js
// v4 端到端集成测试：mock KV / 队列绑定 / AI / fetch，
// 完整跑 cron 入队 → 抓取分片 → 精选 → 推送分片 全链路。
import worker from './index.js';
import * as m from './index.js';

let pass = 0, fail = 0;
const ok = (cond, name) => { if (cond) { pass++; console.log('✓', name); } else { fail++; console.error('✗', name); } };
const rej = async (p, name) => { try { await p; ok(false, name + '（未抛错）'); } catch (e) { ok(true, name); } };

// ---------------- mock 基础设施 ----------------
const NOW = Date.UTC(2026, 9, 25, 13, 40); // UTC 13:40 = 北京 21:40（晚班抓取班）
const WIN = '2026-10-25-pm';

const kvStore = new Map();
const kv = {
  get: async (k, type) => {
    if (!kvStore.has(k)) return null;
    const v = kvStore.get(k);
    return type === 'json' ? JSON.parse(v) : v;
  },
  put: async (k, v) => { kvStore.set(k, typeof v === 'string' ? v : JSON.stringify(v)); },
};

const sent = { 'guji-fetch': [], 'guji-digest': [], 'guji-send': [] };
const qBind = (name) => ({ send: async (body) => { sent[name].push(body); } });

const ai = {
  run: async (model) => model.includes('qwen')
    ? { response: '' } // 模拟 Qwen3 思考模式：空 content（历史事故形态）
    : { response: '[{"idx":1,"title":"测试精选标题","summary":"这是一条中文摘要内容，基于原文概括。"}]' },
  toMarkdown: async () => [{ format: 'markdown', data: '# 富化标题\n这是补全后的正文内容，长度足够通过更丰富才采用的阈值判断逻辑。'.repeat(3) }],
};

const ZHIPU_DIGEST = '[{"idx":1,"title":"智谱精选标题","summary":"智谱生成的中文摘要。"}]';
function cannedFeed(f) {
  if (f.kind === 'edgar') {
    return `<?xml version="1.0"?><feed><entry><title>8-K</title><link href="https://www.sec.gov/x"/><updated>${new Date(NOW).toUTCString()}</updated><summary>Item 1.01: Entry into a Material Definitive Agreement</summary></entry></feed>`;
  }
  if (f.kind === 'html') {
    return `<a class="FeaturedGrid-module__tileLink" href="/news/test-slug-abc">Anthropic 发布新模型能力</a><a class="FeaturedGrid-module__tileLink" href="/news/engineering">栏目页</a>`;
  }
  return `<?xml version="1.0"?><rss><channel><title>t</title><item><title>${f.id === 'deepmind' || f.filter ? 'OpenAI 发布 GPT-6 新模型' : '某公司发布会'}</title><link>https://example.com/${f.id}</link><pubDate>${new Date(NOW).toUTCString()}</pubDate><description>短摘要</description></item></channel></rss>`;
}

globalThis.fetch = async (url, init = {}) => {
  const u = String(url);
  const head = { 'content-type': 'text/html; charset=utf-8' };
  if (u.includes('open.bigmodel.cn')) {
    return new Response(JSON.stringify({ choices: [{ message: { content: ZHIPU_DIGEST } }] }), { status: 200, headers: { 'content-type': 'application/json' } });
  }
  if (u.includes('api.telegram.org')) {
    return new Response(JSON.stringify({ ok: true, result: { message_id: 42 } }), { status: 200, headers: { 'content-type': 'application/json' } });
  }
  if (u.includes('sec.gov')) return new Response(cannedFeed(m.FEEDS.find(f => f.kind === 'edgar')), { status: 200, headers: { 'content-type': 'application/atom+xml' } });
  const f = m.FEEDS.find(x => x.url === u);
  if (f) return new Response(cannedFeed(f), { status: 200, headers: { 'content-type': 'application/rss+xml' } });
  return new Response('<html><body><p>页面正文</p></body></html>', { status: 200, headers: head });
};

const mkMsg = (body) => ({ body, acked: false, ack() { this.acked = true; } });
const mkBatch = (queue, messages) => ({ queue, messages, ackAll() { messages.forEach(x => x.ack()); } });
const pending = [];
const ctx = { waitUntil: p => { pending.push(p); return p; } };
const flush = async () => { await Promise.all(pending.splice(0)); };

// ---------------- 1. cron 入队（时间复用）----------------
{
  const env = { GUJI_KV: kv, AI: ai, GUJI_FETCH_Q: qBind('guji-fetch'), GUJI_DIGEST_Q: qBind('guji-digest'), GUJI_SEND_Q: qBind('guji-send') };
  sent['guji-fetch'].length = 0; sent['guji-digest'].length = 0; sent['guji-send'].length = 0;
  await worker.scheduled({ scheduledTime: NOW, cron: '40,50 21 * * *' }, env, ctx); // 北京 21:40 → 晚班抓取
  await flush();
  ok(sent['guji-fetch'].length === 4 && sent['guji-fetch'][0].window === WIN && sent['guji-fetch'][3].shard === 3, 'cron :40 入队 4 条抓取分片');
  ok(sent['guji-digest'].length === 0, 'cron :40 不入队精选（留给 :50）');
  await worker.scheduled({ scheduledTime: NOW + 10 * 60000, cron: '40,50 21 * * *' }, env, ctx); // :50
  await flush();
  ok(sent['guji-digest'].length === 1 && sent['guji-digest'][0].window === WIN, 'cron :50 入队 1 条精选');
  await worker.scheduled({ scheduledTime: NOW + 20 * 60000, cron: '0 22 * * *' }, env, ctx); // :00
  await flush();
  ok(sent['guji-send'].length === 3 && sent['guji-send'][2].slice === 2, 'cron :00 入队 3 条推送分片');
}

// ---------------- 2. 抓取消费者：4 个分片 ----------------
{
  const env = { GUJI_KV: kv, AI: ai };
  for (let n = 0; n < 4; n++) {
    const msg = mkMsg({ kind: 'fetch', window: WIN, shard: n, feeds: m.FEEDS.slice(n * 10, (n + 1) * 10) });
    await worker.queue(mkBatch('guji-fetch', [msg]), env, ctx);
    ok(msg.acked, `抓取分片 ${n} 消费并 ack`);
  }
  const s0 = JSON.parse(kvStore.get(m.shardKey(WIN, 0)));
  ok(s0.items.some(i => i.srcId === 'anthropic'), '分片 0 含 Anthropic 条目（HTML 兜底源）');
  ok(s0.items.some(i => i.srcId === 'openai'), '分片 0 含 OpenAI 条目');
  const s1 = JSON.parse(kvStore.get(m.shardKey(WIN, 1)));
  ok(s1.items.some(i => i.kind === 'edgar'), '分片 1 含 SEC EDGAR 条目');
  ok(kvStore.has(m.runKey(WIN, 'fetch:0')), '抓取运行记录落库');
}

// ---------------- 3. 精选消费者：分片未就绪 → 抛错重试 ----------------
await rej(worker.queue(mkBatch('guji-digest', [mkMsg({ kind: 'digest', window: '2026-10-26-am', full: false })]), { GUJI_KV: kv, AI: ai }, ctx), '分片缺失 → 抛错（队列重试语义）');

// ---------------- 4. 精选消费者：完整链路（无智谱 key，走 qwen 空 content → llama 兜底）----------------
{
  const env = { GUJI_KV: kv, AI: ai }; // 无 ZHIPU_KEY
  const msg = mkMsg({ kind: 'digest', window: WIN, full: false });
  const t0 = Date.now();
  await worker.queue(mkBatch('guji-digest', [msg]), env, ctx);
  ok(msg.acked, '精选消费并 ack');
  ok(Date.now() - t0 < 30000, '精选在 30 秒内完成（qwen 空 content 重试 + llama 兜底）');
  const latest = JSON.parse(kvStore.get(m.KV_KEY));
  ok(latest.digest.length === 1 && latest.digest[0].title === '测试精选标题', '精选落库：llama 兜底产出 1 条');
  ok(latest.items.length > 10, `条目池 ${latest.items.length} 条 > 10（跨分片合并生效）`);
  ok(latest.window === WIN && latest.lastDigestAt > 0, 'latest 带 window 与 lastDigestAt');
  ok(latest.budget && latest.budget.total <= 50, `精选预算 ${latest.budget.total}/50 ≤ 上限`);
  ok(JSON.parse(kvStore.get(m.runKey(WIN, 'digest'))).ok === true, 'digest 运行记录 ok');

  // 晚报 cutoff：同一窗口再跑一次，无新增 → 回退全窗口（宁薄勿空）
  const msg2 = mkMsg({ kind: 'digest', window: WIN, full: false });
  await worker.queue(mkBatch('guji-digest', [msg2]), env, ctx);
  ok(msg2.acked, '同窗口二次精选：无新增回退全窗口，不报错');
}

// ---------------- 5. 有智谱 key 时优先走智谱 ----------------
{
  const env = { GUJI_KV: kv, AI: ai, ZHIPU_KEY: 'test-key' };
  const msg = mkMsg({ kind: 'digest', window: WIN, full: true });
  await worker.queue(mkBatch('guji-digest', [msg]), env, ctx);
  const latest = JSON.parse(kvStore.get(m.KV_KEY));
  ok(latest.digest[0].title === '智谱精选标题', '有 ZHIPU_KEY → 智谱主力产出');
  ok(msg.acked, 'full=1 手动全量精选 ack');
}

// ---------------- 6. 推送消费者：3 片顺序 + 幂等 + 闸门 ----------------
{
  const env = { GUJI_KV: kv, AI: ai, TG_BOT_TOKEN: 't', TG_CHAT_ID: '111', TG_CHANNEL: '@ch' };
  for (let s = 0; s < 3; s++) {
    const msg = mkMsg({ kind: 'send', window: WIN, slice: s, slices: 3 });
    await worker.queue(mkBatch('guji-send', [msg]), env, ctx);
    ok(msg.acked, `推送分片 ${s} 消费并 ack`);
  }
  ok(kvStore.has(m.runKey(WIN, 'push')), 'slice 0 完成后写「本班已推」标记');
  const pushRec = JSON.parse(kvStore.get(m.runKey(WIN, 'push')));
  ok(pushRec.ok >= 1 && pushRec.total >= 1, `推送记录：${pushRec.ok}/${pushRec.total} 成功`);
  const s0 = JSON.parse(kvStore.get(m.runKey(WIN, 'send:0')));
  ok(s0.budget.total <= 50, `推送分片预算 ${s0.budget.total}/50 ≤ 上限`);

  // 窗口不匹配 → 跳过不推
  const msgBad = mkMsg({ kind: 'send', window: '2026-10-26-am', slice: 0, slices: 3 });
  await worker.queue(mkBatch('guji-send', [msgBad]), env, ctx);
  ok(msgBad.acked, '窗口不匹配 → ack 跳过（不推旧班数据）');
}

// ---------------- 7. 数据过期闸门 + force bypass ----------------
{
  const env = { GUJI_KV: kv, AI: ai, TG_BOT_TOKEN: 't', TG_CHAT_ID: '111' };
  const stale = JSON.parse(kvStore.get(m.KV_KEY));
  stale.generatedAt = Date.now() - 5 * 3600 * 1000; // 5 小时前
  kvStore.set(m.KV_KEY, JSON.stringify(stale));
  const msg = mkMsg({ kind: 'send', window: WIN, slice: 0, slices: 3 });
  await worker.queue(mkBatch('guji-send', [msg]), env, ctx);
  ok(msg.acked, '数据过期 5 小时 → ack 跳过（不推旧内容）');
  const msgF = mkMsg({ kind: 'send', window: WIN, slice: 0, slices: 3, force: true });
  await worker.queue(mkBatch('guji-send', [msgF]), env, ctx);
  ok(msgF.acked, 'force=1 → 过期也推（手动补推）');
}

// ---------------- 8. HTTP 路由：/api/build /api/push 幂等 /api/diag ----------------
{
  const env = { GUJI_KV: kv, AI: ai, GUJI_FETCH_Q: qBind('guji-fetch'), GUJI_DIGEST_Q: qBind('guji-digest'), GUJI_SEND_Q: qBind('guji-send'), TG_BOT_TOKEN: 't', TG_CHAT_ID: '111' };
  sent['guji-fetch'].length = 0; sent['guji-digest'].length = 0; sent['guji-send'].length = 0;

  const rBuild = await worker.fetch(new Request('https://guji.dpdns.org/api/build?full=1'), env, ctx);
  const jBuild = await rBuild.json();
  ok(jBuild.ok && sent['guji-fetch'].length === 4 && sent['guji-digest'].length === 1 && jBuild.enqueued.digest === 1, '/api/build 入队 4 抓取 + 1 精选（full=1）');

  // 清掉第 6 节写下的「本班已推」标记，模拟首次推送
  kvStore.delete(m.runKey(m.currentWindow(Date.now()), 'push'));
  const rPush1 = await worker.fetch(new Request('https://guji.dpdns.org/api/push'), env, ctx);
  ok((await rPush1.json()).enqueued.send === 3, '/api/push 首次入队 3 片');

  // 制造「本班已推」记录在 10 分钟内 → 第二次被幂等拦截
  kvStore.set(m.runKey(m.currentWindow(Date.now()), 'push'), JSON.stringify({ at: Date.now(), ok: 3, total: 3 }));
  sent['guji-send'].length = 0;
  const rPush2 = await worker.fetch(new Request('https://guji.dpdns.org/api/push'), env, ctx);
  const jPush2 = await rPush2.json();
  ok(!!jPush2.skipped && sent['guji-send'].length === 0, '/api/push 幂等：10 分钟内推过 → 跳过');
  const rPush3 = await worker.fetch(new Request('https://guji.dpdns.org/api/push?force=1'), env, ctx);
  ok((await rPush3.json()).enqueued.send === 3 && sent['guji-send'].length === 3, '/api/push?force=1 → 强制入队');

  const rDiag = await worker.fetch(new Request('https://guji.dpdns.org/api/diag'), env, ctx);
  const jDiag = await rDiag.json();
  ok(jDiag.kvFound && jDiag.shards.filter(Boolean).length === 4 && jDiag.digest >= 1, '/api/diag：latest + 4 分片就绪 + 精选数');
  ok(jDiag.aiProbe && typeof jDiag.aiProbe.ok === 'boolean', '/api/diag：AI 探针');

  const rPage = await worker.fetch(new Request('https://guji.dpdns.org/'), env, ctx);
  const html = await rPage.text();
  ok(html.includes('GUJI') && html.includes('今日精选'), '网页渲染：含品牌与精选区块');
}

console.log(`\n结果：${pass} 通过，${fail} 失败`);
process.exit(fail ? 1 : 0);
```

---

## 7. Secrets 与环境变量（不在代码里，`wrangler secret put` 设置）

| Secret | 用途 | 缺失时行为 |
|---|---|---|
| `TG_BOT_TOKEN` | Telegram bot token | 不推送，仅网站 |
| `TG_CHAT_ID` | 创建者私聊 chat id | 只推频道 |
| `TG_CHANNEL` | 公开频道（@guji_ai_daily 或 -100 数字 ID） | 只推私聊 |
| `ZHIPU_KEY` | 智谱 API key（glm-4-plus 主力模型） | 模型链退回 Qwen → llama |
| `JINA_KEY`（可选） | Jina Reader（enrich 快通道） | enrich 走 CF 原生 toMarkdown |

绑定（wrangler.toml）：`GUJI_KV`（KV，namespace id `ca49d41ac8fa4987a0f818cee47237bf`，name `guji-free-kv`）、`AI`（Workers AI）、`GUJI_FETCH_Q`/`GUJI_DIGEST_Q`/`GUJI_SEND_Q`（队列生产者）。

账号信息：Account ID `5b749ffbbfbfed51694fb99fbc7b9b12`；API token（`cfat_…`，Workers 编辑器 template 权限）**已在对话中暴露，应尽快轮换并开启 2FA**（见 §9）。

---

## 8. 部署与验证方法（改完代码必须全套跑过再上线）

### 8.1 首次部署（队列创建）

```bash
cd guji-v2

# ① 建 4 条队列（一次性；wrangler deploy 也会自动建，但显式创建更可控）
npx wrangler queues create guji-fetch
npx wrangler queues create guji-digest
npx wrangler queues create guji-send
npx wrangler queues create guji-dlq

# ② 语法预检 + 测试全家桶（必须全绿）
node --check index.js
node smoke-v4.mjs        # 17 项
node e2e-v4.mjs          # 39 项

# ③ 部署
CLOUDFLARE_API_TOKEN=<token> npx wrangler deploy
```

### 8.2 队列级延迟设置（★ 关键运维命令，重部署后需确认）

**digest 队列的 90 秒延迟靠队列级设置保证**（wrangler.toml 的 producer `delivery_delay` 对已存在绑定不生效，见 §4.2）：

```bash
# 设置（全量覆盖语义，注意带上 message_retention_period）
curl -X PUT "https://api.cloudflare.com/client/v4/accounts/<ACCOUNT_ID>/queues/<DIGEST_QUEUE_ID>" \
  -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"queue_name":"guji-digest","settings":{"delivery_delay":90,"message_retention_period":86400}}'

# 核实（应输出 guji-digest delivery_delay=90s）
curl -s "https://api.cloudflare.com/client/v4/accounts/<ACCOUNT_ID>/queues" \
  -H "Authorization: Bearer <token>" | python3 -c "
import json,sys
for q in json.load(sys.stdin)['result']: print(q['queue_name'], q['settings']['delivery_delay'])"
```

queue_id 查询：上面的 list 接口里 `guji-digest` 对应 `900f722afefc4878882d246523fc5d68`（2026-10-25 创建，一般不变）。**每次 wrangler deploy 后建议核实一次**（当前实测 deploy 不会重置它，但 CF 行为可能变）。消费者内 45s 轮询是主保险，延迟丢失不会致命，但会多烧重试次数。

### 8.3 KV 键迁移（v3 → v4，已执行过，备查）

CLI `wrangler kv` 只读本地 state，迁移必须走 REST API：

```bash
# 读 v3 旧键
curl -s ".../namespaces/<NS>/values/guji:v3:latest" -H "Authorization: Bearer <token>" -o /tmp/v3latest.json
curl -s ".../namespaces/<NS>/values/guji:v3:seen"    -H "Authorization: Bearer <token>" -o /tmp/v3seen.json

# 写 v4 新键（latest 106KB，seen 小）
curl -X PUT ".../namespaces/<NS>/values/guji:v4:latest" -H "Authorization: Bearer <token>" \
  --data-binary @/tmp/v3latest.json -H "Content-Type: application/json"
curl -X PUT ".../namespaces/<NS>/values/guji:v4:seen" -H "Authorization: Bearer <token>" \
  --data-binary @/tmp/v3seen.json -H "Content-Type: application/json"

# 旧 v3 键无需手动删：latest 14 天 TTL、seen 90 天 TTL，自然过期
```

### 8.4 线上验证流程与实测记录

```bash
# ① 手动构建（入队 4 抓取 + 1 精选，精选 90s 后消费）
curl "https://guji.dpdns.org/api/build"            # 走 cutoff（只精选新增）
curl "https://guji.dpdns.org/api/build?full=1"     # 忽略 cutoff，全量精选

# ② 诊断：latest 状态 + 分片就绪 + AI/智谱实时探针
curl https://guji.dpdns.org/api/diag

# ③ 手动推送（幂等：同 window 10 分钟内推过则跳过，?force=1 强制）
curl https://guji.dpdns.org/api/push
curl "https://guji.dpdns.org/api/push?force=1"

# ④ 网页整体检查
curl https://guji.dpdns.org/

# ⑤ KV 运行记录（消费者执行留痕，排「分片消失」专用）
#    guji:v4:run:{window}:fetch:{n} / :digest / :send:{n} / :push
#    见下方 python 片段或 /api/diag
```

**验收标准**：`/api/build` 返回 ok；run:fetch 4 条 ok:true；run:digest ok:true 且 digest ≥ 1；`/api/push` 全部 ok:true；网页「今日精选」区有条目。

**2026-10-25 实测记录（v4 上线日，版本 bfce87d9）**：

| 项 | 结果 |
|---|---|
| 抓取 4 分片 | 730 条原始条目（234/6/240/250），1 源失败，单片预算 12-14/50，+4s ~ +13s 全部落库 |
| 精选 | 12 条，智谱 glm-4-plus 一次成功；run:digest 于 build 后 **t0+104s**（= 90s 队列延迟 + 14s 处理，**延迟端到端验证成功**）；预算 14-15/50（fetch 5 = 4 enrich + 1 智谱；kv 6；ai 4） |
| 推送 3 片 | 26/26 全部成功（9+9+8），单片预算 9-10/50，片间 7s（300ms 限速生效） |
| 幂等守卫 | 二次 `/api/push` 正确返回 skipped |
| 首页 | 条目/源数/精选区块正常渲染 |
| 测试 | smoke 17/17 + e2e 39/39 全绿 |

**★ 15:14/15:25 分片消费者挂起事件（已处置，记录备查）**：

- 现象：连续两轮 `/api/build` 后，4 个抓取消费者只有 1 个（分片 1）完成，分片 0/2/3 无 run:fetch 记录、分片键不出现；digest 消费者因此每轮等 45s 抛错、静默重试
- 推断：3 个消费者调用挂起，占住 max_concurrency=4 的槽位（每轮只剩 1 槽位）；15:38 重新部署后恢复（旧调用被终止）
- 根因未完全定位（疑与当次操作前的 KV 删除/队列状态有关，无日志佐证——沙箱内 wrangler tail 不可用）
- 处置：① 增加诊断埋点（run:fetch 阶段记录：start/fetched/ok/error；digest 静默 throw 路径也留痕）；② 15:43 复测全链路正常；③ cron 路径（:40 抓取与 :50 精选分离）不涉及 `/api/build` 的同时入队，受影响面小
- 若再发：按 run:fetch 的 stage 记录定位死因（停在 start = 调用没跑/挂起；停在 fetched = 分片 KV 写失败）

**定时自检**：每天北京时间 05:40（抓取）→ 05:50（精选）→ 06:00（推送）、21:40 → 21:50 → 22:00。改完代码的第二天早上看推送结果即可确认无人值守链路正常。**v4 首次无人值守 cron 运行：2026-10-25 21:40**（本文档交付时尚未发生，次日以 /api/diag 与推送结果验收）。

**回滚**：`npx wrangler rollback`（或重新部署上一个 Known-good 版本）。注意回滚只回 Worker 代码，**队列级 delay 设置不回滚**（它在队列上，不在 Worker 里）。

---

## 9. 重构建议方向 · 状态盘点

| # | 建议 | 状态 |
|---|---|---|
| 1 | 拆模块（feeds/parsers/digest/telegram/render） | 部分落地：15 区分区已实现关注点分离；物理拆文件仍开放（§5.2-7） |
| 2 | 子请求预算显式化（计数器 + 告警） | **已落地**：createBudget/createNet 全网关 + BUDGET_PLAN 静态表 + 单测断言 |
| 3 | 模型链配置化（声明式 + 熔断） | **已落地**：buildChain 声明式数组；熔断仍开放（§5.2-1） |
| 4 | 纯函数补单测 | **已落地**：smoke 17 + e2e 39，覆盖 parseDigest/tgTruncate/finalizeItems/buildPushPairs/全链路 |
| 5 | 时间单位统一 | **已落地**：toMs/toSec/nowSec |
| 6 | enrich 与 makeDigest 解耦 + 缓存 | 已解耦；缓存仍开放（§5.2-3） |
| 7 | 删除死代码 | **已落地**：feeds-list.mjs 引用已删（文件可删）、renderTg、LAST_DIGEST_KEY |

新增待办（按优先级）：

1. **轮换对话中暴露的 CF API token（`cfat_…`）并开启账号 2FA**——安全债，越早越好
2. 智谱 key 妥善保管（同上，对话中出现过）
3. bot 头像手动上传（Telegram BotFather）
4. 但斌/雪球来源接入评估（用户提过，未排期）
5. DLQ 告警通道（Tail Worker / 邮件），见 §5.2-5
6. 今晚 21:40 首次无人值守 cron 运行次日验收

---

*文档生成时间：2026-10-25 15:50（GMT+8）。代码以第 6 节原文为准，与线上版本 `bfce87d9-ae8f-4285-9c26-14cc5639510d` 一致；如线上已滚动更新，以 `npx wrangler deployments list` 的最新版本为准。*
