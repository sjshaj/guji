# CHANGELOG

> 事实变更清单（叙事版历史见 `DEVLOG.md`）。格式：版本 · 日期 · 变更 · 状态。
> 更新规矩：每次线上部署在这里加一条，DEVLOG 只记「为什么和踩坑」。

## v4.0.1 · 2026-10-25 深夜 · 外部审查驱动修复批次

> 来源：外部 AI 审查链五份文档（PR #1 审查 → 作者复核一 → PR #3 回复 →
> PR #4 调研 → 作者复核二，均存 `docs/`）。落地范围为「作者认同 + 时机成熟」项，
> 逐项立场见 `docs/外部AI审查_作者复核.md` 与 `docs/外部AI调研_作者复核.md`。

**修复**
- **A1 推送失败补推机制**：新增 `guji-retry` 补推队列。consumeSend 失败时分流——
  可重试项按 `p.i` 从原 chunk 回取文本入补推队列（`{kind:'retry', attempts:1}`），
  永久错误（chat not found / bot blocked / user deactivated / not enough rights）ack 放弃
  并写 `run:{window}:send:{slice}:failed` 留痕；consumeRetry 最多 3 轮后记 run + console.error
  - 设计决策：**不用 `msg.retry()`**——Queues 重试是消息级粒度，一片内 9 推 5 时
    retry 整条会重推已成功的 5 条；补推队列天然幂等（DEVLOG §4.5）
- **A2 TG 429 退避**：`retryAfterMs` 从响应体 `parameters.retry_after` 读秒，
  退避 `min(retry_after*1000+250, 30000)`，不再拍脑袋固定 sleep
- **A3 纯数字 guid 解析**：xmlParser 加 `parseTagValue:false`，修复 "0012"→12
  丢前导零；**不加** `htmlEntities:true`（与 stripTags 双重解码，坑 2 同款）
- **A4 注释修正**：wrangler.toml digest producer 的 `delivery_delay` 注明
  「仅对新建队列有效，生产 truth 在队列级 REST API」——本次部署 wrangler 4.152
  已对该字段发 deprecated 警告，实证注释所述
- **C1** 删 `semicapid` 关键词（不在十五区，误匹配风险）
- **C2** 删 `!it.ts && it.srcId !== 'anthropic'` 特例（ts:0 旧方案遗留，改注释）
- **C3** buildPrompt/buildChain 的 Qwen3 开关注释改「历史遗留，实测无效」（DEVLOG 坑 9）
- **C4** probeAI 改探生产链模型：有 ZHIPU_KEY 时探 CF 侧 llama-3.1-8b 兜底层，
  线上实证返回 `@cf/meta/llama-3.1-8b-instruct-fp8`（修复前探的 Qwen3 不在生产链）

**新增**
- smoke-v4 17 → 31 项：retryAfterMs 4 项 / isPermanentTgError 6 项 /
  RETRY_MAX_ROUNDS / A3 数字 guid feed / C1-C2 分类锁定，每项修复都有测试锁行为
- `batch[0]` 依赖注释（consumeSend 的 `max_batch_size=1` 前提写进代码）

**验证**
- 70 项测试全绿（smoke 31 + e2e 39）→ 部署版本 b10295b4
- 5 队列就位（guji-retry id 37549f1c…）→ `/api/diag` 200（items 149 / digest 9）→
  guji-digest 队列级 `delivery_delay=90` 未受部署影响（B4 运维项本轮已核实）

## v4.0.2 · 2026-10-26 凌晨 · 精选环节五项改进 + 审查链收尾

> 来源：外部调研 PR #4 精选专项（作者立场见 `docs/外部AI调研_作者复核.md`）+
> 外部审查 PR #1 剩余认同项（B2/C5/C6/D2，立场见 `docs/外部AI审查_作者复核.md`）。
> 叙事与踩坑见 DEVLOG §4.6。

**精选（§10 核心路径）**
- **P0-④ temperature 0.3→0.1**（`DIGEST_TEMPERATURE`）：低温降编造，
  只求稳不宣称收益
- **P0-② 提示词硬约束清单**：产品名/数字/版本号原文可找、宁省略不补全、
  相对时间照抄、不强化语气、排他性表述仅原文明示才保留
- **P0-③ 代码级忠实度校验**（`checkFaithful`/`filterFaithful`）：数字单位归一化
  （35亿 = $3.5 billion = 3.5e9）、型号模式抽取（GPT-5/H100 规范化）、
  **有意收窄**（纯整数与纯字母产品名不校验，防「35 亿↔3.5 billion」互译与
  中文音译误杀）；剔除明细记 console.error
- **P0-① 精选标准 rubric 化**：四维打分（影响面/信息密度/来源权威/时效性）
  先理由后分数，normScore 容错（夹 1-10、缺维中性 5），parseDigest 总分
  降序后取 12；自报分只排序不设阈值
- **P1-⑤ embedding 事件聚类 v1**：bge-m3 批量 embedding（1 子请求）+
  cosine ≥ 0.85 从严 + 48h 时间门限 + 并查集；「拿不准不合并」
  （0.80-0.85 记 near 供 aiTrace）；ts=0 从严不聚类；失败整体跳过不阻断日报

**其余审查项**
- **B2 `sendBatch` 批量入队**：抓取/推送入队计 1 子请求（cron 班子请求 5→2），
  保序 + `max_concurrency=1` 串行消费者
- **D2 fixture 契约测试 + dry-run 巡检**：`src/__fixtures__/` 真实 feed 快照
  （OpenAI RSS 3 条含 CDATA/实体、Apple EDGAR atom 10 条 ISO-8859-1）；
  新路由 `/api/fetch?dry=1&shard=N` 只抓取解析，不 enrich/AI/推送/写 KV/入队
- **C5 公共 helper**：`mkItem`（三解析器统一条目构造）、`markRun`（9 处埋点统一，
  catch 吞掉语义写明）、`sliceChunks`（两处切分统一）
- **C6 魔法数字入 config**：15 个截断/批量/TTL 常量进 §1

**B4 量化结论**（`docs/外部AI审查_作者复核.md`）：两轮离线统计 106 条原始条目、
跨源同事件候选 0 个——一手来源策略下官方源不互相转载，**维持「低则不上
SimHash」**；该方向由 P1-⑤ 聚类在精选路径覆盖

**新增测试**
- smoke 31 → 79 项：精选五项全套断言（归一化不误杀/型号编造拦截/中文音译
  不误杀/rubric 排序与容错/cosine/cluster 门限与 near）、B2 记账、
  C5 透传与截断、C6 常量、fixture 契约；合计 **118 项**全绿

**验证**
- 118 项测试全绿 → 部署版本 **29f4dedd**（d3c2db9）
- `/api/diag` 200（items 149 / digest 9 / digestErr 空）→ 页面 200 →
  生产 dry-run 分片 0（234 原始 → 61 条，0 失败，预算 11/50）
- 5 队列就位，guji-digest 队列级 `delivery_delay=90` 未受部署影响；
  4 个 cron 表达式（6 触发点）在线
- GitHub 推送走 REST API 通道（git 协议不通）；中途两次脚本事故
  （中文路径 quotepath 转义、泛化重写漏前缀检查）均以 fast-forward
  修正提交修复，文件集验证当场捕获污染，最终全量字节级验证
  27 文件一致（DEVLOG §4.6.6 有完整记录）
- **新 digest 路径未手动触发**（会砍掉 05:50 定时班 fresh 窗口，DEVLOG §4.6.5），
  留给 05:40-06:00 无人值守首班自然验收

## v4.0.2.1 · 2026-10-26 凌晨 · 运维加固批次

> 来源：ROADMAP 剩余项中「不改核心路径、当晚可闭环」的五项（C5/D2/B3/D3/D4）。
> 叙事见 DEVLOG §4.7。

**新增**
- **C5 diag 端点鉴权灰度**（`diagAuthorized`）：诊断端点暴露源健康度与预算消耗，
  不适宜公开。灰度策略——配置 `DIAG_KEY` secret 则 `?key=` 精确匹配，未配置则放行。
  代码先上、secret 后置：一经设置即生效，无需再部署（设置方法见 README「密钥管理」）
- **D2 DLQ 告警链路**：consumeDlq 按班次累加 KV 死信计数（`guji:v4:dlqcount:{window}`），
  consumeSend 最后分片（班次结束点）读取，>0 且未告警过则私聊 TG_CHAT_ID 推送；
  alerted 键（`guji:v4:dlqalerted:{window}`）去重，try/catch 包裹不阻断主流程
- **B3 分片新鲜度防线**（`isStaleShard`）：digest 消费者等待分片时，分片 at 早于
  digest 消息入队时刻 10 分钟以上 → 判定未就绪继续等（digest 消息体新增 `at` 字段）。
  宁可重试/进 DLQ，不误用上一班旧分片；超时时区分为 `shard-stale-timeout` /
  `shard-wait-timeout` 两种 stage
- **D3 GitHub Actions 每日健康巡检**（`.github/workflows/health.yml`）：每天 UTC 22:30
  （北京 06:30，早班推送后）跑 smoke + e2e + `/api/diag` HTTP 检查，digestErr 非空
  或非 200 即失败，自动开/跟进带 health 标签的 issue（需 repo secret `DIAG_KEY`）
- **D4 源健康度聚合**（`aggregateHealth`）：`?health=1` 按需聚合近 3 天 run 记录。
  seen = 该源所在分片的**全部班次**（成功+失败都算参跑——分片级失败时无法知道
  单个源是否无辜，按「该班次没覆盖到」记失败，rate 因此偏保守）；
  分片整体失败退化为该片全源各记一次失败；rate 不除零。consumeFetch markRun 补
  `failedSources` 明细（原先只记数量，diag 无法回答「哪个源在失败」）

**新增测试**
- smoke 79 → 100 项：diagAuthorized 5（灰度放行/正确/错误/缺失/无文案）、
  renderDlqAlert 2、isStaleShard 5（1 分钟新鲜/11 分钟过期/晚于入队新鲜/
  缺 at 不校验/消息缺 at 不校验）、aggregateHealth 6、pastWindow 3（跨月边界）；
  合计 **100 项**全绿（e2e 39 不变）

**验证**
- 100 项测试全绿 → 部署版本 **c739fd45**（9a75bc1）
- `/api/diag` 200（items 149 / digest 9 / digestErr 空）→ 页面 200 →
  `?key=wrong` 灰度放行 200（DIAG_KEY 未配置时的预期行为）→
  `?health=1` 聚合 200（近 3 天 24 次 KV 读、shardRuns=4）
- guji-digest 队列级 `delivery_delay=90` 未受部署影响
- **部署未触发任何班次**（北京 10-26 02:1x，距最近 cron 三个半小时）：
  新代码首班留给 05:40 am 班自然验收（延续 v4.0.2「不手动触发 digest」纪律）；
  D2 DLQ 告警链路未触发（无死信），行为由 smoke 测试锁定
- `?health=1` 显示 shardRuns=4——全部来自 10-25 pm 班 4 分片；更早班次由 v3 跑
  （run 记录体系 v4 才引入），属预期；D4 基线从 10-26 am 班起逐班积累

## v4.0.2.2 · 2026-10-26 上午 · 审查闭环补全：B1 A/B 实验（当前线上）

> 来源：外部审查 B1（PR #1 主张「enrich 改 HTMLRewriter」）+ PR #3 审查者回复的
> 「零成本 A/B」修正方案。作者复核一曾把 B1 从「直接替换」降级为「可选 A/B 实验」，
> 并在调研复核中「全盘采纳」PR #3 的零成本设计——但此前一直未落地；本次补全，
> 闭环「审查 AI 提出、作者认同的问题」。

**新增**
- **B1 A/B 实验**（`htmlRewriterText` + enrichMaterial 回传 abExp）：`fetchViaToMarkdown`
  已抓到整页 HTML，复用它对同份 HTML 裸跑 `HTMLRewriter` 抽文本，与 toMarkdown 的
  desc 两版各取前 200 字记进 `aiTrace`（`trace.stage='b1-ab'`）。纯 CPU、零子请求、
  零 KV 污染、**不替换生产 desc**——只供人眼比对两种抽取的质量，再决定是否值得上。
  Jina 路径无同份 HTML，`rewriter=null` 不进 A/B 采集

**实现要点**
- `fetchViaToMarkdown` / `fetchViaJina` 返回形态改为 `{ md, rewriter }`；
  `enrichMaterial` 回传 abExp 数组，makeDigest 挂入 trace
- HTMLRewriter 是**流式 transform**：必须 `await res.text()` 消费 body 后 handler 才跑完，
  否则 text 恒空（与 toMarkdown 同为 Workers 运行时专属 API，Node smoke 测不到，测试注入 fake 覆盖）

**测试**
- smoke 100 → 105 项：B1 接线（toMarkdown 路径产生采集 / Jina 路径不进 / 两版截断 200 字）；
  HTMLRewriter 是 Workers 专属 API，Node 无原生实现，测试注入 fake 覆盖后还原全局

**代码改动清单（函数级）**
- 新增常量 `AB_EXP_HEAD = 200`（§1 config）：B1 A/B 两版 desc 各记前 200 字（仅 aiTrace，不落库）
- `enrichMaterial`（§11 增强）：新增 `abExp` 数组收集对比数据；当 `got.rewriter` 存在（仅
  toMarkdown 路径有同份 HTML）时，`abExp.push({ url, toMarkdown: 前200字, htmlRewriter: 前200字 })`；
  函数返回值从 `material` **改为 `abExp`**（调用方 makeDigest 用返回值，不再依赖副作用）
- 新增 `htmlRewriterText(html)` helper：用 `HTMLRewriter` 对整页 HTML 裸抽文本
  （选择器覆盖 p/div/span/li/h1-6/a/td/th/article/section/blockquote——导航栏与
  Cookie 声明会混入，这正是要度量的「噪音」），返回清洗并截断到 200 字的纯文本
- `fetchViaToMarkdown`：抓完 HTML 后调 `htmlRewriterText` 得 `rewriter`，
  返回形态从 `string` 改为 `{ md, rewriter }`
- `fetchViaJina`：返回形态同步改为 `{ md, rewriter: null }`（Jina 路径无同份 HTML，不参与 A/B）
- `makeDigest`：解构 `const abExp = await enrichMaterial(...)`，当 `abExp.length` 时
  `trace.push({ stage: 'b1-ab', note, items: abExp })`——自然出现在 `/api/diag` 的 aiTrace 中

**为什么这样改（设计约束）**
- **零成本**：复用 `fetchViaToMarkdown` 已经抓到的整页 HTML（原本抓完就丢弃），
  不再发额外请求——0 子请求、0 KV 写入、纯 CPU，不挤占 digest 班 19/50 预算
- **不替换生产输出**：两版 desc 只进 `aiTrace` 供人眼比对，生产仍只用 toMarkdown 版，
  避免用「未经验证的 HTMLRewriter 裸抽」直接替换已稳定的 enrich 结果（这正是 PR #1 的激进替换方案被否决的原因）
- **只在 toMarkdown 路径采集**：Jina 路径手里没有「同份 HTML」（Jina 直接返回 markdown），
  无法做对照，故 `rewriter: null` 主动不进采集，保证对照样本的可比性

**验证**
- 105 项测试全绿 → 部署版本 **1a09990f**（9fc50e2）→ `/api/diag` 200 / 页面 200 /
  items 149 / digest 9 / digestErr 空
- `trace.stage='b1-ab'` 将在今晚 pm 班真实精选（有薄摘要需 enrich 的条目）时出现于
  `/api/diag` 的 aiTrace，供人眼比对两版 desc 质量

## v4.0.2.3 · 2026-10-26 午间 · 根治「早班不累计」观察盲区 + 冷启动加固

> 现象：线上反复出现「am 班（北京 05:40/05:50）偶尔整班缺失」——`latest.window` 冻结在
> 上一晚 pm 班，`health.shardRuns` 长期只有 4（全为 10-25 pm 班的 4 分片）。今天
> （2026-10-26 05:40 am 班）再次复现，且 health 实际读取的 `2026-10-26-am:0..3` 四条全为空。
> 下游每个阶段都写 `run` 记录，唯独 **cron 触发本身没写任何记录**，导致「cron 没触发」与
> 「触发了但入队挂了」在我们的数据里完全无法区分——这是它反复发生却一直查不清的根。

**修改**
- `scheduled` 处理器重构：同一 promise 既 `ctx.waitUntil(p)`（保活隔离）又 `await p`
  （handler 返回前保证入队真正完成）。冷启动下避免隔离被回收导致早班（长时间空闲后首班）
  入队丢失——这是「早班独发、晚班不发」最可疑的根因（晚班 Worker 温热不踩）
- 触发即写 **cron 心跳** `run:{window}:cron`（任何入队之前）；入队成功写 `run:{window}:enqueue`；
  入队失败写 `run:{window}:cron-error`。三者全部落 KV，**不再像旧版只 `console.error` 随日志过期消失**
- `markRun`：KV 写失败从 `.catch(()=>{})` 静默吞掉改为 `.catch(e=>console.error(...))`
  打到 Workers 日志，诊断期能知道 run 记录是否写入失败，而非凭空消失

**为什么（根因）**
- **不是逻辑 bug**：调度分流与队列消费逻辑完全对称，真有 bug 晚班也会挂。失败发生在
  「cron→入队」最上游，且**只发生在早班**——因为早班是每日长时间空闲（前一日 22:00 推送 →
  次日 05:40，≈7.5h 无流量）之后的第一班，Worker **冷启动**；晚班（21:40）前有白天流量，温热不踩
- 两个候选机制：① `waitUntil` 即发即弃，冷启动隔离回收时 `sendBatch` 入队未真正落队列；
  ② 冷启动下 `sendBatch` 偶发瞬时失败被 `try/catch` 吞掉。二者都因「cron 步零持久记录 +
  markRun 静默吞错」而长期无法定位
- 本次改动同时**加固①**（await 入队）与**补齐观察盲区**：下次再失败，
  `run:{window}:cron` / `:enqueue` / `:cron-error` 会直接指出死在哪一步，确认是 cron 没触发还是入队挂了

**测试**
- smoke 105 → 112 项：scheduled 触发写 cron 心跳 / await 完成后写 enqueue / 抓取班入队 4 分片 /
  精选班 1 条 / 推送班 3 片 / 失败路径心跳先落库 + 写 durable cron-error

**验证 / 已部署**
- 112 项测试全绿；已 redeploy（CF 版本 `d2117803-3877-4350-8487-e29500b58936`，创建 2026-10-26T04:48:54Z，
  100% 生效）并推送（main `847fe701`，byte 校验 0 差异）。下一早班（北京 2026-10-27 05:40）观察
  `run:2026-10-27-am:cron` 是否出现即可确认 cron 是否触发；若 cron 在但无 fetch 分片，则指向队列消费侧（guji-fetch）

## v4.0 · 2026-10-25 · 队列化架构上线

**架构**
- Cloudflare Queues 三分片拓扑：`guji-fetch`（4 消费者并行）/ `guji-digest`（90s 延迟）/
  `guji-send`（串行保序）+ `guji-dlq` 死信队列（重试 3-10 次耗尽落地，只记日志）
- cron 只入队：4 个表达式覆盖 6 个触发点（免费档 5 个上限内），
  handler 按北京时间分钟分流（40 抓取/50 精选/00 推送）
- 分片隔离：39 源 ÷ 10 = 4 片，键 `guji:v4:shard:{window}:{n}`，window = `YYYY-MM-DD-am/pm`
- 子请求预算：抓取 ~13/50、精选 ~19/50、推送 ≤27/50——各阶段独立预算，消除 v3 的 47/50 走钢丝
- `createBudget`/`createNet` 全网关：所有 fetch/KV/AI/队列发送的唯一出口，预算可审计

**新增**
- 消费者全阶段诊断埋点（`guji:v4:run:{window}:{stage}`，TTL 2 天），失败路径不静默
- 队列级延迟投递（`delivery_delay=90`，经 Queues API PUT 设置——
  wrangler.toml 的 producer delay 对已存在绑定不生效，DEVLOG §4.3 坑 12）
- smoke-v4（17 项）+ e2e-v4（39 项）测试体系
- `docs/` 四份运维文档（交接/验收/源病历/Runbook）

**修复**
- 05:50 digest=0 的结构性修复（瞬时失败由队列重试兜底）
- 分片消费者成批消失事件：加埋点观测；重新部署恢复（根因未定位，ROADMAP B2）

**已知取舍**
- digest 消费者同 window 二次 build 读旧分片（ROADMAP B3）
- `/api/diag` 无鉴权（ROADMAP C5）

## v3.x · 2026-10-24 深夜 ~ 10-25 凌晨 · 推送与精选

**新增**
- Telegram 推送：bot + 频道（t.me/guji_ai_daily），一条精选一条消息，
  UTF-8 字节截断（3600），条间退避 300ms 防 429
- 每天两班（北京 06:00/22:00）+ 早晚班互补记忆（`lastDigestAt`，晚报只报新增）
- 「先精选后总结」：makeDigest 两步流程，模型只回 `{idx,title,summary}`，
  链接/分类/来源 idx 回填防幻觉；parseDigest 三级容错
- 模型链：智谱 glm-4-plus 主力（2 尝试）+ llama-3.1-8b 兜底（1 尝试），
  无 ZHIPU_KEY 时退化 Qwen3 + llama；aiTrace 逐次取证
- `env.AI.toMarkdown` 薄摘要增强（替代 Jina Reader），ENRICH_MAX=2 串行 + HTML_MAX=150KB
- /api/diag 诊断端点（KV 日志 + AI 探针 + 智谱探针）
- 十五区重划 + budget/net 全网关（另一 AI 重构产物审查后部署，885 行）

**修复**
- Error 1102：整页 HTML 塞 Blob 内存爆 → 三重降压（DEVLOG §3.3 坑 6）
- UTF-16 截断导致 TG 4096 字节超限被静默拒绝（坑 7）
- Qwen3 空 content：三重开关失效 → 智谱 GLM-4-plus（无思考模式）（坑 9）
- 05:50 定时 digest=0：短期 trace 定位，长期触发 v4（坑 10）

## v2 · 2026-10-24 下午 · splice bug 修复上线

**修复**
- 「只有 2 个源产出」：collect() 合并逻辑错误（splice 语义误用）→
  按 philippdubach/cloudflare-rss-aggregator 参照重写
  （小批次并发 + 每批结果全部保留 + 逐源 try/catch + 整批兜底超时）。
  生产验证 160 条/班（DEVLOG §2）

**变更**
- 删除股票条（Stooq 挂了 + 用户明说非重点）
- EDGAR win 默认显式化（14d）；arXiv 限额 8

## v1 · 2026-10-24 上午 · 初版

**新增**
- 39 源三通道抓取（rss / atom-edgar / html）+ fast-xml-parser v5 统一解析
- 源清单驱动设计（kind/cat/win/url|cik 四要素）
- 时间窗口过滤 + AI_KW 关键词过滤 + 每源限额 + 多源合并去重（normTitle 做 key）
- 暗色卡片网页渲染（七分类）
- SEC EDGAR 通道确立（IR 站点被 Cloudflare 防护挡住的替代方案，DEVLOG §1.3）

**修复**
- EDGAR 403：UA 加联系邮箱（坑 1）
- CDATA 被 stripTags 当标签吃：先去包裹再去标签（坑 2）
- 数字实体解码简化（parseInt radix 混乱，坑 3）

---

*格式约定：每个版本一节，「新增/变更/修复/已知取舍」四类；
版本号规则：架构级变更进主版本号，功能/修复进次号。*
