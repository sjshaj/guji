# CHANGELOG

> 事实变更清单（叙事版历史见 `DEVLOG.md`）。格式：版本 · 日期 · 变更 · 状态。
> 更新规矩：每次线上部署在这里加一条，DEVLOG 只记「为什么和踩坑」。

## v4.0.1 · 2026-10-25 深夜 · 外部审查驱动修复批次（当前线上）

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

## v4.0.2 · 2026-10-26 凌晨 · 精选环节五项改进 + 审查链收尾（当前线上）

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
- **新 digest 路径未手动触发**（会砍掉 05:50 定时班 fresh 窗口，DEVLOG §4.6.5），
  留给 05:40-06:00 无人值守首班自然验收

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
