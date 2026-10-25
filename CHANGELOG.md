# CHANGELOG

> 事实变更清单（叙事版历史见 `DEVLOG.md`）。格式：版本 · 日期 · 变更 · 状态。
> 更新规矩：每次线上部署在这里加一条，DEVLOG 只记「为什么和踩坑」。

## v4.0 · 2026-10-25 · 队列化架构上线（当前线上）

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
