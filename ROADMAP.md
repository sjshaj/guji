# ROADMAP——接下来的更新计划

> 状态更新：2026-10-26 上午。**v4.0.2.2 审查闭环补全（B1 A/B 实验）已部署**
> （C5/D2/B3/D3/D4 五项 + B1 A/B 实验，105 项测试全绿，部署版本 1a09990f）。
> 前情：v4.0.2 精选环节五项改进 + 审查链收尾已部署
> （P0-④②③① + P1-⑤ 精选五项 + B2 sendBatch + D2 fixture/dry-run + C5/C6 重构，
> 118 项测试全绿，部署版本 29f4dedd，5 队列就位，diag 200，生产 dry-run 实证，
> digest delay=90 无恙，6 触发点在线）。
> v4 首班无人值守 cron 验收满分通过（21:40 抓取 4 分片全 ok 834→149 条 /
> 21:50 精选 9 条 digestErr 空 / 22:00 推送 20 条全送达，基线：items 149、
> digest 9、预算峰值 14/50）。外部审查链已形成五份文档（PR #1 审查 →
> 作者复核一 → PR #3 回复 → PR #4 调研 → 作者复核二，均存 `docs/`）。
> **下一步：今晚 pm 班观察 B1 A/B 的 `b1-ab` 采集是否出现在 `/api/diag` aiTrace；
> 设置 DIAG_KEY secret 让 C5 灰度生效；
> 随后按需开 D1 拆模块（需单独排期，当前 2386 行仍单文件）。**
>
> 本文件是「续写接口」：完成一项就划掉一项并记录日期，新增想法直接追加到对应分区。
> 每个条目都标注了**动机/背景**，方便其他 AI 理解为什么要做。

---

## A. 安全（最高优先级，与功能无关）

- [ ] **A1. 轮换在早期对话中明文出现过的 Cloudflare API token**
      - 背景：项目初期 token 以明文形式在对话中传递（现应视为已泄露）
      - 操作：CF 后台 → My Profile → API Tokens → 吊销旧 token → 新建同权限 token
        → 更新本地部署环境变量
      - 验收：旧 token 调 `/user` 返回 401；新 token 可正常部署
- [ ] **A2. 给 Cloudflare 账号开启 2FA**（同背景，token 泄露过 = 密码也可能撞库）
- [ ] **A3. 确认 Telegram bot token 与智谱 key 仅存在于 `wrangler secret`**
      - 验证：`npx wrangler secret list` 应只有 TG_BOT_TOKEN / TG_CHAT_ID / ZHIPU_API_KEY
      - 任何源码文件出现 token = 立即吊销 + 从 git 历史抹除（bfg / filter-repo）
- [ ] **A4. 仓库公开前全量密钥扫描**（每次 push 前跑）：
  ```bash
  grep -rInE 'cfat_[A-Za-z0-9]+|[0-9]{8,10}:AA[A-Za-z0-9_-]{30,}|\.[A-Za-z0-9]{20,}' .
  # 预期零输出
  ```

## B. v4 稳定性（线上运行中暴露的）

- [x] **B1. 今晚 21:40 首验**：v4 首次无人值守 cron（北京 21:40/21:50/22:00 晚班三连）
      - 完整步骤见 `docs/首次cron验收手册.md`（按时间序：diag 字段合格线 → KV run
        记录四条 fetch 全 ok → TG 收报头+N 条 → 页面计数恢复）
      - 基准线：items ≥100、digest 8-12、digestErr 空、4 分片全 ok、DLQ 空
      - **已完成（10-25 22:00，满分）**：详见「已完成」区首班验收条目
- [ ] **B2. 定位「分片消费者成批消失」根因**（DEVLOG §5.5 事件）
      - 现状：15:14/15:25 两次 build 出现 4 个抓取消费者只 1 个完成、其余无痕迹消失；
        重新部署后恢复；沙箱 wrangler tail 不可用，根因未定位
      - 思路：在有 tail 权限的环境复现；或在 consumeFetch 的 fetch 调用加
        AbortSignal.timeout（当前 AI_MS=150000 容忍可能过长，挂起调用占满
        max_concurrency=4 槽位）
- [x] **B3. 分片新鲜度校验**（DEVLOG §5.4 已知取舍）
      - 现状：digest 消费者只检查分片键存在、不校验新鲜度，同 window 二次 build 会读到上轮分片
      - 方案：分片值里已有 `at` 字段，消费时与消息入队时间比对，过期分片触发重试而非误用
      - **已完成（10-26 凌晨，v4.0.2.1）**：`isStaleShard`——digest 消息体带 `at`
        （入队时刻），分片 at 早于其 10 分钟以上判未就绪继续等；超时区分
        `shard-stale-timeout` / `shard-wait-timeout` 两种 stage。真实触发要等
        下次队列异常，行为由 smoke 5 项锁定（DEVLOG §4.7）
- [x] **B4. 每次部署后核实队列级 delivery_delay**（DEVLOG §5.3 遗留运维项）
      - 命令：`PUT /accounts/{acct}/queues/{queue_id}` 读回 settings 确认 delay=90
      - 建议固化为部署脚本最后一步
      - **已完成（10-25 深夜）**：v4.0.1 部署后核实 guji-digest 队列级
        `delivery_delay=90` 未受影响；后续部署仍需重复本项（每次部署都查）
- [ ] **B5. 外部 AI 审查修复批次**（2026-10-25，PR #1，顺序已按作者复核修正）
      - 来源：`docs/外部AI审查_2026-10-25.md`（外部 AI）+ `docs/外部AI审查_作者复核.md`（作者立场：10 项认同 / 4 项打折扣）
      - [x] **批次 1**（正确性 + 删减，半天）：A1 推送失败补推机制（注意：Queues retry 是消息级粒度，失败项需设计补推，倾向单独补推队列）+ A2 429 `retry_after` 退避 + A3 `parseTagValue:false` + A4 注释修正（「仅对新建队列有效，生产 truth 在 CF 后台」）+ C1 `semicapid` 删除 + C2 anthropic 特例删除改注释 + C3 Qwen3 开关注释改「历史遗留实测无效」+ C4 probeAI 改探生产链模型 + `batch[0]` 依赖 `max_batch_size=1` 注释
        - **已完成（10-25 深夜）**：70 项测试全绿、部署 b10295b4、guji-retry 队列就位、
          diag 200、C4 线上实证（aiProbe = llama-3.1-8b）、digest delay=90 核实无恙。
          设计决策与 A1 补推队列细节见 DEVLOG §4.5
      - **批次 2**（效率，1 天）：B2 `sendBatch` 批量入队 + `/api/diag` 双探测
        - **已完成（10-26 凌晨）**：B2 落地（cron 班子请求 5→2，保序 + 串行消费者）；
          diag 双探测其实已在 v4.0.1 随 C4 完成（aiProbe + zhipuProbe 并存）
        - **B1 已完成（10-26 上午）**：A/B 实验落地（v4.0.2.2）——复用已抓 HTML 裸跑 HTMLRewriter，与 toMarkdown 两版 desc 各取前 200 字记 aiTrace，不替换生产输出；今晚 pm 班起可在 `/api/diag` 人眼比对质量后决定是否正式上
      - **批次 3**（重构，1 天）：C5 提 `mkItem/markRun/sliceChunks` helper + C6 魔法数字入 §1 config + D2 fixture 快照契约测试（dry-run 限定只抓取解析、不 enrich/AI/推送）
        - **已完成（10-26 凌晨）**：三 helper + 15 个常量 + 两个真实 fixture +
          dry-run 路由；fixture 契约还实证推翻了「真实 EDGAR atom 会解析失效」的
          静态分析误判（DEVLOG §4.6.3）
      - **批次 4**（可选）：D1 按 15 区拆模块——先拆无依赖区、每次一区、每区跑 118 项测试
      - **批次 5**（可选，需前置数据）：漏合并率统计已结题（见下）；**ETag/304 条件请求**待 CPU 实测
      - **B4 已完成（10-26 凌晨）**：两轮离线统计 106 条原始条目、跨源同事件
        候选 0 个 → 维持「低则不上」；方向由 P1-⑤ embedding 聚类覆盖。
        数据与结论回写 `docs/外部AI审查_作者复核.md`
      - **ETag/304 仍待 CPU 实测**（未做；注意：与 B 区 B3「分片新鲜度」同名不同事，
        后者已于 v4.0.2.1 完成）
      - 总纪律：改前全绿、改后复跑、部署后 `/api/diag` + 页面计数 + 推送验证、判别实验前先清 KV 旧键（DEVLOG 坑 13）

## C. 功能增强（用户提过或明确想要的）

- [ ] **C1. 但斌/雪球来源接入**
      - 背景：用户 10-24 明确要关注但斌（国内知名投资家）；雪球页面有 Cloudflare 防护，
        直抓可能 403——先调研 xueqiu.com/butor 的可达性，再定 RSS 网关还是邮件方案
- [ ] **C2. 更多硬件/数据中心源**
      - 现 B 组靠 SEC EDGAR 覆盖；可评估 Arista/Arista 博客、半导体垂直原创媒体
      - 原则不变：一手优先，二手聚合器不要
- [ ] **C3. Qwen 熔断器**（DEVLOG §4.2 的模型层遗留）
      - 现状：glm-4-plus 主力 + llama 兜底；Workers AI Qwen3 空 content 问题仍在
      - 方案：同一班次内某模型连续 2 次空 content → 自动切兜底并记 run 记录
- [ ] **C4. enrich 结果缓存**
      - 背景：toMarkdown 增强每条都是一个 AI 调用，同一条新闻早晚班可能重复增强
      - 方案：按 URL hash 缓存 600 字摘要到 KV，TTL 7 天
- [x] **C5. `/api/diag` 加鉴权**
      - 现状：诊断端点公开可读（暴露源健康度、预算消耗——信息量不大但没必要公开）
      - 方案：简单的 secret query param 或 CF Access
      - **已完成（10-26 凌晨，v4.0.2.1）**：`diagAuthorized` 灰度——配置 `DIAG_KEY`
        secret 则 `?key=` 精确匹配，未配置则放行（代码先上、secret 后置，设置即生效）。
        **当前生产仍免鉴权（DIAG_KEY 未设置），设置方法见 README「密钥管理」**；
        GitHub repo 同名 secret 是 D3 巡检的前置
- [x] **C6. §10 精选环节改进**（外部调研 PR #4 + `docs/外部AI调研_作者复核.md`）
      - **已完成（10-26 凌晨，部署 29f4dedd）**，五项全部落地：
      - [x] P0-③ 代码级数字/专名校验：单位归一化 + 型号模式抽取 + 有意收窄
        （纯整数/纯字母不校验防误杀）；剔除明细记 console.error
      - [x] P0-② 提示词硬约束清单：原文可找/宁省略不补全/相对时间照抄/排他仅原文明示
      - [x] P0-④ temperature 0.3→0.1（DIGEST_TEMPERATURE）
      - [x] P0-① 四维 rubric：normScore 容错 + 总分降序取 12 + 自报分只排序不设阈值
      - [x] P1-⑤ embedding 聚类 v1：cosine ≥ 0.85 从严 + 48h 门限 + near 记录 +
        失败整体跳过；**上线后人工抽查一周**（观察项：每天 digest 的 aiTrace 里
        cluster/near 计数与合并合理性）
      - **不采纳**：⑥ DeepSeek 模型分层（第二供应商负优化，且「V4-Flash」型号存疑）；
        ⑦ Qwen3-235B 兜底（免费档神经元账未算，先查模型目录与定价再定）
      - **长期**：⑩ 自建评测集（100-200 条人工标注「该不该上日报」）——「没有评测集，
        每次改提示词都是碰运气」，与 ⑥ 的打分需求合并为同一试验田

## D. 工程化（长期健康）

- [ ] **D1. 物理拆模块**（交接文档 §5.2 开放项）
      - 现状：index.js 2386 行单文件，十五个区靠注释分隔
      - 方案：按 §1-§15 拆 ESM 模块（wrangler 支持多入口/打包），保持「一个区一个文件」
      - 注意：拆完必须两个测试文件全绿 + 生产验证后才能部署
- [x] **D2. DLQ 告警**
      - 现状：DLQ 消费者只记 KV 日志，没人看
      - 方案：DLQ 消息落 KV 时，若班次结束仍非空 → 推送一条「⚠️ 今日有 N 条消息进 DLQ」给自己
      - **已完成（10-26 凌晨，v4.0.2.1）**：consumeDlq 按班次累加 KV 计数 →
        consumeSend 最后分片（班次收尾点）读取，>0 且未告警过则私聊 TG_CHAT_ID；
        alerted 键按 window 去重、try/catch 不阻断主流程。无死信时不触发，
        行为由 smoke 锁定（DEVLOG §4.7）
- [x] **D3. GitHub Actions 定时健康检查**（仓库公开后的自然延伸）
      - 每天 UTC 22:30 跑一次 `/api/diag` + smoke 测试，失败开 issue
      - **已完成（10-26 凌晨，v4.0.2.1）**：`.github/workflows/health.yml`，UTC 22:30
        （北京 06:30 早班推送后）+ workflow_dispatch；跑 smoke+e2e+diag HTTP 检查，
        digestErr 非空或非 200 即失败，自动开/跟进 health 标签 issue。
        前置：repo secret `DIAG_KEY`（与 CF secret 同名同值）——**未设置前 diag job
        会因端点放行而通过，设置后才真正校验**
- [x] **D4. 源健康度仪表盘**
      - 现状： failures 只在 build 响应里
      - 方案：`/api/diag` 增加「近 7 天各源成功率」汇总（run 记录里已有数据）
      - **已完成（10-26 凌晨，v4.0.2.1）**：`?health=1` 聚合**近 3 天**（7 天需 56 次
        KV 读超 50 子请求预算，取舍记录在代码注释）；seen=该源所在分片全部班次、
        分片整体失败退化为全源各记一次失败、rate 不除零；markRun 补 failedSources。
        基线从 10-26 am 班起逐班积累

---

## 已完成（存档）

<details>
<summary>展开查看已完成项</summary>

- [x] **v4 队列化上线**（10-25 15:43 全链路复测正常：143 条、精选 12、digestErr 空）
- [x] **90 秒延迟投递就位**（10-25，t0+104s 验证通过；wrangler.toml snake_case 修正 +
      队列级 API 设置，见 DEVLOG §5.3）
- [x] **诊断埋点**（10-25，consumeFetch 全阶段 + consumeDigest 早退路径留痕）
- [x] **交接文档 v4 化**（2452 行，含密钥断言与行数校验）
- [x] **智谱 GLM-4-plus 集成**（10-25 凌晨，解决 Qwen3 空 content）
- [x] **每天两次班次 + 上次精选时间记忆**（10-24 深夜）
- [x] **toMarkdown 替代 Jina Reader**（10-24 下午，Error 1102 三重降压后）
- [x] **SEC EDGAR 源方案**（10-24 中午，替代被 Cloudflare 防护挡住的 B 组硬件 IR 站点）
- [x] **「只有 2 个源」splice bug 修复**（10-24 13:07，160 条/班）
- [x] **GitHub 公开仓库 + 完整开发日志**（10-25 17:30）
- [x] **仓库文档缺口补齐**（10-25 18:20）：LICENSE（MIT 落地）、首次 cron 验收手册、
      39 源健康病历、故障排查 Runbook、CHANGELOG、本地开发指南；
      feeds-list 快照与未引用 logo 归档 docs/archive/
- [x] **外部 AI 审查闭环**（10-25 21:20）：PR #1 零上下文审查报告合并入库；
      作者 AI 逐项复核（10 认同 / 4 打折扣）写入 `docs/外部AI审查_作者复核.md`，
      修复批次与修正落入 B5
- [x] **v4 首班无人值守 cron 验收**（10-25 22:00 推送完成，满分通过）：抓取 4 分片
      全 ok（834 条原始、1 源失败在容错内）、digest 9 条（8-12 线内）、digestErr 空、
      GLM 一次成功 13.4s、推送 20 条全送达（私聊 14 + 频道 6）、预算峰值 14/50
- [x] **外部审查链第五份**（10-25 22:40）：PR #3 审查者回复（认账 A1、接受 B1/B4 打折）、
      PR #4 §10 专项调研（P0 三项/P1 一项限定采纳、⑥⑦不采纳、指正 2502.15242 引用错误）、
      `docs/外部AI调研_作者复核.md` 合并入库，处置落入 C6
- [x] **B5 批次 1：外部审查修复落地**（10-25 深夜，部署 b10295b4）：A1 补推队列
      （guji-retry，消息级 retry 粒度陷阱的解法见 DEVLOG §4.5）+ A2 retry_after 退避 +
      A3 parseTagValue:false + A4 delivery_delay 注释（wrangler 4.152 deprecated 警告实证）+
      C1 semicapid 删除 + C2 anthropic 特例删除 + C3 Qwen3 开关注释 + C4 probeAI 探生产链模型
      （线上 aiProbe = llama-3.1-8b 实证）；smoke 17→31 项；70 项测试全绿；
      5 队列就位；digest delay=90 部署后核实无恙（B4 同步完成）
- [x] **明早 05:40 观察 v4.0.1 首班**（待办）：重点看补推队列零触发（= 首班推送无失败，
      A1 机制待命）、digest 班预算变化、`send:{slice}:failed` run 记录是否出现
- [x] **B5 批次 2/3/5 + C6 精选五项全部落地**（10-26 凌晨，部署 29f4dedd）：
      精选 P0-④②③① + P1-⑤（temperature 0.1 / 硬约束清单 / 代码级校验含有意收窄 /
      四维 rubric 容错排序 / embedding 聚类 0.85 从严）+ B2 sendBatch（cron 班子请求 5→2）+
      D2 fixture + dry-run 路由（生产实证分片 0：234→61，0 失败）+ C5 三 helper +
      C6 15 常量；B4 量化结论「低则不上 SimHash」（106 条样本 0 跨源候选）；
      smoke 31→79，合计 **118 项测试全绿**；diag 200 + 页面 200 + digest delay=90 无恙
- [ ] **明早 05:40 观察 v4.0.2 首班**（下一待办）：重点看 digest 的 aiTrace 里
      cluster/near 计数与合并合理性（P1-⑤ 上线后人工抽查一周的开始）、
      rubric 后 digest 质量（9 条基线是否维持）、补推队列零触发、
      `send:{slice}:failed` 是否出现

</details>

---

*更新指南：完成条目时把 `[ ]` 改 `[x]` 并移到「已完成」区，注明日期；
新想法直接加对应分区，写清动机。保持这份文件与线上现状同步。*
