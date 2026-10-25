# 首次 cron 无人值守运行验收手册（v4 · 2026-10-25 晚）

> 这是 v4 上线后**第一次完全无人值守的 cron 运行**的验收清单。按时间顺序执行，
> 每步给出「查什么、合格线、异常时去哪」。约 25 分钟跑完三班。

## 0. 运行时间线（北京时间为准）

| 时刻 | 事件 | 谁干活 |
|---|---|---|
| 21:40 | 抓取班 cron 触发 | Worker 入队 4 条分片消息 |
| ~21:41-21:46 | 4 个抓取消费者并行 | 每片 ~13 子请求，写分片 KV + run 记录 |
| 21:50 | 精选班 cron 触发 | 入队 1 条 digest 消息（**队列级 delay=90s**，21:51:30 前后才投递） |
| ~21:52-21:56 | 精选消费者 | 读 4 分片 → 合并 → enrich → GLM 精选 → 落 latest KV |
| 22:00 | 推送班 cron 触发 | 入队 3 条 send 消息 |
| ~22:01-22:04 | 推送消费者串行 | 读 latest → Telegram 私聊 + 频道逐条发 |

> 手动触发对比（任何时候可用）：`curl -s "https://guji.dpdns.org/api/build"` ——
> 抓取与精选同时入队，靠 90s 延迟 + 消费者内等待保序。手动 build 的 window
> 与 cron 同窗口时**会读到同窗口旧分片**（已知取舍，见 DEVLOG §4.3 / ROADMAP B3）。

## 1. 21:55 查数据层（/api/diag）

```bash
curl -s "https://guji.dpdns.org/api/diag" | python3 -m json.tool | head -40
```

合格线：

| 字段 | 合格值 | 不合格说明 |
|---|---|---|
| `kvFound` | `true` | false = 精选班没落库，进 §3 排查 |
| `digest` | **8-12** | 0 = 模型链全挂，看 `aiTrace`/`zhipuProbe` |
| `digestErr` | `null` | 非空 = 精选抛错了，值是错误摘要 |
| `items` | **≥100**（历史基准 ~143-160） | 明显偏低 = 多个源失败，看 `failures` |
| `generatedAt` | 21:52-21:56 之间（北京时间） | 更早 = 读到旧数据（分片没更新） |
| `aiTrace` | glm-4-plus 尝试记录，`digest` 字段 > 0 | 全 attempt 空 content = 模型侧问题 |
| `zhipuProbe` | `{ok: true, ms: < 5000}` | ok:false = 智谱 key 或网络问题 |

## 2. 21:55 查消费者埋点（KV run 记录）

CLI 看不到远程 KV（只读本地），必须走 REST：

```bash
ACCT=5b749ffbbfbfed51694fb99fbc7b9b12
NS=ca49d41ac8fa4987a0f818cee47237bf
WIN=$(TZ=Asia/Shanghai date +%Y-%m-%d)-pm   # 晚班；早班是 -am
for s in fetch:0 fetch:1 fetch:2 fetch:3 digest send:0 send:1 send:2 dlq; do
  echo "--- $s"
  curl -s "https://api.cloudflare.com/client/v4/accounts/$ACCT/storage/kv/namespaces/$NS/values/guji:v4:run:$WIN:$s" \
    -H "Authorization: Bearer $CF_TOKEN"
done
```

合格线：

- `fetch:0` ~ `fetch:3` **四条全部存在**，`ok:true`（v4.0 早期埋点字段是
  `items/failures/budget`；埋点版是 `stage` 演进记录，最终态 ok:true）
- `digest` 存在且无 `stage: "shard-wait-timeout"` / `"shard-wait-budget"`
- `send:*` 存在（推送完成留痕）
- `dlq` **不存在或空** = 没有消息进死信队列 ✓
- **任何 run 键缺失 = 对应消费者没跑或跑挂了** → 进 §3

## 3. 22:05 查推送结果（终端验收）

- Telegram 私聊 + 频道（t.me/guji_ai_daily）都收到：**1 条报头 + N 条精选**
  （每条一个消息，带链接和来源）
- 网页 https://guji.dpdns.org 分类计数恢复（芯片/模型/访谈等有数，
  顶部「更新于 22:0x」）

## 4. 异常对照表（出问题直接查表）

| 症状 | 先查 | 最可能根因 |
|---|---|---|
| `fetch:*` 有键缺失 | 该分片 run 记录的 `stage` 字段 | 消费者没被调度 / 挂起占槽位（DEVLOG §4.3 坑 14，重新部署可恢复） |
| `fetch` 全在但 `items` 低 | diag 的 `failures` 列表 | 源侧故障（查 `docs/源病历.md`） |
| `digest` 键缺失或有 wait-timeout | 4 个 fetch 是否都 ok | 抓取班没完成 → 延迟不够或消费者挂了 |
| `digest: 0` + zhipuProbe.ok | `aiTrace` 里 glm 的 textLen | 智谱额度/网络；fallback llama 也应启动 |
| TG 什么都没收到 | send run 记录 + `wrangler secret list` | TG_BOT_TOKEN/TG_CHAT_ID 没配或配错 |
| TG 收到但页面旧 | generatedAt 时间 | 精选班写的 KV 被旧数据覆盖竞争（罕见） |
| DLQ 有消息 | dlq run 记录内容 | 对应消费者重试 3-10 次耗尽——按 stage 定位 |

## 5. 验收通过后

1. 在 `ROADMAP.md` 勾掉 B1，记下实测数字（items/digest/耗时）
2. 若全程无异常：v4 队列化**正式交付**，后续只需每日看推送
3. 若有异常：按 §4 定位，修复后更新 DEVLOG 对应章节（**新坑进 DEVLOG，
   这是规矩**——见 ROADMAP 更新指南）
