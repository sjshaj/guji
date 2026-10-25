# 故障排查 Runbook（症状 → 查什么 → 常见根因）

> 原则（来自 DEVLOG §3.3 坑 7）：**被 catch 吞掉的失败 = 不存在的失败**。
> 所以本系统所有失败路径都留痕——排查从「读痕」开始，不要从「猜」开始。
> 痕在哪：① `/api/diag` 的日志字段 ② KV run 记录 ③ DLQ ④ 页面 failures 折叠块。

## 1. 页面问题

### 1.1 页面显示「今日暂无新内容」

| 查 | 去哪 | 说明 |
|---|---|---|
| KV latest 是否存在/新鲜 | `/api/diag` → `kvFound` / `generatedAt` | false = 从没写过；时间旧 = 抓取班挂了 |
| 手动触发一班 | `curl -s "https://guji.dpdns.org/api/build"` | 返回的 `count/failures/logs` 就是答案 |

### 1.2 页面有内容但分类计数不对

按 `docs/源病历.md` 末尾「排查守则」五步走（failures → filter → 窗口 → 限额 → 管道）。

### 1.3 页面打开极慢

- 首次打开会后台触发 build（~15 秒），之后读 KV + 缓存 120 秒，应 <1 秒
- 慢 = 可能撞上 KV 读取竞争或 build 中，等 10 秒自动刷新逻辑兜底

## 2. 推送问题

### 2.1 Telegram 什么都没收到

1. `wrangler secret list`：有 `TG_BOT_TOKEN` / `TG_CHAT_ID` / `TG_CHANNEL` 吗？
   （缺 = 没配，配了才有推送；不配系统退化为纯网站）
2. KV 里 `send:0/1/2` run 记录存在吗？→ 不存在 = 推送班没跑（cron 或队列问题）
3. 存在但 TG 没有 = 去 BotFather 确认 bot 没被删/频道没被踢

### 2.2 收到一部分（比如有报头没条目）

- 推送是**一条精选一条消息**，条间退避 300ms。收一半 = 中途 429 或某条超 4096 字节
- 4096 字节问题已修（`tgTruncate` 按 UTF-8 截断，DEVLOG 坑 7）——
  若复现，查是不是 renderTgItem 里某字段没走截断

### 2.3 收到的是「英文标题清单」不是中文精选

- 这是 **AI 挂了的降级路径**（buildTgMessages 的 fallback 分支，设计行为）
- 去查为什么 AI 挂了：见 §3

### 2.4 沙箱里测 TG 不通

**正常**——沙箱 DNS 污染，`api.telegram.org` 解析到 Facebook IP。
所有 TG 操作必须经 Worker 内部完成（线上 /api/push 或 cron），
沙箱直连测试没有意义（DEVLOG §4.2 环境事实）。

## 3. 精选 / AI 问题

### 3.1 digest = 0（「精选 0 条」）

按序：

1. `/api/diag` → `digestErr`：非空 = 代码级错误，值是错误摘要
2. `/api/diag` → `zhipuProbe`：`ok:false` = 智谱 key 失效/欠费/网络
3. `/api/diag` → `aiTrace`：逐次尝试的 model/ms/textLen/digest/head
   - 全 `textLen: 0` = 模型空 content（Qwen3 思考模式病，DEVLOG 坑 9；
     有 ZHIPU_KEY 时不该发生——没有才退化到 Qwen3）
   - `textLen` 大但 `digest: 0` = parseDigest 解析失败，看 `head` 前 120 字
     是不是模型没按 JSON 格式输出（提示词被污染/模型不听话）
4. 以上全过 = 素材问题：pool 为空（抓取班失败）或全被过滤

### 3.2 精选条数不稳定（时而 12 时而 3）

- `makeDigest` 素材上限 24 条（material = items.slice(0,24)），
  抓取量波动直接改变精选池——**抓取班产出少时精选必然少**，先查抓取
- 模型「觉得」不重要的条目多时也会少，这是模型判断不是 bug

### 3.3 某次 build 特别慢（>60 秒）

- 正常区间：抓取 ~15-25s（enrich 串行 + toMarkdown GPU 冷启动容忍 90s）
- digest 消费者**故意慢**：90 秒队列延迟是设计（等分片就绪）——
  `run:{window}:digest` 的 `at` 比入队晚 90-110s 是**正常**
- 异常慢：查 aiTrace 的 ms 字段，定位是哪个模型/哪次 enrich 卡住

## 4. 队列 / 消费者问题（v4 特有）

### 4.1 run 记录缺失（消费者没跑）

- `wrangler tail` 在沙箱不可用（观测盲区，DEVLOG §4.3 坑 14）——
  能用的只有 KV 埋点
- **重新部署通常恢复**（15:14/15:25 事件的实测经验），
  但根因未定位——重新部署后如果恢复，在 DEVLOG §4.3 补一条记录

### 4.2 DLQ 有消息

- `wrangler queues consumer guji-dlq` 或 REST 查 DLQ 深度；
  dlq run 记录消费者会写日志（只记不推）
- 消息内容看 stage：fetch 的重试耗尽 = 网络；digest 的 10 次重试耗尽 =
  大概率抓取班没完成（等不到分片）

### 4.3 改了 wrangler.toml 的队列配置，行为没变

**先怀疑：配置没生效**（DEVLOG 坑 12：deliveryDelay camelCase 被静默忽略；
坑 13：旧状态污染验证）。检查单：

1. 部署日志有没有 `Unexpected fields` 警告 → 有 = 字段名错（snake_case！）
2. 字段对但行为没变 → 队列级设置（delay/retention）要走 API PUT，
   不信 wrangler.toml（对已存在绑定不生效）
3. 验证前先 DELETE 同 window 的旧分片键，否则「假完成」

## 5. 紧急恢复（Rollback）

```bash
npx wrangler rollback                      # 回退 Worker 代码到上一版本
# 或：wrangler deployments list 选 Known-good 版本重新部署
```

**注意**（交接文档 §8）：rollback 只回 Worker 代码，
**队列级 delay 设置不回滚**（它在队列上不在 Worker 里）——
回滚后如果精选班提前消费，去 ROADMAP B4 的命令重新设置/核实 delay。

## 6. 排查总原则

1. **先读痕再动手**：diag → run 记录 → DLQ → failures，四层痕定位到阶段，
   再改代码。DEVLOG §2.2 的「逐环节条数法」是祖传方法
2. **一次只改一个变量**：splice bug 时期最大的教训就是多处齐改，
   波动环境下无法归因
3. **修完必须补痕**：新坑进 DEVLOG，新状态进源病历，新待办进 ROADMAP——
   这个仓库的价值全靠这三条维护线
