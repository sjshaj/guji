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
ok(m.PLAN_PEAK === 33 && m.PLAN_PEAK <= m.SUBREQUEST_LIMIT, `预算峰值 ${m.PLAN_PEAK}/50 ≤ 上限（v4.0.2.4 B2 账实对齐：send.slice 19→20、digest 29→33）`);
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

// B2：sendBatch 批量入队（一次调用发 N 条，计 1 个子请求）
const bBatch = m.createBudget({ silent: true });
const netBatch = m.createNet({}, bBatch);
let batchCalls = 0, batchArg = null;
netBatch.qSendBatch({ sendBatch: async (bodies) => { batchCalls++; batchArg = bodies; } }, [{ a: 1 }, { a: 2 }, { a: 3 }], 'test:batch');
await netBatch.qSendBatch({ sendBatch: async () => {} }, [{ a: 1 }, { a: 2 }], 'test:batch2');
ok(batchCalls === 1 && batchArg.length === 3 && bBatch.report().byKind.queue === 2, 'B2：sendBatch 一次调用发 3 条，计 1 个子请求（两次调用 = 2）');
ok(bBatch.report().byKind.queue === 2 && !bBatch.report().breached, 'B2：sendBatch 记账正确，未越限');
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

// ---- v4.0.1 外部审查修复 ----
// A2：TG 429 尊重 retry_after（秒 → ms + 250ms 缓冲，上限 30s，兜底 MSG_RETRY_MS）
ok(m.retryAfterMs({ parameters: { retry_after: 5 } }) === 5250, 'retryAfterMs：retry_after=5s → 5250ms');
ok(m.retryAfterMs({ parameters: { retry_after: 120 } }) === 30000, 'retryAfterMs：上限 30s');
ok(m.retryAfterMs({ ok: false, description: 'x' }) === 1000, 'retryAfterMs：无 retry_after → 兜底 1000ms');
ok(m.retryAfterMs(null) === 1000, 'retryAfterMs：null 安全');

// A1：TG 永久错误分流（瞬时错误不误判为永久，永久错误不漏判）
ok(m.isPermanentTgError('Bad Request: chat not found'), 'isPermanentTgError：chat not found → 永久');
ok(m.isPermanentTgError('Forbidden: bot was blocked by the user'), 'isPermanentTgError：被拉黑 → 永久');
ok(m.isPermanentTgError('Forbidden: bot can\'t initiate conversation with a user'), 'isPermanentTgError：不能发起会话 → 永久');
ok(!m.isPermanentTgError('HTTP 429'), 'isPermanentTgError：429 → 瞬时（不误判）');
ok(!m.isPermanentTgError('network error / fetch failed'), 'isPermanentTgError：网络错误 → 瞬时');
ok(!m.isPermanentTgError(null), 'isPermanentTgError：null 安全');
ok(m.RETRY_MAX_ROUNDS === 3, 'RETRY_MAX_ROUNDS = 3（补推轮数上限）');

// A3：parseTagValue:false——纯数字文本保持字符串，不丢前导零/精度
const numFeed = `<?xml version="1.0"?><rss><channel><title>t</title><item><title>条目</title><link>https://a.com/1</link><description>d</description><pubDate>${new Date().toUTCString()}</pubDate><guid>0012</guid></item></channel></rss>`;
const numParsed = m.parseXmlFeed ? m.parseXmlFeed(numFeed, 'test', '模型') : null;
ok(numParsed && numParsed.length === 1, 'A3：含纯数字 guid 的 feed 正常解析出条目');

// C1：semicapid 死分支已删（含 semiconductor 的标题不再被误归数据中心）
ok(m.recat({ cat: '模型', title: 'Nvidia semiconductor 新芯片' }).cat === '模型', 'C1：semiconductor 不再触发重分类（分支已删）');
ok(m.recat({ cat: '模型', title: 'Nvidia datacenter 扩张' }).cat === '数据中心', 'C1：datacenter 仍正常重分类');

// ---- v4.0.2 精选环节改进（调研 P0，作者复核采纳） ----
// ④ temperature 0.1（低温降编造）
ok(m.DIGEST_TEMPERATURE === 0.1, 'P0-④：DIGEST_TEMPERATURE = 0.1');
ok(m.buildPrompt(items).includes('硬约束'), 'P0-②：提示词含硬约束清单段');

// ③ 忠实度校验：数字（归一化）/型号两类，纯整数与纯字母名不校验（裁量防误杀）
ok(m.checkFaithful('营收达 35 亿美元', 'Nvidia 发布新芯片，营收 $3.5 billion').length === 0, 'P0-③：「35 亿」与「$3.5 billion」归一化相等，不误杀');
ok(m.checkFaithful('Nvidia 发布 RTX 4090 显卡', 'Nvidia 推出 RTX-4090 桌面显卡').length === 0, 'P0-③：型号连字符差异归一化后不误杀');
ok(m.checkFaithful('营收达 80 亿美元', 'Nvidia 财报显示营收 $3.5 billion').length > 0, 'P0-③：数字与原文不符 → 拦截');
ok(m.checkFaithful('OpenAI 发布 GPT-6', 'OpenAI 发布 GPT-5 模型').length > 0, 'P0-③：编造型号 GPT-6 原文没有 → 拦截');
ok(m.checkFaithful('英伟达发布第五代架构', 'Nvidia unveils fifth-gen architecture').length === 0, 'P0-③：中文音译产品名 + 纯整数不校验，不误杀');
ok(m.checkFaithful('', '原文').length === 0 && m.checkFaithful('摘要', '').length === 0, 'P0-③：空输入安全');

// ① rubric 化：四维自报分只排序不设阈值；旧格式兼容；畸形分数容错
const rubricMat = items.slice(0, 5).map((it, i) => ({ ...it, link: 'l' + (i + 1) }));
const rubricOut = m.parseDigest(JSON.stringify([
  { idx: 1, reason: '重要', scores: { impact: 9, density: 8, authority: 9, timeliness: 7 }, title: '低分条', summary: 's1' },
  { idx: 2, reason: '更重要', scores: { impact: 10, density: 9, authority: 9, timeliness: 9 }, title: '高分条', summary: 's2' },
  { idx: 3, reason: '旧格式无分数', title: '旧格式条', summary: 's3' },
  { idx: 4, reason: '畸形分数', scores: { impact: '8', density: 99, authority: -3 }, title: '畸形条', summary: 's4' },
]), rubricMat);
ok(rubricOut.length === 4, 'P0-①：四条全部解析（含旧格式与畸形分数）');
ok(rubricOut[0].title === '高分条', 'P0-①：按四维总分降序，最高分排首');
ok(rubricOut[2].title === '旧格式条' || rubricOut[3].title === '旧格式条', 'P0-①：旧格式条目得中性分（20），不淘汰');
ok(rubricOut.find(x => x.title === '畸形条').score.impact === 8 && rubricOut.find(x => x.title === '畸形条').score.density === 10 && rubricOut.find(x => x.title === '畸形条').score.timeliness === 5, 'P0-①：畸形分数归一化（字符串/超范围/缺维 → 夹 1-10，缺维中性 5）');
ok(rubricOut.every(x => x.score.total >= 4 && x.score.total <= 40), 'P0-①：总分范围 4-40');
ok(m.buildPrompt(items).includes('scores') && m.buildPrompt(items).includes('reason'), 'P0-①：提示词输出契约含 reason + scores');

// v4.0.2.4（外部审查 A1 实锤）：截断的嵌套 rubric 输出 → 第三级容错必须仍恢复完整条目
// 模拟 max_tokens 截断：entry1 完整（含嵌套 scores），entry2 写到一半被切断，且无收尾 ]
// 旧正则 /\{[^{}]*\}/g 遇内层 { 即断，只抽到 scores 碎片（无 idx）→ 全 0 条（容错链变两级半）
const truncatedRubric = '[{"idx":1,"reason":"r","scores":{"impact":8,"density":7,"authority":9,"timeliness":8},"title":"T1","summary":"S1"},{"idx":2,"reason":"r2","scores":{"impact":6,"density":5,';
const dTrunc = m.parseDigest(truncatedRubric, rubricMat);
ok(dTrunc.length === 1 && dTrunc[0].title === 'T1' && dTrunc[0].score.impact === 8, 'A1：截断嵌套 rubric → 第三级容错恢复完整条目（旧正则会丢光成 0 条）');

// ⑤ embedding 事件聚类（限定采纳：0.85 从严 + 48h 门限 + 拿不准不合并）
// 注意 ts 用毫秒（线上契约：pickDate 返回 Date.parse 毫秒值；toSec 期望毫秒入参）
ok(m.CLUSTER_SIMILARITY === 0.85, 'P1-⑤：阈值 0.85（从严，不用调研建议的 0.8）');
ok(m.CLUSTER_WINDOW_H === 48, 'P1-⑤：时间门限 48h');
const v1 = [1, 0, 0], v2 = [1, 0, 0], v3 = [0, 1, 0];
ok(m.cosineSim(v1, v2) === 1 && m.cosineSim(v1, v3) === 0, 'cosineSim：同向 1 / 正交 0');
ok(m.cosineSim(null, v1) === 0 && m.cosineSim([1, 2], [1, 2, 3]) === 0, 'cosineSim：null 与长度不匹配安全');
const nowMs = Date.now();
const clMat = [
  { title: 'OpenAI 发布 GPT-5', ts: nowMs },           // 0
  { title: 'OpenAI launches GPT-5 model', ts: nowMs - 3600 * 1000 }, // 1 同事件（1h 内，相似度高）
  { title: 'Google 发布 Gemini 3', ts: nowMs },         // 2 不同事件
  { title: 'OpenAI 旧闻回顾', ts: nowMs - 72 * 3600 * 1000 }, // 3 超 48h 门限
];
const clEmb = [[1, 0.9, 0], [0.95, 0.9, 0], [0, 0, 1], [1, 0.9, 0]];
const clRes = m.clusterItems(clMat, clEmb);
ok(clRes.clusters.length === 3 && clRes.clusters.some(g => g.length === 2 && g.includes(0) && g.includes(1)), 'P1-⑤：同事件 0/1 合并，异事件 2 独立');
ok(clRes.clusters.some(g => g.length === 1 && g[0] === 3), 'P1-⑤：超 48h 门限不与 0 合并');
const clNear = m.clusterItems(
  [{ title: 'a', ts: nowMs }, { title: 'b', ts: nowMs - 1000 }, { title: 'c', ts: nowMs }, { title: 'd', ts: nowMs - 72 * 3600 * 1000 }],
  [[1, 0], [0.82, 0.5724], [0, 1], [1, 0.9, 0]] // 0-1 余弦精确 0.82（拿不准区间）
);
ok(clNear.near.length >= 1 && clNear.near[0].sim >= 0.8 && clNear.near[0].sim < 0.85, 'P1-⑤：0.80-0.85 接近对记 near（拿不准不合并）');
ok(clNear.clusters.every(g => !(g.length === 2 && g.includes(0) && g.includes(1))), 'P1-⑤：接近对不合入簇');
ok(m.clusterItems([{ title: 'x', ts: nowMs }], [[1, 0]]).clusters.length === 1, 'P1-⑤：单条不聚类');
ok(m.clusterItems([{ title: 'x', ts: 0 }, { title: 'y', ts: 0 }], [[1, 0], [1, 0]]).clusters.length === 2, 'P1-⑤：ts=0 无日期条目从严不聚类');

// ---- C5/C6：公共 helper 与命名常量（外部审查批次 3，作者认同） ----
// mkItem：三处解析器统一的条目构造（source 六字段透传 + 截断命名化）
const mkSrc = { id: 't', name: 'Test', cat: '模型', kind: 'rss', url: 'https://t.com', win: '72h', filter: 1 };
const mkIt = m.mkItem(mkSrc, { title: '标题', link: 'https://t.com/1', ts: 123, desc: 'd' });
ok(mkIt.source === 'Test' && mkIt.srcId === 't' && mkIt.cat === '模型' && mkIt.kind === 'rss' && mkIt.win === '72h' && mkIt.filter === 1, 'C5：mkItem 透传 source 六字段');
ok(m.mkItem(mkSrc, { title: 'x'.repeat(400), link: 'y'.repeat(600), desc: 'z'.repeat(700) }).title.length === 300
  && m.mkItem(mkSrc, { title: 'x'.repeat(400), link: 'y'.repeat(600), desc: 'z'.repeat(700) }).link.length === 500
  && m.mkItem(mkSrc, { title: 'x'.repeat(400), link: 'y'.repeat(600), desc: 'z'.repeat(700) }).desc.length === 600, 'C6：mkItem 按 TITLE/LINK/DESC 常量截断');
ok(m.mkItem(mkSrc, { title: 't', desc: 'z'.repeat(700) }, 300).desc.length === 300, 'C6：mkItem descMax 参数（EDGAR 用 300）');
ok(m.mkItem(mkSrc, {}).ts === 0 && m.mkItem(mkSrc, {}).desc === '', 'C5：mkItem 缺省字段安全');

// sliceChunks：两处切分逻辑统一
const sc = m.sliceChunks([1, 2, 3, 4, 5, 6, 7], 3);
ok(sc.length === 3 && sc[0].length === 3 && sc[2].length === 1, 'C5：sliceChunks 均切（末片承接余数）');
ok(m.sliceChunks([1, 2], 3)[2].length === 0, 'C5：sliceChunks 余数为 0 时末片为空（调用方处理）');
ok(m.sliceChunks([1, 2, 3], 1).length === 1 && m.sliceChunks([], 3).every(c => c.length === 0), 'C5：sliceChunks 单片与空输入');

// C6：命名常量存在且值正确（截断纪律入 config）
ok(m.RUN_TTL === 2 * 86400 && m.PARSE_ITEMS_MAX === 40 && m.EDGAR_ENTRIES_MAX === 8, 'C6：RUN_TTL / 解析上限常量就位');
ok(m.ERR_RECORD_MAX === 300 && m.AI_TRACE_HEAD_MAX === 120, 'C6：错误截断双档常量就位');

// filterFaithful：剔除未过校验条目并留痕，保留合格条目
const mat = [{ title: 'Nvidia 财报', desc: '营收 $3.5 billion，创纪录', link: 'l1', cat: '模型' }];
const fRes = m.filterFaithful([
  { title: '好条目', summary: '营收 35 亿美元', link: 'l1' },
  { title: '编造条目', summary: '营收 80 亿美元', link: 'l1' },
], mat);
ok(fRes.kept.length === 1 && fRes.kept[0].title === '好条目' && fRes.rejected.length === 1, 'P0-③：filterFaithful 剔除编造条目、保留合格条目');

// ---- D2 fixture 契约测试（外部审查 D2，作者认同；dry-run 边界见 /api/fetch?dry=1） ----
// 快照为 2026-10-25 抓取的真实 feed 原样截取（非构造）——源格式变化在这里红，
// 而不是在线上日报里红（DEVLOG 坑 2 CDATA 就是这么栽的）。
import { readFileSync } from 'node:fs';
const FX = (n) => readFileSync(new URL(`./__fixtures__/${n}`, import.meta.url), 'utf8');

// RSS（OpenAI 真实源）：CDATA 标题被正确解包（坑 2 回归锁）、字段完整
const fxRss = m.parseXmlFeed(FX('rss-openai.xml'), { id: 'openai', name: 'OpenAI', cat: '模型', kind: 'rss', url: 'https://openai.com/news/rss.xml' });
ok(fxRss.length === 3, 'fixture RSS：3 个条目全部解析');
ok(fxRss.every(it => it.title && !/[<>]/.test(it.title)), 'fixture RSS：标题非空且无标签残留（CDATA 解包）');
ok(fxRss.every(it => /^https?:\/\//.test(it.link)), 'fixture RSS：link 均为 http(s)');
ok(fxRss.every(it => Number.isFinite(it.ts) && it.ts > 1.6e12), 'fixture RSS：ts 为近期毫秒时间戳');
ok(fxRss.every(it => it.desc.length <= 600), 'fixture RSS：desc ≤ 600 字截断');

// EDGAR atom（Apple 真实源，ISO-8859-1）：Item 事项提取 + Archives 链接 + 日期
const fxEdgar = m.parseEdgar(FX('atom-edgar.xml'), { id: 'e-aapl', name: 'Apple', kind: 'edgar', cat: '芯片算力', cik: '0000320193', type: '8-K' });
ok(fxEdgar.length > 0 && fxEdgar.length <= 8, 'fixture EDGAR：有条目且 ≤ 8（count=10 的 slice）');
ok(fxEdgar[0].title.length > 'Apple · 8-K 事件'.length, 'fixture EDGAR：title 提取到 Item 事项文本（正则捕获组消费 "Item 5.02: " 前缀，主题进 title）');
ok(fxEdgar[0].desc.includes('Item 5.02'), 'fixture EDGAR：desc 含 Item 编号原文');
ok(/sec\.gov\/Archives\//.test(fxEdgar[0].link), 'fixture EDGAR：link 指向 Archives 文档');
ok(fxEdgar[0].desc.includes('Filed:'), 'fixture EDGAR：desc 含 Filed/AccNo 摘要');

// 边缘 RSS（构造，但每个元素对应一个真实坑）：纯数字 guid（A3）+ 实体 + 无日期
const fxEdge = `<?xml version="1.0"?><rss><channel><title>t</title>
<item><title>Edge &amp; Case</title><link>https://a.com/1</link><description>desc &lt;b&gt;bold&lt;/b&gt;</description><guid>0012</guid></item>
<item><title>No Date</title><link>https://a.com/2</link><description>d</description></item>
</channel></rss>`;
const fxEdgeOut = m.parseXmlFeed(fxEdge, { id: 'edge', name: 'Edge', cat: '模型', kind: 'rss', url: 'https://a.com' });
ok(fxEdgeOut.length === 2, 'fixture 边缘：2 条（无日期条目不在此处判死）');
ok(fxEdgeOut[0].title === 'Edge & Case' && fxEdgeOut[0].desc === 'desc bold', 'fixture 边缘：实体解码正确（&amp; &lt; 还原，无双重解码）');

// ---- v4.0.2.1 批次 6：C5 鉴权 / D2 DLQ 告警 / B3 新鲜度 / D4 健康度 ----

// C5 diag 鉴权（未配置放行=灰度；配置后精确匹配）
ok(m.diagAuthorized('x', {}).ok === true, 'diagAuthorized：未配置 DIAG_KEY → 放行（灰度上线）');
ok(m.diagAuthorized('s3cret', { DIAG_KEY: 's3cret' }).ok === true, 'diagAuthorized：key 正确 → 放行');
ok(m.diagAuthorized('wrong', { DIAG_KEY: 's3cret' }).ok === false, 'diagAuthorized：key 错误 → 拒绝');
ok(m.diagAuthorized(null, { DIAG_KEY: 's3cret' }).ok === false, 'diagAuthorized：缺 key → 拒绝');
ok(m.diagAuthorized('s3cret', { DIAG_KEY: 's3cret' }).err === undefined, 'diagAuthorized：放行时不带错误文案');

// D2 DLQ 告警文本（私聊运维向：班次 + 条数 + 排查指向）
const alert = m.renderDlqAlert('2026-10-26-am', 3);
ok(alert.includes('2026-10-26-am') && alert.includes('3 条'), 'renderDlqAlert：含班次与条数');
ok(alert.includes('guji-dlq') && alert.includes('运维'), 'renderDlqAlert：指明死信队列与运维属性');

// B3 分片新鲜度（阈值内算新、超过算旧、缺时间戳不校验=向后兼容）
const MSG_AT = 1792936800000;
ok(m.isStaleShard({ at: MSG_AT - 60000 }, MSG_AT) === false, 'isStaleShard：早 1 分钟的分片 → 新鲜');
ok(m.isStaleShard({ at: MSG_AT - 11 * 60000 }, MSG_AT) === true, 'isStaleShard：早 11 分钟（>10 分钟阈值）→ 过期');
ok(m.isStaleShard({ at: MSG_AT + 5000 }, MSG_AT) === false, 'isStaleShard：晚于入队时刻 → 新鲜（本轮写的）');
ok(m.isStaleShard({}, MSG_AT) === false, 'isStaleShard：分片缺 at → 不校验（旧数据兼容）');
ok(m.isStaleShard({ at: 1 }, 0) === false, 'isStaleShard：消息缺 at → 不校验（在途旧消息兼容）');

// D4 健康度聚合（正常失败归源；分片整体失败退化为全源各记一次；rate 计算）
const feedsByShard = [['OpenAI', 'DeepMind'], ['Nvidia', "Tom's Hardware"]];
const runs = [
  { shard: 0, ok: true, failedSources: ['DeepMind'] },
  { shard: 0, ok: true, failedSources: [] },
  { shard: 1, ok: true, failedSources: [] },
  { shard: 1, ok: false }, // 整片失败：退化路径
];
const h = m.aggregateHealth(runs, feedsByShard);
ok(h.shardRuns === 4, 'aggregateHealth：run 计数');
ok(h.bySource['DeepMind'].fail === 1 && h.bySource['DeepMind'].seen === 2 && h.bySource['DeepMind'].rate === 0.5, 'aggregateHealth：逐源失败归源 + rate=1-1/2');
ok(h.bySource['Nvidia'].fail === 1 && h.bySource['Nvidia'].seen === 2, 'aggregateHealth：整片失败退化为该片全源各记一次');
ok(h.bySource['OpenAI'].fail === 0 && h.bySource['OpenAI'].rate === 1, 'aggregateHealth：无失败源 rate=1');
ok(m.aggregateHealth([], feedsByShard).bySource['OpenAI'].rate === null, 'aggregateHealth：无数据时 rate=null（不除零）');
ok(m.aggregateHealth(runs, feedsByShard).bySource['Nope'] === undefined, 'aggregateHealth：未配置源不出现在输出');

// pastWindow（健康度聚合的历史班次键）
ok(m.pastWindow(Date.UTC(2026, 9, 26, 2, 0), 0, 'am') === '2026-10-26-am', 'pastWindow：当天（北京 10-26 10:00 → am）');
ok(m.pastWindow(Date.UTC(2026, 9, 26, 2, 0), 1, 'pm') === '2026-10-25-pm', 'pastWindow：前一天 pm');
ok(m.pastWindow(Date.UTC(2026, 9, 1, 2, 0), 1, 'am') === '2026-09-30-am', 'pastWindow：跨月（10-01 北京 → 9-30）');

// ---- B1 A/B 实验（外部审查 B1 / PR#3 零成本方案）----
// HTMLRewriter 是 Workers 运行时专属 API，Node 原生无实现 → 注入 fake 测接线，测完还原全局。
const _HTMLRewriter = globalThis.HTMLRewriter;
const _fetch = globalThis.fetch;
const _Response = globalThis.Response;
globalThis.Response = class {
  constructor(body, init) { this._body = body; this.ok = true; this.headers = { get: () => 'text/html' }; }
  text() { return Promise.resolve(String(this._body ?? '')); }
};
globalThis.HTMLRewriter = class {
  on(sel, h) { this.h = h; return this; }
  transform(resp) {
    const text = String(resp._body ?? '').replace(/<[^>]+>/g, ' '); // 模拟 HTMLRewriter 裸抽：去标签留文本
    if (this.h && this.h.text) this.h.text({ text });
    return { text: async () => text };
  }
};
const sampleHtml = '<html><body><nav>Menu Sign in Cookie Policy Terms of Service</nav><p>OpenAI released a new GPU cluster for training large language models. The cluster uses custom accelerators and delivers record throughput for inference workloads across many regions. Researchers reported improvements in both latency and energy efficiency compared to the previous generation of hardware deployed last year.</p><p>Nvidia announced a roadmap for the next two years covering datacenter and edge deployments with partners in multiple countries building local fabs.</p></body></html>';
const mdText = '# Title\n\nSome markdown body text about GPUs and accelerators.';
globalThis.fetch = async () => new Response(sampleHtml); // 覆盖全局 fetch，喂样本 HTML
const mkB1Net = (env) => m.createNet({
  GUJI_KV: { get: async () => null, put: async () => {} },
  AI: { toMarkdown: async () => [{ data: mdText, format: 'text' }] },
  ...env, // ⚠️ 必须合并进第一个参数——createNet(env, budget) 只收两个参数，net.env 即第一参；
          // 旧版把 JINA_KEY 放第三参被静默忽略，「Jina 路径」测试实际跑的是 toMarkdown 路径（假绿）
}, m.createBudget({ silent: true }));
const b1item = { link: 'https://x.test/a', desc: '', title: 't', source: 'X', srcId: 'x', cat: '模型', kind: 'rss', win: '7d', ts: Date.now() };
// toMarkdown 路径（无 JINA_KEY）：应产生 A/B 采集，含两版 desc
const ab1 = await m.enrichMaterial([b1item], mkB1Net({}));
ok(ab1.length === 1, 'B1：toMarkdown 路径产生 1 条 A/B 采集');
ok(ab1[0] && ab1[0].url === 'https://x.test/a', 'B1：采集含原始 url');
ok(ab1[0] && /markdown/.test(ab1[0].toMarkdown) && /OpenAI/.test(ab1[0].htmlRewriter), 'B1：toMarkdown 含 markdown 文本、htmlRewriter 裸抽含正文本');
ok(ab1[0] && ab1[0].toMarkdown.length <= 200 && ab1[0].htmlRewriter.length <= 200, 'B1：两版 desc 均截断至 200 字');
// Jina 路径（有 JINA_KEY，现已真正生效）：无同份 HTML，rewriter=null → 不进 A/B 采集
// （用独立克隆：旧版共享 b1item 被 ab1 改写 desc 后，ab2 靠「md 不再更长」碰巧不采集——假绿）
const ab2 = await m.enrichMaterial([{ ...b1item }], mkB1Net({ JINA_KEY: 'fake' }));
ok(ab2.length === 0, 'B1：Jina 路径无同份 HTML → 不进 A/B 采集（仅 toMarkdown 路径对比）');
// v4.0.2.4（外部审查 B1 修正采样偏差）：原写法把采集裹在「toMarkdown 长度大胜」的 if 内，
// 样本 100% 来自 toMarkdown 赢的场景，实验结果系统性偏向 toMarkdown。修正后只要手里有两版就采。
const b1itemLong = { ...b1item, desc: 'x'.repeat(110) }; // 110 < thin 阈值 120，但远超 md(~55) 长度
const ab1b = await m.enrichMaterial([b1itemLong], mkB1Net({}));
ok(ab1b.length === 1, 'B1：toMarkdown 不比原 desc 长时仍采集（修正「只采 toMarkdown 赢」偏差）');
ok(ab1b[0] && ab1b[0].toMarkdown && ab1b[0].toMarkdown.length > 0 && /OpenAI/.test(ab1b[0].htmlRewriter || ''), 'B1：toMarkdown 偏短时两版仍完整记录（md 存在则为文本，仅 md 为空才 null 占位）');
// 还原全局，避免污染后续与 CI
globalThis.HTMLRewriter = _HTMLRewriter;
globalThis.fetch = _fetch;
globalThis.Response = _Response;

// ---- scheduled 处理器：cron 心跳 + 同步 await 入队（v4.0.2.3 根治「早班不累计」）----
{
  const kvStore = {};
  const sendCalls = [];
  const env = {
    GUJI_KV: { get: async () => null, put: async (k) => { kvStore[k] = true; } },
    GUJI_FETCH_Q: { sendBatch: async (b) => { sendCalls.push(['fetch', b.length]); } },
    GUJI_DIGEST_Q: { send: async () => { sendCalls.push(['digest', 1]); } },
    GUJI_SEND_Q: { sendBatch: async (b) => { sendCalls.push(['send', b.length]); } },
  };
  const ctx = { waitUntil: () => {} };
  // 抓取班：UTC 21:40 = 北京 05:40 am 班
  await m.default.scheduled({ scheduledTime: Date.UTC(2026, 9, 25, 21, 40), cron: '40,50 21 * * *' }, env, ctx);
  ok('guji:v4:run:2026-10-26-am:cron:fetch' in kvStore, 'scheduled：触发即写 cron:fetch 心跳（区分「没触发」与「触发了」）');
  ok('guji:v4:run:2026-10-26-am:enqueue:fetch' in kvStore, 'scheduled：await 入队完成后写 enqueue:fetch 记录（冷启动加固）');
  ok(sendCalls.some(c => c[0] === 'fetch' && c[1] === 4), 'scheduled：抓取班入队 4 分片');
  // 精选班
  await m.default.scheduled({ scheduledTime: Date.UTC(2026, 9, 25, 21, 50), cron: '40,50 21 * * *' }, env, ctx);
  ok(sendCalls.some(c => c[0] === 'digest'), 'scheduled：精选班入队 1 条');
  // 推送班
  await m.default.scheduled({ scheduledTime: Date.UTC(2026, 9, 25, 22, 0), cron: '0 22 * * *' }, env, ctx);
  ok(sendCalls.some(c => c[0] === 'send' && c[1] === 3), 'scheduled：推送班入队 3 片');
  // v4.0.2.4（外部审查 A2）：早班三触发点各写独立键——fetch 缺席 + digest/send 在场时，
  // cron:fetch 仍缺失（不再因同 window 键被覆盖成假阳性，糊住 v4.0.2.3 自己的验证目标）
  ok(kvStore['guji:v4:run:2026-10-26-am:cron:fetch'] && kvStore['guji:v4:run:2026-10-26-am:cron:digest'] && kvStore['guji:v4:run:2026-10-26-am:cron:send'], 'A2：早班三触发点各写独立键（fetch/digest/send 不复用同键）');
  // 失败路径：入队抛错 → 写 durable cron-error 记录（不再仅 console.error 后消失）
  const errKv = {};
  const envErr = {
    GUJI_KV: { get: async () => null, put: async (k) => { errKv[k] = true; } },
    GUJI_FETCH_Q: { sendBatch: async () => { throw new Error('queue cold start boom'); } },
    GUJI_DIGEST_Q: { send: async () => {} },
    GUJI_SEND_Q: { sendBatch: async () => {} },
  };
  await m.default.scheduled({ scheduledTime: Date.UTC(2026, 9, 25, 21, 40), cron: '40,50 21 * * *' }, envErr, ctx);
  ok('guji:v4:run:2026-10-26-am:cron:fetch' in errKv, 'scheduled(失败)：心跳仍先落库');
  ok('guji:v4:run:2026-10-26-am:cron-error:fetch' in errKv, 'scheduled(失败)：入队失败写 durable cron-error:fetch（根治观察盲区）');
}

console.log(`\n结果：${pass} 通过，${fail} 失败`);
process.exit(fail ? 1 : 0);
