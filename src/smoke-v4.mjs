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

console.log(`\n结果：${pass} 通过，${fail} 失败`);
process.exit(fail ? 1 : 0);
