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
