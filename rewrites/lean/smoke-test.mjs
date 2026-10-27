import assert from 'node:assert/strict';
import worker, { FEEDS, SHARD_COUNT, currentWindow, qBatch, parseFeed, parseDigest, extractJsonArray } from './index.js';

assert.equal(FEEDS.length, 30, '源清单应为 30 个');
assert.equal(SHARD_COUNT, FEEDS.length, '每个来源单独分片，避免单个来源故障拖垮其他来源');
assert.equal(currentWindow(Date.parse('2026-10-26T21:40:00Z')), '2026-10-27-am', 'UTC 21:40 应对应北京时间次日早班');
assert.equal(currentWindow(Date.parse('2026-10-26T13:40:00Z')), '2026-10-26-pm', 'UTC 13:40 应对应北京时间晚班');

const sent = [];
await qBatch({ sendBatch: async messages => sent.push(...messages) }, [{ kind: 'fetch', window: '2026-10-26-am', shard: 0 }]);
assert.deepEqual(sent, [{ body: { kind: 'fetch', window: '2026-10-26-am', shard: 0 } }], 'sendBatch 必须使用 {body: message} 包装');

const sampleFeed = `<?xml version="1.0"?><rss version="2.0"><channel><item><title>New AI accelerator announced</title><link>https://example.com/ai</link><pubDate>Sun, 25 Oct 2026 10:00:00 GMT</pubDate><description>Example source summary.</description></item></channel></rss>`;
const parsed = parseFeed(sampleFeed, { id: 'test', name: 'Test', cat: '模型', win: '7d', url: 'https://example.com/feed' });
assert.equal(parsed.length, 1, 'RSS 应解析出一条 item');
assert.equal(parsed[0].title, 'New AI accelerator announced');
assert.equal(parsed[0].link, 'https://example.com/ai');
assert.ok(parsed[0].ts > 0, '应解析发布时间');

const material = [{ cat: '模型', title: 'Test', link: 'https://example.com/ai', source: 'Test', ts: Date.now() }];
const digest = parseDigest('[{"idx":1,"title":"AI 加速器发布","summary":"原文介绍了新加速器。"}]', material);
assert.equal(digest.length, 1, '应按 idx 将 AI 输出映射回原始来源');
assert.equal(digest[0].link, material[0].link, '精选必须保留原文链接');
assert.deepEqual(extractJsonArray('[]'), [], '合法空数组应被识别为有效 JSON');
assert.equal(extractJsonArray('not-json'), null, '无效 JSON 应被识别出来');

const store = new Map();
const kv = {
  async get(key, type) {
    const value = store.get(key);
    if (value == null) return null;
    return type === 'json' ? JSON.parse(value) : value;
  },
  async put(key, value) { store.set(key, value); },
};
let fetchBatch = null;
await worker.scheduled({ scheduledTime: Date.parse('2026-10-26T21:40:00Z'), cron: '40 21 * * *' }, {
  GUJI_KV: kv,
  GUJI_FETCH_Q: { async sendBatch(messages) { fetchBatch = messages; } },
});
assert.equal(fetchBatch.length, SHARD_COUNT, 'Cron 应为每个抓取分片入队一条消息');
assert.ok(fetchBatch.every(message => message.body?.kind === 'fetch'), 'Cron 入队消息的 body 结构必须正确');
assert.equal(fetchBatch[0].body.window, '2026-10-27-am');

console.log(`Smoke tests passed: ${FEEDS.length} feeds, ${SHARD_COUNT} shards, queue wrapper, RSS parsing, digest mapping, Cron window.`);
