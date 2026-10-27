import { XMLParser } from 'fast-xml-parser';

// GUJI · 精简版
// 只保留核心链路：Cron → 抓取队列 → AI 精选 → Telegram 推送队列。
// 批量入队必须使用 [{ body: message }]，不能直接传业务消息数组。

const KV_LATEST = 'guji:v4:latest'; // 复用旧状态，保留 lastDigestAt，避免首次部署重复推送旧内容
const KV_ANTHROPIC_SEEN = 'guji:v4:seen'; // 复用旧 Anthropic 去重记录
const KV_RUN_PREFIX = 'guji:run:';
const Q_FETCH = 'guji-fetch';
const Q_DIGEST = 'guji-digest';
const Q_SEND = 'guji-send';
const Q_DLQ = 'guji-dlq';

const BJ_OFFSET_MS = 8 * 60 * 60 * 1000;
const RUN_TTL = 14 * 24 * 60 * 60;
const SOURCE_BATCH_SIZE = 1;
const SOURCE_CONCURRENCY = 1;
const FETCH_TIMEOUT_MS = 10000;
const SHARD_STALE_MS = 15 * 60 * 1000;
const MATERIAL_MAX = 24;
const DIGEST_MAX = 8;
const TELEGRAM_MAX_ATTEMPTS = 3;
const TELEGRAM_GAP_MS = 1100;
const USER_AGENT = 'GUJI-Digest/1.0';
const MAX_AGE_MS = { '72h': 72 * 3600_000, '7d': 7 * 86400_000, '14d': 14 * 86400_000, '30d': 30 * 86400_000 };

const AI_KEYWORDS = /\b(ai|artificial intelligence|llm|large language model|gpt|chatgpt|claude|gemini|copilot|agent|agentic|machine learning|deep learning|neural|transformer|diffusion|inference|foundation model|gpu|tpu|npu|hbm|accelerator|compute|datacenter|data cent(er|re)|chip|chips|semiconductor|silicon|nvidia|amd|intel|tsmc|foundry|hpc|blackwell|hopper|rubin|robot|humanoid|autonomous|self-driving|openai|anthropic|deepmind|mistral|grok|llama|qwen|deepseek|sora|multimodal|rag|embedding|fine-tun|distillation|quantiz)\b/i;

// 核心源保留一手官方信息、监管文件、精选原始访谈与论文；低优先级重复节目已移出。
// 每个源仅维护一份配置。SEC 仍按公司分别抓取，但共用同一套抓取代码。
const FEEDS = [
  // AI 实验室与生态官方源
  { id: 'openai', name: 'OpenAI', kind: 'rss', cat: '模型', win: '7d', url: 'https://openai.com/news/rss.xml' },
  { id: 'deepmind', name: 'Google DeepMind', kind: 'rss', cat: '模型', win: '7d', filter: true, url: 'https://deepmind.google/blog/rss.xml' },
  { id: 'googleai', name: 'Google AI', kind: 'rss', cat: '模型', win: '7d', url: 'https://blog.google/technology/ai/rss/' },
  { id: 'meta', name: 'Meta', kind: 'rss', cat: '模型', win: '7d', filter: true, url: 'https://about.fb.com/feed/' },
  { id: 'hf', name: 'Hugging Face', kind: 'rss', cat: '模型', win: '7d', url: 'https://huggingface.co/blog/feed.xml' },
  { id: 'anthropic', name: 'Anthropic', kind: 'html', cat: '模型', win: '7d', url: 'https://www.anthropic.com/news' },
  // 芯片与数据中心
  { id: 'nvdanews', name: 'NVIDIA Newsroom', kind: 'rss', cat: '芯片算力', win: '7d', filter: true, url: 'https://nvidianews.nvidia.com/releases.xml' },
  { id: 'nvdablog', name: 'NVIDIA Blog', kind: 'rss', cat: '芯片算力', win: '7d', filter: true, url: 'https://blogs.nvidia.com/feed/' },
  { id: 'arm', name: 'Arm Newsroom', kind: 'rss', cat: '芯片算力', win: '7d', url: 'https://newsroom.arm.com/rss' },
  // 聚合源只负责发现遗漏；入选时保留其链接，并优先追溯所链接的原始报道/公告。
  { id: 'techmeme', name: 'Techmeme（发现与补漏）', kind: 'rss', cat: '科技新闻', win: '72h', filter: true, url: 'https://www.techmeme.com/feed.xml' },
  { id: 'sth', name: 'ServeTheHome', kind: 'rss', cat: '数据中心', win: '72h', filter: true, url: 'https://www.servethehome.com/feed/' },
  // SEC / EDGAR：只保留日常重大事项；财报表格可另做低频任务
  { id: 'e-mrvl', name: 'Marvell·SEC', kind: 'edgar', cat: '芯片算力', win: '14d', cik: '0001835632', type: '8-K' },
  { id: 'e-avgo', name: 'Broadcom·SEC', kind: 'edgar', cat: '芯片算力', win: '14d', cik: '0001730168', type: '8-K' },
  { id: 'e-dell', name: 'Dell·SEC', kind: 'edgar', cat: '数据中心', win: '14d', cik: '0001571996', type: '8-K' },
  { id: 'e-smci', name: 'Supermicro·SEC', kind: 'edgar', cat: '数据中心', win: '14d', cik: '0001375365', type: '8-K' },
  { id: 'e-anet', name: 'Arista·SEC', kind: 'edgar', cat: '数据中心', win: '14d', cik: '0001596532', type: '8-K' },
  { id: 'e-tsm', name: 'TSMC·SEC', kind: 'edgar', cat: '芯片算力', win: '14d', cik: '0001046179', type: '6-K' },
  { id: 'e-intc', name: 'Intel·SEC', kind: 'edgar', cat: '芯片算力', win: '14d', cik: '0000050863', type: '8-K' },
  { id: 'e-nvda', name: 'NVIDIA·SEC', kind: 'edgar', cat: '芯片算力', win: '14d', cik: '0001045810', type: '8-K' },
  { id: 'e-amd', name: 'AMD·SEC', kind: 'edgar', cat: '芯片算力', win: '14d', cik: '0000002488', type: '8-K' },
  { id: 'e-arm', name: 'Arm·SEC', kind: 'edgar', cat: '芯片算力', win: '14d', cik: '0001973239', type: '6-K' },
  { id: 'e-googl', name: 'Alphabet·SEC', kind: 'edgar', cat: '资本', win: '14d', cik: '0001652044', type: '8-K' },
  { id: 'e-msft', name: 'Microsoft·SEC', kind: 'edgar', cat: '模型', win: '14d', cik: '0000789019', type: '8-K' },
  // 原始访谈：每日候选池保留少数节目，按内容重要性而不是节目更新量推送
  { id: 'p-dwarkesh', name: 'Dwarkesh Podcast', kind: 'rss', cat: '访谈观点', win: '30d', url: 'https://apple.dwarkesh-podcast.workers.dev/feed.rss' },
  { id: 'p-nopriors', name: 'No Priors', kind: 'rss', cat: '访谈观点', win: '30d', url: 'https://feeds.megaphone.fm/nopriors' },
  { id: 'p-bg2', name: 'BG2', kind: 'rss', cat: '访谈观点', win: '30d', url: 'https://anchor.fm/s/s06c2370/podcast/rss' },
  { id: 'p-latent', name: 'Latent Space', kind: 'rss', cat: '访谈观点', win: '30d', url: 'https://podcast.latent.space/feed.xml' },
  { id: 'p-cogrev', name: 'Cognitive Revolution', kind: 'rss', cat: '访谈观点', win: '30d', url: 'https://feeds.megaphone.fm/RINTP3108857801' },
  { id: 'p-stratechery', name: 'Stratechery', kind: 'rss', cat: '访谈观点', win: '30d', url: 'https://stratechery.com/feed/' },
  // 多分类 arXiv 一条订阅，避免新增多个相近 RSS 源
  { id: 'arxiv', name: 'arXiv AI/ML/Language', kind: 'rss', cat: '论文', win: '72h', url: 'https://rss.arxiv.org/rss/cs.CL+cs.AI+cs.LG+stat.ML' },
];

const SHARD_COUNT = Math.ceil(FEEDS.length / SOURCE_BATCH_SIZE);
const XML = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  textNodeName: '#text',
  isArray: (name) => ['item', 'entry', 'category'].includes(name),
  processEntities: true,
  parseTagValue: false,
  trimValues: true,
});

const runKey = (runId, stage) => `${KV_RUN_PREFIX}${runId}:${stage}`;
const shardKey = (runId, shard) => `guji:shard:${runId}:${shard}`;
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const now = () => Date.now();
const bjDate = (ms = now()) => new Date(ms + BJ_OFFSET_MS);
const currentWindow = (ms = now()) => {
  const d = bjDate(ms);
  const date = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
  return `${date}-${d.getUTCHours() < 12 ? 'am' : 'pm'}`;
};
const cleanText = (v) => {
  if (v == null) return '';
  if (typeof v === 'string') return decodeEntities(v).replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
  if (typeof v === 'object' && v['#text'] != null) return cleanText(String(v['#text']));
  return '';
};
function decodeEntities(s) {
  return String(s || '')
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&').replace(/&quot;/gi, '"')
    .replace(/&apos;|&#0*39;/gi, "'").replace(/&lt;/gi, '<').replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => { try { return String.fromCodePoint(parseInt(h, 16)); } catch { return _; } })
    .replace(/&#(\d+);/g, (_, d) => { try { return String.fromCodePoint(Number(d)); } catch { return _; } });
}
function pickLink(node) {
  if (typeof node === 'string') return node.trim();
  if (Array.isArray(node)) {
    const preferred = node.find(x => x && x['@_rel'] === 'alternate') || node.find(x => x && x['@_href']);
    return preferred ? String(preferred['@_href'] || preferred['#text'] || '').trim() : '';
  }
  if (node && typeof node === 'object') return String(node['@_href'] || node['#text'] || '').trim();
  return '';
}
function pickDate(...values) {
  for (const value of values) {
    const text = cleanText(value);
    const ms = text ? Date.parse(text) : NaN;
    if (Number.isFinite(ms)) return ms;
  }
  return 0;
}
function makeItem(feed, item) {
  return {
    title: String(item.title || '').replace(/\s+/g, ' ').trim().slice(0, 240),
    link: String(item.link || feed.url || '').trim().slice(0, 1500),
    ts: Number(item.ts) || 0,
    desc: String(item.desc || '').replace(/\s+/g, ' ').trim().slice(0, 900),
    srcId: feed.id,
    source: feed.name,
    cat: feed.cat,
    win: feed.win || '7d',
    filter: !!feed.filter,
  };
}
function parseFeed(xml, feed) {
  let root;
  try { root = XML.parse(xml); } catch { return []; }
  let entries = root?.rss?.channel?.item || root?.feed?.entry || root?.['rdf:RDF']?.item || root?.channel?.item || root?.item || [];
  if (!Array.isArray(entries)) entries = entries ? [entries] : [];
  const out = [];
  for (const entry of entries.slice(0, 40)) {
    if (!entry || typeof entry !== 'object') continue;
    const title = cleanText(entry.title);
    if (!title) continue;
    const link = pickLink(entry.link) || cleanText(entry.id) || cleanText(entry.guid) || feed.url;
    const ts = pickDate(entry.pubDate, entry.published, entry.updated, entry['dc:date'], entry.date);
    const desc = cleanText(entry.description) || cleanText(entry.summary) || cleanText(entry['content:encoded']) || cleanText(entry.content);
    out.push(makeItem(feed, { title, link, ts, desc }));
  }
  return out;
}
function parseEdgar(xml, feed) {
  let root;
  try { root = XML.parse(xml); } catch { return []; }
  let entries = root?.feed?.entry || [];
  if (!Array.isArray(entries)) entries = entries ? [entries] : [];
  return entries.slice(0, 12).map(entry => {
    const summary = cleanText(entry.summary) || cleanText(entry.content);
    const items = [...summary.matchAll(/Item\s+[\d.]+:\s*([^.;]+(?:\.[^\s<]+)?)/gi)].map(m => m[1].trim()).filter(Boolean);
    const filing = feed.type === '13F-HR' ? '13F 持仓' : `${feed.type} 申报`;
    const title = items.length ? `${feed.name} · ${filing}：${items.slice(0, 3).join('；')}` : `${feed.name} · ${filing}`;
    return makeItem(feed, { title, link: pickLink(entry.link), ts: pickDate(entry.updated, entry.published), desc: summary });
  }).filter(item => item.link);
}
function normTitle(title) {
  return String(title || '').toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, '').slice(0, 100);
}
function itemIsAI(item) { return AI_KEYWORDS.test(item.title); }
function ageMs(item, current) {
  const max = MAX_AGE_MS[item.win] || MAX_AGE_MS['7d'];
  return !item.ts || current - item.ts > max || item.ts > current + 6 * 3600_000;
}
function finalizeItems(items) {
  const current = now();
  const kept = [];
  for (const item of items) {
    if (!item.title || !item.link || ageMs(item, current)) continue;
    if (item.filter && !itemIsAI(item)) continue;
    kept.push(item);
  }
  const counts = new Map();
  const byTitle = new Map();
  for (const item of kept.sort((a, b) => b.ts - a.ts)) {
    const count = counts.get(item.srcId) || 0;
    const maxPerSource = item.srcId === 'sth' ? 8 : item.srcId === 'arxiv' ? 8 : 30;
    if (count >= maxPerSource) continue;
    counts.set(item.srcId, count + 1);
    const key = normTitle(item.title);
    if (!key) continue;
    const existing = byTitle.get(key);
    if (existing) {
      existing.also ||= [];
      if (!existing.also.includes(item.source)) existing.also.push(item.source);
      if ((!existing.desc || existing.desc.length < 80) && item.desc) existing.desc = item.desc;
    } else {
      byTitle.set(key, { ...item, also: [] });
    }
  }
  return [...byTitle.values()].sort((a, b) => b.ts - a.ts).slice(0, 80);
}

async function fetchText(url, init = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(url, { ...init, signal: controller.signal, redirect: 'follow' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.text();
  } finally {
    clearTimeout(timer);
  }
}
async function fetchAnthropic(feed, env) {
  const html = await fetchText(feed.url, { headers: { 'User-Agent': USER_AGENT, Accept: 'text/html,*/*' } });
  const seen = (await env.GUJI_KV.get(KV_ANTHROPIC_SEEN, 'json').catch(() => null)) || {};
  const found = [];
  const nextSeen = { ...seen };
  const nav = new Set(['announcements', 'engineering', 'research', 'policy', 'news', 'company', 'public-benefit']);
  const pattern = /<a[^>]*class="[^"]*FeaturedGrid-module[^\"]*tileLink[^\"]*"[^>]*href="\/news\/([a-z0-9-]+)"[^>]*>([\s\S]*?)<\/a>/gi;
  let match;
  while ((match = pattern.exec(html)) && found.length < 12) {
    const slug = match[1];
    if (nav.has(slug) || nextSeen[slug]) continue;
    const title = cleanText(match[2]);
    if (!title || title.length < 8) continue;
    nextSeen[slug] = now();
    found.push(makeItem(feed, { title: `Anthropic：${title}`, link: `https://www.anthropic.com/news/${slug}`, ts: now(), desc: '' }));
  }
  const cutoff = now() - 60 * 86400_000;
  for (const [slug, at] of Object.entries(nextSeen)) {
    const value = Number(at) || 0;
    const atMs = value > 0 && value < 1e12 ? value * 1000 : value; // 兼容旧版以秒保存的时间戳
    if (atMs < cutoff) delete nextSeen[slug];
  }
  if (found.length) await env.GUJI_KV.put(KV_ANTHROPIC_SEEN, JSON.stringify(nextSeen), { expirationTtl: 90 * 86400 });
  return found;
}
async function fetchOne(feed, env) {
  try {
    if (feed.kind === 'html') return { items: await fetchAnthropic(feed, env), failure: null };
    if (feed.kind === 'edgar' && !env.SEC_USER_AGENT) {
      throw new Error('缺少 SEC_USER_AGENT；请配置带真实联系邮箱的 User-Agent，否则 SEC 可能拒绝请求');
    }
    const url = feed.kind === 'edgar'
      ? `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${encodeURIComponent(feed.cik)}&type=${encodeURIComponent(feed.type)}&dateb=&owner=include&count=12&output=atom`
      : feed.url;
    const headers = {
      'User-Agent': feed.kind === 'edgar' ? env.SEC_USER_AGENT : USER_AGENT,
      Accept: 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*',
    };
    const xml = await fetchText(url, { headers });
    const items = feed.kind === 'edgar' ? parseEdgar(xml, feed) : parseFeed(xml, feed);
    return { items, failure: null };
  } catch (error) {
    const failure = { source: feed.name, error: String(error?.message || error).slice(0, 240) };
    console.error(`[fetch] ${feed.id}: ${failure.error}`);
    return { items: [], failure };
  }
}
async function fetchShard(feeds, env) {
  const items = [];
  const failures = [];
  for (let i = 0; i < feeds.length; i += SOURCE_CONCURRENCY) {
    const results = await Promise.all(feeds.slice(i, i + SOURCE_CONCURRENCY).map(feed => fetchOne(feed, env)));
    for (const result of results) {
      items.push(...result.items);
      if (result.failure) failures.push(result.failure);
    }
  }
  return { items, failures };
}
async function putRun(env, runId, stage, value) {
  await env.GUJI_KV.put(runKey(runId, stage), JSON.stringify({ at: now(), ...value }), { expirationTtl: RUN_TTL });
}
async function qBatch(queue, bodies) {
  if (!queue) throw new Error('缺少队列绑定');
  if (!bodies?.length) return;
  // Cloudflare sendBatch 要求每条消息都包装在 {body: ...} 中。
  return queue.sendBatch(bodies.map(body => ({ body })));
}
async function enqueueFetch(env, runId, window) {
  const messages = [];
  for (let shard = 0; shard < SHARD_COUNT; shard++) {
    const feeds = FEEDS.slice(shard * SOURCE_BATCH_SIZE, (shard + 1) * SOURCE_BATCH_SIZE);
    messages.push({ kind: 'fetch', runId, window, shard, feeds, at: now() });
  }
  await qBatch(env.GUJI_FETCH_Q, messages);
}

function extractJsonArray(text) {
  const s = String(text || '').replace(/```(?:json)?/gi, '');
  const start = s.indexOf('[');
  if (start < 0) return null;
  let depth = 0, inString = false, escaped = false;
  for (let i = start; i < s.length; i++) {
    const c = s[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (c === '\\') escaped = true;
      else if (c === '"') inString = false;
      continue;
    }
    if (c === '"') inString = true;
    else if (c === '[') depth++;
    else if (c === ']') {
      depth--;
      if (depth === 0) {
        try { return JSON.parse(s.slice(start, i + 1)); } catch { return null; }
      }
    }
  }
  return null;
}
function aiResultText(result) {
  if (typeof result === 'string') return result;
  if (typeof result?.response === 'string') return result.response;
  if (typeof result?.result === 'string') return result.result;
  if (typeof result?.content === 'string') return result.content;
  return '';
}
const buildPrompt = (items) => `你是科技与 AI 日报编辑。请从以下素材中选择最值得读者知道的 0-${DIGEST_MAX} 条，重点考虑真实影响、信息增量、来源权威性和时效性。不要为了凑数而选择，不要重复讲同一事件。\n\n严格依据提供的标题和摘要，不补充外部知识、不猜测缺失数字、不把观点说成事实。将每条入选内容用简体中文写成简洁标题和 1-2 句摘要。保留原文语气，不强化结论。输出必须是纯 JSON 数组，每项格式：{"idx":1,"title":"中文标题","summary":"中文摘要"}。idx 必须对应素材编号。无值得推送的内容时返回 []。\n\n素材：\n${items.map((it, i) => `${i + 1}. [${it.cat}] ${it.title}\n来源：${it.source}\n链接：${it.link}\n摘要：${(it.desc || '原文未提供摘要').slice(0, 420)}`).join('\n\n')}`;
async function callZhipu(prompt, env) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 45000);
  try {
    const response = await fetch('https://open.bigmodel.cn/api/paas/v4/chat/completions', {
      method: 'POST', signal: controller.signal,
      headers: { Authorization: `Bearer ${env.ZHIPU_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'glm-4-plus', messages: [{ role: 'user', content: prompt }], temperature: 0.1, max_tokens: 3000 }),
    });
    if (!response.ok) throw new Error(`智谱 HTTP ${response.status}`);
    const data = await response.json();
    const text = data?.choices?.[0]?.message?.content;
    if (typeof text !== 'string' || !text.trim()) throw new Error('智谱返回空内容');
    return text;
  } finally { clearTimeout(timer); }
}
async function makeDigest(items, env) {
  const material = items.slice(0, MATERIAL_MAX);
  const prompt = buildPrompt(material);
  const attempts = [];
  if (env.ZHIPU_KEY) {
    try {
      const text = await callZhipu(prompt, env);
      if (!Array.isArray(extractJsonArray(text))) throw new Error('未返回有效 JSON 数组');
      // 有效的 [] 代表模型判断本次没有值得推送的内容，不应当被误判为调用失败。
      return { digest: parseDigest(text, material), provider: 'zhipu', error: null };
    } catch (error) { attempts.push(`zhipu: ${String(error?.message || error)}`); }
  }
  if (env.AI) {
    // 先用轻量指令模型，输出格式无效时再尝试 Qwen。
    for (const model of ['@cf/meta/llama-3.1-8b-instruct-fp8', '@cf/qwen/qwen3-30b-a3b-fp8']) {
      try {
        const result = await env.AI.run(model, {
          messages: [{ role: 'user', content: prompt }], max_tokens: 3000, temperature: 0.1,
          ...(model.includes('qwen3') ? { chat_template_kwargs: { enable_thinking: false } } : {}),
        });
        const text = aiResultText(result);
        if (!Array.isArray(extractJsonArray(text))) throw new Error('未返回有效 JSON 数组');
        return { digest: parseDigest(text, material), provider: model, error: null };
      } catch (error) { attempts.push(`${model}: ${String(error?.message || error)}`); }
    }
  } else attempts.push('Workers AI 绑定缺失');
  return { digest: [], provider: null, error: attempts.join(' | ').slice(0, 1200) || 'AI 未返回有效精选' };
}
function parseDigest(text, material) {
  const arr = extractJsonArray(text);
  if (!Array.isArray(arr)) return [];
  const seen = new Set();
  const out = [];
  for (const row of arr) {
    const idx = Number(row?.idx);
    if (!Number.isInteger(idx) || idx < 1 || idx > material.length || seen.has(idx)) continue;
    const title = String(row.title || '').trim();
    const summary = String(row.summary || '').trim();
    if (!title || !summary) continue;
    seen.add(idx);
    const source = material[idx - 1];
    out.push({ cat: source.cat, title: title.slice(0, 180), summary: summary.slice(0, 1100), source: source.source, link: source.link, ts: source.ts });
  }
  return out.slice(0, DIGEST_MAX);
}
function msFromLegacyTime(value) {
  const n = Number(value) || 0;
  return n > 0 && n < 1e12 ? n * 1000 : n;
}
async function consumeFetch(msg, env) {
  const body = msg.body || {};
  const { runId, window, shard, feeds } = body;
  if (!runId || !window || !Array.isArray(feeds) || !feeds.length) throw new Error(`抓取消息格式错误：${JSON.stringify(body).slice(0, 300)}`);
  const result = await fetchShard(feeds, env);
  await env.GUJI_KV.put(shardKey(runId, shard), JSON.stringify({ items: result.items, failures: result.failures, at: now() }), { expirationTtl: RUN_TTL });
  await putRun(env, runId, `fetch-${shard}`, { ok: true, items: result.items.length, failures: result.failures.length, failureDetails: result.failures });
  msg.ack();
  console.log(`[fetch] ${window} shard=${shard}: ${result.items.length} 条，${result.failures.length} 个源失败`);
}
async function consumeDigest(msg, env) {
  const body = msg.body || {};
  const { runId, window, at = msg.timestamp?.getTime?.() || now(), full = false } = body;
  if (!runId || !window) throw new Error(`精选消息格式错误：${JSON.stringify(body).slice(0, 300)}`);
  const shards = [];
  const missingShards = [];
  for (let shard = 0; shard < SHARD_COUNT; shard++) {
    const stored = await env.GUJI_KV.get(shardKey(runId, shard), 'json');
    if (!stored || !Array.isArray(stored.items) || (stored.at && stored.at < at - SHARD_STALE_MS)) {
      missingShards.push(shard);
      continue;
    }
    shards.push(stored);
  }
  // 常规情况下所有源会在 10 分钟内完成；若个别消费者反复失败，不让一个源拖垮整份日报。
  // 前几次仍然重试；第 5 次开始用已完成分片继续精选，并在推送中明确报告缺失。
  if (missingShards.length && Number(msg.attempts || 1) < 5) {
    console.warn(`[digest] ${window} 缺少分片 ${missingShards.join(',')}；60 秒后重试，attempt=${msg.attempts}`);
    msg.retry({ delaySeconds: 60 });
    return;
  }
  if (missingShards.length) console.error(`[digest] ${window} 使用部分数据继续：缺少分片 ${missingShards.join(',')}`);
  const allItems = finalizeItems(shards.flatMap(s => s.items));
  const failures = [
    ...shards.flatMap(s => s.failures || []),
    ...missingShards.map(shard => ({ source: `抓取分片 ${shard}`, error: '该分片未能完成，日报使用其余已完成来源' })),
  ];
  const previous = await env.GUJI_KV.get(KV_LATEST, 'json').catch(() => null);
  const lastDigestAt = full ? 0 : msFromLegacyTime(previous?.lastDigestAt);
  const newItems = lastDigestAt ? allItems.filter(item => item.ts > lastDigestAt) : allItems;
  let digest = [];
  let aiError = null;
  let provider = null;
  let status = missingShards.length ? 'partial-fetch' : 'no-new-items';
  if (newItems.length) {
    const result = await makeDigest(newItems, env);
    digest = result.digest;
    aiError = result.error;
    provider = result.provider;
    status = result.error ? 'ai-failed' : (digest.length ? (missingShards.length ? 'partial' : 'ok') : (missingShards.length ? 'partial-no-important-items' : 'no-important-items'));
  }
  const data = {
    window, runId, generatedAt: now(), lastDigestAt: (digest.length || status === 'no-important-items') ? now() : (previous?.lastDigestAt || 0),
    missingShards,
    items: allItems, newItems: newItems.slice(0, MATERIAL_MAX), digest, failures,
    status, aiError, provider, totalSources: FEEDS.length,
  };
  await env.GUJI_KV.put(KV_LATEST, JSON.stringify(data), { expirationTtl: RUN_TTL });
  await putRun(env, runId, 'digest', { ok: true, completed: true, status, inputItems: newItems.length, digestItems: digest.length, failures: failures.length, aiError, provider });
  msg.ack();
  console.log(`[digest] ${window}: status=${status}, input=${newItems.length}, selected=${digest.length}`);
}
function safeByteTruncate(text, maxBytes = 3500) {
  const encoder = new TextEncoder();
  if (encoder.encode(text).length <= maxBytes) return text;
  let out = '', bytes = 0;
  for (const ch of String(text)) {
    const size = encoder.encode(ch).length;
    if (bytes + size > maxBytes - 24) break;
    out += ch;
    bytes += size;
  }
  return `${out}…（内容已截断）`;
}
function bjStamp(ms) {
  const d = bjDate(ms);
  return `${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')} ${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`;
}
function tgMessages(data) {
  if (data.digest?.length) {
    const out = [`🤖 GUJI AI 情报｜${data.window.endsWith('-am') ? '早报' : '晚报'}｜${bjStamp(data.generatedAt)}\n抓取候选 ${data.items.length} 条，AI 精选 ${data.digest.length} 条，源失败 ${data.failures.length} 个${data.missingShards?.length ? `（缺失分片 ${data.missingShards.join(',')}）` : ''}`];
    for (const [index, item] of data.digest.entries()) {
      out.push(`${item.cat === '芯片算力' ? '🔩' : item.cat === '数据中心' ? '🏭' : item.cat === '模型' ? '🧠' : item.cat === '资本' ? '💰' : item.cat === '访谈观点' ? '🎙️' : item.cat === '论文' ? '📚' : '📰'} ${item.title}\n\n${item.summary}\n\n${item.source} · ${bjStamp(item.ts)} · ${index + 1}/${data.digest.length}\n${item.link}`);
    }
    return out.map(t => safeByteTruncate(t));
  }
  if (data.status === 'no-new-items') {
    return [`🤖 GUJI AI 情报｜${data.window.endsWith('-am') ? '早报' : '晚报'}\n本次没有发现新的候选信息。系统已完成抓取与筛选。\n近期候选 ${data.items.length} 条；源抓取失败 ${data.failures.length} 个。`];
  }
  if (data.status === 'no-important-items') {
    return [`🤖 GUJI AI 情报｜${data.window.endsWith('-am') ? '早报' : '晚报'}\n已审阅 ${data.newItems.length} 条新候选，本次没有选出值得单独推送的内容。\n源抓取失败 ${data.failures.length} 个。`];
  }
  if (['partial-fetch', 'partial-no-important-items'].includes(data.status)) {
    return [`⚠️ GUJI AI 情报｜${data.window.endsWith('-am') ? '早报' : '晚报'}\n部分来源未能完成抓取，因此本次结果不完整。\n可用候选 ${data.items.length} 条；失败/缺失 ${data.failures.length} 项；缺失分片 ${data.missingShards?.join(',') || '无'}。\n请检查 /api/diag 和 Cloudflare 队列日志。`];
  }
  const rows = (data.newItems || []).slice(0, 8).map(it => `· ${it.title}\n${it.source} · ${it.link}`);
  const intro = `⚠️ GUJI AI 情报｜${data.window.endsWith('-am') ? '早报' : '晚报'}\nAI 中文精选失败，本次暂发原始标题；源失败 ${data.failures.length} 个，缺失分片 ${data.missingShards?.join(',') || '无'}。请检查 /api/diag。`;
  return [safeByteTruncate([intro, ...rows].join('\n\n'))];
}
async function sendTelegramOne(token, chat, text) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST', signal: controller.signal,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chat, text, disable_web_page_preview: true }),
    });
    const payload = await response.json().catch(() => ({}));
    if (response.ok && payload.ok) return { ok: true };
    return { ok: false, status: response.status, error: payload.description || `HTTP ${response.status}`, retryAfter: Number(payload?.parameters?.retry_after) || 0 };
  } finally { clearTimeout(timer); }
}
function isPermanentTelegramError(result) {
  return result.status >= 400 && result.status < 500 && result.status !== 429;
}
async function consumeSend(msg, env) {
  const body = msg.body || {};
  if (body.kind === 'start') {
    const { window, force = false } = body;
    if (!window) throw new Error('推送启动消息缺少 window');
    const data = await env.GUJI_KV.get(KV_LATEST, 'json');
    const runId = data?.runId || window;
    const digestRun = await env.GUJI_KV.get(runKey(runId, 'digest'), 'json');
    if (!data || data.window !== window || !digestRun?.completed) {
      console.warn(`[send] ${window} 的精选尚未完成，60 秒后重试；attempt=${msg.attempts}`);
      msg.retry({ delaySeconds: 60 });
      return;
    }
    const old = await env.GUJI_KV.get(runKey(runId, 'push-enqueued'), 'json').catch(() => null);
    if (old && !force) { msg.ack(); return; }
    const targets = [
      { role: 'private', chat: env.TG_CHAT_ID },
      { role: 'channel', chat: env.TG_CHANNEL },
    ].filter(target => target.chat);
    if (!env.TG_BOT_TOKEN || !targets.length) {
      await putRun(env, window, 'push-error', { ok: false, error: '缺少 TG_BOT_TOKEN 或 TG_CHAT_ID/TG_CHANNEL' });
      msg.ack();
      return;
    }
    const messages = tgMessages(data);
    await qBatch(env.GUJI_SEND_Q, targets.map(target => ({ kind: 'chat', window, role: target.role, chat: target.chat, messages })));
    // qBatch 已成功确认消息持久化；状态写入失败不能让整个启动消息重试造成重复推送。
    await putRun(env, runId, 'push-enqueued', { ok: true, targets: targets.map(t => t.role), messages: messages.length }).catch(error => console.error('[send] 写入推送记录失败：' + error.message));
    msg.ack();
    return;
  }
  if (body.kind !== 'chat') throw new Error(`未知推送消息类型：${String(body.kind)}`);
  const { window, role, chat, messages = [] } = body;
  if (!env.TG_BOT_TOKEN || !chat) {
    await putRun(env, window, `push-${role}`, { ok: false, error: 'Telegram token 或 chat 缺失' }).catch(() => {});
    msg.ack();
    return;
  }
  let okCount = 0;
  const errors = [];
  for (let i = 0; i < messages.length; i++) {
    if (i > 0) await sleep(TELEGRAM_GAP_MS); // 每个目标内串行，尽量遵守 Telegram 单聊天发送频率
    let result = { ok: false, error: '未发送' };
    for (let attempt = 1; attempt <= TELEGRAM_MAX_ATTEMPTS; attempt++) {
      try { result = await sendTelegramOne(env.TG_BOT_TOKEN, chat, messages[i]); }
      catch (error) { result = { ok: false, status: 0, error: String(error?.message || error), retryAfter: 0 }; }
      if (result.ok || isPermanentTelegramError(result) || attempt === TELEGRAM_MAX_ATTEMPTS) break;
      const pause = result.retryAfter ? Math.min(result.retryAfter + 1, 45) * 1000 : 1500 * attempt;
      await sleep(pause);
    }
    if (result.ok) okCount++;
    else errors.push({ index: i, error: result.error || 'unknown', status: result.status || 0 });
  }
  await putRun(env, window, `push-${role}`, { ok: okCount === messages.length, success: okCount, total: messages.length, errors }).catch(error => console.error('[send] 写入推送结果失败：' + error.message));
  msg.ack();
  console.log(`[send] ${window} ${role}: ${okCount}/${messages.length} 条成功${errors.length ? `，失败 ${errors.length}` : ''}`);
}
async function consumeDlq(msg, env) {
  const body = msg.body || {};
  console.error(`[DLQ] ${JSON.stringify(body).slice(0, 700)}`);
  if (body.window) await putRun(env, body.runId || body.window, 'dead-letter', { ok: false, message: body, attempts: msg.attempts }).catch(() => {});
  msg.ack();
}

function isAuthorized(url, env) {
  if (!env.DIAG_KEY) return false;
  return url.searchParams.get('key') === env.DIAG_KEY;
}
function json(value, status = 200) {
  return new Response(JSON.stringify(value, null, 2), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' } });
}
function htmlEscape(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
}
function renderStatusPage(data) {
  if (!data) return '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width"><h2>GUJI AI 日报</h2><p>尚未生成日报。请确认 Cron Triggers 和队列绑定。</p>';
  const entries = (data.digest || []).map(item => `<article><h3><a href="${htmlEscape(item.link)}">${htmlEscape(item.title)}</a></h3><p>${htmlEscape(item.summary)}</p><small>${htmlEscape(item.source)} · ${htmlEscape(bjStamp(item.ts))}</small></article>`).join('');
  return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>GUJI AI 日报</title><style>body{max-width:760px;margin:32px auto;padding:0 16px;font:16px/1.6 system-ui;color:#202124}article{border-top:1px solid #ddd;padding:14px 0}a{color:#175dcc}small{color:#666}</style></head><body><h1>GUJI AI 日报</h1><p>班次：${htmlEscape(data.window)} · 状态：${htmlEscape(data.status)} · 更新：${htmlEscape(bjStamp(data.generatedAt))}</p><p>抓取候选 ${Number(data.items?.length || 0)} 条；AI 精选 ${Number(data.digest?.length || 0)} 条；抓取失败 ${Number(data.failures?.length || 0)} 个源。</p>${entries || '<p>本次没有可展示的中文精选。请查看状态或诊断接口。</p>'}</body></html>`;
}

export default {
  async scheduled(event, env) {
    const at = Number(event.scheduledTime) || now();
    const local = bjDate(at);
    const minute = local.getUTCMinutes();
    const window = currentWindow(at);
    let kind = null;
    if (minute === 40) kind = 'fetch';
    else if (minute === 50) kind = 'digest';
    else if (minute === 0) kind = 'send';
    if (!kind) {
      console.warn(`[cron] 忽略未配置阶段：cron=${event.cron || ''} minute=${minute}`);
      return;
    }
    const runId = window;
    try {
      // 心跳只用于诊断；KV 临时故障不应阻止真正的队列任务入队。
      await putRun(env, runId, `cron-${kind}`, { ok: true, cron: event.cron || '', scheduledAt: at, localMinute: minute })
        .catch(error => console.error(`[cron] 心跳写入失败，不阻断任务：${error?.message || error}`));
      if (kind === 'fetch') {
        await enqueueFetch(env, runId, window);
      } else if (kind === 'digest') {
        await env.GUJI_DIGEST_Q.send({ kind: 'digest', runId, window, at: now(), full: false });
      } else {
        await env.GUJI_SEND_Q.send({ kind: 'start', window, force: false, at: now() });
      }
      await putRun(env, runId, `enqueued-${kind}`, { ok: true, at: now() });
      console.log(`[cron] ${kind} 已入队：${window}`);
    } catch (error) {
      await putRun(env, runId, `cron-error-${kind}`, { ok: false, error: String(error?.message || error).slice(0, 1000) }).catch(() => {});
      console.error(`[cron] ${kind} 入队失败：${error?.stack || error}`);
      throw error; // 不把真正失败伪装成成功的 Cron Invocation
    }
  },

  async queue(batch, env) {
    for (const msg of batch.messages) {
      try {
        if (batch.queue === Q_FETCH) await consumeFetch(msg, env);
        else if (batch.queue === Q_DIGEST) await consumeDigest(msg, env);
        else if (batch.queue === Q_SEND) await consumeSend(msg, env);
        else if (batch.queue === Q_DLQ) await consumeDlq(msg, env);
        else {
          console.error(`[queue] 未配置消费者：${batch.queue}`);
          msg.ack();
        }
      } catch (error) {
        console.error(`[queue] ${batch.queue} 处理失败，交由 Cloudflare 重试：${error?.stack || error}`);
        throw error;
      }
    }
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/api/health') {
      const latest = await env.GUJI_KV.get(KV_LATEST, 'json').catch(() => null);
      return json({ ok: true, worker: 'guji-lean', feedCount: FEEDS.length, shardCount: SHARD_COUNT, latest: latest ? { window: latest.window, status: latest.status, generatedAt: latest.generatedAt, items: latest.items?.length || 0, digest: latest.digest?.length || 0, failures: latest.failures?.length || 0 } : null, telegramConfigured: !!env.TG_BOT_TOKEN && !!(env.TG_CHAT_ID || env.TG_CHANNEL), aiConfigured: !!env.AI || !!env.ZHIPU_KEY, diagKeyConfigured: !!env.DIAG_KEY });
    }
    if (url.pathname === '/api/diag' || url.pathname === '/api/build' || url.pathname === '/api/push') {
      if (!isAuthorized(url, env)) return json({ error: '需要配置 DIAG_KEY，并在 URL 中提供正确的 ?key= 值。' }, 403);
    }
    if (url.pathname === '/api/diag') {
      const window = currentWindow();
      const latest = await env.GUJI_KV.get(KV_LATEST, 'json').catch(() => null);
      // 手动 /api/build 会使用独立 runId；同时查看 cron 的 window 记录，避免诊断页把定时心跳漏掉。
      const latestRunId = latest?.window === window ? (latest.runId || window) : window;
      const stageNames = [
        'cron-fetch', 'enqueued-fetch', 'cron-digest', 'enqueued-digest', 'digest',
        'cron-send', 'enqueued-send', 'push-enqueued', 'push-private', 'push-channel',
        'cron-error-fetch', 'cron-error-digest', 'cron-error-send', 'push-error', 'dead-letter',
        ...Array.from({ length: SHARD_COUNT }, (_, i) => `fetch-${i}`),
      ];
      const readStages = async id => {
        const out = {};
        for (const stage of stageNames) out[stage] = await env.GUJI_KV.get(runKey(id, stage), 'json').catch(() => null);
        return out;
      };
      const [cronStages, latestStages] = await Promise.all([readStages(window), latestRunId === window ? Promise.resolve(null) : readStages(latestRunId)]);
      return json({ window, cronRunId: window, latestRunId, feedCount: FEEDS.length, shardCount: SHARD_COUNT, latest: latest ? { window: latest.window, runId: latest.runId, generatedAt: latest.generatedAt, status: latest.status, itemCount: latest.items?.length || 0, digestCount: latest.digest?.length || 0, failures: latest.failures || [], aiError: latest.aiError || null, provider: latest.provider || null } : null, stages: cronStages, latestRunStages: latestStages, config: { telegramToken: !!env.TG_BOT_TOKEN, privateChat: !!env.TG_CHAT_ID, channel: !!env.TG_CHANNEL, workersAI: !!env.AI, zhipuKey: !!env.ZHIPU_KEY, secUserAgent: !!env.SEC_USER_AGENT, kv: !!env.GUJI_KV } });
    }
    if (url.pathname === '/api/build') {
      const window = currentWindow();
      const runId = `${window}-manual-${now()}`;
      await enqueueFetch(env, runId, window);
      await env.GUJI_DIGEST_Q.send({ kind: 'digest', runId, window, at: now(), full: url.searchParams.get('full') === '1' }, { delaySeconds: 90 });
      return json({ ok: true, window, runId, fetchShards: SHARD_COUNT, digestDelaySeconds: 90 });
    }
    if (url.pathname === '/api/push') {
      const window = currentWindow();
      const force = url.searchParams.get('force') === '1';
      await env.GUJI_SEND_Q.send({ kind: 'start', window, force, at: now() });
      return json({ ok: true, window, queued: true, force });
    }
    const latest = await env.GUJI_KV.get(KV_LATEST, 'json').catch(() => null);
    return new Response(renderStatusPage(latest), { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' } });
  },
};

// Exports used by the included smoke tests.
export { FEEDS, SHARD_COUNT, currentWindow, parseFeed, parseEdgar, finalizeItems, parseDigest, extractJsonArray, qBatch };
