import { XMLParser } from 'fast-xml-parser';

// GUJI · 精简版
// 只保留核心链路：Cron → 抓取队列 → AI 精选 → Telegram 推送队列。
// 批量入队必须使用 [{ body: message }]，不能直接传业务消息数组。

const KV_LATEST = 'guji:v4:latest'; // 复用旧状态，保留 lastDigestAt，避免首次部署重复推送旧内容
const KV_ANTHROPIC_SEEN = 'guji:v4:seen'; // 复用旧 Anthropic 去重记录
const KV_REVIEWED = 'guji:v5:reviewed'; // 按事件链接去重，不再只依赖发布时间
const KV_MAJOR_STATE = 'guji:v5:major-monitor'; // 重大事件监测游标与提醒去重账本
const KV_RUN_PREFIX = 'guji:run:';
const Q_FETCH = 'guji-fetch';
const Q_DIGEST = 'guji-digest';
const Q_SEND = 'guji-send';
const Q_RETRY = 'guji-retry'; // 兼容已有队列中可能残留的补推消息
const Q_DLQ = 'guji-dlq';

const BJ_OFFSET_MS = 8 * 60 * 60 * 1000;
const RUN_TTL = 14 * 24 * 60 * 60;
const SOURCE_BATCH_SIZE = 1;
const SOURCE_CONCURRENCY = 1;
const FETCH_TIMEOUT_MS = 10000;
const SHARD_STALE_MS = 15 * 60 * 1000;
const MATERIAL_MAX = 24;
const ARTICLE_ENRICH_MAX = 6;
const REVIEWED_TTL_MS = 14 * 86400_000;
const DIGEST_MAX = 8;
const TELEGRAM_MAX_ATTEMPTS = 3;
const TELEGRAM_GAP_MS = 1100;
const USER_AGENT = 'Mozilla/5.0 (compatible; GUJI-Digest/1.0; +https://openai.com)';
const MAX_AGE_MS = { '72h': 72 * 3600_000, '7d': 7 * 86400_000, '14d': 14 * 86400_000, '30d': 30 * 86400_000 };

const AI_KEYWORDS = /\b(ai|a\.i\.|artificial intelligence|llms?|large language models?|gpt|chatgpt|claude|gemini|copilot|agents?|agentic|machine learning|deep learning|neural|transformers?|diffusion|inference|foundation models?|gpus?|tpus?|npus?|hbm|accelerators?|compute|datacent(er|re)s?|data cent(er|re)s?|chips?|semiconductors?|silicon|nvidia|amd|intel|tsmc|foundry|hpc|blackwell|hopper|rubin|robotics?|humanoids?|autonomous|self-driving|openai|anthropic|deepmind|mistral|grok|llama|qwen|deepseek|sora|multimodal|rag|embeddings?|fine-tun|distillation|quantiz|model release|model training|inference)\b/i;

// 核心源保留一手官方信息、监管文件、精选原始访谈与论文；低优先级重复节目已移出。
// 每个源仅维护一份配置。SEC 仍按公司分别抓取，但共用同一套抓取代码。
const FEEDS = [
  // AI 实验室与生态官方源
  { id: 'openai', name: 'OpenAI', kind: 'rss', cat: '模型', win: '7d', url: 'https://openai.com/news/rss.xml' },
  { id: 'deepmind', name: 'Google DeepMind', kind: 'rss', cat: '模型', win: '7d', filter: true, url: 'https://deepmind.google/blog/rss.xml' },
  { id: 'googleai', name: 'Google AI', kind: 'rss', cat: '模型', win: '7d', url: 'https://blog.google/innovation-and-ai/technology/ai/rss/' },
  { id: 'meta', name: 'Meta', kind: 'rss', cat: '模型', win: '7d', filter: true, url: 'https://about.fb.com/feed/' },
  { id: 'microsoft-ai', name: 'Microsoft AI Blog', kind: 'rss', cat: '模型', win: '7d', filter: true, url: 'https://blogs.microsoft.com/ai/feed/' },
  { id: 'hf', name: 'Hugging Face', kind: 'rss', cat: '模型', win: '7d', url: 'https://huggingface.co/blog/feed.xml' },
  { id: 'anthropic', name: 'Anthropic', kind: 'html', cat: '模型', win: '7d', url: 'https://www.anthropic.com/news' },
  // 芯片与数据中心
  { id: 'nvdanews', name: 'NVIDIA Newsroom', kind: 'rss', cat: '芯片算力', win: '7d', filter: true, url: 'https://nvidianews.nvidia.com/releases.xml' },
  { id: 'nvdablog', name: 'NVIDIA Blog', kind: 'rss', cat: '芯片算力', win: '7d', filter: true, url: 'https://blogs.nvidia.com/feed/' },
  { id: 'arm', name: 'Arm Newsroom', kind: 'rss', cat: '芯片算力', win: '7d', url: 'https://newsroom.arm.com/rss' },
  // 聚合源只负责发现遗漏；入选时保留其链接，并优先追溯所链接的原始报道/公告。
  { id: 'techmeme', name: 'Techmeme（发现与补漏）', kind: 'rss', cat: '科技新闻', win: '72h', filter: true, url: 'https://www.techmeme.com/feed.xml' },
  { id: 'sth', name: 'ServeTheHome', kind: 'rss', cat: '数据中心', win: '72h', filter: true, url: 'https://www.servethehome.com/feed/' },
  // 独立媒体用于补足官方公告之外的产品、商业与产业报道；Techmeme 仅作发现源。
  { id: 'techcrunch-ai', name: 'TechCrunch AI', kind: 'rss', cat: '科技新闻', win: '72h', url: 'https://techcrunch.com/category/artificial-intelligence/feed/' },
  { id: 'verge-ai', name: 'The Verge AI', kind: 'rss', cat: '科技新闻', win: '72h', url: 'https://www.theverge.com/rss/ai-artificial-intelligence/index.xml' },
  { id: 'ars-ai', name: 'Ars Technica AI', kind: 'rss', cat: '科技新闻', win: '72h', url: 'https://arstechnica.com/ai/feed/' },
  { id: 'mittr-ai', name: 'MIT Technology Review AI', kind: 'rss', cat: '科技新闻', win: '7d', url: 'https://www.technologyreview.com/topic/artificial-intelligence/feed/' },
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

// 重大事件监测：每分钟只轮询一个源，6 个源约每 6 分钟复查一次；避免单次 CPU 超限。
// 该清单与日报源共用原始来源，不创建额外队列。
const MAJOR_FEED_IDS = ['openai', 'deepmind', 'nvdanews', 'techmeme', 'techcrunch-ai', 'microsoft-ai'];
const MAJOR_FEEDS = MAJOR_FEED_IDS.map(id => FEEDS.find(feed => feed.id === id)).filter(Boolean);
const MAJOR_ALERT_MAX = 5;
const MAJOR_STATE_TTL = 30 * 24 * 60 * 60;
const MAJOR_CRITICAL_RE = /security breach|data breach|重大安全|安全漏洞|泄露用户数据|critical vulnerability|recall|出口禁令|export ban|export controls?|antitrust|重大诉讼|lawsuit|bankruptcy|破产|acquisition|acquires|acquired|acquisition deal|merger|收购|并购|billion[- ]dollar|\$\d+(?:\.\d+)?\s*billion|\d+\s*billion|capex|capital expenditure|新一代.*(?:gpu|chip|model)|frontier model|flagship model|state-of-the-art|record revenue|重大合作|strategic partnership|mass production|量产/i;
const MAJOR_SIGNAL_RE = /launch|release|announce|unveil|introduc|new model|open.?source|benchmark|gpu|chip|semiconductor|accelerator|inference|datacenter|data center|compute|hbm|foundry|revenue|earnings|guidance|capex|funding|acqui|merger|order|contract|pricing|regulat|antitrust|lawsuit|security|safety|copyright|export control|agent|reasoning|模型发布|芯片|算力|数据中心|融资|收购|合作|营收|资本开支|出口管制|安全漏洞/i;

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
function nodeRawText(value) {
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) return value.map(nodeRawText).join(' ');
  if (value && typeof value === 'object') return String(value['#text'] ?? '');
  return '';
}
function isTechmemeHost(link) {
  try { return /(^|\.)techmeme\.com$/i.test(new URL(link).hostname); } catch { return false; }
}
function firstExternalAnchor(html, excludedHost = /(^|\.)techmeme\.com$/i) {
  const source = decodeEntities(nodeRawText(html));
  const matches = source.matchAll(/<a\b[^>]*?href=["']([^"']+)["'][^>]*>/gi);
  for (const match of matches) {
    try {
      const u = new URL(match[1], 'https://www.techmeme.com/');
      if (u.protocol !== 'https:' && u.protocol !== 'http:') continue;
      if (excludedHost.test(u.hostname)) continue;
      if (/^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/i.test(u.hostname)) continue;
      return u.toString();
    } catch { /* 跳过不合法链接 */ }
  }
  return '';
}
function sourceDomain(link) {
  try { return new URL(link).hostname.replace(/^www\./i, ''); } catch { return ''; }
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
    const rawDesc = entry.description ?? entry.summary ?? entry['content:encoded'] ?? entry.content;
    let link = pickLink(entry.link) || cleanText(entry.id) || cleanText(entry.guid) || feed.url;
    // Techmeme 只负责发现：若 RSS 链接指向聚合页，尽可能提取描述中的原始报道链接。
    if (feed.id === 'techmeme' && isTechmemeHost(link)) {
      link = firstExternalAnchor(rawDesc) || link;
    }
    const ts = pickDate(entry.pubDate, entry.published, entry.updated, entry['dc:date'], entry.date);
    const desc = cleanText(rawDesc);
    const item = makeItem(feed, { title, link, ts, desc });
    if (feed.id === 'techmeme' && !isTechmemeHost(link)) {
      item.source = sourceDomain(link) || feed.name;
      item.resolvedAggregatorLink = true;
    }
    out.push(item);
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
function itemIsAI(item) { return AI_KEYWORDS.test(`${item.title || ''} ${item.desc || ''}`); }
function ageMs(item, current) {
  const max = MAX_AGE_MS[item.win] || MAX_AGE_MS['7d'];
  // 无发布时间的条目不进入日报，避免首次运行把 RSS 历史条目误当成新消息。
  return !item.ts || current - item.ts > max || item.ts > current + 6 * 3600_000;
}
function sourceRole(item) {
  const id = String(item?.srcId || '');
  if (id === 'techmeme') return 'aggregator';
  if (id === 'arxiv') return 'research';
  if (id.startsWith('p-')) return 'interview';
  if (id.startsWith('e-')) return 'filing';
  if (['techcrunch-ai', 'verge-ai', 'ars-ai', 'mittr-ai', 'sth'].includes(id)) return 'journalism';
  return 'official';
}
function sourceAuthority(item) {
  const role = sourceRole(item);
  const base = { official: 6, journalism: 5, research: 4, filing: 4, interview: 3, aggregator: 1 }[role] || 1;
  // Techmeme 发现到原始文章后，链接已指向真实发布方；仍在候选分配上限内按聚合源处理。
  return item?.resolvedAggregatorLink ? 3.5 : base;
}
function sourceItemLimit(item) {
  const role = sourceRole(item);
  if (role === 'aggregator') return 1;
  if (role === 'filing') return 1;
  if (role === 'interview') return 1;
  if (role === 'research') return 4;
  return 2;
}
function candidateSignal(item) {
  const text = `${item.title || ''} ${item.desc || ''}`.toLowerCase();
  let score = 0;
  const signals = [
    /launch|release|announce|introduc|unveil|debut|new model|open.?source|benchmark|state.of.the.art/, 
    /gpu|chip|semiconductor|accelerator|inference|datacenter|data center|compute|hbm|foundry/, 
    /revenue|earnings|guidance|capex|billion|funding|acqui|merger|order|contract|pricing|price cut/, 
    /regulat|antitrust|lawsuit|security|safety|copyright|policy|export control|robot|agent|reasoning/,
  ];
  for (const re of signals) if (re.test(text)) score++;
  if ((item.desc || '').length > 250) score += 0.5;
  return score;
}
function candidateQuality(item) {
  const ageHours = item.ts ? Math.max(0, (now() - item.ts) / 3600_000) : 168;
  const freshness = Math.max(0, 3 - ageHours / 24);
  return sourceAuthority(item) * 2 + candidateSignal(item) * 1.5 + freshness;
}
function preferredDuplicate(a, b) {
  const ra = sourceAuthority(a), rb = sourceAuthority(b);
  if (ra !== rb) return ra > rb ? a : b;
  if ((a.desc || '').length !== (b.desc || '').length) return (a.desc || '').length > (b.desc || '').length ? a : b;
  return (a.ts || 0) >= (b.ts || 0) ? a : b;
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
    const maxPerSource = item.srcId === 'sth' || item.srcId === 'arxiv' ? 8 : 30;
    if (count >= maxPerSource) continue;
    counts.set(item.srcId, count + 1);
    const key = normTitle(item.title);
    if (!key) continue;
    const existing = byTitle.get(key);
    if (existing) {
      const chosen = preferredDuplicate(existing, item);
      const also = [...new Set([...(existing.also || []), ...(item.also || []), existing.source, item.source])]
        .filter(name => name && name !== chosen.source);
      const desc = (existing.desc || '').length >= (item.desc || '').length ? existing.desc : item.desc;
      byTitle.set(key, { ...chosen, desc: desc || '', ts: Math.max(existing.ts || 0, item.ts || 0), also });
    } else {
      byTitle.set(key, { ...item, also: [] });
    }
  }
  return [...byTitle.values()].sort((a, b) => b.ts - a.ts).slice(0, 120);
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
function extractAnthropicDate(html, anchorIndex) {
  // 只使用卡片或附近 HTML 中明确存在的发布日期；找不到就返回 0，绝不把抓取时间伪装成发布时间。
  const articleStart = html.lastIndexOf('<article', anchorIndex);
  const articleEnd = html.indexOf('</article>', anchorIndex);
  let context = '';
  if (articleStart >= 0 && articleEnd > anchorIndex && articleEnd - articleStart < 7000) {
    context = html.slice(articleStart, articleEnd + 10);
  } else {
    context = html.slice(Math.max(0, anchorIndex - 500), Math.min(html.length, anchorIndex + 1800));
  }
  const candidates = [
    ...[...context.matchAll(/\bdatetime=["']([^"']+)["']/gi)].map(m => m[1]),
    ...[...context.matchAll(/\bdatePublished\s*["']?\s*:\s*["']([^"']+)["']/gi)].map(m => m[1]),
    ...[...context.matchAll(/\bdateCreated\s*["']?\s*:\s*["']([^"']+)["']/gi)].map(m => m[1]),
  ];
  for (const candidate of candidates) {
    const ts = Date.parse(candidate);
    if (Number.isFinite(ts) && ts > 0 && ts <= now() + 6 * 3600_000) return ts;
  }
  return 0;
}

async function fetchAnthropic(feed, env) {
  const html = await fetchText(feed.url, { headers: { 'User-Agent': USER_AGENT, Accept: 'text/html,*/*' } });
  // 不在抓取时提前记为 seen：若之后 AI 审核失败，文章仍必须能在下一轮重新进入候选池。
  // 跨轮去重统一交给 KV_REVIEWED 的事件账本处理。
  const found = [];
  const nav = new Set(['announcements', 'engineering', 'research', 'policy', 'news', 'company', 'public-benefit']);
  const pattern = /<a[^>]*class="[^"]*FeaturedGrid-module[^\"]*tileLink[^\"]*"[^>]*href="\/news\/([a-z0-9-]+)"[^>]*>([\s\S]*?)<\/a>/gi;
  let match;
  while ((match = pattern.exec(html)) && found.length < 12) {
    const slug = match[1];
    if (nav.has(slug)) continue;
    const title = cleanText(match[2]);
    if (!title || title.length < 8) continue;
    const ts = extractAnthropicDate(html, match.index);
    // 页面没有明确发布日期时不伪造时间；这条会被新鲜度过滤器暂时排除。
    found.push(makeItem(feed, { title: `Anthropic：${title}`, link: `https://www.anthropic.com/news/${slug}`, ts, desc: '' }));
  }
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
  const sourceStats = [];
  for (let i = 0; i < feeds.length; i += SOURCE_CONCURRENCY) {
    const group = feeds.slice(i, i + SOURCE_CONCURRENCY);
    const results = await Promise.all(group.map(feed => fetchOne(feed, env)));
    for (let j = 0; j < results.length; j++) {
      const result = results[j];
      const feed = group[j];
      items.push(...result.items);
      if (result.failure) failures.push(result.failure);
      sourceStats.push({ id: feed.id, source: feed.name, ok: !result.failure, itemCount: result.items.length, newestTs: result.items.reduce((max, item) => Math.max(max, item.ts || 0), 0), error: result.failure?.error || null, checkedAt: now() });
    }
  }
  return { items, failures, sourceStats };
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

function canonicalLink(link) {
  try {
    const url = new URL(String(link || ''));
    url.hash = '';
    for (const key of [...url.searchParams.keys()]) {
      if (/^(utm_.+|ref|source|fbclid|gclid|mc_cid|mc_eid)$/i.test(key)) url.searchParams.delete(key);
    }
    url.hostname = url.hostname.toLowerCase();
    url.pathname = url.pathname.replace(/\/+$/, '') || '/';
    return url.toString().slice(0, 1500);
  } catch { return String(link || '').trim().slice(0, 1500); }
}
function eventKey(item) {
  return canonicalLink(item.link) || normTitle(item.title);
}
function selectMaterial(items, limit = MATERIAL_MAX) {
  // 不是按更新时间把前 24 条塞给 AI。先给不同类型来源保留名额，防止高频聚合源垄断。
  const quotas = { journalism: 9, official: 7, filing: 3, interview: 2, research: 2, aggregator: 1 };
  const allGroups = new Map();
  for (const item of items) {
    const key = item.srcId || item.source || 'unknown';
    if (!allGroups.has(key)) allGroups.set(key, []);
    allGroups.get(key).push(item);
  }
  for (const group of allGroups.values()) group.sort((a, b) => candidateQuality(b) - candidateQuality(a) || (b.ts || 0) - (a.ts || 0));
  const sourceKeys = [...allGroups.keys()].sort((a, b) => {
    const aa = allGroups.get(a)[0], bb = allGroups.get(b)[0];
    return candidateQuality(bb) - candidateQuality(aa) || a.localeCompare(b);
  });
  const selected = [];
  const selectedKeys = new Set();
  const sourceCounts = new Map();
  const roleCounts = new Map();
  const takeRole = (role, quota) => {
    while (selected.length < limit && (roleCounts.get(role) || 0) < quota) {
      let added = false;
      // 每轮优先从尚未贡献条目的来源各取一条；只有候选不足时才取同源第二条。
      for (const key of sourceKeys) {
        const group = allGroups.get(key) || [];
        const roleItems = group.filter(item => sourceRole(item) === role);
        if (!roleItems.length) continue;
        const count = sourceCounts.get(key) || 0;
        if (count >= sourceItemLimit(roleItems[0])) continue;
        const item = roleItems.find(candidate => !selectedKeys.has(eventKey(candidate)));
        if (!item) continue;
        selected.push(item);
        selectedKeys.add(eventKey(item));
        sourceCounts.set(key, count + 1);
        roleCounts.set(role, (roleCounts.get(role) || 0) + 1);
        added = true;
        if (selected.length >= limit || (roleCounts.get(role) || 0) >= quota) break;
      }
      if (!added) break;
    }
  };
  for (const role of ['journalism', 'official', 'filing', 'research', 'interview', 'aggregator']) {
    takeRole(role, quotas[role] || 0);
  }
  // 某类源暂时没更新时，剩余名额由其他未选事件补齐；来源硬上限仍生效。
  const remaining = items.slice().sort((a, b) => candidateQuality(b) - candidateQuality(a) || (b.ts || 0) - (a.ts || 0));
  for (const item of remaining) {
    if (selected.length >= limit) break;
    const id = eventKey(item), key = item.srcId || item.source || 'unknown';
    if (selectedKeys.has(id) || (sourceCounts.get(key) || 0) >= sourceItemLimit(item)) continue;
    selected.push(item); selectedKeys.add(id); sourceCounts.set(key, (sourceCounts.get(key) || 0) + 1);
  }
  return selected.sort((a, b) => (b.ts || 0) - (a.ts || 0));
}
function htmlToText(html) {
  return decodeEntities(String(html || '')
    .replace(/<script\b[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript\b[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<svg\b[\s\S]*?<\/svg>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')).trim();
}
async function enrichCandidates(items) {
  // 先按实质信号挑少量高潜力候选，读取正文而不只补缺失摘要；控制额外请求数。
  const targets = items.filter(item => item.link && /^https?:\/\//i.test(item.link) && !isTechmemeHost(item.link))
    .slice().sort((a, b) => candidateQuality(b) - candidateQuality(a) || (b.ts || 0) - (a.ts || 0))
    .slice(0, ARTICLE_ENRICH_MAX);
  for (const item of targets) {
    try {
      const html = await fetchText(item.link, { headers: { 'User-Agent': USER_AGENT, Accept: 'text/html,application/xhtml+xml' } });
      const main = html.match(/<(?:article|main)\b[^>]*>([\s\S]*?)<\/(?:article|main)>/i)?.[1] || html;
      const text = htmlToText(main);
      if (text.length > (item.desc || '').length + 100) {
        item.desc = text.slice(0, 2600);
        item.fullText = true;
      }
    } catch (error) {
      console.warn(`[enrich] ${item.srcId}: ${String(error?.message || error).slice(0, 160)}`);
    }
  }
  return items;
}
async function loadReviewed(env) {
  const data = await env.GUJI_KV.get(KV_REVIEWED, 'json').catch(() => null);
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
  const cutoff = now() - REVIEWED_TTL_MS;
  return Object.fromEntries(Object.entries(data).filter(([, ts]) => Number(ts) >= cutoff).slice(-2000));
}
async function saveReviewed(env, reviewed, items) {
  const at = now();
  for (const item of items) reviewed[eventKey(item)] = at;
  const cutoff = at - REVIEWED_TTL_MS;
  const entries = Object.entries(reviewed).filter(([, ts]) => Number(ts) >= cutoff).sort((a, b) => Number(b[1]) - Number(a[1])).slice(0, 2000);
  await env.GUJI_KV.put(KV_REVIEWED, JSON.stringify(Object.fromEntries(entries)), { expirationTtl: 30 * 86400 });
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
const buildPrompt = (items) => `你是科技与 AI 日报编辑。请从以下素材中选择最值得读者知道的 0-${DIGEST_MAX} 条，先判断是否有实质变化，再综合影响力（0-3）、信息增量（0-3）、来源可靠性（0-2）、与 AI/算力产业的相关性（0-2）。优先选总分约 7 分以上的内容，但不机械凑分或凑数；不要重复讲同一事件。\n\n严格依据提供的标题和摘要，不补充外部知识、不猜测缺失数字、不把观点说成事实。将每条入选内容用简体中文写成简洁标题和 1-2 句摘要。保留原文语气，不强化结论。输出必须是纯 JSON 数组，每项格式：{"idx":1,"title":"中文标题","summary":"中文摘要"}。idx 必须对应素材编号。无值得推送的内容时返回 []。\n\n素材：\n${items.map((it, i) => `${i + 1}. [${it.cat}] ${it.title}\n来源：${it.source}\n链接：${it.link}\n摘要/正文片段：${(it.desc || '原文未提供摘要').slice(0, it.fullText ? 1300 : 420)}`).join('\n\n')}`;
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
  await env.GUJI_KV.put(shardKey(runId, shard), JSON.stringify({ items: result.items, failures: result.failures, sourceStats: result.sourceStats, at: now() }), { expirationTtl: RUN_TTL });
  await putRun(env, runId, `fetch-${shard}`, { ok: true, items: result.items.length, failures: result.failures.length, failureDetails: result.failures, sourceStats: result.sourceStats });
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
  const sourceStats = shards.flatMap(s => s.sourceStats || []);
  const previous = await env.GUJI_KV.get(KV_LATEST, 'json').catch(() => null);
  const reviewedLoaded = await loadReviewed(env);
  // 仅在升级后的第一次运行使用旧时间戳做兼容；之后以标准化 URL/事件去重，避免晚到文章被漏掉。
  const lastDigestAt = full ? 0 : msFromLegacyTime(previous?.lastDigestAt);
  const bootstrap = reviewedLoaded === null;
  const reviewed = reviewedLoaded || {};
  const unseen = allItems.filter(item => {
    const key = eventKey(item);
    if (reviewed[key]) return false;
    return !bootstrap || !lastDigestAt || (item.ts && item.ts > lastDigestAt);
  });
  const material = selectMaterial(unseen, MATERIAL_MAX);
  await enrichCandidates(material);
  let digest = [];
  let aiError = null;
  let provider = null;
  const sourceFailureRatio = sourceStats.length ? failures.length / sourceStats.length : 0;
  const fetchDegraded = missingShards.length > 0 || (allItems.length === 0 && failures.length > 0) || sourceFailureRatio >= 0.4;
  let status = fetchDegraded ? 'partial-fetch' : (material.length ? 'no-important-items' : 'no-new-items');
  if (material.length) {
    const result = await makeDigest(material, env);
    digest = result.digest;
    aiError = result.error;
    provider = result.provider;
    status = result.error ? 'ai-failed' : (digest.length ? ((missingShards.length || sourceFailureRatio >= 0.4) ? 'partial' : 'ok') : (fetchDegraded ? 'partial-no-important-items' : 'no-important-items'));
    // AI 成功完成审核后才标记已看过；AI 故障时保留候选，供下一轮重新处理。
    if (!result.error) await saveReviewed(env, reviewed, material);
  }
  const data = {
    window, runId, generatedAt: now(), lastDigestAt: (digest.length || status === 'no-important-items') ? now() : (previous?.lastDigestAt || 0),
    missingShards,
    items: allItems, newItems: material, digest, failures, sourceStats,
    status, aiError, provider, totalSources: FEEDS.length,
  };
  await env.GUJI_KV.put(KV_LATEST, JSON.stringify(data), { expirationTtl: RUN_TTL });
  await putRun(env, runId, 'digest', { ok: true, completed: true, status, inputItems: material.length, digestItems: digest.length, failures: failures.length, sourceStats, aiError, provider });
  msg.ack();
  console.log(`[digest] ${window}: status=${status}, input=${material.length}, selected=${digest.length}`);
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
    const { window, force = false, pushId = window } = body;
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
    await qBatch(env.GUJI_SEND_Q, targets.map(target => ({ kind: 'chat', window, pushId, role: target.role, chat: target.chat, messages })));
    // qBatch 已成功确认消息持久化；状态写入失败不能让整个启动消息重试造成重复推送。
    await putRun(env, runId, 'push-enqueued', { ok: true, targets: targets.map(t => t.role), messages: messages.length }).catch(error => console.error('[send] 写入推送记录失败：' + error.message));
    msg.ack();
    return;
  }
  if (body.kind !== 'chat') throw new Error(`未知推送消息类型：${String(body.kind)}`);
  const { window, pushId = window, role, chat, messages = [] } = body;
  if (!env.TG_BOT_TOKEN || !chat) {
    await putRun(env, window, `push-${role}`, { ok: false, error: 'Telegram token 或 chat 缺失' }).catch(() => {});
    msg.ack();
    return;
  }
  const progressKey = `guji:push-progress:${encodeURIComponent(pushId)}:${encodeURIComponent(role)}`;
  const progress = await env.GUJI_KV.get(progressKey, 'json').catch(() => null) || {};
  let okCount = Object.keys(progress).filter(k => progress[k] === true).length;
  for (let i = 0; i < messages.length; i++) {
    if (progress[i] === true) continue; // 重试时跳过已成功消息，避免整批重复推送
    if (i > 0) await sleep(TELEGRAM_GAP_MS);
    let result = { ok: false, error: '未发送' };
    for (let attempt = 1; attempt <= TELEGRAM_MAX_ATTEMPTS; attempt++) {
      try { result = await sendTelegramOne(env.TG_BOT_TOKEN, chat, messages[i]); }
      catch (error) { result = { ok: false, status: 0, error: String(error?.message || error), retryAfter: 0 }; }
      if (result.ok || isPermanentTelegramError(result) || attempt === TELEGRAM_MAX_ATTEMPTS) break;
      const pause = result.retryAfter ? Math.min(result.retryAfter + 1, 45) * 1000 : 1500 * attempt;
      await sleep(pause);
    }
    if (!result.ok) {
      await putRun(env, window, `push-${role}`, { ok: false, success: okCount, total: messages.length, failedIndex: i, error: result.error || 'unknown', status: result.status || 0 }).catch(() => {});
      // 让 Cloudflare 重试/进入 DLQ，而不是把失败消息确认丢弃。
      throw new Error(`Telegram ${role} 第 ${i + 1}/${messages.length} 条发送失败：${result.error || 'unknown'}`);
    }
    progress[i] = true;
    okCount++;
    await env.GUJI_KV.put(progressKey, JSON.stringify(progress), { expirationTtl: RUN_TTL });
  }
  await putRun(env, window, `push-${role}`, { ok: true, success: okCount, total: messages.length, errors: [] }).catch(error => console.error('[send] 写入推送结果失败：' + error.message));
  msg.ack();
  console.log(`[send] ${window} ${role}: ${okCount}/${messages.length} 条成功`);
}
async function consumeRetry(msg, env) {
  const body = msg.body || {};
  const { window = 'retry', slice = 0, attempts = 1, pairs = [] } = body;
  if (!Array.isArray(pairs) || !pairs.length) { msg.ack(); return; }
  const failed = [];
  for (const pair of pairs) {
    if (!pair?.chat || !pair?.text || !env.TG_BOT_TOKEN) continue;
    let result;
    try { result = await sendTelegramOne(env.TG_BOT_TOKEN, pair.chat, pair.text); }
    catch (error) { result = { ok: false, status: 0, error: String(error?.message || error), retryAfter: 0 }; }
    if (!result.ok && !isPermanentTelegramError(result)) failed.push(pair);
    if (!result.ok && isPermanentTelegramError(result)) console.error(`[retry] 永久错误，放弃：${result.error}`);
  }
  await putRun(env, window, `retry-${slice}`, { ok: failed.length === 0, attempts, total: pairs.length, failed: failed.length }).catch(() => {});
  if (failed.length && attempts < 3) {
    await env.GUJI_RETRY_Q.send({ kind: 'retry', window, slice, attempts: attempts + 1, pairs: failed });
    console.warn(`[retry] ${window} 仍有 ${failed.length} 条失败，进入第 ${attempts + 1} 轮`);
  } else if (failed.length) {
    console.error(`[retry] ${window} 达到重试上限，剩余失败 ${failed.length} 条`);
  }
  msg.ack();
}

async function consumeDlq(msg, env) {
  const body = msg.body || {};
  console.error(`[DLQ] ${JSON.stringify(body).slice(0, 700)}`);
  if (body.window) await putRun(env, body.runId || body.window, 'dead-letter', { ok: false, message: body, attempts: msg.attempts }).catch(() => {});
  msg.ack();
}

function isPotentialMajor(item) {
  const text = `${item.title || ''} ${item.desc || ''}`;
  if (!itemIsAI(item) || !MAJOR_SIGNAL_RE.test(text)) return false;
  // 官方源、独立媒体和能追溯原始报道的 Techmeme 才能进入提醒审核。
  const trusted = ['official', 'journalism', 'aggregator'].includes(sourceRole(item));
  if (!trusted) return false;
  return MAJOR_CRITICAL_RE.test(text) || sourceRole(item) === 'official' || candidateSignal(item) >= 2;
}
function majorPrompt(items) {
  return `你是 GUJI 的重大事件监测编辑。只挑选会明显改变 AI/芯片/算力/数据中心产业判断的事件。宁可漏掉普通产品更新，也不要把例行博客、普通功能更新、观点文章、旧闻、传闻当重大事件。\n\n只有满足以下至少一项才提醒：重要前沿模型/芯片正式发布或能力出现实质跃迁；重大算力采购、资本开支、订单、融资、并购或战略合作；监管/出口管制/重大诉讼；严重安全事件；重要公司财务或产业格局发生显著变化。必须以给定材料为准，不得补充事实。若没有符合条件的事件，输出 []。\n\n输出纯 JSON 数组，每项格式：{"idx":1,"title":"简体中文标题","summary":"一到两句中文，说明新发生了什么以及为什么重要"}。最多选 ${MAJOR_ALERT_MAX} 条。\n\n候选：\n${items.map((it,i)=>`${i+1}. 来源：${it.source}\n标题：${it.title}\n时间：${it.ts ? bjStamp(it.ts) : '原文未提供'}\n链接：${it.link}\n摘要：${(it.desc || '原文未提供摘要').slice(0,500)}`).join('\n\n')}`;
}
async function classifyMajorAlerts(items, env) {
  if (!items.length) return [];
  const prompt = majorPrompt(items.slice(0, 12));
  let text = '';
  if (env.ZHIPU_KEY) {
    text = await callZhipu(prompt, env);
  } else if (env.AI) {
    const result = await env.AI.run('@cf/qwen/qwen3-30b-a3b-fp8', {
      messages: [{ role: 'user', content: prompt }], max_tokens: 1800, temperature: 0.1,
      chat_template_kwargs: { enable_thinking: false },
    });
    text = aiResultText(result);
  } else {
    // 未配置模型时仅允许极强信号触发，避免把普通更新误报成重大事件。
    return items.filter(item => MAJOR_CRITICAL_RE.test(`${item.title} ${item.desc}`) && sourceRole(item) !== 'aggregator').slice(0, MAJOR_ALERT_MAX).map(item => ({ item, title: item.title, summary: (item.desc || '来源发布了可能影响 AI/算力产业的重要信息。').slice(0, 450) }));
  }
  const rows = extractJsonArray(text);
  if (!Array.isArray(rows)) throw new Error('重大事件模型没有返回有效 JSON 数组');
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const idx = Number(row?.idx);
    if (!Number.isInteger(idx) || idx < 1 || idx > Math.min(items.length, 12) || seen.has(idx)) continue;
    const item = items[idx - 1];
    const title = String(row.title || '').trim();
    const summary = String(row.summary || '').trim();
    if (!title || !summary) continue;
    seen.add(idx);
    out.push({ item, title: title.slice(0, 180), summary: summary.slice(0, 800) });
  }
  return out.slice(0, MAJOR_ALERT_MAX);
}
async function sendMajorAlert(env, alert) {
  if (!env.TG_BOT_TOKEN) throw new Error('缺少 TG_BOT_TOKEN，重大事件提醒无法发送');
  // 与每日日报一致：同时发私聊与频道，避免频道收不到实时突发事件。
  const chats = [env.TG_CHAT_ID, env.TG_CHANNEL].filter(Boolean);
  if (!chats.length) throw new Error('缺少 TG_CHAT_ID/TG_CHANNEL，重大事件提醒无接收目标');
  const item = alert.item;
  const body = `🚨 GUJI 重大事件提醒\n\n${alert.title}\n\n${alert.summary}\n\n来源：${item.source} · ${item.ts ? bjStamp(item.ts) : '原文未提供时间'}\n${item.link}`;
  for (const chat of chats) {
    let result = { ok: false, error: '未发送' };
    for (let attempt = 1; attempt <= TELEGRAM_MAX_ATTEMPTS; attempt++) {
      try { result = await sendTelegramOne(env.TG_BOT_TOKEN, chat, safeByteTruncate(body, 3500)); }
      catch (error) { result = { ok: false, error: String(error?.message || error), status: 0 }; }
      if (result.ok || isPermanentTelegramError(result) || attempt === TELEGRAM_MAX_ATTEMPTS) break;
      await sleep(result.retryAfter ? Math.min(result.retryAfter + 1, 30) * 1000 : attempt * 1200);
    }
    if (!result.ok) throw new Error(`重大事件 Telegram 推送失败（${chat}）：${result.error || 'unknown'}`);
  }
}
async function monitorMajorEvents(event, env) {
  if (!MAJOR_FEEDS.length) return;
  const at = Number(event.scheduledTime) || now();
  // 每分钟确定性轮转，不需要为调度指针额外写 KV。
  const slot = Math.floor(at / 60000) % MAJOR_FEEDS.length;
  const feed = MAJOR_FEEDS[slot];
  const state = await env.GUJI_KV.get(KV_MAJOR_STATE, 'json').catch(() => null) || { feeds: {}, alerted: [] };
  const previousKeys = Array.isArray(state.feeds?.[feed.id]) ? state.feeds[feed.id] : null;
  const result = await fetchOne(feed, env);
  if (result.failure) {
    console.warn(`[major] ${feed.id} 抓取失败，本轮不推进游标：${result.failure.error}`);
    return; // 失败时不更新游标，后续轮次会重新尝试
  }
  const items = result.items.filter(item => item.title && item.link).sort((a,b)=>(b.ts||0)-(a.ts||0)).slice(0,40);
  const currentKeys = items.map(eventKey).filter(Boolean);
  if (previousKeys === null) {
    // 首次部署只建立基线，不把 RSS 里已有的旧文章当作突发事件。
    state.feeds ||= {};
    state.feeds[feed.id] = currentKeys;
    await env.GUJI_KV.put(KV_MAJOR_STATE, JSON.stringify(state), { expirationTtl: MAJOR_STATE_TTL });
    console.log(`[major] ${feed.id} 已建立首次扫描基线：${items.length} 条`);
    return;
  }
  const prior = new Set(previousKeys);
  const newlySeen = items.filter(item => !prior.has(eventKey(item)));
  const alreadyAlerted = new Set(Array.isArray(state.alerted) ? state.alerted : []);
  const candidates = newlySeen.filter(isPotentialMajor).filter(item => !alreadyAlerted.has(eventKey(item))).slice(0,12);
  const newlyAlerted = [];
  try {
    const alerts = await classifyMajorAlerts(candidates, env);
    for (const alert of alerts) {
      const key = eventKey(alert.item);
      if (!key || alreadyAlerted.has(key)) continue;
      await sendMajorAlert(env, alert);
      alreadyAlerted.add(key);
      newlyAlerted.push(key);
      // 先落提醒账本：即使随后另一条发送失败，也不会重复发送已成功的提醒。
      state.alerted = [...alreadyAlerted].slice(-500);
      await env.GUJI_KV.put(KV_MAJOR_STATE, JSON.stringify(state), { expirationTtl: MAJOR_STATE_TTL });
      console.log(`[major] 已发送提醒：${alert.title}`);
    }
  } catch (error) {
    // 不推进本源游标，失败候选会在下一次扫描重新进入审核。
    state.alerted = [...alreadyAlerted].slice(-500);
    if (newlyAlerted.length) await env.GUJI_KV.put(KV_MAJOR_STATE, JSON.stringify(state), { expirationTtl: MAJOR_STATE_TTL }).catch(() => {});
    console.error(`[major] ${feed.id} 事件审核/推送失败：${error?.message || error}`);
    throw error;
  }
  state.feeds ||= {};
  state.feeds[feed.id] = currentKeys;
  state.alerted = [...alreadyAlerted].slice(-500);
  // 无新增事件时不写 KV，降低免费套餐写入量。
  const changed = newlySeen.length > 0 || !previousKeys;
  if (changed || newlyAlerted.length) await env.GUJI_KV.put(KV_MAJOR_STATE, JSON.stringify(state), { expirationTtl: MAJOR_STATE_TTL });
  console.log(`[major] ${feed.id}: 新条目 ${newlySeen.length}，候选 ${candidates.length}，提醒 ${newlyAlerted.length}`);
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
  return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>GUJI AI 日报</title><style>body{max-width:760px;margin:32px auto;padding:0 16px;font:16px/1.6 system-ui;color:#202124}article{border-top:1px solid #ddd;padding:14px 0}a{color:#175dcc}small{color:#666}</style></head><body><h1>GUJI AI 日报</h1><p>班次：${htmlEscape(data.window)} · 状态：${htmlEscape(data.status)} · 更新：${htmlEscape(bjStamp(data.generatedAt))}</p><p>抓取候选 ${Number(data.items?.length || 0)} 条；AI 精选 ${Number(data.digest?.length || 0)} 条；抓取失败 ${Number(data.failures?.length || 0)} 个源。</p><details><summary>来源运行状态</summary><ul>${(data.sourceStats || []).map(x => `<li>${htmlEscape(x.source)}：${x.ok ? '成功' : '失败'}，候选 ${Number(x.itemCount || 0)} 条${x.error ? `；${htmlEscape(x.error)}` : ''}</li>`).join('')}</ul></details>${entries || '<p>本次没有可展示的中文精选。请查看状态或诊断接口。</p>'}</body></html>`;
}

export default {
  async scheduled(event, env) {
    const at = Number(event.scheduledTime) || now();
    // 第五个 Cron Trigger 每分钟运行一次；每次只抓一个监测源，避免免费套餐 10ms CPU 限额被多源解析击穿。
    if (event.cron === '* * * * *') {
      try { await monitorMajorEvents(event, env); }
      catch (error) { console.error(`[major] 本轮失败，将由后续轮询再次发现：${error?.stack || error}`); }
    }
    const local = bjDate(at);
    const minute = local.getUTCMinutes();
    const window = currentWindow(at);
    let kind = null;
    if (minute === 40) kind = 'fetch';
    else if (minute === 50) kind = 'digest';
    else if (minute === 0) kind = 'send';
    if (!kind) {
      if (event.cron !== '* * * * *') console.warn(`[cron] 忽略未配置阶段：cron=${event.cron || ''} minute=${minute}`);
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
        await env.GUJI_SEND_Q.send({ kind: 'start', window, pushId: window, force: false, at: now() });
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
        else if (batch.queue === Q_RETRY) await consumeRetry(msg, env);
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
      return json({ ok: true, worker: 'guji-rewrite-major-alerts', feedCount: FEEDS.length, shardCount: SHARD_COUNT, majorMonitor: { enabled: true, pollIntervalMinutesPerSource: MAJOR_FEEDS.length, sources: MAJOR_FEEDS.map(feed => feed.name) }, latest: latest ? { window: latest.window, status: latest.status, generatedAt: latest.generatedAt, items: latest.items?.length || 0, digest: latest.digest?.length || 0, failures: latest.failures?.length || 0, sourceStats: latest.sourceStats || [] } : null, telegramConfigured: !!env.TG_BOT_TOKEN && !!(env.TG_CHAT_ID || env.TG_CHANNEL), aiConfigured: !!env.AI || !!env.ZHIPU_KEY, diagKeyConfigured: !!env.DIAG_KEY });
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
      return json({ window, cronRunId: window, latestRunId, feedCount: FEEDS.length, shardCount: SHARD_COUNT, latest: latest ? { window: latest.window, runId: latest.runId, generatedAt: latest.generatedAt, status: latest.status, itemCount: latest.items?.length || 0, digestCount: latest.digest?.length || 0, failures: latest.failures || [], sourceStats: latest.sourceStats || [], aiError: latest.aiError || null, provider: latest.provider || null } : null, stages: cronStages, latestRunStages: latestStages, config: { telegramToken: !!env.TG_BOT_TOKEN, privateChat: !!env.TG_CHAT_ID, channel: !!env.TG_CHANNEL, workersAI: !!env.AI, zhipuKey: !!env.ZHIPU_KEY, secUserAgent: !!env.SEC_USER_AGENT, kv: !!env.GUJI_KV } });
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
      await env.GUJI_SEND_Q.send({ kind: 'start', window, pushId: force ? `${window}-force-${now()}` : window, force, at: now() });
      return json({ ok: true, window, queued: true, force });
    }
    const latest = await env.GUJI_KV.get(KV_LATEST, 'json').catch(() => null);
    return new Response(renderStatusPage(latest), { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' } });
  },
};

// Exports used by the included smoke tests.
export { FEEDS, SHARD_COUNT, MAJOR_FEEDS, currentWindow, parseFeed, parseEdgar, finalizeItems, parseDigest, extractJsonArray, qBatch, selectMaterial, canonicalLink, eventKey, sourceRole, candidateQuality, firstExternalAnchor, isPotentialMajor };
