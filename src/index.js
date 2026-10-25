// ============================================================
// GUJI v4 · AI 情报日报（Cloudflare Worker · 严格一手来源）
// 只收：官方发布（大厂官网/博客）、SEC EDGAR 官方 filing、
//       原创工程媒体、深度访谈播客、arXiv 论文
// 不收：二手聚合、编译转载、新闻通稿网站
// ============================================================
//
// ★ v4 架构变革：队列化（Queues）
//   v3 及以前：一个 cron 调用里跑完「抓取 39 源 + 精选」，子请求峰值 47~50/50，
//              余量为 0——每加一个源、每加一次 enrich 都要重算一遍钢丝。
//   v4 现在：  cron 只负责入队；抓取/精选/推送各自是独立的消费者调用，
//              每次调用享有独立的 50 子请求预算，峰值 29/50，余量 21。
//              瞬时失败由队列自动退避重试兜底（05:50 那类事故的根治）。
//
// 数据流：
//   cron :40 ──入队 4 条──▶ [guji-fetch 消费者]   每片 10 源抓取 → 写分片 KV
//   cron :50 ──入队 1 条──▶ [guji-digest 消费者]  读 4 分片 → 合并 → enrich → AI 精选 → 写 latest
//   cron :00 ──入队 3 条──▶ [guji-send 消费者]    读 latest → 顺序分片限速推送 TG
//   任一消费者重试耗尽 → [guji-dlq 消费者] 记日志（不静默丢失）
//
// 本文件按「关注点分区」组织，顺序即依赖顺序（前面的区不依赖后面的区）：
//
//   §1  config    配置与源清单        —— 改源/改窗口/改超时只看这里
//   §2  text      纯文本工具          —— esc/stripTags/normTitle/cleanMd/tgTruncate
//   §3  time      时间工具            —— 统一毫秒/秒单位，消除裸 *1000；currentWindow
//   §4  xml       XML 解析基础设施
//   §5  parsers   源数据解析（纯函数）
//   §6  budget    子请求预算（记账不改流；现为「每阶段独立预算」）
//   §7  net       子请求网关（★ 所有 fetch/KV/AI/队列发送的唯一出口）
//   §8  sources   单源抓取
//   §9  shard     分片抓取与合并后处理（纯函数 finalizeItems）
//   §10 digest    模型链与每日精选
//   §11 telegram  推送（顺序分片 + 单条重试）
//   §12 render    网页渲染
//   §13 queue     队列消费者（抓取分片 / 精选 / 推送分片 / 死信）
//   §14 worker    入口（cron 入队 + 路由）
//   §15 exports   对外导出（供单测/调试引用，Worker 只使用 default 导出）
//
// ⚠️ 硬约束（免费套餐）：单次 Worker 调用最多 50 个子请求（fetch / KV / AI /
//    队列发送都算，第 51 个不抛异常、只在响应里报错——静默失败）。
//    队列化后每个消费者调用独立预算（见 §6 BUDGET_PLAN，峰值 29/50），
//    但纪律不变：新增任何子请求前，先过一遍 §6 budget 的账。
//    除 §7 net 区外，不得出现绕过网关的裸子请求调用。

import { XMLParser } from 'fast-xml-parser';

// ============================================================
// §1 config · 配置与源清单
// ============================================================

// ---------------- KV ----------------
const KV_KEY = 'guji:v4:latest';       // 主状态：{items, failures, generatedAt, digest, lastDigestAt, window, logs, digestErr, aiTrace, budget}
const SEEN_SET_KEY = 'guji:v4:seen';   // 跨调用去重集合（Anthropic slug，单次读写）

// ---------------- 队列 ----------------
// 队列名须与 wrangler.toml [[queues.producers]] 一致；v4 部署时自动创建。
const Q_FETCH = 'guji-fetch';   // 抓取分片（4 条消息 × 10 源）
const Q_DIGEST = 'guji-digest'; // 精选编制（1 条消息）
const Q_SEND = 'guji-send';     // 限速推送（3 条顺序分片消息）
const Q_RETRY = 'guji-retry';   // 推送补推（失败项带 attempts 重入，见 §13.5；v4.0.1 新增）
const Q_DLQ = 'guji-dlq';       // 死信（重试耗尽落地，消费者只记日志）

// ---------------- 分片参数 ----------------
const SHARD_SIZE = 10;          // 每个抓取分片的源数（39 源 → 4 片）
const SEND_SLICES = 3;          // 推送分片数（顺序发送，保序；单片 ≤9 条消息 ×3 次尝试 ≤ 27 子请求）
// 精选消费者等分片就绪的最长时间（手动 /api/build 把抓取与精选同时入队时的竞态保险；
// 生产 cron 路径 :40 抓取 → :50 精选有 10 分钟缓冲，通常首轮即就绪）
const SHARD_WAIT_MS = 45000;
const SHARD_POLL_MS = 5000;

// ---------------- 班次键 ----------------
// 分片/运行记录的 KV 键都带 window，早晚班互不覆盖，旧键靠 TTL 自动清扫。
const shardKey = (window, n) => `guji:v4:shard:${window}:${n}`;
const runKey = (window, stage) => `guji:v4:run:${window}:${stage}`;

// ---------------- 增强（薄摘要补全文）----------------
// v4 起预算充裕（digest 班 18/50），从 v3 的 2 条提高到 4 条：
//   4 分片读 4 + latest 读 1 + 增强 4×2 = 8 + 模型链 4 + 落库 2 = 19/50
// ⚠️ 若下调 AI 链或分片数，这里可以再放；改大前先过 §6 budget 的账。
const ENRICH_MAX = 4;
const HTML_MAX = 150000; // 转 Markdown 前的 HTML 字符上限（控内存/CPU）

// ---------------- 时间窗口（秒）----------------
const MAX_AGE = { '72h': 72 * 3600, '7d': 7 * 86400, '14d': 14 * 86400, '30d': 30 * 86400, '120d': 120 * 86400 };

// ---------------- AI 关键词过滤 ----------------
// 用于 filter:1 的源：只保留标题命中 AI 关键词的条目，否则官方博客会被人事/营销稿刷屏
const AI_KW = /\b(ai|a\.i\.|artificial intelligence|llm|large language model|gpt|chatgpt|claude|gemini|copilot|agent|agentic|machine learning|deep learning|neural|transformer|diffusion|inference|foundation model|gpu|tpu|npu|hbm|accelerator|compute|datacenter|data cent(er|re)|chip|chips|semiconductor|silicon|nvidia|amd|intel|tsmc|foundry|hpc|blackwell|hopper|rubin|robot|humanoid|autonomous|self-driving|lidar|openai|anthropic|deepmind|mistral|grok|llama|qwen|deepseek|sora|multimodal|rag|embedding|fine-tun|distillation|quantiz)\b/i;
function isAI(it) { return AI_KW.test(it.title); }

// ---------------- 源清单 ----------------
// kind: rss | edgar | html
// cat:  芯片算力 | 数据中心 | 模型 | 应用产品 | 资本 | 论文 | 访谈观点
// ⚠️ 源数量只影响分片数与抓取班墙钟时间，不再影响单次调用子请求预算（队列化红利）。
const FEEDS = [
  // ---- 模型 / 实验室官方 ----
  { id:'openai',    name:'OpenAI',           kind:'rss', cat:'模型', win:'7d',  url:'https://openai.com/news/rss.xml' },
  { id:'deepmind',  name:'Google DeepMind',  kind:'rss', cat:'模型', win:'7d',  filter:1, url:'https://deepmind.google/blog/rss.xml' },
  { id:'googleai',  name:'Google AI',        kind:'rss', cat:'模型', win:'7d',  url:'https://blog.google/technology/ai/rss/' },
  { id:'meta',      name:'Meta AI',          kind:'rss', cat:'模型', win:'7d',  filter:1, url:'https://about.fb.com/feed/' },
  { id:'hf',        name:'Hugging Face',     kind:'rss', cat:'模型', win:'7d',  url:'https://huggingface.co/blog/feed.xml' },
  { id:'anthropic', name:'Anthropic',        kind:'html',cat:'模型', win:'7d',  url:'https://www.anthropic.com/news' },

  // ---- 芯片 / 算力 / 数据中心：官方 + 原创工程媒体 ----
  { id:'nvdanews',  name:'NVIDIA Newsroom',  kind:'rss', cat:'芯片算力', win:'7d',  filter:1, url:'https://nvidianews.nvidia.com/releases.xml' },
  { id:'nvdablog',  name:'NVIDIA 博客',       kind:'rss', cat:'芯片算力', win:'7d',  filter:1, url:'https://blogs.nvidia.com/feed/' },
  { id:'arm',       name:'Arm Newsroom',     kind:'rss', cat:'芯片算力', win:'7d',  url:'https://newsroom.arm.com/rss' },
  { id:'tomshw',    name:"Tom's Hardware",   kind:'rss', cat:'芯片算力', win:'72h', filter:1, url:'https://www.tomshardware.com/rss.xml' },
  { id:'sth',       name:'ServeTheHome',     kind:'rss', cat:'数据中心', win:'72h', filter:1, url:'https://www.servethehome.com/feed/' },

  // ---- SEC EDGAR 官方 filing（IR 页被 Cloudflare 挡，这里永远畅通）----
  { id:'e-mrvl', name:'Marvell',   kind:'edgar', cat:'芯片算力', cik:'0001835632', type:'8-K' },
  { id:'e-avgo', name:'Broadcom',  kind:'edgar', cat:'芯片算力', cik:'0001730168', type:'8-K' },
  { id:'e-dell', name:'Dell',      kind:'edgar', cat:'数据中心', cik:'0001571996', type:'8-K' },
  { id:'e-smci', name:'Supermicro',kind:'edgar', cat:'数据中心', cik:'0001375365', type:'8-K' },
  { id:'e-anet', name:'Arista',    kind:'edgar', cat:'数据中心', cik:'0001596532', type:'8-K' },
  { id:'e-tsm',  name:'TSMC',      kind:'edgar', cat:'芯片算力', cik:'0001046179', type:'6-K' },
  { id:'e-intc', name:'Intel',     kind:'edgar', cat:'芯片算力', cik:'0000050863', type:'8-K' },
  { id:'e-nvda', name:'NVIDIA·SEC',kind:'edgar', cat:'芯片算力', cik:'0001045810', type:'8-K' },
  { id:'e-amd',  name:'AMD·SEC',   kind:'edgar', cat:'芯片算力', cik:'000002488',  type:'8-K' },
  { id:'e-arm',  name:'Arm·SEC',   kind:'edgar', cat:'芯片算力', cik:'0001973239', type:'6-K' },
  { id:'e-googl',name:'Alphabet·SEC',kind:'edgar',cat:'资本',    cik:'001652044', type:'8-K' },
  { id:'e-msft', name:'Microsoft·SEC',kind:'edgar',cat:'模型',   cik:'0000789019', type:'8-K' },

  // ---- 深度访谈 / 观点（月更-周更，窗口 30 天）----
  { id:'p-acquired', name:'Acquired',           kind:'rss', cat:'访谈观点', win:'30d', url:'https://feeds.transistor.fm/acquired' },
  { id:'p-dwarkesh', name:'Dwarkesh Podcast',   kind:'rss', cat:'访谈观点', win:'30d', url:'https://apple.dwarkesh-podcast.workers.dev/feed.rss' },
  { id:'p-lex',      name:'Lex Fridman',        kind:'rss', cat:'访谈观点', win:'30d', url:'https://lexfridman.com/feed/podcast/' },
  { id:'p-nopriors', name:'No Priors',          kind:'rss', cat:'访谈观点', win:'30d', url:'https://feeds.megaphone.fm/nopriors' },
  { id:'p-allin',    name:'All-In',             kind:'rss', cat:'访谈观点', win:'30d', url:'https://rss.libsyn.com/shows/254861/destinations/1928300.xml' },
  { id:'p-bg2',      name:'BG2',                kind:'rss', cat:'访谈观点', win:'30d', url:'https://anchor.fm/s/s06c2370/podcast/rss' },
  { id:'p-latent',   name:'Latent Space',       kind:'rss', cat:'访谈观点', win:'30d', url:'https://podcast.latent.space/feed.xml' },
  { id:'p-cogrev',   name:'Cognitive Revolution',kind:'rss',cat:'访谈观点', win:'30d', url:'https://feeds.megaphone.fm/RINTP3108857801' },
  { id:'p-iltb',     name:'Invest Like the Best',kind:'rss',cat:'访谈观点', win:'30d', url:'https://feeds.megaphone.fm/CLS2859450455' },
  { id:'p-20vc',     name:'20VC',               kind:'rss', cat:'访谈观点', win:'30d', url:'https://rss.libsyn.com/shows/61840/destinations/240976.xml' },
  { id:'p-a16z',     name:'AI + a16z',          kind:'rss', cat:'访谈观点', win:'30d', url:'https://feeds.simplecast.com/Hb_IuXOo' },
  { id:'p-training', name:'Sequoia Training Data',kind:'rss',cat:'访谈观点', win:'30d', url:'https://feeds.megaphone.fm/trainingdata' },
  { id:'p-eye',      name:'Eye on AI',          kind:'rss', cat:'访谈观点', win:'30d', url:'https://rss.libsyn.com/shows/123267/destinations/727317.xml' },
  { id:'p-stratechery', name:'Stratechery',     kind:'rss', cat:'访谈观点', win:'30d', url:'https://stratechery.com/feed/' },

  // ---- 投资圈：官方 13F ----
  { id:'e-brk', name:'Berkshire 13F', kind:'edgar', cat:'资本', win:'120d', cik:'0001067983', type:'13F-HR' },

  // ---- 论文 ----
  { id:'arxiv', name:'arXiv cs.CL', kind:'rss', cat:'论文', win:'72h', url:'https://rss.arxiv.org/rss/cs.CL' },
];

// 抓取分片总数：从 FEEDS 推导，勿手改（39 源 → 4 片）
const SHARD_COUNT = Math.ceil(FEEDS.length / SHARD_SIZE);

// ---------------- 抓取参数 ----------------
const UA_CHROME = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36';
export const UA_SEC = 'GUJI AI Digest guji-digest@example.com'; // SEC EDGAR 要求 UA 含联系邮箱
const FETCH_MS = 12000; // 单源超时（参考实现为 15s，这里略收紧）
const BATCH_MS = 15000; // 整批兜底超时（防 Node undici 下 abort 不 reject 导致挂起）
const CONCURRENCY = 4;  // 小批次并发，兼顾生产速度与稳定

// ---------------- AI 模型 ----------------
const AI_MODELS = ['@cf/qwen/qwen3-30b-a3b-fp8', '@cf/meta/llama-3.1-8b-instruct-fp8']; // 主力 + 备选
const AI_MS = 150000; // 首次调用 GPU 冷启动可能 90s+，放宽容忍；队列调用无 15 分钟墙钟限制但仍保守

// ---------------- cron 时间表（UTC，须与 wrangler.toml 一致）----------------
//   40,50 21 * * * → 北京 05:40 抓取入队 / 05:50 精选入队（早班）
//   0    22 * * * → 北京 06:00 推送入队（早报）
//   40,50 13 * * * → 北京 21:40 抓取入队 / 21:50 精选入队（晚班）
//   0    14 * * * → 北京 22:00 推送入队（晚报）
// scheduled  handler 按北京时间的分钟数分流：40 抓取 / 50 精选 / 0 推送。
const FETCH_MINUTE = 40;
const DIGEST_MINUTE = 50;
const SEND_MINUTE = 0;

// ---------------- 渲染 ----------------
const CAT_ORDER = [
  ['芯片算力', '🔩'], ['数据中心', '🏭'], ['模型', '🧠'],
  ['应用产品', '🚀'], ['资本', '💰'], ['访谈观点', '🎙️'], ['论文', '📚']
];

// ============================================================
// §2 text · 纯文本工具（无副作用，可单测）
// ============================================================

const esc = (s) => String(s || '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

const stripTags = (s) => String(s || '')
  .replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g,' ').replace(/&amp;/g,'&')
  .replace(/&#0*39;|&apos;/g, "'").replace(/&quot;/g,'"').replace(/&lt;/g,'<').replace(/&gt;/g,'>')
  .replace(/&#x([0-9a-f]+);/gi, (_, h) => { try { return String.fromCodePoint(parseInt(h, 16)); } catch (e) { return _; } })
  .replace(/&#(\d+);/g, (_, d) => { try { return String.fromCodePoint(parseInt(d, 10)); } catch (e) { return _; } })
  .replace(/\s+/g,' ').trim();

// 同题多源去重的键：小写化 → 只保留字母/数字/汉字 → 截断 50
// （去掉标点与空格，让 "GPT-6 发布" 与 "GPT6发布" 归一）
const normTitle = (s) => String(s||'').toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g,'').slice(0,50);

// markdown 轻量清理：去标题符号/链接语法/强调符，留纯文本给模型
const cleanMd = (s) => String(s || '')
  .replace(/^#{1,6}\s*/gm, '')
  .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
  .replace(/[*_`>|]/g, ' ')
  .replace(/\s+/g, ' ').trim();

/**
 * 按 UTF-8 字节长度截断（Telegram 单条消息上限 4096 字节）。
 *
 * ⚠️ 历史坑：不能用 String.slice —— 它按 UTF-16 计数，
 *    中文 1 字 = 2 单位 = 3 字节，slice(0,3800) 的纯中文实际可达 5700 字节，
 *    会被 Telegram 整条拒绝。必须走 TextEncoder。
 *
 * 注意既有语义：入参非字符串时原样返回（不会变成空串）。
 */
export function tgTruncate(s, maxBytes = 3600) {
  const enc = new TextEncoder();
  const bytes = enc.encode(s);
  if (bytes.length <= maxBytes) return s;
  const cut = bytes.slice(0, maxBytes);
  return new TextDecoder().decode(cut).replace(/\uFFFD+$/, '') + '\n…（已截断）';
}

// ============================================================
// §3 time · 时间工具
// ============================================================
//
// ⚠️ 单位约定（历史混用坑，务必遵守）：
//   - 条目时间戳 it.ts          → 毫秒（来自 Date.parse / Date.now）
//   - lastDigestAt / nowSec     → 秒
//   - MAX_AGE / expirationTtl   → 秒
// 跨单位比较一律走 toMs / toSec，禁止裸写 *1000 或 /1000。

/** 北京时间的 UTC 偏移（UTC+8），毫秒 */
const BJ_OFFSET_MS = 8 * 3600 * 1000;

const pad = (n) => String(n).padStart(2, '0');

/** 秒 → 毫秒 */
const toMs = (sec) => Math.floor(Number(sec) || 0) * 1000;

/** 毫秒 → 秒 */
const toSec = (ms) => Math.floor((Number(ms) || 0) / 1000);

/** 当前时间的秒级时间戳 */
const nowSec = () => Math.floor(Date.now() / 1000);

/** 毫秒时间戳 → 北京时间 MM-DD HH:mm（UTC+8） */
export function bjTime(ts) {
  const d = new Date((ts || Date.now()) + BJ_OFFSET_MS);
  return `${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

/** 毫秒时间戳 → 北京时间的 Date 对象（供需要取 UTC 字段做进一步判断的场合） */
const bjDate = (ts) => new Date((ts || Date.now()) + BJ_OFFSET_MS);

/** 条目（毫秒）是否比 lastAtSec（秒）更新 */
const isFresh = (tsMs, lastAtSec) => (Number(tsMs) || 0) > toMs(lastAtSec);

/**
 * 班次标识：如 2026-10-25-am / 2026-10-25-pm。
 * 抓取分片、运行记录、幂等守卫全部按 window 隔离，早晚班互不覆盖。
 * 入参传 cron 的 event.scheduledTime（名义触发时刻，而非 Date.now()），保证确定性。
 */
export function currentWindow(tsMs = Date.now()) {
  const d = bjDate(tsMs);
  const edition = d.getUTCHours() < 12 ? 'am' : 'pm';
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}-${edition}`;
}

// ============================================================
// §4 xml · 解析基础设施
// ============================================================

// RSS/Atom/RDF 统一解析器（fast-xml-parser v5：原生处理 CDATA、实体、属性）
// 模块级单例：无内部状态，可安全复用（避免每条 feed 重复构造）
// parseTagValue:false——v4.0.1 修复（外部审查 A3）：默认开启会把纯数字文本转 number
//   （"0012"→12），EDGAR CIK/编号类字段有中招风险。不要加 htmlEntities:true——
//   stripTags 已做实体解码，双重解码会把 &amp;lt; 过度解成 <。
const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  textNodeName: '#text',
  isArray: (name) => ['item', 'entry', 'category'].includes(name),
  processEntities: true,
  parseTagValue: false,
  trimValues: true,
});

const pickText = (node) => {
  if (node == null) return '';
  if (typeof node === 'string') return node.trim();
  if (typeof node === 'object' && node['#text'] != null) return String(node['#text']).trim();
  return '';
};

const pickLink = (node) => {
  if (node == null) return '';
  if (typeof node === 'string') return node.trim();
  if (Array.isArray(node)) {
    const alt = node.find(l => l && typeof l === 'object' && l['@_rel'] === 'alternate')
      || node.find(l => l && typeof l === 'object' && l['@_href']);
    return alt ? String(alt['@_href'] || '').trim() : '';
  }
  if (typeof node === 'object' && node['@_href']) return String(node['@_href']).trim();
  if (typeof node === 'object' && node['#text']) return String(node['#text']).trim();
  return '';
};

const pickDate = (...nodes) => {
  for (const n of nodes) {
    const s = pickText(n);
    if (s) { const t = Date.parse(s); if (!isNaN(t)) return t; }
  }
  return 0;
};

// ============================================================
// §5 parsers · 源数据解析（纯函数）
// ============================================================

// RSS / Atom / RDF 通用解析（fast-xml-parser，自动处理 CDATA 与实体）
export function parseXmlFeed(xml, source) {
  let items = [];
  try {
    const doc = xmlParser.parse(xml);
    if (doc.rss?.channel?.item) items = doc.rss.channel.item;
    else if (doc.feed?.entry) items = doc.feed.entry;
    else if (doc['rdf:RDF']?.item) items = doc['rdf:RDF'].item;
    else if (doc.channel?.item) items = doc.channel.item;
    else if (doc.item) items = doc.item;
  } catch (e) { return []; }
  const out = [];
  for (const it of (Array.isArray(items) ? items : []).slice(0, 40)) {
    if (!it || typeof it !== 'object') continue;
    const title = stripTags(pickText(it.title));
    if (!title) continue;
    const link = pickLink(it.link) || pickText(it.id) || pickText(it.guid) || source.url;
    const ts = pickDate(it.pubDate, it.published, it.updated, it['dc:date'], it.date);
    const desc = stripTags(pickText(it.description) || pickText(it.summary) || pickText(it['content:encoded']) || pickText(it.content));
    out.push({
      title: title.slice(0, 300),
      link: String(link).slice(0, 500),
      ts,
      desc: desc.slice(0, 600),
      source: source.name, srcId: source.id, cat: source.cat, kind: source.kind,
      win: source.win, filter: source.filter
    });
  }
  return out;
}

// SEC EDGAR atom：从 summary 提取 "Item X.XX: 主题"
export function parseEdgar(xml, source) {
  let entries = [];
  try {
    const doc = xmlParser.parse(xml);
    entries = doc.feed?.entry || [];
  } catch (e) { return []; }
  const out = [];
  for (const e of (Array.isArray(entries) ? entries : []).slice(0, 8)) {
    if (!e || typeof e !== 'object') continue;
    const link = pickLink(e.link);
    const ts = pickDate(e.updated, e.published);
    const summary = pickText(e.summary) || pickText(e.content);
    const items = [...summary.matchAll(/Item\s+[\d.]+:\s*([^<]+)/g)].map(m => m[1].trim()).filter(Boolean);
    const itemText = items.slice(0, 2).join(' · ');
    const ftype = source.type === '13F-HR' ? '13F 持仓' : source.type + ' 事件';
    const title = itemText ? `${source.name} · ${ftype}：${itemText}` : `${source.name} · ${ftype}`;
    out.push({
      title: title.slice(0, 300),
      link: String(link).slice(0, 500),
      ts,
      desc: stripTags(summary).slice(0, 300),
      source: source.name, srcId: source.id, cat: source.cat, kind: source.kind,
      win: source.win, filter: source.filter
    });
  }
  return out;
}

// ============================================================
// §6 budget · 子请求预算（记账不改流）
// ============================================================
//
// Cloudflare 免费套餐：单次 Worker 调用最多 50 个子请求（fetch / KV / AI /
// 队列发送都算）。⚠️ 第 51 个不抛异常、只在响应里报错——静默失败，
// 这是本项目历史上最难排查的事故模式（digest=0 的根因之一）。
//
// v4 队列化后的预算模型：每个消费者调用独立计数，峰值远低于上限。
//
// 设计原则：
//   1. 计数，但不限流。网关负责记账与告警，**绝不**因预算耗尽而跳过必要的 KV.put。
//      静默丢数据比超限报错危险得多。
//   2. 每次调用都留痕（seq / kind / label / total），明细随运行记录写进 KV，
//      供 /api/diag 在线排查。
//   3. 静态预算表 BUDGET_PLAN 是"应然"，运行时计数是"实然"，
//      二者按阶段一一对应——任何新增子请求都会立刻在账上看出来。

/** 免费套餐单次调用子请求硬上限 */
export const SUBREQUEST_LIMIT = 50;

/** 告警线（免费套餐无 [limits] 可调，只能提前预警） */
export const SUBREQUEST_WARN = 45;

/**
 * 静态预算表：v4 各阶段（每次调用独立预算）。
 * 改动这里的数字前，先确认 wrangler.toml 的 crons 与 FEEDS 数量是否同步。
 */
export const BUDGET_PLAN = [
  { stage: 'cron.enqueue', label: '入队 4 条抓取分片消息（队列发送保守计入）', quota: 4 },
  { stage: 'fetch.shard', label: '单分片：10 源 fetch + Anthropic seen 2 + 诊断埋点 2 + 分片落库 1', quota: 16 },
  { stage: 'digest', label: '读 4 分片 + latest 1 + 增强 4×2 + 模型链 4 + 落库 2（等分片轮询另有 40 预算护栏）', quota: 19 },
  { stage: 'send.slice', label: '单推送片：latest 1 + ≤9 条消息×至多 3 次尝试 + 统计 1', quota: 29 },
];

/** 静态预算峰值（单次调用最大消耗），单测断言它 ≤ SUBREQUEST_LIMIT */
export const PLAN_PEAK = BUDGET_PLAN.reduce((max, p) => Math.max(max, p.quota), 0);

/**
 * 创建一个预算计数器。每个消费者调用各一个实例。
 * @param {object} [opts]
 * @param {number} [opts.limit]   硬上限
 * @param {number} [opts.warnAt]  告警线
 * @param {boolean} [opts.silent] 静音（测试用）
 */
export function createBudget({ limit = SUBREQUEST_LIMIT, warnAt = SUBREQUEST_WARN, silent = false } = {}) {
  /** @type {{seq:number, kind:string, label:string, n:number, total:number}[]} */
  const entries = [];
  let total = 0;
  let seq = 0;
  let breached = false;

  return {
    /** 已消耗的子请求总数 */
    get total() { return total; },
    /** 硬上限 */
    get limit() { return limit; },
    /** 是否已越过硬上限 */
    get breached() { return breached; },

    /**
     * 记一次子请求消耗。
     * @param {number} n      消耗次数（一般为 1）
     * @param {'fetch'|'kv'|'ai'|'queue'} kind
     * @param {string} [label] 可读标签，用于日志与断言
     */
    spend(n = 1, kind = 'fetch', label = '') {
      total += n;
      seq += 1;
      const entry = { seq, kind, label, n, total };
      entries.push(entry);
      if (!silent && total >= warnAt) {
        console.error(`[budget] ${total}/${limit} ⚠️ ${kind}:${label}`);
      }
      if (total > limit && !breached) {
        breached = true;
        console.error(`[budget] ❌ 超出预算 ${total}/${limit} @ ${kind}:${label}`);
      }
      return entry;
    },

    /** 按类型汇总 */
    byKind() {
      const acc = { fetch: 0, kv: 0, ai: 0, queue: 0 };
      for (const e of entries) acc[e.kind] = (acc[e.kind] || 0) + e.n;
      return acc;
    },

    /** 完整明细，供 /api/diag 与单测断言 */
    report() {
      return {
        total,
        limit,
        warnAt,
        breached,
        byKind: this.byKind(),
        entries: entries.slice(),
      };
    },
  };
}

// ============================================================
// §7 net · 子请求网关（★ 所有 fetch / KV / AI / 队列发送的唯一出口）
// ============================================================
//
// 存在的意义：让「这次调用花了多少子请求」变成可数、可断言、可日志的事实，
// 而不是散落在注释里的数学题。
//
// 铁律：
//   - 除本区外，本文件其他区不得出现裸 fetch(、env.GUJI_KV.、env.AI.、
//     env.GUJI_*_Q.send( 调用
//   - 网关只记账，不拦截。预算超了照常执行并告警，绝不静默跳过写操作。

/**
 * 创建绑定在当前 env 与预算计数器上的网络出口。
 * @param {object} env    Worker 环境变量（含 GUJI_KV / AI / 队列绑定 / JINA_KEY / ZHIPU_KEY）
 * @param {object} budget createBudget() 返回的计数器
 */
export function createNet(env, budget) {
  const kv = env && env.GUJI_KV ? env.GUJI_KV : null;
  const ai = env && env.AI ? env.AI : null;

  return {
    /**
     * 发起一次 HTTP 请求（计 1 个子请求）。
     * 签名与全局 fetch 一致，返回原始 Response，由调用方决定如何消费。
     */
    async fetch(url, init = {}, label = '') {
      budget.spend(1, 'fetch', label || url);
      return fetch(url, init);
    },

    /** KV 读（计 1 个子请求）。未绑定 KV 时返回 null 且**不计数**——与基线行为一致。 */
    async kvGet(key, type = 'text', label = '') {
      if (!kv) return null;
      budget.spend(1, 'kv', label || `get:${key}`);
      return kv.get(key, type);
    },

    /** KV 写（计 1 个子请求）。未绑定 KV 时静默跳过且不计数。 */
    async kvPut(key, value, opts = {}, label = '') {
      if (!kv) return undefined;
      budget.spend(1, 'kv', label || `put:${key}`);
      return kv.put(key, value, opts);
    },

    /** Workers AI 推理（计 1 个子请求） */
    async aiRun(model, payload, label = '') {
      budget.spend(1, 'ai', label || model);
      if (!ai) throw new Error('AI 绑定缺失');
      return ai.run(model, payload);
    },

    /** Workers AI toMarkdown（计 1 个子请求）。不支持时返回 null 且**不计数**。 */
    async aiToMarkdown(files, label = '') {
      if (!ai || typeof ai.toMarkdown !== 'function') return null;
      budget.spend(1, 'ai', label || 'toMarkdown');
      return ai.toMarkdown(files);
    },

    /**
     * 队列发送（保守计 1 个子请求；Cloudflare 未明确队列发送是否计入
     * 50 上限，按计入估——cron 入队班最多 4 条，两种口径下都安全）。
     */
    async qSend(binding, body, label = '') {
      if (!binding) throw new Error('队列绑定缺失: ' + (label || '?'));
      budget.spend(1, 'queue', label || 'queue:send');
      return binding.send(body);
    },

    /** 是否已绑定 KV（供调用方做短路判断，避免无谓的分支） */
    get hasKv() { return !!kv; },

    /** 原始 env 只读引用：仅供读取特性开关（如 ZHIPU_KEY / JINA_KEY / TG_*），
     *  ⚠️ 不要绕过网关直接调用 env.GUJI_KV / env.AI / 队列 binding —— 那样会漏记预算。 */
    get env() { return env; },
  };
}

// ============================================================
// §8 sources · 单源抓取
// ============================================================
// 一个源固定 1 次 fetch，失败静默记入 failures，绝不让单个源拖垮整体。
// 参照 cloudflare-rss-aggregator 的 fetcher：AbortController 超时 + try/catch 兜底。

/**
 * 抓取单个源。
 * @param {object} f      FEEDS 中的源定义
 * @param {object} net    createNet() 出口
 * @param {Array}  failures 失败收集器（就地 push）
 * @returns {Promise<object[]>} 解析后的条目（失败时为空数组）
 */
async function fetchFeedOne(f, net, failures) {
  try {
    if (f.kind === 'html') return await fetchAnthropic(f, net);
    const url = f.kind === 'edgar'
      ? `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${f.cik}&type=${f.type}&dateb=&owner=include&count=10&output=atom`
      : f.url;
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), FETCH_MS);
    let xml;
    try {
      const r = await net.fetch(url, {
        signal: ac.signal,
        redirect: 'follow',
        headers: {
          'User-Agent': f.kind === 'edgar' ? UA_SEC : UA_CHROME,
          'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*',
        },
      }, `source:${f.id}`);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      xml = await r.text();
    } finally {
      clearTimeout(timer);
    }
    const parsed = f.kind === 'edgar' ? parseEdgar(xml, f) : parseXmlFeed(xml, f);
    console.error(`✓ ${f.id}: ${xml.length}B → ${parsed.length} 条`);
    return parsed;
  } catch (e) {
    const err = String(e && e.message || e).slice(0, 80);
    failures.push({ name: f.name, err });
    console.error(`✗ ${f.id}: ${err}`);
    return [];
  }
}

/**
 * Anthropic 官网 HTML 兜底（无 RSS）：抓 FeaturedGrid 文章链接 + 标题。
 * ⚠️ 子请求账：1 fetch + 1 KV get（无条件，只要有 KV 绑定）+ 1 KV put（仅当有新 slug）
 *    旧实现是每篇文章各 1 次 get + 1 次 put（最多 24 次），免费套餐装不下，故合并为单次读写。
 *    分片化后只有含 anthropic 的那一片会碰 seen 集合，天然无并发写竞争。
 */
export async function fetchAnthropic(source, net) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), FETCH_MS);
  let html;
  try {
    const r = await net.fetch(source.url, {
      signal: ac.signal,
      redirect: 'follow',
      headers: { 'User-Agent': UA_CHROME, 'Accept': 'text/html,application/xhtml+xml,*/*' },
    }, `source:${source.id}`);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    html = await r.text();
  } finally {
    clearTimeout(timer);
  }
  const out = [];
  const seen = new Set();
  const nowSecValue = nowSec();
  // 一次性读取跨调用去重集合（见上方预算说明）
  const seenMap = (await net.kvGet(SEEN_SET_KEY, 'json', 'seen:get').catch(() => null)) || {};
  const newSeen = {};
  // 栏目页黑名单（TopicLink 类）
  const NAV = new Set(['announcements','engineering','research','policy','news','company','public-benefit']);
  const re = /<a[^>]*class="[^"]*FeaturedGrid-module[^"]*tileLink[^"]*"[^>]*href="\/news\/([a-z0-9-]+)"[^>]*>([\s\S]*?)<\/a>/g;
  let m;
  while ((m = re.exec(html)) && out.length < 12) {
    const slug = m[1];
    if (NAV.has(slug) || seen.has(slug)) continue;
    const title = stripTags(m[2]);
    if (!title || title.length < 8) continue;
    seen.add(slug);
    if (seenMap[slug]) continue; // 之前已推送过，跳过
    newSeen[slug] = nowSecValue;
    out.push({
      title: `Anthropic：${title}`.slice(0, 300),
      link: 'https://www.anthropic.com/news/' + slug,
      ts: Date.now(), // 无日期，按“新出现”算
      desc: '', source: source.name, srcId: source.id, cat: source.cat, kind: source.kind,
      win: source.win, filter: source.filter
    });
  }
  // 一次性写回去重集合，并清理 60 天前的记录（避免无限膨胀）
  if (Object.keys(newSeen).length) {
    const merged = Object.assign({}, seenMap, newSeen);
    for (const k of Object.keys(merged)) if (nowSecValue - merged[k] > 60 * 86400) delete merged[k];
    await net.kvPut(SEEN_SET_KEY, JSON.stringify(merged), { expirationTtl: 90 * 86400 }, 'seen:put').catch(() => {});
  }
  return out;
}

// ============================================================
// §9 shard · 分片抓取与合并后处理
// ============================================================
// v4 起抓取按分片进行：cron 入队 SHARD_COUNT 条消息，消费者每次处理一片
// （10 源，约 13/50 子请求）。本区包含两个纯函数/准纯函数：
//   fetchShardFeeds —— 一片源的抓取（并发 + 兜底超时）
//   finalizeItems   —— 跨分片合并后的后处理（过滤/限额/去重/排序），可单测

// ---------------- 分类微调 ----------------
// 模型类里提到数据中心/能源/代工的，归到数据中心（避免「芯片算力」吃掉一切）
// （v4.0.1：删掉 semicaid——semiconductor 的错别字死分支，永远不命中；如未来要收再显式加）
const DC_KW = /datacenter|data center|data-centre|energy|power grid|foundry|gigawatt|nuclear|reactor|grid|utility|utilities/i;
function recat(item) {
  if (item.cat === '模型' && DC_KW.test(item.title)) item.cat = '数据中心';
  return item;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/** 每源限额：防止单一高产量媒体刷屏 */
const SRC_LIMIT = { tomshw: 8, sth: 8 };

/**
 * 抓取一个分片内的全部源（小批次并发 + 每批兜底超时 + 逐源 try/catch）。
 * @param {object[]} feeds   本分片的源定义（≤ SHARD_SIZE 个）
 * @param {object} net       createNet() 出口
 * @param {Array}  failures  失败收集器（就地 push）
 * @returns {Promise<object[]>} 解析后的条目（拼接，未过滤）
 */
export async function fetchShardFeeds(feeds, net, failures) {
  const results = [];
  for (let i = 0; i < feeds.length; i += CONCURRENCY) {
    const batch = feeds.slice(i, i + CONCURRENCY).map(f => fetchFeedOne(f, net, failures));
    // 兜底：某批整体不 settle（Node undici 下偶发）也不能挂死整个调用
    const settled = await Promise.race([
      Promise.all(batch),
      sleep(BATCH_MS).then(() => null),
    ]);
    if (settled) results.push(...settled);
    else failures.push({ name: `第 ${i / CONCURRENCY + 1} 批`, err: '批次超时' });
  }
  return results.flat();
}

/**
 * 跨分片合并后的后处理（纯函数）：时间窗过滤 → 无日期放行 → AI 关键词 →
 * 每源限额 → arXiv 截断 → 同题多源去重 → 按时间倒序。
 * ⚠️ 语义与 v3 collect() 的后半段完全一致（单测可直接比对）。
 * @param {object[]} all 全部分片拼接的原始条目
 * @returns {object[]} 可进 AI 精选的条目池
 */
export function finalizeItems(all) {
  const nowSecValue = nowSec();
  const kept = [];
  for (const it of all) {
    const win = it.win || (it.kind === 'edgar' ? '14d' : '72h');
    const maxAge = MAX_AGE[win] || MAX_AGE['72h'];
    // ① 超出时间窗口的丢弃（ts 为 0 表示无日期，不在此处判死）
    if (it.ts && nowSecValue - toSec(it.ts) > maxAge) continue;
    // ② 无日期的一律判死（anthropic 源不在此列——fetchAnthropic 已设 ts=抓取时刻，
    //    「无日期只有 anthropic 放行」的特例是 v3 ts:0 方案的遗留，v4.0.1 删除）
    if (!it.ts) continue;
    // ③ filter:1 的源只保留 AI 相关
    if (it.filter && !isAI(it)) continue;
    kept.push(recat(it));
  }

  // 限额只对「其余源」生效；arXiv 单独在下面取最新 8 条
  const srcCount = {};
  const limited = [];
  for (const it of kept) {
    const lim = SRC_LIMIT[it.srcId];
    if (lim) {
      srcCount[it.srcId] = (srcCount[it.srcId] || 0) + 1;
      if (srcCount[it.srcId] > lim) continue;
    }
    limited.push(it);
  }

  const nonArxiv = limited.filter(i => i.srcId !== 'arxiv');
  const arxiv = kept.filter(i => i.srcId === 'arxiv').slice(0, 8);
  // 去重合并（同题多源）
  const byKey = new Map();
  for (const it of nonArxiv.concat(arxiv)) {
    const k = normTitle(it.title);
    if (!k) continue;
    if (byKey.has(k)) {
      const ex = byKey.get(k);
      if (!ex.also.includes(it.source)) ex.also.push(it.source);
    } else {
      it.also = [];
      byKey.set(k, it);
    }
  }
  return [...byKey.values()].sort((a, b) => (b.ts || 0) - (a.ts || 0));
}

// ============================================================
// §10 digest · 每日精选 · 中文总结
// ============================================================
//
// 流程严格按「先整理精选，再总结翻译」：
//   第一步，模型通读今日全部条目，挑出真正重要的（跳过营销/人事/无信息量）；
//   第二步，只对精选结果写中文标题 + 1-2 句中文摘要（基于原文，不编造）。
// 绝不做「一开始就把所有英文条目全文翻译」——那只会产出噪音。

/** 素材上限：先按新鲜度截断，模型再精选，双控输出长度 */
const MATERIAL_MAX = 24;

/** 精选条数硬上限（与提示词「不超过 12 条」对齐） */
export const DIGEST_MAX = 12;

// ---------------- AI 调用 ----------------

async function runAI(net, model, payload) {
  let timer;
  const timeout = new Promise((_, rej) => { timer = setTimeout(() => rej(new Error('AI 超时')), AI_MS); });
  try {
    return await Promise.race([net.aiRun(model, payload, model), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

// 兼容三种响应形态：string / [{content}] / {content}
export function aiText(res) {
  const r = res && (res.response !== undefined ? res.response : res.result);
  if (typeof r === 'string') return r;
  if (Array.isArray(r)) {
    return r.map(x => (x && typeof x === 'object' ? String(x.content ?? x.text ?? '') : String(x))).join('');
  }
  if (r && typeof r === 'object') return String(r.content ?? r.text ?? '');
  return '';
}

// ---------------- 容错解析 ----------------

/**
 * 解析模型输出。容错链：整体 JSON → 截取数组段 → 逐对象提取
 * （抗截断 / ```json 包裹 / 前言废话）。
 * @param {string} text     模型原始输出
 * @param {object[]} material 素材（idx 从 1 开始对应 material[0]）
 */
export function parseDigest(text, material) {
  let arr = null;
  const s = String(text || '').replace(/```(?:json)?/gi, '');
  try { arr = JSON.parse(s); } catch (e) {
    const m = s.match(/\[[\s\S]*\]/);
    if (m) { try { arr = JSON.parse(m[0]); } catch (e2) { arr = null; } }
  }
  if (!Array.isArray(arr)) {
    arr = [];
    const re = /\{[^{}]*\}/g;
    let m;
    while ((m = re.exec(s))) {
      try { arr.push(JSON.parse(m[0])); } catch (e3) { /* 跳过这个残段 */ }
    }
  }
  const out = [];
  const seen = new Set();
  for (const d of arr) {
    const idx = Number(d && d.idx);
    if (!Number.isInteger(idx) || idx < 1 || idx > material.length || seen.has(idx)) continue;
    const title = String(d.title || '').trim();
    const summary = String(d.summary || '').trim();
    if (!title || !summary) continue;
    seen.add(idx);
    const it = material[idx - 1];
    out.push({
      cat: it.cat, title: title.slice(0, 120), summary: summary.slice(0, 200),
      source: it.source, link: it.link, ts: it.ts
    });
    if (out.length >= DIGEST_MAX) break; // 模型不守约时硬截断
  }
  return out;
}

// ---------------- 薄摘要补全文 ----------------
// 两条通道：
//   1. 有 JINA_KEY → Jina Reader（快，免 GPU）
//   2. 否则 → Cloudflare 原生 Workers AI toMarkdown（复用 [ai] 绑定，零额外配置）
// 只补 desc 短的（EDGAR 条款名、官方短通稿），充分的条目不碰；失败静默回退原摘要。

async function enrichMaterial(material, net) {
  const thin = material.filter(it => (!it.desc || it.desc.length < 120) && /^https?:\/\//.test(it.link || ''));
  if (!thin.length) return material;
  const targets = thin.slice(0, ENRICH_MAX); // ⚠️ 上限 4 条：队列化后 digest 班 19/50，余量充足
  for (const it of targets) { // 串行：峰值内存可控（并行大页面易触发 Worker 资源上限 1102）
    try {
      const md = net.env && net.env.JINA_KEY ? await fetchViaJina(it.link, net) : await fetchViaToMarkdown(it.link, net);
      if (md && md.length > (it.desc || '').length + 40) it.desc = md.slice(0, 600); // 确实更丰富才采用
    } catch (e) { /* 静默：保留原摘要 */ }
  }
  return material;
}

async function fetchViaJina(url, net) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 8000);
  try {
    const r = await net.fetch('https://r.jina.ai/' + url, {
      signal: ac.signal, redirect: 'follow',
      headers: { 'Accept': 'text/plain', 'Authorization': 'Bearer ' + net.env.JINA_KEY }
    }, 'enrich:jina');
    if (!r.ok) return null;
    return cleanMd(await r.text()).slice(0, 600);
  } finally { clearTimeout(timer); }
}

// Cloudflare 原生方案（参照 doocs/anything-md）：抓 HTML → env.AI.toMarkdown → 纯文本
// 无需任何 key，走已有 AI 绑定；含整调用超时兜底（GPU 冷启动可能很慢）
async function fetchViaToMarkdown(url, net) {
  if (!net.env?.AI?.toMarkdown) return null;
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 10000);
  let html;
  try {
    const r = await net.fetch(url, {
      signal: ac.signal, redirect: 'follow',
      headers: { 'User-Agent': UA_CHROME, 'Accept': 'text/html,application/xhtml+xml,*/*' }
    }, 'enrich:page');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const ct = r.headers.get('content-type') || '';
    if (!/text\/html|application\/xhtml/i.test(ct)) throw new Error('CT=' + ct.slice(0, 40));
    html = await r.text();
  } finally { clearTimeout(timer); }
  if (!html || html.length < 200) throw new Error('html ' + (html ? html.length : 0) + 'B');
  if (html.length > HTML_MAX) html = html.slice(0, HTML_MAX); // 截断：防止超大页面吃爆内存
  const call = net.aiToMarkdown([{ name: 'page.html', blob: new Blob([html], { type: 'text/html' }) }], 'enrich:md');
  const timeout = new Promise(res => setTimeout(() => res(null), 90000)); // 冷启动容忍
  const results = await Promise.race([call, timeout]);
  const res = results && results[0];
  if (!res) throw new Error('toMarkdown 超时');
  if (res.format === 'error') throw new Error('toMarkdown: ' + String(res.error).slice(0, 80));
  let md = String(res.data || '');
  md = md.replace(/^---\n[\s\S]*?\n---\n*/, ''); // 去 YAML frontmatter
  return cleanMd(md).slice(0, 600);
}

// ---------------- 提示词 ----------------

/**
 * 构建精选提示词。
 * ⚠️ 历史遗留（DEVLOG 坑 9）：开头的 <arg_key:opensource> 与结尾 /no_think 曾被当作
 *    Qwen3 关思考的「开关」，实测三重全失效（Qwen3 会把 max_tokens 耗在 reasoning 上，
 *    返回空 content，曾导致精选恒为 0 条）。最终解是换 GLM-4-plus（无思考模式）。
 *    两个参数对 GLM 无害故保留，但**不再宣称有效**——v4.0.1 注释修正（外部审查 C3）。
 */
export function buildPrompt(material) {
  const list = material.map((it, i) =>
    `${i + 1}. [${it.cat}] ${it.title}（来源：${it.source}）${it.desc ? ' 摘要：' + it.desc.slice(0, 100) : ''}`
  ).join('\n');
  return `<arg_key:opensource>
你是「GUJI AI 情报日报」的主编。下面是今天从各官方一手来源抓到的条目（编号、分类、标题、来源、摘要片段）。
请分两步工作，不要跳步：
第一步【整理精选】：通读全部条目，挑出真正重要、有实质信息量的（如新模型/新产品/重大财报与订单/重要论文/深度观点），总量严格不超过 ${DIGEST_MAX} 条；跳过营销通稿、人事任命、无信息量的条目。
第二步【总结翻译】：只对精选出的条目，为每条写一个中文标题（25 字以内）和一句中文摘要（50 字以内）。必须基于原文信息概括，不得编造原文没有的内容，数字与名称保留原文。
输出严格的 JSON 数组（不要 markdown 代码块标记，不要任何其他文字），每项格式：
{"idx": 条目编号, "title": "中文标题", "summary": "中文摘要"}

今日条目：
${list}

/no_think`;
}

// 智谱 GLM（OpenAI 兼容 v4 接口）。选 glm-4-plus 的原因：
//   - 中文原生，摘要质量稳定高于 llama-3.1-8b
//   - 无思考模式（glm-4.5/4.6 是思考模型，会重蹈 Qwen「空 content」覆辙）
//   - 1-3 秒级响应，无 GPU 冷启动
// ZHIPU_KEY 不存在时整条链路自动跳过，行为退回纯 Cloudflare 模型。
export async function fetchViaZhipu(prompt, net) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 60000);
  try {
    const r = await net.fetch('https://open.bigmodel.cn/api/paas/v4/chat/completions', {
    method: 'POST', signal: ac.signal,
    headers: { 'Authorization': 'Bearer ' + net.env.ZHIPU_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'glm-4-plus',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 1536, temperature: 0.3
      })
    }, 'zhipu:glm-4-plus');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json().catch(() => null);
    const text = j && j.choices && j.choices[0] && j.choices[0].message
      ? String(j.choices[0].message.content || '') : '';
    if (!text.trim()) throw new Error('空 content');
    return text;
  } finally { clearTimeout(timer); }
}

// ---------------- 模型链 ----------------

/**
 * 构造模型链。声明式配置，attempts 之和直接对应 §6 budget 里的 digest 配额（4）。
 *
 * ⚠️ v3 时代「留给 AI 的只有 3 次」的约束随队列化解除（digest 班 19/50）；
 *    兜底模型 llama 给 2 次尝试，直接针对 05:50 那类瞬时失败。
 *
 * @param {object} net
 * @param {string} prompt
 */
export function buildChain(net, prompt) {
  // ⚠️ 历史遗留（DEVLOG 坑 9）：chat_template_kwargs.enable_thinking=false 曾被当作
  //    Qwen3 关思考的「正开关」，实测无效（Qwen3 空 content 的真正解是换 GLM-4-plus）。
  //    参数对 GLM 无害故保留——v4.0.1 注释修正（外部审查 C3）。
  const cfPayload = {
    messages: [{ role: 'user', content: prompt }], max_tokens: 3072, temperature: 0.3,
    chat_template_kwargs: { enable_thinking: false },
  };
  const hasZhipu = !!(net.env && net.env.ZHIPU_KEY);
  return hasZhipu
    ? [
        { name: 'zhipu:glm-4-plus', attempts: 2, call: () => fetchViaZhipu(prompt, net) },
        { name: AI_MODELS[1], attempts: 2, call: async () => aiText(await runAI(net, AI_MODELS[1], cfPayload)) },
      ]
    : [
        { name: AI_MODELS[0], attempts: 2, call: async () => aiText(await runAI(net, AI_MODELS[0], cfPayload)) },
        { name: AI_MODELS[1], attempts: 2, call: async () => aiText(await runAI(net, AI_MODELS[1], cfPayload)) },
      ];
}

/**
 * 生成每日精选。
 * @param {object[]} items 已排序的条目池
 * @param {object} net     createNet() 出口
 * @returns {Promise<{digest: object[]|null, trace: object[]}>}
 */
export async function makeDigest(items, net) {
  // 注意：不再因「AI 绑定缺失」早退——有 ZHIPU_KEY 时纯走智谱链路，
  // 无 key 也无绑定时由 buildChain 自然失败并记入 trace（队列重试兜底）。
  if (!items.length) return { digest: null, trace: [] };
  const trace = []; // 逐模型诊断：供 /api/diag 排查「精选 0 条」
  const material = items.slice(0, MATERIAL_MAX);
  await enrichMaterial(material, net); // 薄摘要先补全文（Jina key 或原生 toMarkdown），模型看得更深
  const prompt = buildPrompt(material);
  const chain = buildChain(net, prompt);
  for (const m of chain) {
    for (let attempt = 1; attempt <= m.attempts; attempt++) {
      const t0 = Date.now();
      try {
        const text = await m.call();
        const digest = parseDigest(text, material);
        trace.push({ model: m.name, attempt, ms: Date.now() - t0, textLen: text.length, digest: digest.length, head: text.slice(0, 120) });
        if (digest.length) {
          console.error(`digest: ${m.name} → ${digest.length} 条精选`);
          return { digest, trace };
        }
        if (text.trim()) break; // 有输出但解析不出：换模型更有希望
        if (attempt < m.attempts) await new Promise(r => setTimeout(r, 2000));
      } catch (e) {
        trace.push({ model: m.name, attempt, ms: Date.now() - t0, err: String(e && e.message).slice(0, 200) });
        console.error(`digest: ${m.name} 失败：${e && e.message}`);
        if (attempt < m.attempts) await new Promise(r => setTimeout(r, 2000));
      }
    }
  }
  return { digest: null, trace };
}

// ============================================================
// §11 telegram · 推送
// ============================================================
// 顺序分片推送：cron 入队 SEND_SLICES 条消息，消费者 max_concurrency=1 串行
// 执行，片内 300ms 限速；单片 ≤9 条消息 × 至多 3 次尝试 = ≤27 个子请求，
// 即使 Telegram 抽风也不越 50 上限（v3 单 invocation 26 条无重试余量的问题解除）。
// 单条消息天然很短，不会像合并大段文本那样超出 4096 字节被整条拒绝。

const CAT_ICON = new Map(CAT_ORDER);

/** 报头：首条消息用（日期 + 班次 + 条数概览） */
export function renderTgHeader(data) {
  const d = bjDate(data.generatedAt || Date.now());
  const p = n => String(n).padStart(2, '0');
  const edition = d.getUTCHours() < 12 ? '☀️ 早报' : '🌆 晚报'; // 6:00 早班 / 22:00 晚班
  const n = (data.digest || []).length;
  return `🤖 GUJI AI 情报 · ${edition} · ${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}\n今日抓取 ${data.items.length} 条，精选 ${n} 条`;
}

/** 单条精选 = 一条消息：短、可读、各自带链接与来源 */
export function renderTgItem(it, idx, total) {
  const icon = CAT_ICON.get(it.cat) || '🔧';
  const when = it.ts ? bjTime(it.ts) : '近期';
  return `${icon} ${it.title}\n\n${it.summary}\n\n${it.source} · ${when} · ${total > 1 ? `(${idx}/${total}) ` : ''}${it.link}`;
}

/**
 * 生成待推送的消息列表。
 * 有精选 → 报头 + N 条精选；无精选（AI 不可用）→ 退回英文标题清单，合并为一条。
 */
export function buildTgMessages(data) {
  const { items, digest } = data;
  const out = [];
  if (digest && digest.length) {
    out.push(`${renderTgHeader(data)}\n\n🧾 今日精选 · 中文总结，共 ${digest.length} 条`);
    for (const [i, it] of digest.entries()) out.push(renderTgItem(it, i + 1, digest.length));
    return out;
  }
  // AI 不可用：退回英文标题清单（合并为一条）
  let msg = renderTgHeader(data) + '\n';
  for (const [cat, icon] of CAT_ORDER) {
    const list = items.filter(i => i.cat === cat).slice(0, 4);
    if (!list.length) continue;
    msg += `\n${icon} ${cat}\n`;
    for (const it of list) msg += `· ${it.title}\n  ${it.link}\n`;
  }
  out.push(msg);
  return out;
}

/**
 * 把「目标 × 消息」拍平成有序对列表（推送分片按这个顺序切）。
 * 目标顺序：TG_CHAT_ID（私聊）→ TG_CHANNEL（订阅频道）。
 */
export function buildPushPairs(env, data) {
  const targets = [env.TG_CHAT_ID, env.TG_CHANNEL].filter(Boolean);
  const msgs = buildTgMessages(data);
  const pairs = [];
  for (const chat of targets) {
    for (const [i, text] of msgs.entries()) pairs.push({ chat, i, text });
  }
  return pairs;
}

/** 消息间隔（同一聊天限速 ~1 条/秒，13 条连发会触发 429） */
const MSG_GAP_MS = 300;

/** 单条消息重试间隔（TG 瞬时 429/5xx；有 retry_after 时以它为准，见 retryAfterMs） */
const MSG_RETRY_MS = 1000;

/** 单条消息最多尝试次数 */
const MSG_MAX_ATTEMPTS = 3;

/** 补推队列最大轮数（每轮内 sendChunk 已有 3 次尝试，轮间重新入队） */
const RETRY_MAX_ROUNDS = 3;

/**
 * TG 429 退避时长：优先尊重响应体 parameters.retry_after（秒，TG Bot API 规范字段），
 * 提前重试会被更狠限流；兜底 MSG_RETRY_MS；硬上限 30s（外部审查 A2 修复）。
 */
function retryAfterMs(j2) {
  const ra = j2 && j2.parameters && Number(j2.parameters.retry_after);
  if (Number.isFinite(ra) && ra > 0) return Math.min(ra * 1000 + 250, 30000);
  return MSG_RETRY_MS;
}

/**
 * TG 永久性错误（重推无意义，应 ack 不重试）：
 * chat 不存在 / 被 bot 拉黑 / 用户注销 / 无权发言等。
 * 与瞬时错误（网络/5xx/限流）分流是补推机制的前提（外部审查 A1）。
 */
const TG_PERMANENT_ERR = /chat not found|bot was blocked|bot can't initiate|user is deactivated|chat_id is empty|group chat was upgraded|not enough rights|PEER_ID_INVALID/i;
const isPermanentTgError = (err) => TG_PERMANENT_ERR.test(String(err || ''));

/**
 * 发送一个推送分片（串行 + 片内限速 + 单条至多 3 次尝试）。
 * @param {object} net   createNet() 出口
 * @param {Array}  chunk buildPushPairs() 的有序子集
 * @returns {Promise<object[]>} 每条消息的推送结果（含 chat/i/ok/msgId/err）
 */
async function sendChunk(net, chunk) {
  const token = net.env.TG_BOT_TOKEN;
  const results = [];
  for (const [j, p] of chunk.entries()) {
    if (j > 0) await sleep(MSG_GAP_MS);
    let ok = false, err = null, msgId = null;
    for (let attempt = 1; attempt <= MSG_MAX_ATTEMPTS && !ok; attempt++) {
      try {
        const r = await net.fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: p.chat, text: tgTruncate(p.text), disable_web_page_preview: p.i === 0 })
        }, `tg:${p.chat}#${p.i}`);
        const j2 = await r.json().catch(() => ({ ok: false, description: '响应非 JSON' }));
        if (j2.ok) { ok = true; msgId = j2.result && j2.result.message_id; }
        else {
          err = j2.description || ('HTTP ' + r.status);
          if (attempt < MSG_MAX_ATTEMPTS) await sleep(retryAfterMs(j2));
        }
      } catch (e) {
        err = String(e && e.message || e);
        if (attempt < MSG_MAX_ATTEMPTS) await sleep(MSG_RETRY_MS);
      }
    }
    results.push({ chat: p.chat, i: p.i, ok, msgId, err: ok ? null : err });
    if (!ok) console.error(`TG 推送失败 (${p.chat} 第${p.i}条): ${err}`);
  }
  return results;
}

/**
 * 全量推送（测试/调试用；生产路径走 §13 的分片消费者）。
 * @returns {Promise<object[]>} 每条消息的推送结果
 */
export async function pushTelegram(net, data) {
  if (!net.env.TG_BOT_TOKEN) return [];
  const pairs = buildPushPairs(net.env, data);
  if (!pairs.length) return [];
  const results = [];
  const K = Math.ceil(pairs.length / SEND_SLICES);
  for (let s = 0; s < SEND_SLICES; s++) {
    results.push(...await sendChunk(net, pairs.slice(s * K, (s + 1) * K)));
  }
  return results;
}

// ============================================================
// §12 render · 网页渲染
// ============================================================
// 移动端优先的单页 HTML。纯函数（输出只依赖入参 data 与 FEEDS/CAT_ORDER）。

/** 每个分类在网页上最多展示条数（访谈观点更新慢，多给一些） */
const CAT_SHOW_LIMIT = (cat) => (cat === '访谈观点' ? 14 : 10);

export function renderPage(data) {
  const { items, failures, generatedAt, digest } = data;
  const total = items.length;
  // 今日精选 · 中文总结（先精选后总结的产物；AI 不可用时自动隐藏）
  let digestBlock = '';
  if (digest && digest.length) {
    const rows = digest.map(d => `<a class="item" href="${esc(d.link)}" target="_blank" rel="noopener">
      <div class="t">${esc(d.title)}</div>
      <div class="m"><span class="src">${esc(d.source)}</span><span class="time">${bjTime(d.ts)}</span></div>
      <div class="desc">${esc(d.summary)}</div>
    </a>`).join('');
    digestBlock = `<section class="digest"><h2>🧾 今日精选 · 中文总结<span class="cnt">${digest.length}</span></h2>${rows}</section>`;
  }
  const catBlocks = [];
  for (const [cat, icon] of CAT_ORDER) {
    const list = items.filter(i => i.cat === cat);
    if (!list.length) continue;
    const limit = CAT_SHOW_LIMIT(cat);
    const shown = list.slice(0, limit);
    const rows = shown.map(it => {
      const ago = it.ts ? bjTime(it.ts) : '近期';
      const multi = it.also && it.also.length ? `<span class="multi">×${it.also.length + 1} 源</span>` : '';
      const desc = it.desc ? `<div class="desc">${esc(it.desc.slice(0, 220))}${it.desc.length > 220 ? '…' : ''}</div>` : '';
      return `<a class="item" href="${esc(it.link)}" target="_blank" rel="noopener">
      <div class="t">${esc(it.title)}${multi}</div>
      <div class="m"><span class="src">${esc(it.source)}</span><span class="time">${ago}</span></div>
      ${desc}
    </a>`;
    }).join('');
    const more = list.length > shown.length ? `<div class="more">…还有 ${list.length - shown.length} 条</div>` : '';
    catBlocks.push(`<section><h2>${icon} ${cat}<span class="cnt">${list.length}</span></h2>${rows}${more}</section>`);
  }
  const failHtml = failures && failures.length
    ? `<details class="fails"><summary>⚠️ ${failures.length} 个源本次抓取失败</summary>${failures.map(f => `<div>${esc(f.name)}：${esc(f.err)}</div>`).join('')}</details>` : '';
  const srcList = FEEDS.map(f => f.name).join(' · ');
  return `<!DOCTYPE html>
<html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0b0d12">
<title>GUJI · AI 情报</title>
<style>
:root{--bg:#0b0d12;--card:#141822;--line:#232a3a;--txt:#e8ecf3;--sub:#8b94a7;--acc:#5b8cff;--multi:#f5a623}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Noto Sans SC",sans-serif;line-height:1.5;padding:14px 14px 40px;max-width:760px;margin:0 auto}
header{padding:10px 2px 16px;border-bottom:1px solid var(--line);margin-bottom:14px}
h1{font-size:20px;letter-spacing:.5px}
h1 .ai{color:var(--acc)}
.meta{color:var(--sub);font-size:12px;margin-top:6px}
section{margin-bottom:22px}
h2{font-size:15px;color:var(--sub);font-weight:600;margin-bottom:8px;display:flex;align-items:center;gap:6px}
.cnt{background:var(--line);border-radius:10px;padding:0 8px;font-size:11px;color:var(--txt)}
.item{display:block;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:11px 13px;margin-bottom:8px;text-decoration:none;color:var(--txt)}
.item:active{opacity:.75}
.t{font-size:14.5px;font-weight:550}
.m{display:flex;gap:10px;margin-top:5px;font-size:11.5px;color:var(--sub)}
.src{color:var(--acc)}
.desc{font-size:12.5px;color:var(--sub);margin-top:6px}
.multi{color:var(--multi);font-size:11px;margin-left:6px}
.more{color:var(--sub);font-size:12px;padding:4px 2px}
.digest .item{border-color:#2b3a55;background:#141a26}
.digest .t{color:#fff}
.fails{margin:18px 0 8px;color:var(--sub);font-size:12px}
.fails summary{cursor:pointer}
.fails div{padding:2px 0}
footer{margin-top:26px;color:#565f73;font-size:10.5px;line-height:1.7;border-top:1px solid var(--line);padding-top:12px}
.refresh{background:none;border:1px solid var(--line);color:var(--sub);border-radius:8px;font-size:11px;padding:3px 10px;margin-left:8px;cursor:pointer}
.tg-btn{display:inline-flex;align-items:center;gap:6px;margin-top:10px;background:#2AABEE;color:#fff;text-decoration:none;font-size:13px;font-weight:600;padding:8px 16px;border-radius:10px}
.tg-btn:active{opacity:.8}
@media(prefers-color-scheme:light){:root{--bg:#f6f7f9;--card:#fff;--line:#e3e6ec;--txt:#1a1d26;--sub:#6b7280;--acc:#2f6bff}}
</style></head><body>
<header>
  <h1>🤖 GUJI <span class="ai">· AI 情报</span><button class="refresh" onclick="location.reload()">刷新</button></h1>
  <div class="meta">${total} 条 · ${new Set(items.map(i=>i.source)).size} 个一手源 · 更新于 ${bjTime(generatedAt)}（北京时间）</div>
  <a class="tg-btn" href="https://t.me/guji_ai_daily" target="_blank" rel="noopener">📢 订阅 Telegram 频道 · 每日早晚推送</a>
</header>
${digestBlock}
${catBlocks.join('') || '<div class="meta">今日暂无新内容，10 秒后自动重试…</div><script>setTimeout(()=>location.reload(),10000)</script>'}
${failHtml}
<footer>来源全部为官方发布 / SEC EDGAR 官方 filing / 原创工程媒体 / 一手访谈播客，无二手聚合。<br>${esc(srcList)}</footer>
</body></html>`;
}

// ============================================================
// §13 queue · 队列消费者
// ============================================================
// 每个消费者调用：独立 budget 计数器 → 干活 → 落运行记录 → ack。
// 抛错 = 消息不 ack = 按 max_retries 退避重试，耗尽进 DLQ（§13.4 记日志）。
// 这就是 v4 对「瞬时失败」的结构性修复：05:50 那类模型/网络抖动
// 不再需要手写重试循环去兜，队列本身就是重试器。

// ---------------- 13.1 抓取分片消费者 ----------------

/**
 * guji-fetch 消费者：每条消息 = 一个分片（10 源）。
 * 子请求账：10 fetch + Anthropic seen 2（仅含 anthropic 的分片）+ 分片落库 1 ≈ 13/50。
 * 分片键带 window 且内容整体覆盖写，重试幂等；分片必然落库（哪怕全失败），
 * digest 消费者以「分片存在」判断抓取完成，不会被单个源的失败卡住。
 *
 * ⚠️ 诊断埋点（2026-10-25 追加）：曾出现「3/4 分片消费者无痕迹消失」
 *    （run:fetch 记录不更新、分片键不出现），因早期 throw 路径全部静默。
 *    现在每个阶段都写 run:fetch 记录（start → fetched → ok:true），
 *    失败路径写 ok:false + stage + err，重试前也能从 KV 看到死在哪一步。
 *    代价：+2 次 KV 写/分片（峰值 16/50，仍远低于上限）。
 */
async function consumeFetch(batch, env) {
  for (const msg of batch.messages) {
    const budget = createBudget();
    const net = createNet(env, budget);
    const body = msg.body;
    const { window, shard, feeds } = body || {};
    // 阶段记录：失败也留痕（ok:false + stage + err），供 KV 事后诊断
    const mark = (stage, extra = {}) => net.kvPut(runKey(window, `fetch:${shard}`), JSON.stringify({
      at: Date.now(), shard, stage, budget: budget.report(), ...extra
    }), { expirationTtl: 2 * 86400 }, `run:fetch:${stage}`).catch(() => {});
    try {
      if (!Array.isArray(feeds) || !feeds.length) {
        await mark('empty-feeds', { ok: false, bodyType: typeof body, bodyKeys: body && typeof body === 'object' ? Object.keys(body) : null });
        msg.ack();
        continue;
      }
      await mark('start', { ok: false, feeds: feeds.length });
      const failures = [];
      const items = await fetchShardFeeds(feeds, net, failures);
      await mark('fetched', { ok: false, items: items.length, failures: failures.length });
      // 写分片（2 天 TTL：覆盖下一班同 window 键前的保洁，异常键自动清扫）
      await net.kvPut(shardKey(window, shard), JSON.stringify({ items, failures, at: Date.now() }),
        { expirationTtl: 2 * 86400 }, `shard:${shard}`);
      await net.kvPut(runKey(window, `fetch:${shard}`), JSON.stringify({
        at: Date.now(), shard, ok: true, items: items.length, failures: failures.length, budget: budget.report()
      }), { expirationTtl: 2 * 86400 }, 'run:fetch').catch(() => {});
      msg.ack();
      console.error(`[fetch] ${window} 分片${shard}：${items.length} 条，失败 ${failures.length} 个源，预算 ${budget.total}/${budget.limit}`);
    } catch (e) {
      await mark('error', { ok: false, err: String(e && e.message || e).slice(0, 300), errName: e && e.name });
      throw e;
    }
  }
}

// ---------------- 13.2 精选消费者 ----------------

/**
 * guji-digest 消费者：读全部分片 → 合并后处理 → 早晚班 cutoff → enrich → AI 精选 → 落库。
 * 子请求账：4 分片读 + latest 1 + 增强 4×2 + 模型链 4 + 落库 2 ≈ 19/50。
 *
 * 关键语义：
 *   - 分片未就绪 → 调用内轮询等待（≤45s，预算护栏 40），仍缺则 throw（队列重试，
 *     每次重试都有全新预算与 45s 等待——生产者绑定另有 90s deliveryDelay 双保险）
 *   - items 为空 → 不碰 latest（保留上一班数据，页面不空屏），throw 重试
 *   - 精选彻底失败 → 仍落库（digest=[]，推送走英文清单兜底）→ 记 run → throw 重试
 *
 * ⚠️ 依赖 wrangler.toml 的 max_batch_size=1：本消费者只处理 batch.messages[0]
 *    （v4.0.1：外部审查指出代码与配置存在隐性耦合，在此显式声明）。
 */
async function consumeDigest(batch, env) {
  const budget = createBudget();
  const net = createNet(env, budget);
  const t0 = Date.now();
  const logs = [];
  const log = m => {
    const s = `${((Date.now() - t0) / 1000).toFixed(1)}s ${m}`;
    logs.push(s);
    console.error('[digest] ' + s);
  };
  const msg = batch.messages[0];
  const { window, full = false } = msg.body || {};
  log(`开始 window=${window} full=${full}`);

  // 1. 等分片就绪（生产者绑定配了 90s deliveryDelay，通常首轮即就绪；
  //    此处轮询是双保险：抓取异常慢时在调用内等，不烧队列重试次数）
  let shards = null;
  const deadline = Date.now() + SHARD_WAIT_MS;
  const markDigest = (extra) => net.kvPut(runKey(window, 'digest'), JSON.stringify({
    at: Date.now(), ok: false, budget: budget.report(), ...extra
  }), { expirationTtl: 2 * 86400 }, 'run:digest:err').catch(() => {});
  while (true) {
    const attempt = [];
    let ready = true;
    for (let n = 0; n < SHARD_COUNT; n++) {
      const s = await net.kvGet(shardKey(window, n), 'json', `shard:${n}`).catch(() => null);
      if (!s || !Array.isArray(s.items)) { ready = false; break; }
      attempt.push(s);
    }
    if (ready) { shards = attempt; break; }
    if (Date.now() >= deadline) {
      await markDigest({ stage: 'shard-wait-timeout', waitedMs: SHARD_WAIT_MS });
      throw new Error(`分片未就绪（等待 ${SHARD_WAIT_MS / 1000}s 仍缺失，window=${window}，抓取班失败？）`);
    }
    // 预算护栏：轮询消耗超过 40 就中止，交给队列重试（每次重试都有全新 50 预算，
    // 10 次重试 × 45s ≈ 7.5 分钟总覆盖，且任何单次调用都不越限）
    if (budget.total > 40) {
      await markDigest({ stage: 'shard-wait-budget', total: budget.total });
      throw new Error(`等分片期间预算已达 ${budget.total}/50，中止本轮等待（队列将重试）`);
    }
    log(`分片未就绪，${SHARD_POLL_MS / 1000}s 后重试…`);
    await sleep(SHARD_POLL_MS);
  }
  const rawItems = shards.flatMap(s => s.items);
  const failures = shards.flatMap(s => s.failures || []);
  log(`分片就绪：${rawItems.length} 条原始条目，${failures.length} 个源失败`);

  // 2. 合并后处理（纯函数，语义同 v3 collect 后半段）
  const items = finalizeItems(rawItems);
  log(`后处理完成：${items.length} 条`);
  if (!items.length) {
    await markDigest({ stage: 'no-items', raw: rawItems.length });
    throw new Error('本班无有效条目（全被时间窗/关键词过滤？）');
  }

  // 3. 早晚班机制：晚报只精选「上次成功精选以来」的新条目，避免与早报重复播报；
  //    一条新增也够起一期；完全没有新增才回退全窗口（宁薄勿空）
  let nowData = null;
  try { nowData = await net.kvGet(KV_KEY, 'json', 'state:latest'); } catch (e) {}
  const lastAt = (!full && nowData) ? Number(nowData.lastDigestAt) || 0 : 0;
  const fresh = lastAt ? items.filter(it => isFresh(it.ts, lastAt)) : [];
  const pool = fresh.length >= 1 ? fresh : items;
  log(`精选素材 ${pool.length} 条（新增 ${fresh.length} 条）`);

  // 4. AI 精选（enrich + 模型链）
  let digest = null, digestErr = null, aiTrace = null;
  try {
    const r = await makeDigest(pool, net);
    digest = r && r.digest;
    aiTrace = r && r.trace;
  } catch (e) { digestErr = String(e && e.message || e).slice(0, 200); }
  log(`精选完成：${digest ? digest.length : 0} 条${digestErr ? ' / 错误：' + digestErr : ''}`);

  // 5. 落库（items 总是保存；精选失败也保存，推送走英文清单兜底）
  const nowSecValue = nowSec();
  const data = {
    items, failures, generatedAt: Date.now(), digest: digest || [],
    lastDigestAt: (digest && digest.length) ? nowSecValue : (nowData ? nowData.lastDigestAt || 0 : 0),
    window, logs, digestErr, aiTrace,
  };
  // ⚠️ 快照必须先于 KV.put 挂载，否则 KV 副本永远没有 budget 字段（v3 遗留坑）。
  //    此快照不含随后的 2 次 put（latest + run），权威完整账以 run:{window}:digest 为准。
  data.budget = budget.report();
  await net.kvPut(KV_KEY, JSON.stringify(data), { expirationTtl: 14 * 86400 }, 'state:save');

  // 6. 精选彻底失败 → 记 run + throw（消息重试；items 已落库，页面与推送有兜底）
  if (!digest || !digest.length) {
    await net.kvPut(runKey(window, 'digest'), JSON.stringify({
      at: Date.now(), ok: false, digestErr, budget: budget.report()
    }), { expirationTtl: 2 * 86400 }, 'run:digest').catch(() => {});
    throw new Error('精选失败：' + (digestErr || '模型链全部失败'));
  }
  await net.kvPut(runKey(window, 'digest'), JSON.stringify({
    at: Date.now(), ok: true, digest: digest.length, budget: budget.report() // put 之后的完整账
  }), { expirationTtl: 2 * 86400 }, 'run:digest').catch(() => {});
  msg.ack();
  log(`完成，预算 ${budget.total}/${budget.limit}`);
}

// ---------------- 13.3 推送分片消费者 ----------------

/**
 * guji-send 消费者：每条消息 = 一个顺序分片（max_concurrency=1 串行执行保序）。
 * 子请求账：latest 1 + ≤9 条消息×至多 3 次尝试 + 统计 1 ≈ 29/50（最坏）。
 *
 * 幂等：slice 0 完成后写 run:{window}:push，/api/push 路由据此防重复入队；
 * 数据新鲜度闸门继承 v3（3 小时），force 可 bypass（手动补推旧内容）。
 *
 * ⚠️ 依赖 wrangler.toml 的 max_batch_size=1：本消费者只处理 batch.messages[0]
 *    （v4.0.1：外部审查指出代码与配置存在隐性耦合，在此显式声明）。
 *
 * v4.0.1（外部审查 A1 修复）：单条 3 次尝试失败不再无条件 ack——瞬时失败项
 * 入 guji-retry 补推队列（带 attempts 轮计数），永久错误（chat not found 等）
 * ack 不重推。修复前失败消息永久退出队列，只能等下一班。
 */
async function consumeSend(batch, env) {
  const budget = createBudget();
  const net = createNet(env, budget);
  const msg = batch.messages[0];
  const { window, slice = 0, slices = SEND_SLICES, force = false } = msg.body || {};

  const data = await net.kvGet(KV_KEY, 'json', 'state:latest').catch(() => null);
  if (!data) throw new Error('KV 无数据（精选班可能未完成，重试）');

  // 新鲜度闸门：抓取/精选班失败时不推 3 小时前的旧内容
  const age = data.generatedAt ? Date.now() - data.generatedAt : Infinity;
  if (!force && age >= 3 * 3600 * 1000) {
    console.error(`[send] 跳过：数据已过期 ${(age / 3600000).toFixed(1)} 小时（抓取班可能失败）`);
    msg.ack();
    return;
  }
  if (data.window && window && data.window !== window && !force) {
    console.error(`[send] 跳过：latest 属于 ${data.window}，本班是 ${window}`);
    msg.ack();
    return;
  }

  const pairs = buildPushPairs(env, data);
  if (!pairs.length) {
    console.error('[send] 跳过：无推送目标（TG_BOT_TOKEN/TG_CHAT_ID/TG_CHANNEL 未配置）');
    msg.ack();
    return;
  }
  const K = Math.ceil(pairs.length / slices);
  const chunk = pairs.slice(slice * K, (slice + 1) * K);
  if (!chunk.length) { msg.ack(); return; }

  const results = await sendChunk(net, chunk);
  const okCount = results.filter(r => r.ok).length;
  const record = { at: Date.now(), slice, ok: okCount, total: results.length, budget: budget.report() };
  await net.kvPut(runKey(window, `send:${slice}`), JSON.stringify(record),
    { expirationTtl: 2 * 86400 }, `run:send:${slice}`).catch(() => {});
  // slice 0 完成即写「本班已推」标记（路由幂等守卫与 diag 用）
  if (slice === 0) {
    await net.kvPut(runKey(window, 'push'), JSON.stringify({ at: Date.now(), ok: okCount, total: results.length }),
      { expirationTtl: 2 * 86400 }, 'run:push').catch(() => {});
  }

  // v4.0.1（外部审查 A1）：失败项分流——瞬时失败入补推队列，永久错误放弃并留痕
  const failed = results.filter(r => !r.ok);
  if (failed.length) {
    const retriable = failed.filter(r => !isPermanentTgError(r.err));
    const permanent = failed.length - retriable.length;
    if (retriable.length) {
      // 失败项按 i（全局消息序号）从 chunk 回取 text，只补推失败的，不重推已成功的
      const retryPairs = retriable.map(r => {
        const src = chunk.find(p => p.i === r.i);
        return { chat: r.chat, i: r.i, text: src ? src.text : '' };
      }).filter(p => p.text);
      if (retryPairs.length) {
        await net.qSend(env.GUJI_RETRY_Q,
          { kind: 'retry', window, slice, attempts: 1, pairs: retryPairs },
          `retry:${slice}:${retryPairs.length}条`);
        console.error(`[send] ${window} 分片${slice}：${retryPairs.length} 条瞬时失败，已入补推队列`);
      }
    }
    if (permanent) {
      console.error(`[send] ${window} 分片${slice}：${permanent} 条永久错误（${failed.filter(r => isPermanentTgError(r.err)).map(r => r.err).join('; ').slice(0, 120)}），放弃重推`);
    }
    await net.kvPut(runKey(window, `send:${slice}:failed`), JSON.stringify({
      at: Date.now(), slice, retried: retriable.length, permanent,
      errors: failed.map(r => ({ i: r.i, chat: r.chat, err: String(r.err).slice(0, 120) }))
    }), { expirationTtl: 2 * 86400 }, `run:send-fail:${slice}`).catch(() => {});
  }
  msg.ack();
  console.error(`[send] ${window} 分片${slice}：${okCount}/${results.length} 成功，预算 ${budget.total}/${budget.limit}`);
}

// ---------------- 13.4 推送补推消费者（v4.0.1 新增） ----------------

/**
 * guji-retry 消费者：消费 guji-send 分流来的失败项（外部审查 A1 修复的方案 2）。
 * 每条消息 = { window, slice, attempts, pairs:[{chat,i,text}] }，只含瞬时失败项。
 *
 * 语义：
 *   - 重推成功 → ack（at-least-once：极端情况下首次超时误判会重复推送，日报可接受）
 *   - 仍有瞬时失败且 attempts < RETRY_MAX_ROUNDS → attempts+1 重新入队（消息级重试天然幂等）
 *   - 达到轮数上限 → 记 run + console.error 后 ack（失败绝不静默，但不无限循环）
 *   - 全是永久错误 → ack（sendChunk 已按 isPermanentTgError 在上游分流过，双保险）
 *
 * ⚠️ 同样依赖 max_batch_size=1，只处理 batch.messages[0]。
 */
async function consumeRetry(batch, env) {
  const budget = createBudget();
  const net = createNet(env, budget);
  const msg = batch.messages[0];
  const { window, slice = 0, attempts = 1, pairs } = msg.body || {};
  if (!pairs || !pairs.length) { msg.ack(); return; }

  const results = await sendChunk(net, pairs);
  const okCount = results.filter(r => r.ok).length;
  const failed = results.filter(r => !r.ok);
  const retriable = failed.filter(r => !isPermanentTgError(r.err));
  await net.kvPut(runKey(window, `retry:${slice}`), JSON.stringify({
    at: Date.now(), slice, attempts, ok: okCount, total: results.length, budget: budget.report()
  }), { expirationTtl: 2 * 86400 }, `run:retry:${slice}`).catch(() => {});

  if (!retriable.length) {
    if (failed.length) console.error(`[retry] ${window} 分片${slice} 第${attempts}轮：${failed.length} 条永久错误，放弃`);
    msg.ack();
    return;
  }
  if (attempts >= RETRY_MAX_ROUNDS) {
    console.error(`[retry] ${window} 分片${slice} 第${attempts}轮仍失败 ${retriable.length} 条（${retriable.map(r => r.err).join('; ').slice(0, 150)}），达到轮数上限，放弃（run KV 已留痕）`);
    msg.ack();
    return;
  }
  const nextPairs = retriable.map(r => {
    const src = pairs.find(p => p.i === r.i);
    return { chat: r.chat, i: r.i, text: src ? src.text : '' };
  }).filter(p => p.text);
  if (!nextPairs.length) { msg.ack(); return; }
  await net.qSend(env.GUJI_RETRY_Q,
    { kind: 'retry', window, slice, attempts: attempts + 1, pairs: nextPairs },
    `retry:${slice}:r${attempts + 1}`);
  console.error(`[retry] ${window} 分片${slice} 第${attempts}轮：${nextPairs.length} 条仍失败，入第 ${attempts + 1} 轮`);
  msg.ack();
}

// ---------------- 13.5 死信消费者 ----------------

/**
 * guji-dlq 消费者：重试耗尽的消息在此落地。只记日志（不静默丢失），
 * 排查入口：wrangler tail 搜 [dlq]，或 /api/diag 看对应 run 记录缺失。
 */
async function consumeDlq(batch, env) {
  for (const msg of batch.messages) {
    console.error(`[dlq] 死信（重试耗尽）：${JSON.stringify(msg.body).slice(0, 500)}`);
    msg.ack();
  }
}

// ============================================================
// §14 worker · 入口（cron 入队 + 路由）
// ============================================================

// ---------------- 诊断辅助 ----------------

/**
 * 实时探测 AI 绑定是否可用（排查「精选 0 条」）。
 * v4.0.1（外部审查 C4 修复）：原探测 AI_MODELS[0]（qwen3）——有 ZHIPU_KEY 时
 * 生产链是 [GLM, llama]，qwen3 永不参与生产，探测的是死模型。改为探测
 * **CF 侧进生产链的模型**：有 key 时是 llama（AI_MODELS[1]），无 key 时是 qwen3。
 */
async function probeAI(net) {
  try {
    const t0 = Date.now();
    const m = net.env.ZHIPU_KEY ? AI_MODELS[1] : AI_MODELS[0];
    const res = await net.aiRun(m, {
      messages: [{ role: 'user', content: '只回复两个字：正常' }], max_tokens: 64,
      chat_template_kwargs: { enable_thinking: false }
    }, 'diag:ai');
    return { ok: true, ms: Date.now() - t0, text: aiText(res).slice(0, 40), model: m };
  } catch (e) {
    return { ok: false, err: String(e && e.message).slice(0, 200), model: net.env.ZHIPU_KEY ? AI_MODELS[1] : AI_MODELS[0] };
  }
}

/** 探测智谱 GLM 是否可用（仅当配置了 ZHIPU_KEY） */
async function probeZhipu(net) {
  try {
    const t0 = Date.now();
    const text = await fetchViaZhipu('只回复两个字：正常', net);
    return { ok: true, ms: Date.now() - t0, text: text.slice(0, 40) };
  } catch (e) {
    return { ok: false, err: String(e && e.message).slice(0, 200) };
  }
}

const JSON_HEADERS = { 'content-type': 'application/json', 'access-control-allow-origin': '*' };

function json(body, pretty = false) {
  return new Response(JSON.stringify(body, null, pretty ? 1 : 0), { headers: JSON_HEADERS });
}

const LOADING_PAGE = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>GUJI</title><style>body{background:#0b0d12;color:#e8ecf3;font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}</style></head><body><div>正在生成今日情报（队列处理中，约 1 分钟）…<script>setTimeout(()=>location.reload(),10000)</script></div></body></html>`;

// ---------------- 入队助手 ----------------

/**
 * 入队抓取分片（cron :40 与 /api/build 共用）。
 * @param {object} env
 * @param {object} net   createNet() 出口（用于记账）
 * @param {string} window 班次标识
 */
async function enqueueFetch(env, net, window) {
  for (let n = 0; n < SHARD_COUNT; n++) {
    const feeds = FEEDS.slice(n * SHARD_SIZE, (n + 1) * SHARD_SIZE);
    await net.qSend(env.GUJI_FETCH_Q, { kind: 'fetch', window, shard: n, feeds }, `fetch:shard${n}`);
  }
}

/**
 * 入队一次「抓取 + 精选」（/api/build 手动全量用）。
 * @param {object} env
 * @param {object} net   createNet() 出口（用于记账）
 * @param {string} window 班次标识
 * @param {boolean} full   忽略 lastDigestAt 重新全量精选
 */
async function enqueueBuild(env, net, window, full) {
  await enqueueFetch(env, net, window);
  await net.qSend(env.GUJI_DIGEST_Q, { kind: 'digest', window, full }, 'digest');
}

/** 入队一次「推送」（3 个顺序分片） */
async function enqueueSend(env, net, window, force) {
  for (let s = 0; s < SEND_SLICES; s++) {
    await net.qSend(env.GUJI_SEND_Q, { kind: 'send', window, slice: s, slices: SEND_SLICES, force }, `send:slice${s}`);
  }
}

// ---------------- 入口 ----------------

export default {
  /**
   * cron：只入队，不干活。
   * 按北京时间的分钟数分流：40 抓取 / 50 精选 / 00 推送。
   * 每次触发 ≤4 个子请求（队列发送，保守计入）。
   */
  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      const budget = createBudget();
      const net = createNet(env, budget);
      try {
        const at = event.scheduledTime || Date.now();
        const window = currentWindow(at);
        const minute = bjDate(at).getUTCMinutes();
        if (minute === FETCH_MINUTE) {
          // 只入队抓取；精选由 :50 的 cron 单独入队（10 分钟缓冲给抓取消费者）
          await enqueueFetch(env, net, window);
          console.error(`[cron] ${window} 抓取入队：${SHARD_COUNT} 分片，预算 ${budget.total}`);
        } else if (minute === DIGEST_MINUTE) {
          await net.qSend(env.GUJI_DIGEST_Q, { kind: 'digest', window, full: false }, 'digest');
          console.error(`[cron] ${window} 精选入队，预算 ${budget.total}`);
        } else if (minute === SEND_MINUTE) {
          await enqueueSend(env, net, window, false);
          console.error(`[cron] ${window} 推送入队：${SEND_SLICES} 片，预算 ${budget.total}`);
        } else {
          console.error(`[cron] 未识别的分钟数 ${minute}（cron=${event.cron}），忽略`);
        }
      } catch (e) {
        console.error(`[cron] 入队失败：${e && e.message}`);
      }
    })());
  },

  /**
   * 队列消费者路由：按 batch.queue 分发。
   * 抛错 → 未 ack 消息按 max_retries 重试，耗尽进 DLQ。
   */
  async queue(batch, env, ctx) {
    const q = batch.queue;
    try {
      if (q === Q_FETCH) await consumeFetch(batch, env);
      else if (q === Q_DIGEST) await consumeDigest(batch, env);
      else if (q === Q_SEND) await consumeSend(batch, env);
      else if (q === Q_RETRY) await consumeRetry(batch, env);
      else if (q === Q_DLQ) await consumeDlq(batch, env);
      else for (const m of batch.messages) m.ack(); // 未知队列：确认消费，不阻塞
    } catch (e) {
      console.error(`[queue] ${q} 消费失败（将按 max_retries 重试，耗尽进 DLQ）：${e && e.message}`);
      throw e;
    }
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 抓取班次：入队抓取分片 + 精选（异步处理，轮询 /api/diag 看结果）
    if (url.pathname === '/api/build') {
      const full = url.searchParams.get('full') === '1';
      const budget = createBudget();
      const net = createNet(env, budget);
      const window = currentWindow(Date.now());
      await enqueueBuild(env, net, window, full);
      return json({ ok: true, window, enqueued: { fetch: SHARD_COUNT, digest: 1 }, budget: budget.report() });
    }

    // 推送班次：入队 3 个顺序分片（幂等：本班 10 分钟内推过则跳过，?force=1 强制）
    if (url.pathname === '/api/push') {
      const force = url.searchParams.get('force') === '1';
      const budget = createBudget();
      const net = createNet(env, budget);
      const window = currentWindow(Date.now());
      if (!force) {
        let pushed = null;
        try { pushed = await net.kvGet(runKey(window, 'push'), 'json', 'guard:push'); } catch (e) {}
        if (pushed && Date.now() - pushed.at < 10 * 60 * 1000) {
          return json({ ok: true, skipped: `本班（${window}）已于 ${bjTime(pushed.at)} 推送过，加 ?force=1 可重推` });
        }
      }
      await enqueueSend(env, net, window, force);
      return json({ ok: true, window, enqueued: { send: SEND_SLICES }, force });
    }

    // 诊断：latest 状态 + 分片就绪情况 + 实时探测 AI 配额（排查「精选 0 条」）
    if (url.pathname === '/api/diag') {
      const budget = createBudget();
      const net = createNet(env, budget);
      let data = null;
      try { data = await net.kvGet(KV_KEY, 'json', 'state:latest'); } catch (e) {}
      const win = (data && data.window) || currentWindow(Date.now());
      const shards = [];
      for (let n = 0; n < SHARD_COUNT; n++) {
        let s = null;
        try { s = await net.kvGet(shardKey(win, n), 'json', `diag:shard:${n}`); } catch (e) {}
        shards.push(s ? { shard: n, items: s.items.length, failures: (s.failures || []).length, at: s.at } : null);
      }
      const aiProbe = await probeAI(net);
      const zhipuProbe = net.env.ZHIPU_KEY ? await probeZhipu(net) : null;
      return json({
        kvFound: !!data, window: win,
        generatedAt: data ? data.generatedAt : null,
        bjTime: data ? bjTime(data.generatedAt) : null,
        items: data ? (data.items || []).length : 0,
        digest: data ? (data.digest || []).length : 0,
        lastDigestAt: data ? data.lastDigestAt : null,
        digestErr: data ? data.digestErr : null,
        logs: data ? data.logs : null,
        aiTrace: data ? data.aiTrace : null,
        budget: data ? data.budget : null, // 上一班精选班的子请求明细
        failures: data ? (data.failures || []).map(f => f.name + ': ' + f.err) : null,
        shards, aiProbe, zhipuProbe
      }, true);
    }

    let data = null;
    try {
      data = await createNet(env, createBudget()).kvGet(KV_KEY, 'json', 'state:latest');
    } catch (e) {}
    if (!data) {
      // 首次打开：后台入队抓取 + 精选，10 秒后刷
      if (ctx?.waitUntil) {
        const budget = createBudget();
        ctx.waitUntil(enqueueBuild(env, createNet(env, budget), currentWindow(Date.now()), false).catch(e => console.error('[prime] ' + (e && e.message))));
      }
      return new Response(LOADING_PAGE, { headers: { 'content-type': 'text/html; charset=utf-8' } });
    }
    return new Response(renderPage(data), {
      headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'public, max-age=120' }
    });
  }
};

// ============================================================
// §15 对外导出
// ------------------------------------------------------------
// 作用域说明：
//   - Cloudflare Workers 只认 default 导出（上方的 scheduled / queue / fetch）。
//   - 其余符号在此集中导出，供单测与线上调试引用；
//     行为相关的函数（renderPage/pushTelegram/…）已在各自分区就地 export，
//     这里只补各区常量与纯工具，不重复导出。
//   - 新增导出请按分区归类，保持「§N 注释 = 一个关注点」的可读性。
// ============================================================

export {
  // §1 config
  KV_KEY, SEEN_SET_KEY,
  Q_FETCH, Q_DIGEST, Q_SEND, Q_RETRY, Q_DLQ,
  SHARD_SIZE, SHARD_COUNT, SEND_SLICES, SHARD_WAIT_MS, SHARD_POLL_MS, shardKey, runKey,
  ENRICH_MAX, HTML_MAX, MAX_AGE,
  AI_KW, isAI, FEEDS,
  UA_CHROME, FETCH_MS, BATCH_MS, CONCURRENCY,
  AI_MODELS, AI_MS,
  FETCH_MINUTE, DIGEST_MINUTE, SEND_MINUTE, CAT_ORDER,
  // §2 text（esc/stripTags/normTitle/cleanMd 为纯函数；tgTruncate 已就地导出）
  esc, stripTags, normTitle, cleanMd,
  // §3 time（bjTime/currentWindow 已就地导出）
  toMs, toSec, nowSec, bjDate, isFresh,
  // §6 budget（SUBREQUEST_LIMIT / SUBREQUEST_WARN / BUDGET_PLAN / PLAN_PEAK 已就地导出）
  // §9 shard（fetchShardFeeds/finalizeItems 已就地导出）
  DC_KW, recat, SRC_LIMIT,
  // §10 digest
  MATERIAL_MAX,
  // §11 telegram
  CAT_ICON, MSG_GAP_MS, MSG_MAX_ATTEMPTS, RETRY_MAX_ROUNDS, isPermanentTgError, retryAfterMs,
};
