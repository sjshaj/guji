export const FEEDS = [
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
  { id:'p-bg2',      name:'BG2',                kind:'rss', cat:'访谈观点', win:'30d', url:'https://anchor.fm/s/f06c2370/podcast/rss' },
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