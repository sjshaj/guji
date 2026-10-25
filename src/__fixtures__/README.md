# __fixtures__ —— 真实源快照（契约测试用）

> v4.0.2 新增（外部审查 D2）。这里放的是**从线上源原样截取的真实响应**，
> 不是手工构造的 XML。用途：契约测试锁住「已知的源格式形态」，
> 生产 dry-run（`/api/fetch?dry=1`）探测「未知的格式漂移」——两者分工：
> fixture 防回归，dry-run 防漂移。

## 文件

| 文件 | 来源 | 截取时间 | 条数 | 锻炼的解析路径 |
|---|---|---|---|---|
| `rss-openai.xml` | openai.com/news/rss.xml | 2026-10-26 凌晨 | 3 | RSS 2.0；**CDATA 包裹的 title**（坑 2：stripTags 会把 CDATA 当标签吃）；HTML 实体（坑 3）；数字 guid |
| `atom-edgar.xml` | efts.sec.gov/LATEST/browse-edgar（Apple, CIK 0000320193） | 2026-10-26 凌晨 | 10 | Atom；**ISO-8859-1 编码**（不是 UTF-8）；entry/category/link 结构；filing 摘要的 Item 编号前缀 |

## 为什么必须是真实快照

构造的 XML 只会包含「你想得到的形态」；真实源带怪癖——CDATA、实体、
非 UTF-8 编码、Item 编号前缀——这些怪癖正是历史 bug 的来源
（DEVLOG 坑 2/3）。v4.0.2 接入 fixture 时，静态分析曾误判
「真实 EDGAR atom 无 summary 元素会让 Item/link/desc 提取失效」，
被本 fixture 的测试实证推翻（DEVLOG §4.6.3）——**读代码说会坏，
跑一遍才知道不会坏**。

## 维护纪律

- 源格式升级导致 smoke 报契约违约时：先判断是真漂移还是快照过期，
  前者修解析器，后者重新截取（**保持原样截取，不手工修饰**）
- 重新截取后在本文档更新「截取时间」
- 不要把含密钥/个人数据的响应放进来的原则同样适用
