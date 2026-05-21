# 阶段 7：Superset BI 看板 + NL2SQL 实证（项目收官）

> 两个目标合一：
> 1. **Superset 接 Trino** → 把湖仓数据可视化（30 分钟）
> 2. **NL2SQL 实证** → 让 LLM 把自然语言翻译成 Trino SQL，对照 Zero-shot / Few-shot / RAG 三种范式
>
> 第 2 个目标会写成 ai-gateway 项目的**第 12 份实证报告**，两个项目首尾相连。

---

## 7.1 Superset 是什么

**开源 BI 工具**，定位等同于 Tableau / Power BI / 阿里 Quick BI。

| 功能 | 干什么 |
|---|---|
| Dataset | 注册一张数据库表为"数据集"，可加计算字段 |
| Chart | 基于 Dataset 拖出图表（柱状图、折线、漏斗、地理热力） |
| Dashboard | 把多个 Chart 组合成"看板" |
| SQL Lab | 在线写 SQL 跑数据库（替代 DataGrip / Trino CLI 的体感） |

**为什么选 Superset 而不是 Tableau**：
- 开源，能本地跑
- 阿里 Quick BI 内核大量参考 Superset
- 对 Trino / Hive / ClickHouse 等 OLAP 引擎原生支持

---

## 7.2 Superset 架构

```
┌─────────────────────┐
│ Superset Webserver  │  ◄─ Flask + React
│   :8088              │
└──────┬──────────────┘
       │
   ┌───┴────────┐
   ▼            ▼
PostgreSQL    Trino  ──────► Iceberg 表 (dw.ads_funnel_daily 等)
(元数据)    (查询引擎)
```

- 元数据库存 dashboard / chart 定义、用户、权限
- 真正的数据查询通过 SQLAlchemy + Trino dialect 走 Trino → Iceberg

---

## 7.3 Superset 部分要做的事（实验 13）

接好 Trino，搭一个跨境电商运营看板，包含：

1. **GMV 大数 Big Number**：本周总 GMV
2. **GMV 国家分布柱图**：按国家排序的 GMV
3. **漏斗图**：曝光 → 点击 → 加购 → 下单 → 支付（用 ads_funnel_daily）
4. **GMV 趋势折线**：按 dt 的 GMV
5. **国家 × 品类热力图**：哪个组合最赚钱

UI 拖拽实现，**不写代码**。这个看板是给运营 / 老板看的，验证"湖仓 → BI"链路通。

---

## 7.4 NL2SQL 是什么 + 为什么重要

**痛点**：运营 / 业务方想看数据，但不会写 SQL。
- 老传统：提需求 → 数据团队写 SQL → 出报表 → 排期 1-2 周
- 看板时代：BI 工具拖拽，但还是要先有 Dataset 和 Chart
- 新趋势：**"上周华东区漏斗转化率"** → AI 自动生成 SQL → 跑 → 出结果

**这就是 NL2SQL**，是阿里云 / Snowflake / Databricks 都在卷的方向。

| 产品 | NL2SQL 方案 |
|---|---|
| 阿里云 | Quick BI **智能问答** + DataWorks **Agent** |
| AWS | QuickSight Q |
| Snowflake | Cortex Analyst |
| Databricks | Genie (Spaces) |

---

## 7.5 NL2SQL 的三种范式（你的对照实验）

### 范式 1: Zero-shot
**只给 schema + 问题**，让 LLM 自己生成 SQL。

Prompt：
```
你是 Trino SQL 专家。下面是表 schema:
[schema 文本...]
问题: 上周各国 GMV
请输出 Trino SQL。
```

**预期**：简单问题能答；复杂问题（多表 JOIN、CTE、窗口函数）经常错。

### 范式 2: Few-shot
**schema + 几个 Q→SQL 示例**，模型学着照葫芦画瓢。

Prompt 多加 3 个示例：
```
示例 1: "今天 GMV" → SELECT SUM(gmv) FROM dw.ads_funnel_daily WHERE dt=current_date
示例 2: "各国 pay_uv" → SELECT country, SUM(pay_uv) ... GROUP BY country
示例 3: "漏斗转化率" → SELECT ..., SUM(pay_uv)*100.0/SUM(impression_uv) ...
```

**预期**：准确率显著提升，模型学会"我们的表怎么写 SQL"的口径。

### 范式 3: RAG (检索增强)
- 把 schema 文档、过往 SQL、业务术语映射做成 embedding 索引
- 问题来了 → 检索 top-K 相关 schema 片段 + 示例 → 喂给 LLM

**预期**：复杂 schema（几十张表）下，比 Few-shot 又好一个台阶。

---

## 7.6 评测设计（实验 14）

**8 道测试题，难度递增**：

| # | 难度 | 问题 | 考点 |
|---|---|---|---|
| 1 | ⭐ | 上周总 GMV 是多少？ | 简单聚合 |
| 2 | ⭐ | 上周哪个国家 GMV 最高？ | ORDER BY + LIMIT |
| 3 | ⭐⭐ | 过去 7 天每个国家每天的 pay_uv | GROUP BY 多维 |
| 4 | ⭐⭐ | 哪个品类的 GMV 占比超过 30%？ | 占比 / 窗口函数 |
| 5 | ⭐⭐ | 整体支付转化率 (pay_uv / impression_uv) | 派生指标 |
| 6 | ⭐⭐⭐ | GMV 环比下降超过 10% 的国家 | LAG 窗口 + 自连接 |
| 7 | ⭐⭐⭐ | 不同 age_group 的 ARPU | 跨表 JOIN (dwd + dws) |
| 8 | ⭐⭐⭐ | 渠道贡献度排名 (channel 来源的 GMV / 总 GMV) | 维度 JOIN + 占比 |

**评分维度**：
- ✅ SQL 能跑通（语法正确）
- ✅ 结果正确（跟人工写的 SQL 对答案一致）
- ⏱️ 单 query 延迟
- 💰 token 消耗 / 成本

**预期对照表**（模板，填实测后的真实数字）：

| 范式 | 准确率 (8 题) | 平均延迟 | 平均 token |
|---|---|---|---|
| Zero-shot | ? / 8 | ? ms | ? |
| Few-shot | ? / 8 | ? ms | ? |
| RAG | ? / 8 | ? ms | ? |

---

## 7.7 模型选择 + 接入方式

复用 ai-gateway 项目的 **Qwen-Turbo (百炼)**：
- API 端点：DashScope
- 已经在 `~/.zshrc` 里设置了 `DASHSCOPE_API_KEY`
- ai-gateway 第 1 份实证就证明了 Qwen-Turbo 是这个规模任务的甜区

**Embedding（RAG 用）**：百炼 `text-embedding-v2`（1536 维），用现成的。

---

## 7.8 跟 ai-gateway 的连接关系

```
ai-gateway 项目                       bigdata-lakehouse 项目
├─ 11 份实证 (RAG / Agent / LoRA)     ├─ 6 份实证 (Parquet/Iceberg/Spark/Trino/Flink/Airflow)
└─ 第 12 份实证 (NL2SQL) ◄──── 跨项目 ───► 用本项目的 Trino + Iceberg 表
```

**这就是你的简历杀手锏**：两个项目首尾相连，从基础设施（湖仓）到 AI 应用层（NL2SQL），覆盖 GenAI SA 的完整能力图谱。

---

## 7.9 本阶段两个实验 / 一份报告

| 实验 | 产出 | 时间 |
|---|---|---|
| 实验 13: Superset 看板 | 5 个 chart 组成一个 Dashboard | 30-45 分钟 |
| 实验 14: NL2SQL 对照 | 8 题 × 3 范式 = 24 次评测，输出对照表 | 1.5-2 小时 |

**报告归档**：
- `bigdata-lakehouse/experiments/07-superset-nl2sql-results.md`（本项目）
- 同步一份到 `ai-gateway/experiments/12-nl2sql-on-lakehouse.md`（兼第 12 份）

---

## 7.10 概念检查（动手前）

- [ ] Superset 跟 Trino / Iceberg 是什么关系？
- [ ] BI 工具跟"自然语言问数 NL2SQL"是替代还是互补？
- [ ] Zero-shot vs Few-shot vs RAG 三种范式各自的杠杆点？
- [ ] 评测 NL2SQL 必须看哪几个维度？
- [ ] 为什么用 Qwen-Turbo 而不是 Qwen-1.5B 本地模型？

---

## 7.11 实操步骤（占位）

```bash
# 1. 起完整栈 + Superset
docker compose -f docker-compose/07-superset.yml up -d --build

# 2. 实验 13: Superset 看板（UI 操作）
#    http://localhost:8088  (admin / admin)

# 3. 实验 14: NL2SQL 对照
source venv/bin/activate
pip install -r requirements.txt
python3 jobs/nl2sql/nl2sql_bench.py
```
