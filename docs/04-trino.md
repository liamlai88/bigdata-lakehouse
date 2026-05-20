# 阶段 4：Trino 查询引擎

> 一句话：**Spark 适合"批处理 ETL"，Trino 适合"交互式查询"** —— 学完这阶段你应该能解释为什么同一个 SQL Trino 比 Spark 快 5-20×（小数据时）。

---

## 4.1 Trino 是什么 / 不是什么

**Trino 的前世今生**：
- 起源：Facebook 2012 年造的 **Presto**
- 分裂：2020 年原 Presto 创始人出走，新分支改名 **Trino**（功能更新更快）
- 现状：阿里云 Hologres / AWS Athena / 字节 ByConity 都借鉴它的架构

**Trino 是**：
- **MPP** (Massively Parallel Processing) 查询引擎
- 跨数据源联邦查询：一句 SQL 同时查 Iceberg + MySQL + Kafka
- **亚秒级**响应（小到中等数据集）
- 交互式分析、BI 后端

**Trino 不是**：
- ETL 引擎（写场景弱、容错差）
- 数据存储（自己不存数据，全靠 connector）
- 流处理引擎

---

## 4.2 Spark vs Trino 核心差别

| | Spark | Trino |
|---|---|---|
| 定位 | 批处理框架 | 查询引擎 |
| 启动时间 | 5-30 秒（启 JVM + Driver + Executor） | 已起好的常驻服务，**0 启动时间** |
| 执行模型 | DAG → Stage → Task（要 shuffle 写磁盘） | **Pipeline 全内存执行** |
| 容错 | RDD lineage 重算丢失分区 | 查询失败整个重跑 |
| 内存模型 | 单查询占用 Executor 内存池 | 多查询共享内存，资源调度精细 |
| 适合 | TB 级 ETL、迭代式 ML、容错重要 | 秒级查询、BI、Ad-hoc |
| 生产搭档 | DataWorks 调度 Spark 跑 ETL | Trino 接 Superset 让运营查询 |

**金句**：
> "如果说 Spark 是个**多面手**（什么都能干），Trino 就是个**专才**（只干快速查询，但极致快）。"

---

## 4.3 Trino 的 MPP 架构

```
                        ┌──────────────────┐
                        │   Coordinator    │   ◄─ 解析 SQL、做 plan、调度
                        │   (单点)          │     - 不存数据
                        └────────┬─────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
       ┌──────────┐       ┌──────────┐       ┌──────────┐
       │ Worker 1 │       │ Worker 2 │       │ Worker N │   ◄─ 真正干活
       │ (Stage 1)│       │ (Stage 1)│       │ (Stage 1)│      Pipeline 执行
       │   ↓ pipe │       │   ↓ pipe │       │   ↓ pipe │      数据在内存里流
       │ (Stage 2)│       │ (Stage 2)│       │ (Stage 2)│
       └──────────┘       └──────────┘       └──────────┘
```

**关键区别**：
- Trino 没有 Spark 那种"Stage 边界写磁盘"的硬隔离
- 一个查询的所有 stage **流水线 (pipeline) 执行**，数据从 Stage 1 → 2 → 3 直接在内存里**流过去**
- Shuffle 也是网络流式的，不像 Spark 要 spill 到磁盘等下一阶段
- 这就是**亚秒级响应**的来源

---

## 4.4 Connector 架构（Trino 真正强大的地方）

Trino 自己不存数据，所有数据通过 **Connector** 接入：

```
       ┌─────────────────────────┐
       │     Trino Coordinator   │
       └──────────┬──────────────┘
                  │
   ┌──────────────┼─────────────┬──────────────┐
   ▼              ▼             ▼              ▼
 Iceberg        MySQL         Kafka          Elastic
Connector     Connector    Connector       Connector
   │              │             │              │
   ▼              ▼             ▼              ▼
 MinIO         RDS         Topics         ES Index
```

**联邦查询的威力**：

```sql
-- 一句 SQL 跨数据源 JOIN：Iceberg 里的事实表 JOIN MySQL 里的实时维度
SELECT i.country, COUNT(*) AS pay_count, m.country_name
FROM iceberg.dw.ads_funnel_daily i
JOIN mysql.crm.country_dim m ON i.country = m.code
WHERE i.dt = DATE '2026-05-20'
GROUP BY i.country, m.country_name;
```

阿里云 / AWS 选 Trino / Presto 的核心理由就是这个**联邦查询**能力。

---

## 4.5 本阶段架构

```
┌────────────┐  ┌──────────┐
│ Spark      │  │ Trino    │  ◄─ 同时连同一个 Iceberg Catalog
│ (写 ETL)   │  │ (查询)    │     看到 Spark 写的表
└──────┬─────┘  └────┬─────┘
       └─────┬───────┘
             ▼
    ┌──────────────────────────────┐
    │ Iceberg REST Catalog + MinIO │
    └──────────────────────────────┘
```

**关键认知**：Iceberg 是**引擎中立**的表格式 → Spark 写、Trino 读、Flink 还能写，**多引擎共用一份数据**。这正是湖仓比传统数仓更"开放"的优势。

---

## 4.6 本阶段一个实验

`exp07_trino_vs_spark.py`：在阶段 3 已经写好的 4 张表 (ods/dwd/dws/ads) 上跑 3 类查询：

1. **点查 (Lookup)**：`SELECT * WHERE event_id = X` —— 1 行结果
2. **小聚合 (Small Aggregation)**：`SELECT country, SUM(amount) FROM dws GROUP BY country` —— ~10 行结果
3. **大 JOIN 聚合**：DWD JOIN dim_user JOIN dim_item GROUP BY ... —— exp06 同款

对照：
- Trino 客户端连 8080 端口跑
- Spark 数字直接用 exp06 已经测出来的
- 量化 "同一个 SQL 谁更快"

---

## 4.7 概念检查（动手前）

- [ ] Trino 的 Coordinator 和 Worker 分别干什么？
- [ ] 为什么 Trino 同一个查询比 Spark 快很多？（提示：pipeline + 常驻 + 内存）
- [ ] 什么场景应该用 Spark 而不是 Trino？
- [ ] Connector 架构带来什么好处？
- [ ] Iceberg 表 Spark 写完 Trino 能直接查吗？为什么？

---

## 4.8 实操步骤

```bash
# 启动完整栈：MinIO + REST + Spark + Trino
docker compose -f docker-compose/04-trino.yml up -d --build

# 装 trino python 客户端
source venv/bin/activate
pip install -r requirements.txt   # 加了 trino

# 跑对照实验
python3 jobs/trino/exp07_trino_vs_spark.py

# 可选：交互式 Trino CLI（直接写 SQL 玩）
docker compose -f docker-compose/04-trino.yml exec trino trino
# 然后:
#   trino> SHOW CATALOGS;
#   trino> SHOW SCHEMAS FROM iceberg;
#   trino> SELECT * FROM iceberg.dw.ads_funnel_daily LIMIT 10;
```
