# 阶段 3：Spark 批处理

> 这一阶段是面试核心。读完动手前应该建立 6 个概念：
> Driver / Executor / DAG / Shuffle / AQE / Catalyst

---

## 3.1 为什么 Spark 取代 MapReduce

**MapReduce 时代**（2004-2014）：
- 计算分两个固定阶段：Map → Shuffle → Reduce
- 每个阶段的中间结果**写到 HDFS 磁盘**
- 一个复杂作业拆成 10 个 MapReduce → 写磁盘 10 次 → 极慢

**Spark 革命**（2014+）：
- 中间结果尽量**留在内存**（RDD = Resilient Distributed Dataset）
- 整个作业先构建一张 **DAG**（有向无环图），优化器看全局，决定哪里要 shuffle、哪里能 pipeline
- 一个作业里 100 个算子，只要不强制要 shuffle 就一直在内存里串

**典型加速**：10× ~ 100×（同一个作业）。

---

## 3.2 Spark 执行模型：Driver / Executor

```
                  ┌─────────────────────┐
                  │   Driver (1 个)      │  ◄─ 你的 Python / Scala 主程序在这跑
                  │   - 构建 DAG         │     - 解析 SQL
                  │   - 切 Stage / Task  │     - 调度任务
                  │   - 不存数据         │
                  └──────────┬──────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
       ┌──────────┐   ┌──────────┐   ┌──────────┐
       │Executor 1 │   │Executor 2 │   │Executor N │ ◄─ 真正干活的进程
       │ - 跑 Task │   │ - 跑 Task │   │ - 跑 Task │     - 多核并行
       │ - 缓存数据 │   │ - 缓存数据 │   │ - 缓存数据 │     - 内存计算
       └──────────┘   └──────────┘   └──────────┘
```

**记牢一句话**：
- **Driver** = 大脑，做规划，**单点**
- **Executor** = 手脚，做计算，**多个并行**
- 你的 `df.show()` 看到的数据，是从 Executor**拉回 Driver** 的（所以打印 1 亿行会 OOM）

**本地模式 (local mode)** vs **集群模式 (cluster mode)**：
- `local[*]`：Driver 和 Executor 都在你这一台机器（多线程模拟）—— 学习 / 调试
- `yarn` / `k8s`：Driver 一台、Executor 几十上百台 —— 生产

我们这次跑 `local[*]`，但代码跟生产**完全一样**，这是 Spark 最大的好处。

---

## 3.3 DAG / Stage / Task：作业怎么被拆分

写一句 SQL：
```sql
SELECT country, SUM(amount)
FROM events
WHERE action_type = 'pay'
GROUP BY country
```

Spark 内部把它编译成 **DAG**：

```
┌──────────┐
│ Read     │   读 Iceberg/Parquet
│ (Stage 1)│
└────┬─────┘
     │ (无 shuffle，pipeline)
     ▼
┌──────────┐
│ Filter   │   WHERE action_type='pay'
│ (Stage 1)│
└────┬─────┘
     │ (要 GROUP BY，必须 shuffle！)
     ▼
═══════════════════════ Stage 1 / 2 边界
     │
     ▼
┌──────────┐
│ Aggregate│   SUM by country
│ (Stage 2)│
└──────────┘
```

**关键规则**：
- **Stage 边界 = Shuffle 边界**
- 一个 Stage 内的算子 **pipeline 执行**（数据在内存流过，不落盘）
- Stage 之间要 **shuffle**：数据按 key 重分布到不同 Executor 上

**Task** = Stage × 分区数。一个 100 分区的 Stage 有 100 个 Task，并行执行。

---

## 3.4 Shuffle：性能头号杀手

什么是 Shuffle：**Executor 之间通过网络重新分配数据**。

比如 `GROUP BY country`：
- US 的数据原本散落在所有 Executor 上
- Shuffle 后所有 US 都要汇到同一个 Executor
- 涉及**网络传输 + 磁盘读写**（中间结果太大放不下内存就 spill 到磁盘）

**触发 Shuffle 的算子**：
- `groupBy`, `join`, `distinct`, `orderBy`, `repartition`
- 不触发的：`filter`, `map`, `select`, `withColumn`

**怎么减少 Shuffle**：
1. **广播 Join (Broadcast Join)**：小表广播到所有 Executor，大表本地 JOIN，零 Shuffle
2. **分区裁剪 + 列裁剪**：从源头少读数据
3. **预聚合**：在 shuffle 前先本地聚合（这是 Spark 默认就做的，但要会看 explain 验证）

---

## 3.5 AQE (Adaptive Query Execution)：Spark 3.0+ 杀手锏

传统 Spark：执行前定好计划，**跑起来不调整**。
AQE：执行中**实时看数据**，**动态调计划**：

| AQE 能力 | 干什么 |
|---|---|
| 动态合并小分区 | shuffle 后发现很多分区很小，合成大分区减少 Task 数 |
| 动态切换 Join 策略 | 跑到 JOIN 时发现一表很小，**临时改成广播 JOIN** |
| 动态处理倾斜 | 某分区特别大（比如 US 用户超多），自动拆成多个小 Task |

**AQE 默认 Spark 3.2+ 开启**，本实验会做"开 vs 关"的对照实验，亲眼看出差异。

---

## 3.6 Catalyst 优化器：SQL → 物理计划

写一句 SQL，Catalyst 内部做四步：

```
SQL 文本
   │
   ▼
Unresolved Logical Plan  ← 还没绑表名
   │
   ▼
Logical Plan             ← 绑了表名，知道字段类型
   │ (规则优化：谓词下推、列裁剪、常量折叠)
   ▼
Optimized Logical Plan
   │ (生成多个物理执行方案)
   ▼
Physical Plans          ← 选 cost 最低的
   │
   ▼
Selected Physical Plan  ← 真正执行
```

**学习要点**：你可以用 `df.explain(True)` 看每一步，**看懂 explain 是 Spark 高手分水岭**。

---

## 3.7 本阶段架构

```
┌───────────────┐
│ Docker Spark  │  ◄─ 跑 PySpark (Python 3.14 跟最新 PySpark 不兼容)
│ local[*] 模式  │
└──────┬────────┘
       │ (Iceberg 表读写)
       ▼
┌──────────────────────────────┐
│ Iceberg REST Catalog + MinIO │  ◄─ 阶段 2 已起好的
└──────────────────────────────┘

数据流（ETL 四层）：
  source.events.parquet (本地)
        │ exp05 Step 1
        ▼
  lakehouse.ods_events       ◄─ ODS: 原始落地，按 dt 分区
        │ exp05 Step 2: 解析 / 清洗 / 加字段
        ▼
  lakehouse.dwd_user_action  ◄─ DWD: 干净的明细
        │ exp05 Step 3: 按用户/天聚合
        ▼
  lakehouse.dws_user_daily   ◄─ DWS: 半成品聚合
        │ exp05 Step 4: 按国家/品类做漏斗
        ▼
  lakehouse.ads_funnel_daily ◄─ ADS: 看板直接读
```

外加两张维度表：`lakehouse.dim_user` / `lakehouse.dim_item`（合成数据）。

---

## 3.8 本阶段三个实验

| 实验 | 目标 |
|---|---|
| `exp05_etl_pipeline.py` | 跑通 ODS → DWD → DWS → ADS 全链路 |
| `exp06_performance_tuning.py` | AQE 开 / 关、广播 join、分区数对照 |

中间一份维度表生成器：`data-gen/generate_dimensions.py`

---

## 3.9 概念检查清单（动手前）

- [ ] Driver 和 Executor 谁负责真正算数据？
- [ ] Stage 边界由什么决定？
- [ ] 哪些算子会触发 Shuffle？
- [ ] Broadcast Join 为什么快？什么时候适用？
- [ ] AQE 解决了传统 Spark 优化器的什么短板？
- [ ] `df.explain(True)` 输出哪 4 个计划？

---

## 3.10 实操步骤

```bash
# 1. 起 Spark + MinIO + REST Catalog
docker compose -f docker-compose/03-spark.yml up -d
# 第一次拉镜像 + 下 Iceberg jar 包，需要 1-2 分钟

# 2. 生成维度数据
source venv/bin/activate
python3 data-gen/generate_dimensions.py

# 3. 跑 ETL 全链路
docker compose -f docker-compose/03-spark.yml exec spark \
  /opt/spark/bin/spark-submit /jobs/exp05_etl_pipeline.py

# 4. 跑性能对照
docker compose -f docker-compose/03-spark.yml exec spark \
  /opt/spark/bin/spark-submit /jobs/exp06_performance_tuning.py
```
