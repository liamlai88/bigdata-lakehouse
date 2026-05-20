# 实验 3：Spark 批处理 + 性能调优 实证报告

> 日期：2026-05-20
> 机器：MacBook Air M5 / 24GB
> 工具：Apache Spark 3.5.3 (Docker, local[*] 模式) + Iceberg 1.6.1 + MinIO
> 数据：100 万行模拟跨境电商事件 + 10 万用户维度 + 1 万商品维度

---

## 一、核心发现（一句话）

**Spark + Iceberg 跑通 ODS → DWD → DWS → ADS 全链路，量化验证 AQE / 广播 Join / 分区数三大调优手段；其中 AQE 自动 coalesce 小分区是当今 Spark 调优的最大杀手锏。**

---

## 二、实验 5：ETL 全链路（数仓四层）

### 跑通的流水线

```
source.events.parquet (100 万行)
        │
        ▼
  dw.ods_events            ODS  原始落地，按 dt 分区
        │ JOIN dim_user + dim_item (broadcast hint)
        ▼
  dw.dwd_user_action       DWD  清洗 + 维度补全
        │ GROUP BY user × dt
        ▼
  dw.dws_user_daily        DWS  用户日聚合
        │ GROUP BY dt × country × category
        ▼
  dw.ads_funnel_daily      ADS  漏斗看板表
```

### 各层行数对照

| 层 | 行数 | 与上层比 | 含义 |
|---|---|---|---|
| ODS  原始落地 | 1,000,000 | — | 1 行 = 1 事件 |
| DWD  清洗 + 维度补全 | 1,000,000 | 100% | event_id (UUID) 无重复 |
| DWS  按 user × dt 聚合 | 983,640 | 98.4% | ~16,400 user-day 合并 |
| ADS  按 country × category 聚合 | **1,860** | **0.19%** | 看板直接读这层 |

**关键认知**：聚合层级越高、行数越少、单行价值越高。
ADS 一行就是 BI 看板上一个单元格 —— 这就是为什么数仓分层。

### Broadcast Join 验证

DWD 阶段 `dwd JOIN dim_user JOIN dim_item` 用 `F.broadcast()` hint，
Spark explain 输出包含 `BroadcastHashJoin` 字样，确认两张维度表都被广播到所有 Executor，零 shuffle。

---

## 三、实验 6：性能调优对照（同一查询 6 种配置）

### 实验设置

固定查询：DWD JOIN dim_user JOIN dim_item，GROUP BY (country, age_group, brand)，每种配置跑 3 次取最快。

### 结果

| 实验 | 配置 A | 配置 B | 加速比 |
|---|---|---|---|
| **AQE** | OFF: 1,548 ms | ON: **421 ms** | **3.68×** |
| **Broadcast Join** | OFF: 352 ms | ON: **208 ms** | **1.70×** |
| **Shuffle 分区数** | 200: 1,453 ms | 16: **313 ms** | **4.64×** |

### 关键解读

**① AQE (3.68×) ≈ Shuffle 200→16 (4.64×) 不是巧合**

两个数字几乎一样大，因为 AQE 的核心收益就是**自动 coalesce 小分区**：

- 没 AQE 时 `shuffle.partitions=200` → 100 万行分到 200 个 Task，每 Task 才 5,000 行
- 调度开销 > 实际计算开销，大部分时间浪费在 Driver 协调
- AQE 在 shuffle 之后自动合并相邻小分区 → 等效于手动调到 ~16

**面试金句**：
> "AQE 的最大价值是把'shuffle.partitions 怎么调'这个老大难问题自动化了。
> Spark 3.2 之前要工程师反复试参数，Spark 3.2+ 默认开 AQE 后基本不用手调。"

**② Broadcast Join 1.7× 为什么没爆炸式收益**

- dim_user (10 万行 ~5MB) 和 dim_item (1 万行 ~400KB) 本身就很小
- 即使关了广播走 sort-merge join，对小表排序也不慢
- 如果维度表换成 5GB 的用户标签表，差距会变成 10× 以上

**结论**：广播 join 的杠杆效应取决于"维度表多小、事实表多大"。

**③ Shuffle 分区数：本地与生产相反**

- **本地 100 万行**：分区少 (16) 快，调度开销小
- **生产 几十亿行**：分区少会 OOM，要 200~2000 分区让数据均匀
- 调优永远是 "数据量 ÷ 单分区目标大小 (~128MB)" 的平衡

---

## 四、踩坑记录

| 坑 | 现象 | 根因 | 解决 |
|---|---|---|---|
| `Invalid table identifier: ods_events` | DROP TABLE 时报错 | namespace 名 `lakehouse` 跟 catalog 同名，SQL 解析器把 `lakehouse.ods_events` 拆成 catalog + 裸表名 | namespace 改名 `dw`，避免跟 catalog 同名 |
| `Unable to load region from any of the providers` | 写 Iceberg 时 AWS SDK v2 报错 | iceberg-aws-bundle 1.6.1 用 SDK v2，强制要 region | 容器内设环境变量 `AWS_REGION=us-east-1` |
| spark-defaults.conf 改了不生效 | 改完不重 build 镜像没用 | 原 Dockerfile 用 COPY 把 conf 烤进镜像 | 改成 volume mount，config 改完容器重启即可 |

---

## 五、性能调优速查表（学完要记住）

| 优化 | 配置项 | 效果 (本实验) | 适用场景 |
|---|---|---|---|
| AQE | `spark.sql.adaptive.enabled=true` | **3.68×** | 几乎所有场景，Spark 3.2+ 默认开 |
| 分区合并 | `spark.sql.adaptive.coalescePartitions.enabled=true` | 同上 | 跟 AQE 一起 |
| 倾斜优化 | `spark.sql.adaptive.skewJoin.enabled=true` | 本实验未触发 | 大表 JOIN，某 key 行数特别多 |
| 广播 Join | `spark.sql.autoBroadcastJoinThreshold=X` | **1.70×** | 一表 < 几十 MB |
| Shuffle 分区数 | `spark.sql.shuffle.partitions=N` | **4.64×** | 本地 16，生产看数据量 |
| 显式 hint | `df.join(F.broadcast(small), ...)` | 强制广播 | 自动判断不准时 |

---

## 六、阿里云 / 业界映射

| 这里学的 | 阿里云 | AWS | Databricks |
|---|---|---|---|
| Spark 引擎 | EMR Spark / MaxCompute (兼容) | EMR Spark / Glue Spark | Databricks Runtime |
| Iceberg Catalog | DLF | Glue / S3 Tables Catalog | Unity Catalog |
| 调度 ETL | DataWorks | Step Functions / MWAA | Workflows |
| 性能调优 | EMR Spark Tuning | Glue Auto Scaling | Photon (向量化执行) |

**SA 面试关键点**：客户从 Hive 迁 Spark + Iceberg，能省 30~70% 计算成本，因为：
1. Iceberg 元数据级操作秒级完成（Hive 几分钟）
2. Spark AQE 自动调优，免去 Hive 时代手调参数
3. 存算分离 + Spot Instance，闲时机器释放

---

## 七、下一阶段引子

本实验留下的问题：
1. **想交互式跑 SQL 查这些表怎么办？** Spark 启动几秒，不适合 BI 交互
   → 阶段 4 Trino（MPP 引擎，亚秒级响应）
2. **数据来一条就要算，不等 T+1 怎么办？**
   → 阶段 5 Flink 流处理

---

## 八、可复现性

```bash
# 启动
docker compose -f docker-compose/03-spark.yml up -d --build
python3 data-gen/generate_dimensions.py

# 跑 ETL
docker compose -f docker-compose/03-spark.yml exec spark \
  /opt/spark/bin/spark-submit /jobs/exp05_etl_pipeline.py

# 跑性能对照
docker compose -f docker-compose/03-spark.yml exec spark \
  /opt/spark/bin/spark-submit /jobs/exp06_performance_tuning.py
```

---

## 九、结论

✅ Spark + Iceberg 跑通完整数仓四层 ETL，验证了"越上层越小越贵"的设计哲学
✅ AQE 3.68× 加速，本质是自动 coalesce 小分区
✅ Broadcast Join 1.7× 加速，杠杆取决于"维度多小、事实多大"
✅ Shuffle 分区数 4.64× 差异：**本地少分区快，生产多分区均衡**
✅ Spark 调优三件套：AQE + 自动广播 + 合理分区数，覆盖 80% 性能问题
