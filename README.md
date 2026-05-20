# bigdata-lakehouse

本地大数据湖仓学习项目 — 电商行为分析场景。

## 总体目标

在 MacBook Air M5 / 24GB 上跑通一套"现代湖仓"全链路，每个组件都伴随**对照实验**和**概念文档**，作为 ai-gateway 之后的第 2 个简历项目，并产出第 12 份实证报告（NL2SQL on Lakehouse）。

## 技术栈

| 层 | 组件 |
|---|---|
| 采集 | Python 埋点模拟器 + Redpanda (Kafka 兼容) |
| 存储 | MinIO (S3) + Parquet |
| 表格式 | Apache Iceberg (REST Catalog) |
| 批处理 | Apache Spark (PySpark) |
| 流处理 | Apache Flink (Flink SQL) |
| 查询 | Trino |
| 调度 | Apache Airflow |
| BI | Apache Superset |
| 上层应用 | ai-gateway `/nl2sql` 接口 |

## 阶段路线

- [ ] 阶段 0：数据建模 — `docs/00-data-model.md`
- [ ] 阶段 1：对象存储 + 列存 — `docs/01-columnar-storage.md`
- [ ] 阶段 2：Iceberg 表格式 — `docs/02-iceberg.md`
- [ ] 阶段 3：Spark 批处理 — `docs/03-spark.md`
- [ ] 阶段 4：Trino 查询引擎 — `docs/04-trino.md`
- [ ] 阶段 5：Flink 流处理 — `docs/05-flink.md`
- [ ] 阶段 6：Airflow 调度 — `docs/06-airflow.md`
- [ ] 阶段 7：Superset + NL2SQL — `experiments/12-nl2sql-on-lakehouse.md`

## 目录

```
docker-compose/   # 分阶段的 compose 文件，按需启停
docs/             # 概念笔记（先于动手）
data-gen/         # 埋点模拟器
jobs/spark/       # PySpark ETL
jobs/flink/       # Flink SQL
airflow/dags/     # 调度 DAG
experiments/      # 对照实验报告
```

## 资源预算

- 同时运行存储 + 批：MinIO + Iceberg REST + Spark + Trino ≈ 5GB
- 同时运行存储 + 流：MinIO + Iceberg REST + Redpanda + Flink ≈ 4GB
- 务必 **按阶段启停**，不要一次全起。
