"""
实验 6：Spark 性能对照 (AQE / Broadcast Join / Partition)
==========================================================

固定查询: 跑 ADS 漏斗聚合 (DWD JOIN dim_user JOIN dim_item GROUP BY)
变量:
  A. AQE 开 vs 关
  B. 强制 broadcast join vs 强制 sort-merge join
  C. shuffle 分区数 16 vs 200

跑完会量化看到每个优化的实际加速比。

前置：已经跑过 exp05_etl_pipeline.py，Iceberg 表都建好了。
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_query(spark, label):
    """每次运行的核心查询：DWD + 维度 → 聚合"""
    t0 = time.perf_counter()
    result = (
        spark.table("dw.dwd_user_action")
        .alias("e")
        .join(spark.table("dw.dim_user").alias("u"), "user_id")
        .join(spark.table("dw.dim_item").alias("i"), "item_id")
        .groupBy("e.country", "u.age_group", "i.brand")
        .agg(
            F.sum("amount").alias("gmv"),
            F.countDistinct("e.user_id").alias("uv"),
        )
        .count()  # collect 一个数字，避免 print 大量数据干扰计时
    )
    dt = time.perf_counter() - t0
    print(f"  {label:40s} {dt * 1000:>8.0f} ms  (rows={result})")
    return dt


def make_spark(app_name, configs: dict):
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for k, v in configs.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


print("\n" + "═" * 70)
print("实验 6：性能调优对照")
print("═" * 70)


# ─────────────────────────────────────────────
# A. AQE 开 vs 关
# ─────────────────────────────────────────────
print("\n[实验 A] AQE 开 vs 关 (其他配置一致)")

# 先关 AQE
SparkSession.builder.getOrCreate().stop()  # 关掉之前的
spark = make_spark(
    "aqe-off",
    {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",  # 也关掉广播，逼出 sort-merge
        "spark.sql.shuffle.partitions": "200",
    },
)
t_aqe_off = []
for i in range(3):
    t_aqe_off.append(run_query(spark, f"AQE OFF run-{i + 1}"))
best_aqe_off = min(t_aqe_off)
spark.stop()

spark = make_spark(
    "aqe-on",
    {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1",  # 仍然关广播，公平对比
        "spark.sql.shuffle.partitions": "200",
    },
)
t_aqe_on = []
for i in range(3):
    t_aqe_on.append(run_query(spark, f"AQE ON  run-{i + 1}"))
best_aqe_on = min(t_aqe_on)
spark.stop()

print(f"\n  → AQE OFF 最快: {best_aqe_off * 1000:.0f} ms")
print(f"  → AQE ON  最快: {best_aqe_on * 1000:.0f} ms")
print(f"  → AQE 加速比: {best_aqe_off / best_aqe_on:.2f}x")


# ─────────────────────────────────────────────
# B. Broadcast Join 开 vs 关
# ─────────────────────────────────────────────
print("\n[实验 B] Broadcast Join 开 vs 关 (AQE 都开)")

spark = make_spark(
    "broadcast-off",
    {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1",  # 关广播
        "spark.sql.shuffle.partitions": "16",
    },
)
t_bc_off = min(run_query(spark, f"Broadcast OFF run-{i + 1}") for i in range(3))
spark.stop()

spark = make_spark(
    "broadcast-on",
    {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "20971520",  # 20MB 阈值
        "spark.sql.shuffle.partitions": "16",
    },
)
t_bc_on = min(run_query(spark, f"Broadcast ON  run-{i + 1}") for i in range(3))
spark.stop()

print(f"\n  → Broadcast OFF 最快: {t_bc_off * 1000:.0f} ms")
print(f"  → Broadcast ON  最快: {t_bc_on * 1000:.0f} ms")
print(f"  → 广播 join 加速比: {t_bc_off / t_bc_on:.2f}x")


# ─────────────────────────────────────────────
# C. Shuffle 分区数 16 vs 200
# ─────────────────────────────────────────────
print("\n[实验 C] shuffle 分区数 16 vs 200 (AQE 关，避免被自动合并)")

spark = make_spark(
    "shuffle-200",
    {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.shuffle.partitions": "200",
    },
)
t_p200 = min(run_query(spark, f"shuffle=200 run-{i + 1}") for i in range(3))
spark.stop()

spark = make_spark(
    "shuffle-16",
    {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.shuffle.partitions": "16",
    },
)
t_p16 = min(run_query(spark, f"shuffle=16  run-{i + 1}") for i in range(3))
spark.stop()

print(f"\n  → shuffle=200 最快: {t_p200 * 1000:.0f} ms")
print(f"  → shuffle=16  最快: {t_p16 * 1000:.0f} ms")
print(f"  → 分区数从 200 调到 16 的影响: {t_p200 / t_p16:.2f}x")
print("  （注意：本地小数据下分区数越少越快；生产几十亿行数据反过来）")


# ─────────────────────────────────────────────
# 汇总
# ─────────────────────────────────────────────
print("\n" + "═" * 70)
print("📊 性能调优对照汇总")
print("═" * 70)
print(
    f"  AQE       OFF/ON      = {best_aqe_off * 1000:.0f}ms / {best_aqe_on * 1000:.0f}ms"
    f"     →  {best_aqe_off / best_aqe_on:.2f}x"
)
print(
    f"  Broadcast OFF/ON      = {t_bc_off * 1000:.0f}ms / {t_bc_on * 1000:.0f}ms"
    f"     →  {t_bc_off / t_bc_on:.2f}x"
)
print(
    f"  Shuffle   200/16      = {t_p200 * 1000:.0f}ms / {t_p16 * 1000:.0f}ms"
    f"     →  {t_p200 / t_p16:.2f}x"
)
print(
    "\n关键认知:\n"
    "  - 本地 100 万行规模下，三种优化效果都是 1.5×~5× 量级\n"
    "  - 生产环境（几十亿行 + 真实集群），同样的优化通常 10×~100×\n"
    "  - 这就是为什么 Spark 调优是大数据工程师的核心技能"
)
