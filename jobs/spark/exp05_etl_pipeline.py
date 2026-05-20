"""
实验 5：Spark ODS → DWD → DWS → ADS 全链路
==============================================

跑完你会理解:
  1. Spark + Iceberg 的标准 ETL 流水线长什么样
  2. 数仓四层每一层在做什么转换
  3. 分区表怎么建、widely used 的 broadcast join 怎么写

注意：
  - 本脚本设计为可重复运行（每次先 DROP 再建）
  - 跑完一次后访问 http://localhost:4040 看 Spark UI

前置：
  docker compose -f docker-compose/03-spark.yml up -d --build
  python3 data-gen/generate_dimensions.py
  # 容器内挂载: /jobs (代码) /data (数据)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time


# ─────────────────────────────────────────────
# 0. 初始化 SparkSession
#    catalog/MinIO 配置已经在 spark-defaults.conf 里了，这里只是命名 app
# ─────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("exp05-etl-pipeline").master("local[*]").getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("Spark Session ready. App ID:", spark.sparkContext.applicationId)
print("Spark UI:", spark.sparkContext.uiWebUrl)
print("Default catalog:", spark.conf.get("spark.sql.defaultCatalog"))
print("=" * 70)


def section(title):
    print(f"\n{'═' * 70}\n  {title}\n{'═' * 70}")


def timed(label, fn):
    t0 = time.perf_counter()
    result = fn()
    print(f"  ⏱  {label}: {time.perf_counter() - t0:.2f}s")
    return result


# ─────────────────────────────────────────────
# 1. ODS 层：加载原始 Parquet → Iceberg ods_events
#    生产里这层是 Kafka → Spark Structured Streaming 落地
#    这里我们直接从阶段 1 的 Parquet 加载
# ─────────────────────────────────────────────
section("① ODS 层：原始数据落地为 Iceberg 表 (按 dt 分区)")

# 先确保 namespace 存在 (catalog=lakehouse, namespace=dw)
# 注意：namespace 千万不要起成跟 catalog 同名，否则 SQL 解析器会把
# `lakehouse.xxx` 拆成 catalog=lakehouse + 表=xxx (没 namespace)，Iceberg 报错
spark.sql("CREATE NAMESPACE IF NOT EXISTS dw")

# 重置实验：删了重建
spark.sql("DROP TABLE IF EXISTS dw.ods_events")
spark.sql("DROP TABLE IF EXISTS dw.dwd_user_action")
spark.sql("DROP TABLE IF EXISTS dw.dws_user_daily")
spark.sql("DROP TABLE IF EXISTS dw.ads_funnel_daily")
spark.sql("DROP TABLE IF EXISTS dw.dim_user")
spark.sql("DROP TABLE IF EXISTS dw.dim_item")

raw = spark.read.parquet("/data/formats/events.zstd.parquet")
print(f"  读 source Parquet：{raw.count():,} 行")

# 从 event_ts (ms) 派生分区字段 dt
ods = raw.withColumn("dt", F.to_date(F.from_unixtime(F.col("event_ts") / 1000)))

# 用 Iceberg DDL 建表，按 dt 分区
spark.sql("""
    CREATE TABLE dw.ods_events (
        event_id     STRING,
        user_id      STRING,
        item_id      STRING,
        action_type  STRING,
        event_ts     BIGINT,
        session_id   STRING,
        amount       DOUBLE,
        country      STRING,
        device       STRING,
        category     STRING,
        channel      STRING,
        dt           DATE
    ) USING iceberg
    PARTITIONED BY (dt)
""")

timed(
    "写入 ods_events",
    lambda: ods.writeTo("dw.ods_events").append(),
)
ods_count = spark.table("dw.ods_events").count()
print(f"  ✅ ods_events 行数: {ods_count:,}")
print("  分区情况:")
spark.sql(
    "SELECT dt, COUNT(*) AS cnt FROM dw.ods_events GROUP BY dt ORDER BY dt LIMIT 5"
).show()


# ─────────────────────────────────────────────
# 2. 维度表：dim_user / dim_item
#    把 Parquet 文件加载到 Iceberg
# ─────────────────────────────────────────────
section("② 维度表：加载 dim_user / dim_item")

dim_user_df = spark.read.parquet("/data/dims/dim_user.parquet")
dim_user_df.writeTo("dw.dim_user").using("iceberg").create()
print(f"  ✅ dim_user: {spark.table('dw.dim_user').count():,} 行")

dim_item_df = spark.read.parquet("/data/dims/dim_item.parquet")
dim_item_df.writeTo("dw.dim_item").using("iceberg").create()
print(f"  ✅ dim_item: {spark.table('dw.dim_item').count():,} 行")


# ─────────────────────────────────────────────
# 3. DWD 层：清洗 + 补维度
#    - 过滤无效记录 (user_id 为空)
#    - 去重 (按 event_id)
#    - 这里 JOIN 用 broadcast hint (维度表小)
# ─────────────────────────────────────────────
section("③ DWD 层：清洗 + 补维度 (broadcast join)")

dwd = (
    spark.table("dw.ods_events")
    .alias("e")
    # 用 broadcast hint 强制广播 dim_user (~5MB)
    .join(F.broadcast(spark.table("dw.dim_user").alias("u")), "user_id", "left")
    .join(F.broadcast(spark.table("dw.dim_item").alias("i")), "item_id", "left")
    .filter(F.col("e.user_id").isNotNull())
    .select(
        "event_id",
        "user_id",
        "item_id",
        "action_type",
        "event_ts",
        "session_id",
        "amount",
        "e.country",
        "device",
        F.col("i.category").alias("item_category"),
        F.col("i.brand").alias("item_brand"),
        F.col("u.channel").alias("user_channel"),
        F.col("u.age_group"),
        "dt",
    )
    .dropDuplicates(["event_id"])
)

# 看一眼物理计划，验证 broadcast 生效
print("\n📋 DWD JOIN 的物理计划（找 BroadcastHashJoin）:")
dwd.explain(False)

spark.sql("""
    CREATE TABLE dw.dwd_user_action (
        event_id      STRING,
        user_id       STRING,
        item_id       STRING,
        action_type   STRING,
        event_ts      BIGINT,
        session_id    STRING,
        amount        DOUBLE,
        country       STRING,
        device        STRING,
        item_category STRING,
        item_brand    STRING,
        user_channel  STRING,
        age_group     STRING,
        dt            DATE
    ) USING iceberg
    PARTITIONED BY (dt)
""")

timed(
    "写入 dwd_user_action",
    lambda: dwd.writeTo("dw.dwd_user_action").append(),
)
dwd_count = spark.table("dw.dwd_user_action").count()
print(f"  ✅ dwd_user_action 行数: {dwd_count:,}")


# ─────────────────────────────────────────────
# 4. DWS 层：用户日聚合
# ─────────────────────────────────────────────
section("④ DWS 层：按 user × dt 聚合")

dws_user_daily = spark.sql("""
    SELECT
        user_id,
        dt,
        country,
        SUM(CASE WHEN action_type='impression' THEN 1 ELSE 0 END) AS impression_cnt,
        SUM(CASE WHEN action_type='click'      THEN 1 ELSE 0 END) AS click_cnt,
        SUM(CASE WHEN action_type='add_cart'   THEN 1 ELSE 0 END) AS add_cart_cnt,
        SUM(CASE WHEN action_type='order'      THEN 1 ELSE 0 END) AS order_cnt,
        SUM(CASE WHEN action_type='pay'        THEN 1 ELSE 0 END) AS pay_cnt,
        COALESCE(SUM(amount), 0)                                  AS pay_amount,
        COUNT(DISTINCT session_id)                                AS session_cnt
    FROM dw.dwd_user_action
    GROUP BY user_id, dt, country
""")

spark.sql("""
    CREATE TABLE dw.dws_user_daily (
        user_id        STRING,
        dt             DATE,
        country        STRING,
        impression_cnt BIGINT,
        click_cnt      BIGINT,
        add_cart_cnt   BIGINT,
        order_cnt      BIGINT,
        pay_cnt        BIGINT,
        pay_amount     DOUBLE,
        session_cnt    BIGINT
    ) USING iceberg
    PARTITIONED BY (dt)
""")

timed(
    "写入 dws_user_daily",
    lambda: dws_user_daily.writeTo("dw.dws_user_daily").append(),
)
dws_count = spark.table("dw.dws_user_daily").count()
print(f"  ✅ dws_user_daily 行数: {dws_count:,}")


# ─────────────────────────────────────────────
# 5. ADS 层：漏斗看板
# ─────────────────────────────────────────────
section("⑤ ADS 层：按 dt × country × category 算漏斗 UV + GMV")

ads_funnel = spark.sql("""
    SELECT
        dt,
        country,
        item_category AS category,
        COUNT(DISTINCT CASE WHEN action_type='impression' THEN user_id END) AS impression_uv,
        COUNT(DISTINCT CASE WHEN action_type='click'      THEN user_id END) AS click_uv,
        COUNT(DISTINCT CASE WHEN action_type='add_cart'   THEN user_id END) AS add_cart_uv,
        COUNT(DISTINCT CASE WHEN action_type='order'      THEN user_id END) AS order_uv,
        COUNT(DISTINCT CASE WHEN action_type='pay'        THEN user_id END) AS pay_uv,
        COALESCE(SUM(CASE WHEN action_type='pay' THEN amount END), 0)       AS gmv
    FROM dw.dwd_user_action
    GROUP BY dt, country, item_category
""")

spark.sql("""
    CREATE TABLE dw.ads_funnel_daily (
        dt            DATE,
        country       STRING,
        category      STRING,
        impression_uv BIGINT,
        click_uv      BIGINT,
        add_cart_uv   BIGINT,
        order_uv      BIGINT,
        pay_uv        BIGINT,
        gmv           DOUBLE
    ) USING iceberg
    PARTITIONED BY (dt)
""")

timed(
    "写入 ads_funnel_daily",
    lambda: ads_funnel.writeTo("dw.ads_funnel_daily").append(),
)
ads_count = spark.table("dw.ads_funnel_daily").count()
print(f"  ✅ ads_funnel_daily 行数: {ads_count:,}")


# ─────────────────────────────────────────────
# 6. 一份漏斗报表预览
# ─────────────────────────────────────────────
section("⑥ 漏斗看板预览 (近 3 天 × 各国)")

spark.sql("""
    SELECT
        dt, country,
        SUM(impression_uv) AS imp_uv,
        SUM(click_uv)      AS clk_uv,
        SUM(pay_uv)        AS pay_uv,
        ROUND(SUM(pay_uv) * 100.0 / NULLIF(SUM(impression_uv), 0), 2)
          AS imp_to_pay_pct,
        ROUND(SUM(gmv), 2) AS gmv
    FROM dw.ads_funnel_daily
    GROUP BY dt, country
    ORDER BY dt DESC, gmv DESC
    LIMIT 15
""").show(truncate=False)


# ─────────────────────────────────────────────
# 7. 各层行数对照
# ─────────────────────────────────────────────
section("⑦ 数仓四层行数对照")

rows = [
    ("ODS  原始落地", ods_count),
    ("DWD  清洗 + 维度补全", dwd_count),
    ("DWS  按 user×dt 聚合", dws_count),
    ("ADS  按 country×category 聚合", ads_count),
]
print(f"  {'层':30s}  行数")
print("  " + "─" * 50)
for label, n in rows:
    print(f"  {label:30s}  {n:>12,}")

print(
    "\n💡 注意聚合层级越往上行数越少，但每行价值越高。"
    "\n   ADS 一行 = 看板上一个单元格，运营直接读。"
)

spark.stop()
print("\n✅ exp05 完成")
