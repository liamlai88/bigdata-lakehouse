"""ETL Step 2: ODS + dim_user + dim_item → DWD"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("etl-step2-dwd").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 维度表如果不存在就重建
spark.sql("DROP TABLE IF EXISTS dw.dim_user")
spark.read.parquet("/data/dims/dim_user.parquet").writeTo("dw.dim_user").using(
    "iceberg"
).create()

spark.sql("DROP TABLE IF EXISTS dw.dim_item")
spark.read.parquet("/data/dims/dim_item.parquet").writeTo("dw.dim_item").using(
    "iceberg"
).create()

spark.sql("DROP TABLE IF EXISTS dw.dwd_user_action")
spark.sql("""
    CREATE TABLE dw.dwd_user_action (
        event_id STRING, user_id STRING, item_id STRING,
        action_type STRING, event_ts BIGINT, session_id STRING,
        amount DOUBLE, country STRING, device STRING,
        item_category STRING, item_brand STRING,
        user_channel STRING, age_group STRING, dt DATE
    ) USING iceberg PARTITIONED BY (dt)
""")

dwd = (
    spark.table("dw.ods_events")
    .alias("e")
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
dwd.writeTo("dw.dwd_user_action").append()
n = spark.table("dw.dwd_user_action").count()
print(f"✅ dwd_user_action: {n:,} rows")
spark.stop()
