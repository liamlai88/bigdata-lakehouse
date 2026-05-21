"""ETL Step 1: 源 Parquet → ODS Iceberg 表 (按 dt 分区)"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("etl-step1-ods").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS dw")
spark.sql("DROP TABLE IF EXISTS dw.ods_events")
spark.sql("""
    CREATE TABLE dw.ods_events (
        event_id STRING, user_id STRING, item_id STRING,
        action_type STRING, event_ts BIGINT, session_id STRING,
        amount DOUBLE, country STRING, device STRING,
        category STRING, channel STRING, dt DATE
    ) USING iceberg PARTITIONED BY (dt)
""")

raw = spark.read.parquet("/data/formats/events.zstd.parquet")
ods = raw.withColumn("dt", F.to_date(F.from_unixtime(F.col("event_ts") / 1000)))
ods.writeTo("dw.ods_events").append()
n = spark.table("dw.ods_events").count()
print(f"✅ ods_events: {n:,} rows")
spark.stop()
