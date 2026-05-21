"""ETL Step 3: DWD → DWS (按 user × dt 聚合)"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("etl-step3-dws").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("DROP TABLE IF EXISTS dw.dws_user_daily")
spark.sql("""
    CREATE TABLE dw.dws_user_daily (
        user_id STRING, dt DATE, country STRING,
        impression_cnt BIGINT, click_cnt BIGINT, add_cart_cnt BIGINT,
        order_cnt BIGINT, pay_cnt BIGINT,
        pay_amount DOUBLE, session_cnt BIGINT
    ) USING iceberg PARTITIONED BY (dt)
""")

dws = spark.sql("""
    SELECT
        user_id, dt, country,
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
dws.writeTo("dw.dws_user_daily").append()
n = spark.table("dw.dws_user_daily").count()
print(f"✅ dws_user_daily: {n:,} rows")
spark.stop()
