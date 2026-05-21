"""ETL Step 4: DWD → ADS (按 country × category × dt 漏斗)"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("etl-step4-ads").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("DROP TABLE IF EXISTS dw.ads_funnel_daily")
spark.sql("""
    CREATE TABLE dw.ads_funnel_daily (
        dt DATE, country STRING, category STRING,
        impression_uv BIGINT, click_uv BIGINT, add_cart_uv BIGINT,
        order_uv BIGINT, pay_uv BIGINT, gmv DOUBLE
    ) USING iceberg PARTITIONED BY (dt)
""")

ads = spark.sql("""
    SELECT
        dt, country, item_category AS category,
        COUNT(DISTINCT CASE WHEN action_type='impression' THEN user_id END) AS impression_uv,
        COUNT(DISTINCT CASE WHEN action_type='click'      THEN user_id END) AS click_uv,
        COUNT(DISTINCT CASE WHEN action_type='add_cart'   THEN user_id END) AS add_cart_uv,
        COUNT(DISTINCT CASE WHEN action_type='order'      THEN user_id END) AS order_uv,
        COUNT(DISTINCT CASE WHEN action_type='pay'        THEN user_id END) AS pay_uv,
        COALESCE(SUM(CASE WHEN action_type='pay' THEN amount END), 0)       AS gmv
    FROM dw.dwd_user_action
    GROUP BY dt, country, item_category
""")
ads.writeTo("dw.ads_funnel_daily").append()
n = spark.table("dw.ads_funnel_daily").count()
print(f"✅ ads_funnel_daily: {n:,} rows")
spark.stop()
