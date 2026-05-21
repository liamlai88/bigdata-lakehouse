-- ============================================================
-- 实验 8: 流式 PV / UV 入湖
-- ============================================================
-- 1. 注册 Kafka source 表 (Flink SQL 用 'kafka' connector 直接读 topic)
-- 2. 注册 Iceberg catalog 和 sink 表
-- 3. INSERT INTO sink SELECT FROM source 用 5 分钟滚动窗口聚合
-- ============================================================

-- 让 SQL Client 输出更友好
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

-- ─────────────────────────────────────────────
-- ① Source: 从 Redpanda topic 'events' 读
-- ─────────────────────────────────────────────
CREATE TABLE source_events (
  event_id    STRING,
  user_id     STRING,
  item_id     STRING,
  action_type STRING,
  event_ts    BIGINT,
  country     STRING,
  device      STRING,
  amount      DOUBLE,
  -- 把 event_ts 转成 Flink 的 TIMESTAMP_LTZ 类型 + watermark 配置
  ts AS TO_TIMESTAMP_LTZ(event_ts, 3),
  -- 允许 5 秒迟到 (实验 8 因为 ordered，几乎不会触发)
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'events',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-exp08',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- ─────────────────────────────────────────────
-- ② Sink: 写到 Iceberg
-- ─────────────────────────────────────────────
CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'minioadmin',
  's3.secret-access-key' = 'minioadmin',
  's3.region' = 'us-east-1'
);

USE CATALOG iceberg;
CREATE DATABASE IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.rt_pv_1min;
CREATE TABLE dw.rt_pv_1min (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  country       STRING,
  pv            BIGINT,
  uv            BIGINT
);

USE CATALOG default_catalog;

-- ─────────────────────────────────────────────
-- ③ 5 分钟滚动窗口聚合 → 入湖
-- ─────────────────────────────────────────────
INSERT INTO iceberg.dw.rt_pv_1min
SELECT
  TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start,
  TUMBLE_END(ts, INTERVAL '1' MINUTE)   AS window_end,
  country,
  COUNT(*)                              AS pv,
  COUNT(DISTINCT user_id)               AS uv
FROM source_events
GROUP BY
  TUMBLE(ts, INTERVAL '1' MINUTE),
  country;
