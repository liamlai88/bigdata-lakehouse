-- ============================================================
-- 实验 9: Watermark 对乱序数据的修正效果
-- ============================================================
-- 跑 2 个并行作业，都消费同样的 Kafka topic：
--   - Job A: 用 Processing Time → 迟到事件归入 ❌ 错误窗口
--   - Job B: 用 Event Time + Watermark → 迟到事件归入 ✅ 正确窗口
-- 然后查 Trino，对比两张结果表
--
-- 前置: 用 `event_producer.py --mode unordered` 灌不有序的数据
-- ============================================================

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

-- ─────────────────────────────────────────────
-- 共用 Kafka source
-- ─────────────────────────────────────────────
CREATE TABLE source_events (
  event_id    STRING,
  user_id     STRING,
  country     STRING,
  event_ts    BIGINT,
  -- Event Time + Watermark
  ts AS TO_TIMESTAMP_LTZ(event_ts, 3),
  -- 允许 30 秒迟到（因为 producer 故意制造 10-30 秒乱序）
  WATERMARK FOR ts AS ts - INTERVAL '30' SECOND,
  -- Processing Time
  proc_ts AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'events',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-exp09',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- ─────────────────────────────────────────────
-- Iceberg sink 表
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

CREATE DATABASE IF NOT EXISTS iceberg.dw;

DROP TABLE IF EXISTS iceberg.dw.rt_pv_by_proctime;
CREATE TABLE iceberg.dw.rt_pv_by_proctime (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  country      STRING,
  pv           BIGINT
);

DROP TABLE IF EXISTS iceberg.dw.rt_pv_by_eventtime;
CREATE TABLE iceberg.dw.rt_pv_by_eventtime (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  country      STRING,
  pv           BIGINT
);

-- ─────────────────────────────────────────────
-- Job A: Processing Time (错误归窗)
-- ─────────────────────────────────────────────
INSERT INTO iceberg.dw.rt_pv_by_proctime
SELECT
  TUMBLE_START(proc_ts, INTERVAL '1' MINUTE),
  TUMBLE_END(proc_ts, INTERVAL '1' MINUTE),
  country,
  COUNT(*)
FROM source_events
GROUP BY TUMBLE(proc_ts, INTERVAL '1' MINUTE), country;

-- ─────────────────────────────────────────────
-- Job B: Event Time + Watermark (正确归窗)
-- ─────────────────────────────────────────────
INSERT INTO iceberg.dw.rt_pv_by_eventtime
SELECT
  TUMBLE_START(ts, INTERVAL '1' MINUTE),
  TUMBLE_END(ts, INTERVAL '1' MINUTE),
  country,
  COUNT(*)
FROM source_events
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), country;

-- ─────────────────────────────────────────────
-- 跑完后用 Trino 对比两张表（在 Trino CLI 里执行）:
--
--   SELECT 'proctime' AS by_what, window_start, country, pv
--   FROM iceberg.dw.rt_pv_by_proctime
--   WHERE window_start >= NOW() - INTERVAL '10' MINUTE
--   UNION ALL
--   SELECT 'eventtime', window_start, country, pv
--   FROM iceberg.dw.rt_pv_by_eventtime
--   WHERE window_start >= NOW() - INTERVAL '10' MINUTE
--   ORDER BY country, window_start, by_what;
--
-- 关键观察: 同一个 country 同一个 window_start, 两张表 pv 不一样
-- ─────────────────────────────────────────────
