"""
实验 7：Trino vs Spark 同 SQL 对照
===================================

3 类查询，每个跑 5 次取最快:
  Q1. 点查        — SELECT * WHERE event_id = X
  Q2. 小聚合      — GROUP BY country
  Q3. 大 JOIN 聚合 — DWD JOIN dim_user JOIN dim_item GROUP BY (跟 exp06 同款)

Spark 数字直接用 exp06 已经测出来的 (Broadcast ON 配置)，也允许你重测。

前置:
  docker compose -f docker-compose/04-trino.yml up -d --build
  source venv/bin/activate
  pip install -r requirements.txt   # 含 trino 客户端
"""

import time
import trino

# ─────────────────────────────────────────────
# Trino 连接（HTTP，无认证）
# ─────────────────────────────────────────────
conn = trino.dbapi.connect(
    host="localhost",
    port=8080,
    user="admin",
    catalog="iceberg",
    schema="dw",
)
cur = conn.cursor()
print("✅ Trino 连接成功 @ http://localhost:8080")


def run(sql, label, repeat=5):
    """跑 SQL repeat 次取最快"""
    best = float("inf")
    rows = None
    for _ in range(repeat):
        t0 = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        dt = time.perf_counter() - t0
        if dt < best:
            best = dt
    print(f"  {label:50s} {best * 1000:>8.0f} ms   (rows={len(rows)})")
    return best, rows


# ─────────────────────────────────────────────
# 先抓一个真实存在的 event_id 给 Q1 用
# ─────────────────────────────────────────────
cur.execute("SELECT event_id FROM dw.ods_events LIMIT 1")
sample_event_id = cur.fetchone()[0]
print(f"📌 用真实 event_id 做点查: {sample_event_id}")

print("\n" + "═" * 75)
print("🔬 Trino 跑 3 类查询")
print("═" * 75)

# Q1. 点查
t_q1, _ = run(
    f"SELECT * FROM dw.ods_events WHERE event_id = '{sample_event_id}'",
    "Q1. 点查 (event_id 单行)",
)

# Q2. 小聚合
t_q2, _ = run(
    """
    SELECT country, SUM(pay_amount) AS gmv, SUM(pay_cnt) AS pay_cnt
    FROM dw.dws_user_daily
    GROUP BY country
    """,
    "Q2. 小聚合 (按 country GROUP BY)",
)

# Q3. 大 JOIN 聚合
t_q3, _ = run(
    """
    SELECT
        e.country,
        u.age_group,
        i.brand,
        SUM(e.amount) AS gmv,
        COUNT(DISTINCT e.user_id) AS uv
    FROM dw.dwd_user_action e
    JOIN dw.dim_user u ON e.user_id = u.user_id
    JOIN dw.dim_item i ON e.item_id = i.item_id
    GROUP BY e.country, u.age_group, i.brand
    """,
    "Q3. 大 JOIN 聚合 (DWD JOIN 双维度 GROUP BY)",
)

# ─────────────────────────────────────────────
# 跟 Spark exp06 的数字对比
# Spark exp06 的 Broadcast ON 数字 = 208ms
# 这里贴个对照表（Spark 数字以你 exp06 输出为准，下面是示例值）
# ─────────────────────────────────────────────
SPARK_BENCH = {
    "Q1. 点查": None,  # exp06 没测这个，留空
    "Q2. 小聚合": None,  # 同上
    "Q3. 大 JOIN 聚合": 0.208,  # exp06 Broadcast ON 实测
}

print("\n" + "═" * 75)
print("📊 Trino vs Spark 对照")
print("═" * 75)
print(f"{'查询':35s} {'Trino':>12s} {'Spark':>12s} {'加速比':>12s}")
print("-" * 75)
for label, t_trino in [
    ("Q1. 点查 (单行)", t_q1),
    ("Q2. 小聚合 GROUP BY country", t_q2),
    ("Q3. 大 JOIN 聚合", t_q3),
]:
    spark_t = SPARK_BENCH.get("Q3. 大 JOIN 聚合") if "Q3" in label else None
    if spark_t:
        speedup = f"{spark_t / t_trino:.1f}×"
        print(
            f"{label:35s} {t_trino * 1000:>10.0f}ms {spark_t * 1000:>10.0f}ms {speedup:>12s}"
        )
    else:
        print(f"{label:35s} {t_trino * 1000:>10.0f}ms {'n/a':>12s} {'n/a':>12s}")

print("\n💡 关键认知:")
print("  - Trino 没有 SparkSession 启动开销，常驻服务，第一次查询就快")
print("  - Trino pipeline 全内存执行，shuffle 也是网络流式")
print("  - 但 ETL 写场景 / 容错要求高的场景，还是要 Spark")
print("  - 生产分工: Spark 跑凌晨批 ETL → Trino 白天给运营/BI 用")

conn.close()
