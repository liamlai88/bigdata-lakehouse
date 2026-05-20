"""
实验 2：本地 Parquet vs MinIO (S3) Parquet
============================================

把 Parquet 上传到 MinIO，再用 s3:// 路径读取，体会：
  - 对象存储是 HTTP 接口（不是文件系统）
  - 列裁剪 / 谓词下推依然有效（关键结论！）
  - 网络略增加延迟

前置：
  docker compose -f docker-compose/01-storage.yml up -d

运行：
  python3 data-gen/exp02_minio_upload.py
"""

import time
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.fs as pafs

# MinIO 连接信息（来自 01-storage.yml）
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "lakehouse"
S3_KEY = "exp01/events.zstd.parquet"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_PARQUET = PROJECT_ROOT / "data/formats/events.zstd.parquet"

assert LOCAL_PARQUET.exists(), (
    f"找不到 {LOCAL_PARQUET}，请先跑 exp01_format_comparison.py"
)


def timed(label, fn, repeat=3):
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        result = fn()
        dt = time.perf_counter() - t0
        best = min(best, dt)
    print(f"  {label}: {best * 1000:.1f} ms")
    return best, result


# ─────────────────────────────────────────────
# 1. 连 MinIO（S3 兼容）
# ─────────────────────────────────────────────
s3 = pafs.S3FileSystem(
    endpoint_override=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    scheme="http",  # MinIO 本地用 http
)
print(f"✅ 已连 MinIO @ http://{MINIO_ENDPOINT}")


# ─────────────────────────────────────────────
# 2. 上传 Parquet 到 MinIO
# ─────────────────────────────────────────────
print(f"\n→ 上传 {LOCAL_PARQUET.name} → s3://{BUCKET}/{S3_KEY}")
t0 = time.perf_counter()
with (
    open(LOCAL_PARQUET, "rb") as src,
    s3.open_output_stream(f"{BUCKET}/{S3_KEY}") as dst,
):
    dst.write(src.read())
print(f"  上传耗时: {(time.perf_counter() - t0) * 1000:.1f} ms")


# ─────────────────────────────────────────────
# 3. 跑同样三个查询：本地 vs MinIO
# ─────────────────────────────────────────────
print("\n🔬 同一份 Parquet, 本地 vs MinIO\n")

print("[本地盘]")
local_full, _ = timed(
    "  a) 全表扫描",
    lambda: pq.read_table(LOCAL_PARQUET),
)
local_sum, _ = timed(
    "  b) SUM(amount) — 列裁剪",
    lambda: pc.sum(pq.read_table(LOCAL_PARQUET, columns=["amount"])["amount"]).as_py(),
)
local_filter, _ = timed(
    "  c) WHERE country='US' — 谓词下推",
    lambda: (
        pq.read_table(
            LOCAL_PARQUET,
            columns=["country"],
            filters=[("country", "=", "US")],
        ).num_rows
    ),
)

print("\n[MinIO (S3)]")
s3_full, _ = timed(
    "  a) 全表扫描",
    lambda: pq.read_table(f"{BUCKET}/{S3_KEY}", filesystem=s3),
)
s3_sum, _ = timed(
    "  b) SUM(amount) — 列裁剪",
    lambda: pc.sum(
        pq.read_table(f"{BUCKET}/{S3_KEY}", filesystem=s3, columns=["amount"])["amount"]
    ).as_py(),
)
s3_filter, _ = timed(
    "  c) WHERE country='US' — 谓词下推",
    lambda: (
        pq.read_table(
            f"{BUCKET}/{S3_KEY}",
            filesystem=s3,
            columns=["country"],
            filters=[("country", "=", "US")],
        ).num_rows
    ),
)


# ─────────────────────────────────────────────
# 4. 对照
# ─────────────────────────────────────────────
print("\n" + "═" * 70)
print(f"{'查询':30s} {'本地':>12s} {'MinIO':>12s} {'倍率':>10s}")
print("─" * 70)
for label, l, s in [
    ("a) 全表扫描", local_full, s3_full),
    ("b) SUM(amount) 列裁剪", local_sum, s3_sum),
    ("c) WHERE US 谓词下推", local_filter, s3_filter),
]:
    print(f"{label:30s} {l * 1000:>10.1f}ms {s * 1000:>10.1f}ms {s / l:>9.2f}x")
print("═" * 70)
print(
    "\n关键结论检查："
    "\n  1. MinIO 全表扫描比本地慢，但列裁剪 / 谓词下推依然把 IO 砍下来"
    "\n  2. 'b' 和 'c' 的 MinIO 耗时应该远小于 'a'，证明优化在对象存储上一样生效"
    "\n  3. 这就是为什么湖仓敢把数据放 S3：Parquet 的优化不依赖文件系统"
)
