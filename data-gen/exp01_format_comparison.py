"""
实验 1：CSV vs Parquet 全方位对照
==================================

把 events.jsonl 转成 4 种格式：
  1. CSV
  2. CSV.gz
  3. Parquet (Snappy 默认压缩)
  4. Parquet (ZSTD 高压缩)

然后对每种格式跑 3 类查询，记录耗时：
  a) 全表扫描     ←  读所有数据
  b) 单列汇总     ←  SELECT SUM(amount) — 只用 1 列
  c) 带过滤汇总   ←  WHERE country='US' — 谓词下推

最后打一张对照表，并写入 experiments/01-columnar-storage-results.md 草稿
"""

import time
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = PROJECT_ROOT / "data/raw/events.jsonl"
OUT_DIR = PROJECT_ROOT / "data/formats"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def human_size(path: Path) -> str:
    size = path.stat().st_size
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def timed(label, fn):
    """跑 fn() 三次取最快，避免冷启动干扰"""
    best = float("inf")
    result = None
    for _ in range(3):
        t0 = time.perf_counter()
        result = fn()
        dt = time.perf_counter() - t0
        best = min(best, dt)
    print(f"    {label}: {best * 1000:.1f} ms")
    return best, result


# ─────────────────────────────────────────────
# 0. 读 JSONL → Arrow Table（内存表，后面写各种格式）
# ─────────────────────────────────────────────
print(f"→ 读取 {RAW_PATH}")
t0 = time.perf_counter()
df = pd.read_json(RAW_PATH, lines=True)
print(
    f"  行数: {len(df):,}  内存占用: {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB"
)
print(f"  读 JSONL 耗时: {time.perf_counter() - t0:.1f}s")
table = pa.Table.from_pandas(df)


# ─────────────────────────────────────────────
# 1. 写出 4 种格式
# ─────────────────────────────────────────────
csv_path = OUT_DIR / "events.csv"
csv_gz_path = OUT_DIR / "events.csv.gz"
pq_snappy_path = OUT_DIR / "events.snappy.parquet"
pq_zstd_path = OUT_DIR / "events.zstd.parquet"

print("\n→ 写 CSV")
df.to_csv(csv_path, index=False)

print("→ 写 CSV.gz")
df.to_csv(csv_gz_path, index=False, compression="gzip")

print("→ 写 Parquet (Snappy)")
pq.write_table(table, pq_snappy_path, compression="snappy")

print("→ 写 Parquet (ZSTD)")
pq.write_table(table, pq_zstd_path, compression="zstd")

print("\n📦 文件大小:")
sizes = {}
for label, p in [
    ("CSV", csv_path),
    ("CSV.gz", csv_gz_path),
    ("Parquet+Snappy", pq_snappy_path),
    ("Parquet+ZSTD", pq_zstd_path),
]:
    sizes[label] = human_size(p)
    print(f"  {label:18s} {sizes[label]}")


# ─────────────────────────────────────────────
# 2. 三类查询基准测试
# ─────────────────────────────────────────────
results = {fmt: {} for fmt in sizes}

print("\n🔬 查询基准测试（每个查询跑 3 次取最快）\n")

# ===== CSV =====
print("[CSV]")
results["CSV"]["full_scan"], _ = timed(
    "  a) 全表扫描",
    lambda: pd.read_csv(csv_path),
)
results["CSV"]["sum_amount"], _ = timed(
    "  b) SUM(amount)",
    lambda: pd.read_csv(csv_path, usecols=["amount"])["amount"].sum(),
)
results["CSV"]["filter_us"], _ = timed(
    "  c) WHERE country='US' 算 count",
    lambda: (pd.read_csv(csv_path, usecols=["country"])["country"] == "US").sum(),
)

# ===== CSV.gz =====
print("\n[CSV.gz]")
results["CSV.gz"]["full_scan"], _ = timed(
    "  a) 全表扫描",
    lambda: pd.read_csv(csv_gz_path),
)
results["CSV.gz"]["sum_amount"], _ = timed(
    "  b) SUM(amount)",
    lambda: pd.read_csv(csv_gz_path, usecols=["amount"])["amount"].sum(),
)
results["CSV.gz"]["filter_us"], _ = timed(
    "  c) WHERE country='US' 算 count",
    lambda: (pd.read_csv(csv_gz_path, usecols=["country"])["country"] == "US").sum(),
)

# ===== Parquet+Snappy =====
print("\n[Parquet+Snappy]")
results["Parquet+Snappy"]["full_scan"], _ = timed(
    "  a) 全表扫描",
    lambda: pq.read_table(pq_snappy_path),
)
results["Parquet+Snappy"]["sum_amount"], _ = timed(
    "  b) SUM(amount) — 列裁剪",
    lambda: pc.sum(pq.read_table(pq_snappy_path, columns=["amount"])["amount"]).as_py(),
)
results["Parquet+Snappy"]["filter_us"], _ = timed(
    "  c) WHERE country='US' — 谓词下推",
    lambda: (
        pq.read_table(
            pq_snappy_path,
            columns=["country"],
            filters=[("country", "=", "US")],
        ).num_rows
    ),
)

# ===== Parquet+ZSTD =====
print("\n[Parquet+ZSTD]")
results["Parquet+ZSTD"]["full_scan"], _ = timed(
    "  a) 全表扫描",
    lambda: pq.read_table(pq_zstd_path),
)
results["Parquet+ZSTD"]["sum_amount"], _ = timed(
    "  b) SUM(amount) — 列裁剪",
    lambda: pc.sum(pq.read_table(pq_zstd_path, columns=["amount"])["amount"]).as_py(),
)
results["Parquet+ZSTD"]["filter_us"], _ = timed(
    "  c) WHERE country='US' — 谓词下推",
    lambda: (
        pq.read_table(
            pq_zstd_path,
            columns=["country"],
            filters=[("country", "=", "US")],
        ).num_rows
    ),
)


# ─────────────────────────────────────────────
# 3. 汇总打表
# ─────────────────────────────────────────────
print("\n" + "═" * 78)
print("📊 对照结果")
print("═" * 78)
print(
    f"{'格式':18s} {'大小':>12s} {'全表扫描':>12s} {'SUM(amount)':>14s} {'WHERE US':>12s}"
)
print("─" * 78)
for fmt in ["CSV", "CSV.gz", "Parquet+Snappy", "Parquet+ZSTD"]:
    r = results[fmt]
    print(
        f"{fmt:18s} {sizes[fmt]:>12s} "
        f"{r['full_scan'] * 1000:>10.1f}ms "
        f"{r['sum_amount'] * 1000:>12.1f}ms "
        f"{r['filter_us'] * 1000:>10.1f}ms"
    )
print("═" * 78)

# 写实验结果 JSON，给后面拼报告用
results_json_path = PROJECT_ROOT / "experiments" / "01-results.json"
results_json_path.parent.mkdir(parents=True, exist_ok=True)
with open(results_json_path, "w") as f:
    json.dump(
        {"sizes": sizes, "timings_seconds": results},
        f,
        indent=2,
    )
print(f"\n→ 结果已存 {results_json_path}")
print("→ 下一步: 写 experiments/01-columnar-storage-results.md 解读结果")
