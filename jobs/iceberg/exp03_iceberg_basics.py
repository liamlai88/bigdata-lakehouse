"""
实验 3：Iceberg 基础 —— 建表 / 写入 / 看元数据
=================================================

跑完你会理解:
  1. Iceberg 表在 MinIO 上长什么样 (data/ + metadata/)
  2. 怎么用 pyiceberg 连 REST Catalog
  3. snapshot 是什么、append 怎么生成新 snapshot

前置:
  docker compose -f docker-compose/02-iceberg.yml up -d
  source venv/bin/activate
  pip install -r requirements.txt
  # 如果还没生成数据：python3 data-gen/generate_events.py
"""

import time
from pathlib import Path

import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SOURCE_PARQUET = PROJECT_ROOT / "data/formats/events.zstd.parquet"

assert SOURCE_PARQUET.exists(), (
    f"找不到 {SOURCE_PARQUET}\n请先跑阶段 1 的 exp01_format_comparison.py"
)

# ─────────────────────────────────────────────
# 1. 连接 REST Catalog
# ─────────────────────────────────────────────
print("→ 连接 Iceberg REST Catalog")
catalog = load_catalog(
    "rest",
    **{
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    },
)
print(f"  ✅ 已连 {catalog.name}")

# ─────────────────────────────────────────────
# 2. 建 namespace (= 数据库 / schema)
# ─────────────────────────────────────────────
NS = "lakehouse"
try:
    catalog.create_namespace(NS)
    print(f"→ 创建 namespace: {NS}")
except NamespaceAlreadyExistsError:
    print(f"→ namespace {NS} 已存在")

# ─────────────────────────────────────────────
# 3. 读源 Parquet，准备 schema
# ─────────────────────────────────────────────
print(f"\n→ 读取源数据 {SOURCE_PARQUET.name}")
source_table = pq.read_table(SOURCE_PARQUET)
print(f"  行数: {source_table.num_rows:,}")
print(f"  列数: {len(source_table.schema)}")

# ─────────────────────────────────────────────
# 4. 建 Iceberg 表 (如果不存在)
# ─────────────────────────────────────────────
TABLE = f"{NS}.events"
try:
    table = catalog.load_table(TABLE)
    print(f"\n→ 表 {TABLE} 已存在，先删了重建（实验便利）")
    catalog.drop_table(TABLE)
    raise NoSuchTableError
except NoSuchTableError:
    print(f"\n→ 创建表 {TABLE}")
    table = catalog.create_table(
        identifier=TABLE,
        schema=source_table.schema,
    )
    print(f"  ✅ 表已创建，location: {table.location()}")
    print("  当前 schema:")
    for field in table.schema().fields:
        print(f"    [{field.field_id}] {field.name}: {field.field_type}")

# ─────────────────────────────────────────────
# 5. 第一次写入 (append) → 生成 snapshot 1
# ─────────────────────────────────────────────
print(f"\n→ 第 1 次 append (全部 {source_table.num_rows:,} 行)")
t0 = time.perf_counter()
table.append(source_table)
print(f"  耗时: {(time.perf_counter() - t0):.1f}s")

# 重新加载表，拿到最新元数据
table.refresh()

# ─────────────────────────────────────────────
# 6. 查询验证
# ─────────────────────────────────────────────
print("\n→ 查询验证")
df = table.scan().to_pandas()
print(f"  行数: {len(df):,}")
print("  示例:")
print(
    df[["user_id", "country", "action_type", "amount"]].head(3).to_string(index=False)
)

# ─────────────────────────────────────────────
# 7. 看 snapshot 列表
# ─────────────────────────────────────────────
print("\n📸 当前 snapshot 列表:")
for snap in table.snapshots():
    print(
        f"  snapshot_id={snap.snapshot_id}  "
        f"timestamp={snap.timestamp_ms}  "
        f"operation={snap.summary['operation']}  "
        f"added_files={snap.summary.get('added-data-files', 'n/a')}  "
        f"added_records={snap.summary.get('added-records', 'n/a')}"
    )

# ─────────────────────────────────────────────
# 8. 看元数据文件位置
# ─────────────────────────────────────────────
print(f"\n📂 表 location: {table.location()}")
print("   去 MinIO 控制台 http://localhost:9001 看这两个子目录:")
print("     warehouse/lakehouse/events/data/      ← 真正的 Parquet 数据")
print("     warehouse/lakehouse/events/metadata/  ← Iceberg 元数据 (json + avro)")

print("\n✅ 实验 3 完成. 下一步: exp04_iceberg_superpowers.py")
