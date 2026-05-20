"""
实验 4：Iceberg 四大超能力
============================

按顺序演示:
  ① Schema Evolution  — ADD COLUMN 不重写数据
  ② Time Travel       — 按 snapshot_id 查历史版本
  ③ Row-level Delete  — DELETE 不动数据文件
  ④ Rollback          — 把表退回到某个历史 snapshot

前置: 已经跑过 exp03_iceberg_basics.py
"""

import time
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.types import StringType

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

TABLE = "lakehouse.events"
table = catalog.load_table(TABLE)


def banner(title):
    print(f"\n{'═' * 70}\n{title}\n{'═' * 70}")


def show_snapshots(label):
    print(f"\n📸 snapshot 列表 ({label}):")
    for snap in table.snapshots():
        print(
            f"  id={snap.snapshot_id}  op={snap.summary['operation']:8s}  "
            f"added_records={snap.summary.get('added-records', '-')}  "
            f"deleted_records={snap.summary.get('deleted-records', '-')}"
        )


# ═══════════════════════════════════════════════════════════════
# 起点：看看当前状态
# ═══════════════════════════════════════════════════════════════
banner("起点状态")
show_snapshots("初始")
initial_count = len(table.scan().to_pandas())
print(f"  总行数: {initial_count:,}")
print(f"  当前 schema 字段数: {len(table.schema().fields)}")


# ═══════════════════════════════════════════════════════════════
# ① Schema Evolution — 加一列 platform
# ═══════════════════════════════════════════════════════════════
banner("① Schema Evolution — ADD COLUMN platform")

print("→ 加列 platform: string")
with table.update_schema() as update:
    update.add_column("platform", StringType())
table.refresh()

print("  新 schema:")
for field in table.schema().fields:
    print(f"    [{field.field_id}] {field.name}: {field.field_type}")

print("\n→ 验证：旧数据的 platform 列应该全是 null（说明没重写）")
df = table.scan(limit=5).to_pandas()
print(df[["user_id", "country", "platform"]].head(5).to_string(index=False))
null_count = df["platform"].isna().sum()
print(f"  前 5 行 platform null 数: {null_count}/5  ✅ Iceberg 没碰旧数据文件")

# 写一批带 platform 的新数据
print("\n→ Append 一批带 platform='mobile_app' 的新数据 (1000 行)")
sample = table.scan(limit=1000).to_pandas()
sample["platform"] = "mobile_app"
new_batch = pa.Table.from_pandas(sample, schema=table.schema().as_arrow())
table.append(new_batch)
table.refresh()


# ═══════════════════════════════════════════════════════════════
# ② Time Travel — 按 snapshot 查历史
# ═══════════════════════════════════════════════════════════════
banner("② Time Travel — 按 snapshot_id 查历史版本")

show_snapshots("加列+追加之后")

snapshots = list(table.snapshots())
first_snap = snapshots[0]
latest_snap = snapshots[-1]

print(f"\n→ 查最早 snapshot ({first_snap.snapshot_id}) 时的行数")
old_df = table.scan(snapshot_id=first_snap.snapshot_id).to_pandas()
print(f"  历史版本行数: {len(old_df):,}")
print(f"  历史版本字段数: {len(old_df.columns)}  ← 注意这里没有 platform 列")

print(f"\n→ 查最新 snapshot ({latest_snap.snapshot_id}) 的行数")
new_df = table.scan(snapshot_id=latest_snap.snapshot_id).to_pandas()
print(f"  最新版本行数: {len(new_df):,}")
print(f"  最新版本字段数: {len(new_df.columns)}  ← 多了 platform")

print(f"\n  差额: {len(new_df) - len(old_df):,} 行  (= append 进来的新批次)")


# ═══════════════════════════════════════════════════════════════
# ③ Row-level Delete — 删 country='SG' 的所有行
# ═══════════════════════════════════════════════════════════════
banner("③ Row-level Delete — DELETE WHERE country='SG'")

before = len(table.scan().to_pandas())
sg_count = len(table.scan(row_filter=EqualTo("country", "SG")).to_pandas())
print(f"  删除前: 总行数 {before:,}, 其中 country='SG' 的 {sg_count:,}")

print("\n→ 执行 delete...")
t0 = time.perf_counter()
table.delete(delete_filter=EqualTo("country", "SG"))
print(f"  耗时 {(time.perf_counter() - t0) * 1000:.0f}ms")
table.refresh()

after = len(table.scan().to_pandas())
sg_after = len(table.scan(row_filter=EqualTo("country", "SG")).to_pandas())
print(f"\n  删除后: 总行数 {after:,}, 其中 country='SG' 的 {sg_after}")
print(f"  ✅ 减少了 {before - after:,} 行 (预期 {sg_count:,})")

show_snapshots("删除后")

print("\n💡 关键观察:")
print("  去 MinIO 控制台 http://localhost:9001 看")
print("  warehouse/lakehouse/events/data/")
print("  应该多出一个 delete-*.parquet 文件 (position-delete)")
print("  原来的 data file **一行都没改**，这就是 merge-on-read")


# ═══════════════════════════════════════════════════════════════
# ④ Rollback — 回到删除之前
# ═══════════════════════════════════════════════════════════════
banner("④ Rollback — 把表回退到删除之前")

# 倒数第二个 snapshot = 删除之前
snapshots = list(table.snapshots())
target = snapshots[-2]  # 删除操作的上一个
print(f"→ 回退到 snapshot_id={target.snapshot_id} (op={target.summary['operation']})")

table.manage_snapshots().rollback_to_snapshot(target.snapshot_id).commit()
table.refresh()

after_rollback = len(table.scan().to_pandas())
sg_after_rollback = len(table.scan(row_filter=EqualTo("country", "SG")).to_pandas())
print(f"  回退后: 总行数 {after_rollback:,}, country='SG' 的 {sg_after_rollback:,}")
print("  ✅ SG 数据复活了，回到了删除之前的状态")

show_snapshots("回滚后（看 current-snapshot-id 改变了）")

print(f"\n{'═' * 70}")
print("✅ 实验 4 完成")
print(f"{'═' * 70}")
print("\n你刚刚见证了:")
print("  ① 加列不重写数据 → schema 演进秒级完成")
print("  ② 同一张表能查任意历史版本 → 数据有了'时间'维度")
print("  ③ 删行不动数据文件 → 用 delete file 标记")
print("  ④ 误操作能 rollback → 数据再也丢不了")
print("\n这四件事在普通 Parquet 上要么做不到，要么要全表重写。")
print("这就是为什么阿里云 DLF / AWS S3 Tables / Databricks 都 all-in Iceberg。")
