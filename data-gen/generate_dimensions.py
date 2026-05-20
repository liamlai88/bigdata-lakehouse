"""
生成维度数据：dim_user / dim_item
=================================

从事件数据里出现过的 user_id / item_id 反推，补齐属性。
真实公司这两张表来自业务库 (MySQL) 的 CDC，这里我们模拟。

输出：
  data/dims/dim_user.parquet
  data/dims/dim_item.parquet
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from faker import Faker

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVENTS_PARQUET = PROJECT_ROOT / "data/formats/events.zstd.parquet"
OUT_DIR = PROJECT_ROOT / "data/dims"
OUT_DIR.mkdir(parents=True, exist_ok=True)

fake = Faker()
random.seed(42)
Faker.seed(42)

COUNTRIES = ["US", "CN", "JP", "VN", "TH", "ID", "PH", "MY", "SG", "IN"]
CHANNELS = ["organic", "google_ads", "facebook_ads", "tiktok_ads", "referral"]
AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55+"]
CATEGORIES = ["electronics", "clothing", "home", "beauty", "sports", "toys"]
BRANDS = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE", "BrandF", "BrandG"]


def main():
    print(f"→ 读 {EVENTS_PARQUET.name} 拿用户/商品 ID 集合")
    df = pq.read_table(
        EVENTS_PARQUET, columns=["user_id", "item_id", "country"]
    ).to_pandas()
    users = df[["user_id", "country"]].drop_duplicates(subset="user_id")
    items = df[["item_id"]].drop_duplicates()
    print(f"  唯一用户: {len(users):,}")
    print(f"  唯一商品: {len(items):,}")

    # dim_user
    print("\n→ 生成 dim_user")
    today = datetime.now().date()
    user_rows = []
    for _, row in users.iterrows():
        register_date = today - timedelta(days=random.randint(1, 365 * 2))
        user_rows.append(
            {
                "user_id": row["user_id"],
                "register_date": register_date,
                "country": row["country"],
                "channel": random.choice(CHANNELS),
                "age_group": random.choice(AGE_GROUPS),
                "is_active": random.random() > 0.1,
            }
        )
    dim_user = pd.DataFrame(user_rows)
    dim_user_path = OUT_DIR / "dim_user.parquet"
    dim_user.to_parquet(dim_user_path, compression="zstd")
    print(
        f"  ✅ {dim_user_path}  ({len(dim_user):,} 行, "
        f"{dim_user_path.stat().st_size / 1024:.0f} KB)"
    )

    # dim_item
    print("\n→ 生成 dim_item")
    item_rows = []
    for _, row in items.iterrows():
        listed_date = today - timedelta(days=random.randint(1, 365))
        category = random.choice(CATEGORIES)
        item_rows.append(
            {
                "item_id": row["item_id"],
                "category": category,
                "sub_category": f"{category}_sub{random.randint(1, 5)}",
                "brand": random.choice(BRANDS),
                "price": round(random.uniform(5, 500), 2),
                "listed_date": listed_date,
            }
        )
    dim_item = pd.DataFrame(item_rows)
    dim_item_path = OUT_DIR / "dim_item.parquet"
    dim_item.to_parquet(dim_item_path, compression="zstd")
    print(
        f"  ✅ {dim_item_path}  ({len(dim_item):,} 行, "
        f"{dim_item_path.stat().st_size / 1024:.0f} KB)"
    )

    print(
        "\n💡 这两张表后面会通过 Spark 加载到 Iceberg：\n"
        "   lakehouse.dim_user / lakehouse.dim_item\n"
        "   在 DWD/DWS 阶段会 join 进事实表"
    )


if __name__ == "__main__":
    main()
