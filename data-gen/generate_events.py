"""
阶段 1：生成模拟埋点数据
========================

生成 100 万条 ODS 层风格的用户行为事件，写到 data/raw/events.jsonl

为什么先写成 JSONL？
- 模拟 Kafka 落地到 ODS 的真实形态：每行一条 JSON
- 后面我们再分别转成 CSV / Parquet 做对照实验

用法:
    python3 data-gen/generate_events.py
    python3 data-gen/generate_events.py --rows 5000000   # 改行数
"""

import argparse
import json
import random
import time
from pathlib import Path
from datetime import datetime, timedelta

from faker import Faker
from tqdm import tqdm

fake = Faker()
random.seed(42)
Faker.seed(42)

# 业务维度池子（保持小，方便看字典编码效果）
COUNTRIES = ["US", "CN", "JP", "VN", "TH", "ID", "PH", "MY", "SG", "IN"]
DEVICES = ["iOS", "Android", "Web"]
CATEGORIES = ["electronics", "clothing", "home", "beauty", "sports", "toys"]
CHANNELS = ["organic", "google_ads", "facebook_ads", "tiktok_ads", "referral"]
ACTIONS = ["impression", "click", "add_cart", "order", "pay"]
# 漏斗权重：曝光最多，支付最少（更真实）
ACTION_WEIGHTS = [60, 25, 8, 5, 2]

NUM_USERS = 100_000
NUM_ITEMS = 10_000


def gen_one_event(base_ts: datetime) -> dict:
    action = random.choices(ACTIONS, weights=ACTION_WEIGHTS, k=1)[0]
    # 事件时间随机抖动到过去 30 天
    days_back = random.randint(0, 29)
    seconds_back = random.randint(0, 86400)
    event_ts = base_ts - timedelta(days=days_back, seconds=seconds_back)

    return {
        "event_id": fake.uuid4(),
        "user_id": f"u_{random.randint(1, NUM_USERS)}",
        "item_id": f"i_{random.randint(1, NUM_ITEMS)}",
        "action_type": action,
        "event_ts": int(event_ts.timestamp() * 1000),  # 毫秒
        "session_id": f"s_{random.randint(1, NUM_USERS * 5)}",
        "amount": round(random.uniform(5, 500), 2) if action == "pay" else None,
        "country": random.choice(COUNTRIES),
        "device": random.choice(DEVICES),
        "category": random.choice(CATEGORIES),
        "channel": random.choice(CHANNELS),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000, help="生成多少行")
    ap.add_argument(
        "--out",
        type=str,
        default="data/raw/events.jsonl",
        help="输出路径，相对项目根目录",
    )
    args = ap.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    out_path = project_root / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    base_ts = datetime.now()
    print(f"→ 生成 {args.rows:,} 条事件到 {out_path}")
    start = time.time()
    with open(out_path, "w") as f:
        for _ in tqdm(range(args.rows)):
            f.write(json.dumps(gen_one_event(base_ts)) + "\n")
    elapsed = time.time() - start
    size_mb = out_path.stat().st_size / 1024 / 1024

    print(f"✅ 完成: {args.rows:,} 行, {size_mb:.1f} MB, 耗时 {elapsed:.1f}s")


if __name__ == "__main__":
    main()
