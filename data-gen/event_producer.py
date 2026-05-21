"""
实时埋点 Producer
==================

往 Redpanda topic 'events' 持续发送模拟埋点。
支持两种模式 (CLI 切换):

  --mode ordered    每条事件 event_ts = 当前时间，无乱序 (实验 8 用)
  --mode unordered  10% 事件 event_ts 故意落后 10-30 秒 (实验 9 Watermark 用)

发送速率默认 100 events/sec，可调。
按 Ctrl+C 停止。

用法:
  python3 data-gen/event_producer.py
  python3 data-gen/event_producer.py --mode unordered --rate 50
  python3 data-gen/event_producer.py --rate 200 --total 10000
"""

import argparse
import json
import random
import signal
import sys
import time

from confluent_kafka import Producer

BROKERS = (
    "localhost:19092"  # Redpanda external listener (容器内的 Flink 走 redpanda:9092)
)
TOPIC = "events"

COUNTRIES = ["US", "CN", "JP", "VN", "TH", "ID", "PH", "MY", "SG", "IN"]
DEVICES = ["iOS", "Android", "Web"]
ACTIONS = ["impression", "click", "add_cart", "order", "pay"]
ACTION_WEIGHTS = [60, 25, 8, 5, 2]

NUM_USERS = 100_000
NUM_ITEMS = 10_000

_total_sent = 0


def make_event(now_ms: int, lateness_ms: int = 0) -> dict:
    """
    生成一条事件。
    lateness_ms > 0 时，event_ts 比 now 早 lateness_ms 毫秒（模拟迟到）
    """
    action = random.choices(ACTIONS, weights=ACTION_WEIGHTS, k=1)[0]
    event_ts = now_ms - lateness_ms
    return {
        "event_id": f"{event_ts}-{random.randint(0, 999999)}",
        "user_id": f"u_{random.randint(1, NUM_USERS)}",
        "item_id": f"i_{random.randint(1, NUM_ITEMS)}",
        "action_type": action,
        "event_ts": event_ts,
        "country": random.choice(COUNTRIES),
        "device": random.choice(DEVICES),
        "amount": round(random.uniform(5, 500), 2) if action == "pay" else None,
        # 调试用：标记这条是否迟到
        "_meta_late_ms": lateness_ms,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"  ❌ delivery failed: {err}")


def stats_printer(start_ts):
    elapsed = time.time() - start_ts
    rate = _total_sent / max(elapsed, 1)
    print(f"  📊 sent={_total_sent:,} | elapsed={elapsed:.0f}s | rate={rate:.1f}/s")


def main():
    global _total_sent

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--mode",
        choices=["ordered", "unordered"],
        default="ordered",
        help="ordered=event_ts=now (exp 8); "
        "unordered=~10pct events late by 10-30s (exp 9)",
    )
    ap.add_argument("--rate", type=int, default=100, help="events / second")
    ap.add_argument("--total", type=int, default=0, help="0 = 无限")
    args = ap.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": BROKERS,
            "linger.ms": 50,
            "compression.type": "snappy",
        }
    )
    print(f"✅ Producer ready → {BROKERS}/{TOPIC}")
    print(f"   mode={args.mode}  rate={args.rate}/s  total={args.total or '∞'}")
    print("   Ctrl+C 停止\n")

    # 处理 Ctrl+C
    def _handle_sigint(sig, frame):
        print("\n→ flushing producer...")
        producer.flush(timeout=5)
        stats_printer(start)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_sigint)

    start = time.time()
    interval = 1.0 / args.rate
    last_stats = start

    while True:
        if args.total and _total_sent >= args.total:
            break

        now_ms = int(time.time() * 1000)

        # 决定是否制造迟到
        if args.mode == "unordered" and random.random() < 0.1:
            lateness_ms = random.randint(10_000, 30_000)
        else:
            lateness_ms = 0

        event = make_event(now_ms, lateness_ms)

        producer.produce(
            TOPIC,
            value=json.dumps(event).encode("utf-8"),
            key=event["user_id"].encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # 触发回调
        _total_sent += 1

        # 每 5 秒打一次统计
        if time.time() - last_stats > 5:
            stats_printer(start)
            last_stats = time.time()

        time.sleep(interval)

    producer.flush(timeout=10)
    stats_printer(start)


if __name__ == "__main__":
    main()
