# 阶段 5：Flink 流处理

> 这一阶段你会遇到一组**完全不同于批处理**的概念。读完前不要碰代码。
> 6 个核心概念：流 vs 批 / Event Time vs Processing Time / Watermark / 窗口 / 状态 / Checkpoint

---

## 5.1 流处理是什么 —— 跟批处理的世界观差别

**批处理的世界观**：
> "我面前有一个静止的数据集，我要从头扫到尾算出结果。"

**流处理的世界观**：
> "数据像河水一样源源不断流过来，**我永远看不到尽头**，但需要每隔一段时间输出一个'当前状态'的快照。"

这个差别带来的连锁后果，是后面所有概念的源头。

| | 批处理 (Spark) | 流处理 (Flink) |
|---|---|---|
| 数据有界吗 | 有界 (Bounded) | 无界 (Unbounded) |
| 计算时机 | 一次性算完输出 | 持续算，持续输出 |
| 延迟 | 分钟 / 小时 | 毫秒 / 秒 |
| 失败怎么办 | 整个作业重跑 | **不能重跑**（数据流走了），靠 Checkpoint 恢复中间状态 |
| 算"GROUP BY 全天" | 直接 GROUP BY 就行 | **不行**（"全天"还没到），需要"窗口" |

---

## 5.2 两种时间 —— 大数据流处理的灵魂概念

这是流处理最反直觉的概念，认真看。

每条事件有**两个时间戳**：

| 时间 | 含义 | 谁打的 |
|---|---|---|
| **Event Time** | 事件**真实发生**的时间 | 客户端 / App 打的 |
| **Processing Time** | 事件**被系统处理**的时间 | Flink 服务器打的 |

**为什么会不一样？**

用户在地铁里点了"加购"按钮：
```
12:00:00  用户实际点击 (Event Time)
12:00:00 ~ 12:05:00  手机离线，App 缓存
12:05:00  到地面，App 上传到后端
12:05:30  Kafka 收到这条消息
12:05:31  Flink 处理 (Processing Time)
```

`Event Time = 12:00:00`，`Processing Time = 12:05:31`，**差了 5 分 31 秒**。

### 为什么必须用 Event Time

假设你算"过去 5 分钟的 PV"。
- 用 Processing Time → 12:05~12:10 这个窗口的结果包含了上面那条 12:00 的事件（错！）
- 用 Event Time → 那条事件归到 12:00~12:05 窗口（对！）

**业务永远关心 Event Time**，因为它问的是"用户在那个时间点做了什么"，不是"我什么时候处理的"。

**但用 Event Time 有个大问题**：数据来得**乱序**。
- 12:05 处理的事件可能来自 12:00、12:03、11:58 任何时刻
- "12:00 ~ 12:05 的窗口"什么时候可以关闭？
- 万一窗口关了之后又来了一条 11:59 的迟到事件呢？

→ 这就是 **Watermark** 要解决的问题。

---

## 5.3 Watermark —— 流处理的"我可以前进了"信号

**定义**：Watermark 是一个时间戳，表示"**事件时间 ≤ 这个时间的数据，我都认为已经到齐了**"。

**举例**：当前 Watermark = `12:04:30`，意味着 Flink 认为：
- "12:04:30 之前的事件已经全部到了"
- 因此 "12:00 ~ 12:05" 的窗口里，12:04:30 之前的数据可以放心算了

**Watermark 怎么生成**：
最简单的策略 = `当前看到的最大 event_ts - 允许迟到的最大时长`

例如允许 5 秒迟到：
```
event 时间序列:  12:00 → 12:01 → 12:03 → 12:02 → 12:04 → ...
                              (12:02 是迟到的)
                                                ↑
                       看到 12:04 时，Watermark = 12:04 - 5s = 12:03:55
```

意思是："12:03:55 之前的我都收齐了，包括那个迟到的 12:02"。

**Watermark 是怎么工作的**：

```
窗口 [12:00, 12:05)  ──────▶ 等到 Watermark ≥ 12:05 → 触发计算 → 输出结果 → 关闭窗口
                                                            ↑
                              如果之后又来了 12:03 的迟到事件:
                                ① 默认丢弃（最常见）
                                ② 配置 allowedLateness → 重新触发计算 + 修正结果
                                ③ 配置 side output → 走"迟到数据"侧分支
```

**Watermark 是流处理"完整性 vs 延迟"的核心权衡**：
- 容忍度越大 → 结果越准但延迟越高
- 容忍度越小 → 延迟低但丢数据风险高

---

## 5.4 窗口 (Window) —— 把无限流切成有限块

流是无限的，但聚合需要"一段时间"。**窗口就是把无限流切成有限块**。

三类窗口：

### 滚动窗口 (Tumbling)
固定大小、**不重叠**。最常用。
```
|──── 5min ────|──── 5min ────|──── 5min ────|
[12:00, 12:05)  [12:05, 12:10)  [12:10, 12:15)
```
用法：每 5 分钟统计一次 PV。

### 滑动窗口 (Sliding)
固定大小、**有重叠**。
```
|────── 10min ──────|
       |────── 10min ──────|
              |────── 10min ──────|
   每 5min 滑一次，每个窗口长 10min
```
用法：每 5 分钟输出"过去 10 分钟的活跃用户"。

### 会话窗口 (Session)
**间隔触发**，没有固定长度。
```
事件:  ●●●●●     [30min 空白]    ●●●     [30min 空白]    ●●●●
       └─session 1─┘            └sess 2┘                └sess 3┘
```
用法：分析用户"一次访问"的行为序列。

---

## 5.5 状态 (State) —— 流处理的"内存"

批处理可以一次性把所有数据载入内存算。流处理不行（数据无限）。
但聚合必须**记住中间结果**（比如"已经累计 PV 1234 了，新来一条 +1"）。

**状态就是 Flink 维护的"中间累计值"**。

```
当前窗口 [12:00, 12:05) 的状态:
  country=US, pv=1234
  country=CN, pv=987
  
新来一条 (country=US, event_ts=12:03:21):
  状态更新为 country=US, pv=1235

Watermark 越过 12:05:
  把整个状态 flush 出去 → 写到下游 (Iceberg / Kafka)
  清空这个窗口的状态
```

**状态后端 (State Backend)** = 状态存哪里：
- **HashMapStateBackend**：放 JVM 堆内存（快，但作业重启会丢，依赖 Checkpoint）
- **RocksDBStateBackend**：放本地磁盘（慢一点，但能存 TB 级状态，生产首选）

---

## 5.6 Checkpoint —— 流处理的容错机制

批处理失败 → 整个作业重跑，因为输入数据还在 HDFS / S3 上。
流处理失败 → 数据已经从 Kafka 流过去了，**重跑不可能**。

**Checkpoint 是 Flink 定期保存"作业当前所有状态 + Kafka 消费 offset"的快照**。

```
作业开始 ──→ ●(ckpt-1) ──→ ●(ckpt-2) ──→ ●(ckpt-3) ──→ ●(ckpt-4) ──→ ✗(挂了)
                                                          ↑
                                              恢复时从这里开始:
                                              1. 加载 ckpt-4 的所有 State
                                              2. 从 ckpt-4 时的 Kafka offset 继续消费
                                              3. 像没挂过一样继续
```

**Exactly-Once 怎么保证**：
- 输入侧：Kafka offset 跟 Checkpoint 一起保存
- 状态侧：Checkpoint 一致性快照
- 输出侧：**两阶段提交 (2PC)** 协议（Iceberg / Kafka 等支持事务的 sink）

这就是为什么 Flink 在金融、广告归因等强一致场景成为事实标准。

---

## 5.7 Flink 架构（执行模型）

```
                    ┌──────────────────┐
                    │  JobManager      │   ◄─ 协调
                    │  - 调度 Task     │
                    │  - 触发 Checkpoint│
                    │  - 恢复故障       │
                    └─────────┬────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
       ┌──────────┐    ┌──────────┐    ┌──────────┐
       │TaskManager│    │TaskManager│    │TaskManager│
       │ slot×4    │    │ slot×4    │    │ slot×4    │  ◄─ 真正干活
       │ + State   │    │ + State   │    │ + State   │
       └──────────┘    └──────────┘    └──────────┘
```

- **JobManager** ≈ Spark Driver
- **TaskManager** ≈ Spark Executor
- **Slot** = 并行计算单元（一个 slot 跑一个并行 task）

跟 Spark 最大的区别：**Flink 的 task 是常驻的**，数据流过；Spark 的 task 是一次性的，数据进来算完就死。

---

## 5.8 本阶段架构

```
┌──────────────────────────┐
│  Python event_producer    │  ◄─ 模拟 App 不断发埋点
│  (kafka-python)           │     故意混入一些迟到的事件
└──────────┬───────────────┘
           │
           ▼
    ┌──────────────────┐
    │  Redpanda        │  ◄─ Kafka 兼容，比 Kafka 轻 5×
    │  topic: events   │
    └──────┬───────────┘
           │
           ▼
    ┌──────────────────┐
    │  Flink           │  ◄─ Flink SQL 跑窗口聚合
    │  - JobManager    │
    │  - TaskManager   │
    └──────┬───────────┘
           │ Iceberg sink
           ▼
    ┌────────────────────────────────┐
    │  MinIO + Iceberg REST Catalog  │  ◄─ 阶段 2-4 已有
    │  写入: dw.rt_pv_5min            │
    └────────────────────────────────┘
```

---

## 5.9 本阶段三个实验设计

### 实验 8：流式 PV/UV 入湖
- Producer 持续发埋点（**有序**，每秒 ~100 条）
- Flink SQL 跑 5 分钟滚动窗口的 PV/UV
- 写到 Iceberg 表 `dw.rt_pv_5min`
- 用 Trino 实时查这张表，验证窗口结果在持续更新

### 实验 9：Watermark 演示
- Producer **故意制造乱序**：
  - 90% 正常发送
  - 10% 缓存 10-30 秒后才发（模拟手机断网）
- 对比两个 Flink 作业：
  - Job A：用 **Processing Time** —— 迟到事件归入错误窗口
  - Job B：用 **Event Time + Watermark** —— 迟到事件归入正确窗口
- 输出对比表，肉眼看见差别

### 实验 10：Exactly-Once 容错
- 启动 Flink 流式作业
- 中途 `kill` TaskManager 容器
- 看 Flink 自动从最近 Checkpoint 恢复
- 验证下游 Iceberg 数据**没有重复、没有丢失**

---

## 5.10 概念检查清单（动手前必须能答）

- [ ] 流处理为什么不能像批处理那样"算完就走"？
- [ ] Event Time 和 Processing Time 哪个由客户端决定？
- [ ] Watermark 解决了什么问题？大致是怎么算出来的？
- [ ] 滚动窗口、滑动窗口、会话窗口各适合什么场景？
- [ ] State 和 Checkpoint 的关系？
- [ ] Flink Exactly-Once 靠什么三件套保证？
- [ ] Flink 和 Spark Structured Streaming 最大区别是什么？

---

## 5.11 实操步骤（占位，等概念确认后再启动）

```bash
# 1. 切到 Flink compose（含 Redpanda + Flink + 之前的 MinIO/REST/Spark/Trino）
docker compose -f docker-compose/05-flink.yml up -d --build

# 2. 在 Mac 装 kafka 客户端
source venv/bin/activate
pip install -r requirements.txt

# 3. 启动 producer（在 Mac 后台跑）
python3 data-gen/event_producer.py &

# 4. 提交 Flink SQL 作业（在容器内跑）
docker compose -f docker-compose/05-flink.yml exec jobmanager \
  /opt/flink/bin/sql-client.sh -f /jobs/exp08_streaming_pv_uv.sql

# 5. 同时打开 Flink Web UI: http://localhost:8082
#    看 Task / Checkpoint / Watermark 实时状态
```

---

## 5.12 阶段 5 阅读自检

读到这里你应该有这些感受：
- "Watermark 这玩意儿确实反直觉，但解决的问题真实存在"
- "流处理跟批处理是两套世界观，不是把批处理跑得更快"
- "Flink 设计 Checkpoint 是为了让流作业能像批作业一样可靠"

如果上面三条还卡在某处，告诉我，我针对性展开。**没卡再继续动手**。
