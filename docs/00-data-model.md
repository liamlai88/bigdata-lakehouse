# 阶段 0：数据建模

> 没有清晰的 schema，后面所有 ETL 都是返工。这一篇不写代码，只画表。

## 0.1 业务场景

模拟一个**跨境电商**（贴你的阿里云海外 SA 目标）App 的用户行为分析平台：

- 用户在 App 里 **曝光商品 → 点击 → 加购 → 下单 → 支付**
- 数据团队要回答：
  1. **漏斗转化率**：曝光→点击→加购→下单→支付 每一步流失多少？
  2. **留存**：新用户次日 / 7 日 / 30 日留存？
  3. **GMV 拆解**：按国家 / 品类 / 渠道的 GMV 分布？
  4. **实时 PV/UV**：过去 5 分钟的活跃用户？
  5. （第 12 份报告）**自然语言问数**：运营直接问"上周东南亚漏斗转化率"

## 0.2 为什么要数仓分层（ODS / DWD / DWS / ADS）

| 层 | 全称 | 干什么 | 典型粒度 |
|---|---|---|---|
| ODS | Operational Data Store | 原始数据落地，几乎不动 | 一条事件 = 一行 |
| DWD | Data Warehouse Detail | 清洗、解析、维度补全 | 一条事件 = 一行 |
| DWS | Data Warehouse Summary | 轻度聚合（按天/小时） | 一个用户一天 = 一行 |
| ADS | Application Data Service | 面向应用的最终结果 | 一个看板指标 = 一行 |

**为什么不直接从 ODS 算 ADS？**

1. **复用**：DWS 算一次，10 个看板共用，省 90% 算力
2. **稳定**：上游 schema 变了，只要 DWD 把口径稳住，ADS 不动
3. **可解释**：出问题时一层层往下查
4. **权限**：ODS 含 PII，ADS 给运营看，分层天然权限隔离

## 0.3 维度建模：星型模型

**事实表 (Fact)**：可加性的度量（金额、次数、时长），高基数、窄长
**维度表 (Dimension)**：描述性的属性（用户画像、商品类目），低基数、宽短

我们这个项目的星型模型：

```
              ┌────────────────┐
              │  dim_user      │
              │  user_id PK    │
              │  country       │
              │  register_date │
              │  channel       │
              └───────┬────────┘
                      │
   ┌─────────┐        │        ┌─────────────┐
   │ dim_date│        │        │  dim_item   │
   │ date PK │◄──┐    │    ┌──►│  item_id PK │
   │ week    │   │    │    │   │  category   │
   │ month   │   │    │    │   │  price      │
   └─────────┘   │    │    │   │  brand      │
                 │    ▼    │   └─────────────┘
                ┌─┴────────┴─────────┐
                │ fact_user_action   │
                │ event_id    PK     │
                │ user_id     FK     │
                │ item_id     FK     │
                │ event_date  FK     │
                │ action_type        │  ◄── impression/click/add_cart/order/pay
                │ event_ts (ms)      │
                │ session_id         │
                │ amount             │  ◄── 仅 pay 有值
                │ properties (json)  │
                └────────────────────┘
```

## 0.4 表 schema 详细定义

### ODS 层

**ods.events**（Kafka topic 直接落地，几乎不动）

| 字段 | 类型 | 说明 |
|---|---|---|
| raw | string | 原始 JSON 字符串 |
| kafka_ts | timestamp | Kafka 写入时间 |
| kafka_partition | int | 分区号 |
| kafka_offset | bigint | offset，用于排查 |

> 设计原则：**ODS 永远是 append-only，schemaless friendly**。Kafka 那边格式变了不会立刻挂掉。

### DWD 层

**dwd.user_action**（解析过、清洗过的明细）

| 字段 | 类型 | 说明 |
|---|---|---|
| event_id | string | UUID，去重用 |
| user_id | string | 用户 ID |
| item_id | string | 商品 ID，曝光/点击/加购/下单/支付才有 |
| action_type | string | impression / click / add_cart / order / pay |
| event_ts | timestamp(ms) | **事件发生时间**（不是入库时间！流处理重点） |
| session_id | string | 会话 ID，30 分钟无活动算新会话 |
| amount | decimal(18,2) | 仅 pay 事件有，其他 null |
| country | string | 从 IP 解析，补到这里 |
| device | string | iOS / Android / Web |
| properties | string (json) | 灵活字段，schema 演进缓冲区 |
| dt | date | **分区字段** = event_ts 的日期 |

**分区策略**：`PARTITION BY dt`，按天分区，Iceberg 隐藏分区可以做到查询自动剪裁。

### DWS 层

**dws.user_daily**（一个用户一天一行）

| 字段 | 类型 |
|---|---|
| user_id | string |
| dt | date |
| impression_cnt | bigint |
| click_cnt | bigint |
| add_cart_cnt | bigint |
| order_cnt | bigint |
| pay_cnt | bigint |
| pay_amount | decimal(18,2) |
| session_cnt | bigint |
| active_seconds | bigint |

**dws.item_daily**（一个商品一天一行）—— 字段类似，略

### ADS 层

**ads.funnel_daily**（漏斗看板直接读这张）

| 字段 | 类型 |
|---|---|
| dt | date |
| country | string |
| category | string |
| impression_uv | bigint |
| click_uv | bigint |
| add_cart_uv | bigint |
| order_uv | bigint |
| pay_uv | bigint |
| gmv | decimal(18,2) |

**ads.retention_daily**（留存看板）

| 字段 | 类型 |
|---|---|
| register_date | date |
| day_offset | int    | ◄── 0=当天, 1=次日, 7=7日 |
| retained_users | bigint |
| total_new_users | bigint |
| retention_rate | double |

### 维度表

**dim_user**

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | string | PK |
| register_date | date | |
| country | string | |
| channel | string | google_ads / organic / referral |
| age_group | string | 18-24 / 25-34 / ... |
| is_active | boolean | SCD Type 1，覆盖更新 |

**dim_item**

| 字段 | 类型 |
|---|---|
| item_id | string |
| category | string |
| sub_category | string |
| brand | string |
| price | decimal(18,2) |
| listed_date | date |

## 0.5 关键概念检查清单

读完应该能答上：

- [ ] 为什么 ODS 要保留原始 JSON，而不是直接结构化？
- [ ] 事实表和维度表的最大区别？
- [ ] 为什么按 `dt` 分区而不是 `event_ts`？
- [ ] DWS 和 ADS 都"聚合"，本质区别是什么？
- [ ] `event_ts`（事件时间）和 `kafka_ts`（处理时间）有什么差别？为什么流处理特别关心这个？

## 0.6 数据量规划（本地模拟）

| 指标 | 目标 |
|---|---|
| 模拟用户数 | 10 万 |
| 模拟商品数 | 1 万 |
| 每天事件数 | 100 万 |
| 模拟天数 | 30 天 |
| ODS 总行数 | 3000 万 |
| 预计 Parquet 体积 | ~2GB |

够小可以本地全量跑，够大能体验 shuffle / 分区裁剪的差别。

## 下一步

→ 阶段 1：用 Python 写一个埋点模拟器，把上述 schema 的数据灌成 JSON Lines，然后我们用 MinIO + Parquet 跑第一个对照实验（CSV vs Parquet 体积/扫描速度）。
