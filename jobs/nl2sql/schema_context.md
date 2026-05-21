# 湖仓表 Schema 上下文（喂给 LLM 用）

> 这份文档是 NL2SQL 实验的 schema 上下文。三种范式都从这里出发。

## Catalog / Database

所有表在 Trino 中通过 `iceberg.dw.<table>` 访问。

## 业务背景

跨境电商 App 用户行为分析。10 个国家、6 个商品品类、5 个渠道。
事件类型 5 种漏斗步：`impression / click / add_cart / order / pay`。

## 表 1: `dw.ads_funnel_daily` — 漏斗看板（看板首选）

| 字段 | 类型 | 含义 |
|---|---|---|
| dt | DATE | 日期分区 |
| country | STRING | 国家 (US/CN/JP/VN/TH/ID/PH/MY/SG/IN) |
| category | STRING | 商品品类 (electronics/clothing/home/beauty/sports/toys) |
| impression_uv | BIGINT | 曝光用户数 |
| click_uv | BIGINT | 点击用户数 |
| add_cart_uv | BIGINT | 加购用户数 |
| order_uv | BIGINT | 下单用户数 |
| pay_uv | BIGINT | 支付用户数 |
| gmv | DOUBLE | 总成交金额 |

## 表 2: `dw.dws_user_daily` — 用户日聚合

| 字段 | 类型 | 含义 |
|---|---|---|
| user_id | STRING | 用户 ID |
| dt | DATE | 日期分区 |
| country | STRING | 国家 |
| impression_cnt | BIGINT | 当日曝光次数 |
| click_cnt | BIGINT | 当日点击次数 |
| pay_cnt | BIGINT | 当日支付次数 |
| pay_amount | DOUBLE | 当日支付金额 |
| session_cnt | BIGINT | 当日会话数 |

## 表 3: `dw.dwd_user_action` — 行为明细 (大表，慎用)

| 字段 | 类型 | 含义 |
|---|---|---|
| event_id | STRING | 事件 ID |
| user_id | STRING | 用户 |
| item_id | STRING | 商品 |
| action_type | STRING | impression/click/add_cart/order/pay |
| event_ts | BIGINT | 事件时间戳 (ms) |
| amount | DOUBLE | 仅 pay 事件有值 |
| country, device, item_category, item_brand, user_channel, age_group | STRING | 维度补全 |
| dt | DATE | 分区 |

## 表 4: `dw.dim_user` — 用户维度

| 字段 | 类型 | 含义 |
|---|---|---|
| user_id | STRING | PK |
| register_date | DATE | 注册日 |
| country | STRING | |
| channel | STRING | organic/google_ads/facebook_ads/tiktok_ads/referral |
| age_group | STRING | 18-24/25-34/35-44/45-54/55+ |
| is_active | BOOLEAN | |

## 表 5: `dw.dim_item` — 商品维度

| 字段 | 类型 | 含义 |
|---|---|---|
| item_id | STRING | PK |
| category | STRING | |
| brand | STRING | |
| price | DOUBLE | |
| listed_date | DATE | |

## Trino SQL 语法注意

- 日期字面量: `DATE '2026-05-20'`
- 时间间隔: `INTERVAL '7' DAY`, `INTERVAL '1' MONTH`
- 当前日期: `current_date`
- 占比计算: `SUM(x) * 100.0 / SUM(y)` (注意整数除法陷阱，用 `100.0`)
- 窗口函数: `LAG(...) OVER (PARTITION BY ... ORDER BY ...)`
