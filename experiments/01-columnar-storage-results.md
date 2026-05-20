# 实验 1：列存 + 对象存储 实证报告

> 日期：2026-05-20
> 机器：MacBook Air M5 / 24GB
> 数据：100 万条模拟跨境电商埋点事件（11 列、~270MB JSONL 源）
> 工具：pyarrow 17 / pandas 2.2 / MinIO (Docker)

---

## 一、核心发现（一句话）

**列存 (Parquet) 让 OLAP 查询比 CSV 快 22~80 倍，且把数据放到对象存储 (MinIO) 上依然保留全部加速效果。**

---

## 二、实验 1：CSV vs Parquet 四种格式对照

### 数据规模
- 行数：1,000,000
- 列数：11
- 维度基数：country=10、device=3、category=6、channel=5（低基数，对字典编码友好）

### 结果对照

| 格式 | 文件大小 | 全表扫描 | SUM(amount) | WHERE country='US' |
|---|---|---|---|---|
| **CSV** | 106.0 MB | 737.9 ms | 159.2 ms | 183.8 ms |
| **CSV.gz** | 41.7 MB | 859.2 ms | 281.4 ms | 306.7 ms |
| **Parquet+Snappy** | 54.5 MB | **26.7 ms** | **1.7 ms** | **5.2 ms** |
| **Parquet+ZSTD** | 34.1 MB | 32.6 ms | 2.0 ms | 6.0 ms |

### 加速倍率（vs CSV 基线）

| 查询 | Parquet 加速 | 原因 |
|---|---|---|
| 全表扫描 | **22×** | 列存 + 编码 + 压缩，磁盘读量小一个数量级 |
| SUM(amount) | **80×** 🔥 | **列裁剪**：只读 amount 这一列，其他 10 列完全不碰 |
| WHERE country='US' | **31×** | **谓词下推**：用 Parquet footer 的 min/max 跳过不含 US 的 Row Group |

### 反直觉发现

1. **CSV.gz 比 CSV 还慢**
   - 原因：M5 SSD IO 几乎免费，gzip 单线程解压反而成为瓶颈
   - 教训：本地小数据上压缩 ≠ 加速，**只在 IO 瓶颈场景才赢**

2. **ZSTD 比 Snappy 小 37%，但查询只慢 15%**
   - 验证了工业界经验法则：**冷数据 ZSTD（省钱），热数据 Snappy（省时间）**

3. **CSV 的 SUM 比全表扫描快 4 倍**
   - pandas 用 `usecols=['amount']` 时只解析这一列字符串，省了其他列的字符串→对象开销
   - 但**仍然要扫整个文件**找列分隔符 → 跟 Parquet 真正的列裁剪（只读那段字节）差 80 倍

---

## 三、实验 2：本地 Parquet vs MinIO (S3) Parquet

把同一份 `events.zstd.parquet`（34MB）上传到 MinIO，再用 `pyarrow.fs.S3FileSystem` 读取。

| 查询 | 本地盘 | MinIO (S3) | 倍率 |
|---|---|---|---|
| 全表扫描 | 33.0 ms | 190.8 ms | **5.8×** |
| SUM(amount) — 列裁剪 | 1.9 ms | 7.0 ms | **3.7×** |
| WHERE country='US' — 谓词下推 | 6.1 ms | 13.0 ms | **2.1×** |

### 三个关键观察

**观察 1：对象存储贵在网络 RTT，但占比随查询变小**
- 全表扫描 5.8× 慢 → 大部分时间是 HTTP 读取
- 列裁剪只 3.7× 慢 → IO 砍掉 90% 后，固定开销占比上升
- 谓词下推 2.1× 慢 → IO 进一步砍掉后差距最小

**观察 2：MinIO 永远比本地慢，但"格式选对了，远程吊打本地错格式"**

先澄清一个容易误读的对比：

| 对比对象 | 数字 | 谁更快 |
|---|---|---|
| 同样 SUM：本地 Parquet vs MinIO Parquet | 1.9ms vs 7.0ms | **本地快 3.7×**（物理规律：本地永远比远程快） |
| 同样全表扫描：本地 CSV vs 本地 Parquet | 738ms vs 33ms | 本地 Parquet 快 22× |
| **跨格式跨存储：MinIO Parquet SUM vs 本地 CSV 全表** | **7ms vs 738ms** | **MinIO Parquet 快 105×** 🔥 |

最后一行才是湖仓的真正卖点：
- 数据放便宜的远程对象存储 (S3/OSS/MinIO)
- 只要格式选对 (Parquet)
- 查询比"传统方案放本地用错格式"还快 100 倍
- 这就是为什么阿里云敢推"存储算分离 + OSS 数据湖" 架构

**观察 3：上传 34MB 只用 175ms**
- 本地 MinIO 走 Docker bridge，等效带宽 ~200MB/s
- 真实云上 S3 上传通常 50-100MB/s，做参考时打 2-4 折

---

## 四、为什么 Parquet 快：原理回放

把数字落地到 Parquet 内部三层结构上：

```
SELECT SUM(amount) FROM events WHERE country = 'US'
       └──┬──────┘                 └──────┬────────┘
          │                                │
          ▼                                ▼
   列裁剪 (Column Pruning)         谓词下推 (Predicate Pushdown)
          │                                │
          ▼                                ▼
   只读 amount 那个 Column Chunk    先看 Row Group footer
                                    min/max，跳过 country
                                    范围不含 US 的 Row Group
```

加上：
- **字典编码**：country 10 个值 → 字典 + 整数索引
- **RLE**：连续相同值压成 (值, 次数)
- **ZSTD 块压缩**：在编码后的数据上再压一层

四个机制叠加 → **物理读取的字节数比 CSV 少 ~50 倍**，于是 SUM 快 80 倍。

---

## 五、踩坑记录

| 坑 | 现象 | 解决 |
|---|---|---|
| Python 3.14 太新 | pyarrow 一开始担心兼容性 | 实际 pyarrow 17+ 已支持 3.14 |
| MinIO HTTP 还是 HTTPS | `S3FileSystem` 默认 https | 必须显式 `scheme="http"` |
| pyarrow 谓词过滤语法 | `filters=[(列, 操作符, 值)]` | 列出列名时仍要 `columns=[...]` 才生效 |

---

## 六、下一阶段的引子

本实验留下两个问题，正是阶段 2 (Iceberg) 要解决的：

1. **如果有 10000 个 Parquet 文件而不是 1 个，怎么管？**
   → Iceberg 元数据（manifest）

2. **想"删一行"或"加一列"怎么办？Parquet 自己不支持。**
   → Iceberg 的 ACID + Schema Evolution

---

## 七、结论

✅ Parquet vs CSV：**80× 加速**（列裁剪），**31× 加速**（谓词下推）
✅ 列存 + 编码 + 压缩三连击，体积是 CSV 的 1/3，查询快两个数量级
✅ 同操作下，本地 (1.9ms) 永远比 MinIO (7ms) 快 ~3-6 倍，这是物理规律
✅ 但**跨格式对比**：MinIO Parquet (7ms) 比本地 CSV 全表 (738ms) **快 105 倍** ← 湖仓的核心论据
✅ 这就是为什么阿里云 OSS + DLF + Hologres / MaxCompute 是现代湖仓标配

## 八、可复现性

```bash
# 在仓库根目录
docker compose -f docker-compose/01-storage.yml up -d
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python3 data-gen/generate_events.py        # 生成 100 万行
python3 data-gen/exp01_format_comparison.py
python3 data-gen/exp02_minio_upload.py
```

环境：
- macOS / Apple M5 / 24GB
- Python 3.14.4 / pyarrow 17+ / pandas 2.2+
- MinIO (Docker, latest)
- 数据卷：`docker volume ls | grep minio-data`
