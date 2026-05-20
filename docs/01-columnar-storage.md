# 阶段 1：对象存储 + 列存

> 本阶段目标：**亲手量出**列存比行存快多少、对象存储和本地盘有什么差别。
> 跑完两个对照实验，你会对"为什么 OLAP 选 Parquet + S3"形成肌肉记忆。

---

## 1.1 先讲 Parquet 内部到底长什么样

之前讲过列存的思路，现在看 Parquet 文件**真实的物理结构**。

一个 Parquet 文件从上到下：

```
┌──────────────────────────────────────────┐
│ File Header (4 字节 "PAR1")              │
├──────────────────────────────────────────┤
│ Row Group 1 (默认 128MB)                  │  ← 大块
│  ┌────────────────────────────────────┐  │
│  │ Column Chunk: user_id              │  │  ← 一列的数据
│  │  ┌──────────────────────────────┐  │  │
│  │  │ Page 1 (默认 1MB)            │  │  │  ← 最小读取单位
│  │  │  - 字典编码 / RLE / Bit-pack │  │  │
│  │  │  - 内部 ZSTD/Snappy 压缩     │  │  │
│  │  └──────────────────────────────┘  │  │
│  │  Page 2, Page 3 ...                │  │
│  └────────────────────────────────────┘  │
│  ┌────────────────────────────────────┐  │
│  │ Column Chunk: country              │  │
│  └────────────────────────────────────┘  │
│  ┌────────────────────────────────────┐  │
│  │ Column Chunk: amount               │  │
│  └────────────────────────────────────┘  │
├──────────────────────────────────────────┤
│ Row Group 2 ...                          │
├──────────────────────────────────────────┤
│ File Footer                              │  ← 元数据：
│  - schema                                │     每个 Row Group 每列
│  - Row Group 列表 + 每列 min/max/null    │     的统计信息
│  - 偏移量索引                            │
├──────────────────────────────────────────┤
│ File Footer Length (4 字节)              │
│ Magic "PAR1"                             │
└──────────────────────────────────────────┘
```

**关键结构记三层**：File → Row Group → Column Chunk → Page

### 这个结构让查询能做的 4 件事

**① 列裁剪 (Projection Pushdown)**
SQL 只要 amount → 引擎只读 amount 那几个 Column Chunk，其他列完全不碰。

**② 谓词下推 (Predicate Pushdown)**
SQL `WHERE country='US'` → 引擎先读 footer，看每个 Row Group 的 country min/max。
如果某个 Row Group 的 country 范围是 'CN' 到 'JP'，**直接跳过这个 Row Group**。

**③ 编码省空间**
- **字典编码**：country 只有 10 个国家 → 字典 {0:US, 1:CN, ...}，原数据存数字 0/1/2
- **RLE (Run-Length)**：连续 1000 个 'US' → 存成 (US, 1000)
- **Bit Packing**：值域小的整数用更少 bit 存

**④ 压缩**
Page 级别再叠一层 ZSTD/Snappy，因为列内同质数据**压缩率极高**（可达 5-10 倍）。

---

## 1.2 对象存储 (MinIO) vs 本地盘 vs HDFS

| | 本地盘 | HDFS | 对象存储 (S3/MinIO/OSS) |
|---|---|---|---|
| 接口 | POSIX (open/read/write) | HDFS API | HTTP REST |
| 扩展性 | 单机磁盘大小 | 集群规模上限 (~100PB) | **几乎无限** |
| 价格 | 高（机器 + 维护） | 中（要养集群） | **极低**（S3 标准 ~$23/TB/月） |
| 延迟 | 微秒 | 毫秒 | **几十毫秒** |
| 元数据 | 文件系统管 | NameNode（单点瓶颈） | 对象 KV 管 |
| 一致性 | 强 | 强 | 强（2020 年后的 S3） |
| 典型用法 | 开发 / 小数据 | Hadoop 老集群 | **现代湖仓默认** |

**为什么云时代对象存储赢了？**

1. **存算分离**：计算节点可以随时加 / 减 / 关，数据不动
   - HDFS 是存算一体，关一个节点要数据迁移
2. **多个引擎共享同一份数据**：S3 上的 Parquet，Spark / Trino / Flink / Snowflake 都能读
3. **便宜得离谱**：HDFS 至少 3 副本，S3 也是冗余但单价低
4. **运维简单**：没有 NameNode / DataNode / ZK 那一坨

**MinIO 是什么？**
- 一个**自己部署的 S3**，协议跟 AWS S3 100% 兼容
- 在本地用 Docker 起来，写代码就跟连真实 S3 一样
- 阿里云 OSS、AWS S3 都是这个接口

---

## 1.3 本阶段两个对照实验

### 实验 1：CSV vs Parquet（在本地盘上）

固定数据：100 万行模拟埋点

| 格式 | 文件大小 | 全表扫描 | 单列汇总 `SUM(amount)` | 带过滤 `WHERE country='US'` |
|---|---|---|---|---|
| CSV |  ?  |  ?  |  ?  |  ?  |
| CSV.gz |  ?  |  ?  |  ?  |  ?  |
| Parquet (Snappy) |  ?  |  ?  |  ?  |  ?  |
| Parquet (ZSTD) |  ?  |  ?  |  ?  |  ?  |

**预期结论**：Parquet+ZSTD 体积最小、`SUM` 和 `WHERE` 最快（数量级差异）。

### 实验 2：本地 Parquet vs MinIO (S3) Parquet

把 Parquet 上传到 MinIO，再用 `s3://` 路径读，对比：

- 写入耗时
- 读取耗时
- 网络开销 (本地是 docker bridge，但能体验"远程对象存储"的接口)

**预期结论**：MinIO 略慢（HTTP 比本地 IO 慢），但**列裁剪和谓词下推依然有效**，对象存储不影响 Parquet 的优势。

---

## 1.4 概念检查清单

读完动手前，确认你能答：

- [ ] Parquet 文件的三层结构是什么？
- [ ] "列裁剪"和"谓词下推"分别在干什么？
- [ ] 为什么列存压缩率比行存高？
- [ ] 对象存储和 HDFS 最大的设计差别是什么？
- [ ] MinIO 和 AWS S3 是什么关系？

---

## 1.5 实操步骤

详见项目根目录的 `README` 和 `data-gen/`、`docker-compose/01-storage.yml`。

简要：
1. `docker compose -f docker-compose/01-storage.yml up -d` → 起 MinIO
2. `python3 data-gen/generate_events.py` → 生成 100 万条模拟事件
3. `python3 data-gen/exp01_format_comparison.py` → 跑格式对照实验
4. `python3 data-gen/exp02_minio_upload.py` → 跑 MinIO 对照实验
5. 看结果，更新 `experiments/01-columnar-storage-results.md`

---

## 1.6 阶段产出

- [ ] 跑通两个实验，记录真实数字
- [ ] 写一份 `experiments/01-columnar-storage-results.md`，对比预期 vs 实际
- [ ] 能用大白话解释"为什么 Parquet 比 CSV 快 10 倍"
