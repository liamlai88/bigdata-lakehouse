# 阶段 2：Iceberg 表格式

> 一句话回顾：**Parquet 是"一个文件怎么存"，Iceberg 是"一堆文件怎么变成一张表"。**
>
> 本阶段你会在 MinIO 上建一张 Iceberg 表，亲手验证 ACID / Schema Evolution / Time Travel / Row-level Delete 四大能力。

---

## 2.1 三层元数据结构（要会画）

每次查询 Iceberg 表，引擎都按这个顺序走：

```
                  ┌─────────────────────────────┐
                  │  Catalog (REST / Hive / Glue)│   ← 表名 → 当前 metadata 指针
                  │  events → metadata-v3.json   │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  ┌─────────────────────────────┐
                  │  metadata.json (表级)        │   ← schema、分区规则、
                  │  - schema (含字段 ID)        │     所有历史 snapshot 列表
                  │  - partition spec            │
                  │  - snapshot list             │
                  └──────────────┬───────────────┘
                                 │ current snapshot
                                 ▼
                  ┌─────────────────────────────┐
                  │  manifest list (snapshot 级) │   ← 这个版本由哪些 manifest 组成
                  │  snap-xxx-1-yyy.avro         │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  ┌─────────────────────────────┐
                  │  manifest (文件清单级)       │   ← 每个文件的统计：
                  │  xxx-m0.avro                 │     路径、行数、每列 min/max
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  ┌─────────────────────────────┐
                  │  data files (.parquet)       │   ← 真正的数据
                  └─────────────────────────────┘
```

**关键认知**：Catalog 是入口的"门牌指针"，Iceberg 真正的元数据是放在对象存储上的一堆 JSON / Avro 文件 —— 这跟 Hive Metastore 把元数据塞进 MySQL 形成鲜明对比。

---

## 2.2 本阶段架构

```
┌─────────────┐     ┌────────────────────┐
│ pyiceberg   │────►│ Iceberg REST       │  ← 给客户端的统一 API
│ (Python)    │     │ Catalog (Docker)   │
└─────────────┘     └──────────┬─────────┘
                               │ 元数据 + 数据
                               ▼
                    ┌────────────────────┐
                    │  MinIO (S3)        │  ← 数据 + 元数据都放这
                    │  bucket: warehouse │
                    └────────────────────┘
```

为什么选 REST Catalog？
- Iceberg 原生有多种 catalog 实现：Hive、JDBC、Glue、REST
- **REST 是社区主推**，引擎中立、扩展性好、阿里云 DLF 也走这条路
- 学完 REST，迁到生产环境的 DLF / AWS Glue 几乎无感

---

## 2.3 本阶段两个实验

### 实验 3：Iceberg 基础 (`exp03_iceberg_basics.py`)
1. 连 REST Catalog，创建 namespace + 表
2. 把上一阶段生成的 Parquet 数据写进去
3. **去 MinIO 控制台**看实际的目录结构：data/ 和 metadata/ 两个子目录
4. 用 pyiceberg API 查询，验证读取正确
5. 看一眼 snapshot 列表

### 实验 4：Iceberg 四大超能力 (`exp04_iceberg_superpowers.py`)
1. **Schema 演进**：`ADD COLUMN platform STRING` → 旧数据自动 null，新数据有值
2. **Time Travel**：分批写入产生 3 个 snapshot，按 snapshot-id 查历史版本
3. **Row-level Delete**：`DELETE WHERE country='SG'`，看 MinIO 上多出一个 `delete-*.parquet`，**原数据文件一行没改**
4. **Rollback**：回滚到删之前的 snapshot

跑完你会看到 MinIO 上文件这样演变：

```
warehouse/events/
├── data/
│   ├── 00000-0-xxx.parquet           ← 第 1 次 append
│   ├── 00001-0-xxx.parquet           ← 第 2 次 append
│   └── 00002-0-xxx-delete.parquet    ← 删除标记，不动数据
└── metadata/
    ├── v1.metadata.json
    ├── v2.metadata.json              ← 每次操作产生新版本
    ├── v3.metadata.json
    ├── snap-xxx-1-yyy.avro           ← manifest list
    ├── xxx-m0.avro                   ← manifest
    └── ...
```

---

## 2.4 概念检查（动手前）

- [ ] 三层元数据是哪三层？
- [ ] Iceberg 的"原子提交"靠什么实现？（提示：Catalog 指针的 CAS）
- [ ] Schema Evolution 为什么能在不动数据的情况下改字段名？（提示：字段 ID）
- [ ] Row-level delete 不重写数据文件，那读的时候怎么知道哪些行被删了？

跑完两个实验你应该都能答上。

---

## 2.5 实操步骤

```bash
# 1. 关掉只有 MinIO 的旧 compose
docker compose -f docker-compose/01-storage.yml down

# 2. 起新 compose（MinIO + REST Catalog）
docker compose -f docker-compose/02-iceberg.yml up -d

# 3. 装新依赖
source venv/bin/activate
pip install -r requirements.txt    # 已加入 pyiceberg

# 4. 跑实验
python3 jobs/iceberg/exp03_iceberg_basics.py
python3 jobs/iceberg/exp04_iceberg_superpowers.py
```

跑完到 MinIO 控制台 http://localhost:9001 看 `warehouse/` bucket 的文件树，对照本文的 ASCII 图。
