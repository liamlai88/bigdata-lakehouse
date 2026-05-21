# 阶段 6：Airflow 调度

> 阶段 5 跑完，本机已经有 6 个数据引擎服务。但工程问题没解决：
> **谁来定时跑 Spark ETL？谁知道 DWD 跑完才能跑 DWS？挂了谁重跑？**
>
> 这是 Airflow 干的事。本阶段比阶段 5 简单，1-2 小时收尾。

---

## 6.1 调度系统要解决什么

### 没有调度器的"原始时代"

凌晨 3 点：crontab 启动 ods.sh
凌晨 3:30：crontab 启动 dwd.sh
凌晨 4:00：crontab 启动 dws.sh
...

**问题**：
1. **依赖没法表达**：dwd 必须等 ods 跑完才能跑，但 crontab 只有"时间"概念，没有"依赖"
2. **失败不知道**：ods 凌晨 3:15 挂了，dwd 仍然 3:30 启动 → 读到不全数据 → 错乱
3. **重跑很痛**：3 月 12 日 ods 算错了，要修复 3 月 12 日 ~ 今天所有下游 → 手动一个一个跑
4. **没有可视化**：100 个作业的依赖关系，全靠工程师脑子记
5. **重试 / 报警 / 历史日志** 全要自己写

### 调度器的核心抽象

**Airflow / DolphinScheduler / DataWorks 都基于同一套抽象**：

```
DAG (Directed Acyclic Graph)
├── Task: ods_to_dwd
├── Task: dwd_to_dws  (depends on ods_to_dwd)
├── Task: dws_to_ads  (depends on dwd_to_dws)
└── Task: send_report (depends on dws_to_ads)
```

**5 个核心概念**：
| 概念 | 中文 | 干什么 |
|---|---|---|
| DAG | 有向无环图 | 一个完整工作流，里面有任务和依赖 |
| Task | 任务 | DAG 中的一个步骤 |
| Operator | 算子 | Task 的"种类"（BashOperator, PythonOperator, SparkSubmitOperator, …）|
| Scheduler | 调度器 | 决定 Task 什么时候运行 |
| Executor | 执行器 | 实际跑 Task 的进程 |

---

## 6.2 Airflow 架构

```
                    ┌────────────────────────┐
                    │   Webserver (UI)       │  ◄─ 看 DAG、看运行、手动触发
                    │   :8090                 │
                    └──────────┬─────────────┘
                               │
                  ┌────────────┴─────────────┐
                  ▼                          ▼
       ┌────────────────────┐     ┌─────────────────┐
       │  Scheduler         │     │  PostgreSQL     │  ◄─ 元数据库：
       │  - 扫描 DAGs        │◄───►│  (metadata)     │     DAG 定义、Task 状态、
       │  - 决定该跑哪个 Task │     │                  │     运行历史、用户、权限
       └─────────┬──────────┘     └─────────────────┘
                 │
                 ▼
       ┌────────────────────┐
       │  Executor          │  ◄─ 把 Task 派给 worker
       │  (Local / Celery / │     单机用 LocalExecutor，
       │   Kubernetes)      │     大集群用 CeleryExecutor / K8sExecutor
       └─────────┬──────────┘
                 │
                 ▼
       ┌────────────────────┐
       │  Worker (Task 执行) │  ◄─ 真正跑 Task 的进程
       │  - BashOperator    │     调用 bash / docker / ssh / spark-submit
       │  - PythonOperator  │
       │  - SparkOperator   │
       └────────────────────┘
```

---

## 6.3 DAG 长什么样（Python 代码）

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ecom_lakehouse_etl",
    start_date=datetime(2026, 5, 1),
    schedule="0 3 * * *",          # 每天凌晨 3 点跑（cron）
    catchup=False,                  # 不补跑历史
    default_args={"retries": 2},
) as dag:

    ods = BashOperator(
        task_id="build_ods",
        bash_command="docker exec lakehouse-spark spark-submit /jobs/etl_step1_ods.py",
    )

    dwd = BashOperator(
        task_id="build_dwd",
        bash_command="docker exec lakehouse-spark spark-submit /jobs/etl_step2_dwd.py",
    )

    dws = BashOperator(
        task_id="build_dws",
        bash_command="docker exec lakehouse-spark spark-submit /jobs/etl_step3_dws.py",
    )

    ads = BashOperator(
        task_id="build_ads",
        bash_command="docker exec lakehouse-spark spark-submit /jobs/etl_step4_ads.py",
    )

    ods >> dwd >> [dws, ads]   # 依赖：ods → dwd → (dws 和 ads 并行)
```

**关键认知**：
- DAG 就是普通 Python 文件，**Airflow 周期性扫描 dags/ 目录**自动加载
- `>>` 运算符表达依赖：`A >> B` = B depends on A
- `schedule="0 3 * * *"` 是 cron 语法（也支持 `@daily`）
- 任务跑挂了，Airflow 按 `retries` 自动重试

---

## 6.4 关键运维概念

### Backfill（回溯执行）
3 月 12 日数据算错了，要补 3/12 ~ 今天 70 天的数据：
```bash
airflow dags backfill -s 2026-03-12 -e 2026-05-21 ecom_lakehouse_etl
```
Airflow 会按依赖关系**并行 / 顺序补 70 次完整 DAG 跑**。这功能在生产是命根子。

### Catchup
DAG `start_date=2026-01-01`，今天 5 月 21 日才上线 → catchup=True 会跑 141 次历史。
**生产几乎都设 `catchup=False`**，避免上线就被历史回填淹没。

### XCom（任务间传值）
Task A 算出某个数（如行数 1000000），Task B 想拿到 → 用 XCom：
```python
def task_a(**ctx):
    ctx['ti'].xcom_push(key='row_count', value=1_000_000)
def task_b(**ctx):
    n = ctx['ti'].xcom_pull(key='row_count', task_ids='task_a')
```
小数据用 XCom，大数据用共享存储（HDFS / S3）。

### Sensor（等待外部事件）
等 Kafka 有数据、等文件到达 S3、等上游表更新：
```python
S3KeySensor(task_id='wait_for_file', bucket_key='s3://...', poke_interval=60)
```

---

## 6.5 本阶段架构 + 实操方案

```
┌──────────────────────────────────────────────┐
│  Airflow (本阶段新增)                          │
│  ├─ postgres-airflow (元数据库)                │
│  ├─ airflow-init (一次性建表 + 建用户)         │
│  ├─ airflow-webserver :8090                   │
│  └─ airflow-scheduler                         │
└───────────────┬──────────────────────────────┘
                │
                │ docker exec 调用 spark-submit
                ▼
┌──────────────────────────────────────────────┐
│  Spark + Iceberg + MinIO + Trino (已有)       │
│  Airflow 编排，跑前面阶段的 ETL                 │
└──────────────────────────────────────────────┘
```

**关键设计选择**：Airflow 在自己的容器里，**通过挂载 `/var/run/docker.sock` 调用宿主机的 docker**，从而 exec 进 Spark 容器跑作业。这是本地开发的标准模式。

---

## 6.6 本阶段两个实验

### 实验 11：基础 DAG —— 跑通 ETL 调度
- 把阶段 3 的 ODS/DWD/DWS/ADS 四个 Spark 步骤包装成 4 个 Task
- 在 Airflow UI 看 DAG 图、看运行、看日志
- 手动触发一次 + 观察依赖执行

### 实验 12：Backfill —— 体验回溯
- 把 `start_date` 设到 5 天前
- 用 `airflow dags backfill` 跑历史
- 看 Airflow 如何按依赖关系 + schedule 串行 / 并行跑 5 个历史 run

---

## 6.7 概念检查（动手前）

- [ ] 为什么 crontab 不够用？至少说出两个理由。
- [ ] DAG / Task / Operator / Scheduler / Executor 各是什么？
- [ ] `catchup=True` vs `False` 有什么区别？
- [ ] Backfill 解决什么问题？
- [ ] XCom 适合传什么数据？不适合传什么？

---

## 6.8 阿里云 / 业界映射

| 这里学的 | 阿里云 | AWS | 字节 |
|---|---|---|---|
| Airflow | **DataWorks** | MWAA (Managed Airflow) | 飞行 (类似) |
| DAG | 工作流 | 同 | 同 |
| Sensor | 触发器 | 同 | 同 |
| 调度引擎 | DataWorks 调度 | Step Functions | 飞行 |

**SA 视角**：阿里云的 DataWorks 是一站式数据中台，但调度内核思想跟 Airflow 完全一致。学会 Airflow → 切到 DataWorks 几小时上手。
