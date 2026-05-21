# 实验 6：Airflow 调度 实证报告

> 日期：2026-05-21
> 工具：Airflow 2.10.4 + LocalExecutor + PostgreSQL (元数据库)

---

## 一、核心发现（一句话）

**把前面阶段散落的 4 个 Spark 步骤组织成一个有依赖、能重试、可视化的 DAG —— 这就是工程化的临界点：从"能跑"到"能上生产"。**

---

## 二、架构

```
┌─────────────────────┐
│  Airflow 一套服务    │
│  ├─ Webserver:8090  │
│  ├─ Scheduler        │
│  ├─ PostgreSQL       │
│  └─ airflow-init     │
└──────┬──────────────┘
       │ docker exec (via mounted /var/run/docker.sock)
       ▼
┌──────────────────────┐
│ lakehouse-spark      │  ← Airflow Task 跑这里
│ /opt/spark/bin/...   │
└──────────────────────┘
```

**关键设计**：Airflow 容器挂宿主机 `docker.sock` + 装 `docker-ce-cli`，从而能 `docker exec` 调 spark 容器。这是本地 / 开发环境的标准模式。生产用 SparkSubmitOperator + Spark on K8s 或 Yarn。

---

## 三、DAG 设计

`ecom_lakehouse_etl` —— 每天凌晨 3 点跑：

```
start → build_ods → build_dwd ─┬─► build_dws ─┐
                                └─► build_ads ─┴─► verify_ads → end
```

| Task | Operator | 跑什么 |
|---|---|---|
| start | EmptyOperator | 入口标记 |
| build_ods | BashOperator → docker exec spark | 源 Parquet → `dw.ods_events` (按 dt 分区) |
| build_dwd | BashOperator → docker exec spark | ODS + 维度 (broadcast join) → `dw.dwd_user_action` |
| build_dws | BashOperator → docker exec spark | 按 user × dt 聚合 → `dw.dws_user_daily` |
| build_ads | BashOperator → docker exec spark | 按 country × category × dt 漏斗 + GMV → `dw.ads_funnel_daily` |
| verify_ads | BashOperator → docker exec trino | 查 ADS 行数验证 |
| end | EmptyOperator | 出口标记 |

**重点设计选择**：DWS 和 DWD 都依赖 DWD，**两者并行**而不是串行。Airflow 用 `dwd >> [dws, ads]` 自动实现 fan-out。

---

## 四、实验 11：基础 DAG 跑通

### 运行结果
- 5 个 Task 全部 success
- 总耗时 2-3 分钟
- `verify_ads` 输出 1086 行（每天 × 国家 × 品类的笛卡尔积，实际命中约 60%）

### 验证的能力

| 能力 | 在本实验中的体现 |
|---|---|
| **依赖管理** | DWD 必须等 ODS 完成；DWS/ADS 必须等 DWD |
| **并行执行** | DWS 和 ADS fan-out 同时跑 |
| **失败重试** | `default_args.retries=2` 自动重试 2 次 |
| **可视化** | Graph 视图实时看 Task 颜色 |
| **历史日志** | 每个 Task 的 stdout/stderr 永久保存 |
| **跨容器调度** | Airflow 容器 → docker exec → Spark/Trino 容器 |

---

## 五、关键概念实测

### DAG 自动加载
DAG Python 文件丢进 `airflow/dags/` 目录，Scheduler 每 10 秒扫描自动加载（compose 配置 `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=10`）。无需重启服务。

### Hello World DAG（XCom 演示）
另一个 `hello_airflow` DAG 演示 Task 间传值：
```python
def compute_lucky_number(**ctx):
    ctx["ti"].xcom_push(key="lucky_number", value=random.randint(1, 100))
# 下游 Task:
{{ ti.xcom_pull(task_ids="compute_lucky_number", key="lucky_number") }}
```
**XCom 设计原则**：传小数据（数字、路径、参数），大数据走共享存储（S3 / HDFS）。

### 为什么需要调度器（vs crontab）
本实验亲证了 crontab 做不到的事：
1. **依赖**：crontab 只有"几点几分"，没有"等谁跑完"
2. **重试**：crontab 失败不重试
3. **可视化**：crontab 100 个任务依赖关系全靠脑子
4. **回溯**：crontab 无 backfill，补历史靠手敲
5. **状态查询**：crontab 没有"昨天的任务跑没跑"

---

## 六、踩坑记录

| 坑 | 现象 | 解决 |
|---|---|---|
| Airflow 默认 8080 跟 Trino 撞 | webserver 端口冲突 | compose 映射成 8090:8080 |
| DAG 容器看不到 spark 容器 | `docker exec` 报"no such container" | 挂载 `/var/run/docker.sock` + 装 docker CLI |
| Airflow 健康检查超时 | webserver 启动慢，scheduler 报 unhealthy | start_period 给 30s+ |
| catchup=True 上线就被淹 | 142 天历史一起触发 | 学习场景设 catchup=False |

---

## 七、阿里云 / 业界映射

| 这里学的 | 阿里云 | AWS |
|---|---|---|
| Airflow DAG | **DataWorks 工作流** | MWAA |
| BashOperator | DataWorks ODPS 节点 | Step Functions task |
| Scheduler cron | DataWorks 调度配置 | EventBridge rule |
| 跨集群依赖 | DataWorks 跨项目依赖 | Step Functions cross-account |

**SA 视角金句**：
> "DataWorks 不是黑盒，调度内核跟 Airflow 完全同构。客户从 DolphinScheduler / Airflow 自建迁 DataWorks，DAG 概念一一对应，迁移成本只有 SQL 改写不是流程改写。"

---

## 八、可复现性

```bash
# 1. 起 Airflow + 已有的 MinIO/REST/Spark/Trino
docker compose -f docker-compose/06-airflow.yml up -d --build

# 2. UI: http://localhost:8090 (admin / admin)

# 3. 先跑 hello_airflow 验证环境（3 个 Task 全绿）

# 4. 再触发 ecom_lakehouse_etl（5 个 Task 全绿，约 2-3 分钟）
```

---

## 九、结论

✅ Airflow 跑通端到端 DAG，5 个 Task 全绿，2-3 分钟
✅ Fan-out 并行（DWS 和 ADS）正确触发
✅ 跨容器调度（Airflow → Spark/Trino）通过 docker socket 标准模式实现
✅ DAG / Operator / XCom / Scheduler / 重试机制全部实测
✅ 跟 DataWorks / MWAA 同构，迁移路径清晰

**最大认知收获**：调度器是数据工程的"基础设施级"工具。前面 5 个阶段是"能跑"，加上 Airflow 才是"能上生产"——这是 SDE 和 Data Engineer 的本质区别之一。
