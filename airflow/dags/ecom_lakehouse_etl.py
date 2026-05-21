"""
DAG: ecom_lakehouse_etl
========================

每天凌晨 3 点跑一次电商湖仓 ETL 四层：
  ODS → DWD → (DWS, ADS 并行)

依赖外部容器：lakehouse-spark (通过宿主机 docker socket exec)

UI: http://localhost:8090
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="ecom_lakehouse_etl",
    description="电商湖仓 ODS → DWD → DWS/ADS 全链路 ETL (跑在 lakehouse-spark 容器)",
    start_date=datetime(2026, 5, 18),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "etl", "iceberg", "spark"],
) as dag:
    start = EmptyOperator(task_id="start")

    # 通过宿主机 docker socket 调用 spark 容器跑作业
    SPARK_CMD = (
        "docker exec lakehouse-spark "
        "/opt/spark/bin/spark-submit /jobs/etl_steps/{script}"
    )

    ods = BashOperator(
        task_id="build_ods",
        bash_command=SPARK_CMD.format(script="step1_ods.py"),
        doc_md="加载源 Parquet → Iceberg `dw.ods_events` (按 dt 分区)",
    )

    dwd = BashOperator(
        task_id="build_dwd",
        bash_command=SPARK_CMD.format(script="step2_dwd.py"),
        doc_md="ODS + dim_user/dim_item (broadcast join) → `dw.dwd_user_action`",
    )

    dws = BashOperator(
        task_id="build_dws",
        bash_command=SPARK_CMD.format(script="step3_dws.py"),
        doc_md="按 user × dt 聚合 → `dw.dws_user_daily`",
    )

    ads = BashOperator(
        task_id="build_ads",
        bash_command=SPARK_CMD.format(script="step4_ads.py"),
        doc_md="按 country × category × dt 算漏斗 + GMV → `dw.ads_funnel_daily`",
    )

    verify = BashOperator(
        task_id="verify_ads",
        bash_command=(
            "docker exec lakehouse-trino trino --execute "
            '"SELECT COUNT(*) FROM iceberg.dw.ads_funnel_daily"'
        ),
        doc_md="跑 Trino 查 ADS 行数，验证 ETL 成功",
    )

    end = EmptyOperator(task_id="end")

    # 依赖：start → ods → dwd → (dws, ads 并行) → verify → end
    start >> ods >> dwd >> [dws, ads] >> verify >> end
