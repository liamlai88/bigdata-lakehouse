"""
DAG: hello_airflow —— 最小化"hello world"，验证 Airflow 跑起来
======================================================================

3 个 Task 演示:
  1. BashOperator: 跑 shell
  2. PythonOperator: 跑 Python，用 XCom 传值
  3. BashOperator: 读 XCom 的值

UI 看到这个能跑通，就证明 Airflow 环境 OK。
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def compute_lucky_number(**ctx):
    import random

    n = random.randint(1, 100)
    print(f"今天的幸运数字: {n}")
    # 推到 XCom，下游能拿到
    ctx["ti"].xcom_push(key="lucky_number", value=n)


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 5, 18),
    schedule=None,  # 不自动调度，只能手动触发
    catchup=False,
    tags=["demo", "hello"],
) as dag:
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo '🎉 Airflow is alive!' && date",
    )

    compute = PythonOperator(
        task_id="compute_lucky_number",
        python_callable=compute_lucky_number,
    )

    read_xcom = BashOperator(
        task_id="read_lucky_number",
        bash_command=(
            "echo 'XCom 拿到的幸运数字: "
            '{{ ti.xcom_pull(task_ids="compute_lucky_number", key="lucky_number") }}\''
        ),
    )

    say_hello >> compute >> read_xcom
