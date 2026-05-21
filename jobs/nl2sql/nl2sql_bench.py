"""
NL2SQL 对照实验：Zero-shot vs Few-shot vs RAG
==============================================

跑 8 道测试题 × 3 种范式 = 24 次评测，输出对照表。

评分:
  ✅ exec_ok    SQL 能跑通
  ✅ row_match  行数跟参考 SQL 一致 (粗筛)
  ⏱️ latency_ms LLM 调用延迟
  💰 tokens     prompt + completion 总 token

依赖:
  - DASHSCOPE_API_KEY 环境变量已设
  - Trino 在 localhost:8080，iceberg.dw.* 表已建好 (跑过 exp05)
  - pip install: dashscope trino
"""

import os
import json
import time
import re
from pathlib import Path

import dashscope
from dashscope import Generation
import trino

# ─────────────────────────────────────────────
# 配置
# ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_DOC = (PROJECT_ROOT / "jobs/nl2sql/schema_context.md").read_text()
TEST_DATA = json.loads((PROJECT_ROOT / "jobs/nl2sql/test_queries.json").read_text())

dashscope.api_key = os.environ["DASHSCOPE_API_KEY"]
MODEL = "qwen-turbo"


# Trino 连接
def get_trino_cur():
    conn = trino.dbapi.connect(
        host="localhost",
        port=8080,
        user="nl2sql_bench",
        catalog="iceberg",
        schema="dw",
    )
    return conn.cursor()


# ─────────────────────────────────────────────
# 三种范式的 prompt 构造
# ─────────────────────────────────────────────
def build_zero_shot(question: str) -> str:
    return f"""你是 Trino SQL 专家。
任务：根据下面的表 schema，把用户问题翻译成一段可执行的 Trino SQL。

{SCHEMA_DOC}

用户问题: {question}

只输出 SQL，不要解释，不要 markdown 代码块。"""


def build_few_shot(question: str) -> str:
    examples = "\n".join(
        f"问: {ex['question']}\nSQL: {ex['sql']}\n"
        for ex in TEST_DATA["few_shot_examples"]
    )
    return f"""你是 Trino SQL 专家。
任务：根据下面的表 schema 和示例，把用户问题翻译成一段可执行的 Trino SQL。

{SCHEMA_DOC}

## 示例（参考写法和口径）
{examples}

用户问题: {question}

只输出 SQL，不要解释，不要 markdown 代码块。"""


# 占位 RAG：本机演示版，把 schema 切成 chunks，按问题关键词朴素检索 top-3
# (生产应该用 embedding + 向量库，但本实验是范式对照重点是 prompt 形态)
def build_rag(question: str) -> str:
    sections = re.split(r"\n## ", SCHEMA_DOC)
    # 朴素关键词重合度
    q_words = set(re.findall(r"\w+", question.lower()))
    scored = sorted(
        [(len(q_words & set(re.findall(r"\w+", s.lower()))), s) for s in sections],
        reverse=True,
    )
    top_k = "\n## ".join(s for _, s in scored[:3])
    examples = "\n".join(
        f"问: {ex['question']}\nSQL: {ex['sql']}\n"
        for ex in TEST_DATA["few_shot_examples"]
    )
    return f"""你是 Trino SQL 专家。
根据下面**最相关的 schema 片段**和示例，回答用户问题。

## 相关 Schema (RAG 检索 top-3)
## {top_k}

## 示例
{examples}

用户问题: {question}

只输出 SQL，不要解释，不要 markdown 代码块。"""


# ─────────────────────────────────────────────
# 调用 LLM
# ─────────────────────────────────────────────
def call_llm(prompt: str):
    t0 = time.perf_counter()
    resp = Generation.call(
        model=MODEL,
        prompt=prompt,
        result_format="message",
    )
    dt = time.perf_counter() - t0
    if resp.status_code != 200:
        return None, dt, 0
    sql = resp.output.choices[0].message.content.strip()
    # 偶尔模型还是会加 markdown 代码块，剥掉
    sql = re.sub(r"^```\w*\n|\n```$", "", sql.strip())
    tokens = resp.usage.total_tokens
    return sql, dt, tokens


# ─────────────────────────────────────────────
# 评分：跑 SQL，看跟参考结果是否一致
# ─────────────────────────────────────────────
def score_sql(generated_sql: str, reference_sql: str):
    cur = get_trino_cur()
    result = {"exec_ok": False, "row_match": False, "rows": 0, "err": None}
    try:
        cur.execute(generated_sql)
        gen_rows = cur.fetchall()
        result["exec_ok"] = True
        result["rows"] = len(gen_rows)

        cur.execute(reference_sql)
        ref_rows = cur.fetchall()
        # 粗筛：行数一致就算 match (严格匹配太苛刻)
        result["row_match"] = len(gen_rows) == len(ref_rows)
    except Exception as e:
        result["err"] = str(e)[:200]
    finally:
        cur.close()
    return result


# ─────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────
def main():
    paradigms = {
        "zero-shot": build_zero_shot,
        "few-shot": build_few_shot,
        "RAG": build_rag,
    }

    rows = []
    for q in TEST_DATA["test_queries"]:
        for paradigm_name, builder in paradigms.items():
            print(f"\n→ Q{q['id']} ({q['difficulty']}⭐) [{paradigm_name}]")
            print(f"   {q['question']}")

            prompt = builder(q["question"])
            sql, latency, tokens = call_llm(prompt)
            if sql is None:
                print("   ❌ LLM call failed")
                continue
            print(f"   SQL: {sql[:120]}...")
            score = score_sql(sql, q["reference_sql"])
            print(
                f"   exec_ok={score['exec_ok']}  row_match={score['row_match']}  "
                f"rows={score['rows']}  latency={latency * 1000:.0f}ms  tokens={tokens}"
            )
            rows.append(
                {
                    "q_id": q["id"],
                    "difficulty": q["difficulty"],
                    "paradigm": paradigm_name,
                    "exec_ok": score["exec_ok"],
                    "row_match": score["row_match"],
                    "rows": score["rows"],
                    "latency_ms": int(latency * 1000),
                    "tokens": tokens,
                    "err": score["err"],
                    "sql": sql,
                }
            )

    # 汇总
    print("\n" + "═" * 75)
    print("📊 对照汇总")
    print("═" * 75)
    for paradigm_name in paradigms:
        sub = [r for r in rows if r["paradigm"] == paradigm_name]
        exec_ok = sum(1 for r in sub if r["exec_ok"])
        match = sum(1 for r in sub if r["row_match"])
        avg_latency = sum(r["latency_ms"] for r in sub) / len(sub)
        avg_tokens = sum(r["tokens"] for r in sub) / len(sub)
        print(
            f"  {paradigm_name:12s}  exec_ok {exec_ok}/{len(sub)}  "
            f"row_match {match}/{len(sub)}  "
            f"avg_latency {avg_latency:.0f}ms  avg_tokens {avg_tokens:.0f}"
        )

    # 存 JSON 给后面写报告用
    out = PROJECT_ROOT / "experiments/07-nl2sql-raw.json"
    out.write_text(json.dumps(rows, indent=2, ensure_ascii=False))
    print(f"\n→ 详细结果存 {out}")


if __name__ == "__main__":
    main()
