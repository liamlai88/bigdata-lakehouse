"""
Superset 配置 — 学习场景最小化
"""

import os

# Flask 加密密钥 (生产必须是随机字符串)
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY", "lakehouse-learning-key-do-not-use-in-prod"
)

# 元数据数据库
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:superset@postgres-superset:5432/superset"
)

# 关掉一些非必需 feature
FEATURE_FLAGS = {
    "DASHBOARD_RBAC": False,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# 允许在 SQL Lab 跑 CREATE TABLE 等（学习场景）
SQLLAB_CTAS_NO_LIMIT = True
