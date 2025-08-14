
# Presto连接配置 - remote模板
# 请根据实际环境修改参数

PRESTO_CONFIG = {
    "host": "10.1.11.39",
    "port": 8000,
    "catalog": "hive",
    "schema": "da",
    "user": "luoruying",
    "auth": None,
}

# 使用说明:
# 1. 修改host为实际的Presto服务器地址
# 2. 确认port、catalog、schema正确
# 3. 运行测试: python3 src/presto_sync.py
