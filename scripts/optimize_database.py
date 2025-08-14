#!/usr/bin/env python3
"""
数据库优化脚本
创建索引以提升查询性能
"""

import sqlite3
import sys
import os
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from config import load_config

def optimize_database(db_path: str):
    """优化数据库索引"""
    print(f"正在优化数据库: {db_path}")
    
    if not os.path.exists(db_path):
        print(f"数据库文件不存在: {db_path}")
        return False
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 获取所有表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        # 创建索引的SQL语句
        index_queries = [
            # cpz_qs_newuser_channel_i_d 表的索引
            "CREATE INDEX IF NOT EXISTS idx_cpz_dt ON cpz_qs_newuser_channel_i_d(dt)",
            "CREATE INDEX IF NOT EXISTS idx_cpz_channel ON cpz_qs_newuser_channel_i_d(channel)",
            "CREATE INDEX IF NOT EXISTS idx_cpz_dt_channel ON cpz_qs_newuser_channel_i_d(dt, channel)",
            "CREATE INDEX IF NOT EXISTS idx_cpz_status ON cpz_qs_newuser_channel_i_d(status)",
            "CREATE INDEX IF NOT EXISTS idx_cpz_verification ON cpz_qs_newuser_channel_i_d(verification_status)",
            
            # 用户行为表索引 (如果存在)
            "CREATE INDEX IF NOT EXISTS idx_user_behavior_date ON user_behavior(date)",
            "CREATE INDEX IF NOT EXISTS idx_user_behavior_user_id ON user_behavior(user_id)",
            
            # 渠道数据表索引 (如果存在)
            "CREATE INDEX IF NOT EXISTS idx_channel_data_date ON channel_data(date)",
            "CREATE INDEX IF NOT EXISTS idx_channel_data_channel ON channel_data(channel_name)",
            
            # 异常记录表索引 (如果存在)
            "CREATE INDEX IF NOT EXISTS idx_anomaly_date ON anomaly_records(detection_date)",
            "CREATE INDEX IF NOT EXISTS idx_anomaly_type ON anomaly_records(anomaly_type)",
        ]
        
        # 执行索引创建
        success_count = 0
        for query in index_queries:
            try:
                cursor.execute(query)
                success_count += 1
                table_name = query.split("ON ")[1].split("(")[0].strip()
                index_name = query.split("INDEX IF NOT EXISTS ")[1].split(" ON")[0]
                print(f"✓ 创建索引: {index_name} on {table_name}")
            except sqlite3.OperationalError as e:
                # 表可能不存在，跳过
                continue
            except Exception as e:
                print(f"✗ 创建索引失败: {e}")
        
        # 执行数据库优化命令
        print("\n执行数据库优化...")
        cursor.execute("ANALYZE")
        cursor.execute("VACUUM")
        
        conn.commit()
        print(f"\n✅ 数据库优化完成! 成功创建 {success_count} 个索引")
        
        # 显示数据库统计信息
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index'")
        total_indexes = cursor.fetchone()[0]
        print(f"📊 数据库总索引数: {total_indexes}")
        
        # 获取数据库大小
        db_size = os.path.getsize(db_path) / (1024 * 1024)  # MB
        print(f"💾 数据库大小: {db_size:.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"❌ 优化失败: {e}")
        return False
    finally:
        conn.close()

def main():
    """主函数"""
    # 获取数据库配置
    db_config = load_config("database")
    db_path = db_config.get("path", "./data/data.db")
    
    # 优化主数据库
    print("=" * 60)
    print("🔧 数据库索引优化工具")
    print("=" * 60)
    
    # 优化所有数据库文件
    data_dir = Path("./data")
    db_files = list(data_dir.glob("*.db"))
    
    print(f"\n发现 {len(db_files)} 个数据库文件")
    
    for db_file in db_files:
        print(f"\n{'='*40}")
        optimize_database(str(db_file))
    
    print("\n" + "=" * 60)
    print("✨ 所有数据库优化完成!")

if __name__ == "__main__":
    main()