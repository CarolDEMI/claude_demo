#!/usr/bin/env python3
"""
手动数据导入脚本
当无法自动连接Presto时，可以手动执行SQL并导入结果
"""
import sqlite3
import pandas as pd
import json
from datetime import datetime
import os

class ManualDataImporter:
    def __init__(self, local_db_path: str = "./data.db"):
        self.local_db_path = local_db_path
    
    def import_from_csv(self, csv_file_path: str, table_name: str = "cpz_qs_newuser_channel_i_d") -> bool:
        """从CSV文件导入数据"""
        if not os.path.exists(csv_file_path):
            print(f"❌ 文件不存在: {csv_file_path}")
            return False
        
        try:
            # 读取CSV文件
            print(f"📖 读取CSV文件: {csv_file_path}")
            df = pd.read_csv(csv_file_path)
            print(f"✅ 成功读取 {len(df)} 条记录")
            
            # 显示数据结构
            print(f"📊 数据结构:")
            print(f"  - 列数: {len(df.columns)}")
            print(f"  - 列名: {list(df.columns)}")
            
            # 导入到SQLite
            return self._import_dataframe(df, table_name)
            
        except Exception as e:
            print(f"❌ CSV导入失败: {e}")
            return False
    
    def import_from_json(self, json_file_path: str, table_name: str = "cpz_qs_newuser_channel_i_d") -> bool:
        """从JSON文件导入数据"""
        if not os.path.exists(json_file_path):
            print(f"❌ 文件不存在: {json_file_path}")
            return False
        
        try:
            # 读取JSON文件
            print(f"📖 读取JSON文件: {json_file_path}")
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 转换为DataFrame
            df = pd.DataFrame(data)
            print(f"✅ 成功读取 {len(df)} 条记录")
            
            # 导入到SQLite
            return self._import_dataframe(df, table_name)
            
        except Exception as e:
            print(f"❌ JSON导入失败: {e}")
            return False
    
    def _import_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """将DataFrame导入到SQLite"""
        try:
            conn = sqlite3.connect(self.local_db_path)
            
            # 检查数据日期范围
            if 'dt' in df.columns:
                dates = df['dt'].unique()
                print(f"📅 数据日期范围: {min(dates)} 到 {max(dates)}")
                
                # 清理相同日期的现有数据
                cursor = conn.cursor()
                for date in dates:
                    cursor.execute(f"DELETE FROM {table_name} WHERE dt = ?", [date])
                    print(f"🗑️ 清理 {date} 的旧数据")
                conn.commit()
            
            # 导入新数据
            df.to_sql(table_name, conn, if_exists='append', index=False)
            
            conn.close()
            print(f"✅ 成功导入 {len(df)} 条记录到 {table_name}")
            return True
            
        except Exception as e:
            print(f"❌ 数据库导入失败: {e}")
            return False
    
    def create_sample_instructions(self):
        """创建手动操作说明"""
        instructions = """
# 手动数据导入说明

## 方法1: 通过DataGrip导出CSV

1. 在DataGrip中打开SQL控制台
2. 执行 cpz_qs_newuser_channel_i_d.sql 脚本
3. 右键查询结果 -> Export Data -> CSV
4. 保存为 data_export.csv
5. 运行: python3 manual_data_import.py --csv data_export.csv

## 方法2: 手动配置Presto连接

1. 编辑 presto_config.py
2. 填入正确的连接参数:
   - host: Presto服务器地址
   - port: 端口（通常8080）
   - catalog: 数据目录
   - schema: 数据库schema（da）
   - user: 用户名

## 方法3: 使用现有模拟数据

如果线上数据暂时无法访问，可以使用模拟数据继续开发:
1. 运行: python3 src/data_updater.py
2. 生成更多模拟数据进行测试

## 示例命令

```bash
# 导入CSV文件
python3 manual_data_import.py --csv /path/to/export.csv

# 导入JSON文件  
python3 manual_data_import.py --json /path/to/export.json

# 生成更多模拟数据
python3 src/data_updater.py
```
"""
        
        with open("data_import_instructions.md", "w") as f:
            f.write(instructions)
        
        print("📄 操作说明已创建: data_import_instructions.md")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="手动数据导入工具")
    parser.add_argument("--csv", help="CSV文件路径")
    parser.add_argument("--json", help="JSON文件路径")
    parser.add_argument("--table", default="cpz_qs_newuser_channel_i_d", help="目标表名")
    parser.add_argument("--instructions", action="store_true", help="生成操作说明")
    
    args = parser.parse_args()
    
    importer = ManualDataImporter()
    
    if args.instructions:
        importer.create_sample_instructions()
        return
    
    if args.csv:
        success = importer.import_from_csv(args.csv, args.table)
    elif args.json:
        success = importer.import_from_json(args.json, args.table)
    else:
        print("请指定要导入的文件类型 (--csv 或 --json)")
        print("或使用 --instructions 查看详细说明")
        return
    
    if success:
        print("🎉 数据导入完成！")
        print("现在可以运行工作流分析最新数据:")
        print("  python3 src/workflow.py")
    else:
        print("❌ 数据导入失败")

if __name__ == "__main__":
    main()