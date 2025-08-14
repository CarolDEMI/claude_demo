#!/usr/bin/env python3
"""
Presto数据库同步脚本
从线上Presto数据库导入数据到本地SQLite
"""
import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import argparse
import sys

# 添加进度显示支持
try:
    from progress_bar import RealProgressBar, MultiStepProgress, progress_iterator
except ImportError:
    # 如果导入失败，使用简单的替代
    class RealProgressBar:
        def __init__(self, *args, **kwargs): pass
        def update(self, *args, **kwargs): pass
        def finish(self, *args, **kwargs): pass
    
    class MultiStepProgress:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def start_step(self, *args): pass
        def complete_step(self, *args): pass
    
    def progress_iterator(iterable, description=""):
        return iterable

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 导入智能连接管理器
try:
    from .presto_connection import PrestoConnectionManager, create_smart_connection
except ImportError:
    # 如果相对导入失败，尝试绝对导入
    import sys
    import os
    sys.path.insert(0, os.path.dirname(__file__))
    from presto_connection import PrestoConnectionManager, create_smart_connection

class PrestoSync:
    def __init__(self, local_db_path: str = "./data/data.db"):
        self.local_db_path = local_db_path
        self.presto_config = self._load_presto_config()
        self.connection_manager = create_smart_connection(self.presto_config)
        
    def _load_presto_config(self) -> Dict:
        """加载Presto连接配置"""
        try:
            # 从配置文件读取连接信息
            import sys
            import os
            # 添加项目根目录到路径
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, project_root)
            from config import PRESTO_CONFIG
            
            logger.info("✅ 从presto_config.py加载配置")
            logger.info(f"📍 连接到: {PRESTO_CONFIG['host']}:{PRESTO_CONFIG['port']}")
            return PRESTO_CONFIG
            
        except ImportError:
            logger.warning("⚠️ 未找到presto_config.py，使用默认配置")
            return self._get_default_config()
        except Exception as e:
            logger.warning(f"⚠️ 配置加载失败: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            "host": "localhost",
            "port": 8080,
            "catalog": "hive",
            "schema": "da",
            "user": "admin"
        }
    
    def connect_presto(self):
        """连接到Presto数据库（使用智能连接管理器）"""
        return self.connection_manager.connect()
    
    def execute_user_data_query(self, start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """执行用户数据查询"""
        
        # 从文件读取的原始SQL
        base_sql = """
        select
            k1.dt,
            k1.ad_channel,
            k1.agent,
            k1.ad_account,
            k1.subchannel,
            k1.status,
            k1.verification_status,
            k1.os_type,
            k1.gender,
            k1.tag,
            k1.age_group,
            k1.dengji,
            count(distinct k1.user_id) as newuser,
            sum(k1.is_returned_1_day) is_returned_1_day,
            sum(k1.zizhu_revenue_1) as zizhu_revenue_1,
            sum(k1.zizhu_revenue_1_aftertax) as zizhu_revenue_1_aftertax
        from
            (SELECT
                t1.dt,
                t1.user_id,
                t1.ad_channel,
                t1.agent,
                t1.ad_account,
                t1.subchannel,
                t1.tag,
                t1.verification_status,
                t1.gender,
                t1.age_group,
                t1.dengji,
                t1.os_type,
                t1.status,
                t1.is_returned_1_day,
                t1.new_user_zizhu_24h_revenue,
                t1.new_user_zizhu_24h_revenue_aftertax,
                t1.cash_cost,
                sum(if(date(t1.dt)=t2.buy_dt,t2.zizhu_revenue,null)) zizhu_revenue_1,
                sum(
                    case when date(t1.dt)=t2.buy_dt and t1.os_type = 'android' then t2.zizhu_revenue*(1-0.006)
                    when date(t1.dt)=t2.buy_dt and t1.os_type = 'iOS' then t2.zizhu_revenue*(1-0.3144) else null end
                ) zizhu_revenue_1_aftertax
            FROM
                (select
                    t0.dt,
                    t0.user_id,
                    t0.ad_channel,
                    t0.agent,
                    t0.ad_account,
                    t0.subchannel,
                    t0.tag,
                    t0.verification_status,
                    t0.gender,
                    t0.age_group,
                    t0.dengji,
                    t0.os_type,
                    t0.status,
                    t0.returned_1d as is_returned_1_day,
                    t5.new_user_zizhu_24h_revenue,
                    case when t0.os_type = 'android' then t5.new_user_zizhu_24h_revenue*(1-0.006)
                        when t0.os_type = 'iOS' then t5.new_user_zizhu_24h_revenue*(1-0.3144) else null end new_user_zizhu_24h_revenue_aftertax,
                    t0.cash_cost
                from
                    (SELECT
                        dt,
                        ad_channel,
                        agent,
                        ad_account,
                        subchannel,
                        tag,
                        user_id,
                        verification_status,
                        gender,
                        CASE
                            WHEN age < 20 THEN '20-'
                            WHEN age BETWEEN 20 AND 23 THEN '20~23'
                            WHEN age BETWEEN 24 AND 30 THEN '24~30'
                            WHEN age BETWEEN 31 AND 35 THEN '31~35'
                            WHEN age BETWEEN 36 AND 40 THEN '36~40'
                            WHEN age > 40 THEN '40+'
                        END as age_group,
                        dengji,
                        os_type,
                        status,
                        returned_1d,
                        cash_cost
                    FROM da.cpz_qs_newuser_channel_i_d
                    WHERE date(dt) BETWEEN date('{start_date}') AND date('{end_date}')
                    ) t0
                    left join
                    (select
                        dt,
                        user_id,
                        COALESCE(new_user_qs_24h_revenue, 0) + COALESCE(new_user_see_24h_revenue, 0) + COALESCE(new_user_vip_24h_revenue, 0) AS new_user_zizhu_24h_revenue
                    from dwd.dwd_qs_daily_active_users_info_wide_table_a_d
                    where dt > '2023-01-01' and is_new_user = 1
                    ) t5 on t0.dt = t5.dt and t0.user_id = t5.user_id
                ) t1
                LEFT JOIN(
                    SELECT
                        date(order_created_time) as buy_dt,
                        user_id,
                        sum(case when product_type in (1,2,3,4,12,14) then price/100 else 0 end) as zizhu_revenue
                    FROM dwd.detail_ttx_order_wide_a_d
                    WHERE date(dt) BETWEEN date('{start_date}') AND date('{end_date}')
                    GROUP BY 1,2
                ) t2 ON t1.user_id=t2.user_id
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
            ) k1
        group by 1,2,3,4,5,6,7,8,9,10,11,12
        """
        
        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        sql = base_sql.format(start_date=start_date, end_date=end_date)
        
        logger.info(f"📊 执行查询: {start_date} 到 {end_date}")
        
        try:
            with self.connection_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn)
                logger.info(f"✅ 查询成功，获得 {len(df)} 条记录")
                return df
                
        except Exception as e:
            logger.error(f"❌ 查询执行失败: {e}")
            return None
    
    def import_to_local_db(self, df: pd.DataFrame, table_name: str = "cpz_qs_newuser_channel_i_d") -> bool:
        """将数据导入本地SQLite数据库"""
        if df is None or df.empty:
            logger.warning("⚠️ 没有数据需要导入")
            return False
        
        # 白名单验证 - 只允许导入到已知的安全表
        ALLOWED_TABLES = [
            'cpz_qs_newuser_channel_i_d',
            'dwd_ttx_market_cash_cost_i_d',
            'dws_ttx_market_media_reports_i_d',
            'dwd_ttx_market_cash_cost_i_d_test'
        ]
        
        if table_name not in ALLOWED_TABLES:
            logger.error(f"❌ 不允许导入到表: {table_name}")
            raise ValueError(f"不允许导入到表: {table_name}")
        
        try:
            # 数据清理和格式化
            logger.info("🧹 清理和格式化数据...")
            
            # 根据表类型处理NULL值
            if table_name == "cpz_qs_newuser_channel_i_d":
                df = df.fillna({
                    'dt': '',
                    'ad_channel': '',
                    'agent': '',
                    'ad_account': '',
                    'subchannel': '',
                    'status': '',
                    'verification_status': '',
                    'os_type': '',
                    'gender': '',
                    'tag': '',
                    'age_group': '',
                    'dengji': '',
                    'newuser': 0,
                    'is_returned_1_day': 0.0,
                    'zizhu_revenue_1': 0.0,
                    'zizhu_revenue_1_aftertax': 0.0
                })
                
                # 确保数据类型正确
                df['newuser'] = df['newuser'].astype(int)
                df['is_returned_1_day'] = pd.to_numeric(df['is_returned_1_day'], errors='coerce').fillna(0.0)
                df['zizhu_revenue_1'] = pd.to_numeric(df['zizhu_revenue_1'], errors='coerce').fillna(0.0)
                df['zizhu_revenue_1_aftertax'] = pd.to_numeric(df['zizhu_revenue_1_aftertax'], errors='coerce').fillna(0.0)
                
                # 用户数据按完整主键去重
                key_cols = ['dt', 'ad_channel', 'agent', 'ad_account', 'subchannel', 'status', 'verification_status', 'os_type', 'gender', 'tag', 'age_group', 'dengji']
                df = df.drop_duplicates(subset=key_cols, keep='last')
                logger.info(f"🔧 用户数据去重后剩余 {len(df)} 条记录")
                
            elif table_name == "dwd_ttx_market_cash_cost_i_d":
                df = df.fillna({
                    'dt': '',
                    'channel': '',
                    'agent': '',
                    'account': '',
                    'ad_plan_id_str': '',
                    'cash_cost': 0.0
                })
                
                # 确保数据类型正确，处理负值
                df['cash_cost'] = pd.to_numeric(df['cash_cost'], errors='coerce').fillna(0.0)
                # 将负值转为0（符合CHECK约束）
                df.loc[df['cash_cost'] < 0, 'cash_cost'] = 0.0
                
                # 成本数据按完整主键去重
                key_cols = ['dt', 'channel', 'agent', 'account', 'ad_plan_id_str']
                df = df.drop_duplicates(subset=key_cols, keep='last')
                logger.info(f"🔧 成本数据去重后剩余 {len(df)} 条记录")
                
            elif table_name == "dws_ttx_market_media_reports_i_d":
                # 素材数据处理
                df = df.fillna({
                    'dt': '',
                    'channel': '',
                    'agent': '',
                    'account': '',
                    'ad_plan_id_str': '',
                    'ad_plan_name': '',
                    'ad_creative_id_str': '',
                    'media_id_str': '',
                    'total_good_verified': 0,
                    'cash_cost': 0.0,
                    'show': 0,
                    'click': 0,
                    'total': 0,
                    'total_good_verified_female': 0,
                    'total_good_verified_male': 0,
                    'total_good_verified_white': 0,
                    'total_good_verified_ios': 0,
                    'total_good_verified_return_1d': 0,
                    'total_payed_amount_good_verified': 0.0
                })
                
                # 确保数据类型正确
                for col in ['total_good_verified', 'show', 'click', 'total', 
                           'total_good_verified_female', 'total_good_verified_male',
                           'total_good_verified_white', 'total_good_verified_ios', 
                           'total_good_verified_return_1d']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                
                for col in ['cash_cost', 'total_payed_amount_good_verified']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                
                # 将负值转为0（符合CHECK约束）
                numeric_cols = ['total_good_verified', 'cash_cost', 'show', 'click', 'total',
                               'total_good_verified_female', 'total_good_verified_male',
                               'total_good_verified_white', 'total_good_verified_ios',
                               'total_good_verified_return_1d', 'total_payed_amount_good_verified']
                for col in numeric_cols:
                    df.loc[df[col] < 0, col] = 0
                
                # 素材数据使用完整主键去重，保留每个账户的独立记录
                key_cols = ['dt', 'channel', 'account', 'ad_plan_id_str', 'ad_creative_id_str', 'media_id_str']
                df = df.drop_duplicates(subset=key_cols, keep='last')
                logger.info(f"🔧 素材数据去重后剩余 {len(df)} 条记录")
            
            logger.info(f"📊 数据统计: {len(df)} 条记录，时间范围: {df['dt'].min()} 到 {df['dt'].max()}")
            
            conn = sqlite3.connect(self.local_db_path)
            
            # 检查是否需要创建表 - 使用参数化查询防止SQL注入
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            table_exists = cursor.fetchone()
            
            if not table_exists:
                logger.info(f"📝 创建表 {table_name}")
                # 根据表类型创建不同的表结构
                if table_name == "cpz_qs_newuser_channel_i_d":
                    create_sql = f"""
                    CREATE TABLE {table_name} (
                        dt TEXT,
                        ad_channel TEXT,
                        agent TEXT,
                        ad_account TEXT,
                        subchannel TEXT,
                        status TEXT,
                        verification_status TEXT,
                        os_type TEXT,
                        gender TEXT,
                        tag TEXT,
                        age_group TEXT,
                        dengji TEXT,
                        newuser INTEGER DEFAULT 0,
                        is_returned_1_day REAL DEFAULT 0.0,
                        zizhu_revenue_1 REAL DEFAULT 0.0,
                        zizhu_revenue_1_aftertax REAL DEFAULT 0.0
                    )
                    """
                elif table_name == "dwd_ttx_market_cash_cost_i_d":
                    create_sql = f"""
                    CREATE TABLE {table_name} (
                        dt TEXT,
                        channel TEXT,
                        agent TEXT,
                        account TEXT,
                        ad_plan_id_str TEXT,
                        cash_cost REAL DEFAULT 0.0
                    )
                    """
                elif table_name == "dws_ttx_market_media_reports_i_d":
                    create_sql = f"""
                    CREATE TABLE {table_name} (
                        dt TEXT NOT NULL,
                        channel TEXT,
                        agent TEXT, 
                        account TEXT,
                        ad_plan_id_str TEXT,
                        ad_plan_name TEXT,
                        ad_creative_id_str TEXT NOT NULL,
                        media_id_str TEXT,
                        total_good_verified INTEGER DEFAULT 0,
                        cash_cost REAL DEFAULT 0,
                        show INTEGER DEFAULT 0,
                        click INTEGER DEFAULT 0,
                        total INTEGER DEFAULT 0,
                        total_good_verified_female INTEGER DEFAULT 0,
                        total_good_verified_male INTEGER DEFAULT 0,
                        total_good_verified_white INTEGER DEFAULT 0,
                        total_good_verified_ios INTEGER DEFAULT 0,
                        total_good_verified_return_1d INTEGER DEFAULT 0,
                        total_payed_amount_good_verified REAL DEFAULT 0,
                        PRIMARY KEY (dt, channel, account, ad_plan_id_str, ad_creative_id_str, media_id_str)
                    )
                    """
                elif table_name == "dwd_ttx_market_cash_cost_i_d_test":
                    create_sql = f"""
                    CREATE TABLE {table_name} (
                        stat_date TEXT,
                        stat_hour INTEGER,
                        channel TEXT,
                        agent TEXT,
                        account TEXT,
                        ad_plan_id_str TEXT,
                        ad_plan_name TEXT,
                        apk TEXT,
                        api TEXT,
                        provider TEXT,
                        show INTEGER DEFAULT 0,
                        click INTEGER DEFAULT 0,
                        cash_cost REAL DEFAULT 0,
                        cost REAL DEFAULT 0,
                        convert INTEGER DEFAULT 0,
                        download_start INTEGER DEFAULT 0,
                        download_finish INTEGER DEFAULT 0,
                        install INTEGER DEFAULT 0,
                        total INTEGER DEFAULT 0,
                        total_male INTEGER DEFAULT 0,
                        total_female INTEGER DEFAULT 0,
                        total_good INTEGER DEFAULT 0,
                        total_good_male INTEGER DEFAULT 0,
                        total_good_female INTEGER DEFAULT 0,
                        total_good_verified INTEGER DEFAULT 0,
                        total_good_verified_male INTEGER DEFAULT 0,
                        total_good_verified_female INTEGER DEFAULT 0,
                        total_good_white INTEGER DEFAULT 0,
                        total_good_verified_white INTEGER DEFAULT 0,
                        total_good_young INTEGER DEFAULT 0,
                        total_good_verified_young INTEGER DEFAULT 0,
                        total_good_old INTEGER DEFAULT 0,
                        total_good_verified_old INTEGER DEFAULT 0,
                        total_good_ios INTEGER DEFAULT 0,
                        total_good_verified_ios INTEGER DEFAULT 0,
                        total_banned INTEGER DEFAULT 0,
                        total_pending INTEGER DEFAULT 0,
                        total_fake INTEGER DEFAULT 0,
                        total_payed_amount REAL DEFAULT 0,
                        total_payed_amount_male REAL DEFAULT 0,
                        total_payed_amount_female REAL DEFAULT 0,
                        total_payed INTEGER DEFAULT 0,
                        audit_durtions INTEGER DEFAULT 0,
                        call_back_total_good_famle INTEGER DEFAULT 0,
                        call_back_total_good_male INTEGER DEFAULT 0,
                        call_back_total_good_total INTEGER DEFAULT 0,
                        call_back_total_signup_total INTEGER DEFAULT 0,
                        ecpm REAL DEFAULT 0,
                        active_plan INTEGER DEFAULT 0,
                        bad_plan INTEGER DEFAULT 0,
                        activation INTEGER DEFAULT 0,
                        total_payed_amount_good REAL DEFAULT 0,
                        total_payed_amount_good_verified REAL DEFAULT 0,
                        total_payed_good INTEGER DEFAULT 0,
                        total_payed_good_verified INTEGER DEFAULT 0,
                        total_good_return_1d INTEGER DEFAULT 0,
                        total_good_verified_return_1d INTEGER DEFAULT 0,
                        hq INTEGER DEFAULT 0,
                        greater_user INTEGER DEFAULT 0,
                        greater_user_male INTEGER DEFAULT 0,
                        greater_user_female INTEGER DEFAULT 0,
                        third_line_city_user INTEGER DEFAULT 0,
                        total_good_verified_twenty INTEGER DEFAULT 0,
                        call_back_total_good_verified_total INTEGER DEFAULT 0,
                        total_payed_amount_after_tax REAL DEFAULT 0,
                        total_payed_amount_after_tax_male REAL DEFAULT 0,
                        total_payed_amount_after_tax_female REAL DEFAULT 0,
                        total_payed_amount_after_tax_good REAL DEFAULT 0,
                        total_payed_amount_after_tax_good_verified REAL DEFAULT 0,
                        account_name TEXT,
                        account_note TEXT,
                        total_good_verified_22_40 INTEGER DEFAULT 0,
                        dt TEXT
                    )
                    """
                else:
                    raise ValueError(f"Unknown table type: {table_name}")
                
                cursor.execute(create_sql)
            
            # 清理目标日期的现有数据 - 表名已验证，安全使用
            dates = df['dt'].unique()
            delete_sql = f"DELETE FROM {table_name} WHERE dt = ?"
            for date in dates:
                cursor.execute(delete_sql, [str(date)])
                logger.info(f"🗑️ 清理 {date} 的旧数据")
            
            # 导入新数据
            df.to_sql(table_name, conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ 成功导入 {len(df)} 条记录到本地数据库")
            return True
            
        except Exception as e:
            logger.error(f"❌ 数据导入失败: {e}")
            return False
    
    def execute_cost_data_query(self, start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """执行成本数据查询"""
        
        # 成本数据SQL
        cost_sql = """
        select
            dt,
            channel,
            agent,
            account,
            ad_plan_id_str,
            sum(cash_cost) cash_cost
        from dwd.dwd_ttx_market_cash_cost_i_d
        where date(dt) BETWEEN date('{start_date}') AND date('{end_date}')
        group by 1,2,3,4,5
        """
        
        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        sql = cost_sql.format(start_date=start_date, end_date=end_date)
        
        logger.info(f"📊 执行成本数据查询: {start_date} 到 {end_date}")
        
        try:
            with self.connection_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn)
                logger.info(f"✅ 成本数据查询成功，获得 {len(df)} 条记录")
                return df
                
        except Exception as e:
            logger.error(f"❌ 成本数据查询失败: {e}")
            return None

    def execute_creative_data_query(self, start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """执行素材数据查询"""
        
        # 素材数据SQL
        creative_sql = """
        select
            dt,
            channel,
            agent,
            account,
            ad_plan_id_str,
            ad_plan_name,
            ad_creative_id_str,
            media_id_str,
            sum(total_good_verified) as total_good_verified,
            sum(cash_cost) as cash_cost,
            sum(show) as show,
            sum(click) as click,
            sum(total) as total,
            sum(total_good_verified_female) as total_good_verified_female,
            sum(total_good_verified_male) as total_good_verified_male,
            sum(total_good_verified_white) as total_good_verified_white,
            sum(total_good_verified_ios) as total_good_verified_ios,
            sum(total_good_verified_return_1d) as total_good_verified_return_1d,
            sum(total_payed_amount_good_verified) as total_payed_amount_good_verified
        from dws.dws_ttx_market_media_reports_i_d
        where date(dt) BETWEEN date('{start_date}') AND date('{end_date}')
        group by 1,2,3,4,5,6,7,8
        """
        
        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        sql = creative_sql.format(start_date=start_date, end_date=end_date)
        
        logger.info(f"📊 执行素材数据查询: {start_date} 到 {end_date}")
        
        try:
            with self.connection_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn)
                logger.info(f"✅ 素材数据查询成功，获得 {len(df)} 条记录")
                return df
                
        except Exception as e:
            logger.error(f"❌ 素材数据查询失败: {e}")
            return None

    def execute_test_cost_data_query(self, start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """执行测试成本数据查询"""
        
        # 测试成本数据SQL - 从8月开始的数据
        test_cost_sql = """
        select *
        from dwd.dwd_ttx_market_cash_cost_i_d
        where date(dt) BETWEEN date('{start_date}') AND date('{end_date}')
          and date(dt) >= date('2025-08-01')
        """
        
        # 设置默认日期范围 - 如果没有指定，从2025-08-01开始
        if not start_date:
            start_date = '2025-08-01'
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 确保不早于2025-08-01
        if start_date < '2025-08-01':
            start_date = '2025-08-01'
        
        sql = test_cost_sql.format(start_date=start_date, end_date=end_date)
        
        logger.info(f"📊 执行测试成本数据查询: {start_date} 到 {end_date}")
        
        try:
            with self.connection_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn)
                logger.info(f"✅ 测试成本数据查询成功，获得 {len(df)} 条记录")
                return df
                
        except Exception as e:
            logger.error(f"❌ 测试成本数据查询失败: {e}")
            return None

    def sync_data(self, start_date: str = None, end_date: str = None) -> bool:
        """完整的数据同步流程"""
        logger.info("🚀 开始数据同步...")
        
        # 1. 从Presto查询用户数据
        user_df = self.execute_user_data_query(start_date, end_date)
        if user_df is None:
            return False
        
        # 2. 数据校验和清理
        if not self.validate_sync_data(user_df, start_date, end_date):
            logger.error("❌ 数据校验失败，终止同步")
            return False
        
        # 3. 从Presto查询成本数据
        cost_df = self.execute_cost_data_query(start_date, end_date)
        if cost_df is None:
            logger.warning("⚠️ 成本数据查询失败，仅同步用户数据")
            cost_sync_success = False
        else:
            cost_sync_success = self.import_to_local_db(cost_df, "dwd_ttx_market_cash_cost_i_d")
        
        # 4. 从Presto查询素材数据
        creative_df = self.execute_creative_data_query(start_date, end_date)
        if creative_df is None:
            logger.warning("⚠️ 素材数据查询失败，跳过素材数据同步")
            creative_sync_success = False
        else:
            creative_sync_success = self.import_to_local_db(creative_df, "dws_ttx_market_media_reports_i_d")
        
        # 5. 导入用户数据到本地数据库
        user_sync_success = self.import_to_local_db(user_df, "cpz_qs_newuser_channel_i_d")
        
        # 6. 同步后校验
        if user_sync_success:
            self.post_sync_validation(start_date, end_date)
        
        # 7. 报告同步结果
        sync_results = []
        if user_sync_success:
            sync_results.append("✅ 用户数据")
        if cost_sync_success:
            sync_results.append("✅ 成本数据")
        if creative_sync_success:
            sync_results.append("✅ 素材数据")
            
        failed_results = []
        if not cost_sync_success:
            failed_results.append("❌ 成本数据")
        if not creative_sync_success:
            failed_results.append("❌ 素材数据")
        
        if user_sync_success:
            success_msg = "同步完成: " + ", ".join(sync_results)
            if failed_results:
                success_msg += " | 失败: " + ", ".join(failed_results)
            logger.info("🎉 " + success_msg)
        else:
            logger.error("❌ 数据同步失败")
        
        return user_sync_success
    
    def validate_sync_data(self, df: pd.DataFrame, start_date: str, end_date: str) -> bool:
        """数据同步前的校验"""
        logger.info("🔍 开始数据校验...")
        
        if df is None or df.empty:
            logger.error("❌ 数据为空")
            return False
        
        # 1. 检查日期范围
        df_dates = pd.to_datetime(df['dt']).dt.date
        start_dt = pd.to_datetime(start_date).date()
        end_dt = pd.to_datetime(end_date).date()
        
        invalid_dates = df_dates[(df_dates < start_dt) | (df_dates > end_dt)]
        if len(invalid_dates) > 0:
            logger.warning(f"⚠️ 发现 {len(invalid_dates)} 条超出日期范围的记录")
        
        # 2. 检查ARPU合理性（避免异常高值）
        df_with_revenue = df[df['zizhu_revenue_1'] > 0].copy()
        if not df_with_revenue.empty:
            df_with_revenue['arpu'] = df_with_revenue['zizhu_revenue_1'] / df_with_revenue['newuser']
            high_arpu = df_with_revenue[df_with_revenue['arpu'] > 500]  # ARPU > 500视为异常
            
            if len(high_arpu) > 0:
                logger.warning(f"⚠️ 发现 {len(high_arpu)} 条ARPU异常高的记录 (>500元)")
                logger.warning(f"最高ARPU: {high_arpu['arpu'].max():.2f}元")
                
                # 记录异常数据详情
                for _, row in high_arpu.head(3).iterrows():
                    logger.warning(f"异常记录: {row['dt']} {row['ad_channel']} ARPU={row['arpu']:.2f}")
        
        # 3. 检查重复记录
        key_cols = ['dt', 'ad_channel', 'agent', 'ad_account', 'subchannel', 'status', 'verification_status', 'os_type', 'gender', 'age_group']
        duplicates = df[df.duplicated(subset=key_cols, keep=False)]
        if len(duplicates) > 0:
            logger.warning(f"⚠️ 发现 {len(duplicates)} 条重复记录")
        
        # 4. 基本统计信息
        total_users = df['newuser'].sum()
        total_revenue = df['zizhu_revenue_1'].sum()
        avg_arpu = total_revenue / total_users if total_users > 0 else 0
        
        logger.info(f"📊 数据概况: {len(df)}条记录, {total_users}用户, {total_revenue:.2f}元收入, 平均ARPU={avg_arpu:.2f}元")
        
        return True
    
    def post_sync_validation(self, start_date: str, end_date: str):
        """同步后的数据校验"""
        logger.info("🔍 同步后数据校验...")
        
        conn = sqlite3.connect(self.local_db_path)
        cursor = conn.cursor()
        
        try:
            # 按日期统计ARPU，检查异常值
            cursor.execute('''
            SELECT 
                dt,
                SUM(zizhu_revenue_1) as total_revenue,
                SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as good_users,
                ROUND(SUM(zizhu_revenue_1) / NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0), 2) as arpu
            FROM cpz_qs_newuser_channel_i_d
            WHERE dt BETWEEN ? AND ?
            GROUP BY dt
            ORDER BY dt
            ''', [start_date, end_date])
            
            results = cursor.fetchall()
            
            for dt, revenue, users, arpu in results:
                logger.info(f"📊 {dt}: 收入={revenue:.2f}元, Good+verified={users}人, ARPU={arpu}元")
                
                # 检查异常值
                if arpu and arpu > 20:
                    logger.warning(f"⚠️ {dt} ARPU异常高: {arpu}元，请检查数据")
                elif arpu and arpu < 3:
                    logger.warning(f"⚠️ {dt} ARPU异常低: {arpu}元，请检查数据")
        
        except Exception as e:
            logger.error(f"❌ 同步后校验失败: {e}")
        finally:
            conn.close()
    
    def check_missing_dates(self, start_date: str = "2025-07-01", end_date: str = None, check_retention: bool = True) -> List[str]:
        """检查本地数据库中缺失的日期，包括次留率未更新的日期"""
        if not end_date:
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"🔍 检查 {start_date} 到 {end_date} 的数据缺失情况...")
        
        # 生成日期范围
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        all_dates = []
        current_dt = start_dt
        while current_dt <= end_dt:
            all_dates.append(current_dt.strftime('%Y-%m-%d'))
            current_dt += timedelta(days=1)
        
        # 查询本地数据库已有的日期
        conn = sqlite3.connect(self.local_db_path)
        cursor = conn.cursor()
        
        try:
            # 1. 检查完全缺失的日期
            cursor.execute('''
            SELECT DISTINCT dt 
            FROM cpz_qs_newuser_channel_i_d 
            WHERE dt BETWEEN ? AND ?
            ORDER BY dt
            ''', [start_date, end_date])
            
            existing_dates = [row[0] for row in cursor.fetchall()]
            missing_dates = [date for date in all_dates if date not in existing_dates]
            
            # 2. 检查次留率未更新的日期（如果启用检查）
            retention_pending_dates = []
            if check_retention:
                # 次留率需要次日数据才能计算，所以排除最后一天
                retention_check_dates = [date for date in existing_dates if date < end_date]
                
                for date in retention_check_dates:
                    # 检查该日期是否有次留率数据
                    cursor.execute('''
                    SELECT 
                        COUNT(*) as total_good_verified,
                        SUM(CASE WHEN is_returned_1_day > 0 THEN 1 ELSE 0 END) as has_retention
                    FROM cpz_qs_newuser_channel_i_d 
                    WHERE dt = ? AND status = 'good' AND verification_status = 'verified'
                    ''', [date])
                    
                    result = cursor.fetchone()
                    if result:
                        total_users, users_with_retention = result
                        # 如果有good+verified用户但次留率为0，说明需要更新
                        if total_users > 0 and users_with_retention == 0:
                            retention_pending_dates.append(date)
            
            # 合并需要处理的日期
            all_missing_dates = list(set(missing_dates + retention_pending_dates))
            all_missing_dates.sort()
            
            logger.info(f"📊 统计结果:")
            logger.info(f"   总天数: {len(all_dates)} 天")
            logger.info(f"   已有数据: {len(existing_dates)} 天")
            logger.info(f"   完全缺失: {len(missing_dates)} 天")
            if check_retention:
                logger.info(f"   次留待更新: {len(retention_pending_dates)} 天")
            logger.info(f"   需要同步: {len(all_missing_dates)} 天")
            
            if missing_dates:
                logger.info(f"📅 缺失日期: {', '.join(missing_dates[:5])}" + 
                           (f" ... (还有{len(missing_dates)-5}天)" if len(missing_dates) > 5 else ""))
            
            if retention_pending_dates:
                logger.info(f"🔄 次留待更新: {', '.join(retention_pending_dates[:5])}" + 
                           (f" ... (还有{len(retention_pending_dates)-5}天)" if len(retention_pending_dates) > 5 else ""))
            
            if not all_missing_dates:
                logger.info("✅ 数据完整，无需同步")
            
            return all_missing_dates
            
        except Exception as e:
            logger.error(f"❌ 检查缺失日期失败: {e}")
            return []
        finally:
            conn.close()
    
    def update_retention_only(self, target_date: str) -> bool:
        """仅更新次留率功能已禁用 - 只能同步真实数据"""
        logger.warning(f"⚠️ 次留率更新功能已禁用，只能同步真实数据")
        logger.info(f"💡 请使用完整数据同步来获取真实的次留率数据")
        return False
    
    def sync_missing_dates(self, start_date: str = "2025-07-01", end_date: str = None, retention_only: bool = False) -> bool:
        """同步缺失日期的数据（带进度显示）"""
        missing_dates = self.check_missing_dates(start_date, end_date)
        
        if not missing_dates:
            logger.info("✅ 无需同步，数据已完整")
            return True
        
        logger.info(f"🚀 开始处理 {len(missing_dates)} 个日期的数据...")
        
        # 使用进度条显示同步进度
        progress_bar = RealProgressBar(len(missing_dates), "同步数据")
        success_count = 0
        
        for i, date in enumerate(missing_dates):
            # 更新进度条状态
            progress_bar.update(0, f"处理 {date}")
            
            # 检查该日期是否只是需要更新次留率
            conn = sqlite3.connect(self.local_db_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute('SELECT COUNT(*) FROM cpz_qs_newuser_channel_i_d WHERE dt = ?', [date])
                has_data = cursor.fetchone()[0] > 0
                
                if has_data and retention_only:
                    # 只更新次留率
                    progress_bar.update(0, f"更新留存率 {date}")
                    if self.update_retention_only(date):
                        success_count += 1
                        progress_bar.update(1, f"✅ {date} 留存率完成")
                    else:
                        progress_bar.update(1, f"❌ {date} 留存率失败")
                else:
                    # 完整同步数据
                    progress_bar.update(0, f"同步数据 {date}")
                    if self.sync_data(date, date):
                        success_count += 1
                        progress_bar.update(1, f"✅ {date} 同步完成")
                    else:
                        progress_bar.update(1, f"❌ {date} 同步失败")
                
            except Exception as e:
                logger.error(f"❌ 处理 {date} 失败: {e}")
                progress_bar.update(1, f"❌ {date} 错误")
            finally:
                conn.close()
        
        # 完成进度条
        if success_count == len(missing_dates):
            progress_bar.finish("🎉 全部成功")
        else:
            progress_bar.finish(f"⚠️ {success_count}/{len(missing_dates)} 成功")
        
        logger.info(f"📊 处理完成: {success_count}/{len(missing_dates)} 个日期处理成功")
        return success_count == len(missing_dates)
    
    def get_data_summary(self, start_date: str = "2025-07-01", end_date: str = None) -> pd.DataFrame:
        """获取本地数据库的数据概况"""
        if not end_date:
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        conn = sqlite3.connect(self.local_db_path)
        
        try:
            query = '''
            SELECT 
                dt,
                COUNT(*) as record_count,
                SUM(newuser) as total_users,
                SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as good_verified_users,
                SUM(zizhu_revenue_1) as total_revenue,
                ROUND(SUM(zizhu_revenue_1) / NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0), 2) as arpu
            FROM cpz_qs_newuser_channel_i_d
            WHERE dt BETWEEN ? AND ?
            GROUP BY dt
            ORDER BY dt
            '''
            
            df = pd.read_sql(query, conn, params=[start_date, end_date])
            return df
            
        except Exception as e:
            logger.error(f"❌ 获取数据概况失败: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def test_connection(self) -> bool:
        """测试连接（使用智能连接管理器）"""
        logger.info("🔍 测试Presto连接...")
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                logger.info("✅ 连接测试成功")
                return True
        except Exception as e:
            logger.error(f"❌ 连接测试失败: {e}")
            return False

    def sync_test_cost_data(self, start_date: str = None, end_date: str = None) -> bool:
        """同步新测试成本表数据"""
        logger.info("🚀 开始同步测试成本数据...")
        
        # 默认从2025年8月开始
        if not start_date:
            start_date = '2025-08-01'
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 确保不早于2025-08-01
        if start_date < '2025-08-01':
            start_date = '2025-08-01'
            logger.info(f"⚠️ 起始日期调整为 {start_date} (测试表仅包含8月以后数据)")
        
        # 1. 从Presto查询测试成本数据
        test_cost_df = self.execute_test_cost_data_query(start_date, end_date)
        if test_cost_df is None:
            logger.error("❌ 测试成本数据查询失败")
            return False
        
        if test_cost_df.empty:
            logger.warning("⚠️ 没有找到测试成本数据")
            return False
        
        logger.info(f"📊 获得测试成本数据: {len(test_cost_df)} 条记录")
        
        # 2. 导入到本地数据库
        success = self.import_to_local_db(test_cost_df, "dwd_ttx_market_cash_cost_i_d_test")
        
        if success:
            logger.info("🎉 测试成本数据同步成功！")
            
            # 显示统计信息
            logger.info(f"📊 数据统计:")
            logger.info(f"   记录数: {len(test_cost_df):,}")
            logger.info(f"   日期范围: {test_cost_df['dt'].min()} 到 {test_cost_df['dt'].max()}")
            logger.info(f"   渠道数: {test_cost_df['channel'].nunique()}")
            logger.info(f"   总成本: ¥{test_cost_df['cash_cost'].sum():,.2f}")
        else:
            logger.error("❌ 测试成本数据同步失败")
        
        return success

def main():
    """主函数：支持命令行参数的数据同步"""
    parser = argparse.ArgumentParser(description='Presto数据同步工具')
    parser.add_argument('--check', action='store_true', help='检查缺失的日期')
    parser.add_argument('--sync-missing', action='store_true', help='同步所有缺失的日期')
    parser.add_argument('--update-retention', action='store_true', help='仅更新次留率，不重新同步数据')
    parser.add_argument('--date', type=str, help='同步指定日期 (格式: YYYY-MM-DD)')
    parser.add_argument('--start-date', type=str, default='2025-07-01', help='开始日期 (默认: 2025-07-01)')
    parser.add_argument('--end-date', type=str, help='结束日期 (默认: 昨天)')
    parser.add_argument('--summary', action='store_true', help='显示数据概况')
    parser.add_argument('--test', action='store_true', help='测试连接')
    parser.add_argument('--sync-test-cost', action='store_true', help='同步测试成本表 (从2025-08-01开始)')
    
    args = parser.parse_args()
    
    syncer = PrestoSync()
    
    # 同步测试成本表
    if args.sync_test_cost:
        logger.info("📥 开始同步测试成本表数据...")
        if syncer.sync_test_cost_data(args.start_date, args.end_date):
            logger.info("🎉 测试成本数据同步成功！")
        else:
            logger.error("❌ 测试成本数据同步失败")
        return
    
    # 测试连接
    if args.test or not any([args.check, args.sync_missing, args.update_retention, args.date, args.summary, args.sync_test_cost]):
        if not syncer.test_connection():
            logger.error("❌ 无法连接到Presto数据库")
            logger.error("请检查:")
            logger.error("1. Presto服务是否运行")
            logger.error("2. 网络连接是否正常") 
            logger.error("3. 连接配置是否正确")
            logger.error("4. 是否安装了presto-python-client: pip install presto-python-client")
            return
        
        if args.test:
            logger.info("✅ 连接测试成功")
            return
    
    # 检查缺失日期
    if args.check:
        syncer.check_missing_dates(args.start_date, args.end_date)
        return
    
    # 同步指定日期
    if args.date:
        logger.info(f"📥 同步指定日期: {args.date}")
        if syncer.sync_data(args.date, args.date):
            logger.info("🎉 数据同步成功！")
        else:
            logger.error("❌ 数据同步失败")
        return
    
    # 仅更新次留率
    if args.update_retention:
        missing_dates = syncer.check_missing_dates(args.start_date, args.end_date)
        if syncer.sync_missing_dates(args.start_date, args.end_date, retention_only=True):
            logger.info("🎉 次留率更新完成！")
        else:
            logger.error("❌ 部分次留率更新失败")
        return
    
    # 同步缺失日期
    if args.sync_missing:
        if syncer.sync_missing_dates(args.start_date, args.end_date):
            logger.info("🎉 缺失数据同步完成！")
        else:
            logger.error("❌ 部分数据同步失败")
        return
    
    # 显示数据概况
    if args.summary:
        logger.info("📊 生成数据概况...")
        df = syncer.get_data_summary(args.start_date, args.end_date)
        if not df.empty:
            print("\n=== 数据概况 ===")
            print(df.to_string(index=False))
            print(f"\n📈 统计:")
            print(f"  总天数: {len(df)} 天")
            print(f"  总用户: {df['total_users'].sum():,} 人")
            print(f"  总收入: {df['total_revenue'].sum():,.2f} 元")
            print(f"  平均ARPU: {df['arpu'].mean():.2f} 元")
        return
    
    # 默认行为：同步最近7天数据
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"📅 默认同步最近7天: {start_date} 到 {end_date}")
    
    if syncer.sync_data(start_date, end_date):
        logger.info("🎉 数据同步成功！")
    else:
        logger.error("❌ 数据同步失败")

if __name__ == '__main__':
    main()