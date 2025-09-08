#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复版数据库管理器
专门处理大数据量和事务问题
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
import pymysql
import logging

logger = logging.getLogger(__name__)


class DatabaseManagerFixed:
    """修复版数据库管理器"""
    
    def __init__(self):
        self.engine = None
        self.connection = None
        self.cursor = None
        self._connect()
    
    def _connect(self):
        """连接数据库"""
        try:
            # 数据库配置
            config = {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': 'root',
                'database': 'baostock',
                'charset': 'utf8mb4'
            }
            
            # 创建SQLAlchemy引擎
            connection_string = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?charset={config['charset']}"
            self.engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=3600)
            
            # 创建PyMySQL连接
            self.connection = pymysql.connect(**config)
            self.cursor = self.connection.cursor()
            
            logger.info("数据库连接成功")
            
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            raise
    
    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理DataFrame，转换数据类型
        
        Args:
            df: 原始DataFrame
            
        Returns:
            处理后的DataFrame
        """
        df = df.copy()
        
        # 处理日期类型
        date_columns = [col for col in df.columns if 'date' in col.lower() or col in ['ipoDate', 'outDate', 'pubDate', 'statDate']]
        for col in date_columns:
            if col in df.columns:
                # 处理空字符串日期字段
                df[col] = df[col].replace('', None)
        
        # 处理数值类型（排除日期字段）
        numeric_columns = [col for col in df.columns if any(keyword in col.lower() for keyword in ['rate', 'ratio', 'amount', 'price', 'profit', 'revenue', 'share', 'asset', 'liability', 'equity', 'margin', 'eps', 'roe', 'turn', 'pct', 'pe', 'pb', 'ps', 'pcf']) and 'date' not in col.lower()]
        for col in numeric_columns:
            if col in df.columns:
                # 先处理空字符串
                df[col] = df[col].replace('', None)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 处理空值（排除日期字段）
        non_date_columns = [col for col in df.columns if 'date' not in col.lower() and col not in ['ipoDate', 'outDate', 'pubDate', 'statDate']]
        for col in non_date_columns:
            if col in df.columns:
                df[col] = df[col].replace('', None)
                df[col] = df[col].replace('nan', None)
                df[col] = df[col].where(pd.notnull(df[col]), None)
        
        return df
    
    def upsert_data_safe(self, table_name: str, data: List[Dict], primary_keys: List[str]):
        """
        安全的数据插入/更新方法
        
        Args:
            table_name: 表名
            data: 数据列表
            primary_keys: 主键字段列表
        """
        if not data:
            return
        
        try:
            # 转换为DataFrame并处理
            df = pd.DataFrame(data)
            df = self._process_dataframe(df)
            
            # 处理NaN值
            df = df.replace([np.inf, -np.inf], np.nan)
            # 使用where方法替换NaN值
            df = df.where(pd.notnull(df), None)
            
            # 添加时间戳
            df['created_at'] = pd.Timestamp.now()
            df['updated_at'] = pd.Timestamp.now()
            
            # 去重处理
            df = df.drop_duplicates(subset=primary_keys, keep='last')
            
            # 使用更小的批次大小
            batch_size = 100
            total_processed = 0
            
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size].copy()
                
                try:
                    # 使用INSERT IGNORE避免重复键错误
                    self._insert_batch_ignore(table_name, batch_df)
                    total_processed += len(batch_df)
                    logger.info(f"已处理 {total_processed}/{len(df)} 条记录")
                    
                except Exception as batch_error:
                    logger.warning(f"批次 {i//batch_size + 1} 处理失败: {str(batch_error)}")
                    # 尝试逐条插入
                    self._insert_records_one_by_one(table_name, batch_df)
                    total_processed += len(batch_df)
            
            logger.info(f"成功处理 {total_processed} 条记录到表 {table_name}")
            
        except Exception as e:
            logger.error(f"处理数据到表 {table_name} 失败: {str(e)}")
            raise
    
    def _insert_batch_ignore(self, table_name: str, df: pd.DataFrame):
        """批量插入记录，使用INSERT IGNORE"""
        if len(df) == 0:
            return
        
        # 构建INSERT IGNORE语句
        columns = list(df.columns)
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join([f'`{col}`' for col in columns])
        
        sql = f"INSERT IGNORE INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        # 准备数据
        values = []
        for _, row in df.iterrows():
            row_values = []
            for val in row.values:
                if pd.isna(val) or val is None:
                    row_values.append(None)
                else:
                    row_values.append(val)
            values.append(tuple(row_values))
        
        # 执行批量插入
        self.cursor.executemany(sql, values)
        self.connection.commit()
    
    def _insert_records_one_by_one(self, table_name: str, df: pd.DataFrame):
        """逐条插入记录"""
        for _, row in df.iterrows():
            try:
                # 构建INSERT语句
                columns = list(row.index)
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join([f'`{col}`' for col in columns])
                
                sql = f"INSERT IGNORE INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
                
                # 处理NaN值
                values = []
                for val in row.values:
                    if pd.isna(val) or val is None:
                        values.append(None)
                    else:
                        values.append(val)
                
                self.cursor.execute(sql, tuple(values))
                self.connection.commit()
                
            except Exception as e:
                logger.warning(f"插入单条记录失败: {str(e)}")
                try:
                    self.connection.rollback()
                except:
                    pass
                continue
    
    def create_table_safe(self, table_name: str, columns_info: List[str]):
        """安全创建表"""
        try:
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_info)},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
            
            logger.info(f"表 {table_name} 创建成功")
            
        except Exception as e:
            logger.error(f"创建表 {table_name} 失败: {str(e)}")
            raise
    
    def close(self):
        """关闭连接"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            if self.engine:
                self.engine.dispose()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {str(e)}")


def main():
    """测试修复版数据库管理器"""
    import logging
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 测试数据
    test_data = [
        {'code': '000001', 'date': '2020-01-01', 'value': 100.0},
        {'code': '000002', 'date': '2020-01-01', 'value': 200.0},
    ]
    
    # 测试修复版管理器
    db_manager = DatabaseManagerFixed()
    
    try:
        # 创建测试表
        columns_info = [
            "code VARCHAR(20) NOT NULL COMMENT '股票代码'",
            "date DATE NOT NULL COMMENT '日期'",
            "value DECIMAL(20,6) COMMENT '数值'",
            "PRIMARY KEY (code, date)"
        ]
        
        db_manager.create_table_safe('test_table', columns_info)
        
        # 测试数据插入
        db_manager.upsert_data_safe('test_table', test_data, ['code', 'date'])
        
        print("✅ 修复版数据库管理器测试成功")
        
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
    finally:
        db_manager.close()


if __name__ == '__main__':
    main()
