#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理模块
"""

import pymysql
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Any, Optional, Union
import logging
from datetime import datetime, date
import os

from config import Config

class DatabaseManager:
    """数据库管理类"""
    
    def __init__(self):
        self.config = Config.DATABASE_CONFIG
        self.engine = None
        self.logger = logging.getLogger(__name__)
        self._connect()
    
    def _connect(self):
        """连接数据库"""
        try:
            # 创建SQLAlchemy引擎
            database_url = Config.get_database_url()
            self.engine = create_engine(
                database_url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # 测试连接
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info("数据库连接成功")
            
        except Exception as e:
            self.logger.error(f"数据库连接失败: {str(e)}")
            raise
    
    def create_database(self):
        """创建数据库"""
        try:
            # 先连接到MySQL服务器（不指定数据库）
            temp_config = self.config.copy()
            temp_config.pop('database', None)
            
            temp_engine = create_engine(
                f"mysql+pymysql://{temp_config['user']}:{temp_config['password']}@{temp_config['host']}:{temp_config['port']}?charset={temp_config['charset']}"
            )
            
            with temp_engine.connect() as conn:
                # 创建数据库
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.config['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
                conn.commit()
            
            self.logger.info(f"数据库 {self.config['database']} 创建成功")
            
        except Exception as e:
            self.logger.error(f"创建数据库失败: {str(e)}")
            raise
    
    def create_tables(self):
        """创建数据表"""
        try:
            # 读取SQL文件
            sql_file = os.path.join(os.path.dirname(__file__), 'schema.sql')
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 分割SQL语句
            sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            with self.engine.connect() as conn:
                for sql in sql_statements:
                    if sql:
                        conn.execute(text(sql))
                conn.commit()
            
            self.logger.info("数据表创建成功")
            
        except Exception as e:
            self.logger.error(f"创建数据表失败: {str(e)}")
            raise
    
    def insert_data(self, table_name: str, data: Union[List[Dict], pd.DataFrame], 
                   if_exists: str = 'append', chunk_size: int = 1000) -> int:
        """
        插入数据到数据库
        
        Args:
            table_name: 表名
            data: 数据（字典列表或DataFrame）
            if_exists: 如果表存在时的处理方式 ('append', 'replace', 'fail')
            chunk_size: 批量插入大小
            
        Returns:
            插入的记录数
        """
        try:
            if isinstance(data, list) and len(data) == 0:
                return 0
            
            # 转换为DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data.copy()
            
            # 处理日期类型
            df = self._process_dataframe(df)
            
            # 插入数据
            rows_inserted = df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunk_size,
                method='multi'
            )
            
            self.logger.info(f"成功插入 {len(df)} 条记录到表 {table_name}")
            return len(df)
            
        except Exception as e:
            self.logger.error(f"插入数据到表 {table_name} 失败: {str(e)}")
            raise
    
    def upsert_data(self, table_name: str, data: Union[List[Dict], pd.DataFrame], 
                   primary_keys: List[str]) -> int:
        """
        插入或更新数据（基于主键）
        
        Args:
            table_name: 表名
            data: 数据
            primary_keys: 主键字段列表
            
        Returns:
            处理的记录数
        """
        try:
            if isinstance(data, list) and len(data) == 0:
                return 0
            
            # 转换为DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data.copy()
            
            # 处理日期类型
            df = self._process_dataframe(df)
            
            # 添加时间戳字段
            df['created_at'] = pd.Timestamp.now()
            df['updated_at'] = pd.Timestamp.now()
            
            if len(df) == 0:
                return 0
            
            # 使用更高效的批量去重方法
            new_records = []
            for _, row in df.iterrows():
                # 构建查询条件
                conditions = []
                params = {}
                for i, key in enumerate(primary_keys):
                    if key in row and pd.notna(row[key]):
                        conditions.append(f"{key} = :param_{i}")
                        params[f'param_{i}'] = row[key]
                
                if conditions:
                    # 检查记录是否已存在
                    sql = f"SELECT COUNT(*) as count FROM {table_name} WHERE {' AND '.join(conditions)}"
                    result_df = self.query_data(sql, params)
                    if result_df.iloc[0]['count'] == 0:
                        new_records.append(row.to_dict())
            
            if new_records:
                # 批量插入新记录
                new_df = pd.DataFrame(new_records)
                new_df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi')
                self.logger.info(f"成功插入 {len(new_records)} 条新记录到 {table_name}")
                return len(new_records)
            else:
                self.logger.info(f"没有新数据需要插入到 {table_name}，所有记录已存在")
                return 0
            
            
        except Exception as e:
            self.logger.error(f"Upsert数据到表 {table_name} 失败: {str(e)}")
            raise
    
    def query_data(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        查询数据
        
        Args:
            sql: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果DataFrame
        """
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(sql), params)
                else:
                    result = conn.execute(text(sql))
                
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
                
        except Exception as e:
            self.logger.error(f"查询数据失败: {str(e)}")
            raise
    
    def delete_data(self, table_name: str, where_clause: Optional[str] = None, 
                   params: Optional[Dict] = None) -> int:
        """
        删除数据
        
        Args:
            table_name: 表名
            where_clause: WHERE条件（可选）
            params: 参数
            
        Returns:
            删除的记录数
        """
        try:
            with self.engine.connect() as conn:
                if where_clause:
                    sql = f"DELETE FROM {table_name} WHERE {where_clause}"
                else:
                    sql = f"DELETE FROM {table_name}"
                
                if params:
                    result = conn.execute(text(sql), params)
                else:
                    result = conn.execute(text(sql))
                
                conn.commit()
                deleted_rows = result.rowcount
                self.logger.info(f"成功删除表 {table_name} 中的 {deleted_rows} 条记录")
                return deleted_rows
                
        except Exception as e:
            self.logger.error(f"删除数据失败: {str(e)}")
            raise
    
    def _execute_sql(self, sql: str, params: Optional[Dict] = None):
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            params: 参数
        """
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(sql), params)
                else:
                    result = conn.execute(text(sql))
                conn.commit()
                return result
        except Exception as e:
            self.logger.error(f"执行SQL失败: {str(e)}")
            raise
    
    def get_latest_date(self, table_name: str, date_column: str = 'date', 
                       code_column: Optional[str] = None, code: Optional[str] = None) -> Optional[date]:
        """
        获取表中指定股票的最新日期
        
        Args:
            table_name: 表名
            date_column: 日期字段名
            code_column: 股票代码字段名
            code: 股票代码
            
        Returns:
            最新日期
        """
        try:
            sql = f"SELECT MAX({date_column}) as latest_date FROM {table_name}"
            params = {}
            
            if code_column and code:
                sql += f" WHERE {code_column} = %(code)s"
                params['code'] = code
            
            df = self.query_data(sql, params)
            
            if not df.empty and pd.notna(df.iloc[0]['latest_date']):
                return df.iloc[0]['latest_date']
            
            return None
            
        except Exception as e:
            self.logger.error(f"获取最新日期失败: {str(e)}")
            return None
    
    def get_existing_codes(self, table_name: str, code_column: str = 'code') -> List[str]:
        """
        获取表中已存在的股票代码列表
        
        Args:
            table_name: 表名
            code_column: 股票代码字段名
            
        Returns:
            股票代码列表
        """
        try:
            sql = f"SELECT DISTINCT {code_column} FROM {table_name}"
            df = self.query_data(sql)
            return df[code_column].tolist()
            
        except Exception as e:
            self.logger.error(f"获取已存在股票代码失败: {str(e)}")
            return []
    
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
    
    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("数据库连接已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
