#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWD层财务数据处理模块
将财务数据按股票代码和日期进行截面化处理
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.manager_fixed import DatabaseManagerFixed as DatabaseManager
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class DWDProcessor:
    """DWD层财务数据处理器"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
        # 财务数据表映射
        self.financial_tables = {
            'stock_profit': 'dwd_stock_profit',
            'stock_balance': 'dwd_stock_balance', 
            'stock_cashflow': 'dwd_stock_cashflow',
            'stock_operation': 'dwd_stock_operation',
            'stock_growth': 'dwd_stock_growth',
            'stock_dupont': 'dwd_stock_dupont'
        }
    
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """
        获取表的所有列名
        
        Args:
            table_name: 表名
            
        Returns:
            列名列表
        """
        with self.db_manager.engine.connect() as conn:
            result = conn.execute(text(f"DESCRIBE {table_name}"))
            columns = [row[0] for row in result.fetchall()]
            return columns
    
    def process_financial_table_with_sql(self, source_table: str, target_table: str, start_date: str, end_date: str):
        """
        使用SQL直接处理财务数据表，生成截面数据
        
        Args:
            source_table: 源表名
            target_table: 目标表名
            start_date: 开始日期
            end_date: 结束日期
        """
        logger.info(f"开始使用SQL处理 {source_table} -> {target_table}")
        
        try:
            with self.db_manager.engine.connect() as conn:
                # 获取源表的列名
                source_columns = self.get_table_columns(source_table)
                
                # 构建财务指标字段列表（排除系统字段）
                exclude_fields = ['code', 'pubDate', 'statDate', 'created_at', 'updated_at']
                financial_fields = [col for col in source_columns if col not in exclude_fields]
                financial_fields_str = ', '.join([f'f.{col}' for col in financial_fields])
                
                # 构建INSERT字段列表
                insert_fields = ['code', 'date'] + financial_fields + ['pubDate', 'statDate']
                insert_fields_str = ', '.join(insert_fields)
                
                # 构建VALUES字段列表
                values_fields = ['f.code', 't.calendar_date'] + [f'f.{col}' for col in financial_fields] + ['f.pubDate', 'f.statDate']
                values_fields_str = ', '.join(values_fields)
                
                # 使用SQL直接生成截面数据
                sql = f"""
                INSERT INTO {target_table} ({insert_fields_str})
                SELECT {values_fields_str}
                FROM {source_table} f
                CROSS JOIN trade_dates t
                WHERE t.calendar_date BETWEEN :start_date AND :end_date
                AND t.is_trading_day = 1
                AND f.pubDate IS NOT NULL
                AND t.calendar_date > f.pubDate
                AND (
                    t.calendar_date < (
                        SELECT COALESCE(MIN(f2.pubDate), :end_date)
                        FROM {source_table} f2
                        WHERE f2.code = f.code
                        AND f2.pubDate > f.pubDate
                    )
                )
                ON DUPLICATE KEY UPDATE
                {', '.join([f'{col} = VALUES({col})' for col in financial_fields])},
                pubDate = VALUES(pubDate),
                statDate = VALUES(statDate),
                updated_at = CURRENT_TIMESTAMP
                """
                
                # 执行SQL
                result = conn.execute(text(sql), {
                    'start_date': start_date,
                    'end_date': end_date
                })
                
                conn.commit()
                
                # 获取插入的记录数
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}"))
                count = count_result.fetchone()[0]
                
                logger.info(f"成功处理 {source_table} -> {target_table}: {count} 条记录")
                
        except Exception as e:
            logger.error(f"处理 {source_table} 失败: {str(e)}")
            raise
    
    def process_financial_table(self, source_table: str, target_table: str, start_date: str, end_date: str):
        """
        处理单个财务数据表（使用SQL方法）
        
        Args:
            source_table: 源表名
            target_table: 目标表名
            start_date: 开始日期
            end_date: 结束日期
        """
        # 使用SQL方法处理
        self.process_financial_table_with_sql(source_table, target_table, start_date, end_date)
    
    def process_all_financial_tables(self, start_date: str = '2020-01-01', end_date: str = None):
        """
        处理所有财务数据表
        
        Args:
            start_date: 开始日期
            end_date: 结束日期，默认为今天
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"开始处理所有财务数据表: {start_date} 到 {end_date}")
        
        # 创建DWD表
        self.create_dwd_tables()
        
        # 处理每个财务数据表
        for source_table, target_table in self.financial_tables.items():
            try:
                self.process_financial_table(source_table, target_table, start_date, end_date)
            except Exception as e:
                logger.error(f"处理 {source_table} 失败: {str(e)}")
                continue
        
        logger.info("所有财务数据表处理完成")
    
    def create_dwd_tables(self):
        """创建DWD表"""
        try:
            with self.db_manager.engine.connect() as conn:
                # 读取DWD schema文件
                with open('database/dwd_schema.sql', 'r', encoding='utf-8') as f:
                    schema_sql = f.read()
                
                # 执行SQL创建表
                for statement in schema_sql.split(';'):
                    statement = statement.strip()
                    if statement:
                        conn.execute(text(statement))
                
                conn.commit()
                logger.info("DWD表创建完成")
                
        except Exception as e:
            logger.error(f"创建DWD表失败: {str(e)}")
            raise
    
    def get_dwd_data_summary(self) -> Dict[str, int]:
        """
        获取DWD数据汇总
        
        Returns:
            各DWD表的记录数
        """
        summary = {}
        
        with self.db_manager.engine.connect() as conn:
            for target_table in self.financial_tables.values():
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}"))
                    count = result.fetchone()[0]
                    summary[target_table] = count
                except Exception as e:
                    logger.warning(f"获取 {target_table} 记录数失败: {str(e)}")
                    summary[target_table] = 0
        
        return summary


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='DWD层财务数据处理')
    parser.add_argument('--start-date', default='2020-01-01', help='开始日期')
    parser.add_argument('--end-date', help='结束日期')
    parser.add_argument('--table', help='指定处理的表名')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = DWDProcessor()
    
    if args.table:
        # 处理指定表
        if args.table in processor.financial_tables:
            source_table = args.table
            target_table = processor.financial_tables[args.table]
            processor.process_financial_table(source_table, target_table, args.start_date, args.end_date)
        else:
            logger.error(f"未知的表名: {args.table}")
    else:
        # 处理所有表
        processor.process_all_financial_tables(args.start_date, args.end_date)
    
    # 显示汇总信息
    summary = processor.get_dwd_data_summary()
    print("\n📊 DWD数据汇总:")
    for table, count in summary.items():
        print(f"  {table}: {count:,} 条记录")


if __name__ == '__main__':
    main()
