#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BaoStock2DB 配置文件
"""

import os
from typing import Dict, Any

class Config:
    """配置类"""
    
    # 数据库配置
    DATABASE_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'baostock',
        'charset': 'utf8mb4'
    }
    
    # BaoStock配置
    BAOSTOCK_CONFIG = {
        'login_timeout': 30,
        'request_timeout': 30,
        'max_retries': 5,  # 增加重试次数
        'retry_delay': 3   # 增加重试延迟
    }
    
    # 数据获取配置
    DATA_CONFIG = {
        'batch_size': 50,   # 减少批量处理大小
        'max_workers': 2,   # 减少最大并发数
        'chunk_size': 500,  # 减少数据块大小
        'default_start_date': '2020-01-01',  # 默认开始日期
        'default_end_date': None,  # 默认结束日期（None表示当前日期）
        'strict_error_handling': True,  # 严格异常处理，遇到错误立即停止
    }
    
    # 成分股类型配置
    INDEX_TYPES = {
        'sz50': {
            'name': '上证50',
            'function': 'query_sz50_stocks',
            'description': '上证50成分股'
        },
        'hs300': {
            'name': '沪深300',
            'function': 'query_hs300_stocks', 
            'description': '沪深300成分股'
        },
        'zz500': {
            'name': '中证500',
            'function': 'query_zz500_stocks',
            'description': '中证500成分股'
        },
        'all': {
            'name': '全部股票',
            'function': 'query_all_stock',
            'description': '全部股票'
        }
    }
    
    # 数据表配置
    TABLE_CONFIG = {
        'stock_basic': {
            'name': 'stock_basic',
            'description': '股票基本信息表',
            'primary_key': ['code']
        },
        'stock_kline': {
            'name': 'stock_kline',
            'description': '股票K线数据表',
            'primary_key': ['code', 'date', 'frequency']
        },
        'stock_profit': {
            'name': 'stock_profit',
            'description': '股票盈利能力数据表',
            'primary_key': ['code', 'statDate']
        },
        'stock_operation': {
            'name': 'stock_operation',
            'description': '股票营运能力数据表',
            'primary_key': ['code', 'statDate']
        },
        'stock_growth': {
            'name': 'stock_growth',
            'description': '股票成长能力数据表',
            'primary_key': ['code', 'statDate']
        },
        'stock_balance': {
            'name': 'stock_balance',
            'description': '股票偿债能力数据表',
            'primary_key': ['code', 'statDate']
        },
        'stock_cashflow': {
            'name': 'stock_cashflow',
            'description': '股票现金流量数据表',
            'primary_key': ['code', 'statDate']
        },
        'stock_dupont': {
            'name': 'stock_dupont',
            'description': '股票杜邦指标数据表',
            'primary_key': ['code', 'statDate']
        },
        'stock_performance': {
            'name': 'stock_performance',
            'description': '股票业绩快报数据表',
            'primary_key': ['code', 'performanceExpStatDate']
        },
        'stock_forecast': {
            'name': 'stock_forecast',
            'description': '股票业绩预告数据表',
            'primary_key': ['code', 'profitForcastExpStatDate']
        },
        'stock_industry': {
            'name': 'stock_industry',
            'description': '股票行业分类数据表',
            'primary_key': ['code']
        },
        'trade_dates': {
            'name': 'trade_dates',
            'description': '交易日历表',
            'primary_key': ['calendar_date']
        },
        'macro_deposit_rate': {
            'name': 'macro_deposit_rate',
            'description': '存款利率数据表',
            'primary_key': ['pubDate']
        },
        'macro_loan_rate': {
            'name': 'macro_loan_rate',
            'description': '贷款利率数据表',
            'primary_key': ['pubDate']
        },
        'macro_reserve_ratio': {
            'name': 'macro_reserve_ratio',
            'description': '存款准备金率数据表',
            'primary_key': ['pubDate']
        },
        'macro_money_supply': {
            'name': 'macro_money_supply',
            'description': '货币供应量数据表',
            'primary_key': ['statYear', 'statMonth']
        },
        'stock_adjust_factor': {
            'name': 'stock_adjust_factor',
            'description': '股票复权因子数据表',
            'primary_key': ['code', 'dividOperateDate']
        },
        'stock_dividend': {
            'name': 'stock_dividend',
            'description': '股票除权除息信息表',
            'primary_key': ['code', 'dividOperateDate']
        }
    }
    
    # 日志配置
    LOG_CONFIG = {
        'level': 'INFO',
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'file': 'baostock2db.log',
        'max_bytes': 10 * 1024 * 1024,  # 10MB
        'backup_count': 5
    }
    
    @classmethod
    def get_database_url(cls) -> str:
        """获取数据库连接URL"""
        config = cls.DATABASE_CONFIG
        return f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?charset={config['charset']}"
    
    @classmethod
    def get_table_config(cls, table_name: str) -> Dict[str, Any]:
        """获取表配置"""
        return cls.TABLE_CONFIG.get(table_name, {})
    
    @classmethod
    def get_index_config(cls, index_type: str) -> Dict[str, Any]:
        """获取成分股配置"""
        return cls.INDEX_TYPES.get(index_type, {})
