"""
数据获取模块

负责从各种数据源获取原始数据，包括：
- 股票基础信息
- K线数据
- 财务数据
- 行业分类数据
- 指数成分股数据
"""

from .data_fetcher import BaoStockDataFetcher
from .batch_processor import BatchProcessor

__all__ = ['BaoStockDataFetcher', 'BatchProcessor']
