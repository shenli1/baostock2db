#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量处理模块
"""

import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_acquisition.data_fetcher import BaoStockDataFetcher
from database.manager_fixed import DatabaseManagerFixed as DatabaseManager
from config import Config

class BatchProcessor:
    """批量处理器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_manager = DatabaseManager()
        self.data_fetcher = BaoStockDataFetcher()
        self.config = Config.DATA_CONFIG
    
    def process_stock_list(self, index_type: str = 'all', 
                          update_basic_info: bool = True) -> List[str]:
        """
        处理股票列表，获取股票基本信息
        
        Args:
            index_type: 指数类型
            update_basic_info: 是否更新基本信息
            
        Returns:
            股票代码列表
        """
        try:
            self.logger.info(f"开始处理{index_type}股票列表")
            
            # 获取股票列表
            stocks = self.data_fetcher.get_stock_list(index_type)
            
            if not stocks:
                self.logger.warning(f"未获取到{index_type}股票数据")
                return []
            
            # 提取股票代码
            stock_codes = []
            for stock in stocks:
                if 'code' in stock:
                    stock_codes.append(stock['code'])
            
            self.logger.info(f"获取到 {len(stock_codes)} 只股票")
            
            # 更新股票基本信息
            if update_basic_info:
                self._update_stock_basic_info(stocks)
            
            return stock_codes
            
        except Exception as e:
            self.logger.error(f"处理股票列表失败: {str(e)}")
            raise
    
    def _update_stock_basic_info(self, stocks: List[Dict[str, Any]]):
        """更新股票基本信息"""
        try:
            if not stocks:
                return
            
            # 保存到数据库
            self.db_manager.upsert_data('stock_basic', stocks, ['code'])
            self.logger.info(f"更新了 {len(stocks)} 条股票基本信息")
            
        except Exception as e:
            self.logger.error(f"更新股票基本信息失败: {str(e)}")
            raise
    
    def process_kline_data(self, stock_codes: List[str], 
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None,
                          frequency: str = 'd',
                          adjustflag: str = '3',
                          incremental: bool = False,
                          max_workers: int = 4) -> Dict[str, int]:
        """
        批量处理K线数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            adjustflag: 复权类型
            incremental: 是否增量更新
            max_workers: 最大并发数
            
        Returns:
            处理结果统计
        """
        try:
            self.logger.info(f"开始批量处理K线数据，股票数量: {len(stock_codes)}")
            
            # 设置默认日期
            if not end_date:
                # 使用当前日期作为结束日期
                from datetime import date
                end_date = date.today().strftime('%Y-%m-%d')
            
            if not start_date:
                if incremental:
                    # 增量更新：从最新日期开始
                    start_date = end_date
                else:
                    # 全量更新：从默认开始日期
                    start_date = self.config['default_start_date']
            
            # 统计结果
            stats = {
                'total_stocks': len(stock_codes),
                'success_count': 0,
                'failed_count': 0,
                'total_records': 0,
                'failed_stocks': []
            }
            
            # 使用线程池并发处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务
                future_to_code = {}
                for code in stock_codes:
                    future = executor.submit(
                        self._process_single_stock_kline,
                        code, start_date, end_date, frequency, adjustflag, incremental
                    )
                    future_to_code[future] = code
                
                # 处理结果
                with tqdm(total=len(stock_codes), desc="处理K线数据") as pbar:
                    for future in as_completed(future_to_code):
                        code = future_to_code[future]
                        try:
                            result = future.result()
                            if result['success']:
                                stats['success_count'] += 1
                                stats['total_records'] += result['records']
                            else:
                                # 遇到失败，立即抛出异常阻断处理
                                error_msg = f"处理股票 {code} 失败: {result['error']}"
                                self.logger.error(error_msg)
                                raise Exception(error_msg)
                        except Exception as e:
                            # 遇到异常，立即抛出异常阻断处理
                            error_msg = f"处理股票 {code} 异常: {str(e)}"
                            self.logger.error(error_msg)
                            raise Exception(error_msg) from e
                        
                        pbar.update(1)
            
            self.logger.info(f"K线数据处理完成: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"批量处理K线数据失败: {str(e)}")
            raise
    
    def _process_single_stock_kline(self, code: str, start_date: str, end_date: str,
                                   frequency: str, adjustflag: str, incremental: bool) -> Dict[str, Any]:
        """处理单只股票的K线数据"""
        try:
            # 如果是增量更新，检查最新日期
            if incremental:
                latest_date = self.db_manager.get_latest_date('stock_kline', 'date', 'code', code)
                if latest_date:
                    # 从最新日期的下一天开始
                    next_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
                    if next_date > end_date:
                        return {'success': True, 'records': 0, 'message': '数据已是最新'}
                    start_date = next_date
            
            # 获取K线数据
            kline_data = self.data_fetcher.get_stock_kline_data(
                code, start_date, end_date, frequency, adjustflag
            )
            
            if not kline_data:
                error_msg = f"股票 {code} 没有K线数据"
                self.logger.warning(error_msg)
                raise Exception(error_msg)
            
            # 保存到数据库
            self.db_manager.upsert_data('stock_kline', kline_data, ['code', 'date', 'frequency'])
            
            return {'success': True, 'records': len(kline_data)}
            
        except Exception as e:
            # 直接抛出异常，不再返回失败状态
            error_msg = f"处理股票 {code} K线数据失败: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def process_financial_data(self, stock_codes: List[str], 
                              start_date: str, end_date: str,
                              data_types: List[str] = None,
                              max_workers: int = 4) -> Dict[str, int]:
        """
        批量处理财务数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            data_types: 数据类型列表
            max_workers: 最大并发数
            
        Returns:
            处理结果统计
        """
        try:
            if not data_types:
                data_types = ['profit', 'operation', 'growth', 'balance', 'cashflow', 'dupont']
            
            self.logger.info(f"开始批量处理财务数据，股票数量: {len(stock_codes)}")
            
            # 统计结果
            stats = {
                'total_stocks': len(stock_codes),
                'success_count': 0,
                'failed_count': 0,
                'total_records': 0,
                'failed_stocks': []
            }
            
            # 使用线程池并发处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务
                future_to_code = {}
                for code in stock_codes:
                    future = executor.submit(
                        self._process_single_stock_financial,
                        code, start_date, end_date, data_types
                    )
                    future_to_code[future] = code
                
                # 处理结果
                with tqdm(total=len(stock_codes), desc="处理财务数据") as pbar:
                    for future in as_completed(future_to_code):
                        code = future_to_code[future]
                        try:
                            result = future.result()
                            if result['success']:
                                stats['success_count'] += 1
                                stats['total_records'] += result['records']
                            else:
                                # 遇到失败，立即抛出异常阻断处理
                                error_msg = f"处理股票 {code} 失败: {result['error']}"
                                self.logger.error(error_msg)
                                raise Exception(error_msg)
                        except Exception as e:
                            # 遇到异常，立即抛出异常阻断处理
                            error_msg = f"处理股票 {code} 异常: {str(e)}"
                            self.logger.error(error_msg)
                            raise Exception(error_msg) from e
                        
                        pbar.update(1)
            
            self.logger.info(f"财务数据处理完成: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"批量处理财务数据失败: {str(e)}")
            raise
    
    def _process_single_stock_financial(self, code: str, start_date: str, end_date: str,
                                       data_types: List[str]) -> Dict[str, Any]:
        """处理单只股票的财务数据"""
        try:
            total_records = 0
            
            # 生成年份和季度的组合
            year_quarters = self._generate_year_quarters(start_date, end_date)
            
            for data_type in data_types:
                for year, quarter in year_quarters:
                    # 获取财务数据
                    financial_data = self.data_fetcher.get_financial_data(code, year, quarter, data_type)
                    
                    if financial_data:
                        # 确定表名
                        table_name = f'stock_{data_type}'
                        
                        # 确定主键
                        primary_keys = ['code', 'statDate']
                        
                        # 保存到数据库
                        self.db_manager.upsert_data(table_name, financial_data, primary_keys)
                        total_records += len(financial_data)
            
            return {'success': True, 'records': total_records}
            
        except Exception as e:
            # 直接抛出异常，不再返回失败状态
            error_msg = f"处理股票 {code} 财务数据失败: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def _generate_year_quarters(self, start_date: str, end_date: str) -> List[tuple]:
        """
        根据日期范围生成年份和季度的组合
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            年份和季度的组合列表 [(year, quarter), ...]
        """
        from datetime import datetime
        
        start_year = int(start_date[:4])
        start_month = int(start_date[5:7])
        end_year = int(end_date[:4])
        end_month = int(end_date[5:7])
        
        year_quarters = []
        
        # 计算开始年份的季度
        start_quarter = (start_month - 1) // 3 + 1
        
        # 计算结束年份的季度
        end_quarter = (end_month - 1) // 3 + 1
        
        # 生成所有年份和季度的组合
        for year in range(start_year, end_year + 1):
            if year == start_year:
                # 开始年份，从开始季度到第4季度
                quarters = range(start_quarter, 5)
            elif year == end_year:
                # 结束年份，从第1季度到结束季度
                quarters = range(1, end_quarter + 1)
            else:
                # 中间年份，包含所有季度
                quarters = range(1, 5)
            
            for quarter in quarters:
                year_quarters.append((str(year), str(quarter)))
        
        return year_quarters
    
    def process_performance_data(self, stock_codes: List[str],
                                start_date: str, end_date: str,
                                data_types: List[str] = None,
                                max_workers: int = 4) -> Dict[str, int]:
        """
        批量处理业绩数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            data_types: 数据类型列表
            max_workers: 最大并发数
            
        Returns:
            处理结果统计
        """
        try:
            if not data_types:
                data_types = ['express', 'forecast']
            
            self.logger.info(f"开始批量处理业绩数据，股票数量: {len(stock_codes)}")
            
            # 统计结果
            stats = {
                'total_stocks': len(stock_codes),
                'success_count': 0,
                'failed_count': 0,
                'total_records': 0,
                'failed_stocks': []
            }
            
            # 使用线程池并发处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务
                future_to_code = {}
                for code in stock_codes:
                    future = executor.submit(
                        self._process_single_stock_performance,
                        code, start_date, end_date, data_types
                    )
                    future_to_code[future] = code
                
                # 处理结果
                with tqdm(total=len(stock_codes), desc="处理业绩数据") as pbar:
                    for future in as_completed(future_to_code):
                        code = future_to_code[future]
                        try:
                            result = future.result()
                            if result['success']:
                                stats['success_count'] += 1
                                stats['total_records'] += result['records']
                            else:
                                stats['failed_count'] += 1
                                stats['failed_stocks'].append(code)
                                self.logger.error(f"处理股票 {code} 失败: {result['error']}")
                        except Exception as e:
                            stats['failed_count'] += 1
                            stats['failed_stocks'].append(code)
                            self.logger.error(f"处理股票 {code} 异常: {str(e)}")
                        
                        pbar.update(1)
            
            self.logger.info(f"业绩数据处理完成: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"批量处理业绩数据失败: {str(e)}")
            raise
    
    def _process_single_stock_performance(self, code: str, start_date: str, end_date: str,
                                         data_types: List[str]) -> Dict[str, Any]:
        """处理单只股票的业绩数据"""
        try:
            total_records = 0
            
            for data_type in data_types:
                # 获取业绩数据
                performance_data = self.data_fetcher.get_performance_data(code, start_date, end_date, data_type)
                
                if performance_data:
                    # 确定表名和主键
                    if data_type == 'express':
                        table_name = 'stock_performance'
                        primary_keys = ['code', 'performanceExpStatDate']
                    else:  # forecast
                        table_name = 'stock_forecast'
                        primary_keys = ['code', 'profitForcastExpStatDate']
                    
                    # 保存到数据库
                    self.db_manager.upsert_data(table_name, performance_data, primary_keys)
                    total_records += len(performance_data)
            
            return {'success': True, 'records': total_records}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def process_industry_data(self, stock_codes: List[str] = None) -> int:
        """
        处理行业分类数据
        
        Args:
            stock_codes: 股票代码列表，None表示处理所有股票
            
        Returns:
            处理的记录数
        """
        try:
            self.logger.info("开始处理行业分类数据")
            
            # 获取行业数据
            industry_data = self.data_fetcher.get_industry_data()
            
            if not industry_data:
                self.logger.warning("未获取到行业分类数据")
                return 0
            
            # 如果指定了股票代码，过滤数据
            if stock_codes:
                filtered_data = [item for item in industry_data if item.get('code') in stock_codes]
                industry_data = filtered_data
            
            # 保存到数据库
            self.db_manager.upsert_data('stock_industry', industry_data, ['code'])
            
            self.logger.info(f"处理了 {len(industry_data)} 条行业分类数据")
            return len(industry_data)
            
        except Exception as e:
            self.logger.error(f"处理行业分类数据失败: {str(e)}")
            raise
    
    def process_trade_dates(self, start_date: str, end_date: str) -> int:
        """
        处理交易日历数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            处理的记录数
        """
        try:
            self.logger.info(f"开始处理交易日历数据: {start_date} 到 {end_date}")
            
            # 获取交易日历数据
            trade_dates = self.data_fetcher.get_trade_dates(start_date, end_date)
            
            if not trade_dates:
                self.logger.warning("未获取到交易日历数据")
                return 0
            
            # 保存到数据库
            self.db_manager.upsert_data('trade_dates', trade_dates, ['calendar_date'])
            
            self.logger.info(f"处理了 {len(trade_dates)} 条交易日历数据")
            return len(trade_dates)
            
        except Exception as e:
            self.logger.error(f"处理交易日历数据失败: {str(e)}")
            raise
    
    def process_macro_data(self, start_date: str, end_date: str,
                          data_types: List[str] = None) -> Dict[str, int]:
        """
        处理宏观经济数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            data_types: 数据类型列表
            
        Returns:
            处理结果统计
        """
        try:
            if not data_types:
                data_types = ['deposit_rate', 'loan_rate', 'reserve_ratio', 'money_supply']
            
            self.logger.info(f"开始处理宏观经济数据: {start_date} 到 {end_date}")
            
            stats = {}
            
            for data_type in data_types:
                try:
                    # 获取宏观经济数据
                    macro_data = self.data_fetcher.get_macro_data(start_date, end_date, data_type)
                    
                    if macro_data:
                        # 确定表名和主键
                        table_name = f'macro_{data_type}'
                        
                        if data_type == 'money_supply':
                            primary_keys = ['statYear', 'statMonth']
                        else:
                            primary_keys = ['pubDate']
                        
                        # 保存到数据库
                        self.db_manager.upsert_data(table_name, macro_data, primary_keys)
                        stats[data_type] = len(macro_data)
                        self.logger.info(f"处理了 {len(macro_data)} 条{data_type}数据")
                    else:
                        stats[data_type] = 0
                        self.logger.warning(f"未获取到{data_type}数据")
                        
                except Exception as e:
                    self.logger.error(f"处理{data_type}数据失败: {str(e)}")
                    stats[data_type] = 0
            
            self.logger.info(f"宏观经济数据处理完成: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"批量处理宏观经济数据失败: {str(e)}")
            raise
    
    def process_adjust_factor_data(self, stock_codes: List[str],
                                  start_date: str, end_date: str,
                                  max_workers: int = 4) -> Dict[str, int]:
        """
        批量处理复权因子数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            max_workers: 最大并发数
            
        Returns:
            处理结果统计
        """
        try:
            self.logger.info(f"开始批量处理复权因子数据，股票数量: {len(stock_codes)}")
            
            # 统计结果
            stats = {
                'total_stocks': len(stock_codes),
                'success_count': 0,
                'failed_count': 0,
                'total_records': 0,
                'failed_stocks': []
            }
            
            # 使用线程池并发处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务
                future_to_code = {}
                for code in stock_codes:
                    future = executor.submit(
                        self._process_single_stock_adjust_factor,
                        code, start_date, end_date
                    )
                    future_to_code[future] = code
                
                # 处理结果
                with tqdm(total=len(stock_codes), desc="处理复权因子数据") as pbar:
                    for future in as_completed(future_to_code):
                        code = future_to_code[future]
                        try:
                            result = future.result()
                            if result['success']:
                                stats['success_count'] += 1
                                stats['total_records'] += result['records']
                            else:
                                stats['failed_count'] += 1
                                stats['failed_stocks'].append(code)
                                self.logger.error(f"处理股票 {code} 失败: {result['error']}")
                        except Exception as e:
                            stats['failed_count'] += 1
                            stats['failed_stocks'].append(code)
                            self.logger.error(f"处理股票 {code} 异常: {str(e)}")
                        
                        pbar.update(1)
            
            self.logger.info(f"复权因子数据处理完成: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"批量处理复权因子数据失败: {str(e)}")
            raise
    
    def _process_single_stock_adjust_factor(self, code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """处理单只股票的复权因子数据"""
        try:
            # 获取复权因子数据
            adjust_data = self.data_fetcher.get_adjust_factor_data(code, start_date, end_date)
            
            if not adjust_data:
                return {'success': True, 'records': 0, 'message': '无数据'}
            
            # 保存到数据库
            self.db_manager.upsert_data('stock_adjust_factor', adjust_data, ['code', 'dividOperateDate'])
            
            return {'success': True, 'records': len(adjust_data)}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def process_dividend_data(self, stock_codes: List[str], year: str,
                             year_type: str = 'report',
                             max_workers: int = 4) -> Dict[str, int]:
        """
        批量处理除权除息数据
        
        Args:
            stock_codes: 股票代码列表
            year: 年份
            year_type: 年份类型
            max_workers: 最大并发数
            
        Returns:
            处理结果统计
        """
        try:
            self.logger.info(f"开始批量处理除权除息数据，股票数量: {len(stock_codes)}")
            
            # 统计结果
            stats = {
                'total_stocks': len(stock_codes),
                'success_count': 0,
                'failed_count': 0,
                'total_records': 0,
                'failed_stocks': []
            }
            
            # 使用线程池并发处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务
                future_to_code = {}
                for code in stock_codes:
                    future = executor.submit(
                        self._process_single_stock_dividend,
                        code, year, year_type
                    )
                    future_to_code[future] = code
                
                # 处理结果
                with tqdm(total=len(stock_codes), desc="处理除权除息数据") as pbar:
                    for future in as_completed(future_to_code):
                        code = future_to_code[future]
                        try:
                            result = future.result()
                            if result['success']:
                                stats['success_count'] += 1
                                stats['total_records'] += result['records']
                            else:
                                stats['failed_count'] += 1
                                stats['failed_stocks'].append(code)
                                self.logger.error(f"处理股票 {code} 失败: {result['error']}")
                        except Exception as e:
                            stats['failed_count'] += 1
                            stats['failed_stocks'].append(code)
                            self.logger.error(f"处理股票 {code} 异常: {str(e)}")
                        
                        pbar.update(1)
            
            self.logger.info(f"除权除息数据处理完成: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"批量处理除权除息数据失败: {str(e)}")
            raise
    
    def _process_single_stock_dividend(self, code: str, year: str, year_type: str) -> Dict[str, Any]:
        """处理单只股票的除权除息数据"""
        try:
            # 获取除权除息数据
            dividend_data = self.data_fetcher.get_dividend_data(code, year, year_type)
            
            if not dividend_data:
                return {'success': True, 'records': 0, 'message': '无数据'}
            
            # 保存到数据库
            self.db_manager.upsert_data('stock_dividend', dividend_data, ['code', 'dividOperateDate'])
            
            return {'success': True, 'records': len(dividend_data)}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def close(self):
        """关闭连接"""
        if hasattr(self, 'db_manager'):
            self.db_manager.close()
        if hasattr(self, 'data_fetcher'):
            self.data_fetcher._logout()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
