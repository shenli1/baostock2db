#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BaoStock数据获取模块
"""

import baostock as bs
import pandas as pd
import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, date, timedelta
import time
import re

from config import Config

class BaoStockDataFetcher:
    """BaoStock数据获取类"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.is_logged_in = False
        self.config = Config.BAOSTOCK_CONFIG
        self._login()
    
    def _login(self):
        """登录BaoStock"""
        try:
            result = bs.login()
            if result.error_code == '0':
                self.is_logged_in = True
                self.logger.info("BaoStock登录成功")
            else:
                self.logger.error(f"BaoStock登录失败: {result.error_msg}")
                raise Exception(f"登录失败: {result.error_msg}")
        except Exception as e:
            self.logger.error(f"BaoStock登录异常: {str(e)}")
            raise
    
    def _logout(self):
        """登出BaoStock"""
        try:
            if self.is_logged_in:
                bs.logout()
                self.is_logged_in = False
                self.logger.info("BaoStock登出成功")
        except Exception as e:
            self.logger.error(f"BaoStock登出异常: {str(e)}")
    
    def _retry_request(self, func, *args, **kwargs):
        """重试请求"""
        last_exception = None
        for attempt in range(self.config['max_retries']):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                error_str = str(e).lower()
                
                # 检查是否是网络相关错误
                is_network_error = any(keyword in error_str for keyword in [
                    '网络接收错误', 'utf-8', 'codec', 'decompressing', 
                    'invalid', 'connection', 'timeout', 'socket'
                ])
                
                if attempt == self.config['max_retries'] - 1:
                    # 最后一次重试失败，抛出异常
                    self.logger.error(f"请求失败，已重试{self.config['max_retries']}次: {str(e)}")
                    raise Exception(f"请求失败，已重试{self.config['max_retries']}次: {str(e)}") from e
                
                # 计算延迟时间（指数退避）
                delay = self.config['retry_delay'] * (2 ** attempt)
                if is_network_error:
                    delay = min(delay * 2, 30)  # 网络错误使用更长的延迟，最大30秒
                
                self.logger.warning(f"请求失败，第{attempt + 1}次重试，{delay}秒后重试: {str(e)}")
                time.sleep(delay)
        
        # 如果所有重试都失败，抛出最后一个异常
        if last_exception:
            raise Exception(f"请求失败，已重试{self.config['max_retries']}次: {str(last_exception)}") from last_exception
    
    def get_stock_list(self, index_type: str = 'all') -> List[Dict[str, Any]]:
        """
        获取股票列表
        
        Args:
            index_type: 指数类型 ('sz50', 'hs300', 'zz500', 'all')
            
        Returns:
            股票列表
        """
        try:
            index_config = Config.get_index_config(index_type)
            if not index_config:
                raise ValueError(f"不支持的指数类型: {index_type}")
            
            function_name = index_config['function']
            
            if function_name == 'query_all_stock':
                # 获取所有股票需要指定日期
                today = datetime.now().strftime('%Y-%m-%d')
                result = bs.query_all_stock(day=today)
            else:
                # 成分股接口不需要参数
                result = getattr(bs, function_name)()
            
            if result.error_code != '0':
                raise Exception(f"获取股票列表失败: {result.error_msg}")
            
            stocks = []
            while result.next():
                row_data = result.get_row_data()
                if row_data:
                    # 根据字段名构建字典
                    field_names = result.fields
                    stock_info = {}
                    
                    for i, value in enumerate(row_data):
                        if i < len(field_names):
                            field_name = field_names[i]
                            stock_info[field_name] = value
                    
                    stocks.append(stock_info)
            
            self.logger.info(f"获取到 {len(stocks)} 只股票")
            return stocks
            
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {str(e)}")
            raise
    
    def get_stock_basic_info(self, code: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取股票基本信息
        
        Args:
            code: 股票代码，None表示获取所有股票
            
        Returns:
            股票基本信息列表
        """
        try:
            result = bs.query_stock_basic(code=code)
            
            if result.error_code != '0':
                raise Exception(f"获取股票基本信息失败: {result.error_msg}")
            
            stocks = []
            while result.next():
                row_data = result.get_row_data()
                if row_data:
                    field_names = result.fields
                    stock_info = {}
                    
                    for i, value in enumerate(row_data):
                        if i < len(field_names):
                            field_name = field_names[i]
                            stock_info[field_name] = value
                    
                    stocks.append(stock_info)
            
            self.logger.info(f"获取到 {len(stocks)} 条股票基本信息")
            return stocks
            
        except Exception as e:
            self.logger.error(f"获取股票基本信息失败: {str(e)}")
            raise
    
    def get_stock_kline_data(self, code: str, start_date: str, end_date: str, 
                           frequency: str = 'd', adjustflag: str = '3') -> List[Dict[str, Any]]:
        """
        获取股票K线数据
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率 ('d', 'w', 'm', '5', '15', '30', '60')
            adjustflag: 复权类型 ('1', '2', '3')
            
        Returns:
            K线数据列表
            
        Raises:
            Exception: 当获取数据失败时抛出异常
        """
        def _fetch_kline_data():
            """内部获取K线数据的函数"""
            # 构建字段列表
            fields = "date,code,open,close,high,low,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
            
            result = bs.query_history_k_data_plus(
                code=code,
                fields=fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag
            )
            
            if result.error_code != '0':
                error_msg = f"获取K线数据失败: {result.error_msg}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
            
            kline_data = []
            try:
                while result.next():
                    row_data = result.get_row_data()
                    if row_data:
                        field_names = result.fields
                        kline_info = {}
                        
                        # 确保字段数量匹配
                        min_length = min(len(row_data), len(field_names))
                        for i in range(min_length):
                            field_name = field_names[i]
                            kline_info[field_name] = row_data[i]
                        
                        # 如果数据字段多于字段名，记录警告
                        if len(row_data) > len(field_names):
                            self.logger.warning(f"数据字段数量({len(row_data)})多于字段名数量({len(field_names)})")
                        
                        # 添加频率信息
                        kline_info['frequency'] = frequency
                        kline_data.append(kline_info)
            except Exception as e:
                error_msg = f"解析K线数据失败: {str(e)}"
                self.logger.error(error_msg)
                raise Exception(error_msg) from e
            
            return kline_data
        
        try:
            # 使用重试机制获取数据
            kline_data = self._retry_request(_fetch_kline_data)
            
            if len(kline_data) == 0:
                error_msg = f"股票 {code} 在指定时间范围内没有K线数据"
                self.logger.warning(error_msg)
                raise Exception(error_msg)
            
            self.logger.info(f"获取到 {len(kline_data)} 条K线数据")
            return kline_data
            
        except Exception as e:
            error_msg = f"获取股票 {code} K线数据失败: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def get_financial_data(self, code: str, year: str, quarter: str, 
                          data_type: str) -> List[Dict[str, Any]]:
        """
        获取财务数据
        
        Args:
            code: 股票代码
            year: 年份
            quarter: 季度
            data_type: 数据类型 ('profit', 'operation', 'growth', 'balance', 'cashflow', 'dupont')
            
        Returns:
            财务数据列表
        """
        try:
            function_map = {
                'profit': bs.query_profit_data,
                'operation': bs.query_operation_data,
                'growth': bs.query_growth_data,
                'balance': bs.query_balance_data,
                'cashflow': bs.query_cash_flow_data,
                'dupont': bs.query_dupont_data
            }
            
            if data_type not in function_map:
                raise ValueError(f"不支持的财务数据类型: {data_type}")
            
            result = function_map[data_type](
                code=code,
                year=year,
                quarter=quarter
            )
            
            if result.error_code != '0':
                raise Exception(f"获取{data_type}数据失败: {result.error_msg}")
            
            financial_data = []
            while result.next():
                row_data = result.get_row_data()
                if row_data:
                    field_names = result.fields
                    financial_info = {}
                    
                    for i, value in enumerate(row_data):
                        if i < len(field_names):
                            field_name = field_names[i]
                            financial_info[field_name] = value
                    
                    financial_data.append(financial_info)
            
            self.logger.info(f"获取到 {len(financial_data)} 条{data_type}数据")
            return financial_data
            
        except Exception as e:
            self.logger.error(f"获取{data_type}数据失败: {str(e)}")
            raise
    
    def get_performance_data(self, code: str, start_date: str, end_date: str, 
                           data_type: str) -> List[Dict[str, Any]]:
        """
        获取业绩数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            data_type: 数据类型 ('express', 'forecast')
            
        Returns:
            业绩数据列表
        """
        try:
            function_map = {
                'express': bs.query_performance_express_report,
                'forecast': bs.query_forecast_report
            }
            
            if data_type not in function_map:
                raise ValueError(f"不支持的业绩数据类型: {data_type}")
            
            result = function_map[data_type](
                code=code,
                start_date=start_date,
                end_date=end_date
            )
            
            if result.error_code != '0':
                raise Exception(f"获取{data_type}数据失败: {result.error_msg}")
            
            performance_data = []
            while result.next():
                row_data = result.get_row_data()
                if row_data:
                    field_names = result.fields
                    performance_info = {}
                    
                    for i, value in enumerate(row_data):
                        if i < len(field_names):
                            field_name = field_names[i]
                            performance_info[field_name] = value
                    
                    performance_data.append(performance_info)
            
            self.logger.info(f"获取到 {len(performance_data)} 条{data_type}数据")
            return performance_data
            
        except Exception as e:
            self.logger.error(f"获取{data_type}数据失败: {str(e)}")
            raise
    
    def get_industry_data(self, code: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取行业分类数据
        
        Args:
            code: 股票代码，None表示获取所有股票
            
        Returns:
            行业分类数据列表
        """
        try:
            result = bs.query_stock_industry(code=code)
            
            if result.error_code != '0':
                raise Exception(f"获取行业分类数据失败: {result.error_msg}")
            
            industry_data = []
            while result.next():
                row_data = result.get_row_data()
                if row_data:
                    field_names = result.fields
                    industry_info = {}
                    
                    for i, value in enumerate(row_data):
                        if i < len(field_names):
                            field_name = field_names[i]
                            industry_info[field_name] = value
                    
                    industry_data.append(industry_info)
            
            self.logger.info(f"获取到 {len(industry_data)} 条行业分类数据")
            return industry_data
            
        except Exception as e:
            self.logger.error(f"获取行业分类数据失败: {str(e)}")
            raise
    
    def get_trade_dates(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        获取交易日历
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日历数据列表
        """
        try:
            result = bs.query_trade_dates(start_date=start_date, end_date=end_date)
            
            if result.error_code != '0':
                raise Exception(f"获取交易日历失败: {result.error_msg}")
            
            trade_dates = []
            while result.next():
                row_data = result.get_row_data()
                if row_data:
                    field_names = result.fields
                    trade_info = {}
                    
                    for i, value in enumerate(row_data):
                        if i < len(field_names):
                            field_name = field_names[i]
                            trade_info[field_name] = value
                    
                    trade_dates.append(trade_info)
            
            self.logger.info(f"获取到 {len(trade_dates)} 条交易日历数据")
            return trade_dates
            
        except Exception as e:
            self.logger.error(f"获取交易日历失败: {str(e)}")
            raise
    
    def get_macro_data(self, start_date: str, end_date: str, 
                      data_type: str) -> List[Dict[str, Any]]:
        """
        获取宏观经济数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            data_type: 数据类型 ('deposit_rate', 'loan_rate', 'reserve_ratio', 'money_supply')
            
        Returns:
            宏观经济数据列表
        """
        try:
            function_map = {
                'deposit_rate': bs.query_deposit_rate_data,
                'loan_rate': bs.query_loan_rate_data,
                'reserve_ratio': bs.query_required_reserve_ratio_data,
                'money_supply': bs.query_money_supply_data_month
            }
            
            if data_type not in function_map:
                raise ValueError(f"不支持的宏观经济数据类型: {data_type}")
            
            # 货币供应量数据需要特殊处理日期格式
            if data_type == 'money_supply':
                # 转换为YYYY-MM格式
                start_date = start_date[:7]  # YYYY-MM-DD -> YYYY-MM
                end_date = end_date[:7]
            
            result = function_map[data_type](
                start_date=start_date,
                end_date=end_date
            )
            
            if result.error_code != '0':
                raise Exception(f"获取{data_type}数据失败: {result.error_msg}")
            
            macro_data = []
            while result.next():
                row_data = result.get_row_data()
                if row_data:
                    field_names = result.fields
                    macro_info = {}
                    
                    for i, value in enumerate(row_data):
                        if i < len(field_names):
                            field_name = field_names[i]
                            macro_info[field_name] = value
                    
                    macro_data.append(macro_info)
            
            self.logger.info(f"获取到 {len(macro_data)} 条{data_type}数据")
            return macro_data
            
        except Exception as e:
            self.logger.error(f"获取{data_type}数据失败: {str(e)}")
            raise
    
    def get_adjust_factor_data(self, code: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        获取复权因子数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            复权因子数据列表
        """
        try:
            result = bs.query_adjust_factor(
                code=code,
                start_date=start_date,
                end_date=end_date
            )
            
            if result.error_code != '0':
                raise Exception(f"获取复权因子数据失败: {result.error_msg}")
            
            adjust_data = []
            while result.next():
                row_data = result.get_row_data()
                if row_data:
                    field_names = result.fields
                    adjust_info = {}
                    
                    for i, value in enumerate(row_data):
                        if i < len(field_names):
                            field_name = field_names[i]
                            adjust_info[field_name] = value
                    
                    adjust_data.append(adjust_info)
            
            self.logger.info(f"获取到 {len(adjust_data)} 条复权因子数据")
            return adjust_data
            
        except Exception as e:
            self.logger.error(f"获取复权因子数据失败: {str(e)}")
            raise
    
    def get_dividend_data(self, code: str, year: str, year_type: str = 'report') -> List[Dict[str, Any]]:
        """
        获取除权除息数据
        
        Args:
            code: 股票代码
            year: 年份
            year_type: 年份类型 ('report', 'operate')
            
        Returns:
            除权除息数据列表
        """
        try:
            result = bs.query_dividend_data(
                code=code,
                year=year,
                yearType=year_type
            )
            
            if result.error_code != '0':
                raise Exception(f"获取除权除息数据失败: {result.error_msg}")
            
            dividend_data = []
            while result.next():
                row_data = result.get_row_data()
                if row_data:
                    field_names = result.fields
                    dividend_info = {}
                    
                    for i, value in enumerate(row_data):
                        if i < len(field_names):
                            field_name = field_names[i]
                            dividend_info[field_name] = value
                    
                    dividend_data.append(dividend_info)
            
            self.logger.info(f"获取到 {len(dividend_data)} 条除权除息数据")
            return dividend_data
            
        except Exception as e:
            self.logger.error(f"获取除权除息数据失败: {str(e)}")
            raise
    
    def get_latest_trading_date(self) -> str:
        """获取最新交易日"""
        try:
            today = datetime.now()
            # 向前查找最近的交易日
            for i in range(10):  # 最多向前查找10天
                check_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
                trade_dates = self.get_trade_dates(check_date, check_date)
                
                if trade_dates and trade_dates[0].get('is_trading_day') == '1':
                    return check_date
            
            # 如果没找到，返回今天
            return today.strftime('%Y-%m-%d')
            
        except Exception as e:
            self.logger.error(f"获取最新交易日失败: {str(e)}")
            return datetime.now().strftime('%Y-%m-%d')
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._logout()
