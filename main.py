#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BaoStock2DB 主应用程序
"""

import click
import logging
import sys
import time
from datetime import datetime, date, timedelta
from typing import List, Optional

from config import Config
from database.manager import DatabaseManager
from batch_processor import BatchProcessor

# 配置日志
logging.basicConfig(
    level=getattr(logging, Config.LOG_CONFIG['level']),
    format=Config.LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(Config.LOG_CONFIG['file'], encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

@click.group()
def cli():
    """BaoStock2DB - 将BaoStock数据导入到MySQL数据库"""
    pass

@cli.command()
@click.option('--force', is_flag=True, help='强制重新创建数据库和表')
def init(force):
    """初始化数据库和表结构"""
    try:
        logger.info("开始初始化数据库...")
        
        with DatabaseManager() as db_manager:
            # 创建数据库
            db_manager.create_database()
            
            # 创建表
            if force:
                logger.info("强制重新创建表结构...")
            db_manager.create_tables()
        
        logger.info("数据库初始化完成")
        click.echo("✅ 数据库初始化完成")
        
    except Exception as e:
        logger.error(f"数据库初始化失败: {str(e)}")
        click.echo(f"❌ 数据库初始化失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--index-type', type=click.Choice(['sz50', 'hs300', 'zz500', 'all']), 
              default='all', help='指数类型')
@click.option('--update-basic', is_flag=True, help='更新股票基本信息')
def update_stocks(index_type, update_basic):
    """更新股票列表和基本信息"""
    try:
        logger.info(f"开始更新{index_type}股票列表...")
        
        with BatchProcessor() as processor:
            stock_codes = processor.process_stock_list(index_type, update_basic)
        
        logger.info(f"成功更新 {len(stock_codes)} 只股票")
        click.echo(f"✅ 成功更新 {len(stock_codes)} 只{index_type}股票")
        
    except Exception as e:
        logger.error(f"更新股票列表失败: {str(e)}")
        click.echo(f"❌ 更新股票列表失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--index-type', type=click.Choice(['sz50', 'hs300', 'zz500', 'all']), 
              default='all', help='指数类型')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--frequency', type=click.Choice(['d', 'w', 'm', '5', '15', '30', '60']), 
              default='d', help='数据频率')
@click.option('--adjustflag', type=click.Choice(['1', '2', '3']), 
              default='3', help='复权类型')
@click.option('--incremental', is_flag=True, help='增量更新')
@click.option('--max-workers', default=2, help='最大并发数')
def update_kline(index_type, start_date, end_date, frequency, adjustflag, incremental, max_workers):
    """更新K线数据"""
    try:
        logger.info(f"开始更新{index_type}K线数据...")
        
        # 设置默认日期
        if not start_date:
            start_date = Config.DATA_CONFIG['default_start_date']
        if not end_date:
            from datetime import date
            end_date = date.today().strftime('%Y-%m-%d')
        
        logger.info(f"更新日期范围: {start_date} 至 {end_date}")
        
        with BatchProcessor() as processor:
            # 获取股票列表
            stock_codes = processor.process_stock_list(index_type, update_basic_info=False)
            
            if not stock_codes:
                click.echo("❌ 未获取到股票列表")
                return
            
            # 处理K线数据
            stats = processor.process_kline_data(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag,
                incremental=incremental,
                max_workers=max_workers
            )
        
        logger.info(f"K线数据更新完成: {stats}")
        click.echo(f"✅ K线数据更新完成:")
        click.echo(f"   总股票数: {stats['total_stocks']}")
        click.echo(f"   成功: {stats['success_count']}")
        click.echo(f"   失败: {stats['failed_count']}")
        click.echo(f"   总记录数: {stats['total_records']}")
        
        if stats['failed_stocks']:
            click.echo(f"   失败股票: {', '.join(stats['failed_stocks'][:10])}{'...' if len(stats['failed_stocks']) > 10 else ''}")
        
    except Exception as e:
        logger.error(f"更新K线数据失败: {str(e)}")
        click.echo(f"❌ 更新K线数据失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--index-type', type=click.Choice(['sz50', 'hs300', 'zz500', 'all']), 
              default='all', help='指数类型')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)，默认使用2020-01-01')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)，默认使用当前日期')
@click.option('--data-types', help='数据类型，逗号分隔 (profit,operation,growth,balance,cashflow,dupont)')
@click.option('--max-workers', default=2, help='最大并发数')
def update_financial(index_type, start_date, end_date, data_types, max_workers):
    """更新财务数据"""
    try:
        logger.info(f"开始更新{index_type}财务数据...")
        
        # 设置默认日期
        if not start_date:
            start_date = Config.DATA_CONFIG['default_start_date']
        if not end_date:
            from datetime import datetime
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"更新财务数据: {start_date} 至 {end_date}")
        
        # 解析数据类型
        if data_types:
            data_type_list = [dt.strip() for dt in data_types.split(',')]
        else:
            data_type_list = ['profit', 'operation', 'growth', 'balance', 'cashflow', 'dupont']
        
        with BatchProcessor() as processor:
            # 获取股票列表
            stock_codes = processor.process_stock_list(index_type, update_basic_info=False)
            
            if not stock_codes:
                click.echo("❌ 未获取到股票列表")
                return
            
            # 处理财务数据
            stats = processor.process_financial_data(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date,
                data_types=data_type_list,
                max_workers=max_workers
            )
        
        logger.info(f"财务数据更新完成: {stats}")
        click.echo(f"✅ 财务数据更新完成:")
        click.echo(f"   总股票数: {stats['total_stocks']}")
        click.echo(f"   成功: {stats['success_count']}")
        click.echo(f"   失败: {stats['failed_count']}")
        click.echo(f"   总记录数: {stats['total_records']}")
        
        if stats['failed_stocks']:
            click.echo(f"   失败股票: {', '.join(stats['failed_stocks'][:10])}{'...' if len(stats['failed_stocks']) > 10 else ''}")
        
    except Exception as e:
        logger.error(f"更新财务数据失败: {str(e)}")
        click.echo(f"❌ 更新财务数据失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--index-type', type=click.Choice(['sz50', 'hs300', 'zz500', 'all']), 
              default='all', help='指数类型')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--data-types', help='数据类型，逗号分隔 (express,forecast)')
@click.option('--max-workers', default=2, help='最大并发数')
def update_performance(index_type, start_date, end_date, data_types, max_workers):
    """更新业绩数据"""
    try:
        logger.info(f"开始更新{index_type}业绩数据...")
        
        # 设置默认日期
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # 解析数据类型
        if data_types:
            data_type_list = [dt.strip() for dt in data_types.split(',')]
        else:
            data_type_list = ['express', 'forecast']
        
        with BatchProcessor() as processor:
            # 获取股票列表
            stock_codes = processor.process_stock_list(index_type, update_basic_info=False)
            
            if not stock_codes:
                click.echo("❌ 未获取到股票列表")
                return
            
            # 处理业绩数据
            stats = processor.process_performance_data(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date,
                data_types=data_type_list,
                max_workers=max_workers
            )
        
        logger.info(f"业绩数据更新完成: {stats}")
        click.echo(f"✅ 业绩数据更新完成:")
        click.echo(f"   总股票数: {stats['total_stocks']}")
        click.echo(f"   成功: {stats['success_count']}")
        click.echo(f"   失败: {stats['failed_count']}")
        click.echo(f"   总记录数: {stats['total_records']}")
        
        if stats['failed_stocks']:
            click.echo(f"   失败股票: {', '.join(stats['failed_stocks'][:10])}{'...' if len(stats['failed_stocks']) > 10 else ''}")
        
    except Exception as e:
        logger.error(f"更新业绩数据失败: {str(e)}")
        click.echo(f"❌ 更新业绩数据失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--index-type', type=click.Choice(['sz50', 'hs300', 'zz500', 'all']), 
              default='all', help='指数类型')
def update_industry(index_type):
    """更新行业分类数据"""
    try:
        logger.info(f"开始更新{index_type}行业分类数据...")
        
        with BatchProcessor() as processor:
            # 获取股票列表
            stock_codes = processor.process_stock_list(index_type, update_basic_info=False)
            
            # 处理行业数据
            records = processor.process_industry_data(stock_codes)
        
        logger.info(f"行业分类数据更新完成: {records} 条记录")
        click.echo(f"✅ 行业分类数据更新完成: {records} 条记录")
        
    except Exception as e:
        logger.error(f"更新行业分类数据失败: {str(e)}")
        click.echo(f"❌ 更新行业分类数据失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--data-types', help='数据类型，逗号分隔 (deposit_rate,loan_rate,reserve_ratio,money_supply)')
def update_macro(start_date, end_date, data_types):
    """更新宏观经济数据"""
    try:
        logger.info("开始更新宏观经济数据...")
        
        # 设置默认日期
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # 解析数据类型
        if data_types:
            data_type_list = [dt.strip() for dt in data_types.split(',')]
        else:
            data_type_list = ['deposit_rate', 'loan_rate', 'reserve_ratio', 'money_supply']
        
        with BatchProcessor() as processor:
            # 处理宏观经济数据
            stats = processor.process_macro_data(
                start_date=start_date,
                end_date=end_date,
                data_types=data_type_list
            )
        
        logger.info(f"宏观经济数据更新完成: {stats}")
        click.echo(f"✅ 宏观经济数据更新完成:")
        for data_type, count in stats.items():
            click.echo(f"   {data_type}: {count} 条记录")
        
    except Exception as e:
        logger.error(f"更新宏观经济数据失败: {str(e)}")
        click.echo(f"❌ 更新宏观经济数据失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
def update_trade_dates(start_date, end_date):
    """更新交易日历数据"""
    try:
        logger.info("开始更新交易日历数据...")
        
        # 设置默认日期
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        with BatchProcessor() as processor:
            # 处理交易日历数据
            records = processor.process_trade_dates(start_date, end_date)
        
        logger.info(f"交易日历数据更新完成: {records} 条记录")
        click.echo(f"✅ 交易日历数据更新完成: {records} 条记录")
        
    except Exception as e:
        logger.error(f"更新交易日历数据失败: {str(e)}")
        click.echo(f"❌ 更新交易日历数据失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--index-type', type=click.Choice(['sz50', 'hs300', 'zz500', 'all']), 
              default='all', help='指数类型')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--max-workers', default=2, help='最大并发数')
def update_adjust_factor(index_type, start_date, end_date, max_workers):
    """更新复权因子数据"""
    try:
        logger.info(f"开始更新{index_type}复权因子数据...")
        
        # 设置默认日期
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        with BatchProcessor() as processor:
            # 获取股票列表
            stock_codes = processor.process_stock_list(index_type, update_basic_info=False)
            
            if not stock_codes:
                click.echo("❌ 未获取到股票列表")
                return
            
            # 处理复权因子数据
            stats = processor.process_adjust_factor_data(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date,
                max_workers=max_workers
            )
        
        logger.info(f"复权因子数据更新完成: {stats}")
        click.echo(f"✅ 复权因子数据更新完成:")
        click.echo(f"   总股票数: {stats['total_stocks']}")
        click.echo(f"   成功: {stats['success_count']}")
        click.echo(f"   失败: {stats['failed_count']}")
        click.echo(f"   总记录数: {stats['total_records']}")
        
        if stats['failed_stocks']:
            click.echo(f"   失败股票: {', '.join(stats['failed_stocks'][:10])}{'...' if len(stats['failed_stocks']) > 10 else ''}")
        
    except Exception as e:
        logger.error(f"更新复权因子数据失败: {str(e)}")
        click.echo(f"❌ 更新复权因子数据失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--index-type', type=click.Choice(['sz50', 'hs300', 'zz500', 'all']), 
              default='all', help='指数类型')
@click.option('--year', required=True, help='年份 (YYYY)')
@click.option('--year-type', type=click.Choice(['report', 'operate']), 
              default='report', help='年份类型')
@click.option('--max-workers', default=2, help='最大并发数')
def update_dividend(index_type, year, year_type, max_workers):
    """更新除权除息数据"""
    try:
        logger.info(f"开始更新{index_type}除权除息数据...")
        
        with BatchProcessor() as processor:
            # 获取股票列表
            stock_codes = processor.process_stock_list(index_type, update_basic_info=False)
            
            if not stock_codes:
                click.echo("❌ 未获取到股票列表")
                return
            
            # 处理除权除息数据
            stats = processor.process_dividend_data(
                stock_codes=stock_codes,
                year=year,
                year_type=year_type,
                max_workers=max_workers
            )
        
        logger.info(f"除权除息数据更新完成: {stats}")
        click.echo(f"✅ 除权除息数据更新完成:")
        click.echo(f"   总股票数: {stats['total_stocks']}")
        click.echo(f"   成功: {stats['success_count']}")
        click.echo(f"   失败: {stats['failed_count']}")
        click.echo(f"   总记录数: {stats['total_records']}")
        
        if stats['failed_stocks']:
            click.echo(f"   失败股票: {', '.join(stats['failed_stocks'][:10])}{'...' if len(stats['failed_stocks']) > 10 else ''}")
        
    except Exception as e:
        logger.error(f"更新除权除息数据失败: {str(e)}")
        click.echo(f"❌ 更新除权除息数据失败: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--index-type', type=click.Choice(['sz50', 'hs300', 'zz500', 'all']), 
              default='all', help='指数类型')
@click.option('--data-types', help='数据类型，逗号分隔 (kline,financial,performance,industry,macro,trade_dates,adjust_factor,dividend)')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--year', help='年份 (YYYY)')
@click.option('--quarter', type=click.Choice(['1', '2', '3', '4']), help='季度')
@click.option('--incremental', is_flag=True, help='增量更新K线数据')
@click.option('--max-workers', default=2, help='最大并发数')
@click.option('--max-stocks', type=int, help='最大股票数量（用于测试）')
@click.option('--delay-seconds', default=0.1, help='股票间延迟秒数')
def update_all(index_type, data_types, start_date, end_date, year, quarter, incremental, max_workers, max_stocks, delay_seconds):
    """更新所有数据（基于单只股票测试逻辑）"""
    try:
        logger.info(f"开始更新{index_type}所有数据...")
        
        # 解析数据类型
        if data_types:
            data_type_list = [dt.strip() for dt in data_types.split(',')]
        else:
            data_type_list = ['kline', 'financial', 'performance', 'industry', 'macro', 'trade_dates', 'adjust_factor', 'dividend']
        
        # 设置默认日期
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = Config.DATA_CONFIG['default_start_date']  # 使用配置的默认开始日期
        logger.info(f"更新日期范围: {start_date} 至 {end_date}")
        
        with BatchProcessor() as processor:
            # 获取股票列表
            stock_codes = processor.process_stock_list(index_type, update_basic_info=True)
            
            if not stock_codes:
                click.echo("❌ 未获取到股票列表")
                return
            
            # 限制股票数量（用于测试）
            if max_stocks:
                stock_codes = stock_codes[:max_stocks]
            
            click.echo(f"📊 开始更新 {len(stock_codes)} 只{index_type}股票的数据...")
            
            # 使用改进的更新逻辑：遍历每只股票
            successful_stocks = 0
            failed_stocks = []
            
            for i, stock_code in enumerate(stock_codes, 1):
                click.echo(f"正在更新第 {i}/{len(stock_codes)} 只股票: {stock_code}")
                
                try:
                    # 更新单只股票的所有数据类型
                    stock_success = True
                    
                    # 1. 基本信息（如果包含在数据类型中）
                    if 'basic' in data_type_list or not data_types:
                        try:
                            basic_info = processor.data_fetcher.get_stock_basic_info(stock_code)
                            if basic_info:
                                processor.db_manager.upsert_data('stock_basic', basic_info, ['code'])
                        except Exception as e:
                            logger.warning(f"股票 {stock_code} 基本信息更新失败: {str(e)}")
                    
                    # 2. K线数据
                    if 'kline' in data_type_list or not data_types:
                        try:
                            kline_data = processor.data_fetcher.get_stock_kline_data(stock_code, start_date, end_date, adjustflag='2')
                            if kline_data:
                                processor.db_manager.upsert_data('stock_kline', kline_data, ['code', 'date'])
                        except Exception as e:
                            logger.warning(f"股票 {stock_code} K线数据更新失败: {str(e)}")
                            stock_success = False
                    
                    # 3. 财务数据
                    if 'financial' in data_type_list or not data_types:
                        try:
                            financial_types = ['profit', 'operation', 'growth', 'balance', 'cashflow', 'dupont']
                            year_quarters = processor._generate_year_quarters(start_date, end_date)
                            
                            for data_type in financial_types:
                                for year, quarter in year_quarters:
                                    financial_data = processor.data_fetcher.get_financial_data(stock_code, year, quarter, data_type)
                                    if financial_data:
                                        table_name = f'stock_{data_type}'
                                        processor.db_manager.upsert_data(table_name, financial_data, ['code', 'statDate'])
                        except Exception as e:
                            logger.warning(f"股票 {stock_code} 财务数据更新失败: {str(e)}")
                            stock_success = False
                    
                    # 4. 业绩数据
                    if 'performance' in data_type_list or not data_types:
                        try:
                            performance_types = ['express', 'forecast']
                            for data_type in performance_types:
                                performance_data = processor.data_fetcher.get_performance_data(stock_code, start_date, end_date, data_type)
                                if performance_data:
                                    table_name = f'stock_{data_type}'
                                    processor.db_manager.upsert_data(table_name, performance_data, ['code', 'statDate'])
                        except Exception as e:
                            logger.warning(f"股票 {stock_code} 业绩数据更新失败: {str(e)}")
                    
                    # 5. 行业数据
                    if 'industry' in data_type_list or not data_types:
                        try:
                            industry_data = processor.data_fetcher.get_industry_data(stock_code)
                            if industry_data:
                                processor.db_manager.upsert_data('stock_industry', industry_data, ['code'])
                        except Exception as e:
                            logger.warning(f"股票 {stock_code} 行业数据更新失败: {str(e)}")
                    
                    # 6. 复权因子数据
                    if 'adjust_factor' in data_type_list or not data_types:
                        try:
                            adjust_data = processor.data_fetcher.get_adjust_factor_data(stock_code, start_date, end_date)
                            if adjust_data:
                                processor.db_manager.upsert_data('stock_adjust_factor', adjust_data, ['code', 'date'])
                        except Exception as e:
                            logger.warning(f"股票 {stock_code} 复权因子数据更新失败: {str(e)}")
                    
                    # 7. 除权除息数据
                    if 'dividend' in data_type_list or not data_types:
                        try:
                            current_year = datetime.now().year
                            for year in range(current_year - 3, current_year + 1):
                                dividend_data = processor.data_fetcher.get_dividend_data(stock_code, str(year))
                                if dividend_data:
                                    processor.db_manager.upsert_data('stock_dividend', dividend_data, ['code', 'dividOperateDate'])
                        except Exception as e:
                            logger.warning(f"股票 {stock_code} 除权除息数据更新失败: {str(e)}")
                    
                    if stock_success:
                        successful_stocks += 1
                    else:
                        failed_stocks.append(stock_code)
                    
                    # 添加延迟避免API限制
                    if delay_seconds > 0:
                        time.sleep(delay_seconds)
                        
                except Exception as e:
                    logger.error(f"股票 {stock_code} 更新失败: {str(e)}")
                    failed_stocks.append(stock_code)
            
            # 8. 宏观经济数据（一次性更新）
            if 'macro' in data_type_list or not data_types:
                click.echo("🌍 更新宏观经济数据...")
                try:
                    stats = processor.process_macro_data(start_date, end_date)
                    total_macro = sum(stats.values())
                    click.echo(f"   宏观经济数据: {total_macro} 条记录")
                except Exception as e:
                    logger.warning(f"宏观经济数据更新失败: {str(e)}")
            
            # 9. 交易日历数据（一次性更新）
            if 'trade_dates' in data_type_list or not data_types:
                click.echo("📅 更新交易日历数据...")
                try:
                    records = processor.process_trade_dates(start_date, end_date)
                    click.echo(f"   交易日历数据: {records} 条记录")
                except Exception as e:
                    logger.warning(f"交易日历数据更新失败: {str(e)}")
        
        # 输出更新结果
        success_rate = (successful_stocks / len(stock_codes) * 100) if stock_codes else 0
        click.echo(f"✅ {index_type}数据更新完成:")
        click.echo(f"   总股票数: {len(stock_codes)}")
        click.echo(f"   成功更新: {successful_stocks}")
        click.echo(f"   失败数量: {len(failed_stocks)}")
        click.echo(f"   成功率: {success_rate:.1f}%")
        
        if failed_stocks:
            click.echo(f"   失败股票: {', '.join(failed_stocks[:10])}{'...' if len(failed_stocks) > 10 else ''}")
        
        logger.info(f"{index_type}所有数据更新完成: {successful_stocks}/{len(stock_codes)} 成功")
        
    except Exception as e:
        logger.error(f"更新所有数据失败: {str(e)}")
        click.echo(f"❌ 更新所有数据失败: {str(e)}")
        sys.exit(1)

@cli.command()
def status():
    """查看数据库状态"""
    try:
        with DatabaseManager() as db_manager:
            # 获取各表的记录数
            tables = [
                'stock_basic', 'stock_kline', 'stock_profit', 'stock_operation',
                'stock_growth', 'stock_balance', 'stock_cashflow', 'stock_dupont',
                'stock_performance', 'stock_forecast', 'stock_industry',
                'trade_dates', 'macro_deposit_rate', 'macro_loan_rate',
                'macro_reserve_ratio', 'macro_money_supply',
                'stock_adjust_factor', 'stock_dividend'
            ]
            
            click.echo("📊 数据库状态:")
            click.echo("=" * 50)
            
            for table in tables:
                try:
                    sql = f"SELECT COUNT(*) as count FROM {table}"
                    df = db_manager.query_data(sql)
                    count = df.iloc[0]['count'] if not df.empty else 0
                    click.echo(f"{table:<25}: {count:>10,} 条记录")
                except Exception as e:
                    click.echo(f"{table:<25}: {'错误':>10} ({str(e)})")
        
    except Exception as e:
        logger.error(f"查看数据库状态失败: {str(e)}")
        click.echo(f"❌ 查看数据库状态失败: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    cli()
