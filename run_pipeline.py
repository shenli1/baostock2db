#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多因子策略完整流水线

整合数据获取、数据加工、因子衍生、多因子策略四个模块
"""

import argparse
import logging
from datetime import datetime

# 导入各个模块
from data_acquisition.data_fetcher import BaoStockDataFetcher
from data_acquisition.batch_processor import BatchProcessor
from data_processing.dwd_processor import DWDProcessor
from data_processing.base_factor_processor import BaseFactorProcessor
from factor_derivation.factor_generation_fixed import FactorGeneratorFixed
from multi_factor_strategy.multi_factor_strategy_fixed import MultiFactorStrategyFixed
from multi_factor_strategy.optimized_multi_factor_strategy import OptimizedMultiFactorStrategy
from database.manager_fixed import DatabaseManagerFixed


class MultiFactorPipeline:
    """多因子策略完整流水线"""
    
    def __init__(self):
        """初始化流水线"""
        self.db_manager = DatabaseManagerFixed()
        self.data_fetcher = BaoStockDataFetcher()
        self.batch_processor = BatchProcessor()
        self.dwd_processor = DWDProcessor()
        self.base_factor_processor = BaseFactorProcessor()
        self.factor_generator = FactorGeneratorFixed()
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def run_data_acquisition(self, start_date: str, end_date: str):
        """运行数据获取模块"""
        self.logger.info("=== 开始数据获取 ===")
        
        try:
            # 获取HS300成分股
            self.logger.info("获取HS300成分股数据")
            self.data_fetcher.get_hs300_stocks()
            
            # 批量获取股票数据
            self.logger.info("批量获取股票数据")
            self.batch_processor.update_all_stocks(start_date, end_date)
            
            self.logger.info("数据获取完成")
            
        except Exception as e:
            self.logger.error(f"数据获取失败: {str(e)}")
            raise
    
    def run_data_processing(self, start_date: str = '2020-06-01'):
        """运行数据加工模块"""
        self.logger.info("=== 开始数据加工 ===")
        
        try:
            # 处理DWD层数据
            self.logger.info("处理DWD层数据")
            self.dwd_processor.process_all_tables()
            
            # 构建基础因子表
            self.logger.info("构建基础因子表")
            self.base_factor_processor.create_base_factor_table(start_date)
            
            self.logger.info("数据加工完成")
            
        except Exception as e:
            self.logger.error(f"数据加工失败: {str(e)}")
            raise
    
    def run_factor_derivation(self, start_date: str, end_date: str):
        """运行因子衍生模块"""
        self.logger.info("=== 开始因子衍生 ===")
        
        try:
            # 生成各种因子
            self.logger.info("生成技术因子")
            self.factor_generator.generate_technical_factors(start_date, end_date)
            
            self.logger.info("生成基本面因子")
            self.factor_generator.generate_fundamental_factors(start_date, end_date)
            
            self.logger.info("生成横截面因子")
            self.factor_generator.generate_cross_sectional_factors(start_date, end_date)
            
            self.logger.info("因子衍生完成")
            
        except Exception as e:
            self.logger.error(f"因子衍生失败: {str(e)}")
            raise
    
    def run_multi_factor_strategy(self, start_date: str, end_date: str, 
                                 strategy_type: str = 'optimized', **kwargs):
        """运行多因子策略模块"""
        self.logger.info("=== 开始多因子策略 ===")
        
        try:
            if strategy_type == 'fixed':
                strategy = MultiFactorStrategyFixed(
                    start_date=start_date,
                    end_date=end_date,
                    **kwargs
                )
            else:  # optimized
                strategy = OptimizedMultiFactorStrategy(
                    start_date=start_date,
                    end_date=end_date,
                    **kwargs
                )
            
            strategy.run_optimized_strategy() if strategy_type == 'optimized' else strategy.run_strategy()
            
            self.logger.info("多因子策略完成")
            
        except Exception as e:
            self.logger.error(f"多因子策略失败: {str(e)}")
            raise
    
    def run_full_pipeline(self, start_date: str, end_date: str, 
                         strategy_type: str = 'optimized', **kwargs):
        """运行完整流水线"""
        self.logger.info("开始运行完整多因子策略流水线")
        
        try:
            # 1. 数据获取
            self.run_data_acquisition(start_date, end_date)
            
            # 2. 数据加工
            self.run_data_processing()
            
            # 3. 因子衍生
            self.run_factor_derivation(start_date, end_date)
            
            # 4. 多因子策略
            self.run_multi_factor_strategy(start_date, end_date, strategy_type, **kwargs)
            
            self.logger.info("完整流水线运行成功！")
            
        except Exception as e:
            self.logger.error(f"流水线运行失败: {str(e)}")
            raise
        finally:
            self.close()
    
    def close(self):
        """关闭所有连接"""
        self.db_manager.close()
        self.data_fetcher.close()
        self.factor_generator.close()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='多因子策略完整流水线')
    
    # 基本参数
    parser.add_argument('--start-date', default='2020-06-01', help='开始日期')
    parser.add_argument('--end-date', default='2020-12-31', help='结束日期')
    parser.add_argument('--strategy-type', choices=['fixed', 'optimized'], 
                       default='optimized', help='策略类型')
    
    # 策略参数
    parser.add_argument('--rebalance-freq', type=int, default=10, help='调仓频率')
    parser.add_argument('--top-n', type=int, default=50, help='选股数量')
    parser.add_argument('--min-score', type=float, default=0.0, help='最小因子得分')
    
    # 流水线步骤
    parser.add_argument('--data-acquisition', action='store_true', help='只运行数据获取')
    parser.add_argument('--data-processing', action='store_true', help='只运行数据加工')
    parser.add_argument('--factor-derivation', action='store_true', help='只运行因子衍生')
    parser.add_argument('--multi-factor-strategy', action='store_true', help='只运行多因子策略')
    parser.add_argument('--full-pipeline', action='store_true', help='运行完整流水线')
    
    args = parser.parse_args()
    
    # 创建流水线
    pipeline = MultiFactorPipeline()
    
    try:
        if args.full_pipeline or not any([args.data_acquisition, args.data_processing, 
                                        args.factor_derivation, args.multi_factor_strategy]):
            # 运行完整流水线
            pipeline.run_full_pipeline(
                start_date=args.start_date,
                end_date=args.end_date,
                strategy_type=args.strategy_type,
                rebalance_freq=args.rebalance_freq,
                top_n=args.top_n,
                min_score=args.min_score
            )
        else:
            # 运行指定步骤
            if args.data_acquisition:
                pipeline.run_data_acquisition(args.start_date, args.end_date)
            
            if args.data_processing:
                pipeline.run_data_processing()
            
            if args.factor_derivation:
                pipeline.run_factor_derivation(args.start_date, args.end_date)
            
            if args.multi_factor_strategy:
                pipeline.run_multi_factor_strategy(
                    args.start_date, args.end_date, args.strategy_type,
                    rebalance_freq=args.rebalance_freq,
                    top_n=args.top_n,
                    min_score=args.min_score
                )
        
        print("\n🎉 流水线运行成功！")
        
    except Exception as e:
        print(f"\n❌ 流水线运行失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
