#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复版因子生成模块
使用修复版数据库管理器，避免事务和性能问题
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from database.manager_fixed import DatabaseManagerFixed
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class FactorGeneratorFixed:
    """修复版因子生成器"""
    
    def __init__(self):
        self.db_manager = DatabaseManagerFixed()
    
    def get_base_factor_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取基础因子数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            基础因子数据DataFrame
        """
        with self.db_manager.engine.connect() as conn:
            query = """
            SELECT 
                code, date, close, volume, amount, turn, pctChg,
                peTTM, pbMRQ, psTTM, pcfNcfTTM,
                roeAvg, npMargin, gpMargin, netProfit, epsTTM, MBRevenue,
                currentRatio, quickRatio, cashRatio, liabilityToAsset, assetToEquity,
                CAToAsset, NCAToAsset, ebitToInterest, CFOToOR, CFOToNP,
                NRTurnRatio, INVTurnRatio, CATurnRatio, AssetTurnRatio,
                YOYEquity, YOYAsset, YOYNI, YOYEPSBasic,
                dupontROE, dupontAssetStoEquity, dupontAssetTurn,
                industry, code_name
            FROM dwd_stock_base_factor
            WHERE date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY code, date
            """
            
            df = pd.read_sql(query, conn, params={
                'start_date': start_date,
                'end_date': end_date
            })
            
            logger.info(f"获取基础因子数据: {len(df)} 条记录")
            return df
    
    def generate_technical_factors_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        优化的技术因子生成
        
        Args:
            df: 基础因子数据
            
        Returns:
            包含技术因子的DataFrame
        """
        logger.info("开始生成技术因子...")
        
        # 按股票代码分组计算技术因子
        technical_factors = []
        
        for code, group in df.groupby('code'):
            group = group.sort_values('date').copy()
            
            # 只计算核心技术因子，避免过度计算
            group['momentum_1m'] = group['close'].pct_change(20)  # 1个月动量
            group['momentum_3m'] = group['close'].pct_change(60)  # 3个月动量
            group['reversal_5d'] = -group['close'].pct_change(5)  # 5日反转
            group['volatility_20d'] = group['pctChg'].rolling(20).std()  # 20日波动率
            group['volume_ratio_20d'] = group['volume'] / group['volume'].rolling(20).mean()  # 成交量比率
            
            # 价格位置因子
            group['price_position_20d'] = (group['close'] - group['close'].rolling(20).min()) / (group['close'].rolling(20).max() - group['close'].rolling(20).min())
            
            # RSI因子
            delta = group['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            group['rsi_14d'] = 100 - (100 / (1 + rs))
            
            technical_factors.append(group)
        
        result_df = pd.concat(technical_factors, ignore_index=True)
        logger.info(f"技术因子生成完成: {len(result_df)} 条记录")
        return result_df
    
    def generate_fundamental_factors_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        优化的基本面因子生成
        
        Args:
            df: 基础因子数据
            
        Returns:
            包含基本面因子的DataFrame
        """
        logger.info("开始生成基本面因子...")
        
        # 估值因子
        df['pe_ratio'] = df['peTTM']
        df['pb_ratio'] = df['pbMRQ']
        df['ps_ratio'] = df['psTTM']
        df['pcf_ratio'] = df['pcfNcfTTM']
        
        # 盈利质量因子
        df['roe'] = df['roeAvg']
        df['net_profit_margin'] = df['npMargin']
        df['gross_profit_margin'] = df['gpMargin']
        df['eps'] = df['epsTTM']
        
        # 财务健康因子
        df['current_ratio'] = df['currentRatio']
        df['quick_ratio'] = df['quickRatio']
        df['cash_ratio'] = df['cashRatio']
        df['debt_to_asset'] = df['liabilityToAsset']
        df['asset_to_equity'] = df['assetToEquity']
        
        # 现金流因子
        df['cash_to_asset'] = df['CAToAsset']
        df['cfo_to_revenue'] = df['CFOToOR']
        df['cfo_to_net_profit'] = df['CFOToNP']
        
        # 运营效率因子
        df['receivable_turnover'] = df['NRTurnRatio']
        df['inventory_turnover'] = df['INVTurnRatio']
        df['current_asset_turnover'] = df['CATurnRatio']
        df['total_asset_turnover'] = df['AssetTurnRatio']
        
        # 成长因子
        df['equity_growth'] = df['YOYEquity']
        df['asset_growth'] = df['YOYAsset']
        df['net_profit_growth'] = df['YOYNI']
        df['eps_growth'] = df['YOYEPSBasic']
        
        # 杜邦分析因子
        df['dupont_roe'] = df['dupontROE']
        df['dupont_equity_multiplier'] = df['dupontAssetStoEquity']
        df['dupont_asset_turnover'] = df['dupontAssetTurn']
        
        # 复合因子
        df['quality_score'] = (
            df['roe'].fillna(0) * 0.3 +
            df['net_profit_margin'].fillna(0) * 0.2 +
            df['current_ratio'].fillna(0) * 0.2 +
            df['cfo_to_revenue'].fillna(0) * 0.3
        )
        
        df['value_score'] = (
            -df['pe_ratio'].fillna(999) * 0.4 +
            -df['pb_ratio'].fillna(999) * 0.3 +
            -df['ps_ratio'].fillna(999) * 0.3
        )
        
        df['growth_score'] = (
            df['equity_growth'].fillna(0) * 0.3 +
            df['asset_growth'].fillna(0) * 0.2 +
            df['net_profit_growth'].fillna(0) * 0.3 +
            df['eps_growth'].fillna(0) * 0.2
        )
        
        logger.info("基本面因子生成完成")
        return df
    
    def generate_cross_sectional_factors_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        优化的横截面因子生成
        
        Args:
            df: 包含技术因子和基本面因子的数据
            
        Returns:
            包含横截面因子的DataFrame
        """
        logger.info("开始生成横截面因子...")
        
        # 按日期分组计算横截面排名
        cross_sectional_factors = []
        
        for date, group in df.groupby('date'):
            group = group.copy()
            
            # 只计算核心因子的横截面排名
            core_factor_columns = [
                'momentum_1m', 'momentum_3m', 'reversal_5d', 'volatility_20d',
                'pe_ratio', 'pb_ratio', 'ps_ratio', 'roe', 'net_profit_margin',
                'current_ratio', 'quick_ratio', 'cash_ratio',
                'quality_score', 'value_score', 'growth_score'
            ]
            
            for col in core_factor_columns:
                if col in group.columns:
                    # 计算百分位数排名
                    group[f'{col}_rank'] = group[col].rank(pct=True)
                    # 计算标准化分数
                    group[f'{col}_zscore'] = (group[col] - group[col].mean()) / group[col].std()
            
            cross_sectional_factors.append(group)
        
        result_df = pd.concat(cross_sectional_factors, ignore_index=True)
        logger.info(f"横截面因子生成完成: {len(result_df)} 条记录")
        return result_df
    
    def save_factors_to_database_safe(self, df: pd.DataFrame, table_name: str = 'stock_factors_fixed'):
        """
        安全保存因子数据到数据库
        
        Args:
            df: 因子数据
            table_name: 表名
        """
        logger.info(f"保存因子数据到表: {table_name}")
        
        try:
            # 创建因子表
            self.create_factor_table_safe(df, table_name)
            
            # 分批保存数据
            batch_size = 1000
            total_records = len(df)
            
            for i in range(0, total_records, batch_size):
                batch_df = df.iloc[i:i+batch_size]
                batch_data = batch_df.to_dict('records')
                
                self.db_manager.upsert_data_safe(table_name, batch_data, ['code', 'date'])
                logger.info(f"已保存 {min(i+batch_size, total_records)}/{total_records} 条记录")
            
            logger.info(f"因子数据保存成功: {total_records} 条记录")
            
        except Exception as e:
            logger.error(f"保存因子数据失败: {str(e)}")
            raise
    
    def create_factor_table_safe(self, df: pd.DataFrame, table_name: str):
        """安全创建因子表"""
        try:
            # 获取DataFrame的列信息
            columns_info = []
            
            # 首先添加主键列
            columns_info.append("code VARCHAR(20) NOT NULL COMMENT '股票代码'")
            columns_info.append("date DATE NOT NULL COMMENT '日期'")
            
            # 然后添加其他列
            for col in df.columns:
                if col in ['code', 'date']:
                    continue
                elif df[col].dtype in ['int64', 'float64']:
                    columns_info.append(f"{col} DECIMAL(20,6) COMMENT '{col}'")
                elif df[col].dtype == 'object':
                    columns_info.append(f"{col} VARCHAR(200) COMMENT '{col}'")
                else:
                    columns_info.append(f"{col} TEXT COMMENT '{col}'")
            
            # 添加主键约束
            columns_info.append("PRIMARY KEY (code, date)")
            
            self.db_manager.create_table_safe(table_name, columns_info)
            
        except Exception as e:
            logger.error(f"创建因子表失败: {str(e)}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        self.db_manager.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='修复版因子生成器')
    parser.add_argument('--start-date', default='2020-06-01', help='开始日期')
    parser.add_argument('--end-date', help='结束日期')
    parser.add_argument('--table-name', default='dws_stock_factors', help='因子表名')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    generator = FactorGeneratorFixed()
    
    try:
        # 获取基础因子数据
        df = generator.get_base_factor_data(args.start_date, args.end_date)
        
        # 生成技术因子
        df = generator.generate_technical_factors_optimized(df)
        
        # 生成基本面因子
        df = generator.generate_fundamental_factors_optimized(df)
        
        # 生成横截面因子
        df = generator.generate_cross_sectional_factors_optimized(df)
        
        # 保存因子数据
        generator.save_factors_to_database_safe(df, args.table_name)
        
        print(f"\n📊 修复版因子生成完成:")
        print(f"  总记录数: {len(df):,}")
        print(f"  因子数量: {len(df.columns) - 2}")  # 减去code和date列
        print(f"  保存到表: {args.table_name}")
        
    except Exception as e:
        print(f"❌ 因子生成失败: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        generator.close()


if __name__ == '__main__':
    main()
