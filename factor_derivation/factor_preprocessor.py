#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子预处理模块
参考alphasickle项目的因子预处理方法，对dws_stock_factors表进行预处理
包括：缺失值处理、去极值处理、标准化处理、中性化处理
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from database.manager_fixed import DatabaseManagerFixed
from sqlalchemy import text
import logging
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)


class FactorPreprocessor:
    """因子预处理器"""
    
    def __init__(self):
        self.db_manager = DatabaseManagerFixed()
        
        # 动态识别的因子列（运行时确定）
        self.factor_columns = []
        
        # 需要中性化的因子（动态识别）
        self.neutralize_factor_list = []
        
        # 基础信息列（非因子列）
        self.base_columns = ['code', 'date', 'industry', 'code_name', 'close', 'volume', 'amount', 'pctChg']
        
        # 排除的列（不参与因子预处理）
        self.exclude_columns = [
            'code', 'date', 'industry', 'code_name', 'close', 'volume', 'amount', 'pctChg',
            'created_at', 'updated_at', 'pubDate', 'statDate', 'frequency', 'open', 'high', 'low', 
            'preclose', 'adjustflag', 'turn', 'tradestatus', 'isST', 'totalShare', 'liqaShare',
            # 原始数据列（不参与因子预处理）
            'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM', 'roeAvg', 'npMargin', 'gpMargin', 'netProfit', 
            'epsTTM', 'MBRevenue', 'currentRatio', 'quickRatio', 'cashRatio', 'liabilityToAsset', 
            'assetToEquity', 'CAToAsset', 'NCAToAsset', 'ebitToInterest', 'CFOToOR', 'CFOToNP', 
            'NRTurnRatio', 'INVTurnRatio', 'CATurnRatio', 'AssetTurnRatio', 'YOYEquity', 'YOYAsset', 
            'YOYNI', 'YOYEPSBasic', 'dupontROE', 'dupontAssetStoEquity', 'dupontAssetTurn'
        ]
    
    def detect_factor_columns(self, table_name: str = 'dws_stock_factors') -> List[str]:
        """
        动态检测因子表中的因子列
        
        Args:
            table_name: 因子表名
            
        Returns:
            因子列名列表
        """
        with self.db_manager.engine.connect() as conn:
            # 获取表结构
            query = f"DESCRIBE {table_name}"
            result = conn.execute(text(query))
            columns_info = result.fetchall()
            
            # 提取列名
            all_columns = [row[0] for row in columns_info]
            
            # 识别因子列（排除基础信息列）
            factor_columns = []
            for col in all_columns:
                if col not in self.exclude_columns:
                    factor_columns.append(col)
            
            logger.info(f"检测到 {len(factor_columns)} 个因子列: {factor_columns[:10]}{'...' if len(factor_columns) > 10 else ''}")
            return factor_columns
    
    def get_factor_data(self, start_date: str, end_date: str, table_name: str = 'dws_stock_factors') -> pd.DataFrame:
        """
        获取因子数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            table_name: 因子表名
            
        Returns:
            因子数据DataFrame
        """
        # 每次重新检测因子列，确保从正确的表获取列信息
        factor_columns = self.detect_factor_columns(table_name)
        
        with self.db_manager.engine.connect() as conn:
            # 构建查询SQL，包含所有需要的列
            factor_cols = ', '.join(factor_columns)
            query = f"""
            SELECT 
                code, date, industry, code_name,
                close, volume, amount, pctChg,
                {factor_cols}
            FROM {table_name}
            WHERE date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY code, date
            """
            
            df = pd.read_sql(query, conn, params={
                'start_date': start_date,
                'end_date': end_date
            })
            
            logger.info(f"获取因子数据: {len(df)} 条记录，{len(factor_columns)} 个因子")
            return df
    
    def handle_missing_values(self, df: pd.DataFrame, method: str = 'forward_fill') -> pd.DataFrame:
        """
        处理缺失值
        
        Args:
            df: 因子数据
            method: 处理方法 ('forward_fill', 'backward_fill', 'mean', 'median', 'drop')
            
        Returns:
            处理后的数据
        """
        logger.info(f"开始处理缺失值，方法: {method}")
        
        df_processed = df.copy()
        
        # 动态识别因子列（排除基础信息列）
        factor_columns = [col for col in df.columns if col not in self.exclude_columns]
        
        if method == 'forward_fill':
            # 前向填充，然后后向填充处理剩余NULL值
            df_processed[factor_columns] = df_processed.groupby('code')[factor_columns].fillna(method='ffill')
            df_processed[factor_columns] = df_processed.groupby('code')[factor_columns].fillna(method='bfill')
        elif method == 'backward_fill':
            # 后向填充，然后前向填充处理剩余NULL值
            df_processed[factor_columns] = df_processed.groupby('code')[factor_columns].fillna(method='bfill')
            df_processed[factor_columns] = df_processed.groupby('code')[factor_columns].fillna(method='ffill')
        elif method == 'mean':
            # 均值填充
            df_processed[factor_columns] = df_processed[factor_columns].fillna(df_processed[factor_columns].mean())
        elif method == 'median':
            # 中位数填充
            df_processed[factor_columns] = df_processed[factor_columns].fillna(df_processed[factor_columns].median())
        elif method == 'drop':
            # 删除缺失值
            df_processed = df_processed.dropna(subset=factor_columns)
        
        # 如果还有NULL值，用0填充
        remaining_nulls = df_processed[factor_columns].isnull().sum().sum()
        if remaining_nulls > 0:
            logger.warning(f"仍有 {remaining_nulls} 个NULL值，用0填充")
            df_processed[factor_columns] = df_processed[factor_columns].fillna(0)
        
        # 统计缺失值处理情况
        missing_before = df[factor_columns].isnull().sum().sum()
        missing_after = df_processed[factor_columns].isnull().sum().sum()
        logger.info(f"缺失值处理完成: {missing_before} -> {missing_after}")
        
        return df_processed
    
    def winsorize_factors(self, df: pd.DataFrame, method: str = 'quantile', 
                         limits: Tuple[float, float] = (0.01, 0.99)) -> pd.DataFrame:
        """
        去极值处理（Winsorization）
        
        Args:
            df: 因子数据
            method: 去极值方法 ('quantile', 'std', 'mad')
            limits: 限制范围，对于quantile方法为(下分位数, 上分位数)，对于std/mad为(下限倍数, 上限倍数)
            
        Returns:
            处理后的数据
        """
        logger.info(f"开始去极值处理，方法: {method}, 限制: {limits}")
        
        df_processed = df.copy()
        
        # 动态识别因子列（排除基础信息列）
        factor_columns = [col for col in df.columns if col not in self.exclude_columns]
        
        for col in factor_columns:
            if col not in df_processed.columns:
                continue
                
            series = df_processed[col].dropna()
            if len(series) == 0:
                continue
            
            if method == 'quantile':
                # 分位数方法
                lower_limit = series.quantile(limits[0])
                upper_limit = series.quantile(limits[1])
            elif method == 'std':
                # 标准差方法
                mean = series.mean()
                std = series.std()
                lower_limit = mean - limits[0] * std
                upper_limit = mean + limits[1] * std
            elif method == 'mad':
                # 中位数绝对偏差方法
                median = series.median()
                mad = np.median(np.abs(series - median))
                lower_limit = median - limits[0] * mad
                upper_limit = median + limits[1] * mad
            else:
                raise ValueError(f"不支持的去极值方法: {method}")
            
            # 应用限制
            df_processed[col] = np.clip(df_processed[col], lower_limit, upper_limit)
        
        logger.info("去极值处理完成")
        return df_processed
    
    def standardize_factors(self, df: pd.DataFrame, method: str = 'zscore') -> pd.DataFrame:
        """
        标准化处理
        
        Args:
            df: 因子数据
            method: 标准化方法 ('zscore', 'minmax', 'robust')
            
        Returns:
            处理后的数据
        """
        logger.info(f"开始标准化处理，方法: {method}")
        
        df_processed = df.copy()
        
        # 动态识别因子列（排除基础信息列）
        factor_columns = [col for col in df.columns if col not in self.exclude_columns]
        
        for col in factor_columns:
            if col not in df_processed.columns:
                continue
                
            series = df_processed[col].dropna()
            if len(series) == 0:
                continue
            
            if method == 'zscore':
                # Z-score标准化
                mean = series.mean()
                std = series.std()
                if std != 0:
                    df_processed[col] = (df_processed[col] - mean) / std
                else:
                    # 如果标准差为0，所有值设为0
                    logger.warning(f"因子 {col} 标准差为0，设为0")
                    df_processed[col] = 0
            elif method == 'minmax':
                # Min-Max标准化
                min_val = series.min()
                max_val = series.max()
                if max_val != min_val:
                    df_processed[col] = (df_processed[col] - min_val) / (max_val - min_val)
                else:
                    # 如果最大值等于最小值，所有值设为0
                    logger.warning(f"因子 {col} 最大值等于最小值，设为0")
                    df_processed[col] = 0
            elif method == 'robust':
                # 鲁棒标准化（使用中位数和MAD）
                median = series.median()
                mad = np.median(np.abs(series - median))
                if mad != 0:
                    df_processed[col] = (df_processed[col] - median) / mad
                else:
                    # 如果MAD为0，所有值设为0
                    logger.warning(f"因子 {col} MAD为0，设为0")
                    df_processed[col] = 0
            else:
                raise ValueError(f"不支持的标准化方法: {method}")
        
        logger.info("标准化处理完成")
        return df_processed
    
    def select_neutralize_factors(self, df: pd.DataFrame) -> List[str]:
        """
        智能选择需要中性化的因子
        
        Args:
            df: 因子数据
            
        Returns:
            需要中性化的因子列表
        """
        # 动态识别因子列（排除基础信息列）
        factor_columns = [col for col in df.columns if col not in self.exclude_columns]
        
        # 基于因子名称模式选择需要中性化的因子
        neutralize_patterns = [
            'momentum', 'reversal', 'volatility', 'volume_ratio', 'price_position', 'rsi',
            'pe_ratio', 'pb_ratio', 'ps_ratio', 'pcf_ratio',
            'roe', 'net_profit_margin', 'gross_profit_margin', 'eps',
            'current_ratio', 'quick_ratio', 'cash_ratio', 'debt_to_asset', 'asset_to_equity',
            'cash_to_asset', 'cfo_to_revenue', 'cfo_to_net_profit',
            'receivable_turnover', 'inventory_turnover', 'current_asset_turnover', 'total_asset_turnover',
            'equity_growth', 'asset_growth', 'net_profit_growth', 'eps_growth',
            'dupont_roe', 'dupont_equity_multiplier', 'dupont_asset_turnover',
            'quality_score', 'value_score', 'growth_score'
        ]
        
        neutralize_factors = []
        for factor in factor_columns:
            # 检查因子名是否匹配任何模式
            if any(pattern in factor.lower() for pattern in neutralize_patterns):
                neutralize_factors.append(factor)
        
        logger.info(f"选择 {len(neutralize_factors)} 个因子进行中性化: {neutralize_factors[:10]}{'...' if len(neutralize_factors) > 10 else ''}")
        return neutralize_factors
    
    def neutralize_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        中性化处理（去除行业和市值影响）
        
        Args:
            df: 因子数据
            
        Returns:
            处理后的数据
        """
        logger.info("开始中性化处理")
        
        # 动态选择需要中性化的因子
        if not self.neutralize_factor_list:
            self.neutralize_factor_list = self.select_neutralize_factors(df)
        
        df_processed = df.copy()
        
        # 计算市值（使用收盘价*成交量作为代理）
        df_processed['market_cap_proxy'] = df_processed['close'] * df_processed['volume']
        df_processed['log_market_cap'] = np.log(df_processed['market_cap_proxy'] + 1)
        
        # 按日期分组进行中性化
        neutralized_data = []
        
        for date, group in df_processed.groupby('date'):
            group = group.copy()
            
            # 准备中性化变量
            if 'industry' in group.columns:
                # 行业虚拟变量
                industry_dummies = pd.get_dummies(group['industry'], prefix='industry')
            else:
                industry_dummies = pd.DataFrame()
            
            # 市值变量
            market_cap = group['log_market_cap'].fillna(group['log_market_cap'].mean())
            
            # 合并中性化变量
            if not industry_dummies.empty:
                X = pd.concat([industry_dummies, market_cap], axis=1)
            else:
                X = market_cap.values.reshape(-1, 1)
            
            # 对每个因子进行中性化
            for factor in self.neutralize_factor_list:
                if factor not in group.columns:
                    continue
                    
                y = group[factor].fillna(group[factor].mean())
                
                if len(y.dropna()) < 10:  # 数据点太少，跳过
                    continue
                
                try:
                    # 线性回归中性化
                    model = LinearRegression()
                    model.fit(X, y)
                    y_pred = model.predict(X)
                    group[f'{factor}_neutralized'] = y - y_pred
                except Exception as e:
                    logger.warning(f"中性化因子 {factor} 失败: {str(e)}")
                    group[f'{factor}_neutralized'] = y
            
            neutralized_data.append(group)
        
        result_df = pd.concat(neutralized_data, ignore_index=True)
        
        # 更新因子列名
        neutralized_columns = [f'{col}_neutralized' for col in self.neutralize_factor_list]
        self.factor_columns.extend(neutralized_columns)
        
        logger.info("中性化处理完成")
        return result_df
    
    def create_factor_ic_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算因子IC（信息系数）分析
        
        Args:
            df: 因子数据
            
        Returns:
            包含IC分析的数据
        """
        logger.info("开始计算因子IC分析")
        
        df_processed = df.copy()
        
        # 动态识别因子列（排除基础信息列）
        factor_columns = [col for col in df.columns if col not in self.exclude_columns]
        
        # 计算未来收益率（使用下一天的收益率）
        df_processed = df_processed.sort_values(['code', 'date'])
        df_processed['future_return'] = df_processed.groupby('code')['pctChg'].shift(-1)
        
        # 按日期分组计算IC
        ic_results = []
        
        for date, group in df_processed.groupby('date'):
            if group['future_return'].isnull().all():
                continue
                
            date_ic = {'date': date}
            
            for factor in factor_columns:
                if factor not in group.columns:
                    continue
                    
                # 计算IC（Spearman相关系数）
                factor_values = group[factor].dropna()
                future_returns = group.loc[factor_values.index, 'future_return'].dropna()
                
                if len(factor_values) > 5 and len(future_returns) > 5:
                    # 确保索引对齐
                    common_idx = factor_values.index.intersection(future_returns.index)
                    if len(common_idx) > 5:
                        ic = factor_values.loc[common_idx].corr(future_returns.loc[common_idx], method='spearman')
                        date_ic[f'{factor}_ic'] = ic
            
            ic_results.append(date_ic)
        
        ic_df = pd.DataFrame(ic_results)
        logger.info(f"IC分析完成，计算了 {len(ic_df)} 个交易日")
        
        return ic_df
    
    def save_preprocessed_factors(self, df: pd.DataFrame, table_name: str = 'dws_stock_factors_preprocessed'):
        """
        动态保存预处理后的因子数据
        
        Args:
            df: 预处理后的因子数据
            table_name: 目标表名
        """
        logger.info(f"保存预处理后的因子数据到表: {table_name}")
        logger.info(f"数据包含 {len(df.columns)} 列: {list(df.columns)}")
        
        try:
            # 动态创建预处理因子表
            self.create_preprocessed_table(df, table_name)
            
            # 分批保存数据
            batch_size = 1000
            total_records = len(df)
            
            for i in range(0, total_records, batch_size):
                batch_df = df.iloc[i:i+batch_size]
                batch_data = batch_df.to_dict('records')
                
                self.db_manager.upsert_data_safe(table_name, batch_data, ['code', 'date'])
                logger.info(f"已保存 {min(i+batch_size, total_records)}/{total_records} 条记录")
            
            logger.info(f"预处理因子数据保存成功: {total_records} 条记录")
            
        except Exception as e:
            logger.error(f"保存预处理因子数据失败: {str(e)}")
            raise
    
    def drop_table_if_exists(self, table_name: str):
        """删除表（如果存在）"""
        try:
            with self.db_manager.engine.connect() as conn:
                # 检查表是否存在
                check_query = f"SHOW TABLES LIKE '{table_name}'"
                result = conn.execute(text(check_query))
                if result.fetchone():
                    # 表存在，删除它
                    drop_query = f"DROP TABLE {table_name}"
                    conn.execute(text(drop_query))
                    conn.commit()
                    logger.info(f"已删除现有表: {table_name}")
                else:
                    logger.info(f"表 {table_name} 不存在，无需删除")
        except Exception as e:
            logger.warning(f"删除表 {table_name} 时出错: {str(e)}")
            # 不抛出异常，继续执行

    def create_preprocessed_table(self, df: pd.DataFrame, table_name: str):
        """动态创建预处理因子表"""
        try:
            # 先删除已存在的表
            self.drop_table_if_exists(table_name)
            
            # 获取DataFrame的列信息
            columns_info = []
            
            # 首先添加主键列
            columns_info.append("code VARCHAR(20) NOT NULL COMMENT '股票代码'")
            columns_info.append("date DATE NOT NULL COMMENT '日期'")
            
            # 然后添加其他列，根据实际数据类型动态创建
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
            
            logger.info(f"创建预处理表 {table_name}，包含 {len(columns_info)} 列")
            self.db_manager.create_table_safe(table_name, columns_info)
            
        except Exception as e:
            logger.error(f"创建预处理因子表失败: {str(e)}")
            raise
    
    def run_full_preprocessing(self, start_date: str, end_date: str, 
                             table_name: str = 'dws_stock_factors',
                             output_table_name: str = None,
                             missing_method: str = 'forward_fill',
                             winsorize_method: str = 'quantile',
                             winsorize_limits: Tuple[float, float] = (0.01, 0.99),
                             standardize_method: str = 'zscore',
                             neutralize: bool = True,
                             ic_analysis: bool = True) -> pd.DataFrame:
        """
        运行完整的因子预处理流程
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            table_name: 输入因子表名
            output_table_name: 输出表名（如果为None，则自动生成）
            missing_method: 缺失值处理方法
            winsorize_method: 去极值方法
            winsorize_limits: 去极值限制
            standardize_method: 标准化方法
            neutralize: 是否进行中性化
            ic_analysis: 是否进行IC分析
            
        Returns:
            预处理后的因子数据
        """
        logger.info("=== 开始因子预处理流程 ===")
        
        # 动态生成输出表名
        if output_table_name is None:
            output_table_name = f"{table_name}_preprocessed"
        
        logger.info(f"输入表: {table_name}")
        logger.info(f"输出表: {output_table_name}")
        
        try:
            # 1. 获取因子数据
            logger.info("步骤1: 获取因子数据")
            df = self.get_factor_data(start_date, end_date, table_name)
            
            # 2. 处理缺失值
            logger.info("步骤2: 处理缺失值")
            df = self.handle_missing_values(df, method=missing_method)
            
            # 3. 去极值处理
            logger.info("步骤3: 去极值处理")
            df = self.winsorize_factors(df, method=winsorize_method, limits=winsorize_limits)
            
            # 4. 标准化处理
            logger.info("步骤4: 标准化处理")
            df = self.standardize_factors(df, method=standardize_method)
            
            # 5. 中性化处理
            if neutralize:
                logger.info("步骤5: 中性化处理")
                df = self.neutralize_factors(df)
            
            # 6. IC分析
            if ic_analysis:
                logger.info("步骤6: IC分析")
                ic_df = self.create_factor_ic_analysis(df)
                # 保存IC分析结果
                self.save_ic_analysis(ic_df)
            
            # 7. 保存预处理结果
            logger.info("步骤7: 保存预处理结果")
            self.save_preprocessed_factors(df, output_table_name)
            
            logger.info("=== 因子预处理流程完成 ===")
            logger.info(f"预处理结果已保存到表: {output_table_name}")
            return df
            
        except Exception as e:
            logger.error(f"因子预处理失败: {str(e)}")
            raise
    
    def save_ic_analysis(self, ic_df: pd.DataFrame, table_name: str = 'factor_ic_analysis'):
        """保存IC分析结果"""
        try:
            # 先删除已存在的IC分析表
            self.drop_table_if_exists(table_name)
            
            # 创建IC分析表
            columns_info = [
                "date DATE NOT NULL COMMENT '日期'",
                "PRIMARY KEY (date)"
            ]
            
            # 添加IC列
            for col in ic_df.columns:
                if col != 'date':
                    columns_info.append(f"{col} DECIMAL(10,6) COMMENT '{col}'")
            
            self.db_manager.create_table_safe(table_name, columns_info)
            
            # 保存数据
            batch_data = ic_df.to_dict('records')
            self.db_manager.upsert_data_safe(table_name, batch_data, ['date'])
            
            logger.info(f"IC分析结果保存成功: {len(ic_df)} 条记录")
            
        except Exception as e:
            logger.error(f"保存IC分析结果失败: {str(e)}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        self.db_manager.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='因子预处理器')
    parser.add_argument('--start-date', default='2020-06-01', help='开始日期')
    parser.add_argument('--end-date', help='结束日期')
    parser.add_argument('--table-name', default='dws_stock_factors', help='输入因子表名')
    parser.add_argument('--output-table-name', help='输出表名（默认：{table_name}_preprocessed）')
    parser.add_argument('--missing-method', choices=['forward_fill', 'backward_fill', 'mean', 'median', 'drop'], 
                       default='forward_fill', help='缺失值处理方法')
    parser.add_argument('--winsorize-method', choices=['quantile', 'std', 'mad'], 
                       default='quantile', help='去极值方法')
    parser.add_argument('--winsorize-limits', nargs=2, type=float, default=[0.01, 0.99], 
                       help='去极值限制')
    parser.add_argument('--standardize-method', choices=['zscore', 'minmax', 'robust'], 
                       default='zscore', help='标准化方法')
    parser.add_argument('--no-neutralize', action='store_true', help='跳过中性化处理')
    parser.add_argument('--no-ic-analysis', action='store_true', help='跳过IC分析')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    preprocessor = FactorPreprocessor()
    
    try:
        # 运行完整预处理流程
        df = preprocessor.run_full_preprocessing(
            start_date=args.start_date,
            end_date=args.end_date,
            table_name=args.table_name,
            output_table_name=args.output_table_name,
            missing_method=args.missing_method,
            winsorize_method=args.winsorize_method,
            winsorize_limits=tuple(args.winsorize_limits),
            standardize_method=args.standardize_method,
            neutralize=not args.no_neutralize,
            ic_analysis=not args.no_ic_analysis
        )
        
        print(f"\n📊 因子预处理完成:")
        print(f"  总记录数: {len(df):,}")
        print(f"  因子数量: {len([col for col in df.columns if col not in ['code', 'date', 'industry', 'code_name']]):,}")
        print(f"  保存到表: dws_stock_factors_preprocessed")
        
    except Exception as e:
        print(f"❌ 因子预处理失败: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        preprocessor.close()


if __name__ == '__main__':
    main()
