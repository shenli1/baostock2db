#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版单因子分析模块
不依赖alphalens，使用pandas和numpy进行基础的单因子分析
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import warnings
import os
warnings.filterwarnings('ignore')

# 设置matplotlib中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

from database.manager_fixed import DatabaseManagerFixed
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class SimpleFactorAnalyzer:
    """简化版单因子分析器"""
    
    def __init__(self):
        self.db_manager = DatabaseManagerFixed()
        
        # 基础信息列（非因子列）
        self.base_columns = ['code', 'date', 'industry', 'code_name', 'close', 'volume', 'amount', 'pctChg']
        
        # 排除的列（不参与因子分析）
        self.exclude_columns = [
            'code', 'date', 'industry', 'code_name', 'close', 'volume', 'amount', 'pctChg',
            'created_at', 'updated_at', 'pubDate', 'statDate', 'frequency', 'open', 'high', 'low', 
            'preclose', 'adjustflag', 'turn', 'tradestatus', 'isST', 'totalShare', 'liqaShare',
            # 原始数据列
            'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM', 'roeAvg', 'npMargin', 'gpMargin', 'netProfit', 
            'epsTTM', 'MBRevenue', 'currentRatio', 'quickRatio', 'cashRatio', 'liabilityToAsset', 
            'assetToEquity', 'CAToAsset', 'NCAToAsset', 'ebitToInterest', 'CFOToOR', 'CFOToNP', 
            'NRTurnRatio', 'INVTurnRatio', 'CATurnRatio', 'AssetTurnRatio', 'YOYEquity', 'YOYAsset', 
            'YOYNI', 'YOYEPSBasic', 'dupontROE', 'dupontAssetStoEquity', 'dupontAssetTurn',
            # 衍生列
            'market_cap_proxy', 'log_market_cap'
        ]
    
    def detect_factor_columns(self, table_name: str) -> List[str]:
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
    
    def get_factor_data(self, start_date: str, end_date: str, table_name: str) -> pd.DataFrame:
        """
        获取因子数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            table_name: 因子表名
            
        Returns:
            因子数据DataFrame
        """
        # 检测因子列
        factor_columns = self.detect_factor_columns(table_name)
        
        with self.db_manager.engine.connect() as conn:
            # 构建查询SQL
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
            
            # 确保日期列是datetime类型
            df['date'] = pd.to_datetime(df['date'])
            
            logger.info(f"获取因子数据: {len(df)} 条记录，{len(factor_columns)} 个因子")
            return df
    
    def calculate_future_returns(self, df: pd.DataFrame, periods: List[int] = [1, 5, 10]) -> pd.DataFrame:
        """
        计算未来N天收益率排名
        
        Args:
            df: 包含价格数据的DataFrame
            periods: 未来收益率计算周期列表
            
        Returns:
            包含未来收益率排名的DataFrame
        """
        df_returns = df.copy()
        
        # 按股票代码分组计算未来收益率
        for period in periods:
            # 计算未来N天的收益率
            future_returns = (
                df_returns.groupby('code')['close']
                .pct_change(periods=period)
                .shift(-period)  # 向前移动，得到未来收益率
            )
            
            # 对每日收益率进行排名（0-1之间，1表示最高收益）
            df_returns[f'future_return_{period}d'] = (
                df_returns.groupby('date')[future_returns.name]
                .rank(pct=True, method='dense')
            )
        
        logger.info(f"计算未来收益率排名完成，周期: {periods}")
        return df_returns

    def calculate_ic(self, factor_values: pd.Series, returns: pd.Series) -> float:
        """
        计算信息系数（IC）
        
        Args:
            factor_values: 因子值
            returns: 收益率
            
        Returns:
            IC值
        """
        # 确保索引对齐
        common_idx = factor_values.index.intersection(returns.index)
        if len(common_idx) < 10:
            return np.nan
        
        factor_clean = factor_values.loc[common_idx].dropna()
        returns_clean = returns.loc[common_idx].dropna()
        
        # 再次确保对齐
        final_idx = factor_clean.index.intersection(returns_clean.index)
        if len(final_idx) < 10:
            return np.nan
        
        return factor_clean.loc[final_idx].corr(returns_clean.loc[final_idx], method='spearman')
    
    def calculate_quantile_returns(self, df: pd.DataFrame, factor_name: str, 
                                 quantiles: int = 5) -> pd.DataFrame:
        """
        计算分层收益
        
        Args:
            df: 数据
            factor_name: 因子名称
            quantiles: 分层数量
            
        Returns:
            分层收益数据
        """
        quantile_returns = []
        
        for date, group in df.groupby('date'):
            if len(group) < 20:  # 数据点太少，跳过
                continue
            
            try:
                # 按因子值排序并分层
                group_sorted = group.sort_values(factor_name)
                group_sorted['quantile'] = pd.qcut(range(len(group_sorted)), 
                                                 quantiles, labels=False, duplicates='drop')
                
                # 计算各层收益
                quantile_stats = group_sorted.groupby('quantile')['future_return_1d'].agg(['mean', 'std', 'count'])
                quantile_stats['date'] = date
                quantile_returns.append(quantile_stats.reset_index())
                
            except ValueError as e:
                logger.warning(f"日期 {date} 分层失败: {str(e)}")
                continue
        
        if quantile_returns:
            return pd.concat(quantile_returns, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def calculate_overall_quantile_returns(self, df: pd.DataFrame, factor_name: str, 
                                         quantiles: int = 5) -> pd.DataFrame:
        """
        计算整体分层收益（不分日期）
        
        Args:
            df: 数据
            factor_name: 因子名称
            quantiles: 分层数量
            
        Returns:
            分层收益数据
        """
        try:
            # 按因子值排序并分层
            df_sorted = df.sort_values(factor_name)
            df_sorted['quantile'] = pd.qcut(range(len(df_sorted)), 
                                           quantiles, labels=False, duplicates='drop')
            
            # 计算各层收益统计
            quantile_stats = df_sorted.groupby('quantile')['future_return_1d'].agg([
                'mean', 'std', 'count', 'min', 'max'
            ]).reset_index()
            
            # 添加分位数标签
            quantile_stats['quantile_label'] = quantile_stats['quantile'].apply(
                lambda x: f'Q{x+1}' if not pd.isna(x) else 'N/A'
            )
            
            return quantile_stats
            
        except Exception as e:
            logger.warning(f"整体分层收益计算失败: {str(e)}")
            return pd.DataFrame()
    
    def calculate_detailed_quantile_analysis(self, df: pd.DataFrame, factor_name: str,
                                           quantiles: int = 5) -> Dict[str, Any]:
        """
        计算详细的分组对比分析
        
        Args:
            df: 包含因子和收益数据的DataFrame
            factor_name: 因子名称
            quantiles: 分层数量
            
        Returns:
            详细分组分析结果
        """
        try:
            # 按因子值排序并分层
            df_sorted = df.sort_values(factor_name)
            df_sorted['quantile'] = pd.qcut(range(len(df_sorted)), 
                                          quantiles, labels=False, duplicates='drop')
            
            # 计算各分层的详细统计
            quantile_analysis = []
            
            for q in range(quantiles):
                q_data = df_sorted[df_sorted['quantile'] == q]
                if len(q_data) == 0:
                    continue
                    
                # 基础统计
                stats = {
                    'quantile': q + 1,
                    'quantile_label': f'Q{q+1}',
                    'count': len(q_data),
                    'factor_mean': q_data[factor_name].mean(),
                    'factor_std': q_data[factor_name].std(),
                    'factor_min': q_data[factor_name].min(),
                    'factor_max': q_data[factor_name].max(),
                    'return_mean': q_data['future_return_1d'].mean(),
                    'return_std': q_data['future_return_1d'].std(),
                    'return_min': q_data['future_return_1d'].min(),
                    'return_max': q_data['future_return_1d'].max(),
                    'return_median': q_data['future_return_1d'].median(),
                    'win_rate': (q_data['future_return_1d'] > 0.5).mean(),  # 排名>0.5的比例
                    'sharpe_ratio': q_data['future_return_1d'].mean() / q_data['future_return_1d'].std() if q_data['future_return_1d'].std() > 0 else 0
                }
                
                # 计算相对表现（相对于市场平均）
                market_avg = df_sorted['future_return_1d'].mean()
                stats['excess_return'] = stats['return_mean'] - market_avg
                stats['relative_performance'] = stats['return_mean'] / market_avg if market_avg != 0 else 1
                
                quantile_analysis.append(stats)
            
            # 计算分层间的对比指标
            if len(quantile_analysis) >= 2:
                top_quantile = quantile_analysis[-1]  # 最高分位数
                bottom_quantile = quantile_analysis[0]  # 最低分位数
                
                # 多空收益差
                long_short_spread = top_quantile['return_mean'] - bottom_quantile['return_mean']
                
                # 信息比率（基于分层收益差）
                spread_std = np.std([q['return_mean'] for q in quantile_analysis])
                information_ratio = long_short_spread / spread_std if spread_std > 0 else 0
                
                # 单调性检验（Spearman相关系数）
                quantile_ranks = [q['quantile'] for q in quantile_analysis]
                return_means = [q['return_mean'] for q in quantile_analysis]
                monotonicity = np.corrcoef(quantile_ranks, return_means)[0, 1] if len(quantile_ranks) > 1 else 0
                
                return {
                    'quantile_analysis': quantile_analysis,
                    'long_short_spread': long_short_spread,
                    'information_ratio': information_ratio,
                    'monotonicity': monotonicity,
                    'top_quantile': top_quantile,
                    'bottom_quantile': bottom_quantile
                }
            else:
                return {
                    'quantile_analysis': quantile_analysis,
                    'long_short_spread': 0,
                    'information_ratio': 0,
                    'monotonicity': 0,
                    'top_quantile': None,
                    'bottom_quantile': None
                }
                
        except Exception as e:
            logger.warning(f"详细分组分析计算失败: {str(e)}")
            return {
                'quantile_analysis': [],
                'long_short_spread': 0,
                'information_ratio': 0,
                'monotonicity': 0,
                'top_quantile': None,
                'bottom_quantile': None
            }
    
    def analyze_single_factor(self, df: pd.DataFrame, factor_name: str, 
                            quantiles: int = 5, save_plots: bool = True, 
                            output_dir: str = "factor_analysis_plots") -> Dict[str, Any]:
        """
        分析单个因子
        
        Args:
            df: 数据
            factor_name: 因子名称
            quantiles: 分层数量
            save_plots: 是否保存图表
            output_dir: 图表输出目录
            
        Returns:
            分析结果
        """
        try:
            # 先计算未来收益率
            df_with_returns = self.calculate_future_returns(df, periods=[1, 5, 10])
            
            # 计算IC（使用1天未来收益率）
            ic_values = []
            for date, group in df_with_returns.groupby('date'):
                if len(group) < 10:
                    continue
                
                # 使用计算好的未来1天收益率
                group_clean = group.dropna(subset=[factor_name, 'future_return_1d'])
                if len(group_clean) < 10:
                    continue
                
                # 计算IC
                ic = self.calculate_ic(group_clean[factor_name], group_clean['future_return_1d'])
                if not np.isnan(ic):
                    ic_values.append(ic)
            
            # IC统计
            ic_mean = np.mean(ic_values) if ic_values else np.nan
            ic_std = np.std(ic_values) if ic_values else np.nan
            ic_ir = ic_mean / ic_std if ic_std != 0 and not np.isnan(ic_std) else np.nan
            
            # 计算分层收益
            quantile_returns = self.calculate_overall_quantile_returns(df_with_returns, factor_name, quantiles)
            top_return = bottom_return = spread = np.nan
            
            if not quantile_returns.empty and len(quantile_returns) >= 2:
                top_return = quantile_returns.iloc[-1]['mean']
                bottom_return = quantile_returns.iloc[0]['mean']
                spread = top_return - bottom_return
            
            # 计算详细分组分析
            detailed_quantile_analysis = self.calculate_detailed_quantile_analysis(df_with_returns, factor_name, quantiles)
            
            # 因子分布统计
            factor_values = df[factor_name].dropna()
            factor_mean = factor_values.mean()
            factor_std = factor_values.std()
            factor_skew = factor_values.skew()
            factor_kurt = factor_values.kurtosis()
            
            # 先创建基础结果
            results = {
                'factor_name': factor_name,
                'ic_mean': ic_mean,
                'ic_std': ic_std,
                'ic_ir': ic_ir,
                'ic_count': len(ic_values),
                'top_quantile_return': top_return,
                'bottom_quantile_return': bottom_return,
                'spread': spread,
                'factor_mean': factor_mean,
                'factor_std': factor_std,
                'factor_skew': factor_skew,
                'factor_kurt': factor_kurt,
                'data_points': len(factor_values),
                'quantile_returns': quantile_returns,
                'detailed_quantile_analysis': detailed_quantile_analysis
            }
            
            # 创建图表
            plots = {}
            saved_plots = {}
            html_report = None
            if save_plots:
                try:
                    # 使用真实的分层收益数据创建图表
                    plots = self.create_factor_plots(df_with_returns, factor_name, ic_values, quantile_returns)
                    saved_plots = self.save_factor_plots(plots, factor_name, output_dir)
                    
                    # 创建HTML报告
                    html_report = self.create_html_report(df_with_returns, factor_name, ic_values, results, output_dir)
                    
                except Exception as e:
                    logger.warning(f"创建或保存因子 {factor_name} 图表时出错: {str(e)}")
            
            # 更新结果，添加图表和HTML报告
            results.update({
                'quantile_returns': quantile_returns,
                'plots': plots,
                'saved_plots': saved_plots,
                'html_report': html_report
            })
            
            logger.info(f"因子 {factor_name} 分析完成: IC={ic_mean:.4f}, IR={ic_ir:.4f}, Spread={spread:.4f}")
            return results
            
        except Exception as e:
            logger.error(f"因子 {factor_name} 分析失败: {str(e)}")
            return {
                'factor_name': factor_name,
                'error': str(e),
                'ic_mean': np.nan,
                'ic_std': np.nan,
                'ic_ir': np.nan,
                'top_quantile_return': np.nan,
                'bottom_quantile_return': np.nan,
                'spread': np.nan
            }
    
    def create_factor_plots(self, df: pd.DataFrame, factor_name: str, 
                          ic_values: List[float], quantile_returns: pd.DataFrame) -> Dict[str, Any]:
        """
        创建因子分析图表
        
        Args:
            df: 数据
            factor_name: 因子名称
            ic_values: IC值列表
            quantile_returns: 分层收益数据
            
        Returns:
            图表字典
        """
        plots = {}
        
        try:
            # 1. 因子分布图
            plt.figure(figsize=(10, 6))
            df[factor_name].hist(bins=50, alpha=0.7, edgecolor='black')
            plt.title(f'{factor_name} - 因子分布')
            plt.xlabel('因子值')
            plt.ylabel('频数')
            plt.grid(True, alpha=0.3)
            plots['distribution'] = plt.gcf()
            plt.close()
            
            # 2. IC时间序列图
            if ic_values and len(ic_values) > 1:
                plt.figure(figsize=(12, 6))
                plt.plot(ic_values, marker='o', linewidth=2, markersize=4)
                plt.title(f'{factor_name} - IC时间序列')
                plt.xlabel('时间')
                plt.ylabel('IC值')
                plt.grid(True, alpha=0.3)
                plt.axhline(y=0, color='r', linestyle='--', alpha=0.7)
                plots['ic_timeseries'] = plt.gcf()
                plt.close()
            
            # 3. 分层收益柱状图
            if not quantile_returns.empty and 'mean' in quantile_returns.columns:
                plt.figure(figsize=(12, 8))
                
                # 创建柱状图
                quantile_labels = quantile_returns['quantile_label'].tolist()
                mean_returns = quantile_returns['mean'].tolist()
                std_returns = quantile_returns['std'].tolist()
                
                # 设置颜色（从低到高，绿色到红色）
                colors = plt.cm.RdYlGn(np.linspace(0.2, 0.8, len(quantile_labels)))
                
                bars = plt.bar(quantile_labels, mean_returns, color=colors, 
                              edgecolor='black', linewidth=1, alpha=0.8)
                
                # 添加误差线
                plt.errorbar(quantile_labels, mean_returns, yerr=std_returns, 
                           fmt='none', color='black', capsize=5, capthick=2)
                
                # 添加数值标签
                for i, (bar, mean_ret, std_ret) in enumerate(zip(bars, mean_returns, std_returns)):
                    height = bar.get_height()
                    plt.text(bar.get_x() + bar.get_width()/2., height + std_ret + 0.001,
                            f'{mean_ret:.4f}\n(±{std_ret:.4f})', 
                            ha='center', va='bottom', fontsize=9, fontweight='bold')
                
                plt.title(f'{factor_name} - 分层收益分析', fontsize=14, fontweight='bold')
                plt.xlabel('分位数', fontsize=12)
                plt.ylabel('平均收益率', fontsize=12)
                plt.grid(True, alpha=0.3, axis='y')
                plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
                
                # 添加说明文字
                plt.figtext(0.02, 0.02, f'数据点: {quantile_returns["count"].sum():,} | 最高-最低收益差: {max(mean_returns) - min(mean_returns):.4f}', 
                           fontsize=10, style='italic')
                
                plt.tight_layout()
                plots['returns_bar'] = plt.gcf()
                plt.close()
            
            # 4. 因子与收益散点图
            plt.figure(figsize=(10, 6))
            plt.scatter(df[factor_name], df['future_return_1d'], alpha=0.5, s=20)
            plt.title(f'{factor_name} - 因子值与未来收益关系')
            plt.xlabel('因子值')
            plt.ylabel('未来1天收益')
            plt.grid(True, alpha=0.3)
            plots['scatter'] = plt.gcf()
            plt.close()
            
        except Exception as e:
            logger.warning(f"创建因子 {factor_name} 图表时出错: {str(e)}")
        
        return plots
    
    def save_factor_plots(self, plots: Dict[str, Any], factor_name: str, 
                         output_dir: str = "factor_analysis_plots") -> Dict[str, str]:
        """
        保存因子分析图表
        
        Args:
            plots: 图表字典
            factor_name: 因子名称
            output_dir: 输出目录
            
        Returns:
            保存的图表文件路径字典
        """
        # 创建输出目录
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 创建因子子目录
        factor_dir = os.path.join(output_dir, factor_name)
        if not os.path.exists(factor_dir):
            os.makedirs(factor_dir)
        
        saved_plots = {}
        
        try:
            # 保存因子分布图
            if 'distribution' in plots and plots['distribution'] is not None:
                dist_path = os.path.join(factor_dir, f'{factor_name}_distribution.png')
                plots['distribution'].savefig(dist_path, dpi=300, bbox_inches='tight')
                saved_plots['distribution'] = dist_path
                logger.info(f"保存因子分布图: {dist_path}")
            
            # 保存IC时间序列图
            if 'ic_timeseries' in plots and plots['ic_timeseries'] is not None:
                ic_path = os.path.join(factor_dir, f'{factor_name}_ic_timeseries.png')
                plots['ic_timeseries'].savefig(ic_path, dpi=300, bbox_inches='tight')
                saved_plots['ic_timeseries'] = ic_path
                logger.info(f"保存IC时间序列图: {ic_path}")
            
            # 保存分层收益柱状图
            if 'returns_bar' in plots and plots['returns_bar'] is not None:
                bar_path = os.path.join(factor_dir, f'{factor_name}_returns_bar.png')
                plots['returns_bar'].savefig(bar_path, dpi=300, bbox_inches='tight')
                saved_plots['returns_bar'] = bar_path
                logger.info(f"保存分层收益柱状图: {bar_path}")
            
            # 保存散点图
            if 'scatter' in plots and plots['scatter'] is not None:
                scatter_path = os.path.join(factor_dir, f'{factor_name}_scatter.png')
                plots['scatter'].savefig(scatter_path, dpi=300, bbox_inches='tight')
                saved_plots['scatter'] = scatter_path
                logger.info(f"保存散点图: {scatter_path}")
            
        except Exception as e:
            logger.error(f"保存因子 {factor_name} 图表时出错: {str(e)}")
        
        return saved_plots
    
    def create_html_report(self, df: pd.DataFrame, factor_name: str, 
                          ic_values: List[float], results: Dict[str, Any],
                          output_dir: str = "factor_analysis_plots") -> str:
        """
        创建HTML格式的因子分析报告
        
        Args:
            df: 数据
            factor_name: 因子名称
            ic_values: IC值列表
            results: 分析结果
            output_dir: 输出目录
            
        Returns:
            HTML文件路径
        """
        try:
            # 创建输出目录
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # 创建因子子目录
            factor_dir = os.path.join(output_dir, factor_name)
            if not os.path.exists(factor_dir):
                os.makedirs(factor_dir)
            
            # 生成HTML文件路径
            html_path = os.path.join(factor_dir, f'{factor_name}_analysis_report.html')
            
            # 创建HTML报告
            html_content = f"""
            <!DOCTYPE html>
            <html lang="zh-CN">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>{factor_name} - 因子分析报告</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                    .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                    h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
                    h2 {{ color: #34495e; margin-top: 30px; }}
                    .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
                    .stat-card {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; text-align: center; }}
                    .stat-value {{ font-size: 24px; font-weight: bold; margin-bottom: 5px; }}
                    .stat-label {{ font-size: 14px; opacity: 0.9; }}
                    .chart-container {{ margin: 20px 0; text-align: center; }}
                    .chart-container img {{ max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 4px; }}
                    table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                    th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                    th {{ background-color: #f8f9fa; font-weight: bold; }}
                    .positive {{ color: #27ae60; font-weight: bold; }}
                    .negative {{ color: #e74c3c; font-weight: bold; }}
                    .neutral {{ color: #95a5a6; }}
                    .info-box {{ background-color: #e8f4fd; border-left: 4px solid #3498db; padding: 15px; margin: 20px 0; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>📊 {factor_name} - 因子分析报告</h1>
                    
                    <h2>📈 关键指标</h2>
                    <div class="stats-grid">
                        <div class="stat-card">
                            <div class="stat-value">{results.get('ic_mean', 0):.4f}</div>
                            <div class="stat-label">IC均值</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{results.get('ic_std', 0):.4f}</div>
                            <div class="stat-label">IC标准差</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{results.get('ic_ir', 0):.4f}</div>
                            <div class="stat-label">IC信息比率</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{results.get('data_points', 0):,}</div>
                            <div class="stat-label">数据点数</div>
                        </div>
                    </div>
                    
                    <h2>📊 分层收益分析</h2>
                    <div class="info-box">
                        <p><strong>最高分位数收益:</strong> <span class="{'positive' if results.get('top_quantile_return', 0) > 0 else 'negative' if results.get('top_quantile_return', 0) < 0 else 'neutral'}">{results.get('top_quantile_return', 0):.4f}</span></p>
                        <p><strong>最低分位数收益:</strong> <span class="{'positive' if results.get('bottom_quantile_return', 0) > 0 else 'negative' if results.get('bottom_quantile_return', 0) < 0 else 'neutral'}">{results.get('bottom_quantile_return', 0):.4f}</span></p>
                        <p><strong>分层收益差:</strong> <span class="{'positive' if results.get('spread', 0) > 0 else 'negative' if results.get('spread', 0) < 0 else 'neutral'}">{results.get('spread', 0):.4f}</span></p>
                    </div>
                    
                    <h3>分层收益详细表格</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>分位数</th>
                                <th>平均收益</th>
                                <th>标准差</th>
                                <th>样本数</th>
                                <th>最小收益</th>
                                <th>最大收益</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            # 添加分层收益详细数据
            if 'quantile_returns' in results and not results['quantile_returns'].empty:
                for _, row in results['quantile_returns'].iterrows():
                    mean_ret = row['mean']
                    std_ret = row['std']
                    count = row['count']
                    min_ret = row['min']
                    max_ret = row['max']
                    
                    html_content += f"""
                            <tr>
                                <td>{row['quantile_label']}</td>
                                <td class="{'positive' if mean_ret > 0 else 'negative' if mean_ret < 0 else 'neutral'}">{mean_ret:.4f}</td>
                                <td>{std_ret:.4f}</td>
                                <td>{count:,}</td>
                                <td class="{'positive' if min_ret > 0 else 'negative' if min_ret < 0 else 'neutral'}">{min_ret:.4f}</td>
                                <td class="{'positive' if max_ret > 0 else 'negative' if max_ret < 0 else 'neutral'}">{max_ret:.4f}</td>
                            </tr>
                    """
            else:
                html_content += """
                            <tr>
                                <td colspan="6" class="neutral">暂无分层收益数据</td>
                            </tr>
                """
            
            html_content += f"""
                        </tbody>
                    </table>
                    
                    <h2>📈 图表分析</h2>
                    <div class="chart-container">
                        <h3>因子分布图</h3>
                        <img src="{factor_name}_distribution.png" alt="因子分布图">
                    </div>
                    
                    <div class="chart-container">
                        <h3>IC时间序列图</h3>
                        <img src="{factor_name}_ic_timeseries.png" alt="IC时间序列图">
                    </div>
                    
                    <div class="chart-container">
                        <h3>分层收益柱状图</h3>
                        <img src="{factor_name}_returns_bar.png" alt="分层收益柱状图">
                    </div>
                    
                    <div class="chart-container">
                        <h3>因子与收益关系图</h3>
                        <img src="{factor_name}_scatter.png" alt="因子与收益关系图">
                    </div>
                    
                    <h2>📋 因子统计信息</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>统计指标</th>
                                <th>数值</th>
                                <th>说明</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>因子均值</td>
                                <td>{results.get('factor_mean', 0):.6f}</td>
                                <td>因子值的平均水平</td>
                            </tr>
                            <tr>
                                <td>因子标准差</td>
                                <td>{results.get('factor_std', 0):.6f}</td>
                                <td>因子值的离散程度</td>
                            </tr>
                            <tr>
                                <td>偏度</td>
                                <td>{results.get('factor_skew', 0):.4f}</td>
                                <td>因子分布的对称性</td>
                            </tr>
                            <tr>
                                <td>峰度</td>
                                <td>{results.get('factor_kurt', 0):.4f}</td>
                                <td>因子分布的尖锐程度</td>
                            </tr>
                            <tr>
                                <td>IC计算次数</td>
                                <td>{len(ic_values)}</td>
                                <td>有效IC计算的天数</td>
                            </tr>
                        </tbody>
                    </table>
                    
                    <h2>📋 分析总结</h2>
                    <p><strong>因子名称:</strong> {factor_name}</p>
                    <p><strong>分析时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p><strong>数据期间:</strong> {df['date'].min().strftime('%Y-%m-%d')} 到 {df['date'].max().strftime('%Y-%m-%d')}</p>
                    <p><strong>总数据点:</strong> {len(df):,}</p>
                    
                    <h3>因子有效性评估</h3>
                    <ul>
                        <li><strong>IC信息比率:</strong> {results.get('ic_ir', 0):.4f} - {self._get_ic_rating(results.get('ic_ir', 0))}</li>
                        <li><strong>分层收益差:</strong> {results.get('spread', 0):.4f}</li>
                        <li><strong>数据质量:</strong> 基于 {results.get('data_points', 0):,} 个有效数据点</li>
                        <li><strong>因子稳定性:</strong> {self._get_stability_rating(results.get('factor_std', 0), results.get('factor_mean', 0))}</li>
                    </ul>
                    
                    <div class="info-box">
                        <h4>📝 报告说明</h4>
                        <p>本报告基于简化版单因子分析框架生成，包含因子的完整分析结果。图表文件保存在同一目录下，可以单独查看。</p>
                        <p><strong>IC信息比率解释:</strong></p>
                        <ul>
                            <li>IR > 0.1: 因子表现良好</li>
                            <li>0.05 < IR ≤ 0.1: 因子表现一般</li>
                            <li>0 < IR ≤ 0.05: 因子表现较弱</li>
                            <li>IR ≤ 0: 因子无预测能力</li>
                        </ul>
                    </div>
                </div>
            </body>
            </html>
            """
            
            # 保存HTML文件
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"创建HTML分析报告: {html_path}")
            return html_path
            
        except Exception as e:
            logger.error(f"创建因子 {factor_name} HTML报告失败: {str(e)}")
            return None
    
    def _get_ic_rating(self, ic_ir: float) -> str:
        """获取IC信息比率评级"""
        if ic_ir > 0.1:
            return "表现良好"
        elif ic_ir > 0.05:
            return "表现一般"
        elif ic_ir > 0:
            return "表现较弱"
        else:
            return "无预测能力"
    
    def _get_stability_rating(self, factor_std: float, factor_mean: float) -> str:
        """获取因子稳定性评级"""
        if factor_std == 0:
            return "完全稳定"
        cv = abs(factor_std / factor_mean) if factor_mean != 0 else float('inf')
        if cv < 0.1:
            return "非常稳定"
        elif cv < 0.3:
            return "比较稳定"
        elif cv < 0.5:
            return "一般稳定"
        else:
            return "不够稳定"
    
    def analyze_all_factors(self, start_date: str, end_date: str, table_name: str, 
                          quantiles: int = 5, max_factors: int = None,
                          save_plots: bool = True, output_dir: str = "factor_analysis_plots") -> Dict[str, Any]:
        """
        分析表中的所有因子
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            table_name: 因子表名
            quantiles: 分层数量
            max_factors: 最大分析因子数量（None表示分析所有）
            save_plots: 是否保存图表
            output_dir: 图表输出目录
            
        Returns:
            所有因子的分析结果
        """
        logger.info(f"开始分析表 {table_name} 中的所有因子")
        
        # 获取因子数据
        df = self.get_factor_data(start_date, end_date, table_name)
        
        # 检测因子列
        factor_columns = self.detect_factor_columns(table_name)
        
        if max_factors:
            factor_columns = factor_columns[:max_factors]
        
        all_results = {}
        summary_stats = []
        
        for i, factor_name in enumerate(factor_columns):
            logger.info(f"分析因子 {i+1}/{len(factor_columns)}: {factor_name}")
            
            try:
                # 分析因子
                result = self.analyze_single_factor(df, factor_name, quantiles, save_plots, output_dir)
                all_results[factor_name] = result
                
                # 添加到汇总统计
                if 'error' not in result:
                    summary_stats.append({
                        'factor_name': factor_name,
                        'ic_mean': result['ic_mean'],
                        'ic_std': result['ic_std'],
                        'ic_ir': result['ic_ir'],
                        'spread': result['spread'],
                        'factor_mean': result['factor_mean'],
                        'factor_std': result['factor_std'],
                        'factor_skew': result['factor_skew'],
                        'factor_kurt': result['factor_kurt'],
                        'data_points': result['data_points']
                    })
                
            except Exception as e:
                logger.error(f"分析因子 {factor_name} 时出错: {str(e)}")
                all_results[factor_name] = {
                    'factor_name': factor_name,
                    'error': str(e)
                }
        
        # 创建汇总统计
        if summary_stats:
            summary_df = pd.DataFrame(summary_stats)
            summary_df = summary_df.sort_values('ic_ir', ascending=False)
            
            # 保存汇总结果
            self.save_analysis_summary(summary_df, table_name, start_date, end_date)
            
            # 创建整合的HTML报告
            effective_factors = [row['factor_name'] for _, row in summary_df.iterrows() 
                               if not np.isnan(row['ic_ir']) and row['ic_ir'] > 0.05]
            if effective_factors:
                self.create_consolidated_html_report(
                    effective_factors, all_results, summary_df, 
                    start_date, end_date, table_name, output_dir
                )
            
            logger.info(f"因子分析完成，共分析 {len(summary_stats)} 个因子")
            logger.info(f"有效因子数量: {len(effective_factors)}")
            logger.info(f"IC信息比率排名前5: {summary_df.head()['factor_name'].tolist()}")
            
            return {
                'summary': summary_df,
                'detailed_results': all_results,
                'effective_factors': effective_factors,
                'total_factors': len(factor_columns),
                'analyzed_factors': len(summary_stats),
                'failed_factors': len(factor_columns) - len(summary_stats)
            }
        else:
            logger.warning("没有成功分析任何因子")
            return {
                'summary': pd.DataFrame(),
                'detailed_results': all_results,
                'total_factors': len(factor_columns),
                'analyzed_factors': 0,
                'failed_factors': len(factor_columns)
            }
    
    def create_analysis_summary_table(self, table_name: str):
        """创建分析汇总表"""
        try:
            summary_table_name = f"{table_name}_factor_analysis_summary"
            
            # 检查表是否存在
            with self.db_manager.engine.connect() as conn:
                check_query = f"SHOW TABLES LIKE '{summary_table_name}'"
                result = conn.execute(text(check_query))
                if result.fetchone():
                    logger.info(f"表 {summary_table_name} 已存在")
                    return summary_table_name
            
            # 创建表
            create_table_sql = f"""
            CREATE TABLE {summary_table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                factor_name VARCHAR(100) NOT NULL,
                table_name VARCHAR(100) NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                analysis_date DATETIME NOT NULL,
                ic_mean DECIMAL(10, 6),
                ic_std DECIMAL(10, 6),
                ic_ir DECIMAL(10, 6),
                ic_count INT,
                top_quantile_return DECIMAL(10, 6),
                bottom_quantile_return DECIMAL(10, 6),
                spread DECIMAL(10, 6),
                factor_mean DECIMAL(15, 8),
                factor_std DECIMAL(15, 8),
                factor_skew DECIMAL(10, 6),
                factor_kurt DECIMAL(10, 6),
                data_points INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_factor_analysis (factor_name, table_name, start_date, end_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            with self.db_manager.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
                logger.info(f"创建分析汇总表: {summary_table_name}")
            
            return summary_table_name
            
        except Exception as e:
            logger.error(f"创建分析汇总表失败: {str(e)}")
            return None

    def create_consolidated_html_report(self, effective_factors: List[str], all_results: Dict[str, Any], 
                                      summary_df: pd.DataFrame, start_date: str, end_date: str, 
                                      table_name: str, output_dir: str = "factor_analysis_plots") -> str:
        """
        创建整合的有效因子分析HTML报告
        
        Args:
            effective_factors: 有效因子列表
            all_results: 所有因子分析结果
            summary_df: 汇总统计DataFrame
            start_date: 开始日期
            end_date: 结束日期
            table_name: 表名
            output_dir: 输出目录
            
        Returns:
            HTML文件路径
        """
        try:
            # 创建输出目录
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # 生成HTML文件路径
            html_path = os.path.join(output_dir, f"{table_name}_effective_factors_analysis.html")
            
            # 创建HTML报告
            html_content = f"""
            <!DOCTYPE html>
            <html lang="zh-CN">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>有效因子分析报告 - {table_name}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                    .container {{ max-width: 1400px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                    h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
                    h2 {{ color: #34495e; margin-top: 30px; }}
                    .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
                    .stat-card {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; text-align: center; }}
                    .stat-value {{ font-size: 24px; font-weight: bold; margin-bottom: 5px; }}
                    .stat-label {{ font-size: 14px; opacity: 0.9; }}
                    table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                    th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                    th {{ background-color: #f8f9fa; font-weight: bold; }}
                    .positive {{ color: #27ae60; font-weight: bold; }}
                    .negative {{ color: #e74c3c; font-weight: bold; }}
                    .neutral {{ color: #95a5a6; }}
                    .factor-section {{ margin: 30px 0; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #fafafa; }}
                    .factor-header {{ background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); color: white; padding: 15px; margin: -20px -20px 20px -20px; border-radius: 8px 8px 0 0; }}
                    .info-box {{ background-color: #e8f4fd; border-left: 4px solid #3498db; padding: 15px; margin: 20px 0; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>📊 有效因子分析报告</h1>
                    
                    <div class="info-box">
                        <h3>📋 分析概览</h3>
                        <p><strong>数据表:</strong> {table_name}</p>
                        <p><strong>分析期间:</strong> {start_date} 至 {end_date}</p>
                        <p><strong>分析时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                        <p><strong>有效因子数量:</strong> {len(effective_factors)}</p>
                    </div>
                    
                    <h2>📈 有效因子排名</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>排名</th>
                                <th>因子名称</th>
                                <th>IC均值</th>
                                <th>IC标准差</th>
                                <th>IC信息比率</th>
                                <th>分层收益差</th>
                                <th>数据点数</th>
                                <th>评级</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            # 添加有效因子排名表格
            for i, factor_name in enumerate(effective_factors, 1):
                if factor_name in all_results and 'error' not in all_results[factor_name]:
                    result = all_results[factor_name]
                    ic_ir = result.get('ic_ir', 0)
                    
                    # 评级
                    if ic_ir > 0.2:
                        rating = "优秀"
                        rating_class = "positive"
                    elif ic_ir > 0.1:
                        rating = "良好"
                        rating_class = "positive"
                    elif ic_ir > 0.05:
                        rating = "一般"
                        rating_class = "neutral"
                    else:
                        rating = "较差"
                        rating_class = "negative"
                    
                    html_content += f"""
                            <tr>
                                <td>{i}</td>
                                <td><strong>{factor_name}</strong></td>
                                <td class="{'positive' if result.get('ic_mean', 0) > 0 else 'negative' if result.get('ic_mean', 0) < 0 else 'neutral'}">{result.get('ic_mean', 0):.4f}</td>
                                <td>{result.get('ic_std', 0):.4f}</td>
                                <td class="{'positive' if ic_ir > 0 else 'negative' if ic_ir < 0 else 'neutral'}">{ic_ir:.4f}</td>
                                <td class="{'positive' if result.get('spread', 0) > 0 else 'negative' if result.get('spread', 0) < 0 else 'neutral'}">{result.get('spread', 0):.4f}</td>
                                <td>{result.get('data_points', 0):,}</td>
                                <td class="{rating_class}">{rating}</td>
                            </tr>
                    """
            
            html_content += """
                        </tbody>
                    </table>
                    
                    <h2>📊 有效因子详细分析</h2>
            """
            
            # 为每个有效因子创建详细分析部分
            for factor_name in effective_factors:
                if factor_name in all_results and 'error' not in all_results[factor_name]:
                    result = all_results[factor_name]
                    
                    html_content += f"""
                    <div class="factor-section">
                        <div class="factor-header">
                            <h3>📈 {factor_name}</h3>
                        </div>
                        
                        <div class="stats-grid">
                            <div class="stat-card">
                                <div class="stat-value">{result.get('ic_mean', 0):.4f}</div>
                                <div class="stat-label">IC均值</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-value">{result.get('ic_std', 0):.4f}</div>
                                <div class="stat-label">IC标准差</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-value">{result.get('ic_ir', 0):.4f}</div>
                                <div class="stat-label">IC信息比率</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-value">{result.get('data_points', 0):,}</div>
                                <div class="stat-label">数据点数</div>
                            </div>
                        </div>
                        
                        <h4>📊 分层收益分析</h4>
                        <table>
                            <thead>
                                <tr>
                                    <th>指标</th>
                                    <th>数值</th>
                                    <th>说明</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr>
                                    <td>最高分位数收益</td>
                                    <td class="{'positive' if result.get('top_quantile_return', 0) > 0 else 'negative' if result.get('top_quantile_return', 0) < 0 else 'neutral'}">{result.get('top_quantile_return', 0):.4f}</td>
                                    <td>因子值最高分位数的平均收益</td>
                                </tr>
                                <tr>
                                    <td>最低分位数收益</td>
                                    <td class="{'positive' if result.get('bottom_quantile_return', 0) > 0 else 'negative' if result.get('bottom_quantile_return', 0) < 0 else 'neutral'}">{result.get('bottom_quantile_return', 0):.4f}</td>
                                    <td>因子值最低分位数的平均收益</td>
                                </tr>
                                <tr>
                                    <td>分层收益差</td>
                                    <td class="{'positive' if result.get('spread', 0) > 0 else 'negative' if result.get('spread', 0) < 0 else 'neutral'}">{result.get('spread', 0):.4f}</td>
                                    <td>最高分位数收益 - 最低分位数收益</td>
                                </tr>
                            </tbody>
                        </table>
                        
                        <h4>📈 详细分组对比分析</h4>
                        <table>
                            <thead>
                                <tr>
                                    <th>分位数</th>
                                    <th>样本数</th>
                                    <th>因子均值</th>
                                    <th>收益均值</th>
                                    <th>收益标准差</th>
                                    <th>胜率</th>
                                    <th>夏普比率</th>
                                    <th>超额收益</th>
                                    <th>相对表现</th>
                                </tr>
                            </thead>
                            <tbody>
            """
            
            # 添加详细分组分析表格
            detailed_analysis = result.get('detailed_quantile_analysis', {})
            quantile_analysis = detailed_analysis.get('quantile_analysis', [])
            
            for q_analysis in quantile_analysis:
                html_content += f"""
                                <tr>
                                    <td><strong>{q_analysis['quantile_label']}</strong></td>
                                    <td>{q_analysis['count']:,}</td>
                                    <td>{q_analysis['factor_mean']:.4f}</td>
                                    <td class="{'positive' if q_analysis['return_mean'] > 0.5 else 'negative' if q_analysis['return_mean'] < 0.5 else 'neutral'}">{q_analysis['return_mean']:.4f}</td>
                                    <td>{q_analysis['return_std']:.4f}</td>
                                    <td class="{'positive' if q_analysis['win_rate'] > 0.5 else 'negative' if q_analysis['win_rate'] < 0.5 else 'neutral'}">{q_analysis['win_rate']:.2%}</td>
                                    <td class="{'positive' if q_analysis['sharpe_ratio'] > 0 else 'negative' if q_analysis['sharpe_ratio'] < 0 else 'neutral'}">{q_analysis['sharpe_ratio']:.4f}</td>
                                    <td class="{'positive' if q_analysis['excess_return'] > 0 else 'negative' if q_analysis['excess_return'] < 0 else 'neutral'}">{q_analysis['excess_return']:.4f}</td>
                                    <td class="{'positive' if q_analysis['relative_performance'] > 1 else 'negative' if q_analysis['relative_performance'] < 1 else 'neutral'}">{q_analysis['relative_performance']:.4f}</td>
                                </tr>
                """
            
            html_content += """
                            </tbody>
                        </table>
                        
                        <h4>📊 分组分析总结</h4>
                        <table>
                            <thead>
                                <tr>
                                    <th>指标</th>
                                    <th>数值</th>
                                    <th>说明</th>
                                </tr>
                            </thead>
                            <tbody>
            """
            
            # 添加分组分析总结
            if detailed_analysis:
                html_content += f"""
                                <tr>
                                    <td>多空收益差</td>
                                    <td class="{'positive' if detailed_analysis.get('long_short_spread', 0) > 0 else 'negative' if detailed_analysis.get('long_short_spread', 0) < 0 else 'neutral'}">{detailed_analysis.get('long_short_spread', 0):.4f}</td>
                                    <td>最高分位数与最低分位数收益差</td>
                                </tr>
                                <tr>
                                    <td>信息比率</td>
                                    <td class="{'positive' if detailed_analysis.get('information_ratio', 0) > 0 else 'negative' if detailed_analysis.get('information_ratio', 0) < 0 else 'neutral'}">{detailed_analysis.get('information_ratio', 0):.4f}</td>
                                    <td>基于分层收益差的信息比率</td>
                                </tr>
                                <tr>
                                    <td>单调性</td>
                                    <td class="{'positive' if detailed_analysis.get('monotonicity', 0) > 0.5 else 'negative' if detailed_analysis.get('monotonicity', 0) < -0.5 else 'neutral'}">{detailed_analysis.get('monotonicity', 0):.4f}</td>
                                    <td>分位数与收益的相关系数（>0.5为强单调性）</td>
                                </tr>
                """
            
            html_content += """
                            </tbody>
                        </table>
                    </div>
                    """
            
            html_content += f"""
                    <div class="info-box">
                        <h4>📝 报告说明</h4>
                        <p>本报告基于自定义因子分析框架生成，包含所有有效因子的完整分析结果。</p>
                        <p><strong>有效因子标准:</strong> IC信息比率 > 0.05</p>
                        <p><strong>评级标准:</strong></p>
                        <ul>
                            <li>优秀: IC信息比率 > 0.2</li>
                            <li>良好: 0.1 < IC信息比率 ≤ 0.2</li>
                            <li>一般: 0.05 < IC信息比率 ≤ 0.1</li>
                            <li>较差: IC信息比率 ≤ 0.05</li>
                        </ul>
                        <p><strong>收益率计算:</strong> 使用股票收益率的日排名（0-1之间，1表示最高收益）</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            # 保存HTML文件
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"创建整合HTML分析报告: {html_path}")
            return html_path
            
        except Exception as e:
            logger.error(f"创建整合HTML报告失败: {str(e)}")
            return None

    def save_analysis_summary(self, summary_df: pd.DataFrame, table_name: str, 
                            start_date: str, end_date: str):
        """保存分析汇总结果"""
        try:
            # 创建汇总表
            summary_table_name = f"{table_name}_factor_analysis_summary"
            
            # 确保表存在
            self.create_analysis_summary_table(table_name)
            
            # 添加分析信息
            summary_df['table_name'] = table_name
            summary_df['start_date'] = start_date
            summary_df['end_date'] = end_date
            summary_df['analysis_date'] = datetime.now()
            
            # 保存到数据库
            batch_data = summary_df.to_dict('records')
            self.db_manager.upsert_data_safe(summary_table_name, batch_data, ['factor_name', 'table_name'])
            
            logger.info(f"分析汇总结果已保存到表: {summary_table_name}")
            
        except Exception as e:
            logger.error(f"保存分析汇总结果失败: {str(e)}")
    
    def generate_analysis_report(self, results: Dict[str, Any], output_file: str = None):
        """生成分析报告"""
        if not results['summary'].empty:
            summary_df = results['summary']
            
            # 创建报告
            report = f"""
# 单因子分析报告

## 分析概览
- 总因子数: {results['total_factors']}
- 成功分析: {results['analyzed_factors']}
- 分析失败: {results['failed_factors']}

## 因子排名（按IC信息比率）

| 排名 | 因子名称 | IC均值 | IC标准差 | IC信息比率 | 分层收益差 | 因子均值 | 因子标准差 | 偏度 | 峰度 |
|------|----------|--------|----------|------------|------------|----------|------------|------|------|
"""
            
            for i, (_, row) in enumerate(summary_df.head(10).iterrows(), 1):
                report += f"| {i} | {row['factor_name']} | {row['ic_mean']:.4f} | {row['ic_std']:.4f} | {row['ic_ir']:.4f} | {row['spread']:.4f} | {row['factor_mean']:.4f} | {row['factor_std']:.4f} | {row['factor_skew']:.4f} | {row['factor_kurt']:.4f} |\n"
            
            # 保存报告
            if output_file is None:
                output_file = f"factor_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report)
            
            logger.info(f"分析报告已保存到: {output_file}")
            return report
        else:
            logger.warning("没有分析结果可生成报告")
            return None
    
    def close(self):
        """关闭数据库连接"""
        self.db_manager.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='简化版单因子分析器')
    parser.add_argument('--start-date', required=True, help='开始日期')
    parser.add_argument('--end-date', required=True, help='结束日期')
    parser.add_argument('--table-name', required=True, help='因子表名')
    parser.add_argument('--quantiles', type=int, default=5, help='分层数量')
    parser.add_argument('--max-factors', type=int, help='最大分析因子数量')
    parser.add_argument('--output-file', help='输出报告文件名')
    parser.add_argument('--save-plots', action='store_true', help='保存图表')
    parser.add_argument('--output-dir', default='factor_analysis_plots', help='图表输出目录')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    analyzer = SimpleFactorAnalyzer()
    
    try:
        # 运行单因子分析
        results = analyzer.analyze_all_factors(
            start_date=args.start_date,
            end_date=args.end_date,
            table_name=args.table_name,
            quantiles=args.quantiles,
            max_factors=args.max_factors,
            save_plots=args.save_plots,
            output_dir=args.output_dir
        )
        
        # 生成报告
        report = analyzer.generate_analysis_report(results, args.output_file)
        
        print(f"\n📊 单因子分析完成:")
        print(f"  总因子数: {results['total_factors']}")
        print(f"  成功分析: {results['analyzed_factors']}")
        print(f"  分析失败: {results['failed_factors']}")
        
        if not results['summary'].empty:
            print(f"\n🏆 IC信息比率排名前5:")
            for i, (_, row) in enumerate(results['summary'].head(5).iterrows(), 1):
                print(f"  {i}. {row['factor_name']}: {row['ic_ir']:.4f}")
        
    except Exception as e:
        print(f"❌ 单因子分析失败: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        analyzer.close()


if __name__ == '__main__':
    main()