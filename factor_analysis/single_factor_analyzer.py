#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单因子分析模块
基于alphalens框架对因子表进行单因子分析
支持动态识别各种结构不同的因子表
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

try:
    import alphalens
    from alphalens import plotting
    from alphalens import performance
    from alphalens import utils
    ALPHALENS_AVAILABLE = True
except ImportError:
    ALPHALENS_AVAILABLE = False
    print("警告: alphalens未安装，请运行 pip install alphalens")

from database.manager_fixed import DatabaseManagerFixed
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class SingleFactorAnalyzer:
    """单因子分析器"""
    
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
    
    def prepare_alphalens_data(self, df: pd.DataFrame, factor_name: str, 
                              periods: List[int] = [1, 5, 10]) -> Tuple[pd.Series, pd.Series]:
        """
        准备alphalens分析所需的数据格式
        
        Args:
            df: 因子数据
            factor_name: 因子名称
            periods: 未来收益率计算周期
            
        Returns:
            (factor_data, prices) 用于alphalens分析的元组
        """
        # 确保日期列没有时区信息
        df = df.copy()
        if df['date'].dt.tz is not None:
            df['date'] = df['date'].dt.tz_localize(None)
        
        # 计算未来收益率
        df_with_returns = self.calculate_future_returns(df, periods)
        
        # 准备因子数据
        factor_data = df_with_returns.set_index(['date', 'code'])[factor_name].dropna()
        
        # 准备价格数据（使用收盘价）
        prices = df_with_returns.set_index(['date', 'code'])['close'].dropna()
        
        # 确保两个Series的索引对齐
        common_index = factor_data.index.intersection(prices.index)
        factor_data = factor_data.loc[common_index]
        prices = prices.loc[common_index]
        
        # 确保索引没有时区信息
        if hasattr(factor_data.index, 'tz') and factor_data.index.tz is not None:
            factor_data.index = factor_data.index.tz_localize(None)
        if hasattr(prices.index, 'tz') and prices.index.tz is not None:
            prices.index = prices.index.tz_localize(None)
        
        logger.info(f"准备因子 {factor_name} 的alphalens数据: {len(factor_data)} 个数据点")
        return factor_data, prices
    
    def save_alphalens_plots(self, plots: Dict[str, Any], factor_name: str, 
                           output_dir: str = "factor_analysis_plots") -> Dict[str, str]:
        """
        保存alphalens图表
        
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
                plt.figure(figsize=(10, 6))
                plots['distribution']
                plt.title(f'{factor_name} - 因子分布')
                plt.tight_layout()
                dist_path = os.path.join(factor_dir, f'{factor_name}_distribution.png')
                plt.savefig(dist_path, dpi=300, bbox_inches='tight')
                plt.close()
                saved_plots['distribution'] = dist_path
                logger.info(f"保存因子分布图: {dist_path}")
            
            # 保存IC时间序列图
            if 'ic_ts' in plots and plots['ic_ts'] is not None:
                plt.figure(figsize=(12, 6))
                plots['ic_ts']
                plt.title(f'{factor_name} - IC时间序列')
                plt.tight_layout()
                ic_path = os.path.join(factor_dir, f'{factor_name}_ic_timeseries.png')
                plt.savefig(ic_path, dpi=300, bbox_inches='tight')
                plt.close()
                saved_plots['ic_timeseries'] = ic_path
                logger.info(f"保存IC时间序列图: {ic_path}")
            
            # 保存分层收益柱状图
            if 'returns_bar' in plots and plots['returns_bar'] is not None:
                plt.figure(figsize=(10, 6))
                plots['returns_bar']
                plt.title(f'{factor_name} - 分层收益柱状图')
                plt.tight_layout()
                bar_path = os.path.join(factor_dir, f'{factor_name}_returns_bar.png')
                plt.savefig(bar_path, dpi=300, bbox_inches='tight')
                plt.close()
                saved_plots['returns_bar'] = bar_path
                logger.info(f"保存分层收益柱状图: {bar_path}")
            
            # 保存分层收益热力图
            if 'returns_heatmap' in plots and plots['returns_heatmap'] is not None:
                plt.figure(figsize=(12, 8))
                plots['returns_heatmap']
                plt.title(f'{factor_name} - 分层收益热力图')
                plt.tight_layout()
                heatmap_path = os.path.join(factor_dir, f'{factor_name}_returns_heatmap.png')
                plt.savefig(heatmap_path, dpi=300, bbox_inches='tight')
                plt.close()
                saved_plots['returns_heatmap'] = heatmap_path
                logger.info(f"保存分层收益热力图: {heatmap_path}")
            
            # 保存累积收益图
            if 'cumulative_returns' in plots and plots['cumulative_returns'] is not None:
                plt.figure(figsize=(12, 6))
                plots['cumulative_returns']
                plt.title(f'{factor_name} - 累积收益')
                plt.tight_layout()
                cum_path = os.path.join(factor_dir, f'{factor_name}_cumulative_returns.png')
                plt.savefig(cum_path, dpi=300, bbox_inches='tight')
                plt.close()
                saved_plots['cumulative_returns'] = cum_path
                logger.info(f"保存累积收益图: {cum_path}")
            
            # 保存因子自相关图
            if 'autocorrelation' in plots and plots['autocorrelation'] is not None:
                plt.figure(figsize=(10, 6))
                plots['autocorrelation']
                plt.title(f'{factor_name} - 因子自相关')
                plt.tight_layout()
                autocorr_path = os.path.join(factor_dir, f'{factor_name}_autocorrelation.png')
                plt.savefig(autocorr_path, dpi=300, bbox_inches='tight')
                plt.close()
                saved_plots['autocorrelation'] = autocorr_path
                logger.info(f"保存因子自相关图: {autocorr_path}")
            
            # 保存收益表格（如果有的话）
            if 'returns_table' in plots and plots['returns_table'] is not None:
                # 将表格保存为CSV
                table_path = os.path.join(factor_dir, f'{factor_name}_returns_table.csv')
                plots['returns_table'].to_csv(table_path, encoding='utf-8-sig')
                saved_plots['returns_table'] = table_path
                logger.info(f"保存收益表格: {table_path}")
            
        except Exception as e:
            logger.error(f"保存因子 {factor_name} 图表时出错: {str(e)}")
        
        return saved_plots
    
    def create_tear_sheet_html(self, factor_data_clean: pd.Series, forward_returns: pd.DataFrame, 
                              factor_name: str, output_dir: str = "factor_analysis_plots") -> str:
        """
        创建HTML格式的因子分析报告
        
        Args:
            factor_data_clean: 清理后的因子数据
            forward_returns: 未来收益率数据
            factor_name: 因子名称
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
            
            # 计算基本统计信息
            ic = alphalens.performance.factor_information_coefficient(factor_data_clean, forward_returns)
            returns, mean_ret_by_q, std_agg_by_q = alphalens.performance.mean_return_by_quantile(
                factor_data_clean, forward_returns, by_group=False
            )
            
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
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>📊 {factor_name} - 因子分析报告</h1>
                    
                    <h2>📈 关键指标</h2>
                    <div class="stats-grid">
                        <div class="stat-card">
                            <div class="stat-value">{ic.mean():.4f}</div>
                            <div class="stat-label">IC均值</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{ic.std():.4f}</div>
                            <div class="stat-label">IC标准差</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{ic.mean() / ic.std() if ic.std() != 0 else 0:.4f}</div>
                            <div class="stat-label">IC信息比率</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{len(factor_data_clean):,}</div>
                            <div class="stat-label">数据点数</div>
                        </div>
                    </div>
                    
                    <h2>📊 分层收益分析</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>分位数</th>
                                <th>平均收益</th>
                                <th>标准差</th>
                                <th>夏普比率</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            # 添加分层收益数据
            for i, (quantile, row) in enumerate(mean_ret_by_q.iterrows()):
                mean_ret = row.iloc[0] if hasattr(row, 'iloc') else row
                std_ret = std_agg_by_q.iloc[i] if i < len(std_agg_by_q) else 0
                sharpe = mean_ret / std_ret if std_ret != 0 else 0
                
                html_content += f"""
                            <tr>
                                <td>Q{quantile + 1}</td>
                                <td class="{'positive' if mean_ret > 0 else 'negative' if mean_ret < 0 else 'neutral'}">{mean_ret:.4f}</td>
                                <td>{std_ret:.4f}</td>
                                <td class="{'positive' if sharpe > 0 else 'negative' if sharpe < 0 else 'neutral'}">{sharpe:.4f}</td>
                            </tr>
                """
            
            html_content += """
                        </tbody>
                    </table>
                    
                    <h2>📈 图表分析</h2>
                    <div class="chart-container">
                        <h3>因子分布图</h3>
                        <img src="{}_distribution.png" alt="因子分布图">
                    </div>
                    
                    <div class="chart-container">
                        <h3>IC时间序列图</h3>
                        <img src="{}_ic_timeseries.png" alt="IC时间序列图">
                    </div>
                    
                    <div class="chart-container">
                        <h3>因子与收益关系图</h3>
                        <img src="{}_scatter.png" alt="因子与收益关系图">
                    </div>
                    
                    <h2>📋 分析总结</h2>
                    <p><strong>因子名称:</strong> {}</p>
                    <p><strong>分析时间:</strong> {}</p>
                    <p><strong>数据期间:</strong> {} 到 {}</p>
                    <p><strong>总数据点:</strong> {:,}</p>
                    
                    <h3>因子有效性评估</h3>
                    <ul>
                        <li><strong>IC信息比率:</strong> {:.4f} - {}
                        <li><strong>分层收益差:</strong> {:.4f}</li>
                        <li><strong>数据质量:</strong> 基于 {} 个有效数据点</li>
                    </ul>
                    
                    <div style="margin-top: 40px; padding: 20px; background-color: #f8f9fa; border-left: 4px solid #3498db;">
                        <h4>📝 报告说明</h4>
                        <p>本报告基于alphalens框架生成，包含因子的完整分析结果。图表文件保存在同一目录下，可以单独查看。</p>
                    </div>
                </div>
            </body>
            </html>
            """.format(
                factor_name, factor_name, factor_name,
                factor_name, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                factor_data_clean.index.get_level_values('date').min().strftime('%Y-%m-%d'),
                factor_data_clean.index.get_level_values('date').max().strftime('%Y-%m-%d'),
                len(factor_data_clean),
                ic.mean() / ic.std() if ic.std() != 0 else 0,
                "表现良好" if (ic.mean() / ic.std() if ic.std() != 0 else 0) > 0.1 else "需要改进" if (ic.mean() / ic.std() if ic.std() != 0 else 0) < 0 else "中性",
                mean_ret_by_q.iloc[-1] - mean_ret_by_q.iloc[0] if len(mean_ret_by_q) > 0 else 0,
                len(factor_data_clean)
            )
            
            # 保存HTML文件
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"创建HTML分析报告: {html_path}")
            return html_path
            
        except Exception as e:
            logger.error(f"创建因子 {factor_name} HTML报告失败: {str(e)}")
            return None
    
    def analyze_single_factor(self, factor_name: str, factor_data: pd.Series, 
                            prices: pd.Series, quantiles: int = 5, 
                            save_plots: bool = True, output_dir: str = "factor_analysis_plots") -> Dict[str, Any]:
        """
        对单个因子进行alphalens分析
        
        Args:
            factor_name: 因子名称
            factor_data: 因子数据
            prices: 价格数据
            quantiles: 分层数量
            save_plots: 是否保存图表
            output_dir: 图表输出目录
            
        Returns:
            分析结果字典
        """
        if not ALPHALENS_AVAILABLE:
            raise ImportError("alphalens未安装，请运行 pip install alphalens")
        
        try:
            # 确保索引没有时区信息
            if hasattr(factor_data.index, 'tz') and factor_data.index.tz is not None:
                factor_data.index = factor_data.index.tz_localize(None)
            if hasattr(prices.index, 'tz') and prices.index.tz is not None:
                prices.index = prices.index.tz_localize(None)
            
            # 确保MultiIndex的每个级别都没有时区信息
            if isinstance(factor_data.index, pd.MultiIndex):
                new_levels = []
                for level in factor_data.index.levels:
                    if hasattr(level, 'tz') and level.tz is not None:
                        new_levels.append(level.tz_localize(None))
                    else:
                        new_levels.append(level)
                factor_data.index = factor_data.index.set_levels(new_levels)
            
            if isinstance(prices.index, pd.MultiIndex):
                new_levels = []
                for level in prices.index.levels:
                    if hasattr(level, 'tz') and level.tz is not None:
                        new_levels.append(level.tz_localize(None))
                    else:
                        new_levels.append(level)
                prices.index = prices.index.set_levels(new_levels)
            
            # 创建alphalens数据
            factor_data_clean, forward_returns = alphalens.utils.get_clean_factor_and_forward_returns(
                factor_data, prices, quantiles=quantiles, periods=(1, 5, 10)
            )
            
            # IC分析
            ic = alphalens.performance.factor_information_coefficient(factor_data_clean, forward_returns)
            
            # 分层回测
            returns, mean_ret_by_q, std_agg_by_q = alphalens.performance.mean_return_by_quantile(
                factor_data_clean, forward_returns, by_group=False
            )
            
            # 因子分布
            factor_dist = alphalens.plotting.plot_distribution(factor_data_clean)
            
            # 分层收益
            factor_returns = alphalens.plotting.plot_returns_table(mean_ret_by_q, returns)
            
            # IC时间序列
            ic_ts = alphalens.plotting.plot_information_coefficient(ic)
            
            # 分层收益图
            factor_returns_plot = alphalens.plotting.plot_quantile_returns_bar(mean_ret_by_q)
            
            # 分层收益热力图
            factor_heatmap = alphalens.plotting.plot_quantile_returns_heatmap(mean_ret_by_q)
            
            # 累积收益
            cumulative_returns = alphalens.plotting.plot_cumulative_returns(returns)
            
            # 因子自相关
            factor_autocorr = alphalens.plotting.plot_factor_rank_autocorrelation(factor_data_clean)
            
            # 计算统计指标
            ic_mean = ic.mean()
            ic_std = ic.std()
            ic_ir = ic_mean / ic_std if ic_std != 0 else 0
            
            # 分层收益统计
            top_quantile_returns = mean_ret_by_q.iloc[-1]  # 最高分层收益
            bottom_quantile_returns = mean_ret_by_q.iloc[0]  # 最低分层收益
            spread = top_quantile_returns - bottom_quantile_returns
            
            # 准备图表数据
            plots_data = {
                'distribution': factor_dist,
                'returns_table': factor_returns,
                'ic_ts': ic_ts,
                'returns_bar': factor_returns_plot,
                'returns_heatmap': factor_heatmap,
                'cumulative_returns': cumulative_returns,
                'autocorrelation': factor_autocorr
            }
            
            # 保存图表（如果启用）
            saved_plots = {}
            html_tear_sheet = None
            if save_plots:
                try:
                    # 保存PNG图表
                    saved_plots = self.save_alphalens_plots(plots_data, factor_name, output_dir)
                    
                    # 创建HTML tear sheet
                    html_tear_sheet = self.create_tear_sheet_html(
                        factor_data_clean, forward_returns, factor_name, output_dir
                    )
                    
                except Exception as e:
                    logger.warning(f"保存因子 {factor_name} 图表失败: {str(e)}")
            
            results = {
                'factor_name': factor_name,
                'ic_mean': ic_mean,
                'ic_std': ic_std,
                'ic_ir': ic_ir,
                'top_quantile_returns': top_quantile_returns,
                'bottom_quantile_returns': bottom_quantile_returns,
                'spread': spread,
                'quantiles': quantiles,
                'data_points': len(factor_data_clean),
                'plots': plots_data,
                'saved_plots': saved_plots,
                'html_tear_sheet': html_tear_sheet
            }
            
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
                'top_quantile_returns': np.nan,
                'bottom_quantile_returns': np.nan,
                'spread': np.nan
            }
    
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
        effective_factors = []
        
        for i, factor_name in enumerate(factor_columns):
            logger.info(f"分析因子 {i+1}/{len(factor_columns)}: {factor_name}")
            
            try:
                # 准备数据（包含未来收益率计算）
                factor_data, prices = self.prepare_alphalens_data(df, factor_name, periods=[1, 5, 10])
                
                if len(factor_data) < 100:  # 数据点太少，跳过
                    logger.warning(f"因子 {factor_name} 数据点太少({len(factor_data)})，跳过")
                    continue
                
                # 分析因子（不保存图表）
                result = self.analyze_single_factor(factor_name, factor_data, prices, quantiles, False, output_dir)
                all_results[factor_name] = result
                
                # 添加到汇总统计
                if 'error' not in result:
                    summary_stats.append({
                        'factor_name': factor_name,
                        'ic_mean': result['ic_mean'],
                        'ic_std': result['ic_std'],
                        'ic_ir': result['ic_ir'],
                        'spread': result['spread'],
                        'data_points': result['data_points']
                    })
                    
                    # 判断是否为有效因子（IC信息比率 > 0.05）
                    if not np.isnan(result['ic_ir']) and result['ic_ir'] > 0.05:
                        effective_factors.append(factor_name)
                
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
                'effective_factors': [],
                'total_factors': len(factor_columns),
                'analyzed_factors': 0,
                'failed_factors': len(factor_columns)
            }
    
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
                                    <td class="{'positive' if result.get('top_quantile_returns', 0) > 0 else 'negative' if result.get('top_quantile_returns', 0) < 0 else 'neutral'}">{result.get('top_quantile_returns', 0):.4f}</td>
                                    <td>因子值最高分位数的平均收益</td>
                                </tr>
                                <tr>
                                    <td>最低分位数收益</td>
                                    <td class="{'positive' if result.get('bottom_quantile_returns', 0) > 0 else 'negative' if result.get('bottom_quantile_returns', 0) < 0 else 'neutral'}">{result.get('bottom_quantile_returns', 0):.4f}</td>
                                    <td>因子值最低分位数的平均收益</td>
                                </tr>
                                <tr>
                                    <td>分层收益差</td>
                                    <td class="{'positive' if result.get('spread', 0) > 0 else 'negative' if result.get('spread', 0) < 0 else 'neutral'}">{result.get('spread', 0):.4f}</td>
                                    <td>最高分位数收益 - 最低分位数收益</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    """
            
            html_content += f"""
                    <div class="info-box">
                        <h4>📝 报告说明</h4>
                        <p>本报告基于alphalens框架生成，包含所有有效因子的完整分析结果。</p>
                        <p><strong>有效因子标准:</strong> IC信息比率 > 0.05</p>
                        <p><strong>评级标准:</strong></p>
                        <ul>
                            <li>优秀: IC信息比率 > 0.2</li>
                            <li>良好: 0.1 < IC信息比率 ≤ 0.2</li>
                            <li>一般: 0.05 < IC信息比率 ≤ 0.1</li>
                            <li>较差: IC信息比率 ≤ 0.05</li>
                        </ul>
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
    
    
    def close(self):
        """关闭数据库连接"""
        self.db_manager.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='单因子分析器')
    parser.add_argument('--start-date', required=True, help='开始日期')
    parser.add_argument('--end-date', required=True, help='结束日期')
    parser.add_argument('--table-name', required=True, help='因子表名')
    parser.add_argument('--quantiles', type=int, default=5, help='分层数量')
    parser.add_argument('--max-factors', type=int, help='最大分析因子数量')
    parser.add_argument('--output-file', help='输出报告文件名')
    parser.add_argument('--save-plots', action='store_true', help='保存alphalens图表')
    parser.add_argument('--output-dir', default='factor_analysis_plots', help='图表输出目录')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    analyzer = SingleFactorAnalyzer()
    
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
        
        print(f"\n📊 单因子分析完成:")
        print(f"  总因子数: {results['total_factors']}")
        print(f"  成功分析: {results['analyzed_factors']}")
        print(f"  分析失败: {results['failed_factors']}")
        print(f"  有效因子数: {len(results.get('effective_factors', []))}")
        
        if not results['summary'].empty:
            print(f"\n🏆 IC信息比率排名前5:")
            for i, (_, row) in enumerate(results['summary'].head(5).iterrows(), 1):
                print(f"  {i}. {row['factor_name']}: {row['ic_ir']:.4f}")
        
        if results.get('effective_factors'):
            print(f"\n✅ 有效因子: {', '.join(results['effective_factors'])}")
            print(f"📄 整合HTML报告已生成")
        
    except Exception as e:
        print(f"❌ 单因子分析失败: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        analyzer.close()


if __name__ == '__main__':
    main()
