#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优化版多因子策略
基于IC分析结果优化因子权重和策略参数
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from factor_derivation.factor_generation_fixed import FactorGeneratorFixed
from database.manager_fixed import DatabaseManagerFixed

logger = logging.getLogger(__name__)


class OptimizedMultiFactorStrategy:
    """优化版多因子策略"""
    
    def __init__(self, start_date: str, end_date: str, rebalance_freq: int = 10, 
                 top_n: int = 50, min_score: float = 0.0):
        """
        初始化优化版多因子策略
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            rebalance_freq: 调仓频率（交易日）
            top_n: 选股数量
            min_score: 最小因子得分
        """
        self.start_date = start_date
        self.end_date = end_date
        self.rebalance_freq = rebalance_freq
        self.top_n = top_n
        self.min_score = min_score
        
        # 初始化组件
        self.factor_generator = FactorGeneratorFixed()
        self.db_manager = DatabaseManagerFixed()
        
        # 基于IC分析结果优化的因子权重
        self.factor_weights = {
            # 高IC因子，正权重
            'pb_ratio_rank': 0.25,      # IC=0.069
            'pe_ratio_rank': 0.20,      # IC=0.065
            'momentum_3m_rank': 0.15,   # IC=0.060
            'volatility_20d_rank': 0.10, # IC=0.047
            
            # 低估值因子，负权重（偏好低估值）
            'pe_ratio': -0.15,
            'pb_ratio': -0.10,
            'ps_ratio': -0.05,
            
            # 质量因子
            'roe': 0.10,
            'quality_score': 0.10
        }
        
        logger.info(f"优化版多因子策略初始化完成: {start_date} 到 {end_date}")
    
    def run_optimized_strategy(self):
        """运行优化版策略"""
        try:
            logger.info("开始运行优化版多因子策略")
            
            # 1. 获取基础因子数据
            logger.info("步骤1: 获取基础因子数据")
            df = self.factor_generator.get_base_factor_data(self.start_date, self.end_date)
            
            # 2. 生成优化因子
            logger.info("步骤2: 生成优化因子")
            df = self.generate_optimized_factors(df)
            
            # 3. 因子预处理
            logger.info("步骤3: 因子预处理")
            df = self.preprocess_optimized_factors(df)
            
            # 4. 多因子组合
            logger.info("步骤4: 多因子组合")
            df = self.combine_optimized_factors(df)
            
            # 5. 优化回测
            logger.info("步骤5: 优化回测")
            backtest_results = self.optimized_backtest(df)
            
            # 6. 生成报告
            logger.info("步骤6: 生成策略报告")
            self.generate_optimized_report(backtest_results)
            
            logger.info("优化版多因子策略完成")
            
        except Exception as e:
            logger.error(f"优化版多因子策略运行失败: {str(e)}")
            raise
        finally:
            self.close()
    
    def generate_optimized_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成优化因子"""
        logger.info("开始生成优化因子")
        
        df_factors = df.copy()
        
        # 按股票代码分组计算技术因子
        for code, group in df_factors.groupby('code'):
            group = group.sort_values('date').copy()
            
            # 计算动量因子
            group['momentum_1m'] = group['close'].pct_change(20)
            group['momentum_3m'] = group['close'].pct_change(60)
            group['momentum_6m'] = group['close'].pct_change(120)
            
            # 计算反转因子
            group['reversal_5d'] = -group['close'].pct_change(5)
            group['reversal_10d'] = -group['close'].pct_change(10)
            
            # 计算波动率因子
            group['volatility_20d'] = group['pctChg'].rolling(20).std()
            group['volatility_60d'] = group['pctChg'].rolling(60).std()
            
            # 计算成交量因子
            group['volume_ratio_20d'] = group['volume'] / group['volume'].rolling(20).mean()
            
            # 更新DataFrame
            for col in ['momentum_1m', 'momentum_3m', 'momentum_6m', 'reversal_5d', 'reversal_10d', 
                       'volatility_20d', 'volatility_60d', 'volume_ratio_20d']:
                df_factors.loc[df_factors['code'] == code, col] = group[col]
        
        # 基本面因子
        df_factors['pe_ratio'] = df_factors['peTTM']
        df_factors['pb_ratio'] = df_factors['pbMRQ']
        df_factors['ps_ratio'] = df_factors['psTTM']
        df_factors['roe'] = df_factors['roeAvg']
        
        # 质量因子
        df_factors['quality_score'] = (
            df_factors['roe'].fillna(0) * 0.4 +
            df_factors['npMargin'].fillna(0) * 0.3 +
            df_factors['currentRatio'].fillna(0) * 0.3
        )
        
        # 按日期分组计算横截面排名
        for date, group in df_factors.groupby('date'):
            group = group.copy()
            
            # 计算排名因子
            for col in ['momentum_3m', 'volatility_20d', 'pe_ratio', 'pb_ratio', 'ps_ratio', 'roe', 'quality_score']:
                if col in group.columns:
                    group[f'{col}_rank'] = group[col].rank(pct=True)
            
            # 更新DataFrame
            for col in ['momentum_3m_rank', 'volatility_20d_rank', 'pe_ratio_rank', 'pb_ratio_rank', 
                       'ps_ratio_rank', 'roe_rank', 'quality_score_rank']:
                if col in group.columns:
                    df_factors.loc[df_factors['date'] == date, col] = group[col]
        
        logger.info("优化因子生成完成")
        return df_factors
    
    def preprocess_optimized_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化因子预处理"""
        logger.info("开始优化因子预处理")
        
        df_processed = df.copy()
        
        # 处理缺失值
        factor_columns = ['momentum_3m', 'volatility_20d', 'pe_ratio', 'pb_ratio', 'ps_ratio', 
                         'roe', 'quality_score', 'momentum_3m_rank', 'volatility_20d_rank', 
                         'pe_ratio_rank', 'pb_ratio_rank', 'ps_ratio_rank', 'roe_rank', 'quality_score_rank']
        
        for col in factor_columns:
            if col in df_processed.columns:
                df_processed[col] = df_processed[col].fillna(df_processed[col].median())
        
        # 异常值处理（更温和的Winsorization）
        for col in factor_columns:
            if col in df_processed.columns:
                lower_bound = df_processed[col].quantile(0.05)
                upper_bound = df_processed[col].quantile(0.95)
                df_processed[col] = df_processed[col].clip(lower=lower_bound, upper=upper_bound)
        
        # 标准化处理
        for col in factor_columns:
            if col in df_processed.columns:
                mean_val = df_processed[col].mean()
                std_val = df_processed[col].std()
                if std_val > 0:
                    df_processed[col] = (df_processed[col] - mean_val) / std_val
        
        logger.info("优化因子预处理完成")
        return df_processed
    
    def combine_optimized_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化多因子组合"""
        logger.info("开始优化多因子组合")
        
        df_combined = df.copy()
        df_combined = df_combined.reset_index(drop=True)
        
        # 计算综合因子得分
        df_combined['factor_score'] = 0.0
        
        for factor, weight in self.factor_weights.items():
            if factor in df_combined.columns:
                df_combined['factor_score'] += df_combined[factor] * weight
                logger.info(f"添加因子 {factor}，权重: {weight}")
        
        # 按日期分组，计算排名
        df_combined['factor_rank'] = df_combined.groupby('date')['factor_score'].rank(pct=True)
        
        # 计算每日期望的股票数量
        stock_counts = df_combined.groupby('date')['code'].count()
        df_combined['total_stocks'] = df_combined['date'].map(stock_counts)
        
        # 筛选股票（选择前top_n只股票）
        df_combined['selected'] = (
            (df_combined['factor_rank'] >= (1 - self.top_n / df_combined['total_stocks'])) &
            (df_combined['factor_score'] >= self.min_score)
        )
        
        logger.info("优化多因子组合完成")
        return df_combined
    
    def optimized_backtest(self, df: pd.DataFrame) -> Dict[str, Any]:
        """优化回测"""
        logger.info("开始优化回测")
        
        # 获取调仓日期
        rebalance_dates = df['date'].drop_duplicates().sort_values().reset_index(drop=True)
        rebalance_dates = rebalance_dates[::self.rebalance_freq]
        
        portfolio_returns = []
        portfolio_positions = []
        
        for i, date in enumerate(rebalance_dates):
            if i == 0:
                continue
                
            # 获取当前持仓
            current_positions = df[(df['date'] == date) & (df['selected'] == True)]['code'].tolist()
            
            if len(current_positions) == 0:
                continue
            
            # 计算下一期收益
            next_date = rebalance_dates.iloc[i + 1] if i + 1 < len(rebalance_dates) else df['date'].max()
            
            period_returns = []
            for code in current_positions:
                stock_data = df[(df['code'] == code) & (df['date'] >= date) & (df['date'] <= next_date)]
                if len(stock_data) > 0:
                    period_return = stock_data['pctChg'].sum()
                    period_returns.append(period_return)
            
            if period_returns:
                portfolio_return = np.mean(period_returns)
                portfolio_returns.append(portfolio_return)
                portfolio_positions.append({
                    'date': date,
                    'positions': current_positions,
                    'return': portfolio_return,
                    'num_stocks': len(current_positions)
                })
        
        # 计算策略表现
        if portfolio_returns:
            total_return = np.prod([1 + r for r in portfolio_returns]) - 1
            annual_return = (1 + total_return) ** (252 / len(portfolio_returns)) - 1
            volatility = np.std(portfolio_returns) * np.sqrt(252)
            sharpe_ratio = annual_return / volatility if volatility > 0 else 0
            max_drawdown = self.calculate_max_drawdown(portfolio_returns)
            
            backtest_results = {
                'total_return': total_return,
                'annual_return': annual_return,
                'volatility': volatility,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'num_rebalances': len(portfolio_returns),
                'avg_positions': np.mean([p['num_stocks'] for p in portfolio_positions]),
                'portfolio_returns': portfolio_returns,
                'portfolio_positions': portfolio_positions
            }
        else:
            backtest_results = {
                'total_return': 0,
                'annual_return': 0,
                'volatility': 0,
                'sharpe_ratio': 0,
                'max_drawdown': 0,
                'num_rebalances': 0,
                'avg_positions': 0,
                'portfolio_returns': [],
                'portfolio_positions': []
            }
        
        logger.info("优化回测完成")
        return backtest_results
    
    def calculate_max_drawdown(self, returns: List[float]) -> float:
        """计算最大回撤"""
        if not returns:
            return 0
        
        cumulative = np.cumprod([1 + r for r in returns])
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        return np.min(drawdown)
    
    def generate_optimized_report(self, backtest_results: Dict):
        """生成优化报告"""
        logger.info("生成优化策略报告")
        
        report = f"""
# 优化版多因子策略报告

## 策略参数
- 回测期间: {self.start_date} 到 {self.end_date}
- 调仓频率: {self.rebalance_freq} 个交易日
- 选股数量: {self.top_n}
- 最小因子得分: {self.min_score}

## 优化因子权重配置（基于IC分析）
"""
        
        for factor, weight in self.factor_weights.items():
            report += f"- {factor}: {weight:.2f}\n"
        
        report += f"""
## 策略回测结果
- 总收益率: {backtest_results['total_return']:.2%}
- 年化收益率: {backtest_results['annual_return']:.2%}
- 年化波动率: {backtest_results['volatility']:.2%}
- 夏普比率: {backtest_results['sharpe_ratio']:.3f}
- 最大回撤: {backtest_results['max_drawdown']:.2%}
- 调仓次数: {backtest_results['num_rebalances']}
- 平均持仓数量: {backtest_results['avg_positions']:.1f}

## 策略分析
"""
        
        if backtest_results['sharpe_ratio'] > 1.0:
            report += "✅ 策略表现优秀，夏普比率超过1.0\n"
        elif backtest_results['sharpe_ratio'] > 0.5:
            report += "⚠️ 策略表现一般，夏普比率在0.5-1.0之间\n"
        else:
            report += "❌ 策略表现较差，夏普比率低于0.5\n"
        
        if backtest_results['max_drawdown'] > -0.2:
            report += "✅ 最大回撤控制在20%以内\n"
        else:
            report += "⚠️ 最大回撤较大，需要优化风险控制\n"
        
        # 优化建议
        report += """
## 优化建议
1. 基于IC分析结果调整因子权重
2. 增加调仓频率，提高策略响应速度
3. 优化选股数量，平衡收益和风险
4. 考虑加入行业中性化处理
5. 实施动态权重调整机制
"""
        
        # 保存报告
        with open('optimized_multi_factor_strategy_report.md', 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(report)
        logger.info("优化策略报告生成完成")
    
    def close(self):
        """关闭连接"""
        self.factor_generator.close()
        self.db_manager.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='优化版多因子策略')
    parser.add_argument('--start-date', default='2020-06-01', help='开始日期')
    parser.add_argument('--end-date', default='2020-12-31', help='结束日期')
    parser.add_argument('--rebalance-freq', type=int, default=10, help='调仓频率')
    parser.add_argument('--top-n', type=int, default=50, help='选股数量')
    parser.add_argument('--min-score', type=float, default=0.0, help='最小因子得分')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建并运行策略
    strategy = OptimizedMultiFactorStrategy(
        start_date=args.start_date,
        end_date=args.end_date,
        rebalance_freq=args.rebalance_freq,
        top_n=args.top_n,
        min_score=args.min_score
    )
    
    try:
        strategy.run_optimized_strategy()
        print("\n🎉 优化版多因子策略运行成功！")
        
    except Exception as e:
        print(f"\n❌ 策略运行失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
