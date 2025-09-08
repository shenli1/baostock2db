#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复版多因子策略
使用修复版数据库管理器，避免事务和性能问题
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
import argparse
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from factor_derivation.factor_generation_fixed import FactorGeneratorFixed
from database.manager_fixed import DatabaseManagerFixed

logger = logging.getLogger(__name__)


class MultiFactorStrategyFixed:
    """修复版多因子策略"""
    
    def __init__(self, start_date: str, end_date: str, rebalance_freq: int = 20, 
                 top_n: int = 60, min_score: float = 0.5):
        """
        初始化多因子策略
        
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
        
        # 策略参数
        self.factor_weights = {
            'momentum_1m': 0.15,
            'momentum_3m': 0.10,
            'reversal_5d': 0.10,
            'volatility_20d': -0.10,  # 负权重，偏好低波动
            'pe_ratio': -0.15,  # 负权重，偏好低估值
            'pb_ratio': -0.10,
            'ps_ratio': -0.10,
            'roe': 0.15,
            'net_profit_margin': 0.10,
            'current_ratio': 0.05,
            'quality_score': 0.20,
            'value_score': 0.15,
            'growth_score': 0.10
        }
        
        logger.info(f"多因子策略初始化完成: {start_date} 到 {end_date}")
    
    def run_complete_pipeline(self):
        """运行完整的多因子策略流程"""
        try:
            logger.info("开始运行完整的多因子策略流程")
            
            # 1. 生成因子数据
            logger.info("步骤1: 生成因子数据")
            df_factors = self.generate_factors()
            
            # 2. 因子预处理
            logger.info("步骤2: 因子预处理")
            df_processed = self.preprocess_factors(df_factors)
            
            # 3. 单因子测试
            logger.info("步骤3: 单因子测试")
            factor_ic_results = self.single_factor_test(df_processed)
            
            # 4. 多因子组合
            logger.info("步骤4: 多因子组合")
            df_combined = self.combine_factors(df_processed)
            
            # 5. 策略回测
            logger.info("步骤5: 策略回测")
            backtest_results = self.backtest_strategy(df_combined)
            
            # 6. 生成报告
            logger.info("步骤6: 生成策略报告")
            self.generate_strategy_report(factor_ic_results, backtest_results)
            
            logger.info("多因子策略流程完成")
            
        except Exception as e:
            logger.error(f"多因子策略运行失败: {str(e)}")
            raise
        finally:
            self.close()
    
    def generate_factors(self) -> pd.DataFrame:
        """生成因子数据"""
        logger.info("开始生成因子数据")
        
        # 获取基础因子数据
        df = self.factor_generator.get_base_factor_data(self.start_date, self.end_date)
        
        # 生成技术因子
        df = self.factor_generator.generate_technical_factors_optimized(df)
        
        # 生成基本面因子
        df = self.factor_generator.generate_fundamental_factors_optimized(df)
        
        # 生成横截面因子
        df = self.factor_generator.generate_cross_sectional_factors_optimized(df)
        
        # 保存因子数据
        self.factor_generator.save_factors_to_database_safe(df, 'stock_factors_strategy')
        
        logger.info(f"因子数据生成完成: {len(df)} 条记录")
        return df
    
    def preprocess_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """因子预处理"""
        logger.info("开始因子预处理")
        
        df_processed = df.copy()
        
        # 1. 处理缺失值
        logger.info("处理缺失值")
        for col in df_processed.columns:
            if col not in ['code', 'date', 'industry', 'code_name']:
                if df_processed[col].dtype in ['int64', 'float64']:
                    # 数值型因子用中位数填充
                    df_processed[col] = df_processed[col].fillna(df_processed[col].median())
        
        # 2. 异常值处理（Winsorization）
        logger.info("异常值处理")
        for col in df_processed.columns:
            if col not in ['code', 'date', 'industry', 'code_name'] and df_processed[col].dtype in ['int64', 'float64']:
                # 1%和99%分位数Winsorization
                lower_bound = df_processed[col].quantile(0.01)
                upper_bound = df_processed[col].quantile(0.99)
                df_processed[col] = df_processed[col].clip(lower=lower_bound, upper=upper_bound)
        
        # 3. 标准化处理
        logger.info("标准化处理")
        for col in df_processed.columns:
            if col not in ['code', 'date', 'industry', 'code_name'] and df_processed[col].dtype in ['int64', 'float64']:
                # Z-score标准化
                mean_val = df_processed[col].mean()
                std_val = df_processed[col].std()
                if std_val > 0:
                    df_processed[col] = (df_processed[col] - mean_val) / std_val
        
        logger.info("因子预处理完成")
        return df_processed
    
    def single_factor_test(self, df: pd.DataFrame) -> Dict[str, float]:
        """单因子测试"""
        logger.info("开始单因子测试")
        
        # 计算未来收益率
        df_test = df.copy()
        df_test = df_test.sort_values(['code', 'date']).reset_index(drop=True)
        
        # 使用更安全的方式计算未来收益率
        future_returns_1d = []
        future_returns_5d = []
        future_returns_20d = []
        
        for code, group in df_test.groupby('code'):
            group = group.sort_values('date').reset_index(drop=True)
            
            # 计算1日未来收益率
            future_1d = group['pctChg'].shift(-1)
            future_returns_1d.extend(future_1d.tolist())
            
            # 计算5日未来收益率
            future_5d = group['pctChg'].rolling(5).sum().shift(-5)
            future_returns_5d.extend(future_5d.tolist())
            
            # 计算20日未来收益率
            future_20d = group['pctChg'].rolling(20).sum().shift(-20)
            future_returns_20d.extend(future_20d.tolist())
        
        df_test['future_return_1d'] = future_returns_1d
        df_test['future_return_5d'] = future_returns_5d
        df_test['future_return_20d'] = future_returns_20d
        
        # 计算IC值
        ic_results = {}
        factor_columns = [col for col in df_test.columns if col not in ['code', 'date', 'industry', 'code_name', 'pctChg', 'future_return_1d', 'future_return_5d', 'future_return_20d']]
        
        for factor in factor_columns:
            if factor in df_test.columns:
                # 计算与未来1日收益率的IC
                ic_1d = df_test[factor].corr(df_test['future_return_1d'])
                ic_5d = df_test[factor].corr(df_test['future_return_5d'])
                ic_20d = df_test[factor].corr(df_test['future_return_20d'])
                
                ic_results[factor] = {
                    'ic_1d': ic_1d if not pd.isna(ic_1d) else 0,
                    'ic_5d': ic_5d if not pd.isna(ic_5d) else 0,
                    'ic_20d': ic_20d if not pd.isna(ic_20d) else 0,
                    'avg_ic': np.mean([ic_1d, ic_5d, ic_20d]) if not any(pd.isna([ic_1d, ic_5d, ic_20d])) else 0
                }
        
        # 保存IC结果
        ic_df = pd.DataFrame(ic_results).T
        ic_df.to_csv('factor_ic_results.csv')
        
        logger.info(f"单因子测试完成，测试了 {len(ic_results)} 个因子")
        return ic_results
    
    def combine_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """多因子组合"""
        logger.info("开始多因子组合")
        
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
        
        # 筛选股票
        df_combined['selected'] = (
            (df_combined['factor_rank'] >= (1 - self.top_n / df_combined['total_stocks'])) &
            (df_combined['factor_score'] >= self.min_score)
        )
        
        logger.info("多因子组合完成")
        return df_combined
    
    def backtest_strategy(self, df: pd.DataFrame) -> Dict[str, Any]:
        """策略回测"""
        logger.info("开始策略回测")
        
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
            
            # 计算持仓权重（等权重）
            weight = 1.0 / len(current_positions)
            
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
        
        logger.info("策略回测完成")
        return backtest_results
    
    def calculate_max_drawdown(self, returns: List[float]) -> float:
        """计算最大回撤"""
        cumulative = np.cumprod([1 + r for r in returns])
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        return np.min(drawdown)
    
    def generate_strategy_report(self, factor_ic_results: Dict, backtest_results: Dict):
        """生成策略报告"""
        logger.info("生成策略报告")
        
        report = f"""
# 多因子策略报告

## 策略参数
- 回测期间: {self.start_date} 到 {self.end_date}
- 调仓频率: {self.rebalance_freq} 个交易日
- 选股数量: {self.top_n}
- 最小因子得分: {self.min_score}

## 因子权重配置
"""
        
        for factor, weight in self.factor_weights.items():
            report += f"- {factor}: {weight:.2f}\n"
        
        report += f"""
## 单因子测试结果
测试了 {len(factor_ic_results)} 个因子，主要因子IC表现：

"""
        
        # 按平均IC排序
        sorted_factors = sorted(factor_ic_results.items(), key=lambda x: abs(x[1]['avg_ic']), reverse=True)
        for factor, ic_data in sorted_factors[:10]:
            report += f"- {factor}: IC_1d={ic_data['ic_1d']:.3f}, IC_5d={ic_data['ic_5d']:.3f}, IC_20d={ic_data['ic_20d']:.3f}, 平均IC={ic_data['avg_ic']:.3f}\n"
        
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
        
        # 保存报告
        with open('multi_factor_strategy_report_fixed.md', 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(report)
        logger.info("策略报告生成完成")
    
    def close(self):
        """关闭连接"""
        self.factor_generator.close()
        self.db_manager.close()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='修复版多因子策略')
    parser.add_argument('--start-date', default='2020-06-01', help='开始日期')
    parser.add_argument('--end-date', default='2021-12-31', help='结束日期')
    parser.add_argument('--rebalance-freq', type=int, default=20, help='调仓频率')
    parser.add_argument('--top-n', type=int, default=60, help='选股数量')
    parser.add_argument('--min-score', type=float, default=0.5, help='最小因子得分')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建并运行策略
    strategy = MultiFactorStrategyFixed(
        start_date=args.start_date,
        end_date=args.end_date,
        rebalance_freq=args.rebalance_freq,
        top_n=args.top_n,
        min_score=args.min_score
    )
    
    try:
        strategy.run_complete_pipeline()
        print("\n🎉 修复版多因子策略运行成功！")
        
    except Exception as e:
        print(f"\n❌ 策略运行失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
