"""
多因子策略模块

负责构建和回测多因子投资策略，包括：
- 因子预处理
- 因子组合
- 策略回测
- 绩效分析
"""

from .multi_factor_strategy_fixed import MultiFactorStrategyFixed
from .optimized_multi_factor_strategy import OptimizedMultiFactorStrategy

__all__ = ['MultiFactorStrategyFixed', 'OptimizedMultiFactorStrategy']
