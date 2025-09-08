"""
因子衍生模块

负责基于基础数据生成各种因子，包括：
- 技术因子（动量、反转、波动率等）
- 基本面因子（估值、盈利、质量等）
- 横截面因子（排名、标准化等）
"""

from .factor_generation_fixed import FactorGeneratorFixed

__all__ = ['FactorGeneratorFixed']
