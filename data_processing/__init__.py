"""
数据加工模块

负责将原始数据加工成标准化的数据仓库层(DWD)数据，包括：
- 财务数据截面化处理
- 基础因子表构建
- 数据质量检查
"""

from .dwd_processor import DWDProcessor
from .base_factor_processor import BaseFactorProcessor

__all__ = ['DWDProcessor', 'BaseFactorProcessor']
