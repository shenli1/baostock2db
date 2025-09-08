# 项目结构说明

## 整体架构

本项目采用模块化设计，将多因子量化投资策略系统分为四个核心模块：

```
akshare2db/
├── data_acquisition/           # 数据获取模块
├── data_processing/           # 数据加工模块  
├── factor_derivation/         # 因子衍生模块
├── multi_factor_strategy/     # 多因子策略模块
├── database/                  # 数据库相关
├── run_pipeline.py           # 完整流水线入口
├── config.py                 # 配置文件
└── requirements.txt          # 依赖包
```

## 模块详细说明

### 1. 数据获取模块 (data_acquisition/)

**功能**: 从各种数据源获取原始数据

**文件结构**:
```
data_acquisition/
├── __init__.py              # 模块初始化
├── data_fetcher.py         # 数据获取器
├── batch_processor.py      # 批量处理器
└── main.py                # 数据获取主程序
```

**核心类**:
- `BaoStockDataFetcher`: 负责从baostock获取各种股票数据
- `BatchProcessor`: 负责批量处理和数据存储

**主要功能**:
- 获取股票基础信息
- 获取K线数据（支持前复权）
- 获取财务数据（资产负债表、利润表、现金流量表等）
- 获取行业分类数据
- 获取指数成分股数据
- 批量数据存储到MySQL

### 2. 数据加工模块 (data_processing/)

**功能**: 将原始数据加工成标准化的数据仓库层(DWD)数据

**文件结构**:
```
data_processing/
├── __init__.py                    # 模块初始化
├── dwd_processor.py              # DWD层处理器
└── base_factor_processor.py      # 基础因子处理器
```

**核心类**:
- `DWDProcessor`: 负责将原始财务数据加工成截面数据
- `BaseFactorProcessor`: 负责构建基础因子表

**主要功能**:
- 财务数据截面化处理（将时间序列数据转换为截面数据）
- 构建基础因子表（包含技术指标和财务指标）
- 数据质量检查和清洗
- 时间对齐处理

### 3. 因子衍生模块 (factor_derivation/)

**功能**: 基于基础数据生成各种因子

**文件结构**:
```
factor_derivation/
├── __init__.py                    # 模块初始化
└── factor_generation_fixed.py    # 因子生成器
```

**核心类**:
- `FactorGeneratorFixed`: 负责生成各种类型的因子

**主要功能**:
- 技术因子生成（动量、反转、波动率、成交量等）
- 基本面因子生成（估值、盈利、质量、成长等）
- 横截面因子生成（排名、标准化、行业中性化等）
- 因子存储和管理

### 4. 多因子策略模块 (multi_factor_strategy/)

**功能**: 构建和回测多因子投资策略

**文件结构**:
```
multi_factor_strategy/
├── __init__.py                           # 模块初始化
├── multi_factor_strategy_fixed.py       # 修复版策略
└── optimized_multi_factor_strategy.py   # 优化版策略
```

**核心类**:
- `MultiFactorStrategyFixed`: 基础版多因子策略
- `OptimizedMultiFactorStrategy`: 优化版多因子策略

**主要功能**:
- 因子预处理（缺失值处理、异常值处理、标准化等）
- 因子组合（基于IC分析的多因子权重配置）
- 策略回测（完整的回测框架）
- 绩效分析（收益率、夏普比率、最大回撤等指标）

### 5. 数据库模块 (database/)

**功能**: 数据库连接和表结构管理

**文件结构**:
```
database/
├── manager_fixed.py          # 数据库管理器
├── schema.sql               # 原始表结构
├── dwd_schema.sql          # DWD层表结构
└── base_factor_schema.sql  # 基础因子表结构
```

**核心类**:
- `DatabaseManagerFixed`: 负责数据库连接和操作

**主要功能**:
- 数据库连接管理
- 表结构创建和管理
- 数据插入和更新
- 事务管理

## 数据流程

```
原始数据获取 → 数据加工 → 因子衍生 → 多因子策略
     ↓              ↓          ↓          ↓
  data_acquisition → data_processing → factor_derivation → multi_factor_strategy
```

### 1. 数据获取阶段
- 从akshare和baostock获取原始数据
- 存储到MySQL数据库的原始表中

### 2. 数据加工阶段
- 将原始财务数据加工成截面数据
- 构建包含技术指标和财务指标的基础因子表

### 3. 因子衍生阶段
- 基于基础因子表生成各种因子
- 存储因子数据到专门的因子表中

### 4. 多因子策略阶段
- 组合多个因子构建投资策略
- 进行策略回测和绩效分析

## 使用方式

### 1. 完整流水线运行
```bash
python run_pipeline.py --full-pipeline --start-date 2020-06-01 --end-date 2020-12-31
```

### 2. 分模块运行
```bash
# 只运行数据获取
python run_pipeline.py --data-acquisition --start-date 2020-06-01 --end-date 2020-12-31

# 只运行数据加工
python run_pipeline.py --data-processing

# 只运行因子衍生
python run_pipeline.py --factor-derivation --start-date 2020-06-01 --end-date 2020-12-31

# 只运行多因子策略
python run_pipeline.py --multi-factor-strategy --start-date 2020-06-01 --end-date 2020-12-31
```

### 3. 单独运行各模块
```bash
# 数据获取模块
cd data_acquisition && python main.py update-all

# 数据加工模块
cd data_processing && python dwd_processor.py

# 因子衍生模块
cd factor_derivation && python factor_generation_fixed.py

# 多因子策略模块
cd multi_factor_strategy && python optimized_multi_factor_strategy.py
```

## 扩展性

本项目的模块化设计使得系统具有良好的扩展性：

1. **新增数据源**: 可以在`data_acquisition`模块中添加新的数据获取器
2. **新增因子**: 可以在`factor_derivation`模块中添加新的因子生成逻辑
3. **新增策略**: 可以在`multi_factor_strategy`模块中添加新的策略实现
4. **新增数据加工**: 可以在`data_processing`模块中添加新的数据处理逻辑

## 配置管理

所有配置都集中在`config.py`文件中，包括：
- 数据库连接配置
- 数据获取配置
- 策略参数配置
- 日志配置等

这种设计使得系统配置更加集中和易于管理。
