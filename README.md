# 多因子量化投资策略系统

基于akshare和baostock的完整多因子量化投资策略系统，包含数据获取、数据加工、因子衍生、多因子策略四个核心模块。

## 系统架构

```
多因子量化投资策略系统
├── 数据获取 (Data Acquisition)
├── 数据加工 (Data Processing) 
├── 因子衍生 (Factor Derivation)
└── 多因子策略 (Multi-Factor Strategy)
```

## 功能特性

### 1. 数据获取模块
- **股票基础信息**: 从akshare获取股票列表、行业分类等
- **K线数据**: 从baostock获取前复权K线数据
- **财务数据**: 获取财务报表、财务指标等
- **指数数据**: 获取HS300等指数成分股数据
- **批量处理**: 支持大规模数据的批量获取和存储

### 2. 数据加工模块
- **DWD层处理**: 将原始财务数据加工成截面数据
- **基础因子表**: 构建包含技术指标和财务指标的基础因子表
- **数据质量**: 自动处理缺失值、异常值和数据类型转换
- **时间对齐**: 确保不同数据源的时间对齐

### 3. 因子衍生模块
- **技术因子**: 动量、反转、波动率、成交量等技术指标
- **基本面因子**: 估值、盈利、质量、成长等财务指标
- **横截面因子**: 排名、标准化、行业中性化等处理
- **因子存储**: 自动创建因子表并存储计算结果

### 4. 多因子策略模块
- **因子组合**: 基于IC分析的多因子权重配置
- **策略回测**: 完整的策略回测框架
- **绩效分析**: 收益率、夏普比率、最大回撤等指标
- **风险控制**: 支持多种风险控制机制

## 项目结构

```
akshare2db/
├── data_acquisition/           # 数据获取模块
│   ├── __init__.py
│   ├── data_fetcher.py        # 数据获取器
│   ├── batch_processor.py     # 批量处理器
│   └── main.py               # 数据获取主程序
├── data_processing/           # 数据加工模块
│   ├── __init__.py
│   ├── dwd_processor.py      # DWD层处理器
│   └── base_factor_processor.py # 基础因子处理器
├── factor_derivation/         # 因子衍生模块
│   ├── __init__.py
│   └── factor_generation_fixed.py # 因子生成器
├── multi_factor_strategy/     # 多因子策略模块
│   ├── __init__.py
│   ├── multi_factor_strategy_fixed.py # 修复版策略
│   └── optimized_multi_factor_strategy.py # 优化版策略
├── database/                  # 数据库相关
│   ├── manager_fixed.py      # 数据库管理器
│   ├── schema.sql           # 原始表结构
│   ├── dwd_schema.sql       # DWD层表结构
│   └── base_factor_schema.sql # 基础因子表结构
├── run_pipeline.py          # 完整流水线入口
├── config.py                # 配置文件
├── requirements.txt         # 依赖包
└── README.md               # 项目说明
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 1. 配置数据库

修改 `config.py` 中的数据库连接信息：

```python
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database',
    'charset': 'utf8mb4'
}
```

### 2. 运行完整流水线

```bash
# 运行完整的多因子策略流水线
python run_pipeline.py --full-pipeline --start-date 2020-06-01 --end-date 2020-12-31

# 使用优化版策略
python run_pipeline.py --full-pipeline --strategy-type optimized --rebalance-freq 10 --top-n 50
```

### 3. 运行单个模块

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

### 4. 单独运行各模块

```bash
# 数据获取模块
cd data_acquisition
python main.py update-all

# 数据加工模块
cd data_processing
python dwd_processor.py
python base_factor_processor.py

# 因子衍生模块
cd factor_derivation
python factor_generation_fixed.py

# 多因子策略模块
cd multi_factor_strategy
python optimized_multi_factor_strategy.py --start-date 2020-06-01 --end-date 2020-12-31
```

## 数据表结构

### 原始数据表
- `stock_basic`: 股票基础信息
- `stock_kline`: K线数据
- `stock_express`: 财务快报数据
- `stock_balance`: 资产负债表数据
- `stock_income`: 利润表数据
- `stock_cashflow`: 现金流量表数据
- `stock_operation`: 运营能力数据
- `stock_growth`: 成长能力数据
- `stock_profit`: 盈利能力数据
- `stock_industry`: 行业分类数据
- `index_stock`: 指数成分股数据

### DWD层数据表
- `dwd_stock_balance`: 截面化资产负债表数据
- `dwd_stock_income`: 截面化利润表数据
- `dwd_stock_cashflow`: 截面化现金流量表数据
- `dwd_stock_operation`: 截面化运营能力数据
- `dwd_stock_growth`: 截面化成长能力数据
- `dwd_stock_profit`: 截面化盈利能力数据

### 因子数据表
- `dwd_stock_base_factor`: 基础因子表
- `stock_factors_technical`: 技术因子表
- `stock_factors_fundamental`: 基本面因子表
- `stock_factors_cross_sectional`: 横截面因子表

## 策略参数说明

- `--start-date`: 回测开始日期
- `--end-date`: 回测结束日期
- `--rebalance-freq`: 调仓频率（交易日）
- `--top-n`: 选股数量
- `--min-score`: 最小因子得分阈值
- `--strategy-type`: 策略类型（fixed/optimized）

## 注意事项

1. 确保MySQL服务已启动
2. 确保数据库用户有足够的权限
3. 首次运行会创建所有数据表
4. 数据获取需要网络连接
5. 建议在非交易时间运行数据获取
6. 因子计算需要足够的历史数据
7. 策略回测结果仅供参考，不构成投资建议

## 许可证

MIT License