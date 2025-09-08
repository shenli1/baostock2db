# BaoStock2DB

一个基于BaoStock的股票数据获取和存储工具，支持将BaoStock的股票数据批量导入到MySQL数据库中。

## 功能特性

- 🚀 **批量数据获取**: 支持批量获取历史数据和增量获取当日数据
- 🔄 **自动去重**: 智能识别并防止数据重复
- 📊 **多数据类型**: 支持K线数据、财务数据、业绩数据、行业分类、宏观经济数据等
- 🎯 **成分股支持**: 支持按不同成分股类型（上证50、沪深300、中证500）批量更新
- ⚡ **并发处理**: 支持多线程并发处理，提高数据获取效率
- 🛡️ **错误处理**: 完善的错误处理和重试机制
- 📝 **详细日志**: 完整的操作日志记录

## 安装要求

- Python 3.7+
- MySQL 5.7+
- BaoStock 0.8.9+

## 安装步骤

1. 克隆项目
```bash
git clone <repository-url>
cd akshare2db
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 配置数据库
确保MySQL服务运行在 `localhost:3306`，用户名和密码为 `root/root`，数据库名为 `baostock`。

4. 初始化数据库
```bash
python main.py init
```

## 使用方法

### 命令行接口

#### 1. 初始化数据库
```bash
python main.py init
```

#### 2. 更新股票列表
```bash
# 更新所有股票
python main.py update-stocks

# 更新上证50股票
python main.py update-stocks --index-type sz50

# 更新沪深300股票
python main.py update-stocks --index-type hs300

# 更新中证500股票
python main.py update-stocks --index-type zz500
```

#### 3. 更新K线数据
```bash
# 全量更新K线数据
python main.py update-kline --index-type hs300 --start-date 2020-01-01 --end-date 2023-12-31

# 增量更新K线数据
python main.py update-kline --index-type hs300 --incremental

# 更新不同频率的K线数据
python main.py update-kline --index-type hs300 --frequency w  # 周线
python main.py update-kline --index-type hs300 --frequency m  # 月线
```

#### 4. 更新财务数据
```bash
# 更新2023年第四季度财务数据
python main.py update-financial --index-type hs300 --year 2023 --quarter 4

# 只更新盈利能力数据
python main.py update-financial --index-type hs300 --year 2023 --quarter 4 --data-types profit
```

#### 5. 更新业绩数据
```bash
# 更新业绩快报和预告数据
python main.py update-performance --index-type hs300 --start-date 2023-01-01 --end-date 2023-12-31

# 只更新业绩快报数据
python main.py update-performance --index-type hs300 --data-types express
```

#### 6. 更新行业分类数据
```bash
python main.py update-industry --index-type hs300
```

#### 7. 更新宏观经济数据
```bash
# 更新所有宏观经济数据
python main.py update-macro --start-date 2020-01-01 --end-date 2023-12-31

# 只更新存款利率数据
python main.py update-macro --data-types deposit_rate
```

#### 8. 更新交易日历
```bash
python main.py update-trade-dates --start-date 2020-01-01 --end-date 2023-12-31
```

#### 9. 更新复权因子数据
```bash
python main.py update-adjust-factor --index-type hs300 --start-date 2020-01-01 --end-date 2023-12-31
```

#### 10. 更新除权除息数据
```bash
python main.py update-dividend --index-type hs300 --year 2023
```

#### 11. 一键更新所有数据
```bash
# 更新所有数据
python main.py update-all --index-type hs300

# 只更新指定类型的数据
python main.py update-all --index-type hs300 --data-types kline,financial,performance
```

#### 12. 查看数据库状态
```bash
python main.py status
```

### 程序化使用

```python
from batch_processor import BatchProcessor

# 创建批量处理器
with BatchProcessor() as processor:
    # 获取股票列表
    stock_codes = processor.process_stock_list('hs300')
    
    # 更新K线数据
    stats = processor.process_kline_data(
        stock_codes=stock_codes,
        start_date='2023-01-01',
        end_date='2023-12-31',
        incremental=False
    )
    
    print(f"更新了 {stats['total_records']} 条K线数据")
```

## 数据库表结构

### 股票数据表
- `stock_basic`: 股票基本信息
- `stock_kline`: K线数据
- `stock_profit`: 盈利能力数据
- `stock_operation`: 营运能力数据
- `stock_growth`: 成长能力数据
- `stock_balance`: 偿债能力数据
- `stock_cashflow`: 现金流量数据
- `stock_dupont`: 杜邦指标数据
- `stock_performance`: 业绩快报数据
- `stock_forecast`: 业绩预告数据
- `stock_industry`: 行业分类数据
- `stock_adjust_factor`: 复权因子数据
- `stock_dividend`: 除权除息数据

### 宏观经济数据表
- `macro_deposit_rate`: 存款利率数据
- `macro_loan_rate`: 贷款利率数据
- `macro_reserve_ratio`: 存款准备金率数据
- `macro_money_supply`: 货币供应量数据

### 其他数据表
- `trade_dates`: 交易日历数据

## 配置说明

### 数据库配置
在 `config.py` 中修改数据库连接配置：
```python
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'baostock',
    'charset': 'utf8mb4'
}
```

### 数据获取配置
```python
DATA_CONFIG = {
    'batch_size': 100,  # 批量处理大小
    'max_workers': 4,   # 最大并发数
    'chunk_size': 1000, # 数据块大小
    'default_start_date': '1990-01-01',  # 默认开始日期
}
```

## 注意事项

1. **数据频率**: BaoStock的数据更新有时间限制，建议在数据更新后使用
2. **并发控制**: 建议根据网络状况调整并发数，避免请求过于频繁
3. **增量更新**: 使用增量更新模式可以避免重复获取已有数据
4. **错误处理**: 程序会自动重试失败的请求，但建议定期检查日志
5. **数据完整性**: 建议定期检查数据完整性，确保数据质量

## 日志文件

程序运行日志保存在 `baostock2db.log` 文件中，包含详细的执行信息和错误记录。

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 联系方式

如有问题，请通过GitHub Issues联系。
