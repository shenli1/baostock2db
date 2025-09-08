# BaoStock2DB 使用示例

## 快速开始

### 1. 初始化数据库
```bash
python main.py init
```

### 2. 更新股票基本信息
```bash
# 更新所有股票基本信息
python main.py update-stocks

# 更新沪深300股票基本信息
python main.py update-stocks --index-type hs300

# 更新上证50股票基本信息
python main.py update-stocks --index-type sz50

# 更新中证500股票基本信息
python main.py update-stocks --index-type zz500
```

### 3. 更新K线数据
```bash
# 全量更新沪深300股票K线数据（2023年全年）
python main.py update-kline --index-type hs300 --start-date 2023-01-01 --end-date 2023-12-31

# 增量更新沪深300股票K线数据（只获取最新数据）
python main.py update-kline --index-type hs300 --incremental

# 更新周线数据
python main.py update-kline --index-type hs300 --frequency w --start-date 2023-01-01 --end-date 2023-12-31

# 更新月线数据
python main.py update-kline --index-type hs300 --frequency m --start-date 2023-01-01 --end-date 2023-12-31
```

### 4. 更新财务数据
```bash
# 更新2023年第四季度财务数据
python main.py update-financial --index-type hs300 --year 2023 --quarter 4

# 只更新盈利能力数据
python main.py update-financial --index-type hs300 --year 2023 --quarter 4 --data-types profit

# 更新多种财务数据类型
python main.py update-financial --index-type hs300 --year 2023 --quarter 4 --data-types profit,operation,growth
```

### 5. 更新业绩数据
```bash
# 更新2023年业绩数据
python main.py update-performance --index-type hs300 --start-date 2023-01-01 --end-date 2023-12-31

# 只更新业绩快报数据
python main.py update-performance --index-type hs300 --data-types express
```

### 6. 更新行业分类数据
```bash
python main.py update-industry --index-type hs300
```

### 7. 更新宏观经济数据
```bash
# 更新2023年宏观经济数据
python main.py update-macro --start-date 2023-01-01 --end-date 2023-12-31

# 只更新存款利率数据
python main.py update-macro --data-types deposit_rate
```

### 8. 更新交易日历
```bash
python main.py update-trade-dates --start-date 2023-01-01 --end-date 2023-12-31
```

### 9. 更新复权因子数据
```bash
python main.py update-adjust-factor --index-type hs300 --start-date 2023-01-01 --end-date 2023-12-31
```

### 10. 更新除权除息数据
```bash
python main.py update-dividend --index-type hs300 --year 2023
```

### 11. 一键更新所有数据
```bash
# 更新沪深300所有数据
python main.py update-all --index-type hs300

# 只更新指定类型的数据
python main.py update-all --index-type hs300 --data-types kline,financial,performance
```

### 12. 查看数据库状态
```bash
python main.py status
```

## 实际使用场景

### 场景1：每日数据更新
```bash
# 每日增量更新K线数据
python main.py update-kline --index-type hs300 --incremental

# 更新交易日历
python main.py update-trade-dates --start-date $(date -d "yesterday" +%Y-%m-%d) --end-date $(date +%Y-%m-%d)
```

### 场景2：季度财务数据更新
```bash
# 更新最新季度财务数据
python main.py update-financial --index-type hs300 --year 2024 --quarter 1
```

### 场景3：历史数据补全
```bash
# 补全2020-2023年的历史K线数据
python main.py update-kline --index-type hs300 --start-date 2020-01-01 --end-date 2023-12-31

# 补全历史财务数据
python main.py update-financial --index-type hs300 --year 2020 --quarter 1
python main.py update-financial --index-type hs300 --year 2020 --quarter 2
python main.py update-financial --index-type hs300 --year 2020 --quarter 3
python main.py update-financial --index-type hs300 --year 2020 --quarter 4
```

### 场景4：多指数数据更新
```bash
# 更新多个指数的数据
python main.py update-all --index-type sz50
python main.py update-all --index-type hs300
python main.py update-all --index-type zz500
```

## 程序化使用示例

### Python脚本示例
```python
from batch_processor import BatchProcessor
from datetime import datetime, timedelta

# 创建批量处理器
with BatchProcessor() as processor:
    # 获取股票列表
    stock_codes = processor.process_stock_list('hs300')
    print(f"获取到 {len(stock_codes)} 只股票")
    
    # 更新K线数据
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    stats = processor.process_kline_data(
        stock_codes=stock_codes[:10],  # 只处理前10只股票
        start_date=start_date,
        end_date=end_date,
        incremental=True,
        max_workers=4
    )
    
    print(f"K线数据更新完成: {stats}")
    
    # 更新财务数据
    year = datetime.now().strftime('%Y')
    quarter = str((datetime.now().month - 1) // 3 + 1)
    
    stats = processor.process_financial_data(
        stock_codes=stock_codes[:10],
        year=year,
        quarter=quarter,
        data_types=['profit', 'operation'],
        max_workers=4
    )
    
    print(f"财务数据更新完成: {stats}")
```

### 定时任务示例
```bash
#!/bin/bash
# 每日数据更新脚本

# 设置日期
TODAY=$(date +%Y-%m-%d)
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

# 更新股票基本信息
python main.py update-stocks --index-type hs300

# 增量更新K线数据
python main.py update-kline --index-type hs300 --incremental

# 更新交易日历
python main.py update-trade-dates --start-date $YESTERDAY --end-date $TODAY

# 更新宏观经济数据（每月1号执行）
if [ $(date +%d) = "01" ]; then
    python main.py update-macro --start-date $YESTERDAY --end-date $TODAY
fi

echo "数据更新完成: $TODAY"
```

## 性能优化建议

### 1. 并发设置
```bash
# 根据网络状况调整并发数
python main.py update-kline --index-type hs300 --max-workers 8
```

### 2. 分批处理
```bash
# 对于大量数据，可以分批处理
python main.py update-kline --index-type all --start-date 2020-01-01 --end-date 2020-12-31
python main.py update-kline --index-type all --start-date 2021-01-01 --end-date 2021-12-31
python main.py update-kline --index-type all --start-date 2022-01-01 --end-date 2022-12-31
```

### 3. 增量更新
```bash
# 优先使用增量更新
python main.py update-kline --index-type hs300 --incremental
```

## 错误处理

### 1. 查看日志
```bash
tail -f baostock2db.log
```

### 2. 重试失败的操作
```bash
# 如果某些股票数据获取失败，可以重新运行
python main.py update-kline --index-type hs300 --incremental
```

### 3. 检查数据完整性
```bash
python main.py status
```

## 数据查询示例

### MySQL查询示例
```sql
-- 查询股票基本信息
SELECT * FROM stock_basic WHERE code_name LIKE '%银行%';

-- 查询K线数据
SELECT * FROM stock_kline 
WHERE code = 'sh.600000' 
AND date >= '2023-01-01' 
ORDER BY date DESC;

-- 查询财务数据
SELECT * FROM stock_profit 
WHERE code = 'sh.600000' 
AND statDate >= '2023-01-01'
ORDER BY statDate DESC;

-- 统计各表数据量
SELECT 
    'stock_basic' as table_name, COUNT(*) as count FROM stock_basic
UNION ALL
SELECT 'stock_kline', COUNT(*) FROM stock_kline
UNION ALL
SELECT 'stock_profit', COUNT(*) FROM stock_profit;
```

## 注意事项

1. **数据更新频率**: BaoStock的数据通常在交易日17:30后更新
2. **网络稳定性**: 建议在网络稳定的环境下运行
3. **数据库备份**: 定期备份重要数据
4. **监控日志**: 定期检查日志文件，及时处理错误
5. **资源使用**: 根据服务器性能调整并发数和批处理大小
