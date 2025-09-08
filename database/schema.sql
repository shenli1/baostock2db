-- BaoStock2DB 数据库表结构
-- 创建数据库
CREATE DATABASE IF NOT EXISTS baostock CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE baostock;

-- 股票基本信息表
CREATE TABLE IF NOT EXISTS stock_basic (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    code_name VARCHAR(100) NOT NULL COMMENT '股票名称',
    ipoDate DATE COMMENT '上市日期',
    outDate DATE COMMENT '退市日期',
    type INT COMMENT '股票类型',
    status INT COMMENT '上市状态',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code),
    INDEX idx_code_name (code_name),
    INDEX idx_ipo_date (ipoDate),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票基本信息表';

-- 股票K线数据表
CREATE TABLE IF NOT EXISTS stock_kline (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    date DATE NOT NULL COMMENT '交易日期',
    frequency VARCHAR(10) NOT NULL DEFAULT 'd' COMMENT '数据频率',
    open DECIMAL(10,4) COMMENT '开盘价',
    high DECIMAL(10,4) COMMENT '最高价',
    low DECIMAL(10,4) COMMENT '最低价',
    close DECIMAL(10,4) COMMENT '收盘价',
    preclose DECIMAL(10,4) COMMENT '前收盘价',
    volume BIGINT COMMENT '成交量',
    amount DECIMAL(20,2) COMMENT '成交额',
    adjustflag VARCHAR(10) COMMENT '复权状态',
    turn DECIMAL(10,6) COMMENT '换手率',
    tradestatus VARCHAR(10) COMMENT '交易状态',
    pctChg DECIMAL(10,6) COMMENT '涨跌幅',
    peTTM DECIMAL(10,6) COMMENT '滚动市盈率',
    pbMRQ DECIMAL(10,6) COMMENT '市净率',
    psTTM DECIMAL(10,6) COMMENT '滚动市销率',
    pcfNcfTTM DECIMAL(10,6) COMMENT '滚动市现率',
    isST VARCHAR(10) COMMENT '是否ST',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, date, frequency),
    INDEX idx_code (code),
    INDEX idx_date (date),
    INDEX idx_frequency (frequency),
    INDEX idx_code_date (code, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票K线数据表';

-- 股票盈利能力数据表
CREATE TABLE IF NOT EXISTS stock_profit (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    pubDate DATE COMMENT '发布日期',
    statDate DATE NOT NULL COMMENT '统计日期',
    roeAvg DECIMAL(10,6) COMMENT '净资产收益率(平均)',
    npMargin DECIMAL(10,6) COMMENT '销售净利率',
    gpMargin DECIMAL(10,6) COMMENT '销售毛利率',
    netProfit DECIMAL(20,2) COMMENT '净利润',
    epsTTM DECIMAL(10,6) COMMENT '每股收益',
    MBRevenue DECIMAL(20,2) COMMENT '主营营业收入',
    totalShare DECIMAL(20,2) COMMENT '总股本',
    liqaShare DECIMAL(20,2) COMMENT '流通股本',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, statDate),
    INDEX idx_code (code),
    INDEX idx_stat_date (statDate),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票盈利能力数据表';

-- 股票营运能力数据表
CREATE TABLE IF NOT EXISTS stock_operation (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    pubDate DATE COMMENT '发布日期',
    statDate DATE NOT NULL COMMENT '统计日期',
    NRTurnRatio DECIMAL(10,6) COMMENT '应收账款周转率',
    NRTurnDays DECIMAL(10,6) COMMENT '应收账款周转天数',
    INVTurnRatio DECIMAL(10,6) COMMENT '存货周转率',
    INVTurnDays DECIMAL(10,6) COMMENT '存货周转天数',
    CATurnRatio DECIMAL(10,6) COMMENT '流动资产周转率',
    AssetTurnRatio DECIMAL(10,6) COMMENT '总资产周转率',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, statDate),
    INDEX idx_code (code),
    INDEX idx_stat_date (statDate),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票营运能力数据表';

-- 股票成长能力数据表
CREATE TABLE IF NOT EXISTS stock_growth (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    pubDate DATE COMMENT '发布日期',
    statDate DATE NOT NULL COMMENT '统计日期',
    YOYEquity DECIMAL(10,6) COMMENT '净资产增长率',
    YOYAsset DECIMAL(10,6) COMMENT '总资产增长率',
    YOYNI DECIMAL(10,6) COMMENT '净利润增长率',
    YOYEPSBasic DECIMAL(10,6) COMMENT '基本每股收益增长率',
    YOYPNI DECIMAL(10,6) COMMENT '归属母公司股东净利润增长率',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, statDate),
    INDEX idx_code (code),
    INDEX idx_stat_date (statDate),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票成长能力数据表';

-- 股票偿债能力数据表
CREATE TABLE IF NOT EXISTS stock_balance (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    pubDate DATE COMMENT '发布日期',
    statDate DATE NOT NULL COMMENT '统计日期',
    currentRatio DECIMAL(10,6) COMMENT '流动比率',
    quickRatio DECIMAL(10,6) COMMENT '速动比率',
    cashRatio DECIMAL(10,6) COMMENT '现金比率',
    YOYLiability DECIMAL(10,6) COMMENT '总负债同比增长率',
    liabilityToAsset DECIMAL(10,6) COMMENT '资产负债率',
    assetToEquity DECIMAL(10,6) COMMENT '权益乘数',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, statDate),
    INDEX idx_code (code),
    INDEX idx_stat_date (statDate),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票偿债能力数据表';

-- 股票现金流量数据表
CREATE TABLE IF NOT EXISTS stock_cashflow (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    pubDate DATE COMMENT '发布日期',
    statDate DATE NOT NULL COMMENT '统计日期',
    CAToAsset DECIMAL(10,6) COMMENT '总资产现金回收率',
    NCAToAsset DECIMAL(10,6) COMMENT '非流动资产现金回收率',
    tangibleAssetToAsset DECIMAL(10,6) COMMENT '有形资产占总资产比例',
    ebitToInterest DECIMAL(10,6) COMMENT '息税前利润/利息费用',
    CFOToOR DECIMAL(10,6) COMMENT '经营现金流/营业收入',
    CFOToNP DECIMAL(10,6) COMMENT '经营现金流/净利润',
    CFOToGr DECIMAL(10,6) COMMENT '经营现金流/营业收入',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, statDate),
    INDEX idx_code (code),
    INDEX idx_stat_date (statDate),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票现金流量数据表';

-- 股票杜邦指标数据表
CREATE TABLE IF NOT EXISTS stock_dupont (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    pubDate DATE COMMENT '发布日期',
    statDate DATE NOT NULL COMMENT '统计日期',
    dupontROE DECIMAL(10,6) COMMENT '净资产收益率',
    dupontAssetStoEquity DECIMAL(10,6) COMMENT '权益乘数',
    dupontAssetTurn DECIMAL(10,6) COMMENT '总资产周转率',
    dupontPnitoni DECIMAL(10,6) COMMENT '归属母公司股东的净利润/净利润',
    dupontNitogr DECIMAL(10,6) COMMENT '净利润/营业总收入',
    dupontTaxBurden DECIMAL(10,6) COMMENT '净利润/利润总额',
    dupontIntburden DECIMAL(10,6) COMMENT '利润总额/息税前利润',
    dupontEbittogr DECIMAL(10,6) COMMENT '息税前利润/营业总收入',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, statDate),
    INDEX idx_code (code),
    INDEX idx_stat_date (statDate),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票杜邦指标数据表';

-- 股票业绩快报数据表
CREATE TABLE IF NOT EXISTS stock_performance (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    performanceExpPubDate DATE COMMENT '业绩快报发布日期',
    performanceExpStatDate DATE NOT NULL COMMENT '业绩快报统计日期',
    performanceExpUpdateDate DATE COMMENT '业绩快报更新日期',
    performanceExpressTotalAsset DECIMAL(20,2) COMMENT '总资产',
    performanceExpressNetAsset DECIMAL(20,2) COMMENT '净资产',
    performanceExpressEPSChgPct DECIMAL(10,6) COMMENT '每股收益变化率',
    performanceExpressROEWa DECIMAL(10,6) COMMENT '净资产收益率',
    performanceExpressEPSDiluted DECIMAL(10,6) COMMENT '稀释每股收益',
    performanceExpressGRYOY DECIMAL(10,6) COMMENT '营业收入同比增长率',
    performanceExpressOPYOY DECIMAL(10,6) COMMENT '营业利润同比增长率',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, performanceExpStatDate),
    INDEX idx_code (code),
    INDEX idx_stat_date (performanceExpStatDate),
    INDEX idx_pub_date (performanceExpPubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票业绩快报数据表';

-- 股票业绩快报数据表
CREATE TABLE IF NOT EXISTS stock_express (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    pubDate DATE COMMENT '发布日期',
    statDate DATE NOT NULL COMMENT '统计日期',
    sRevenueYoy DECIMAL(10,6) COMMENT '营业收入同比增长率',
    sProfitYoy DECIMAL(10,6) COMMENT '营业利润同比增长率',
    totalProfitYoy DECIMAL(10,6) COMMENT '利润总额同比增长率',
    nIncomeYoy DECIMAL(10,6) COMMENT '净利润同比增长率',
    totalAssetsYoy DECIMAL(10,6) COMMENT '总资产同比增长率',
    totalEquityYoy DECIMAL(10,6) COMMENT '净资产同比增长率',
    sRevenue DECIMAL(20,2) COMMENT '营业收入',
    sProfit DECIMAL(20,2) COMMENT '营业利润',
    totalProfit DECIMAL(20,2) COMMENT '利润总额',
    nIncome DECIMAL(20,2) COMMENT '净利润',
    totalAssets DECIMAL(20,2) COMMENT '总资产',
    totalEquity DECIMAL(20,2) COMMENT '净资产',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, statDate),
    INDEX idx_code (code),
    INDEX idx_stat_date (statDate),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票业绩快报数据表';

-- 股票业绩预告数据表
CREATE TABLE IF NOT EXISTS stock_forecast (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    profitForcastExpPubDate DATE COMMENT '业绩预告发布日期',
    profitForcastExpStatDate DATE NOT NULL COMMENT '业绩预告统计日期',
    profitForcastType VARCHAR(100) COMMENT '预告类型',
    profitForcastAbstract TEXT COMMENT '预告摘要',
    profitForcastChgPctUp DECIMAL(10,6) COMMENT '预告净利润同比增长率上限',
    profitForcastChgPctDwn DECIMAL(10,6) COMMENT '预告净利润同比增长率下限',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, profitForcastExpStatDate),
    INDEX idx_code (code),
    INDEX idx_stat_date (profitForcastExpStatDate),
    INDEX idx_pub_date (profitForcastExpPubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票业绩预告数据表';

-- 股票行业分类数据表
CREATE TABLE IF NOT EXISTS stock_industry (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    updateDate DATE COMMENT '更新日期',
    code_name VARCHAR(100) COMMENT '股票名称',
    industry VARCHAR(200) COMMENT '所属行业',
    industryClassification VARCHAR(100) COMMENT '行业分类',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code),
    INDEX idx_code_name (code_name),
    INDEX idx_industry (industry),
    INDEX idx_update_date (updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票行业分类数据表';

-- 交易日历表
CREATE TABLE IF NOT EXISTS trade_dates (
    calendar_date DATE NOT NULL COMMENT '日历日期',
    is_trading_day INT COMMENT '是否交易日，1=是，0=否',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (calendar_date),
    INDEX idx_is_trading_day (is_trading_day)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='交易日历表';

-- 存款利率数据表
CREATE TABLE IF NOT EXISTS macro_deposit_rate (
    pubDate DATE NOT NULL COMMENT '发布日期',
    demandDepositRate DECIMAL(10,6) COMMENT '活期存款利率',
    fixedDepositRate3Month DECIMAL(10,6) COMMENT '3个月定期存款利率',
    fixedDepositRate6Month DECIMAL(10,6) COMMENT '6个月定期存款利率',
    fixedDepositRate1Year DECIMAL(10,6) COMMENT '1年定期存款利率',
    fixedDepositRate2Year DECIMAL(10,6) COMMENT '2年定期存款利率',
    fixedDepositRate3Year DECIMAL(10,6) COMMENT '3年定期存款利率',
    fixedDepositRate5Year DECIMAL(10,6) COMMENT '5年定期存款利率',
    installmentFixedDepositRate1Year DECIMAL(10,6) COMMENT '1年零存整取利率',
    installmentFixedDepositRate3Year DECIMAL(10,6) COMMENT '3年零存整取利率',
    installmentFixedDepositRate5Year DECIMAL(10,6) COMMENT '5年零存整取利率',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (pubDate),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='存款利率数据表';

-- 贷款利率数据表
CREATE TABLE IF NOT EXISTS macro_loan_rate (
    pubDate DATE NOT NULL COMMENT '发布日期',
    loanRate6Month DECIMAL(10,6) COMMENT '6个月贷款利率',
    loanRate6MonthTo1Year DECIMAL(10,6) COMMENT '6个月至1年贷款利率',
    loanRate1YearTo3Year DECIMAL(10,6) COMMENT '1年至3年贷款利率',
    loanRate3YearTo5Year DECIMAL(10,6) COMMENT '3年至5年贷款利率',
    loanRateAbove5Year DECIMAL(10,6) COMMENT '5年以上贷款利率',
    mortgateRateBelow5Year DECIMAL(10,6) COMMENT '5年以下房贷利率',
    mortgateRateAbove5Year DECIMAL(10,6) COMMENT '5年以上房贷利率',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (pubDate),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='贷款利率数据表';

-- 存款准备金率数据表
CREATE TABLE IF NOT EXISTS macro_reserve_ratio (
    pubDate DATE NOT NULL COMMENT '发布日期',
    effectiveDate DATE COMMENT '生效日期',
    bigInstitutionsRatioPre DECIMAL(10,6) COMMENT '大型机构调整前准备金率',
    bigInstitutionsRatioAfter DECIMAL(10,6) COMMENT '大型机构调整后准备金率',
    mediumInstitutionsRatioPre DECIMAL(10,6) COMMENT '中型机构调整前准备金率',
    mediumInstitutionsRatioAfter DECIMAL(10,6) COMMENT '中型机构调整后准备金率',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (pubDate),
    INDEX idx_pub_date (pubDate),
    INDEX idx_effective_date (effectiveDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='存款准备金率数据表';

-- 货币供应量数据表
CREATE TABLE IF NOT EXISTS macro_money_supply (
    statYear INT NOT NULL COMMENT '统计年份',
    statMonth INT NOT NULL COMMENT '统计月份',
    m0Month DECIMAL(20,2) COMMENT 'M0货币供应量',
    m0YOY DECIMAL(10,6) COMMENT 'M0同比增长率',
    m0ChainRelative DECIMAL(10,6) COMMENT 'M0环比增长率',
    m1Month DECIMAL(20,2) COMMENT 'M1货币供应量',
    m1YOY DECIMAL(10,6) COMMENT 'M1同比增长率',
    m1ChainRelative DECIMAL(10,6) COMMENT 'M1环比增长率',
    m2Month DECIMAL(20,2) COMMENT 'M2货币供应量',
    m2YOY DECIMAL(10,6) COMMENT 'M2同比增长率',
    m2ChainRelative DECIMAL(10,6) COMMENT 'M2环比增长率',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (statYear, statMonth),
    INDEX idx_stat_year (statYear),
    INDEX idx_stat_month (statMonth)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='货币供应量数据表';

-- 股票复权因子数据表
CREATE TABLE IF NOT EXISTS stock_adjust_factor (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    dividOperateDate DATE NOT NULL COMMENT '除权除息日',
    foreAdjustFactor DECIMAL(10,6) COMMENT '前复权因子',
    backAdjustFactor DECIMAL(10,6) COMMENT '后复权因子',
    adjustFactor DECIMAL(10,6) COMMENT '复权因子',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, dividOperateDate),
    INDEX idx_code (code),
    INDEX idx_divid_date (dividOperateDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票复权因子数据表';

-- 股票除权除息信息表
CREATE TABLE IF NOT EXISTS stock_dividend (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    dividPreNoticeDate DATE COMMENT '分红预告日期',
    dividAgmPumDate DATE COMMENT '股东大会日期',
    dividPlanAnnounceDate DATE COMMENT '分红方案公告日期',
    dividPlanDate DATE COMMENT '分红方案日期',
    dividRegistDate DATE COMMENT '股权登记日',
    dividOperateDate DATE NOT NULL COMMENT '除权除息日',
    dividPayDate DATE COMMENT '派息日',
    dividStockMarketDate DATE COMMENT '股票上市日期',
    dividCashPsBeforeTax DECIMAL(10,6) COMMENT '每股税前派息',
    dividCashPsAfterTax VARCHAR(100) COMMENT '每股税后派息',
    dividStocksPs DECIMAL(10,6) COMMENT '每股送股',
    dividCashStock TEXT COMMENT '分红方案描述',
    dividReserveToStockPs VARCHAR(100) COMMENT '每股转增股',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, dividOperateDate),
    INDEX idx_code (code),
    INDEX idx_divid_date (dividOperateDate),
    INDEX idx_regist_date (dividRegistDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票除权除息信息表';
