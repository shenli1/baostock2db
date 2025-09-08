-- DWD层财务数据表结构
-- 将财务数据按股票代码和日期进行截面化处理

-- DWD层利润表数据
CREATE TABLE IF NOT EXISTS dwd_stock_profit (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    date DATE NOT NULL COMMENT '日期',
    roeAvg DECIMAL(10,6) COMMENT 'ROE平均值',
    npMargin DECIMAL(10,6) COMMENT '净利润率',
    gpMargin DECIMAL(10,6) COMMENT '毛利率',
    netProfit DECIMAL(20,2) COMMENT '净利润',
    epsTTM DECIMAL(10,6) COMMENT '每股收益TTM',
    MBRevenue DECIMAL(20,2) COMMENT '主营业务收入',
    totalShare DECIMAL(20,2) COMMENT '总股本',
    liqaShare DECIMAL(20,2) COMMENT '流通股本',
    pubDate DATE COMMENT '原始发布日期',
    statDate DATE COMMENT '原始统计日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, date),
    INDEX idx_code (code),
    INDEX idx_date (date),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='DWD层利润表数据';

-- DWD层资产负债表数据
CREATE TABLE IF NOT EXISTS dwd_stock_balance (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    date DATE NOT NULL COMMENT '日期',
    currentRatio DECIMAL(10,6) COMMENT '流动比率',
    quickRatio DECIMAL(10,6) COMMENT '速动比率',
    cashRatio DECIMAL(10,6) COMMENT '现金比率',
    YOYLiability DECIMAL(10,6) COMMENT '负债同比增长率',
    liabilityToAsset DECIMAL(10,6) COMMENT '资产负债率',
    assetToEquity DECIMAL(10,6) COMMENT '权益乘数',
    pubDate DATE COMMENT '原始发布日期',
    statDate DATE COMMENT '原始统计日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, date),
    INDEX idx_code (code),
    INDEX idx_date (date),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='DWD层资产负债表数据';

-- DWD层现金流量表数据
CREATE TABLE IF NOT EXISTS dwd_stock_cashflow (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    date DATE NOT NULL COMMENT '日期',
    CAToAsset DECIMAL(10,6) COMMENT '流动资产占总资产比例',
    NCAToAsset DECIMAL(10,6) COMMENT '非流动资产占总资产比例',
    tangibleAssetToAsset DECIMAL(10,6) COMMENT '有形资产占总资产比例',
    ebitToInterest DECIMAL(10,6) COMMENT '息税前利润/利息费用',
    CFOToOR DECIMAL(10,6) COMMENT '经营现金流/营业收入',
    CFOToNP DECIMAL(10,6) COMMENT '经营现金流/净利润',
    CFOToGr DECIMAL(10,6) COMMENT '经营现金流/毛利润',
    pubDate DATE COMMENT '原始发布日期',
    statDate DATE COMMENT '原始统计日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, date),
    INDEX idx_code (code),
    INDEX idx_date (date),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='DWD层现金流量表数据';

-- DWD层运营能力数据
CREATE TABLE IF NOT EXISTS dwd_stock_operation (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    date DATE NOT NULL COMMENT '日期',
    NRTurnRatio DECIMAL(10,6) COMMENT '应收账款周转率',
    NRTurnDays DECIMAL(10,6) COMMENT '应收账款周转天数',
    INVTurnRatio DECIMAL(10,6) COMMENT '存货周转率',
    INVTurnDays DECIMAL(10,6) COMMENT '存货周转天数',
    CATurnRatio DECIMAL(10,6) COMMENT '流动资产周转率',
    AssetTurnRatio DECIMAL(10,6) COMMENT '总资产周转率',
    pubDate DATE COMMENT '原始发布日期',
    statDate DATE COMMENT '原始统计日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, date),
    INDEX idx_code (code),
    INDEX idx_date (date),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='DWD层运营能力数据';

-- DWD层成长能力数据
CREATE TABLE IF NOT EXISTS dwd_stock_growth (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    date DATE NOT NULL COMMENT '日期',
    YOYEquity DECIMAL(10,6) COMMENT '净资产同比增长率',
    YOYAsset DECIMAL(10,6) COMMENT '总资产同比增长率',
    YOYNI DECIMAL(10,6) COMMENT '净利润同比增长率',
    YOYEPSBasic DECIMAL(10,6) COMMENT '基本每股收益同比增长率',
    YOYPNI DECIMAL(10,6) COMMENT '归属母公司净利润同比增长率',
    pubDate DATE COMMENT '原始发布日期',
    statDate DATE COMMENT '原始统计日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, date),
    INDEX idx_code (code),
    INDEX idx_date (date),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='DWD层成长能力数据';

-- DWD层杜邦分析数据
CREATE TABLE IF NOT EXISTS dwd_stock_dupont (
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    date DATE NOT NULL COMMENT '日期',
    dupontROE DECIMAL(10,6) COMMENT '杜邦ROE',
    dupontAssetStoEquity DECIMAL(10,6) COMMENT '权益乘数',
    dupontAssetTurn DECIMAL(10,6) COMMENT '总资产周转率',
    dupontPnitoni DECIMAL(10,6) COMMENT '净利润/营业总收入',
    dupontNitogr DECIMAL(10,6) COMMENT '营业总收入/总资产',
    dupontTaxBurden DECIMAL(10,6) COMMENT '税收负担',
    dupontIntburden DECIMAL(10,6) COMMENT '利息负担',
    dupontEbittogr DECIMAL(10,6) COMMENT '息税前利润/营业总收入',
    pubDate DATE COMMENT '原始发布日期',
    statDate DATE COMMENT '原始统计日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (code, date),
    INDEX idx_code (code),
    INDEX idx_date (date),
    INDEX idx_pub_date (pubDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='DWD层杜邦分析数据';
