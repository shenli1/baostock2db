-- DWD层基础因子表
-- 以stock_kline为主表，关联所有DWD财务数据和行业分类数据

CREATE TABLE IF NOT EXISTS dwd_stock_base_factor (
    -- 主键和基础信息
    code VARCHAR(20) NOT NULL COMMENT '股票代码',
    date DATE NOT NULL COMMENT '日期',
    
    -- K线数据 (来自stock_kline)
    frequency VARCHAR(10) COMMENT '数据频率',
    open DECIMAL(10,4) COMMENT '开盘价',
    high DECIMAL(10,4) COMMENT '最高价',
    low DECIMAL(10,4) COMMENT '最低价',
    close DECIMAL(10,4) COMMENT '收盘价',
    preclose DECIMAL(10,4) COMMENT '前收盘价',
    volume BIGINT COMMENT '成交量',
    amount DECIMAL(20,2) COMMENT '成交额',
    adjustflag VARCHAR(10) COMMENT '复权类型',
    turn DECIMAL(15,6) COMMENT '换手率',
    tradestatus VARCHAR(10) COMMENT '交易状态',
    pctChg DECIMAL(15,6) COMMENT '涨跌幅',
    peTTM DECIMAL(15,6) COMMENT '市盈率TTM',
    pbMRQ DECIMAL(15,6) COMMENT '市净率MRQ',
    psTTM DECIMAL(15,6) COMMENT '市销率TTM',
    pcfNcfTTM DECIMAL(15,6) COMMENT '市现率TTM',
    isST VARCHAR(10) COMMENT '是否ST',
    
    -- 行业分类数据 (来自stock_industry)
    code_name VARCHAR(100) COMMENT '股票名称',
    industry VARCHAR(200) COMMENT '行业',
    industryClassification VARCHAR(100) COMMENT '行业分类标准',
    
    -- 利润表数据 (来自dwd_stock_profit)
    roeAvg DECIMAL(10,6) COMMENT 'ROE平均值',
    npMargin DECIMAL(10,6) COMMENT '净利润率',
    gpMargin DECIMAL(10,6) COMMENT '毛利率',
    netProfit DECIMAL(20,2) COMMENT '净利润',
    epsTTM DECIMAL(10,6) COMMENT '每股收益TTM',
    MBRevenue DECIMAL(20,2) COMMENT '主营业务收入',
    totalShare DECIMAL(20,2) COMMENT '总股本',
    liqaShare DECIMAL(20,2) COMMENT '流通股本',
    profit_pubDate DATE COMMENT '利润表发布日期',
    profit_statDate DATE COMMENT '利润表统计日期',
    
    -- 资产负债表数据 (来自dwd_stock_balance)
    currentRatio DECIMAL(10,6) COMMENT '流动比率',
    quickRatio DECIMAL(10,6) COMMENT '速动比率',
    cashRatio DECIMAL(10,6) COMMENT '现金比率',
    YOYLiability DECIMAL(10,6) COMMENT '负债同比增长率',
    liabilityToAsset DECIMAL(10,6) COMMENT '资产负债率',
    assetToEquity DECIMAL(10,6) COMMENT '权益乘数',
    balance_pubDate DATE COMMENT '资产负债表发布日期',
    balance_statDate DATE COMMENT '资产负债表统计日期',
    
    -- 现金流量表数据 (来自dwd_stock_cashflow)
    CAToAsset DECIMAL(10,6) COMMENT '流动资产占总资产比例',
    NCAToAsset DECIMAL(10,6) COMMENT '非流动资产占总资产比例',
    tangibleAssetToAsset DECIMAL(10,6) COMMENT '有形资产占总资产比例',
    ebitToInterest DECIMAL(10,6) COMMENT '息税前利润/利息费用',
    CFOToOR DECIMAL(10,6) COMMENT '经营现金流/营业收入',
    CFOToNP DECIMAL(10,6) COMMENT '经营现金流/净利润',
    CFOToGr DECIMAL(10,6) COMMENT '经营现金流/毛利润',
    cashflow_pubDate DATE COMMENT '现金流量表发布日期',
    cashflow_statDate DATE COMMENT '现金流量表统计日期',
    
    -- 运营能力数据 (来自dwd_stock_operation)
    NRTurnRatio DECIMAL(10,6) COMMENT '应收账款周转率',
    NRTurnDays DECIMAL(10,6) COMMENT '应收账款周转天数',
    INVTurnRatio DECIMAL(10,6) COMMENT '存货周转率',
    INVTurnDays DECIMAL(10,6) COMMENT '存货周转天数',
    CATurnRatio DECIMAL(10,6) COMMENT '流动资产周转率',
    AssetTurnRatio DECIMAL(10,6) COMMENT '总资产周转率',
    operation_pubDate DATE COMMENT '运营能力数据发布日期',
    operation_statDate DATE COMMENT '运营能力数据统计日期',
    
    -- 成长能力数据 (来自dwd_stock_growth)
    YOYEquity DECIMAL(10,6) COMMENT '净资产同比增长率',
    YOYAsset DECIMAL(10,6) COMMENT '总资产同比增长率',
    YOYNI DECIMAL(10,6) COMMENT '净利润同比增长率',
    YOYEPSBasic DECIMAL(10,6) COMMENT '基本每股收益同比增长率',
    YOYPNI DECIMAL(10,6) COMMENT '归属母公司净利润同比增长率',
    growth_pubDate DATE COMMENT '成长能力数据发布日期',
    growth_statDate DATE COMMENT '成长能力数据统计日期',
    
    -- 杜邦分析数据 (来自dwd_stock_dupont)
    dupontROE DECIMAL(10,6) COMMENT '杜邦ROE',
    dupontAssetStoEquity DECIMAL(10,6) COMMENT '权益乘数',
    dupontAssetTurn DECIMAL(10,6) COMMENT '总资产周转率',
    dupontPnitoni DECIMAL(10,6) COMMENT '净利润/营业总收入',
    dupontNitogr DECIMAL(10,6) COMMENT '营业总收入/总资产',
    dupontTaxBurden DECIMAL(10,6) COMMENT '税收负担',
    dupontIntburden DECIMAL(10,6) COMMENT '利息负担',
    dupontEbittogr DECIMAL(10,6) COMMENT '息税前利润/营业总收入',
    dupont_pubDate DATE COMMENT '杜邦分析数据发布日期',
    dupont_statDate DATE COMMENT '杜邦分析数据统计日期',
    
    -- 系统字段
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    PRIMARY KEY (code, date),
    INDEX idx_code (code),
    INDEX idx_date (date),
    INDEX idx_industry (industry),
    INDEX idx_pe (peTTM),
    INDEX idx_pb (pbMRQ),
    INDEX idx_roe (roeAvg)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='DWD层基础因子表';
