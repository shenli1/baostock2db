#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础因子表处理器
以stock_kline为主表，关联所有DWD财务数据和行业分类数据
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from database.manager import DatabaseManager
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class BaseFactorProcessor:
    """基础因子表处理器"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
        # DWD表映射
        self.dwd_tables = {
            'profit': 'dwd_stock_profit',
            'balance': 'dwd_stock_balance',
            'cashflow': 'dwd_stock_cashflow',
            'operation': 'dwd_stock_operation',
            'growth': 'dwd_stock_growth',
            'dupont': 'dwd_stock_dupont'
        }
    
    def create_base_factor_table(self):
        """创建基础因子表"""
        try:
            with self.db_manager.engine.connect() as conn:
                # 读取schema文件
                with open('database/base_factor_schema.sql', 'r', encoding='utf-8') as f:
                    schema_sql = f.read()
                
                # 执行SQL创建表
                for statement in schema_sql.split(';'):
                    statement = statement.strip()
                    if statement:
                        conn.execute(text(statement))
                
                conn.commit()
                logger.info("基础因子表创建完成")
                
        except Exception as e:
            logger.error(f"创建基础因子表失败: {str(e)}")
            raise
    
    def populate_base_factor_data(self, start_date: str = '2020-06-01', end_date: str = None):
        """
        填充基础因子数据
        
        Args:
            start_date: 开始日期，默认为2020-06-01
            end_date: 结束日期，默认为今天
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 确保开始日期不早于2020-06-01
        if start_date < '2020-06-01':
            start_date = '2020-06-01'
            logger.info(f"开始日期已调整为: {start_date}")
        
        logger.info(f"开始填充基础因子数据: {start_date} 到 {end_date}")
        
        try:
            with self.db_manager.engine.connect() as conn:
                # 构建复杂的关联SQL
                sql = f"""
                INSERT INTO dwd_stock_base_factor (
                    -- 主键和基础信息
                    code, date,
                    
                    -- K线数据
                    frequency, open, high, low, close, preclose, volume, amount,
                    adjustflag, turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST,
                    
                    -- 行业分类数据
                    code_name, industry, industryClassification,
                    
                    -- 利润表数据
                    roeAvg, npMargin, gpMargin, netProfit, epsTTM, MBRevenue, totalShare, liqaShare,
                    profit_pubDate, profit_statDate,
                    
                    -- 资产负债表数据
                    currentRatio, quickRatio, cashRatio, YOYLiability, liabilityToAsset, assetToEquity,
                    balance_pubDate, balance_statDate,
                    
                    -- 现金流量表数据
                    CAToAsset, NCAToAsset, tangibleAssetToAsset, ebitToInterest, CFOToOR, CFOToNP, CFOToGr,
                    cashflow_pubDate, cashflow_statDate,
                    
                    -- 运营能力数据
                    NRTurnRatio, NRTurnDays, INVTurnRatio, INVTurnDays, CATurnRatio, AssetTurnRatio,
                    operation_pubDate, operation_statDate,
                    
                    -- 成长能力数据
                    YOYEquity, YOYAsset, YOYNI, YOYEPSBasic, YOYPNI,
                    growth_pubDate, growth_statDate,
                    
                    -- 杜邦分析数据
                    dupontROE, dupontAssetStoEquity, dupontAssetTurn, dupontPnitoni, dupontNitogr,
                    dupontTaxBurden, dupontIntburden, dupontEbittogr,
                    dupont_pubDate, dupont_statDate
                )
                SELECT 
                    -- 主键和基础信息
                    k.code, k.date,
                    
                    -- K线数据
                    k.frequency, k.open, k.high, k.low, k.close, k.preclose, k.volume, k.amount,
                    k.adjustflag, k.turn, k.tradestatus, k.pctChg, k.peTTM, k.pbMRQ, k.psTTM, k.pcfNcfTTM, k.isST,
                    
                    -- 行业分类数据
                    i.code_name, i.industry, i.industryClassification,
                    
                    -- 利润表数据
                    p.roeAvg, p.npMargin, p.gpMargin, p.netProfit, p.epsTTM, p.MBRevenue, p.totalShare, p.liqaShare,
                    p.pubDate, p.statDate,
                    
                    -- 资产负债表数据
                    b.currentRatio, b.quickRatio, b.cashRatio, b.YOYLiability, b.liabilityToAsset, b.assetToEquity,
                    b.pubDate, b.statDate,
                    
                    -- 现金流量表数据
                    c.CAToAsset, c.NCAToAsset, c.tangibleAssetToAsset, c.ebitToInterest, c.CFOToOR, c.CFOToNP, c.CFOToGr,
                    c.pubDate, c.statDate,
                    
                    -- 运营能力数据
                    o.NRTurnRatio, o.NRTurnDays, o.INVTurnRatio, o.INVTurnDays, o.CATurnRatio, o.AssetTurnRatio,
                    o.pubDate, o.statDate,
                    
                    -- 成长能力数据
                    g.YOYEquity, g.YOYAsset, g.YOYNI, g.YOYEPSBasic, g.YOYPNI,
                    g.pubDate, g.statDate,
                    
                    -- 杜邦分析数据
                    d.dupontROE, d.dupontAssetStoEquity, d.dupontAssetTurn, d.dupontPnitoni, d.dupontNitogr,
                    d.dupontTaxBurden, d.dupontIntburden, d.dupontEbittogr,
                    d.pubDate, d.statDate
                    
                FROM stock_kline k
                LEFT JOIN stock_industry i ON k.code = i.code
                LEFT JOIN dwd_stock_profit p ON k.code = p.code AND k.date = p.date
                LEFT JOIN dwd_stock_balance b ON k.code = b.code AND k.date = b.date
                LEFT JOIN dwd_stock_cashflow c ON k.code = c.code AND k.date = c.date
                LEFT JOIN dwd_stock_operation o ON k.code = o.code AND k.date = o.date
                LEFT JOIN dwd_stock_growth g ON k.code = g.code AND k.date = g.date
                LEFT JOIN dwd_stock_dupont d ON k.code = d.code AND k.date = d.date
                WHERE k.date BETWEEN :start_date AND :end_date
                ON DUPLICATE KEY UPDATE
                    -- K线数据更新
                    frequency = VALUES(frequency), open = VALUES(open), high = VALUES(high), low = VALUES(low),
                    close = VALUES(close), preclose = VALUES(preclose), volume = VALUES(volume), amount = VALUES(amount),
                    adjustflag = VALUES(adjustflag), turn = VALUES(turn), tradestatus = VALUES(tradestatus),
                    pctChg = VALUES(pctChg), peTTM = VALUES(peTTM), pbMRQ = VALUES(pbMRQ), psTTM = VALUES(psTTM),
                    pcfNcfTTM = VALUES(pcfNcfTTM), isST = VALUES(isST),
                    
                    -- 行业分类数据更新
                    code_name = VALUES(code_name), industry = VALUES(industry), industryClassification = VALUES(industryClassification),
                    
                    -- 财务数据更新
                    roeAvg = VALUES(roeAvg), npMargin = VALUES(npMargin), gpMargin = VALUES(gpMargin),
                    netProfit = VALUES(netProfit), epsTTM = VALUES(epsTTM), MBRevenue = VALUES(MBRevenue),
                    totalShare = VALUES(totalShare), liqaShare = VALUES(liqaShare),
                    profit_pubDate = VALUES(profit_pubDate), profit_statDate = VALUES(profit_statDate),
                    
                    currentRatio = VALUES(currentRatio), quickRatio = VALUES(quickRatio), cashRatio = VALUES(cashRatio),
                    YOYLiability = VALUES(YOYLiability), liabilityToAsset = VALUES(liabilityToAsset), assetToEquity = VALUES(assetToEquity),
                    balance_pubDate = VALUES(balance_pubDate), balance_statDate = VALUES(balance_statDate),
                    
                    CAToAsset = VALUES(CAToAsset), NCAToAsset = VALUES(NCAToAsset), tangibleAssetToAsset = VALUES(tangibleAssetToAsset),
                    ebitToInterest = VALUES(ebitToInterest), CFOToOR = VALUES(CFOToOR), CFOToNP = VALUES(CFOToNP), CFOToGr = VALUES(CFOToGr),
                    cashflow_pubDate = VALUES(cashflow_pubDate), cashflow_statDate = VALUES(cashflow_statDate),
                    
                    NRTurnRatio = VALUES(NRTurnRatio), NRTurnDays = VALUES(NRTurnDays), INVTurnRatio = VALUES(INVTurnRatio),
                    INVTurnDays = VALUES(INVTurnDays), CATurnRatio = VALUES(CATurnRatio), AssetTurnRatio = VALUES(AssetTurnRatio),
                    operation_pubDate = VALUES(operation_pubDate), operation_statDate = VALUES(operation_statDate),
                    
                    YOYEquity = VALUES(YOYEquity), YOYAsset = VALUES(YOYAsset), YOYNI = VALUES(YOYNI),
                    YOYEPSBasic = VALUES(YOYEPSBasic), YOYPNI = VALUES(YOYPNI),
                    growth_pubDate = VALUES(growth_pubDate), growth_statDate = VALUES(growth_statDate),
                    
                    dupontROE = VALUES(dupontROE), dupontAssetStoEquity = VALUES(dupontAssetStoEquity), dupontAssetTurn = VALUES(dupontAssetTurn),
                    dupontPnitoni = VALUES(dupontPnitoni), dupontNitogr = VALUES(dupontNitogr), dupontTaxBurden = VALUES(dupontTaxBurden),
                    dupontIntburden = VALUES(dupontIntburden), dupontEbittogr = VALUES(dupontEbittogr),
                    dupont_pubDate = VALUES(dupont_pubDate), dupont_statDate = VALUES(dupont_statDate),
                    
                    updated_at = CURRENT_TIMESTAMP
                """
                
                # 执行SQL
                result = conn.execute(text(sql), {
                    'start_date': start_date,
                    'end_date': end_date
                })
                
                conn.commit()
                
                # 获取插入的记录数
                count_result = conn.execute(text("SELECT COUNT(*) FROM dwd_stock_base_factor"))
                count = count_result.fetchone()[0]
                
                logger.info(f"成功填充基础因子数据: {count} 条记录")
                
        except Exception as e:
            logger.error(f"填充基础因子数据失败: {str(e)}")
            raise
    
    def get_base_factor_summary(self) -> Dict[str, Any]:
        """
        获取基础因子数据汇总
        
        Returns:
            数据汇总信息
        """
        summary = {}
        
        with self.db_manager.engine.connect() as conn:
            # 总记录数
            result = conn.execute(text("SELECT COUNT(*) FROM dwd_stock_base_factor"))
            summary['total_records'] = result.fetchone()[0]
            
            # 股票数量
            result = conn.execute(text("SELECT COUNT(DISTINCT code) FROM dwd_stock_base_factor"))
            summary['stock_count'] = result.fetchone()[0]
            
            # 日期范围
            result = conn.execute(text("SELECT MIN(date), MAX(date) FROM dwd_stock_base_factor"))
            min_date, max_date = result.fetchone()
            summary['date_range'] = {'min': min_date, 'max': max_date}
            
            # 各财务数据表的覆盖率
            for table_type, table_name in self.dwd_tables.items():
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM dwd_stock_base_factor 
                    WHERE {table_type}_pubDate IS NOT NULL
                """))
                count = result.fetchone()[0]
                summary[f'{table_type}_coverage'] = count
            
            # 行业覆盖率
            result = conn.execute(text("SELECT COUNT(*) FROM dwd_stock_base_factor WHERE industry IS NOT NULL"))
            summary['industry_coverage'] = result.fetchone()[0]
        
        return summary


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='基础因子表处理器')
    parser.add_argument('--start-date', default='2020-06-01', help='开始日期')
    parser.add_argument('--end-date', help='结束日期')
    parser.add_argument('--create-table', action='store_true', help='创建表')
    parser.add_argument('--populate', action='store_true', help='填充数据')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = BaseFactorProcessor()
    
    if args.create_table:
        processor.create_base_factor_table()
    
    if args.populate:
        processor.populate_base_factor_data(args.start_date, args.end_date)
    
    # 显示汇总信息
    summary = processor.get_base_factor_summary()
    print("\n📊 基础因子数据汇总:")
    print(f"  总记录数: {summary['total_records']:,}")
    print(f"  股票数量: {summary['stock_count']}")
    print(f"  日期范围: {summary['date_range']['min']} 到 {summary['date_range']['max']}")
    print(f"  行业覆盖率: {summary['industry_coverage']:,}")
    
    for table_type in ['profit', 'balance', 'cashflow', 'operation', 'growth', 'dupont']:
        coverage = summary.get(f'{table_type}_coverage', 0)
        print(f"  {table_type}数据覆盖率: {coverage:,}")


if __name__ == '__main__':
    main()
