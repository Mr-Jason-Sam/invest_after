WITH TRADE_DATE_ADJ AS (
    -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- 已处理当天收益的开始日期
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     HOLDING_BOND_CODE AS (
         -- 持仓股票池
         SELECT DISTINCT VC_WIND_CODE
         FROM ZHFX.THOLDINGDETAILBOND HDB
         WHERE HDB.L_FUND_ID IN (${fund_ids})
           AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     BOND_QUOTE AS (SELECT BOND_INFO.L_TRADE_DATE,
                           BOND_INFO.VC_WIND_CODE,
                           DECODE(
                                   BOND_INFO.L_RECOM_CNBD,
                               -- 行权
                                   1, BOND_INFO.EN_FULL_CNBD_IFEXE + BOND_INFO.EN_PRINCIPAL + BOND_INFO.EN_INTEREST,
                               -- 不行权
                                   0, BOND_INFO.EN_FULL_CNBD + BOND_INFO.EN_PRINCIPAL + BOND_INFO.EN_INTEREST,
                                   NULL)                                                                  PRICE,
                           LAG(DECODE(
                                       BOND_INFO.L_RECOM_CNBD,
                                   -- 行权
                                       1, BOND_INFO.EN_FULL_CNBD_IFEXE + BOND_INFO.EN_PRINCIPAL + BOND_INFO.EN_INTEREST,
                                   -- 不行权
                                       0, BOND_INFO.EN_FULL_CNBD + BOND_INFO.EN_PRINCIPAL + BOND_INFO.EN_INTEREST,
                                       NULL), 1, NULL)
                               OVER (PARTITION BY BOND_INFO.VC_WIND_CODE ORDER BY BOND_INFO.L_TRADE_DATE) PRICE_PRE
                    FROM HOLDING_BOND_CODE HBC
                             LEFT JOIN ZHFX.TBONDINFO BOND_INFO ON HBC.VC_WIND_CODE = BOND_INFO.VC_WIND_CODE,
                         TRADE_DATE_ADJ TDA
                    WHERE BOND_INFO.L_TRADE_DATE BETWEEN TDA.BEGIN_DATE_LAST AND ${end_date}),
     HOLDING_BOND_INFO AS (
         -- 持仓债券信息
         SELECT HDB.L_FUND_ID,
                HDB.L_TRADE_DATE,
                HDB.VC_WIND_CODE,
                BDESC.VC_SHORT_NAME,
                BDESC.VC_BOND_TYPE1_WIND,
                BDESC.VC_BOND_TYPE2_WIND,
                HDB.EN_VALUE_MARKET_PRE,
                HDB.EN_VALUE_MARKET,
                -- 收益 = 公允价格变动 + 债券利息 + 投资收益
                HDB.EN_INCREMENT + HDB.EN_INTEREST + HDB.EN_INVEST_INCOME                       PROFIT_VALUE,
                -- 票息收入 = 债券利息
                HDB.EN_INTEREST                                                                 INTEREST_VALUE,
                -- 资本利得 = 公允价格变动 + 投资收益
                HDB.EN_INCREMENT + HDB.EN_INVEST_INCOME                                         CAPITAL_PROFIT_VALUE,
                -- 债券日收益 = (T日全价[行权or不行权] + T日兑付兑息) / T-1日全价
                DECODE(BOND_QUOTE.PRICE_PRE, 0, 0, BOND_QUOTE.PRICE / BOND_QUOTE.PRICE_PRE - 1) DAY_PROFIT
         FROM ZHFX.THOLDINGDETAILBOND HDB
                  LEFT JOIN ZHFX.TBONDDESCRIPTION BDESC
                            ON HDB.VC_WIND_CODE = BDESC.VC_WIND_CODE
                  LEFT JOIN BOND_QUOTE ON HDB.VC_WIND_CODE = BOND_QUOTE.VC_WIND_CODE AND
                                          HDB.L_TRADE_DATE = BOND_QUOTE.L_TRADE_DATE
         WHERE HDB.L_FUND_ID IN (${fund_ids})
           AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     HOLDING_DATE_INFO AS (
         -- 持仓分析
         SELECT TOTAL_HDB.L_FUND_ID,
                TOTAL_HDB.VC_WIND_CODE,
                MAX(TOTAL_HDB.L_TRADE_DATE) MAX_HOLDING_DATE,
                MIN(TOTAL_HDB.L_TRADE_DATE) SETUP_DATE
         FROM HOLDING_BOND_CODE HBC
                  LEFT JOIN ZHFX.THOLDINGDETAILBOND TOTAL_HDB
                            ON HBC.VC_WIND_CODE = TOTAL_HDB.VC_WIND_CODE
         WHERE TOTAL_HDB.L_FUND_ID IN (${fund_ids})
           AND TOTAL_HDB.L_TRADE_DATE <= ${end_date}
         GROUP BY TOTAL_HDB.L_FUND_ID, TOTAL_HDB.VC_WIND_CODE),
     BOND_RANGE_PROFIT_DETAILS AS (
         -- 债券区间收益
         SELECT VC_WIND_CODE,
                EXP(SUM(LN(1 + DAY_PROFIT))) - 1 RANGE_PROFIT
         FROM HOLDING_BOND_INFO
         WHERE L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY VC_WIND_CODE),
     PRODUCT_PROFIT_INFO AS (
         -- 产品信息
         SELECT FUND_AS.*,
                EXP(SUM(LN(1 + FUND_AS.DAY_PROFIT))
                        OVER (PARTITION BY FUND_AS.L_FUND_ID ORDER BY FUND_AS.L_TRADE_DATE)) /
                (1 + FUND_AS.DAY_PROFIT) PROFIT_ADJ
         FROM (SELECT FUND_ASSETS.*,
                      DECODE(FUND_ASSETS.FUND_ASSETS_NET_PRE, 0, 0,
                             FUND_ASSETS.FUND_ASSETS_NET / FUND_ASSETS.FUND_ASSETS_NET_PRE - 1) DAY_PROFIT
               FROM (SELECT L_FUND_ID,
                            L_TRADE_DATE,
                            EN_FUND_ASSET_NET                                             FUND_ASSETS_NET,
                            -- 昨日净资产
                            EN_FUND_ASSET_NET_PRE +
                                -- 买入项(数据库中为负数，则为累加项)
                            EN_APPLY_BAL + EN_APPLY_DIRECT + EN_APPEND_BAL +
                                -- 卖出项
                            EN_REDEEM_BAL + EN_REDEEM_DIRECT + EN_EXTRACT_BAL +
                                -- 其他：分红、分红再投、业绩报酬
                            EN_FUND_DIVIDEND + EN_FUND_DIVIDEND_INVEST + EN_INCOME_REWARD FUND_ASSETS_NET_PRE
                     FROM ZHFX.TFUNDRETURNDETAIL
                     WHERE L_FUND_ID IN (${fund_ids})
                       AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}) FUND_ASSETS) FUND_AS),
     HOLDING_DETAILS AS (
         -- 持仓贡献详情
         SELECT HDB.L_FUND_ID                                                                            FUND_ID,
                HDB.VC_WIND_CODE                                                                         BOND_CODE,
                MAX(HDB.VC_SHORT_NAME)                                                                   BOND_NAME,
                MAX(HDB.VC_BOND_TYPE1_WIND)                                                              WIND_1,
                MAX(HDB.VC_BOND_TYPE2_WIND)                                                              WIND_2,
                -- 收益 = 子项累加
                SUM(HDB.PROFIT_VALUE)                                                                    PROFIT_MKT,
                SUM(HDB.INTEREST_VALUE)                                                                  INTEREST_PROFIT_MKT,
                SUM(HDB.CAPITAL_PROFIT_VALUE)                                                            CAPITAL_PROFIT_MKT,
                -- 贡献 = SUM(个券贡献 / T-1组合市值 * 组合收益调整项)
                -- 对产品
                SUM(DECODE(PPI.FUND_ASSETS_NET_PRE, 0, 0,
                           HDB.PROFIT_VALUE / PPI.FUND_ASSETS_NET_PRE * PPI.PROFIT_ADJ))                 PROFIT_CTB_PD,
                SUM(DECODE(PPI.FUND_ASSETS_NET_PRE, 0, 0, HDB.INTEREST_VALUE / PPI.FUND_ASSETS_NET_PRE *
                                                          PPI.PROFIT_ADJ))                               INTEREST_PROFIT_CTB_PD,
                SUM(DECODE(PPI.FUND_ASSETS_NET_PRE, 0, 0, HDB.CAPITAL_PROFIT_VALUE / PPI.FUND_ASSETS_NET_PRE *
                                                          PPI.PROFIT_ADJ))                               CAPITAL_PROFIT_CTB_PD,
                -- 对债券组合
                SUM(DECODE(BOND_MKT_INFO.TOTAL_BOND_MKT_PRE, 0, 0,
                           HDB.PROFIT_VALUE / BOND_MKT_INFO.TOTAL_BOND_MKT_PRE * PPI.PROFIT_ADJ))        PROFIT_CTB_BD,
                SUM(DECODE(BOND_MKT_INFO.TOTAL_BOND_MKT_PRE, 0, 0,
                           HDB.INTEREST_VALUE / BOND_MKT_INFO.TOTAL_BOND_MKT_PRE *
                           PPI.PROFIT_ADJ))                                                              INTEREST_PROFIT_CTB_BD,
                SUM(DECODE(BOND_MKT_INFO.TOTAL_BOND_MKT_PRE, 0, 0,
                           HDB.CAPITAL_PROFIT_VALUE / BOND_MKT_INFO.TOTAL_BOND_MKT_PRE *
                           PPI.PROFIT_ADJ))                                                              CAPITAL_PROFIT_CTB_BD,
                -- 持有收益率：持有期间，债券本身收益率
                EXP(SUM(LN(1 + DECODE(HDB.EN_VALUE_MARKET_PRE, 0, 0, NULL, 0, 1) * HDB.DAY_PROFIT))) - 1 HOLDING_PROFIT,
                -- 报告期收益率
                MAX(BRPD.RANGE_PROFIT) AS                                                                RANGE_PROFIT,
                MAX(DECODE(PPI.FUND_ASSETS_NET, 0, 0, HDB.EN_VALUE_MARKET / PPI.FUND_ASSETS_NET))        MAX_POSITION,
                MIN(DECODE(PPI.FUND_ASSETS_NET, 0, 0, HDB.EN_VALUE_MARKET / PPI.FUND_ASSETS_NET))        MIN_POSITION,
                AVG(DECODE(PPI.FUND_ASSETS_NET, 0, 0, HDB.EN_VALUE_MARKET / PPI.FUND_ASSETS_NET))        AVG_POSITION
         FROM HOLDING_BOND_INFO HDB
                  LEFT JOIN PRODUCT_PROFIT_INFO PPI
                            ON HDB.L_FUND_ID = PPI.L_FUND_ID AND HDB.L_TRADE_DATE = PPI.L_TRADE_DATE
                  LEFT JOIN (SELECT L_FUND_ID, L_TRADE_DATE, SUM(EN_VALUE_MARKET_PRE) TOTAL_BOND_MKT_PRE
                             FROM HOLDING_BOND_INFO
                             GROUP BY L_FUND_ID, L_TRADE_DATE) BOND_MKT_INFO
                            ON HDB.L_FUND_ID = BOND_MKT_INFO.L_FUND_ID AND HDB.L_TRADE_DATE = BOND_MKT_INFO.L_TRADE_DATE
                  LEFT JOIN HOLDING_DATE_INFO HDI
                            ON HDB.L_FUND_ID = HDI.L_FUND_ID AND HDB.VC_WIND_CODE = HDI.VC_WIND_CODE
                  LEFT JOIN BOND_RANGE_PROFIT_DETAILS BRPD ON HDB.VC_WIND_CODE = BRPD.VC_WIND_CODE
         GROUP BY HDB.L_FUND_ID, HDB.VC_WIND_CODE)
SELECT HD.FUND_ID,
       HD.BOND_CODE,
       HD.BOND_NAME,
       HD.WIND_1,
       HD.WIND_2,
       TDA.BEGIN_DATE,
       TDA.END_DATE,
       HDI.SETUP_DATE,
       DECODE(HDI.MAX_HOLDING_DATE, TDA.END_DATE, NULL, HDI.MAX_HOLDING_DATE) CLEAR_DATE,
       -- 收益金额
       HD.PROFIT_MKT,
       HD.INTEREST_PROFIT_MKT,
       HD.CAPITAL_PROFIT_MKT,
       -- 收益贡献
       HD.PROFIT_CTB_PD,
       HD.INTEREST_PROFIT_CTB_PD,
       HD.CAPITAL_PROFIT_CTB_PD,
       HD.PROFIT_CTB_BD,
       HD.INTEREST_PROFIT_CTB_BD,
       HD.CAPITAL_PROFIT_CTB_BD,
       -- 持有收益率
       HD.HOLDING_PROFIT,
       -- 报告期收益率
       HD.RANGE_PROFIT,
       HD.MIN_POSITION,
       HD.AVG_POSITION,
       HD.MAX_POSITION
FROM HOLDING_DETAILS HD
         LEFT JOIN HOLDING_DATE_INFO HDI ON HD.FUND_ID = HDI.L_FUND_ID AND HD.BOND_CODE = HDI.VC_WIND_CODE,
     TRADE_DATE_ADJ TDA
ORDER BY FUND_ID, PROFIT_CTB_PD DESC;