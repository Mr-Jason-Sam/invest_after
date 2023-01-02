WITH TD_ADJ AS (
    -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- 已处理当天收益的开始日期
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd')
        AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd'))
SELECT RV_ASSETS.VALUE_AGING,
       RV_ASSETS.L_TRADE_DATE,
       RV_ASSETS.RV_DAY_PROFIT_RATIO,
       FUND_ASSETS.EN_NAV_PRE * (RV_ASSETS.RV_DAY_PROFIT_RATIO + 1)              RV_NAV,
       FUND_ASSETS.EN_NAV_PRE,
       DECODE(RV_ASSETS.VALUE_AGING, 'T+1', '--', FUND_ASSETS.EN_NAV)         AS EN_NAV,
       DECODE(RV_ASSETS.VALUE_AGING, 'T+1', '--', RV_ASSETS.RV_DIFF)          AS RV_DIFF,
       DECODE(RV_ASSETS.VALUE_AGING, 'T+1', '--', RV_ASSETS.DAY_PROFIT_RATIO) AS DAY_PROFIT_RATIO,
       TD_ADJ.END_DATE
FROM (SELECT RV_ASSETS.*,
             -- 估值时效
             VA.VALUE_AGING,
             -- 估值差异
             RV_ASSETS.RV_DAY_PROFIT_RATIO - RV_ASSETS.DAY_PROFIT_RATIO RV_DIFF,
             EXP(SUM(LN(1 + RV_ASSETS.RV_DAY_PROFIT_RATIO))
                     OVER (PARTITION BY RV_ASSETS.L_FUND_ID ORDER BY RV_ASSETS.L_TRADE_DATE)) -
             1                                                          RV_RANGE_PF_RATIO
      FROM (SELECT REVALUATION_ASSETS.L_FUND_ID,
                   REVALUATION_ASSETS.L_TRADE_DATE,
                   -- 重估日收益率
                   REVALUATION_ASSETS.REVALUATION_PF /
                   REVALUATION_ASSETS.FUND_ASSETS_NET_PRE     RV_DAY_PROFIT_RATIO,
                   -- 原日收益率
                   REVALUATION_ASSETS.EN_FUND_ASSET_NET /
                   REVALUATION_ASSETS.FUND_ASSETS_NET_PRE - 1 DAY_PROFIT_RATIO
            FROM (SELECT ASSETS_BASE.*,
                         -- 净资产估值
                         ASSETS_BASE.EN_FUND_ASSET_NET - ASSETS_BASE.FUND_ASSETS_NET_PRE
                             - ASSETS_BASE.DAY_PF + ASSETS_BASE.DAY_PF_REVALUE REVALUATION_PF
                  FROM (SELECT RTN.L_FUND_ID,
                               RTN.L_TRADE_DATE,
                               RTN.EN_FUND_ASSET_NET,
                               -- 昨日重估净资产
                               RTN.EN_FUND_ASSET_NET_PRE +
                                   -- 买入项(数据库中为负数，则为累加项)
                               RTN.EN_APPLY_BAL + RTN.EN_APPLY_DIRECT + RTN.EN_APPEND_BAL +
                                   -- 卖出项
                               RTN.EN_REDEEM_BAL + RTN.EN_REDEEM_DIRECT + RTN.EN_EXTRACT_BAL +
                                   -- 其他：分红、分红再投、业绩报酬
                               RTN.EN_FUND_DIVIDEND + RTN.EN_FUND_DIVIDEND_INVEST +
                               RTN.EN_INCOME_REWARD FUND_ASSETS_NET_PRE,

                               HDF.DAY_PF_REVALUE,
                               HDF.MKT_INIT_REVALUE,
                               HDF.DAY_PF,
                               HDF.MKT_INIT
                        FROM ZHFX.TFUNDRETURNDETAIL RTN
                                 LEFT JOIN (SELECT HDF.L_FUND_ID,
                                                   HDF.L_TRADE_DATE,
                                                   SUM(EN_PROFIT_REVALUE)      DAY_PF_REVALUE,
                                                   SUM(EN_MARKET_INIT_REVALUE) MKT_INIT_REVALUE,
                                                   SUM(EN_PROFIT)              DAY_PF,
                                                   SUM(EN_MARKET_INIT)         MKT_INIT
                                            FROM ZHFX.THOLDINGDETAILFUND HDF,
                                                 TD_ADJ
                                            WHERE HDF.L_FUND_ID = ${ztbh}
                                              AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE
                                                AND TD_ADJ.END_DATE
                                            GROUP BY HDF.L_FUND_ID, HDF.L_TRADE_DATE) HDF
                                           ON RTN.L_FUND_ID = HDF.L_FUND_ID AND RTN.L_TRADE_DATE = HDF.L_TRADE_DATE,
                             TD_ADJ
                        WHERE RTN.L_FUND_ID = ${ztbh}
                          AND RTN.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE) ASSETS_BASE
                  ORDER BY L_TRADE_DATE) REVALUATION_ASSETS) RV_ASSETS
               LEFT JOIN ZHFX.TFUNDVALUEAGING VA
                         ON RV_ASSETS.L_FUND_ID = VA.FUND_ID
      WHERE VA.CUR_SIGN = 1) RV_ASSETS
         LEFT JOIN (SELECT L_TRADE_DATE,
                           L_FUND_ID,
                           EN_NAV,
                           LAG(FUND_ASSETS.EN_NAV, 1, NULL)
                               OVER (PARTITION BY FUND_ASSETS.L_FUND_ID ORDER BY FUND_ASSETS.L_TRADE_DATE) EN_NAV_PRE
                    FROM ZHFX.TFUNDASSET FUND_ASSETS
                    WHERE L_FUND_ID = ${ztbh}) FUND_ASSETS
                   ON RV_ASSETS.L_FUND_ID = FUND_ASSETS.L_FUND_ID
                       AND RV_ASSETS.L_TRADE_DATE = FUND_ASSETS.L_TRADE_DATE,
     TD_ADJ
WHERE RV_ASSETS.L_TRADE_DATE = TD_ADJ.END_DATE
ORDER BY RV_ASSETS.L_TRADE_DATE
