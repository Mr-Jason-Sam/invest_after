WITH TD_ADJ AS (
    -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- 已处理当天收益的开始日期
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd')
        AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')),
     PD_ASSETS AS (
         -- 产品资产信息
         SELECT FRD.L_FUND_ID,
                FRD.L_TRADE_DATE,
                FRD.EN_FUND_ASSET_NET,
                FRD.EN_FUND_ASSET_NET_PRE +
                    -- 买入项(数据库中为负数，则为累加项)
                FRD.EN_APPLY_BAL + FRD.EN_APPLY_DIRECT + FRD.EN_APPEND_BAL +
                    -- 卖出项
                FRD.EN_REDEEM_BAL + FRD.EN_REDEEM_DIRECT + FRD.EN_EXTRACT_BAL +
                    -- 其他：分红、分红再投、业绩报酬
                FRD.EN_FUND_DIVIDEND + FRD.EN_FUND_DIVIDEND_INVEST +
                FRD.EN_INCOME_REWARD FUND_ASSETS_NET_PRE
         FROM ZHFX.TFUNDRETURNDETAIL FRD,
              TD_ADJ
         WHERE FRD.L_FUND_ID = ${ztbh}
           AND FRD.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE),
     SINGLE_FUND_ANAYSIS AS (
         -- 个基交易分析
         SELECT
             -- 个基代码
             SIF.VC_WIND_CODE,
             MFD.VC_STOCK_CODE,
             -- 个基名称
             MFD.VC_STOCK_NAME,
             -- 日期
             SIF.L_TRADE_DATE,
             -- 买入比例，
             NVL(TFF.BUY_RATIO, 0)  AS                                                   BUY_RATIO,
             -- 卖出比例，
             NVL(TFF.SELL_RATIO, 0) AS                                                   SELL_RATIO,
             -- 个基累计收益率
             EXP(SUM(LN(CASE
                            WHEN MFD.VC_MARKET_TYPE IN ('场外') THEN
                                DECODE(
                                        SIF.EN_NAV_ADJUSTED_PRE,
                                        0, NULL,
                                        SIF.EN_NAV_ADJUSTED / SIF.EN_NAV_ADJUSTED_PRE
                                    )
                            ELSE
                                DECODE(
                                        SIF.EN_PRICE_CLOSE_PRE,
                                        0, NULL,
                                        SIF.EN_PRICE_CLOSE / SIF.EN_PRICE_CLOSE_PRE
                                    ) END
                         )) OVER (PARTITION BY
                 SIF.VC_WIND_CODE ORDER BY SIF.L_TRADE_DATE)) - 1                        FUND_RANGE_PROFIT_RATIO,
             -- 基准累计收益
             EXP(SUM(LN(1 + SIF.EN_PROFIT_BENCH))
                     OVER (PARTITION BY SIF.VC_WIND_CODE ORDER BY SIF.L_TRADE_DATE)) - 1 BM_RANGE_PROFIT_RATIO
             -- TODO 行业累计收益
         FROM ZHFX.TSTOCKINFOFUND SIF
                  LEFT JOIN (SELECT DISTINCT TFF.L_FUND_ID,
                                             TFF.VC_WIND_CODE
                             FROM ZHFX.TTRADEFLOWFUND TFF,
                                  TD_ADJ
                             WHERE TFF.L_FUND_ID = ${ztbh}
                               AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE
                                 AND TD_ADJ.END_DATE) TRADE_FUND_CODE
                            ON SIF.VC_WIND_CODE = TRADE_FUND_CODE.VC_WIND_CODE
                  LEFT JOIN (SELECT TFF.L_FUND_ID,
                                    TFF.VC_STOCK_CODE,
                                    TFF.VC_WIND_CODE,
                                    TFF.L_TRADE_DATE,
                                    -- 买入比例
                                    SUM(CASE
                                            WHEN TFF.VC_BUSIN_TYPE IN ('买入', '转换_转入', '申购')
                                                THEN DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0,
                                                            TFF.EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE)
                                            ELSE 0 END) BUY_RATIO,
                                    -- 卖出比例
                                    SUM(CASE
                                            WHEN TFF.VC_BUSIN_TYPE IN ('卖出', '转换_转出', '赎回')
                                                THEN DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0,
                                                            TFF.EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE) * -1
                                            ELSE 0 END) SELL_RATIO
                             FROM ZHFX.TTRADEFLOWFUND TFF
                                      LEFT JOIN PD_ASSETS PA
                                                ON TFF.L_FUND_ID = PA.L_FUND_ID
                                                    AND TFF.L_TRADE_DATE = PA.L_TRADE_DATE,
                                  TD_ADJ
                             WHERE TFF.L_FUND_ID = ${ztbh}
                               AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE
                                 AND TD_ADJ.END_DATE
                             GROUP BY TFF.L_FUND_ID,
                                      TFF.VC_STOCK_CODE,
                                      TFF.VC_WIND_CODE,
                                      TFF.L_TRADE_DATE) TFF
                            ON SIF.VC_WIND_CODE = TFF.VC_WIND_CODE
                                AND SIF.L_TRADE_DATE = TFF.L_TRADE_DATE
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON SUBSTR(SIF.VC_WIND_CODE, 0, 6) = MFD.VC_STOCK_CODE
                 ,
              TD_ADJ
         WHERE MFD.VC_STOCK_CODE = ${FUND_CODE}
           AND SIF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE)
-- 个基交易分析
SELECT *
FROM SINGLE_FUND_ANAYSIS
ORDER BY L_TRADE_DATE
