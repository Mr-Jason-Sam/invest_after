WITH TD_ADJ AS (
    -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- 已处理当天收益的开始日期
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE,
           COUNT(L_TRADE_DATE)    SAMPLE_DATES
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd')
        AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')),

     HOLDING_BASE_QUOTE AS (
         -- 基金行情信息
         SELECT HDF.*,
                MFD.VC_FUND_TYPE_WIND_SECOND,
                DECODE(HDF.EN_MARKET_INIT, 0, 0, HDF.EN_PROFIT / HDF.EN_MARKET_INIT) FUND_DAY_PROFIT_RATIO
               FROM ZHFX.THOLDINGDETAILFUND HDF
                        LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                                  ON HDF.VC_STOCK_CODE = MFD.VC_STOCK_CODE,
                    TD_ADJ
               WHERE HDF.L_FUND_ID = ${ztbh}
                 AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE_LAST AND TD_ADJ.END_DATE
               ORDER BY HDF.L_TRADE_DATE),
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
          AND FRD.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
    ),
     FUND_GROUP_INFO AS (
         -- 基金详情
         SELECT HBQ.L_FUND_ID,
                HBQ.VC_STOCK_CODE,
                MIN(HBQ.L_TRADE_DATE)             MIN_DATE,
                MAX(HBQ.L_TRADE_DATE)             MAX_DATE,
                AVG(DECODE(PA.EN_FUND_ASSET_NET, 0, 0, HBQ.EN_VALUE_MARKET / PA.EN_FUND_ASSET_NET)) AVG_POSITION,
                COUNT(HBQ.EN_VALUE_MARKET)            HOLDING_DAYS,
                MAX(TD_ADJ.SAMPLE_DATES) AS       SAMPLE_DATES
         FROM HOLDING_BASE_QUOTE HBQ
                  LEFT JOIN PD_ASSETS PA
                            ON HBQ.L_FUND_ID = PA.L_FUND_ID
                                AND HBQ.L_TRADE_DATE = PA.L_TRADE_DATE,
              TD_ADJ
         GROUP BY HBQ.L_FUND_ID, HBQ.VC_STOCK_CODE),
     FUND_LATEST_INFO AS (SELECT HBQ.L_FUND_ID,
                                 HBQ.VC_STOCK_CODE,
                                 DECODE(PA.EN_FUND_ASSET_NET, 0, 0, HBQ.EN_VALUE_MARKET / PA.EN_FUND_ASSET_NET) POSITION
                          FROM HOLDING_BASE_QUOTE HBQ
                                   LEFT JOIN PD_ASSETS PA
                                             ON HBQ.L_FUND_ID = PA.L_FUND_ID
                                                 AND HBQ.L_TRADE_DATE = PA.L_TRADE_DATE,
                               TD_ADJ
                          WHERE PA.L_TRADE_DATE = TD_ADJ.END_DATE),
     FUND_SELECTOR AS (SELECT FGI.L_FUND_ID,
                              FGI.VC_STOCK_CODE,
                              FGI.AVG_POSITION,
                              FGI.HOLDING_DAYS,
                              FGI.SAMPLE_DATES,
                              FLI.POSITION,
                              ROW_NUMBER() OVER (PARTITION BY FGI.L_FUND_ID ORDER BY FGI.AVG_POSITION DESC NULLS LAST ) AVG_POSI_RANK,
                              ROW_NUMBER() OVER (PARTITION BY FGI.L_FUND_ID ORDER BY FLI.POSITION DESC NULLS LAST )     LATEST_POSI_RANK,
                              ROW_NUMBER() OVER (PARTITION BY FGI.L_FUND_ID ORDER BY FGI.HOLDING_DAYS DESC NULLS LAST ) HOLDING_DAYS_RANK
                       FROM FUND_GROUP_INFO FGI
                                LEFT JOIN FUND_LATEST_INFO FLI
                                          ON FGI.L_FUND_ID = FLI.L_FUND_ID
                                              AND FGI.VC_STOCK_CODE = FLI.VC_STOCK_CODE),
     AVG_POSI_CORR_CODE_LIST AS (SELECT FUND_SELECTOR.VC_STOCK_CODE
                                 FROM FUND_SELECTOR
                                 WHERE FUND_SELECTOR.AVG_POSI_RANK <= 10),
     HOLDINGS_CORR_CODE_LIST AS (SELECT FUND_SELECTOR.VC_STOCK_CODE
                                 FROM FUND_SELECTOR
                                 WHERE FUND_SELECTOR.HOLDING_DAYS_RANK <= 10),
     LATEST_POSI_CODE_LIST AS (SELECT FUND_SELECTOR.VC_STOCK_CODE
                               FROM FUND_SELECTOR
                               WHERE FUND_SELECTOR.LATEST_POSI_RANK <= 10),
     TOTAL_CODE_LIST AS (SELECT FUND_SELECTOR.VC_STOCK_CODE
                         FROM FUND_SELECTOR),
     CORR_HBQ AS (
         -- 相关性基金选择
         SELECT HBQ.*
         FROM HOLDING_BASE_QUOTE HBQ
         WHERE HBQ.VC_STOCK_CODE IN (SELECT VC_STOCK_CODE FROM ${TABLE})),
     CORRELATION AS (
         -- 相关性
         SELECT HBQ.L_FUND_ID,
                HBQ.L_TRADE_DATE,
                HBQ.VC_STOCK_CODE,
                HBQ.VC_STOCK_NAME,
                HBQ.FUND_DAY_PROFIT_RATIO,
                HBQ_CP.VC_STOCK_CODE         COMP_STK_CODE,
                HBQ_CP.VC_STOCK_NAME         COMP_STK_NAME,
                HBQ_CP.FUND_DAY_PROFIT_RATIO COMP_DPR,
                CORR(HBQ.FUND_DAY_PROFIT_RATIO, HBQ_CP.FUND_DAY_PROFIT_RATIO)
                     OVER (
                         PARTITION BY
                             HBQ.L_FUND_ID,
                             HBQ.VC_STOCK_CODE,
                             HBQ_CP.VC_STOCK_CODE
                         ORDER BY HBQ.L_TRADE_DATE
                         ) AS                CORR
         FROM CORR_HBQ HBQ
                  LEFT JOIN CORR_HBQ HBQ_CP
                            ON HBQ.L_FUND_ID = HBQ_CP.L_FUND_ID
                                AND HBQ.L_TRADE_DATE = HBQ_CP.L_TRADE_DATE),
     CORR_DATA AS (
         -- 相关性
         SELECT CR.L_FUND_ID,
                CR.L_TRADE_DATE,
                CR.VC_STOCK_CODE,
                CR.VC_STOCK_NAME,
                CR.COMP_STK_CODE,
                CR.COMP_STK_NAME,
                CR.CORR,
                FS.HOLDING_DAYS,
                FS.SAMPLE_DATES,
                FS.AVG_POSITION,
                FS.POSITION,
                COMP_FS.AVG_POSITION         COMP_AVG_POSITION,
                COMP_FS.HOLDING_DAYS         COMP_HOLDING_DAYS,
                COMP_FS.POSITION             COMP_POSITION,
                FS.AVG_POSI_RANK,
                FS.HOLDING_DAYS_RANK,
                FS.LATEST_POSI_RANK,
                COMP_FS.AVG_POSI_RANK     AS COMP_AVG_POSI_RANK,
                COMP_FS.HOLDING_DAYS_RANK AS COMP_HOLDING_DAYS_RANK,
                COMP_FS.LATEST_POSI_RANK  AS COMP_LATEST_POSI_RANK
         FROM CORRELATION CR
                  LEFT JOIN FUND_SELECTOR FS
                            ON CR.L_FUND_ID = FS.L_FUND_ID
                                AND CR.VC_STOCK_CODE = FS.VC_STOCK_CODE
                  LEFT JOIN FUND_SELECTOR COMP_FS
                            ON CR.L_FUND_ID = COMP_FS.L_FUND_ID
                                AND CR.COMP_STK_CODE = COMP_FS.VC_STOCK_CODE
                 ,
              TD_ADJ
         WHERE L_TRADE_DATE = TD_ADJ.END_DATE
         ORDER BY L_TRADE_DATE, FUND_DAY_PROFIT_RATIO)
-- -- 相关性代码
-- SELECT VC_STOCK_CODE
-- FROM CORR_CODE_LIST

-- 相关性数据
SELECT CD.*,
       CASE
           WHEN CD.AVG_POSI_RANK < CD.COMP_AVG_POSI_RANK THEN
               NULL
           ELSE
               CD.CORR
           END AVG_POSI_CORR,
       CASE
           WHEN CD.LATEST_POSI_RANK < CD.COMP_LATEST_POSI_RANK THEN
               NULL
           ELSE
               CD.CORR
           END LATEST_POSI_CORR,
       CASE
           WHEN CD.HOLDING_DAYS_RANK < CD.COMP_HOLDING_DAYS_RANK THEN
               NULL
           ELSE
               CD.CORR
           END HOLDING_DAYS_CORR
FROM CORR_DATA CD


