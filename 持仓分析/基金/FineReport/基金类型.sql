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
                MFD.VC_FUND_TYPE_WIND_SECOND
               FROM ZHFX.THOLDINGDETAILFUND HDF
                        LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                                  ON HDF.VC_STOCK_CODE = MFD.VC_STOCK_CODE,
                    TD_ADJ
               WHERE HDF.L_FUND_ID = ${ztbh}
                 AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE_LAST AND TD_ADJ.END_DATE
               ORDER BY HDF.L_TRADE_DATE),
     FOF_ASSETS AS (
        -- 基金组合资产
        SELECT
            HDF.L_FUND_ID,
            HDF.L_TRADE_DATE,
            SUM(HDF.EN_MARKET_INIT) FOF_MV_INIT,
            SUM(HDF.EN_VALUE_MARKET) FOF_MV
            FROM ZHFX.THOLDINGDETAILFUND HDF,
             TD_ADJ
            WHERE HDF.L_FUND_ID = ${ztbh}
          AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE GROUP BY HDF.L_FUND_ID,
            HDF.L_TRADE_DATE
    ),
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
     FUND_TYPE_CLASSIFY AS (
--          -- 基金分类
--          SELECT HBQ.L_FUND_ID,
--                 HBQ.L_TRADE_DATE,
--                 'WIND一级'                    CLASSIFY,
--                 HBQ.VC_FUND_TYPE_WIND         TYPE,
--                 SUM(MKT_REVALUE / PA.FOF_MKT) POSITION
--          FROM HOLDING_BASE_QUOTE HBQ
--                   LEFT JOIN PD_ASSETS PA
--                             ON HBQ.L_FUND_ID = PA.L_FUND_ID
--                                 AND HBQ.L_TRADE_DATE = PA.L_TRADE_DATE
--          GROUP BY HBQ.L_FUND_ID,
--                   HBQ.L_TRADE_DATE,
--                   HBQ.VC_FUND_TYPE_WIND
--          UNION ALL
         SELECT HBQ.L_FUND_ID,
                HBQ.L_TRADE_DATE,
                'WIND二级'                                              CLASSIFY,
                HBQ.VC_FUND_TYPE_WIND_SECOND                            TYPE,
                SUM(DECODE(FA.FOF_MV, 0, 0, HBQ.EN_VALUE_MARKET / FA.FOF_MV)) FOF_POSITION,
                SUM(DECODE(PA.EN_FUND_ASSET_NET, 0, 0, HBQ.EN_VALUE_MARKET / PA.EN_FUND_ASSET_NET)) PD_POSITION
         FROM HOLDING_BASE_QUOTE HBQ
                  LEFT JOIN PD_ASSETS PA
                            ON HBQ.L_FUND_ID = PA.L_FUND_ID
                                AND HBQ.L_TRADE_DATE = PA.L_TRADE_DATE
         LEFT JOIN FOF_ASSETS FA
                            ON HBQ.L_FUND_ID = FA.L_FUND_ID
                                AND HBQ.L_TRADE_DATE = FA.L_TRADE_DATE
         GROUP BY HBQ.L_FUND_ID,
                  HBQ.L_TRADE_DATE,
                  HBQ.VC_FUND_TYPE_WIND_SECOND)

-- 基金类型
SELECT *
FROM FUND_TYPE_CLASSIFY
ORDER BY L_FUND_ID, L_TRADE_DATE, FOF_POSITION DESC