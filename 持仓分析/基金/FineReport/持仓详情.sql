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
     FUND_GROUP_INFO AS (
         -- 基金详情
         SELECT HBQ.L_FUND_ID,
                HBQ.VC_STOCK_CODE,
                MIN(HBQ.L_TRADE_DATE)                                                           MIN_DATE,
                MAX(HBQ.L_TRADE_DATE)                                                           MAX_DATE,
                MIN(DECODE(FA.FOF_MV, 0, 0, HBQ.EN_VALUE_MARKET / FA.FOF_MV))                     MIN_POSITION,
                AVG(DECODE(FA.FOF_MV, 0, 0, HBQ.EN_VALUE_MARKET / FA.FOF_MV))                     AVG_POSITION,
                AVG(DECODE(PA.EN_FUND_ASSET_NET, 0, 0, HBQ.EN_VALUE_MARKET / PA.EN_FUND_ASSET_NET)) AVG_PD_POSITION,
                MAX(DECODE(FA.FOF_MV, 0, 0, HBQ.EN_VALUE_MARKET / FA.FOF_MV))                     MAX_POSITION,
                COUNT(HBQ.EN_VALUE_MARKET)                                                          HOLDING_DAYS,
                MAX(TD_ADJ.SAMPLE_DATES) AS                                                     SAMPLE_DATES
         FROM HOLDING_BASE_QUOTE HBQ
                  LEFT JOIN PD_ASSETS PA
                            ON HBQ.L_FUND_ID = PA.L_FUND_ID
                                AND HBQ.L_TRADE_DATE = PA.L_TRADE_DATE
              LEFT JOIN FOF_ASSETS FA
                            ON HBQ.L_FUND_ID = FA.L_FUND_ID
                                AND HBQ.L_TRADE_DATE = FA.L_TRADE_DATE,
              TD_ADJ
         GROUP BY HBQ.L_FUND_ID, HBQ.VC_STOCK_CODE),
    HOLDINGS_REPORT_INFO AS (
         -- 持仓报告期信息
         SELECT SIF.L_TRADE_DATE,
                SIF.VC_WIND_CODE,
                DECODE(SIF.EN_NAV_ADJUSTED_PRE, 0, 0, SIF.EN_NAV_ADJUSTED / SIF.EN_NAV_ADJUSTED_PRE - 1) DAY_PF_RATIO
         FROM ZHFX.TSTOCKINFOFUND SIF,
              TD_ADJ
         WHERE SIF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE
             AND TD_ADJ.END_DATE),
     FUND_PROFIT_INFO AS (
         -- 基金的业绩表现
         SELECT HDF.L_FUND_ID,
                HDF.VC_STOCK_CODE,
                -- 报告期收益率
                EXP(SUM(LN(1 + HRI.DAY_PF_RATIO))) - 1                                       REPORT_PEIOD_PF_RATIO,
                -- 持有期收益率
                EXP(SUM(LN(DECODE(HDF.EN_MARKET_INIT, 0, 0,
                                  HDF.EN_PROFIT / HDF.EN_MARKET_INIT) + 1))) - 1 HOLDING_PF_RATIO,
                -- 持有期收益金额
                SUM(HDF.EN_PROFIT)                                                           HOLDING_PF_VALUE
         FROM HOLDING_BASE_QUOTE HDF
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON HDF.VC_STOCK_CODE = MFD.VC_STOCK_CODE
                  LEFT JOIN HOLDINGS_REPORT_INFO HRI
                            ON HDF.L_TRADE_DATE = HRI.L_TRADE_DATE
                                AND MFD.VC_WIND_CODE = HRI.VC_WIND_CODE
         GROUP BY HDF.L_FUND_ID, HDF.VC_STOCK_CODE),
     LATEST_ASSETS_INFO AS (
         -- 最新资产信息
         SELECT HBQ.L_FUND_ID,
                HBQ.VC_STOCK_CODE,
                HBQ.L_TRADE_DATE,
                DECODE(FA.FOF_MV, 0, 0, HBQ.EN_VALUE_MARKET / FA.FOF_MV)                     FOF_POSITION,
                DECODE(PA.EN_FUND_ASSET_NET, 0, 0, HBQ.EN_VALUE_MARKET / PA.EN_FUND_ASSET_NET) PD_POSITION
         FROM HOLDING_BASE_QUOTE HBQ
                  LEFT JOIN PD_ASSETS PA
                            ON HBQ.L_FUND_ID = PA.L_FUND_ID
                                AND HBQ.L_TRADE_DATE = PA.L_TRADE_DATE
             LEFT JOIN FOF_ASSETS FA
                            ON HBQ.L_FUND_ID = FA.L_FUND_ID
                                AND HBQ.L_TRADE_DATE = FA.L_TRADE_DATE,
              TD_ADJ
         WHERE HBQ.L_TRADE_DATE = TD_ADJ.END_DATE),
     FUND_DETAILS AS (
         -- 基金详细信息
         SELECT FGI.L_FUND_ID,
                FGI.VC_STOCK_CODE,
                FGI.MIN_DATE,
                FGI.MAX_DATE,
                FGI.MIN_POSITION,
                FGI.AVG_POSITION,
                FGI.AVG_PD_POSITION,
                FGI.MAX_POSITION,
                NVL(LAI.FOF_POSITION, 0)         AS FOF_POSITION,
                NVL(LAI.PD_POSITION, 0)          AS PD_POSITION,
                FGI.HOLDING_DAYS,
                FGI.SAMPLE_DATES,

                FPI.HOLDING_PF_VALUE / 1e4 AS HOLDING_PEIOD_PF_VALUE,
                FPI.HOLDING_PF_RATIO,
                FPI.REPORT_PEIOD_PF_RATIO,

                MFD.VC_STOCK_NAME,
                MFD.VC_MANAGER_NAME,
                MFD.VC_FUND_MANAGER,
                MFD.VC_FUND_TYPE_WIND,
                MFD.VC_FUND_TYPE_WIND_SECOND

         FROM FUND_GROUP_INFO FGI
                  LEFT JOIN FUND_PROFIT_INFO FPI
                            ON FGI.L_FUND_ID = FPI.L_FUND_ID
                                AND FGI.VC_STOCK_CODE = FPI.VC_STOCK_CODE
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON FPI.VC_STOCK_CODE = MFD.VC_STOCK_CODE
                  LEFT JOIN LATEST_ASSETS_INFO LAI
                            ON FGI.L_FUND_ID = LAI.L_FUND_ID
                                AND FGI.VC_STOCK_CODE = LAI.VC_STOCK_CODE)
-- 集中度详情
SELECT FD.*,
       (TO_CHAR(FD.HOLDING_DAYS) || '/' || TO_CHAR(FD.SAMPLE_DATES))                                  HOLDINGS_TO_SAMPLE,
       ROW_NUMBER() OVER (PARTITION BY FD.L_FUND_ID ORDER BY PD_POSITION DESC NULLS LAST )            LATEST_POSITION_RANK,
       ROW_NUMBER() OVER (PARTITION BY FD.L_FUND_ID ORDER BY AVG_PD_POSITION DESC NULLS LAST )        AVG_POSITION_RANK,
       ROW_NUMBER() OVER (PARTITION BY FD.L_FUND_ID ORDER BY HOLDING_DAYS DESC NULLS LAST )           HOLDING_DAYS_RANK,
       ROW_NUMBER() OVER (PARTITION BY FD.L_FUND_ID ORDER BY HOLDING_PEIOD_PF_VALUE DESC NULLS LAST ) HP_PF_VALUE_RANK,
       ROW_NUMBER() OVER (PARTITION BY FD.L_FUND_ID ORDER BY REPORT_PEIOD_PF_RATIO DESC NULLS LAST )  RP_PF_RATIO_RANK
FROM FUND_DETAILS FD