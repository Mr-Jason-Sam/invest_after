WITH CONSTANTS AS (
    -- 常量
    SELECT 'SW_1'   SW_1,
           'SW_2'   SW_2,
           'SW_3'   SW_3,
           'SEC_1'  SEC_1,
           'SEC_2'  SEC_2,
           'WIND_1' WIND_1,
           'WIND_2' WIND_2,
           'WIND_3' WIND_3,
           'ZX_1'   ZX_1,
           'ZX_2'   ZX_2,
           'ZX_3'   ZX_3,

           '0331'   Q1,
           '0630'   Q2,
           '0930'   Q3,
           '1231'   Q4
    FROM DUAL),
     TD_ADJ AS (
         -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
         SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
                -- 已处理当天收益的开始日期
                MIN(L_TRADE_DATE)      BEGIN_DATE,
                MAX(L_TRADE_DATE)      END_DATE,
                COUNT(L_TRADE_DATE)    SAMPLE_DATES
         FROM ZHFX.TCALENDAR
         WHERE L_DATE = L_TRADE_DATE
           AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd')
             AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')),

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
--     ASSETS_DATE AS (
--         -- 资产日期
--         SELECT MIN(L_TRADE_DATE) MIN_DATE, MAX(L_TRADE_DATE) MAX_DATE FROM PD_ASSETS
--     ),
     HOLDING_WITH_REPORT AS (
         -- 持仓待报告期
         SELECT HDF.*,
                CASE
                    WHEN (TRUNC(HDF.L_TRADE_DATE / 10000) - 1) * 10000 + TO_NUMBER(CONS.Q4)
                             <= HDF.L_TRADE_DATE AND HDF.L_TRADE_DATE <
                                                     TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q1)
                        THEN
                        (TRUNC(HDF.L_TRADE_DATE / 10000) - 1) * 10000 + TO_NUMBER(CONS.Q4)
                    WHEN TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q1)
                             <= HDF.L_TRADE_DATE AND HDF.L_TRADE_DATE <
                                                     TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q2)
                        THEN
                        TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q1)
                    WHEN TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q2)
                             <= HDF.L_TRADE_DATE AND HDF.L_TRADE_DATE <
                                                     TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q3)
                        THEN
                        TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q2)
                    ELSE
                        TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q3)
                    END REPORT_DATE,
                CASE
                    WHEN (TRUNC(HDF.L_TRADE_DATE / 10000) - 1) * 10000 + TO_NUMBER(CONS.Q4)
                             <= HDF.L_TRADE_DATE AND HDF.L_TRADE_DATE <
                                                     TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q2)
                        THEN
                        (TRUNC(HDF.L_TRADE_DATE / 10000) - 1) * 10000 + TO_NUMBER(CONS.Q4)
                    WHEN TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q2)
                             <= HDF.L_TRADE_DATE AND HDF.L_TRADE_DATE <
                                                     TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q4)
                        THEN
                        TRUNC(HDF.L_TRADE_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q2)
                    END TOTAL_INFO_REPORT_DATE
         FROM HOLDING_BASE_QUOTE HDF,
              CONSTANTS CONS,
--               ASSETS_DATE AD,
              TD_ADJ
         WHERE HDF.L_FUND_ID = ${ztbh}
           AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
--            AND HDF.VC_MARKET_TYPE = '场外'
--            AND HDF.VC_STOCK_NAME NOT LIKE '%联接%'
--                  AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
     ),
     TYPE_PENETRATE AS (
         -- 大类穿透（全部基金）
         SELECT HDF.L_FUND_ID,
                HDF.VC_WIND_CODE                                       ROOT_FUND_CODE,
                HDF.L_TRADE_DATE                                       TRADE_DATE,
                DECODE(FA.FOF_MV, 0, 0, HDF.EN_VALUE_MARKET / FA.FOF_MV) FOF_POSITION,
                DECODE(PA.EN_FUND_ASSET_NET, 0, 0, HDF.EN_VALUE_MARKET / PA.EN_FUND_ASSET_NET) PF_POSITION,

                FA_OUT.VC_FUND_CODE                                    UF_FUND_CODE,
                FA_OUT.L_REPORT_DATE AS                                UF_REPORT_DATE,

                -- 基金净资产
                -- UF == UNIT_FUND
                FA_OUT.EN_FUND_VALUE                                   UF_MKT,
                -- 股票资产
                FA_OUT.EN_SHARE_ASSET                                  UF_STK_MKT,
                -- 债券资产
                FA_OUT.EN_BOND_ASSET                                   UF_BOND_MKT,
                -- 基金资产
                FA_OUT.EN_FUND_ASSET                                   UF_FUND_MKT,
                -- 现金资产
                FA_OUT.EN_BANK_DEPOSIT                                 UF_CASH_MKT,

                -- 利率债资产
                FA_OUT.EN_BOND_ASSET_RATE                              UF_BOND_RATE_MKT,
                -- 可转债资产
                FA_OUT.EN_BOND_ASSET_CONVERT                           UF_BOND_CONVERT_MKT,
                -- 信用债资产
                FA_OUT.EN_BOND_ASSET - FA_OUT.EN_BOND_ASSET_RATE       UF_BOND_CREDIT_MKT,
                -- 修正久期
                FA_OUT.EN_MODIDURA                                     UF_MODIDURA
         FROM HOLDING_WITH_REPORT HDF
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON HDF.VC_STOCK_CODE = MFD.VC_STOCK_CODE
                  LEFT JOIN PD_ASSETS PA
                            ON HDF.L_FUND_ID = PA.L_FUND_ID
                                AND HDF.L_TRADE_DATE = PA.L_TRADE_DATE
             LEFT JOIN FOF_ASSETS FA
                            ON HDF.L_FUND_ID = FA.L_FUND_ID
                                AND HDF.L_TRADE_DATE = FA.L_TRADE_DATE
                  LEFT JOIN ZHFX.TFUNDASSETOUT FA_OUT
                            ON MFD.VC_WIND_CODE = FA_OUT.VC_FUND_CODE
                                AND HDF.REPORT_DATE = FA_OUT.L_REPORT_DATE),
     ASSETS_CONFIG_PENETRATE AS (
         -- 资产配置穿透
         SELECT TP.L_FUND_ID,
                TP.TRADE_DATE,
                MAX(TP.UF_REPORT_DATE)                                                              AS     UF_REPORT_DATE,
                -- 股票
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_STK_MKT / TP.UF_MKT * TP.FOF_POSITION))         STK_POSITION,
                -- 债券
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_BOND_MKT / TP.UF_MKT * TP.FOF_POSITION))        BOND_POSITION,
                -- 基金
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_FUND_MKT / TP.UF_MKT * TP.FOF_POSITION)) AS     FUND_POSITION,
                -- 现金
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_CASH_MKT / TP.UF_MKT * TP.FOF_POSITION))        CASH_POSITION,
                -- 利率债
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_BOND_RATE_MKT / TP.UF_MKT * TP.FOF_POSITION))   RATE_BOND_POSITION,
                -- 可转债
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_BOND_CONVERT_MKT / TP.UF_MKT *
                                            TP.FOF_POSITION))                                              CONVERT_BOND_POSITION,
                -- 信用债
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_BOND_CREDIT_MKT / TP.UF_MKT * TP.FOF_POSITION)) CREDIT_BOND_POSITION,
                -- 修正久期
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_MODIDURA / TP.UF_MKT * TP.FOF_POSITION))        MODIDURA,

                -- 股票
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_STK_MKT / TP.UF_MKT * TP.PF_POSITION))         STK_PD_POSITION,
                -- 债券
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_BOND_MKT / TP.UF_MKT * TP.PF_POSITION))        BOND_PD_POSITION,
                -- 基金
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_FUND_MKT / TP.UF_MKT * TP.PF_POSITION)) AS     FUND_PD_POSITION,
                -- 现金
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_CASH_MKT / TP.UF_MKT * TP.PF_POSITION))        CASH_PD_POSITION,
                -- 利率债
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_BOND_RATE_MKT / TP.UF_MKT * TP.PF_POSITION))   RATE_BOND_PD_POSITION,
                -- 可转债
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_BOND_CONVERT_MKT / TP.UF_MKT *
                                            TP.PF_POSITION))                                     CONVERT_BOND_PD_POSITION,
                -- 信用债
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_BOND_CREDIT_MKT / TP.UF_MKT * TP.PF_POSITION)) CREDIT_BOND_PD_POSITION,
                -- 修正久期
                SUM(DECODE(TP.UF_MKT, 0, 0, NULL, 0, TP.UF_MODIDURA / TP.UF_MKT * TP.PF_POSITION))        MODIDURA_PD_POSITION

         FROM TYPE_PENETRATE TP
         GROUP BY TP.L_FUND_ID, TP.TRADE_DATE
         ORDER BY TP.L_FUND_ID, TP.TRADE_DATE)
-- 大类穿透
SELECT *
FROM ASSETS_CONFIG_PENETRATE

