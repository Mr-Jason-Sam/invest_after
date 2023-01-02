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
     END_DATE_YEAR_REPORT_DATE AS (
         -- 获取最终日期的年报日
         SELECT CASE
                    WHEN (TRUNC(TD_ADJ.END_DATE / 10000) - 1) * 10000 + TO_NUMBER(CONS.Q4)
                             <= TD_ADJ.END_DATE AND TD_ADJ.END_DATE <
                                                    TRUNC(TD_ADJ.END_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q2)
                        THEN
                        (TRUNC(TD_ADJ.END_DATE / 10000) - 1) * 10000 + TO_NUMBER(CONS.Q4)
                    WHEN TRUNC(TD_ADJ.END_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q2)
                             <= TD_ADJ.END_DATE AND TD_ADJ.END_DATE <
                                                    TRUNC(TD_ADJ.END_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q4)
                        THEN
                        TRUNC(TD_ADJ.END_DATE / 10000) * 10000 + TO_NUMBER(CONS.Q2)
                    END REPORT_DATE
         FROM TD_ADJ,
              CONSTANTS CONS),
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
              TD_ADJ
         WHERE HDF.L_FUND_ID = ${ztbh}
           AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE),
     PENETRATE AS (
         -- 穿透
         SELECT INFO.*,
                CASE
                    WHEN UF_FUND_STK_INDUSTRY IN
                         ('纺织服饰', '家用电器', '美容护理', '农林牧渔', '轻工制造', '商贸零售', '社会服务',
                          '食品饮料')
                        THEN '申万-消费'
                    WHEN UF_FUND_STK_INDUSTRY IN ('医药生物') THEN '申万-医药'
                    WHEN UF_FUND_STK_INDUSTRY IN ('电力设备', '公用事业', '机械设备', '建筑装饰', '汽车', '综合')
                        THEN '申万-中游制造'
                    WHEN UF_FUND_STK_INDUSTRY IN ('传媒', '电子', '计算机', '通信') THEN '申万-TMT'
                    WHEN UF_FUND_STK_INDUSTRY IN ('环保', '基础化工', '建筑材料', '交通运输') THEN '申万-周期'
                    WHEN UF_FUND_STK_INDUSTRY IN ('国防军工') THEN '申万-军工'
                    WHEN UF_FUND_STK_INDUSTRY IN ('钢铁', '煤炭', '石油石化', '有色金属') THEN '申万-资源'
                    WHEN UF_FUND_STK_INDUSTRY IN ('房地产', '非银金融', '银行') THEN '申万-金融地产'
                    ELSE '其他'
                    END                                                         UF_FUND_STK_INDUSTRY_COMPLEX,
                DECODE(INFO.UF_MKT, 0, 0,
                       INFO.UF_ASSETS_MKT / INFO.UF_MKT * INFO.FOF_POSITION) AS FOF_POSI,
                DECODE(INFO.UF_MKT, 0, 0,
                       INFO.UF_ASSETS_MKT / INFO.UF_MKT * INFO.PD_POSITION)  AS PD_POSI
         FROM (SELECT HDF.L_FUND_ID,
                      HDF.VC_WIND_CODE                                                           ROOT_FUND_CODE,
                      HDF.L_TRADE_DATE                                                           TRADE_DATE,

                      DECODE(FA.FOF_MV, 0, 0, HDF.EN_VALUE_MARKET / FA.FOF_MV)                     FOF_POSITION,
                      DECODE(PA.EN_FUND_ASSET_NET, 0, 0, HDF.EN_VALUE_MARKET / PA.EN_FUND_ASSET_NET) PD_POSITION,

                      FA_OUT.VC_FUND_CODE                                                        UF_FUND_CODE,
                      FA_OUT.VC_FUND_NAME                                                        UF_FUND_NAME,
                      FA_OUT.L_REPORT_DATE AS                                                    UF_REPORT_DATE,

                      -- 基金净资产
                      -- UF == UNIT_FUND
                      FA_OUT.EN_FUND_VALUE                                                       UF_MKT,
                      -- 股票资产
                      FA_OUT.EN_SHARE_ASSET                                                      UF_STK_MKT,
                      -- 债券资产
                      FA_OUT.EN_BOND_ASSET                                                       UF_BOND_MKT,
                      -- 基金资产
                      FA_OUT.EN_FUND_ASSET                                                       UF_FUND_MKT,
                      -- 现金资产
                      FA_OUT.EN_BANK_DEPOSIT                                                     UF_CASH_MKT,

                      -- 利率债资产
                      FA_OUT.EN_BOND_ASSET_RATE                                                  UF_BOND_RATE_MKT,
                      -- 可转债资产
                      FA_OUT.EN_BOND_ASSET_CONVERT                                               UF_BOND_CONVERT_MKT,
                      -- 信用债资产
                      FA_OUT.EN_BOND_ASSET - FA_OUT.EN_BOND_ASSET_RATE                           UF_BOND_CREDIT_MKT,
                      -- 修正久期
                      FA_OUT.EN_MODIDURA                                                         UF_MODIDURA,

                      -- 持仓股票代码
                      HOLDINGS_OUT.VC_STOCK_CODE                                                 UF_ASSETS_CODE,
                      -- 持仓股票名称
                      HOLDINGS_OUT.VC_STOCK_NAME                                                 UF_ASSETS_NAME,
                      -- 持仓市值
                      HOLDINGS_OUT.EN_VALUE                                                      UF_ASSETS_MKT,
                      -- 行业
                      SIS.VC_INDUSTRY_SW_FIRST                                                   UF_FUND_STK_INDUSTRY

               FROM HOLDING_WITH_REPORT HDF
                        LEFT JOIN PD_ASSETS PA
                                  ON HDF.L_FUND_ID = PA.L_FUND_ID
                                      AND HDF.L_TRADE_DATE = PA.L_TRADE_DATE
                   LEFT JOIN FOF_ASSETS FA
                                  ON HDF.L_FUND_ID = FA.L_FUND_ID
                                      AND HDF.L_TRADE_DATE = FA.L_TRADE_DATE
                        LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                                  ON HDF.VC_STOCK_CODE = MFD.VC_STOCK_CODE
                        LEFT JOIN ZHFX.TFUNDASSETOUT FA_OUT
                                  ON FA_OUT.VC_FUND_CODE = MFD.VC_WIND_CODE
                                      AND FA_OUT.L_REPORT_DATE = HDF.TOTAL_INFO_REPORT_DATE
                        LEFT JOIN ZHFX.THOLDINGDETAILSHAREOUT HOLDINGS_OUT
                                  ON HOLDINGS_OUT.VC_FUND_CODE = FA_OUT.VC_FUND_CODE
                                      AND HOLDINGS_OUT.L_REPORT_DATE = FA_OUT.L_REPORT_DATE
                        LEFT JOIN ZHFX.TSTOCKINFOSHARE SIS
                                  ON HOLDINGS_OUT.VC_STOCK_CODE = SIS.VC_WIND_CODE
                                      AND HDF.L_TRADE_DATE = SIS.L_TRADE_DATE
                       ,
                    TD_ADJ,
                    END_DATE_YEAR_REPORT_DATE EDYRD
               WHERE HDF.L_TRADE_DATE = TD_ADJ.END_DATE
                 AND SIS.L_TRADE_DATE = TD_ADJ.END_DATE
                 AND FA_OUT.L_REPORT_DATE = EDYRD.REPORT_DATE
                 AND HOLDINGS_OUT.L_REPORT_DATE = EDYRD.REPORT_DATE) INFO),
     STK_DETAILS AS (
         -- 个股集中度
         SELECT PT_INFO.L_FUND_ID,
                PT_INFO.TRADE_DATE,
                PT_INFO.UF_ASSETS_CODE,
                MAX(PT_INFO.UF_ASSETS_NAME)               AS UF_ASSETS_NAME,
                MAX(PT_INFO.UF_REPORT_DATE)               AS UF_REPORT_DATE,
                MAX(PT_INFO.UF_FUND_STK_INDUSTRY)         AS UF_FUND_STK_INDUSTRY,
                MAX(PT_INFO.UF_FUND_STK_INDUSTRY_COMPLEX) AS UF_FUND_STK_INDUSTRY_COMPLEX,
                SUM(PT_INFO.FOF_POSI)                        UF_ASSETS_SUM
         FROM PENETRATE PT_INFO
         GROUP BY PT_INFO.L_FUND_ID, PT_INFO.TRADE_DATE, PT_INFO.UF_ASSETS_CODE),
     STK_PENETRATE AS (
         -- 股票行业穿透
         SELECT PT_INFO.L_FUND_ID,
                PT_INFO.TRADE_DATE,
                PT_INFO.UF_ASSETS_CODE,
                PT_INFO.UF_ASSETS_NAME,
                PT_INFO.UF_REPORT_DATE,
                PT_INFO.UF_FUND_STK_INDUSTRY,
                PT_INFO.UF_FUND_STK_INDUSTRY_COMPLEX,
                PT_INFO.FOF_POSI,
                SUM(PT_INFO.FOF_POSI)
                    OVER ( PARTITION BY PT_INFO.L_FUND_ID, PT_INFO.TRADE_DATE, PT_INFO.UF_ASSETS_CODE)               UF_ASSETS_SUM,
                SUM(PT_INFO.FOF_POSI)
                    OVER ( PARTITION BY PT_INFO.L_FUND_ID, PT_INFO.TRADE_DATE, PT_INFO.UF_FUND_STK_INDUSTRY)         IDS_SUM,
                SUM(PT_INFO.FOF_POSI)
                    OVER ( PARTITION BY PT_INFO.L_FUND_ID, PT_INFO.TRADE_DATE, PT_INFO.UF_FUND_STK_INDUSTRY_COMPLEX) IDS_CPLX_SUM
         FROM PENETRATE PT_INFO),
     STK_IDS_PENETRATE_SHOW AS (
         -- 资产一层穿透分组
         SELECT AP.L_FUND_ID,
                AP.TRADE_DATE,
                AP.UF_ASSETS_CODE,
                AP.UF_ASSETS_NAME,
                AP.UF_REPORT_DATE,
                AP.UF_FUND_STK_INDUSTRY || '【' || TO_CHAR(ROUND(AP.IDS_SUM, 4) * 100, 'fm9990.0099') ||
                '%】' AS IDS,
                AP.UF_FUND_STK_INDUSTRY_COMPLEX || '【' || TO_CHAR(ROUND(AP.IDS_CPLX_SUM, 4) * 100, 'fm9990.0099') ||
                '%】' AS IDS_CPLX,
                AP.FOF_POSI
         FROM STK_PENETRATE AP
         ORDER BY AP.L_FUND_ID,
                  AP.TRADE_DATE DESC, AP.IDS_CPLX_SUM DESC, AP.IDS_SUM DESC)

-- 股票穿透
SELECT *
FROM STK_PENETRATE

-- -- 股票穿透展示
-- SELECT *
-- FROM STK_IDS_PENETRATE_SHOW

-- -- 股票详情
-- SELECT PT_INFO.L_FUND_ID,
--        PT_INFO.TRADE_DATE,
--        PT_INFO.UF_ASSETS_CODE,
--        MAX(PT_INFO.UF_ASSETS_NAME)               AS UF_ASSETS_NAME,
--        MAX(PT_INFO.UF_REPORT_DATE)               AS UF_REPORT_DATE,
--        MAX(PT_INFO.UF_FUND_STK_INDUSTRY)         AS UF_FUND_STK_INDUSTRY,
--        MAX(PT_INFO.UF_FUND_STK_INDUSTRY_COMPLEX) AS UF_FUND_STK_INDUSTRY_COMPLEX,
--        SUM(PT_INFO.FOF_POSI)                        UF_ASSETS_SUM,
--        SUM(PT_INFO.PD_POSI)                         UF_ASSETS_PD_SUM
-- FROM PENETRATE PT_INFO
-- GROUP BY PT_INFO.L_FUND_ID, PT_INFO.TRADE_DATE, PT_INFO.UF_ASSETS_CODE
-- ORDER BY UF_ASSETS_SUM DESC
-- -- 持股详情
-- SELECT *
-- FROM PENETRATE
-- WHERE UF_ASSETS_CODE = '${UF_ASSETS_CODE}'

-- SELECT * FROM PENETRATE
