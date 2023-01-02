WITH CONSTANTS AS (
    -- 常量
    SELECT 'SW_1'      SW_1,
           'SW_2'      SW_2,
           'SW_3'      SW_3,
           'SEC_1'     SEC_1,
           'SEC_2'     SEC_2,
           'WIND_1'    WIND_1,
           'WIND_2'    WIND_2,
           'WIND_3'    WIND_3,
           'ZX_1'      ZX_1,
           'ZX_2'      ZX_2,
           'ZX_3'      ZX_3,
           '000300.SH' HZ_300,
           '.HK'       HK_SUFFIX,
           'HK'        HK_NAME,
           '---'       OTHERS_TAG,
           'OTHERS'    OTHERS_NAME
    FROM DUAL),
     TRADE_DATE_BEGIN_END AS (SELECT MIN(CLD.L_TRADE_DATE_LAST) L_BEGIN_DATE_LAST,
                                     MAX(CLD.L_TRADE_DATE_NEXT) L_BEGIN_DATE_NEXT,
                                     MIN(CLD.L_TRADE_DATE)      L_BEGIN_DATE,
                                     MAX(CLD.L_TRADE_DATE)      L_END_DATE
                              FROM TCALENDAR CLD
                              WHERE CLD.L_DATE = CLD.L_TRADE_DATE
                                AND CLD.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     TRADE_DATE_RANGE AS (SELECT CLD.L_DATE,
                                 CLD.L_TRADE_DATE,
                                 CLD.L_TRADE_DATE_LAST,
                                 CLD.L_TRADE_DATE_NEXT
                          FROM TCALENDAR CLD,
                               TRADE_DATE_BEGIN_END TD_BE
                          WHERE CLD.L_DATE = CLD.L_TRADE_DATE
                            AND CLD.L_DATE BETWEEN TD_BE.L_BEGIN_DATE AND TD_BE.L_END_DATE),
     STK_HOLDING_INFO_BASE AS (
         -- 基金的股票组合
         SELECT HOLDING_INFO.*,
                CASE
                    WHEN SUBSTR(HOLDING_INFO.VC_WIND_CODE, -3) = CONS.HK_SUFFIX THEN CONS.HK_NAME
                    WHEN HOLDING_INFO.INDUSTRY_TYPE_ORIGIN = CONS.OTHERS_TAG THEN CONS.OTHERS_NAME
                    ELSE NVL(HOLDING_INFO.INDUSTRY_TYPE_ORIGIN, CONS.OTHERS_NAME) END INDUSTRY_TYPE,
                DECODE(
                        SUM(HOLDING_INFO.EN_VALUE_MARKET_PRE)
                            OVER ( PARTITION BY HOLDING_INFO.L_FUND_ID, HOLDING_INFO.L_TRADE_DATE),
                        0, 0,
                        HOLDING_INFO.EN_VALUE_MARKET_PRE / SUM(HOLDING_INFO.EN_VALUE_MARKET_PRE)
                                                               OVER ( PARTITION BY HOLDING_INFO.L_FUND_ID, HOLDING_INFO.L_TRADE_DATE)
                    )                                                                 WEIGHT_PRE
         FROM (SELECT HDS.L_FUND_ID,
                      HDS.L_TRADE_DATE,
                      HDS.VC_WIND_CODE,
                      HDS.EN_VALUE_MARKET,
                      TDR.L_TRADE_DATE_NEXT,
                      HDS.EN_VALUE_MARKET_PRE,
                      DECODE(
                              ${industry_id},
                              CONS.SW_1, SIS.VC_INDUSTRY_SW_FIRST,
                              CONS.SW_2, SIS.VC_INDUSTRY_SW_SECOND,
                              CONS.SW_3, SIS.VC_INDUSTRY_SW_THIRD,
                              CONS.SEC_1, SIS.VC_INDUSTRY_SEC_FIRST,
                              CONS.SEC_2, SIS.VC_INDUSTRY_SEC_SECOND,
                              CONS.WIND_1, SIS.VC_INDUSTRY_WIND_FIRST,
                              CONS.WIND_2, SIS.VC_INDUSTRY_WIND_SECOND,
                              CONS.WIND_3, SIS.VC_INDUSTRY_WIND_THIRD,
                              SIS.VC_INDUSTRY_SW_FIRST
                          )                             INDUSTRY_TYPE_ORIGIN,
                      SIS.EN_PRICE_CLOSE,
                      SIS.EN_PRICE_CLOSE_PRE,
--                       DECODE(SIS.EN_PRICE_CLOSE_PRE, 0, 0,
--                              SIS.EN_PRICE_CLOSE / SIS.EN_PRICE_CLOSE_PRE - 1) DAY_PROFIT_RATIO,
                      NVL(HDS_IPO.EN_IPO_BALANCE, 0) AS EN_IPO_BALANCE
               FROM ZHFX.THOLDINGDETAILSHARE HDS
                        LEFT JOIN ZHFX.THOLDINGDETAILSHAREIPO HDS_IPO
                                  ON HDS.L_FUND_ID = HDS_IPO.L_FUND_ID AND HDS.VC_WIND_CODE = HDS_IPO.VC_WIND_CODE AND
                                     HDS.L_TRADE_DATE = HDS_IPO.L_TRADE_DATE
                        LEFT JOIN ZHFX.TSTOCKINFOSHARE SIS
                                  ON HDS.VC_WIND_CODE = SIS.VC_WIND_CODE AND HDS.L_TRADE_DATE = SIS.L_TRADE_DATE,
                    TRADE_DATE_RANGE TDR,
                    CONSTANTS CONS
               WHERE HDS.L_FUND_ID = ${fund_id}
                 AND HDS.L_TRADE_DATE = TDR.L_TRADE_DATE
                 AND NOT (SUBSTR(HDS.VC_STOCK_CODE, 1, 3) = '360' AND LENGTH(HDS.VC_STOCK_CODE) = 6)) HOLDING_INFO,
              CONSTANTS CONS
         WHERE HOLDING_INFO.EN_IPO_BALANCE = 0),
     FUND_SETUP_INFO AS (SELECT MIN(L_BEGIN_DATE) SETUP_DATE
                         FROM ZHFX.TFUNDINFO),
     STK_HOLDING_PROFIT AS (
         -- 处理非A股收益
         SELECT SHIB.L_FUND_ID,
                SHIB.L_TRADE_DATE,
                SHIB.VC_WIND_CODE,
                EXP(SUM(LN(DECODE(NVL(SIS.EN_PRICE_CLOSE_PRE, 0), 0, 1,
                                  NVL(SIS.EN_PRICE_CLOSE / SIS.EN_PRICE_CLOSE_PRE, 1))))) - 1 DAY_PROFIT_RATIO
         FROM (SELECT DISTINCT SHIB.L_FUND_ID, SHIB.L_TRADE_DATE, SHIB.VC_WIND_CODE
               FROM STK_HOLDING_INFO_BASE SHIB) SHIB,
              ZHFX.TSTOCKINFOSHARE SIS,
              ZHFX.TCALENDAR CLD
         WHERE SHIB.L_TRADE_DATE = CLD.L_TRADE_DATE
           AND SIS.L_TRADE_DATE = CLD.L_DATE
           AND SHIB.VC_WIND_CODE = SIS.VC_WIND_CODE
         GROUP BY SHIB.L_FUND_ID, SHIB.L_TRADE_DATE, SHIB.VC_WIND_CODE),
     STK_HOLDING_INFO AS (
         -- 融合非A股日收益率
         SELECT SHIB.*,
                SHP.DAY_PROFIT_RATIO
         FROM STK_HOLDING_INFO_BASE SHIB
                  LEFT JOIN STK_HOLDING_PROFIT SHP
                            ON SHIB.L_FUND_ID = SHP.L_FUND_ID AND SHIB.VC_WIND_CODE = SHP.VC_WIND_CODE AND
                               SHIB.L_TRADE_DATE = SHP.L_TRADE_DATE),
     BENCHMARK_HOLDING_INFO AS (
         -- 基准的股票组合
         SELECT BM_HOLDING.*,
                CASE
                    WHEN SUBSTR(BM_HOLDING.VC_STOCK_CODE, -3) = CONS.HK_SUFFIX THEN CONS.HK_NAME
                    WHEN BM_HOLDING.INDUSTRY_TYPE_ORIGIN = CONS.OTHERS_TAG THEN CONS.OTHERS_NAME
                    ELSE NVL(BM_HOLDING.INDUSTRY_TYPE_ORIGIN, CONS.OTHERS_NAME)
                    END                                                                                                                         INDUSTRY_TYPE,
                DECODE(
                        SUM(BM_HOLDING.EN_VALUE_MARKET_PRE)
                            OVER ( PARTITION BY BM_HOLDING.VC_INDEX_CODE, BM_HOLDING.L_TRADE_DATE),
                        0, 0,
                        BM_HOLDING.EN_VALUE_MARKET_PRE / SUM(BM_HOLDING.EN_VALUE_MARKET_PRE)
                                                             OVER ( PARTITION BY BM_HOLDING.VC_INDEX_CODE, BM_HOLDING.L_TRADE_DATE)
                    )                                                                                                                           WEIGHT_PRE,
                RANK() OVER (PARTITION BY BM_HOLDING.VC_INDEX_CODE,BM_HOLDING.VC_STOCK_CODE,BM_HOLDING.L_TRADE_DATE ORDER BY BM_HOLDING.L_DATE) DATE_RANK
         FROM (SELECT IMS.VC_INDEX_CODE,
                      IMS.VC_INDEX_NAME,
                      TDR.L_DATE,
                      IMS.L_TRADE_DATE,
                      IMS.VC_STOCK_CODE,
                      IMS.EN_CAPITAL_CAL * SIS.EN_PRICE_CLOSE                 EN_VALUE_MARKET,
                      IMS.EN_CAPITAL_CAL * SIS.EN_PRICE_CLOSE_PRE             EN_VALUE_MARKET_PRE,
                      DECODE(
                              ${industry_id},
                              CONS.SW_1, SIS.VC_INDUSTRY_SW_FIRST,
                              CONS.SW_2, SIS.VC_INDUSTRY_SW_SECOND,
                              CONS.SW_3, SIS.VC_INDUSTRY_SW_THIRD,
                              CONS.SEC_1, SIS.VC_INDUSTRY_SEC_FIRST,
                              CONS.SEC_2, SIS.VC_INDUSTRY_SEC_SECOND,
                              CONS.WIND_1, SIS.VC_INDUSTRY_WIND_FIRST,
                              CONS.WIND_2, SIS.VC_INDUSTRY_WIND_SECOND,
                              CONS.WIND_3, SIS.VC_INDUSTRY_WIND_THIRD,
                              SIS.VC_INDUSTRY_SW_FIRST
                          )                                                   INDUSTRY_TYPE_ORIGIN,
                      DECODE(SIS.EN_PRICE_CLOSE_PRE, 0, 0,
                             SIS.EN_PRICE_CLOSE / SIS.EN_PRICE_CLOSE_PRE - 1) DAY_PROFIT_RATIO
               FROM ZHFX.TINDEXMEMBERSHARE IMS
                        LEFT JOIN ZHFX.TSTOCKINFOSHARE SIS
                                  ON IMS.L_TRADE_DATE = SIS.L_TRADE_DATE AND IMS.VC_STOCK_CODE = SIS.VC_WIND_CODE,
                    TRADE_DATE_RANGE TDR,
                    CONSTANTS CONS,
                    FUND_SETUP_INFO SPH
               WHERE IMS.VC_INDEX_CODE = ${stk_index_code}
                 AND IMS.L_TRADE_DATE = TDR.L_DATE
                 AND IMS.L_TRADE_DATE >= SPH.SETUP_DATE) BM_HOLDING,
              CONSTANTS CONS),
    BENCHMARK_BASE_INFO AS (
        SELECT MAX(VC_INDEX_CODE) INDEX_CODE, MAX(VC_INDEX_NAME) INDEX_NAME FROM BENCHMARK_HOLDING_INFO
    ),
    PD_BASE_INFO AS (
        SELECT L_FUND_ID FROM ZHFX.TFUNDINFO WHERE L_FUND_ID = ${fund_id}
    ),
     STK_BM_INDUSTRY_INFO AS (
         -- 股票组合和基准的行业信息
         SELECT PFL_IDS_INFO.L_FUND_ID,
                BM_INS_INFO.VC_INDEX_CODE,
                BM_INS_INFO.VC_INDEX_NAME,
                NVL(PFL_IDS_INFO.L_TRADE_DATE, BM_INS_INFO.L_TRADE_DATE)   AS L_TRADE_DATE,
                NVL(PFL_IDS_INFO.INDUSTRY_TYPE, BM_INS_INFO.INDUSTRY_TYPE) AS INDUSTRY_TYPE,
                NVL(PFL_IDS_INFO.PFL_IDS_DAY_PROFIT_RATIO, 0)              AS PFL_IDS_DAY_PROFIT_RATIO,
                NVL(PFL_IDS_INFO.PFL_IDS_WEIGHT_PRE, 0)                    AS PFL_IDS_WEIGHT_PRE,
                NVL(PFL_IDS_INFO.PFL_IDS_DAY_PROFIT, 0)                    AS PFL_IDS_DAY_PROFIT,
                NVL(BM_INS_INFO.BM_IDS_DAY_PROFIT_RATIO, 0)                AS BM_IDS_DAY_PROFIT_RATIO,
                CASE
                    WHEN SUM(NVL(BM_INS_INFO.BM_IDS_WEIGHT_PRE, 0)) OVER (PARTITION BY BM_INS_INFO.L_TRADE_DATE) = 0 AND
                         BM_INS_INFO.INDUSTRY_TYPE = CONS.HK_NAME THEN 1.0
                    ELSE NVL(BM_INS_INFO.BM_IDS_WEIGHT_PRE, 0) END         AS BM_IDS_WEIGHT_PRE
         FROM (SELECT SHI.*,
                      DECODE(
                              SHI.PFL_IDS_WEIGHT_PRE,
                              0, 0,
                              SHI.PFL_IDS_PF_SUM / SHI.PFL_IDS_WEIGHT_PRE) PFL_IDS_DAY_PROFIT_RATIO
               FROM (SELECT L_FUND_ID,
                            L_TRADE_DATE,
                            INDUSTRY_TYPE,
                            SUM(WEIGHT_PRE * DAY_PROFIT_RATIO)          PFL_IDS_PF_SUM,
                            SUM(WEIGHT_PRE)                             PFL_IDS_WEIGHT_PRE,
                            SUM(EN_VALUE_MARKET_PRE * DAY_PROFIT_RATIO) PFL_IDS_DAY_PROFIT
                     FROM STK_HOLDING_INFO
                     GROUP BY L_FUND_ID, L_TRADE_DATE, INDUSTRY_TYPE) SHI) PFL_IDS_INFO
                  FULL JOIN (SELECT BHI.*,
                                    DECODE(
                                            BHI.BM_IDS_WEIGHT_PRE,
                                            0, 0,
                                            BHI.BM_IDS_DAY_PF_SUM / BHI.BM_IDS_WEIGHT_PRE) BM_IDS_DAY_PROFIT_RATIO
                             FROM (SELECT BHI.VC_INDEX_CODE,
                                          BHI.VC_INDEX_NAME,
                                          BHI.L_TRADE_DATE,
                                          BHI.INDUSTRY_TYPE,
                                          SUM(BHI.WEIGHT_PRE * BHI.DAY_PROFIT_RATIO) BM_IDS_DAY_PF_SUM,
                                          SUM(BHI.WEIGHT_PRE)                        BM_IDS_WEIGHT_PRE
                                   FROM (SELECT BHI.L_TRADE_DATE,
                                                BHI.VC_INDEX_CODE,
                                                MAX(BHI.VC_INDEX_NAME)                           AS VC_INDEX_NAME,
                                                BHI.VC_STOCK_CODE,
                                                SUM(DECODE(BHI.DATE_RANK, 1, BHI.WEIGHT_PRE, 0)) AS WEIGHT_PRE,
                                                EXP(SUM(LN(1 + BHI.DAY_PROFIT_RATIO))) - 1       AS DAY_PROFIT_RATIO,
                                                MAX(BHI.INDUSTRY_TYPE)                           AS INDUSTRY_TYPE
                                         FROM BENCHMARK_HOLDING_INFO BHI
                                         GROUP BY BHI.L_TRADE_DATE, BHI.VC_INDEX_CODE, BHI.VC_STOCK_CODE) BHI
                                   GROUP BY VC_INDEX_CODE, VC_INDEX_NAME, L_TRADE_DATE, INDUSTRY_TYPE) BHI) BM_INS_INFO
                            ON PFL_IDS_INFO.L_TRADE_DATE = BM_INS_INFO.L_TRADE_DATE AND
                               PFL_IDS_INFO.INDUSTRY_TYPE = BM_INS_INFO.INDUSTRY_TYPE,
              CONSTANTS CONS),
     STK_BM_INDUSTRY_DPR AS (
         -- 产品与基准的日收益率
         SELECT PFL.L_FUND_ID,
                BM.VC_INDEX_CODE,
                BM.VC_INDEX_NAME,
                NVL(PFL.L_TRADE_DATE, BM.L_TRADE_DATE) AS L_TRADE_DATE,
                NVL(PFL.PFL_DAY_PROFIT_RATIO, 0)       AS PFL_DAY_PROFIT_RATIO,
                NVL(BM.BM_DAY_PROFIT_RATIO, 0)         AS BM_DAY_PROFIT_RATIO
         FROM (SELECT L_FUND_ID, L_TRADE_DATE, SUM(WEIGHT_PRE * DAY_PROFIT_RATIO) PFL_DAY_PROFIT_RATIO
               FROM STK_HOLDING_INFO
               GROUP BY L_FUND_ID, L_TRADE_DATE) PFL
                  FULL JOIN (SELECT VC_INDEX_CODE,
                                    VC_INDEX_NAME,
                                    L_TRADE_DATE,
                                    SUM(WEIGHT_PRE * DAY_PROFIT_RATIO) BM_DAY_PROFIT_RATIO
                             FROM BENCHMARK_HOLDING_INFO
                             GROUP BY VC_INDEX_CODE, VC_INDEX_NAME, L_TRADE_DATE) BM
                            ON PFL.L_TRADE_DATE = BM.L_TRADE_DATE),
     STK_BM_INDUSTRY_PFL_ADJUST AS (
         -- 调整项
         SELECT L_FUND_ID,
                VC_INDEX_CODE,
                L_TRADE_DATE,
                EXP(SUM(LN(1 + PFL_DAY_PROFIT_RATIO)) OVER (ORDER BY L_TRADE_DATE)) /
                (1 + PFL_DAY_PROFIT_RATIO) STK_PFL_PROFIT_ADJ,
                EXP(SUM(LN(1 + BM_DAY_PROFIT_RATIO)) OVER (ORDER BY L_TRADE_DATE DESC)) /
                (1 + BM_DAY_PROFIT_RATIO)  BM_PROFIT_ADJ,
                EXP(SUM(LN(1 + PFL_DAY_PROFIT_RATIO)) OVER (ORDER BY L_TRADE_DATE)) / (1 + PFL_DAY_PROFIT_RATIO) *
                EXP(SUM(LN(1 + BM_DAY_PROFIT_RATIO)) OVER (ORDER BY L_TRADE_DATE DESC)) /
                (1 + BM_DAY_PROFIT_RATIO)  RELATIVE_PROFIT_ADJ
         FROM STK_BM_INDUSTRY_DPR STK_BM_PROFIT),
     PROFIT_CTB AS (
         -- 收益贡献
         SELECT CTB_BASE.*,
                CTB_BASE.SELECTOR_PROFIT_CTB + CTB_BASE.CONFIG_PROFIT_CTB ALPHA_PROFIT_CTB
         FROM (
                  -- 基础数据
                  SELECT MIN(SBII.L_TRADE_DATE)
                             OVER (PARTITION BY 1)                                             BEGIN_DATE,
                         MAX(SBII.L_TRADE_DATE)
                             OVER (PARTITION BY 1)                                             END_DATE,
                         SBII.L_FUND_ID,
                         SBII.VC_INDEX_CODE,
                         SBII.VC_INDEX_NAME,
                         SBII.L_TRADE_DATE,
                         SBII.INDUSTRY_TYPE,
                         -- 股票组合收益金额 = T-1市值 * T日收益率
                         SUM(SBII.PFL_IDS_DAY_PROFIT)
                             OVER (PARTITION BY SBII.INDUSTRY_TYPE ORDER BY SBII.L_TRADE_DATE) STK_PFL_PROFIT_VALUE,
                         -- 组合贡献 = SUM(个股T-1权重 * 个股T日收益率 * 产品T-1累计净值【即产品本身调整项】)
                         SUM(SBII.PFL_IDS_WEIGHT_PRE * SBII.PFL_IDS_DAY_PROFIT_RATIO *
                             SBPA.STK_PFL_PROFIT_ADJ)
                             OVER (PARTITION BY SBII.INDUSTRY_TYPE ORDER BY SBII.L_TRADE_DATE) STK_PFL_PROFIT_CTB,
                         -- 基准贡献 同上
                         SUM(SBII.BM_IDS_WEIGHT_PRE * SBII.BM_IDS_DAY_PROFIT_RATIO *
                             SBPA.BM_PROFIT_ADJ)
                             OVER (PARTITION BY SBII.INDUSTRY_TYPE ORDER BY SBII.L_TRADE_DATE) BM_PFL_PROFIT_CTB,
                         -- 配置贡献 = (组合行业权重 - 基准行业权重) * (基准行业收益率 - 基准收益率)
                         -- 选股贡献 = 基准行业权重 * (组合行业收益率 - 基准行业收益率)
                         -- 交互贡献 = (组合行业权重 - 基准行业权重) * (组合行业收益率 - 基准行业收益率)
                         -- 超额贡献 = 配置贡献 + 选股贡献 + 交互贡献
                         -- 自上而下投资：选股贡献 = 选股贡献 + 交互贡献 = 基准行业权重 * (组合行业收益率 - 基准行业收益率) + (组合行业权重 - 基准行业权重) * (组合行业收益率 - 基准行业收益率) = 组合行业权重 * (组合行业收益率 - 基准行业收益率)
                         -- 自下而上投资：配置贡献 = 配置贡献 + 交互贡献 = (组合行业权重 - 基准行业权重) * (基准行业收益率 - 基准收益率) + (组合行业权重 - 基准行业权重) * (组合行业收益率 - 基准行业收益率) = (组合行业权重 - 基准行业权重) * (组合行业收益率 - 基准收益率)
                         -- 除以SBPA.BM_PROFIT_ADJ原因：每一期往后的调整项去除
                         CASE
                             WHEN ${invest_top_to_down} = 1 THEN SUM(SBII.PFL_IDS_WEIGHT_PRE *
                                                                     (SBII.PFL_IDS_DAY_PROFIT_RATIO - SBII.BM_IDS_DAY_PROFIT_RATIO) *
                                                                     SBPA.RELATIVE_PROFIT_ADJ)
                                                                     OVER (PARTITION BY SBII.INDUSTRY_TYPE ORDER BY SBII.L_TRADE_DATE) /
                                                                 SBPA.BM_PROFIT_ADJ
                             ELSE SUM(SBII.BM_IDS_WEIGHT_PRE *
                                      (SBII.PFL_IDS_DAY_PROFIT_RATIO - SBII.BM_IDS_DAY_PROFIT_RATIO) *
                                      SBPA.RELATIVE_PROFIT_ADJ)
                                      OVER (PARTITION BY SBII.INDUSTRY_TYPE ORDER BY SBII.L_TRADE_DATE) /
                                  SBPA.BM_PROFIT_ADJ
                             END                                                               SELECTOR_PROFIT_CTB,
                         CASE
                             WHEN ${invest_top_to_down} = 1 THEN SUM(
                                                                             (SBII.PFL_IDS_WEIGHT_PRE - SBII.BM_IDS_WEIGHT_PRE) *
                                                                             (SBII.BM_IDS_DAY_PROFIT_RATIO - SBIDPR.BM_DAY_PROFIT_RATIO) *
                                                                             SBPA.RELATIVE_PROFIT_ADJ)
                                                                             OVER (PARTITION BY SBII.INDUSTRY_TYPE ORDER BY SBII.L_TRADE_DATE) /
                                                                 SBPA.BM_PROFIT_ADJ
                             ELSE SUM((SBII.PFL_IDS_WEIGHT_PRE - SBII.BM_IDS_WEIGHT_PRE) *
                                      (SBII.PFL_IDS_DAY_PROFIT_RATIO - SBIDPR.BM_DAY_PROFIT_RATIO) *
                                      SBPA.RELATIVE_PROFIT_ADJ)
                                      OVER (PARTITION BY SBII.INDUSTRY_TYPE ORDER BY SBII.L_TRADE_DATE) /
                                  SBPA.BM_PROFIT_ADJ
                             END                                                               CONFIG_PROFIT_CTB
                  FROM STK_BM_INDUSTRY_INFO SBII,
                       STK_BM_INDUSTRY_DPR SBIDPR,
                       STK_BM_INDUSTRY_PFL_ADJUST SBPA
                  WHERE SBII.L_TRADE_DATE = SBIDPR.L_TRADE_DATE
                    AND SBII.L_TRADE_DATE = SBPA.L_TRADE_DATE) CTB_BASE)
SELECT L_TRADE_DATE,
       PD_BASE_INFO.L_FUND_ID,
       BENCHMARK_BASE_INFO.INDEX_CODE,
       BENCHMARK_BASE_INFO.INDEX_NAME,
       PF_CTB.BEGIN_DATE,
       PF_CTB.END_DATE,
       PF_CTB.INDUSTRY_TYPE,
       PF_CTB.STK_PFL_PROFIT_VALUE,
       PF_CTB.STK_PFL_PROFIT_CTB,
       PF_CTB.BM_PFL_PROFIT_CTB,
       PF_CTB.SELECTOR_PROFIT_CTB,
       PF_CTB.CONFIG_PROFIT_CTB,
       PF_CTB.ALPHA_PROFIT_CTB
FROM PROFIT_CTB PF_CTB, BENCHMARK_BASE_INFO, PD_BASE_INFO
ORDER BY L_TRADE_DATE DESC, INDUSTRY_TYPE
;
