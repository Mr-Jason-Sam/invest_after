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
     HOLDINGS_BASE_INFO AS (
         -- 基础持仓数据
         SELECT HDF.*
         FROM ZHFX.THOLDINGDETAILFUND HDF,
              TD_ADJ
         WHERE HDF.L_FUND_ID = ${ztbh}
           AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE),
     FUND_DD_INFO AS (
         -- 基金回撤信息
         SELECT DD_INFO_DETAILS.*,
                ROW_NUMBER() OVER (
                    PARTITION BY DD_INFO_DETAILS.L_FUND_ID,
                        DD_INFO_DETAILS.VC_STOCK_CODE
                    ORDER BY DD_INFO_DETAILS.MAX_DD
                    ) DD_NUM,
                ROW_NUMBER() OVER (
                    PARTITION BY DD_INFO_DETAILS.L_FUND_ID,
                        DD_INFO_DETAILS.VC_STOCK_CODE
                    ORDER BY DD_INFO_DETAILS.DD_DATE DESC
                    ) RECENT_DD_DATE_NUM
         FROM (SELECT DD_INFO.L_FUND_ID,
                      DD_INFO.VC_STOCK_CODE,
                      DD_INFO.MAX_RANGE_PROFIT,
                      MIN(DD_INFO.L_TRADE_DATE)  DD_BEGIN_DATE,
                      MAX(DD_INFO.NEXT_NAV_DATE) FIX_DD_DATE,
                      MAX(DD_INFO.L_TRADE_DATE)  DD_END_DATE,
                      MIN(DD_INFO.FUND_DD)       MAX_DD,
                      MIN(DECODE(DD_INFO.RANGE_PROFIT_RATIO, DD_INFO.MIN_RANGE_PROFIT, DD_INFO.L_TRADE_DATE,
                                 '99991231'))    DD_DATE
               FROM (SELECT FP.*,
                            (1 + FP.RANGE_PROFIT_RATIO) / (1 + FP.MAX_RANGE_PROFIT) - 1 FUND_DD,
                            MIN(FP.RANGE_PROFIT_RATIO)
                                OVER (PARTITION BY FP.L_FUND_ID, FP.MAX_RANGE_PROFIT)   MIN_RANGE_PROFIT
                     FROM (SELECT FP.L_FUND_ID,
                                  FP.L_TRADE_DATE,
                                  FP.VC_STOCK_CODE,
                                  FP.RANGE_PROFIT_RATIO,
                                  -- 区间最大收益率
                                  GREATEST(MAX(FP.RANGE_PROFIT_RATIO)
                                               OVER (PARTITION BY FP.L_FUND_ID, FP.VC_STOCK_CODE ORDER BY FP.L_TRADE_DATE),
                                           0)                                                    MAX_RANGE_PROFIT,
                                  -- 下一净值日
                                  LEAD(FP.L_TRADE_DATE, 1, '99991231')
                                       OVER (PARTITION BY FP.L_FUND_ID ORDER BY FP.L_TRADE_DATE) NEXT_NAV_DATE
                           FROM (SELECT HDF.*,
                                        EXP(SUM(LN(DECODE(HDF.EN_MARKET_INIT, 0, 0,
                                                          HDF.EN_PROFIT / HDF.EN_MARKET_INIT) + 1))
                                                OVER (PARTITION BY HDF.L_FUND_ID, HDF.VC_STOCK_CODE ORDER BY HDF.L_TRADE_DATE)) RANGE_PROFIT_RATIO
                                 FROM HOLDINGS_BASE_INFO HDF) FP) FP
                     WHERE FP.RANGE_PROFIT_RATIO != FP.MAX_RANGE_PROFIT) DD_INFO
               GROUP BY DD_INFO.L_FUND_ID,
                        DD_INFO.VC_STOCK_CODE,
                        DD_INFO.MAX_RANGE_PROFIT) DD_INFO_DETAILS),
     FUND_MAX_DD_RESULT AS (SELECT FDI.*
                            FROM FUND_DD_INFO FDI
                            WHERE DD_NUM = 1),
     FUND_RECENT_DD_RESULT AS (SELECT FDI.*
                               FROM FUND_DD_INFO FDI
                               WHERE RECENT_DD_DATE_NUM = 1),
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
                EXP(SUM(LN(1 + HRI.DAY_PF_RATIO))) - 1                           REPORT_PEIOD_PF_RATIO,
                -- 持有期收益率
                EXP(SUM(LN(DECODE(HDF.EN_MARKET_INIT, 0, 0,
                                  HDF.EN_PROFIT / HDF.EN_MARKET_INIT) + 1))) - 1 HOLDING_PF_RATIO,
                -- 持有期收益金额
                SUM(HDF.EN_PROFIT)                                               HOLDING_PF_VALUE
         FROM HOLDINGS_BASE_INFO HDF
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON HDF.VC_STOCK_CODE = MFD.VC_STOCK_CODE
                  LEFT JOIN HOLDINGS_REPORT_INFO HRI
                            ON HDF.L_TRADE_DATE = HRI.L_TRADE_DATE
                                AND MFD.VC_WIND_CODE = HRI.VC_WIND_CODE
         GROUP BY HDF.L_FUND_ID, HDF.VC_STOCK_CODE),
     FUND_FPM AS (SELECT FPI.L_FUND_ID,
                         FPI.VC_STOCK_CODE,
                         -- 持有收益率
                         FPI.REPORT_PEIOD_PF_RATIO,
                         -- 实际收益率
                         FPI.HOLDING_PF_RATIO,
                         -- 实际收益金额
                         FPI.HOLDING_PF_VALUE,
                         -- 最大回撤
                         FMDR.MAX_DD AS MAX_DD,
                         -- 最近回撤
                         FRDR.MAX_DD AS RECENT_DD
                  FROM FUND_PROFIT_INFO FPI
                           LEFT JOIN FUND_MAX_DD_RESULT FMDR
                                     ON FPI.L_FUND_ID = FMDR.L_FUND_ID
                                         AND FPI.VC_STOCK_CODE = FMDR.VC_STOCK_CODE
                           LEFT JOIN FUND_RECENT_DD_RESULT FRDR
                                     ON FPI.L_FUND_ID = FRDR.L_FUND_ID
                                         AND FPI.VC_STOCK_CODE = FRDR.VC_STOCK_CODE),
     FOF_PFL_PROFIT AS (
         -- FOF组合收益
         SELECT HOLD_INFO.*,
                DECODE(HOLD_INFO.FOF_MV_INIT, 0, 0,
                       HOLD_INFO.DAY_PF / HOLD_INFO.FOF_MV_INIT) DAY_PF_RATIO
         FROM (SELECT HDF.L_FUND_ID            FUND_ID,
                      HDF.L_TRADE_DATE         TRADE_DATE,
                      SUM(HDF.EN_VALUE_MARKET) FOF_MV,
                      SUM(HDF.EN_MARKET_INIT)  FOF_MV_INIT,
                      SUM(HDF.EN_PROFIT)       DAY_PF
               FROM ZHFX.THOLDINGDETAILFUND HDF,
                    TD_ADJ ADJ
               WHERE HDF.L_FUND_ID = ${ztbh}
                 AND HDF.L_TRADE_DATE BETWEEN ADJ.BEGIN_DATE AND ADJ.END_DATE
               GROUP BY HDF.L_FUND_ID, HDF.L_TRADE_DATE) HOLD_INFO
         ORDER BY HOLD_INFO.TRADE_DATE),
     HOLDING_POSITION_INFO AS (
         -- 持仓仓位信息
         SELECT POSI_INFO.L_FUND_ID,
                POSI_INFO.VC_STOCK_CODE,
                COUNT(FOF_POSITION) HOLDING_DAYS,
                MIN(FOF_POSITION)   MIN_POSI,
                MAX(FOF_POSITION)   MAX_POSI,
                AVG(FOF_POSITION)   AVG_POSI,
                MIN(PD_POSITION)    MIN_PD_POSI,
                MAX(PD_POSITION)    MAX_PD_POSI,
                AVG(PD_POSITION)    AVG_PD_POSI
         FROM (SELECT HDF.L_FUND_ID,
                      HDF.L_TRADE_DATE,
                      HDF.VC_STOCK_CODE,
                      DECODE(FPP.FOF_MV, 0, 0, HDF.EN_VALUE_MARKET / FPP.FOF_MV)                       FOF_POSITION,
                      DECODE(FRD.EN_FUND_ASSET_NET, 0, 0, HDF.EN_VALUE_MARKET / FRD.EN_FUND_ASSET_NET) PD_POSITION
               FROM HOLDINGS_BASE_INFO HDF
                        LEFT JOIN FOF_PFL_PROFIT FPP
                                  ON HDF.L_FUND_ID = FPP.FUND_ID
                                      AND HDF.L_TRADE_DATE = FPP.TRADE_DATE
                        LEFT JOIN ZHFX.TFUNDRETURNDETAIL FRD
                                  ON HDF.L_FUND_ID = FRD.L_FUND_ID
                                      AND HDF.L_TRADE_DATE = FRD.L_TRADE_DATE) POSI_INFO
         GROUP BY POSI_INFO.L_FUND_ID, POSI_INFO.VC_STOCK_CODE),
     ALPHA_INFO AS (
         -- 调整项信息
         SELECT FPP.FUND_ID                                                                            L_FUND_ID,
                FPP.TRADE_DATE                                                                         L_TRADE_DATE,
                EXP(SUM(LN(1 + FPP.DAY_PF_RATIO)) OVER (ORDER BY TRADE_DATE)) / (1 + FPP.DAY_PF_RATIO) ALPHA
         FROM FOF_PFL_PROFIT FPP),
     ITEM_PROFIT_CTB AS (
         -- 收益贡献
         -- 贡献 = sum(日收益金额 / T-1总市值 * ALPHA)
         SELECT HDF.L_FUND_ID,
                HDF.VC_STOCK_CODE,
                SUM(DECODE(FPP.FOF_MV_INIT, 0, 0, HDF.EN_PROFIT / FPP.FOF_MV_INIT) * AI.ALPHA) CTB
         FROM HOLDINGS_BASE_INFO HDF
                  LEFT JOIN FOF_PFL_PROFIT FPP
                            ON HDF.L_FUND_ID = FPP.FUND_ID
                                AND HDF.L_TRADE_DATE = FPP.TRADE_DATE
                  LEFT JOIN ALPHA_INFO AI
                            ON HDF.L_FUND_ID = AI.L_FUND_ID
                                AND HDF.L_TRADE_DATE = AI.L_TRADE_DATE
         GROUP BY HDF.L_FUND_ID,
                  HDF.VC_STOCK_CODE),
     LATEST_ASSETS_INFO AS (
         -- 最新贡献资产信息
         SELECT HDF.L_FUND_ID,
                HDF.L_TRADE_DATE,
                HDF.VC_STOCK_CODE,
                DECODE(FRD.EN_FUND_ASSET_NET, 0, 0, HDF.EN_VALUE_MARKET / FRD.EN_FUND_ASSET_NET) PD_POSITION
         FROM HOLDINGS_BASE_INFO HDF
                  LEFT JOIN ZHFX.TFUNDRETURNDETAIL FRD
                            ON HDF.L_FUND_ID = FRD.L_FUND_ID
                                AND HDF.L_TRADE_DATE = FRD.L_TRADE_DATE,
              TD_ADJ
         WHERE FRD.L_TRADE_DATE = TD_ADJ.END_DATE),
     FUND_ANAYSIS AS (
         -- 贡献分析
         SELECT PC.*,
                HPI.HOLDING_DAYS,
                HPI.MIN_POSI,
                HPI.AVG_POSI,
                HPI.MAX_POSI,
                HPI.MIN_PD_POSI,
                HPI.AVG_PD_POSI,
                HPI.MAX_PD_POSI,
                NVL(LAI.PD_POSITION, 0)      AS PD_POSITION,
                MFD.VC_STOCK_NAME,
                MFD.VC_FUND_TYPE_WIND,
                MFD.VC_FUND_TYPE_WIND_SECOND,

                PFM.REPORT_PEIOD_PF_RATIO,
                PFM.HOLDING_PF_RATIO,
                -- 单位：万元
                PFM.HOLDING_PF_VALUE / 10000 AS HOLDING_PF_VALUE,
                PFM.MAX_DD,
                PFM.RECENT_DD
         FROM ITEM_PROFIT_CTB PC
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON PC.VC_STOCK_CODE = MFD.VC_STOCK_CODE
                  LEFT JOIN HOLDING_POSITION_INFO HPI
                            ON PC.L_FUND_ID = HPI.L_FUND_ID
                                AND PC.VC_STOCK_CODE = HPI.VC_STOCK_CODE
                  LEFT JOIN FUND_FPM PFM
                            ON PC.L_FUND_ID = PFM.L_FUND_ID
                                AND PC.VC_STOCK_CODE = PFM.VC_STOCK_CODE
                  LEFT JOIN LATEST_ASSETS_INFO LAI
                            ON PC.L_FUND_ID = LAI.L_FUND_ID
                                AND PC.VC_STOCK_CODE = LAI.VC_STOCK_CODE
         ORDER BY PC.L_FUND_ID, PC.VC_STOCK_CODE),
     CTB_FP_RANK AS (SELECT FA.*,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY CTB DESC)                   CTB_RANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY AVG_POSI DESC)              AVG_POSI_RANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY HOLDING_PF_RATIO DESC)      HOLDING_PF_RATIO_RANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY HOLDING_PF_VALUE DESC)      HOLDING_PF_VALUE_RANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY REPORT_PEIOD_PF_RATIO DESC) REPORT_PEIOD_PF_RATIO_RANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY HOLDING_DAYS DESC)          HOLDING_DAYS_RANK,

                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY CTB)                        CTB_RERANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY AVG_POSI)                   AVG_POSI_RERANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY HOLDING_PF_RATIO)           HOLDING_PF_RATIO_RERANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY HOLDING_PF_VALUE)           HOLDING_PF_VALUE_RERANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY REPORT_PEIOD_PF_RATIO)      REPORT_PEIOD_PF_RATIO_RERANK,
                            ROW_NUMBER() OVER (PARTITION BY FA.L_FUND_ID ORDER BY HOLDING_DAYS)               HOLDING_DAYS_RERANK
                     FROM FUND_ANAYSIS FA)


-- 贡献排行
SELECT CFR.*
FROM CTB_FP_RANK CFR
