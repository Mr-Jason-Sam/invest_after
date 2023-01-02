WITH
    -- 交易日调整
    TRADE_DATE_ADJ AS (
        -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
        SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
               -- 已处理当天收益的开始日期
               MIN(L_TRADE_DATE)      BEGIN_DATE,
               MAX(L_TRADE_DATE)      END_DATE
        FROM ZHFX.TCALENDAR
        WHERE L_DATE = L_TRADE_DATE
          AND L_TRADE_DATE BETWEEN ${startdate}
            AND ${enddate}),
    FOF_PFL_PROFIT AS (
        -- FOF组合收益
        SELECT HOLD_INFO.*,
               DECODE(HOLD_INFO.FOF_MV_INIT, 0, 0,
                      HOLD_INFO.DAY_PF / HOLD_INFO.FOF_MV_INIT) DAY_PF_RATIO
        FROM (SELECT HDF.L_FUND_ID FUND_ID,
                     HDF.L_TRADE_DATE TRADE_DATE,
                     SUM(HDF.EN_MARKET_INIT) FOF_MV_INIT,
                     SUM(HDF.EN_PROFIT) DAY_PF
              FROM ZHFX.THOLDINGDETAILFUND HDF,
                   TRADE_DATE_ADJ ADJ
              WHERE HDF.L_FUND_ID = ${ztbh}
                AND HDF.L_TRADE_DATE BETWEEN ADJ.BEGIN_DATE AND ADJ.END_DATE
              GROUP BY HDF.L_FUND_ID, HDF.L_TRADE_DATE) HOLD_INFO
        ORDER BY HOLD_INFO.TRADE_DATE),
    PFL_PF_ADJ AS (
        -- 组合调整项
        SELECT PFL_PF.*,
               EXP(SUM(LN(1 + PFL_PF.DAY_PF_RATIO)) OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE)) /
               (1 + PFL_PF.DAY_PF_RATIO) PF_ADJ
        FROM FOF_PFL_PROFIT PFL_PF),
    DD_CTB AS (
        -- 回撤贡献
        SELECT HOLDING_PF.FUND_ID,
               HOLDING_PF.FUND_CODE,
               MAX(HOLDING_PF.FUND_NAME)        AS               FUND_NAME,
               MAX(HOLDING_PF.INVEST_TYPE)      AS               INVEST_TYPE,
               MAX(HOLDING_PF.WIND_SECODE_TYPE) AS               WIND_SECODE_TYPE,
               SUM(HOLDING_PF.DAY_CTB_RATIO * HOLDING_PF.PF_ADJ) PF_CTB
        FROM (SELECT HDF.L_FUND_ID AS                                                                    FUND_ID,
                     HDF.VC_STOCK_CODE                                                                   FUND_CODE,
                     MFD.VC_STOCK_NAME                                                                   FUND_NAME,
                     MFD.VC_INVEST_TYPE                                                                  INVEST_TYPE,
                     MFD.VC_FUND_TYPE_WIND_SECOND                                                        WIND_SECODE_TYPE,
                     PFL_INFO.TRADE_DATE,
                     PFL_PF_ADJ.PF_ADJ,
                     -- 日贡献率 = 日初仓位 * 日收益率 = 日收益 / 组合总市值 = (公允价值变动 + 投资收益) / 组合总市值
                     DECODE(PFL_INFO.FOF_MV_INIT, 0, 0,
                            HDF.EN_PROFIT / PFL_INFO.FOF_MV_INIT) DAY_CTB_RATIO
              FROM ZHFX.THOLDINGDETAILFUND HDF
                       LEFT JOIN FOF_PFL_PROFIT PFL_INFO
                                 ON HDF.L_FUND_ID = PFL_INFO.FUND_ID
                                     AND HDF.L_TRADE_DATE = PFL_INFO.TRADE_DATE
                       LEFT JOIN PFL_PF_ADJ
                                 ON HDF.L_FUND_ID = PFL_PF_ADJ.FUND_ID
                                     AND HDF.L_TRADE_DATE = PFL_PF_ADJ.TRADE_DATE
                       LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                                 ON HDF.VC_STOCK_CODE = MFD.VC_STOCK_CODE,
                  TRADE_DATE_ADJ TDA
              WHERE HDF.L_FUND_ID = ${ztbh}
                AND HDF.L_TRADE_DATE BETWEEN TDA.BEGIN_DATE
                  AND TDA.END_DATE) HOLDING_PF
        GROUP BY HOLDING_PF.FUND_ID, HOLDING_PF.FUND_CODE),
    CTB_HIGH AS (SELECT *
                 FROM DD_CTB
                 ORDER BY PF_CTB DESC),
    CTB_LOW AS (SELECT *
                FROM DD_CTB
                ORDER BY PF_CTB)
SELECT *
FROM ${TABLE}
