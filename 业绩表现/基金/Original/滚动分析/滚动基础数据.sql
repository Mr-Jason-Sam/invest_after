WITH
    -- 常量
    CONSTANTS AS (
        -- 常量值
        SELECT 244        ONE_YEAR_TRADE_DATE,
               365        ONE_YEAR_NATUAL_DAYS,
               'yyyymmdd' DATE_FORMAT,
               36000      CASH_YEAR_PROFIT,
               1.5        CASH_BASE,
               99991231   MAX_TRADE_DATE,
               00000000   MIN_TRADE_DATE,
               9999       MAX_DAYS,
               0          ZERO,
               100        PENCENT
        FROM DUAL),
    -- 交易日调整
    TRADE_DATE_ADJ AS (
        -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
        SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
               -- 已处理当天收益的开始日期
               MIN(L_TRADE_DATE)      BEGIN_DATE,
               MAX(L_TRADE_DATE)      END_DATE
        FROM ZHFX.TCALENDAR
        WHERE L_DATE = L_TRADE_DATE
          AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd')
            AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')),
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
    BASE_PROFIT AS (
        -- 组合收益
        SELECT ORIGIN_PF.*,
               -- 滚动收益
               DECODE(ORIGIN_PF.ROLLING_DAYS, ${period}, ORIGIN_PF.ROLLING_BASE_PF,
                      NULL) ROLLING_PROFIT,
               -- 滚动收益概率
               DECODE(ORIGIN_PF.ROLLING_DAYS, ${period},
                      CUME_DIST() OVER (PARTITION BY ORIGIN_PF.FUND_ID, DECODE(ORIGIN_PF.ROLLING_DAYS, ${period}, 0, 1) ORDER BY ORIGIN_PF.ROLLING_BASE_PF DESC),
                      NULL) ROLLING_PROFIT_PROB
        FROM (SELECT PFL_PF.*,
                     -- 区间产品收益率
                     EXP(SUM(LN(1 + PFL_PF.DAY_PF_RATIO))
                             OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE)) -
                     1                                                                                                PFL_RANGE_PF_RATIO,
                     -- 滚动收益
                     EXP(SUM(LN(1 + PFL_PF.DAY_PF_RATIO))
                             OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS ${period} - 1 PRECEDING)) -
                     1                                                                                                ROLLING_BASE_PF,
                     -- 滚动时间
                     COUNT(PFL_PF.DAY_PF_RATIO)
                           OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS ${period} - 1 PRECEDING) ROLLING_DAYS
              FROM FOF_PFL_PROFIT PFL_PF) ORIGIN_PF),
    ROLLING_PROFIT_PROB AS (
        -- 滚动收益概率分位
        SELECT BP.FUND_ID,
               PERCENTILE_CONT(0) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)    ROLLING_PF_0P,
               PERCENTILE_CONT(0.05) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT) ROLLING_PF_5P,
               PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)  ROLLING_PF_10P,
               PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)  ROLLING_PF_20P,
               PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)  ROLLING_PF_30P,
               PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)  ROLLING_PF_40P,
               PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)  ROLLING_PF_50P,
               PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)  ROLLING_PF_60P,
               PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)  ROLLING_PF_70P,
               PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)  ROLLING_PF_80P,
               PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)  ROLLING_PF_90P,
               PERCENTILE_CONT(1) WITHIN GROUP ( ORDER BY BP.ROLLING_PROFIT)    ROLLING_PF_100P
        FROM BASE_PROFIT BP
        GROUP BY BP.FUND_ID)
-- SELECT *
-- FROM BASE_PROFIT WHERE ROLLING_DAYS = ${period}
-- ORDER BY TRADE_DATE
SELECT *
FROM ROLLING_PROFIT_PROB