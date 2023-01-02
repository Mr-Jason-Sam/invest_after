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
          AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd')
              AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')),
    DATE_INFO AS (
        -- 获取区间交易日内的信息
        SELECT CLD.L_TRADE_DATE,
               CLD.L_TRADE_DATE_LAST,
               CLD.L_TRADE_DATE_NEXT,
               CLD.L_DATE_DIFF_LAST
        FROM ZHFX.TCALENDAR CLD,
             TRADE_DATE_ADJ ADJ
        WHERE CLD.L_DATE = CLD.L_TRADE_DATE
          AND CLD.L_DATE BETWEEN ADJ.BEGIN_DATE AND ADJ.END_DATE),
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
    STK_INDEX_PROFIT AS (
        -- 股票指数收益
        SELECT CLD.L_TRADE_DATE,
               IDX_INFO.VC_WIND_CODE       CODE,
               MAX(IDX_INFO.VC_STOCK_NAME) NAME,
               EXP(SUM(LN(1 + IDX_INFO.IDX_DAY_PF_RATIO))) * EXP(SUM(LN(1 + IDX_INFO.EN_EXCHRATE_PROFIT))) -
               1                           DAY_PF_RATIO
        FROM (SELECT STK_IDX.L_TRADE_DATE,
                     STK_IDX.VC_WIND_CODE,
                     STK_IDX.VC_STOCK_NAME,
                     DECODE(
                             STK_IDX.EN_PRICE_CLOSE_PRE, 0, 0,
                             STK_IDX.EN_PRICE_CLOSE / STK_IDX.EN_PRICE_CLOSE_PRE - 1
                         ) IDX_DAY_PF_RATIO,
                     STK_IDX.EN_EXCHRATE_PROFIT
              FROM ZHFX.TINDEXINFOSHARE STK_IDX
              WHERE STK_IDX.VC_WIND_CODE = '${stk_index_code}'
                AND STK_IDX.L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd') AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')) IDX_INFO,
             ZHFX.TCALENDAR CLD
        WHERE IDX_INFO.L_TRADE_DATE = CLD.L_DATE
        GROUP BY IDX_INFO.VC_WIND_CODE, CLD.L_TRADE_DATE),
    BOND_INDEX_PROFIT AS (
        -- 债券指数收益
        SELECT BOND_IDX.L_TRADE_DATE,
               BOND_IDX.VC_WIND_CODE,
               BOND_IDX.VC_STOCK_NAME,
               DECODE(
                       BOND_IDX.EN_PRICE_CLOSE_PRE, 0, 0,
                       BOND_IDX.EN_PRICE_CLOSE / BOND_IDX.EN_PRICE_CLOSE_PRE - 1
                   ) DAY_PF_RATIO
        FROM ZHFX.TINDEXINFOBOND BOND_IDX
        WHERE BOND_IDX.L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd') AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')
          AND BOND_IDX.VC_WIND_CODE = '${bond_index_code}'),
    CASH_INDEX_PROFIT AS (
        -- 现金指数收益
        SELECT CASH_IDX_INFO.*,
               DATE_INFO.L_DATE_DIFF_LAST DIFF_LAST,
               DATE_INFO.L_TRADE_DATE
        FROM (SELECT CBOND_BM.S_INFO_WINDCODE,
                     TO_NUMBER(CBOND_BM.TRADE_DT)                                                 EFF_DT,
                     LEAD(TO_NUMBER(CBOND_BM.TRADE_DT), 1, CONS.MAX_TRADE_DATE)
                          OVER (PARTITION BY CBOND_BM.S_INFO_WINDCODE ORDER BY CBOND_BM.TRADE_DT) NEXT_EFF_DT,
                     CBOND_BM.B_INFO_RATE / CONS.PENCENT                                          RATE
              FROM STAGE.WIND2_CBONDBENCHMARK@DBLINK_DC CBOND_BM,
                   CONSTANTS CONS
              WHERE CBOND_BM.S_INFO_WINDCODE = '${cash_index_code}'
                AND CBOND_BM.TRADE_DT >= '20150101') CASH_IDX_INFO
                 RIGHT JOIN DATE_INFO
                            ON DATE_INFO.L_TRADE_DATE >= CASH_IDX_INFO.EFF_DT
                                AND DATE_INFO.L_TRADE_DATE < CASH_IDX_INFO.NEXT_EFF_DT),
    FUND_INDEX_PROFIT AS (
        -- 基金指数收益
        SELECT FUND_IDX.L_TRADE_DATE,
               FUND_IDX.VC_WIND_CODE,
               FUND_IDX.VC_STOCK_NAME,
               DECODE(
                       FUND_IDX.EN_PRICE_CLOSE_PRE, 0, 0,
                       FUND_IDX.EN_PRICE_CLOSE / FUND_IDX.EN_PRICE_CLOSE_PRE - 1
                   ) DAY_PF_RATIO
        FROM ZHFX.TINDEXINFOFUND FUND_IDX
        WHERE FUND_IDX.L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd') AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')
          AND FUND_IDX.VC_WIND_CODE = '${fund_index_code}'),
    BENCHMARK_PROFIT AS (
        -- 基准组合
        SELECT PRO_PF.TRADE_DATE,
               PRO_PF.FUND_ID                                                       BASE_FUND_ID,
               STK_PF.DAY_PF_RATIO                                                  STK_DAY_PF_RATIO,
               BOND_PF.DAY_PF_RATIO                                                 BOND_DAY_PF_RATIO,
               CASH_PF.RATE                                                         CASH_RATE,
               FUND_PF.DAY_PF_RATIO                                                 FUND_DAY_PF_RATIO,
               NVL(${stk_weight} / 100, 0) * NVL(STK_PF.DAY_PF_RATIO, 0) +
               NVL(${bond_weight} / 100, 0) * NVL(BOND_PF.DAY_PF_RATIO, 0) +
               NVL(${cash_weight} / 100, 0) * NVL(CASH_PF.RATE / CONS.ONE_YEAR_NATUAL_DAYS * CASH_PF.DIFF_LAST, 0) +
               NVL(${fund_weight} / 100, 0) * NVL(FUND_PF.DAY_PF_RATIO, 0)          BM_DAY_PF_RATIO,
               NVL(CASH_PF.RATE / CONS.ONE_YEAR_NATUAL_DAYS * CASH_PF.DIFF_LAST, 0) FREE_RISK_RATE
        FROM FOF_PFL_PROFIT PRO_PF,
             STK_INDEX_PROFIT STK_PF,
             BOND_INDEX_PROFIT BOND_PF,
             CASH_INDEX_PROFIT CASH_PF,
             FUND_INDEX_PROFIT FUND_PF,
             CONSTANTS CONS
        WHERE PRO_PF.TRADE_DATE = STK_PF.L_TRADE_DATE(+)
          AND PRO_PF.TRADE_DATE = BOND_PF.L_TRADE_DATE(+)
          AND PRO_PF.TRADE_DATE = CASH_PF.L_TRADE_DATE(+)
          AND PRO_PF.TRADE_DATE = FUND_PF.L_TRADE_DATE(+)),
    BASE_PROFIT AS (
        -- 组合收益
        SELECT BASE_PF.*,
               -- 区间产品回撤
               (1 + BASE_PF.PFL_RANGE_PF_RATIO) / (1 + BASE_PF.MAX_RANGE_PROFIT) - 1   PD_DD,
               -- 区间基准回撤
               (1 + BASE_PF.BM_RANGE_PF_RATIO) / (1 + BASE_PF.MAX_BM_RANGE_PROFIT) - 1 BM_DD,
               -- 区间超额回撤
               BASE_PF.RANGE_ALPHA - BASE_PF.MAX_RANGE_ALPHA                           ALPHA_DD
        FROM (SELECT PF.*,
                     -- 区间最大收益率
                     GREATEST(MAX(PF.PFL_RANGE_PF_RATIO) OVER (PARTITION BY PF.FUND_ID ORDER BY PF.TRADE_DATE),
                              0) MAX_RANGE_PROFIT,
                     -- 基准区间最大收益率
                     GREATEST(MAX(PF.BM_RANGE_PF_RATIO) OVER (PARTITION BY PF.FUND_ID ORDER BY PF.TRADE_DATE),
                              0) MAX_BM_RANGE_PROFIT,
                     -- 区间最大超额
                     GREATEST(MAX(PF.RANGE_ALPHA) OVER (PARTITION BY PF.FUND_ID ORDER BY PF.TRADE_DATE),
                              0) MAX_RANGE_ALPHA
              FROM (SELECT ORIGIN_PF.*,
                           -- 区间超额收益率
                           ORIGIN_PF.PFL_RANGE_PF_RATIO - ORIGIN_PF.BM_RANGE_PF_RATIO RANGE_ALPHA
                    FROM (SELECT PFL_PF.*,
                                 -- 区间产品收益率
                                 EXP(SUM(LN(1 + PFL_PF.DAY_PF_RATIO))
                                         OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE)) -
                                 1                                                                                                                                     PFL_RANGE_PF_RATIO,
                                 -- 区间基准收益率
                                 EXP(SUM(LN(1 + BM_PF.BM_DAY_PF_RATIO))
                                         OVER (PARTITION BY BM_PF.BASE_FUND_ID ORDER BY BM_PF.TRADE_DATE)) -
                                 1                                                                                                                                     BM_RANGE_PF_RATIO
                          FROM FOF_PFL_PROFIT PFL_PF
                                   LEFT JOIN BENCHMARK_PROFIT BM_PF
                                       ON PFL_PF.TRADE_DATE = BM_PF.TRADE_DATE
                                              AND PFL_PF.FUND_ID = BM_PF.BASE_FUND_ID,
                               CONSTANTS CONS) ORIGIN_PF) PF) BASE_PF,
             CONSTANTS CONS)
SELECT FUND_ID,
       TRADE_DATE,
       -- 组合回撤
       PD_DD,
       -- 基准回撤
       BM_DD,
       -- 超额回撤
       ALPHA_DD
FROM BASE_PROFIT
ORDER BY TRADE_DATE
