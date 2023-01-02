WITH
    -- ����
    CONSTANTS AS (
        -- ����ֵ
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
    -- �����յ���
    TRADE_DATE_ADJ AS (
        -- �����յ�����������ʼ�յ�������棬����Ҫ����T-1�����ݣ�
        SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
               -- �Ѵ���������Ŀ�ʼ����
               MIN(L_TRADE_DATE)      BEGIN_DATE,
               MAX(L_TRADE_DATE)      END_DATE
        FROM ZHFX.TCALENDAR
        WHERE L_DATE = L_TRADE_DATE
          AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd') 
              AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')),
    DATE_INFO AS (
        -- ��ȡ���佻�����ڵ���Ϣ
        SELECT CLD.L_TRADE_DATE,
               CLD.L_TRADE_DATE_LAST,
               CLD.L_TRADE_DATE_NEXT,
               CLD.L_DATE_DIFF_LAST
        FROM ZHFX.TCALENDAR CLD,
             TRADE_DATE_ADJ ADJ
        WHERE CLD.L_DATE = CLD.L_TRADE_DATE
          AND CLD.L_DATE BETWEEN ADJ.BEGIN_DATE AND ADJ.END_DATE),
    FOF_PFL_PROFIT AS (
        -- FOF�������
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
        -- ��Ʊָ������
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
              WHERE STK_IDX.VC_WIND_CODE = '${stockidxcode}'
                AND STK_IDX.L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd') AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')) IDX_INFO,
             ZHFX.TCALENDAR CLD
        WHERE IDX_INFO.L_TRADE_DATE = CLD.L_DATE
        GROUP BY IDX_INFO.VC_WIND_CODE, CLD.L_TRADE_DATE),
    BOND_INDEX_PROFIT AS (
        -- ծȯָ������
        SELECT BOND_IDX.L_TRADE_DATE,
               BOND_IDX.VC_WIND_CODE,
               BOND_IDX.VC_STOCK_NAME,
               DECODE(
                       BOND_IDX.EN_PRICE_CLOSE_PRE, 0, 0,
                       BOND_IDX.EN_PRICE_CLOSE / BOND_IDX.EN_PRICE_CLOSE_PRE - 1
                   ) DAY_PF_RATIO
        FROM ZHFX.TINDEXINFOBOND BOND_IDX
        WHERE BOND_IDX.L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd') AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')
          AND BOND_IDX.VC_WIND_CODE = '${bondidxcode}'),
    CASH_INDEX_PROFIT AS (
        -- �ֽ�ָ������
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
              WHERE CBOND_BM.S_INFO_WINDCODE = '${cashidxcode}'
                AND CBOND_BM.TRADE_DT >= '20150101') CASH_IDX_INFO
                 RIGHT JOIN DATE_INFO
                            ON DATE_INFO.L_TRADE_DATE >= CASH_IDX_INFO.EFF_DT
                                AND DATE_INFO.L_TRADE_DATE < CASH_IDX_INFO.NEXT_EFF_DT),
    FUND_INDEX_PROFIT AS (
        -- ����ָ������
        SELECT FUND_IDX.L_TRADE_DATE,
               FUND_IDX.VC_WIND_CODE,
               FUND_IDX.VC_STOCK_NAME,
               DECODE(
                       FUND_IDX.EN_PRICE_CLOSE_PRE, 0, 0,
                       FUND_IDX.EN_PRICE_CLOSE / FUND_IDX.EN_PRICE_CLOSE_PRE - 1
                   ) DAY_PF_RATIO
        FROM ZHFX.TINDEXINFOFUND FUND_IDX
        WHERE FUND_IDX.L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd') AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')
          AND FUND_IDX.VC_WIND_CODE = '${fundidxcode}'),
    BENCHMARK_PROFIT AS (
        -- ��׼���
        SELECT PRO_PF.TRADE_DATE,
               PRO_PF.FUND_ID                                                       BASE_FUND_ID,
               STK_PF.DAY_PF_RATIO                                                  STK_DAY_PF_RATIO,
               BOND_PF.DAY_PF_RATIO                                                 BOND_DAY_PF_RATIO,
               CASH_PF.RATE                                                         CASH_RATE,
               FUND_PF.DAY_PF_RATIO                                                 FUND_DAY_PF_RATIO,
               NVL(${stockidxweight} / 100, 0) * NVL(STK_PF.DAY_PF_RATIO, 0) +
               NVL(${bondidxweight} / 100, 0) * NVL(BOND_PF.DAY_PF_RATIO, 0) +
               NVL(${cashidxweight} / 100, 0) * NVL(CASH_PF.RATE / CONS.ONE_YEAR_NATUAL_DAYS * CASH_PF.DIFF_LAST, 0) +
               NVL(${fundidxweight} / 100, 0) * NVL(FUND_PF.DAY_PF_RATIO, 0)        BM_DAY_PF_RATIO,
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
        -- �������
        SELECT BASE_PF.*,
               -- �����Ʒ�س�
               (1 + BASE_PF.PFL_RANGE_PF_RATIO) / (1 + BASE_PF.MAX_RANGE_PROFIT) - 1   PD_DD,
               -- �����׼�س�
               (1 + BASE_PF.BM_RANGE_PF_RATIO) / (1 + BASE_PF.MAX_BM_RANGE_PROFIT) - 1 BM_DD,
               -- ���䳬��س�
               BASE_PF.RANGE_ALPHA - BASE_PF.MAX_RANGE_ALPHA                           ALPHA_DD,
               -- ��һ���������������
               LAG(BASE_PF.MAX_RANGE_PROFIT, 1, 0)
                   OVER (PARTITION BY BASE_PF.FUND_ID ORDER BY BASE_PF.TRADE_DATE)     MAX_RANGE_PROFIT_PRE,
               -- ��һ�������׼���������
               LAG(BASE_PF.MAX_BM_RANGE_PROFIT, 1, 0)
                   OVER (PARTITION BY BASE_PF.FUND_ID ORDER BY BASE_PF.TRADE_DATE)     MAX_BM_RANGE_PROFIT_PRE,
               -- ��һ��������󳬶�
               LAG(BASE_PF.MAX_RANGE_ALPHA, 1, 0)
                   OVER (PARTITION BY BASE_PF.FUND_ID ORDER BY BASE_PF.TRADE_DATE)     MAX_RANGE_ALPHA_PRE,
               -- ��һ��ֵ��
               LEAD(BASE_PF.TRADE_DATE, 1, CONS.MAX_TRADE_DATE)
                    OVER (PARTITION BY BASE_PF.FUND_ID ORDER BY BASE_PF.TRADE_DATE)    NEXT_NAV_DATE
        FROM (SELECT PF.*,
                     -- �������������
                     GREATEST(MAX(PF.PFL_RANGE_PF_RATIO) OVER (PARTITION BY PF.FUND_ID ORDER BY PF.TRADE_DATE),
                              0) MAX_RANGE_PROFIT,
                     -- ��׼�������������
                     GREATEST(MAX(PF.BM_RANGE_PF_RATIO) OVER (PARTITION BY PF.FUND_ID ORDER BY PF.TRADE_DATE),
                              0) MAX_BM_RANGE_PROFIT,
                     -- ������󳬶�
                     GREATEST(MAX(PF.RANGE_ALPHA) OVER (PARTITION BY PF.FUND_ID ORDER BY PF.TRADE_DATE),
                              0) MAX_RANGE_ALPHA
              FROM (SELECT ORIGIN_PF.*,
                           -- ���䳬��������
                           ORIGIN_PF.PFL_RANGE_PF_RATIO - ORIGIN_PF.BM_RANGE_PF_RATIO RANGE_ALPHA,
                           -- ��������
                           DECODE(ORIGIN_PF.ROLLING_DAYS, ${period}, ORIGIN_PF.ROLLING_BASE_PF,
                                  NULL)                                               ROLLING_PROFIT,
                           -- �����������
                           DECODE(ORIGIN_PF.ROLLING_DAYS, ${period},
                                  CUME_DIST() OVER (PARTITION BY ORIGIN_PF.FUND_ID, DECODE(ORIGIN_PF.ROLLING_DAYS, ${period}, 0, 1) ORDER BY ORIGIN_PF.ROLLING_BASE_PF DESC),
                                  NULL)                                               ROLLING_PROFIT_PROB
                    FROM (SELECT PFL_PF.*,
                                 BM_PF.BM_DAY_PF_RATIO,
                                 BM_PF.FREE_RISK_RATE,
                                 -- �ճ���
                                 PFL_PF.DAY_PF_RATIO - BM_PF.BM_DAY_PF_RATIO                                                                              DAY_ALPHA,
                                 -- �����Ʒ������
                                 EXP(SUM(LN(1 + PFL_PF.DAY_PF_RATIO))
                                         OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE)) -
                                 1                                                                                                                        PFL_RANGE_PF_RATIO,
                                 -- �����׼������
                                 EXP(SUM(LN(1 + BM_PF.BM_DAY_PF_RATIO))
                                         OVER (PARTITION BY BM_PF.BASE_FUND_ID ORDER BY BM_PF.TRADE_DATE)) -
                                 1                                                                                                                        BM_RANGE_PF_RATIO,

                                 -- �����������
                                 CASE
                                     WHEN COUNT(PFL_PF.DAY_PF_RATIO)
                                                OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS ${period} - 1 PRECEDING) <
                                          ${period} THEN NULL
                                     ELSE STDDEV_SAMP(PFL_PF.DAY_PF_RATIO - BM_PF.BM_DAY_PF_RATIO)
                                                      OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS ${period} - 1 PRECEDING) *
                                          SQRT(CONS.ONE_YEAR_TRADE_DATE)
                                     END                                                                                                                  ROLLING_TE,
                                 CASE
                                     WHEN COUNT(PFL_PF.DAY_PF_RATIO)
                                                OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS ${period} - 1 PRECEDING) <
                                          ${period} THEN NULL
                                     ELSE STDDEV_SAMP(PFL_PF.DAY_PF_RATIO)
                                                      OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS ${period} - 1 PRECEDING) *
                                          SQRT(CONS.ONE_YEAR_TRADE_DATE)
                                     END                                                                                                                  ROLLING_VOL,
                                 CASE
                                     WHEN COUNT(BM_PF.BM_DAY_PF_RATIO)
                                                OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS ${period} - 1 PRECEDING) <
                                          ${period} THEN NULL
                                     ELSE STDDEV_SAMP(BM_PF.BM_DAY_PF_RATIO)
                                                      OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS ${period} - 1 PRECEDING) *
                                          SQRT(CONS.ONE_YEAR_TRADE_DATE)
                                     END                                                                                                                  BM_ROLLING_VOL,
                                 -- ��������
                                 EXP(SUM(LN(1 + PFL_PF.DAY_PF_RATIO))
                                         OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS BETWEEN CURRENT ROW AND ${period} - 1 FOLLOWING)) -
                                 1                                                                                                                        ROLLING_BASE_PF,
                                 -- ����ʱ��
                                 COUNT(PFL_PF.DAY_PF_RATIO)
                                       OVER (PARTITION BY PFL_PF.FUND_ID ORDER BY PFL_PF.TRADE_DATE ROWS BETWEEN CURRENT ROW AND ${period} - 1 FOLLOWING) ROLLING_DAYS
                          FROM FOF_PFL_PROFIT PFL_PF
                                   LEFT JOIN BENCHMARK_PROFIT BM_PF
                                             ON PFL_PF.TRADE_DATE = BM_PF.TRADE_DATE
                                                 AND PFL_PF.FUND_ID = BM_PF.BASE_FUND_ID,
                               CONSTANTS CONS) ORIGIN_PF,
                         CONSTANTS CONS) PF) BASE_PF,
             CONSTANTS CONS)
SELECT FUND_ID,
       TRADE_DATE,
       -- ����ۼ�������
       PFL_RANGE_PF_RATIO,
       -- ��׼�ۼ�������
       BM_RANGE_PF_RATIO,
       -- ����������
       RANGE_ALPHA,
       -- ��ϻس�
       PD_DD,
       -- ��׼�س�
       BM_DD,
       -- ����س�
       ALPHA_DD,
       -- ����N�첨����
       ROLLING_VOL,
       -- ��׼����N�첨����
       BM_ROLLING_VOL,
       -- �����������
       ROLLING_TE
FROM BASE_PROFIT
ORDER BY TRADE_DATE
