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
          AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd')
            AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')),
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
              WHERE STK_IDX.VC_WIND_CODE = ${stockidxcode}
                AND STK_IDX.L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd') AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')) IDX_INFO,
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
        WHERE BOND_IDX.L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd') AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')
          AND BOND_IDX.VC_WIND_CODE = ${bondidxcode}),
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
              WHERE CBOND_BM.S_INFO_WINDCODE = ${cashidxcode}
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
        WHERE FUND_IDX.L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd') AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')
          AND FUND_IDX.VC_WIND_CODE = ${fundidxcode}),
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
                                                 AND PFL_PF.FUND_ID = BM_PF.BASE_FUND_ID) ORIGIN_PF) PF) BASE_PF,
             CONSTANTS CONS),
    PERFORMANCE AS (
        -- ����ҵ������
        SELECT PFM.*,
               -- �������
               CASE
                   WHEN PFM.PD_MAX_DD = 0 THEN NULL
                   -- TODO Ӧ���ý���������
                   WHEN PFM.DAY_PROFIT_DAYS <= CONS.ONE_YEAR_TRADE_DATE THEN PFM.PFL_RANGE_PF_RATIO / ABS(PFM.PD_MAX_DD)
                   ELSE PFM.PF_RATIO_ANN / ABS(PFM.PD_MAX_DD)
                   END                                CALMAR_RATIO,
               -- ��������
               PFL_RANGE_PF_RATIO - BM_RANGE_PF_RATIO PF_RATIO_ALPHA,
               -- �껯��������
               PFM.PF_RATIO_ANN - PFM.BM_PF_RATIO_ANN PF_RATIO_ALPHA_ANN
        FROM (SELECT BASE_PFM.*,
                     -- �껯������
                     BASE_PFM.VOL_SAMP * SQRT(CONS.ONE_YEAR_TRADE_DATE)                                 VOL_ANN,
                     -- �껯�������
                     BASE_PFM.TR_SAMP * SQRT(CONS.ONE_YEAR_TRADE_DATE)                                  TR_ANN,
                     -- ���ձ���
                     DECODE(BASE_PFM.VOL_SAMP, 0, NULL, BASE_PFM.AVG_PD_DAY_PROFIT / BASE_PFM.VOL_SAMP *
                                                        SQRT(CONS.ONE_YEAR_TRADE_DATE))                 SHARPE_RATIO,
                     -- ����ŵ����
                     DECODE(BASE_PFM.PF_SAMP, 0, NULL,
                            BASE_PFM.AVG_PD_DAY_PROFIT / SQRT(BASE_PFM.PF_SAMP / (BASE_PFM.DAY_PROFIT_DAYS - 1)) *
                            SQRT(CONS.ONE_YEAR_TRADE_DATE))                                             SORTINO_RATIO,
                     -- ��Ϣ����
                     DECODE(BASE_PFM.TR_SAMP, 0, NULL,
                            BASE_PFM.AVG_DAY_ALPHA / BASE_PFM.TR_SAMP * SQRT(CONS.ONE_YEAR_TRADE_DATE)) INFO_RATIO,
                     -- �껯������
                     POWER(BASE_PFM.PFL_RANGE_PF_RATIO + 1, CONS.ONE_YEAR_TRADE_DATE / BASE_PFM.DAY_PROFIT_DAYS) -
                     1                                                                                  PF_RATIO_ANN,
                     -- �껯��׼������
                     POWER(BASE_PFM.BM_RANGE_PF_RATIO + 1, CONS.ONE_YEAR_TRADE_DATE / BASE_PFM.DAY_PROFIT_DAYS) -
                     1                                                                                  BM_PF_RATIO_ANN
              FROM (
                       -- ����ҵ������
                       SELECT PF.FUND_ID,
                              MAX(TRADE_DATE)                                                                MAX_TRADE_DATE,
                              MIN(TRADE_DATE)                                                                MIN_TRADE_DATE,
                              SUM(POWER(LEAST(PF.DAY_PF_RATIO - PF.FREE_RISK_RATE, 0), 2))                   PF_SAMP,
                              AVG(PF.DAY_PF_RATIO - PF.FREE_RISK_RATE)                                       AVG_PD_DAY_PROFIT,
                              AVG(PF.DAY_ALPHA)                                                              AVG_DAY_ALPHA,
                              -- ������
                              EXP(SUM(LN(1 + PF.DAY_PF_RATIO))) - 1                                          PFL_RANGE_PF_RATIO,
                              -- ��������������
                              COUNT(PF.DAY_PF_RATIO)                                                         DAY_PROFIT_DAYS,
                              -- ��׼������
                              EXP(SUM(LN(1 + PF.BM_DAY_PF_RATIO))) - 1                                       BM_RANGE_PF_RATIO,
                              -- ������
                              STDDEV_SAMP(PF.DAY_PF_RATIO)                                                   VOL_SAMP,
                              -- ���س�
                              MIN(PF.PD_DD)                                                                  PD_MAX_DD,
                              -- �������
                              STDDEV_SAMP(PF.DAY_PF_RATIO - PF.BM_DAY_PF_RATIO)                              TR_SAMP,
                              -- �������س�
                              MIN(PF.ALPHA_DD)                                                               ALPHA_MAX_DD,
                              -- ʤ��
                              SUM(CASE WHEN PF.DAY_PF_RATIO >= 0 THEN 1 ELSE 0 END) / COUNT(PF.DAY_PF_RATIO) DAY_WIN,
                              -- ����ʤ��
                              SUM(CASE WHEN PF.DAY_ALPHA >= 0 THEN 1 ELSE 0 END) /
                              COUNT(PF.DAY_ALPHA)                                                            ALPHA_DAY_WIN,
                              -- ӯ����
                              CASE
                                  WHEN SUM(CASE WHEN PF.DAY_PF_RATIO >= 0 THEN 1 ELSE 0 END) = 0 OR
                                       SUM(CASE WHEN PF.DAY_PF_RATIO < 0 THEN 1 ELSE 0 END) = 0 THEN NULL
                                  ELSE SUM(CASE WHEN PF.DAY_PF_RATIO >= 0 THEN PF.DAY_PF_RATIO ELSE 0 END) /
                                       SUM(CASE WHEN PF.DAY_PF_RATIO >= 0 THEN 1 ELSE 0 END) /
                                       SUM(CASE WHEN PF.DAY_PF_RATIO < 0 THEN ABS(PF.DAY_PF_RATIO) ELSE 0 END) *
                                       SUM(CASE WHEN PF.DAY_PF_RATIO < 0 THEN 1 ELSE 0 END)
                                  END                                                                        DAY_WIN_LOSE,
                              -- ����ӯ����
                              CASE
                                  WHEN SUM(CASE WHEN PF.DAY_ALPHA >= 0 THEN 1 ELSE 0 END) = 0 OR
                                       SUM(CASE WHEN PF.DAY_ALPHA < 0 THEN 1 ELSE 0 END) = 0 THEN NULL
                                  ELSE SUM(CASE WHEN PF.DAY_ALPHA >= 0 THEN PF.DAY_ALPHA ELSE 0 END) /
                                       SUM(CASE WHEN PF.DAY_ALPHA >= 0 THEN 1 ELSE 0 END) /
                                       SUM(CASE WHEN PF.DAY_ALPHA < 0 THEN ABS(PF.DAY_ALPHA) ELSE 0 END) *
                                       SUM(CASE WHEN PF.DAY_ALPHA < 0 THEN 1 ELSE 0 END)
                                  END                                                                        ALPHA_DAY_WIN_LOSE,
                              -- �¸߱�
                              SUM(CASE WHEN PF.PFL_RANGE_PF_RATIO > PF.MAX_RANGE_PROFIT_PRE THEN 1 ELSE 0 END) /
                              COUNT(PF.PFL_RANGE_PF_RATIO)                                                   NEW_HIGH_RATIO,
                              -- �����¸߱�
                              SUM(CASE WHEN PF.RANGE_ALPHA > PF.MAX_RANGE_ALPHA_PRE THEN 1 ELSE 0 END) /
                              COUNT(PF.RANGE_ALPHA)                                                          ALPHA_NEW_HIGH_RATIO
                       FROM BASE_PROFIT PF
                       GROUP BY PF.FUND_ID) BASE_PFM,
                   CONSTANTS CONS) PFM,
             CONSTANTS CONS)
-- SELECT PFM.FUND_ID,
--        PFM.MIN_TRADE_DATE,
--        PFM.MAX_TRADE_DATE,
--        -- ����ָ��
--        -- ����ָ��
--        -- �ۼ�������
--        PFM.PFL_RANGE_PF_RATIO,
--        -- �껯�ۼ�������
--        PFM.PF_RATIO_ANN,
--        -- ����ָ��
--        -- �껯������
--        PFM.VOL_ANN,
--        -- ���س�
--        PFM.PD_MAX_DD,
--        -- ���յ�������
--        -- ���ձ���
--        PFM.SHARPE_RATIO,
--        -- ����ŵ����
--        PFM.SORTINO_RATIO,
--        -- �������
--        PFM.CALMAR_RATIO,
--        -- ����
--        -- ��ʤ��
--        PFM.DAY_WIN,
--        -- ��ӯ����
--        PFM.DAY_WIN_LOSE,
--        -- �¸߱�
--        PFM.NEW_HIGH_RATIO,
--
--        -- ���ָ��
--        -- ����ָ��
--        -- ��׼�ۼ�������
--        PFM.BM_RANGE_PF_RATIO,
--        -- ��׼�껯�ۼ�������
--        PFM.BM_PF_RATIO_ANN,
--        -- ����������
--        PFM.PF_RATIO_ALPHA,
--        -- �껯����������
--        PFM.PF_RATIO_ALPHA_ANN,
--        -- ����ָ��
--        -- �껯�������
--        PFM.TR_ANN,
--        -- �������س�
--        PFM.ALPHA_MAX_DD,
--        -- ���յ�������
--        -- �껯��Ϣ����
--        PFM.INFO_RATIO,
--        -- ����
--        -- ������ʤ��
--        PFM.ALPHA_DAY_WIN,
--        -- ������ӯ����
--        PFM.ALPHA_DAY_WIN_LOSE,
--        -- �����¸߱�
--        PFM.ALPHA_NEW_HIGH_RATIO
-- FROM PERFORMANCE PFM

SELECT * FROM FOF_PFL_PROFIT

