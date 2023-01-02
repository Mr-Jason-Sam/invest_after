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
          AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
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
    PRODUCT_PROFIT AS (
        -- ��Ʒ�Ļ�����Ϣ�ͻ���������ָ��
        SELECT PD_PF.L_FUND_ID                                       FUND_ID,
               PD_PF.L_TRADE_DATE                                    TRADE_DATE,
               PD_PF.FUND_ASSETS_NET,
               PD_PF.FUND_ASSETS_NET_PRE,
               PD_PF.FUND_ASSETS_NET / PD_PF.FUND_ASSETS_NET_PRE - 1 DAY_PROFIT
        FROM (
                 -- ��Ʒ�Ļ�����Ϣ
                 SELECT RTN.L_FUND_ID,
                        RTN.L_TRADE_DATE,
                        RTN.EN_FUND_ASSET_NET                                                     FUND_ASSETS_NET,
                        -- ���վ��ʲ�
                        RTN.EN_FUND_ASSET_NET_PRE +
                            -- ������(���ݿ���Ϊ��������Ϊ�ۼ���)
                        RTN.EN_APPLY_BAL + RTN.EN_APPLY_DIRECT + RTN.EN_APPEND_BAL +
                            -- ������
                        RTN.EN_REDEEM_BAL + RTN.EN_REDEEM_DIRECT + RTN.EN_EXTRACT_BAL +
                            -- �������ֺ졢�ֺ���Ͷ��ҵ������
                        RTN.EN_FUND_DIVIDEND + RTN.EN_FUND_DIVIDEND_INVEST + RTN.EN_INCOME_REWARD FUND_ASSETS_NET_PRE
                 FROM ZHFX.TFUNDRETURNDETAIL RTN,
                      TRADE_DATE_ADJ TDA
                 WHERE RTN.L_FUND_ID IN (${fund_ids})
                   AND RTN.L_TRADE_DATE BETWEEN TDA.BEGIN_DATE AND TDA.END_DATE) PD_PF),
    STK_INDEX_PROFIT AS (
        -- ��Ʊָ������
        SELECT CLD.L_TRADE_DATE,
               IDX_INFO.VC_WIND_CODE                                                                         CODE,
               MAX(IDX_INFO.VC_STOCK_NAME)                                                                   NAME,
               EXP(SUM(LN(1 + IDX_INFO.IDX_DAY_PROFIT))) * EXP(SUM(LN(1 + IDX_INFO.EN_EXCHRATE_PROFIT))) - 1 DAY_PROFIT
        FROM (SELECT STK_IDX.L_TRADE_DATE,
                     STK_IDX.VC_WIND_CODE,
                     STK_IDX.VC_STOCK_NAME,
                     DECODE(
                             STK_IDX.EN_PRICE_CLOSE_PRE, 0, 0,
                             STK_IDX.EN_PRICE_CLOSE / STK_IDX.EN_PRICE_CLOSE_PRE - 1
                         ) IDX_DAY_PROFIT,
                     STK_IDX.EN_EXCHRATE_PROFIT
              FROM ZHFX.TINDEXINFOSHARE STK_IDX,
                   TRADE_DATE_ADJ TDJ
              WHERE STK_IDX.VC_WIND_CODE = ${stk_index_code}
                AND STK_IDX.L_TRADE_DATE BETWEEN TDJ.BEGIN_DATE AND TDJ.END_DATE) IDX_INFO,
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
                   ) DAY_PROFIT
        FROM ZHFX.TINDEXINFOBOND BOND_IDX,
             DATE_INFO
        WHERE BOND_IDX.L_TRADE_DATE = DATE_INFO.L_TRADE_DATE
          AND BOND_IDX.VC_WIND_CODE = ${bond_index_code}),
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
              WHERE CBOND_BM.S_INFO_WINDCODE = ${cash_index_code}
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
                   ) DAY_PROFIT
        FROM ZHFX.TINDEXINFOFUND FUND_IDX,
             DATE_INFO
        WHERE FUND_IDX.L_TRADE_DATE = DATE_INFO.L_TRADE_DATE
          AND FUND_IDX.VC_WIND_CODE = ${fund_index_code}),
    BENCHMARK_PROFIT AS (
        -- ��׼���
        SELECT PRO_PF.TRADE_DATE,
               PRO_PF.FUND_ID                                                       BASE_FUND_ID,
               STK_PF.DAY_PROFIT                                                    STK_DAY_PROFIT,
               BOND_PF.DAY_PROFIT                                                   BOND_DAY_PROFIT,
               CASH_PF.RATE                                                         CASH_RATE,
               FUND_PF.DAY_PROFIT                                                   FUND_DAY_PROFIT,
               NVL(${stk_weight}, 0) * NVL(STK_PF.DAY_PROFIT, 0) +
               NVL(${bond_weight}, 0) * NVL(BOND_PF.DAY_PROFIT, 0) +
               NVL(${cash_weight}, 0) * NVL(CASH_PF.RATE / CONS.ONE_YEAR_NATUAL_DAYS * CASH_PF.DIFF_LAST, 0) +
               NVL(${fund_weight}, 0) * NVL(FUND_PF.DAY_PROFIT, 0)                  BM_DAY_PROFIT,
               NVL(CASH_PF.RATE / CONS.ONE_YEAR_NATUAL_DAYS * CASH_PF.DIFF_LAST, 0) FREE_RISK_RATE
        FROM PRODUCT_PROFIT PRO_PF,
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
               (1 + BASE_PF.PD_RANGE_PROFIT) / (1 + BASE_PF.MAX_RANGE_PROFIT) - 1    PD_DD,
               -- �����׼�س�
               (1 + BASE_PF.BM_RANGE_PROFIT) / (1 + BASE_PF.MAX_BM_RANGE_PROFIT) - 1 BM_DD,
               -- ���䳬��س�
               BASE_PF.RANGE_ALPHA - BASE_PF.MAX_RANGE_ALPHA                         ALPHA_DD,
               -- ��һ���������������
               LAG(BASE_PF.MAX_RANGE_PROFIT, 1, 0)
                   OVER (PARTITION BY BASE_PF.FUND_ID ORDER BY BASE_PF.TRADE_DATE)   MAX_RANGE_PROFIT_PRE,
               -- ��һ�������׼���������
               LAG(BASE_PF.MAX_BM_RANGE_PROFIT, 1, 0)
                   OVER (PARTITION BY BASE_PF.FUND_ID ORDER BY BASE_PF.TRADE_DATE)   MAX_BM_RANGE_PROFIT_PRE,
               -- ��һ��������󳬶�
               LAG(BASE_PF.MAX_RANGE_ALPHA, 1, 0)
                   OVER (PARTITION BY BASE_PF.FUND_ID ORDER BY BASE_PF.TRADE_DATE)   MAX_RANGE_ALPHA_PRE,
               -- ��һ��ֵ��
               LEAD(BASE_PF.TRADE_DATE, 1, CONS.MAX_TRADE_DATE)
                    OVER (PARTITION BY BASE_PF.FUND_ID ORDER BY BASE_PF.TRADE_DATE)  NEXT_NAV_DATE
        FROM (SELECT PF.*,
                     -- �������������
                     -- TODO Ϊʲô��greatest
                     GREATEST(MAX(PF.PD_RANGE_PROFIT) OVER (PARTITION BY PF.FUND_ID ORDER BY PF.TRADE_DATE),
                              0) MAX_RANGE_PROFIT,
                     -- ��׼�������������
                     GREATEST(MAX(PF.BM_RANGE_PROFIT) OVER (PARTITION BY PF.FUND_ID ORDER BY PF.TRADE_DATE),
                              0) MAX_BM_RANGE_PROFIT,
                     -- ������󳬶�
                     GREATEST(MAX(PF.RANGE_ALPHA) OVER (PARTITION BY PF.FUND_ID ORDER BY PF.TRADE_DATE),
                              0) MAX_RANGE_ALPHA
              FROM (SELECT ORIGIN_PF.*,
                           -- ���䳬��������
                           ORIGIN_PF.PD_RANGE_PROFIT - ORIGIN_PF.BM_RANGE_PROFIT RANGE_ALPHA,
                           -- ��������
                           DECODE(ORIGIN_PF.ROLLING_DAYS, ${rolling_trade_dates}, ORIGIN_PF.ROLLING_BASE_PF,
                                  NULL)                                          ROLLING_PROFIT,
                           -- �����������
                           DECODE(ORIGIN_PF.ROLLING_DAYS, ${rolling_trade_dates},
                                  CUME_DIST() OVER (PARTITION BY ORIGIN_PF.FUND_ID, DECODE(ORIGIN_PF.ROLLING_DAYS, ${rolling_trade_dates}, 0, 1) ORDER BY ORIGIN_PF.ROLLING_BASE_PF DESC),
                                  NULL)                                          ROLLING_PROFIT_PROB
                    FROM (SELECT PD_PF.*,
                                 BM_PF.BM_DAY_PROFIT,
                                 BM_PF.FREE_RISK_RATE,
                                 -- �ճ���
                                 PD_PF.DAY_PROFIT - BM_PF.BM_DAY_PROFIT                                                                                              DAY_ALPHA,
                                 -- �����Ʒ������
                                 EXP(SUM(LN(1 + PD_PF.DAY_PROFIT))
                                         OVER (PARTITION BY PD_PF.FUND_ID ORDER BY PD_PF.TRADE_DATE)) -
                                 1                                                                                                                                   PD_RANGE_PROFIT,
                                 -- �����׼������
                                 EXP(SUM(LN(1 + BM_PF.BM_DAY_PROFIT))
                                         OVER (PARTITION BY BM_PF.BASE_FUND_ID ORDER BY BM_PF.TRADE_DATE)) -
                                 1                                                                                                                                   BM_RANGE_PROFIT,
                                 -- ��������
                                 EXP(SUM(LN(1 + PD_PF.DAY_PROFIT))
                                         OVER (PARTITION BY PD_PF.FUND_ID ORDER BY PD_PF.TRADE_DATE ROWS BETWEEN CURRENT ROW AND ${rolling_trade_dates} - 1 FOLLOWING)) -
                                 1                                                                                                                                   ROLLING_BASE_PF,
                                 -- ����ʱ��
                                 COUNT(PD_PF.DAY_PROFIT)
                                       OVER (PARTITION BY PD_PF.FUND_ID ORDER BY PD_PF.TRADE_DATE ROWS BETWEEN CURRENT ROW AND ${rolling_trade_dates} - 1 FOLLOWING) ROLLING_DAYS
                          FROM PRODUCT_PROFIT PD_PF
                                   LEFT JOIN BENCHMARK_PROFIT BM_PF
                                       ON PD_PF.TRADE_DATE = BM_PF.TRADE_DATE
                                              AND PD_PF.FUND_ID = BM_PF.BASE_FUND_ID) ORIGIN_PF) PF) BASE_PF,
             CONSTANTS CONS),
    DRAWDOWN_BASE AS (
        -- �س���Ϣ
        SELECT DD_INFO.*,
               -- �س�����
               ROW_NUMBER() OVER (PARTITION BY DD_INFO.FUND_ID ORDER BY DD_INFO.PD_DD, DD_INFO.DD_BEGIN_DATE) DD_ROW,
               -- �޸�����
               DECODE(
                       DD_INFO.FIX_DD_DATE,
                       NULL, NULL,
                       CONS.MAX_TRADE_DATE, CONS.MAX_DAYS,
                       ZHFX.FCALTRADEDATE(STARTDATE => DD_INFO.DD_DATE, ENDDATE => DD_INFO.FIX_DD_DATE)
                   )                                                                                          FIX_TRADE_DATES,
               -- �γ�����
               DECODE(
                       DD_INFO.DD_BEGIN_DATE,
                       NULL, NULL,
                       ZHFX.FCALTRADEDATE(STARTDATE => DD_INFO.DD_BEGIN_DATE, ENDDATE => DD_INFO.DD_DATE)
                   )                                                                                          GEN_DD_TRADE_DATES
        FROM (
                 -- �س�������Ϣ
                 SELECT DD_BASE.FUND_ID,
                        DD_BASE.MAX_RANGE_PROFIT,
                        MIN(DD_BASE.PD_DD)               PD_DD,
                        MIN(DD_BASE.TRADE_DATE)          DD_BEGIN_DATE,
                        MIN(DECODE(DD_BASE.PD_RANGE_PROFIT, DD_BASE.MIN_RANGE_PROFIT, DD_BASE.TRADE_DATE,
                                   CONS.MAX_TRADE_DATE)) DD_DATE,
                        MAX(DD_BASE.NEXT_NAV_DATE)       FIX_DD_DATE,
                        MAX(DD_BASE.TRADE_DATE)          DD_END_DATE
                 FROM (SELECT BP.*,
                              MIN(BP.PD_RANGE_PROFIT)
                                  OVER (PARTITION BY BP.FUND_ID, BP.MAX_RANGE_PROFIT) MIN_RANGE_PROFIT
                       FROM BASE_PROFIT BP
--                        where BP.PD_RANGE_PROFIT != BP.MAX_RANGE_PROFIT
                      ) DD_BASE,
                      CONSTANTS CONS
                 GROUP BY FUND_ID, MAX_RANGE_PROFIT) DD_INFO,
             CONSTANTS CONS),
    DRAWDOWN_ANAYSIS AS (
        -- �س�����
        SELECT DDA_INFO.*,
               DDA_INFO.RANGE_PROFIT_IN_DD - DDA_INFO.BM_RANGE_PROFIT_IN_DD ALPHA_PROFIT_IN_DD
        FROM (
                 -- ��������
                 SELECT DDB.FUND_ID,
                        DDB.DD_ROW,
                        DDB.PD_DD,
                        DDB.DD_BEGIN_DATE,
                        DDB.DD_DATE,
                        DDB.FIX_DD_DATE,
                        DDB.GEN_DD_TRADE_DATES,
                        DDB.FIX_TRADE_DATES,
                        EXP(SUM(LN(1 + BP.BM_DAY_PROFIT))) - 1 BM_RANGE_PROFIT_IN_DD,
                        EXP(SUM(LN(1 + BP.DAY_PROFIT))) - 1    RANGE_PROFIT_IN_DD
                 FROM DRAWDOWN_BASE DDB
                          LEFT JOIN BASE_PROFIT BP ON BP.FUND_ID = DDB.FUND_ID AND
                                                      BP.TRADE_DATE BETWEEN DDB.DD_BEGIN_DATE AND DDB.DD_DATE
                 GROUP BY DDB.FUND_ID,
                          DDB.PD_DD,
                          DDB.DD_BEGIN_DATE,
                          DDB.DD_DATE,
                          DDB.FIX_DD_DATE,
                          DDB.GEN_DD_TRADE_DATES,
                          DDB.FIX_TRADE_DATES,
                          DDB.DD_ROW) DDA_INFO),
    DRAWDOWN_FINALLY AS (
        -- ���һ���س�
        SELECT *
        FROM DRAWDOWN_ANAYSIS
        WHERE (FUND_ID, DD_BEGIN_DATE) IN (SELECT DDA.FUND_ID,
                                                  MAX(DDA.DD_BEGIN_DATE) FINALLY_BEGIN_DATE
                                           FROM DRAWDOWN_ANAYSIS DDA
                                           GROUP BY DDA.FUND_ID)),
    PERFORMANCE AS (
        -- ����ҵ������
        SELECT PFM.*,
               -- �������
               CASE
                   WHEN PFM.PD_MAX_DD = 0 THEN NULL
                   -- TODO Ӧ���ý���������
                   WHEN PFM.DAY_PROFIT_DAYS <= CONS.ONE_YEAR_TRADE_DATE THEN PFM.PD_RANGE_PROFIT / ABS(PFM.PD_MAX_DD)
                   ELSE PFM.PROFIT_ANN / ABS(PFM.PD_MAX_DD)
                   END                            CALMAR_RATIO,
               -- ��������
               PD_RANGE_PROFIT - BM_RANGE_PROFIT  PROFIT_ALPHA,
               -- �껯��������
               PFM.PROFIT_ANN - PFM.BM_PROFIT_ANN PROFIT_ALPHA_ANN
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
                     POWER(BASE_PFM.PD_RANGE_PROFIT + 1, CONS.ONE_YEAR_TRADE_DATE / BASE_PFM.DAY_PROFIT_DAYS) -
                     1                                                                                  PROFIT_ANN,
                     -- �껯��׼������
                     POWER(BASE_PFM.BM_RANGE_PROFIT + 1, CONS.ONE_YEAR_TRADE_DATE / BASE_PFM.DAY_PROFIT_DAYS) -
                     1                                                                                  BM_PROFIT_ANN
              FROM (
                       -- ����ҵ������
                       SELECT PF.FUND_ID,
                              MIN(PF.TRADE_DATE)                                                         PD_BEGIN_DATE,
                              MAX(PF.TRADE_DATE)                                                         PD_END_DATE,
                              SUM(POWER(LEAST(PF.DAY_PROFIT - PF.FREE_RISK_RATE, 0), 2))                 PF_SAMP,
                              AVG(PF.DAY_PROFIT - PF.FREE_RISK_RATE)                                     AVG_PD_DAY_PROFIT,
                              AVG(PF.DAY_ALPHA)                                                          AVG_DAY_ALPHA,
                              -- ������
                              EXP(SUM(LN(1 + PF.DAY_PROFIT))) - 1                                        PD_RANGE_PROFIT,
                              -- ��������������
                              COUNT(PF.DAY_PROFIT)                                                       DAY_PROFIT_DAYS,
                              -- ��׼������
                              EXP(SUM(LN(1 + PF.BM_DAY_PROFIT))) - 1                                     BM_RANGE_PROFIT,
                              -- ������
                              STDDEV_SAMP(PF.DAY_PROFIT)                                                 VOL_SAMP,
                              -- ���س�
                              MIN(PF.PD_DD)                                                              PD_MAX_DD,
                              -- �������
                              STDDEV_SAMP(PF.DAY_PROFIT - PF.BM_DAY_PROFIT)                              TR_SAMP,
                              -- �������س�
                              MIN(PF.ALPHA_DD)                                                           ALPHA_MAX_DD,
                              -- ʤ��
                              SUM(CASE WHEN PF.DAY_PROFIT >= 0 THEN 1 ELSE 0 END) / COUNT(PF.DAY_PROFIT) DAY_WIN,
                              -- ����ʤ��
                              SUM(CASE WHEN PF.DAY_ALPHA >= 0 THEN 1 ELSE 0 END) /
                              COUNT(PF.DAY_ALPHA)                                                        ALPHA_DAY_WIN,
                              -- ӯ����
                              CASE
                                  WHEN SUM(CASE WHEN PF.DAY_PROFIT >= 0 THEN 1 ELSE 0 END) = 0 OR
                                       SUM(CASE WHEN PF.DAY_PROFIT < 0 THEN 1 ELSE 0 END) = 0 THEN NULL
                                  ELSE SUM(CASE WHEN PF.DAY_PROFIT >= 0 THEN PF.DAY_PROFIT ELSE 0 END) /
                                       SUM(CASE WHEN PF.DAY_PROFIT >= 0 THEN 1 ELSE 0 END) /
                                       SUM(CASE WHEN PF.DAY_PROFIT < 0 THEN ABS(PF.DAY_PROFIT) ELSE 0 END) *
                                       SUM(CASE WHEN PF.DAY_PROFIT < 0 THEN 1 ELSE 0 END)
                                  END                                                                    DAY_WIN_LOSE,
                              -- ����ӯ����
                              CASE
                                  WHEN SUM(CASE WHEN PF.DAY_ALPHA >= 0 THEN 1 ELSE 0 END) = 0 OR
                                       SUM(CASE WHEN PF.DAY_ALPHA < 0 THEN 1 ELSE 0 END) = 0 THEN NULL
                                  ELSE SUM(CASE WHEN PF.DAY_ALPHA >= 0 THEN PF.DAY_ALPHA ELSE 0 END) /
                                       SUM(CASE WHEN PF.DAY_ALPHA >= 0 THEN 1 ELSE 0 END) /
                                       SUM(CASE WHEN PF.DAY_ALPHA < 0 THEN ABS(PF.DAY_ALPHA) ELSE 0 END) *
                                       SUM(CASE WHEN PF.DAY_ALPHA < 0 THEN 1 ELSE 0 END)
                                  END                                                                    ALPHA_DAY_WIN_LOSE,
                              -- �¸߱�
                              SUM(CASE WHEN PF.PD_RANGE_PROFIT > PF.MAX_RANGE_PROFIT_PRE THEN 1 ELSE 0 END) /
                              COUNT(PF.PD_RANGE_PROFIT)                                                  NEW_HIGH_RATIO,
                              -- �����¸߱�
                              SUM(CASE WHEN PF.RANGE_ALPHA > PF.MAX_RANGE_ALPHA_PRE THEN 1 ELSE 0 END) /
                              COUNT(PF.RANGE_ALPHA)                                                      ALPHA_NEW_HIGH_RATIO
                       FROM BASE_PROFIT PF
                       GROUP BY PF.FUND_ID) BASE_PFM,
                   CONSTANTS CONS) PFM,
             CONSTANTS CONS),
    ROLLING_PROFIT_MAX_MIN AS (
        -- ��ֵ�������λ
        SELECT RHP_EXTEND.FUND_ID,
               -- ����ֵ
               0.0 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_0P,
               0.05 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT ROLLING_PF_VT_5P,
               0.1 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_10P,
               0.2 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_20P,
               0.3 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_30P,
               0.4 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_40P,
               0.5 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_50P,
               0.6 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_60P,
               0.7 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_70P,
               0.8 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_80P,
               0.9 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_90P,
               1.0 * RHP_EXTEND.DISTANCE_PROFIT + RHP_EXTEND.MIN_ROLLING_PROFIT  ROLLING_PF_VT_100P
        FROM (
                 -- ��ֵ����
                 SELECT RHP_PRO.*,
                        RHP_PRO.MAX_ROLLING_PROFIT - RHP_PRO.MIN_ROLLING_PROFIT DISTANCE_PROFIT
                 FROM (SELECT BP.FUND_ID,
                              MAX(BP.ROLLING_PROFIT) MAX_ROLLING_PROFIT,
                              MIN(BP.ROLLING_PROFIT) MIN_ROLLING_PROFIT
                       FROM BASE_PROFIT BP
                       GROUP BY BP.FUND_ID) RHP_PRO) RHP_EXTEND),
    ROLLING_PROFIT_PROB AS (
        -- ����������ʷ�λ
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
SELECT PFM.FUND_ID,
       FUND_INFO.VC_FUND_CODE,
       FUND_INFO.VC_FUND_NAME,
       PFM.PD_BEGIN_DATE,
       PFM.PD_END_DATE,
       PFM.PD_RANGE_PROFIT,
       PFM.PROFIT_ANN,
       PFM.BM_RANGE_PROFIT,
       PFM.BM_PROFIT_ANN,
       PFM.PROFIT_ALPHA,
       PFM.PROFIT_ALPHA_ANN,
       PFM.VOL_ANN,
       PFM.TR_ANN,
       PFM.ALPHA_MAX_DD,
       PFM.SHARPE_RATIO,
       PFM.SORTINO_RATIO,
       PFM.CALMAR_RATIO,
       PFM.INFO_RATIO,

       -- ���س���Ϣ
       MAX_DDA.PD_DD,
       MAX_DDA.DD_BEGIN_DATE,
       MAX_DDA.DD_DATE,
       MAX_DDA.GEN_DD_TRADE_DATES,
       MAX_DDA.BM_RANGE_PROFIT_IN_DD,
       MAX_DDA.ALPHA_PROFIT_IN_DD,
       MAX_DDA.FIX_DD_DATE,
       MAX_DDA.FIX_TRADE_DATES,

       -- ��ǰ�س���Ϣ
       FINALLY_DDA.PD_DD,
       FINALLY_DDA.DD_BEGIN_DATE,
       FINALLY_DDA.DD_DATE,
       FINALLY_DDA.GEN_DD_TRADE_DATES,
       FINALLY_DDA.BM_RANGE_PROFIT_IN_DD,
       FINALLY_DDA.ALPHA_PROFIT_IN_DD,
       FINALLY_DDA.FIX_DD_DATE,
       FINALLY_DDA.FIX_TRADE_DATES,

--        -- ���������������ֵ����
--        RPS.ROLLING_PF_VT_0P,
--        RPS.ROLLING_PF_VT_5P,
--        RPS.ROLLING_PF_VT_10P,
--        RPS.ROLLING_PF_VT_20P,
--        RPS.ROLLING_PF_VT_30P,
--        RPS.ROLLING_PF_VT_40P,
--        RPS.ROLLING_PF_VT_50P,
--        RPS.ROLLING_PF_VT_60P,
--        RPS.ROLLING_PF_VT_70P,
--        RPS.ROLLING_PF_VT_80P,
--        RPS.ROLLING_PF_VT_90P,
--        RPS.ROLLING_PF_VT_100P,

       -- ������������������ʵֵ��
       RPR.ROLLING_PF_0P,
       RPR.ROLLING_PF_5P,
       RPR.ROLLING_PF_10P,
       RPR.ROLLING_PF_20P,
       RPR.ROLLING_PF_30P,
       RPR.ROLLING_PF_40P,
       RPR.ROLLING_PF_50P,
       RPR.ROLLING_PF_60P,
       RPR.ROLLING_PF_70P,
       RPR.ROLLING_PF_80P,
       RPR.ROLLING_PF_90P,
       RPR.ROLLING_PF_100P

FROM PERFORMANCE PFM
         LEFT JOIN ZHFX.TFUNDINFO FUND_INFO ON PFM.FUND_ID = FUND_INFO.L_FUND_ID
         LEFT JOIN DRAWDOWN_ANAYSIS MAX_DDA ON PFM.FUND_ID = MAX_DDA.FUND_ID AND MAX_DDA.DD_ROW = 1
         LEFT JOIN DRAWDOWN_FINALLY FINALLY_DDA
                   ON PFM.FUND_ID = FINALLY_DDA.FUND_ID
                       AND FINALLY_DDA.FIX_DD_DATE = '99991231'
--          left join ROLLING_PROFIT_MAX_MIN RPS on PFM.FUND_ID = RPS.FUND_ID
         LEFT JOIN ROLLING_PROFIT_PROB RPR ON PFM.FUND_ID = RPR.FUND_ID
;
