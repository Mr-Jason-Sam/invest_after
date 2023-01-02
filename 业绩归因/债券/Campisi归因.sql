WITH CONSTANTS AS (
    -- ����
    SELECT
        -- ��ծID
        1232       CTY_BOND_ID,
        'yyyymmdd' DATE_FORMAT,
        365        ONE_YEAR_DAYS,
        244        ONE_YEAR_TRADE_DATES
    FROM DUAL),
     BOND_HOLDING_INFO AS (
         -- ծȯ�ֲ���Ϣ
         SELECT HDB.L_FUND_ID,
                HDB.L_TRADE_DATE,
                HDB.VC_WIND_CODE,
                HDB.VC_STOCK_NAME,
                HDB.L_AMOUNT,
                HDB.L_AMOUNT_PRE,
                -- ��ȯ���ʲ� = �ֲ���ֵ + Ӧ����Ϣ
                HDB.EN_VALUE_MARKET + HDB.EN_INTEREST_ACCUMULATED              NET_ASSETS,
                -- T-1���ʲ�
                HDB.EN_VALUE_MARKET_PRE + HDB.EN_INTEREST_ACCUMULATED_PRE      NET_ASSETS_PRE,
                -- ��ԭ��Ϣ˰��ֵ
                HDB.EN_VALUE_MARKET_REVTAX + HDB.EN_ACCRINT_REVTAX             REVTAX_NET_ASSETS,
                -- T-1��ԭ��Ϣ˰��ֵ
                HDB.EN_VALUE_MARKET_REVTAX_PRE + HDB.EN_ACCRINT_REVTAX_PRE     REVTAX_NET_ASSETS_PRE,
                -- ���棨���۽����ԣ� = ���ʼ۸�䶯 + Ͷ������ + ��Ϣ - ��ֵ˰
                HDB.EN_INCREMENT + HDB.EN_INTEREST + HDB.EN_INVEST_INCOME -
                HDB.EN_INTEREST_VAT                                            PROFIT_VALUE,
                -- ƱϢ����
                HDB.EN_INTEREST - HDB.EN_INTEREST_VAT                          INTEREST_PROFIT_VALUE,

                -- ծȯ����
                DECODE(BD.VC_BOND_TYPE2_WIND, '��������ծ', BD.VC_BOND_TYPE2_WIND,
                       NVL(BD.VC_BOND_TYPE1_WIND, '����'))                     BOND_TYPE,
                -- ���ڼ��
                CASE
                    WHEN CLD.L_TRADE_DATE <= BD.L_MATURITY_DATE THEN TO_DATE(CLD.L_TRADE_DATE, CONS.DATE_FORMAT) -
                                                                     TO_DATE(CLD.L_TRADE_DATE_LAST, CONS.DATE_FORMAT)
                    ELSE TO_DATE(BD.L_MATURITY_DATE, CONS.DATE_FORMAT) -
                         TO_DATE(CLD.L_TRADE_DATE_LAST, CONS.DATE_FORMAT)
                    END                                                        MATURITY_DATE_DIST,

                -- ��Ϣ�Ҹ�
                NVL(BOND_INFO.EN_PRINCIPAL, 0) + NVL(BOND_INFO.EN_INTEREST, 0) CASH_FLOW,

                -- ȫ��
                DECODE(BOND_INFO.L_RECOM_CNBD, 1, BOND_INFO.EN_FULL_CNBD_IFEXE,
                       BOND_INFO.EN_FULL_CNBD)                                 PRICE,
                -- ��������
                DECODE(BOND_INFO.L_RECOM_CNBD, 1, BOND_INFO.EN_MODIDURA_CNBD_IFEXE,
                       BOND_INFO.EN_MODIDURA_CNBD)                             MD_DURA,
                -- ����������
                DECODE(BOND_INFO.L_RECOM_CNBD, 1, BOND_INFO.EN_YTM_CNBD_IFEXE,
                       BOND_INFO.EN_YTM_CNBD)                                  YTM,
                -- --ʣ����Ȩ����
                DECODE(BOND_INFO.L_RECOM_CNBD, 1, BOND_INFO.EN_MATU_CNBD_IFEXE,
                       BOND_INFO.EN_MATU_CNBD)                                 REDIUE_DATES,

                --T-1
                CLD.L_TRADE_DATE_LAST                                          L_TRADE_DATE_PRE,
                -- ȫ��
                DECODE(BOND_INFO_PRE.L_RECOM_CNBD, 1, BOND_INFO_PRE.EN_FULL_CNBD_IFEXE,
                       BOND_INFO_PRE.EN_FULL_CNBD)                             PRICE_PRE,
                -- ��������
                DECODE(BOND_INFO_PRE.L_RECOM_CNBD, 1, BOND_INFO_PRE.EN_MODIDURA_CNBD_IFEXE,
                       BOND_INFO_PRE.EN_MODIDURA_CNBD)                         MD_DURA_PRE,
                -- ����������
                DECODE(BOND_INFO_PRE.L_RECOM_CNBD, 1, BOND_INFO_PRE.EN_YTM_CNBD_IFEXE,
                       BOND_INFO_PRE.EN_YTM_CNBD)                              YTM_PRE,
                -- --ʣ����Ȩ����
                DECODE(BOND_INFO_PRE.L_RECOM_CNBD, 1, BOND_INFO_PRE.EN_MATU_CNBD_IFEXE,
                       BOND_INFO_PRE.EN_MATU_CNBD)                             REDIUE_DATES_PRE,

                -- T-1��ֵ
                COALESCE(BOND_INFO_PRE.EN_PAR_CNBD, BOND_INFO.EN_PAR, BOND_INFO_PRE.EN_PAR_CNBD,
                         BOND_INFO.EN_PAR)                                     FACE_VALUE_PRE,
                -- T-1Ʊ������
                NVL(BOND_INFO_PRE.EN_COUPONRATE, BOND_INFO.EN_COUPONRATE)      COUPONRATE_PRE
         FROM ZHFX.THOLDINGDETAILBOND HDB
                  -- �������ռ����ծȯ����
                  LEFT JOIN ZHFX.TBONDDESCRIPTION BD ON HDB.VC_WIND_CODE = BD.VC_WIND_CODE
             -- ����T�յĹ�ֵ
                  LEFT JOIN ZHFX.TBONDINFO BOND_INFO
                            ON HDB.L_TRADE_DATE = BOND_INFO.L_TRADE_DATE AND HDB.VC_WIND_CODE = BOND_INFO.VC_WIND_CODE
             -- ����T-1�Ĺ�ֵ
                  LEFT JOIN ZHFX.TCALENDAR CLD ON HDB.L_TRADE_DATE = CLD.L_DATE
                  LEFT JOIN ZHFX.TBONDINFO BOND_INFO_PRE ON CLD.L_TRADE_DATE_LAST = BOND_INFO_PRE.L_TRADE_DATE AND
                                                            HDB.VC_WIND_CODE = BOND_INFO_PRE.VC_WIND_CODE,
              CONSTANTS CONS
         WHERE HDB.L_FUND_ID IN (${fund_ids})
           AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     BOND_CURVE AS (
         -- ��ծ��������
         SELECT BHI.*,
                (BC_T1_Y1.EN_YIELD +
                 (BC_T1_Y1.EN_YIELD_NEXT - BC_T1_Y1.EN_YIELD) / (BC_T1_Y1.EN_CURVE_TERM_NEXT - BC_T1_Y1.EN_CURVE_TERM) *
                 (BHI.REDIUE_DATES - BC_T1_Y1.EN_CURVE_TERM)) / 100     Y1T1,
                (BC_T1_Y0.EN_YIELD +
                 (BC_T1_Y0.EN_YIELD_NEXT - BC_T1_Y0.EN_YIELD) / (BC_T1_Y0.EN_CURVE_TERM_NEXT - BC_T1_Y0.EN_CURVE_TERM) *
                 (BHI.REDIUE_DATES_PRE - BC_T1_Y0.EN_CURVE_TERM)) / 100 Y0T1,
                (BC_T0_Y1.EN_YIELD +
                 (BC_T0_Y1.EN_YIELD_NEXT - BC_T0_Y1.EN_YIELD) / (BC_T0_Y1.EN_CURVE_TERM_NEXT - BC_T0_Y1.EN_CURVE_TERM) *
                 (BHI.REDIUE_DATES - BC_T0_Y1.EN_CURVE_TERM)) / 100     Y1T0,
                (BC_T0_Y0.EN_YIELD +
                 (BC_T0_Y0.EN_YIELD_NEXT - BC_T0_Y0.EN_YIELD) / (BC_T0_Y0.EN_CURVE_TERM_NEXT - BC_T0_Y0.EN_CURVE_TERM) *
                 (BHI.REDIUE_DATES_PRE - BC_T0_Y0.EN_CURVE_TERM)) / 100 Y0T0
         FROM (SELECT BHI_TMP.*,
                      CASE
                          WHEN BHI_TMP.REDIUE_DATES < 0.25 THEN 0
                          WHEN BHI_TMP.REDIUE_DATES < 0.5 THEN 0.25
                          WHEN BHI_TMP.REDIUE_DATES < 1 THEN 0.5
                          WHEN BHI_TMP.REDIUE_DATES < 2 THEN 1
                          WHEN BHI_TMP.REDIUE_DATES < 3 THEN 2
                          WHEN BHI_TMP.REDIUE_DATES < 5 THEN 3
                          WHEN BHI_TMP.REDIUE_DATES < 7 THEN 5
                          WHEN BHI_TMP.REDIUE_DATES < 10 THEN 7
                          WHEN BHI_TMP.REDIUE_DATES < 20 THEN 10
                          WHEN BHI_TMP.REDIUE_DATES < 30 THEN 20
                          WHEN BHI_TMP.REDIUE_DATES < 50 THEN 30
                          END REDIUE_DATES_RANGE,
                      CASE
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 0.25 THEN 0
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 0.5 THEN 0.25
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 1 THEN 0.5
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 2 THEN 1
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 3 THEN 2
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 5 THEN 3
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 7 THEN 5
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 10 THEN 7
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 20 THEN 10
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 30 THEN 20
                          WHEN BHI_TMP.REDIUE_DATES_PRE < 50 THEN 30
                          END REDIUE_DATES_PRE_RANGE,
                      CONS.CTY_BOND_ID
               FROM BOND_HOLDING_INFO BHI_TMP,
                    CONSTANTS CONS) BHI
                  LEFT JOIN
              ZHFX.TBONDCURVE BC_T1_Y1
              ON
                          BC_T1_Y1.L_CURVE_NUMBER = BHI.CTY_BOND_ID
                      AND BC_T1_Y1.L_TRADE_DATE = BHI.L_TRADE_DATE
                      AND BC_T1_Y1.EN_CURVE_TERM = BHI.REDIUE_DATES_RANGE
                  LEFT JOIN
              ZHFX.TBONDCURVE BC_T1_Y0
              ON
                          BC_T1_Y0.L_CURVE_NUMBER = BHI.CTY_BOND_ID
                      AND BC_T1_Y0.L_TRADE_DATE = BHI.L_TRADE_DATE
                      AND BC_T1_Y0.EN_CURVE_TERM = BHI.REDIUE_DATES_PRE_RANGE
                  LEFT JOIN
              ZHFX.TBONDCURVE BC_T0_Y1
              ON
                          BC_T0_Y1.L_CURVE_NUMBER = BHI.CTY_BOND_ID
                      AND BC_T0_Y1.L_TRADE_DATE = BHI.L_TRADE_DATE_PRE
                      AND BC_T0_Y1.EN_CURVE_TERM = BHI.REDIUE_DATES_RANGE
                  LEFT JOIN
              ZHFX.TBONDCURVE BC_T0_Y0
              ON
                          BC_T0_Y0.L_CURVE_NUMBER = BHI.CTY_BOND_ID
                      AND BC_T0_Y0.L_TRADE_DATE = BHI.L_TRADE_DATE_PRE
                      AND BC_T0_Y0.EN_CURVE_TERM = BHI.REDIUE_DATES_PRE_RANGE),
     BOND_PROFIT_CTB AS (
         -- ��������
         SELECT CAMPISI.L_FUND_ID,
                CAMPISI.L_TRADE_DATE,
                SUM(CAMPISI.TOTAL_PROFIT) / SUM(CAMPISI.MKT_PRE)                                                DAY_PROFIT,
                SUM(CAMPISI.COUPON_PROFIT) / SUM(CAMPISI.MKT_PRE)                                               COUPON,
                SUM(CAMPISI.ACCRETION_PROFIT) / SUM(CAMPISI.MKT_PRE)                                            ACCRETION,
                SUM(CAMPISI.ROLLDOWN_PROFIT) / SUM(CAMPISI.MKT_PRE)                                             ROLLDOWN,
                SUM(CAMPISI.SHIFT_PROFIT) / SUM(CAMPISI.MKT_PRE)                                                SHIFT,
                SUM(CAMPISI.TWIST_PROFIT) / SUM(CAMPISI.MKT_PRE)                                                TWIST,
                SUM(CAMPISI.SPREAD_PROFIT) / SUM(CAMPISI.MKT_PRE)                                               SPREAD,
                -- �в�
                SUM(CAMPISI.TOTAL_PROFIT - CAMPISI.COUPON_PROFIT - CAMPISI.ACCRETION_PROFIT - CAMPISI.ROLLDOWN_PROFIT -
                    CAMPISI.SHIFT_PROFIT - CAMPISI.TWIST_PROFIT - CAMPISI.SPREAD_PROFIT) / SUM(CAMPISI.MKT_PRE) RESIDUAL
         FROM (SELECT BC.L_FUND_ID,
                      BC.L_TRADE_DATE,
                      BC.VC_WIND_CODE,
                      BC.VC_STOCK_NAME,
                      -- T-1��ֵ
                      NVL(BC.PRICE_PRE * BC.L_AMOUNT_PRE, BC.REVTAX_NET_ASSETS_PRE) MKT_PRE,
                      CASE
                          WHEN BC.PRICE_PRE IS NOT NULL AND BC.PRICE IS NOT NULL
                              THEN (BC.PRICE + BC.CASH_FLOW - BC.PRICE_PRE) * BC.L_AMOUNT_PRE
                          WHEN BC.PRICE IS NOT NULL
                              THEN (BC.PRICE + BC.CASH_FLOW) * BC.L_AMOUNT_PRE - BC.REVTAX_NET_ASSETS_PRE
                          ELSE BC.PROFIT_VALUE + BC.REVTAX_NET_ASSETS - BC.NET_ASSETS -
                               (BC.REVTAX_NET_ASSETS_PRE - BC.NET_ASSETS_PRE)
                          END                                                       TOTAL_PROFIT,
                      -- ƱϢ����
                      NVL(BC.FACE_VALUE_PRE * BC.L_AMOUNT_PRE * BC.COUPONRATE_PRE / CONS.ONE_YEAR_DAYS *
                          BC.MATURITY_DATE_DIST, 0)                                 COUPON_PROFIT,
                      -- ��������
                      CASE
                          WHEN BC.YTM_PRE IS NOT NULL THEN
                                              NVL(BC.PRICE_PRE * BC.L_AMOUNT_PRE, BC.REVTAX_NET_ASSETS_PRE) *
                                              BC.YTM_PRE /
                                              CONS.ONE_YEAR_DAYS * BC.MATURITY_DATE_DIST -
                                              NVL(BC.FACE_VALUE_PRE * BC.L_AMOUNT_PRE * BC.COUPONRATE_PRE /
                                                  CONS.ONE_YEAR_DAYS * BC.MATURITY_DATE_DIST, 0)
                          ELSE 0
                          END                                                       ACCRETION_PROFIT,
                      -- �������
                      CASE
                          WHEN BC.MD_DURA_PRE IS NOT NULL AND BC.Y1T0 IS NOT NULL AND BC.Y0T0 IS NOT NULL THEN
                                  - NVL(BC.PRICE_PRE * BC.L_AMOUNT_PRE, BC.REVTAX_NET_ASSETS_PRE) * BC.MD_DURA_PRE *
                                  (BC.Y1T0 - BC.Y0T0)
                          ELSE 0
                          END                                                       ROLLDOWN_PROFIT,
                      -- ƽ������
                      CASE
                          WHEN BC.MD_DURA_PRE IS NOT NULL AND BC.Y0T1 IS NOT NULL AND BC.Y0T0 IS NOT NULL THEN
                                  - NVL(BC.PRICE_PRE * BC.L_AMOUNT_PRE, BC.REVTAX_NET_ASSETS_PRE) * BC.MD_DURA_PRE *
                                  (BC.Y0T1 - BC.Y0T0)
                          ELSE 0
                          END                                                       SHIFT_PROFIT,
                      -- Ť������
                      CASE
                          WHEN BC.MD_DURA_PRE IS NOT NULL AND BC.Y1T1 IS NOT NULL AND BC.Y1T0 IS NOT NULL AND
                               BC.Y0T1 IS NOT NULL AND
                               BC.Y0T0 IS NOT NULL THEN
                                  - NVL(BC.PRICE_PRE * BC.L_AMOUNT_PRE, BC.REVTAX_NET_ASSETS_PRE) *
                                  BC.MD_DURA_PRE *
                                  (BC.Y1T1 - BC.Y1T0 - BC.Y0T1 + BC.Y0T0)
                          ELSE 0
                          END                                                       TWIST_PROFIT,
                      -- ��������
                      CASE
                          WHEN BC.MD_DURA_PRE IS NOT NULL AND BC.YTM IS NOT NULL AND BC.Y1T1 IS NOT NULL AND
                               BC.YTM_PRE IS NOT NULL AND
                               BC.Y0T0 IS NOT NULL THEN
                                  - NVL(BC.PRICE_PRE * BC.L_AMOUNT_PRE, BC.REVTAX_NET_ASSETS_PRE) *
                                  BC.MD_DURA_PRE *
                                  (BC.YTM - BC.Y1T1 - BC.YTM_PRE + BC.Y0T0)
                          ELSE 0
                          END                                                       SPREAD_PROFIT
               FROM BOND_CURVE BC,
                    CONSTANTS CONS
               WHERE BC.BOND_TYPE NOT IN ('��תծ', '�ɽ���ծ')
                 AND BC.L_AMOUNT_PRE != 0) CAMPISI
         GROUP BY CAMPISI.L_FUND_ID, CAMPISI.L_TRADE_DATE),
     BOND_PROFIT_RISK AS (
         -- ���������
         SELECT BPC.L_FUND_ID,
                MIN(BPC.L_TRADE_DATE) OVER (PARTITION BY 1) BEGIN_DATE,
                BPC.L_TRADE_DATE END_DATE,
                BPC.DAY_PROFIT,
                EXP(SUM(LN(1 + BPC.DAY_PROFIT)) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE)) -
                1                                                                                              RANGE_PROFIT,
                SUM(BPC.COUPON * BPC.PROFIT_ADJ)
                    OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE)                             AS COUPON,
                SUM(BPC.ACCRETION * BPC.PROFIT_ADJ)
                    OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE)                             AS ACCRETION,
                SUM(BPC.ROLLDOWN * BPC.PROFIT_ADJ)
                    OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE)                             AS ROLLDOWN,
                SUM(BPC.SHIFT * BPC.PROFIT_ADJ) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) AS SHIFT,
                SUM(BPC.TWIST * BPC.PROFIT_ADJ) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) AS TWIST,
                SUM(BPC.SPREAD * BPC.PROFIT_ADJ)
                    OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE)                             AS SPREAD,
                SUM(BPC.RESIDUAL * BPC.PROFIT_ADJ)
                    OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE)                             AS RESIDUAL,
                COVAR_SAMP(BPC.COUPON, BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) /
                STDDEV_SAMP(BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) *
                SQRT(CONS.ONE_YEAR_TRADE_DATES)                                                                COUPON_RISK,
                COVAR_SAMP(BPC.ACCRETION, BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) /
                STDDEV_SAMP(BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) *
                SQRT(CONS.ONE_YEAR_TRADE_DATES)                                                                ACCRETION_RISK,
                COVAR_SAMP(BPC.ROLLDOWN, BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) /
                STDDEV_SAMP(BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) *
                SQRT(CONS.ONE_YEAR_TRADE_DATES)                                                                ROLLDOWN_RISK,
                COVAR_SAMP(BPC.SHIFT, BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) /
                STDDEV_SAMP(BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) *
                SQRT(CONS.ONE_YEAR_TRADE_DATES)                                                                SHIFT_RISK,
                COVAR_SAMP(BPC.TWIST, BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) /
                STDDEV_SAMP(BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) *
                SQRT(CONS.ONE_YEAR_TRADE_DATES)                                                                TWIST_RISK,
                COVAR_SAMP(BPC.SPREAD, BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) /
                STDDEV_SAMP(BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) *
                SQRT(CONS.ONE_YEAR_TRADE_DATES)                                                                SPREAD_RISK,
                COVAR_SAMP(BPC.RESIDUAL, BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) /
                STDDEV_SAMP(BPC.DAY_PROFIT) OVER (PARTITION BY BPC.L_FUND_ID ORDER BY BPC.L_TRADE_DATE) *
                SQRT(CONS.ONE_YEAR_TRADE_DATES)                                                                RESIDUAL_RISK
         FROM (SELECT BPC_TMP.*,
                      EXP(SUM(LN(1 + BPC_TMP.DAY_PROFIT))
                              OVER (PARTITION BY BPC_TMP.L_FUND_ID ORDER BY BPC_TMP.L_TRADE_DATE)) /
                      (1 + BPC_TMP.DAY_PROFIT) PROFIT_ADJ
               FROM BOND_PROFIT_CTB BPC_TMP) BPC,
              CONSTANTS CONS)
SELECT *
FROM BOND_PROFIT_RISK;


