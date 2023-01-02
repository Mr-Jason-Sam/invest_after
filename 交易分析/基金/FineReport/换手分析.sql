WITH TD_ADJ AS (
    -- �����յ�����������ʼ�յ�������棬����Ҫ����T-1�����ݣ�
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- �Ѵ���������Ŀ�ʼ����
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd')
        AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')),
     TRADE_DATES AS (SELECT CLD.L_TRADE_DATE
                     FROM ZHFX.TCALENDAR CLD,
                          TD_ADJ
                     WHERE L_DATE = L_TRADE_DATE
                       AND CLD.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE),
     TRADE_FLOW_TD AS (SELECT MIN(TFF.L_TRADE_DATE) TF_BEGIN, MAX(TFF.L_TRADE_DATE) TF_END
                       FROM ZHFX.TTRADEFLOWFUND TFF,
                            TD_ADJ
                       WHERE TFF.L_FUND_ID = ${ztbh}
                         AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE),
    FOF_ASSETS AS (
        -- ��������ʲ�
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
        -- ��Ʒ�ʲ���Ϣ
        SELECT FRD.L_FUND_ID,
               FRD.L_TRADE_DATE,
               FRD.EN_FUND_ASSET_NET,
               FRD.EN_FUND_ASSET_NET_PRE +
                   -- ������(���ݿ���Ϊ��������Ϊ�ۼ���)
               FRD.EN_APPLY_BAL + FRD.EN_APPLY_DIRECT + FRD.EN_APPEND_BAL +
                   -- ������
               FRD.EN_REDEEM_BAL + FRD.EN_REDEEM_DIRECT + FRD.EN_EXTRACT_BAL +
                   -- �������ֺ졢�ֺ���Ͷ��ҵ������
               FRD.EN_FUND_DIVIDEND + FRD.EN_FUND_DIVIDEND_INVEST +
               FRD.EN_INCOME_REWARD FUND_ASSETS_NET_PRE,
            FRD.EN_INCOME_REWARD                        EQUITY,

                      -- T�տ��þ����꣺ �깺�� - ��ضֱ�� + ר����
                      (FRD.EN_APPLY_DIRECT + FRD.EN_APPEND_BAL) -
                      (FRD.EN_REDEEM_DIRECT + FRD.EN_EXTRACT_BAL) APPLY_REDEEM_NET,
                      -- T�տ����깺��
                      FRD.EN_APPLY_DIRECT + FRD.EN_APPEND_BAL     APPLY_ASSTES,
                      -- T+1�����þ����꣺�깺�� - ��ض������
                      FRD.EN_APPLY_BAL - FRD.EN_REDEEM_BAL        APPLY_REDEEM_NET_NEXT
        FROM ZHFX.TFUNDRETURNDETAIL FRD,
             TD_ADJ
        WHERE FRD.L_FUND_ID = ${ztbh}
          AND FRD.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
    ),
     TRADE_FLOW_BASE AS (
         -- ������ˮ
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '�깺') THEN EN_LIQUIDATE_BALANCE
                        ELSE 0 END) BUY_BALANCE,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '���') THEN EN_LIQUIDATE_BALANCE
                        ELSE 0 END) SELL_BALANCE,
                SUM(EN_FEE_TRADE)   FEE
         FROM ZHFX.TTRADEFLOWFUND,
              TD_ADJ
         WHERE L_FUND_ID = ${ztbh}
           AND L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
         GROUP BY L_FUND_ID, L_TRADE_DATE),
     TURNOVER_ANAYSIS AS (
         -- ���ַ���
         SELECT PA.L_FUND_ID,
                PA.L_TRADE_DATE,
                -- ��ϻ�����
                DECODE(
                        FA.FOF_MV_INIT,
                        0, NULL,
                        (NVL(TFB.BUY_BALANCE, 0) + NVL(TFB.SELL_BALANCE, 0)) /
                        (FA.FOF_MV_INIT + GREATEST(NVL(TFB.BUY_BALANCE, 0) - NVL(TFB.SELL_BALANCE, 0), 0))
                    )                                                FOF_TURNOVER_RATIO,
                -- ��Ʒ������
                DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL, (NVL(TFB.BUY_BALANCE, 0) + NVL(TFB.SELL_BALANCE, 0)) /
                (PA.FUND_ASSETS_NET_PRE + GREATEST(PA.EQUITY, 0)))
                    PD_TURNOVER_RATIO,
                -- ��Ʒ���������ʣ��ʲ��䶯Ӱ�죩
             DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL , (NVL(TFB.BUY_BALANCE, 0) + NVL(TFB.SELL_BALANCE, 0) -
                 ABS(PA.EQUITY) * FA.FOF_MV_INIT / PA.FUND_ASSETS_NET_PRE) /
                (PA.FUND_ASSETS_NET_PRE + GREATEST(PA.EQUITY, 0)))
                    PD_ACTIVE_ASSETS_TR,
                -- ��Ʒ���������ʣ��տ����ʲ�Ӱ�죩
             DECODE(PA.FUND_ASSETS_NET_PRE + PA.APPLY_ASSTES, 0, NULL, CASE
                    -- T��ʣ����ý��
                    -- 1��T�վ����� - ABS(LEAST(T+1�վ�����, 0)) >= 0 => T���ʽ�ʣ�౻������
                    WHEN GREATEST(PA.APPLY_REDEEM_NET, 0) - ABS(LEAST(PA.APPLY_REDEEM_NET_NEXT, 0)) >= 0 THEN
                        (GREATEST(NVL(TFB.BUY_BALANCE, 0) -
                                  (GREATEST(PA.APPLY_REDEEM_NET, 0) - ABS(LEAST(PA.APPLY_REDEEM_NET_NEXT, 0))), 0) +
                         NVL(TFB.SELL_BALANCE, 0))
                    -- 2��T�վ����� > 0 AND T�վ����� - ABS(LEAST(T+1�վ�����, 0)) < 0 => T�ձ���������Ԥ��T+1��ؽ��
                    WHEN ABS(LEAST(PA.APPLY_REDEEM_NET_NEXT, 0)) - GREATEST(PA.APPLY_REDEEM_NET, 0) >= 0 THEN
                        (GREATEST(NVL(TFB.SELL_BALANCE, 0) -
                                  (ABS(LEAST(PA.APPLY_REDEEM_NET_NEXT, 0)) - GREATEST(PA.APPLY_REDEEM_NET, 0)), 0) +
                         NVL(TFB.BUY_BALANCE, 0))
                    ELSE (NVL(TFB.SELL_BALANCE, 0) + NVL(TFB.BUY_BALANCE, 0))
                    END / (PA.FUND_ASSETS_NET_PRE + PA.APPLY_ASSTES))
                 PD_ACTIVE_USE_TR,
                -- ���׷���
                NVL(TFB.FEE, 0)                                      TRADE_FEE,
                -- ���׽��
                NVL(TFB.BUY_BALANCE + TFB.SELL_BALANCE, 0)           TRADE_BALANCE,
                -- ���׳ɱ�
                DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL, NVL(TFB.FEE, 0) / PA.FUND_ASSETS_NET_PRE)            TRADE_FEE_PD_RATIO
         FROM PD_ASSETS PA
             LEFT JOIN FOF_ASSETS FA
                 ON PA.L_FUND_ID = FA.L_FUND_ID
                        AND PA.L_TRADE_DATE = FA.L_TRADE_DATE
                  LEFT JOIN TRADE_FLOW_BASE TFB
                            ON PA.L_FUND_ID = TFB.L_FUND_ID
                                AND PA.L_TRADE_DATE = TFB.L_TRADE_DATE),
     TURNOVER_RESULT AS (
         -- �����ʷ������
         SELECT TA.L_FUND_ID,
                TA.L_TRADE_DATE,
                -- ��Ʒ������
                TA.PD_TURNOVER_RATIO,
                -- ��ϻ�����
                TA.FOF_TURNOVER_RATIO,
                -- ��Ʒ���������ʣ��ʲ����棩
                TA.PD_ACTIVE_ASSETS_TR,
                -- ��Ʒ���������ʣ������ʽ���棩
                TA.PD_ACTIVE_USE_TR,
                -- ���׽��
                TA.TRADE_BALANCE,
                -- ���׷���
                TA.TRADE_FEE,
                -- ���ñ�������Ʒ��
                TA.TRADE_FEE_PD_RATIO,
                -- �ۼ�
                COUNT(TA.L_TRADE_DATE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         SAMPLE,
                MIN(TA.L_TRADE_DATE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         MIN_TRADE_DATE,
                MAX(TA.L_TRADE_DATE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         MAX_TRADE_DATE,
                -- �ۼƲ�Ʒ������
                SUM(TA.PD_TURNOVER_RATIO) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         PD_TR_ACC,
                -- �ۼƻ�����ϻ�����
                SUM(TA.FOF_TURNOVER_RATIO) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         FOF_TR_ACC,
                -- �ۼ�������Ʒ�����ʣ��ʲ����棩
                SUM(TA.PD_ACTIVE_ASSETS_TR) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         PD_ACTIVE_ASSETS_TR_ACC,
                -- �ۼ�������Ʒ�����ʣ������ʽ���棩
                SUM(TA.PD_ACTIVE_USE_TR) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         PD_ACTIVE_USE_TR_ACC,
                -- �ۼƽ��׷���
                SUM(TA.TRADE_FEE_PD_RATIO) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         TRADE_FEE_PD_RATIO_ACC,
                -- �ۼƽ��׷�
                SUM(TA.TRADE_FEE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         TRADE_FEE_ACC,
                -- ƽ�����׷���
                DECODE(SUM(TA.TRADE_BALANCE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE),
                       0, NULL,
                       SUM(TA.TRADE_FEE) OVER ( PARTITION BY
                           TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE) /
                       SUM(TA.TRADE_BALANCE) OVER ( PARTITION BY
                           TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)) AVG_FEE_RATIO
         FROM TURNOVER_ANAYSIS TA),
     TURNOVER_END_DATE AS (SELECT MAX(L_TRADE_DATE) MAX_TD
                           FROM TURNOVER_RESULT)

-- ������
SELECT TR.*,
       -- �껯�ۼƲ�Ʒ������
       TR.PD_TR_ACC * (244 / SAMPLE)               PD_TR_ANN,
       -- �껯�ۼƻ�����ϻ�����
       TR.FOF_TR_ACC * (244 / SAMPLE)              FOF_TR_ANN,
       -- �껯�ۼ�������Ʒ�����ʣ��ʽ�㣩
       TR.PD_ACTIVE_ASSETS_TR_ACC * (244 / SAMPLE) PD_ACTIVE_ASSETS_TR_ANN,
       -- �껯�ۼƲ�Ʒ�����ʣ������ʽ�
       TR.PD_ACTIVE_USE_TR_ACC * (244 / SAMPLE)    PD_ACTIVE_USE_TR_ANN,
       --�껯���׷���
       TR.TRADE_FEE_PD_RATIO_ACC * (244 / SAMPLE)  TRADE_FEE_PD_RATIO_ANN
FROM TURNOVER_RESULT TR
--    , TURNOVER_END_DATE RED
-- WHERE TR.L_TRADE_DATE = RED.MAX_TD