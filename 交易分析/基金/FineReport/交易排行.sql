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
    FOF_ASSETS AS (
        -- ��������ʲ�
        SELECT
            HDF.L_FUND_ID,
            HDF.L_TRADE_DATE,
            SUM(HDF.EN_MARKET_INIT) FOF_MV_INIT
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
               FRD.EN_INCOME_REWARD FUND_ASSETS_NET_PRE
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
     TRADE_FLOW_TOP_INFO AS (
         -- ������ˮ��Ϣ
         SELECT TFF.*,
                MFD.VC_WIND_CODE,
                MFD.VC_STOCK_NAME,
                MFD.VC_FUND_TYPE_WIND,
                MFD.VC_FUND_TYPE_WIND_SECOND,
                ROW_NUMBER() OVER (PARTITION BY TFF.L_FUND_ID ORDER BY BUY_PD_RATIO DESC)   BUY_PD_RANK,
                ROW_NUMBER() OVER (PARTITION BY TFF.L_FUND_ID ORDER BY SELL_PD_RATIO DESC)  SELL_PD_RANK,
                ROW_NUMBER() OVER (PARTITION BY TFF.L_FUND_ID ORDER BY BUY_FOF_RATIO DESC)  BUY_FOF_RANK,
                ROW_NUMBER() OVER (PARTITION BY TFF.L_FUND_ID ORDER BY SELL_FOF_RATIO DESC) SELL_FOF_RANK,
                ROW_NUMBER() OVER (PARTITION BY TFF.L_FUND_ID ORDER BY BUY_BALANCE DESC)    BUY_BALANCE_RANK,
                ROW_NUMBER() OVER (PARTITION BY TFF.L_FUND_ID ORDER BY SELL_BALANCE DESC)   SELL_BALANCE_RANK
         FROM (SELECT TFF.L_FUND_ID,
                      TFF.VC_STOCK_CODE,
                      SUM(CASE
                              WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '�깺')
                                  THEN TFF.EN_LIQUIDATE_BALANCE
                              ELSE 0 END) / 1e4 BUY_BALANCE,
                      SUM(CASE
                              WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '���')
                                  THEN TFF.EN_LIQUIDATE_BALANCE
                              ELSE 0 END) / 1e4 SELL_BALANCE,
                      SUM(CASE
                              WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '�깺')
                                  THEN TFF.EN_FEE_TRADE
                              ELSE 0 END)       BUY_FEE,
                      SUM(CASE
                              WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '���')
                                  THEN TFF.EN_FEE_TRADE
                              ELSE 0 END)       SELL_FEE,
                      SUM(CASE
                              WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '�깺')
                                  THEN DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0,
                                              TFF.EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE)
                              ELSE 0 END)       BUY_PD_RATIO,
                      SUM(CASE
                              WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '���')
                                  THEN DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0,
                                              TFF.EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE)
                              ELSE 0 END)       SELL_PD_RATIO,
                      SUM(CASE
                              WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '�깺')
                                  THEN DECODE(FA.FOF_MV_INIT, 0, 0, TFF.EN_LIQUIDATE_BALANCE / FA.FOF_MV_INIT)
                              ELSE 0 END)       BUY_FOF_RATIO,
                      SUM(CASE
                              WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '���')
                                  THEN DECODE(FA.FOF_MV_INIT, 0, 0, TFF.EN_LIQUIDATE_BALANCE / FA.FOF_MV_INIT)
                              ELSE 0 END)       SELL_FOF_RATIO
               FROM ZHFX.TTRADEFLOWFUND TFF
                        LEFT JOIN PD_ASSETS PA
                                  ON TFF.L_FUND_ID = PA.L_FUND_ID
                                      AND TFF.L_TRADE_DATE = PA.L_TRADE_DATE
                    LEFT JOIN FOF_ASSETS FA
                                  ON TFF.L_FUND_ID = FA.L_FUND_ID
                                      AND TFF.L_TRADE_DATE = FA.L_TRADE_DATE,
                    TD_ADJ
               WHERE TFF.L_FUND_ID = ${ztbh}
                 AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
               GROUP BY TFF.L_FUND_ID, TFF.VC_STOCK_CODE) TFF
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON TFF.VC_STOCK_CODE = MFD.VC_STOCK_CODE)
-- ��������Topֵ
SELECT TLTI.*
FROM TRADE_FLOW_TOP_INFO TLTI
ORDER BY L_FUND_ID, SELL_FOF_RANK
