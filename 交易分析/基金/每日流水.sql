WITH TD_ADJ AS (
    -- �����յ�����������ʼ�յ�������棬����Ҫ����T-1�����ݣ�
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- �Ѵ���������Ŀ�ʼ����
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd')
        AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')),
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
           AND FRD.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE),
     TRADE_FLOW_BUSIN_TYPE AS (
         -- ������ˮ
         SELECT TFF.L_FUND_ID,
                TFF.L_TRADE_DATE,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('����') THEN
                            DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0, EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE)
                        ELSE 0 END) BUY_RATIO,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('����') THEN
                                DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0, EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE) * -1
                        ELSE 0 END) SELL_RATIO,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('ת��_ת��') THEN
                            DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0, EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE)
                        ELSE 0 END) CHANGE_IN_RATIO,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('ת��_ת��') THEN
                                DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0, EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE) * -1
                        ELSE 0 END) CHANGE_OUT_RATIO,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('�깺') THEN
                            DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0, EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE)
                        ELSE 0 END) APPLY_RATIO,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('���') THEN
                                DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0, EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE) * -1
                        ELSE 0 END) REDEEM_RATIO
         FROM ZHFX.TTRADEFLOWFUND TFF
                  LEFT JOIN PD_ASSETS PA
                            ON TFF.L_FUND_ID = PA.L_FUND_ID
                                AND TFF.L_TRADE_DATE = PA.L_TRADE_DATE,
              TD_ADJ
         WHERE TFF.L_FUND_ID = ${ztbh}
           AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
         GROUP BY TFF.L_FUND_ID, TFF.L_TRADE_DATE
         ORDER BY TFF.L_FUND_ID,
                  TFF.L_TRADE_DATE)
SELECT *
FROM TRADE_FLOW_BUSIN_TYPE,
     TD_ADJ
ORDER BY L_TRADE_DATE
-- WHERE L_TRADE_DATE = TD_ADJ.END_DATE