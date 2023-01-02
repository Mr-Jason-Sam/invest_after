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
     TRADE_FLOW_BUSIN_TYPE AS (
         -- ������ˮ
         SELECT TFF.L_FUND_ID,
                TFF.L_TRADE_DATE,
                TFF.VC_WIND_CODE,
                MFD.VC_STOCK_NAME,
                TFF.VC_BUSIN_TYPE,
                -- ��Ԫ����
                EN_LIQUIDATE_BALANCE / 1e4                    TRADE_BALANCE,
                -- ��Ԫ��Ԫ
                EN_FEE_TRADE                                    FEE_BALACE,
                DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL, EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE) TRADE_RATIO,
                DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL, EN_FEE_TRADE / PA.FUND_ASSETS_NET_PRE)         FEE_RATIO
         FROM ZHFX.TTRADEFLOWFUND TFF
                  LEFT JOIN PD_ASSETS PA
                            ON TFF.L_FUND_ID = PA.L_FUND_ID
                                AND TFF.L_TRADE_DATE = PA.L_TRADE_DATE
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON TFF.VC_WIND_CODE = MFD.VC_WIND_CODE,
              TD_ADJ
         WHERE TFF.L_FUND_ID = ${ztbh}
           AND TFF.VC_BUSIN_TYPE IN ('����', '����', 'ת��_ת��',
                                     'ת��_ת��', '�깺', '���')
           AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
         ORDER BY TFF.L_FUND_ID,
                  TFF.L_TRADE_DATE,
                  TFF.VC_WIND_CODE,
                  TRADE_RATIO)
SELECT TFBT.*
FROM TRADE_FLOW_BUSIN_TYPE TFBT
--      ,TD_ADJ
-- WHERE L_TRADE_DATE = TD_ADJ.END_DATE
