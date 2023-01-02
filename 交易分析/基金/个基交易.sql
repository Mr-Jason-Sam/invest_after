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
     SINGLE_FUND_ANAYSIS AS (
         -- �������׷���
         SELECT
             -- ��������
             SIF.VC_WIND_CODE,
             MFD.VC_STOCK_CODE,
             -- ��������
             MFD.VC_STOCK_NAME,
             -- ����
             SIF.L_TRADE_DATE,
             -- ���������
             NVL(TFF.BUY_RATIO, 0)  AS                                                   BUY_RATIO,
             -- ����������
             NVL(TFF.SELL_RATIO, 0) AS                                                   SELL_RATIO,
             -- �����ۼ�������
             EXP(SUM(LN(CASE
                            WHEN MFD.VC_MARKET_TYPE IN ('����') THEN
                                DECODE(
                                        SIF.EN_NAV_ADJUSTED_PRE,
                                        0, NULL,
                                        SIF.EN_NAV_ADJUSTED / SIF.EN_NAV_ADJUSTED_PRE
                                    )
                            ELSE
                                DECODE(
                                        SIF.EN_PRICE_CLOSE_PRE,
                                        0, NULL,
                                        SIF.EN_PRICE_CLOSE / SIF.EN_PRICE_CLOSE_PRE
                                    ) END
                         )) OVER (PARTITION BY
                 SIF.VC_WIND_CODE ORDER BY SIF.L_TRADE_DATE)) - 1                        FUND_RANGE_PROFIT_RATIO,
             -- ��׼�ۼ�����
             EXP(SUM(LN(1 + SIF.EN_PROFIT_BENCH))
                     OVER (PARTITION BY SIF.VC_WIND_CODE ORDER BY SIF.L_TRADE_DATE)) - 1 BM_RANGE_PROFIT_RATIO
             -- TODO ��ҵ�ۼ�����
         FROM ZHFX.TSTOCKINFOFUND SIF
                  LEFT JOIN (SELECT DISTINCT TFF.L_FUND_ID,
                                             TFF.VC_WIND_CODE
                             FROM ZHFX.TTRADEFLOWFUND TFF,
                                  TD_ADJ
                             WHERE TFF.L_FUND_ID = ${ztbh}
                               AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE
                                 AND TD_ADJ.END_DATE) TRADE_FUND_CODE
                            ON SIF.VC_WIND_CODE = TRADE_FUND_CODE.VC_WIND_CODE
                  LEFT JOIN (SELECT TFF.L_FUND_ID,
                                    TFF.VC_STOCK_CODE,
                                    TFF.VC_WIND_CODE,
                                    TFF.L_TRADE_DATE,
                                    -- �������
                                    SUM(CASE
                                            WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '�깺')
                                                THEN DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0,
                                                            TFF.EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE)
                                            ELSE 0 END) BUY_RATIO,
                                    -- ��������
                                    SUM(CASE
                                            WHEN TFF.VC_BUSIN_TYPE IN ('����', 'ת��_ת��', '���')
                                                THEN DECODE(PA.FUND_ASSETS_NET_PRE, 0, 0,
                                                            TFF.EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE) * -1
                                            ELSE 0 END) SELL_RATIO
                             FROM ZHFX.TTRADEFLOWFUND TFF
                                      LEFT JOIN PD_ASSETS PA
                                                ON TFF.L_FUND_ID = PA.L_FUND_ID
                                                    AND TFF.L_TRADE_DATE = PA.L_TRADE_DATE,
                                  TD_ADJ
                             WHERE TFF.L_FUND_ID = ${ztbh}
                               AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE
                                 AND TD_ADJ.END_DATE
                             GROUP BY TFF.L_FUND_ID,
                                      TFF.VC_STOCK_CODE,
                                      TFF.VC_WIND_CODE,
                                      TFF.L_TRADE_DATE) TFF
                            ON SIF.VC_WIND_CODE = TFF.VC_WIND_CODE
                                AND SIF.L_TRADE_DATE = TFF.L_TRADE_DATE
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON SUBSTR(SIF.VC_WIND_CODE, 0, 6) = MFD.VC_STOCK_CODE
                 ,
              TD_ADJ
         WHERE MFD.VC_STOCK_CODE = ${FUND_CODE}
           AND SIF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE)
-- �������׷���
SELECT *
FROM SINGLE_FUND_ANAYSIS
ORDER BY L_TRADE_DATE
