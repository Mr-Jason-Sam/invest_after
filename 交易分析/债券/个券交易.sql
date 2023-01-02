WITH HOLDING_BOND_CODE AS (
    -- �ֲֹ�Ʊ��
    SELECT DISTINCT VC_WIND_CODE
    FROM ZHFX.THOLDINGDETAILBOND HDB
    WHERE HDB.L_FUND_ID IN (${fund_ids})
      AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     BOND_QUOTE AS (SELECT BOND_INFO.L_TRADE_DATE,
                           BOND_INFO.VC_WIND_CODE,
                           DECODE(
                                   BOND_INFO.L_RECOM_CNBD,
                               -- ��Ȩ
                                   1, BOND_INFO.EN_FULL_CNBD_IFEXE + BOND_INFO.EN_PRINCIPAL + BOND_INFO.EN_INTEREST,
                               -- ����Ȩ
                                   0, BOND_INFO.EN_FULL_CNBD + BOND_INFO.EN_PRINCIPAL + BOND_INFO.EN_INTEREST,
                                   NULL)                                                                  PRICE,
                           LAG(BOND_INFO.EN_FULL_CNBD, 1, NULL)
                               OVER (PARTITION BY BOND_INFO.VC_WIND_CODE ORDER BY BOND_INFO.L_TRADE_DATE) PRICE_PRE
                    FROM HOLDING_BOND_CODE HBC
                             LEFT JOIN ZHFX.TBONDINFO BOND_INFO ON HBC.VC_WIND_CODE = BOND_INFO.VC_WIND_CODE
                    WHERE BOND_INFO.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     TRADE_FLOW_BOND AS (
         -- ������ˮ����
         SELECT TFB.L_FUND_ID,
                TFB.L_TRADE_DATE,
                TFB.VC_WIND_CODE,
                TFB.VC_STOCK_NAME,
                TFB.EN_DEAL_PRICE,
                QUOTE.PRICE,
                CASE
                    WHEN TFB.VC_BUSIN_TYPE IN ('����', '��������')
                        THEN TFB.EN_DEAL_AMOUNT
                    ELSE 0 END                                                                       BUY_AMOUNT,
                CASE
                    WHEN TFB.VC_BUSIN_TYPE IN ('����', '��������')
                        THEN TFB.EN_LIQUIDATE_BALANCE
                    ELSE 0 END                                                                       BUY_BALANCE,
                CASE
                    WHEN TFB.VC_BUSIN_TYPE IN ('����') THEN TFB.EN_DEAL_AMOUNT
                    ELSE 0 END                                                                       SELL_AMOUNT,
                CASE
                    WHEN TFB.VC_BUSIN_TYPE IN ('����') THEN TFB.EN_LIQUIDATE_BALANCE
                    ELSE 0 END                                                                       SELL_BALANCE,
                CASE WHEN TFB.VC_BUSIN_TYPE IN ('��Ϣ�Ҹ�') THEN TFB.EN_DEAL_AMOUNT ELSE 0 END       CASH_AMOUNT,
                CASE WHEN TFB.VC_BUSIN_TYPE IN ('��Ϣ�Ҹ�') THEN TFB.EN_LIQUIDATE_BALANCE ELSE 0 END CASH_BALANCE
         FROM ZHFX.TTRADEFLOWBOND TFB
                  LEFT JOIN BOND_QUOTE QUOTE
                            ON TFB.VC_WIND_CODE = QUOTE.VC_WIND_CODE AND TFB.L_TRADE_DATE = QUOTE.L_TRADE_DATE
         WHERE TFB.L_FUND_ID IN (${fund_ids})
           AND TFB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     TRADE_DETAILS AS (
         -- ����ͳ��
         -- ��ϸ���
         SELECT BOND_FLOW_RANGE_INFO.L_FUND_ID                                                        FUND_ID,
                BOND_FLOW_RANGE_INFO.VC_WIND_CODE                                                     BOND_CODE,
                BOND_FLOW_RANGE_INFO.BOND_NAME,
                -- ���״���
                SUM(BOND_FLOW_RANGE_INFO.TRADE_CNT)                                                   TRADE_COUNT,
                -- �������
                SUM(BOND_FLOW_RANGE_INFO.BUY_CNT)                                                     BUY_COUNT,
                -- ��������
                SUM(BOND_FLOW_RANGE_INFO.SELL_CNT)                                                    SELL_COUNT,

                -- ���׶�
                SUM(BOND_FLOW_RANGE_INFO.TOTAL_BALANCE)    AS                                         TOTAL_BALANCE,
                -- �����
                SUM(BOND_FLOW_RANGE_INFO.BUY_BALANCE)      AS                                         BUY_BALANCE,
                -- ������
                SUM(BOND_FLOW_RANGE_INFO.SELL_BALANCE)     AS                                         SELL_BALANCE,

                -- ���ױ���
                SUM(BOND_FLOW_RANGE_INFO.TRADE_RATIO)      AS                                         TRADE_RATIO,
                -- ���뽻�ױ���
                SUM(BOND_FLOW_RANGE_INFO.BUY_TRADE_RATIO)  AS                                         BUY_TRADE_RATIO,
                -- �������ױ���
                SUM(BOND_FLOW_RANGE_INFO.SELL_TRADE_RATIO) AS                                         SELL_TRADE_RATIO,

                -- ����ʤ��
                DECODE(SUM(BOND_FLOW_RANGE_INFO.TRADE_CNT), 0, 0,
                       SUM(BOND_FLOW_RANGE_INFO.TRADE_WIN_CNT) / SUM(BOND_FLOW_RANGE_INFO.TRADE_CNT)) TRADE_WIN_RATIO,
                -- ����ʤ��
                DECODE(SUM(BOND_FLOW_RANGE_INFO.BUY_CNT), 0, 0,
                       SUM(BOND_FLOW_RANGE_INFO.BUY_WIN_CNT) / SUM(BOND_FLOW_RANGE_INFO.BUY_CNT))     BUY_WIN_RATIO,
                -- ����ʤ��
                DECODE(SUM(BOND_FLOW_RANGE_INFO.SELL_CNT), 0, 0,
                       SUM(BOND_FLOW_RANGE_INFO.SELL_WIN_CNT) / SUM(BOND_FLOW_RANGE_INFO.SELL_CNT))   SELL_WIN_RATIO
         FROM (SELECT BOND_FLOW_DATE_INFO.*,
                      -- ���ױ��� = ���׶� / ���չ�Ʊ��Ͼ��ʲ�
                      DECODE(BOND_FLOW_DATE_INFO.BOND_ASSETS, 0, 0,
                             BOND_FLOW_DATE_INFO.TOTAL_BALANCE / BOND_FLOW_DATE_INFO.BOND_ASSETS)     TRADE_RATIO,
                      -- ���뽻�ױ���
                      DECODE(BOND_FLOW_DATE_INFO.BOND_ASSETS, 0, 0,
                             BOND_FLOW_DATE_INFO.BUY_BALANCE / BOND_FLOW_DATE_INFO.BOND_ASSETS)       BUY_TRADE_RATIO,
                      --�������ױ���
                      DECODE(BOND_FLOW_DATE_INFO.BOND_ASSETS, 0, 0,
                             ABS(BOND_FLOW_DATE_INFO.SELL_BALANCE) / BOND_FLOW_DATE_INFO.BOND_ASSETS) SELL_TRADE_RATIO
               FROM (SELECT TFB.*,
                            -- ծȯ�ʲ�
                            BOND_BALANCE_INFO.ASSETS_PRE + GREATEST(BOND_BALANCE_INFO.TOTAL_BALANCE, 0) BOND_ASSETS
                     FROM (SELECT L_FUND_ID,
                                  VC_WIND_CODE,
                                  MAX(VC_STOCK_NAME)                                BOND_NAME,
                                  L_TRADE_DATE,
                                  -- ���״���
                                  COUNT(1)                                          TRADE_CNT,
                                  -- �������
                                  SUM(CASE WHEN BUY_BALANCE > 0 THEN 1 ELSE 0 END)  BUY_CNT,
                                  -- ��������
                                  SUM(CASE WHEN SELL_BALANCE > 0 THEN 1 ELSE 0 END) SELL_CNT,
                                  -- ���׶�
                                  SUM(BUY_BALANCE + SELL_BALANCE)                   TOTAL_BALANCE,
                                  -- �����
                                  SUM(BUY_BALANCE)                                  BUY_BALANCE,
                                  -- ������
                                  SUM(SELL_BALANCE)                                 SELL_BALANCE,
                                  -- ����ʤ������
                                  SUM(CASE
                                          WHEN BUY_BALANCE > 0 AND EN_DEAL_PRICE <= PRICE OR
                                               SELL_BALANCE > 0 AND EN_DEAL_PRICE >= PRICE THEN 1
                                          ELSE 0
                                      END)                                          TRADE_WIN_CNT,
                                  -- ����ʱ������ʤ������
                                  SUM(CASE
                                          WHEN BUY_BALANCE > 0 AND EN_DEAL_PRICE <= PRICE THEN 1
                                          ELSE 0
                                      END)                                          BUY_WIN_CNT,
                                  -- ����ʱ������ʤ������
                                  SUM(CASE
                                          WHEN SELL_BALANCE > 0 AND EN_DEAL_PRICE >= PRICE THEN 1
                                          ELSE 0
                                      END)                                          SELL_WIN_CNT
                           FROM TRADE_FLOW_BOND
                           GROUP BY L_FUND_ID, VC_WIND_CODE, L_TRADE_DATE) TFB
                              LEFT JOIN (SELECT L_FUND_ID,
                                                L_TRADE_DATE,
                                                EN_FUND_ASSET_NET_PRE ASSETS_PRE,
                                                EN_APPLY_BAL + EN_REDEEM_BAL + EN_APPLY_DIRECT + EN_REDEEM_DIRECT +
                                                EN_FUND_DIVIDEND + EN_FUND_DIVIDEND_INVEST + EN_APPEND_BAL +
                                                EN_EXTRACT_BAL        TOTAL_BALANCE
                                         FROM ZHFX.TFUNDRETURNDETAIL
                                         WHERE L_FUND_ID IN (${fund_ids})
                                           AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}) BOND_BALANCE_INFO
                                        ON TFB.L_FUND_ID = BOND_BALANCE_INFO.L_FUND_ID
                                            AND TFB.L_TRADE_DATE =
                                                BOND_BALANCE_INFO.L_TRADE_DATE) BOND_FLOW_DATE_INFO) BOND_FLOW_RANGE_INFO
         GROUP BY BOND_FLOW_RANGE_INFO.L_FUND_ID, BOND_FLOW_RANGE_INFO.VC_WIND_CODE, BOND_FLOW_RANGE_INFO.BOND_NAME)
SELECT TD.FUND_ID,
       TD.BOND_CODE,
       TD.BOND_NAME,
       TD.TRADE_COUNT,
       TD.BUY_COUNT,
       TD.SELL_COUNT,
       TD.TOTAL_BALANCE,
       TD.TRADE_RATIO,
       TD.BUY_BALANCE,
       TD.BUY_TRADE_RATIO,
       TD.SELL_BALANCE,
       TD.SELL_TRADE_RATIO,
       TD.TRADE_WIN_RATIO,
       TD.BUY_WIN_RATIO,
       TD.SELL_WIN_RATIO
FROM TRADE_DETAILS TD
ORDER BY FUND_ID, TD.BUY_TRADE_RATIO DESC;