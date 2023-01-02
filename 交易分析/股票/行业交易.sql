WITH CONSTANTS AS (
    -- ����
    SELECT 'SW_1'      SW_1,
           'SW_2'      SW_2,
           'SW_3'      SW_3,
           'SEC_1'     SEC_1,
           'SEC_2'     SEC_2,
           'WIND_1'    WIND_1,
           'WIND_2'    WIND_2,
           'WIND_3'    WIND_3,
           'ZX_1'      ZX_1,
           'ZX_2'      ZX_2,
           'ZX_3'      ZX_3,
           '000300.SH' HZ_300,
           '.HK'       HK_SUFFIX,
           'HK'        HK_NAME,
           '---'       OTHERS_TAG,
           'OTHERS'    OTHERS_NAME
    FROM DUAL),
     TRADE_DATE_BEGIN_PRE AS (
         -- �����յ�����������ʼ�յ�������棬����Ҫ����T-1�����ݣ�
         SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST
         FROM ZHFX.TCALENDAR
         WHERE L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     TRADE_FLOW_STK AS (
         -- ����������
         SELECT FLOW.*,
                CASE
                    WHEN SUBSTR(FLOW.VC_WIND_CODE, -3) = CONS.HK_SUFFIX THEN CONS.HK_NAME
                    WHEN FLOW.INDUSTRY_TYPE_ORIGIN = CONS.OTHERS_TAG THEN CONS.OTHERS_NAME
                    ELSE NVL(FLOW.INDUSTRY_TYPE_ORIGIN, CONS.OTHERS_NAME) END INDUSTRY_TYPE
         FROM (SELECT FLOW.L_FUND_ID,
                      FLOW.L_TRADE_DATE,
                      FLOW.VC_WIND_CODE,
                      SIS.VC_STOCK_NAME,
                      -- �����
                      CASE
                          WHEN FLOW.VC_BUSIN_TYPE IN ('����', '��������') THEN FLOW.EN_DEAL_BALANCE
                          ELSE 0 END BUY_BALANCE,
                      -- ������
                      CASE
                          WHEN FLOW.VC_BUSIN_TYPE IN ('����', '��������') THEN FLOW.EN_DEAL_BALANCE
                          ELSE 0 END SELL_BALANCE,
                      -- �ɽ����
                      FLOW.EN_DEAL_PRICE,
                      -- ���̼�
                      SIS.EN_PRICE_CLOSE,
                      DECODE(
                              ${industry_id},
                              CONS.SW_1, SIS.VC_INDUSTRY_SW_FIRST,
                              CONS.SW_2, SIS.VC_INDUSTRY_SW_SECOND,
                              CONS.SW_3, SIS.VC_INDUSTRY_SW_THIRD,
                              CONS.SEC_1, SIS.VC_INDUSTRY_SEC_FIRST,
                              CONS.SEC_2, SIS.VC_INDUSTRY_SEC_SECOND,
                              CONS.WIND_1, SIS.VC_INDUSTRY_WIND_FIRST,
                              CONS.WIND_2, SIS.VC_INDUSTRY_WIND_SECOND,
                              CONS.WIND_3, SIS.VC_INDUSTRY_WIND_THIRD,
                              SIS.VC_INDUSTRY_SW_FIRST
                          )          INDUSTRY_TYPE_ORIGIN
               FROM ZHFX.TTRADEFLOWSHARE FLOW
                        LEFT JOIN ZHFX.TSTOCKINFOSHARE SIS
                                  ON FLOW.L_TRADE_DATE = SIS.L_TRADE_DATE AND FLOW.VC_WIND_CODE = SIS.VC_WIND_CODE
                       ,
                    CONSTANTS CONS
               WHERE FLOW.L_FUND_ID = ${fund_id}
                 AND FLOW.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
                 AND FLOW.VC_BUSIN_TYPE IN ('����', '��������', '����', '��������')) FLOW,
              CONSTANTS CONS),
     TRADE_DETAILS AS (
         -- ��ϸ���
         SELECT FLOW_RANGE_INFO.L_FUND_ID                FUND_ID,
                FLOW_RANGE_INFO.INDUSTRY_TYPE,
                -- ���ױ���
                SUM(FLOW_RANGE_INFO.TRADE_RATIO)      AS TRADE_RATIO,
                -- ���뽻�ױ���
                SUM(FLOW_RANGE_INFO.BUY_TRADE_RATIO)  AS BUY_TRADE_RATIO,
                -- �������ױ���
                SUM(FLOW_RANGE_INFO.SELL_TRADE_RATIO) AS SELL_TRADE_RATIO,
                -- ��ҵ������
                SUM(FLOW_RANGE_INFO.IDS_CHANGE_RATIO) AS IDS_CHANGE_RATIO
         FROM (SELECT FLOW_DATE_INFO.*,
                      -- ���ױ��� = ���׶� / ���չ�Ʊ��Ͼ��ʲ�
                      DECODE(FLOW_DATE_INFO.STK_ASSETS, 0, 0,
                             FLOW_DATE_INFO.TOTAL_BALANCE / FLOW_DATE_INFO.STK_ASSETS) TRADE_RATIO,
                      -- ���뽻�ױ���
                      DECODE(FLOW_DATE_INFO.STK_ASSETS, 0, 0,
                             FLOW_DATE_INFO.BUY_BALANCE / FLOW_DATE_INFO.STK_ASSETS)   BUY_TRADE_RATIO,
                      --�������ױ���
                      DECODE(FLOW_DATE_INFO.STK_ASSETS, 0, 0,
                             FLOW_DATE_INFO.SELL_BALANCE / FLOW_DATE_INFO.STK_ASSETS)  SELL_TRADE_RATIO,
                      -- ��ҵ������
                      DECODE(FLOW_DATE_INFO.IDS_ASSETS, 0, 0,
                             FLOW_DATE_INFO.TOTAL_BALANCE / FLOW_DATE_INFO.IDS_ASSETS) IDS_CHANGE_RATIO
               FROM (SELECT IDS_TFB.*,
                            -- �ʲ�
                            DECODE(FUND_ASSETS.STK_ASSETS_PRE, 0, 0,
                                   FUND_ASSETS.STK_ASSETS_PRE + GREATEST(BALANCE_INFO.NAV_BALANCE, 0)) STK_ASSETS,
                            DECODE(IDS_ASSETS.IDS_MV_PRE, 0, 0,
                                   IDS_ASSETS.IDS_MV_PRE + GREATEST(IDS_TFB.NAV_BALANCE, 0)) AS        IDS_ASSETS
                     FROM (SELECT L_FUND_ID,
                                  L_TRADE_DATE,
                                  INDUSTRY_TYPE,
                                  -- �ܶ�
                                  SUM(BUY_BALANCE + SELL_BALANCE) TOTAL_BALANCE,
                                  -- �������
                                  SUM(BUY_BALANCE - SELL_BALANCE) NAV_BALANCE,
                                  -- �����
                                  SUM(BUY_BALANCE)                BUY_BALANCE,
                                  -- ������
                                  SUM(SELL_BALANCE)               SELL_BALANCE
                           FROM TRADE_FLOW_STK
                           GROUP BY L_FUND_ID, L_TRADE_DATE, INDUSTRY_TYPE) IDS_TFB
                              LEFT JOIN (
                         -- ����T-1�ʲ�
                         SELECT L_FUND_ID,
                                L_TRADE_DATE,
                                -- T-1��Ʊ���ʲ�
                                LAG(EN_SHARE_ASSET, 1, 0)
                                    OVER (PARTITION BY L_FUND_ID ORDER BY L_TRADE_DATE) STK_ASSETS_PRE
                         FROM ZHFX.TFUNDASSET,
                              TRADE_DATE_BEGIN_PRE
                         WHERE L_FUND_ID = ${fund_id}
                           AND L_TRADE_DATE BETWEEN TRADE_DATE_BEGIN_PRE.BEGIN_DATE_LAST AND ${end_date}) FUND_ASSETS
                                        ON IDS_TFB.L_FUND_ID = FUND_ASSETS.L_FUND_ID AND
                                           IDS_TFB.L_TRADE_DATE = FUND_ASSETS.L_TRADE_DATE
                              LEFT JOIN (
                         -- ����T��ծȯ���׾������
                         SELECT L_FUND_ID,
                                L_TRADE_DATE,
                                SUM(BUY_BALANCE - SELL_BALANCE) NAV_BALANCE
                         FROM TRADE_FLOW_STK
                         GROUP BY L_FUND_ID, L_TRADE_DATE) BALANCE_INFO
                                        ON IDS_TFB.L_FUND_ID = BALANCE_INFO.L_FUND_ID AND
                                           IDS_TFB.L_TRADE_DATE = BALANCE_INFO.L_TRADE_DATE
                              LEFT JOIN(
                         -- ��ҵT-1�ʲ�
                         SELECT HDS.L_FUND_ID,
                                HDS.L_TRADE_DATE,
                                HDS.INDUSTRY_TYPE,
                                SUM(HDS.MV_PRE) IDS_MV_PRE
                         FROM (SELECT HDS.*,
                                      CASE
                                          WHEN SUBSTR(HDS.VC_WIND_CODE, -3) = CONS.HK_SUFFIX THEN CONS.HK_NAME
                                          WHEN HDS.INDUSTRY_TYPE_ORIGIN = CONS.OTHERS_TAG THEN CONS.OTHERS_NAME
                                          ELSE NVL(HDS.INDUSTRY_TYPE_ORIGIN, CONS.OTHERS_NAME) END INDUSTRY_TYPE
                               FROM (SELECT HDS.L_FUND_ID,
                                            HDS.L_TRADE_DATE,
                                            HDS.VC_WIND_CODE,
                                            HDS.EN_VALUE_MARKET_PRE MV_PRE,
                                            DECODE(
                                                    ${industry_id},
                                                    CONS.SW_1, SIS.VC_INDUSTRY_SW_FIRST,
                                                    CONS.SW_2, SIS.VC_INDUSTRY_SW_SECOND,
                                                    CONS.SW_3, SIS.VC_INDUSTRY_SW_THIRD,
                                                    CONS.SEC_1, SIS.VC_INDUSTRY_SEC_FIRST,
                                                    CONS.SEC_2, SIS.VC_INDUSTRY_SEC_SECOND,
                                                    CONS.WIND_1, SIS.VC_INDUSTRY_WIND_FIRST,
                                                    CONS.WIND_2, SIS.VC_INDUSTRY_WIND_SECOND,
                                                    CONS.WIND_3, SIS.VC_INDUSTRY_WIND_THIRD,
                                                    SIS.VC_INDUSTRY_SW_FIRST
                                                )                   INDUSTRY_TYPE_ORIGIN
                                     FROM ZHFX.THOLDINGDETAILSHARE HDS
                                              LEFT JOIN ZHFX.TSTOCKINFOSHARE SIS
                                                        ON HDS.L_TRADE_DATE = SIS.L_TRADE_DATE AND
                                                           HDS.VC_WIND_CODE = SIS.VC_WIND_CODE,
                                          CONSTANTS CONS
                                     WHERE HDS.L_FUND_ID = ${fund_id}
                                       AND HDS.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}) HDS,
                                    CONSTANTS CONS) HDS
                         GROUP BY HDS.L_FUND_ID, HDS.L_TRADE_DATE, HDS.INDUSTRY_TYPE) IDS_ASSETS
                                       ON IDS_TFB.L_FUND_ID = IDS_ASSETS.L_FUND_ID AND
                                          IDS_TFB.L_TRADE_DATE = IDS_ASSETS.L_TRADE_DATE AND
                                          IDS_TFB.INDUSTRY_TYPE =
                                          IDS_ASSETS.INDUSTRY_TYPE) FLOW_DATE_INFO) FLOW_RANGE_INFO
         GROUP BY FLOW_RANGE_INFO.L_FUND_ID, FLOW_RANGE_INFO.INDUSTRY_TYPE)
SELECT *
FROM TRADE_DETAILS
ORDER BY IDS_CHANGE_RATIO DESC;