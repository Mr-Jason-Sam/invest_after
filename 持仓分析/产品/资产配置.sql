WITH CONSTANTS AS (SELECT '�Ͻ���' SH_EXCHANGE_NAME,
                          '���' SZ_EXCHANGE_NAME,
                          '�۽���' HK_EXCHANGE_NAME,
                          'HK'     HK_EXCHANGE_CODE,
                          '�۹�ͨ' HK_SH_SZ_MARKET_NAME,
                          '60'     SH_MAIN_CODE_PREFIX,
                          '00'     SZ_MAIN_CODE_PREFIX,
                          '30'     GEM_CODE_PREFIX,
                          '68'     STM_CODE_PREFIX
                   FROM DUAL),
     ASSETS_INFO AS (
         -- �ʲ�����
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                EN_FUND_VALUE                        NET_ASSETS,
                EN_FUND_VALUE_TOTAL,

                -- �ֽ�
                -- ���д��
                EN_BANK_DEPOSIT                      BANK_DEPOSIT_MV,
                -- ������������㱸����
                EN_BFJ - EN_BFJ_FUTURES - EN_BFJ_IRS LEAST_PROVISIONS_MV,
                -- ���������㱣֤��
                EN_BZJ - EN_BZJ_FUTURES - EN_BZJ_IRS EXCHANGE_CASH_DEPOSIT_MV,

                -- ���ʻ���
                -- ���ʻ������㱸����
                EN_BFJ_IRS                           PROVISIONS_MV,
                -- ���ʻ��������֤��
                EN_BZJ_IRS                           CASH_DEPOSIT_MV,

                -- �ع�
                -- ��ع�
                -- ��������Ѻʽ�ع�
                EN_RZHG_MARKET                       PLEDGE_RV_REPO_MV,
                -- ������Э��ʽ�ع�
                EN_RZHG_MARKET_OUT                   PROTOCOL_RV_REPO_MV,
                -- ���м䣨��Ѻʽ�����ʽ���ع�
                EN_RZHG_BANK                         BANK_RV_REPO_MV,
                -- ���ع�
                -- ��������Ѻʽ�ع�
                EN_RQHG_MARKET                       PLEDGE_REPO_MV,
                -- ������Э��ʽ�ع�
                EN_RQHG_MARKET_OUT                   PROTOCOL_REPO_MV,
                -- ���м䣨��Ѻʽ�����ʽ���ع�
                EN_RQHG_BANK                         BANK_REPO_MV,

                -- �ڻ����㱸����
                EN_BFJ_FUTURES                       FUTURES_PROVISIONS_MV,
                -- �ڻ������֤��
                EN_BZJ_FUTURES                       FUTURES_CASH_DEPOSIT_MV,
                -- ��ͷ��Լ��ֵ
                EN_FUTURES_ASSET_LONG                FUTURES_LONG_MV,
                -- ��ͷ��Լ��ֵ
                EN_FUTURES_ASSET_SHORT               FUTURES_SHORT_MV
         FROM ZHFX.TFUNDASSET
         WHERE L_FUND_ID IN (${fund_ids})
           AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     STK_ASSETS_INFO AS (
         -- ��Ʊ�ʲ�����
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                -- �������
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) THEN EN_VALUE_MARKET
                        ELSE 0 END) SH_SZ_MV,
                -- A������
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            AND REGEXP_LIKE(VC_WIND_CODE,
                                            '^(' || CONS.SZ_MAIN_CODE_PREFIX || '|' || CONS.SH_MAIN_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )               A_MAIN_MV,
                -- A�ɴ�ҵ�壨Growth Enterprise Market��
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            AND REGEXP_LIKE(VC_WIND_CODE, '^(' || CONS.GEM_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )               GEM_MV,
                -- A�ɿƴ��壨Science and Technology innovation board Market��
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            AND REGEXP_LIKE(VC_WIND_CODE, '^(' || CONS.STM_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )               STM_MV,
                -- �۹�ͨ
                SUM(CASE
                        WHEN VC_MARKET_TYPE = CONS.HK_SH_SZ_MARKET_NAME
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )               HK_SH_SZ_MV,
                -- QDII
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.HK_EXCHANGE_CODE, CONS.HK_EXCHANGE_NAME)
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )               QDII_MV
         FROM ZHFX.THOLDINGDETAILSHARE,
              CONSTANTS CONS
         WHERE L_FUND_ID IN (${fund_ids})
           AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY L_FUND_ID, L_TRADE_DATE),
     NEW_STK_ASSETS_INFO AS (
         -- �¹�
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                SUM(EN_VALUE_MARKET) NEW_STK_MV,
                -- �¹ɣ����������
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_SH_SZ_MV,
                -- �¹ɣ�A�����壩
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            AND REGEXP_LIKE(VC_WIND_CODE, '^(' || CONS.SZ_MAIN_CODE_PREFIX || '|' ||
                                                          CONS.SH_MAIN_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_A_MAIN_MV,
                -- �¹ɣ�A�ɴ�ҵ�壩
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) AND
                             REGEXP_LIKE(VC_WIND_CODE, '^(' || CONS.GEM_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_GEM_MV,
                -- �¹ɣ�A�ɿƴ��壩
                SUM(CASE
                        WHEN VC_MARKET_TYPE = CONS.HK_SH_SZ_MARKET_NAME
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_STM_MV,
                -- �¹ɣ��۹�ͨ��
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) AND
                             REGEXP_LIKE(VC_WIND_CODE, '^(' || CONS.STM_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_HK_SH_SZ_MV,
                -- QDII
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.HK_EXCHANGE_CODE, CONS.HK_EXCHANGE_NAME)
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_QDII_MV
         FROM ZHFX.THOLDINGDETAILSHAREIPO,
              CONSTANTS CONS
         WHERE L_FUND_ID IN (${fund_ids})
           AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY L_FUND_ID, L_TRADE_DATE),
     RESTRICTED_STK_ASSETS AS (
         -- ���۹�
         SELECT RS.L_FUND_ID,
                HDS.L_TRADE_DATE,
                SUM(HDS.EN_VALUE_MARKET) RESTRICTED_STK_MV
         FROM ZHFX.TRESTRICTSHARE RS
                  LEFT JOIN ZHFX.THOLDINGDETAILSHARE HDS
                            ON RS.L_FUND_ID = HDS.L_FUND_ID AND RS.VC_STOCK_CODE = HDS.VC_WIND_CODE
         WHERE RS.L_FUND_ID IN (${fund_ids})
           AND HDS.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
           AND RS.L_RELIEVE_DATE > HDS.L_TRADE_DATE
         GROUP BY RS.L_FUND_ID, HDS.L_TRADE_DATE),
     BOND_ASSETS_INFO AS (
         -- ծȯ�ʲ�
         SELECT HDB.L_FUND_ID,
                HDB.L_TRADE_DATE,
                SUM(HDB.EN_VALUE_MARKET) BOND_MV,
                -- ����ծ
                SUM(CASE
                        WHEN BD.VC_BOND_TYPE1_WIND IN ('��ծ', '����Ʊ��') OR
                             BD.VC_BOND_TYPE2_WIND = '��������ծ'
                            THEN EN_VALUE_MARKET
                        ELSE 0 END)      IRB_MV,
                -- ��תծ����ת�ɽ���ծ��
                SUM(CASE
                        WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END)      CHANGE_BOND_MV,
                -- ����ծ
                SUM(CASE
                        WHEN BD.VC_IS_YXZ = '��'
                            THEN EN_VALUE_MARKET
                        ELSE 0 END)      PERPETUAL_MV,
                -- �����ʱ�ծ
                SUM(CASE
                        WHEN BD.VC_BOND_TYPE2_WIND = '��ҵ���дμ�ծȯ'
                            THEN EN_VALUE_MARKET
                        ELSE 0 END)      SECOND_BOND_MV
         FROM ZHFX.THOLDINGDETAILBOND HDB
                  LEFT JOIN ZHFX.TBONDDESCRIPTION BD ON HDB.VC_WIND_CODE = BD.VC_WIND_CODE,
              CONSTANTS CONS
         WHERE HDB.L_FUND_ID IN (${fund_ids})
           AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY HDB.L_FUND_ID, HDB.L_TRADE_DATE),
     CITY_ASSETS AS (
         -- ��Ͷծ
         SELECT HDB.L_FUND_ID,
                HDB.L_TRADE_DATE,
                SUM(HDB.EN_VALUE_MARKET) CITY_MV
         FROM ZHFX.THOLDINGDETAILBOND HDB
                  LEFT JOIN ZHFX.TUCIBONDISSUERCJ UCI ON HDB.VC_STOCK_NAME = UCI.VC_ISSUER_NAME
         WHERE HDB.L_FUND_ID IN (${fund_ids})
           AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY HDB.L_FUND_ID, HDB.L_TRADE_DATE),
     FUND_ASSETS_INFO AS (
         -- ����
         SELECT HDF.L_FUND_ID,
                HDF.L_TRADE_DATE,
                SUM(CASE WHEN HDF.VC_MARKET_TYPE IN ('�Ͻ���', '���') THEN HDF.EN_VALUE_MARKET ELSE 0 END) IN_MK_MV,
                SUM(CASE WHEN HDF.VC_MARKET_TYPE IN ('����') THEN HDF.EN_VALUE_MARKET ELSE 0 END)             OUT_MK_MV
         FROM ZHFX.THOLDINGDETAILFUND HDF
                  LEFT JOIN ZHFX.TSTOCKINFOFUND SIF ON HDF.VC_WIND_CODE = SIF.VC_WIND_CODE AND
                                                       HDF.L_TRADE_DATE = SIF.L_TRADE_DATE
         WHERE HDF.L_FUND_ID IN (${fund_ids})
           AND HDF.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY HDF.L_FUND_ID, HDF.L_TRADE_DATE),
     FUTURES_ASSETS_INFO AS (
         -- �ڻ��ʲ�
         SELECT HDF.L_FUND_ID,
                HDF.L_TRADE_DATE,
                SUM(CASE WHEN VC_STOCK_TYPE IN ('��ָ�ڻ�') THEN EN_VALUE_MARKET ELSE 0 END) STK_IDX_MV,
                SUM(CASE WHEN VC_STOCK_TYPE IN ('��ծ�ڻ�') THEN EN_VALUE_MARKET ELSE 0 END) CTY_BOND_MV,
                SUM(CASE WHEN VC_STOCK_TYPE IN ('��Ʒ�ڻ�') THEN EN_VALUE_MARKET ELSE 0 END) COMMODITY_MV
         FROM ZHFX.THOLDINGDETAILFUTURES HDF
                  LEFT JOIN ZHFX.TSTOCKINFOFUTURES SIF
                            ON HDF.VC_WIND_CODE = SIF.VC_WIND_CODE AND
                               HDF.L_TRADE_DATE = SIF.L_TRADE_DATE
         WHERE HDF.L_FUND_ID IN (${fund_ids})
           AND HDF.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY HDF.L_FUND_ID, HDF.L_TRADE_DATE)
SELECT ASSETS_INFO.L_FUND_ID,
       ASSETS_INFO.L_TRADE_DATE,
       ASSETS_INFO.NET_ASSETS,

       -- �ֽ�
       -- ���д��
       NVL((ASSETS_INFO.BANK_DEPOSIT_MV + ASSETS_INFO.LEAST_PROVISIONS_MV + ASSETS_INFO.EXCHANGE_CASH_DEPOSIT_MV) / ASSETS_INFO.NET_ASSETS, 0)          CASH_POSI,
       NVL(ASSETS_INFO.BANK_DEPOSIT_MV / ASSETS_INFO.NET_ASSETS, 0)          BANK_DEPOSIT_POSI,
       -- ������������㱸����
       NVL(ASSETS_INFO.LEAST_PROVISIONS_MV / ASSETS_INFO.NET_ASSETS, 0)      LEAST_PROVISIONS_POSI,
       -- ���������㱣֤��
       NVL(ASSETS_INFO.EXCHANGE_CASH_DEPOSIT_MV / ASSETS_INFO.NET_ASSETS, 0) EXCHANGE_CASH_DEPOSIT_POSI,

       -- ���ʻ���
       -- ���ʻ������㱸����
       NVL(ASSETS_INFO.PROVISIONS_MV / ASSETS_INFO.NET_ASSETS, 0)            PROVISIONS_POSI,
       --���ʻ��������֤��
       NVL(ASSETS_INFO.CASH_DEPOSIT_MV / ASSETS_INFO.NET_ASSETS, 0)          CASH_DEPOSIT_POSI,
       -- �ع�
       -- ��ع�
       -- ��������Ѻʽ�ع�
       NVL(ASSETS_INFO.PLEDGE_RV_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)        PLEDGE_RV_REPO_POSI,
       -- ������Э��ʽ�ع�
       NVL(ASSETS_INFO.PROTOCOL_RV_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)      PROTOCOL_RV_REPO_POSI,
       -- ���м䣨��Ѻʽ�����ʽ���ع�
       NVL(ASSETS_INFO.BANK_RV_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)          BANK_RV_REPO_POSI,
       -- ���ع�
       -- ��������Ѻʽ�ع�
       NVL(ASSETS_INFO.PLEDGE_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)           PLEDGE_REPO_POSI,
       -- ������Э��ʽ�ع�
       NVL(ASSETS_INFO.PROTOCOL_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)         PROTOCOL_REPO_POSI,
       -- ���м䣨��Ѻʽ�����ʽ���ع�
       NVL(ASSETS_INFO.BANK_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)             BANK_REPO_POSI,

       -- �ڻ����㱸����
       NVL(ASSETS_INFO.FUTURES_PROVISIONS_MV / ASSETS_INFO.NET_ASSETS, 0)    FUTURES_PROVISIONS_POSI,
       -- �ڻ������֤��
       NVL(ASSETS_INFO.FUTURES_CASH_DEPOSIT_MV / ASSETS_INFO.NET_ASSETS, 0)  FUTURES_CASH_DEPOSIT_POSI,
       -- ��ͷ��Լ��ֵ
       NVL(ASSETS_INFO.FUTURES_LONG_MV / ASSETS_INFO.NET_ASSETS, 0)          FUTURES_LONG_POSI,
       -- ��ͷ��Լ��ֵ
       NVL(ASSETS_INFO.FUTURES_SHORT_MV / ASSETS_INFO.NET_ASSETS, 0)         FUTURES_SHORT_POSI,

       -- ��Ʊ
       -- �������
       NVL(SAI.SH_SZ_MV / ASSETS_INFO.NET_ASSETS, 0)                         SH_SZ_POSI,
       -- A������
       NVL(SAI.A_MAIN_MV / ASSETS_INFO.NET_ASSETS, 0)                        A_MAIN_POSI,
       -- A�ɴ�ҵ��
       NVL(SAI.GEM_MV / ASSETS_INFO.NET_ASSETS, 0)                           GEM_POSI,
       -- A�ɿƴ���
       NVL(SAI.STM_MV / ASSETS_INFO.NET_ASSETS, 0)                           STM_POSI,
       -- �۹�ͨ
       NVL(SAI.HK_SH_SZ_MV / ASSETS_INFO.NET_ASSETS, 0)                      HK_SH_SZ_POSI,
       -- QDII
       NVL(SAI.QDII_MV / ASSETS_INFO.NET_ASSETS, 0)                          QDII_POSI,
       -- �¹�
       NVL(NSAI.NEW_STK_MV / ASSETS_INFO.NET_ASSETS, 0)                      NEW_STK_POSI,
       -- �¹ɣ����������
       NVL(NSAI.NEW_STK_SH_SZ_MV / ASSETS_INFO.NET_ASSETS, 0)                NEW_STK_SH_SZ_POSI,
       -- �¹ɣ�A�����壩
       NVL(NSAI.NEW_STK_A_MAIN_MV / ASSETS_INFO.NET_ASSETS, 0)               NEW_STK_A_MAIN_POSI,
       -- �¹ɣ�A�ɴ�ҵ�壩
       NVL(NSAI.NEW_STK_GEM_MV / ASSETS_INFO.NET_ASSETS, 0)                  NEW_STK_GEM_POSI,
       -- �¹ɣ�A�ɿƴ��壩
       NVL(NSAI.NEW_STK_STM_MV / ASSETS_INFO.NET_ASSETS, 0)                  NEW_STK_STM_POSI,
       -- �¹ɣ��۹�ͨ��
       NVL(NSAI.NEW_STK_HK_SH_SZ_MV / ASSETS_INFO.NET_ASSETS, 0)             NEW_STK_HK_SH_SZ_POSI,
       -- �¹ɣ�QDII��
       NVL(NSAI.NEW_STK_QDII_MV / ASSETS_INFO.NET_ASSETS, 0)                 NEW_STK_QDII_POSI,
       -- ���۹�
       NVL(RSA.RESTRICTED_STK_MV / ASSETS_INFO.NET_ASSETS, 0)                RESTRICTED_STK_POSI,

       -- ծȯ
       -- ����ծ
       NVL(BAI.IRB_MV / ASSETS_INFO.NET_ASSETS, 0)                           BOND_IRB_POSI,
       -- ����ծ
       NVL((BAI.BOND_MV - BAI.IRB_MV) / ASSETS_INFO.NET_ASSETS, 0)           BOND_CREDIT_POSI,
       -- ��תծ����ת�ɽ���ծ��
       NVL(BAI.CHANGE_BOND_MV / ASSETS_INFO.NET_ASSETS, 0)                   BOND_CHANGE_BOND_POSI,
       -- ����ծ
       NVL(BAI.PERPETUAL_MV / ASSETS_INFO.NET_ASSETS, 0)                     BOND_PERPETUAL_POSI,
       -- �����ʱ�ծ
       NVL(BAI.SECOND_BOND_MV / ASSETS_INFO.NET_ASSETS, 0)                   BOND_SECOND_BOND_POSI,
       -- ��Ͷծ
       NVL(CA.CITY_MV / ASSETS_INFO.NET_ASSETS, 0)                           BOND_CITY_POSI,

       -- ����
       -- ����
       NVL(FAI.IN_MK_MV / ASSETS_INFO.NET_ASSETS, 0)                         FUND_IN_MK_POSI,
       -- ����
       NVL(FAI.OUT_MK_MV / ASSETS_INFO.NET_ASSETS, 0)                        FUND_OUT_MK_POSI,

       -- �ڻ�
       -- ��ָ�ڻ�
       NVL(FT_AI.STK_IDX_MV / ASSETS_INFO.NET_ASSETS, 0)                     FT_STK_IDX_POSI,
       -- ��ծ�ڻ�
       NVL(FT_AI.CTY_BOND_MV / ASSETS_INFO.NET_ASSETS, 0)                    FT_CTY_BOND_POSI,
       -- ��Ʒ�ڻ�
       NVL(FT_AI.COMMODITY_MV / ASSETS_INFO.NET_ASSETS, 0)                   FT_COMMODITY_POSI
FROM ASSETS_INFO
         LEFT JOIN STK_ASSETS_INFO SAI
                   ON ASSETS_INFO.L_FUND_ID = SAI.L_FUND_ID
                       AND ASSETS_INFO.L_TRADE_DATE = SAI.L_TRADE_DATE
         LEFT JOIN NEW_STK_ASSETS_INFO NSAI
                   ON ASSETS_INFO.L_FUND_ID = NSAI.L_FUND_ID
                       AND ASSETS_INFO.L_TRADE_DATE = NSAI.L_TRADE_DATE
         LEFT JOIN RESTRICTED_STK_ASSETS RSA
                   ON ASSETS_INFO.L_FUND_ID = RSA.L_FUND_ID
                       AND ASSETS_INFO.L_TRADE_DATE = RSA.L_TRADE_DATE
         LEFT JOIN BOND_ASSETS_INFO BAI
                   ON ASSETS_INFO.L_FUND_ID = BAI.L_FUND_ID
                       AND ASSETS_INFO.L_TRADE_DATE = BAI.L_TRADE_DATE
         LEFT JOIN CITY_ASSETS CA
                   ON ASSETS_INFO.L_FUND_ID = CA.L_FUND_ID
                       AND ASSETS_INFO.L_TRADE_DATE = CA.L_TRADE_DATE
         LEFT JOIN FUND_ASSETS_INFO FAI
                   ON ASSETS_INFO.L_FUND_ID = FAI.L_FUND_ID
                       AND ASSETS_INFO.L_TRADE_DATE = FAI.L_TRADE_DATE
         LEFT JOIN FUTURES_ASSETS_INFO FT_AI
                   ON ASSETS_INFO.L_FUND_ID = FT_AI.L_FUND_ID
                       AND ASSETS_INFO.L_TRADE_DATE = FT_AI.L_TRADE_DATE
ORDER BY ASSETS_INFO.L_FUND_ID, ASSETS_INFO.L_TRADE_DATE DESC
;
