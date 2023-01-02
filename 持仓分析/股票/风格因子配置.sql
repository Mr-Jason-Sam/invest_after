WITH CONSTANTS AS (
    -- ����
    SELECT 'SW_1'       SW_1,
           'SW_2'       SW_2,
           'SW_3'       SW_3,
           'SEC_1'      SEC_1,
           'SEC_2'      SEC_2,
           'WIND_1'     WIND_1,
           'WIND_2'     WIND_2,
           'WIND_3'     WIND_3,
           'ZX_1'       ZX_1,
           'ZX_2'       ZX_2,
           'ZX_3'       ZX_3,
           '000300.SH' HS_300,

           '�Ͻ���'     SH_EXCHANGE_NAME,
           '���'     SZ_EXCHANGE_NAME,
           '�۽���'     HK_EXCHANGE_NAME,
           'HK'         HK_EXCHANGE_CODE,
           '�۹�ͨ'     HK_SH_SZ_MARKET_NAME,
           '����'       OTHER,
           '60'         SH_MAIN_CODE_PREFIX,
           '00'         SZ_MAIN_CODE_PREFIX,
           '30'         GEM_CODE_PREFIX,
           '68'         STM_CODE_PREFIX,
           'A��-����'   A_MAIN_BLOCK,
           'A��-��ҵ��' A_GEM_BLOCK,
           'A��-�ƴ���' A_STM_BLOCK,
           'HK-�۽���'  HK_BLOCK,
           '360'        PREFERRED_STK_PREFIX,

           20           DEFAULT_STK_TOP_N,
           10           DEFAULT_INDUSTRY_TOP_N,

           -- ����������Ϣ����1�����group by fund_id trade_date��
           'ALL'        "ALL",
           -- ���и��ɻ�����Ϣ
           1            ALL_STK,
           -- ���ɼ��ж�����
           2            STK_CFG,
           -- ��ƱǰN����Ϣ
           3            STK_TOP_INFO,
           -- ��ҵ���ж�����
           4            IDS_CFG,
           -- ��ҵǰN����Ϣ
           5            IDS_TOP_INFO,
           -- �г��������
           6            INVEST_MARKET_CFG,
           -- Ͷ�ʷ������
           7            INVEST_STYLE_CFG,
           -- ���򸴺ϰ������
           8            SW_CP_CFG,
           -- �����������
           9            STYLE_FACTOR_CFG,
           -- PE����
           10           PE_CFG,
           -- PB����
           11           PB_CFG,
           -- PEG����
           12           PEG_CFG
    FROM DUAL),
     STK_INFO AS (
         -- ��Ʊ��Ϣ��
         SELECT STK_BASE_INFO.*,
                -- ������ֵ����
                -- �ܲ�λ = ȫ����ֵ / �ʲ���ֵ
                DECODE(STK_BASE_INFO.NET_ASSETS, 0, 0,
                       STK_BASE_INFO.STK_MKT / STK_BASE_INFO.NET_ASSETS)                                                                  NET_ASSETS_POSITION,
                -- ��Ʊ��ϲ�λ =  ȫ����ֵ / ��Ʊ������ʲ�
                DECODE(SUM(STK_BASE_INFO.STK_MKT)
                           OVER ( PARTITION BY STK_BASE_INFO.L_FUND_ID, STK_BASE_INFO.L_TRADE_DATE), 0, 0,
                       STK_BASE_INFO.STK_MKT / SUM(STK_BASE_INFO.STK_MKT)
                                                   OVER ( PARTITION BY STK_BASE_INFO.L_FUND_ID, STK_BASE_INFO.L_TRADE_DATE))              STK_PFL_POSITION,
                ROW_NUMBER() OVER ( PARTITION BY STK_BASE_INFO.L_FUND_ID, STK_BASE_INFO.L_TRADE_DATE ORDER BY STK_BASE_INFO.STK_MKT DESC) MARKET_RANK
         FROM (SELECT
                   -- ������Ϣ
                   HDS.L_FUND_ID,
                   HDS.VC_WIND_CODE,
                   HDS.L_TRADE_DATE,
                   SIS.VC_STOCK_NAME,

                   -- ������Ϣ
                   SIS.EN_PRICE_CLOSE,

                   -- �ֲ���Ϣ
                   HDS.L_AMOUNT,
                   HDS.EN_VALUE_MARKET - NVL(HDS_IPO.EN_VALUE_MARKET, 0) STK_MKT,
                   -- ���ʲ�
                   FS.EN_FUND_VALUE                                      NET_ASSETS,

                   -- �����г�
                   SIS.VC_MARKET_TYPE,
                   -- �г�����
                   CASE
                       WHEN SIS.VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) AND
                            REGEXP_LIKE(HDS.VC_WIND_CODE,
                                        '^(' || CONS.SZ_MAIN_CODE_PREFIX || '|' || CONS.SH_MAIN_CODE_PREFIX ||
                                        ')') THEN CONS.A_MAIN_BLOCK
                       WHEN SIS.VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) AND
                            REGEXP_LIKE(HDS.VC_WIND_CODE, '^(' || CONS.GEM_CODE_PREFIX || ')') THEN CONS.A_GEM_BLOCK
                       WHEN SIS.VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) AND
                            REGEXP_LIKE(HDS.VC_WIND_CODE, '^(' || CONS.STM_CODE_PREFIX || ')') THEN CONS.A_STM_BLOCK
                       WHEN SIS.VC_MARKET_TYPE IN (CONS.HK_EXCHANGE_CODE, CONS.HK_EXCHANGE_NAME) THEN CONS.HK_BLOCK
                       ELSE CONS.OTHER
                       END                                               INVEST_MARKET,

                   -- ��������Ϣ
                   SIS.EN_VAL_PE,
                   SIS.EN_VAL_PB,
                   SIS.EN_VAL_PEG_EST,
                   SIS.EN_VAL_ROE,

                   -- ��ҵ
                   SIS.VC_INDUSTRY_SW_FIRST,
                   SIS.VC_INDUSTRY_SW_SECOND,
                   SIS.VC_INDUSTRY_SW_THIRD,
                   SIS.VC_INDUSTRY_SEC_FIRST,
                   SIS.VC_INDUSTRY_SEC_SECOND,
                   SIS.VC_INDUSTRY_ZX_FIRST,
                   SIS.VC_INDUSTRY_ZX_SECOND,
                   SIS.VC_INDUSTRY_ZX_THIRD,
                   SIS.VC_INDUSTRY_WIND_FIRST,
                   SIS.VC_INDUSTRY_WIND_SECOND,
                   SIS.VC_INDUSTRY_WIND_THIRD,
                   -- ������SW��ҵ
                   CASE
                       WHEN VC_INDUSTRY_SW_FIRST IN
                            ('��֯����', '���õ���', '���ݻ���', 'ũ������', '�Ṥ����', '��ó����', '������',
                             'ʳƷ����')
                           THEN '����-����'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('ҽҩ����') THEN '����-ҽҩ'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('�����豸', '������ҵ', '��е�豸', '����װ��', '����', '�ۺ�')
                           THEN '����-��������'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('��ý', '����', '�����', 'ͨ��') THEN '����-TMT'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('����', '��������', '��������', '��ͨ����') THEN '����-����'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('��������') THEN '����-����'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('����', 'ú̿', 'ʯ��ʯ��', '��ɫ����') THEN '����-��Դ'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('���ز�', '��������', '����') THEN '����-���ڵز�'
                       ELSE '����'
                       END                                               SW_COMPLEX,
                   -- ѡ����ҵ��Ĭ������һ��
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
                       )                                                 SELECT_INDUSTRY,

                   -- Ͷ�ʷ��
                   SIS.VC_INVEST_TYPE
               FROM ZHFX.THOLDINGDETAILSHARE HDS
                        LEFT JOIN ZHFX.THOLDINGDETAILSHAREIPO HDS_IPO
                                  ON HDS.L_FUND_ID = HDS_IPO.L_FUND_ID AND HDS.VC_WIND_CODE = HDS_IPO.VC_WIND_CODE AND
                                     HDS.L_TRADE_DATE = HDS_IPO.L_TRADE_DATE
                        LEFT JOIN ZHFX.TFUNDASSET FS
                                  ON HDS.L_FUND_ID = FS.L_FUND_ID AND HDS.L_TRADE_DATE = FS.L_TRADE_DATE
                        LEFT JOIN ZHFX.TSTOCKINFOSHARE SIS
                                  ON HDS.VC_WIND_CODE = SIS.VC_WIND_CODE AND HDS.L_TRADE_DATE = SIS.L_TRADE_DATE,
                    CONSTANTS CONS
               WHERE HDS.L_FUND_ID = ${fund_id}
                 AND HDS.L_TRADE_DATE BETWEEN ${begin_date}
                   AND ${end_date}
                 -- TODO ��ȷ��
                 AND HDS.EN_VALUE_MARKET - NVL(HDS_IPO.EN_VALUE_MARKET
                   , 0)
                   > 0
                 --�޳����ȹ�
                 AND SUBSTR(HDS.VC_WIND_CODE
                         , 1
                         , 3) != CONS.PREFERRED_STK_PREFIX) STK_BASE_INFO),
     STYLE_FACTOR_CONFIG AS (
         -- Barra�ķ���������ã�TODO ��ʱ֧�ֵ�����Ʊ��϶�Ӧ��ָ��)
         SELECT PD_BM_POS.TRADE_DATE,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_SIZE, 0))                           PD_SIZE,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_SIZENL, 0))                         PD_SIZENL,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_LIQUIDTY, 0))                       PD_LIQUIDTY,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_EARNYILD, 0))                       PD_EARNYILD,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_BTOP, 0))                           PD_BTOP,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_BETA, 0))                           PD_BETA,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_GROWTH, 0))                         PD_GROWTH,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_LEVERAGE, 0))                       PD_LEVERAGE,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_MOMENTUM, 0))                       PD_MOMENTUM,
                SUM(PD_BM_POS.STK_PFL_POSI * NVL(FACTOR.EN_FACTOR_EXPO_RESVOL, 0))                         PD_RESVOL,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_SIZE, 0))                                BM_SIZE,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_SIZENL, 0))                              BM_SIZENL,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_LIQUIDTY, 0))                            BM_LIQUIDTY,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_EARNYILD, 0))                            BM_EARNYILD,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_BTOP, 0))                                BM_BTOP,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_BETA, 0))                                BM_BETA,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_GROWTH, 0))                              BM_GROWTH,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_LEVERAGE, 0))                            BM_LEVERAGE,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_MOMENTUM, 0))                            BM_MOMENTUM,
                SUM(PD_BM_POS.BM_POSI * NVL(FACTOR.EN_FACTOR_EXPO_RESVOL, 0))                              BM_RESVOL,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_SIZE, 0))     ALPHA_SIZE,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_SIZENL, 0))   ALPHA_SIZENL,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_LIQUIDTY, 0)) ALPHA_LIQUIDTY,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_EARNYILD, 0)) ALPHA_EARNYILD,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_BTOP, 0))     ALPHA_BTOP,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_BETA, 0))     ALPHA_BETA,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_GROWTH, 0))   ALPHA_GROWTH,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_LEVERAGE, 0)) ALPHA_LEVERAGE,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_MOMENTUM, 0)) ALPHA_MOMENTUM,
                SUM((PD_BM_POS.STK_PFL_POSI - PD_BM_POS.BM_POSI) * NVL(FACTOR.EN_FACTOR_EXPO_RESVOL, 0))   ALPHA_RESVOL
         FROM (
                  -- ��Ʒ��Ʊ��Ϻͻ�׼�Ĺ�Ʊ���Ȩ����Ϣ
                  SELECT STK_QUOTE.L_FUND_ID,
                         INDEX_QUOTE.VC_INDEX_CODE,
                         NVL(STK_QUOTE.VC_WIND_CODE, INDEX_QUOTE.VC_STOCK_CODE) STK_CODE,
                         NVL(STK_QUOTE.L_TRADE_DATE, INDEX_QUOTE.L_TRADE_DATE)  TRADE_DATE,
                         NVL(STK_QUOTE.STK_PFL_POSITION, 0)                     STK_PFL_POSI,
                         NVL(INDEX_QUOTE.IDX_PFL_POSITION, 0)                   BM_POSI
                  FROM (
                           -- ��Ʒ��Ʊ���
                           SELECT L_FUND_ID,
                                  VC_WIND_CODE,
                                  L_TRADE_DATE,
                                  STK_PFL_POSITION
                           FROM STK_INFO) STK_QUOTE
                           FULL JOIN (
                      -- ��׼��Ʊ���
                      SELECT IMS.VC_INDEX_CODE,
                             IMS.L_TRADE_DATE,
                             IMS.VC_STOCK_CODE,
                             DECODE(SUM(IMS.EN_VALUE_CLOSE)
                                        OVER ( PARTITION BY IMS.VC_INDEX_CODE, IMS.L_TRADE_DATE), 0, 0,
                                    IMS.EN_VALUE_CLOSE / SUM(IMS.EN_VALUE_CLOSE)
                                                             OVER ( PARTITION BY IMS.VC_INDEX_CODE, IMS.L_TRADE_DATE)) IDX_PFL_POSITION
                      FROM ZHFX.TINDEXMEMBERSHARE IMS, CONSTANTS CONS
                      WHERE IMS.VC_INDEX_CODE = NVL(${index_code}, CONS.HS_300)
                        AND IMS.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}) INDEX_QUOTE
                                     ON STK_QUOTE.VC_WIND_CODE = INDEX_QUOTE.VC_STOCK_CODE AND
                                        STK_QUOTE.L_TRADE_DATE = INDEX_QUOTE.L_TRADE_DATE) PD_BM_POS
                  LEFT JOIN (
             -- �������
             SELECT BFE.L_TRADE_DATE,
                    BFE.VC_STOCK_CODE,
                    BFE.VC_STOCK_NAME,
                    -- ��Ʊ������
                    BFE.EN_STOCK_RETURN / 100.0     EN_STOCK_RETURN,
                    -- ����������
                    BFE.EN_STOCK_SPE_RETURN / 100.0 EN_STOCK_SPE_RETURN,
                    -- ��Ʊ����
                    BFE.EN_STOCK_RISK,
                    -- �������
                    BFE.EN_STOCK_SPE_RISK,
                    -- ������ӱ�¶
                    -- ��ģ
                    BFE.EN_FACTOR_EXPO_SIZE,
                    -- ����
                    BFE.EN_FACTOR_EXPO_SIZENL,
                    -- ������
                    BFE.EN_FACTOR_EXPO_LIQUIDTY,
                    -- ӯ��
                    BFE.EN_FACTOR_EXPO_EARNYILD,
                    -- ��ֵ
                    BFE.EN_FACTOR_EXPO_BTOP,
                    -- BETA
                    BFE.EN_FACTOR_EXPO_BETA,
                    -- �ɳ�
                    BFE.EN_FACTOR_EXPO_GROWTH,
                    -- �ܸ�
                    BFE.EN_FACTOR_EXPO_LEVERAGE,
                    -- ����
                    BFE.EN_FACTOR_EXPO_MOMENTUM,
                    -- ���ನ����
                    BFE.EN_FACTOR_EXPO_RESVOL,
                    -- ��������
                    BFE.EN_FACTOR_EXPO_COUNTRY
             FROM ZHFX.TBARRAFACTOREXPOSURE BFE
             WHERE BFE.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}) FACTOR
                            ON PD_BM_POS.STK_CODE = FACTOR.VC_STOCK_CODE AND PD_BM_POS.TRADE_DATE = FACTOR.L_TRADE_DATE
         GROUP BY TRADE_DATE)
SELECT *
FROM STYLE_FACTOR_CONFIG
ORDER BY TRADE_DATE;