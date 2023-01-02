WITH CONSTANTS AS (SELECT
                       -- ��ծ��֤������Ȩ
                       1          CN_BOND_SEC_PUT_BACK_EXE,
                       -- ��ծ������Ȩ
                       2          CN_BOND_PUT_BACK_EXE,
                       -- ��ծ�Ƽ�
                       3          CN_BOND_RCM,
                       -- ��ծ��֤�Ƽ�
                       4          CN_BOND_SEC_RCM,
                       0.0001     ONE_BP,
                       'WIND_1'   WIND_1,
                       'WIND_2'   WIND_2,
                       -- ��תծ
                       '��תծ'   TSF_BOND_NAME,
                       -- �ɽ���ծ
                       '�ɽ���ծ' CHANGE_BOND_NAME
                   FROM DUAL),
     BOND_INFO AS (
         -- ծȯ������Ϣ
         SELECT BOND_BASE_INFO.*,
                BOND_BASE_INFO.MV / NAV                                                                                 PD_POSITION,
                DECODE(
                        SUM(BOND_BASE_INFO.MV) OVER ( PARTITION BY BOND_BASE_INFO.FUND_ID, BOND_BASE_INFO.TRADE_DATE),
                        0, 0,
                        BOND_BASE_INFO.MV / SUM(BOND_BASE_INFO.MV)
                                                OVER ( PARTITION BY BOND_BASE_INFO.FUND_ID, BOND_BASE_INFO.TRADE_DATE)) BOND_PFL_POSITION
         FROM (
                  -- ծȯ������Ϣ
                  SELECT HDB.L_FUND_ID                                                 FUND_ID,
                         HDB.L_TRADE_DATE                                              TRADE_DATE,
                         FA.EN_FUND_VALUE                                              NAV,
                         HDB.VC_WIND_CODE                                              WINDCODE,
                         HDB.VC_STOCK_NAME                                             SEC_NAME,
                         HDB.L_AMOUNT                                                  VOL,
                         HDB.EN_VALUE_MARKET + HDB.EN_INTEREST_ACCUMULATED             MV,
                         BD.VC_BOND_TYPE1_WIND                                         BONDTYPE1_WIND,
                         BD.VC_BOND_TYPE2_WIND                                         BONDTYPE2_WIND,
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��ծ', '����Ʊ��') OR BD.VC_BOND_TYPE2_WIND = '��������ծ'
                                 THEN '����ծ'
                             WHEN BD.VC_BOND_TYPE1_WIND = '�ط�����ծ' THEN '�ط�����ծ'
                             WHEN BD.VC_BOND_TYPE1_WIND = 'ͬҵ�浥' THEN 'ͬҵ�浥'
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ') THEN '��תծ�ɽ�ծ'
                             WHEN BD.VC_BOND_TYPE2_WIND IN ('��ҵ����ծ', '��ҵ���дμ�ծȯ') AND BD.VC_IS_YXZ = '��'
                                 THEN '��ҵ��������ծ'
                             WHEN BD.VC_BOND_TYPE2_WIND = '��ҵ���дμ�ծȯ' THEN '��ҵ���дμ�ծ'
                             WHEN BD.VC_BOND_TYPE2_WIND = '֤ȯ��˾ծ' AND BD.VC_IS_SUBORD = '�μ�ծ' THEN '֤ȯ��˾�μ�ծ'
                             WHEN BD.VC_BOND_TYPE1_WIND = '����ծ' THEN '��������ծ'
                             WHEN UCI.VC_ISSUER_NAME IS NOT NULL AND BD.VC_IS_YXZ = '��' THEN '��Ͷ����'
                             WHEN UCI.VC_ISSUER_NAME IS NOT NULL AND BD.VC_ISSUE_TYPE IN ('˽ļ', '����') THEN '��Ͷ�ǹ���'
                             WHEN UCI.VC_ISSUER_NAME IS NOT NULL THEN '������Ͷ'
                             WHEN BD.VC_IS_YXZ = '��' THEN '��ҵ����'
                             WHEN BD.VC_ISSUE_TYPE IN ('˽ļ', '����') THEN '��ҵ�ǹ���'
                             ELSE '������ҵ'
                             END                                                       BONDTYPE1,
                         BD.VC_IS_YXZ                                                  IS_YXZ,
                         DECODE(UCI.VC_ISSUER_NAME, NULL, '��', '��')                  IS_CTZ,
                         DECODE(BD.VC_BOND_TYPE2_WIND, '��ҵ���дμ�ծȯ', '��', '��') IS_2ND_CAPITAL_BOND,
                         BD.VC_COMP_NAME                                               ISSUER,
                         NVL(BD.VC_PROVINCE, '����')                                   PROVINCE,
                         NVL(BD.VC_COMP_IND2_WIND, '����')                             IND2_WIND,
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��ծ', '����Ʊ��') OR BD.VC_BOND_TYPE2_WIND = '��������ծ'
                                 THEN '����ծ'
                             ELSE COALESCE(BI.VC_ISSUER_RATING, BI.VC_BOND_RATING, '������')
                             END                                                       ISSUER_RATING, --��������������������ȡծ������
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��ծ', '����Ʊ��') OR BD.VC_BOND_TYPE2_WIND = '��������ծ'
                                 THEN '����ծ'
                             ELSE COALESCE(BI.VC_BOND_RATING, BI.VC_ISSUER_RATING, '������')
                             END                                                       BOND_RATING,   --ծ����������ծ������ȡ��������
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��ծ', '����Ʊ��') OR BD.VC_BOND_TYPE2_WIND = '��������ծ'
                                 THEN '����ծ'
                             ELSE NVL(BI.VC_BOND_RATING_CNBD, '������')
                             END                                                       RATING_CNBD,   --��ծ��������
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��ծ', '����Ʊ��') OR BD.VC_BOND_TYPE2_WIND = '��������ծ'
                                 THEN '����ծ'
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('ͬҵ�浥', '��������ȯ')
                                 THEN COALESCE(BI.VC_BOND_RATING_INTER, BI.VC_ISSUER_RATING_INTER, '������')
                             ELSE COALESCE(BI.VC_BOND_RATING_INTER, '������')
                             END                                                       RATING_CJ,     --�����ڲ�����
                         BI.EN_COUPONRATE                                              COUPON_RATE,
                         BI.EN_PAR * HDB.L_AMOUNT                                      PAR_VALUE,
                         CASE
                             WHEN INSTR(BD.VC_SP_PROVISION, '����') > 0 THEN COALESCE(BI.EN_MATU_CNBD_IFEXE,
                                                                                      BI.EN_MATU_CNBD,
                                                                                      BI.EN_MATU_WIND_IFEXE,
                                                                                      BI.EN_MATU_WIND, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MATU_CNBD_IFEXE, BI.EN_MATU_CNBD),
                                           BI.EN_MATU_WIND, 0)
                             END                                                       MATU1,         --��ծ������Ȩ
                         COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MATU_CNBD_IFEXE, BI.EN_MATU_CNBD), BI.EN_MATU_WIND,
                                  0)                                                   MATU2,         --��ծ�Ƽ�

                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ') THEN 0
                             WHEN REGEXP_LIKE(BD.VC_SP_PROVISION, '����') THEN COALESCE(BI.EN_MODIDURA_CNBD_IFEXE,
                                                                                        BI.EN_MODIDURA_CNBD,
                                                                                        BI.EN_MODIDURA_CSI_IFEXE,
                                                                                        BI.EN_MODIDURA_CSI,
                                                                                        BI.EN_MATU_WIND_IFEXE * 0.95,
                                                                                        BI.EN_MATU_WIND * 0.95, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                           BI.EN_MATU_WIND * 0.95, 0)
                             END                                                       MODIDURA1,     --��ծ������Ȩ
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ') THEN 0
                             WHEN BD.VC_MARKET != '���м�' AND REGEXP_LIKE(BD.VC_SP_PROVISION, '����') THEN COALESCE(
                                     BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI, BI.EN_MODIDURA_CNBD_IFEXE,
                                     BI.EN_MODIDURA_CNBD, BI.EN_MATU_WIND_IFEXE * 0.95, BI.EN_MATU_WIND * 0.95, 0)
                             WHEN BD.VC_MARKET != '���м�' THEN COALESCE(
                                     DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                     DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                     BI.EN_MATU_WIND * 0.95, 0)
                             WHEN REGEXP_LIKE(BD.VC_SP_PROVISION, '����') THEN COALESCE(BI.EN_MODIDURA_CNBD_IFEXE,
                                                                                        BI.EN_MODIDURA_CNBD,
                                                                                        BI.EN_MODIDURA_CSI_IFEXE,
                                                                                        BI.EN_MODIDURA_CSI,
                                                                                        BI.EN_MATU_WIND_IFEXE * 0.95,
                                                                                        BI.EN_MATU_WIND * 0.95, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                           BI.EN_MATU_WIND * 0.95, 0)
                             END                                                       MODIDURA2,     --��ծ��֤������Ȩ
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ') THEN 0
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                           BI.EN_MATU_WIND * 0.95, 0)
                             END                                                       MODIDURA3,     --��ծ�Ƽ�
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ') THEN 0
                             WHEN BD.VC_MARKET != '���м�' THEN COALESCE(
                                     DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                     DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                     BI.EN_MATU_WIND * 0.95, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                           BI.EN_MATU_WIND * 0.95, 0)
                             END                                                       MODIDURA4,     --��ծ��֤�Ƽ�
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ') THEN 0
                             WHEN REGEXP_LIKE(BD.VC_SP_PROVISION, '����') THEN COALESCE(BI.EN_YTM_CNBD_IFEXE,
                                                                                        BI.EN_YTM_CNBD,
                                                                                        BI.EN_YTM_CSI_IFEXE,
                                                                                        BI.EN_YTM_CSI,
                                                                                        BI.EN_COUPONRATE, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                           BI.EN_COUPONRATE, 0)
                             END                                                       YTM1,          --��ծ������Ȩ
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ') THEN 0
                             WHEN BD.VC_MARKET != '���м�' AND REGEXP_LIKE(BD.VC_SP_PROVISION, '����') THEN COALESCE(
                                     BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD,
                                     BI.EN_COUPONRATE, 0)
                             WHEN BD.VC_MARKET != '���м�' THEN COALESCE(
                                     DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                     DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                     BI.EN_COUPONRATE, 0)
                             WHEN REGEXP_LIKE(BD.VC_SP_PROVISION, '����') THEN COALESCE(BI.EN_YTM_CNBD_IFEXE,
                                                                                        BI.EN_YTM_CNBD,
                                                                                        BI.EN_YTM_CSI_IFEXE,
                                                                                        BI.EN_YTM_CSI,
                                                                                        BI.EN_COUPONRATE, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                           BI.EN_COUPONRATE, 0)
                             END                                                       YTM2,          --��ծ��֤������Ȩ
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ') THEN 0
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                           BI.EN_COUPONRATE, 0)
                             END                                                       YTM3,          --��ծ�Ƽ�
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('��תծ', '�ɽ���ծ') THEN 0
                             WHEN BD.VC_MARKET != '���м�' THEN COALESCE(
                                     DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                     DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                     BI.EN_COUPONRATE, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                           BI.EN_COUPONRATE, 0)
                             END                                                       YTM4           --��ծ��֤�Ƽ�
                  FROM ZHFX.THOLDINGDETAILBOND HDB
                           LEFT JOIN
                       ZHFX.TFUNDASSET FA
                       ON
                                   FA.L_FUND_ID = HDB.L_FUND_ID
                               AND FA.L_TRADE_DATE = HDB.L_TRADE_DATE
                           LEFT JOIN
                       ZHFX.TBONDDESCRIPTION BD
                       ON
                           BD.VC_WIND_CODE = HDB.VC_WIND_CODE
                           LEFT JOIN
                       ZHFX.TBONDINFO BI
                       ON
                                   BI.VC_WIND_CODE = HDB.VC_WIND_CODE
                               AND BI.L_TRADE_DATE = HDB.L_TRADE_DATE
                           LEFT JOIN
                       ZHFX.TUCIBONDISSUERCJ UCI
                       ON
                           UCI.VC_ISSUER_NAME = BD.VC_COMP_NAME
                  WHERE HDB.L_FUND_ID IN (${fund_ids})
                    AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
                    AND HDB.L_AMOUNT != 0) BOND_BASE_INFO),
     SELECTOR_BOND_INFO AS (
         -- ѡ���������ڡ�ʣ����ڡ����������ʵ�����
         SELECT BASE_INFO.*,
                -- DVBP ��Duration Value BP�� = �������� * ȫ����ֵ * 1BP = �������� * ȫ����ֵ * 0.0001
                BASE_INFO.MODIDURA * MV * CONS.ONE_BP DVBP
         FROM (SELECT BOND_INFO.*,
                      DECODE(
                              ${bond_type},
                              CONS.WIND_1, BOND_INFO.BONDTYPE1_WIND,
                              CONS.WIND_2, BOND_INFO.BONDTYPE2_WIND,
                              BOND_INFO.BONDTYPE1
                          ) BOND_TYPE,
                      DECODE(
                              ${modidura_type},
                          -- ��ծ��֤������Ȩ
                              CONS.CN_BOND_SEC_PUT_BACK_EXE, BOND_INFO.MATU1,
                          -- ��ծ�Ƽ�
                              CONS.CN_BOND_RCM, BOND_INFO.MATU2,
                          -- ��ծ��֤�Ƽ�
                              CONS.CN_BOND_SEC_RCM, BOND_INFO.MATU2,
                              BOND_INFO.MATU1
                          ) MATU,
                      DECODE(
                              ${modidura_type},
                          -- ��ծ��֤������Ȩ
                              CONS.CN_BOND_SEC_PUT_BACK_EXE, BOND_INFO.MODIDURA2,
                          -- ��ծ�Ƽ�
                              CONS.CN_BOND_RCM, BOND_INFO.MODIDURA3,
                          -- ��ծ��֤�Ƽ�
                              CONS.CN_BOND_SEC_RCM, BOND_INFO.MODIDURA4,
                              BOND_INFO.MODIDURA1
                          ) MODIDURA,
                      DECODE(
                              ${modidura_type},
                          -- ��ծ��֤������Ȩ
                              CONS.CN_BOND_SEC_PUT_BACK_EXE, BOND_INFO.YTM2,
                          -- ��ծ�Ƽ�
                              CONS.CN_BOND_RCM, BOND_INFO.YTM3,
                          -- ��ծ��֤�Ƽ�
                              CONS.CN_BOND_SEC_RCM, BOND_INFO.YTM4,
                              BOND_INFO.YTM1
                          ) YTM
               FROM BOND_INFO,
                    CONSTANTS CONS) BASE_INFO,
              CONSTANTS CONS),
     BOND_TYPE_CONFIG AS (
         -- ��������������
         SELECT GROUP_CONF.*,
                GROUP_CONF.BTG_MV * GROUP_CONF.BTG_MODIDURA * CONS.ONE_BP YG_DVBP
         FROM (SELECT SBI.FUND_ID,
                      SBI.TRADE_DATE,
                      SBI.BOND_TYPE,
                      SUM(SBI.MV)                      BTG_MV,
                      SUM(SBI.PD_POSITION)             BTG_PD_POSI,
                      SUM(SBI.BOND_PFL_POSITION)       BTG_BOND_PFL_POSI,
                      DECODE(SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END *
                                 SBI.MODIDURA) /
                             SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END)) BTG_MODIDURA,
                      DECODE(SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END * SBI.YTM) /
                             SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END)) BTG_YTM

               FROM SELECTOR_BOND_INFO SBI,
                    CONSTANTS CONS
               GROUP BY SBI.FUND_ID, SBI.TRADE_DATE, SBI.BOND_TYPE) GROUP_CONF,
              CONSTANTS CONS)
SELECT *
FROM BOND_TYPE_CONFIG;