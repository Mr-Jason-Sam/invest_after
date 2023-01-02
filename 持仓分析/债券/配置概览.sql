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
                       -- ��ծ�ڻ�ID
                       1232       CN_BOND_ID,
                       -- ���ع�����
                       '���ع�'   REPO_NAME,
                       -- ��ع�����
                       '��ع�'   REV_REPO_NAME,
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
     BOND_SPREAD_INFO AS (
         -- ��ծ�仯��Ϣ
         SELECT SBI.*,
                SBI.YTM - (BC.EN_YIELD + (SBI.MATU - BC.EN_CURVE_TERM) / (BC.EN_CURVE_TERM_NEXT - BC.EN_CURVE_TERM) *
                                         (BC.EN_YIELD_NEXT - BC.EN_YIELD)) / 100 SPREAD
         FROM (SELECT SBI.*,
                      CASE
                          WHEN SBI.MATU < 0.25 THEN 0
                          WHEN SBI.MATU < 0.5 THEN 0.25
                          WHEN SBI.MATU < 1 THEN 0.5
                          WHEN SBI.MATU < 2 THEN 1
                          WHEN SBI.MATU < 3 THEN 2
                          WHEN SBI.MATU < 5 THEN 3
                          WHEN SBI.MATU < 7 THEN 5
                          WHEN SBI.MATU < 10 THEN 7
                          WHEN SBI.MATU < 20 THEN 10
                          WHEN SBI.MATU < 30 THEN 20
                          WHEN SBI.MATU < 50 THEN 30
                          END MATU_GROUP,
                      CONS.CN_BOND_ID
               FROM SELECTOR_BOND_INFO SBI,
                    CONSTANTS CONS) SBI
                  LEFT JOIN
              ZHFX.TBONDCURVE BC
              ON
                          BC.L_CURVE_NUMBER = SBI.CN_BOND_ID
                      AND BC.L_TRADE_DATE = SBI.TRADE_DATE
                      AND BC.EN_CURVE_TERM = SBI.MATU_GROUP),

     REPO_INFO AS (
         -- �ع���Ϣ
         SELECT FA.L_FUND_ID,
                FA.L_TRADE_DATE,
                SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0))     REPO_AMOUNT,
                CASE
                    WHEN SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0)) = 0 THEN 0
                    ELSE SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REPO_NAME,
                                    TF_HG.EN_DEAL_BALANCE * TF_HG.EN_RATE / 100, 0)) /
                         SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0))
                    END                                                                               REPO_RATE,
                SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REV_REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0)) REVREPO_AMOUNT,
                CASE
                    WHEN SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REV_REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0)) = 0
                        THEN 0
                    ELSE SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REV_REPO_NAME,
                                    TF_HG.EN_DEAL_BALANCE * TF_HG.EN_RATE / 100, 0)) /
                         SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REV_REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0))
                    END                                                                               REVREPO_RATE
         FROM ZHFX.TFUNDASSET FA
                  LEFT JOIN
              ZHFX.TTRADEFLOWHG TF_HG
              ON
                          TF_HG.L_FUND_ID = FA.L_FUND_ID
                      AND TF_HG.L_HG_DATE <= FA.L_TRADE_DATE
                      AND DECODE(TF_HG.L_EXPIRE_DATE_ADVANCE, 0, TF_HG.L_EXPIRE_DATE, TF_HG.L_EXPIRE_DATE_ADVANCE) >
                          FA.L_TRADE_DATE,
              CONSTANTS CONS
         WHERE FA.L_FUND_ID IN (${fund_ids})
           AND FA.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY FA.L_FUND_ID, FA.L_TRADE_DATE),
     BOND_TOP_CONFIG AS (
         -- ǰN��ծȯ
         SELECT BOND_BASE_INFO.FUND_ID,
                BOND_BASE_INFO.TRADE_DATE,
                SUM(CASE WHEN BOND_BASE_INFO.PD_POSI_RANK = 1 THEN BOND_BASE_INFO.PD_POSITION ELSE 0 END)   TOP_1_POSITION,
                SUM(CASE WHEN BOND_BASE_INFO.PD_POSI_RANK <= 5 THEN BOND_BASE_INFO.PD_POSITION ELSE 0 END)  TOP_5_POSITION,
                SUM(CASE WHEN BOND_BASE_INFO.PD_POSI_RANK <= 10 THEN BOND_BASE_INFO.PD_POSITION ELSE 0 END) TOP_10_POSITION,
                SUM(CASE WHEN BOND_BASE_INFO.PD_POSI_RANK <= 20 THEN BOND_BASE_INFO.PD_POSITION ELSE 0 END) TOP_20_POSITION
         FROM (SELECT BOND_INFO.FUND_ID,
                      BOND_INFO.TRADE_DATE,
                      BOND_INFO.PD_POSITION,
                      ROW_NUMBER() OVER (PARTITION BY BOND_INFO.FUND_ID, BOND_INFO.TRADE_DATE ORDER BY BOND_INFO.BOND_PFL_POSITION DESC) PD_POSI_RANK
               FROM BOND_INFO) BOND_BASE_INFO GROUP BY BOND_BASE_INFO.FUND_ID,
                BOND_BASE_INFO.TRADE_DATE),
     IND_TOP_CONFIG AS (
         -- ǰN������
         SELECT FUND_ID,
                TRADE_DATE,
                SUM(CASE WHEN ISSUER_PD_RANK = 1 THEN ISSUER_PD_POSI ELSE 0 END)   TOP_1_POSITION,
                SUM(CASE WHEN ISSUER_PD_RANK <= 5 THEN ISSUER_PD_POSI ELSE 0 END)  TOP_5_POSITION,
                SUM(CASE WHEN ISSUER_PD_RANK <= 10 THEN ISSUER_PD_POSI ELSE 0 END) TOP_10_POSITION,
                SUM(CASE WHEN ISSUER_PD_RANK <= 20 THEN ISSUER_PD_POSI ELSE 0 END) TOP_20_POSITION
         FROM (SELECT FUND_ID,
                      TRADE_DATE,
                      ISSUER_PD_POSI,
                      ROW_NUMBER() OVER (PARTITION BY FUND_ID, TRADE_DATE ORDER BY ISSUER_PD_POSI DESC) ISSUER_PD_RANK
               FROM (SELECT FUND_ID,
                            TRADE_DATE,
                            ISSUER,
                            SUM(PD_POSITION) ISSUER_PD_POSI
                     FROM BOND_INFO
                     GROUP BY FUND_ID, TRADE_DATE, ISSUER)) GROUP BY FUND_ID, TRADE_DATE),
     PRODUCT_PFL_INFO AS (
         -- ��Ʒծȯ�����Ϣ
         SELECT BSI.FUND_ID,
                BSI.TRADE_DATE,
                -- �ܲ�λ
                BSI.MV / BSI.NAV          POSITION,
                -- ʣ������
                BSI.MATU,
                -- ��Ʒ����
                BSI.MV_MODIDURA / BSI.NAV PRODUCT_MODIDURA,
                -- ծȯ��Ͼ���
                BSI.MODIDURA,
                -- ծȯ���YTM
                BSI.YTM,
                -- ��ƷYTM
                (BSI.MV_YTM - NVL(REPO_INFO.REPO_AMOUNT * REPO_INFO.REPO_RATE, 0) +
                 NVL(REPO_INFO.REVREPO_AMOUNT * REPO_INFO.REVREPO_RATE, 0)) /
                BSI.NAV                   PRODUCT_YTM,
                -- ��������
                BSI.SPREAD,
                -- ծȯ����
                BSI.BOND_NUM,
                -- ���ع�����
                REPO_INFO.REPO_RATE,
                -- ��ع�������
                REPO_INFO.REVREPO_RATE,
                -- Ʊ������
                BSI.COUPON_RATE,

                -- ��Ͷծ
                CTZ_VOL,
                CTZ_PD_POSITION,
                CTZ_BOND_PFL_POSITION,
                -- ����ծ
                YXZ_VOL,
                YXZ_PD_POSITION,
                YXZ_BOND_PFL_POSITION,
                -- �����ʱ�ծ
                SECOND_CAPITAL_VOL,
                SECOND_CAPITAL_PD_POSITION,
                SECOND_CAPITAL_BOND_PFL_POSI,

                -- ǰN��ծȯ
                BTC.TOP_1_POSITION        BOND_TOP_1_POSI,
                BTC.TOP_5_POSITION        BOND_TOP_5_POSI,
                BTC.TOP_10_POSITION       BOND_TOP_10_POSI,
                BTC.TOP_20_POSITION       BOND_TOP_20_POSI,
                -- ǰN������
                ITC.TOP_1_POSITION        IND_TOP_1_POSI,
                ITC.TOP_5_POSITION        IND_TOP_5_POSI,
                ITC.TOP_10_POSITION       IND_TOP_10_POSI,
                ITC.TOP_20_POSITION       IND_TOP_20_POSI

         FROM (SELECT BSI.FUND_ID,
                      BSI.TRADE_DATE,
                      MAX(BSI.NAV)                                                         NAV,
                      SUM(BSI.MV)                                                          MV,
                      SUM(CASE
                              WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                              ELSE BSI.MV * BSI.MODIDURA END)                              MV_MODIDURA,
                      SUM(BSI.MV * BSI.YTM)                                                MV_YTM,
                      DECODE(SUM(BSI.MV), 0, 0, SUM(BSI.MV * BSI.MATU) / SUM(BSI.MV))      MATU,
                      DECODE(SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END *
                                 BSI.MODIDURA) /
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END))                                     MODIDURA,
                      DECODE(SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END * BSI.YTM) /
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END))                                     YTM,
                      DECODE(SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END *
                                 BSI.SPREAD) /
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END))                                     SPREAD,
                      DECODE(
                              SUM(CASE
                                      WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                      ELSE BSI.PAR_VALUE END),
                              0, 0,
                              SUM(CASE
                                      WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                      ELSE BSI.PAR_VALUE * BSI.COUPON_RATE END) / SUM(
                                      CASE
                                          WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                          ELSE BSI.PAR_VALUE END))                         COUPON_RATE,
                      COUNT(1)                                                             BOND_NUM,

                      -- ��Ͷծ ��������λ������
                      SUM(DECODE(BSI.IS_CTZ, '��', BSI.VOL, 0))                            CTZ_VOL,
                      SUM(DECODE(BSI.IS_CTZ, '��', BSI.PD_POSITION, 0))                    CTZ_PD_POSITION,
                      SUM(DECODE(BSI.IS_CTZ, '��', BSI.BOND_PFL_POSITION, 0))              CTZ_BOND_PFL_POSITION,
                      -- ����ծ ��������λ������
                      SUM(DECODE(BSI.IS_YXZ, '��', BSI.VOL, 0))                            YXZ_VOL,
                      SUM(DECODE(BSI.IS_YXZ, '��', BSI.PD_POSITION, 0))                    YXZ_PD_POSITION,
                      SUM(DECODE(BSI.IS_YXZ, '��', BSI.BOND_PFL_POSITION, 0))              YXZ_BOND_PFL_POSITION,
                      -- �����ʱ�ծ ��������λ������
                      SUM(DECODE(BSI.IS_2ND_CAPITAL_BOND, '��', BSI.VOL, 0))               SECOND_CAPITAL_VOL,
                      SUM(DECODE(BSI.IS_2ND_CAPITAL_BOND, '��', BSI.PD_POSITION, 0))       SECOND_CAPITAL_PD_POSITION,
                      SUM(DECODE(BSI.IS_2ND_CAPITAL_BOND, '��', BSI.BOND_PFL_POSITION, 0)) SECOND_CAPITAL_BOND_PFL_POSI
               FROM BOND_SPREAD_INFO BSI,
                    CONSTANTS CONS
               GROUP BY BSI.FUND_ID,
                        BSI.TRADE_DATE) BSI
                  LEFT JOIN
              REPO_INFO
              ON
                          REPO_INFO.L_FUND_ID = BSI.FUND_ID
                      AND REPO_INFO.L_TRADE_DATE = BSI.TRADE_DATE
                  LEFT JOIN BOND_TOP_CONFIG BTC ON BSI.FUND_ID = BTC.FUND_ID AND BSI.TRADE_DATE = BTC.TRADE_DATE
                  LEFT JOIN IND_TOP_CONFIG ITC ON BSI.FUND_ID = ITC.FUND_ID AND BSI.TRADE_DATE = ITC.TRADE_DATE)
SELECT *
FROM PRODUCT_PFL_INFO ORDER BY FUND_ID, TRADE_DATE DESC;