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
     HOLDINGS_BASE_INFO AS (
         -- �����ֲ�����
         SELECT HDF.*
         FROM ZHFX.THOLDINGDETAILFUND HDF,
              TD_ADJ
         WHERE HDF.L_FUND_ID = ${ztbh}
           AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE),
     FOF_PFL_PROFIT AS (
         -- FOF�������
         SELECT HOLD_INFO.*,
                DECODE(HOLD_INFO.FOF_MV_INIT, 0, 0,
                       HOLD_INFO.DAY_PF / HOLD_INFO.FOF_MV_INIT) DAY_PF_RATIO
         FROM (SELECT HDF.L_FUND_ID            FUND_ID,
                      HDF.L_TRADE_DATE         TRADE_DATE,
                      SUM(HDF.EN_VALUE_MARKET) FOF_MV,
                      SUM(HDF.EN_MARKET_INIT)  FOF_MV_INIT,
                      SUM(HDF.EN_PROFIT)       DAY_PF
               FROM ZHFX.THOLDINGDETAILFUND HDF,
                    TD_ADJ ADJ
               WHERE HDF.L_FUND_ID = ${ztbh}
                 AND HDF.L_TRADE_DATE BETWEEN ADJ.BEGIN_DATE AND ADJ.END_DATE
               GROUP BY HDF.L_FUND_ID, HDF.L_TRADE_DATE) HOLD_INFO
         ORDER BY HOLD_INFO.TRADE_DATE),
     ALPHA_INFO AS (
         -- ��������Ϣ
         SELECT FPP.FUND_ID                                                                            L_FUND_ID,
                FPP.TRADE_DATE                                                                         L_TRADE_DATE,
                EXP(SUM(LN(1 + FPP.DAY_PF_RATIO)) OVER (ORDER BY TRADE_DATE)) / (1 + FPP.DAY_PF_RATIO) ALPHA
         FROM FOF_PFL_PROFIT FPP),
     PROFIT_DAY_CTB_ACC AS (
         -- ���湱��
         -- ���� = sum(�������� / T-1����ֵ * ALPHA)
         SELECT HDF.L_FUND_ID,
                HDF.VC_STOCK_CODE,
                HDF.L_TRADE_DATE,
                DECODE(FPP.FOF_MV_INIT, 0, 0, HDF.EN_PROFIT / FPP.FOF_MV_INIT) * AI.ALPHA CTB
         FROM HOLDINGS_BASE_INFO HDF
                  LEFT JOIN FOF_PFL_PROFIT FPP
                            ON HDF.L_FUND_ID = FPP.FUND_ID
                                AND HDF.L_TRADE_DATE = FPP.TRADE_DATE
                  LEFT JOIN ALPHA_INFO AI
                            ON HDF.L_FUND_ID = AI.L_FUND_ID
                                AND HDF.L_TRADE_DATE = AI.L_TRADE_DATE),
     FUND_PF_CLASSIFY_BASE AS (
         -- ����ÿ�շ���
         SELECT PC.L_FUND_ID,
                PC.L_TRADE_DATE,
                '����'                                                                  CLASSIFY,
                MFD.VC_FUND_TYPE_WIND_SECOND                                            TYPE,
                SUM(CASE WHEN MFD.VC_MARKET_TYPE IN ('����') THEN PC.CTB ELSE 0 END) AS CTB
         FROM PROFIT_DAY_CTB_ACC PC
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON PC.VC_STOCK_CODE = MFD.VC_STOCK_CODE
         GROUP BY PC.L_FUND_ID,
                  PC.L_TRADE_DATE,
                  '����',
                  MFD.VC_FUND_TYPE_WIND_SECOND
         UNION ALL
         SELECT PC.L_FUND_ID,
                PC.L_TRADE_DATE,
                '����'                                                                      CLASSIFY,
                MFD.VC_FUND_TYPE_WIND                                                       TYPE,
                SUM(CASE WHEN MFD.VC_MARKET_TYPE NOT IN ('����') THEN PC.CTB ELSE 0 END) AS CTB
         FROM PROFIT_DAY_CTB_ACC PC
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON PC.VC_STOCK_CODE = MFD.VC_STOCK_CODE
         GROUP BY PC.L_FUND_ID,
                  PC.L_TRADE_DATE,
                  '����',
                  MFD.VC_FUND_TYPE_WIND),
     FUND_PF_CLASSIFY AS (
         -- ���׷���
         SELECT INFO.L_FUND_ID,
                INFO.L_TRADE_DATE,
                INFO.CLASSIFY,
                INFO.TYPE,
                SUM(NVL(PF_CLASSIFY.CTB, 0))
                    OVER (PARTITION BY
                        INFO.L_FUND_ID,
                        INFO.CLASSIFY,
                        INFO.TYPE
                        ORDER BY
                            INFO.L_TRADE_DATE) AS CTB
         FROM FUND_PF_CLASSIFY_BASE PF_CLASSIFY
                  RIGHT JOIN (
             -- ��������
             SELECT TYPE.L_FUND_ID,
                    TYPE.CLASSIFY,
                    TYPE.TYPE,
                    TD.L_TRADE_DATE

             FROM (SELECT DISTINCT L_FUND_ID,
                                   CLASSIFY,
                                   TYPE
                   FROM FUND_PF_CLASSIFY_BASE FPCB) TYPE,
                  (SELECT L_TRADE_DATE
                   FROM ZHFX.TCALENDAR
                   WHERE L_DATE = L_TRADE_DATE
                     AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd')
                       AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')) TD) INFO
                             ON PF_CLASSIFY.L_FUND_ID = INFO.L_FUND_ID
                                 AND PF_CLASSIFY.CLASSIFY = INFO.CLASSIFY
                                 AND PF_CLASSIFY.TYPE = INFO.TYPE
                                 AND PF_CLASSIFY.L_TRADE_DATE = INFO.L_TRADE_DATE),
     FUND_PF_CLASSIFY_GROUP AS (SELECT L_FUND_ID,
                                       L_TRADE_DATE,
                                       CLASSIFY,
                                       SUM(CTB) CLASSIFY_CTB
                                FROM FUND_PF_CLASSIFY
                                GROUP BY L_FUND_ID, L_TRADE_DATE, CLASSIFY),
     FUND_PF_CLASSIFY_TOTAL AS (SELECT FPC.L_FUND_ID,
                                       FPC.L_TRADE_DATE,
                                       FPC.CLASSIFY,
                                       FPC.TYPE,
                                       FPC.CTB
                                FROM FUND_PF_CLASSIFY FPC
                                UNION ALL
                                SELECT FPCG.L_FUND_ID,
                                       FPCG.L_TRADE_DATE,
                                       FPCG.CLASSIFY,
                                       NULL              TYPE,
                                       FPCG.CLASSIFY_CTB CTB
                                FROM FUND_PF_CLASSIFY_GROUP FPCG
                                ORDER BY L_TRADE_DATE, CLASSIFY, CTB DESC)
-- ��������
SELECT FPCT.*,
       CASE
           WHEN FPCT.TYPE IS NULL THEN
               FPCT.CLASSIFY
           ELSE
               FPCT.TYPE || '��' || FPCT.CLASSIFY || '��'
           END NAME
FROM FUND_PF_CLASSIFY_TOTAL FPCT

