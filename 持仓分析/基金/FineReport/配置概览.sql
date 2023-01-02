WITH TD_ADJ AS (
    -- �����յ�����������ʼ�յ�������棬����Ҫ����T-1�����ݣ�
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- �Ѵ���������Ŀ�ʼ����
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE,
           COUNT(L_TRADE_DATE)    SAMPLE_DATES
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd')
        AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')),

     HOLDING_BASE_QUOTE AS (
         -- ����������Ϣ
         SELECT HDF.*,
                MFD.VC_FUND_TYPE_WIND_SECOND
         FROM ZHFX.THOLDINGDETAILFUND HDF
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON HDF.VC_STOCK_CODE = MFD.VC_STOCK_CODE,
              TD_ADJ
         WHERE HDF.L_FUND_ID = ${ztbh}
           AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE_LAST AND TD_ADJ.END_DATE
         ORDER BY HDF.L_TRADE_DATE),
     FOF_ASSETS AS (
         -- ��������ʲ�
         SELECT HBQ.L_FUND_ID,
                HBQ.L_TRADE_DATE,
                SUM(DECODE(HBQ.VC_MARKET_TYPE, '����', HBQ.EN_VALUE_MARKET, 0))                   EXCHANGE_OUT,
                SUM((CASE WHEN HBQ.VC_MARKET_TYPE <> '����' THEN HBQ.EN_VALUE_MARKET ELSE 0 END)) EXCHANGE_IN,
                SUM(HBQ.EN_DIVIEND_CASH)                                                          DIVIEND_CASH
         FROM HOLDING_BASE_QUOTE HBQ
         GROUP BY HBQ.L_FUND_ID, HBQ.L_TRADE_DATE)
-- ���ø���
SELECT FOF_ASSETS.L_FUND_ID,
       FOF_ASSETS.L_TRADE_DATE,
       (FOF_ASSETS.EXCHANGE_IN + FOF_ASSETS.EXCHANGE_OUT + FOF_ASSETS.DIVIEND_CASH) / 1e8 FOF_MKT,
       FOF_ASSETS.EXCHANGE_IN / 1e8  AS                                                   EXCHANGE_IN,
       FOF_ASSETS.EXCHANGE_OUT / 1e8 AS                                                   EXCHANGE_OUT,
       FOF_ASSETS.DIVIEND_CASH / 1e8 AS                                                   DIVIEND_CASH
FROM FOF_ASSETS
ORDER BY L_FUND_ID, L_TRADE_DATE