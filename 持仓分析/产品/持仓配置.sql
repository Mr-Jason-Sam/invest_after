WITH CONSTANTS AS (SELECT 'STOCK'   STOCK,
                          'BOND'    BOND,
                          'FUND'    FUND,
                          'FUTURES' FUTURES
                   FROM DUAL),
     PROFILIO AS
         (SELECT HDS.L_FUND_ID,
                 HDS.L_TRADE_DATE,
                 HDS.VC_WIND_CODE,
                 HDS.VC_STOCK_NAME,
                 CONS.STOCK TYPE,
                 HDS.EN_VALUE_MARKET
          FROM ZHFX.THOLDINGDETAILSHARE HDS,
               CONSTANTS CONS
          WHERE HDS.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
            AND HDS.L_FUND_ID IN (${fund_ids})
          UNION ALL
          SELECT HDB.L_FUND_ID,
                 HDB.L_TRADE_DATE,
                 HDB.VC_WIND_CODE,
                 HDB.VC_STOCK_NAME,
                 CONS.BOND TYPE,
                 HDB.EN_VALUE_MARKET
          FROM ZHFX.THOLDINGDETAILBOND HDB,
               CONSTANTS CONS
          WHERE HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
            AND HDB.L_FUND_ID IN (${fund_ids})
          UNION ALL
          SELECT HDF.L_FUND_ID,
                 HDF.L_TRADE_DATE,
                 HDF.VC_WIND_CODE,
                 HDF.VC_STOCK_NAME,
                 CONS.FUND TYPE,
                 HDF.EN_VALUE_MARKET
          FROM ZHFX.THOLDINGDETAILFUND HDF,
               CONSTANTS CONS
          WHERE HDF.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
            AND HDF.L_FUND_ID IN (${fund_ids})
          UNION ALL
          SELECT HDF.L_FUND_ID,
                 HDF.L_TRADE_DATE,
                 HDF.VC_WIND_CODE,
                 HDF.VC_STOCK_NAME,
                 CONS.FUTURES TYPE,
                 HDF.EN_VALUE_MARKET
          FROM ZHFX.THOLDINGDETAILFUTURES HDF,
               CONSTANTS CONS
          WHERE HDF.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
            AND HDF.L_FUND_ID IN (${fund_ids})),
     PROFILIO_RANK AS (SELECT PFL.L_FUND_ID,
                              PFL.L_TRADE_DATE,
                              PFL.VC_WIND_CODE,
                              PFL.VC_STOCK_NAME,
                              PFL.TYPE,
                              PFL.EN_VALUE_MARKET,
                              ROW_NUMBER() OVER (PARTITION BY PFL.L_FUND_ID,PFL.L_TRADE_DATE ORDER BY PFL.EN_VALUE_MARKET DESC) MKT_RANK
                       FROM PROFILIO PFL),
     CONCENTRATION AS (
         -- 集中度1、5、10、20
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                SUM(CASE WHEN MKT_RANK = 1 THEN EN_VALUE_MARKET ELSE 0 END)   TOP_1_MARKET_VALUE,
                SUM(CASE WHEN MKT_RANK <= 5 THEN EN_VALUE_MARKET ELSE 0 END)  TOP_5_MARKET_VALUE,
                SUM(CASE WHEN MKT_RANK <= 10 THEN EN_VALUE_MARKET ELSE 0 END) TOP_10_MARKET_VALUE,
                SUM(CASE WHEN MKT_RANK <= 20 THEN EN_VALUE_MARKET ELSE 0 END) TOP_20_MARKET_VALUE
         FROM PROFILIO_RANK
         GROUP BY L_FUND_ID, L_TRADE_DATE)
SELECT FUND_ASSETS.L_FUND_ID                                                                  FUND_ID,
       FUND_ASSETS.L_TRADE_DATE                                                               TRADE_DATE,
       FUND_ASSETS.EN_FUND_VALUE                                                              NET_ASSETS,
       FUND_ASSETS.EN_FUND_VALUE_TOTAL                                                        TOTAL_ASSETS,
       FUND_ASSETS.EN_FUND_SHARE                                                              FUND_SHARE,
       FUND_ASSETS.EN_BANK_DEPOSIT / FUND_ASSETS.EN_FUND_VALUE                                CASH_POSI,
       FUND_ASSETS.EN_SHARE_ASSET / FUND_ASSETS.EN_FUND_VALUE                                 STOCK_POSI,
       -- 债券投资 + 债券利息
       (FUND_ASSETS.EN_BOND_ASSET + FUND_ASSETS.EN_INTEREST_BOND) / FUND_ASSETS.EN_FUND_VALUE BOND_POSI,
       FUND_ASSETS.EN_FUND_ASSET / FUND_ASSETS.EN_FUND_VALUE                                  FUND_POSI,
       -- 融券回购资产 + 回购利息
       (FUND_ASSETS.EN_RQHG + FUND_ASSETS.EN_INTEREST_HG) / FUND_ASSETS.EN_FUND_VALUE         REVERST_REPO_POSI,
       -- 期货备付金 + 期货保证金
       (FUND_ASSETS.EN_BFJ_FUTURES + FUND_ASSETS.EN_BZJ_FUTURES) / FUND_ASSETS.EN_FUND_VALUE  FUTURES_POSI,
       -- 利率备付金 + 利率保证金
       (FUND_ASSETS.EN_BFJ_IRS + FUND_ASSETS.EN_BZJ_IRS) / FUND_ASSETS.EN_FUND_VALUE          IRS_POSI,
       -- 融资资产
       1 + FUND_ASSETS.EN_RZHG / FUND_ASSETS.EN_FUND_VALUE                                    LEVER_POSI,
       CCR.TOP_1_MARKET_VALUE,
       CCR.TOP_5_MARKET_VALUE,
       CCR.TOP_10_MARKET_VALUE,
       CCR.TOP_20_MARKET_VALUE
FROM ZHFX.TFUNDASSET FUND_ASSETS
         LEFT JOIN CONCENTRATION CCR
                   ON FUND_ASSETS.L_FUND_ID = CCR.L_FUND_ID AND FUND_ASSETS.L_TRADE_DATE = CCR.L_TRADE_DATE
WHERE FUND_ASSETS.L_FUND_ID IN (${fund_ids})
  AND FUND_ASSETS.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
ORDER BY FUND_ID, TRADE_DATE DESC