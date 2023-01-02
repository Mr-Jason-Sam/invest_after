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
                 HDS.EN_VALUE_MARKET,
                 HDS.L_AMOUNT
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
                 HDB.EN_VALUE_MARKET,
                 HDB.L_AMOUNT
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
                 HDF.EN_VALUE_MARKET,
                 HDF.L_AMOUNT
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
                 HDF.EN_VALUE_MARKET,
                 HDF.L_AMOUNT
          FROM ZHFX.THOLDINGDETAILFUTURES HDF,
               CONSTANTS CONS
          WHERE HDF.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
            AND HDF.L_FUND_ID IN (${fund_ids}))

SELECT *
FROM (SELECT PFL.L_FUND_ID,
             PFL.L_TRADE_DATE,
             PFL.VC_WIND_CODE,
             PFL.VC_STOCK_NAME,
             PFL.TYPE,
             PFL.L_AMOUNT,
             PFL.EN_VALUE_MARKET,
             PFL.EN_VALUE_MARKET / FUND_ASSETS.EN_FUND_VALUE                                                   NET_ASSETS_RATIO,
             PFL.EN_VALUE_MARKET / FUND_ASSETS.EN_FUND_VALUE_TOTAL                                             TOTAL_ASSETS_RATIO,
             ROW_NUMBER() OVER (PARTITION BY PFL.L_FUND_ID,PFL.L_TRADE_DATE ORDER BY PFL.EN_VALUE_MARKET DESC) MKT_RANK
      FROM PROFILIO PFL
               LEFT JOIN
           ZHFX.TFUNDASSET FUND_ASSETS
           ON
                       FUND_ASSETS.L_FUND_ID = PFL.L_FUND_ID
                   AND FUND_ASSETS.L_TRADE_DATE = PFL.L_TRADE_DATE)
WHERE MKT_RANK BETWEEN NVL(${min_rank}, 0) AND ${max_rank}
ORDER BY L_FUND_ID, L_TRADE_DATE DESC, MKT_RANK