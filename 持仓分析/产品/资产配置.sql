WITH CONSTANTS AS (SELECT '上交所' SH_EXCHANGE_NAME,
                          '深交所' SZ_EXCHANGE_NAME,
                          '港交所' HK_EXCHANGE_NAME,
                          'HK'     HK_EXCHANGE_CODE,
                          '港股通' HK_SH_SZ_MARKET_NAME,
                          '60'     SH_MAIN_CODE_PREFIX,
                          '00'     SZ_MAIN_CODE_PREFIX,
                          '30'     GEM_CODE_PREFIX,
                          '68'     STM_CODE_PREFIX
                   FROM DUAL),
     ASSETS_INFO AS (
         -- 资产数据
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                EN_FUND_VALUE                        NET_ASSETS,
                EN_FUND_VALUE_TOTAL,

                -- 现金
                -- 银行存款
                EN_BANK_DEPOSIT                      BANK_DEPOSIT_MV,
                -- 交易所最低清算备付金
                EN_BFJ - EN_BFJ_FUTURES - EN_BFJ_IRS LEAST_PROVISIONS_MV,
                -- 交易所结算保证金
                EN_BZJ - EN_BZJ_FUTURES - EN_BZJ_IRS EXCHANGE_CASH_DEPOSIT_MV,

                -- 利率互换
                -- 利率互换清算备付金
                EN_BFJ_IRS                           PROVISIONS_MV,
                -- 利率互换存出保证金
                EN_BZJ_IRS                           CASH_DEPOSIT_MV,

                -- 回购
                -- 逆回购
                -- 交易所质押式回购
                EN_RZHG_MARKET                       PLEDGE_RV_REPO_MV,
                -- 交易所协议式回购
                EN_RZHG_MARKET_OUT                   PROTOCOL_RV_REPO_MV,
                -- 银行间（质押式、买断式）回购
                EN_RZHG_BANK                         BANK_RV_REPO_MV,
                -- 正回购
                -- 交易所质押式回购
                EN_RQHG_MARKET                       PLEDGE_REPO_MV,
                -- 交易所协议式回购
                EN_RQHG_MARKET_OUT                   PROTOCOL_REPO_MV,
                -- 银行间（质押式、买断式）回购
                EN_RQHG_BANK                         BANK_REPO_MV,

                -- 期货清算备付金
                EN_BFJ_FUTURES                       FUTURES_PROVISIONS_MV,
                -- 期货存出保证金
                EN_BZJ_FUTURES                       FUTURES_CASH_DEPOSIT_MV,
                -- 多头合约价值
                EN_FUTURES_ASSET_LONG                FUTURES_LONG_MV,
                -- 空头合约价值
                EN_FUTURES_ASSET_SHORT               FUTURES_SHORT_MV
         FROM ZHFX.TFUNDASSET
         WHERE L_FUND_ID IN (${fund_ids})
           AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     STK_ASSETS_INFO AS (
         -- 股票资产数据
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                -- 沪深交易所
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) THEN EN_VALUE_MARKET
                        ELSE 0 END) SH_SZ_MV,
                -- A股主板
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            AND REGEXP_LIKE(VC_WIND_CODE,
                                            '^(' || CONS.SZ_MAIN_CODE_PREFIX || '|' || CONS.SH_MAIN_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )               A_MAIN_MV,
                -- A股创业板（Growth Enterprise Market）
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            AND REGEXP_LIKE(VC_WIND_CODE, '^(' || CONS.GEM_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )               GEM_MV,
                -- A股科创板（Science and Technology innovation board Market）
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            AND REGEXP_LIKE(VC_WIND_CODE, '^(' || CONS.STM_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )               STM_MV,
                -- 港股通
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
         -- 新股
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                SUM(EN_VALUE_MARKET) NEW_STK_MV,
                -- 新股（沪深交易所）
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_SH_SZ_MV,
                -- 新股（A股主板）
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME)
                            AND REGEXP_LIKE(VC_WIND_CODE, '^(' || CONS.SZ_MAIN_CODE_PREFIX || '|' ||
                                                          CONS.SH_MAIN_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_A_MAIN_MV,
                -- 新股（A股创业板）
                SUM(CASE
                        WHEN VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) AND
                             REGEXP_LIKE(VC_WIND_CODE, '^(' || CONS.GEM_CODE_PREFIX || ')')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_GEM_MV,
                -- 新股（A股科创板）
                SUM(CASE
                        WHEN VC_MARKET_TYPE = CONS.HK_SH_SZ_MARKET_NAME
                            THEN EN_VALUE_MARKET
                        ELSE 0 END
                    )                NEW_STK_STM_MV,
                -- 新股（港股通）
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
         -- 限售股
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
         -- 债券资产
         SELECT HDB.L_FUND_ID,
                HDB.L_TRADE_DATE,
                SUM(HDB.EN_VALUE_MARKET) BOND_MV,
                -- 利率债
                SUM(CASE
                        WHEN BD.VC_BOND_TYPE1_WIND IN ('国债', '央行票据') OR
                             BD.VC_BOND_TYPE2_WIND = '政策银行债'
                            THEN EN_VALUE_MARKET
                        ELSE 0 END)      IRB_MV,
                -- 可转债（可转可交换债）
                SUM(CASE
                        WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债')
                            THEN EN_VALUE_MARKET
                        ELSE 0 END)      CHANGE_BOND_MV,
                -- 永续债
                SUM(CASE
                        WHEN BD.VC_IS_YXZ = '是'
                            THEN EN_VALUE_MARKET
                        ELSE 0 END)      PERPETUAL_MV,
                -- 二级资本债
                SUM(CASE
                        WHEN BD.VC_BOND_TYPE2_WIND = '商业银行次级债券'
                            THEN EN_VALUE_MARKET
                        ELSE 0 END)      SECOND_BOND_MV
         FROM ZHFX.THOLDINGDETAILBOND HDB
                  LEFT JOIN ZHFX.TBONDDESCRIPTION BD ON HDB.VC_WIND_CODE = BD.VC_WIND_CODE,
              CONSTANTS CONS
         WHERE HDB.L_FUND_ID IN (${fund_ids})
           AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY HDB.L_FUND_ID, HDB.L_TRADE_DATE),
     CITY_ASSETS AS (
         -- 城投债
         SELECT HDB.L_FUND_ID,
                HDB.L_TRADE_DATE,
                SUM(HDB.EN_VALUE_MARKET) CITY_MV
         FROM ZHFX.THOLDINGDETAILBOND HDB
                  LEFT JOIN ZHFX.TUCIBONDISSUERCJ UCI ON HDB.VC_STOCK_NAME = UCI.VC_ISSUER_NAME
         WHERE HDB.L_FUND_ID IN (${fund_ids})
           AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY HDB.L_FUND_ID, HDB.L_TRADE_DATE),
     FUND_ASSETS_INFO AS (
         -- 基金
         SELECT HDF.L_FUND_ID,
                HDF.L_TRADE_DATE,
                SUM(CASE WHEN HDF.VC_MARKET_TYPE IN ('上交所', '深交所') THEN HDF.EN_VALUE_MARKET ELSE 0 END) IN_MK_MV,
                SUM(CASE WHEN HDF.VC_MARKET_TYPE IN ('场外') THEN HDF.EN_VALUE_MARKET ELSE 0 END)             OUT_MK_MV
         FROM ZHFX.THOLDINGDETAILFUND HDF
                  LEFT JOIN ZHFX.TSTOCKINFOFUND SIF ON HDF.VC_WIND_CODE = SIF.VC_WIND_CODE AND
                                                       HDF.L_TRADE_DATE = SIF.L_TRADE_DATE
         WHERE HDF.L_FUND_ID IN (${fund_ids})
           AND HDF.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY HDF.L_FUND_ID, HDF.L_TRADE_DATE),
     FUTURES_ASSETS_INFO AS (
         -- 期货资产
         SELECT HDF.L_FUND_ID,
                HDF.L_TRADE_DATE,
                SUM(CASE WHEN VC_STOCK_TYPE IN ('股指期货') THEN EN_VALUE_MARKET ELSE 0 END) STK_IDX_MV,
                SUM(CASE WHEN VC_STOCK_TYPE IN ('国债期货') THEN EN_VALUE_MARKET ELSE 0 END) CTY_BOND_MV,
                SUM(CASE WHEN VC_STOCK_TYPE IN ('商品期货') THEN EN_VALUE_MARKET ELSE 0 END) COMMODITY_MV
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

       -- 现金
       -- 银行存款
       NVL((ASSETS_INFO.BANK_DEPOSIT_MV + ASSETS_INFO.LEAST_PROVISIONS_MV + ASSETS_INFO.EXCHANGE_CASH_DEPOSIT_MV) / ASSETS_INFO.NET_ASSETS, 0)          CASH_POSI,
       NVL(ASSETS_INFO.BANK_DEPOSIT_MV / ASSETS_INFO.NET_ASSETS, 0)          BANK_DEPOSIT_POSI,
       -- 交易所最低清算备付金
       NVL(ASSETS_INFO.LEAST_PROVISIONS_MV / ASSETS_INFO.NET_ASSETS, 0)      LEAST_PROVISIONS_POSI,
       -- 交易所结算保证金
       NVL(ASSETS_INFO.EXCHANGE_CASH_DEPOSIT_MV / ASSETS_INFO.NET_ASSETS, 0) EXCHANGE_CASH_DEPOSIT_POSI,

       -- 利率互换
       -- 利率互换清算备付金
       NVL(ASSETS_INFO.PROVISIONS_MV / ASSETS_INFO.NET_ASSETS, 0)            PROVISIONS_POSI,
       --利率互换存出保证金
       NVL(ASSETS_INFO.CASH_DEPOSIT_MV / ASSETS_INFO.NET_ASSETS, 0)          CASH_DEPOSIT_POSI,
       -- 回购
       -- 逆回购
       -- 交易所质押式回购
       NVL(ASSETS_INFO.PLEDGE_RV_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)        PLEDGE_RV_REPO_POSI,
       -- 交易所协议式回购
       NVL(ASSETS_INFO.PROTOCOL_RV_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)      PROTOCOL_RV_REPO_POSI,
       -- 银行间（质押式、买断式）回购
       NVL(ASSETS_INFO.BANK_RV_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)          BANK_RV_REPO_POSI,
       -- 正回购
       -- 交易所质押式回购
       NVL(ASSETS_INFO.PLEDGE_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)           PLEDGE_REPO_POSI,
       -- 交易所协议式回购
       NVL(ASSETS_INFO.PROTOCOL_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)         PROTOCOL_REPO_POSI,
       -- 银行间（质押式、买断式）回购
       NVL(ASSETS_INFO.BANK_REPO_MV / ASSETS_INFO.NET_ASSETS, 0)             BANK_REPO_POSI,

       -- 期货清算备付金
       NVL(ASSETS_INFO.FUTURES_PROVISIONS_MV / ASSETS_INFO.NET_ASSETS, 0)    FUTURES_PROVISIONS_POSI,
       -- 期货存出保证金
       NVL(ASSETS_INFO.FUTURES_CASH_DEPOSIT_MV / ASSETS_INFO.NET_ASSETS, 0)  FUTURES_CASH_DEPOSIT_POSI,
       -- 多头合约价值
       NVL(ASSETS_INFO.FUTURES_LONG_MV / ASSETS_INFO.NET_ASSETS, 0)          FUTURES_LONG_POSI,
       -- 空头合约价值
       NVL(ASSETS_INFO.FUTURES_SHORT_MV / ASSETS_INFO.NET_ASSETS, 0)         FUTURES_SHORT_POSI,

       -- 股票
       -- 沪深交易所
       NVL(SAI.SH_SZ_MV / ASSETS_INFO.NET_ASSETS, 0)                         SH_SZ_POSI,
       -- A股主板
       NVL(SAI.A_MAIN_MV / ASSETS_INFO.NET_ASSETS, 0)                        A_MAIN_POSI,
       -- A股创业板
       NVL(SAI.GEM_MV / ASSETS_INFO.NET_ASSETS, 0)                           GEM_POSI,
       -- A股科创板
       NVL(SAI.STM_MV / ASSETS_INFO.NET_ASSETS, 0)                           STM_POSI,
       -- 港股通
       NVL(SAI.HK_SH_SZ_MV / ASSETS_INFO.NET_ASSETS, 0)                      HK_SH_SZ_POSI,
       -- QDII
       NVL(SAI.QDII_MV / ASSETS_INFO.NET_ASSETS, 0)                          QDII_POSI,
       -- 新股
       NVL(NSAI.NEW_STK_MV / ASSETS_INFO.NET_ASSETS, 0)                      NEW_STK_POSI,
       -- 新股（沪深交易所）
       NVL(NSAI.NEW_STK_SH_SZ_MV / ASSETS_INFO.NET_ASSETS, 0)                NEW_STK_SH_SZ_POSI,
       -- 新股（A股主板）
       NVL(NSAI.NEW_STK_A_MAIN_MV / ASSETS_INFO.NET_ASSETS, 0)               NEW_STK_A_MAIN_POSI,
       -- 新股（A股创业板）
       NVL(NSAI.NEW_STK_GEM_MV / ASSETS_INFO.NET_ASSETS, 0)                  NEW_STK_GEM_POSI,
       -- 新股（A股科创板）
       NVL(NSAI.NEW_STK_STM_MV / ASSETS_INFO.NET_ASSETS, 0)                  NEW_STK_STM_POSI,
       -- 新股（港股通）
       NVL(NSAI.NEW_STK_HK_SH_SZ_MV / ASSETS_INFO.NET_ASSETS, 0)             NEW_STK_HK_SH_SZ_POSI,
       -- 新股（QDII）
       NVL(NSAI.NEW_STK_QDII_MV / ASSETS_INFO.NET_ASSETS, 0)                 NEW_STK_QDII_POSI,
       -- 限售股
       NVL(RSA.RESTRICTED_STK_MV / ASSETS_INFO.NET_ASSETS, 0)                RESTRICTED_STK_POSI,

       -- 债券
       -- 利率债
       NVL(BAI.IRB_MV / ASSETS_INFO.NET_ASSETS, 0)                           BOND_IRB_POSI,
       -- 信用债
       NVL((BAI.BOND_MV - BAI.IRB_MV) / ASSETS_INFO.NET_ASSETS, 0)           BOND_CREDIT_POSI,
       -- 可转债（可转可交换债）
       NVL(BAI.CHANGE_BOND_MV / ASSETS_INFO.NET_ASSETS, 0)                   BOND_CHANGE_BOND_POSI,
       -- 永续债
       NVL(BAI.PERPETUAL_MV / ASSETS_INFO.NET_ASSETS, 0)                     BOND_PERPETUAL_POSI,
       -- 二级资本债
       NVL(BAI.SECOND_BOND_MV / ASSETS_INFO.NET_ASSETS, 0)                   BOND_SECOND_BOND_POSI,
       -- 城投债
       NVL(CA.CITY_MV / ASSETS_INFO.NET_ASSETS, 0)                           BOND_CITY_POSI,

       -- 基金
       -- 场内
       NVL(FAI.IN_MK_MV / ASSETS_INFO.NET_ASSETS, 0)                         FUND_IN_MK_POSI,
       -- 场外
       NVL(FAI.OUT_MK_MV / ASSETS_INFO.NET_ASSETS, 0)                        FUND_OUT_MK_POSI,

       -- 期货
       -- 股指期货
       NVL(FT_AI.STK_IDX_MV / ASSETS_INFO.NET_ASSETS, 0)                     FT_STK_IDX_POSI,
       -- 国债期货
       NVL(FT_AI.CTY_BOND_MV / ASSETS_INFO.NET_ASSETS, 0)                    FT_CTY_BOND_POSI,
       -- 商品期货
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
