WITH FUND_PROFIT AS (
    -- 基金场内外收益
    SELECT FUND_PF.L_FUND_ID,
           FUND_PF.L_TRADE_DATE,
           SUM(CASE WHEN VC_MARKET_TYPE IN ('上交所', '深交所') THEN DAY_PROFIT ELSE 0 END) IN_MK_DAY_PROFIT,
           SUM(CASE WHEN VC_MARKET_TYPE IN ('场外') THEN DAY_PROFIT ELSE 0 END)             OUT_MK_DAY_PROFIT,
           SUM(EN_FEE_TRADE) AS                                                             FEE_PROFIT
    FROM (SELECT L_FUND_ID,
                 L_TRADE_DATE,
                 VC_MARKET_TYPE,
                 VC_STOCK_CODE,
                 EN_INVEST_INCOME + EN_INCREMENT + EN_DIVIEND_CASH DAY_PROFIT,
                 EN_FEE_TRADE
          FROM ZHFX.THOLDINGDETAILFUND
          WHERE L_FUND_ID IN (${find_ids})
            AND L_TRADE_DATE BETWEEN ${begin_date} AND $(end_date)) FUND_PF
    GROUP BY FUND_PF.L_FUND_ID, FUND_PF.L_TRADE_DATE),
     FUTURES_PROFIT AS (
         -- 期货基础收益
         SELECT FUTURES_PF.L_FUND_ID,
                FUTURES_PF.L_TRADE_DATE,
                SUM(CASE WHEN VC_STOCK_TYPE IN ('股指期货') THEN DAY_PROFIT ELSE 0 END) STK_IDX_DAY_PROFIT,
                SUM(CASE WHEN VC_STOCK_TYPE IN ('国债期货') THEN DAY_PROFIT ELSE 0 END) CTY_BOND_DAY_PROFIT,
                SUM(CASE WHEN VC_STOCK_TYPE IN ('商品期货') THEN DAY_PROFIT ELSE 0 END) COMMODITY_DAY_PROFIT,
                SUM(EN_FEE_TRADE) AS                                                    FEE_PROFIT
         FROM (SELECT HDF.L_FUND_ID,
                      HDF.L_TRADE_DATE,
                      HDF.VC_STOCK_CODE,
                      SIF.VC_STOCK_TYPE,
                      HDF.EN_INVEST_INCOME + HDF.EN_INCREMENT DAY_PROFIT,
                      HDF.EN_FEE_TRADE
               FROM ZHFX.THOLDINGDETAILFUTURES HDF
                        LEFT JOIN ZHFX.TSTOCKINFOFUTURES SIF
                                  ON HDF.VC_STOCK_CODE = SIF.VC_STOCK_CODE
                                      AND HDF.L_TRADE_DATE = SIF.L_TRADE_DATE
               WHERE HDF.L_FUND_ID IN (${find_ids})
                 AND HDF.L_TRADE_DATE BETWEEN ${begin_date} AND $(end_date)) FUTURES_PF
         GROUP BY FUTURES_PF.L_FUND_ID, FUTURES_PF.L_TRADE_DATE),
     PRODUCT_PROFIT AS (
         -- 产品收益
         SELECT FRD.L_FUND_ID,
                FRD.L_TRADE_DATE,
                FRD.EN_FUND_ASSET_NET                                         FUND_VALUE,
                FRD.EN_FUND_ASSET_NET_PRE                                     FUND_VALUE_PRE,
                FRD.EN_FUND_ASSET_NET_PRE +
                FRD.EN_APPLY_BAL + FRD.EN_APPLY_DIRECT + FRD.EN_REDEEM_BAL +
                FRD.EN_REDEEM_DIRECT +
                FRD.EN_FUND_DIVIDEND + FRD.EN_FUND_DIVIDEND_INVEST + FRD.EN_INCOME_REWARD +
                FRD.EN_APPEND_BAL + FRD.EN_EXTRACT_BAL                        FUND_VALUE_LAST,
                FRD.EN_APPLY_BAL + FRD.EN_APPLY_DIRECT + FRD.EN_REDEEM_BAL +
                FRD.EN_REDEEM_DIRECT + FRD.EN_APPEND_BAL + FRD.EN_EXTRACT_BAL APPLY_REDEEM,
                FRD.EN_FUND_DIVIDEND + FRD.EN_FUND_DIVIDEND_INVEST            FUND_DIVIDEND,
                FRD.EN_INCOME_REWARD,
                FRD.EN_INTEREST_IN_CASH                                       CASH_INCOME,
                FRD.EN_INCREMENT_SHARE + FRD.EN_INVEST_INCOME_SHARE +
                FRD.EN_DIVIEND_INCOME_SHARE                                   STK_INCOME,
                FRD.EN_INTEREST_IN_BOND + FRD.EN_INCREMENT_BOND +
                FRD.EN_INVEST_INCOME_BOND                                     BOND_INCOME,
                FRD.EN_INCREMENT_FUND + FRD.EN_INVEST_INCOME_FUND +
                FRD.EN_DIVIEND_INCOME_FUND                                    FUND_INCOME,
                FRD.EN_INTEREST_IN_HG + FRD.EN_INTEREST_OUT_HG                HG_INCOME,
                FRD.EN_INCREMENT_FUTURES + FRD.EN_INVEST_INCOME_FUTURES       FUTURES_INCOME,
                FRD.EN_INTEREST_IN_IRS + FRD.EN_INCREMENT_IRS +
                FRD.EN_INVEST_INCOME_IRS                                      IRS_INCOME,
                FRD.EN_INCOME_OTHER                                           OTHER_INCOME,
                FRD.EN_FEE_MANAGEMENT + FRD.EN_FEE_TRUSTEESHIP + FRD.EN_FEE_SALE_SERVICE +
                FRD.EN_FEE_TRADE + FRD.EN_DIVIEND_INCOME_TAX +
                FRD.EN_FEE_ZZS + FRD.EN_FEE_OTHER + FRD.EN_INTEREST_IN_ZZS +
                FRD.EN_INCREMENT_ZZS + FRD.EN_INVEST_INCOME_ZZS               FEE_INCOME,
                FRD.EN_INTEREST_IN_HG                                         INCOME_HG_RQ,
                FRD.EN_INTEREST_OUT_HG                                        INCOME_HG_RZ,
                FRD.EN_INTEREST_IN_BOND                                       INCOME_BOND_INTEREST,
                FRD.EN_INCREMENT_BOND + FRD.EN_INVEST_INCOME_BOND             INCOME_BOND_INVEST
         FROM ZHFX.TFUNDRETURNDETAIL FRD
         WHERE FRD.L_FUND_ID IN (${find_ids})
           AND FRD.L_TRADE_DATE BETWEEN ${begin_date} AND $(end_date)),
     PRODUCT_PROFIT_DETAILS AS (
         -- 产品收益明细
         SELECT PF.*,
                PF.INCOME_STK_STGY + PF.INCOME_STK_TRADE + PF.INCOME_STK_FEE +
                PF.INCOME_STK_EXR + PF.INCOME_STK_OTHER + PF.INCOME_STK_IPO AS STK_PF
         FROM (SELECT FRD.*,
                      -- 股票
                      NVL(IDS.EN_INCOME_STGY, 0) + NVL(IDS.EN_INCOME_HK_STGY, 0)                  INCOME_STK_STGY,
                      NVL(IDS.EN_INCOME_STGY, 0)                                                  INCOME_STK_STGY_A,
                      NVL(IDS.EN_INCOME_HK_STGY, 0)                                               INCOME_STK_STGY_HK,
                      NVL(IDS.EN_INCOME_TRADE, 0) + NVL(IDS.EN_INCOME_HK_TRADE, 0)                INCOME_STK_TRADE,
                      NVL(IDS.EN_INCOME_FEE, 0) + NVL(IDS.EN_INCOME_FEE_HK, 0)                    INCOME_STK_FEE,
                      NVL(IDS.EN_INCOME_EXR, 0) + NVL(IDS.EN_INCOME_HK_EXR, 0)                    INCOME_STK_EXR,
                      NVL(IDS.EN_INCOME_OTHER, 0) + NVL(IDS.EN_INCOME_HK_OTHER, 0)                INCOME_STK_OTHER,
                      NVL(IDS.EN_INCOME_IPO, 0) + NVL(IDS.EN_INCOME_FEE_IPO, 0)                   INCOME_STK_IPO,
                      NVL(IDS.EN_INCOME_IPO_STAR, 0) + NVL(IDS.EN_INCOME_FEE_IPO_STAR, 0)         INCOME_STK_IPO_STAR,
                      NVL(IDS.EN_INCOME_IPO_OTHER, 0) +
                      NVL(IDS.EN_INCOME_FEE_IPO_OTHER, 0)                                         INCOME_STK_IPO_OTHER,
                      NVL(IDS.EN_VALUE_MARKET_PRE_STGY, 0) +
                      NVL(IDS.EN_VALUE_MARKET_PRE_STGY_HK, 0)                                     VALUE_MARKET_PRE_STGY,
                      NVL(IDS.EN_VALUE_MARKET_PRE_STGY, 0)                                        VALUE_MARKET_PRE_STGY_A,
                      NVL(IDS.EN_VALUE_MARKET_PRE_STGY_HK, 0)                                     VALUE_MARKET_PRE_STGY_HK,

                      -- 债券
                      FRD.FUND_VALUE - FRD.FUND_VALUE_LAST -
                      FRD.CASH_INCOME - FRD.STK_INCOME - FRD.FUND_INCOME - FRD.HG_INCOME -
                      FRD.FUTURES_INCOME - FRD.IRS_INCOME - FRD.OTHER_INCOME - FRD.FEE_INCOME     INCOME_BOND,
                      -- 其他
                      FRD.OTHER_INCOME + FRD.STK_INCOME - NVL(IDS.EN_INCOME, 0)                   INCOME_OTHER,
                      -- 费用
                      FRD.FEE_INCOME - NVL(IDS.EN_INCOME_FEE, 0) - NVL(IDS.EN_INCOME_FEE_HK, 0) -
                      NVL(IDS.EN_INCOME_FEE_IPO, 0)                                               INCOME_FEE,

                      -- 基金：场内、场外、交易
                      NVL(FP.IN_MK_DAY_PROFIT, 0)      AS                                         IN_MK_DAY_PROFIT,
                      NVL(FP.OUT_MK_DAY_PROFIT, 0)     AS                                         OUT_MK_DAY_PROFIT,
                      NVL(FP.FEE_PROFIT, 0)            AS                                         FUND_FEE_PF,
                      -- 期货：国债、股指、商品
                      NVL(FTP.CTY_BOND_DAY_PROFIT, 0)  AS                                         CTY_BOND_DAY_PROFIT,
                      NVL(FTP.STK_IDX_DAY_PROFIT, 0)   AS                                         STK_IDX_DAY_PROFIT,
                      NVL(FTP.COMMODITY_DAY_PROFIT, 0) AS                                         COMMODITY_DAY_PROFIT,
                      NVL(FTP.FEE_PROFIT, 0)                                                      FUTURES_FEE_PF,
                      -- 产品日收益率
                      DECODE(FRD.FUND_VALUE_LAST, 0, 0, FRD.FUND_VALUE / FRD.FUND_VALUE_LAST - 1) DAY_PROFIT_RATIO
               FROM PRODUCT_PROFIT FRD
                        LEFT JOIN ZHFX.TINCOMEDETAILSHARE IDS
                                  ON FRD.L_FUND_ID = IDS.L_FUND_ID AND FRD.L_TRADE_DATE = IDS.L_TRADE_DATE
                        LEFT JOIN FUND_PROFIT FP
                                  ON FRD.L_FUND_ID = FP.L_FUND_ID AND FRD.L_TRADE_DATE = FP.L_TRADE_DATE
                        LEFT JOIN FUTURES_PROFIT FTP
                                  ON FRD.L_FUND_ID = FTP.L_FUND_ID AND FRD.L_TRADE_DATE = FTP.L_TRADE_DATE
               WHERE FRD.L_FUND_ID IN (${find_ids})
                 AND FRD.L_TRADE_DATE BETWEEN ${begin_date} AND $(end_date)) PF),
     PDODUCT_PROFIT_ADJ_INFO AS (
         -- 产品调整项
         SELECT PPD.L_FUND_ID,
                PPD.L_TRADE_DATE,
                EXP(SUM(LN(1 + PPD.DAY_PROFIT_RATIO)) OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE)) /
                (1 + PPD.DAY_PROFIT_RATIO) PROFIT_ADJ
         FROM PRODUCT_PROFIT_DETAILS PPD),
     PROFIT_CTB AS (
         -- 收益贡献
         SELECT PPD.L_FUND_ID,
                PPD.L_TRADE_DATE,
                -- 产品收益率（总贡献）
                EXP(SUM(LN(1 + PPD.DAY_PROFIT_RATIO))
                        OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE)) -
                1                                                               RANGE_PROFIT_RATIO,
                -- 现金
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.CASH_INCOME / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) CASH_CTB,
                -- 股票总贡献
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.STK_PF / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) STK_CTB,
                -- 股票策略
                SUM(DECODE(PPD.FUND_VALUE_PRE, 0, 0, PPD.INCOME_STK_STGY / PPD.FUND_VALUE_PRE * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) STK_STGY_CTB,
                -- 股票交易
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_STK_TRADE / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) STK_TRADE_CTB,
                -- 股票费用
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_STK_FEE / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) STK_FEE_CTB,
                -- IPO贡献
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_STK_IPO / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) STK_IPO_CTB,
                -- 汇率
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_STK_EXR / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) STK_EXR_CTB,
                -- 估值差异
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_STK_OTHER / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) STK_OTHER_CTB,
                -- 申赎影响
                SUM((DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_STK_STGY / PPD.FUND_VALUE_LAST) -
                     DECODE(PPD.FUND_VALUE_PRE, 0, 0, PPD.INCOME_STK_STGY / PPD.FUND_VALUE_PRE)) *
                    PPAI.PROFIT_ADJ)
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) SHARE_APLRED,
                -- 债券总贡献
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_BOND / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) BOND_CTB,
                -- 债券票息
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_BOND_INTEREST / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) BOND_INTEREST_CTB,
                -- 债券资本利得
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_BOND_INVEST / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) BOND_INVEST_CTB,
                -- 回购总贡献
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.HG_INCOME / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) HG_CTB,
                -- 正回购（融资回购）
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_HG_RZ / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) HG_RZ_CTB,
                -- 逆回购（融券回购）
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_HG_RQ / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) HG_RQ_CTB,
                -- 基金：场内、场外、交易
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.FUND_INCOME / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FUND_CTB,
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.IN_MK_DAY_PROFIT / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FUND_IN_MK_CTB,
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.OUT_MK_DAY_PROFIT / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FUND_OUT_MK_CTB,
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.FUND_FEE_PF / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FUND_FEE_CTB,
                -- 期货：国债、股指、商品
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.FUTURES_INCOME / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FUTURES_CTB,
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.CTY_BOND_DAY_PROFIT / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FUTURES_CTY_BOND_CTB,
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.STK_IDX_DAY_PROFIT / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FUTURES_STK_IDX_CTB,
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.COMMODITY_DAY_PROFIT / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FUTURES_COMMODITY_CTB,
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.FUTURES_FEE_PF / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FUTURES_FEE_CTB,
                -- 其他贡献
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_OTHER / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) OTHER_CTB,
                -- 费用贡献
                SUM(DECODE(PPD.FUND_VALUE_LAST, 0, 0, PPD.INCOME_FEE / PPD.FUND_VALUE_LAST * PPAI.PROFIT_ADJ))
                    OVER (PARTITION BY PPD.L_FUND_ID ORDER BY PPD.L_TRADE_DATE) FEE_CTB
         FROM PRODUCT_PROFIT_DETAILS PPD
                  LEFT JOIN PDODUCT_PROFIT_ADJ_INFO PPAI
                            ON PPD.L_FUND_ID = PPAI.L_FUND_ID AND PPD.L_TRADE_DATE = PPAI.L_TRADE_DATE)

SELECT *
FROM PROFIT_CTB
ORDER BY L_FUND_ID, L_TRADE_DATE DESC
;