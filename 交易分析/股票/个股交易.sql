WITH TRADE_DATE_BEGIN_PRE AS (
    -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST
    FROM ZHFX.TCALENDAR
    WHERE L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
     TRADE_FLOW_STK AS (
         -- 处理买入金额
         SELECT FLOW.L_FUND_ID,
                FLOW.L_TRADE_DATE,
                FLOW.VC_WIND_CODE,
                QUOTE.VC_STOCK_NAME,
                -- 净买入额
                FLOW.EN_DEAL_BALANCE * CASE WHEN FLOW.VC_BUSIN_TYPE IN ('卖出', '大宗卖出') THEN -1 ELSE 1 END BALANCE,
                -- 成交金融
                FLOW.EN_DEAL_PRICE,
                -- 收盘价
                QUOTE.EN_PRICE_CLOSE
         FROM ZHFX.TTRADEFLOWSHARE FLOW
                  LEFT JOIN ZHFX.TSTOCKINFOSHARE QUOTE
                            ON FLOW.L_TRADE_DATE = QUOTE.L_TRADE_DATE AND FLOW.VC_WIND_CODE = QUOTE.VC_WIND_CODE
         WHERE FLOW.L_FUND_ID IN (${fund_ids})
           AND FLOW.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
           AND FLOW.VC_BUSIN_TYPE IN ('卖出', '大宗卖出', '买入', '大宗买入')),
-- 交易详情
     TRADE_DETAILS AS (
         -- 明细拆分
         SELECT STK_FLOW_RANGE_INFO.L_FUND_ID                                                       FUND_ID,
                STK_FLOW_RANGE_INFO.VC_WIND_CODE                                                    STK_CODE,
                MIN(STK_FLOW_RANGE_INFO.STOCK_NAME)                                                 STK_NAME,
                -- 交易次数
                SUM(STK_FLOW_RANGE_INFO.TRADE_CNT)                                                  TRADE_COUNT,
                -- 买入次数
                SUM(STK_FLOW_RANGE_INFO.BUY_CNT)                                                    BUY_COUNT,
                -- 卖出次数
                SUM(STK_FLOW_RANGE_INFO.SELL_CNT)                                                   SELL_COUNT,

                -- 交易比例
                SUM(STK_FLOW_RANGE_INFO.STK_TRADE_RATIO)                                            TRADE_RATIO,
                -- 买入交易比例
                SUM(STK_FLOW_RANGE_INFO.STK_BUY_TRADE_RATIO)                                        BUY_TRADE_RATIO,
                -- 卖出交易比例
                SUM(STK_FLOW_RANGE_INFO.STK_SELL_TRADE_RATIO)                                       SELL_TRADE_RATIO,

                -- 交易胜率
                DECODE(SUM(STK_FLOW_RANGE_INFO.TRADE_CNT), 0, 0,
                       SUM(STK_FLOW_RANGE_INFO.TRADE_WIN_CNT) / SUM(STK_FLOW_RANGE_INFO.TRADE_CNT)) TRADE_WIN_RATIO,
                -- 买入胜率
                DECODE(SUM(STK_FLOW_RANGE_INFO.BUY_CNT), 0, 0,
                       SUM(STK_FLOW_RANGE_INFO.BUY_WIN_CNT) / SUM(STK_FLOW_RANGE_INFO.BUY_CNT))     BUY_WIN_RATIO,
                -- 卖出胜率
                DECODE(SUM(STK_FLOW_RANGE_INFO.SELL_CNT), 0, 0,
                       SUM(STK_FLOW_RANGE_INFO.SELL_WIN_CNT) / SUM(STK_FLOW_RANGE_INFO.SELL_CNT))   SELL_WIN_RATIO
         FROM (SELECT STK_FLOW_DATE_INFO.*,
                      -- 交易比例 = 交易额 / 当日股票组合净资产
                      DECODE(STK_FLOW_DATE_INFO.STK_ASSETS, 0, 0,
                             STK_FLOW_DATE_INFO.TOTAL_BALANCE / STK_FLOW_DATE_INFO.STK_ASSETS)     STK_TRADE_RATIO,
                      -- 买入交易比例
                      DECODE(STK_FLOW_DATE_INFO.STK_ASSETS, 0, 0,
                             STK_FLOW_DATE_INFO.BUY_BALANCE / STK_FLOW_DATE_INFO.STK_ASSETS)       STK_BUY_TRADE_RATIO,
                      --卖出交易比例
                      DECODE(STK_FLOW_DATE_INFO.STK_ASSETS, 0, 0,
                             ABS(STK_FLOW_DATE_INFO.SELL_BALANCE) / STK_FLOW_DATE_INFO.STK_ASSETS) STK_SELL_TRADE_RATIO
               FROM (SELECT TFS.*,
                            -- 股票资产
                            FUND_ASSETS.STK_ASSETS_PRE + GREATEST(STK_BALANCE_INFO.STK_TOTAL_BALANCE, 0) STK_ASSETS
                     FROM (SELECT L_FUND_ID,
                                  VC_WIND_CODE,
                                  L_TRADE_DATE,
                                  MIN(VC_STOCK_NAME)                                      STOCK_NAME,
                                  -- 交易次数
                                  COUNT(1)                                                TRADE_CNT,
                                  -- 买入次数
                                  SUM(CASE WHEN BALANCE > 0 THEN 1 ELSE 0 END)            BUY_CNT,
                                  -- 卖出次数
                                  SUM(CASE WHEN BALANCE < 0 THEN 1 ELSE 0 END)            SELL_CNT,
                                  -- 总交易额
                                  SUM(ABS(BALANCE))                                       TOTAL_BALANCE,
                                  -- 买入额
                                  SUM(CASE WHEN BALANCE > 0 THEN BALANCE ELSE 0 END)      BUY_BALANCE,
                                  -- 卖出额
                                  SUM(CASE WHEN BALANCE < 0 THEN -1 * BALANCE ELSE 0 END) SELL_BALANCE,
                                  -- 交易胜利次数
                                  SUM(CASE
                                          WHEN BALANCE > 0 AND EN_DEAL_PRICE <= EN_PRICE_CLOSE OR
                                               BALANCE < 0 AND EN_DEAL_PRICE >= EN_PRICE_CLOSE THEN 1
                                          ELSE 0
                                      END)                                                TRADE_WIN_CNT,
                                  -- 买入时，交易胜利次数
                                  SUM(CASE
                                          WHEN BALANCE > 0 AND EN_DEAL_PRICE <= EN_PRICE_CLOSE THEN 1
                                          ELSE 0
                                      END)                                                BUY_WIN_CNT,
                                  -- 卖出时，交易胜利次数
                                  SUM(CASE
                                          WHEN BALANCE < 0 AND EN_DEAL_PRICE >= EN_PRICE_CLOSE THEN 1
                                          ELSE 0
                                      END)                                                SELL_WIN_CNT
                           FROM TRADE_FLOW_STK
                           GROUP BY L_FUND_ID, VC_WIND_CODE, L_TRADE_DATE) TFS
                              LEFT JOIN (
                         -- 处理T-1资产
                         SELECT L_FUND_ID,
                                L_TRADE_DATE,
                                -- T-1股票净资产
                                LAG(EN_SHARE_ASSET, 1, 0)
                                    OVER (PARTITION BY L_FUND_ID ORDER BY L_TRADE_DATE) STK_ASSETS_PRE
                         FROM ZHFX.TFUNDASSET,
                              TRADE_DATE_BEGIN_PRE
                         WHERE L_FUND_ID IN (${fund_ids})
                           AND L_TRADE_DATE BETWEEN TRADE_DATE_BEGIN_PRE.BEGIN_DATE_LAST AND ${end_date}) FUND_ASSETS
                                        ON TFS.L_FUND_ID = FUND_ASSETS.L_FUND_ID AND
                                           TFS.L_TRADE_DATE = FUND_ASSETS.L_TRADE_DATE
                              LEFT JOIN (
                         -- 处理T日股票交易
                         SELECT L_FUND_ID,
                                L_TRADE_DATE,
                                SUM(BALANCE) STK_TOTAL_BALANCE
                         FROM TRADE_FLOW_STK
                         GROUP BY L_FUND_ID, L_TRADE_DATE) STK_BALANCE_INFO
                                        ON TFS.L_FUND_ID = STK_BALANCE_INFO.L_FUND_ID AND TFS.L_TRADE_DATE =
                                                                                          STK_BALANCE_INFO.L_TRADE_DATE) STK_FLOW_DATE_INFO) STK_FLOW_RANGE_INFO
         GROUP BY STK_FLOW_RANGE_INFO.L_FUND_ID, STK_FLOW_RANGE_INFO.VC_WIND_CODE)
SELECT TD.FUND_ID,
       TD.STK_CODE,
       TD.STK_NAME,
       TD.TRADE_COUNT,
       TD.BUY_COUNT,
       TD.SELL_COUNT,
       TD.TRADE_RATIO,
       TD.BUY_TRADE_RATIO,
       TD.SELL_TRADE_RATIO,
       TD.TRADE_WIN_RATIO,
       TD.BUY_WIN_RATIO,
       TD.SELL_WIN_RATIO
FROM TRADE_DETAILS TD
;
