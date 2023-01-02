WITH
-- 交易日调整
TRADE_DATE_ADJ AS (
    -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- 已处理当天收益的开始日期
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
HOLDING_FILTER AS (
    -- 持仓标的筛选
    SELECT HOLDING_INFO.*
    FROM (SELECT HDS.*, NVL(HDS_IPO.EN_IPO_BALANCE, 0) AS EN_IPO_BALANCE
          FROM ZHFX.THOLDINGDETAILSHARE HDS
                   LEFT JOIN ZHFX.THOLDINGDETAILSHAREIPO HDS_IPO
                             ON HDS.L_FUND_ID = HDS_IPO.L_FUND_ID AND HDS.VC_WIND_CODE = HDS_IPO.VC_WIND_CODE AND
                                HDS.L_TRADE_DATE = HDS_IPO.L_TRADE_DATE
          WHERE HDS.L_FUND_ID IN (${fund_ids})
            AND HDS.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
            AND NOT (SUBSTR(HDS.VC_STOCK_CODE, 1, 3) = '360' AND LENGTH(HDS.VC_STOCK_CODE) = 6)) HOLDING_INFO
    WHERE HOLDING_INFO.EN_IPO_BALANCE = 0),
HOLDING_STK_CODE AS (
    -- 持仓股票池
    SELECT DISTINCT HOLDING_INFO.VC_WIND_CODE
    FROM HOLDING_FILTER HOLDING_INFO),
STK_QUOTE AS (
    -- 股票行情
    SELECT STK_INFO.*,
           NVL(DECODE(STK_INFO.EN_PRICE_CLOSE_PRE, 0, 0,
                      STK_INFO.EN_PRICE_CLOSE / STK_INFO.EN_PRICE_CLOSE_PRE - 1), 0) DAY_PROFIT_RATIO
    FROM HOLDING_FILTER HDS
             LEFT JOIN ZHFX.TSTOCKINFOSHARE STK_INFO
                       ON HDS.L_TRADE_DATE = STK_INFO.L_TRADE_DATE AND HDS.VC_WIND_CODE = STK_INFO.VC_WIND_CODE),
FUND_ASSETS_INFO AS (SELECT L_FUND_ID,
                            L_TRADE_DATE,
                            EN_FUND_ASSET_NET                                             FUND_ASSETS_NET,
                            -- 昨日净资产
                            EN_FUND_ASSET_NET_PRE +
                                -- 买入项(数据库中为负数，则为累加项)
                            EN_APPLY_BAL + EN_APPLY_DIRECT + EN_APPEND_BAL +
                                -- 卖出项
                            EN_REDEEM_BAL + EN_REDEEM_DIRECT + EN_EXTRACT_BAL +
                                -- 其他：分红、分红再投、业绩报酬
                            EN_FUND_DIVIDEND + EN_FUND_DIVIDEND_INVEST + EN_INCOME_REWARD FUND_ASSETS_NET_PRE
                     FROM ZHFX.TFUNDRETURNDETAIL
                     WHERE L_FUND_ID IN (${fund_ids})
                       AND L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}),
STK_PFL_INFO AS (
    -- 股票组合数据
    SELECT HDS.L_FUND_ID,
           HDS.L_TRADE_DATE,
           SUM(HDS.EN_VALUE_MARKET_PRE) STK_PFL_MV_PRE
    FROM HOLDING_FILTER HDS
    GROUP BY HDS.L_FUND_ID,
             HDS.L_TRADE_DATE),
WEIGHT_INFO AS (
    -- 权重
    SELECT WEIGHT_INFO_OIRIGIN.*,
           NVL(STK_PFL_WEIGHT_PRE_ORIGIN, 0) STK_PFL_WEIGHT_PRE,
           NVL(PD_WEIGHT_PRE_ORIGIN, 0)      PD_WEIGHT_PRE
    FROM (SELECT HDS.L_FUND_ID,
                 HDS.VC_WIND_CODE,
                 HDS.L_TRADE_DATE,
                 DECODE(
                         SUM(HDS.EN_VALUE_MARKET_PRE) OVER (PARTITION BY HDS.L_FUND_ID, HDS.L_TRADE_DATE),
                         0, 0,
                         HDS.EN_VALUE_MARKET_PRE /
                         (SUM(HDS.EN_VALUE_MARKET_PRE)
                              OVER (PARTITION BY HDS.L_FUND_ID, HDS.L_TRADE_DATE))
                     ) STK_PFL_WEIGHT_PRE_ORIGIN,
                 DECODE(
                         FA.FUND_ASSETS_NET_PRE,
                         0, 0,
                         HDS.EN_VALUE_MARKET_PRE / FA.FUND_ASSETS_NET_PRE
                     ) PD_WEIGHT_PRE_ORIGIN
          FROM HOLDING_FILTER HDS
                   LEFT JOIN FUND_ASSETS_INFO FA ON HDS.L_TRADE_DATE = FA.L_TRADE_DATE AND HDS.L_FUND_ID = FA.L_FUND_ID
          WHERE HDS.L_FUND_ID IN (${fund_ids})
            AND HDS.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}) WEIGHT_INFO_OIRIGIN),
STK_PROFIT_ADJ_INFO AS (
    -- 股票权重调整项
    SELECT STK_PROFIT.L_TRADE_DATE,
           EXP(SUM(LN(1 + STK_PROFIT.STK_PORFOLIO_PROFIT)) OVER (ORDER BY STK_PROFIT.L_TRADE_DATE)) /
           (1 + STK_PROFIT.STK_PORFOLIO_PROFIT)      STK_PFL_PROFIT_ADJ,
           EXP(SUM(LN(1 + STK_PROFIT.PD_PORFOLIO_PROFIT_RATIO)) OVER (ORDER BY STK_PROFIT.L_TRADE_DATE)) /
           (1 + STK_PROFIT.PD_PORFOLIO_PROFIT_RATIO) PD_PROFIT_ADJ
    FROM (SELECT STK_QUOTE.L_TRADE_DATE,
                 SUM(WEIGHT_INFO.STK_PFL_WEIGHT_PRE * STK_QUOTE.DAY_PROFIT_RATIO) STK_PORFOLIO_PROFIT,
                 SUM(WEIGHT_INFO.PD_WEIGHT_PRE * STK_QUOTE.DAY_PROFIT_RATIO)      PD_PORFOLIO_PROFIT_RATIO
          FROM STK_QUOTE
                   LEFT JOIN WEIGHT_INFO ON STK_QUOTE.VC_WIND_CODE = WEIGHT_INFO.VC_WIND_CODE AND
                                            STK_QUOTE.L_TRADE_DATE = WEIGHT_INFO.L_TRADE_DATE
          GROUP BY STK_QUOTE.L_TRADE_DATE) STK_PROFIT),
HOLDING_DATE_INFO AS (
    -- 持仓分析
    SELECT TOTAL_HDS.L_FUND_ID,
           TOTAL_HDS.VC_WIND_CODE,
           MIN(L_TRADE_DATE) SETUP_DATE
    FROM HOLDING_STK_CODE HSC
             LEFT JOIN HOLDING_FILTER TOTAL_HDS
                       ON HSC.VC_WIND_CODE = TOTAL_HDS.VC_WIND_CODE
    WHERE TOTAL_HDS.L_FUND_ID IN (${fund_ids})
      AND TOTAL_HDS.L_TRADE_DATE <= ${end_date}
    GROUP BY TOTAL_HDS.L_FUND_ID, TOTAL_HDS.VC_WIND_CODE),
STK_RANGE_PROFIT_DETAILS AS (
    -- 股票区间收益
    SELECT VC_WIND_CODE,
           EXP(SUM(LN(1 + DAY_PROFIT_RATIO))) - 1 RANGE_PROFIT
    FROM STK_QUOTE
    WHERE L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
    GROUP BY VC_WIND_CODE),
-- 持仓详情
HOLDING_DETAILS AS (
    -- 持仓 & 信息
    SELECT HOLDING_INFO.*,
           -- 报告期收益率：报告期内的个股收益率
           SRPD.RANGE_PROFIT AS RANGE_PROFIT,
           -- 清仓日
           DECODE(MAX_HOLDING_DATE, TDA.END_DATE, NULL, MAX_HOLDING_DATE) CLEAR_DATE
    FROM (SELECT
              -- 产品序号
              HDS.L_FUND_ID                                          FUND_ID,
              -- 股票代码
              HDS.VC_WIND_CODE                                       STK_CODE,
              -- 股票名称
              MAX(HDS.VC_STOCK_NAME)                                 STK_NAME,
              -- 申万一级
              MAX(STK_QUOTE.VC_INDUSTRY_SW_FIRST)                    SW_1,
              -- 申万二级
              MAX(STK_QUOTE.VC_INDUSTRY_SW_SECOND)                   SW_2,
              -- 申万三级
              MAX(STK_QUOTE.VC_INDUSTRY_SW_THIRD)                    SW_3,
              -- 建仓日
              MIN(HOLDING_DATE_INFO.SETUP_DATE)                      SETUP_DATE,
              -- 最大持仓日
              MAX(HDS.L_TRADE_DATE)                                  MAX_HOLDING_DATE,

              -- 收益金额 = 公允价格变动损益 + 投资收益
              SUM(HDS.EN_INCREMENT + HDS.EN_INVEST_INCOME + HDS.EN_DIVIEND_CASH)           PROFIT_BALANCE,
              -- 股票组合收益贡献 = sum (T-1权重 * 个股T日收益率 * 股票组合收益调整项)
              SUM(WEIGHT_INFO.STK_PFL_WEIGHT_PRE * STK_QUOTE.DAY_PROFIT_RATIO *
                  SPAI.STK_PFL_PROFIT_ADJ)                           STK_PFL_PROFIT_CTB,
              -- 产品组合收益贡献 = sum (T日收益 / T-1产品净资产 * 产品收益调整项)
              SUM((HDS.EN_INCREMENT + HDS.EN_INVEST_INCOME) / FUND_ASSETS.FUND_ASSETS_NET_PRE *
                  SPAI.PD_PROFIT_ADJ)                                PD_PROFIT_CTB,
              -- 持有期收益率：持有期内的个股收益率
              EXP(SUM(LN(1 + DECODE(WEIGHT_INFO.STK_PFL_WEIGHT_PRE, 0, 0, 1) * STK_QUOTE.DAY_PROFIT_RATIO))) -
              1                                                      HOLDING_PROFIT,
              MIN(HDS.EN_VALUE_MARKET / FUND_ASSETS.FUND_ASSETS_NET) MIN_POSITION,
              -- 最高仓位
              MAX(HDS.EN_VALUE_MARKET / FUND_ASSETS.FUND_ASSETS_NET) MAX_POSITION,
              -- 平均仓位
              AVG(HDS.EN_VALUE_MARKET / FUND_ASSETS.FUND_ASSETS_NET) AVG_POSITION
              -- 股票持仓
          FROM HOLDING_FILTER HDS
                   -- 权重信息
                   LEFT JOIN WEIGHT_INFO
                             ON WEIGHT_INFO.VC_WIND_CODE = HDS.VC_WIND_CODE AND
                                WEIGHT_INFO.L_FUND_ID = HDS.L_FUND_ID AND
                                WEIGHT_INFO.L_TRADE_DATE = HDS.L_TRADE_DATE
              -- 股票信息
                   LEFT JOIN STK_QUOTE
                             ON HDS.VC_WIND_CODE = STK_QUOTE.VC_WIND_CODE AND
                                HDS.L_TRADE_DATE = STK_QUOTE.L_TRADE_DATE
                   LEFT JOIN STK_PROFIT_ADJ_INFO SPAI ON HDS.L_TRADE_DATE = SPAI.L_TRADE_DATE
              -- 持仓日信息
                   LEFT JOIN HOLDING_DATE_INFO
                             ON HDS.L_FUND_ID = HOLDING_DATE_INFO.L_FUND_ID AND
                                HDS.VC_WIND_CODE = HOLDING_DATE_INFO.VC_WIND_CODE
              -- 产品资产
                   LEFT JOIN FUND_ASSETS_INFO FUND_ASSETS
                             ON HDS.L_TRADE_DATE = FUND_ASSETS.L_TRADE_DATE AND
                                HDS.L_FUND_ID = FUND_ASSETS.L_FUND_ID
              -- 股票权重
                   LEFT JOIN STK_PFL_INFO SPI
                             ON HDS.L_TRADE_DATE = SPI.L_TRADE_DATE AND
                                HDS.L_FUND_ID = SPI.L_FUND_ID
          WHERE HDS.L_FUND_ID IN (${fund_ids})
            AND HDS.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
          GROUP BY HDS.L_FUND_ID,
                   HDS.VC_WIND_CODE) HOLDING_INFO
              -- 股票区间收益率
                   LEFT JOIN STK_RANGE_PROFIT_DETAILS SRPD
                             ON HOLDING_INFO.STK_CODE = SRPD.VC_WIND_CODE,
         TRADE_DATE_ADJ TDA)

SELECT HD.FUND_ID,
       HD.STK_CODE,
       HD.STK_NAME,
       HD.SW_1,
       HD.SW_2,
       HD.SW_3,
       TDA.BEGIN_DATE,
       TDA.END_DATE,
       HD.SETUP_DATE,
       HD.CLEAR_DATE,
       -- 收益金额
       HD.PROFIT_BALANCE,
       -- 收益贡献(股票组合)
       HD.STK_PFL_PROFIT_CTB,
       -- 收益贡献(产品组合)
       HD.PD_PROFIT_CTB,
       -- 持有收益率
       HD.HOLDING_PROFIT,
       -- 报告期收益率

       HD.RANGE_PROFIT,
       HD.MIN_POSITION,
       HD.AVG_POSITION,
       HD.MAX_POSITION
FROM HOLDING_DETAILS HD,
     TRADE_DATE_ADJ TDA
ORDER BY FUND_ID, STK_PFL_PROFIT_CTB DESC
;
