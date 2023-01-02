WITH TD_ADJ AS (
    -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- 已处理当天收益的开始日期
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd')
        AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')),
      FOF_ASSETS AS (
        -- 基金组合资产
        SELECT
            HDF.L_FUND_ID,
            HDF.L_TRADE_DATE,
            SUM(HDF.EN_MARKET_INIT) FOF_MV_INIT
            FROM ZHFX.THOLDINGDETAILFUND HDF,
             TD_ADJ
            WHERE HDF.L_FUND_ID = ${ztbh}
          AND HDF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE GROUP BY HDF.L_FUND_ID,
            HDF.L_TRADE_DATE
    ),
     PD_ASSETS AS (
        -- 产品资产信息
        SELECT FRD.L_FUND_ID,
               FRD.L_TRADE_DATE,
               FRD.EN_FUND_ASSET_NET,
               FRD.EN_FUND_ASSET_NET_PRE +
                   -- 买入项(数据库中为负数，则为累加项)
               FRD.EN_APPLY_BAL + FRD.EN_APPLY_DIRECT + FRD.EN_APPEND_BAL +
                   -- 卖出项
               FRD.EN_REDEEM_BAL + FRD.EN_REDEEM_DIRECT + FRD.EN_EXTRACT_BAL +
                   -- 其他：分红、分红再投、业绩报酬
               FRD.EN_FUND_DIVIDEND + FRD.EN_FUND_DIVIDEND_INVEST +
               FRD.EN_INCOME_REWARD FUND_ASSETS_NET_PRE
        FROM ZHFX.TFUNDRETURNDETAIL FRD,
             TD_ADJ
        WHERE FRD.L_FUND_ID = ${ztbh}
          AND FRD.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
    ),
     TRADE_FLOW_BUSIN_TYPE AS (
         -- 交易流水
         SELECT TFF.L_FUND_ID,
                TFF.L_TRADE_DATE,
                TFF.VC_WIND_CODE,
                MFD.VC_STOCK_NAME,
                TFF.VC_BUSIN_TYPE,
                -- 单元：万
                EN_LIQUIDATE_BALANCE / 1e4                    TRADE_BALANCE,
                -- 单元：元
                EN_FEE_TRADE                                    FEE_BALACE,
                DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL, EN_LIQUIDATE_BALANCE / PA.FUND_ASSETS_NET_PRE) TRADE_RATIO,
                DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL, EN_FEE_TRADE / PA.FUND_ASSETS_NET_PRE)         FEE_RATIO
         FROM ZHFX.TTRADEFLOWFUND TFF
                  LEFT JOIN PD_ASSETS PA
                            ON TFF.L_FUND_ID = PA.L_FUND_ID
                                AND TFF.L_TRADE_DATE = PA.L_TRADE_DATE
                  LEFT JOIN ZHFX.TMUTUALFUNDDESCRIPTION MFD
                            ON TFF.VC_WIND_CODE = MFD.VC_WIND_CODE,
              TD_ADJ
         WHERE TFF.L_FUND_ID = ${ztbh}
           AND TFF.VC_BUSIN_TYPE IN ('买入', '卖出', '转换_转入',
                                     '转换_转出', '申购', '赎回')
           AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
         ORDER BY TFF.L_FUND_ID,
                  TFF.L_TRADE_DATE,
                  TFF.VC_WIND_CODE,
                  TRADE_RATIO)
SELECT TFBT.*
FROM TRADE_FLOW_BUSIN_TYPE TFBT
--      ,TD_ADJ
-- WHERE L_TRADE_DATE = TD_ADJ.END_DATE
