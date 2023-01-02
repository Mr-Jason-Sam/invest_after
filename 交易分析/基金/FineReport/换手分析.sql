WITH TD_ADJ AS (
    -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
    SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
           -- 已处理当天收益的开始日期
           MIN(L_TRADE_DATE)      BEGIN_DATE,
           MAX(L_TRADE_DATE)      END_DATE
    FROM ZHFX.TCALENDAR
    WHERE L_DATE = L_TRADE_DATE
      AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE('${startdate}', 'yyyy-mm-dd'), 'yyyymmdd')
        AND TO_CHAR(TO_DATE('${enddate}', 'yyyy-mm-dd'), 'yyyymmdd')),
     TRADE_DATES AS (SELECT CLD.L_TRADE_DATE
                     FROM ZHFX.TCALENDAR CLD,
                          TD_ADJ
                     WHERE L_DATE = L_TRADE_DATE
                       AND CLD.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE),
     TRADE_FLOW_TD AS (SELECT MIN(TFF.L_TRADE_DATE) TF_BEGIN, MAX(TFF.L_TRADE_DATE) TF_END
                       FROM ZHFX.TTRADEFLOWFUND TFF,
                            TD_ADJ
                       WHERE TFF.L_FUND_ID = ${ztbh}
                         AND TFF.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE),
    FOF_ASSETS AS (
        -- 基金组合资产
        SELECT
            HDF.L_FUND_ID,
            HDF.L_TRADE_DATE,
            SUM(HDF.EN_MARKET_INIT) FOF_MV_INIT,
            SUM(HDF.EN_VALUE_MARKET) FOF_MV
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
               FRD.EN_INCOME_REWARD FUND_ASSETS_NET_PRE,
            FRD.EN_INCOME_REWARD                        EQUITY,

                      -- T日可用净申赎： 申购额 - 赎回额（直销 + 专户）
                      (FRD.EN_APPLY_DIRECT + FRD.EN_APPEND_BAL) -
                      (FRD.EN_REDEEM_DIRECT + FRD.EN_EXTRACT_BAL) APPLY_REDEEM_NET,
                      -- T日可用申购额
                      FRD.EN_APPLY_DIRECT + FRD.EN_APPEND_BAL     APPLY_ASSTES,
                      -- T+1可以用净申赎：申购额 - 赎回额（代销）
                      FRD.EN_APPLY_BAL - FRD.EN_REDEEM_BAL        APPLY_REDEEM_NET_NEXT
        FROM ZHFX.TFUNDRETURNDETAIL FRD,
             TD_ADJ
        WHERE FRD.L_FUND_ID = ${ztbh}
          AND FRD.L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
    ),
     TRADE_FLOW_BASE AS (
         -- 交易流水
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('买入', '转换_转入', '申购') THEN EN_LIQUIDATE_BALANCE
                        ELSE 0 END) BUY_BALANCE,
                SUM(CASE
                        WHEN VC_BUSIN_TYPE IN ('卖出', '转换_转出', '赎回') THEN EN_LIQUIDATE_BALANCE
                        ELSE 0 END) SELL_BALANCE,
                SUM(EN_FEE_TRADE)   FEE
         FROM ZHFX.TTRADEFLOWFUND,
              TD_ADJ
         WHERE L_FUND_ID = ${ztbh}
           AND L_TRADE_DATE BETWEEN TD_ADJ.BEGIN_DATE AND TD_ADJ.END_DATE
         GROUP BY L_FUND_ID, L_TRADE_DATE),
     TURNOVER_ANAYSIS AS (
         -- 换手分析
         SELECT PA.L_FUND_ID,
                PA.L_TRADE_DATE,
                -- 组合换手率
                DECODE(
                        FA.FOF_MV_INIT,
                        0, NULL,
                        (NVL(TFB.BUY_BALANCE, 0) + NVL(TFB.SELL_BALANCE, 0)) /
                        (FA.FOF_MV_INIT + GREATEST(NVL(TFB.BUY_BALANCE, 0) - NVL(TFB.SELL_BALANCE, 0), 0))
                    )                                                FOF_TURNOVER_RATIO,
                -- 产品换手率
                DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL, (NVL(TFB.BUY_BALANCE, 0) + NVL(TFB.SELL_BALANCE, 0)) /
                (PA.FUND_ASSETS_NET_PRE + GREATEST(PA.EQUITY, 0)))
                    PD_TURNOVER_RATIO,
                -- 产品主动换手率（资产变动影响）
             DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL , (NVL(TFB.BUY_BALANCE, 0) + NVL(TFB.SELL_BALANCE, 0) -
                 ABS(PA.EQUITY) * FA.FOF_MV_INIT / PA.FUND_ASSETS_NET_PRE) /
                (PA.FUND_ASSETS_NET_PRE + GREATEST(PA.EQUITY, 0)))
                    PD_ACTIVE_ASSETS_TR,
                -- 产品主动换手率（日可用资产影响）
             DECODE(PA.FUND_ASSETS_NET_PRE + PA.APPLY_ASSTES, 0, NULL, CASE
                    -- T日剩余可用金额
                    -- 1、T日净申赎 - ABS(LEAST(T+1日净申赎, 0)) >= 0 => T日资金剩余被动买入
                    WHEN GREATEST(PA.APPLY_REDEEM_NET, 0) - ABS(LEAST(PA.APPLY_REDEEM_NET_NEXT, 0)) >= 0 THEN
                        (GREATEST(NVL(TFB.BUY_BALANCE, 0) -
                                  (GREATEST(PA.APPLY_REDEEM_NET, 0) - ABS(LEAST(PA.APPLY_REDEEM_NET_NEXT, 0))), 0) +
                         NVL(TFB.SELL_BALANCE, 0))
                    -- 2、T日净申赎 > 0 AND T日净申赎 - ABS(LEAST(T+1日净申赎, 0)) < 0 => T日被动卖出，预留T+1赎回金额
                    WHEN ABS(LEAST(PA.APPLY_REDEEM_NET_NEXT, 0)) - GREATEST(PA.APPLY_REDEEM_NET, 0) >= 0 THEN
                        (GREATEST(NVL(TFB.SELL_BALANCE, 0) -
                                  (ABS(LEAST(PA.APPLY_REDEEM_NET_NEXT, 0)) - GREATEST(PA.APPLY_REDEEM_NET, 0)), 0) +
                         NVL(TFB.BUY_BALANCE, 0))
                    ELSE (NVL(TFB.SELL_BALANCE, 0) + NVL(TFB.BUY_BALANCE, 0))
                    END / (PA.FUND_ASSETS_NET_PRE + PA.APPLY_ASSTES))
                 PD_ACTIVE_USE_TR,
                -- 交易费用
                NVL(TFB.FEE, 0)                                      TRADE_FEE,
                -- 交易金额
                NVL(TFB.BUY_BALANCE + TFB.SELL_BALANCE, 0)           TRADE_BALANCE,
                -- 交易成本
                DECODE(PA.FUND_ASSETS_NET_PRE, 0, NULL, NVL(TFB.FEE, 0) / PA.FUND_ASSETS_NET_PRE)            TRADE_FEE_PD_RATIO
         FROM PD_ASSETS PA
             LEFT JOIN FOF_ASSETS FA
                 ON PA.L_FUND_ID = FA.L_FUND_ID
                        AND PA.L_TRADE_DATE = FA.L_TRADE_DATE
                  LEFT JOIN TRADE_FLOW_BASE TFB
                            ON PA.L_FUND_ID = TFB.L_FUND_ID
                                AND PA.L_TRADE_DATE = TFB.L_TRADE_DATE),
     TURNOVER_RESULT AS (
         -- 换手率分析结果
         SELECT TA.L_FUND_ID,
                TA.L_TRADE_DATE,
                -- 产品换手率
                TA.PD_TURNOVER_RATIO,
                -- 组合换手率
                TA.FOF_TURNOVER_RATIO,
                -- 产品主动换手率（资产层面）
                TA.PD_ACTIVE_ASSETS_TR,
                -- 产品主动换手率（可用资金层面）
                TA.PD_ACTIVE_USE_TR,
                -- 交易金额
                TA.TRADE_BALANCE,
                -- 交易费用
                TA.TRADE_FEE,
                -- 费用比例（产品）
                TA.TRADE_FEE_PD_RATIO,
                -- 累计
                COUNT(TA.L_TRADE_DATE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         SAMPLE,
                MIN(TA.L_TRADE_DATE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         MIN_TRADE_DATE,
                MAX(TA.L_TRADE_DATE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         MAX_TRADE_DATE,
                -- 累计产品换手率
                SUM(TA.PD_TURNOVER_RATIO) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         PD_TR_ACC,
                -- 累计基金组合换手率
                SUM(TA.FOF_TURNOVER_RATIO) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         FOF_TR_ACC,
                -- 累计主动产品换手率（资产层面）
                SUM(TA.PD_ACTIVE_ASSETS_TR) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         PD_ACTIVE_ASSETS_TR_ACC,
                -- 累计主动产品换手率（可用资金层面）
                SUM(TA.PD_ACTIVE_USE_TR) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         PD_ACTIVE_USE_TR_ACC,
                -- 累计交易费率
                SUM(TA.TRADE_FEE_PD_RATIO) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         TRADE_FEE_PD_RATIO_ACC,
                -- 累计交易费
                SUM(TA.TRADE_FEE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)         TRADE_FEE_ACC,
                -- 平均交易费率
                DECODE(SUM(TA.TRADE_BALANCE) OVER ( PARTITION BY
                    TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE),
                       0, NULL,
                       SUM(TA.TRADE_FEE) OVER ( PARTITION BY
                           TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE) /
                       SUM(TA.TRADE_BALANCE) OVER ( PARTITION BY
                           TA.L_FUND_ID ORDER BY TA.L_TRADE_DATE)) AVG_FEE_RATIO
         FROM TURNOVER_ANAYSIS TA),
     TURNOVER_END_DATE AS (SELECT MAX(L_TRADE_DATE) MAX_TD
                           FROM TURNOVER_RESULT)

-- 换手率
SELECT TR.*,
       -- 年化累计产品换手率
       TR.PD_TR_ACC * (244 / SAMPLE)               PD_TR_ANN,
       -- 年化累计基金组合换手率
       TR.FOF_TR_ACC * (244 / SAMPLE)              FOF_TR_ANN,
       -- 年化累计主动产品换手率（资金层）
       TR.PD_ACTIVE_ASSETS_TR_ACC * (244 / SAMPLE) PD_ACTIVE_ASSETS_TR_ANN,
       -- 年化累计产品换手率（可用资金）
       TR.PD_ACTIVE_USE_TR_ACC * (244 / SAMPLE)    PD_ACTIVE_USE_TR_ANN,
       --年化交易费率
       TR.TRADE_FEE_PD_RATIO_ACC * (244 / SAMPLE)  TRADE_FEE_PD_RATIO_ANN
FROM TURNOVER_RESULT TR
--    , TURNOVER_END_DATE RED
-- WHERE TR.L_TRADE_DATE = RED.MAX_TD