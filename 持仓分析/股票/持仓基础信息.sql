WITH STK_INFO AS (
    -- 股票信息表
    SELECT
        -- 基础信息
        HDS.L_FUND_ID,
        HDS.VC_WIND_CODE,
        HDS.L_TRADE_DATE,

        -- 持仓信息
        HDS.EN_VALUE_MARKET - NVL(HDS_IPO.EN_VALUE_MARKET, 0) STK_MKT
    FROM ZHFX.THOLDINGDETAILSHARE HDS
             LEFT JOIN ZHFX.THOLDINGDETAILSHAREIPO HDS_IPO
                       ON HDS.L_FUND_ID = HDS_IPO.L_FUND_ID AND HDS.VC_WIND_CODE = HDS_IPO.VC_WIND_CODE AND
                          HDS.L_TRADE_DATE = HDS_IPO.L_TRADE_DATE
    WHERE HDS.L_FUND_ID = ${fund_id}
      AND HDS.L_TRADE_DATE BETWEEN ${begin_date}
        AND ${end_date}
      AND HDS.EN_VALUE_MARKET - NVL(HDS_IPO.EN_VALUE_MARKET
        , 0)
        > 0
      --剔除优先股
      AND SUBSTR(HDS.VC_WIND_CODE
              , 1
              , 3) != '360')
SELECT STK_INFO.L_FUND_ID,
       STK_INFO.L_TRADE_DATE,
       -- 持股数量
       STK_INFO.STK_NUM,
       -- 持仓市值
       STK_INFO.STK_MV,
       -- 总仓位
       STK_INFO.STK_MV / FS.EN_FUND_VALUE NET_POSITION
FROM (SELECT STK_INFO.L_FUND_ID,
             STK_INFO.L_TRADE_DATE,
             -- 持仓股票数量
             COUNT(1)              STK_NUM,
             -- 持仓市值
             SUM(STK_INFO.STK_MKT) STK_MV
      FROM STK_INFO
      GROUP BY STK_INFO.L_FUND_ID, STK_INFO.L_TRADE_DATE) STK_INFO
         LEFT JOIN ZHFX.TFUNDASSET FS
                   ON STK_INFO.L_FUND_ID = FS.L_FUND_ID AND STK_INFO.L_TRADE_DATE = FS.L_TRADE_DATE

ORDER BY STK_INFO.L_FUND_ID, STK_INFO.L_TRADE_DATE DESC;