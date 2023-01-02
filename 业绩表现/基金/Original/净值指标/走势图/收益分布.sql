WITH
    -- 常量
    CONSTANTS AS (
        -- 常量值
        SELECT 10 GROUP_NUM
        FROM DUAL),
    -- 交易日调整
    TRADE_DATE_ADJ AS (
        -- 交易日调整（包含起始日当天的收益，则需要考虑T-1的数据）
        SELECT MIN(L_TRADE_DATE_LAST) BEGIN_DATE_LAST,
               -- 已处理当天收益的开始日期
               MIN(L_TRADE_DATE)      BEGIN_DATE,
               MAX(L_TRADE_DATE)      END_DATE
        FROM ZHFX.TCALENDAR
        WHERE L_DATE = L_TRADE_DATE
          AND L_TRADE_DATE BETWEEN TO_CHAR(TO_DATE(${startdate}, 'yyyy-mm-dd'), 'yyyymmdd')
            AND TO_CHAR(TO_DATE(${enddate}, 'yyyy-mm-dd'), 'yyyymmdd')),
    FOF_PFL_PROFIT AS (
        -- FOF组合收益
        SELECT HOLD_INFO.*,
               DECODE(HOLD_INFO.FOF_MV_INIT, 0, 0,
                      HOLD_INFO.DAY_PF / HOLD_INFO.FOF_MV_INIT) DAY_PF_RATIO
        FROM (SELECT HDF.L_FUND_ID FUND_ID,
                     HDF.L_TRADE_DATE TRADE_DATE,
                     SUM(HDF.EN_MARKET_INIT) FOF_MV_INIT,
                     SUM(HDF.EN_PROFIT) DAY_PF
              FROM ZHFX.THOLDINGDETAILFUND HDF,
                   TRADE_DATE_ADJ ADJ
              WHERE HDF.L_FUND_ID = ${ztbh}
                AND HDF.L_TRADE_DATE BETWEEN ADJ.BEGIN_DATE AND ADJ.END_DATE
              GROUP BY HDF.L_FUND_ID, HDF.L_TRADE_DATE) HOLD_INFO
        ORDER BY HOLD_INFO.TRADE_DATE),
    PROFIT_BORDER AS (
        -- 日收益率边界
        SELECT PFL_PF.FUND_ID,
               MIN(PFL_PF.DAY_PF_RATIO) MIN_PF_RATIO,
               MAX(PFL_PF.DAY_PF_RATIO) MAX_PF_RATIO
        FROM FOF_PFL_PROFIT PFL_PF
        GROUP BY PFL_PF.FUND_ID),
    PROFIT_BORDER_GROUP AS (
        -- 边界分组
        SELECT PF_BD.FUND_ID,
               GROUP_INFO.GROUP_NO,
               PF_BD.MIN_PF_RATIO +
               (PF_BD.MAX_PF_RATIO - PF_BD.MIN_PF_RATIO) / CONS.GROUP_NUM * (GROUP_INFO.GROUP_NO - 1) GROUP_MIN,
               PF_BD.MIN_PF_RATIO +
               (PF_BD.MAX_PF_RATIO - PF_BD.MIN_PF_RATIO) / CONS.GROUP_NUM * GROUP_INFO.GROUP_NO       GROUP_MAX
        FROM PROFIT_BORDER PF_BD,
             (SELECT ROWNUM GROUP_NO
              FROM DUAL,
                   CONSTANTS CONS
              CONNECT BY ROWNUM <= CONS.GROUP_NUM) GROUP_INFO,
             CONSTANTS CONS)
SELECT PF_BD_GROUP.FUND_ID,
       PF_BD_GROUP.GROUP_NO,
       PF_BD_GROUP.GROUP_MIN,
       PF_BD_GROUP.GROUP_MAX,
       TO_CHAR(PF_BD_GROUP.GROUP_MIN * 100, 'FM990.00') || '~' || TO_CHAR(PF_BD_GROUP.GROUP_MAX * 100, 'FM990.00') ||
       '%'                                      RANGE,
       SUM(CASE
               WHEN PFL_PF.DAY_PF_RATIO >= PF_BD_GROUP.GROUP_MIN AND PFL_PF.DAY_PF_RATIO < PF_BD_GROUP.GROUP_MAX THEN 1
               ELSE 0 END)                      TIMES,
       ROUND(SUM(CASE
                     WHEN PFL_PF.DAY_PF_RATIO >= PF_BD_GROUP.GROUP_MIN AND PFL_PF.DAY_PF_RATIO < PF_BD_GROUP.GROUP_MAX
                         THEN 1
                     ELSE 0 END) / COUNT(1), 4) TIMES_PRECENT
FROM PROFIT_BORDER_GROUP PF_BD_GROUP
         LEFT JOIN FOF_PFL_PROFIT PFL_PF
                   ON PF_BD_GROUP.FUND_ID = PFL_PF.FUND_ID
GROUP BY PF_BD_GROUP.FUND_ID,
         PF_BD_GROUP.GROUP_NO,
         PF_BD_GROUP.GROUP_MIN,
         PF_BD_GROUP.GROUP_MAX
ORDER BY PF_BD_GROUP.FUND_ID,
         PF_BD_GROUP.GROUP_NO
