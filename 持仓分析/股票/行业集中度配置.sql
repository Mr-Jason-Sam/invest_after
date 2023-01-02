WITH CONSTANTS AS (
    -- 常量
    SELECT 'SW_1'       SW_1,
           'SW_2'       SW_2,
           'SW_3'       SW_3,
           'SEC_1'      SEC_1,
           'SEC_2'      SEC_2,
           'WIND_1'     WIND_1,
           'WIND_2'     WIND_2,
           'WIND_3'     WIND_3,
           'ZX_1'       ZX_1,
           'ZX_2'       ZX_2,
           'ZX_3'       ZX_3,

           '上交所'     SH_EXCHANGE_NAME,
           '深交所'     SZ_EXCHANGE_NAME,
           '港交所'     HK_EXCHANGE_NAME,
           'HK'         HK_EXCHANGE_CODE,
           '港股通'     HK_SH_SZ_MARKET_NAME,
           '其他'       OTHER,
           '60'         SH_MAIN_CODE_PREFIX,
           '00'         SZ_MAIN_CODE_PREFIX,
           '30'         GEM_CODE_PREFIX,
           '68'         STM_CODE_PREFIX,
           'A股-主板'   A_MAIN_BLOCK,
           'A股-创业板' A_GEM_BLOCK,
           'A股-科创板' A_STM_BLOCK,
           'HK-港交所'  HK_BLOCK,
           '360'        PREFERRED_STK_PREFIX,

           20           DEFAULT_STK_TOP_N,
           10           DEFAULT_INDUSTRY_TOP_N,

           -- 所有配置信息（除1以外的group by fund_id trade_date）
           'ALL'        "ALL",
           -- 所有个股基本信息
           1            ALL_STK,
           -- 个股集中度配置
           2            STK_CFG,
           -- 股票前N大信息
           3            STK_TOP_INFO,
           -- 行业集中度配置
           4            IDS_CFG,
           -- 行业前N大信息
           5            IDS_TOP_INFO,
           -- 市场板块配置
           6            INVEST_MARKET_CFG,
           -- 投资风格配置
           7            INVEST_STYLE_CFG,
           -- 申万复合板块配置
           8            SW_CP_CFG,
           -- 风格因子配置
           9            STYLE_FACTOR_CFG,
           -- PE配置
           10           PE_CFG,
           -- PB配置
           11           PB_CFG,
           -- PEG配置
           12           PEG_CFG
    FROM DUAL),
     STK_INFO AS (
         -- 股票信息表
         SELECT STK_BASE_INFO.*,
                -- 个股市值排名
                -- 总仓位 = 全价市值 / 资产净值
                DECODE(STK_BASE_INFO.NET_ASSETS, 0, 0,
                       STK_BASE_INFO.STK_MKT / STK_BASE_INFO.NET_ASSETS)                                                                  PD_POSITION,
                -- 股票组合仓位 =  全价市值 / 股票组合总资产
                DECODE(SUM(STK_BASE_INFO.STK_MKT)
                           OVER ( PARTITION BY STK_BASE_INFO.L_FUND_ID, STK_BASE_INFO.L_TRADE_DATE), 0, 0,
                       STK_BASE_INFO.STK_MKT / SUM(STK_BASE_INFO.STK_MKT)
                                                   OVER ( PARTITION BY STK_BASE_INFO.L_FUND_ID, STK_BASE_INFO.L_TRADE_DATE))              STK_PFL_POSITION,
                ROW_NUMBER() OVER ( PARTITION BY STK_BASE_INFO.L_FUND_ID, STK_BASE_INFO.L_TRADE_DATE ORDER BY STK_BASE_INFO.STK_MKT DESC) MARKET_RANK
         FROM (SELECT
                   -- 基础信息
                   HDS.L_FUND_ID,
                   HDS.VC_WIND_CODE,
                   HDS.L_TRADE_DATE,
                   SIS.VC_STOCK_NAME,

                   -- 行情信息
                   SIS.EN_PRICE_CLOSE,

                   -- 持仓信息
                   HDS.L_AMOUNT,
                   HDS.EN_VALUE_MARKET - NVL(HDS_IPO.EN_VALUE_MARKET, 0) STK_MKT,
                   -- 净资产
                   FS.EN_FUND_VALUE                                      NET_ASSETS,

                   -- 交易市场
                   SIS.VC_MARKET_TYPE,
                   -- 市场划分
                   CASE
                       WHEN SIS.VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) AND
                            REGEXP_LIKE(HDS.VC_WIND_CODE,
                                        '^(' || CONS.SZ_MAIN_CODE_PREFIX || '|' || CONS.SH_MAIN_CODE_PREFIX ||
                                        ')') THEN CONS.A_MAIN_BLOCK
                       WHEN SIS.VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) AND
                            REGEXP_LIKE(HDS.VC_WIND_CODE, '^(' || CONS.GEM_CODE_PREFIX || ')') THEN CONS.A_GEM_BLOCK
                       WHEN SIS.VC_MARKET_TYPE IN (CONS.SH_EXCHANGE_NAME, CONS.SZ_EXCHANGE_NAME) AND
                            REGEXP_LIKE(HDS.VC_WIND_CODE, '^(' || CONS.STM_CODE_PREFIX || ')') THEN CONS.A_STM_BLOCK
                       WHEN SIS.VC_MARKET_TYPE IN (CONS.HK_EXCHANGE_CODE, CONS.HK_EXCHANGE_NAME) THEN CONS.HK_BLOCK
                       ELSE CONS.OTHER
                       END                                               INVEST_MARKET,

                   -- 基本面信息
                   SIS.EN_VAL_PE,
                   SIS.EN_VAL_PB,
                   SIS.EN_VAL_PEG_EST,
                   SIS.EN_VAL_ROE,

                   -- 行业
                   SIS.VC_INDUSTRY_SW_FIRST,
                   SIS.VC_INDUSTRY_SW_SECOND,
                   SIS.VC_INDUSTRY_SW_THIRD,
                   SIS.VC_INDUSTRY_SEC_FIRST,
                   SIS.VC_INDUSTRY_SEC_SECOND,
                   SIS.VC_INDUSTRY_ZX_FIRST,
                   SIS.VC_INDUSTRY_ZX_SECOND,
                   SIS.VC_INDUSTRY_ZX_THIRD,
                   SIS.VC_INDUSTRY_WIND_FIRST,
                   SIS.VC_INDUSTRY_WIND_SECOND,
                   SIS.VC_INDUSTRY_WIND_THIRD,
                   -- 复合型SW行业
                   CASE
                       WHEN VC_INDUSTRY_SW_FIRST IN
                            ('纺织服饰', '家用电器', '美容护理', '农林牧渔', '轻工制造', '商贸零售', '社会服务',
                             '食品饮料')
                           THEN '申万-消费'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('医药生物') THEN '申万-医药'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('电力设备', '公用事业', '机械设备', '建筑装饰', '汽车', '综合')
                           THEN '申万-中游制造'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('传媒', '电子', '计算机', '通信') THEN '申万-TMT'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('环保', '基础化工', '建筑材料', '交通运输') THEN '申万-周期'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('国防军工') THEN '申万-军工'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('钢铁', '煤炭', '石油石化', '有色金属') THEN '申万-资源'
                       WHEN VC_INDUSTRY_SW_FIRST IN ('房地产', '非银金融', '银行') THEN '申万-金融地产'
                       ELSE '其他'
                       END                                               SW_COMPLEX,
                   -- 选择行业，默认申万一级
                   DECODE(
                           ${industry_id},
                           CONS.SW_1, SIS.VC_INDUSTRY_SW_FIRST,
                           CONS.SW_2, SIS.VC_INDUSTRY_SW_SECOND,
                           CONS.SW_3, SIS.VC_INDUSTRY_SW_THIRD,
                           CONS.SEC_1, SIS.VC_INDUSTRY_SEC_FIRST,
                           CONS.SEC_2, SIS.VC_INDUSTRY_SEC_SECOND,
                           CONS.WIND_1, SIS.VC_INDUSTRY_WIND_FIRST,
                           CONS.WIND_2, SIS.VC_INDUSTRY_WIND_SECOND,
                           CONS.WIND_3, SIS.VC_INDUSTRY_WIND_THIRD,
                           SIS.VC_INDUSTRY_SW_FIRST
                       )                                                 SELECT_INDUSTRY,

                   -- 投资风格
                   SIS.VC_INVEST_TYPE
               FROM ZHFX.THOLDINGDETAILSHARE HDS
                        LEFT JOIN ZHFX.THOLDINGDETAILSHAREIPO HDS_IPO
                                  ON HDS.L_FUND_ID = HDS_IPO.L_FUND_ID AND HDS.VC_WIND_CODE = HDS_IPO.VC_WIND_CODE AND
                                     HDS.L_TRADE_DATE = HDS_IPO.L_TRADE_DATE
                        LEFT JOIN ZHFX.TFUNDASSET FS
                                  ON HDS.L_FUND_ID = FS.L_FUND_ID AND HDS.L_TRADE_DATE = FS.L_TRADE_DATE
                        LEFT JOIN ZHFX.TSTOCKINFOSHARE SIS
                                  ON HDS.VC_WIND_CODE = SIS.VC_WIND_CODE AND HDS.L_TRADE_DATE = SIS.L_TRADE_DATE,
                    CONSTANTS CONS
               WHERE HDS.L_FUND_ID = ${fund_id}
                 AND HDS.L_TRADE_DATE BETWEEN ${begin_date}
                   AND ${end_date}
                 -- TODO 待确认
                 AND HDS.EN_VALUE_MARKET - NVL(HDS_IPO.EN_VALUE_MARKET
                   , 0)
                   > 0
                 --剔除优先股
                 AND SUBSTR(HDS.VC_WIND_CODE
                         , 1
                         , 3) != CONS.PREFERRED_STK_PREFIX) STK_BASE_INFO),
     INDUSTRY_CONFIG AS (
         -- 行业配置
         SELECT IDS_BASE_CFG.*,
                ROW_NUMBER() OVER (PARTITION BY L_FUND_ID, L_TRADE_DATE ORDER BY STK_MKT DESC) INDUSTRY_RANK
         FROM (SELECT STK_INFO.L_FUND_ID,
                      STK_INFO.L_TRADE_DATE,
                      STK_INFO.SELECT_INDUSTRY,
                      SUM(STK_INFO.PD_POSITION)      AS PD_POSITION,
                      SUM(STK_INFO.STK_MKT)          AS STK_MKT,
                      SUM(STK_INFO.STK_PFL_POSITION) AS STK_PFL_POSITION
               FROM STK_INFO
               GROUP BY STK_INFO.L_FUND_ID, STK_INFO.L_TRADE_DATE, SELECT_INDUSTRY) IDS_BASE_CFG),
     INDUSTRY_TOP_INFO AS (
         -- 行业集中度
         SELECT L_FUND_ID,
                L_TRADE_DATE,
                SUM(CASE WHEN INDUSTRY_RANK = 1 THEN PD_POSITION END)   IDS_PD_POSI_TOP_1,
                SUM(CASE WHEN INDUSTRY_RANK <= 5 THEN PD_POSITION END)  IDS_PD_POSI_TOP_5,
                SUM(CASE WHEN INDUSTRY_RANK <= 10 THEN PD_POSITION END) IDS_PD_POSI_TOP_10,
                SUM(CASE WHEN INDUSTRY_RANK <= 20 THEN PD_POSITION END) IDS_PD_POSI_TOP_20,
                SUM(CASE WHEN INDUSTRY_RANK = 1 THEN STK_PFL_POSITION END)   IDS_PFL_POSI_TOP_1,
                SUM(CASE WHEN INDUSTRY_RANK <= 5 THEN STK_PFL_POSITION END)  IDS_PFL_POSI_TOP_5,
                SUM(CASE WHEN INDUSTRY_RANK <= 10 THEN STK_PFL_POSITION END) IDS_PFL_POSI_TOP_10,
                SUM(CASE WHEN INDUSTRY_RANK <= 20 THEN STK_PFL_POSITION END) IDS_PFL_POSI_TOP_20
         FROM INDUSTRY_CONFIG
         GROUP BY L_FUND_ID, L_TRADE_DATE)
SELECT *
FROM INDUSTRY_TOP_INFO ORDER BY L_FUND_ID, L_TRADE_DATE DESC;