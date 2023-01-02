WITH CONSTANTS AS (SELECT
                       -- 中债中证回售行权
                       1          CN_BOND_SEC_PUT_BACK_EXE,
                       -- 中债回售行权
                       2          CN_BOND_PUT_BACK_EXE,
                       -- 中债推荐
                       3          CN_BOND_RCM,
                       -- 中债中证推荐
                       4          CN_BOND_SEC_RCM,
                       0.0001     ONE_BP,
                       'WIND_1'   WIND_1,
                       'WIND_2'   WIND_2,
                       -- 可转债
                       '可转债'   TSF_BOND_NAME,
                       -- 可交换债
                       '可交换债' CHANGE_BOND_NAME
                   FROM DUAL),
     BOND_INFO AS (
         -- 债券基本信息
         SELECT BOND_BASE_INFO.*,
                BOND_BASE_INFO.MV / NAV                                                                                 PD_POSITION,
                DECODE(
                        SUM(BOND_BASE_INFO.MV) OVER ( PARTITION BY BOND_BASE_INFO.FUND_ID, BOND_BASE_INFO.TRADE_DATE),
                        0, 0,
                        BOND_BASE_INFO.MV / SUM(BOND_BASE_INFO.MV)
                                                OVER ( PARTITION BY BOND_BASE_INFO.FUND_ID, BOND_BASE_INFO.TRADE_DATE)) BOND_PFL_POSITION
         FROM (
                  -- 债券基础信息
                  SELECT HDB.L_FUND_ID                                                 FUND_ID,
                         HDB.L_TRADE_DATE                                              TRADE_DATE,
                         FA.EN_FUND_VALUE                                              NAV,
                         HDB.VC_WIND_CODE                                              WINDCODE,
                         HDB.VC_STOCK_NAME                                             SEC_NAME,
                         HDB.L_AMOUNT                                                  VOL,
                         HDB.EN_VALUE_MARKET + HDB.EN_INTEREST_ACCUMULATED             MV,
                         BD.VC_BOND_TYPE1_WIND                                         BONDTYPE1_WIND,
                         BD.VC_BOND_TYPE2_WIND                                         BONDTYPE2_WIND,
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('国债', '央行票据') OR BD.VC_BOND_TYPE2_WIND = '政策银行债'
                                 THEN '利率债'
                             WHEN BD.VC_BOND_TYPE1_WIND = '地方政府债' THEN '地方政府债'
                             WHEN BD.VC_BOND_TYPE1_WIND = '同业存单' THEN '同业存单'
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债') THEN '可转债可交债'
                             WHEN BD.VC_BOND_TYPE2_WIND IN ('商业银行债', '商业银行次级债券') AND BD.VC_IS_YXZ = '是'
                                 THEN '商业银行永续债'
                             WHEN BD.VC_BOND_TYPE2_WIND = '商业银行次级债券' THEN '商业银行次级债'
                             WHEN BD.VC_BOND_TYPE2_WIND = '证券公司债' AND BD.VC_IS_SUBORD = '次级债' THEN '证券公司次级债'
                             WHEN BD.VC_BOND_TYPE1_WIND = '金融债' THEN '其他金融债'
                             WHEN UCI.VC_ISSUER_NAME IS NOT NULL AND BD.VC_IS_YXZ = '是' THEN '城投永续'
                             WHEN UCI.VC_ISSUER_NAME IS NOT NULL AND BD.VC_ISSUE_TYPE IN ('私募', '定向') THEN '城投非公开'
                             WHEN UCI.VC_ISSUER_NAME IS NOT NULL THEN '其他城投'
                             WHEN BD.VC_IS_YXZ = '是' THEN '产业永续'
                             WHEN BD.VC_ISSUE_TYPE IN ('私募', '定向') THEN '产业非公开'
                             ELSE '其他产业'
                             END                                                       BONDTYPE1,
                         BD.VC_IS_YXZ                                                  IS_YXZ,
                         DECODE(UCI.VC_ISSUER_NAME, NULL, '否', '是')                  IS_CTZ,
                         DECODE(BD.VC_BOND_TYPE2_WIND, '商业银行次级债券', '是', '否') IS_2ND_CAPITAL_BOND,
                         BD.VC_COMP_NAME                                               ISSUER,
                         NVL(BD.VC_PROVINCE, '其他')                                   PROVINCE,
                         NVL(BD.VC_COMP_IND2_WIND, '其他')                             IND2_WIND,
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('国债', '央行票据') OR BD.VC_BOND_TYPE2_WIND = '政策银行债'
                                 THEN '利率债'
                             ELSE COALESCE(BI.VC_ISSUER_RATING, BI.VC_BOND_RATING, '无评级')
                             END                                                       ISSUER_RATING, --主体评级，无主体评级取债项评级
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('国债', '央行票据') OR BD.VC_BOND_TYPE2_WIND = '政策银行债'
                                 THEN '利率债'
                             ELSE COALESCE(BI.VC_BOND_RATING, BI.VC_ISSUER_RATING, '无评级')
                             END                                                       BOND_RATING,   --债项评级，无债项评级取主体评级
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('国债', '央行票据') OR BD.VC_BOND_TYPE2_WIND = '政策银行债'
                                 THEN '利率债'
                             ELSE NVL(BI.VC_BOND_RATING_CNBD, '无评级')
                             END                                                       RATING_CNBD,   --中债隐含评级
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('国债', '央行票据') OR BD.VC_BOND_TYPE2_WIND = '政策银行债'
                                 THEN '利率债'
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('同业存单', '短期融资券')
                                 THEN COALESCE(BI.VC_BOND_RATING_INTER, BI.VC_ISSUER_RATING_INTER, '无评级')
                             ELSE COALESCE(BI.VC_BOND_RATING_INTER, '无评级')
                             END                                                       RATING_CJ,     --创金内部评级
                         BI.EN_COUPONRATE                                              COUPON_RATE,
                         BI.EN_PAR * HDB.L_AMOUNT                                      PAR_VALUE,
                         CASE
                             WHEN INSTR(BD.VC_SP_PROVISION, '回售') > 0 THEN COALESCE(BI.EN_MATU_CNBD_IFEXE,
                                                                                      BI.EN_MATU_CNBD,
                                                                                      BI.EN_MATU_WIND_IFEXE,
                                                                                      BI.EN_MATU_WIND, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MATU_CNBD_IFEXE, BI.EN_MATU_CNBD),
                                           BI.EN_MATU_WIND, 0)
                             END                                                       MATU1,         --中债回售行权
                         COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MATU_CNBD_IFEXE, BI.EN_MATU_CNBD), BI.EN_MATU_WIND,
                                  0)                                                   MATU2,         --中债推荐

                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债') THEN 0
                             WHEN REGEXP_LIKE(BD.VC_SP_PROVISION, '回售') THEN COALESCE(BI.EN_MODIDURA_CNBD_IFEXE,
                                                                                        BI.EN_MODIDURA_CNBD,
                                                                                        BI.EN_MODIDURA_CSI_IFEXE,
                                                                                        BI.EN_MODIDURA_CSI,
                                                                                        BI.EN_MATU_WIND_IFEXE * 0.95,
                                                                                        BI.EN_MATU_WIND * 0.95, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                           BI.EN_MATU_WIND * 0.95, 0)
                             END                                                       MODIDURA1,     --中债回售行权
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债') THEN 0
                             WHEN BD.VC_MARKET != '银行间' AND REGEXP_LIKE(BD.VC_SP_PROVISION, '回售') THEN COALESCE(
                                     BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI, BI.EN_MODIDURA_CNBD_IFEXE,
                                     BI.EN_MODIDURA_CNBD, BI.EN_MATU_WIND_IFEXE * 0.95, BI.EN_MATU_WIND * 0.95, 0)
                             WHEN BD.VC_MARKET != '银行间' THEN COALESCE(
                                     DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                     DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                     BI.EN_MATU_WIND * 0.95, 0)
                             WHEN REGEXP_LIKE(BD.VC_SP_PROVISION, '回售') THEN COALESCE(BI.EN_MODIDURA_CNBD_IFEXE,
                                                                                        BI.EN_MODIDURA_CNBD,
                                                                                        BI.EN_MODIDURA_CSI_IFEXE,
                                                                                        BI.EN_MODIDURA_CSI,
                                                                                        BI.EN_MATU_WIND_IFEXE * 0.95,
                                                                                        BI.EN_MATU_WIND * 0.95, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                           BI.EN_MATU_WIND * 0.95, 0)
                             END                                                       MODIDURA2,     --中债中证回售行权
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债') THEN 0
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                           BI.EN_MATU_WIND * 0.95, 0)
                             END                                                       MODIDURA3,     --中债推荐
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债') THEN 0
                             WHEN BD.VC_MARKET != '银行间' THEN COALESCE(
                                     DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                     DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                     BI.EN_MATU_WIND * 0.95, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_MODIDURA_CNBD_IFEXE, BI.EN_MODIDURA_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_MODIDURA_CSI_IFEXE, BI.EN_MODIDURA_CSI),
                                           BI.EN_MATU_WIND * 0.95, 0)
                             END                                                       MODIDURA4,     --中债中证推荐
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债') THEN 0
                             WHEN REGEXP_LIKE(BD.VC_SP_PROVISION, '回售') THEN COALESCE(BI.EN_YTM_CNBD_IFEXE,
                                                                                        BI.EN_YTM_CNBD,
                                                                                        BI.EN_YTM_CSI_IFEXE,
                                                                                        BI.EN_YTM_CSI,
                                                                                        BI.EN_COUPONRATE, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                           BI.EN_COUPONRATE, 0)
                             END                                                       YTM1,          --中债回售行权
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债') THEN 0
                             WHEN BD.VC_MARKET != '银行间' AND REGEXP_LIKE(BD.VC_SP_PROVISION, '回售') THEN COALESCE(
                                     BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD,
                                     BI.EN_COUPONRATE, 0)
                             WHEN BD.VC_MARKET != '银行间' THEN COALESCE(
                                     DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                     DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                     BI.EN_COUPONRATE, 0)
                             WHEN REGEXP_LIKE(BD.VC_SP_PROVISION, '回售') THEN COALESCE(BI.EN_YTM_CNBD_IFEXE,
                                                                                        BI.EN_YTM_CNBD,
                                                                                        BI.EN_YTM_CSI_IFEXE,
                                                                                        BI.EN_YTM_CSI,
                                                                                        BI.EN_COUPONRATE, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                           BI.EN_COUPONRATE, 0)
                             END                                                       YTM2,          --中债中证回售行权
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债') THEN 0
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                           BI.EN_COUPONRATE, 0)
                             END                                                       YTM3,          --中债推荐
                         CASE
                             WHEN BD.VC_BOND_TYPE1_WIND IN ('可转债', '可交换债') THEN 0
                             WHEN BD.VC_MARKET != '银行间' THEN COALESCE(
                                     DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                     DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                     BI.EN_COUPONRATE, 0)
                             ELSE COALESCE(DECODE(BI.L_RECOM_CNBD, 1, BI.EN_YTM_CNBD_IFEXE, BI.EN_YTM_CNBD),
                                           DECODE(BI.L_RECOM_CSI, 1, BI.EN_YTM_CSI_IFEXE, BI.EN_YTM_CSI),
                                           BI.EN_COUPONRATE, 0)
                             END                                                       YTM4           --中债中证推荐
                  FROM ZHFX.THOLDINGDETAILBOND HDB
                           LEFT JOIN
                       ZHFX.TFUNDASSET FA
                       ON
                                   FA.L_FUND_ID = HDB.L_FUND_ID
                               AND FA.L_TRADE_DATE = HDB.L_TRADE_DATE
                           LEFT JOIN
                       ZHFX.TBONDDESCRIPTION BD
                       ON
                           BD.VC_WIND_CODE = HDB.VC_WIND_CODE
                           LEFT JOIN
                       ZHFX.TBONDINFO BI
                       ON
                                   BI.VC_WIND_CODE = HDB.VC_WIND_CODE
                               AND BI.L_TRADE_DATE = HDB.L_TRADE_DATE
                           LEFT JOIN
                       ZHFX.TUCIBONDISSUERCJ UCI
                       ON
                           UCI.VC_ISSUER_NAME = BD.VC_COMP_NAME
                  WHERE HDB.L_FUND_ID IN (${fund_ids})
                    AND HDB.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
                    AND HDB.L_AMOUNT != 0) BOND_BASE_INFO),
     SELECTOR_BOND_INFO AS (
         -- 选择修正久期、剩余久期、到期收益率的数据
         SELECT BASE_INFO.*,
                -- DVBP （Duration Value BP） = 修正久期 * 全价市值 * 1BP = 修正久期 * 全价市值 * 0.0001
                BASE_INFO.MODIDURA * MV * CONS.ONE_BP DVBP
         FROM (SELECT BOND_INFO.*,
                      DECODE(
                              ${bond_type},
                              CONS.WIND_1, BOND_INFO.BONDTYPE1_WIND,
                              CONS.WIND_2, BOND_INFO.BONDTYPE2_WIND,
                              BOND_INFO.BONDTYPE1
                          ) BOND_TYPE,
                      DECODE(
                              ${modidura_type},
                          -- 中债中证回售行权
                              CONS.CN_BOND_SEC_PUT_BACK_EXE, BOND_INFO.MATU1,
                          -- 中债推荐
                              CONS.CN_BOND_RCM, BOND_INFO.MATU2,
                          -- 中债中证推荐
                              CONS.CN_BOND_SEC_RCM, BOND_INFO.MATU2,
                              BOND_INFO.MATU1
                          ) MATU,
                      DECODE(
                              ${modidura_type},
                          -- 中债中证回售行权
                              CONS.CN_BOND_SEC_PUT_BACK_EXE, BOND_INFO.MODIDURA2,
                          -- 中债推荐
                              CONS.CN_BOND_RCM, BOND_INFO.MODIDURA3,
                          -- 中债中证推荐
                              CONS.CN_BOND_SEC_RCM, BOND_INFO.MODIDURA4,
                              BOND_INFO.MODIDURA1
                          ) MODIDURA,
                      DECODE(
                              ${modidura_type},
                          -- 中债中证回售行权
                              CONS.CN_BOND_SEC_PUT_BACK_EXE, BOND_INFO.YTM2,
                          -- 中债推荐
                              CONS.CN_BOND_RCM, BOND_INFO.YTM3,
                          -- 中债中证推荐
                              CONS.CN_BOND_SEC_RCM, BOND_INFO.YTM4,
                              BOND_INFO.YTM1
                          ) YTM
               FROM BOND_INFO,
                    CONSTANTS CONS) BASE_INFO,
              CONSTANTS CONS),
     BOND_TYPE_CONFIG AS (
         -- 到期收益率配置
         SELECT GROUP_CONF.*,
                GROUP_CONF.BTG_MV * GROUP_CONF.BTG_MODIDURA * CONS.ONE_BP YG_DVBP
         FROM (SELECT SBI.FUND_ID,
                      SBI.TRADE_DATE,
                      SBI.BOND_TYPE,
                      SUM(SBI.MV)                      BTG_MV,
                      SUM(SBI.PD_POSITION)             BTG_PD_POSI,
                      SUM(SBI.BOND_PFL_POSITION)       BTG_BOND_PFL_POSI,
                      DECODE(SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END *
                                 SBI.MODIDURA) /
                             SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END)) BTG_MODIDURA,
                      DECODE(SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END * SBI.YTM) /
                             SUM(CASE
                                     WHEN SBI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE SBI.MV END)) BTG_YTM

               FROM SELECTOR_BOND_INFO SBI,
                    CONSTANTS CONS
               GROUP BY SBI.FUND_ID, SBI.TRADE_DATE, SBI.BOND_TYPE) GROUP_CONF,
              CONSTANTS CONS)
SELECT *
FROM BOND_TYPE_CONFIG;