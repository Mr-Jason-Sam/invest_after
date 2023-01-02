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
                       -- 国债期货ID
                       1232       CN_BOND_ID,
                       -- 正回购名称
                       '正回购'   REPO_NAME,
                       -- 逆回购名称
                       '逆回购'   REV_REPO_NAME,
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
     BOND_SPREAD_INFO AS (
         -- 国债变化信息
         SELECT SBI.*,
                SBI.YTM - (BC.EN_YIELD + (SBI.MATU - BC.EN_CURVE_TERM) / (BC.EN_CURVE_TERM_NEXT - BC.EN_CURVE_TERM) *
                                         (BC.EN_YIELD_NEXT - BC.EN_YIELD)) / 100 SPREAD
         FROM (SELECT SBI.*,
                      CASE
                          WHEN SBI.MATU < 0.25 THEN 0
                          WHEN SBI.MATU < 0.5 THEN 0.25
                          WHEN SBI.MATU < 1 THEN 0.5
                          WHEN SBI.MATU < 2 THEN 1
                          WHEN SBI.MATU < 3 THEN 2
                          WHEN SBI.MATU < 5 THEN 3
                          WHEN SBI.MATU < 7 THEN 5
                          WHEN SBI.MATU < 10 THEN 7
                          WHEN SBI.MATU < 20 THEN 10
                          WHEN SBI.MATU < 30 THEN 20
                          WHEN SBI.MATU < 50 THEN 30
                          END MATU_GROUP,
                      CONS.CN_BOND_ID
               FROM SELECTOR_BOND_INFO SBI,
                    CONSTANTS CONS) SBI
                  LEFT JOIN
              ZHFX.TBONDCURVE BC
              ON
                          BC.L_CURVE_NUMBER = SBI.CN_BOND_ID
                      AND BC.L_TRADE_DATE = SBI.TRADE_DATE
                      AND BC.EN_CURVE_TERM = SBI.MATU_GROUP),

     REPO_INFO AS (
         -- 回购信息
         SELECT FA.L_FUND_ID,
                FA.L_TRADE_DATE,
                SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0))     REPO_AMOUNT,
                CASE
                    WHEN SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0)) = 0 THEN 0
                    ELSE SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REPO_NAME,
                                    TF_HG.EN_DEAL_BALANCE * TF_HG.EN_RATE / 100, 0)) /
                         SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0))
                    END                                                                               REPO_RATE,
                SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REV_REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0)) REVREPO_AMOUNT,
                CASE
                    WHEN SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REV_REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0)) = 0
                        THEN 0
                    ELSE SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REV_REPO_NAME,
                                    TF_HG.EN_DEAL_BALANCE * TF_HG.EN_RATE / 100, 0)) /
                         SUM(DECODE(TF_HG.VC_ENTRUST_DIRECTION, CONS.REV_REPO_NAME, TF_HG.EN_DEAL_BALANCE, 0))
                    END                                                                               REVREPO_RATE
         FROM ZHFX.TFUNDASSET FA
                  LEFT JOIN
              ZHFX.TTRADEFLOWHG TF_HG
              ON
                          TF_HG.L_FUND_ID = FA.L_FUND_ID
                      AND TF_HG.L_HG_DATE <= FA.L_TRADE_DATE
                      AND DECODE(TF_HG.L_EXPIRE_DATE_ADVANCE, 0, TF_HG.L_EXPIRE_DATE, TF_HG.L_EXPIRE_DATE_ADVANCE) >
                          FA.L_TRADE_DATE,
              CONSTANTS CONS
         WHERE FA.L_FUND_ID IN (${fund_ids})
           AND FA.L_TRADE_DATE BETWEEN ${begin_date} AND ${end_date}
         GROUP BY FA.L_FUND_ID, FA.L_TRADE_DATE),
     BOND_TOP_CONFIG AS (
         -- 前N大债券
         SELECT BOND_BASE_INFO.FUND_ID,
                BOND_BASE_INFO.TRADE_DATE,
                SUM(CASE WHEN BOND_BASE_INFO.PD_POSI_RANK = 1 THEN BOND_BASE_INFO.PD_POSITION ELSE 0 END)   TOP_1_POSITION,
                SUM(CASE WHEN BOND_BASE_INFO.PD_POSI_RANK <= 5 THEN BOND_BASE_INFO.PD_POSITION ELSE 0 END)  TOP_5_POSITION,
                SUM(CASE WHEN BOND_BASE_INFO.PD_POSI_RANK <= 10 THEN BOND_BASE_INFO.PD_POSITION ELSE 0 END) TOP_10_POSITION,
                SUM(CASE WHEN BOND_BASE_INFO.PD_POSI_RANK <= 20 THEN BOND_BASE_INFO.PD_POSITION ELSE 0 END) TOP_20_POSITION
         FROM (SELECT BOND_INFO.FUND_ID,
                      BOND_INFO.TRADE_DATE,
                      BOND_INFO.PD_POSITION,
                      ROW_NUMBER() OVER (PARTITION BY BOND_INFO.FUND_ID, BOND_INFO.TRADE_DATE ORDER BY BOND_INFO.BOND_PFL_POSITION DESC) PD_POSI_RANK
               FROM BOND_INFO) BOND_BASE_INFO GROUP BY BOND_BASE_INFO.FUND_ID,
                BOND_BASE_INFO.TRADE_DATE),
     IND_TOP_CONFIG AS (
         -- 前N大主体
         SELECT FUND_ID,
                TRADE_DATE,
                SUM(CASE WHEN ISSUER_PD_RANK = 1 THEN ISSUER_PD_POSI ELSE 0 END)   TOP_1_POSITION,
                SUM(CASE WHEN ISSUER_PD_RANK <= 5 THEN ISSUER_PD_POSI ELSE 0 END)  TOP_5_POSITION,
                SUM(CASE WHEN ISSUER_PD_RANK <= 10 THEN ISSUER_PD_POSI ELSE 0 END) TOP_10_POSITION,
                SUM(CASE WHEN ISSUER_PD_RANK <= 20 THEN ISSUER_PD_POSI ELSE 0 END) TOP_20_POSITION
         FROM (SELECT FUND_ID,
                      TRADE_DATE,
                      ISSUER_PD_POSI,
                      ROW_NUMBER() OVER (PARTITION BY FUND_ID, TRADE_DATE ORDER BY ISSUER_PD_POSI DESC) ISSUER_PD_RANK
               FROM (SELECT FUND_ID,
                            TRADE_DATE,
                            ISSUER,
                            SUM(PD_POSITION) ISSUER_PD_POSI
                     FROM BOND_INFO
                     GROUP BY FUND_ID, TRADE_DATE, ISSUER)) GROUP BY FUND_ID, TRADE_DATE),
     PRODUCT_PFL_INFO AS (
         -- 产品债券组合信息
         SELECT BSI.FUND_ID,
                BSI.TRADE_DATE,
                -- 总仓位
                BSI.MV / BSI.NAV          POSITION,
                -- 剩余期限
                BSI.MATU,
                -- 产品久期
                BSI.MV_MODIDURA / BSI.NAV PRODUCT_MODIDURA,
                -- 债券组合久期
                BSI.MODIDURA,
                -- 债券组合YTM
                BSI.YTM,
                -- 产品YTM
                (BSI.MV_YTM - NVL(REPO_INFO.REPO_AMOUNT * REPO_INFO.REPO_RATE, 0) +
                 NVL(REPO_INFO.REVREPO_AMOUNT * REPO_INFO.REVREPO_RATE, 0)) /
                BSI.NAV                   PRODUCT_YTM,
                -- 信用利差
                BSI.SPREAD,
                -- 债券数量
                BSI.BOND_NUM,
                -- 正回购利率
                REPO_INFO.REPO_RATE,
                -- 逆回购理利率
                REPO_INFO.REVREPO_RATE,
                -- 票面利率
                BSI.COUPON_RATE,

                -- 城投债
                CTZ_VOL,
                CTZ_PD_POSITION,
                CTZ_BOND_PFL_POSITION,
                -- 永续债
                YXZ_VOL,
                YXZ_PD_POSITION,
                YXZ_BOND_PFL_POSITION,
                -- 二级资本债
                SECOND_CAPITAL_VOL,
                SECOND_CAPITAL_PD_POSITION,
                SECOND_CAPITAL_BOND_PFL_POSI,

                -- 前N大债券
                BTC.TOP_1_POSITION        BOND_TOP_1_POSI,
                BTC.TOP_5_POSITION        BOND_TOP_5_POSI,
                BTC.TOP_10_POSITION       BOND_TOP_10_POSI,
                BTC.TOP_20_POSITION       BOND_TOP_20_POSI,
                -- 前N大主体
                ITC.TOP_1_POSITION        IND_TOP_1_POSI,
                ITC.TOP_5_POSITION        IND_TOP_5_POSI,
                ITC.TOP_10_POSITION       IND_TOP_10_POSI,
                ITC.TOP_20_POSITION       IND_TOP_20_POSI

         FROM (SELECT BSI.FUND_ID,
                      BSI.TRADE_DATE,
                      MAX(BSI.NAV)                                                         NAV,
                      SUM(BSI.MV)                                                          MV,
                      SUM(CASE
                              WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                              ELSE BSI.MV * BSI.MODIDURA END)                              MV_MODIDURA,
                      SUM(BSI.MV * BSI.YTM)                                                MV_YTM,
                      DECODE(SUM(BSI.MV), 0, 0, SUM(BSI.MV * BSI.MATU) / SUM(BSI.MV))      MATU,
                      DECODE(SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END *
                                 BSI.MODIDURA) /
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END))                                     MODIDURA,
                      DECODE(SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END * BSI.YTM) /
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END))                                     YTM,
                      DECODE(SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END), 0, 0,
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END *
                                 BSI.SPREAD) /
                             SUM(CASE
                                     WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                     ELSE BSI.MV END))                                     SPREAD,
                      DECODE(
                              SUM(CASE
                                      WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                      ELSE BSI.PAR_VALUE END),
                              0, 0,
                              SUM(CASE
                                      WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                      ELSE BSI.PAR_VALUE * BSI.COUPON_RATE END) / SUM(
                                      CASE
                                          WHEN BSI.BONDTYPE1_WIND IN (CONS.TSF_BOND_NAME, CONS.CHANGE_BOND_NAME) THEN 0
                                          ELSE BSI.PAR_VALUE END))                         COUPON_RATE,
                      COUNT(1)                                                             BOND_NUM,

                      -- 城投债 数量、仓位、比例
                      SUM(DECODE(BSI.IS_CTZ, '是', BSI.VOL, 0))                            CTZ_VOL,
                      SUM(DECODE(BSI.IS_CTZ, '是', BSI.PD_POSITION, 0))                    CTZ_PD_POSITION,
                      SUM(DECODE(BSI.IS_CTZ, '是', BSI.BOND_PFL_POSITION, 0))              CTZ_BOND_PFL_POSITION,
                      -- 永续债 数量、仓位、比例
                      SUM(DECODE(BSI.IS_YXZ, '是', BSI.VOL, 0))                            YXZ_VOL,
                      SUM(DECODE(BSI.IS_YXZ, '是', BSI.PD_POSITION, 0))                    YXZ_PD_POSITION,
                      SUM(DECODE(BSI.IS_YXZ, '是', BSI.BOND_PFL_POSITION, 0))              YXZ_BOND_PFL_POSITION,
                      -- 二级资本债 数量、仓位、比例
                      SUM(DECODE(BSI.IS_2ND_CAPITAL_BOND, '是', BSI.VOL, 0))               SECOND_CAPITAL_VOL,
                      SUM(DECODE(BSI.IS_2ND_CAPITAL_BOND, '是', BSI.PD_POSITION, 0))       SECOND_CAPITAL_PD_POSITION,
                      SUM(DECODE(BSI.IS_2ND_CAPITAL_BOND, '是', BSI.BOND_PFL_POSITION, 0)) SECOND_CAPITAL_BOND_PFL_POSI
               FROM BOND_SPREAD_INFO BSI,
                    CONSTANTS CONS
               GROUP BY BSI.FUND_ID,
                        BSI.TRADE_DATE) BSI
                  LEFT JOIN
              REPO_INFO
              ON
                          REPO_INFO.L_FUND_ID = BSI.FUND_ID
                      AND REPO_INFO.L_TRADE_DATE = BSI.TRADE_DATE
                  LEFT JOIN BOND_TOP_CONFIG BTC ON BSI.FUND_ID = BTC.FUND_ID AND BSI.TRADE_DATE = BTC.TRADE_DATE
                  LEFT JOIN IND_TOP_CONFIG ITC ON BSI.FUND_ID = ITC.FUND_ID AND BSI.TRADE_DATE = ITC.TRADE_DATE)
SELECT *
FROM PRODUCT_PFL_INFO ORDER BY FUND_ID, TRADE_DATE DESC;