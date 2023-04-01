# API

https://www.reb.or.kr/r-one/main.do


# 건물유형별 부동산 거래 건수 조회 상세기능 명세


## url

https://api.odcloud.kr/api/RealEstateTradingSvc/v1/getRealEstateTradingCountBuildType

## query

- research_date
- region_cd
- deal_obj
  - (Equal) 거래유형코드 (01:토지, 03:건축물, 04:주택, 06:주택매매)

```python
    {
    "ALL_CNT": 24492,
    # 주거용_소계_건수
    "live_sum_cnt": 333,
    "BULD_USE11_CNT": 913,
    "BULD_USE12_CNT": 402,
    "BULD_USE13_CNT": 6733,
    "BULD_USE14_CNT": 680,
    "BULD_USE15_CNT": 8723,
    "BULD_USE20_CNT": 4991,
    "BULD_USE21_CNT": 0,
    "BULD_USE22_CNT": 0,
    "BULD_USE30_CNT": 623,
    "BULD_USE40_CNT": 405,
    "BULD_USE50_CNT": 1022,
    "DEAL_OBJ": "01",
    "LEVEL_NO": "0",
    "LIVE_SUM_CNT": 17451,
    "REGION_CD": "11000",
    "REGION_NM": "서울",
    "RESEARCH_DATE": "202102"
}
```


# 공동주택 실거래가격지수 통계 조회 서비스

https://www.data.go.kr/iim/api/selectAPIAcountView.do#/API%20%EB%AA%A9%EB%A1%9D/getgetAptRealTradingPriceIndex

