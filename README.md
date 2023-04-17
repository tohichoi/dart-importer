# DART and REB data importer

## DART

Data Analysis, Retrieval and Transfer System of FINANCIAL SUPERVISORY SERVICE of Korea(DART) provides 

## REB

Korea Real Estate Board(https://www.reb.or.kr/rebEng/main.do) provides the following data with REST API.
  
  - Official Notification of Real Estate Prices
  - Real Estate Survey Statistics
  - Compensation-Consignment
  - Subscription Work
  - Submission of Explanatory Materials for Actual Transaction Scrutiny

## What it does?

### filings to meaningful data, visualization, investing idea 

  - Open DART([https://opendart.fss.or.kr/](https://englishdart.fss.or.kr/)) data to ElasticSearch
  - Corporation code
  - Report on Business Performance according to Consolidated Financial Statements (Fair Disclosure)

### importing realestate data(https://www.reb.or.kr/reb/main.do)

  - getRealEstateTradingCount: 조사일자, 지역코드, 거래유형 값을 이용하여 부동산 거래 건수 정보를 제공
  - getRealEstateTradingCountBuildType: 건물유형별 부동산 거래 건수 조회
  - getAptRealTradingPriceIndex: 공동주택 실거래가격지수 통계 조회 서비스
  - getAptRealTradingPriceIndexSize: 공동주택 실거래가격지수 통계 조회 서비스

### visualize 

  - kibana

![Screenshot from 2023-04-17 17-31-32-2](https://user-images.githubusercontent.com/10368601/232430492-f926e1d8-0af5-4b0e-917a-e568d66d7804.png)
![Screenshot from 2023-04-17 17-32-04](https://user-images.githubusercontent.com/10368601/232430516-751be8a9-239d-495c-9760-8623b3ed009f.png)
