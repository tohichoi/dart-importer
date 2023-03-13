# 이용현황

## login

```
export DARTID="tohichoi@naver.com"
curl -u "$DARTID:$(cat dart-password.txt)" -c dart-cookie.txt https://opendart.fss.or.kr/uat/uia/egovLoginUsr.do
```

```
export DARTID="tohichoi@naver.com"
curl -X POST -d "id=$DARTID" -d "password=$(cat dart-password.txt)" -c dart-cookie.txt https://opendart.fss.or.kr/uat/uia/egovLoginUsr.do
```


```
curl -X POST -u "$DARTID:$(cat dart-password.txt)" -b dart-cookie.txt https://opendart.fss.or.kr/mng/apiUsageStatusView.do \
-d "userSeq=00076704" \
-d "apiKey=2af8772970052740c7eba5e5b4add00b5bdc8842" \
-d "adLimitAt=N" \
-d "creatDt=2023-03-13"
```
