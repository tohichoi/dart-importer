# elastic search

https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-cli-run-dev-mode


```shell
docker pull docker.elastic.co/elasticsearch/elasticsearch:8.6.2

docker network create elastic

docker rm elasticsearch  
sudo su -c 'echo 262144 > "/proc/sys/vm/max_map_count"'

export ELASTIC_BOOTSTRAP_PASSWORD_FILE="./elastic-bootstrap-password.txt"
docker run --name elasticsearch --rm -e ES_JAVA_OPTS="-Xms1g -Xmx1g" --net elastic -p 9200:9200 -v ./config/elastic:/usr/share/elasticsearch/config  -v ./elastic-bootstrap-password.txt:/run/secrets/elastic-bootstrap-password.txt -e ELASTIC_PASSWORD_FILE=/run/secrets/elastic-bootstrap-password.txt -it docker.elastic.co/elasticsearch/elasticsearch:8.6.2
```

## OUTPUT

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Elasticsearch security features have been automatically configured!
✅ Authentication is enabled and cluster connections are encrypted.

ℹ️  Password for the elastic user (reset with `bin/elasticsearch-reset-password -u elastic`):
  5mM39VykAuhzbAt8R9Lu

ℹ️  HTTP CA certificate SHA-256 fingerprint:
  899a9a19078a064b24c0b2c535f33ba24fc6f93a04fb4fb1c143548493ac118b

ℹ️  Configure Kibana to use this cluster:
• Run Kibana and click the configuration link in the terminal when Kibana starts.
• Copy the following enrollment token and paste it into Kibana in your browser (valid for the next 30 minutes):
  eyJ2ZXIiOiI4LjYuMiIsImFkciI6WyIxNzIuMjMuMC4yOjkyMDAiXSwiZmdyIjoiODk5YTlhMTkwNzhhMDY0YjI0YzBiMmM1MzVmMzNiYTI0ZmM2ZjkzYTA0ZmI0ZmIxYzE0MzU0ODQ5M2FjMTE4YiIsImtleSI6IktKNTMwWVlCSkk2MXNrS2liYk5MOmtuT1ZIWWJYUUxLd3ZMem1TQUF6SHcifQ==

ℹ️ Configure other nodes to join this cluster:
• Copy the following enrollment token and start new Elasticsearch nodes with `bin/elasticsearch --enrollment-token <token>` (valid for the next 30 minutes):
  eyJ2ZXIiOiI4LjYuMiIsImFkciI6WyIxNzIuMjMuMC4yOjkyMDAiXSwiZmdyIjoiODk5YTlhMTkwNzhhMDY0YjI0YzBiMmM1MzVmMzNiYTI0ZmM2ZjkzYTA0ZmI0ZmIxYzE0MzU0ODQ5M2FjMTE4YiIsImtleSI6IktwNTMwWVlCSkk2MXNrS2liYk5NOk12eklXUUdyU21pZTE1MDgxYTYwelEifQ==

  If you're running in Docker, copy the enrollment token and run:
  `docker run -e "ENROLLMENT_TOKEN=<token>" docker.elastic.co/elasticsearch/elasticsearch:8.6.2`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

```shell
mkdir config/elastic

docker cp elasticsearch:/usr/share/elasticsearch/config/certs/http_ca.crt config/elastic/cert

export ELASTIC_USER=elastic
# 암호 새로 만들기
export ELASTIC_PASSWORD="$(cat elastic-bootstrap-password.txt)"
# docker exec -it elasticsearch bin/elasticsearch-reset-password -u elastic -s -b
# export ELASTIC_PASSWORD="0TZuAlIONcxx1EVcqZQI"
export ELASTIC_CERT="config/elastic/certs/ca/ca.crt"
export CURL_CA_BUNDLE=$ELASTIC_CERT
export ELASTIC_SERVER=https://localhost:9200

# Open a new terminal and verify that you can connect to your Elasticsearch cluster
curl -u "$ELASTIC_USER:$ELASTIC_PASSWORD" $ELASTIC_SERVER
```

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD -X POST "$ELASTIC_SERVER/customer/_doc/1?pretty" -H 'Content-Type: application/json' -d'
{
  "firstname": "Jennifer",
  "lastname": "Walters"
}
'
```

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD -X GET "$ELASTIC_SERVER/customer/_doc/1?pretty"
```

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD  -X PUT "$ELASTIC_SERVER/customer/_bulk?pretty" -H 'Content-Type: application/json' -d'
{ "create": { } }
{ "firstname": "Monica","lastname":"Rambeau"}
{ "create": { } }
{ "firstname": "Carol","lastname":"Danvers"}
{ "create": { } }
{ "firstname": "Wanda","lastname":"Maximoff"}
{ "create": { } }
{ "firstname": "Jennifer","lastname":"Takeda"}
'
```

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD -X GET "$ELASTIC_SERVER/customer/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query" : {
    "match" : { "firstname": "Jennifer" }
  }
}
'
```


# Kibana

```shell
docker pull docker.elastic.co/kibana/kibana:8.6.2

docker run --name kib-01 --rm --net elastic -p 5601:5601 -v ./config/kibana/kibana.yml:/usr/share/kibana/config/kibana.yml docker.elastic.co/kibana/kibana:8.6.2
```

goto http://0.0.0.0:5601/?code=212856

```shell
# token 다시 생성
docker-compose exec elastic /usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token -s kibana
```

# kibana 로그인

elastic 
ufK5rMb-sG_QSCHwVm8W

# https://www.elastic.co/guide/en/elasticsearch/reference/current/term-level-queries.html

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD -X GET "$ELASTIC_SERVER/corp_code/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query" : {
    "match" : { "corp_name": "삼성" }
  }
}
'
```

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD -X GET "$ELASTIC_SERVER/corp_code/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query" : {
    "wildcard": {
      "corp_name": {
        "value": "삼성전자*",
        "boost": 1.0,
        "rewrite": "constant_score"
      }
    }
  }
}
'
```

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD -X GET "$ELASTIC_SERVER/corp_code/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query" : {
    "wildcard": {
      "corp_name": {
        "value": "삼성전자*",
        "boost": 1.0,
        "rewrite": "constant_score"
      }
    }
  }
}
'
```

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD -X GET "$ELASTIC_SERVER/corp_code/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query" : {
    "fuzzy": {
      "corp_name": {
        "value": "삼성",
        "boost": 1.0,
        "rewrite": "constant_score"
      }
    }
  }
}
'
```

## delete all

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD -XPOST '$ELASTIC_SERVER/corp_code/_delete_by_query?conflicts=proceed&pretty' -H 'Content-Type: application/json' -d'
{
    "query": {
        "match_all": {}
    }
}'
```

## count

```shell
curl -u $ELASTIC_USER:$ELASTIC_PASSWORD -XGET '$ELASTIC_SERVER/corp_code/_count'
```
