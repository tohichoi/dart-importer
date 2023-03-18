import logging
import sys

from elasticsearch.helpers import scan

from config import QUARTER_CODES
from post_data import esclient


def query_corp_code_count(client) -> int:
    logging.disable(sys.maxsize)
    resp = client.count(index='corp_code')
    logging.disable(logging.NOTSET)

    return resp['count']


def query_corp_data(client, corp_code, year: int) -> list:
    # 연단위로만 체크하자
    # resp = es.search(index="test-index", query={"match_all": {}})
    # print("Got %d Hits:" % resp['hits']['total']['value'])
    # for hit in resp['hits']['hits']:
    #     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
    hits = []
    logging.disable(sys.maxsize)
    for qc in QUARTER_CODES:
        resp = client.search(index="corp_data", query={
            "bool": {
                "must": [
                    {"term": {"corp_code": corp_code}},
                    {"term": {"bsns_year": str(year)}},
                    {"term": {"reprt_code": qc}}
                ]
            }
        })
        hits.append(resp['hits']['total']['value'])
    logging.disable(logging.NOTSET)

    return hits


def query_corp_code_doc(client, corp_code):
    # r = elastic_session.get(ELASTICSEARCH_URL + '/corp_code/_search',
    #                          data={
    #                              "query": {
    #                                 "term": {
    #                                      "corp_code": corp_code
    #                                  }
    #                                 }
    #                              },
    #                          headers={'Content-Type': 'application/json'})
    logging.disable(sys.maxsize)
    resp = client.get(index="corp_code", id=corp_code)
    logging.disable(logging.NOTSET)

    return resp


def query_corp_code_list(client):
    its = scan(client, query={"query": {"match_all": {}}}, index="corp_code", scroll="360m")
    return [doc['_source']['corp_code'] for doc in its]
