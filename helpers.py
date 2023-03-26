import logging
import sys
import json
from elasticsearch.helpers import scan
from pathlib import Path
from config import QUARTER_CODES


def query_corp_code_count(client) -> int:
    logging.disable(sys.maxsize)
    resp = client.count(index='corp_code')
    logging.disable(logging.NOTSET)

    return resp['count']


def query_corp_quarter_doc(client, doc):
    query_fields = []
    fields = ["corp_code", "bsns_year", "reprt_code", 'rcept_no', 'account_id', 'thstrm_nm']
    for f in fields:
        query_fields.append({"term": {f: doc[f]}})
    resp = client.search(index="corp_data", query={
        "bool": {
            "must": query_fields
        }
    })
    return resp['hits']['total']['value'] > 0


def query_corp_quarter_data(client, corp_code, year: int, quarter: int) -> bool:
    # resp = es.search(index="test-index", query={"match_all": {}})
    # print("Got %d Hits:" % resp['hits']['total']['value'])
    # for hit in resp['hits']['hits']:
    #     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
    logging.disable(sys.maxsize)
    qcs = dict(zip(range(1, 5), QUARTER_CODES))
    resp = client.search(index="corp_data", query={
        "bool": {
            "must": [
                {"term": {"corp_code": corp_code}},
                {"term": {"bsns_year": str(year)}},
                {"term": {"reprt_code": qcs[quarter]}}
            ]
        }
    })
    logging.disable(logging.NOTSET)

    return resp['hits']['total']['value'] > 0


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
        hits.append(resp['hits']['total']['value'] > 0)
    logging.disable(logging.NOTSET)

    return hits


def query_corp_name(client, corp_code):
    resp = query_corp_code_doc(client, corp_code)
    return resp['_source']['corp_name']


def query_corp_info_doc(client, corp_code):
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
    resp = client.search(index="corp_info", query={
        "match": {
            "corp_code": {
                "query": corp_code
            }
        }
    })
    logging.disable(logging.NOTSET)

    return resp


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


def is_valid_dart_result_file(file):
    d = dict()
    if type(file) == str:
        p = Path(file)
        d = json.loads(p.read_text())
    elif isinstance(file, Path):
        d = json.loads(file.read_text())
    elif type(file) == dict:
        d = file
    else:
        raise ValueError(f'Invalid data: {str(file)}')

    return ('status' in d) and (d['status'].strip() in ['000', '013'])
