import sys
import json
import time

import pendulum
import requests
from elasticsearch import NotFoundError, Elasticsearch
from elasticsearch.helpers import scan
from pathlib import Path
from config import QUARTER_CODES, REB_REGION_CODES, ELASTIC_USER, ELASTIC_PASSWORD, ELASTICSEARCH_URL, \
    ELASTIC_CERTFILE_FINGERPRINT
import logging
import coloredlogs

logfmt = "%(asctime)s %(levelname)10s %(message)s"
coloredlogs.install(fmt=logfmt)

logging.basicConfig(
    level=logging.DEBUG,
    format=logfmt,
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logging.Formatter.converter = time.gmtime
logger = logging.getLogger()

# load_dotenv()

elastic_session = requests.Session()
elastic_session.auth = (ELASTIC_USER, ELASTIC_PASSWORD)
# elastic_session.verify = ELASTIC_CERTFILE
elastic_session.verify = False

esclient = Elasticsearch(
    ELASTICSEARCH_URL,
    # ca_certs=ELASTIC_CERTFILE,
    ssl_assert_fingerprint=ELASTIC_CERTFILE_FINGERPRINT,
    basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
)


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


def reb_load_region_codes(filepath):
    if len(REB_REGION_CODES) > 0:
        return REB_REGION_CODES

    with open(filepath) as fd:
        for line in fd.readlines():
            tokens = line.split()
            if len(tokens) != 2:
                continue
            REB_REGION_CODES[tokens[0]] = tokens[1]

    return REB_REGION_CODES


def query_index_imported(index_name):
    #
    # low-level api client
    #
    # r = elastic_session.get(ELASTICSEARCH_URL + '/corp_code/_count')
    # if r.status_code == requests.codes.ok:
    #     rj = r.json()
    #     logger.info(f"corp_code has {rj['count']} records")
    #     return rj['count']
    # elif r.status_code == 404:
    #     logger.info(f'{r.status_code} : maybe index is not created.')
    #     return 0
    # else:
    #     logger.error(f'{r.status_code}')
    #     r.raise_for_status()
    resp = esclient.search(index=index_name, query={"match_all": {}})
    return resp['hits']['total']['value']


def get_time_frame(doc) -> dict:
    # doc["time_frame"]: {
    #   "gte": "2015-10-31 12:00:00",
    #   "lte": "2015-11-01"
    # pass
    qc = doc['reprt_code']
    y = int(doc['bsns_year'])
    month = QUARTER_CODES.index(qc) * 3 + 1
    tf = {
        'gte': pendulum.datetime(y, month, 1).start_of('month').start_of('day').in_tz('Asia/Seoul').to_iso8601_string(),
        'lte': pendulum.datetime(y, month + 2, 1).end_of('month').end_of('day').in_tz('Asia/Seoul').to_iso8601_string()
    }
    doc.update({'time_frame': tf})
    return doc


def delete_documents(client, indices):
    try:
        for ind in indices:
            client.delete_by_query(index=ind, query={"match_all": {}})
    except NotFoundError:
        pass
