#!/usr/bin/env python

import json
import zipfile
from pprint import pprint
import sys
import pendulum
import requests
from bs4 import BeautifulSoup
import time
import logging
import coloredlogs
from tqdm import tqdm
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import streaming_bulk

from config import DART_RESULT_DIR, ELASTIC_USER, ELASTIC_PASSWORD, ELASTIC_CERTFILE_FINGERPRINT, \
    ELASTICSEARCH_URL, QUARTER_CODES

logfmt = "%(asctime)s %(levelname)s %(message)s"
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


def parse_corp_code(filename):
    """

    Args:
        filename: zipfile path

    Returns:

    """
    logger.info(f'Parsing {filename}')
    zf = zipfile.ZipFile(filename)
    soup = BeautifulSoup(zf.read('CORPCODE.xml'), features="xml")
    return [t for t in soup.result.find_all('list') if len(t.stock_code.text.strip()) > 0]


def generate_corp_code_doc(code_info_list):
    """corp code generator
    
        refer to fetch_corp_code()

    Args:
        code_info_list (BeautifulSoup tag): _description_

    Yields:
        dict: _description_
    """
    for ci in code_info_list:
        # code_info[ci.corp_name.text]
        doc = {
            '_id': ci.corp_code.text.strip(),
            'corp_code': ci.corp_code.text.strip(),
            'corp_name': ci.corp_name.text.strip(),
            'stock_code': ci.stock_code.text.strip(),
            'modify_date': ci.modify_date.text.strip()
        }

        # 주식코드가 없는 회사는 빼자
        # parse_corp_code() 에서 체크(데이터 수 계산할 때 부정확)
        # if len(doc['stock_code']) < 1:
        #     continue

        yield doc


def parse_corp_code_OLD(filename, do_post):
    with open(filename) as fd:
        logger.info(f'Parsing {filename}')
        soup = BeautifulSoup(fd.read(), features="xml")
    code_info_list = soup.result.find_all('list')
    code_info = dict()
    post_body_data = ''
    with tqdm(total=len(code_info_list), desc='Parsing') as pbar:
        for ci in code_info_list:
            # code_info[ci.corp_name.text]
            d = {
                'corp_code': ci.corp_code.text,
                'corp_name': ci.corp_name.text,
                'stock_code': ci.stock_code.text,
                'modify_date': ci.modify_date.text
            }

            if do_post:
                post_body_data += '{ "create": {} }\n'
                post_body_data += json.dumps(d) + '\n'

            pbar.update(1)

    if do_post:
        r = elastic_session.post(ELASTICSEARCH_URL + '/corp_code/_bulk?pretty',
                                 data=post_body_data,
                                 headers={'Content-Type': 'application/json'})
        if r.status_code == requests.codes.ok:
            logger.info('Posting OK')
            nimported = query_corp_code_imported()
            logger.info(f'Posted data : {nimported}')
        else:
            logger.error(f'{r.status_code}')
            r.raise_for_status()


# 고유번호


def analyze_corp_info(data):
    # "account_id": "ifrs-full_ComprehensiveIncome",
    # "account_nm": "총포괄손익",
    for ydata in data:
        for d in ydata['list']:
            if d['account_nm'] == '총포괄손익':
                pprint(d)


def query_corp_code_imported():
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
    resp = esclient.search(index="corp_code", query={"match_all": {}})
    return resp['hits']['total']['value']


# low-level api client
# https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/examples.html
# def delete_all_data():
#     r = elastic_session.get(
#         ELASTICSEARCH_URL + '/corp_code/_delete_by_query?conflicts=proceed&pretty',
#         data={
#             "query": {
#                 "match_all": {}
#             }
#         })


# https://github.com/elastic/elasticsearch-py/blob/main/examples/bulk-ingest
def create_index(client, indices):
    """Creates an index in Elasticsearch if one isn't already there."""

    # field types
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
    # migrating to 8
    # https://www.elastic.co/guide/en/elasticsearch/client/python-api/master/migration.html
    # body deprecation
    # https://stackoverflow.com/questions/71577892/how-change-the-syntax-in-elasticsearch-8-where-body-parameter-is-deprecated
    if 'corp_code' in indices:
        client.options(ignore_status=400).indices.create(
            index="corp_code",
            settings={"number_of_shards": 1},
            mappings={
                "properties": {
                    "corp_code": {"type": "search_as_you_type"},
                    "corp_name": {"type": "search_as_you_type"},
                    "stock_code": {"type": "text"},
                    "modify_date": {
                        "type": "date",
                        "format": "yyyyMMdd"}
                }
            },
            # ignore
            # >It’s good to know: Use an ignore parameter with the one or more status codes you want to overlook when you want to avoid raising an exception.
            # Troubleshooting the “400 Resource-Already-Exists” error message
            # If you try to create an index name(indices.create) that has already been created, the RequestError(400, 'resource_already_exists_exception)' will appear.
        )

    if 'corp_data' in indices:
        # https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020
        # "rcept_no": "20220516001751",1
        # "reprt_code": "11013",
        # "bsns_year": "2022",
        # "corp_code": "00126380",
        # "sj_div": "BS",
        # "sj_nm": "재무상태표",
        # "account_id": "ifrs-full_CurrentLiabilities",
        # "account_nm": "유동부채",
        # "account_detail": "-",
        # "thstrm_nm": "제 54 기 1분기말",
        # "thstrm_amount": "56799776000000",
        # "frmtrm_nm": "제 53 기말",
        # "frmtrm_amount": "53067303000000",
        # "ord": "20",
        # "currency": "KRW"
        client.options(ignore_status=400).indices.create(
            index="corp_data",
            settings={"number_of_shards": 1},
            mappings={
                "properties": {
                    # 접수번호
                    "rcept_no": {"type": "text"},
                    # 보고서 코드
                    "reprt_code": {"type": "text"},
                    # 사업 연도
                    "bsns_year": {"type": "date", "format": "yyyy"},
                    # 고유번호
                    "corp_code": {"type": "search_as_you_type"},
                    # 재무제표구분
                    # BS : 재무상태표 IS : 손익계산서 CIS : 포괄손익계산서 CF : 현금흐름표 SCE : 자본변동표
                    "sj_div": {"type": "search_as_you_type"},
                    # 재무제표명
                    "sj_nm": {"type": "search_as_you_type"},
                    # 계정ID
                    # XBRL 표준계정ID ※ 표준계정ID가 아닐경우 ""-표준계정코드 미사용-"" 표시
                    "account_id": {"type": "text"},
                    # 계정명
                    "account_nm": {"type": "search_as_you_type"},
                    # 계정상세
                    # ※ 자본변동표에만 출력 ex) 계정 상세명칭 예시 - 자본 [member]|지배기업 소유주지분 - 자본 [member]|지배기업 소유주지분|기타포괄손익누계액 [member]
                    "account_detail": {"type": "text"},
                    # 당기명
                    "thstrm_nm": {"type": "text"},
                    # 당기금액
                    # 9,999,999,999 ※ 분/반기 보고서이면서 (포괄)손익계산서 일 경우 [3개월] 금액
                    "thstrm_amount": {"type": "long"},
                    # 당기누적금액
                    "thstrm_add_amount": {"type": "long"},
                    # 전기명
                    "frmtrm_nm": {"type": "text"},
                    # 전기금액
                    "frmtrm_amount": {"type": "long"},
                    # 전기명(분/반기)
                    "frmtrm_q_nm": {"type": "text"},
                    # 전기금액(분/반기)
                    # ※ 분/반기 보고서이면서 (포괄)손익계산서 일 경우 [3개월] 금액
                    "frmtrm_q_amount": {"type": "long"},
                    # 전기누적금액
                    "frmtrm_add_amount": {"type": "long"},
                    # 전전기명
                    "bfefrmtrm_nm": {"type": "text"},
                    # 전전기금액
                    "bfefrmtrm_amount": {"type": "long"},
                    # 계정과목 정렬순서
                    "ord": {"type": "integer"},
                    # 통화 단위
                    "currency": {"type": "text"},
                    # 기간
                    "time_frame": {
                        "type": "date_range",
                        # https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
                        "format": "strict_date_optional_time_nanos"
                    },
                }
            },
        )

    client.options(ignore_status=400).indices.create(
        index="corp_import_history",
        settings={"number_of_shards": 1},
        mappings={
            "properties": {
                "corp_code": {"type": "search_as_you_type"},
                "year": {"type": "date", "format": "yyyy"},
                "reprt_code": {"type": "text"},
                "created_time": {
                    "type": "date",
                    # https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
                    "format": "strict_date_optional_time_nanos"
                },
                "number_of_imported_documents": {
                    "type": "integer"
                }
            }
        },
        # ignore
        # >It’s good to know: Use an ignore parameter with the one or more status codes you want to overlook when you want to avoid raising an exception.
        # Troubleshooting the “400 Resource-Already-Exists” error message
        # If you try to create an index name(indices.create) that has already been created, the RequestError(400, 'resource_already_exists_exception)' will appear.
    )


def delete_documents(client, indices):
    try:
        if 'corp_code' in indices:
            client.delete_by_query(index='corp_code', query={"match_all": {}})
        if 'corp_data' in indices:
            client.delete_by_query(index='corp_data', query={"match_all": {}})
    except NotFoundError:
        pass


def get_fetched_docs():
    """이미 fetch된 doc 리스트 구하기
    
        - 9만개 이상의 corp_code가 존재함
        - 전부 한 디렉토리에 넣어야 하나? => not effective
    """
    pass


# def post_quarter_corp_data_bulk(client, data: dict) -> int:
#     if type(data) != dict:
#         logger.error(f'Data type is not dict : {str(type(data))}')
#         return -1
#
#     if data['status'] != '000':
#         logger.error(f'Status code is {data["status"]}({data["message"]})')
#         return -1
#
#     docs = data['list']
#     number_of_docs = len(docs)
#     progress = tqdm(unit="docs", total=number_of_docs)
#     successes = 0
#     logging.disable(sys.maxsize)
#     for ok, action in streaming_bulk(
#             client=client, index="corp_data", actions=(d for d in docs),
#     ):
#         progress.update(1)
#         successes += ok
#     logging.disable(logging.NOTSET)
#     # print("Indexed %d/%d documents" % (successes, number_of_docs))
#
#     return successes


def post_quarter_corp_data_history(client, corp_code, qdata: dict, successes):
    docs = qdata['list']

    '''
                    "corp_code": {"type": "search_as_you_type"},
                    "year": {"type": "date", "format": "yyyy"},
                    "reprt_code": {"type": "text"},
                    "created_time": {
                        "type": "date",
                        # https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
                        "format": "strict_date_optional_time_nanos"
                    },
                    "number_of_imported_documents": {
                        "type": "integer"
                    }
    '''
    history = {
        'corp_code': corp_code,
        'year': qdata['list'][0]['bsns_year']
    }


def _get_time_frame(doc) -> dict:
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


def post_quarter_corp_data(client, corp_code, qdata: dict) -> int:
    if type(qdata) != dict:
        logger.error(f'Data type is not dict : {str(type(qdata))}')
        return -1

    if qdata['status'] != '000':
        logger.error(f'Status code is {qdata["status"]}({qdata["message"]})')
        return -1

    docs = qdata['list']
    number_of_docs = len(docs)
    progress = tqdm(unit="docs", total=number_of_docs)
    successes = 0
    logging.disable(sys.maxsize)
    for doc in docs:
        # create 는 id 필요. index 는 불필요
        doc = _get_time_frame(doc)
        resp = client.index(index="corp_data", document=doc)
        # http status 200 or 201
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/201
        if resp['result'] == 'created':
            successes += 1
        progress.update(1)
    logging.disable(logging.NOTSET)

    # logger.info("Indexed %d/%d documents" % (successes, number_of_docs))
    post_quarter_corp_data_history(client, corp_code, qdata, successes)

    return successes


def post_year_corp_data(client, corp_code, ydata: list):
    ns = []
    for qdata in ydata:
        ns.append(post_quarter_corp_data(client, corp_code, qdata))
    return ns


def post_corp_code(client):
    corp_code_output_filename = f'{DART_RESULT_DIR}/corp-code.zip'

    logger.info('Checking index status ... ')
    n = query_corp_code_imported()
    if n == 0:
        logger.info('Parsing corp code')
        corp_code_list = parse_corp_code(corp_code_output_filename)
        number_of_docs = len(corp_code_list)
        progress = tqdm(unit="docs", total=number_of_docs)
        successes = 0
        logging.disable(sys.maxsize)
        for ok, action in streaming_bulk(
                client=client, index="corp_code", actions=generate_corp_code_doc(corp_code_list),
        ):
            progress.update(1)
            successes += ok
        logging.disable(logging.NOTSET)
        # print("Indexed %d/%d documents" % (successes, number_of_docs))

    return n


def post_all_corp_data(client):
    pass
