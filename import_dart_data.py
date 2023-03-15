#!/usr/bin/env python

import argparse
import collections
import json
from pathlib import Path
from pprint import pprint
import sys
import pendulum
import requests
from requests import get  # to make GET request
from bs4 import BeautifulSoup
from dotenv import dotenv_values
import os
import time
import logging
import coloredlogs
import subprocess
from tqdm import tqdm
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import streaming_bulk, scan

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

config = {
    # **dotenv_values("../docker-elk/.env"),  # load shared development variables
    **dotenv_values(".env"),  # load sensitive variables
    **os.environ,  # override loaded values with environment variables
}

DART_API_KEY = config['DART_API_KEY']
DART_RESULT_DIR = config['DART_RESULT_DIR']
ELASTIC_USER = config['ELASTIC_USER']
ELASTIC_PASSWORD = config['ELASTIC_PASSWORD']
ELASTIC_CERTFILE = config['ELASTIC_CERTFILE']
ELASTIC_CERTFILE_FINGERPRINT = config['ELASTIC_CERTFILE_FINGERPRINT']
ELASTICSEARCH_URL = config['ELASTICSEARCH_URL']

dart_base_params = {
    "crtfc_key": DART_API_KEY,
}

dart_params = {
    # 보고서 리스트
    "list": {
        # 검색시작 접수일자(YYYYMMDD)
        "bgn_de": None,
        "end_de": None,
        # 최종보고서만 검색여부(Y or N)
        "last_reprt_at": "N",
        # 공시유형 A : 정기공시
        "pblntf_ty": "A",
        # 접수일자: date
        # 회사명: crp
        # 보고서명: rpt
        # ※ 기본값: date
        "sort": "crp",
        # 오름차순(asc), 내림차순(desc)
        "sort_mth": "asc",
        # 페이지 번호(1~n) 기본값 : 1
        "page_no": "1",
        # 페이지당 건수(1~100) 기본값 : 10, 최대값 : 100
        "page_count": "100"
    },
    # 단일회사 전체 재무제표
    "fnlttSinglAcntAll": {
        # bsns_year	사업연도	STRING(4)	Y	사업연도(4자리) ※ 2015년 이후 부터 정보제공
        'bsns_year': None,
        # 1분기보고서 : 11013
        # 반기보고서 : 11012
        # 3분기보고서 : 11014
        # 사업보고서 : 11011
        'reprt_code': "11011",
        # CFS:연결재무제표, OFS:재무제표
        'fs_div': 'OFS'
    }
}

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

QUARTER_CODES = ['11013', '11012', '11014', '11011']


def parse_corp_code(filename):
    with open(filename) as fd:
        logger.info(f'Parsing {filename}')
        soup = BeautifulSoup(fd.read(), features="xml")
    return soup.result.find_all('list')


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
            '_id': ci.corp_code.text,
            'corp_code': ci.corp_code.text,
            'corp_name': ci.corp_name.text,
            'stock_code': ci.stock_code.text,
            'modify_date': ci.modify_date.text
        }

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
            nimported = check_corp_code_imported()
            logger.info(f'Posted data : {nimported}')
        else:
            logger.error(f'{r.status_code}')
            r.raise_for_status()


def download(url, params, output_filename):
    with requests.Session() as s:
        r = s.get(url, params=params)
        p = Path(output_filename)
        if not p.parent.exists():
            p.parent.mkdir()
        p.write_bytes(r.content)
        # with open(output_filename, mode) as fd:
        #     fd.write(r.content)
        # p=Path(output_filename)
        # if p.suffix == '.json':
        #     p2=
        #     subprocess.run(['jq', '.', '<', ])

        # actually dict
        return r.json()


# 고유번호
def fetch_corp_code_from_dart(output_filename):
    """고유번호
    
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018

    Args:
        output_filename (_type_): _description_
    """
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    if os.path.exists(output_filename):
        logger.info(f'We have {output_filename}. Fetching corp_code is skipped.')
    else:
        logger.info('Querying corp_code ... ')
        params = dart_base_params | {}
        download(url, params, output_filename)

    p = Path(output_filename)
    px = p.with_name('CORPCODE.xml')
    if not px.exists():
        subprocess.run(f'unzip {p.absolute()} -d {p.parent}', shell=True)

    if not px.exists():
        logger.error(f'Cannot generate file : {p.absolute()}')


def get_corp_code_doc(corp_code):
    # r = elastic_session.get(ELASTICSEARCH_URL + '/corp_code/_search',
    #                          data={
    #                              "query": {
    #                                 "term": {
    #                                      "corp_code": corp_code
    #                                  }
    #                                 }
    #                              },
    #                          headers={'Content-Type': 'application/json'})
    resp = esclient.get(index="corp_code", id=corp_code)
    return resp['_source']


def get_corp_info_from_dart(corp_code, years) -> dict:
    """get_corp_info_from_dart

    공시정보:
    https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001
    기업개황:
    https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019002
    공시서류원본파일:
    https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019003

    Args:
        corp_code (_type_): _description_
        years (_type_): _description_

    Returns:
        dict : year<int>, [1q, 2q, ..., 4q]
    """

    def _get_quarter_financial_statements(year, quarter):
        pass

    def _get_year_financial_statements(year: int):
        url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
        corp_name = get_corp_code_doc(corp_code)['corp_name']
        output_filename = f'{DART_RESULT_DIR}/corp_data/{corp_code}-{corp_name}/financial-statement-{year}-<quarter>.json'
        p = Path(output_filename)
        if p.exists():
            logger.warning(f'{p.name} exists. Skipping fetching.')

        if not p.parent.exists():
            os.makedirs(p.parent)
        dart_query_params = dart_base_params | dart_params['fnlttSinglAcntAll']
        dart_query_params['corp_code'] = str(corp_code)
        dart_query_params['bsns_year'] = str(year)
        # 1분기보고서 : 11013
        # 반기보고서 : 11012
        # 3분기보고서 : 11014
        # 사업보고서 : 11011
        qdata = []
        rt_dict = dict(zip(QUARTER_CODES, [f'{i}Q' for i in range(1, 5)]))
        for rt in QUARTER_CODES:
            dart_query_params['reprt_code'] = rt
            of = output_filename.replace('<quarter>', rt_dict[rt])
            d = download(url, dart_query_params, of)
            qdata.append(d)

        return qdata

    logger.info('Querying Financial Statement ... ')
    data = dict()
    for y in years:
        data[y] = _get_year_financial_statements(y)

    # for d in data['list']:
    #     print(d['account_nm'])

    return data


def analyze_corp_info(data):
    # "account_id": "ifrs-full_ComprehensiveIncome",
    # "account_nm": "총포괄손익",
    for ydata in data:
        for d in ydata['list']:
            if d['account_nm'] == '총포괄손익':
                pprint(d)


def check_corp_code_imported():
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


def import_corp_code(client):
    corp_code_filename = f'{DART_RESULT_DIR}/CORPCODE.xml'
    corp_code_output_filename = f'{DART_RESULT_DIR}/corp-code.zip'

    logger.info('Fetching corp code from DART system')
    fetch_corp_code_from_dart(corp_code_output_filename)

    logger.info('Checking index status ... ')
    if check_corp_code_imported() == 0:
        logger.info('Parsing corp code')
        corp_code_list = parse_corp_code(corp_code_filename)
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


def get_fetched_docs():
    """이미 fetch된 doc 리스트 구하기
    
        - 9만개 이상의 corp_code가 존재함
        - 전부 한 디렉토리에 넣어야 하나? => not effective
    """
    pass


def get_corp_info_from_elastic(client, corp_code, year: int) -> dict:
    # 연단위로만 체크하자
    # resp = es.search(index="test-index", query={"match_all": {}})
    # print("Got %d Hits:" % resp['hits']['total']['value'])
    # for hit in resp['hits']['hits']:
    #     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
    hits = dict()
    for qc in QUARTER_CODES:
        resp = client.search(index="corp_code", query={
            "bool": {
                "must": [
                    {"term": {"corp_code": corp_code}},
                    {"term": {"bsns_year": str(year)}},
                    {"term": {"reprt_code": qc}}
                ]
            }
        })

        hits[qc] = resp['hits']['total']['value']

    return hits


def import_one_corp_data(client, corp_code, years) -> list:
    ns = []

    # check if corp is already imported
    ysdata = get_corp_info_from_dart(corp_code, years)
    for year, ydata in ysdata.items():
        n = upload_corp_year_data(client, corp_code, ydata)
        ns.append(n)
    return ns


def import_corp_data(client) -> dict:
    nc = collections.defaultdict(list)
    years = list(range(2017, 2023))
    quarters = list(range(1, 5))
    its = scan(client, query={"query": {"match_all": {}}}, index="corp_code")
    for doc in its:
        corp_code = doc['_source']['corp_code']
        # [
        #   [
        #     [-1, -1, -1, -1],
        #     [-1, -1, -1, -1],
        #     [-1, -1, -1, -1],
        #     [-1, -1, -1, -1],
        #     [-1, -1, -1, -1],
        #     [-1, -1, -1, -1]
        #   ]
        # ]
        num_data = import_one_corp_data(client, corp_code, years)
        nc['corp_code'] = num_data
    return nc


def upload_corp_quarter_data_bulk(client, data: dict) -> int:
    if type(data) != dict:
        logger.error(f'Data type is not dict : {str(type(data))}')
        return -1

    if data['status'] != '000':
        logger.error(f'Status code is {data["status"]}({data["message"]})')
        return -1

    docs = data['list']
    number_of_docs = len(docs)
    progress = tqdm(unit="docs", total=number_of_docs)
    successes = 0
    logging.disable(sys.maxsize)
    for ok, action in streaming_bulk(
            client=client, index="corp_data", actions=(d for d in docs),
    ):
        progress.update(1)
        successes += ok
    logging.disable(logging.NOTSET)
    # print("Indexed %d/%d documents" % (successes, number_of_docs))

    return successes


def upload_corp_quarter_data_history(client, corp_code, qdata: dict, successes):
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


def upload_corp_quarter_data(client, corp_code, qdata: dict) -> int:
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
    upload_corp_quarter_data_history(client, corp_code, qdata, successes)

    return successes


def upload_corp_year_data(client, corp_code, ysdata: list):
    ns = []
    for qdata in ysdata:
        ns.append(upload_corp_quarter_data(client, corp_code, qdata))
    return ns


def main():
    indices = ['corp_code', 'corp_data']
    parser = argparse.ArgumentParser(description='dart importer')
    parser.add_argument(
        '--create-index',
        help='Create ElasticSearch Index. Example: ./import_dart_data.py --create-index corp_code corp_data',
        choices=indices, nargs="+", default=[])
    parser.add_argument(
        '--delete-documents', help='Delete all documents',
        choices=indices, nargs="+", default=[])
    parser.add_argument(
        '--import-data', help='Import data',
        choices=indices, nargs="+", default=[])
    # parser.add_argument(
    #     '--import-corp-data', help='Import corp data(filings, ...)', action='store_true')

    args = parser.parse_args()

    if len(args.create_index) > 0:
        create_index(esclient, args.create_index)

    if len(args.delete_documents):
        ans = input("WARNING: Delete all data? Type 'delete' to proceed.\nYour choice: ")
        if ans.strip().lower() == 'delete':
            delete_documents(esclient, args.delete_documents)
        else:
            print('Cancelled.')

    if 'corp_code' in args.import_data:
        import_corp_code(esclient)

    if 'corp_data' in args.import_data:
        import_corp_data(esclient)

    # # 삼성전자
    # data = get_corp_info_from_dart('00126380', list(range(2021, 2023)))
    # analyze_corp_info(data)
    # elastic_session.close()


if __name__ == '__main__':
    main()
