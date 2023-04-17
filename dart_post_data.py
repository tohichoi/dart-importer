#!/usr/bin/env python
import json
import logging
import re
import sys
import zipfile
from pathlib import Path
from pprint import pprint

import pendulum
import requests
from bs4 import BeautifulSoup
from elasticsearch.helpers import streaming_bulk
from tqdm import tqdm

from config import DART_RESULT_DIR, ELASTICSEARCH_URL, KRX_KOSPI200_DATA_FILE
from helpers import query_corp_info_doc, query_corp_data, query_corp_name, query_corp_quarter_doc, query_index_imported, \
    get_time_frame, logger, elastic_session
from dart_manage_file import DartFileManagerEx


def dart_parse_corp_info(filename):
    """

    Args:
        filename: zipfile path

    Returns:

    """
    logger.info(f'Parsing {filename}')
    zf = zipfile.ZipFile(filename)
    return [Path(f).with_suffix('') for f in zf.namelist()]


def dart_parse_corp_code(filename):
    """

    Args:
        filename: zipfile path

    Returns:

    """
    logger.info(f'Parsing {filename}')
    zf = zipfile.ZipFile(filename)
    soup = BeautifulSoup(zf.read('CORPCODE.xml'), features="xml")
    # return [t for t in soup.result.find_all('list') if len(t.stock_code.text.strip()) > 0]
    return [t for t in soup.result.find_all('list')]


def dart_generate_corp_code_doc(code_info_list):
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


def dart_generate_corp_info_doc(client, filename):
    logger.info(f'Parsing {filename}')
    zf = zipfile.ZipFile(filename)
    for f in zf.namelist():
        ci = json.loads(zf.read(f))
        resp = query_corp_info_doc(client, ci['corp_code'])
        if resp['hits']['total']['value'] > 0:
            continue
        for k in ['status', 'message']:
            del (ci[k])
        try:
            pendulum.from_format(ci['est_dt'], 'YYYYMMDD')
        except ValueError:
            ci['est_dt'] = pendulum.now().to_date_string().replace('-', '')
        yield ci
    zf.close()


def dart_parse_corp_code_OLD(filename, do_post):
    with open(filename) as fd:
        logger.info(f'Parsing {filename}')
        soup = BeautifulSoup(fd.read(), features="xml")
    code_info_list = soup.result.find_all('list')
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
            nimported = query_index_imported(None)
            logger.info(f'Posted data : {nimported}')
        else:
            logger.error(f'{r.status_code}')
            r.raise_for_status()


# 고유번호


def dart_analyze_corp_info(data):
    # "account_id": "ifrs-full_ComprehensiveIncome",
    # "account_nm": "총포괄손익",
    for ydata in data:
        for d in ydata['list']:
            if d['account_nm'] == '총포괄손익':
                pprint(d)


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
def dart_create_index(client, indices):
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
                    "corp_code": {"type": "keyword"},
                    "corp_name": {"type": "keyword"},
                    "stock_code": {"type": "keyword"},
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

    # https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019002
    if 'corp_info' in indices:
        client.options(ignore_status=400).indices.create(
            index="corp_info",
            settings={"number_of_shards": 1},
            mappings={
                "properties": {
                    "corp_code": {"type": "keyword"},  # "00126380",
                    "corp_name": {"type": "keyword"},  # "삼성전자(주)",
                    "corp_name_eng": {"type": "text"},  # "SAMSUNG ELECTRONICS CO,.LTD",
                    "stock_name": {"type": "keyword"},  # "삼성전자",
                    "stock_code": {"type": "keyword"},  # "005930",
                    "ceo_nm": {"type": "text"},  # "한종희, 경계현",
                    # corp_cls: 법인구분 : Y(유가), K(코스닥), N(코넥스), E(기타)
                    "corp_cls": {"type": "keyword"},  # "Y", 법인구분 : Y(유가), K(코스닥), N(코넥스), E(기타)
                    "jurir_no": {"type": "keyword"},  # "1301110006246",
                    "bizr_no": {"type": "keyword"},  # "1248100998",
                    "adres": {"type": "text"},  # "경기도 수원시 영통구  삼성로 129 (매탄동)",
                    "hm_url": {"type": "text"},  # "www.samsung.com/sec",
                    "ir_url": {"type": "text"},  # "",
                    "phn_no": {"type": "keyword"},  # "02-2255-0114",
                    "fax_no": {"type": "keyword"},  # "031-200-7538",
                    "induty_code": {"type": "keyword"},  # "264",
                    "est_dt": {
                        "type": "date",
                        "format": "yyyyMMdd"},  # "19690113",
                    "acc_mt": {"type": "keyword"},  # "12",
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
                    "rcept_no": {"type": "keyword"},
                    # 보고서 코드
                    "reprt_code": {"type": "keyword"},
                    # 사업 연도
                    "bsns_year": {"type": "date", "format": "yyyy"},
                    # 고유번호
                    "corp_code": {"type": "keyword"},
                    # 재무제표구분
                    # BS : 재무상태표 IS : 손익계산서 CIS : 포괄손익계산서 CF : 현금흐름표 SCE : 자본변동표
                    "sj_div": {"type": "keyword"},
                    # 재무제표명
                    "sj_nm": {"type": "keyword"},
                    # 계정ID
                    # XBRL 표준계정ID ※ 표준계정ID가 아닐경우 ""-표준계정코드 미사용-"" 표시
                    "account_id": {"type": "keyword"},
                    # 계정명
                    "account_nm": {"type": "keyword"},
                    # 계정상세
                    # ※ 자본변동표에만 출력 ex) 계정 상세명칭 예시 - 자본 [member]|지배기업 소유주지분 - 자본 [member]|지배기업 소유주지분|기타포괄손익누계액 [member]
                    "account_detail": {"type": "text"},
                    # 당기명
                    "thstrm_nm": {"type": "keyword"},
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
                "corp_code": {"type": "keyword"},
                "year": {"type": "date", "format": "yyyy"},
                "reprt_code": {"type": "keyword"},
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

    client.options(ignore_status=400).indices.create(
        index="kospi200",
        settings={"number_of_shards": 1},
        mappings={
            "properties": {
                "corp_name": {"type": "keyword"},
                "stock_code": {"type": "keyword"},
                "market_capitalization": {"type": "long"},
                "date": {"type": "date", "format": "yyyyMMdd"},
            }
        },
    )


def dart_get_fetched_docs():
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


def dart_post_quarter_corp_data_history(client, corp_code, qdata: dict, successes):
    docs = qdata['list']

    '''
                    "corp_code": {"type": "text"},
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


def dart_post_quarter_corp_data(client, corp_code, qdata: dict) -> int:
    if type(qdata) != dict:
        raise TypeError(f'Data type is not dict : {str(type(qdata))}')
        # logger.error(f'Data type is not dict : {str(type(qdata))}')

    if qdata['status'] != '000':
        raise ValueError(f'Fetched data status code is {qdata["status"]} ({qdata["message"]})')

    docs = qdata['list']
    number_of_docs = len(docs)
    progress = tqdm(unit="docs", total=number_of_docs, desc='Quarter', leave=False)
    successes = 0
    logging.disable(sys.maxsize)
    for doc in docs:
        # create 는 id 필요. index 는 불필요
        doc = get_time_frame(doc)
        if not query_corp_quarter_doc(client, doc):
            resp = client.index(index="corp_data", document=doc)
            # http status 200 or 201
            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/201
            if resp['result'] == 'created':
                successes += 1
            progress.update(1)
        else:
            logger.info('data exists')
    logging.disable(logging.NOTSET)

    # logger.info("Indexed %d/%d documents" % (successes, number_of_docs))
    dart_post_quarter_corp_data_history(client, corp_code, qdata, successes)

    return successes


def dart_post_year_corp_data(client, corp_code, year, ydata: list):
    ns = []
    corp_name = query_corp_name(client, corp_code)
    for idx, qdata in enumerate(ydata):
        try:
            ns.append(dart_post_quarter_corp_data(client, corp_code, qdata))
        except (ValueError, TypeError) as e:
            logger.error(f'{corp_name}/{year}/{idx}th : {e}')
            continue
    return ns


def dart_post_corp_code(client):
    corp_code_output_filename = f'{DART_RESULT_DIR}/corp-code.zip'

    logger.info('Checking index status ... ')
    n = query_index_imported('corp_code')
    if n == 0:
        logger.info('Parsing corp code')
        corp_code_list = dart_parse_corp_code(corp_code_output_filename)
        number_of_docs = len(corp_code_list)
        progress = tqdm(unit="docs", total=number_of_docs)
        successes = 0
        logging.disable(sys.maxsize)
        for ok, action in streaming_bulk(
                client=client, index="corp_code", actions=dart_generate_corp_code_doc(corp_code_list),
        ):
            progress.update(1)
            successes += ok
        logging.disable(logging.NOTSET)
        # print("Indexed %d/%d documents" % (successes, number_of_docs))

    return n


def dart_post_corp_info(client):
    corp_info_output_filename = f'{DART_RESULT_DIR}/corp_info.zip'

    # logger.info('Checking index status ... ')
    # n = query_index_imported('corp_info')
    # if n == 0:
    logger.info('Parsing corp info')
    # corp_info_list = parse_corp_code(corp_info_output_filename)
    # number_of_docs = len(corp_info_list)
    # progress = tqdm(unit="docs", total=number_of_docs)
    successes = 0
    # logging.disable(sys.maxsize)
    for ok, action in streaming_bulk(
            client=client, index="corp_info", actions=dart_generate_corp_info_doc(client, corp_info_output_filename),
    ):
        # progress.update(1)
        successes += ok
    # logging.disable(logging.NOTSET)
    # print("Indexed %d/%d documents" % (successes, number_of_docs))

    return successes


def dart_post_all_corp_data(client):
    data_dir = Path(DART_RESULT_DIR).joinpath('corp_data')
    corps = [p for p in list(data_dir.rglob('*')) if p.is_dir()]
    progress_corp = tqdm(unit="corps", total=len(corps), desc='Corporations', colour='blue')
    for p in corps:
        m = re.match(r'([0-9]+)-(.+)$', p.name)
        if m:
            corp_code = m.group(1)
            corp_name = m.group(2)
            progress_corp.set_description(corp_name)
            dfm = DartFileManagerEx(data_dir=DART_RESULT_DIR, corp_code=corp_code, corp_name=corp_name,
                                    data_file_prefix='financial-statements', logger=logger)
            if dfm.is_valid():
                progress_years = tqdm(unit='years', total=len(dfm.corp_data), leave=False, colour='yellow')
                for year, ydata in dfm.corp_data.items():
                    progress_years.set_description(f'Year {year}')
                    hits = query_corp_data(client, corp_code, year)
                    # quarter * 4
                    if sum(hits) != 4:
                        dart_post_year_corp_data(client, corp_code, year, ydata)
                    progress_years.update(1)
        progress_corp.update(1)


def dart_generate_kospi200_doc(client, data):
    for d in data:
        yield d


def dart_post_kospi200(client):
    f = Path(KRX_KOSPI200_DATA_FILE).with_suffix('.json')
    if not f.exists():
        raise FileNotFoundError(f.absolute())

    with open(f, 'rt', encoding='utf-8-sig') as fd:
        data = json.load(fd)

    successes = 0

    for doc in data:
        resp = client.index(index="kospi200", document=doc)
        # http status 200 or 201
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/201
        if resp['result'] == 'created':
            successes += 1
    # for ok, action in streaming_bulk(
    #         client=client, index="kospi200", actions=generate_kospi200_doc(client, data),
    # ):
    #     successes += ok

    return successes

