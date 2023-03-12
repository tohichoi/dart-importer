#!/usr/bin/env python

import json
from pathlib import Path
from pprint import pprint
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
import unittest
from elasticsearch import Elasticsearch


logfmt = "%(asctime)s.%(msecs)03dZ %(levelname)s %(message)s"
coloredlogs.install(fmt=logfmt)

logging.basicConfig(
    level=logging.DEBUG,
    format=logfmt,
    datefmt="%Y-%m-%dT%H:%M:%S",
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

# CORP_CODE = 

dart_base_params={
    "crtfc_key":DART_API_KEY,
    'corp_code': None
}

dart_params={
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

def parse_corp_code(filename, do_post):
    with open(filename) as fd:
        logger.info(f'Parsing {filename}')
        soup = BeautifulSoup(fd.read(), features="xml")
    code_info_list = soup.result.find_all('list')
    code_info=dict()
    post_body_data = ''
    with tqdm(total=len(code_info_list), desc='Parsing') as pbar:
        for ci in code_info_list:
            # code_info[ci.corp_name.text]
            d = {
                'code': ci.corp_code.text,
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
            
            
def download(url, params, output_filename, is_binary):
    with requests.Session() as s:
        r = s.get(url, params=params)
        html = r.content
        p=Path(output_filename)
        if not p.parent.exists():
            p.parent.mkdir()
        p.write_bytes(r.content)
        # with open(output_filename, mode) as fd:
        #     fd.write(r.content)
        # p=Path(output_filename)
        # if p.suffix == '.json':
        #     p2=
        #     subprocess.run(['jq', '.', '<', ])

# 고유번호
def get_corp_code(output_filename):
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    if os.path.exists(output_filename):
        logger.info(f'Result file exists : {output_filename}')
        return
    logger.info('Querying Corporation Code ... ')
    download(url, dart_query_params, output_filename, True)


def get_corp_info(corp_code):
    r = elastic_session.get(ELASTICSEARCH_URL + '/corp_code/_search',
                             data={
                                 "query": {
                                    "term": {
                                         "corp_code": corp_code
                                     }
                                    }
                                 },
                             headers={'Content-Type': 'application/json'})

    
def get_corp_info(corp_code, years):
    # 공시정보
    # https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001
    # 기업개황
    # https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019002
    # 공시서류원본파일
    # https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019003
    data = None
    def _get_financial_statements(year:int):
        url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
        output_filename = f'{DART_RESULT_DIR}/corp_data/{corp_code}-{corp_name}/finantial-statement={year}.json'
        p=Path(output_filename)
        if p.exists():
            logger.warning(f'{p.name} exists. Skipping fetching.')
        else:
            if not p.parent.exists():
                os.makedirs(p.parent)
            dart_query_params = dart_base_params | dart_params['fnlttSinglAcntAll']
            dart_query_params['year'] = str(year)
            download(url, dart_query_params, output_filename, True)

        with open(output_filename) as fd:
            data = json.load(fd)
    
        return data
    
    logger.info('Querying Financial Statement ... ')
    data = []
    for y in years:
        data.append(_get_financial_statements(y))

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
    r = elastic_session.get(ELASTICSEARCH_URL + '/corp_code/_count')
    if r.status_code == requests.codes.ok:
        rj = r.json()
        logger.info(f"corp_code has {rj['count']} records")
        return rj['count']
    elif r.status_code == 404:
        logger.info(f'{r.status_code} : maybe index is not created.')
        return 0
    else:
        logger.error(f'{r.status_code}')
        r.raise_for_status()


def delete_all_data():
    r = elastic_session.get(
        ELASTICSEARCH_URL + '/corp_code/_delete_by_query?conflicts=proceed&pretty',
        data={
            "query": {
                "match_all": {}
            }
        })


class Test(unittest.TestCase):
    def setUp(self):
        pass
    
    def test_elasticsearch_client(self):

        # Password for the 'elastic' user generated by Elasticsearch
        # ELASTIC_PASSWORD = "<password>"

        # Create the client instance
        client = Elasticsearch(
            ELASTICSEARCH_URL,
            # ca_certs=ELASTIC_CERTFILE,
            ssl_assert_fingerprint=ELASTIC_CERTFILE_FINGERPRINT,
            basic_auth=("elastic", ELASTIC_PASSWORD)
        )

        # Successful response!
        info = client.info()
        print(info)
        # {'name': 'instance-0000000000', 'cluster_name': ...}
    
    
if __name__ == '__main__':
    corp_code_filename = f'{DART_RESULT_DIR}/CORPCODE.xml'
    corp_code_output_filename = f'{DART_RESULT_DIR}/corp-code.zip'

    logger.info('Fetching corp code')
    get_corp_code(corp_code_output_filename)

    logger.info('Parsing corp code')
    if check_corp_code_imported() == 0:
        parse_corp_code(corp_code_filename, True)
    
    # 삼성전자
    data = get_corp_info('00126380', list(range(2021, 2023)))
    analyze_corp_info(data)
    elastic_session.close()