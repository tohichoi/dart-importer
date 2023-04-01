import collections
import csv
import json
import os
import time
from pathlib import Path
from ssl import SSLZeroReturnError

import pendulum
import requests
from elasticsearch.helpers import scan
from requests.exceptions import SSLError
from tqdm import tqdm

from dart_post_data import logger
from config import DART_RESULT_DIR, dart_base_params, dart_params, QUARTER_CODES, DART_CORPCODE_DATA_FILE, \
    KRX_KOSPI200_DATA_FILE
from helpers import query_corp_data, query_corp_code_doc
from dart_manage_file import DartFileManagerEx


class DARTMaxUsageError(Exception):
    pass


def dart_check_max_usage(r: dict, service_name: str):
    if service_name == 'dart':
        if r['status'] == '020':
            return r['message']
    return None


def dart_fetch_kospi200():
    f = Path(KRX_KOSPI200_DATA_FILE)
    if not f.exists():
        raise FileNotFoundError(f.absolute())

    with open(f) as fd:
        reader = csv.reader(fd)
        nlines = 0
        data = []
        # 종목코드,종목명,현재가,대비,등락률,거래대금(원),상장시가총액(원)
        for row in reader:
            nlines += 1
            if nlines == 1:
                continue
            # print(row)
            record = {
                'corp_name': row[1],
                'stock_code': row[0],
                'market_capitalization': int(row[-1].replace(',', '')),
                'date': pendulum.now().to_date_string().replace('-', '')
            }
            data.append(record)

    fout = f.with_suffix('.json').absolute()
    with open(fout, 'w') as fd:
        json.dump(data, fd, indent=4)

    return data


def dart_fetch_corp_code():
    """고유번호

        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018

    Args:
        output_filename (_type_): _description_
    """
    logger.info('Fetching corp code from DART system')
    output_filename = DART_CORPCODE_DATA_FILE

    if os.path.exists(output_filename):
        # logger.info(f'We have {output_filename}. Fetching corp_code is skipped.')
        return
    else:
        logger.info('Querying corp_code ... ')
        params = dart_base_params | {}
        url = "https://opendart.fss.or.kr/api/corpCode.xml"
        download(url, params, output_filename)

    # p = Path(output_filename)
    # px = p.with_name('CORPCODE.xml')
    # if not px.exists():
    #     subprocess.run(f'unzip {p.absolute()} -d {p.parent}', shell=True)
    #
    # if not px.exists():
    #     logger.error(f'Cannot generate file : {p.absolute()}')


def dart_fetch_one_corp_data(client, corp_code, corp_name, years) -> dict:
    dfm = DartFileManagerEx(data_dir=DART_RESULT_DIR, corp_code=corp_code, corp_name=corp_name,
                            data_file_prefix='financial-statements', logger=logger)
    corp_data = dict()
    for year in years:
        hits = query_corp_data(client, corp_code, year)
        if sum(hits) >= 4:
            # logger.info(f'remote corp_data {corp_name}-{year} exists ')
            continue
        # check if corp is already imported
        if dfm.has_year_data(year):
            # logger.info(f'local corp_data {corp_name}-{year} exists ')
            pass
        else:
            year_corp_data = dart_fetch_year_corp_data(corp_code, year)
            corp_data.update({year: year_corp_data})
            # n = upload_year_corp_data(client, corp_code, year_corp_data)
            # ns.append(n)
    dfm.save(corp_data)

    return corp_data


def dart_fetch_all_corp_data(client) -> dict:
    nc = collections.defaultdict(list)
    years = list(range(2017, 2023))
    # quarters = list(range(1, 5))
    # its = scan(client, query={"query": {"match_all": {}}}, index="corp_code", scroll="360m")
    check_its = scan(client, query={"query": {"match_all": {}}}, index="corp_code", scroll="360m")
    # 00126380 : 삼성전자
    # 00128564-삼천리M&C
    # check_its = [query_corp_code_doc(client, cc) for cc in ["00126380", "00309503", "00162461"]]
    its = []
    # check local file
    for doc in check_its:
        corp_code = doc['_source']['corp_code']
        corp_name = query_corp_code_doc(client, corp_code)['_source']['corp_name']
        dfm = DartFileManagerEx(data_dir=DART_RESULT_DIR, corp_code=corp_code, corp_name=corp_name,
                                data_file_prefix='financial-statements', logger=logger)
        if dfm.is_valid():
            continue
        its.append(doc)

    # total = query_corp_code_count(client)
    total = len(its)
    pbar = tqdm(desc='Fetching CorpData', total=total)
    for doc in its:
        corp_code = doc['_source']['corp_code']
        corp_name = query_corp_code_doc(client, corp_code)['_source']['corp_name']
        pbar.set_description(corp_name)
        dart_fetch_one_corp_data(client, corp_code, corp_name, years)
        pbar.update(1)
    return nc


def dart_fetch_corp_data(corp_code, corp_name, years) -> dict:
    """get_corp_info_from_dart

    공시정보:
    https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001
    기업개황:
    https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019002
    공시서류원본파일:
    https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019003

    Args:
        corp_name:
        corp_code (_type_): _description_
        years (_type_): _description_

    Returns:
        dict : year<int>, [1q, 2q, ..., 4q]
    """

    logger.info('Querying Financial Statement ... ')
    dfm = DartFileManagerEx(data_dir=DART_RESULT_DIR, corp_code=corp_code, corp_name=corp_name,
                            data_file_prefix='financial-statements', logger=logger)
    corp_data = dfm.load()
    if corp_data is None:
        corp_data = dict()

    for y in years:
        if y not in corp_data:
            corp_data[y] = dart_fetch_year_corp_data(corp_code, y)

    dfm.save(corp_data)

    return corp_data


def dart_fetch_year_corp_data(corp_code, year: int) -> list:
    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
    # output_filename = f'{DART_RESULT_DIR}/corp_data/{corp_code}-{corp_name}/financial-statement-{year}-<quarter>.json'
    # p = Path(output_filename)
    # if p.exists():
    #     logger.warning(f'{p.name} exists. Skipping fetching.')
    #
    # if not p.parent.exists():
    #     os.makedirs(p.parent)
    dart_query_params = dart_base_params | dart_params['fnlttSinglAcntAll']
    dart_query_params['corp_code'] = str(corp_code)
    dart_query_params['bsns_year'] = str(year)
    # 1분기보고서 : 11013
    # 반기보고서 : 11012
    # 3분기보고서 : 11014
    # 사업보고서 : 11011
    ydata = []
    # rt_dict = dict(zip(QUARTER_CODES, [f'{i}Q' for i in range(1, 5)]))
    for rt in QUARTER_CODES:
        dart_query_params['reprt_code'] = rt
        # of = output_filename.replace('<quarter>', rt_dict[rt])
        of = None
        d = download(url, dart_query_params, of)
        max_usage = dart_check_max_usage(d)
        if max_usage:
            raise DARTMaxUsageError(max_usage)
        ydata.append(d)

    return ydata


def dart_fetch_corp_info(corp_codes):
    """고유번호

        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018

    Args:
        output_filename (_type_): _description_
    """
    # check_its = scan(client, query={"query": {"match_all": {}}}, index="corp_code", scroll="360m")
    pbar = tqdm('Fetching corp_info', total=len(corp_codes))
    for corp_code in corp_codes:
        output_filename = Path(DART_RESULT_DIR).joinpath(f'corp_info/{corp_code}.json')
        pbar.set_description(corp_code)
        if Path.exists(output_filename):
            logger.info(f'We have {output_filename}. Fetching is skipped.')
        else:
            url = "https://opendart.fss.or.kr/api/company.json"
            # logger.info('Querying corp_code ... ')
            params = dart_base_params | {'corp_code': corp_code}
            download(url, params, output_filename)
        pbar.update(1)


def download(url, params, output_filename, headers=None):
    retry = 5
    data = None
    for i in range(retry):
        try:
            r = requests.get(url, params=params, headers=headers)
            p = None
            if output_filename:
                p = Path(output_filename)
                if not p.parent.exists():
                    p.parent.mkdir()
                p.write_bytes(r.content)
            data = r.json()
            max_usage = None
            if 'dart' in url:
                max_usage = dart_check_max_usage(data, 'dart')
            elif 'reb' in url:
                max_usage = dart_check_max_usage(data, 'reb')

            if max_usage:
                if p:
                    p.unlink()
                raise DARTMaxUsageError(max_usage)
            break
        except (SSLZeroReturnError, SSLError):
            time.sleep(5)
            continue

    return data
