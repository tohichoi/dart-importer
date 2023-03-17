import collections
import os
from pathlib import Path

import requests
from elasticsearch.helpers import scan
from tqdm import tqdm

from config import DART_RESULT_DIR, dart_base_params, dart_params, QUARTER_CODES, DART_CORPCODE_DATA_FILE
from helpers import query_corp_data, query_corp_code_doc, query_corp_code_count
from manage_dart_file import DartFileManager
from post_data import logger


def fetch_corp_code():

    logger.info('Fetching corp code from DART system')
    fetch_corp_code_from_dart(DART_CORPCODE_DATA_FILE)


def fetch_one_corp_data(client, corp_code, corp_name, years) -> dict:
    dfm = DartFileManager(data_dir=DART_RESULT_DIR, corp_code=corp_code, corp_name=corp_name,
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
            year_corp_data = fetch_year_corp_data(corp_code, year)
            corp_data.update({year: year_corp_data})
            # n = upload_year_corp_data(client, corp_code, year_corp_data)
            # ns.append(n)
    dfm.save(corp_data)

    return corp_data


def fetch_all_corp_data(client) -> dict:
    nc = collections.defaultdict(list)
    years = list(range(2017, 2023))
    quarters = list(range(1, 5))
    its = scan(client, query={"query": {"match_all": {}}}, index="corp_code")
    total = query_corp_code_count(client)
    pbar = tqdm(desc='Fetching CorpData', total=total)
    for doc in its:
        corp_code = doc['_source']['corp_code']
        corp_name = query_corp_code_doc(client, corp_code)['corp_name']
        pbar.set_description(corp_name)
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
        fetch_one_corp_data(client, corp_code, corp_name, years)
        pbar.update(1)
    return nc


def fetch_corp_data(corp_code, corp_name, years) -> dict:
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
    dfm = DartFileManager(data_dir=DART_RESULT_DIR, corp_code=corp_code, corp_name=corp_name,
                          data_file_prefix='financial-statements', logger=logger)
    corp_data = dfm.load()
    if corp_data is None:
        corp_data = dict()

    for y in years:
        if y not in corp_data:
            corp_data[y] = fetch_year_corp_data(corp_code, y)

    dfm.save(corp_data)

    return corp_data


def fetch_year_corp_data(corp_code, year: int) -> list:
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
        ydata.append(d)

    return ydata


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

    # p = Path(output_filename)
    # px = p.with_name('CORPCODE.xml')
    # if not px.exists():
    #     subprocess.run(f'unzip {p.absolute()} -d {p.parent}', shell=True)
    #
    # if not px.exists():
    #     logger.error(f'Cannot generate file : {p.absolute()}')


def download(url, params, output_filename):
    with requests.Session() as s:
        r = s.get(url, params=params)
        if output_filename:
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
