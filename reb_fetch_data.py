import json
import time
from pathlib import Path

from tqdm import tqdm

from config import config, REB_REGION_CODES
from dart_fetch_data import download
from helpers import logger
from reb_post_data import reb_deal_obj_map, reb_apt_type_map, reb_contract_type_map, reb_size_gbn_map
import logging


def reb_download_page(url, params, output_filename, headers):
    # download first page
    data = download(url, params, output_filename, headers)
    if data:
        max_count = data.get('matchCount', 0)
        current_count = data.get('currentCount', 0)

    return data


def reb_preprocess_getRealEstateTradingCount():
    p = Path(config['REB_RESULT_DIR']) / Path(f'getRealEstateTradingCount/')
    poutdir = p / Path(f'preprocessed')
    poutdir.mkdir(exist_ok=True)
    successes = 0
    for f in p.glob('data-*.json'):
        logger.info(f'Preprocessing {f.name}')
        docs = json.loads(f.read_bytes())
        for doc in docs:
            deal_obj = doc['DEAL_OBJ']
            doc['DEAL_OBJ'] = reb_deal_obj_map[doc['DEAL_OBJ']]
        poutfile = poutdir.joinpath(f.name)
        poutfile.write_text(json.dumps(docs))


def reb_postprocess_doc(docs):
    new_docs = []
    for doc in docs:
        if 'DEAL_OBJ' in doc:
            doc['DEAL_OBJ'] = reb_deal_obj_map[doc['DEAL_OBJ']]
        if 'APT_TYPE' in doc:
            doc['APT_TYPE'] = reb_apt_type_map[doc['APT_TYPE']]
        if 'CONTRACT_TYPE' in doc:
            doc['CONTRACT_TYPE'] = reb_contract_type_map[doc['CONTRACT_TYPE']]
        if 'SIZE_GBN' in doc:
            doc['SIZE_GBN'] = reb_size_gbn_map[doc['SIZE_GBN']]
        new_docs.append(doc)
    return new_docs


def reb_get_fetch_param(gte, lte):
    npages = 100

    headers = {
        "Authorization": config['REB_API_KEY'],
        "accept": "application/json"
    }

    params = {
        'serviceKey': config['REB_API_KEY'],
        'page': 1,
        'perPage': npages,
        'returnType': 'json',
        'cond[RESEARCH_DATE::GTE]': gte,
        'cond[RESEARCH_DATE::LTE]': lte,
    }

    return headers, params


def reb_get_data(url: str, outdir: str, gte: str, lte: str, extra_params=None):
    headers, params = reb_get_fetch_param(gte, lte)

    if extra_params:
        params.update(extra_params)

    poutdir = Path(outdir)
    if not poutdir.exists():
        poutdir.mkdir(parents=True, exist_ok=True)

    progress1 = tqdm(total=len(REB_REGION_CODES))
    for region_code, region_name in REB_REGION_CODES.items():
        # progress1.colour = 'red'
        progress1.set_description(f'{gte}-{lte} : {region_name}')
        # logger.info(f'Fetching {region_name}')
        params['cond[REGION_CD::EQ]'] = region_code
        output_filename = poutdir / Path(f'data-{region_name}.json')

        # download first page
        bare_data = []
        params['page'] = 1
        data = download(url, params, None, headers)
        progress1.update(1)
        if data:
            progress1.colour = 'green'
            # time.sleep(1)
            max_count = data.get('matchCount', None)
            if not max_count:
                progress1.colour = 'red'
                logging.error(data)
            elif max_count == 0:
                logger.error(f'{region_name} has no data ')
                continue
            current_count = data.get('currentCount', 0)
            bare_data += reb_postprocess_doc(data['data'])
            # progress2 = tqdm(total=max_count, desc=region_name)
            while current_count < max_count:
                params.update({'page': params['page'] + 1})
                data = download(url, params, None, headers)
                bare_data += reb_postprocess_doc(data['data'])
                current_count += data['currentCount']
                # progress2.update(current_count)
            with open(output_filename, 'w') as fd:
                json.dump(bare_data, fd, indent=4)
            # progress2.close()

        # progress1.update(1)


def reb_fetch_getRealEstateTradingCountBuildType(outdir, gte: str, lte: str):
    api_name = 'RealEstateTradingSvc'
    index = 'getRealEstateTradingCountBuildType'
    url = f'https://api.odcloud.kr/api/{api_name}/v1/{index}'

    reb_get_data(url, outdir, gte, lte)


def reb_fetch_getRealEstateTradingCount(outdir, gte: str, lte: str):
    api_name = 'RealEstateTradingSvc'
    index = 'getAptRealTradingPriceIndex'
    url = f'https://api.odcloud.kr/api/{api_name}/v1/{index}'

    reb_get_data(url, outdir, gte, lte)


def reb_fetch_getAptRealTradingPriceIndex(outdir, gte: str, lte: str):
    api_name = 'RealTradingPriceIndexSvc'
    index = 'getAptRealTradingPriceIndex'
    url = f'https://api.odcloud.kr/api/{api_name}/v1/{index}'

    reb_get_data(url, outdir, gte, lte, None)


def reb_fetch_getAptRealTradingPriceIndexSize(outdir, gte: str, lte: str):
    api_name = 'RealTradingPriceIndexSvc'
    index = 'getAptRealTradingPriceIndexSize'
    url = f'https://api.odcloud.kr/api/{api_name}/v1/{index}'

    for sz_key, sz_val in reb_size_gbn_map.items():
        extra_params = {
            'cond[SIZE_GBN::EQ]': sz_key  # all
        }
        new_outdir = str(Path(outdir) / Path(sz_val))
        reb_get_data(url, new_outdir, gte, lte, extra_params)
