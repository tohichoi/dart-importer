import json
import logging
import sys

from elasticsearch.helpers import streaming_bulk
from tqdm import tqdm

# 토지거래 01
# 순수토지거래 02
# 건축물거래 03
# 주택거래 04
# 아파트거래 05
# 주택매매거래 06
# 아파트매매거래 07
# 토지매매거래 08

reb_deal_obj_map = {
    "01": "토지거래",
    "02": "순수토지거래",
    "03": "건축물거래",
    "04": "주택거래",
    "05": "아파트거래",
    "06": "주택매매거래",
    "07": "아파트매매거래",
    "08": "토지매매거래",
}

reb_apt_type_map = {
    "0": "공동주택",
    "1": "아파트",
    "3": "연립다세대",
}

reb_contract_type_map = {
    "0": "매매",
    "1": "전세",
}


# 40㎡이하	40~60㎡이하	60㎡~85㎡이하	85㎡~135㎡이하	135㎡초과
reb_size_gbn_map = {
    "0": "전체",
    # 40㎡이하
    "1": "초소형",
    # 40~60㎡이하
    "2": "소형",
    # 60㎡~85㎡이하
    "3": "중소형",
    # 85㎡~135㎡이하
    "4": "중대형",
    # 135㎡초과
    "5": "대형",
}

reb_index_mappings = {
    'getRealEstateTradingCount': {
        "index": "reb_getrealestatetradingcount",
        "settings": {"number_of_shards": 1},
        "mappings": {
            "properties": {
                "ALL_CNT": {"type": "integer"},
                "DEAL_OBJ": {"type": "keyword"},
                "LEVEL_NO": {"type": "integer"},
                "REGION_CD": {"type": "keyword"},
                "REGION_NM": {"type": "keyword"},
                "RESEARCH_DATE": {"type": "date", "format": "yyyyMM"},
            }
        }
    },
    # 건물유형별 부동산 거래 건수 조회 상세기능 명세
    'getRealEstateTradingCountBuildType': {
        "index": "reb_getrealestatetradingcountbuildtype",
        "settings": {"number_of_shards": 1},
        "mappings": {
            "properties": {
                "ALL_CNT": {"type": "integer"},
                "DEAL_OBJ": {"type": "keyword"},
                "LEVEL_NO": {"type": "integer"},
                "REGION_CD": {"type": "keyword"},
                "REGION_NM": {"type": "keyword"},
                "RESEARCH_DATE": {"type": "date", "format": "yyyyMM"},
                "LIVE_SUM_COUNT": {"type": "integer"},
                "BULD_USE11_CNT": {"type": "integer"},
                "BULD_USE12_CNT": {"type": "integer"},
                "BULD_USE13_CNT": {"type": "integer"},
                "BULD_USE14_CNT": {"type": "integer"},
                "BULD_USE15_CNT": {"type": "integer"},
                "BULD_USE20_CNT": {"type": "integer"},
                "BULD_USE21_CNT": {"type": "integer"},
                "BULD_USE22_CNT": {"type": "integer"},
                "BULD_USE30_CNT": {"type": "integer"},
                "BULD_USE40_CNT": {"type": "integer"},
                "BULD_USE50_CNT": {"type": "integer"},

            }
        }
    },
    'getAptRealTradingPriceIndex': {
        'index': "reb_getaptrealtradingpriceindex",
        "settings": {"number_of_shards": 1},
        "mappings": {
            "properties": {
                # 아파트 타입 (0: 공동주택, 1: 아파트, 3: 연립다세대)
                "APT_TYPE": {"type": "keyword"},
                # 계약 타입 (0: 매매, 1: 전세)
                "CONTRACT_TYPE": {"type": "keyword"},
                "INDICES": {"type": "float"},
                "LEVEL_NO": {"type": "integer"},
                "REGION_CD": {"type": "keyword"},
                "REGION_NM": {"type": "keyword"},
                "RESEARCH_DATE": {"type": "date", "format": "yyyyMM"},
            }
        }
    }
}


def reb_post_data(client, index, in_dir):
    # p = Path(config['REB_RESULT_DIR']) / Path(f'getRealEstateTradingCount/')
    p = in_dir
    successes = 0
    for f in p.glob('data-*.json'):
        # logger.info(f'Processing {f.name}')
        docs = json.loads(f.read_bytes())
        progress = tqdm(unit="docs", total=len(docs), desc=f.name)
        successes = 0
        logging.disable(sys.maxsize)
        for ok, action in streaming_bulk(
                client=client, index=reb_index_mappings[index]['index'],
                actions=_reb_generate_doc(docs)
        ):
            progress.update(1)
            successes += ok
        logging.disable(logging.NOTSET)
    print(f'Posted {successes} documents()')


def reb_post_getRealEstateTradingCountBuildType(client, in_dir):
    reb_post_data(client, 'getRealEstateTradingCountBuildType', in_dir)


def reb_post_getRealEstateTradingCount(client, in_dir):
    reb_post_data(client, 'getRealEstateTradingCount', in_dir)


def reb_post_getAptRealTradingPriceIndex(client, in_dir):
    reb_post_data(client, 'getAptRealTradingPriceIndex', in_dir)


def _reb_generate_doc(docs):
    for doc in docs:
        yield doc


def reb_create_index(client, indices):
    for ind in indices:
        if ind in reb_index_mappings:
            r = client.options(ignore_status=400).indices.create(
                **reb_index_mappings[ind]
            )
            print(r)
