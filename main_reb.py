#!/usr/bin/env python

import argparse
from pathlib import Path

from config import config
from fetch_data import fetch_reb_getRealEstateTradingCount
from helpers import reb_load_region_codes


def main():
    indices = {
        'getRealEstateTradingCount': '조사일자, 지역코드, 거래유형 값을 이용하여 부동산 거래 건수 정보를 제공'
    }

    parser = argparse.ArgumentParser(description='reb importer')
    parser.add_argument(
        '--create-index',
        help='Create ElasticSearch Index. Example: ./main_reb.py --create-index getRealEstateTradingCount',
        choices=indices, nargs="+", default=[])
    parser.add_argument(
        '--delete-documents', help='Delete all documents',
        choices=indices, nargs="+", default=[])
    parser.add_argument(
        '--fetch', help='Fetch data',
        choices=indices, nargs="+", default=[])
    parser.add_argument(
        '--post', help='Post data',
        choices=indices, nargs="+", default=[])
    # parser.add_argument(
    #     '--import-corp-data', help='Import corp data(filings, ...)', action='store_true')

    args = parser.parse_args()

    reb_load_region_codes(Path(config['REB_RESULT_DIR']) / Path(config['REG_REGION_CODES_FILE']))

    # corp_data
    if 'getRealEstateTradingCount' in args.fetch:
        fetch_reb_getRealEstateTradingCount()

    # if 'reb1' in args.post:
    #     post_kospi200(esclient)


if __name__ == '__main__':
    main()