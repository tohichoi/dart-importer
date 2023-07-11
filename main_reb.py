#!/usr/bin/env python

import argparse
import sys
from pathlib import Path

import pendulum

from config import config
from reb_fetch_data import reb_preprocess_getRealEstateTradingCount, reb_fetch_getRealEstateTradingCount, \
    reb_fetch_getRealEstateTradingCountBuildType, reb_fetch_getAptRealTradingPriceIndex, \
    reb_fetch_getAptRealTradingPriceIndexSize
from helpers import reb_load_region_codes, esclient, logger, delete_documents
from reb_post_data import reb_post_getRealEstateTradingCount, reb_create_index, \
    reb_post_getRealEstateTradingCountBuildType, reb_post_getAptRealTradingPriceIndex, \
    reb_post_getAptRealTradingPriceIndexSize, reb_index_mappings


def main():
    indices = {
        'getRealEstateTradingCount': '조사일자, 지역코드, 거래유형 값을 이용하여 부동산 거래 건수 정보를 제공',
        'getRealEstateTradingCountBuildType': '건물유형별 부동산 거래 건수 조회',
        'getAptRealTradingPriceIndex': '공동주택 실거래가격지수 통계 조회 서비스',
        'getAptRealTradingPriceIndexSize': '공동주택 실거래가격지수 통계 조회 서비스',
    }

    now = pendulum.now(tz="Asia/Seoul")
    # gte = now.subtract(months=1).start_of('month').format('YYYYMM')
    gte = now.start_of('month').format('YYYYMM')
    lte = now.start_of('month').format('YYYYMM')
    parser = argparse.ArgumentParser(description='reb importer')
    parser.add_argument('--create-index',
                        help='Create ElasticSearch Index. Example: ./main_reb.py --create-index getRealEstateTradingCount',
                        choices=indices.keys(), nargs="+", default=[])
    parser.add_argument('--delete-documents', help='Delete all documents', nargs="+", default=[])
    parser.add_argument('--fetch', help='Fetch data', choices=indices, nargs="+", default=indices.keys())
    parser.add_argument('--post', help='Post data', choices=indices, nargs="+", default=indices.keys())
    parser.add_argument('--in-dir', help='Input directory', type=str)
    parser.add_argument('--preprocess', help='Preprocess data', choices=indices, nargs="+", default=[])
    parser.add_argument('--gte', help='Begin date (YYYYMM)', type=str, default=gte)
    parser.add_argument('--lte', help='End date (YYYYMM)', type=str, default=lte)
    parser.add_argument(
        '--out-dir', help='Output directory. default is <index_name>/<gte>-<lte>', type=str, default=None)
    # parser.add_argument(
    #     '--import-corp-data', help='Import corp data(filings, ...)', action='store_true')

    args = parser.parse_args()

    if len(args.delete_documents):
        ans = input("WARNING: Delete all data? Type 'delete' to proceed.\nYour choice: ")
        if ans.strip().lower() == 'delete':
            indices = [reb_index_mappings[ind]['index'] for ind in args.delete_documents]
            delete_documents(esclient, indices)
        else:
            print('Cancelled.')

    if args.gte or args.lte:
        try:
            pendulum.from_format(args.gte, 'YYYYMM')
            pendulum.from_format(args.lte, 'YYYYMM')
        except ValueError as e:
            logger.error(f'Invalid date format(YYYYMM) : {args.gte} - {args.lte}')
            sys.exit(1)

    if not args.out_dir:
        args.out_dir = Path(config['REB_RESULT_DIR']) / Path('getRealEstateTradingCount') / Path(
            f'{args.gte}-{args.lte}')
        args.out_dir.mkdir(exist_ok=True, parents=True)

    reb_load_region_codes(Path(config['REB_RESULT_DIR']) / Path(config['REG_REGION_CODES_FILE']))

    if len(args.create_index) > 0:
        reb_create_index(esclient, args.create_index)

    if 'getRealEstateTradingCount' in args.fetch:
        reb_fetch_getRealEstateTradingCount(args.out_dir, args.gte, args.lte)

    if 'getRealEstateTradingCount' in args.post:
        if not args.in_dir:
            logger.error('--in-dir error')
            sys.exit(1)

        in_dir = Path(args.in_dir)
        if args.post is None or not in_dir.exists():
            logger.error('--in-dir error')
            sys.exit(1)
        reb_post_getRealEstateTradingCount(esclient, in_dir)

    if 'getRealEstateTradingCountBuildType' in args.fetch:
        reb_fetch_getRealEstateTradingCountBuildType(args.out_dir, args.gte, args.lte)

    if 'getRealEstateTradingCountBuildType' in args.post:
        in_dir = Path(args.in_dir)
        if not in_dir.exists():
            logger.error('--in-dir error')
            sys.exit(1)
        reb_post_getRealEstateTradingCountBuildType(esclient, in_dir)

    if 'getAptRealTradingPriceIndex' in args.fetch:
        reb_fetch_getAptRealTradingPriceIndex(args.out_dir, args.gte, args.lte)

    if 'getAptRealTradingPriceIndex' in args.post:
        in_dir = Path(args.in_dir)
        if not in_dir.exists():
            logger.error('--in-dir error')
            sys.exit(1)
        reb_post_getAptRealTradingPriceIndex(esclient, in_dir)

    if 'getAptRealTradingPriceIndexSize' in args.fetch:
        reb_fetch_getAptRealTradingPriceIndexSize(args.out_dir, args.gte, args.lte)

    if 'getAptRealTradingPriceIndexSize' in args.post:
        in_dir = Path(args.in_dir)
        if not in_dir.exists():
            logger.error('--in-dir error')
            sys.exit(1)
        reb_post_getAptRealTradingPriceIndexSize(esclient, in_dir)

    if 'getRealEstateTradingCount' in args.preprocess:
        reb_preprocess_getRealEstateTradingCount()



if __name__ == '__main__':
    main()
