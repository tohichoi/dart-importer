#!/usr/bin/env python

import argparse
import sys
from pathlib import Path

import pendulum

from config import config
from fetch_data import reb_fetch_getRealEstateTradingCount, reb_preprocess_getRealEstateTradingCount
from helpers import reb_load_region_codes, esclient, logger
from post_data import reb_create_index, reb_post_getRealEstateTradingCount


def main():
    indices = {
        'getRealEstateTradingCount': '조사일자, 지역코드, 거래유형 값을 이용하여 부동산 거래 건수 정보를 제공'
    }

    now = pendulum.now(tz="Asia/Seoul")
    # gte = now.subtract(months=1).start_of('month').format('YYYYMM')
    gte = now.start_of('month').format('YYYYMM')
    lte = now.start_of('month').format('YYYYMM')
    parser = argparse.ArgumentParser(description='reb importer')
    parser.add_argument('--create-index',
                        help='Create ElasticSearch Index. Example: ./main_reb.py --create-index getRealEstateTradingCount',
                        choices=indices, nargs="+", default=[])
    parser.add_argument('--delete-documents', help='Delete all documents', choices=indices, nargs="+", default=[])
    parser.add_argument('--fetch', help='Fetch data', choices=indices, nargs="+", default=[])
    parser.add_argument('--post', help='Post data', choices=indices, nargs="+", default=[])
    parser.add_argument('--in-dir', help='Input directory', type=str)
    parser.add_argument('--preprocess', help='Preprocess data', choices=indices, nargs="+", default=[])
    parser.add_argument('--gte', help='Begin date (YYYYMM)', type=str, default=gte)
    parser.add_argument('--lte', help='End date (YYYYMM)', type=str, default=lte)
    parser.add_argument(
        '--out-dir', help='Output directory. default is <index_name>/<gte>-<lte>', nargs=1, default=None)
    # parser.add_argument(
    #     '--import-corp-data', help='Import corp data(filings, ...)', action='store_true')

    args = parser.parse_args()

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
        args.out_dir.mkdir(exist_ok=True)

    reb_load_region_codes(Path(config['REB_RESULT_DIR']) / Path(config['REG_REGION_CODES_FILE']))

    if len(args.create_index) > 0:
        reb_create_index(esclient, args.create_index)

    if 'getRealEstateTradingCount' in args.fetch:
        reb_fetch_getRealEstateTradingCount(args.outdir, args.gte, args.lte)

    if 'getRealEstateTradingCount' in args.post:
        in_dir = Path(args.in_dir)
        if not in_dir.exists():
            logger.error('--in-dir error')
            sys.exit(1)
        reb_post_getRealEstateTradingCount(esclient, in_dir)

    if 'getRealEstateTradingCount' in args.preprocess:
        reb_preprocess_getRealEstateTradingCount()

    # if 'reb1' in args.post:
    #     post_kospi200(esclient)


if __name__ == '__main__':
    main()
