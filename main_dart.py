#!/usr/bin/env python

import argparse

from dart_fetch_data import dart_fetch_corp_code, dart_fetch_all_corp_data, dart_fetch_corp_info, dart_fetch_kospi200
from helpers import query_corp_code_list, delete_documents
from dart_post_data import dart_create_index, esclient, dart_post_corp_code, dart_post_all_corp_data, dart_post_corp_info, \
    dart_post_kospi200


def main():
    indices = ['corp_code', 'corp_data', 'corp_info', 'kospi200', 'reb1']
    parser = argparse.ArgumentParser(description='dart importer')
    parser.add_argument(
        '--create-index',
        help='Create ElasticSearch Index. Example: ./dart_post_data.py --create-index corp_code corp_data',
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

    if len(args.create_index) > 0:
        dart_create_index(esclient, args.create_index)

    if len(args.delete_documents):
        ans = input("WARNING: Delete all data? Type 'delete' to proceed.\nYour choice: ")
        if ans.strip().lower() == 'delete':
            delete_documents(esclient, args.delete_documents)
        else:
            print('Cancelled.')

    # corp_code
    if 'corp_code' in args.fetch:
        dart_fetch_corp_code()

    if 'corp_code' in args.post:
        dart_post_corp_code(esclient)

    # corp_code
    if 'corp_info' in args.fetch:
        dart_fetch_corp_info(query_corp_code_list(esclient))

    if 'corp_info' in args.post:
        dart_post_corp_info(esclient)

    # corp_data
    if 'corp_data' in args.fetch:
        dart_fetch_all_corp_data(esclient)

    if 'corp_data' in args.post:
        dart_post_all_corp_data(esclient)

    # corp_data
    if 'kospi200' in args.fetch:
        dart_fetch_kospi200()

    if 'kospi200' in args.post:
        dart_post_kospi200(esclient)


    # # 삼성전자
    # data = get_corp_info_from_dart('00126380', list(range(2021, 2023)))
    # analyze_corp_info(data)
    # elastic_session.close()


if __name__ == '__main__':
    main()
