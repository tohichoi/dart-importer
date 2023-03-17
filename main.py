#!/usr/bin/env python

import argparse

from fetch_data import fetch_corp_code, fetch_all_corp_data
from post_data import create_index, esclient, delete_documents, post_corp_code


def main():
    indices = ['corp_code', 'corp_data']
    parser = argparse.ArgumentParser(description='dart importer')
    parser.add_argument(
        '--create-index',
        help='Create ElasticSearch Index. Example: ./post_data.py --create-index corp_code corp_data',
        choices=indices, nargs="+", default=[])
    parser.add_argument(
        '--delete-documents', help='Delete all documents',
        choices=indices, nargs="+", default=[])
    parser.add_argument(
        '--fetch-data', help='Fetch data',
        choices=indices, nargs="+", default=[])
    parser.add_argument(
        '--post-data', help='Post data',
        choices=indices, nargs="+", default=[])
    # parser.add_argument(
    #     '--import-corp-data', help='Import corp data(filings, ...)', action='store_true')

    args = parser.parse_args()

    if len(args.create_index) > 0:
        create_index(esclient, args.create_index)

    if len(args.delete_documents):
        ans = input("WARNING: Delete all data? Type 'delete' to proceed.\nYour choice: ")
        if ans.strip().lower() == 'delete':
            delete_documents(esclient, args.delete_documents)
        else:
            print('Cancelled.')

    if 'corp_code' in args.fetch_data:
        fetch_corp_code()

    if 'corp_code' in args.post_data:
        post_corp_code(esclient)

    if 'corp_data' in args.fetch_data:
        fetch_all_corp_data(esclient)

    if 'corp_data' in args.post_data:
        post_all_corp_data(esclient)

    # # 삼성전자
    # data = get_corp_info_from_dart('00126380', list(range(2021, 2023)))
    # analyze_corp_info(data)
    # elastic_session.close()


if __name__ == '__main__':
    main()
