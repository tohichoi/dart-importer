import logging
import unittest
import zipfile
from pathlib import Path
from unittest import TestCase

import config
from post_data import esclient, post_year_corp_data, post_quarter_corp_data, post_corp_code
from config import ELASTIC_PASSWORD, ELASTIC_CERTFILE_FINGERPRINT, ELASTICSEARCH_URL, DART_CORPCODE_DATA_FILE
from fetch_data import fetch_one_corp_data, fetch_corp_data, fetch_corp_code
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
import sys

from manage_dart_file import DartFileManager, DartFileManagerEx


class TestDartFileCleaner(TestCase):
    def test(self):
        # data_dir = sys.argv[1]
        # dry_run = int(sys.argv[2])
        data_dir = 'data/dart'
        dry_run = 1
        dfc = DartFileCleanerEx(data_dir=data_dir)
        dfc.clean_all_data(dry_run=dry_run)


class TestFetchCorpCode(TestCase):
    def test(self):
        fetch_corp_code()
        self.assertTrue(Path(DART_CORPCODE_DATA_FILE).exists())

        zf = zipfile.ZipFile(DART_CORPCODE_DATA_FILE)
        self.assertTrue('CORPCODE.xml' in zf.namelist())


class TestPostCorpCode(TestCase):
    def test(self):
        n = post_corp_code(esclient)
        self.assertGreater(n, 0)


class TestElasticCount(TestCase):
    def test(self):
        resp = esclient.count(index='corp_code')
        print(resp)


class TestFetchOneCorpData(TestCase):
    def setUp(self) -> None:
        self.esclient = esclient
        self.corp_code = "00126380"
        self.corp_name = '삼성전자'

    def test(self):
        # years = list(range(2017, 2023))
        years = list(range(2017, 2023))
        corp_data = fetch_one_corp_data(esclient, self.corp_code, self.corp_name, years)
        self.assertEqual(len(corp_data), len(years))
        for k, ydata in corp_data.items():
            self.assertEqual(len(ydata), 4)
            for qdata in ydata:
                self.assertTrue('status' in qdata)
                self.assertTrue('message' in qdata)
                # "list" 는 없을 수도 있음
                # self.assertTrue('list' in qdata)


class Test(TestCase):
    def setUp(self):
        # Create the client instance
        # Password for the 'elastic' user generated by Elasticsearch
        # ELASTIC_PASSWORD = "<password>"
        # self.esclient = Elasticsearch(
        #     ELASTICSEARCH_URL,
        #     # ca_certs=ELASTIC_CERTFILE,
        #     ssl_assert_fingerprint=ELASTIC_CERTFILE_FINGERPRINT,
        #     basic_auth=("elastic", ELASTIC_PASSWORD)
        # )
        self.esclient = esclient
        self.corp_code = "00126380"
        self.corp_name = '삼성전자'

    def test_get_corp_quarter_info_from_dart(self):
        years = [2022]
        data = fetch_corp_data(self.corp_code, years)
        # print(data)
        # self.assertIsNotNone(data)
        # data는 1Q~4Q
        self.assertEqual(len(data), len(years))

        year = years[0]
        n = post_quarter_corp_data(self.esclient, self.corp_code, data[year][0])
        self.assertGreaterEqual(n, 1)

    def test_get_corp_year_info_from_dart(self):
        years = [2022]
        corp_data = fetch_corp_data(self.corp_code, self.corp_name, years)
        # print(data)
        # self.assertIsNotNone(data)
        # data는 1Q~4Q
        self.assertEqual(len(corp_data), len(years))
        self.assertTrue(2022 in corp_data)
        ns = post_year_corp_data(self.esclient, self.corp_code, corp_data[2022])
        self.assertGreaterEqual(len(ns), 1)

    def test_import_one_corp_data(self):
        years = [2021, 2022]
        num_data = fetch_one_corp_data(self.esclient, self.corp_code, self.corp_name, years)
        self.assertGreaterEqual(len(num_data), 1)

    def test_elasticsearch_client(self):
        info = self.esclient.info()
        print(info)
        self.assertGreater(len(info), 0)

    def test_query_id(self):
        # 삼성잔자
        resp = self.esclient.get(index="corp_code", id=self.corp_code)
        # print(resp)
        # {'_index': 'corp_code', '_id': '00126380', '_version': 1, '_seq_no': 288931, '_primary_term': 1, 
        # 'found': True, 
        # '_source': {'code': '00126380', 'corp_name': '삼성전자', 'stock_code': '005930', 'modify_date': '20230110'}}
        self.assertEqual(resp['_source']['corp_code'], self.corp_code)
        self.assertEqual(resp['_source']['corp_name'], self.corp_name)

    def test_query_all_docs(self):
        logging.disable(sys.maxsize)  # Python 3
        n = 0
        for doc in scan(self.esclient, query={"query": {"match_all": {}}}, index="corp_code"):
            if n == 0:
                print(doc)
            n += 1
        logging.disable(logging.NOTSET)
        self.assertEqual(n, 97568)


class TestDFM(TestCase):
    def setUp(self) -> None:
        self.dfm_load = DartFileManager(data_dir='data/dart-testing', corp_code="00126380", corp_name='삼성전자',
                                        data_file_prefix='financial-statements')

        self.dfm_save = DartFileManager(data_dir='data/dart-testing', corp_code="00126380", corp_name='삼성전자-testing',
                                        data_file_prefix='financial-statements')

    def test_load(self):
        ysdata = self.dfm_load.load()
        self.assertEqual(list(ysdata.keys())[0], 2022)
        self.assertEqual(len(ysdata[2022]), 4)

    def test_save(self):
        ysdata = self.dfm_load.load()
        self.dfm_save.save(ysdata)
        zf = Path(self.dfm_save._zipfile)
        self.assertTrue(zf.exists())
        self.assertTrue(zf.lstat().st_size > 1024)


if __name__ == '__main__':
    unittest.main()