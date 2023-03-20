#!/usr/bin/env python
import json
import os
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path
import logging
import argparse

logging.getLogger().setLevel(logging.DEBUG)


class DartFileManager:
    def __init__(self, **kwargs):
        """

        Args:
            **kwargs:
                data_dir :
                corp_code :
                corp_name :
                logger : None
        """

        self.config = kwargs

    @property
    def _corp_dir(self):
        return Path(f'{self.config["data_dir"]}/corp_data/{self.config["corp_code"]}-{self.config["corp_name"]}/')

    @property
    def _prefix(self):
        return self.config['data_file_prefix']

    @property
    def _zipfile(self):
        return f'{self._corp_dir}/{self._prefix}-{self.config["corp_code"]}-{self.config["corp_name"]}.zip'

    @property
    def logger(self):
        return self.config['logger']

    def get_filelist(self):
        p = Path(self._corp_dir)
        fl1 = list(p.glob('*.json'))
        if len(fl1) > 0:
            return fl1

        fl2 = list(p.glob('*.zip'))
        if len(fl2) > 0:
            zf = zipfile.ZipFile(fl2[0])
            return zf.namelist()

        return None

    def has_year_data(self, year):
        try:
            zf = zipfile.ZipFile(self._zipfile, mode='r')
        except FileNotFoundError:
            return False

        for f in zf.namelist():
            m = re.match(self._prefix + r'-([0-9]{4})-([1-4])Q.json', f)
            if m:
                y = int(m.group(1))
                if year == y:
                    return True
        return False

    def has_quarter_data(self, quarter):
        zf = zipfile.ZipFile(self._zipfile, mode='r')
        for f in zf.namelist():
            m = re.match(self._prefix + r'-([0-9]{4})-([1-4])Q.json', f)
            if m:
                q = int(m.group(2))
                if quarter == q:
                    return True
        return False

    def load(self):
        cd = self._corp_dir
        if not cd.exists():
            # logger.warning(f'{p.name} exists. Skipping fetching.')
            # raise FileNotFoundError(f'File {self._corp_dir} not found')
            if self.logger:
                self.logger.error(f'File {self._corp_dir} not found')
            return None

        if not Path(self._zipfile).exists():
            # raise FileNotFoundError(f'File {self._zipfile} not found')
            if self.logger:
                self.logger.error(f'File {self._zipfile} not found')
            return None

        corp_data = defaultdict(list)
        zf = zipfile.ZipFile(self._zipfile, mode='r')
        for f in zf.namelist():
            m = re.match(self._prefix + r'-([0-9]{4})-([1-4]Q).json', f)
            if m:
                year = int(m.group(1))
                with zf.open(f) as fd:
                    corp_data[year].append(fd.read().decode())
        zf.close()
        return corp_data

    def save(self, corp_data: dict):
        cd = self._corp_dir
        if not cd.exists():
            os.makedirs(cd)

        prefix = self.config['data_file_prefix']
        zf = zipfile.ZipFile(self._zipfile, mode='w', compression=zipfile.ZIP_BZIP2, compresslevel=9)
        for year, ydata in corp_data.items():
            for i, qdata in enumerate(ydata):
                f = cd.joinpath(f'{prefix}-{year}-{i + 1}Q.json')
                with open(f, 'w') as fd:
                    fd.write(json.dumps(qdata))
                zf.write(f, arcname=f.name)
                f.unlink()
        zf.close()

        return self._zipfile


class DartFileManagerEx(DartFileManager):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _extract_config(self, zp):
        # dart/corp_data/<corp_code>-<corp_name>/financial_statements-([0-9]+)-[^\.]+.zip
        m = re.match(r".*/([^0-9]+)-([0-9]+)-([^\.]+)\.zip", zp)
        if m:
            self.config.update({'data_file_prefix': m.group(1)})
            self.config.update({'corp_code': m.group(2)})
            self.config.update({'corp_name': m.group(3)})
            return True
        return False

    def is_valid(self):
        corp_data = self.load()
        if corp_data:
            return self._is_valid_corp_data(self.load())
        return False

    def _is_valid_corp_data(self, corp_data):
        status = []
        n = 0
        for year, ydata in corp_data.items():
            n += len(ydata)
            for qdata in ydata:
                qjdata = json.loads(qdata)
                # https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020
                # 000 :정상
                # 013 :조회된 데이타가 없습니다.
                status.append(1 if qjdata['status'] in ['000', '013'] else 0)

        return sum(status) == n

    def clean_all_data(self, dry_run=1):
        r = Path(self.config['data_dir'])
        for zp in r.rglob('*.zip'):
            if not self._extract_config(str(zp)):
                continue
            corp_data = self.load()

            if not self._is_valid_corp_data(corp_data):
                logging.info(f'INVALID : {self.config["corp_code"]}:{self.config["corp_name"]}')
                if not dry_run:
                    zp.unlink()
                    zp.parent.rmdir()
            logging.info(f'VALID : {self.config["corp_code"]}:{self.config["corp_name"]}')


def clean_corp_info(data_dir, dry_run):
    r = Path(data_dir + 'corp_info')
    print(f'Data dir : {r}')
    invalid_files = []
    if not r.is_dir():
        print(f'Invalid dir : {r}')
        return

    files = list(r.glob('*.json'))
    if len(files) < 1:
        print('File not found')
        return

    for f in files:
        with open(f) as fd:
            data = json.load(fd)
            if data['status'] not in ['000', '013']:
                invalid_files.append(f)
                print(f'INVALID: {f} : {data["status"]}')
            else:
                print(f'VALID: {f} : {data["status"]}')

    if not dry_run:
        for f in invalid_files:
            f.unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='dart file manager')
    indices = ['corp_data', 'corp_info']
    parser.add_argument('--clean', help='Clean corp_data', choices=indices, nargs="+", default=[], required=True)

    parser.add_argument('--data-dir', type=str, required=True, help='Specifiy data dir', default='data/dart')

    parser.add_argument('--dry-run', action='store_true', default=True)
    args = parser.parse_args()
    logging.info(args)
    data_dir = args.data_dir
    dry_run = args.dry_run

    if 'corp_data' in args.clean:
        dfc = DartFileManagerEx(data_dir=data_dir)
        dfc.clean_all_data(dry_run=dry_run)

    if 'corp_info' in args.clean:
        clean_corp_info(data_dir, dry_run)

