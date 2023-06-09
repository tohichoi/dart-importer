#!/usr/bin/env python
import os
import re
import zipfile
from collections import defaultdict
from pathlib import Path


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
        zf = zipfile.ZipFile(self._zipfile, mode='r')
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
                    fd.write(str(qdata))
                zf.write(f, arcname=f.name)
                f.unlink()
        zf.close()
