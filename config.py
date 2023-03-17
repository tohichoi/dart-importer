import os

from dotenv import dotenv_values

config = {
    # **dotenv_values("../docker-elk/.env"),  # load shared development variables
    **dotenv_values(".env"),  # load sensitive variables
    **os.environ,  # override loaded values with environment variables
}
DART_API_KEY = config['DART_API_KEY']
DART_RESULT_DIR = config['DART_RESULT_DIR']
DART_CORPCODE_DATA_FILE = f'{DART_RESULT_DIR}/corp-code.zip'
ELASTIC_USER = config['ELASTIC_USER']
ELASTIC_PASSWORD = config['ELASTIC_PASSWORD']
ELASTIC_CERTFILE = config['ELASTIC_CERTFILE']
ELASTIC_CERTFILE_FINGERPRINT = config['ELASTIC_CERTFILE_FINGERPRINT']
ELASTICSEARCH_URL = config['ELASTICSEARCH_URL']

dart_base_params = {
    "crtfc_key": DART_API_KEY,
}
dart_params = {
    # 보고서 리스트
    "list": {
        # 검색시작 접수일자(YYYYMMDD)
        "bgn_de": None,
        "end_de": None,
        # 최종보고서만 검색여부(Y or N)
        "last_reprt_at": "N",
        # 공시유형 A : 정기공시
        "pblntf_ty": "A",
        # 접수일자: date
        # 회사명: crp
        # 보고서명: rpt
        # ※ 기본값: date
        "sort": "crp",
        # 오름차순(asc), 내림차순(desc)
        "sort_mth": "asc",
        # 페이지 번호(1~n) 기본값 : 1
        "page_no": "1",
        # 페이지당 건수(1~100) 기본값 : 10, 최대값 : 100
        "page_count": "100"
    },
    "corp_info": {
        # corp_code
    },

    # 단일회사 전체 재무제표
    "fnlttSinglAcntAll": {
        # bsns_year	사업연도	STRING(4)	Y	사업연도(4자리) ※ 2015년 이후 부터 정보제공
        'bsns_year': None,
        # 1분기보고서 : 11013
        # 반기보고서 : 11012
        # 3분기보고서 : 11014
        # 사업보고서 : 11011
        'reprt_code': "11011",
        # CFS:연결재무제표, OFS:재무제표
        'fs_div': 'OFS'
    }
}
QUARTER_CODES = ['11013', '11012', '11014', '11011']
