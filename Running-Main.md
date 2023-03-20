# Running Main

## Setup

### Getting source code

```
cd Workspace
mkdir dart
cd dart
git clone https://github.com/tohichoi/dart-importer
cd dart-importer
```

### virtualenv

```
virtualenv venv
source venv/bin/activate
```

### package install

```
pip install -r requirements.txt
```

### create .env

```
cp env.example .env

cd config/elastic/certs/es01
openssl x509 -noout -fingerprint -sha256 -inform pem -in es01.crt
# copy FingerPrint strings and paste it into .env
cd -
mkdir -p config/elastic
docker cp dart-importer_es01_1:/usr/share/elasticsearch/config/certs config/elastic/
```

## creating index

```
source ./venv/bin/activate
python main.py --create-index corp_code
python main.py --create-index corp_data
```

## fetching corp_code

```
# DART_RESULT_DIR=./data/dart in .env
mkdir -p data/dart
python main.py --fetch corp_code
```

## posting corp_code

```
python main.py --post corp_code
```

## fetching corp_data

```
python main.py --fetch corp_data
```