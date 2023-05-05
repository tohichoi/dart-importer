#/bin/bash

curl -X POST 'http://www.samjungenr.com:5609/api/data_views/corp_data' \
--header 'Content-Type: application/json;charset=UTF-8' \
--header 'kbn-xsrf: true' \
--header "Authorization: ApiKey $KIBANA_API_KEY" \
-d @kibana_custom_label_corp_data.json 