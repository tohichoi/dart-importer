#!/bin/bash

KIBANA_HOST="http://www.samjungenr.com:5609"
echo $KIBANA_API_KEY

# shellcheck disable=SC2016
curl -X POST "$KIBANA_HOST/api/data_views/data_view" \
--header 'Content-Type: application/json;charset=UTF-8' \
--header 'kbn-xsrf: true' \
--header "Authorization: ApiKey $KIBANA_API_KEY" \
-d @kibana_custom_label_corp_data.json 