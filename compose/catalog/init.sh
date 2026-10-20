#!/bin/sh
# Idempotent catalog bootstrap: bucket in RustFS, Lakekeeper bootstrap, `keystone` warehouse.
set -eu
S3=http://rustfs:9000
LK=http://lakekeeper:8181

until curl -fs "$LK/health" >/dev/null; do sleep 1; done
until curl -s -o /dev/null "$S3/"; do sleep 1; done

# PUT bucket with SigV4 (curl >= 7.75). 200 = created, 409 = already there.
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT --aws-sigv4 "aws:amz:local-01:s3" \
  --user "$S3_ACCESS_KEY:$S3_SECRET_KEY" "$S3/keystone")
echo "bucket keystone: $code"
case "$code" in 200|409) ;; *) exit 1 ;; esac

# Bootstrap answers 4xx once it has already run.
curl -s -X POST "$LK/management/v1/bootstrap" -H 'Content-Type: application/json' \
  -d '{"accept-terms-of-use": true}' -o /dev/null -w 'bootstrap: %{http_code}\n'

if curl -fs "$LK/management/v1/warehouse" | grep -q '"name":"keystone"'; then
  echo "warehouse keystone: exists"
  exit 0
fi
curl -fs -X POST "$LK/management/v1/warehouse" -H 'Content-Type: application/json' -d "{
  \"warehouse-name\": \"keystone\",
  \"storage-profile\": {
    \"type\": \"s3\", \"bucket\": \"keystone\", \"key-prefix\": \"warehouse\",
    \"endpoint\": \"$S3\", \"region\": \"local-01\", \"path-style-access\": true,
    \"flavor\": \"s3-compat\", \"sts-enabled\": false
  },
  \"storage-credential\": {
    \"type\": \"s3\", \"credential-type\": \"access-key\",
    \"aws-access-key-id\": \"$S3_ACCESS_KEY\", \"aws-secret-access-key\": \"$S3_SECRET_KEY\"
  }
}"
echo
echo "warehouse keystone: created"
