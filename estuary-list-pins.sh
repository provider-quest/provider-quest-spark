#! /bin/bash

. .env

curl -H "Authorization: Bearer $ESTUARY_TOKEN" 'https://api.estuary.tech/pinning/pins' | jq | less
