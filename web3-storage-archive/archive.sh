#! /bin/bash

. ../.env

#DIR=ips-baidu
#echo node put-files.js --token=\$WEB3_STORAGE_API_TOKEN ../estuary-archive/$DIR
#node put-files.js --token=$WEB3_STORAGE_API_TOKEN ../estuary-archive/$DIR

# too many open files
#echo node put-files.js --token=\$WEB3_STORAGE_API_TOKEN ../estuary-archive
#node put-files.js --token=$WEB3_STORAGE_API_TOKEN ../estuary-archive

for d in ls ../estuary-archive-2/*; do
  if [ -d "$d" ]; then
    echo ">>" node put-files.js --token=\$WEB3_STORAGE_API_TOKEN $d
    node put-files.js --token=$WEB3_STORAGE_API_TOKEN $d
  fi
done
