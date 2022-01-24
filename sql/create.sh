#! /bin/bash

if [ -z "$PGPASSWORD" ]; then
  echo Please set PGPASSWORD
  exit 1
fi

psql --host 144.217.233.191 -U postgres -d postgres -p 5432 -f create-provider-power-daily.sql 

