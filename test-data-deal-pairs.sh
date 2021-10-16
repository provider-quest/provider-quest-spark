#! /bin/bash

(
  cd ../work/output/deals/by_pairs/by_verified/aggr_hourly/json

  for d in `ls | grep 'date=' | sort | tail -14`; do
    find $d -name '*.json' | xargs cat
  done | jq "{ \
	  	start: .window.start, \
		client: .clientProvider | split(\"-\")[0], \
		provider: .clientProvider | split(\"-\")[1], \
		verified: .verifiedDeal, \
		count: .count, \
		size: .[\"avg(pieceSizeDouble)\"] \
		}" | jq -s
)
