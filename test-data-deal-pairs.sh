#! /bin/bash

(
  cd ../work/output/deals/by_pairs/by_verified/aggr_hourly/json

  find date\=2021-10-0* -name '*.json' | 
	  xargs cat | 
	  jq "{ \
	  	start: .window.start, \
		client: .clientProvider | split(\"-\")[0], \
		provider: .clientProvider | split(\"-\")[1], \
		verified: .verifiedDeal, \
		count: .count, \
		size: .[\"avg(pieceSizeDouble)\"] \
		}" | jq -s
)
