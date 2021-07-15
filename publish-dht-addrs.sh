#! /bin/sh

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest DHT addrs
mkdir -p dist/dht-addrs-latest
if [ -f output/dht_addrs/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls output/dht_addrs/json_latest_subset/part*.json | head -1)
  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    miners: map({ \
      key: .miner, value: { \
        epoch: .[\"last(epoch)\"], \
        timestamp: .[\"last(timestamp)\"], \
        collectedFrom: .[\"last(collectedFrom)\"], \
        peerId: .[\"last(peerId)\"], \
        multiaddrs: .[\"last(multiaddrs)\"], \
        dnsLookups: \
          (if .[\"last(dnsLookups)\"] then \
            .[\"last(dnsLookups)\"] | to_entries | map(.value = [.value | map(fromjson)]) | from_entries \
          else \
            null \
          end) \
      } | to_entries | [(.[] | select(.value != null))] | from_entries \
    }) | from_entries \
  }" > dist/dht-addrs-latest/dht-addrs-latest.json
fi
(cd dist/dht-addrs-latest; head dht-addrs-latest.json; hub bucket push -y)

