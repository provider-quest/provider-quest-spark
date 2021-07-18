#! /bin/sh

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest multiaddrs-ips
mkdir -p dist/multiaddrs-ips-latest

if [ -f output/multiaddrs_ips/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls output/multiaddrs_ips/json_latest_subset/part*.json | head -1)
  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    multiaddrsIps: map({ \
      miner: .miner, \
      maddr: .maddr, \
      peerId: .peerId, \
      ip: .ip, \
      epoch: .[\"last(epoch)\"], \
      timestamp: .[\"last(timestamp)\"], \
      chain: .[\"last(chain)\"], \
      dht: .[\"last(dht)\"] \
    } | to_entries | [(.[] | select(.value != null))] | from_entries) \
  }" > dist/multiaddrs-ips-latest/multiaddrs-ips-latest.json
fi

(cd dist/multiaddrs-ips-latest; head multiaddrs-ips-latest.json; hub bucket push -y)
