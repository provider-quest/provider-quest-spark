#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

TARGET=$WORK_DIR/dist/multiaddrs-ips-latest
if [ ! -d $TARGET ]; then
        mkdir -p $TARGET
        cd $TARGET
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest multiaddrs-ips
if [ -f $MULTIADDRS_IPS_OUTPUT_DIR/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls $MULTIADDRS_IPS_OUTPUT_DIR/json_latest_subset/part*.json | head -1)
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
  }" > $TMP/multiaddrs-ips-latest.json
fi

(
  set -e

  cd $TARGET

  mv $TMP/multiaddrs-ips-latest.json .
  echo "multiaddrs-ips-latest.json:"
  head multiaddrs-ips-latest.json
)

