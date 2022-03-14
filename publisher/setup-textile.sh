#! /bin/bash

if [ ! -f ~/.textile/auth.yml ]; then
	mkdir -p ~/.textile
	cat <<EOF > ~/.textile/auth.yml
api: api.hub.textile.io:443
apikey: ""
apiminerindex: api.minerindex.hub.textile.io:443
apisecret: ""
identity: ""
org: ""
session: $(echo $TEXTILE_SESSION)
token: ""
EOF
fi
