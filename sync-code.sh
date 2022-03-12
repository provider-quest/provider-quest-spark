#! /bin/bash

POD_UID=$(kubectl -n argo get pod spark -o json | jq -r .metadata.uid)
PVC=$(kubectl -n argo get pvc spark -o json | jq -r .spec.volumeName)
rsync -vaP --exclude node_modules --exclude sync-code.sh root@nuc2-wired:/var/lib/kubelet/pods/$POD_UID/volumes/kubernetes.io~csi/$PVC/mount/provider-quest-spark/* .
