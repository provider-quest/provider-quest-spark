#! /bin/bash

#POD=spark
#PVC_NAME=spark
#POD_UID=$(kubectl -n argo get pod $POD -o json | jq -r .metadata.uid)
#PVC=$(kubectl -n argo get pvc spark -o json | jq -r .spec.volumeName)
#rsync -vaP --exclude node_modules --exclude sync-code.sh root@nuc2-wired:/var/lib/kubelet/pods/$POD_UID/volumes/kubernetes.io~csi/$PVC/mount/provider-quest-spark/* .

POD=publisher-test-pod
POD_UID=$(kubectl -n argo get pod $POD -o json | jq -r .metadata.uid)
rsync -vaP --exclude node_modules --exclude sync-code.sh root@nuc2-wired:/var/lib/kubelet/pods/$POD_UID/volumes/kubernetes.io~empty-dir/work-vol/provider-quest-spark/* .

