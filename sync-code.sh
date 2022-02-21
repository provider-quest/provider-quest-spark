#! /bin/bash

POD_UID=$(kubectl get pod pq-spark -o json | jq -r .metadata.uid)
PVC=$(kubectl get pvc pq-spark -o json | jq -r .spec.volumeName)
rsync -vaP --exclude node_modules root@nuc2-wired:/var/lib/kubelet/pods/$POD_UID/volumes/kubernetes.io~csi/$PVC/mount/provider-quest-spark/* .
