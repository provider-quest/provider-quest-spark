#! /bin/bash

POD_UID=$(kubectl get pod pq-spark -o json | jq -r .metadata.uid)
rsync -vaP root@nuc2-wired:/var/lib/kubelet/pods/$POD_UID/volumes/kubernetes.io~csi/pvc-31e42aed-d3f7-48d2-b55d-4613dedb6f1a/mount/provider-quest-spark/* .
