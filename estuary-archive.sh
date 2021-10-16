#! /bin/bash

cd estuary-archive-2
rsync -vaP ../input/* .
ipfs add -r .

