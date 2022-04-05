#! /bin/bash

docker rm -f pq-spark

if [ "$1" = "--root" ]; then
  docker run -it -u 0 --cap-add=SYS_ADMIN --entrypoint /bin/bash --cap-add=SYS_ADMIN --name pq-spark pq-spark
else
  docker run -it --cap-add=SYS_ADMIN --entrypoint /bin/bash --cap-add=SYS_ADMIN --name pq-spark pq-spark
fi
