#! /bin/bash

docker rm -f pq-spark
docker run -it --cap-add=SYS_ADMIN --entrypoint /bin/bash --cap-add=SYS_ADMIN --name pq-spark pq-spark
#docker run -it -u 0 --cap-add=SYS_ADMIN --entrypoint /bin/bash --cap-add=SYS_ADMIN --name pq-spark pq-spark
