#! /bin/bash

docker run -it --cap-add=SYS_ADMIN --entrypoint /bin/bash --cap-add=SYS_ADMIN --name pq-spark pq-spark
