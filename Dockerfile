FROM ubuntu

RUN useradd ubuntu

RUN apt update
RUN apt install -y python3 vim less openjdk-11-jre wget

WORKDIR /opt/spark
RUN wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
RUN tar xf spark-3.2.1-bin-hadoop3.2.tgz

USER ubuntu

WORKDIR /home/ubuntu

RUN mkdir tmp
RUN mkdir work

WORKDIR /home/ubuntu/provider-quest-spark

COPY . .

ENTRYPOINT ["tail", "-f", "/dev/null"]
