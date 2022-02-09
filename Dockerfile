FROM ubuntu

RUN useradd ubuntu

RUN apt update
RUN apt install -y python3 vim less openjdk-11-jre wget

WORKDIR /opt/spark
RUN wget -q https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
RUN tar xf spark-3.2.1-bin-hadoop3.2.tgz

RUN mkdir /home/ubuntu
RUN chown ubuntu. /home/ubuntu

USER ubuntu

WORKDIR /home/ubuntu

RUN pwd
RUN ls -l
RUN mkdir -p tmp
RUN mkdir -p work

WORKDIR /home/ubuntu/provider-quest-spark

COPY . .

ENTRYPOINT ["tail", "-f", "/dev/null"]
