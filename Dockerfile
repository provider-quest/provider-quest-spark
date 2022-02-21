FROM ubuntu

RUN useradd ubuntu

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata
RUN apt install -y python3 vim less openjdk-11-jre wget tmux curl psmisc htop rsync build-essential

RUN curl -fsSL https://deb.nodesource.com/setup_17.x | bash -
RUN apt install -y nodejs

WORKDIR /opt/spark
RUN wget -q https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
RUN tar xf spark-3.2.1-bin-hadoop3.2.tgz

RUN mkdir /home/ubuntu

WORKDIR /home/ubuntu/provider-quest-spark

COPY . .

RUN chown -R ubuntu. /home/ubuntu

USER ubuntu

RUN mkdir -p node_modules
RUN npm install

ENTRYPOINT ["tail", "-f", "/dev/null"]
