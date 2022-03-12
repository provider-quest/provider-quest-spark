FROM ubuntu

RUN useradd ubuntu

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata
RUN apt install -y python3 vim less openjdk-11-jre wget tmux curl psmisc htop rsync build-essential cowsay
RUN apt install -y gconf-service libasound2 libatk1.0-0 libatk-bridge2.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libexpat1 libfontconfig1 libgcc1 libgconf-2-4 libgdk-pixbuf2.0-0 libglib2.0-0 libgtk-3-0 libnspr4 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 ca-certificates fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils libgbm-dev libxkbcommon-dev libxdamage-dev libgtk-3-dev

RUN curl -fsSL https://deb.nodesource.com/setup_17.x | bash -
RUN apt install -y nodejs

WORKDIR /tmp
RUN wget -q https://github.com/textileio/textile/releases/download/v2.6.17/hub_v2.6.17_linux-amd64.tar.gz
RUN tar xf hub_v2.6.17_linux-amd64.tar.gz
WORKDIR /tmp/hub_v2.6.17_linux-amd64
RUN ./install
RUN rm -rf /tmp/hub*

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
