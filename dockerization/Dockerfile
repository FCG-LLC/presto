FROM portus.cs.int:5000/prod/cs-base-java
MAINTAINER lm@cs.com
ARG destEnv

RUN locale-gen en_US.UTF-8
RUN update-locale LANG=en_US.UTF-8

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y \
    paxctl \
    pax-utils

RUN groupadd -g 10023 presto
RUN adduser --no-create-home --system --shell /bin/false --gecos 'presto dedicated user' --uid 10023 --gid 10023 --disabled-password presto

RUN mkdir -p /presto-server/etc/catalog

WORKDIR /presto-server

ADD files/etc/catalog/hyena.properties etc/catalog
ADD files/etc/config.properties etc/
ADD files/etc/jvm.config etc/
ADD files/etc/log.properties etc/
ADD files/etc/node.properties etc/

ADD *.deb /
RUN dpkg --force-all -i /*.deb

#RUN apt-get install presto=*$destEnv

RUN paxctl -c /usr/lib/jvm/java-8-oracle/jre/bin/java && paxctl -m /usr/lib/jvm/java-8-oracle/jre/bin/java
