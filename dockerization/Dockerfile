FROM portus.cs.int:5000/prod/cs-base-java
LABEL maintainer "devops@collective-sense.com"

ARG destEnv
ARG debVer

RUN locale-gen en_US.UTF-8 && \
    update-locale LANG=en_US.UTF-8

ENV LANG="en_US.UTF-8" \
    LANGUAGE="en_US:en" \
    LC_ALL="en_US.UTF-8"

RUN echo "deb http://aptly.cs.int/public xenial $destEnv" >> /etc/apt/sources.list
RUN printf "Package: * \nPin: release a=xenial, o=aptly.cs.int \nPin-Priority: 1600 \n" > /etc/apt/preferences

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y \
    paxctl \
    pax-utils

RUN groupadd -g 10023 presto && \
    adduser --no-create-home --home /opt/presto --system --shell /bin/false --gecos 'presto dedicated user' --uid 10023 --gid 10023 --disabled-password presto

RUN groupadd -g 10024 hyena && \
    adduser --no-create-home --system --shell /bin/false --gecos 'hyena dedicated user' --uid 10024 --gid 10024 --disabled-password hyena && \
    usermod -a -G hyena presto

RUN apt-get install -y nanomsg
RUN apt-get install -y presto=${debVer}

RUN paxctl -c /usr/lib/jvm/java-8-oracle/jre/bin/java && \
    paxctl -m /usr/lib/jvm/java-8-oracle/jre/bin/java

RUN chmod 755 /opt/presto/bin/launcher \
              /opt/presto/bin/launcher.py

RUN mkdir -p /opt/presto/etc/catalog

WORKDIR /opt/presto/

# enable DBs
COPY files/etc/catalog/hyena.properties \
     files/etc/catalog/topdis_postgresql.properties \
     etc/catalog/

COPY files/etc/config.properties \
     files/etc/jvm.config \
     files/etc/log.properties \
     files/etc/node.properties \
     etc/

RUN mkdir -p /var/presto/data && chown -R presto:presto /var/presto/data

VOLUME /var/presto/data

RUN chown -R presto.presto /opt/presto

USER presto
CMD /opt/presto/bin/launcher run
