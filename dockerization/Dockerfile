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

RUN groupadd -g 10035 presto && \
    adduser --no-create-home --home /opt/presto --system --shell /bin/false --gecos 'presto dedicated user' --uid 10035 --gid 10035 --disabled-password presto

RUN groupadd -g 10036 hyena-tmp && \
   usermod -a -G hyena-tmp presto

RUN apt-get install -y nanomsg
RUN apt-get install -y presto=$debVer

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

COPY launcher_entrypoint.sh /opt/presto/launcher_entrypoint.sh
RUN chown presto:presto /opt/presto/launcher_entrypoint.sh

USER presto
ENTRYPOINT "/opt/presto/launcher_entrypoint.sh"
