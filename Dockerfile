FROM portus.cs.int:5000/prod/cs-dbuild-japps

ARG destEnv

RUN apt-get update && apt-get install -y maven

# Gradle
ENV GRADLE_VERSION=4.4
RUN wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip && \
    mkdir /opt/gradle && \
    unzip -d /opt/gradle gradle-${GRADLE_VERSION}-bin.zip
ENV PATH "${PATH}:/opt/gradle/gradle-${GRADLE_VERSION}/bin"

# UID is 1003, because builder user has UID 1003
# This is ugly, but probably better for now than `chmod -R o+w` (or isn't?)
RUN adduser --uid 1003 --disabled-login --disabled-password --gecos "builder" builder

USER builder

ADD build_entrypoint.sh ./build_entrypoint.sh

WORKDIR /build
VOLUME /build

ENTRYPOINT "./build_entrypoint.sh"
