FROM portus.cs.int:5000/prod/cs-dbuild-japps

RUN apt-get update && apt-get install -y maven

RUN adduser --uid 1000 --disabled-login --disabled-password --gecos "builder" builder

USER builder

WORKDIR /build
VOLUME /build

ENTRYPOINT ["/usr/bin/mvn"]
CMD ["-P", "deb", "install", "-DskipTests"]
