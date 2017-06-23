FROM portus.cs.int:5000/prod/cs-dbuild-japps

RUN apt-get update && apt-get install -y maven

# UID is 1003, because builder user has UID 1003
# This is ugly, but probably better for now than `chmod -R o+w` (or isn't?)
RUN adduser --uid 1003 --disabled-login --disabled-password --gecos "builder" builder

USER builder

WORKDIR /build
VOLUME /build

ENTRYPOINT ["/usr/bin/mvn"]
CMD ["-P", "deb", "install", "-DskipTests"]
