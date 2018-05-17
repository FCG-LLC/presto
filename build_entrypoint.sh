#!/usr/bin/env bash

gradle publishToMavenLocal -b ./presto-hyena/hyena-java/build.gradle -x test
/usr/bin/mvn -P deb install -DskipTests
