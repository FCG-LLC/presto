#!/usr/bin/env bash

gradle publishToMavenLocal -b ./presto-hyena/hyena-api/build.gradle -x test
/usr/bin/mvn -P deb install -DskipTests
