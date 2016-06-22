#!/bin/sh

export PATH=$PATH:$JAVA_HOME/bin
export MAVEN_OPTS="-Xmx3g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
./make-distribution.sh --mvn mvn/apache-maven-3.3.3/bin/mvn --tgz --skip-java-test -Dhadoop.version=2.0.0-mr1-cdh4.2.0
mv dist spark-1.5.0
tar czf spark-1.5.0.tgz spark-1.5.0
mkdir .dist/
mv spark-1.5.0.tgz .dist/

