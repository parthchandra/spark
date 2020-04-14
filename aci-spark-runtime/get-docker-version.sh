#!/usr/bin/env bash

set -o pipefail
set -e
set -x
export PATH=$PATH:$JAVA_HOME/bin

function exit_with_usage {
  set +x
  echo "get-docker-version.sh - tool for finding Dockerfile version"
  echo ""
  echo "usage:"
  cl_options="[--with-hadoop]"
  echo "get-docker-version.sh $cl_options"
  echo ""
  exit 1
}

WITH_HADOOP=false

# Parse arguments
while (( "$#" )); do
  case $1 in
    --with-hadoop)
      WITH_HADOOP=true
      ;;
    --*)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
    -*)
      break
      ;;
    *)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
  esac
  shift
done

MVN="./build/mvn"

VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ \
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
    
SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SPARK_HADOOP_VERSION=$("$MVN" help:evaluate -Dexpression=hadoop.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
    
if [[ "$VERSION" == *-SNAPSHOT ]]; then
  if [ "$WITH_HADOOP" == "false" ]; then
    TGZ_VERSION=`echo $VERSION | sed -e 's/-SNAPSHOT$/-no-hadoop-SNAPSHOT/g'`
  else
    TGZ_VERSION=`echo $VERSION | sed -e 's/-SNAPSHOT$/-'$SPARK_HADOOP_VERSION'-SNAPSHOT/g'`
  fi
else
  if [ "$WITH_HADOOP" == "false" ]; then
    TGZ_VERSION="${VERSION}-no-hadoop"
  else
    TGZ_VERSION="${VERSION}"
  fi
fi    

FULL_VERSION=$SCALA_VERSION-$TGZ_VERSION

echo $FULL_VERSION
