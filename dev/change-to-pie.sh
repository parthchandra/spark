#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

usage() {
  echo "Usage: $(basename $0) [-h|--help] <version>
example:
  $(basename $0) 2.4.4-pie1.0.0-SNAPSHOT
where :
  -h| --help Display this help text
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_VERSION=$1

cd "`dirname "$0"`/.."
find . -type f -name "pom.xml" -print0 | xargs -0 sed -i '' 's/>org.apache.spark</>com.apple.pie.spark</g'
build/mvn versions:set -DnewVersion=$TO_VERSION -DgenerateBackupPoms=false
