#!/bin/bash
#
# Copyright 2010 Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cd ~/benchmark/flockdb

VERSION="1.8.16-SNAPSHOT"

if java -version 2>&1 |grep "1\.5"; then
  echo "Java must be at least 1.6"
  exit 1
fi

echo "Killing any running flockdb..."
curl http://localhost:9990/shutdown >/dev/null 2>/dev/null
sleep 3
killall java

echo "Launching flockdb..."
rm -R spool
mkdir -p spool/kestrel
rm *.log

JAVA_OPTS="-Xms256m -Xmx6G -XX:NewSize=64m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -server"
java $JAVA_OPTS -jar ./dist/flockdb/flockdb-${VERSION}.jar /home/bgehrels/benchmark/Diplomarbeit-Code/production.scala &

sleep 30

echo
echo "Done."
