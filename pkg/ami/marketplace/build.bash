#!/usr/bin/env bash

################################################################################
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2022 QuestDB
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
################################################################################

set -euxo pipefail


echo ""

# data partition format & mount
sudo mkfs -t ext4  /dev/xvdb
echo '/dev/xvdb /var/lib/questdb  ext4  defaults,nofail  0  2' | sudo tee -a /etc/fstab > /dev/null
sudo mount /var/lib/questdb

sudo yum install -y jose jq

# questb binary
wget https://github.com/questdb/questdb/releases/download/$QUESTDB_VERSION/questdb-$QUESTDB_VERSION-no-jre-bin.tar.gz
tar xf questdb-$QUESTDB_VERSION-no-jre-bin.tar.gz
cp -f questdb-$QUESTDB_VERSION-no-jre-bin/questdb.jar /usr/local/bin/questdb.jar

# config
sudo mv /tmp/assets/cloudwatch.template.json /etc/questdb/
sudo mv /tmp/assets/server.conf /etc/questdb/
sudo mkdir -p /var/lib/questdb/conf/
sudo cp /etc/questdb/server.conf /var/lib/questdb/conf/server.conf
sudo mv /tmp/scripts/1-per-boot.sh /var/lib/cloud/scripts/per-boot/
sudo mv /tmp/assets/systemd.service /usr/lib/systemd/system/questdb.service
