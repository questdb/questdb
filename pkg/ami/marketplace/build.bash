#!/usr/bin/env bash

################################################################################
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2026 QuestDB
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
QUESTDB_DATA_DIR=/var/lib/questdb

# Install dependencies
sudo dnf update -y -q
sudo dnf install -y -q \
    java-11-amazon-corretto-headless \
    ec2-instance-connect \
    amazon-ssm-agent \
    amazon-cloudwatch-agent \
    jq

sudo mv /tmp/scripts/1-per-boot.sh /var/lib/cloud/scripts/per-boot/
sudo mv /tmp/assets/99-questdb.conf /etc/sysctl.d/

# Amend bashrc
sudo cat /tmp/assets/bashrc >> /etc/bashrc
. /tmp/assets/bashrc

echo ""

# data partition format & mount
sudo mkfs -t ext4  /dev/xvdb
mkdir -p ${QUESTDB_DATA_DIR}
echo "/dev/xvdb ${QUESTDB_DATA_DIR}  ext4  defaults,nofail  0  2" | sudo tee -a /etc/fstab > /dev/null
cat /etc/fstab
sudo mount ${QUESTDB_DATA_DIR}

groupadd -g 10001 questdb
useradd -u 10001 -g 10001 -d ${QUESTDB_DATA_DIR} -M -s /sbin/nologin questdb

# QuestDB binary
wget https://github.com/questdb/questdb/releases/download/$QUESTDB_VERSION/questdb-$QUESTDB_VERSION-no-jre-bin.tar.gz
tar xf questdb-$QUESTDB_VERSION-no-jre-bin.tar.gz
cp -f questdb-$QUESTDB_VERSION-no-jre-bin/questdb.jar /usr/local/bin/questdb.jar
rm -f questdb-$QUESTDB_VERSION-no-jre-bin.tar.gz
rm -rf questdb-$QUESTDB_VERSION-no-jre-bin

# config
sudo mkdir -p ${QUESTDB_DATA_DIR}/conf/
sudo mv /tmp/assets/server.conf ${QUESTDB_DATA_DIR}/conf/server.conf

# Setup systemd
echo "SystemMaxUse=1G" | sudo tee -a /etc/systemd/journald.conf
sudo systemctl restart systemd-journald
sudo mv /tmp/assets/systemd.service /usr/lib/systemd/system/questdb.service
sudo mkdir -p /etc/systemd/system/questdb.service.d/
sudo mv /tmp/assets/override.conf /etc/systemd/system/questdb.service.d/override.conf
sudo systemctl daemon-reload
sudo systemctl enable questdb
