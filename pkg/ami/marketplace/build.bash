#!/usr/bin/env bash

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
sudo mv /tmp/scripts/1-per-boot.sh /var/lib/cloud/scripts/per-boot/
sudo mv /tmp/assets/systemd.service /usr/lib/systemd/system/questdb.service