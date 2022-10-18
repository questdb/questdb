#!/bin/bash

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

if [ ! -f "/var/lib/questdb/conf/server.conf" ]; then
  cp /etc/questdb/server.conf /var/lib/questdb/conf/server.conf
fi

if [ ! -f "/var/lib/questdb/conf/full_auth.json" ]; then
    # influxdb and postgres auth
    jose jwk gen -i '{"alg":"ES256", "kid": "testUser1"}' -o /var/lib/questdb/conf/full_auth.json
fi

# setting variables in subshell for cloudinit script
{ KID=$(cat /var/lib/questdb/conf/full_auth.json | jq -r '.kid'); X=$(cat /var/lib/questdb/conf/full_auth.json | jq -r '.x'); Y=$(cat /var/lib/questdb/conf/full_auth.json | jq -r '.y'); }
{ PASS=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-32}; echo;); }

sed -i "s/PG_PASSWORD_REPLACE/$PASS/g" /var/lib/questdb/conf/server.conf

if [ ! -f "/var/lib/questdb/conf/auth.txt" ]; then
echo "$KID ec-p-256-sha256 $X $Y" | tee /var/lib/questdb/conf/auth.txt
fi
 
systemctl enable questdb
systemctl start questdb
