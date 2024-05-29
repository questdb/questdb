#!/bin/bash

################################################################################
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2024 QuestDB
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
  # setting variables in subshell for cloudinit script
  PASS=$(tr -dc '_A-Z-a-z-0-9' < /dev/urandom | head -c${1:-32}; echo)

  # Check if the configuration file exists before attempting to replace the placeholder
  if [ -f "/var/lib/questdb/conf/server.conf" ]; then
    sed -i "s/PG_PASSWORD_REPLACE/$PASS/g" /var/lib/questdb/conf/server.conf
  else
    echo "Configuration file /var/lib/questdb/conf/server.conf does not exist."
    exit 1
  fi
fi

# Enable and start QuestDB service
if systemctl enable questdb; then
  echo "QuestDB service enabled."
else
  echo "Failed to enable QuestDB service."
  exit 1
fi

if systemctl start questdb; then
  echo "QuestDB service started."
else
  echo "Failed to start QuestDB service."
  exit 1
fi

