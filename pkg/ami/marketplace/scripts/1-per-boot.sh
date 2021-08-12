#!/bin/bash

sudo cp /etc/questdb/server.conf /var/lib/questdb/conf/

# influxdb and postgres auth
jose jwk gen -i '{"alg":"ES256", "kid": "testUser1"}' -o /var/lib/questdb/conf/full_auth.json
# setting variables in subshell for cloudinit script
{ PASS=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-32}; echo;); }
{ KID=$(cat /var/lib/questdb/conf/full_auth.json | jq -r '.kid'); X=$(cat /var/lib/questdb/conf/full_auth.json | jq -r '.x'); Y=$(cat /var/lib/questdb/conf/full_auth.json | jq -r '.y'); }
sed -i "s/PG_PASSWORD_REPLACE/$PASS/g" /var/lib/questdb/conf/server.conf
echo "$KID ec-p-256-sha256 $X $Y" | tee /var/lib/questdb/conf/auth.txt
 
systemctl enable questdb
systemctl start questdb
