#!/bin/sh

sleep 15

# Init cloudwatch
instanceDetails=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document)
id=$(echo "$instanceDetails" | jq -r .instanceId)
region=$(echo "$instanceDetails" | jq -r .region)

export ENVIRONMENT=$(aws ec2 describe-tags --region "$region" --filters "Name=resource-id,Values=${id}" | jq -r '[.Tags[] | select(.Key | contains("Environment")).Value][0]')

envsubst '${ENVIRONMENT}' < /etc/questdb/cloudwatch.template.json > /etc/questdb/cloudwatch.json

/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/etc/questdb/cloudwatch.json -s
