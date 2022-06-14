#!/usr/bin/env bash

# depends on:
# * curl (https://curl.haxx.se/)
# * jq (https://stedolan.github.io/jq/)
# * tar (https://www.gnu.org/software/tar/)

set -euox pipefail

# location of package document file. It contains version number of latest released package
packument_url="https://registry.npmjs.org/@questdb/web-console"

# download package document file and extract latest version number
version=$(curl -sX GET $packument_url | jq --raw-output '.["dist-tags"]["latest"]')

# download latest version web-console tarball
tarball_url="https://registry.npmjs.org/@questdb/web-console/-/web-console-$version.tgz"
curl -X GET -o web-console.tgz $tarball_url

temp_folder=$(mktemp -d)

# extract tarball into tmp folder which later should be used in core/src/main/assembly/web-console.xml
tar -xzf web-console.tgz --directory $temp_folder
mv $temp_folder/package/dist/* $temp_folder 
rm -r $temp_folder/package
echo $temp_folder
