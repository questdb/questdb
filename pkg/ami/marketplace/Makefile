.DEFAULT_GOAL := build



AWS_REGION 		?= eu-west-1

packer ?= packer
git    ?= git
aws    ?= aws
jq     ?= jq

build: QUESTDB_VERSION?=$(shell $(git) rev-parse --short HEAD)-snapshot
build:
	$(packer) build \
	-var ami_regions=$(AMI_REGIONS) \
	-var questdb_version=$(QUESTDB_VERSION) \
	packer.json

build_release: QUESTDB_VERSION?=$(shell $(git) symbolic-ref -q --short HEAD || git describe --tags --exact-match)
build_release: AMI_REGIONS?=$(shell export AWS_REGION=$(AWS_REGION); $(aws) ec2 describe-regions | $(jq) -r '.Regions | map(.RegionName) | join(",")')
build_release:
	$(packer) build \
	-var ami_regions=$(AMI_REGIONS) \
	-var questdb_version=$(QUESTDB_VERSION) \
	-var ami_groups=all \
	packer.json


-include ../Mk/phonies