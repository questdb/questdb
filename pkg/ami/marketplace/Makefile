.DEFAULT_GOAL := build



AWS_REGION 		?= eu-west-1

packer ?= packer
git    ?= git
aws    ?= aws
jq     ?= jq

PACKER_AMAZON_PLUGIN_VERSION ?= 1.3.10

install_aws_plugin:
	$(packer) plugins install github.com/hashicorp/amazon $(PACKER_AMAZON_PLUGIN_VERSION)

build: QUESTDB_VERSION?=$(shell $(git) rev-parse --short HEAD)-snapshot
build:
	$(packer) build \
	-var ami_regions=$(AMI_REGIONS) \
	-var questdb_version=$(QUESTDB_VERSION) \
	packer.json

build_release: QUESTDB_VERSION?=$(shell $(git) symbolic-ref -q --short HEAD || git describe --tags --exact-match)
build_release: AMI_REGIONS?=$(shell export AWS_REGION=$(AWS_REGION); $(aws) ec2 describe-regions | $(jq) -r '.Regions | map(.RegionName) | join(",")')
build_release: FORCE_DEREGISTER?=false
build_release: FORCE_DELETE_SNAPSHOT?=false
build_release:
	$(packer) build \
	-var ami_regions=$(AMI_REGIONS) \
	-var questdb_version=$(QUESTDB_VERSION) \
	-var ami_groups=all \
	-var force_deregister=$(FORCE_DEREGISTER) \
	-var force_delete_snapshot=$(FORCE_DELETE_SNAPSHOT) \
	packer.json


-include ../Mk/phonies
