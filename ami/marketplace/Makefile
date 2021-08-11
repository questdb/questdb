.DEFAULT_GOAL := build

QUESTDB_VERSION ?= 6.0.4

packer ?= packer
git    ?= git

build:
	$(packer) build -var questb_version=$(QUESTDB_VERSION) packer.json


build_release: QUESTDB_VERSION:=$(shell $(git) symbolic-ref -q --short HEAD || git describe --tags --exact-match)
	$(packer) build -var questb_version=$(QUESTDB_VERSION) packer.json

-include ../Mk/phonies