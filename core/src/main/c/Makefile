.DEFAULT_GOAL := get_cbinaries
SHELL = bash

export AWS_REGION           ?= eu-west-1

BUCKET_NAME 		?= questdb-releases
BUCKET_PATH			?= releases/c_binaries
DEFAULT_BINARY_PATH ?= ../resources/io/questdb/bin
DEVOPS_ORG			?= https://dev.azure.com/questdb
DEVOPS_PROJECT		?= questdb
AZ_PIPELINE_ID      ?= 6

aws   ?= aws
az    ?= az
jq    ?= jq
git   ?= git



ifeq (, $(shell which aws))
$(error "No aws cli in $(PATH)")
endif

ifeq (, $(shell which az))
$(error "No az cli in $(PATH)")
endif

ifeq (, $(shell which jq))
$(error "No jq in $(PATH)")
endif

ifndef AZURE_DEVOPS_EXT_PAT
$(error AZURE_DEVOPS_EXT_PAT is undefined, check README)
endif

get_cbinaries: az_login az_trigger_pipeline

README:
	@ echo "1. make sure you have installed all the dependencies listed in https://questdb.slab.com/posts/development-station-configuration-zw6gdow8"
	@ echo "2. You need az-cli, aws cli, jq and azure devops token set env variable"
	@ echo "3. to start build run 'make get_cbinaries'"

az_login:
	@ $(az) devops configure --defaults organization=$(DEVOPS_ORG) project=$(DEVOPS_PROJECT) --use-git-aliases true

az_trigger_pipeline: GIT_ID?=$(shell $(git) rev-parse --short HEAD)
az_trigger_pipeline: GIT_BRANCH?=$(shell $(git) symbolic-ref -q --short HEAD || git describe --tags --exact-match)
az_trigger_pipeline:
	$(eval pipeline_id := $(shell $(az) pipelines run --detect true  --id $(AZ_PIPELINE_ID) --branch $(GIT_BRANCH) --output json --only-show-errors | $(jq) -r '.id') )
	@ echo "Build branch: $(GIT_BRANCH) - commit: $(GIT_ID)"
	@ echo "link: $(DEVOPS_ORG)/$(DEVOPS_PROJECT)/_build/results?buildId=$(pipeline_id)"
	@ $(MAKE) wait ID=$(pipeline_id)
	@ $(MAKE) az_download_cbinaries ID=$(pipeline_id)

az_download_cbinaries: ID=$(ID)
az_download_cbinaries:
	@ echo "Updating local binaries"
	$(az) pipelines runs artifact download --detect true --artifact-name drop --path $(DEFAULT_BINARY_PATH) --run-id $(ID)

wait: WAIT_TIMEOUT:=2000
wait: ID=$(ID)
wait:
	$(eval timeout := $(shell echo "`date +%s` + $(WAIT_TIMEOUT)" | bc ))
	@ echo Waiting for pipeline to be completed
	@ while [ "`date +%s`" -le "$(timeout)" ]; do \
		status="`$(az) pipelines build show --project=$(DEVOPS_PROJECT) --id $(ID) --detect true --organization $(DEVOPS_ORG) | $(jq) -r '.status'`"; \
		if [ "$$status" == "completed" ]; then \
			exit 0 || echo "Pipeline completed with id $(ID)"; \
		fi; \
		echo -n "."; \
		sleep 5; \
	done; \
	echo "ERROR timeout $(WAIT_TIMEOUT)sec"; \
	exit 1