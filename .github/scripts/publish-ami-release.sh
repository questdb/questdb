#!/usr/bin/env bash
#
# Copyright (c) 2014-2019 Appsicle
# Copyright (c) 2019-2026 QuestDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly script_dir
repo_dir="$(cd "${script_dir}/../.." && pwd)"
readonly repo_dir

usage() {
    echo "usage: $0 <preflight|publish> <release-version> [comma-separated-ami-regions]" >&2
    exit 2
}

[[ "$#" -ge 2 && "$#" -le 3 ]] || usage

command="$1"
release_version="$2"
release_name="questdb-${release_version}-al2023-x86_64-ebs"

case "${command}" in
    preflight)
        [[ "$#" -eq 2 ]] || usage
        ami_regions="$(aws ec2 describe-regions --query 'Regions[].RegionName' --output text | tr '\t' ',')"
        if [[ -z "${ami_regions}" ]]; then
            echo "AWS returned no AMI regions" >&2
            exit 1
        fi
        for region in ${ami_regions//,/ }; do
            count="$(aws ec2 describe-images --region "${region}" --owners self --filters "Name=name,Values=${release_name}" --query 'length(Images)' --output text)"
            if [[ "${count}" != 0 ]]; then
                echo "release AMI already exists in ${region}: ${release_name}" >&2
                exit 1
            fi
        done
        printf '%s\n' "${ami_regions}"
        ;;
    publish)
        [[ "$#" -eq 3 ]] || usage
        ami_regions="$3"
        if [[ -z "${ami_regions}" ]]; then
            echo "AMI regions must be supplied after a successful preflight" >&2
            exit 1
        fi
        (
            cd "${repo_dir}/pkg/ami/marketplace"
            make install_aws_plugin
            make build_release \
                AMI_REGIONS="${ami_regions}" \
                QUESTDB_VERSION="${release_version}" \
                FORCE_DEREGISTER=false \
                FORCE_DELETE_SNAPSHOT=false
        )
        ;;
    *) usage ;;
esac
