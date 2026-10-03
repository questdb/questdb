# Release steps

This guide is the canonical procedure for a QuestDB release. A release is
incomplete until GitHub/AMI publication and Maven Central publication both
succeed.

## Prepare the immutable release tag

Before `mvn -B release:prepare`, use a clean branch whose checked-out head is
known to match its remote. Inspect all release POMs for unresolved external
SNAPSHOT dependencies, including `questdb.client.version`. Do not activate
`local-client`: it is only a branch/source-build reactor profile and cannot
substitute for a released external client dependency.

Create the draft GitHub release first, on
https://github.com/questdb/questdb/releases: set its tag to the intended
version, choose the option that creates the tag on publish, and write the
release notes in the style of the previous releases. Do not create the git
tag by hand. The release workflow uploads the archives to this draft and
publishes it; without the draft, `publish-github` fails at `gh release view`.

Then prepare the tag:

```bash
mvn -B release:prepare
```

`release:prepare` runs `clean verify` and is non-atomic. Maven Release Plugin
3.1.1 commits release POMs, creates the tag, rewrites the POMs to the next
development version, and commits that rewrite. Its `pushChanges` and `resume`
defaults are true, so a release commit and tag can already be remote before a
later step fails. Do not run `release:perform`; it does not publish Maven
Central and must not activate the Central profile.

The release plugin's own `check-dependency-snapshots` phase rejects SNAPSHOT
dependencies and plugins before it rewrites any POM, so preparation needs no
extra guard. The Central deploy below carries its own `requireReleaseDeps`
rule inside the `maven-central-release` profile.

Pushing the tag starts the release workflow, which publishes GitHub assets,
Maven Central, and AMIs without further input. If the tag's POM still pins a
SNAPSHOT client or any other SNAPSHOT dependency, the Central job fails at
`requireReleaseDeps` before it uploads anything. Do not repair the tag with
`local-client`, a substituted branch jar, or a tag move. Correct the
dependency and prepare a new patch version instead.

## Select verified package inputs

The tag-push workflow selects its own package inputs by immutable artifact ID.
The Maven Central job downloads the `rust-native-libs` and
`third-party-licenses` artifacts produced by the same run; it never selects by
name or from another run. A manual dispatch packages and verifies only and
cannot publish Maven Central, GitHub assets, or AMIs.

For manual recovery, select one successful package set by immutable GitHub
Actions run ID. The run must be the tag-push run or a manual dispatch of the
exact unchanged tag used only to regenerate expired package evidence. A branch
run is never a release input.

Record the run event, ref, `head_sha`, action versions, container identities,
Rust version, and every selected artifact's ID, GitHub digest, producer job,
and actual producer attempt. A selected set may combine a rerun's artifacts
with earlier successful dependencies from the same run. Retain each artifact's
actual producer attempt rather than relabeling it with the last attempt.

Resolve exactly one `native-release-provenance` artifact through the GitHub
API. Record its ID and API digest outside the manifest, download it by ID, and
verify the downloaded archive digest against the API value. Assert that the
run `head_sha` equals `tag^{commit}`. The provenance table must resolve exactly
one artifact for every recorded payload/evidence name and match its ID, digest,
producer job, producer attempt, native manifest hash, and license manifest
hash. Download `rust-native-libs` by its recorded ID to
`core/target/native-libs` and `third-party-licenses` by its recorded ID to the
repository root in a fresh detached checkout of the tag. Do not run `clean`
after either download.

## Publish Maven Central

The tag-push workflow publishes Maven Central automatically after Linux and
Windows packaging and GitHub asset publication succeed. Configure these values
once:

- Repository variable `MAVEN_RELEASE_AWS_REGION`.
- `maven-release` environment secret `MAVEN_RELEASE_AWS_ROLE_ARN`.
- `maven-release` environment secret `MAVEN_RELEASE_AWS_SECRET_ARN`.
- The referenced AWS JSON secret must define `MAVEN_GPG_PRIVATE_KEY`,
  `MAVEN_CENTRAL_USERNAME`, and `MAVEN_CENTRAL_PASSWORD`. It may define
  `MAVEN_GPG_PASSPHRASE`; omit it or leave it empty for a key without a
  passphrase.
- The repository or organization Actions policy must allow
  `aws-actions/configure-aws-credentials@d979d5b3a71173a29b74b5b88418bfda9437d885`
  and
  `aws-actions/aws-secretsmanager-get-secrets@a9a7eb4e2f2871d30dc5b892576fde60a2ecc802`.
  GitHub validates action policy before evaluating job conditions, so even a
  package-only dispatch fails at startup until both exact pins are allowed.
  The AMI job uses the same `configure-aws-credentials` pin.
- Repository secret `AMI_RELEASE_AWS_ROLE_ARN`, the role the AMI job assumes
  (`github_ami_publisher_questdb` in `questdb/ci-infra`).

The role's trust policy admits only GitHub OIDC tokens for this repository,
the `maven-release` environment, this workflow, and a tag ref
(`github_releaser_questdb` in `questdb/ci-infra`). Leave the `maven-release`
environment without required reviewers: a reviewer gate pauses every release
on a manual approval, and the tag push is already the release decision.

The job needs no human step between the tag push and the published artifact.
It checks out the immutable tagged SHA, downloads the same run's verified
native and license artifacts, and builds one signed Central bundle with
publication skipped. After `verify-central-bundle.py` accepts that bundle, the
job records its SHA-256 and uploads those exact bytes to the Central Publisher
API with `publishingType=USER_MANAGED`; it never rebuilds or re-signs. Central
answers with a deployment ID, which every later request uses:

1. The job polls the deployment's status until Central reports `VALIDATED`
   (up to 10 minutes). `FAILED`, an unexpected state, or a status answer for a
   different deployment ID stops the job.
2. The job sends the single irreversible publish request and requires HTTP 204.
3. The job polls until Central reports `PUBLISHED` (up to 40 minutes).

The upload and the publish request each run once, without retries. Only the
read-only status polls retry, and only on DNS, connect, timeout, and
send/receive failures or HTTP 408, 429, and 5xx. Every failure path prints the
deployment ID for the recovery table below.

Do not add `local-client`. The active-profile Central gate, staged native
validation, signed-bundle verification, and verify-phase core-jar check fail
before upload if the dependency or native inputs are wrong. The tag's POM is a
release version, so the `requireReleaseDeps` rule inside the
`maven-central-release` profile rejects a SNAPSHOT client pin or any other
SNAPSHOT dependency.

## GitHub assets and AMIs

The [Github Release - Binaries](https://github.com/questdb/questdb/actions/workflows/github-binaries-release.yml)
workflow packages four Rust libraries: Linux x86-64, Linux aarch64, macOS
aarch64, and Windows x86-64. Intel macOS is not a release artifact. Tag pushes
may publish GitHub assets, Maven Central, and AMIs only after packaging
succeeds. Branch and manual dispatch runs package and verify only.

GitHub asset publication uploads absent assets and reuses an existing asset
only after a byte-for-byte SHA-256 match. AMI publication obtains short-lived
AWS credentials through GitHub OIDC: the job assumes the role named by the
repository secret `AMI_RELEASE_AWS_ROLE_ARN` after checkout and the Packer
install. The workflow holds no long-lived AWS access keys. AMI publication
inventories the source and every release destination region for same-version
AMIs before Packer. Release-mode Packer disables force deregistration and force snapshot
deletion.

## Recovery

Never use a blanket workflow rerun after GitHub asset upload or AMI creation
has begun. Inventory side effects and resume only identified missing work.

| State | Required action |
| --- | --- |
| `release:prepare` is interrupted or remote state is ambiguous | Before resume, rollback, deletion, or rerun, inventory local release state, local and remote branch heads, remote tag target, workflow runs, GitHub assets, AMIs and snapshots in the source and every configured destination region, and Central. A remote tag means publication may have started. Never blindly invoke `release:prepare`, `release:rollback`, or move/delete the tag. Use local rollback/clean only after refs and external state prove untouched. |
| A transient packaging failure occurs with unchanged tagged source and no publication | Retry only the failed package job. When evidence expired, dispatch that exact immutable tag and repeat all run, ID, digest, producer-attempt, and `head_sha` checks. |
| A source or workflow defect exists in the tagged commit | Cancel that version and prepare a new patch after correcting the defect. Do not use substitute branch/local artifacts, repoint the tag, or treat an exact-tag package-only dispatch as a fix. |
| GitHub asset upload started | Inventory every asset. Retry only missing assets. Reuse an existing asset only when its checksum equals the verified archive; stop on a mismatch. |
| AMI creation started | Inventory same-version AMIs and snapshots in the source and every configured destination region. Existing AMIs stop automated duplicate publication. Preserve evidence and resume only explicit missing work after review. |
| Central fails before it reports a deployment ID | Confirm in the Central Portal that no `questdb-<version>` deployment and no published artifact exist for the version before retrying only the failed Central job. An upload request that timed out may still have created a deployment. |
| Central reports a deployment ID but fails before the publish request succeeds | Inspect that exact deployment in the Central Portal. Publish or drop it deliberately; do not rerun the upload and create a second deployment. |
| Central accepts the publish request but the job later fails or times out | Do not rerun or delete the tag. Monitor the recorded deployment ID; Central owns the asynchronous publication. |
| Central succeeds while GitHub/AMI is incomplete | Do not redeploy Central. Inventory side effects and resume only the missing GitHub or AMI work. |
| Published content is bad | Preserve evidence, stop retries, and release a new patch version. |

Maven Central releases are immutable after the publish request. Central already
containing the version is a hard stop rather than a reason to redeploy or move
the tag.

## Release Docker image

Azure [Build Docker Image](https://dev.azure.com/questdb/questdb/_build?definitionId=22)
automatically builds and releases Docker images after a release tag is pushed.
If it fails, re-run that pipeline for the same tag. As a manual fallback,
run the pipeline's own commands from a detached checkout of the tag on a host
of each architecture, then assemble the multi-arch manifest the way
`ci/docker-release-pipeline.yml` does:

```bash
case "$(uname -m)" in
  x86_64|amd64)
    host_platform=linux/amd64
    ;;
  aarch64|arm64)
    host_platform=linux/arm64
    ;;
  *)
    echo "Unsupported release-host architecture: $(uname -m)" >&2
    exit 1
    ;;
esac
docker buildx build -f core/Dockerfile --platform "${host_platform}" \
  --build-arg tag_name=<version> \
  --output "type=image,name=questdb/questdb,push-by-digest=true,push=true" .
docker buildx build -f core/Dockerfile --platform "${host_platform}" --target rhel \
  --build-arg tag_name=<version> \
  --output "type=image,name=questdb/questdb,push-by-digest=true,push=true" .
```

Retain the same immutable tag and release evidence throughout recovery.

## Update remaining release targets

Update the demo environment, Helm chart, and other release targets only after
the immutable tag, verified packages, GitHub/AMI publication, and Maven
Central publication have been recorded.
