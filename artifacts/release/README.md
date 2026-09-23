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

Create the draft GitHub release and prepare the tag:

```bash
mvn -B release:prepare
```

`release:prepare` runs `clean verify` and is non-atomic. Maven Release Plugin
3.1.1 commits release POMs, creates the tag, rewrites the POMs to the next
development version, and commits that rewrite. Its `pushChanges` and `resume`
defaults are true, so a release commit and tag can already be remote before a
later step fails. Do not run `release:perform`; it does not publish Maven
Central and must not activate the Central profile.

After preparation, inspect the POM at the immutable tag. Stop before Central
deployment if its client pin or any other external dependency is a SNAPSHOT.
Do not repair the tag with `local-client`, a substituted branch jar, or a tag
move. Correct the dependency and prepare a new patch version instead.

## Select verified package inputs

Select one successful package set by immutable GitHub Actions run ID. The run
must be the tag-push run or a `package-only` dispatch of the exact unchanged
tag used only to regenerate expired package evidence. A branch run is never a
release input and a package-only dispatch never publishes GitHub assets or
AMIs.

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
repository root in a fresh detached checkout of the tag. Initialize the pinned
client submodule when required. Do not run `clean` after either download.

## Publish Maven Central

The immutable tag must resolve a released external client. From the repository
root, publish the verified aggregate jar with:

```bash
mvn -B -pl core -am deploy -DskipTests -Dmaven.test.skip=true -DskipNative \
  -P build-web-console,include-rust-native-artifacts,maven-central-release
```

Do not add `local-client`. The active-profile Central gate, staged native
validation, and verify-phase core-jar check fail before Central publication if
the dependency, provenance, or native inputs are wrong.

## GitHub assets and AMIs

The [Github Release - Binaries](https://github.com/questdb/questdb/actions/workflows/github-binaries-release.yml)
workflow packages four Rust libraries: Linux x86-64, Linux aarch64, macOS
aarch64, and Windows x86-64. Intel macOS is not a release artifact. Tag pushes
may publish GitHub assets and AMIs only after packaging succeeds. Branch and
`package-only` dispatch runs package and verify only.

GitHub asset publication uploads absent assets and reuses an existing asset
only after a byte-for-byte SHA-256 match. AMI publication inventories the
source and every release destination region for same-version AMIs before
Packer. Release-mode Packer disables force deregistration and force snapshot
deletion.

## Recovery

Never use a blanket workflow rerun after GitHub asset upload or AMI creation
has begun. Inventory side effects and resume only identified missing work.

| State | Required action |
| --- | --- |
| `release:prepare` is interrupted or remote state is ambiguous | Before resume, rollback, deletion, or rerun, inventory local release state, local and remote branch heads, remote tag target, workflow runs, GitHub assets, AMIs and snapshots in the source and every configured destination region, and Central. A remote tag means publication may have started. Never blindly invoke `release:prepare`, `release:rollback`, or move/delete the tag. Use local rollback/clean only after refs and external state prove untouched. |
| A transient packaging failure occurs with unchanged tagged source and no publication | Retry only the failed package job. When evidence expired, dispatch that exact immutable tag in `package-only` mode and repeat all run, ID, digest, producer-attempt, and `head_sha` checks. |
| A source or workflow defect exists in the tagged commit | Cancel that version and prepare a new patch after correcting the defect. Do not use substitute branch/local artifacts, repoint the tag, or treat an exact-tag package-only dispatch as a fix. |
| GitHub asset upload started | Inventory every asset. Retry only missing assets. Reuse an existing asset only when its checksum equals the verified archive; stop on a mismatch. |
| AMI creation started | Inventory same-version AMIs and snapshots in the source and every configured destination region. Existing AMIs stop automated duplicate publication. Preserve evidence and resume only explicit missing work after review. |
| Central failed after GitHub/AMI work | Inspect Central for the version before an exact-tag Central retry. Never blindly redeploy. |
| Central succeeded while GitHub/AMI is incomplete | Do not redeploy Central. Inventory side effects and resume only the missing GitHub or AMI work. |
| Published content is bad | Preserve evidence, stop retries, and release a new patch version. |

Maven Central uses immutable, immediately published releases. Central already
containing the version is a hard stop rather than a reason to redeploy or move
the tag.

## Release Docker image

Azure [Build Docker Image](https://dev.azure.com/questdb/questdb/_build?definitionId=22)
automatically builds and releases Docker images after a release tag is pushed.
If it fails, follow the Docker release procedure in `core/README.md`; retain
the same immutable tag and release evidence throughout recovery.

## Update remaining release targets

Update the demo environment, Helm chart, and other release targets only after
the immutable tag, verified packages, GitHub/AMI publication, and Maven
Central publication have been recorded.
