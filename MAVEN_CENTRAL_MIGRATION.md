# Maven Central Publishing Migration (2024)

## Overview

This document describes the migration from the old Maven Central publishing system (using nexus-staging-maven-plugin) to the new Central Portal system that became mandatory in 2024.

## Changes Made

### 1. Updated Maven Release Plugin
- **Old version**: 2.5.3
- **New version**: 3.1.1
- **Reason**: Better compatibility with new Central Portal system

### 2. Replaced nexus-staging-maven-plugin with central-publishing-maven-plugin
- **Old plugin**: `org.sonatype.plugins:nexus-staging-maven-plugin:1.6.14`
- **New plugin**: `org.sonatype.central:central-publishing-maven-plugin:0.8.0`
- **Reason**: The nexus-staging-maven-plugin is deprecated as of 2024 due to Maven Central API changes

### 3. Updated Distribution Management URLs
- **Old URL**: `https://oss.sonatype.org/service/local/staging/deploy/maven2`
- **New URL**: `https://central.sonatype.com/api/v1/publisher/deployments/download/`
- **Reason**: New Central Portal endpoints

### 4. Updated GPG Plugin
- **Old version**: 1.6
- **New version**: 3.2.7
- **Added**: `--pinentry-mode loopback` for better CI/CD compatibility

## Required Setup

### 1. Central Portal Account
You need to create an account at https://central.sonatype.com and generate a new token.

### 2. Update settings.xml
```xml
<settings>
    <servers>
        <server>
            <id>central</id>
            <username>{Your Central Portal username}</username>
            <password>{Your Central Portal token}</password>
        </server>
    </servers>
</settings>
```

### 3. GPG Key Setup
Make sure you have a valid GPG key configured for signing artifacts.

## Migration Benefits

1. **Simplified Process**: No more manual staging repository management
2. **Better CI/CD Integration**: Automatic publishing with validation
3. **Future-Proof**: Uses the official 2024+ publishing method
4. **Enhanced Security**: Better GPG integration

## Usage

The release process remains the same:

```bash
mvn release:prepare
mvn release:perform
```

The new plugin will automatically:
1. Upload artifacts to Central Portal
2. Validate the bundle
3. Publish to Maven Central (if `waitUntil` is set to `published`)

## Configuration Options

The central-publishing-maven-plugin supports several configuration options:

- `publishingServerId`: Server ID in settings.xml (set to "central")
- `waitUntil`: Can be "uploaded", "validated", or "published"
- `autoPublish`: Set to true for automatic publishing after validation

## Troubleshooting

If you encounter issues:

1. Ensure your Central Portal credentials are correct
2. Verify GPG key is properly configured
3. Check that all required POM metadata is present (description, license, developers, SCM)
4. Review the Central Portal dashboard for deployment status

## References

- [Central Portal Documentation](https://central.sonatype.org/publish/publish-portal-maven/)
- [central-publishing-maven-plugin](https://central.sonatype.com/artifact/org.sonatype.central/central-publishing-maven-plugin)