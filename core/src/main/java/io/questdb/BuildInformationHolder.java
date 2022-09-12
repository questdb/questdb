/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class BuildInformationHolder implements BuildInformation, CharSequence {
    public static final BuildInformationHolder INSTANCE = fetchBuildInformation();
    private static final String UNKNOWN = "Unknown Version";

    private final CharSequence questDbVersion;
    private final CharSequence commitHash;
    private final CharSequence jdkVersion;

    private final String buildKey;

    public BuildInformationHolder() {
        this(UNKNOWN, UNKNOWN, UNKNOWN);
    }

    @TestOnly
    public BuildInformationHolder(CharSequence questDbVersion, CharSequence commitHash, CharSequence jdkVersion) {
        this.questDbVersion = questDbVersion;
        this.commitHash = commitHash;
        this.jdkVersion = jdkVersion;
        this.buildKey = questDbVersion + ":" + commitHash + ":" + jdkVersion;
    }

    @Override
    public CharSequence getQuestDbVersion() {
        return questDbVersion;
    }

    @Override
    public CharSequence getJdkVersion() {
        return jdkVersion;
    }

    @Override
    public CharSequence getCommitHash() {
        return commitHash;
    }

    @Override
    public String toString() {
        return buildKey;
    }

    @Override
    public int length() {
        return buildKey.length();
    }

    @Override
    public char charAt(int index) {
        return buildKey.charAt(index);
    }

    @NotNull
    @Override
    public CharSequence subSequence(int start, int end) {
        return buildKey.subSequence(start, end);
    }

    private static BuildInformationHolder fetchBuildInformation() {
        try {
            final Attributes manifestAttributes = getManifestAttributes();
            return new BuildInformationHolder(
                    getQuestDbVersion(manifestAttributes),
                    getCommitHash(manifestAttributes),
                    getJdkVersion(manifestAttributes)
            );
        } catch (IOException e) {
            return new BuildInformationHolder();
        }
    }

    private static Attributes getManifestAttributes() throws IOException {
        final Enumeration<URL> resources = ServerMain.class.getClassLoader()
                .getResources("META-INF/MANIFEST.MF");
        while (resources.hasMoreElements()) {
            try (InputStream is = resources.nextElement().openStream()) {
                final Manifest manifest = new Manifest(is);
                final Attributes attributes = manifest.getMainAttributes();
                if ("org.questdb".equals(attributes.getValue("Implementation-Vendor-Id"))) {
                    return manifest.getMainAttributes();
                }
            }
        }
        return new Attributes();
    }

    private static CharSequence getQuestDbVersion(final Attributes manifestAttributes) {
        final CharSequence version = manifestAttributes.getValue("Implementation-Version");
        return version != null ? version : "[DEVELOPMENT]";
    }

    private static CharSequence getJdkVersion(final Attributes manifestAttributes) {
        final CharSequence version = manifestAttributes.getValue("Build-Jdk");
        return version != null ? version : "Unknown Version";
    }

    private static CharSequence getCommitHash(final Attributes manifestAttributes) {
        final CharSequence version = manifestAttributes.getValue("Build-Commit-Hash");
        return version != null ? version : "Unknown Version";
    }
}
