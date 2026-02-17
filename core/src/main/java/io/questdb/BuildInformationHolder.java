/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class BuildInformationHolder implements BuildInformation, CharSequence {
    private static final String UNKNOWN = "unknown";
    private final String buildKey;
    private final String commitHash;
    private final String jdkVersion;
    private final String swName;
    private final String swVersion;

    public BuildInformationHolder() {
        this(BuildInformationHolder.class);
    }

    public BuildInformationHolder(Class<?> clazz) {
        String swVersion;
        String swName;
        String commitHash;
        String jdkVersion;
        try {
            final Attributes manifestAttributes = getManifestAttributes(clazz);
            swVersion = getAttr(manifestAttributes, "Implementation-Version", "[DEVELOPMENT]");
            swName = getAttr(manifestAttributes, "Implementation-Title", UNKNOWN);
            commitHash = getAttr(manifestAttributes, "Build-Commit-Hash", UNKNOWN);
            jdkVersion = getAttr(manifestAttributes, "Build-Jdk", UNKNOWN);
        } catch (IOException e) {
            swVersion = UNKNOWN;
            swName = UNKNOWN;
            commitHash = UNKNOWN;
            jdkVersion = UNKNOWN;
        }
        this.swVersion = swVersion;
        this.swName = swName;
        this.commitHash = commitHash;
        this.jdkVersion = jdkVersion;
        buildKey = makeBuildKey(swVersion, commitHash, jdkVersion);
    }

    public BuildInformationHolder(String swVersion, String commitHash, String jdkVersion, String swName) {
        this.swVersion = swVersion;
        this.commitHash = commitHash;
        this.jdkVersion = jdkVersion;
        this.swName = swName;
        buildKey = makeBuildKey(swVersion, commitHash, jdkVersion);
    }

    @Override
    public char charAt(int index) {
        return buildKey.charAt(index);
    }

    @Override
    public String getCommitHash() {
        return commitHash;
    }

    @Override
    public String getJdkVersion() {
        return jdkVersion;
    }

    @Override
    public String getSwName() {
        return swName;
    }

    @Override
    public String getSwVersion() {
        return swVersion;
    }

    @Override
    public int length() {
        return buildKey.length();
    }

    @NotNull
    @Override
    public CharSequence subSequence(int start, int end) {
        return buildKey.subSequence(start, end);
    }

    @Override
    public String toString() {
        return buildKey;
    }

    private static String getAttr(final Attributes manifestAttributes, String attributeName, String defaultValue) {
        final String value = manifestAttributes.getValue(attributeName);
        return value != null ? value : defaultValue;
    }

    private static Attributes getManifestAttributes(Class<?> clazz) throws IOException {
        InputStream is = clazz.getResourceAsStream("/META-INF/MANIFEST.MF");
        if (is != null) {
            try {
                final Attributes attributes = new Manifest(is).getMainAttributes();
                final String vendor = attributes.getValue("Implementation-Vendor-Id");
                if (vendor != null && vendor.contains("questdb")) {
                    return attributes;
                }
            } finally {
                is.close();
            }
        }
        return new Attributes();
    }

    private String makeBuildKey(CharSequence swVersion, CharSequence commitHash, CharSequence jdkVersion) {
        return swVersion + ":" + commitHash + ":" + jdkVersion;
    }
}
