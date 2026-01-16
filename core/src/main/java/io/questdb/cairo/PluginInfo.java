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

package io.questdb.cairo;

/**
 * Immutable holder for plugin metadata.
 * <p>
 * This class stores information about a loaded plugin including its name,
 * version, description, author, and other metadata extracted from the
 * {@code @PluginFunctions} annotation.
 */
public final class PluginInfo {
    private final String pluginName;
    private final String version;
    private final String description;
    private final String author;
    private final String url;
    private final String license;
    private final int functionCount;

    public PluginInfo(
            String pluginName,
            String version,
            String description,
            String author,
            String url,
            String license,
            int functionCount
    ) {
        this.pluginName = pluginName;
        this.version = version != null && !version.isEmpty() ? version : "unknown";
        this.description = description != null ? description : "";
        this.author = author != null ? author : "";
        this.url = url != null ? url : "";
        this.license = license != null ? license : "";
        this.functionCount = functionCount;
    }

    public String getPluginName() {
        return pluginName;
    }

    public String getVersion() {
        return version;
    }

    public String getDescription() {
        return description;
    }

    public String getAuthor() {
        return author;
    }

    public String getUrl() {
        return url;
    }

    public String getLicense() {
        return license;
    }

    public int getFunctionCount() {
        return functionCount;
    }

    /**
     * Compares two semantic version strings.
     * Strips any suffix after dash (like -SNAPSHOT) before comparing.
     *
     * @param v1 first version string
     * @param v2 second version string
     * @return negative if v1 < v2, zero if equal, positive if v1 > v2
     */
    public static int compareVersions(String v1, String v2) {
        // Strip suffixes like -SNAPSHOT, -RELEASE, etc.
        v1 = stripVersionSuffix(v1);
        v2 = stripVersionSuffix(v2);

        String[] parts1 = v1.split("\\.");
        String[] parts2 = v2.split("\\.");

        int maxLen = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < maxLen; i++) {
            int num1 = i < parts1.length ? parseVersionPart(parts1[i]) : 0;
            int num2 = i < parts2.length ? parseVersionPart(parts2[i]) : 0;
            if (num1 != num2) {
                return num1 - num2;
            }
        }
        return 0;
    }

    private static String stripVersionSuffix(String version) {
        int dashIdx = version.indexOf('-');
        return dashIdx > 0 ? version.substring(0, dashIdx) : version;
    }

    private static int parseVersionPart(String part) {
        try {
            return Integer.parseInt(part);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "PluginInfo{" +
                "name='" + pluginName + '\'' +
                ", version='" + version + '\'' +
                ", functions=" + functionCount +
                '}';
    }
}
