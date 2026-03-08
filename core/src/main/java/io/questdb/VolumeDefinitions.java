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

import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public class VolumeDefinitions implements Mutable {
    private static final char SEPARATOR = ',';
    private final LowerCaseCharSequenceObjHashMap<String> aliasToVolumeRoot = new LowerCaseCharSequenceObjHashMap<>(4);
    private int hi;
    private int limit;
    private int lo;

    @TestOnly
    @Override
    public void clear() {
        aliasToVolumeRoot.clear();
        lo = hi = limit = 0;
    }

    public void forEach(LowerCaseCharSequenceObjHashMap.CharSequenceObjConsumer<String> action) {
        aliasToVolumeRoot.forEach(action);
    }

    public VolumeDefinitions of(@Nullable CharSequence definitions, @NotNull Path path, @NotNull String root) throws ServerConfigurationException {
        if (definitions != null) {
            // 'any-case-alias' -> 'absolute path to volume' (quotes are optional)
            aliasToVolumeRoot.clear();
            limit = definitions.length();
            lo = 0;
            int separatorCount = 0;
            String alias = null;
            for (int i = 0; i < limit; i++) {
                char c = definitions.charAt(i);
                if (c == '-' && i + 1 < limit && definitions.charAt(i + 1) == '>') { // found arrow
                    if (alias != null) {
                        throw new ServerConfigurationException("invalid syntax, missing volume path at offset " + lo);
                    }
                    alias = Chars.toString(popToken(definitions, i, false));
                    lo = ++i + 1; // move over '>'
                } else if (SEPARATOR == c) {
                    if (alias == null) {
                        throw new ServerConfigurationException("invalid syntax, missing alias at offset " + lo);
                    }
                    addVolumeDefinition(alias, popToken(definitions, i, false), path, root);
                    alias = null;
                    lo = i + 1;
                    separatorCount++;
                }
            }
            finishVolumeDefinitions(alias, popToken(definitions, limit, true), path, root, separatorCount);
        }
        return this;
    }

    public @Nullable CharSequence resolveAlias(@NotNull CharSequence alias) {
        return aliasToVolumeRoot.get(alias);
    }

    public @Nullable CharSequence resolvePath(@NotNull CharSequence path) {
        for (int i = 0, n = aliasToVolumeRoot.keys().size(); i < n; i++) {
            final CharSequence candidateAlias = aliasToVolumeRoot.keys().get(i);
            final CharSequence candidatePath = aliasToVolumeRoot.get(candidateAlias);
            if (Chars.equals(candidatePath, path)) {
                return candidateAlias;
            }
        }
        return null;
    }

    private void addVolumeDefinition(String alias, CharSequence volumePath, Path path, String root) throws ServerConfigurationException {
        int len = volumePath.length();
        if (len > 0 && volumePath.charAt(len - 1) == Files.SEPARATOR) {
            len--;
        }
        if (!Files.isDirOrSoftLinkDir(path.of(volumePath, 0, len).$())) {
            throw new ServerConfigurationException("inaccessible volume [path=" + volumePath + ']');
        }
        String volumeRoot = Utf8s.toString(path);
        if (volumeRoot.equals(root)) {
            throw new ServerConfigurationException("standard volume cannot have an alias [alias=" + alias + ", root=" + root + ']');
        }
        if (!aliasToVolumeRoot.put(alias, volumeRoot)) {
            throw new ServerConfigurationException("duplicate alias [alias=" + alias + ']');
        }
    }

    private void finishVolumeDefinitions(String alias, CharSequence volumePath, Path path, String root, int separatorCount) throws ServerConfigurationException {
        if (volumePath != null) {
            if (alias == null) {
                throw new ServerConfigurationException("invalid syntax, dangling alias [alias=" + volumePath + ", offset=" + lo + ']');
            }
            addVolumeDefinition(alias, volumePath, path, root);
        } else if (alias != null) {
            throw new ServerConfigurationException("invalid syntax, missing volume path at offset " + hi);
        } else if (separatorCount > 0) {
            throw new ServerConfigurationException("invalid syntax, dangling separator [sep='" + SEPARATOR + "']");
        }
    }

    private CharSequence popToken(CharSequence definitions, int i, boolean isOptional) throws ServerConfigurationException {
        while (lo < limit && definitions.charAt(lo) == ' ') { // left trim
            lo++;
        }
        hi = i;
        while (hi > lo && definitions.charAt(hi - 1) == ' ') { // right trim
            hi--;
        }
        if (hi < lo + 1) {
            if (isOptional) {
                return null;
            }
            throw new ServerConfigurationException("empty value at offset " + hi);
        }
        if (definitions.charAt(hi - 1) == '\'') {
            if (definitions.charAt(lo) != '\'') {
                throw new ServerConfigurationException("missing opening quote at offset " + lo);
            }
            if (--hi < ++lo + 1) {
                if (isOptional) {
                    return null;
                }
                throw new ServerConfigurationException("empty value at offset " + hi);
            }
        } else if (definitions.charAt(lo) == '\'') {
            throw new ServerConfigurationException("missing closing quote at offset " + (hi - 1));
        }
        return definitions.subSequence(lo, hi);
    }
}
