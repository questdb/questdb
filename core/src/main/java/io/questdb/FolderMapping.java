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

import io.questdb.std.Files;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FolderMapping {

    private static final char SEPARATOR = ',';

    private final LowerCaseCharSequenceObjHashMap<CharSequence> map = new LowerCaseCharSequenceObjHashMap<>(4);
    private int lo, hi, limit;

    public void forEach(LowerCaseCharSequenceObjHashMap.CharSequenceObjConsumer<CharSequence> action) {
        map.forEach(action);
    }

    public FolderMapping of(@Nullable CharSequence text, @NotNull Path path) throws ServerConfigurationException {
        if (text != null) {
            // 'any-case-alias' -> 'absolute path to volume' (quotes are optional)
            map.clear();
            limit = text.length();
            lo = 0;
            int separatorCount = 0;
            CharSequence alias = null;
            for (int i = 0; i < limit; i++) {
                char c = text.charAt(i);
                if (c == '-' && i + 1 < limit && text.charAt(i + 1) == '>') { // found arrow
                    if (alias != null) {
                        throw new ServerConfigurationException("invalid syntax, missing volume path at offset " + lo);
                    }
                    alias = popToken(text, i);
                    lo = ++i + 1; // move over '>'
                } else if (SEPARATOR == c) {
                    if (alias == null) {
                        throw new ServerConfigurationException("invalid syntax, missing alias at offset " + lo);
                    }
                    addVolumeDefinition(alias, popToken(text, i), path);
                    alias = null;
                    lo = i + 1;
                    separatorCount++;
                }
            }
            finishVolumeDefinitions(alias, popLastToken(text), path, separatorCount);
        }
        return this;
    }

    public @Nullable CharSequence resolveAlias(@NotNull CharSequence alias) {
        return map.get(alias);
    }

    private void addVolumeDefinition(CharSequence alias, CharSequence volumePath, Path path) throws ServerConfigurationException {
        if (!Files.isDirOrSoftLinkDir(path.of(volumePath).$())) {
            throw new ServerConfigurationException("inaccessible volume [path=" + path + ']');
        }
        if (!map.put(alias, volumePath)) {
            throw new ServerConfigurationException("duplicate alias [alias=" + alias + ']');
        }
    }

    private void finishVolumeDefinitions(CharSequence alias, CharSequence volumePath, Path path, int separatorCount) throws ServerConfigurationException {
        if (volumePath != null) {
            if (alias == null) {
                throw new ServerConfigurationException("invalid syntax, dangling alias [alias=" + volumePath + ", offset=" + lo + ']');
            }
            addVolumeDefinition(alias, volumePath, path);
        } else if (alias != null) {
            throw new ServerConfigurationException("invalid syntax, missing volume path at offset " + hi);
        } else if (separatorCount > 0) {
            throw new ServerConfigurationException("invalid syntax, dangling separator [sep='" + SEPARATOR + "']");
        }
    }

    private CharSequence popLastToken(CharSequence paths) throws ServerConfigurationException {
        return popToken(paths, limit, true);
    }

    private CharSequence popToken(CharSequence paths, int i, boolean isOptional) throws ServerConfigurationException {
        while (lo < limit && paths.charAt(lo) == ' ') { // left trim
            lo++;
        }
        hi = i;
        while (hi > lo && paths.charAt(hi - 1) == ' ') { // right trim
            hi--;
        }
        if (hi < lo + 1) {
            if (isOptional) {
                return null;
            }
            throw new ServerConfigurationException("empty value at offset " + hi);
        }
        if (paths.charAt(hi - 1) == '\'') {
            if (paths.charAt(lo) != '\'') {
                throw new ServerConfigurationException("missing opening quote at offset " + lo);
            }
            if (--hi < ++lo + 1) {
                if (isOptional) {
                    return null;
                }
                throw new ServerConfigurationException("empty value at offset " + hi);
            }
        } else if (paths.charAt(lo) == '\'') {
            throw new ServerConfigurationException("missing closing quote at offset " + (hi - 1));
        }
        return paths.subSequence(lo, hi);
    }

    private CharSequence popToken(CharSequence paths, int i) throws ServerConfigurationException {
        return popToken(paths, i, false);
    }
}
