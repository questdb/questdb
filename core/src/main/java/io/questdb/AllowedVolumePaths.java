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
import org.jetbrains.annotations.TestOnly;

public class AllowedVolumePaths {

    private static final char SEPARATOR = ',';

    private final LowerCaseCharSequenceObjHashMap<CharSequence> allowedVolumePaths = new LowerCaseCharSequenceObjHashMap<>(4);
    private int start, end, len;

    public void forEach(LowerCaseCharSequenceObjHashMap.CharSequenceObjConsumer<CharSequence> action) {
        allowedVolumePaths.forEach(action);
    }

    @TestOnly
    public boolean isValidVolumeAlias(@NotNull CharSequence alias) {
        return allowedVolumePaths.contains(alias);
    }

    public void of(@Nullable CharSequence paths, @NotNull Path path) throws ServerConfigurationException {
        if (paths == null) {
            return;
        }
        
        // any-case-alias -> 'absolute path to volume' (quotes are optional)
        allowedVolumePaths.clear();
        len = paths.length();
        start = 0;
        CharSequence alias = null;
        for (int i = 0; i < len; i++) {
            char c = paths.charAt(i);
            if (c == '-' && i + 1 < len && paths.charAt(i + 1) == '>') {
                // found arrow
                alias = nextToken(paths, i, false);
                i++; // move over '>'
                start = i + 1;
            } else if (SEPARATOR == c) {
                // found path
                if (alias == null) {
                    throw new ServerConfigurationException("invalid syntax, missing alias declaration at offset " + start);
                }
                CharSequence volumePath = nextToken(paths, i, false);
                if (!Files.isDirOrSoftLinkDir(path.of(volumePath).$())) {
                    throw ServerConfigurationException.forInvalidVolumePath(path);
                }
                allowedVolumePaths.put(alias, volumePath);
                start = i + 1;
                alias = null;
            }
        }
        CharSequence volumePath = nextToken(paths, len, true);
        if (alias == null) {
            if (volumePath != null) {
                throw new ServerConfigurationException("invalid syntax, missing path declaration at offset " + start);
            }
            // a trailing comma is ok
        } else {
            if (volumePath == null) {
                throw new ServerConfigurationException("empty value at offset " + end);
            }
            if (!Files.isDirOrSoftLinkDir(path.of(volumePath).$())) {
                throw ServerConfigurationException.forInvalidVolumePath(path);
            }
            allowedVolumePaths.put(alias, volumePath);
        }
    }

    public @Nullable CharSequence resolveAlias(@NotNull CharSequence alias) {
        return allowedVolumePaths.get(alias);
    }

    private CharSequence nextToken(CharSequence paths, int i, boolean isOptional) throws ServerConfigurationException {
        while (start < len && paths.charAt(start) == ' ') {
            start++;
        }
        end = i;
        while (end > start && paths.charAt(end - 1) == ' ') {
            end--;
        }
        if (end < start + 1) {
            if (isOptional) {
                return null;
            }
            throw new ServerConfigurationException("empty value at offset " + end);
        }
        if (paths.charAt(end - 1) == '\'') {
            if (paths.charAt(start) != '\'') {
                throw new ServerConfigurationException("missing opening quote at offset " + start);
            }
            start++;
            end--;
            if (end < start + 1) {
                if (isOptional) {
                    return null;
                }
                throw new ServerConfigurationException("empty value at offset " + end);
            }
        } else if (paths.charAt(start) == '\'') {
            throw new ServerConfigurationException("missing closing quote at offset " + (end - 1));
        }
        return paths.subSequence(start, end);
    }
}
