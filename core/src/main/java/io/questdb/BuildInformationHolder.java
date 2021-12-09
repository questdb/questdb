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

public class BuildInformationHolder implements BuildInformation, CharSequence {
    private final CharSequence questDbVersion;
    private final CharSequence commitHash;
    private final CharSequence jdkVersion;
    private final String buildKey;

    public BuildInformationHolder() {
        this(
                "Unknown Version",
                "Unknown Version",
                "Unknown Version"
        );
    }

    public BuildInformationHolder(
            CharSequence questDbVersion,
            CharSequence commitHash,
            CharSequence jdkVersion
    ) {
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
}
