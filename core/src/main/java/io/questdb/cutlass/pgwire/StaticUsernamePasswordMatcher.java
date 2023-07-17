/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.pgwire;

import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.nio.charset.StandardCharsets;

public final class StaticUsernamePasswordMatcher implements UsernamePasswordMatcher {
    private int defaultPasswordLen;
    private long defaultPasswordPtr;
    private String defaultUsername;
    private int roPasswordLen;
    private long roPasswordPtr;
    private String roUsername;

    public StaticUsernamePasswordMatcher(PGWireConfiguration configuration) {
        String configuredDefaultUsername = configuration.getDefaultUsername();
        String defaultPassword = configuration.getDefaultPassword();

        if (Chars.empty(configuredDefaultUsername) || Chars.empty(defaultPassword)) {
            defaultUsername = null;
            defaultPasswordPtr = 0;
            defaultPasswordLen = 0;
        } else {
            defaultUsername = configuredDefaultUsername;
            byte[] defaultPasswordBytes = defaultPassword.getBytes(StandardCharsets.UTF_8);
            int defaultPasswordUtfLength = defaultPasswordBytes.length;
            defaultPasswordPtr = Unsafe.malloc(defaultPasswordUtfLength, MemoryTag.NATIVE_DEFAULT);
            defaultPasswordLen = defaultPasswordUtfLength;
            for (int i = 0; i < defaultPasswordUtfLength; i++) {
                Unsafe.getUnsafe().putByte(defaultPasswordPtr + i, defaultPasswordBytes[i]);
            }
        }

        String readOnlyUsername = configuration.getReadOnlyUsername();
        String readOnlyPassword = configuration.getReadOnlyPassword();
        if (configuration.isReadOnlyUserEnabled() && !Chars.empty(readOnlyUsername) && !Chars.empty(readOnlyPassword)) {
            byte[] roPasswordBytes = readOnlyPassword.getBytes(StandardCharsets.UTF_8);
            roPasswordLen = roPasswordBytes.length;
            roPasswordPtr = Unsafe.malloc(roPasswordLen, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < roPasswordLen; i++) {
                Unsafe.getUnsafe().putByte(roPasswordPtr + i, roPasswordBytes[i]);
            }
            this.roUsername = readOnlyUsername;
        } else {
            this.roUsername = null;
            this.roPasswordPtr = 0;
            this.roPasswordLen = 0;
        }
    }

    @Override
    public void close() {
        if (defaultPasswordLen != 0) {
            defaultUsername = null;
            defaultPasswordPtr = Unsafe.free(defaultPasswordPtr, defaultPasswordLen, MemoryTag.NATIVE_DEFAULT);
            defaultPasswordLen = 0;
        }
        if (roPasswordLen != 0) {
            roUsername = null;
            roPasswordPtr = Unsafe.free(roPasswordPtr, roPasswordLen, MemoryTag.NATIVE_DEFAULT);
            roPasswordLen = 0;
        }
    }

    @Override
    public boolean verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        boolean matchRo = roUsername != null
                && passwordLen == roPasswordLen
                && Chars.equals(roUsername, username)
                && Vect.memeq(roPasswordPtr, passwordPtr, passwordLen);

        return matchRo || (!Chars.empty(defaultUsername)
                && Chars.equals(defaultUsername, username)
                && passwordLen == defaultPasswordLen
                && Vect.memeq(defaultPasswordPtr, passwordPtr, passwordLen)
        );
    }
}
