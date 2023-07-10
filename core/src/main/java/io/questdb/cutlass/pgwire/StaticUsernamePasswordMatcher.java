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

public class StaticUsernamePasswordMatcher implements UsernamePasswordMatcher {
    private final int defaultPasswordLen;
    private final String defaultUsername;
    private final String roUsername;
    private long defaultPasswordPtr;
    private int roPasswordLen;
    private long roPasswordPtr;

    public StaticUsernamePasswordMatcher(PGWireConfiguration configuration) {
        // todo: test empty password
        this.defaultUsername = configuration.getDefaultUsername();
        byte[] defaultPasswordBytes = configuration.getDefaultPassword().getBytes(StandardCharsets.UTF_8);
        int defaultPasswordUtfLength = defaultPasswordBytes.length;
        defaultPasswordPtr = Unsafe.malloc(defaultPasswordUtfLength, MemoryTag.NATIVE_DEFAULT);
        defaultPasswordLen = defaultPasswordUtfLength;
        for (int i = 0; i < defaultPasswordUtfLength; i++) {
            Unsafe.getUnsafe().putByte(defaultPasswordPtr + i, defaultPasswordBytes[i]);
        }
        if (configuration.isReadOnlyUserEnabled()) {
            byte[] roPasswordBytes = configuration.getReadOnlyPassword().getBytes(StandardCharsets.UTF_8);
            int roPasswordUtfLength = roPasswordBytes.length;
            roPasswordPtr = Unsafe.malloc(roPasswordUtfLength, MemoryTag.NATIVE_DEFAULT);
            roPasswordLen = roPasswordUtfLength;
            for (int i = 0; i < roPasswordUtfLength; i++) {
                Unsafe.getUnsafe().putByte(roPasswordPtr + i, roPasswordBytes[i]);
            }
            this.roUsername = configuration.getReadOnlyUsername();
        } else {
            this.roUsername = null;
            this.roPasswordPtr = 0;
            this.roPasswordLen = -1;
        }
    }

    @Override
    public void close() {
        defaultPasswordPtr = Unsafe.free(defaultPasswordPtr, defaultPasswordLen, MemoryTag.NATIVE_DEFAULT);
        if (roPasswordLen != -1) {
            roPasswordPtr = Unsafe.free(roPasswordPtr, roPasswordLen, MemoryTag.NATIVE_DEFAULT);
            roPasswordLen = -1;
        }
    }

    @Override
    public boolean verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        boolean matchRo = roUsername != null
                && passwordLen == roPasswordLen
                && Chars.equals(roUsername, username)
                && Vect.memeq(roPasswordPtr, passwordPtr, passwordLen);

        return matchRo
                || (Chars.equals(defaultUsername, username)
                && passwordLen == defaultPasswordLen
                && Vect.memeq(defaultPasswordPtr, passwordPtr, passwordLen));
    }
}
