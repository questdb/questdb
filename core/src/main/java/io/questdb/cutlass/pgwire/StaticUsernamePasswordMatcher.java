/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.Vect;

public class StaticUsernamePasswordMatcher implements UsernamePasswordMatcher {
    private final int passwordLen;
    private final long passwordPtr;
    private final String username;

    public StaticUsernamePasswordMatcher(String username, long passwordPtr, int passwordLen) {
        assert !Chars.empty(username);
        assert passwordPtr != 0;
        assert passwordLen > 0;

        this.username = username;
        this.passwordPtr = passwordPtr;
        this.passwordLen = passwordLen;
    }

    @Override
    public boolean verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        return username != null
                && this.username != null
                && Chars.equals(this.username, username)
                && this.passwordLen == passwordLen
                && Vect.memeq(this.passwordPtr, passwordPtr, passwordLen);
    }
}
