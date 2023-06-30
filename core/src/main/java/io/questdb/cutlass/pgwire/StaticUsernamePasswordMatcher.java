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

public class StaticUsernamePasswordMatcher implements UsernamePasswordMatcher {
    private final String defaultPassword;
    private final String defaultUsername;
    private final String roPassword;
    private final String roUsername;

    public StaticUsernamePasswordMatcher(PGWireConfiguration configuration) {
        this.defaultUsername = configuration.getDefaultUsername();
        this.defaultPassword = configuration.getDefaultPassword();
        if (configuration.isReadOnlyUserEnabled()) {
            this.roUsername = configuration.getReadOnlyUsername();
            this.roPassword = configuration.getReadOnlyPassword();
        } else {
            this.roUsername = null;
            this.roPassword = null;
        }
    }

    @Override
    public boolean verifyPassword(CharSequence username, CharSequence password) {
        boolean matchRo = roUsername != null && roPassword != null && Chars.equals(roUsername, username) && Chars.equals(roPassword, password);
        return matchRo || Chars.equals(defaultUsername, username) && Chars.equals(defaultPassword, password);
    }
}
