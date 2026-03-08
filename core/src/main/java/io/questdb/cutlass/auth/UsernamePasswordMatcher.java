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

package io.questdb.cutlass.auth;

public interface UsernamePasswordMatcher {

    /**
     * Verify password for a user.
     * <p>
     * When the user has no active password then this method returns 0.
     * This also return 0 when username is empty or null.
     *
     * @param username    name of user, cannot be null or empty
     * @param passwordPtr pointer to password
     * @param passwordLen length of password
     * @return a positive byte representing the authentication type, 0 (AUTH_TYPE_NONE) means wrong credentials
     */
    byte verifyPassword(CharSequence username, long passwordPtr, int passwordLen);
}
