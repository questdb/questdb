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

package io.questdb.cutlass.http.client;

/**
 * Interface for receiving HTTP response data.
 */

import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8s;

public interface Response {
    default void copyTextTo(Utf8Sink sink) {
        Fragment fragment;
        while ((fragment = recv()) != null) {
            Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
        }
    }

    default void copyTextTo(Utf16Sink sink) {
        Fragment fragment;
        while ((fragment = recv()) != null) {
            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
        }
    }

    default void discard() {
        //noinspection StatementWithEmptyBody
        while ((recv()) != null) {
        }
    }

    /**
     * Receives the next fragment of response data using the default timeout.
     *
     * @return the received fragment
     */
    Fragment recv();

    /**
     * Receives the next fragment of response data with the specified timeout.
     *
     * @param timeout the timeout in milliseconds
     * @return the received fragment
     */
    Fragment recv(int timeout);
}
