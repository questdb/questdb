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

package io.questdb.griffin;

import io.questdb.cairo.sql.TableReferenceOutOfDateException;

/**
 * Thrown when an EXPIRE ROWS decision cannot be used because the resolved source is in a policy transition,
 * or because a keep-filter was chosen from a metadata version that a concurrent SET/DROP EXPIRE has already
 * replaced. It extends {@link TableReferenceOutOfDateException}, so callers can use their existing metadata
 * drift retry or deferral paths. {@link SqlCompilerImpl} retries the signal internally for ordinary compilation,
 * while materializing compilation propagates it to the CREATE or refresh operation that owns the stable guard.
 * <p>
 * One shared instance is thrown as a control-flow signal; its stack trace is never shown.
 */
public final class ExpiryPolicyVersionChangedException extends TableReferenceOutOfDateException {
    static final ExpiryPolicyVersionChangedException INSTANCE = new ExpiryPolicyVersionChangedException();
    private static final String MESSAGE =
            "cached query plan cannot be used because the row-expiry policy changed during compilation";

    private ExpiryPolicyVersionChangedException() {
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return MESSAGE;
    }

    @Override
    public String getMessage() {
        return MESSAGE;
    }
}
