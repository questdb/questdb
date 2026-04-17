/*+*****************************************************************************
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

/**
 * Internal control-flow signal thrown by SqlCodeGenerator.generateFill when the
 * fully-resolved groupBy metadata contains a PREV source column whose type is
 * not supported by the fast path. Callers of generateFill catch this specific
 * type, restore the stashed SAMPLE BY node onto the model, and re-dispatch to
 * generateSampleBy to produce a legacy cursor.
 * <p>
 * Singleton: the exception carries no position or message - it is not
 * user-facing. The zero-allocation rare path justifies reusing one instance.
 * {@link #fillInStackTrace()} is overridden to skip stack walking since the
 * control-flow use does not need a stack trace.
 */
public final class FallbackToLegacyException extends SqlException {
    public static final FallbackToLegacyException INSTANCE = new FallbackToLegacyException();

    private FallbackToLegacyException() {
        super();
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
