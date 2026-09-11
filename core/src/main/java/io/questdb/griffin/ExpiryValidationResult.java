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

import io.questdb.std.IntList;

/**
 * Result of binding and classifying an EXPIRE ROWS predicate. The compiler returns the
 * classification with the successful validation so DDL does not compile the predicate again.
 */
public final class ExpiryValidationResult {
    public static final ExpiryValidationResult MONOTONIC = new ExpiryValidationResult(false, true, true, new IntList());
    public static final ExpiryValidationResult NON_MONOTONIC = new ExpiryValidationResult(false, true, false, new IntList());

    private final boolean hasClock;
    private final boolean isDeterministic;
    private final boolean isMonotonic;
    private final IntList referencedColumnIndexes;

    public ExpiryValidationResult(
            boolean hasClock,
            boolean isDeterministic,
            boolean isMonotonic,
            IntList referencedColumnIndexes
    ) {
        this.hasClock = hasClock;
        this.isDeterministic = isDeterministic;
        this.isMonotonic = isMonotonic;
        this.referencedColumnIndexes = referencedColumnIndexes;
    }

    public IntList getReferencedColumnIndexes() {
        return referencedColumnIndexes;
    }

    public boolean hasClock() {
        return hasClock;
    }

    public boolean isDeterministic() {
        return isDeterministic;
    }

    public boolean isMonotonic() {
        return isMonotonic;
    }
}
