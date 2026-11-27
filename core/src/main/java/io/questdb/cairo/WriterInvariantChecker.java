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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects violations of the writer's column-mapping and truncation invariants, reported by the checks that run
 * when {@link CairoConfiguration#isDebugWriterInvariantCheckEnabled()} is on (the test harness turns it on, production
 * never does).
 * <p>
 * A violation is recorded rather than thrown: several checks run inside a writer's close, where an exception would
 * skip the rest of the cleanup. The test harness fails the test when the count is non-zero at the end of it.
 */
public final class WriterInvariantChecker {
    private static final Log LOG = LogFactory.getLog(WriterInvariantChecker.class);
    private static final AtomicLong violationCount = new AtomicLong();
    private static volatile String lastViolation;

    private WriterInvariantChecker() {
    }

    public static String getLastViolation() {
        return lastViolation;
    }

    public static long getViolationCount() {
        return violationCount.get();
    }

    public static void reportViolation(CharSequence message) {
        final String msg = message.toString();
        lastViolation = msg;
        violationCount.incrementAndGet();
        LOG.critical().$("writer invariant violation: ").$(msg).$();
    }
}
