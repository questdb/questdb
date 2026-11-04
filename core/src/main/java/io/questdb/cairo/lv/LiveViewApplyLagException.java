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

package io.questdb.cairo.lv;

import io.questdb.cairo.TableToken;
import io.questdb.std.FiberLocal;

/**
 * Cooperative yield when replay or rebuild needs a base seqTxn not yet applied.
 * Deferral advances no watermark and spends no flush-retry budget. Rebuild must wait
 * through the highest base transaction represented in the view's durable output.
 * Blocking would starve the worker and can deadlock a single-threaded apply/drain.
 * <p>
 * This thread-local flyweight deliberately is not a CairoException. Recovery catches
 * must pass it to the apply-lag handler instead of counting it as a refresh failure.
 */
public class LiveViewApplyLagException extends RuntimeException {
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final FiberLocal<LiveViewApplyLagException> tlException =
            new FiberLocal<>(LiveViewApplyLagException::new);
    private long appliedSeqTxn;
    private CharSequence baseTableName;
    private long targetSeqTxn;

    public static LiveViewApplyLagException instance(TableToken baseToken, long targetSeqTxn, long appliedSeqTxn) {
        LiveViewApplyLagException ex = tlException.get();
        // This is to have a correct stack trace in local debugging with -ea option.
        assert (ex = new LiveViewApplyLagException()) != null;
        ex.baseTableName = baseToken.getTableName();
        ex.targetSeqTxn = targetSeqTxn;
        ex.appliedSeqTxn = appliedSeqTxn;
        return ex;
    }

    public long getAppliedSeqTxn() {
        return appliedSeqTxn;
    }

    public CharSequence getBaseTableName() {
        return baseTableName;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        StackTraceElement[] result = EMPTY_STACK_TRACE;
        // This is to have a correct stack trace reported in CI.
        assert (result = super.getStackTrace()) != null;
        return result;
    }

    public long getTargetSeqTxn() {
        return targetSeqTxn;
    }
}
