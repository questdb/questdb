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
import io.questdb.std.CarrierLocal;

/**
 * Cooperative-yield signal thrown by the live-view refresh worker when a
 * drain-triggered O3 replay needs the base table applied to a seqTxn that
 * {@code ApplyWal2TableJob} has not reached yet. Unlike a hard failure, this is
 * not an error condition: the refresh worker unwinds its current cycle without
 * advancing any watermark or touching the flush-retry budget, and the next
 * fallback scan re-triggers the view once the base apply has caught up.
 * <p>
 * It exists to keep the replay path <em>cooperative</em>. Block-spinning inside
 * {@code LiveViewRefreshJob.waitForApply} until the apply catches up starves the
 * refresh worker for up to the whole flush-retry budget, and on the
 * single-threaded refresh/drain model the fuzz harness drives it deadlocks
 * outright: the same thread that has to advance the base apply is the one
 * spinning. Throwing instead yields the worker so the apply can proceed.
 * <p>
 * Deliberately NOT a {@link io.questdb.cairo.CairoException}: several O3-replay
 * callers wrap their work in a {@code catch (CairoException)} that recovers or
 * re-derives, and this signal must bypass all of them and reach the top-level
 * apply-lag handler in {@code LiveViewRefreshJob.refreshInstance}. It is a
 * thread-local flyweight (no stack trace, no per-throw allocation), reused
 * across refresh cycles on the same worker.
 */
public class LiveViewApplyLagException extends RuntimeException {
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final CarrierLocal<LiveViewApplyLagException> tlException =
            new CarrierLocal<>(LiveViewApplyLagException::new);
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
