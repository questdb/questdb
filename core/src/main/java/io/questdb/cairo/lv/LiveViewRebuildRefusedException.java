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

import io.questdb.std.CarrierLocal;

/**
 * Thrown out of the live-view refresh worker's whole-view rebuild when
 * {@link LiveViewRebuildRestatementGuard} refuses it: the rebuild would have dropped rows
 * the view retains. Nothing durable has moved by the time it is thrown - the replacement
 * is uncommitted, and a timeline the rebuild would have retired is still on disk - so the
 * rebuild's caller stops the view rather than counting a failure. The guard, not this
 * signal, carries the evidence.
 * <p>
 * Deliberately NOT a {@link io.questdb.cairo.CairoException}, for the reason
 * {@link LiveViewApplyLagException} gives: the rebuild's callers wrap it in catch arms that
 * count, retry or invalidate, and a refusal is none of those. It must reach the one arm per
 * caller that recognizes it. It is a thread-local flyweight (no stack trace, no per-throw
 * allocation).
 */
public class LiveViewRebuildRefusedException extends RuntimeException {
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final CarrierLocal<LiveViewRebuildRefusedException> tlException =
            new CarrierLocal<>(LiveViewRebuildRefusedException::new);

    public static LiveViewRebuildRefusedException instance() {
        LiveViewRebuildRefusedException ex = tlException.get();
        // This is to have a correct stack trace in local debugging with -ea option.
        assert (ex = new LiveViewRebuildRefusedException()) != null;
        return ex;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        StackTraceElement[] result = EMPTY_STACK_TRACE;
        // This is to have a correct stack trace reported in CI.
        assert (result = super.getStackTrace()) != null;
        return result;
    }
}
