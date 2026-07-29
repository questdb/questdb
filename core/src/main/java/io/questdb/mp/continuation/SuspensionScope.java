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

package io.questdb.mp.continuation;

import io.questdb.std.CarrierLocal;
import org.jetbrains.annotations.Nullable;

public final class SuspensionScope {
    private static final CarrierLocal<FiberCancellationSignal> CANCELLATION_SIGNAL = new CarrierLocal<>();
    private static final CarrierLocal<Mode> CURRENT = new CarrierLocal<>();

    public static @Nullable Mode enter(Mode mode) {
        final Mode previous = CURRENT.get();
        CURRENT.set(mode);
        return previous;
    }

    public static @Nullable FiberCancellationSignal enterCancellationSignal(
            @Nullable FiberCancellationSignal cancellationSignal
    ) {
        final FiberCancellationSignal previous = CANCELLATION_SIGNAL.get();
        CANCELLATION_SIGNAL.set(cancellationSignal);
        return previous;
    }

    public static @Nullable FiberCancellationSignal getCancellationSignal() {
        return CANCELLATION_SIGNAL.get();
    }

    public static @Nullable Mode getMode() {
        return CURRENT.get();
    }

    public static void initializeCarrier() {
        CANCELLATION_SIGNAL.get();
        CURRENT.get();
    }

    public static void restore(@Nullable Mode mode) {
        CURRENT.set(mode);
    }

    public static void restoreCancellationSignal(@Nullable FiberCancellationSignal cancellationSignal) {
        CANCELLATION_SIGNAL.set(cancellationSignal);
    }

    private SuspensionScope() {
    }

    public enum Mode {
        BLOCKING,
        FIBER,
        FORBIDDEN
    }
}
