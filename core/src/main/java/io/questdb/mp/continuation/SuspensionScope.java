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
    private static final CarrierLocal<CarrierScope> SCOPE = CarrierLocal.withInitial(CarrierScope::new);

    public static @Nullable Mode enter(Mode mode) {
        final CarrierScope scope = SCOPE.get();
        final Mode previous = scope.mode;
        scope.mode = mode;
        return previous;
    }

    public static @Nullable FiberCancellationSignal enterCancellationSignal(
            @Nullable FiberCancellationSignal cancellationSignal
    ) {
        final CarrierScope scope = SCOPE.get();
        final FiberCancellationSignal previous = scope.cancellationSignal;
        scope.cancellationSignal = cancellationSignal;
        return previous;
    }

    public static @Nullable FiberCancellationSignal getCancellationSignal() {
        return SCOPE.get().cancellationSignal;
    }

    public static @Nullable Mode getMode() {
        return SCOPE.get().mode;
    }

    public static void initializeCarrier() {
        SCOPE.get();
    }

    public static void restore(@Nullable Mode mode) {
        SCOPE.get().mode = mode;
    }

    public static void restoreCancellationSignal(@Nullable FiberCancellationSignal cancellationSignal) {
        SCOPE.get().cancellationSignal = cancellationSignal;
    }

    static CarrierScope scope() {
        return SCOPE.get();
    }

    private SuspensionScope() {
    }

    public enum Mode {
        BLOCKING,
        FIBER,
        FORBIDDEN
    }

    static final class CarrierScope {
        FiberCancellationSignal cancellationSignal;
        Fiber fiber;
        Mode mode;
    }
}
