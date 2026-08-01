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

import java.util.concurrent.locks.Lock;

public final class SuspensionScope {
    private static final CarrierLocal<CarrierScope> SCOPE = CarrierLocal.withInitial(CarrierScope::new);

    public static @Nullable Mode enter(Mode mode) {
        final CarrierScope scope = SCOPE.get();
        final Mode previous = scope.mode;
        scope.mode = mode;
        return previous;
    }

    // The shared scope handle halves the carrier-identity lookups of an enter/restore pair; only
    // valid when no suspension can occur in between, which BLOCKING guarantees.
    public static @Nullable Mode enterBlocking(CarrierScope scope) {
        final Mode previous = scope.mode;
        scope.mode = Mode.BLOCKING;
        return previous;
    }

    public static @Nullable FiberCancellationSignal enterCancellationSignal(
            @Nullable FiberCancellationSignal cancellationSignal
    ) {
        final CarrierScope scope = SCOPE.get();
        final FiberCancellationSignal previous = scope.cancellationSignal;
        scope.cancellationSignal = cancellationSignal;
        scope.cancellationSignalGeneration = cancellationSignal != null
                ? cancellationSignal.getGeneration()
                : CancellationBinding.NO_GENERATION;
        return previous;
    }

    public static void enterRoleSwitchReadLock(CarrierScope scope, Lock lock) {
        final Fiber fiber = scope.fiber;
        final Lock currentLock = fiber != null ? fiber.getRoleSwitchReadLock() : scope.roleSwitchReadLock;
        final int currentDepth = fiber != null
                ? fiber.getRoleSwitchReadLockDepth()
                : scope.roleSwitchReadLockDepth;
        if (currentLock != null && currentLock != lock) {
            throw new IllegalStateException("another role-switch read lock is already held");
        }
        if (currentDepth == Integer.MAX_VALUE) {
            throw new IllegalStateException("role-switch read lock depth overflow");
        }
        if (fiber != null) {
            fiber.setRoleSwitchReadLock(lock, currentDepth + 1);
        } else {
            scope.roleSwitchReadLock = lock;
            scope.roleSwitchReadLockDepth = currentDepth + 1;
        }
        if (currentDepth == 0) {
            scope.roleSwitchReadLockMode = scope.mode;
            if (scope.mode == Mode.FIBER) {
                scope.mode = Mode.BLOCKING;
            }
        }
    }

    public static void enterTimerShards(@Nullable TimerShards timerShards) {
        SCOPE.get().timerShards = timerShards;
    }

    public static CancellationBinding getCancellationBindingScratch() {
        return SCOPE.get().cancellationBindingScratch;
    }

    public static @Nullable FiberCancellationSignal getCancellationSignal() {
        return SCOPE.get().cancellationSignal;
    }

    public static long getCancellationSignalGeneration() {
        return SCOPE.get().cancellationSignalGeneration;
    }

    public static @Nullable Mode getMode() {
        return SCOPE.get().mode;
    }

    public static int getRoleSwitchReadLockDepth(CarrierScope scope, Lock lock) {
        final Fiber fiber = scope.fiber;
        final Lock currentLock = fiber != null ? fiber.getRoleSwitchReadLock() : scope.roleSwitchReadLock;
        if (currentLock != lock) {
            return 0;
        }
        return fiber != null ? fiber.getRoleSwitchReadLockDepth() : scope.roleSwitchReadLockDepth;
    }

    public static @Nullable TimerShards getTimerShards() {
        return SCOPE.get().timerShards;
    }

    public static boolean hasRoleSwitchReadLock(CarrierScope scope) {
        final Fiber fiber = scope.fiber;
        return fiber != null ? fiber.getRoleSwitchReadLock() != null : scope.roleSwitchReadLock != null;
    }

    public static boolean hasRoleSwitchReadLock(CarrierScope scope, Lock lock) {
        final Fiber fiber = scope.fiber;
        return (fiber != null ? fiber.getRoleSwitchReadLock() : scope.roleSwitchReadLock) == lock;
    }

    public static void initializeCarrier() {
        SCOPE.get();
    }

    // Distinct from Fiber.isMounted(): a mounted fiber inside a BLOCKING scope must make blocking
    // progress instead of parking.
    public static boolean isFiberMode() {
        return SCOPE.get().mode == Mode.FIBER;
    }

    public static void leaveRoleSwitchReadLock(CarrierScope scope, Lock lock) {
        final Fiber fiber = scope.fiber;
        final Lock currentLock = fiber != null ? fiber.getRoleSwitchReadLock() : scope.roleSwitchReadLock;
        final int currentDepth = fiber != null
                ? fiber.getRoleSwitchReadLockDepth()
                : scope.roleSwitchReadLockDepth;
        if (currentLock != lock || currentDepth < 1) {
            throw new IllegalMonitorStateException("role-switch read lock is not held by this execution");
        }
        final boolean isFinalRelease = currentDepth == 1;
        if (fiber != null) {
            fiber.setRoleSwitchReadLock(isFinalRelease ? null : lock, currentDepth - 1);
        } else {
            scope.roleSwitchReadLock = isFinalRelease ? null : lock;
            scope.roleSwitchReadLockDepth = currentDepth - 1;
        }
        if (isFinalRelease) {
            scope.mode = scope.roleSwitchReadLockMode;
            scope.roleSwitchReadLockMode = null;
        }
    }

    public static void restore(@Nullable Mode mode) {
        SCOPE.get().mode = mode;
    }

    public static void restoreCancellationSignal(@Nullable FiberCancellationSignal cancellationSignal) {
        restoreCancellationSignal(
                cancellationSignal,
                cancellationSignal != null
                        ? cancellationSignal.getGeneration()
                        : CancellationBinding.NO_GENERATION
        );
    }

    public static void restoreCancellationSignal(
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration
    ) {
        final CarrierScope scope = SCOPE.get();
        scope.cancellationSignal = cancellationSignal;
        scope.cancellationSignalGeneration = cancellationSignalGeneration;
    }

    public static void restoreMode(CarrierScope scope, @Nullable Mode mode) {
        scope.mode = mode;
    }

    public static CarrierScope scope() {
        return SCOPE.get();
    }

    private SuspensionScope() {
    }

    public enum Mode {
        BLOCKING,
        FIBER,
        FORBIDDEN
    }

    // Opaque outside this package: the fields stay package-private.
    public static final class CarrierScope {
        final CancellationBinding cancellationBindingScratch = new CancellationBinding();
        FiberCancellationSignal cancellationSignal;
        long cancellationSignalGeneration = CancellationBinding.NO_GENERATION;
        Fiber fiber;
        Mode mode;
        Lock roleSwitchReadLock;
        int roleSwitchReadLockDepth;
        Mode roleSwitchReadLockMode;
        TimerShards timerShards;
    }
}
