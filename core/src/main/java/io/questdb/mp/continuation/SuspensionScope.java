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
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
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

    public static void enterCancellationSignal(
            @Nullable FiberCancellationSignal cancellationSignal
    ) {
        enterCancellationSignal(
                cancellationSignal,
                cancellationSignal != null
                        ? cancellationSignal.getGeneration()
                        : CancellationBinding.NO_GENERATION
        );
    }

    public static void enterCancellationSignal(
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration
    ) {
        final CarrierScope scope = SCOPE.get();
        scope.cancellationSignal = cancellationSignal;
        scope.cancellationSignalGeneration = cancellationSignalGeneration;
    }

    public static void enterCancellationSource(@Nullable CancellationBinding.Source cancellationSource) {
        SCOPE.get().cancellationSource = cancellationSource;
    }

    public static void enterRoleSwitchReadLock(CarrierScope scope, Lock lock) {
        final RoleSwitchReadLockState state = getRoleSwitchReadLockState(scope);
        final boolean isFirstLock = !state.hasAny();
        state.enter(lock);
        if (isFirstLock) {
            state.setPreviousMode(scope.mode);
            if (scope.mode == Mode.FIBER) {
                scope.mode = Mode.BLOCKING;
            }
        }
    }

    public static void enterRoleSwitchWriteLock(CarrierScope scope) {
        if (scope.fiber != null) {
            throw new IllegalStateException("fiber cannot own a role-switch write lock");
        }
        if (scope.roleSwitchWriteLockDepth == Integer.MAX_VALUE) {
            throw new IllegalStateException("role-switch write lock depth overflow");
        }
        scope.roleSwitchWriteLockDepth++;
    }

    public static void enterSupplementalCancellationSignal(
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration
    ) {
        final CarrierScope scope = SCOPE.get();
        scope.supplementalCancellationSignal = cancellationSignal;
        scope.supplementalCancellationSignalGeneration = cancellationSignalGeneration;
    }

    public static void enterTimerShards(@Nullable TimerShards timerShards) {
        SCOPE.get().timerShards = timerShards;
    }

    public static CancellationBinding getCancellationBindingScratch() {
        return getCancellationBindingScratch(SCOPE.get());
    }

    public static CancellationBinding getCancellationBindingScratch(CarrierScope scope) {
        return scope.fiber != null
                ? scope.fiber.getCancellationBindingScratch()
                : scope.cancellationBindingScratch;
    }

    public static @Nullable FiberCancellationSignal getCancellationSignal() {
        return SCOPE.get().cancellationSignal;
    }

    public static @Nullable FiberCancellationSignal getCancellationSignal(CarrierScope scope) {
        return scope.cancellationSignal;
    }

    public static long getCancellationSignalGeneration() {
        return SCOPE.get().cancellationSignalGeneration;
    }

    public static long getCancellationSignalGeneration(CarrierScope scope) {
        return scope.cancellationSignalGeneration;
    }

    public static @Nullable CancellationBinding.Source getCancellationSource() {
        return SCOPE.get().cancellationSource;
    }

    public static @Nullable CancellationBinding.Source getCancellationSource(CarrierScope scope) {
        return scope.cancellationSource;
    }

    public static @Nullable Mode getMode() {
        return SCOPE.get().mode;
    }

    public static int getRoleSwitchReadLockDepth(CarrierScope scope, Lock lock) {
        return getRoleSwitchReadLockState(scope).getDepth(lock);
    }

    public static @Nullable FiberCancellationSignal getSupplementalCancellationSignal() {
        return SCOPE.get().supplementalCancellationSignal;
    }

    public static @Nullable FiberCancellationSignal getSupplementalCancellationSignal(CarrierScope scope) {
        return scope.supplementalCancellationSignal;
    }

    public static long getSupplementalCancellationSignalGeneration() {
        return SCOPE.get().supplementalCancellationSignalGeneration;
    }

    public static long getSupplementalCancellationSignalGeneration(CarrierScope scope) {
        return scope.supplementalCancellationSignalGeneration;
    }


    public static @Nullable TimerShards getTimerShards(CarrierScope scope) {
        return scope.timerShards;
    }

    public static boolean hasRoleSwitchReadLock(CarrierScope scope, Lock lock) {
        return getRoleSwitchReadLockState(scope).hasLock(lock);
    }

    public static boolean hasAnyRoleSwitchLock() {
        return hasAnyRoleSwitchLock(SCOPE.get());
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
        final RoleSwitchReadLockState state = getRoleSwitchReadLockState(scope);
        state.leave(lock);
        if (!state.hasAny()) {
            scope.mode = state.takePreviousMode();
        }
    }

    public static void leaveRoleSwitchWriteLock(CarrierScope scope) {
        if (scope.roleSwitchWriteLockDepth < 1) {
            throw new IllegalMonitorStateException("role-switch write lock is not held by this carrier");
        }
        scope.roleSwitchWriteLockDepth--;
    }

    public static void restore(@Nullable Mode mode) {
        SCOPE.get().mode = mode;
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

    static boolean hasAnyRoleSwitchLock(CarrierScope scope) {
        return scope.roleSwitchReadLocks.hasAny()
                || scope.roleSwitchWriteLockDepth > 0
                || (scope.fiber != null && scope.fiber.getRoleSwitchReadLockState().hasAny());
    }

    private static RoleSwitchReadLockState getRoleSwitchReadLockState(CarrierScope scope) {
        final Fiber fiber = scope.fiber;
        return fiber != null ? fiber.getRoleSwitchReadLockState() : scope.roleSwitchReadLocks;
    }

    private SuspensionScope() {
    }

    public enum Mode {
        BLOCKING,
        FIBER
    }

    // Opaque outside this package: the fields stay package-private.
    public static final class CarrierScope {
        final CancellationBinding cancellationBindingScratch = new CancellationBinding();
        FiberCancellationSignal cancellationSignal;
        long cancellationSignalGeneration = CancellationBinding.NO_GENERATION;
        CancellationBinding.Source cancellationSource;
        Fiber fiber;
        Mode mode;
        final RoleSwitchReadLockState roleSwitchReadLocks = new RoleSwitchReadLockState();
        int roleSwitchWriteLockDepth;
        FiberCancellationSignal supplementalCancellationSignal;
        long supplementalCancellationSignalGeneration = CancellationBinding.NO_GENERATION;
        TimerShards timerShards;
    }

    static final class RoleSwitchReadLockState {
        private IntList extraDepths;
        private ObjList<Lock> extraLocks;
        private int holdCount;
        private @Nullable Mode previousMode;
        private int primaryDepth;
        private Lock primaryLock;

        void clear() {
            if (extraDepths != null) {
                extraDepths.clear();
                extraLocks.clear();
            }
            holdCount = 0;
            previousMode = null;
            primaryDepth = 0;
            primaryLock = null;
        }

        void enter(Lock lock) {
            if (holdCount == Integer.MAX_VALUE) {
                throw new IllegalStateException("role-switch read lock depth overflow");
            }
            if (primaryLock == lock) {
                primaryDepth++;
            } else {
                final int index = indexOf(lock);
                if (index > -1) {
                    extraDepths.setQuick(index, extraDepths.getQuick(index) + 1);
                } else if (primaryLock == null) {
                    primaryDepth = 1;
                    primaryLock = lock;
                } else {
                    addExtra(lock);
                }
            }
            holdCount++;
        }

        @Nullable
        Lock getAnyLock() {
            if (primaryLock != null) {
                return primaryLock;
            }
            return extraLocks != null && extraLocks.size() > 0 ? extraLocks.getLast() : null;
        }

        int getDepth(Lock lock) {
            if (primaryLock == lock) {
                return primaryDepth;
            }
            final int index = indexOf(lock);
            return index > -1 ? extraDepths.getQuick(index) : 0;
        }

        int getHoldCount() {
            return holdCount;
        }

        @Nullable
        Mode getPreviousMode() {
            return previousMode;
        }

        boolean hasAny() {
            return holdCount > 0;
        }

        boolean hasLock(Lock lock) {
            return getDepth(lock) > 0;
        }

        void leave(Lock lock) {
            if (primaryLock == lock) {
                if (--primaryDepth == 0) {
                    primaryLock = null;
                }
            } else {
                final int index = indexOf(lock);
                if (index < 0) {
                    throw new IllegalMonitorStateException("role-switch read lock is not held by this execution");
                }
                final int depth = extraDepths.getQuick(index) - 1;
                if (depth == 0) {
                    extraDepths.removeIndex(index);
                    extraLocks.remove(index);
                } else {
                    extraDepths.setQuick(index, depth);
                }
            }
            holdCount--;
        }

        void setPreviousMode(@Nullable Mode previousMode) {
            this.previousMode = previousMode;
        }

        @Nullable
        Mode takePreviousMode() {
            final Mode mode = previousMode;
            previousMode = null;
            return mode;
        }

        private void addExtra(Lock lock) {
            if (extraLocks == null) {
                final ObjList<Lock> locks = new ObjList<>();
                final IntList depths = new IntList();
                locks.add(lock);
                depths.add(1);
                extraDepths = depths;
                extraLocks = locks;
                return;
            }
            extraLocks.add(lock);
            try {
                extraDepths.add(1);
            } catch (Throwable th) {
                extraLocks.remove(extraLocks.size() - 1);
                throw th;
            }
        }

        private int indexOf(Lock lock) {
            if (extraLocks != null) {
                for (int i = 0, n = extraLocks.size(); i < n; i++) {
                    if (extraLocks.getQuick(i) == lock) {
                        return i;
                    }
                }
            }
            return -1;
        }
    }
}
