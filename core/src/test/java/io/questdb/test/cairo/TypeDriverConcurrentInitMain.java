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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.IntTypeDriver;
import io.questdb.cairo.PhysicalDescriptor;
import io.questdb.cairo.WireKind;
import io.questdb.cairo.arr.ArrayTypeDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Entry point for {@code TypeDriverTest.testConcurrentClassInit}: in a fresh JVM, one thread per
 * start argument waits on a barrier, then initialises the type classes starting from its own
 * class, so first use starts at every end at once. A thread still alive after the join
 * timeout means the class initialisers deadlock. An argument {@code lookups=<hash>} names the
 * hash of {@link #lookups()} a normal start gives, which the classes must give once initialised.
 * Exit code 0 means every thread finished and the classes agree; any other output or exit code
 * is the failure message.
 */
public final class TypeDriverConcurrentInitMain {
    static final String LOOKUPS_ARG = "lookups=";
    private static final String DRIVER_START = "driver:";
    private static final long JOIN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(20);

    private TypeDriverConcurrentInitMain() {
    }

    public static void main(String[] args) throws Exception {
        final List<String> starts = new ArrayList<>();
        String expectedLookups = null;
        for (String arg : args) {
            if (arg.startsWith(LOOKUPS_ARG)) {
                expectedLookups = arg.substring(LOOKUPS_ARG.length());
            } else {
                starts.add(arg);
            }
        }
        final CyclicBarrier barrier = new CyclicBarrier(starts.size());
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread[] threads = new Thread[starts.size()];
        for (int i = 0; i < threads.length; i++) {
            final String start = starts.get(i);
            threads[i] = new Thread(() -> {
                try {
                    barrier.await(JOIN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                    touch(start);
                } catch (BrokenBarrierException | InterruptedException | TimeoutException e) {
                    failure.compareAndSet(null, e);
                } catch (Throwable th) {
                    failure.compareAndSet(null, th);
                }
            }, "init-" + start);
            threads[i].start();
        }
        final long deadline = System.currentTimeMillis() + JOIN_TIMEOUT_MILLIS;
        for (Thread thread : threads) {
            thread.join(Math.max(1, deadline - System.currentTimeMillis()));
        }
        for (Thread thread : threads) {
            if (thread.isAlive()) {
                System.out.println("FAILED: deadlock, thread " + thread.getName() + " still initialising");
                for (Thread t : threads) {
                    System.out.println(t.getName() + " " + t.getState());
                    for (StackTraceElement frame : t.getStackTrace()) {
                        System.out.println("    at " + frame);
                    }
                }
                System.exit(2);
            }
        }
        if (failure.get() != null) {
            System.out.println("FAILED: " + failure.get());
            failure.get().printStackTrace(System.out);
            System.exit(1);
        }
        check(ColumnTypeTag.of(ColumnType.INT) == ColumnTypeTag.INT, "tag lookup");
        check(ColumnType.getTypeDriver(ColumnType.INT) == IntTypeDriver.INSTANCE, "int driver");
        check(ColumnType.sizeOf(ColumnType.INT) == 4 && ColumnType.pow2SizeOf(ColumnType.LONG) == 3, "widths");
        check(ColumnType.isVarSize(ColumnType.VARCHAR) && !ColumnType.isVarSize(ColumnType.INT), "var-size tier");
        check(ColumnType.getDriver(ColumnType.ARRAY) == ArrayTypeDriver.INSTANCE, "array driver");
        check("INT".equals(ColumnType.nameOf(ColumnType.INT)) && "TIMESTAMP_NS".equals(ColumnType.nameOf(ColumnType.TIMESTAMP_NANO)), "names");
        checkLookups(expectedLookups);
        System.out.println("OK " + String.join(",", args));
    }

    /**
     * The start arguments that begin with the first use of one type driver, one per tag that has
     * a driver.
     */
    static List<String> driverStarts() {
        final List<String> starts = new ArrayList<>();
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            if (tag.code() >= 0 && ColumnType.findTypeDriver(tag.code()) != null) {
                starts.add(DRIVER_START + tag.name());
            }
        }
        return starts;
    }

    /**
     * Fails the run when {@link #lookups()} hashes to something other than {@code expected}, the
     * hash a normal start gives; does nothing when no hash was given.
     */
    static void checkLookups(String expected) {
        if (expected != null) {
            final String lookups = lookups();
            if (!expected.equals(hashOf(lookups))) {
                System.out.println("FAILED: the tables filled on first use differ from a normal start:");
                System.out.println(lookups);
                System.exit(1);
            }
        }
    }

    static String hashOf(String lookups) {
        return Integer.toHexString(lookups.hashCode());
    }

    /**
     * Every answer of the tables the type classes fill on first use, one line per tag: the
     * accessor and family-arm opcodes ({@code PhysicalDescriptor.Opcodes}), the wire kind
     * ({@code WireKind.Kinds}), and against every tag the narrowing, widening and built-in
     * widening casts and the overload distance ({@code ColumnType.Relations}). The answers must
     * not depend on which class a JVM initialised first.
     */
    static String lookups() {
        final StringBuilder sb = new StringBuilder();
        for (short from = 0; from <= ColumnType.MAX_TAG; from++) {
            sb.append(from)
                    .append(' ').append(PhysicalDescriptor.accessorOpcodeOf(from))
                    .append(' ').append(PhysicalDescriptor.familyArmOpcodeOf(from))
                    .append(' ').append(WireKind.of(from))
                    .append(':');
            for (short to = 0; to <= ColumnType.MAX_TAG; to++) {
                sb.append(' ')
                        .append(ColumnType.isNarrowingCastForTesting(from, to) ? 'N' : '.')
                        .append(ColumnType.isWideningCastForTesting(from, to) ? 'W' : '.')
                        .append(ColumnType.isBuiltInWideningCast(from, to) ? 'B' : '.');
                // no overload takes UNDEFINED
                if (to > ColumnType.UNDEFINED) {
                    sb.append(ColumnType.overloadDistance(from, to));
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            System.out.println("FAILED: " + what);
            System.exit(1);
        }
    }

    static void touch(String start) {
        if (start.startsWith(DRIVER_START)) {
            ColumnType.getTypeDriver(ColumnTypeTag.valueOf(start.substring(DRIVER_START.length())).code());
            return;
        }
        switch (start) {
            // ColumnType first, through the lazy width and name paths into the type drivers
            case "type" -> {
                ColumnType.sizeOf(ColumnType.INT);
                ColumnType.isVarSize(ColumnType.VARCHAR);
                ColumnType.nameOf(ColumnType.GEOLONG);
            }
            case "tag" -> ColumnTypeTag.of(ColumnType.SYMBOL);
            // every type driver, through the lookup by tag
            case "drivers" -> {
                for (ColumnTypeTag tag : ColumnTypeTag.values()) {
                    if (tag.code() >= 0) {
                        ColumnType.findTypeDriver(tag.code());
                    }
                }
            }
            case "leaf" -> IntTypeDriver.INSTANCE.getMovement();
            // the leaf whose static initialiser calls back into ColumnType
            case "array" -> ArrayTypeDriver.INSTANCE.getTag();
            // the tables filled on first use, each from every type driver
            case "relations" -> ColumnType.overloadDistance(ColumnType.INT, ColumnType.LONG);
            case "opcodes" -> PhysicalDescriptor.accessorOpcodeOf(ColumnType.INT);
            case "kinds" -> WireKind.of(ColumnType.INT);
            default -> throw new IllegalArgumentException(start);
        }
    }
}
