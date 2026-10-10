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
import io.questdb.cairo.arr.ArrayTypeDriver;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Entry point for {@code TypeDriverTest.testConcurrentClassInit}: in a fresh JVM, one thread per
 * argument waits on a barrier, then initialises the type classes starting from its own
 * class, so first use starts at every end at once. A thread still alive after the join
 * timeout means the class initialisers deadlock. Exit code 0 means every thread finished and
 * the classes agree; any other output or exit code is the failure message.
 */
public final class TypeDriverConcurrentInitMain {
    private static final long JOIN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(20);

    private TypeDriverConcurrentInitMain() {
    }

    public static void main(String[] args) throws Exception {
        final CyclicBarrier barrier = new CyclicBarrier(args.length);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread[] threads = new Thread[args.length];
        for (int i = 0; i < args.length; i++) {
            final String start = args[i];
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
        System.out.println("OK " + String.join(",", args));
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            System.out.println("FAILED: " + what);
            System.exit(1);
        }
    }

    private static void touch(String start) throws ClassNotFoundException {
        switch (start) {
            // ColumnType first, through the lazy width and name paths into the type drivers
            case "type" -> {
                ColumnType.sizeOf(ColumnType.INT);
                ColumnType.isVarSize(ColumnType.VARCHAR);
                ColumnType.nameOf(ColumnType.GEOLONG);
            }
            case "tag" -> ColumnTypeTag.of(ColumnType.SYMBOL);
            case "drivers" ->
                    Class.forName("io.questdb.cairo.TypeDrivers", true, TypeDriverConcurrentInitMain.class.getClassLoader());
            case "leaf" -> IntTypeDriver.INSTANCE.getMovement();
            // the leaf whose static initialiser calls back into ColumnType
            case "array" -> ArrayTypeDriver.INSTANCE.getTag();
            default -> throw new IllegalArgumentException(start);
        }
    }
}
