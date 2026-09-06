/*******************************************************************************
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

package io.questdb.test.std;

import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concurrent {@link Files#mkdirs} on paths that SHARE a parent must all succeed.
 * <p>
 * {@code mkdirs} walks the path component by component and skips any that already exists, so it reads
 * as idempotent -- but the check and the {@code mkdir} are not atomic. Two threads creating
 * {@code <day>/E0} and {@code <day>/E1} can both find {@code <day>} missing, and the loser used to get
 * the raw errno back. {@code TableUtils#createDirsOrFail} turns that into a critical CairoException,
 * which fails the WAL apply and SUSPENDS the table.
 * <p>
 * Composite partitioning is what made it reachable, and this test mirrors that shape rather than the
 * SQL: cells of one day share a parent directory, so two O3 cell tasks of the same day race on
 * creating the day container. A plain table's partition directories are leaves under the table root
 * and share no parent. It surfaced on a CI agent as
 * {@code [17] could not create directories [file=.../2023-01-02/exch=E1/]}.
 * <p>
 * <b>It is a race, but not a narrow one.</b> Measured on this machine with the fix reverted:
 * <b>1410 of 3200</b> calls failed, every one with errno 17. Roughly two in five losers under
 * contention, which is why the barrier and the fresh per-round parent are enough -- no sleep, no
 * retry loop. So this does fail reliably without the fix rather than merely often, though what makes
 * the fix sound is not the sample size: {@code mkdirs} now reports failure only when the directory is
 * genuinely absent afterwards, which is the property the caller needs and does not depend on timing.
 */
public class FilesMkdirsRaceTest extends AbstractTest {

    private static final int ROUNDS = 400;
    private static final int THREADS = 8;

    @Test
    public void testConcurrentMkdirsOnASharedParentAllSucceed() throws Exception {
        final String root = temp.newFolder("mkdirs-race").getAbsolutePath();
        final AtomicInteger failures = new AtomicInteger();
        final StringBuilder firstFailure = new StringBuilder();

        for (int round = 0; round < ROUNDS; round++) {
            // A FRESH shared parent each round: once it exists, every thread takes the skip branch
            // and the window is gone, so reusing one would test nothing after the first round.
            final String day = root + Files.SEPARATOR + "2023-01-" + round;
            final CyclicBarrier barrier = new CyclicBarrier(THREADS);
            final Thread[] threads = new Thread[THREADS];
            for (int t = 0; t < THREADS; t++) {
                final int id = t;
                threads[t] = new Thread(() -> {
                    try (Path path = new Path()) {
                        path.of(day).concat("cell=" + id);
                        barrier.await();
                        final int r = Files.mkdirs(path.slash(), 509);
                        if (r != 0) {
                            synchronized (firstFailure) {
                                if (failures.getAndIncrement() == 0) {
                                    firstFailure.append("mkdirs returned ").append(r)
                                            .append(" for ").append(path)
                                            .append(" (errno=").append(Os.errno()).append(')');
                                }
                            }
                        }
                    } catch (Throwable e) {
                        synchronized (firstFailure) {
                            if (failures.getAndIncrement() == 0) {
                                firstFailure.append(e);
                            }
                        }
                    }
                });
                threads[t].start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
        }

        Assert.assertEquals(
                "concurrent mkdirs on a shared parent failed " + failures.get() + " time(s); losing the"
                        + " race to create a parent component must be success, not an errno the caller"
                        + " turns into a suspended table. First: " + firstFailure,
                0, failures.get()
        );
    }
}
