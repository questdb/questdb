/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.view;

import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.tools.LogCapture;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class ViewCompilerJobTest extends AbstractViewTest {
    private final String[] breakingSqls = new String[]{
            "RENAME TABLE " + TABLE1 + " TO " + TABLE3,
            "ALTER TABLE " + TABLE1 + " DROP COLUMN v",
            "ALTER TABLE " + TABLE1 + " RENAME COLUMN v to v_renamed",
            "RENAME TABLE " + TABLE2 + " TO " + TABLE4,
            "ALTER TABLE " + TABLE2 + " DROP COLUMN v",
            "ALTER TABLE " + TABLE2 + " RENAME COLUMN v to v_renamed"
    };
    private final String[] fixingSqls = new String[]{
            "RENAME TABLE " + TABLE3 + " TO " + TABLE1,
            "ALTER TABLE " + TABLE1 + " ADD COLUMN v LONG",
            "ALTER TABLE " + TABLE1 + " RENAME COLUMN v_renamed to v",
            "RENAME TABLE " + TABLE4 + " TO " + TABLE2,
            "ALTER TABLE " + TABLE2 + " ADD COLUMN v LONG",
            "ALTER TABLE " + TABLE2 + " RENAME COLUMN v_renamed to v"
    };
    private final Rnd rnd = new Rnd();
    private final String[] viewQueries = new String[]{
            "select ts, k, max(v) as value from " + TABLE1 + " where v > 4",
            "select ts, min(v) as value from " + TABLE2 + " where v > 6",
            "select ts, max(5 * v) as v_max from " + TABLE1 + " where v > 4",
            "select ts, k, sqrt(v) as v from " + TABLE2 + " where v > 3",
            "select value from " + VIEW1,
            "select t1.ts, t2.v from " + TABLE1 + " t1 join " + TABLE2 + " t2 on k"
    };

    @Test
    public void testRepeatedFailuresLogTheInvalidationAtErrorOnlyOnTheFlip() throws Exception {
        // The ERROR promotion exists so an operator watching for ERROR sees a view go invalid. It must
        // fire on the FLIP only. SqlCompilerImpl.compileExecutionModel enqueues a recompile from its
        // catch-all, so every failed query that merely names a broken view re-asserts the invalid state
        // through this same method -- an unguarded promotion turns one broken view on a 1 Hz dashboard
        // into ~86k ERROR lines a day. Counting matters here: asserting "an ERROR was logged" passes
        // either way.
        final int failingQueries = 20;
        final String viewQuery = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
        final String expectedErrorMessage = "table does not exist [table=" + TABLE1 + "]";
        // "updating view state [view=view1, invalid=true" preceded by the level header, which is " E "
        // or " ERROR " depending on cairo.log.level.verbose.
        final String invalidRecordRE = "updating view state \\[view=" + VIEW1 + ", invalid=true";
        final String errorInvalidRecordRE = " (E|ERROR) \\S+ " + invalidRecordRE;

        assertMemoryLeak(() -> {
            setCurrentMicros(1750345200000000L);
            createTable(TABLE1);
            createView(VIEW1, viewQuery, TABLE1);
            compileView(VIEW1);

            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                // The flip.
                execute("RENAME TABLE " + TABLE1 + " TO " + TABLE3);
                drainWalQueue();
                drainViewQueue();
                assertViewState(VIEW1, expectedErrorMessage);
                capture.drain();
                assertEquals(
                        "the flip into invalid must be logged at ERROR",
                        1,
                        capture.countLoggedRE(errorInvalidRecordRE)
                );

                // Re-assertions. An operator's dashboard keeps querying the broken view.
                for (int i = 0; i < failingQueries; i++) {
                    assertExceptionNoLeakCheck("select * from " + VIEW1, -1, expectedErrorMessage);
                    drainViewQueue();
                }
                capture.drain();

                // Every failed query still re-asserts the invalid state -- the guard demotes those
                // records, it does not suppress them ...
                assertEquals(
                        "each failed query must still re-assert the invalid state",
                        failingQueries + 1,
                        capture.countLoggedRE(invalidRecordRE)
                );
                // ... and exactly one of them, the flip, is an ERROR.
                assertEquals(
                        "only the flip may be logged at ERROR",
                        1,
                        capture.countLoggedRE(errorInvalidRecordRE)
                );
            } finally {
                capture.stop();
            }
        });
    }

    @Test
    public void testConcurrentEventProcessing() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            for (int i = 0; i < viewQueries.length; i++) {
                createView("view" + i, viewQueries[i]);
                compileView("view" + i);
            }

            final ObjList<Thread> threads = new ObjList<>();
            final ObjList<ViewCompilerJob> compilerJobs = new ObjList<>();
            final ConcurrentHashMap<Throwable> errors = new ConcurrentHashMap<>();
            final AtomicBoolean stop = new AtomicBoolean(false);

            final int numOfCompileJobs = 4;
            for (int i = 0; i < numOfCompileJobs; i++) {
                final ViewCompilerJob compilerJob = new ViewCompilerJob(i, engine);
                compilerJobs.add(compilerJob);
                final int finalI = i;
                final Thread th = new Thread(() -> {
                    try {
                        while (!stop.get()) {
                            compilerJob.run();
                            Os.sleep(1);
                        }
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errors.put("compileThread " + finalI, e);
                    }
                });
                th.setName("compileThread " + finalI);
                threads.add(th);
                th.start();
            }

            final int numOfTableChangeThreads = 2;
            final int numOfSqls = breakingSqls.length / 2;
            for (int i = 0; i < numOfTableChangeThreads; i++) {
                final int finalI = i;
                final Thread th = new Thread(() -> {
                    while (!stop.get()) {
                        try {
                            final int index = finalI * numOfSqls + rnd.nextInt(numOfSqls);
                            execute(breakingSqls[index]);
                            execute(fixingSqls[index]);
                        } catch (Throwable e) {
                            e.printStackTrace(System.out);
                            errors.put("tableChangeThread " + finalI, e);
                        }
                        Os.sleep(1);
                    }
                });
                th.setName("tableChangeThread " + finalI);
                threads.add(th);
                th.start();
            }

            Os.sleep(1000);
            stop.set(true);

            for (int i = 0; i < threads.size(); i++) {
                threads.getQuick(i).join();
            }

            assertEquals(0, errors.size());

            drainWalQueue(engine);
            for (int i = 0; i < compilerJobs.size(); i++) {
                compilerJobs.getQuick(i).run();
            }
            drainWalQueue(engine);

            for (int i = 0; i < viewQueries.length; i++) {
                compileView("view" + i);
            }
        });
    }
}
