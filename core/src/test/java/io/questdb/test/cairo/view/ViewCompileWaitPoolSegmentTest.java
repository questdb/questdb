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

package io.questdb.test.cairo.view;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A DDL of a non-WAL table that fails to lock the table's readers tells the view compile runs that
 * hold the table from its other holders, in every segment of the reader pool.
 */
public class ViewCompileWaitPoolSegmentTest extends AbstractViewTest {
    private static final LogCapture capture = new LogCapture();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // two readers per segment, so that the third borrower of a table lands in the second segment
        setProperty(PropertyKey.DEBUG_CAIRO_POOL_SEGMENT_SIZE, 2);
        AbstractViewTest.setUpStatic();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testTruncateFailsAtOnceWhenQueryHoldsSecondPoolSegment() throws Exception {
        // A view compile run holds t's reader in the first slot of the pool, and a query holds t's
        // reader in the second segment of the pool. The lock attempt of TRUNCATE fails on the run.
        // TRUNCATE finds the query among the holders of t, and fails at once, as it does without
        // views, instead of waiting for the run.
        assertMemoryLeak(() -> {
            // the server default; a TRUNCATE that waited would log its wait before it fails
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 500);
            createTableWithRows("t");
            createView("v", "SELECT * FROM t");
            // queues the compile of the views over t
            execute("ALTER TABLE t ADD COLUMN b INT");
            final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
            final long testThread = Thread.currentThread().threadId();
            // the slot of each borrow of t's reader that neither the run nor this thread makes
            final StringBuffer otherBorrows = new StringBuffer();
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                run.onEvent(factoryType, thread, tableToken, event, segment, position);
                if (thread != testThread
                        && thread != run.getCompileThreadId()
                        && factoryType == PoolListener.SRC_READER
                        && (event == PoolListener.EV_GET || event == PoolListener.EV_CREATE)) {
                    otherBorrows.append(segment).append(':').append(position).append(' ');
                }
            });
            ReaderHoldingThread query = null;
            try {
                // the run borrows t's reader in the first slot
                run.start();
                try (TableReader ignore = engine.getReader("t")) {
                    // the second slot is busy, so the query borrows a reader in the second segment
                    query = ReaderHoldingThread.start(engine, "t");
                }
                Assert.assertEquals("1:0 ", otherBorrows.toString());
                assertTruncateFails();
            } finally {
                run.join();
                engine.setPoolListener(null);
                if (query != null) {
                    query.close();
                }
            }
            capture.drain();
            capture.assertNotLogged(ViewCompileParker.ddlWaitLine("t"));

            execute("TRUNCATE TABLE t");
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\n");
            drainViewQueue();
            assertViewState("v");
            assertViewColumns("v", "a:INT, b:INT");
        });
    }

    private static void assertTruncateFails() {
        try {
            execute("TRUNCATE TABLE t");
            Assert.fail("expected to fail: TRUNCATE TABLE t");
        } catch (SqlException e) {
            TestUtils.assertEquals("there is an active query against 't'. Try again.", e.getFlyweightMessage());
        }
    }
}
