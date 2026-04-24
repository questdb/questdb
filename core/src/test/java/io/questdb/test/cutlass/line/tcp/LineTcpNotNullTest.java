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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies how the ILP TCP receiver interacts with NOT NULL column constraints.
 * <p>
 * The TCP/WAL ingest path goes through {@code LineWalAppender}, which calls
 * {@code TableWriter.Row.append()}. Both {@code TableWriter} and {@code WalWriter}
 * enforce NOT NULL inside {@code rowAppend()} by throwing
 * {@code CairoException} with the message "NOT NULL constraint violation".
 * The receiver catches the exception and either disconnects (when
 * {@code disconnectOnError} is set) or logs and drops the offending row.
 * <p>
 * These tests pin down the visible end-state for each scenario instead of
 * asserting against the receiver log; the row-rejection contract is "the
 * row must not appear in the table".
 */
public class LineTcpNotNullTest extends AbstractLineTcpReceiverTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
    }

    @Test
    public void testIlpAutoCreateRespectsNotNullOnExistingTable() throws Exception {
        // Pre-create a table with a NOT NULL column. ILP auto-create should add
        // the new column "extra" without changing the NOT NULL flag of the
        // existing "x" column. The newly auto-created column itself must be
        // nullable (ILP cannot express NOT NULL).
        runInContext((receiver) -> {
            execute("""
                    CREATE TABLE ilp_autocreate (
                        ts TIMESTAMP NOT NULL,
                        x DOUBLE NOT NULL,
                        y DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            String lineData = "ilp_autocreate x=1.5,y=2.0,extra=3.0 631150000000000000\n";
            send("ilp_autocreate", WAIT_ENGINE_TABLE_RELEASE, () -> sendToSocket(lineData));
            drainWalQueue();

            try (TableReader reader = engine.getReader("ilp_autocreate")) {
                TableReaderMetadata md = reader.getMetadata();
                assertTrue("ts must remain NOT NULL", md.isNotNull(md.getColumnIndex("ts")));
                assertTrue("x must remain NOT NULL", md.isNotNull(md.getColumnIndex("x")));
                assertFalse("y must remain nullable", md.isNotNull(md.getColumnIndex("y")));
                int extraIdx = md.getColumnIndex("extra");
                assertTrue("ILP auto-create must have added the 'extra' column", extraIdx >= 0);
                assertFalse("auto-created columns must not carry NOT NULL", md.isNotNull(extraIdx));
            }

            assertSql(
                    """
                            x\ty\textra\tts
                            1.5\t2.0\t3.0\t1989-12-31T23:26:40.000000Z
                            """,
                    "SELECT x, y, extra, ts FROM ilp_autocreate"
            );
        });
    }

    @Test
    public void testIlpOmittedNotNullColumnIsRejected() throws Exception {
        // Pre-create a table with a NOT NULL column. ILP cannot express the
        // NOT NULL column "x" because the line below omits it; the WalWriter
        // must reject the row and the table must remain empty.
        // FIXME: if this test starts failing, ILP is silently bypassing
        // NOT NULL enforcement -- the constraint check in WalWriter.rowAppend()
        // is no longer reached on this path.
        runInContext((receiver) -> {
            execute("""
                    CREATE TABLE ilp_not_null (
                        ts TIMESTAMP NOT NULL,
                        x DOUBLE NOT NULL,
                        y DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            // Send a line that supplies only "y" -- the NOT NULL "x" is missing.
            String lineData = "ilp_not_null y=2.0 631150000000000000\n";
            send("ilp_not_null", WAIT_ENGINE_TABLE_RELEASE, () -> sendToSocket(lineData));
            drainWalQueue();

            // Expect zero rows: the WAL apply must have rejected the violating row.
            assertSql(
                    """
                            count
                            0
                            """,
                    "SELECT count() FROM ilp_not_null"
            );
        });
    }
}
