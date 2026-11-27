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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * SQL reads every FLOAT or DOUBLE NaN as NULL, so a dedup key of these types takes every NaN as one key, whatever
 * its bits. The NaN with the sign bit set, which the negation of a NULL gives, is the case here.
 */
public class DedupNanKeyTest extends AbstractCairoTest {
    private static final long NEG_NAN_DOUBLE_BITS = 0xfff8_0000_0000_0000L;
    private static final int NEG_NAN_FLOAT_BITS = 0xffc0_0000;

    @Test
    public void testNanKeysInOneCommitDouble() throws Exception {
        assertNanKeysInOneCommit("DOUBLE");
    }

    @Test
    public void testNanKeysInOneCommitFloat() throws Exception {
        assertNanKeysInOneCommit("FLOAT");
    }

    @Test
    public void testNanKeysNativeDouble() throws Exception {
        assertNanKeysAcrossCommits("DOUBLE", false);
    }

    @Test
    public void testNanKeysNativeFloat() throws Exception {
        assertNanKeysAcrossCommits("FLOAT", false);
    }

    @Test
    public void testNanKeysParquetDouble() throws Exception {
        assertNanKeysAcrossCommits("DOUBLE", true);
    }

    @Test
    public void testNanKeysParquetFloat() throws Exception {
        assertNanKeysAcrossCommits("FLOAT", true);
    }

    // Appends a row with a NULL key: the NaN of the type, or that NaN with the sign bit set.
    private static void appendRow(WalWriter w, String timestamp, boolean isSignedNan, long v) throws Exception {
        TableWriter.Row row = w.newRow(MicrosTimestampDriver.floor(timestamp));
        if (w.getMetadata().getColumnType(1) == ColumnType.FLOAT) {
            row.putFloat(1, isSignedNan ? Float.intBitsToFloat(NEG_NAN_FLOAT_BITS) : Float.NaN);
        } else {
            row.putDouble(1, isSignedNan ? Double.longBitsToDouble(NEG_NAN_DOUBLE_BITS) : Double.NaN);
        }
        row.putLong(2, v);
        row.append();
    }

    private static void createTable(String keyType) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, d " + keyType + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, d)");
    }

    // Each commit writes a NULL key of another NaN at one timestamp: UPSERT keeps one row, the last one.
    private void assertNanKeysAcrossCommits(String keyType, boolean isParquet) throws Exception {
        assertMemoryLeak(() -> {
            createTable(keyType);
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName("t"))) {
                appendRow(w, "2024-01-01", false, 1);
                w.commit();
                appendRow(w, "2024-01-01", true, 2);
                appendRow(w, "2024-01-02", false, 100);
                w.commit();
            }
            drainWalQueue();
            assertRows("""
                    ts\td\tv
                    2024-01-01T00:00:00.000000Z\tnull\t2
                    2024-01-02T00:00:00.000000Z\tnull\t100
                    """);

            if (isParquet) {
                execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
            }
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName("t"))) {
                appendRow(w, "2024-01-01", false, 3);
                w.commit();
                appendRow(w, "2024-01-01", true, 4);
                w.commit();
            }
            drainWalQueue();
            assertRows("""
                    ts\td\tv
                    2024-01-01T00:00:00.000000Z\tnull\t4
                    2024-01-02T00:00:00.000000Z\tnull\t100
                    """);
        });
    }

    private void assertNanKeysInOneCommit(String keyType) throws Exception {
        assertMemoryLeak(() -> {
            createTable(keyType);
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName("t"))) {
                appendRow(w, "2024-01-01", true, 1);
                appendRow(w, "2024-01-01", false, 2);
                appendRow(w, "2024-01-01", true, 3);
                w.commit();
            }
            drainWalQueue();
            assertRows("""
                    ts\td\tv
                    2024-01-01T00:00:00.000000Z\tnull\t3
                    """);
        });
    }

    private void assertRows(String expected) throws Exception {
        assertQuery("SELECT ts, d, v FROM t ORDER BY ts, v").timestamp("ts").expectSize().returns(expected);
    }
}
