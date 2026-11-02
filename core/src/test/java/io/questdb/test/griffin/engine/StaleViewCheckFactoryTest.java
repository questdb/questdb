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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.engine.StaleViewCheckFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The compiler wraps the plan of a query that reads a view in {@link StaleViewCheckFactory}.
 * Its {@code getBaseFactory()} returns the base of its base, so the default implementations of
 * the factory properties that ask {@code getBaseFactory()} would skip the wrapper's own base.
 * These tests check that the wrapper reports the properties of the plan it wraps.
 */
public class StaleViewCheckFactoryTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        inputRoot = root;
    }

    @Test
    public void testReportsExternalDataSourceOfBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE xs AS (SELECT x::INT i, (x * 1_000_000)::TIMESTAMP ts FROM long_sequence(3))");
            encodeTable("xs", "xs.parquet");
            execute("CREATE VIEW v_ext AS (SELECT * FROM read_parquet('xs.parquet'))");
            execute("CREATE VIEW v_tab AS (SELECT * FROM xs)");
            drainWalAndViewQueues();

            try (RecordCursorFactory factory = select("SELECT * FROM v_ext")) {
                assertWrapsViewPlan(factory);
                Assert.assertTrue(factory.usesExternalDataSource());
            }
            try (RecordCursorFactory factory = select("SELECT * FROM v_tab")) {
                assertWrapsViewPlan(factory);
                Assert.assertFalse(factory.usesExternalDataSource());
            }
        });
    }

    @Test
    public void testReportsParquetConvertedColumnsOfBase() throws Exception {
        // Regression test for #7807. A Parquet export picked the zero-copy DIRECT_PAGE_FRAME mode
        // for a view over a Parquet partition with a column whose type had changed since, and
        // handed the encoder the column in its old layout.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO pt
                    SELECT (x * 3)::INT, timestamp_sequence('2024-01-01T00:00:00.000000Z', 28_800_000_000)
                    FROM long_sequence(6)""");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE pt ALTER COLUMN c TYPE VARCHAR");
            drainWalQueue();
            // the same table without the type change
            execute("CREATE TABLE pn (c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO pn
                    SELECT (x * 3)::INT, timestamp_sequence('2024-01-01T00:00:00.000000Z', 28_800_000_000)
                    FROM long_sequence(6)""");
            drainWalQueue();
            execute("ALTER TABLE pn CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("CREATE VIEW v_converted AS (SELECT * FROM pt)");
            execute("CREATE VIEW v_plain AS (SELECT * FROM pn)");
            drainWalAndViewQueues();

            try (RecordCursorFactory factory = select("SELECT * FROM v_converted")) {
                assertWrapsViewPlan(factory);
                Assert.assertTrue(factory.hasParquetConvertedColumns(sqlExecutionContext));
                Assert.assertEquals(
                        ParquetExportMode.TEMP_TABLE,
                        ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext)
                );
            }
            try (RecordCursorFactory factory = select("SELECT * FROM v_plain")) {
                assertWrapsViewPlan(factory);
                Assert.assertFalse(factory.hasParquetConvertedColumns(sqlExecutionContext));
                Assert.assertEquals(
                        ParquetExportMode.DIRECT_PAGE_FRAME,
                        ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext)
                );
            }
        });
    }

    private static void assertWrapsViewPlan(RecordCursorFactory factory) {
        // select() wraps the plan in QueryProgress, which wraps the view check
        Assert.assertTrue(ParquetExportMode.unwrapFactory(factory) instanceof StaleViewCheckFactory);
    }

    private static void encodeTable(CharSequence tableName, CharSequence fileName) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root).concat(fileName);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }
}
