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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.std.Decimal256;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

/**
 * A decimal value wider than a column's storage must never be squeezed into it,
 * neither by the writer nor by the parquet row group pushdown filter. The literals
 * below have an unscaled value of 2^128 - 2^64, whose 64-bit limbs are individually
 * all-zeros or all-ones: narrowing it to 128 bits yields -2^64 and to 64 bits yields 0,
 * either of which prunes every row group of a table holding small positive values.
 */
public class DecimalStorageWidthTest extends AbstractCairoTest {

    @Test
    public void testPushdownKeepsWiderLiteralOnDecimal128Column() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(38,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= '3402823669209384634449278633580586598.40'::DECIMAL(39,2) ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1000000.10
                            5000000.50
                            9999999.98
                            9999999.99
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testPushdownKeepsWiderLiteralOnDecimal64Column() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= '3402823669209384634449278633580586598.40'::DECIMAL(39,2) ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1000000.10
                            5000000.50
                            9999999.98
                            9999999.99
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testWriterRejectsValueWiderThanColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE w (val DECIMAL(18,0), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (TableWriter writer = getWriter("w")) {
                // -2^63 is a legal 19-digit decimal, but stored as a DECIMAL64 it is the NULL sentinel
                TableWriter.Row row = writer.newRow(0L);
                try {
                    row.putDecimal(0, Decimal256.fromBigDecimal(new BigDecimal("-9223372036854775808")));
                    Assert.fail("expected the value to be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "value does not fit in column type");
                }
                row.cancel();

                // the most negative value the column can hold still round-trips
                row = writer.newRow(0L);
                row.putDecimal(0, Decimal256.fromBigDecimal(new BigDecimal("-999999999999999999")));
                row.append();
                writer.commit();
            }
            assertQuery("SELECT val FROM w")
                    .noLeakCheck()
                    .expectSize()
                    .returns("val\n-999999999999999999\n");
        });
    }
}
