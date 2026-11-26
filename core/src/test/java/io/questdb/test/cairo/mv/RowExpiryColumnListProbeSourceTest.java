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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * CREATE MATERIALIZED VIEW with a column list probes its EXPIRE ROWS policy over a source of the listed columns, see
 * {@link RowExpiryUtil#buildColumnListProbeSource}: a NULL of each listed type next to the query's designated
 * timestamp. The source must have exactly the listed types, for every type a list can declare, or a policy CREATE
 * probes would not see the view's columns. A persisted type added later without a SQL spelling fails here.
 */
public class RowExpiryColumnListProbeSourceTest extends AbstractCairoTest {

    @Test
    public void testSourceDesignatesTimestampTheQueryLacks() throws Exception {
        // The query of a GROUP BY timestamp_floor() view without ORDER BY has no designated timestamp, while the list
        // has one: the source designates the query's column at the list's position, so that a policy probe reads the
        // view's designated timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsbase (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String query = "SELECT timestamp_floor('1h', ts) AS ts, last(v) AS v FROM tsbase";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
            }
            for (int timestampIndex = 0; timestampIndex < 2; timestampIndex++) {
                final GenericRecordMetadata list = new GenericRecordMetadata();
                for (int column = 0; column < 2; column++) {
                    list.add(column == timestampIndex ? newColumn("TS", ColumnType.TIMESTAMP) : newColumn("v", ColumnType.DOUBLE));
                }
                list.setTimestampIndex(timestampIndex);
                try (RecordCursorFactory factory = select("SELECT * FROM " + RowExpiryUtil.buildColumnListProbeSource(list, "ts", query))) {
                    Assert.assertEquals(timestampIndex, factory.getMetadata().getTimestampIndex());
                    Assert.assertEquals(ColumnType.TIMESTAMP, factory.getMetadata().getColumnType(timestampIndex));
                }
            }
        });
    }

    @Test
    public void testSourceHasTheListedColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsbase (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final IntList types = everyListedType();
            // every persisted tag, both timestamp precisions, every geohash width, every decimal precision and scale
            // and every array dimensionality
            Assert.assertEquals(3_113, types.size());
            final int chunkSize = 300;
            for (int lo = 0, n = types.size(); lo < n; lo += chunkSize) {
                final int hi = Math.min(lo + chunkSize, n);
                // the timestamp first, in the middle and last, in another letter case than the query's
                for (int timestampIndex : new int[]{0, (hi - lo) / 2, hi - lo}) {
                    final GenericRecordMetadata list = new GenericRecordMetadata();
                    for (int i = lo, column = 0; column <= hi - lo; column++) {
                        if (column == timestampIndex) {
                            list.add(newColumn("TS", ColumnType.TIMESTAMP));
                        } else {
                            list.add(newColumn("c" + i, types.getQuick(i)));
                            i++;
                        }
                    }
                    list.setTimestampIndex(timestampIndex);
                    final String source = RowExpiryUtil.buildColumnListProbeSource(list, "ts", "SELECT ts, v FROM tsbase");
                    try (RecordCursorFactory factory = select("SELECT * FROM " + source)) {
                        final RecordMetadata metadata = factory.getMetadata();
                        Assert.assertEquals(list.getColumnCount(), metadata.getColumnCount());
                        for (int i = 0, m = list.getColumnCount(); i < m; i++) {
                            Assert.assertEquals(list.getColumnName(i), ColumnType.nameOf(list.getColumnType(i)), ColumnType.nameOf(metadata.getColumnType(i)));
                            Assert.assertTrue(
                                    list.getColumnName(i) + " vs " + metadata.getColumnName(i),
                                    Chars.equalsIgnoreCase(list.getColumnName(i), metadata.getColumnName(i))
                            );
                        }
                        Assert.assertEquals(timestampIndex, metadata.getTimestampIndex());
                    }
                }
            }
        });
    }

    @Test
    public void testSourceText() {
        final GenericRecordMetadata list = new GenericRecordMetadata();
        list.add(newColumn("k \"q\"", ColumnType.SYMBOL));
        list.add(newColumn("TS", ColumnType.TIMESTAMP));
        list.add(newColumn("x", ColumnType.LONG128));
        list.add(newColumn("d", ColumnType.getDecimalType(18, 3)));
        list.setTimestampIndex(1);
        Assert.assertEquals(
                "(SELECT CAST(NULL AS SYMBOL) AS \"k \"\"q\"\"\", \"ts\", to_long128(NULL, NULL) AS \"x\", CAST(NULL AS DECIMAL(18,3)) AS \"d\""
                        + " FROM ((SELECT ts FROM t) TIMESTAMP(\"ts\")))",
                RowExpiryUtil.buildColumnListProbeSource(list, "ts", "SELECT ts FROM t")
        );
    }

    private static IntList everyListedType() {
        final IntList types = new IntList();
        for (int tag = 1; tag < ColumnType.NULL; tag++) {
            if (!ColumnType.isPersisted(tag)) {
                continue;
            }
            switch (tag) {
                case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG, ColumnType.GEOHASH,
                     ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64,
                     ColumnType.DECIMAL128, ColumnType.DECIMAL256, ColumnType.DECIMAL, ColumnType.ARRAY -> {
                    // added below, per width
                }
                case ColumnType.TIMESTAMP -> {
                    types.add(ColumnType.TIMESTAMP_MICRO);
                    types.add(ColumnType.TIMESTAMP_NANO);
                }
                default -> types.add(tag);
            }
        }
        for (int bits = 1; bits <= ColumnType.GEOLONG_MAX_BITS; bits++) {
            types.add(ColumnType.getGeoHashTypeWithBits(bits));
        }
        for (int precision = 1; precision <= 76; precision++) {
            for (int scale = 0; scale <= precision; scale++) {
                types.add(ColumnType.getDecimalType(precision, scale));
            }
        }
        for (int dims = 1; dims <= ColumnType.ARRAY_NDIMS_LIMIT; dims++) {
            types.add(ColumnType.encodeArrayType(ColumnType.DOUBLE, dims));
        }
        return types;
    }

    private static TableColumnMetadata newColumn(String name, int type) {
        return new TableColumnMetadata(name, type, IndexType.NONE, 0, true, null, -1, false, -1, true, 128);
    }
}
