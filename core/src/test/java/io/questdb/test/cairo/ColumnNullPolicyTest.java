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
import io.questdb.cairo.NullPolicy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ColumnNullPolicyTest extends AbstractCairoTest {

    @Test
    public void testWriterReaderAndQueryMetadataAnswerTheDefinitionPolicy() throws Exception {
        // getColumnNullPolicy is a RecordMetadata default, so writer, reader and query metadata
        // answer alike: the column type driver's policy, NONE exactly for BOOLEAN, BYTE, SHORT and
        // CHAR; a deleted column, where the metadata keeps one, has no policy and is skipped
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        a_boolean BOOLEAN, a_byte BYTE, a_short SHORT, a_char CHAR, a_int INT, a_long LONG,
                        a_date DATE, a_ts_us TIMESTAMP, a_ts_ns TIMESTAMP_NS, a_float FLOAT, a_double DOUBLE,
                        a_string STRING, a_varchar VARCHAR, a_symbol SYMBOL, a_binary BINARY, a_long256 LONG256,
                        a_uuid UUID, a_long128 LONG128, a_ipv4 IPv4,
                        a_geo_b GEOHASH(1c), a_geo_s GEOHASH(3c), a_geo_i GEOHASH(6c), a_geo_l GEOHASH(12c),
                        a_dec8 DECIMAL(2, 1), a_dec16 DECIMAL(4, 2), a_dec32 DECIMAL(9, 3), a_dec64 DECIMAL(18, 4),
                        a_dec128 DECIMAL(38, 5), a_dec256 DECIMAL(76, 6), a_arr DOUBLE[], a_arr2 DOUBLE[][],
                        a_dropped INT, ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("ALTER TABLE t DROP COLUMN a_dropped");
            drainWalQueue();
            try (
                    TableWriter writer = getWriter("t");
                    TableReader reader = getReader("t");
                    RecordCursorFactory factory = select("SELECT * FROM t")
            ) {
                assertPolicies(writer.getMetadata());
                assertPolicies(reader.getMetadata());
                assertPolicies(factory.getMetadata());
            }
        });
    }

    private static void assertPolicies(RecordMetadata metadata) {
        int noneCount = 0;
        int liveCount = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int type = metadata.getColumnType(i);
            if (type < 0) {
                continue;
            }
            liveCount++;
            final short tag = ColumnType.tagOf(type);
            final boolean isValueOnly = tag == ColumnType.BOOLEAN || tag == ColumnType.BYTE
                    || tag == ColumnType.SHORT || tag == ColumnType.CHAR;
            final NullPolicy policy = metadata.getColumnNullPolicy(i);
            Assert.assertEquals(metadata.getColumnName(i), ColumnType.getTypeDriver(type).getNullPolicy(), policy);
            Assert.assertEquals(metadata.getColumnName(i), isValueOnly ? NullPolicy.NONE : NullPolicy.SENTINEL, policy);
            if (policy == NullPolicy.NONE) {
                noneCount++;
            }
        }
        Assert.assertEquals(32, liveCount);
        Assert.assertEquals(4, noneCount);
    }
}
