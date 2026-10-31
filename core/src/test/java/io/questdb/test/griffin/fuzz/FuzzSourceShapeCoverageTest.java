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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.fuzz.types.IntType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the SAMPLE BY inner queries of {@link FuzzSource}: they nest the stream GROUP BY under the outer clauses,
 * so the stream on/off axis reaches it under LATEST ON, windows, SAMPLE BY, GROUP BY and LIMIT.
 */
public class FuzzSourceShapeCoverageTest {

    @Test
    public void testSubqueriesSampleByKeepingTheTimestamp() {
        final ObjList<FuzzColumn> columns = new ObjList<>();
        columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
        columns.add(new FuzzColumn("i", IntType.INSTANCE));
        final FuzzSource base = FuzzSource.direct(new FuzzTable("t", columns, "ts"));
        final Rnd rnd = new Rnd();
        int keyed = 0;
        int plain = 0;
        for (int i = 0; i < 1_000; i++) {
            final FuzzSource source = rnd.nextBoolean() ? FuzzSource.subquery(rnd, base) : FuzzSource.cte(rnd, base, i);
            final String sql = source.getPrefixSql() + source.getFromSql();
            if (sql.contains(" SAMPLE BY ")) {
                final FuzzTable table = source.getTable();
                Assert.assertTrue(source.isOrderStable());
                if (sql.contains(" ORDER BY ts, k")) {
                    keyed++;
                    Assert.assertTrue(sql, sql.contains("SELECT ts AS ts, i AS k, count() AS cnt FROM t SAMPLE BY "));
                    Assert.assertNull(table.getTsColumnName());
                    Assert.assertEquals(3, table.getColumns().size());
                } else {
                    plain++;
                    Assert.assertTrue(sql, sql.contains("SELECT ts AS ts, count() AS cnt FROM t SAMPLE BY "));
                    Assert.assertEquals("ts", table.getTsColumnName());
                    Assert.assertEquals(2, table.getColumns().size());
                }
            }
        }
        Assert.assertTrue("only " + keyed + " keyed and " + plain + " plain of 1000 sources sample by", keyed > 25 && plain > 25);
    }
}
