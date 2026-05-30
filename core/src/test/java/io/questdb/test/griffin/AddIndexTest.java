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
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AddIndexTest extends AbstractCairoTest {

    @Test
    public void testAddIndexToColumnWithTop() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table trades as (
                                select\s
                                    rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                    rnd_double() price,\s
                                    timestamp_sequence(172800000000, 1) ts\s
                                from long_sequence(1000)
                            ) timestamp(ts) partition by DAY"""
            );
            execute("alter table trades add column sym2 symbol");
            execute("alter table trades alter column sym2 add index");

            assertSql("sym\tprice\tts\tsym2\n", "trades where sym2 = 'ABB'");
        });
    }

    @Test
    public void testAddIndexToColumnWithTop2() throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = (int) configuration.getDataAppendPageSize() / Integer.BYTES + 1;
            execute(
                    "create table trades as (\n" +
                            "    select \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                            "        rnd_double() price, \n" +
                            "        timestamp_sequence(172800000000, 1) ts \n" +
                            "    from long_sequence(" + rowCount + ")\n" +
                            ") timestamp(ts) partition by DAY"
            );

            execute("alter table trades add column sym2 symbol");
            execute(
                    "insert into trades \n" +
                            "    select \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                            "        rnd_double() price, \n" +
                            "        timestamp_sequence(172800000000 + " + rowCount + ", 1) ts, \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym2 \n" +
                            "    from long_sequence(" + rowCount + ")\n"
            );

            execute("alter table trades alter column sym2 add index");
            // While row count is derived from append page size, the expected row count value is hardcoded
            // as a string. Test will fail should append page size change.
            assertSql("""
                    count
                    175654
                    """, "select count(*) from trades where sym2 = 'ABB'");
        });
    }

    @Test
    public void testAddIndexToIndexedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table trades as (
                                select\s
                                    rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                    rnd_double() price,\s
                                    timestamp_sequence(172800000000, 36000000) ts\s
                                from long_sequence(10000)
                            ) timestamp(ts) partition by DAY"""
            );
            execute("alter table trades alter column sym add index");

            try {
                execute("alter table trades alter column sym add index");
                Assert.fail();
            } catch (SqlException | CairoException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "already indexed");
            }
        });
    }

    @Test
    public void testAlterTableAlterColumnSyntaxError1() throws Exception {
        assertQuery("alter table trades alter columnz")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(25, "'column' or 'partition' expected");
    }

    @Test
    public void testAlterTableAlterColumnSyntaxError2() throws Exception {
        assertQuery("alter table trades alter column price add index")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(32, "indexes are only supported for symbol type [column=price, type=DOUBLE]");
    }

    @Test
    public void testAlterTableAlterColumnSyntaxError3() throws Exception {
        assertQuery("alter table trades alter column sym add index")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ), index(sym) timestamp(ts) partition by DAY""")
                .fails(12, "column is already indexed [column=sym]");
    }

    @Test
    public void testAlterTableAttachPartitionSyntaxError1() throws Exception {
        assertQuery("alter table trades attach bucket")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(26, "'partition' expected");
    }

    @Test
    public void testAlterTableDropColumnSyntaxError1() throws Exception {
        assertQuery("alter table trades drop bucket")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(24, "'column' or 'partition' expected");
    }

    @Test
    public void testAlterTableRenameColumnSyntaxError1() throws Exception {
        assertQuery("alter table trades rename bucket")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(26, "'column' expected");
    }

    @Test
    public void testBeforeAndAfterIndex() throws Exception {
        final String expected = """
                sym\tprice\tts
                ABB\t0.8043224099968393\t1970-01-03T00:00:00.000000Z
                HBC\t0.6508594025855301\t1970-01-03T00:00:00.001080Z
                HBC\t0.7905675319675964\t1970-01-03T00:00:00.001440Z
                ABB\t0.22452340856088226\t1970-01-03T00:00:00.001800Z
                ABB\t0.3491070363730514\t1970-01-03T00:00:00.002160Z
                ABB\t0.7611029514995744\t1970-01-03T00:00:00.002520Z
                ABB\t0.4217768841969397\t1970-01-03T00:00:00.002880Z
                HBC\t0.0367581207471136\t1970-01-03T00:00:00.003240Z
                HBC\t0.8799634725391621\t1970-01-03T00:00:00.004680Z
                HBC\t0.5249321062686694\t1970-01-03T00:00:00.005040Z
                HBC\t0.1911234617573182\t1970-01-03T00:00:00.006480Z
                ABB\t0.5793466326862211\t1970-01-03T00:00:00.006840Z
                ABB\t0.42281342727402726\t1970-01-03T00:00:00.008280Z
                HBC\t0.810161274171258\t1970-01-03T00:00:00.008640Z
                HBC\t0.022965637512889825\t1970-01-03T00:00:00.009360Z
                ABB\t0.7763904674818695\t1970-01-03T00:00:00.009720Z
                HBC\t0.0011075361080621349\t1970-01-03T00:00:00.010440Z
                """;

        assertQuery("select * from trades where (sym = 'ABB' or sym = 'HBC')")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("alter table trades alter column sym add index")
                .timestamp("ts")
                .returns(expected, expected);
    }
}
