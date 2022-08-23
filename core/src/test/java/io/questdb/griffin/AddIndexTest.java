/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AddIndexTest extends AbstractGriffinTest {

    @Test
    public void testAddIndexToIndexedColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table trades as (\n" +
                            "    select \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                            "        rnd_double() price, \n" +
                            "        timestamp_sequence(172800000000, 36000000) ts \n" +
                            "    from long_sequence(10000)\n" +
                            ") timestamp(ts) partition by DAY",
                    sqlExecutionContext
            );
            compile("alter table trades alter column sym add index", sqlExecutionContext);

            try {
                compile("alter table trades alter column sym add index", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "already indexed");
            }
        });
    }

    @Test
    public void testAddIndexToColumnWithTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table trades as (\n" +
                            "    select \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                            "        rnd_double() price, \n" +
                            "        timestamp_sequence(172800000000, 1) ts \n" +
                            "    from long_sequence(1000)\n" +
                            ") timestamp(ts) partition by DAY",
                    sqlExecutionContext
            );
            compile("alter table trades add column sym2 symbol", sqlExecutionContext);
            compile("alter table trades alter column sym2 add index", sqlExecutionContext);

            assertSql("trades where sym2 = 'ABB'", "sym\tprice\tts\tsym2\n");
        });
    }

    @Test
    public void testAddIndexToColumnWithTop2() throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = (int) configuration.getDataAppendPageSize() / Integer.BYTES + 1;
            compiler.compile(
                    "create table trades as (\n" +
                            "    select \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                            "        rnd_double() price, \n" +
                            "        timestamp_sequence(172800000000, 1) ts \n" +
                            "    from long_sequence(" + rowCount + ")\n" +
                            ") timestamp(ts) partition by DAY",
                    sqlExecutionContext
            );

            compile("alter table trades add column sym2 symbol", sqlExecutionContext);
            compiler.compile(
                    "insert into trades \n" +
                            "    select \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                            "        rnd_double() price, \n" +
                            "        timestamp_sequence(172800000000 + " + rowCount + ", 1) ts, \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym2 \n" +
                            "    from long_sequence(" + rowCount + ")\n",
                    sqlExecutionContext
            );

            compile("alter table trades alter column sym2 add index", sqlExecutionContext);
            assertSql("select count(*) from trades where sym2 = 'ABB'", "count\n" +
                    "1398605\n");
        });
    }

    @Test
    public void testBeforeAndAfterIndex() throws Exception {
        final String expected = "sym\tprice\tts\n" +
                "ABB\t0.8043224099968393\t1970-01-03T00:00:00.000000Z\n" +
                "HBC\t0.6508594025855301\t1970-01-03T00:00:00.001080Z\n" +
                "HBC\t0.7905675319675964\t1970-01-03T00:00:00.001440Z\n" +
                "ABB\t0.22452340856088226\t1970-01-03T00:00:00.001800Z\n" +
                "ABB\t0.3491070363730514\t1970-01-03T00:00:00.002160Z\n" +
                "ABB\t0.7611029514995744\t1970-01-03T00:00:00.002520Z\n" +
                "ABB\t0.4217768841969397\t1970-01-03T00:00:00.002880Z\n" +
                "HBC\t0.0367581207471136\t1970-01-03T00:00:00.003240Z\n" +
                "HBC\t0.8799634725391621\t1970-01-03T00:00:00.004680Z\n" +
                "HBC\t0.5249321062686694\t1970-01-03T00:00:00.005040Z\n" +
                "HBC\t0.1911234617573182\t1970-01-03T00:00:00.006480Z\n" +
                "ABB\t0.5793466326862211\t1970-01-03T00:00:00.006840Z\n" +
                "ABB\t0.42281342727402726\t1970-01-03T00:00:00.008280Z\n" +
                "HBC\t0.810161274171258\t1970-01-03T00:00:00.008640Z\n" +
                "HBC\t0.022965637512889825\t1970-01-03T00:00:00.009360Z\n" +
                "ABB\t0.7763904674818695\t1970-01-03T00:00:00.009720Z\n" +
                "HBC\t0.0011075361080621349\t1970-01-03T00:00:00.010440Z\n";

        assertQuery(
                expected,
                "select * from trades where (sym = 'ABB' or sym = 'HBC')",
                "create table trades as (\n" +
                        "    select \n" +
                        "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360) ts \n" +
                        "    from long_sequence(30)\n" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "alter table trades alter column sym add index",
                expected,
                true
        );
    }

    @Test
    public void testAlterTableAlterColumnSyntaxError1() throws Exception {
        assertFailure(
                "alter table trades alter columnz",
                "create table trades as (\n" +
                        "    select \n" +
                        "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360) ts \n" +
                        "    from long_sequence(30)\n" +
                        ") timestamp(ts) partition by DAY",
                25,
                "'column' or 'partition' expected"
        );
    }

    @Test
    public void testAlterTableDropColumnSyntaxError1() throws Exception {
        assertFailure(
                "alter table trades drop bucket",
                "create table trades as (\n" +
                        "    select \n" +
                        "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360) ts \n" +
                        "    from long_sequence(30)\n" +
                        ") timestamp(ts) partition by DAY",
                24,
                "'column' or 'partition' expected"
        );
    }

    @Test
    public void testAlterTableAttachPartitionSyntaxError1() throws Exception {
        assertFailure(
                "alter table trades attach bucket",
                "create table trades as (\n" +
                        "    select \n" +
                        "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360) ts \n" +
                        "    from long_sequence(30)\n" +
                        ") timestamp(ts) partition by DAY",
                26,
                "'partition' expected"
        );
    }

    @Test
    public void testAlterTableRenameColumnSyntaxError1() throws Exception {
        assertFailure(
                "alter table trades rename bucket",
                "create table trades as (\n" +
                        "    select \n" +
                        "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360) ts \n" +
                        "    from long_sequence(30)\n" +
                        ") timestamp(ts) partition by DAY",
                26,
                "'column' expected"
        );
    }
}
