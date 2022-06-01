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

import io.questdb.cairo.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DropIndexTest extends AbstractGriffinTest {
    @Test
    public void testDropIndexOfNonIndexedColumnShouldFail() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table sensors as (\n" +
                            "    select \n" +
                            "        rnd_symbol('ALPHA', 'OMEGA', 'THETA') sensor_id, \n" +
                            "        rnd_int() temperature, \n" +
                            "        timestamp_sequence(172800000000, 36000000) ts \n" +
                            "    from long_sequence(100)\n" +
                            ") timestamp(ts) partition by DAY",
                    sqlExecutionContext
            );

            try {
                compile("alter table sensors alter column sensor_id drop index", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "column 'sensor_id' is not indexed");
            }
        });
    }

    @Test
    public void testDropIndexOfIndexedColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table sensors as (\n" +
                            "    select \n" +
                            "        rnd_symbol('ALPHA', 'OMEGA', 'THETA') sensor_id, \n" +
                            "        rnd_int() temperature, \n" +
                            "        timestamp_sequence(172800000000, 36000000) ts \n" +
                            "    from long_sequence(100)\n" +
                            ") timestamp(ts) partition by DAY",
                    sqlExecutionContext
            );

            compile("alter table sensors alter column sensor_id add index capacity 128", sqlExecutionContext);

            try (TableReader tableReader = new TableReader(configuration, "sensors")) {
                try (TableReaderMetadata metadata = tableReader.getMetadata()) {
                    final int sensorIdColIdx = metadata.getColumnIndex("sensor_id");
                    final TableColumnMetadata sensorIdColMetadata = metadata.getColumnQuick(sensorIdColIdx);
                    Assert.assertTrue(sensorIdColMetadata.isIndexed());
                    Assert.assertEquals(sensorIdColMetadata.getIndexValueBlockCapacity(), 128);
                }
            }

            compile("alter table sensors alter column sensor_id drop index", sqlExecutionContext);

            // TODO: clear a FD leak ...

            try (TableReader tableReader = new TableReader(configuration, "sensors")) {
                try (TableReaderMetadata metadata = tableReader.getMetadata()) {
                    final int sensorIdColIdx = metadata.getColumnIndex("sensor_id");
                    final TableColumnMetadata sensorIdColMetadata = metadata.getColumnQuick(sensorIdColIdx);
                    Assert.assertFalse(sensorIdColMetadata.isIndexed());
                    Assert.assertEquals(sensorIdColMetadata.getIndexValueBlockCapacity(), configuration.getIndexValueBlockSize());
                }
            }
        });
    }

    @Test
    public void testAlterTableAlterColumnDropIndexSyntaxErrors0() throws Exception {
        assertFailure(
                "alter table sensors alter column sensor_id dope index",
                "create table sensors as (\n" +
                        "    select \n" +
                        "        rnd_symbol('ALPHA', 'OMEGA', 'THETA') sensor_id, \n" +
                        "        rnd_int() temperature, \n" +
                        "        timestamp_sequence(172800000000, 36000000) ts \n" +
                        "    from long_sequence(100)\n" +
                        ") timestamp(ts) partition by DAY",
                43,
                "'add', 'drop', 'cache' or 'nocache' expected found 'dope'"
        );
    }

    @Test
    public void testAlterTableAlterColumnDropIndexSyntaxErrors1() throws Exception {
        assertFailure(
                "alter table sensors alter column sensor_id drop",
                "create table sensors as (\n" +
                        "    select \n" +
                        "        rnd_symbol('ALPHA', 'OMEGA', 'THETA') sensor_id, \n" +
                        "        rnd_int() temperature, \n" +
                        "        timestamp_sequence(172800000000, 36000000) ts \n" +
                        "    from long_sequence(100)\n" +
                        ") timestamp(ts) partition by DAY",
                47,
                "'index' expected"
        );
    }

    @Test
    public void testAlterTableAlterColumnDropIndexSyntaxErrors2() throws Exception {
        assertFailure(
                "alter table sensors alter column sensor_id drop index,",
                "create table sensors as (\n" +
                        "    select \n" +
                        "        rnd_symbol('ALPHA', 'OMEGA', 'THETA') sensor_id, \n" +
                        "        rnd_int() temperature, \n" +
                        "        timestamp_sequence(172800000000, 36000000) ts \n" +
                        "    from long_sequence(100)\n" +
                        ") timestamp(ts) partition by DAY",
                53,
                "unexpected token [,] while trying to drop index"
        );
    }
}
