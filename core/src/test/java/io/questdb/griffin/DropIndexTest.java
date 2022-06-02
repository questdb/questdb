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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

public class DropIndexTest extends AbstractGriffinTest {
    @Test
    public void testDropIndexOfNonIndexedColumnShouldFail() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table sensors as (\n" +
                            "    select \n" +
                            "        rnd_symbol('ALPHA', 'OMEGA', 'THETA') sensor_id, \n" +
                            "        rnd_int() temperature, \n" +
                            "        timestamp_sequence(0, 36000000) ts \n" +
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
    public void testDropIndexOfIndexedColumnPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table sensors as (\n" +
                            "    select\n" +
                            "        rnd_symbol('ALPHA', 'OMEGA', 'THETA') sensor_id, \n" +
                            "        rnd_int() temperature, \n" +
                            "        timestamp_sequence(0, 36000000) ts \n" +
                            "    from long_sequence(1000)\n" +
                            "), index(sensor_id capacity 32) timestamp(ts) partition by HOUR",
                    sqlExecutionContext
            );
            verifyIndexMetadata("sensors", "sensor_id", true, 32);

            compile("alter table sensors alter column sensor_id drop index", sqlExecutionContext);
            verifyIndexMetadata("sensors", "sensor_id", false, configuration.getIndexValueBlockSize());
        });
    }

    @Test
    public void testDropIndexOfIndexedColumnNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table sensors as (\n" +
                            "    select\n" +
                            "        rnd_symbol('ALPHA', 'OMEGA', 'THETA') sensor_id, \n" +
                            "        rnd_int() temperature, \n" +
                            "        timestamp_sequence(0, 36000000) ts \n" +
                            "    from long_sequence(100)\n" +
                            "), index(sensor_id capacity 32) timestamp(ts) partition by NONE",
                    sqlExecutionContext
            );
            verifyIndexMetadata("sensors", "sensor_id", true, 32);

            compile("alter table sensors alter column sensor_id drop index", sqlExecutionContext);
            verifyIndexMetadata("sensors", "sensor_id", false, configuration.getIndexValueBlockSize());
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

    private void verifyIndexMetadata(String tableName, String columnName, boolean isIndexed, int indexValueBlockSize) throws Exception {
        try (TableReader tableReader = new TableReader(configuration, tableName)) {
            try (TableReaderMetadata metadata = tableReader.getMetadata()) {
                final int colIdx = metadata.getColumnIndex(columnName);
                final TableColumnMetadata colMetadata = metadata.getColumnQuick(colIdx);
                Assert.assertEquals(isIndexed, colMetadata.isIndexed());
                Assert.assertEquals(indexValueBlockSize, colMetadata.getIndexValueBlockCapacity());

                final Path tablePath = FileSystems.getDefault().getPath((String) configuration.getRoot(), tableName);
                final Set<Path> indexFiles = Files.find(
                        tablePath,
                        Integer.MAX_VALUE,
                        (path, _attrs) -> isIndexRelatedFile(tablePath, columnName, path)
                ).collect(Collectors.toSet());
                if (isIndexed) {
                    if (PartitionBy.isPartitioned(metadata.getPartitionBy())) {
                        Assert.assertTrue(indexFiles.size() > 2);
                    } else {
                        Assert.assertEquals(2, indexFiles.size());
                    }
                } else {
                    Assert.assertTrue(indexFiles.isEmpty());
                }
            }
        }
    }

    private static boolean isIndexRelatedFile(Path tablePath, String colName, Path filePath) {
        final String fn = filePath.getFileName().toString();
        return (fn.equals(colName + ".k") || fn.equals(colName + ".v")) && !filePath.getParent().equals(tablePath);
    }
}
