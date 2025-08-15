/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.PartitionUpdater;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class PartitionUpdaterTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public PartitionUpdaterTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testBadUpdate() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "x";
            final long rows = 10;
            final FilesFacade ff = configuration.getFilesFacade();
            execute("create table " + tableName + " as (select" +
                    " x id," +
                    " timestamp_sequence(400000000000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                    " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by day");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader(tableName);
                    PartitionUpdater updater = new PartitionUpdater(ff)
            ) {
                // Initial partition dir created.
                final TableToken table = engine.getTableTokenIfExists(tableName);
                path
                        .concat(root)
                        .concat(table.getDirNameUtf8())
                        .concat("1970-01-05")
                        .slash$();
                final int partitionDirLen = path.size();
                Assert.assertTrue(ff.exists(path.$()));
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);

                final CairoException badPathExc = Assert.assertThrows(
                        CairoException.class,
                        () -> PartitionEncoder.encode(descriptor, path));
                TestUtils.assertContains(
                        badPathExc.getMessage(),
                        "Could not create parquet file");

                path.trimTo(partitionDirLen);
                path.put(".1").slash$();
                ff.mkdirs(path, configuration.getMkDirMode());
                Assert.assertTrue(ff.exists(path.$()));
                Assert.assertTrue(ff.isDirOrSoftLinkDir(path.$()));
                path.concat("data.parquet").$();

                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                // The parquet file has been created.
                // The old directory is still there. We will not do the extra accounting here in this test.
                Assert.assertTrue(ff.exists(path.$()));
                final long parquetPartitionSize = ff.length(path.$());

                // Update the partition, adding a row.
                updater.of(
                        path.$(),
                        configuration.getWriterFileOpenOpts(),
                        parquetPartitionSize,
                        1,  // index of the timestamp column
                        0, // uncompressed
                        false,
                        false,
                        0,
                        0
                );

                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);

                // Once with a bad row group id
                final CairoException badGroupIdExc = Assert.assertThrows(
                        CairoException.class,
                        () -> updater.updateRowGroup((short) 1000, descriptor));
                TestUtils.assertContains(
                        badGroupIdExc.getMessage(),
                        "Row group ordinal must be less then 1, got 1000");

                // Once again with the correct row group id
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                updater.updateRowGroup((short) 0, descriptor);
                updater.updateFileMetadata();

                final long updatedParquetPartitionSize = ff.length(path.$());
                Assert.assertTrue(updatedParquetPartitionSize > parquetPartitionSize);
            }
        });
    }
}
