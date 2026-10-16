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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.PartitionDeltaWriter;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class CairoConfigurationWrapperTest extends AbstractCairoTest {

    @Test
    public void testForwardsPartitionDeltaWriter() {
        // An outer wrapper must reach the inner writer; a null one makes TableWriter drop Delta rows.
        final PartitionDeltaWriter deltaWriter = new StubPartitionDeltaWriter();
        final CairoConfiguration inner = new CairoConfigurationWrapper(configuration) {
            @Override
            public PartitionDeltaWriter newPartitionDeltaWriter() {
                return deltaWriter;
            }
        };

        Assert.assertSame(deltaWriter, new CairoConfigurationWrapper(inner).newPartitionDeltaWriter());
    }

    private static class StubPartitionDeltaWriter implements PartitionDeltaWriter {
        @Override
        public void checkAttach(TableWriter writer, long partitionTimestamp, long baseRowCount, long boundsAddr) {
        }

        @Override
        public void detach(TableWriter writer, int partitionIndex, Path detachedPartitionPath) {
        }

        @Override
        public void initialize(TableWriter writer, int partitionIndex) {
        }

        @Override
        public boolean purge(Path partitionDir) {
            return true;
        }

        @Override
        public void readTimestampBounds(TableWriter writer, int partitionIndex, long boundsAddr) {
        }

        @Override
        public boolean rollback(TableWriter writer, int partitionIndex, long seqTxn) {
            return false;
        }

        @Override
        public void writeCommit(
                TableWriter writer,
                long partitionTimestamp,
                long partitionNameTxn,
                ReadOnlyObjList<? extends MemoryCR> o3Columns,
                long sortedTimestampsAddr,
                long srcOooLo,
                long srcOooHi,
                long seqTxn,
                long commitTimestamp
        ) {
        }
    }
}
