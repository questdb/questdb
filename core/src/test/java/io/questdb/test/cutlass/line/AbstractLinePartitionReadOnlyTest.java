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

package io.questdb.test.cutlass.line;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractBootstrapTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.util.concurrent.TimeUnit;


public class AbstractLinePartitionReadOnlyTest extends AbstractBootstrapTest {
    protected static final String TABLE_START_CONTENT = "min(ts)\tmax(ts)\tcount()\n" +
            "2022-12-08T00:05:11.070207Z\t2022-12-08T23:56:06.447339Z\t277\n" +
            "2022-12-09T00:01:17.517546Z\t2022-12-09T23:57:23.964885Z\t278\n" +
            "2022-12-10T00:02:35.035092Z\t2022-12-10T23:58:41.482431Z\t278\n" +
            "2022-12-11T00:03:52.552638Z\t2022-12-11T23:59:58.999977Z\t278\n";
    protected static final String firstPartitionName = "2022-12-08";
    protected static final long firstPartitionTs; // nanos
    protected static final String futurePartitionName = "2022-12-12";
    protected static final long futurePartitionTs; // nanos
    protected static final String lastPartitionName = "2022-12-11";
    protected static final long lastPartitionTs; // nanos
    protected static final long lineTsStep = TimeUnit.MILLISECONDS.toNanos(1L); // min resolution of Micros.toString(long)
    protected static final String secondPartitionName = "2022-12-09";
    protected static final long secondPartitionTs; // nanos

    protected static final String thirdPartitionName = "2022-12-10";
    protected static final long thirdPartitionTs; // nanos

    @Rule
    public TestName testName = new TestName();

    protected static void checkPartitionReadOnlyState(CairoEngine engine, TableToken tableToken, boolean... partitionIsReadOnly) {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        try (TableReader reader = engine.getReader(tableToken)) {
            TxReader txFile = reader.getTxFile();
            int partitionCount = txFile.getPartitionCount();
            Assert.assertTrue(partitionCount <= partitionIsReadOnly.length);
            for (int i = 0; i < partitionCount; i++) {
                Assert.assertEquals(txFile.isPartitionReadOnly(i), partitionIsReadOnly[i]);
                Assert.assertEquals(txFile.isPartitionReadOnlyByPartitionTimestamp(txFile.getPartitionTimestampByIndex(i)), partitionIsReadOnly[i]);
            }
        }
    }

    static {
        try {
            firstPartitionTs = MicrosFormatUtils.parseTimestamp(firstPartitionName + "T00:00:00.000Z") * 1000L;
            secondPartitionTs = MicrosFormatUtils.parseTimestamp(secondPartitionName + "T00:00:00.000Z") * 1000L;
            thirdPartitionTs = MicrosFormatUtils.parseTimestamp(thirdPartitionName + "T00:00:00.000Z") * 1000L;
            lastPartitionTs = MicrosFormatUtils.parseTimestamp(lastPartitionName + "T00:00:00.000Z") * 1000L;
            futurePartitionTs = MicrosFormatUtils.parseTimestamp(futurePartitionName + "T00:00:00.000Z") * 1000L;
        } catch (NumericException impossible) {
            throw new RuntimeException(impossible);
        }
    }

    static {
        LogFactory.getLog(AbstractLinePartitionReadOnlyTest.class);
    }
}
