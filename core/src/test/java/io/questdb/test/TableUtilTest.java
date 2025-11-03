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

package io.questdb.test;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mig.Mig702;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.calculateTxnLagChecksum;

public class TableUtilTest {
    @Test
    public void tableUtilsChecksumMatchesWith702Migration() {
        // This test may break in the future.
        // DO NOT CHANGE 702 migrations then, but write a new migration instead.

        Rnd rnd = new Rnd();
        for (int i = 0; i < 10; i++) {
            long txn = rnd.nextLong();
            long seqTxn = rnd.nextLong();
            int lagRowCount = rnd.nextInt();
            long lagMinTimestamp = rnd.nextLong();
            long lagMaxTimestamp = rnd.nextLong();
            int lagTxnCount = rnd.nextInt();

            long a = TableUtils.calculateTxnLagChecksum(txn, seqTxn, lagRowCount, lagMinTimestamp, lagMaxTimestamp, lagTxnCount);
            long b = Mig702.calculateTxnLagChecksum(txn, seqTxn, lagRowCount, lagMinTimestamp, lagMaxTimestamp, lagTxnCount);

            Assert.assertEquals(a, b);
        }

    }

    @Test
    public void testLagChecksum() {
        long a = calculateTxnLagChecksum(0, 0, 0, Long.MIN_VALUE, Long.MAX_VALUE, 0);
        Assert.assertEquals(923520, a);

        long b = calculateTxnLagChecksum(1, 1, 0, Long.MIN_VALUE, Long.MAX_VALUE, 0);
        Assert.assertEquals(892768, b);

        long c = calculateTxnLagChecksum(1256, 10, 0, 0, Micros.DAY_MICROS, -1000);
        Assert.assertEquals(-1535571678, c);

        long d = calculateTxnLagChecksum(1257, 10, 1, 0, Micros.DAY_MICROS, -1001);
        Assert.assertEquals(-1535536465, d);

        long e = calculateTxnLagChecksum(1258, 11, 0, 0, Micros.DAY_MICROS, 1);
        Assert.assertEquals(-1535631686, e);
    }
}
