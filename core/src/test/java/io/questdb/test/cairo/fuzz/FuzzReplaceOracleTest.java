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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.fuzz.FuzzInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import org.junit.Test;

/**
 * Pins the replace-commit oracle of {@link FuzzRunner}: every replace fuzz test treats
 * the non-WAL table it builds as the expected result. A WAL replace commit with bounds
 * [lo, hi) deletes the rows with lo &lt;= ts &lt; hi, so the windows below put their
 * edges exactly on existing rows: at lo, at hi - 1 and at hi. Rows the table holds
 * before the list starts go through {@code removeRowsReplacedByTransactions}, rows an
 * earlier transaction of the same list writes go through {@code calculateReplaceRanges}.
 * The non-WAL table must match a hand-written expectation, and the WAL table must match
 * the non-WAL table.
 */
public class FuzzReplaceOracleTest extends AbstractFuzzTest {
    private static final long DAY = MicrosTimestampDriver.floor("2022-02-24");
    private static final String[] SYMBOLS = {"a", "b", "c"};

    @Test
    public void testReplaceWindowEdgesOnExistingRows() throws Exception {
        // initial rows: one per second from 2022-02-24T00:00:00 (FuzzRunner.createInitialTable)
        final int initialRowCount = 20;
        fuzzer.setFuzzCounts(false, 1, 1, 5, 5, 5, initialRowCount, 1, -1, 0);
        assertMemoryLeak(() -> {
            fuzzer.createInitialTableWal("w", initialRowCount);
            drainWalQueue();
            fuzzer.createInitialTableNonWal("nw", null);

            final Rnd rnd = new Rnd();
            final ObjList<FuzzTransaction> transactions = new ObjList<>();
            try {
                // lo on 00:03, hi on 00:06: 00:03..00:05 go, 00:06 stays
                transactions.add(replace(rnd, sec(3), sec(6)));
                // lo just past 00:08, hi - 1 on 00:10: 00:09 and 00:10 go, 00:08 stays,
                // and the commit brings 00:09.5
                transactions.add(replace(rnd, sec(8) + 1, sec(10) + 1, sec(9) + 500_000));
                // a plain insert, then two later replaces in the same list over its rows
                transactions.add(insert(rnd, sec(15) + 500_000, sec(17)));
                // lo == hi - 1 == the inserted 00:15.5, which must never reach the oracle
                transactions.add(replace(rnd, sec(15) + 500_000, sec(15) + 500_001));
                // [00:16, 00:17): the initial 00:16 goes; hi is 00:17, so both the initial
                // and the inserted 00:17 stay
                transactions.add(replace(rnd, sec(16), sec(17)));

                fuzzer.applyNonWal(transactions, "nw", rnd);
                fuzzer.applyWal(transactions, "w", 1, rnd);
            } finally {
                Misc.freeObjListAndClear(transactions);
            }

            assertQuery("SELECT ts FROM nw")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts
                            2022-02-24T00:00:00.000000Z
                            2022-02-24T00:00:01.000000Z
                            2022-02-24T00:00:02.000000Z
                            2022-02-24T00:00:06.000000Z
                            2022-02-24T00:00:07.000000Z
                            2022-02-24T00:00:08.000000Z
                            2022-02-24T00:00:09.500000Z
                            2022-02-24T00:00:11.000000Z
                            2022-02-24T00:00:12.000000Z
                            2022-02-24T00:00:13.000000Z
                            2022-02-24T00:00:14.000000Z
                            2022-02-24T00:00:15.000000Z
                            2022-02-24T00:00:17.000000Z
                            2022-02-24T00:00:17.000000Z
                            2022-02-24T00:00:18.000000Z
                            2022-02-24T00:00:19.000000Z
                            """);
            assertSqlCursors("nw", "w");
        });
    }

    private static FuzzTransaction insert(Rnd rnd, long... timestamps) {
        final FuzzTransaction transaction = new FuzzTransaction();
        for (long ts : timestamps) {
            transaction.operationList.add(new FuzzInsertOperation(rnd.nextLong(), rnd.nextLong(), ts, 0, 0, 0, 5, SYMBOLS));
        }
        return transaction;
    }

    // A replace commit over [lo, hi), the bounds WalWriter.commitWithParams takes,
    // bringing one row per timestamp.
    private static FuzzTransaction replace(Rnd rnd, long lo, long hi, long... timestamps) {
        final FuzzTransaction transaction = insert(rnd, timestamps);
        transaction.setReplaceRange(lo, hi);
        return transaction;
    }

    private static long sec(int seconds) {
        return DAY + seconds * Micros.SECOND_MICROS;
    }
}
