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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.TestFilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import org.junit.Assert;
import org.junit.Test;

public class WalTableListFunctionFactoryTest extends AbstractGriffinTest {
    @Test
    public void testWalTablesSelectAll() throws Exception {
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Chars.contains(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {
            cloneCreateTable("A", false);
            cloneCreateTable("B", true);
            cloneCreateTable("C", true);
            cloneCreateTable("D", true);

            executeInsert("insert into B values (1, 'A', '2022-12-05T01', 'B')");
            compile("update B set x = 101");
            executeInsert("insert into B values (2, 'C', '2022-12-05T02', 'D')");
            executeInsert("insert into C values (1, 'A', '2022-12-05T01', 'B')");
            executeInsert("insert into C values (2, 'C', '2022-12-05T02', 'D')");
            executeInsert("insert into D values (1, 'A', '2022-12-05T01', 'B')");

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.getTableToken("B")));
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.getTableToken("C")));
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.getTableToken("D")));

            assertSql("wal_tables() order by name", "name\tsuspended\twriterTxn\tsequencerTxn\n" +
                    "B\ttrue\t1\t3\n" +
                    "C\tfalse\t2\t2\n" +
                    "D\tfalse\t1\t1\n");

            assertSql("select name, suspended, writerTxn from wal_tables() order by name", "name\tsuspended\twriterTxn\n" +
                    "B\ttrue\t1\n" +
                    "C\tfalse\t2\n" +
                    "D\tfalse\t1\n");
            assertSql("select name, suspended, writerTxn from wal_tables() where name = 'B'", "name\tsuspended\twriterTxn\n" +
                    "B\ttrue\t1\n");
        });
    }

    private void cloneCreateTable(final String tableName, boolean isWal) throws SqlException {
        compile("create table " + tableName + " (" +
                "x long," +
                "sym symbol," +
                "ts timestamp," +
                "sym2 symbol" +
                ") timestamp(ts) partition by DAY" + (isWal ? " WAL" : ""));
    }
}
