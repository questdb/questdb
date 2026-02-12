/*******************************************************************************
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

import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertEquals;

public class DdlListenerTest extends AbstractCairoTest {

    @Test
    public void testDdlListener() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    assertEquals("admin", securityContext.getPrincipal());
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("z", columnName));
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("v", columnName));
                    Assert.assertFalse(cascadePermissions);
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("z", oldColumnName));
                    Assert.assertTrue(Chars.equals("v", newColumnName));
                    callbackCounters[2]++;
                }

                @Override
                public void onTableDropped(String tableName, boolean cascadePermissions) {
                    assertEquals("tab2", tableName);
                    Assert.assertFalse(cascadePermissions);
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    assertEquals("admin", securityContext.getPrincipal());
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertEquals(TableUtils.TABLE_KIND_REGULAR_TABLE, tableKind);
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    assertEquals("tab", oldTableToken.getTableName());
                    assertEquals("tab2", newTableToken.getTableName());
                    callbackCounters[5]++;
                }
            });

            engine.execute("create table tab(ts timestamp, x long, y byte) timestamp(ts) partition by day wal");
            drainWalQueue();
            engine.execute("alter table tab add column z varchar");
            drainWalQueue();
            engine.execute("alter table tab rename column z to v");
            drainWalQueue();
            engine.execute("alter table tab drop column v");
            drainWalQueue();
            engine.execute("rename table tab to tab2");
            drainWalQueue();
            engine.execute("drop table tab2");
            drainWalQueue();

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(1, callbackCounter);
            }
        });
    }
}
