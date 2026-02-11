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

import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class TableTokenTest {
    private static final Log LOG = LogFactory.getLog(TableTokenTest.class);

    @Test
    public void testBasics() {
        final TableToken t1 = new TableToken("table1", "dir1", null, 1, true, false, false);
        Assert.assertEquals("table1", t1.getTableName());
        Assert.assertEquals("dir1", t1.getDirName());
        final boolean dirNameIdentity = t1.getDirName() == t1.getDirNameUtf8().toString();
        Assert.assertTrue(dirNameIdentity);
        Assert.assertEquals(1, t1.getTableId());
        Assert.assertTrue(t1.isWal());
        Assert.assertEquals(t1.getTableId(), t1.hashCode());

        final String descr = t1.toString();
        Assert.assertEquals("TableToken{tableName=table1, dirName=dir1, tableId=1, isView=false, isMatView=false, isWal=true, isSystem=false, isProtected=false, isPublic=false}", descr);

        final TableToken t2 = t1.renamed("table2");
        Assert.assertEquals("table2", t2.getTableName());
        Assert.assertEquals("dir1", t2.getDirName());
        Assert.assertEquals(1, t2.getTableId());
        Assert.assertTrue(t2.isWal());

        Assert.assertNotEquals(t1, t2);
        final TableToken t1b = new TableToken("table1", "dir1", null, 1, true, false, false);

        Assert.assertEquals(t1, t1b);

        final TableToken t3 = new TableToken("table3", "dir3", null, 3, false, true, true);
        Assert.assertEquals("table3", t3.getTableName());
        Assert.assertEquals("dir3", t3.getDirName());
        Assert.assertEquals(3, t3.getTableId());
        Assert.assertFalse(t3.isWal());
        Assert.assertTrue(t3.isSystem());
        Assert.assertTrue(t3.isProtected());
        Assert.assertEquals(t3.getTableId(), t3.hashCode());

        final String descr3 = t3.toString();
        Assert.assertEquals("TableToken{tableName=table3, dirName=dir3, tableId=3, isView=false, isMatView=false, isWal=false, isSystem=true, isProtected=true, isPublic=false}", descr3);
    }

    @Test
    public void testToSink() {
        final String[] strings = new String[]{
                "hello",
                "hello\u0000world",
                "àèìòù",
                "ðãµ¶",
                "🧊🦞"
        };

        for (String str : strings) {
            final TableToken tt1 = new TableToken(str, "dir1", null, 1, false, false, false);
            LOG.xinfo().$("Testing logging a fancy pants table token: >>>").$(tt1).$("<<<").$();
        }
    }
}
