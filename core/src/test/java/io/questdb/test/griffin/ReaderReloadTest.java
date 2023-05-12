/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ReaderReloadTest extends AbstractGriffinTest {


    @Test
    public void testReaderReloadFails() throws Exception {
        AtomicBoolean failToOpen = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
                if (Chars.endsWith(name, "new_col.d.1") && failToOpen.get()) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {

            compile("create table x as (select x, x as y, timestamp_sequence('2022-02-24', 1000000000) ts from long_sequence(100)) timestamp(ts) partition by DAY");

            TableToken xTableToken = engine.verifyTableName("x");
            TableReader reader1 = engine.getReader(xTableToken);
            Assert.assertNotNull(reader1);
            reader1.close();
            assertSql("select sum(x) / sum(x) from x", "column\n" +
                    "1\n");

            compile("alter table x add column new_col int");
            compile("insert into x select x, x, timestamp_sequence('2022-02-25T14', 1000000000) ts, x % 2 from long_sequence(100)");
            failToOpen.set(true);

            try (TableReader reader2 = engine.getReader(xTableToken)) {
                Assert.assertSame(reader1, reader2);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-only");
            }

            failToOpen.set(false);
            try (TableReader reader2 = engine.getReader(xTableToken)) {
                Assert.assertNotEquals(reader1, reader2);
            }
        });
    }
}
