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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * {@code CairoEngine.checkTableDroppable} vetoes dropping a table outright, independently of
 * permissions. Enterprise overrides it for the view audit table, so every route that drops a table
 * has to consult it - a veto that only one route honours is not a veto.
 */
public class DropTableVetoTest extends AbstractCairoTest {

    private static final String UNDROPPABLE = "undroppable";
    private static final String VETO_MESSAGE = "this table cannot be dropped";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public void checkTableDroppable(TableToken tableToken) {
                if (Chars.equalsIgnoreCase(tableToken.getTableName(), UNDROPPABLE)) {
                    throw CairoException.nonCritical().put(VETO_MESSAGE);
                }
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDropAllTablesHonoursTheVeto() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ordinary (s SYMBOL)");
            execute("CREATE TABLE " + UNDROPPABLE + " (s SYMBOL)");

            try {
                execute("DROP ALL TABLES");
                fail("expected DROP ALL TABLES to report the vetoed table");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), VETO_MESSAGE);
            }

            assertNull("an ordinary table should still have been dropped", engine.getTableTokenIfExists("ordinary"));
            assertNotNull("the vetoed table must survive DROP ALL TABLES", engine.getTableTokenIfExists(UNDROPPABLE));
        });
    }

    @Test
    public void testDropTableHonoursTheVeto() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + UNDROPPABLE + " (s SYMBOL)");

            try {
                execute("DROP TABLE " + UNDROPPABLE);
                fail("expected DROP TABLE to be vetoed");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), VETO_MESSAGE);
            }

            assertNotNull("the vetoed table must survive DROP TABLE", engine.getTableTokenIfExists(UNDROPPABLE));
        });
    }

    @Test
    public void testTextImportOverwriteHonoursTheVeto() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + UNDROPPABLE + " (s SYMBOL)");
            execute("INSERT INTO " + UNDROPPABLE + " VALUES ('kept')");

            // Overwriting drops the table and creates one to the file's shape, so it is a drop too.
            try (TextLoader loader = new TextLoader(engine)) {
                loader.setState(TextLoader.ANALYZE_STRUCTURE);
                loader.configureDestination(new Utf8String(UNDROPPABLE), true, Atomicity.SKIP_ROW, PartitionBy.NONE, null, null);
                loadText(loader, "a,b\r\n1,2\r\n");
                fail("expected the overwrite to be vetoed");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), VETO_MESSAGE);
            }

            // The table survives with its own shape and rows, not the file's.
            assertQuery("SELECT * FROM " + UNDROPPABLE)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s
                            kept
                            """);
        });
    }

    private static void loadText(TextLoader loader, String text) throws Exception {
        final byte[] bytes = text.getBytes(Files.UTF_8);
        final long buf = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_TEXT_PARSER_RSS);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(buf + i, bytes[i]);
            }
            loader.parse(buf, buf + bytes.length, AllowAllSecurityContext.INSTANCE);
            loader.wrapUp();
        } finally {
            Unsafe.free(buf, bytes.length, MemoryTag.NATIVE_TEXT_PARSER_RSS);
        }
    }
}
