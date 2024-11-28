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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ShowCreateTableTest extends AbstractCairoTest {

    @Test
    public void testMinimalDdl() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp)");
            assertSql("ddl\n" +
                    "CREATE TABLE foo ( \n" +
                    "\tts TIMESTAMP\n" +
                    ");\n", "show create table foo");
        });
    }

    @Test
    public void testWithSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol capacity 512 nocache)");
            assertSql("ddl\n" +
                    "CREATE TABLE foo ( \n" +
                    "\tts TIMESTAMP,\n" +
                    "\ts SYMBOL CAPACITY 512 NOCACHE\n" +
                    ");\n", "show create table foo");
        });
    }

    @Test
    public void testWithSymbolAndIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol capacity 512 nocache index capacity 1024)");
            assertSql("ddl\n" +
                    "CREATE TABLE foo ( \n" +
                    "\tts TIMESTAMP,\n" +
                    "\ts SYMBOL CAPACITY 512 NOCACHE INDEX CAPACITY 1024\n" +
                    ");\n", "show create table foo");
        });
    }

    @Test
    public void testWithSymbolDefaultsToCache() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol)");
            assertSql("ddl\n" +
                    "CREATE TABLE foo ( \n" +
                    "\tts TIMESTAMP,\n" +
                    "\ts SYMBOL CAPACITY 256 CACHE\n" +
                    ");\n", "show create table foo");
        });
    }

}
