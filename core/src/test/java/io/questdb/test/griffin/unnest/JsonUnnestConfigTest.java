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

package io.questdb.test.griffin.unnest;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests for the configurable JSON UNNEST max value size
 * ({@code cairo.json.unnest.max.value.size}). Isolated from
 * {@link JsonUnnestTest} because {@code setProperty()} persists
 * across tests within the same class.
 */
public class JsonUnnestConfigTest extends AbstractCairoTest {

    @Test
    public void testConfigMaxValueSizeLargerAllowsBigValues() throws Exception {
        // A value that exceeds the default 4096 should succeed with a larger limit.
        node1.getConfigurationOverrides().setProperty(
                PropertyKey.CAIRO_JSON_UNNEST_MAX_VALUE_SIZE, 16_384
        );
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            StringBuilder bigVal = new StringBuilder();
            bigVal.append("x".repeat(5000));
            execute("INSERT INTO t VALUES ('[{\"s\":\"" + bigVal + "\"}]')");
            assertQueryNoLeakCheck(
                    "s\n"
                            + bigVal + "\n",
                    "SELECT u.s FROM t, UNNEST(t.payload COLUMNS(s VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testConfigMaxValueSizeOverflow() throws Exception {
        // Override to a small limit so a normal-length value triggers overflow.
        node1.getConfigurationOverrides().setProperty(
                PropertyKey.CAIRO_JSON_UNNEST_MAX_VALUE_SIZE, 8
        );
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"s\":\"longer_than_8_bytes\"}]')");
            assertException(
                    "SELECT u.s FROM t, UNNEST(t.payload COLUMNS(s VARCHAR)) u",
                    0,
                    "exceeds maximum size"
            );
        });
    }
}
