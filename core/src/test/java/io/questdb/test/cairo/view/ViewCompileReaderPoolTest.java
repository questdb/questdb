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

package io.questdb.test.cairo.view;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import org.junit.BeforeClass;
import org.junit.Test;

public class ViewCompileReaderPoolTest extends AbstractViewTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Two readers per table make real pool exhaustion cheap and deterministic.
        setProperty(PropertyKey.CAIRO_READER_POOL_MAX_SEGMENTS, 1);
        setProperty(PropertyKey.DEBUG_CAIRO_POOL_SEGMENT_SIZE, 2);
        AbstractViewTest.setUpStatic();
    }

    @Test
    public void testCompileOnFullReaderPoolStillInvalidatesView() throws Exception {
        // A view compile that finds the reader pool of its table full met no lock: it marks the view
        // invalid with the pool's error, and leaves no busy retry.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createTableWithRows("t");
            createView("v", "SELECT * FROM t");
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN b INT");
            final TableToken tableToken = engine.verifyTableName("t");
            try (
                    TableReader ignored1 = engine.getReader(tableToken);
                    TableReader ignored2 = engine.getReader(tableToken)
            ) {
                drainViewQueue();
            }
            assertViewStatus("v", "invalid", "[-1]: table busy [reason=pool size exceeded]", "2025-06-19T11:00:00.000000Z");
            assertViewColumns("v", "a:INT");
            assertPendingCompileBusyRetries("");
        });
    }
}
