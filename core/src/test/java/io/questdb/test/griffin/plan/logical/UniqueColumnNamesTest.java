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

package io.questdb.test.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.plan.logical.UniqueColumnNames;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class UniqueColumnNamesTest {
    @Test
    public void testMetadataReservesNamesIgnoringCase() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("X_1", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("x_3", ColumnType.LONG));
        final UniqueColumnNames names = new UniqueColumnNames().of(metadata);
        final CharacterStore store = new CharacterStore(64, 4);
        TestUtils.assertEquals("x_2", names.take("x", store, '_'));
        TestUtils.assertEquals("X_4", names.take("X", store, '_'));
        TestUtils.assertEquals("x_1_1", names.take("x_1", store, '_'));
        TestUtils.assertEquals("fresh", names.take("fresh", store, '_'));
        names.of(metadata);
        TestUtils.assertEquals("x_2", names.take("x", store, '_'));
    }

    @Test
    public void testReleasedUnderscoreSuffixIsReused() {
        final UniqueColumnNames names = new UniqueColumnNames();
        final CharacterStore store = new CharacterStore(64, 4);
        TestUtils.assertEquals("x", names.take("x", store));
        TestUtils.assertEquals("x1", names.take("x", store));
        TestUtils.assertEquals("x_", names.take("x_", store));
        TestUtils.assertEquals("x_1", names.take("x", store, '_'));
        TestUtils.assertEquals("x_2", names.take("x_", store));
        TestUtils.assertEquals("x_3", names.take("x", store, '_'));
        names.release("X_1");
        TestUtils.assertEquals("x_1", names.take("x", store, '_'));
        TestUtils.assertEquals("x2", names.take("x", store));
    }
}
