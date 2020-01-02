/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import org.junit.Assert;
import org.junit.Test;

public class TableColumnMetadataTest {
    @Test
    public void testHasIndex() {
        TableColumnMetadata metadata = new TableColumnMetadata("x", ColumnType.INT, true, 0, true);
        Assert.assertEquals(0, metadata.getIndexValueBlockCapacity());
        Assert.assertTrue(metadata.isIndexed());
        Assert.assertTrue(metadata.isSymbolTableStatic());
    }

    @Test
    public void testNoIndex() {
        TableColumnMetadata metadata = new TableColumnMetadata("x", ColumnType.INT, false, 0, false);
        Assert.assertEquals(0, metadata.getIndexValueBlockCapacity());
        Assert.assertFalse(metadata.isIndexed());
        Assert.assertFalse(metadata.isSymbolTableStatic());
    }

}