/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.common.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class TableColumnMetadataTest {
    @Test
    public void testHasIndex() {
        TableColumnMetadata metadata = new TableColumnMetadata("x", ColumnType.INT, true, 0);
        Assert.assertEquals(0, metadata.getIndexValueBlockCapacity());
        Assert.assertTrue(metadata.isIndexed());
    }

    @Test
    public void testNoIndex() {
        TableColumnMetadata metadata = new TableColumnMetadata("x", ColumnType.INT, false, 0);
        Assert.assertEquals(0, metadata.getIndexValueBlockCapacity());
        Assert.assertFalse(metadata.isIndexed());
    }

}