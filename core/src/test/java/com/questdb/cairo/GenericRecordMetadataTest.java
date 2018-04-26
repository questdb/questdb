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
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GenericRecordMetadataTest {
    @Test
    public void testDuplicateColumn() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("abc", ColumnType.INT));
        metadata.add(new TableColumnMetadata("cde", ColumnType.INT));

        Assert.assertEquals(2, metadata.getColumnCount());

        Assert.assertEquals(0, metadata.getColumnIndexQuiet("abc"));
        Assert.assertEquals(1, metadata.getColumnIndexQuiet("cde"));
        try {
            metadata.add(new TableColumnMetadata("abc", ColumnType.FLOAT));
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "Duplicate column");
        }

        Assert.assertEquals(0, metadata.getColumnIndexQuiet("abc"));
        Assert.assertEquals(ColumnType.INT, metadata.getColumnType(0));
    }
}