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

package com.questdb.griffin.engine.map;

import com.questdb.cairo.GenericRecordMetadata;
import com.questdb.cairo.TableColumnMetadata;
import com.questdb.common.ColumnType;
import com.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DirectMapTest {
    @Test
    public void testSimple() {
        final GenericRecordMetadata keyMetadata = new GenericRecordMetadata();
        keyMetadata.add(new TableColumnMetadata("x", ColumnType.STRING));

        final GenericRecordMetadata valueMetadata = new GenericRecordMetadata();
        valueMetadata.add(new TableColumnMetadata("y", ColumnType.LONG));

        HashMap<String, Long> verify = new HashMap<>();

        final int n = 1000;
        Rnd rnd = new Rnd();
        try (DirectMap map = new DirectMap(n, 4 * 1024 * 1024, 0.5f, keyMetadata, valueMetadata)) {
            for (int i = 0; i < n; i++) {
                DirectMap.Key key = map.key();
                CharSequence str = rnd.nextChars(10);
                long value = rnd.nextLong();
                key.putStr(str);

                long index = map.keyIndex(key);
                Assert.assertTrue(index > -1);
                DirectMap.Values values = map.valuesAt(key, index);
                values.putLong(0, value);
                verify.put(str.toString(), value);
            }

            for (Map.Entry<String, Long> e : verify.entrySet()) {
                DirectMap.Key w = map.key();
                w.putStr(e.getKey());
                long index = map.keyIndex(w);
                Assert.assertTrue(index < 0);
                DirectMap.Values values = map.valuesAt(w, index);
                Assert.assertNotNull(values);
                Assert.assertEquals((long) e.getValue(), values.getLong(0));
            }

            for (DirectMapEntry e : map) {
                Long value = verify.get(e.getStr(1));
                Assert.assertNotNull(value);
                Assert.assertEquals((long) value, e.getLong(0));

            }
            Assert.assertEquals(n, map.size());
        }
    }
}