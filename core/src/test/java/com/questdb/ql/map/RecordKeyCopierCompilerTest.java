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

package com.questdb.ql.map;

import com.questdb.parser.sql.AbstractOptimiserTest;
import com.questdb.ql.RecordSource;
import com.questdb.std.BytecodeAssembler;
import com.questdb.std.IntList;
import com.questdb.store.*;
import org.junit.Assert;
import org.junit.Test;

public class RecordKeyCopierCompilerTest extends AbstractOptimiserTest {
    @Test
    public void testCompiler() throws Exception {
        try (JournalWriter w = compiler.createWriter(FACTORY_CONTAINER.getFactory(), "create table x (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING, y BOOLEAN) timestamp(t) partition by MONTH record hint 100")) {
            JournalEntryWriter ew = w.entryWriter();
            IntList keyColumns = new IntList();
            ew.putInt(0, 12345);
            keyColumns.add(0);

            ew.put(1, (byte) 128);
            keyColumns.add(1);

            ew.putShort(2, (short) 6500);
            keyColumns.add(2);

            ew.putLong(3, 123456789);
            keyColumns.add(3);

            ew.putFloat(4, 0.345f);
            keyColumns.add(4);

            ew.putDouble(5, 0.123456789);
            keyColumns.add(5);

            ew.putDate(6, 10000000000L);
            keyColumns.add(6);

            ew.putSym(9, "xyz");
            keyColumns.add(9);

            ew.putStr(10, "abc");
            keyColumns.add(10);

            ew.putBool(11, true);
            keyColumns.add(11);

            ew.append();
            w.commit();


            try (RecordSource src = compileSource("x")) {
                RecordKeyCopierCompiler cc = new RecordKeyCopierCompiler(new BytecodeAssembler());
                RecordKeyCopier copier = cc.compile(src.getMetadata(), keyColumns);

                IntList valueTypes = new IntList();
                valueTypes.add(ColumnType.DOUBLE);

                MetadataTypeResolver metadataTypeResolver = new MetadataTypeResolver();
                TypeListResolver typeListResolver = new TypeListResolver();

                try (DirectMap map = new DirectMap(1024, metadataTypeResolver.of(src.getMetadata(), keyColumns), typeListResolver.of(valueTypes))) {

                    RecordCursor cursor = src.prepareCursor(FACTORY_CONTAINER.getFactory());
                    try {
                        while (cursor.hasNext()) {
                            Record r = cursor.next();
                            DirectMap.KeyWriter kw = map.keyWriter();
                            copier.copy(r, kw);
                            DirectMapValues val = map.getOrCreateValues();
                            val.putDouble(0, 5000.01);
                        }

                        cursor.toTop();
                        while (cursor.hasNext()) {
                            Record r = cursor.next();
                            DirectMap.KeyWriter kw = map.keyWriter();
                            copier.copy(r, kw);
                            Assert.assertEquals(map.getValues().getDouble(0), 5000.01, 0.00000001);
                        }
                    } finally {
                        cursor.releaseCursor();
                    }
                }
            }
        }
    }
}