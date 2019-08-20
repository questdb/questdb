/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cairo.map;

import com.questdb.cairo.ColumnFilter;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.RecordSink;
import com.questdb.cairo.sql.Record;
import com.questdb.std.BytecodeAssembler;
import com.questdb.std.Transient;

public class RecordValueSinkFactory {

    public static RecordValueSink getInstance(BytecodeAssembler asm, ColumnTypes columnTypes, @Transient ColumnFilter columnFilter) {
        asm.init(RecordSink.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(RecordValueSink.class);

        int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        int rGetTimestamp = asm.poolInterfaceMethod(Record.class, "getTimestamp", "(I)J");
        int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        int rGetChar = asm.poolInterfaceMethod(Record.class, "getChar", "(I)C");
        int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        //
        int wPutInt = asm.poolInterfaceMethod(MapValue.class, "putInt", "(II)V");
        int wPutLong = asm.poolInterfaceMethod(MapValue.class, "putLong", "(IJ)V");
        int wPutByte = asm.poolInterfaceMethod(MapValue.class, "putByte", "(IB)V");
        int wPutShort = asm.poolInterfaceMethod(MapValue.class, "putShort", "(IS)V");
        int wPutChar = asm.poolInterfaceMethod(MapValue.class, "putChar", "(IC)V");
        int wPutBool = asm.poolInterfaceMethod(MapValue.class, "putBool", "(IZ)V");
        int wPutFloat = asm.poolInterfaceMethod(MapValue.class, "putFloat", "(IF)V");
        int wPutDouble = asm.poolInterfaceMethod(MapValue.class, "putDouble", "(ID)V");
        int wPutDate = asm.poolInterfaceMethod(MapValue.class, "putDate", "(IJ)V");
        int wPutTimestamp = asm.poolInterfaceMethod(MapValue.class, "putTimestamp", "(IJ)V");

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lcom/questdb/cairo/sql/Record;Lcom/questdb/cairo/map/MapValue;)V");

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(0);
        asm.methodCount(2);
        asm.defineDefaultConstructor();

        asm.startMethod(copyNameIndex, copySigIndex, 4, 3);

        int n = columnFilter.getColumnCount();
        for (int i = 0; i < n; i++) {

            int index = columnFilter.getColumnIndex(i);
            asm.aload(2);
            asm.iconst(i);
            asm.aload(1);
            asm.iconst(index);

            switch (columnTypes.getColumnType(index)) {
                case ColumnType.INT:
                case ColumnType.SYMBOL:
                    asm.invokeInterface(rGetInt, 1);
                    asm.invokeInterface(wPutInt, 2);
                    break;
                case ColumnType.LONG:
                    asm.invokeInterface(rGetLong, 1);
                    asm.invokeInterface(wPutLong, 3);
                    break;
                case ColumnType.DATE:
                    asm.invokeInterface(rGetDate, 1);
                    asm.invokeInterface(wPutDate, 3);
                    break;
                case ColumnType.TIMESTAMP:
                    asm.invokeInterface(rGetTimestamp, 1);
                    asm.invokeInterface(wPutTimestamp, 3);
                    break;
                case ColumnType.BYTE:
                    asm.invokeInterface(rGetByte, 1);
                    asm.invokeInterface(wPutByte, 2);
                    break;
                case ColumnType.SHORT:
                    asm.invokeInterface(rGetShort, 1);
                    asm.invokeInterface(wPutShort, 2);
                    break;
                case ColumnType.CHAR:
                    asm.invokeInterface(rGetChar, 1);
                    asm.invokeInterface(wPutChar, 2);
                    break;
                case ColumnType.BOOLEAN:
                    asm.invokeInterface(rGetBool, 1);
                    asm.invokeInterface(wPutBool, 2);
                    break;
                case ColumnType.FLOAT:
                    asm.invokeInterface(rGetFloat, 1);
                    asm.invokeInterface(wPutFloat, 2);
                    break;
                case ColumnType.DOUBLE:
                    asm.invokeInterface(rGetDouble, 1);
                    asm.invokeInterface(wPutDouble, 3);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        asm.return_();
        asm.endMethodCode();

        // exceptions
        asm.putShort(0);

        // we have to add stack map table as branch target
        // jvm requires it

        // attributes: 0 (void, no stack verification)
        asm.putShort(0);

        asm.endMethod();

        // class attribute count
        asm.putShort(0);

        return asm.newInstance();
    }
}
