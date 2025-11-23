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

package io.questdb.cairo.map;

import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Transient;

public class RecordValueSinkFactory {

    public static RecordValueSink getInstance(BytecodeAssembler asm, ColumnTypes columnTypes, @Transient ColumnFilter columnFilter) {
        asm.init(RecordSink.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("io/questdb/cairo/valuesink"));
        int interfaceClassIndex = asm.poolClass(RecordValueSink.class);

        int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        int rGetIPv4 = asm.poolInterfaceMethod(Record.class, "getIPv4", "(I)I");
        int rGetGeoInt = asm.poolInterfaceMethod(Record.class, "getGeoInt", "(I)I");
        int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        int rGetGeoLong = asm.poolInterfaceMethod(Record.class, "getGeoLong", "(I)J");
        int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        int rGetTimestamp = asm.poolInterfaceMethod(Record.class, "getTimestamp", "(I)J");
        int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        int rGetGeoByte = asm.poolInterfaceMethod(Record.class, "getGeoByte", "(I)B");
        int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        int rGetGeoShort = asm.poolInterfaceMethod(Record.class, "getGeoShort", "(I)S");
        int rGetChar = asm.poolInterfaceMethod(Record.class, "getChar", "(I)C");
        int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        int rGetDecimal8 = asm.poolInterfaceMethod(Record.class, "getDecimal8", "(I)B");
        int rGetDecimal16 = asm.poolInterfaceMethod(Record.class, "getDecimal16", "(I)S");
        int rGetDecimal32 = asm.poolInterfaceMethod(Record.class, "getDecimal32", "(I)I");
        int rGetDecimal64 = asm.poolInterfaceMethod(Record.class, "getDecimal64", "(I)J");
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
        int wPutDecimal128 = asm.poolInterfaceMethod(MapValue.class, "putDecimal128", "(ILio/questdb/cairo/sql/Record;I)V");
        int wPutDecimal256 = asm.poolInterfaceMethod(MapValue.class, "putDecimal256", "(ILio/questdb/cairo/sql/Record;I)V");

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lio/questdb/cairo/sql/Record;Lio/questdb/cairo/map/MapValue;)V");

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

            int index = columnFilter.getColumnIndexFactored(i);
            // stack: []
            asm.aload(2);
            // stack: [MapValue]
            asm.iconst(i);
            // stack: [MapValue, index]
            asm.aload(1);
            // stack: [MapValue, index, Record]
            asm.iconst(index);
            // stack: [MapValue, index, Record, columnIndex]

            int columnType = columnTypes.getColumnType(index);
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.INT:
                case ColumnType.SYMBOL:
                    asm.invokeInterface(rGetInt, 1);
                    asm.invokeInterface(wPutInt, 2);
                    break;
                case ColumnType.IPv4:
                    asm.invokeInterface(rGetIPv4, 1);
                    asm.invokeInterface(wPutInt, 2);
                    break;
                case ColumnType.GEOINT:
                    asm.invokeInterface(rGetGeoInt, 1);
                    asm.invokeInterface(wPutInt, 2);
                    break;
                case ColumnType.LONG:
                    asm.invokeInterface(rGetLong, 1);
                    asm.invokeInterface(wPutLong, 3);
                    break;
                case ColumnType.GEOLONG:
                    asm.invokeInterface(rGetGeoLong, 1);
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
                case ColumnType.GEOBYTE:
                    asm.invokeInterface(rGetGeoByte, 1);
                    asm.invokeInterface(wPutByte, 2);
                    break;
                case ColumnType.SHORT:
                    asm.invokeInterface(rGetShort, 1);
                    asm.invokeInterface(wPutShort, 2);
                    break;
                case ColumnType.GEOSHORT:
                    asm.invokeInterface(rGetGeoShort, 1);
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
                case ColumnType.DECIMAL8:
                    // stack: [MapValue, index, Record, columnIndex]
                    asm.invokeInterface(rGetDecimal8, 1);
                    // stack: [MapValue, index, byte]
                    asm.invokeInterface(wPutByte, 2);
                    // stack: []
                    break;
                case ColumnType.DECIMAL16:
                    // stack: [MapValue, index, Record, columnIndex]
                    asm.invokeInterface(rGetDecimal16, 1);
                    // stack: [MapValue, index, short]
                    asm.invokeInterface(wPutShort, 2);
                    // stack: []
                    break;
                case ColumnType.DECIMAL32:
                    // stack: [MapValue, index, Record, columnIndex]
                    asm.invokeInterface(rGetDecimal32, 1);
                    // stack: [MapValue, index, int]
                    asm.invokeInterface(wPutInt, 2);
                    // stack: []
                    break;
                case ColumnType.DECIMAL64:
                    // stack: [MapValue, index, Record, columnIndex]
                    asm.invokeInterface(rGetDecimal64, 1);
                    // stack: [MapValue, index, long]
                    asm.invokeInterface(wPutLong, 3);
                    // stack: []
                    break;
                case ColumnType.DECIMAL128:
                    // stack: [MapValue, index, Record, columnIndex]
                    asm.invokeInterface(wPutDecimal128, 3);
                    // stack: []
                    break;
                case ColumnType.DECIMAL256:
                    // stack: [MapValue, index, Record, columnIndex]
                    asm.invokeInterface(wPutDecimal256, 3);
                    // stack: []
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
