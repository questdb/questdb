/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

public class RecordSinkFactory {

    public static RecordSink getInstance(BytecodeAssembler asm, ColumnTypes columnTypes, @Transient ColumnFilter columnFilter, boolean symAsString) {
        return getInstance(asm, columnTypes, columnFilter, symAsString, null);
    }

    public static RecordSink getInstance(
            BytecodeAssembler asm,
            ColumnTypes columnTypes,
            @Transient ColumnFilter columnFilter,
            boolean symAsString,
            @Transient @Nullable IntList skewIndex
    ) {
        asm.init(RecordSink.class);
        asm.setupPool();
        final int thisClassIndex = asm.poolClass(asm.poolUtf8("io/questdb/cairo/sink"));
        final int interfaceClassIndex = asm.poolClass(RecordSink.class);

        final int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        final int rGetIPv4 = asm.poolInterfaceMethod(Record.class, "getIPv4", "(I)I");
        final int rGetGeoInt = asm.poolInterfaceMethod(Record.class, "getGeoInt", "(I)I");
        final int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        final int rGetGeoLong = asm.poolInterfaceMethod(Record.class, "getGeoLong", "(I)J");
        final int rGetLong256 = asm.poolInterfaceMethod(Record.class, "getLong256A", "(I)Lio/questdb/std/Long256;");
        final int rGetLong128Lo = asm.poolInterfaceMethod(Record.class, "getLong128Lo", "(I)J");
        final int rGetLong128Hi = asm.poolInterfaceMethod(Record.class, "getLong128Hi", "(I)J");
        final int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        final int rGetTimestamp = asm.poolInterfaceMethod(Record.class, "getTimestamp", "(I)J");
        final int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        final int rGetGeoByte = asm.poolInterfaceMethod(Record.class, "getGeoByte", "(I)B");
        final int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        final int rGetGeoShort = asm.poolInterfaceMethod(Record.class, "getGeoShort", "(I)S");
        final int rGetChar = asm.poolInterfaceMethod(Record.class, "getChar", "(I)C");
        final int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        final int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        final int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        final int rGetStr = asm.poolInterfaceMethod(Record.class, "getStr", "(I)Ljava/lang/CharSequence;");
        final int rGetSym = asm.poolInterfaceMethod(Record.class, "getSym", "(I)Ljava/lang/CharSequence;");
        final int rGetBin = asm.poolInterfaceMethod(Record.class, "getBin", "(I)Lio/questdb/std/BinarySequence;");
        final int rGetRecord = asm.poolInterfaceMethod(Record.class, "getRecord", "(I)Lio/questdb/cairo/sql/Record;");

        //
        final int wPutInt = asm.poolInterfaceMethod(RecordSinkSPI.class, "putInt", "(I)V");
        final int wSkip = asm.poolInterfaceMethod(RecordSinkSPI.class, "skip", "(I)V");
        final int wPutLong = asm.poolInterfaceMethod(RecordSinkSPI.class, "putLong", "(J)V");
        final int wPutLong256 = asm.poolInterfaceMethod(RecordSinkSPI.class, "putLong256", "(Lio/questdb/std/Long256;)V");
        final int wPutLong128 = asm.poolInterfaceMethod(RecordSinkSPI.class, "putLong128", "(JJ)V");
        final int wPutByte = asm.poolInterfaceMethod(RecordSinkSPI.class, "putByte", "(B)V");
        final int wPutShort = asm.poolInterfaceMethod(RecordSinkSPI.class, "putShort", "(S)V");
        final int wPutChar = asm.poolInterfaceMethod(RecordSinkSPI.class, "putChar", "(C)V");
        final int wPutBool = asm.poolInterfaceMethod(RecordSinkSPI.class, "putBool", "(Z)V");
        final int wPutFloat = asm.poolInterfaceMethod(RecordSinkSPI.class, "putFloat", "(F)V");
        final int wPutDouble = asm.poolInterfaceMethod(RecordSinkSPI.class, "putDouble", "(D)V");
        final int wPutStr = asm.poolInterfaceMethod(RecordSinkSPI.class, "putStr", "(Ljava/lang/CharSequence;)V");
        final int wPutDate = asm.poolInterfaceMethod(RecordSinkSPI.class, "putDate", "(J)V");
        final int wPutTimestamp = asm.poolInterfaceMethod(RecordSinkSPI.class, "putTimestamp", "(J)V");
        final int wPutBin = asm.poolInterfaceMethod(RecordSinkSPI.class, "putBin", "(Lio/questdb/std/BinarySequence;)V");
        final int wPutRecord = asm.poolInterfaceMethod(RecordSinkSPI.class, "putRecord", "(Lio/questdb/cairo/sql/Record;)V");

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lio/questdb/cairo/sql/Record;Lio/questdb/cairo/RecordSinkSPI;)V");

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(0);
        asm.methodCount(2);
        asm.defineDefaultConstructor();

        asm.startMethod(copyNameIndex, copySigIndex, 7, 5);

        int n = columnFilter.getColumnCount();
        for (int i = 0; i < n; i++) {
            int index = columnFilter.getColumnIndex(i);
            final int factor = columnFilter.getIndexFactor(index);
            index = (index * factor - 1);
            final int type = columnTypes.getColumnType(index);

            if (factor < 0) {
                int size = ColumnType.sizeOf(type);

                // skip n-bytes
                if (size > 0) {
                    asm.aload(2);
                    asm.iconst(size);
                    asm.invokeInterface(wSkip, 1);
                    continue;
                }
            }

            switch (factor * ColumnType.tagOf(type)) {
                case ColumnType.INT:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetInt, 1);
                    asm.invokeInterface(wPutInt, 1);
                    break;
                case ColumnType.IPv4:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetIPv4, 1);
                    asm.invokeInterface(wPutInt, 1);
                    break;
                case ColumnType.SYMBOL:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    if (symAsString) {
                        asm.invokeInterface(rGetSym, 1);
                        asm.invokeInterface(wPutStr, 1);
                    } else {
                        asm.invokeInterface(rGetInt, 1);
                        asm.invokeInterface(wPutInt, 1);
                    }
                    break;
                case ColumnType.LONG:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetLong, 1);
                    asm.invokeInterface(wPutLong, 2);
                    break;
                case ColumnType.DATE:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetDate, 1);
                    asm.invokeInterface(wPutDate, 2);
                    break;
                case ColumnType.TIMESTAMP:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetTimestamp, 1);
                    asm.invokeInterface(wPutTimestamp, 2);
                    break;
                case ColumnType.BYTE:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetByte, 1);
                    asm.invokeInterface(wPutByte, 1);
                    break;
                case ColumnType.SHORT:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetShort, 1);
                    asm.invokeInterface(wPutShort, 1);
                    break;
                case ColumnType.CHAR:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetChar, 1);
                    asm.invokeInterface(wPutChar, 1);
                    break;
                case ColumnType.BOOLEAN:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetBool, 1);
                    asm.invokeInterface(wPutBool, 1);
                    break;
                case ColumnType.FLOAT:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetFloat, 1);
                    asm.invokeInterface(wPutFloat, 1);
                    break;
                case ColumnType.DOUBLE:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetDouble, 1);
                    asm.invokeInterface(wPutDouble, 2);
                    break;
                case ColumnType.STRING:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetStr, 1);
                    asm.invokeInterface(wPutStr, 1);
                    break;
                case ColumnType.BINARY:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetBin, 1);
                    asm.invokeInterface(wPutBin, 1);
                    break;
                case ColumnType.LONG256:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetLong256, 1);
                    asm.invokeInterface(wPutLong256, 1);
                    break;
                case ColumnType.RECORD:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetRecord, 1);
                    asm.invokeInterface(wPutRecord, 1);
                    break;
                case ColumnType.GEOBYTE:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetGeoByte, 1);
                    asm.invokeInterface(wPutByte, 1);
                    break;
                case ColumnType.GEOSHORT:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetGeoShort, 1);
                    asm.invokeInterface(wPutShort, 1);
                    break;
                case ColumnType.GEOINT:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetGeoInt, 1);
                    asm.invokeInterface(wPutInt, 1);
                    break;
                case ColumnType.GEOLONG:
                    asm.aload(2);
                    asm.aload(1);
                    asm.iconst(getSkewedIndex(index, skewIndex));
                    asm.invokeInterface(rGetGeoLong, 1);
                    asm.invokeInterface(wPutLong, 2);
                    break;
                case ColumnType.LONG128:
                    // fall though
                case ColumnType.UUID:
                    int skewedIndex = getSkewedIndex(index, skewIndex);
                    asm.aload(2);

                    asm.aload(1);
                    asm.iconst(skewedIndex);
                    asm.invokeInterface(rGetLong128Lo, 1);

                    asm.aload(1);
                    asm.iconst(skewedIndex);
                    asm.invokeInterface(rGetLong128Hi, 1);

                    asm.invokeInterface(wPutLong128, 4);
                    break;
                default:
                    break;
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

    private static int getSkewedIndex(int src, @Transient @Nullable IntList skewIndex) {
        if (skewIndex == null) {
            return src;
        }
        return skewIndex.getQuick(src);
    }
}
