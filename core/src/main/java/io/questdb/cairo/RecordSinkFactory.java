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

import io.questdb.cairo.sql.Record;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Transient;

public class RecordSinkFactory {

    public static RecordSink getInstance(BytecodeAssembler asm, ColumnTypes columnTypes, @Transient ColumnFilter columnFilter, boolean symAsString) {
        asm.init(RecordSink.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("io/questdb/cairo/sink"));
        int interfaceClassIndex = asm.poolClass(RecordSink.class);

        int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        int rGetLong256 = asm.poolInterfaceMethod(Record.class, "getLong256A", "(I)Lio/questdb/std/Long256;");
        int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        int rGetTimestamp = asm.poolInterfaceMethod(Record.class, "getTimestamp", "(I)J");
        int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        int rGetChar = asm.poolInterfaceMethod(Record.class, "getChar", "(I)C");
        int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        int rGetStr = asm.poolInterfaceMethod(Record.class, "getStr", "(I)Ljava/lang/CharSequence;");
        int rGetSym = asm.poolInterfaceMethod(Record.class, "getSym", "(I)Ljava/lang/CharSequence;");
        final int rGetBin = asm.poolInterfaceMethod(Record.class, "getBin", "(I)Lio/questdb/std/BinarySequence;");
        //
        int wPutInt = asm.poolInterfaceMethod(RecordSinkSPI.class, "putInt", "(I)V");
        int wPutLong = asm.poolInterfaceMethod(RecordSinkSPI.class, "putLong", "(J)V");
        int wPutLong256 = asm.poolInterfaceMethod(RecordSinkSPI.class, "putLong256", "(Lio/questdb/std/Long256;)V");
        int wPutByte = asm.poolInterfaceMethod(RecordSinkSPI.class, "putByte", "(B)V");
        int wPutShort = asm.poolInterfaceMethod(RecordSinkSPI.class, "putShort", "(S)V");
        int wPutChar = asm.poolInterfaceMethod(RecordSinkSPI.class, "putChar", "(C)V");
        int wPutBool = asm.poolInterfaceMethod(RecordSinkSPI.class, "putBool", "(Z)V");
        int wPutFloat = asm.poolInterfaceMethod(RecordSinkSPI.class, "putFloat", "(F)V");
        int wPutDouble = asm.poolInterfaceMethod(RecordSinkSPI.class, "putDouble", "(D)V");
        int wPutStr = asm.poolInterfaceMethod(RecordSinkSPI.class, "putStr", "(Ljava/lang/CharSequence;)V");
        int wPutDate = asm.poolInterfaceMethod(RecordSinkSPI.class, "putDate", "(J)V");
        int wPutTimestamp = asm.poolInterfaceMethod(RecordSinkSPI.class, "putTimestamp", "(J)V");
        final int wPutBin = asm.poolInterfaceMethod(RecordSinkSPI.class, "putBin", "(Lio/questdb/std/BinarySequence;)V");

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lio/questdb/cairo/sql/Record;Lio/questdb/cairo/RecordSinkSPI;)V");

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
            asm.aload(1);
            asm.iconst(index);

            switch (columnTypes.getColumnType(index)) {
                case ColumnType.INT:
                    asm.invokeInterface(rGetInt, 1);
                    asm.invokeInterface(wPutInt, 1);
                    break;
                case ColumnType.SYMBOL:
                    if (symAsString) {
                        asm.invokeInterface(rGetSym, 1);
                        asm.invokeInterface(wPutStr, 1);
                    } else {
                        asm.invokeInterface(rGetInt, 1);
                        asm.invokeInterface(wPutInt, 1);
                    }
                    break;
                case ColumnType.LONG:
                    asm.invokeInterface(rGetLong, 1);
                    asm.invokeInterface(wPutLong, 2);
                    break;
                case ColumnType.DATE:
                    asm.invokeInterface(rGetDate, 1);
                    asm.invokeInterface(wPutDate, 2);
                    break;
                case ColumnType.TIMESTAMP:
                    asm.invokeInterface(rGetTimestamp, 1);
                    asm.invokeInterface(wPutTimestamp, 2);
                    break;
                case ColumnType.BYTE:
                    asm.invokeInterface(rGetByte, 1);
                    asm.invokeInterface(wPutByte, 1);
                    break;
                case ColumnType.SHORT:
                    asm.invokeInterface(rGetShort, 1);
                    asm.invokeInterface(wPutShort, 1);
                    break;
                case ColumnType.CHAR:
                    asm.invokeInterface(rGetChar, 1);
                    asm.invokeInterface(wPutChar, 1);
                    break;
                case ColumnType.BOOLEAN:
                    asm.invokeInterface(rGetBool, 1);
                    asm.invokeInterface(wPutBool, 1);
                    break;
                case ColumnType.FLOAT:
                    asm.invokeInterface(rGetFloat, 1);
                    asm.invokeInterface(wPutFloat, 1);
                    break;
                case ColumnType.DOUBLE:
                    asm.invokeInterface(rGetDouble, 1);
                    asm.invokeInterface(wPutDouble, 2);
                    break;
                case ColumnType.STRING:
                    asm.invokeInterface(rGetStr, 1);
                    asm.invokeInterface(wPutStr, 1);
                    break;
                case ColumnType.BINARY:
                    asm.invokeInterface(rGetBin, 1);
                    asm.invokeInterface(wPutBin, 1);
                    break;
                case ColumnType.LONG256:
                    asm.invokeInterface(rGetLong256, 1);
                    asm.invokeInterface(wPutLong256, 1);
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
}
