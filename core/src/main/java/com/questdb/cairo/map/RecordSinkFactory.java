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

package com.questdb.cairo.map;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.ColumnType;
import com.questdb.std.BytecodeAssembler;
import com.questdb.std.IntList;
import com.questdb.std.Transient;

public final class RecordSinkFactory {

    public static RecordSink getInstance(
            @Transient BytecodeAssembler asm,
            @Transient RecordMetadata meta,
            @Transient IntList columns,
            boolean symbolAsString) {

        asm.init(RecordSink.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(RecordSink.class);

        final int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        final int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        final int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        final int rGetTimestamp = asm.poolInterfaceMethod(Record.class, "getTimestamp", "(I)J");
        final int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        final int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        final int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        final int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        final int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        final int rGetStr = asm.poolInterfaceMethod(Record.class, "getStr", "(I)Ljava/lang/CharSequence;");
        final int rGetSym = asm.poolInterfaceMethod(Record.class, "getSym", "(I)Ljava/lang/CharSequence;");
        final int rGetBin = asm.poolInterfaceMethod(Record.class, "getBin", "(I)Lcom/questdb/std/BinarySequence;");
        //
        final int wPutByte = asm.poolMethod(QMap.Key.class, "putByte", "(B)V");
        final int wPutShort = asm.poolMethod(QMap.Key.class, "putShort", "(S)V");
        final int wPutInt = asm.poolMethod(QMap.Key.class, "putInt", "(I)V");
        final int wPutLong = asm.poolMethod(QMap.Key.class, "putLong", "(J)V");
        final int wPutBool = asm.poolMethod(QMap.Key.class, "putBool", "(Z)V");
        final int wPutFloat = asm.poolMethod(QMap.Key.class, "putFloat", "(F)V");
        final int wPutDouble = asm.poolMethod(QMap.Key.class, "putDouble", "(D)V");
        final int wPutStr = asm.poolMethod(QMap.Key.class, "putStr", "(Ljava/lang/CharSequence;)V");
        final int wPutBin = asm.poolMethod(QMap.Key.class, "putBin", "(Lcom/questdb/std/BinarySequence;)V");
        //
        final int copyNameIndex = asm.poolUtf8("copy");
        final int copySigIndex = asm.poolUtf8("(Lcom/questdb/cairo/sql/Record;Lcom/questdb/cairo/map/QMap$Key;)V");

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(0);
        asm.methodCount(2);
        asm.defineDefaultConstructor();

        asm.startMethod(copyNameIndex, copySigIndex, 4, 3);

        final int n = columns.size();
        for (int i = 0; i < n; i++) {

            int index = columns.getQuick(i);
            asm.aload(2);
            asm.aload(1);
            asm.iconst(index);

            switch (meta.getColumnType(index)) {
                case ColumnType.INT:
                    asm.invokeInterface(rGetInt, 1);
                    asm.invokeVirtual(wPutInt);
                    break;
                case ColumnType.SYMBOL:
                    if (symbolAsString) {
                        asm.invokeInterface(rGetSym, 1);
                        asm.invokeVirtual(wPutStr);
                    } else {
                        asm.invokeInterface(rGetInt, 1);
                        asm.i2l();
                        asm.invokeVirtual(wPutLong);
                    }
                    break;
                case ColumnType.LONG:
                    asm.invokeInterface(rGetLong, 1);
                    asm.invokeVirtual(wPutLong);
                    break;
                case ColumnType.DATE:
                    asm.invokeInterface(rGetDate, 1);
                    asm.invokeVirtual(wPutLong);
                    break;
                case ColumnType.TIMESTAMP:
                    asm.invokeInterface(rGetTimestamp, 1);
                    asm.invokeVirtual(wPutLong);
                    break;
                case ColumnType.BYTE:
                    asm.invokeInterface(rGetByte, 1);
                    asm.invokeVirtual(wPutByte);
                    break;
                case ColumnType.SHORT:
                    asm.invokeInterface(rGetShort, 1);
                    asm.invokeVirtual(wPutShort);
                    break;
                case ColumnType.BOOLEAN:
                    asm.invokeInterface(rGetBool, 1);
                    asm.invokeVirtual(wPutBool);
                    break;
                case ColumnType.FLOAT:
                    asm.invokeInterface(rGetFloat, 1);
                    asm.invokeVirtual(wPutFloat);
                    break;
                case ColumnType.DOUBLE:
                    asm.invokeInterface(rGetDouble, 1);
                    asm.invokeVirtual(wPutDouble);
                    break;
                case ColumnType.STRING:
                    asm.invokeInterface(rGetStr, 1);
                    asm.invokeVirtual(wPutStr);
                    break;
                default:
                    // binary
                    asm.invokeInterface(rGetBin, 1);
                    asm.invokeVirtual(wPutBin);
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
