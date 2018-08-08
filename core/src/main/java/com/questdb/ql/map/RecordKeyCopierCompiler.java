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

import com.questdb.std.BytecodeAssembler;
import com.questdb.std.IntList;
import com.questdb.std.Transient;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.RecordMetadata;

public class RecordKeyCopierCompiler {
    private final BytecodeAssembler asm;

    public RecordKeyCopierCompiler(BytecodeAssembler asm) {
        this.asm = asm;
    }

    public RecordKeyCopier compile(RecordMetadata meta, @Transient IntList columns) {
        return compile(meta, columns, false);
    }

    public RecordKeyCopier compile(RecordMetadata meta, @Transient IntList columns, boolean symAsString) {
        asm.init(RecordKeyCopier.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(RecordKeyCopier.class);

        int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        int rGetStr = asm.poolInterfaceMethod(Record.class, "getFlyweightStr", "(I)Ljava/lang/CharSequence;");
        int rGetSym = asm.poolInterfaceMethod(Record.class, "getSym", "(I)Ljava/lang/CharSequence;");

        //
        int wPutInt = asm.poolMethod(DirectMap.KeyWriter.class, "putInt", "(I)V");
        int wPutLong = asm.poolMethod(DirectMap.KeyWriter.class, "putLong", "(J)V");
        //
        int wPutByte = asm.poolMethod(DirectMap.KeyWriter.class, "putByte", "(B)V");
        int wPutShort = asm.poolMethod(DirectMap.KeyWriter.class, "putShort", "(S)V");
        int wPutBool = asm.poolMethod(DirectMap.KeyWriter.class, "putBool", "(Z)V");
        int wPutFloat = asm.poolMethod(DirectMap.KeyWriter.class, "putFloat", "(F)V");
        int wPutDouble = asm.poolMethod(DirectMap.KeyWriter.class, "putDouble", "(D)V");
        int wPutStr = asm.poolMethod(DirectMap.KeyWriter.class, "putStr", "(Ljava/lang/CharSequence;)V");

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lcom/questdb/store/Record;Lcom/questdb/ql/map/DirectMap$KeyWriter;)V");

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(0);
        asm.methodCount(2);
        asm.defineDefaultConstructor();

        asm.startMethod(copyNameIndex, copySigIndex, 4, 3);

        int n = columns.size();
        for (int i = 0; i < n; i++) {

            int index = columns.getQuick(i);
            asm.aload(2);
            asm.aload(1);
            asm.iconst(index);

            switch (meta.getColumnQuick(index).getType()) {
                case ColumnType.INT:
                    asm.invokeInterface(rGetInt, 1);
                    asm.invokeVirtual(wPutInt);
                    break;
                case ColumnType.SYMBOL:
                    if (symAsString) {
                        asm.invokeInterface(rGetSym, 1);
                        asm.invokeVirtual(wPutStr);
                    } else {
                        asm.invokeInterface(rGetInt, 1);
                        asm.invokeVirtual(wPutInt);
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
