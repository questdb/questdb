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

package com.questdb.parser.sql;

import com.questdb.std.BytecodeAssembler;
import com.questdb.store.ColumnType;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.Record;
import com.questdb.store.RecordMetadata;

public class CopyHelperCompiler {
    private final BytecodeAssembler asm;

    public CopyHelperCompiler(BytecodeAssembler asm) {
        this.asm = asm;
    }

    public CopyHelper compile(RecordMetadata from, RecordMetadata to) {
        int tsIndex = to.getTimestampIndex();
        asm.init(CopyHelper.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(CopyHelper.class);

        int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        //
        int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        int rGetSym = asm.poolInterfaceMethod(Record.class, "getSym", "(I)Ljava/lang/CharSequence;");
        int rGetStr = asm.poolInterfaceMethod(Record.class, "getFlyweightStr", "(I)Ljava/lang/CharSequence;");
        int rGetBin = asm.poolInterfaceMethod(Record.class, "getBin", "(I)Lcom/questdb/std/DirectInputStream;");
        //
        int wPutInt = asm.poolInterfaceMethod(JournalEntryWriter.class, "putInt", "(II)V");
        int wPutLong = asm.poolInterfaceMethod(JournalEntryWriter.class, "putLong", "(IJ)V");
        int wPutDate = asm.poolInterfaceMethod(JournalEntryWriter.class, "putDate", "(IJ)V");
        //
        int wPutByte = asm.poolInterfaceMethod(JournalEntryWriter.class, "put", "(IB)V");
        int wPutShort = asm.poolInterfaceMethod(JournalEntryWriter.class, "putShort", "(IS)V");
        int wPutBool = asm.poolInterfaceMethod(JournalEntryWriter.class, "putBool", "(IZ)V");
        int wPutFloat = asm.poolInterfaceMethod(JournalEntryWriter.class, "putFloat", "(IF)V");
        int wPutDouble = asm.poolInterfaceMethod(JournalEntryWriter.class, "putDouble", "(ID)V");
        int wPutSym = asm.poolInterfaceMethod(JournalEntryWriter.class, "putSym", "(ILjava/lang/CharSequence;)V");
        int wPutStr = asm.poolInterfaceMethod(JournalEntryWriter.class, "putStr", "(ILjava/lang/CharSequence;)V");
        int wPutBin = asm.poolInterfaceMethod(JournalEntryWriter.class, "putBin", "(ILjava/io/InputStream;)V");

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lcom/questdb/store/Record;Lcom/questdb/store/JournalEntryWriter;)V");

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(0);
        asm.methodCount(2);
        asm.defineDefaultConstructor();

        asm.startMethod(copyNameIndex, copySigIndex, 4, 3);

        int n = from.getColumnCount();
        for (int i = 0; i < n; i++) {

            // do not copy timestamp, it will be copied externally to this helper

            if (i == tsIndex) {
                continue;
            }

            asm.aload(2);
            asm.iconst(i);
            asm.aload(1);
            asm.iconst(i);

            switch (from.getColumnQuick(i).getType()) {
                case ColumnType.INT:
                    asm.invokeInterface(rGetInt, 1);
                    switch (to.getColumnQuick(i).getType()) {
                        case ColumnType.LONG:
                            asm.i2l();
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.i2l();
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.SHORT:
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.FLOAT:
                            asm.i2f();
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.i2d();
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        default:
                            asm.invokeInterface(wPutInt, 2);
                            break;
                    }
                    break;
                case ColumnType.LONG:
                    asm.invokeInterface(rGetLong, 1);
                    switch (to.getColumnQuick(i).getType()) {
                        case ColumnType.INT:
                            asm.l2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.DATE:
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.SHORT:
                            asm.l2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            asm.l2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.FLOAT:
                            asm.l2f();
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.l2d();
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        default:
                            asm.invokeInterface(wPutLong, 3);
                            break;
                    }
                    break;
                case ColumnType.DATE:
                    asm.invokeInterface(rGetDate, 1);
                    switch (to.getColumnQuick(i).getType()) {
                        case ColumnType.INT:
                            asm.l2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.SHORT:
                            asm.l2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            asm.l2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.FLOAT:
                            asm.l2f();
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.l2d();
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        default:
                            asm.invokeInterface(wPutDate, 3);
                            break;
                    }
                    break;
                case ColumnType.BYTE:
                    asm.invokeInterface(rGetByte, 1);
                    switch (to.getColumnQuick(i).getType()) {
                        case ColumnType.INT:
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.i2l();
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.i2l();
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.SHORT:
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.FLOAT:
                            asm.i2f();
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.i2d();
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        default:
                            asm.invokeInterface(wPutByte, 2);
                            break;
                    }
                    break;
                case ColumnType.SHORT:
                    asm.invokeInterface(rGetShort, 1);
                    switch (to.getColumnQuick(i).getType()) {
                        case ColumnType.INT:
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.i2l();
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.i2l();
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.BYTE:
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.FLOAT:
                            asm.i2f();
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.i2d();
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        default:
                            asm.invokeInterface(wPutShort, 2);
                            break;
                    }
                    break;
                case ColumnType.BOOLEAN:
                    asm.invokeInterface(rGetBool, 1);
                    asm.invokeInterface(wPutBool, 2);
                    break;
                case ColumnType.FLOAT:
                    asm.invokeInterface(rGetFloat, 1);
                    switch (to.getColumnQuick(i).getType()) {
                        case ColumnType.INT:
                            asm.f2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.f2l();
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.f2l();
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.SHORT:
                            asm.f2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            asm.f2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.f2d();
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        default:
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                    }
                    break;
                case ColumnType.DOUBLE:
                    asm.invokeInterface(rGetDouble, 1);
                    switch (to.getColumnQuick(i).getType()) {
                        case ColumnType.INT:
                            asm.d2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.d2l();
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.d2l();
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.SHORT:
                            asm.d2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            asm.d2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.FLOAT:
                            asm.d2f();
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        default:
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                    }
                    break;
                case ColumnType.SYMBOL:
                    asm.invokeInterface(rGetSym, 1);
                    switch (to.getColumnQuick(i).getType()) {
                        case ColumnType.STRING:
                            asm.invokeInterface(wPutStr, 2);
                            break;
                        default:
                            asm.invokeInterface(wPutSym, 2);
                            break;
                    }
                    break;
                case ColumnType.STRING:
                    asm.invokeInterface(rGetStr, 1);
                    switch (to.getColumnQuick(i).getType()) {
                        case ColumnType.SYMBOL:
                            asm.invokeInterface(wPutSym, 2);
                            break;
                        default:
                            asm.invokeInterface(wPutStr, 2);
                            break;
                    }
                    break;
                case ColumnType.BINARY:
                    asm.invokeInterface(rGetBin, 1);
                    asm.invokeInterface(wPutBin, 2);
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
