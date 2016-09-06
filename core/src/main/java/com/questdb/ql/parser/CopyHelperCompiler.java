/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.parser;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.BytecodeAssembler;
import com.questdb.ql.impl.sort.ComparatorCompiler;
import com.questdb.store.ColumnType;

public class CopyHelperCompiler {
    private final BytecodeAssembler asm = new BytecodeAssembler();

    public CopyHelper compile(RecordMetadata metadata) {
        asm.clear();
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(asm.poolUtf8("com/questdb/ql/parser/CopyHelper"));
        int recordClassIndex = asm.poolClass(asm.poolUtf8("com/questdb/ql/Record"));
        int writerClassIndex = asm.poolClass(asm.poolUtf8("com/questdb/JournalEntryWriter"));

        int rGetInt = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getInt"), asm.poolUtf8("(I)I")));
        // shared sig
        int rIntLong = asm.poolUtf8("(I)J");
        int rGetLong = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getLong"), rIntLong));
        int rGetDate = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getDate"), rIntLong));
        //
        int rGetByte = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("get"), asm.poolUtf8("(I)B")));
        int rGetShort = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getShort"), asm.poolUtf8("(I)S")));
        int rGetBool = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getBool"), asm.poolUtf8("(I)Z")));
        int rGetFloat = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getFloat"), asm.poolUtf8("(I)F")));
        int rGetDouble = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getDouble"), asm.poolUtf8("(I)D")));
        int rGetSym = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getSym"), asm.poolUtf8("(I)Ljava/lang/String;")));
        int rGetStr = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getFlyweightStr"), asm.poolUtf8("(I)Ljava/lang/CharSequence;")));
        int rGetBin = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getBin"), asm.poolUtf8("(I)Lcom/questdb/std/DirectInputStream;")));

        //
        int wPutInt = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putInt"), asm.poolUtf8("(II)V")));
        int wIntLong = asm.poolUtf8("(IJ)V");
        int wPutLong = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putLong"), wIntLong));
        int wPutDate = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putDate"), wIntLong));
        //
        int wPutByte = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("put"), asm.poolUtf8("(IB)V")));
        int wPutShort = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putShort"), asm.poolUtf8("(IS)V")));
        int wPutBool = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putBool"), asm.poolUtf8("(IZ)V")));
        int wPutFloat = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putFloat"), asm.poolUtf8("(IF)V")));
        int wPutDouble = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putDouble"), asm.poolUtf8("(ID)V")));
        int wPutSym = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putSym"), asm.poolUtf8("(ILjava/lang/CharSequence;)V")));
        int wPutStr = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putStr"), asm.poolUtf8("(ILjava/lang/CharSequence;)V")));
        int wPutBin = asm.poolInterfaceMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putBin"), asm.poolUtf8("(ILjava/io/InputStream;)V")));

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lcom/questdb/ql/Record;Lcom/questdb/JournalEntryWriter;)V");

        asm.finishPool();
        asm.defineClass(1, thisClassIndex);
        // interface count
        asm.putShort(1);
        asm.putShort(interfaceClassIndex);
        // field count
        asm.putShort(0);
        // method count
        asm.putShort(2);
        asm.defineDefaultConstructor();

        asm.startMethod(0x01, copyNameIndex, copySigIndex, 4, 3);

        int n = metadata.getColumnCount();
        for (int i = 0; i < n; i++) {
            asm.put(BytecodeAssembler.aload_2);
            asm.putConstant(i);
            asm.put(BytecodeAssembler.aload_1);
            asm.putConstant(i);

            switch (metadata.getColumnQuick(i).getType()) {
                case ColumnType.INT:
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
                case ColumnType.BYTE:
                    asm.invokeInterface(rGetByte, 1);
                    asm.invokeInterface(wPutByte, 2);
                    break;
                case ColumnType.SHORT:
                    asm.invokeInterface(rGetShort, 1);
                    asm.invokeInterface(wPutShort, 2);
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
                case ColumnType.SYMBOL:
                    asm.invokeInterface(rGetSym, 1);
                    asm.invokeInterface(wPutSym, 2);
                    break;
                case ColumnType.STRING:
                    asm.invokeInterface(rGetStr, 1);
                    asm.invokeInterface(wPutStr, 2);
                    break;
                case ColumnType.BINARY:
                    asm.invokeInterface(rGetBin, 1);
                    asm.invokeInterface(wPutBin, 2);
                    break;
                default:
                    break;
            }
        }

        asm.put(BytecodeAssembler.return_);
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

        try {
            return (CopyHelper) asm.loadClass(ComparatorCompiler.class).newInstance();
        } catch (Exception e) {
            throw new JournalRuntimeException("Cannot instantiate comparator: ", e);
        }
    }
}
