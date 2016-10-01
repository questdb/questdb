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

package com.questdb.ql.impl.map;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.BytecodeAssembler;
import com.questdb.ql.Record;
import com.questdb.std.IntList;
import com.questdb.store.ColumnType;

public class RecordKeyCopierCompiler {
    private final BytecodeAssembler asm = new BytecodeAssembler();

    public RecordKeyCopier compile(RecordMetadata meta, IntList columns) {
        asm.clear();
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(RecordKeyCopier.class);
        int recordClassIndex = asm.poolClass(Record.class);
        int writerClassIndex = asm.poolClass(DirectMap.KeyWriter.class);

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
        int rGetStr = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8("getFlyweightStr"), asm.poolUtf8("(I)Ljava/lang/CharSequence;")));

        //
        int wPutInt = asm.poolMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putInt"), asm.poolUtf8("(I)V")));
        int wIntLong = asm.poolUtf8("(J)V");
        int wPutLong = asm.poolMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putLong"), wIntLong));
        //
        int wPutByte = asm.poolMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putByte"), asm.poolUtf8("(B)V")));
        int wPutShort = asm.poolMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putShort"), asm.poolUtf8("(S)V")));
        int wPutBool = asm.poolMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putBool"), asm.poolUtf8("(Z)V")));
        int wPutFloat = asm.poolMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putFloat"), asm.poolUtf8("(F)V")));
        int wPutDouble = asm.poolMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putDouble"), asm.poolUtf8("(D)V")));
        int wPutStr = asm.poolMethod(writerClassIndex, asm.poolNameAndType(asm.poolUtf8("putStr"), asm.poolUtf8("(Ljava/lang/CharSequence;)V")));

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lcom/questdb/ql/Record;Lcom/questdb/ql/impl/map/DirectMap$KeyWriter;)V");

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

        int n = columns.size();
        for (int i = 0; i < n; i++) {

            int index = columns.getQuick(i);
            asm.put(BytecodeAssembler.aload_2);
            asm.put(BytecodeAssembler.aload_1);
            asm.putConstant(index);

            switch (meta.getColumnQuick(index).getType()) {
                case ColumnType.INT:
                case ColumnType.SYMBOL:
                    asm.invokeInterface(rGetInt, 1);
                    asm.invokeVirtual(wPutInt);
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
            return (RecordKeyCopier) asm.loadClass(this.getClass()).newInstance();
        } catch (Exception e) {
            throw new JournalRuntimeException("Cannot instantiate comparator: ", e);
        }
    }
}
