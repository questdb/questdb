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

package com.questdb.ql.impl.sort;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.ex.JournalUnsupportedTypeException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.BytecodeAssembler;
import com.questdb.ql.parser.QueryParser;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.IntList;
import com.questdb.std.Transient;
import com.questdb.store.ColumnType;

public class ComparatorCompiler {
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final CharSequenceIntHashMap typeMap = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap methodMap = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap comparatorMap = new CharSequenceIntHashMap();
    private final IntList fieldIndices = new IntList();
    private final IntList fieldNameIndices = new IntList();
    private final IntList fieldTypeIndices = new IntList();
    private final IntList fieldRecordAccessorIndicesA = new IntList();
    private final IntList fieldRecordAccessorIndicesB = new IntList();
    private final IntList comparatorAccessorIndices = new IntList();
    private final IntList branches = new IntList();

    public RecordComparator compile(RecordMetadata m, @Transient IntList keyColumnIndices) {

        assert keyColumnIndices.size() < QueryParser.MAX_ORDER_BY_COLUMNS;

        asm.clear();
        asm.setupPool();
        int stackMapTableIndex = asm.poolUtf8("StackMapTable");
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(asm.poolUtf8("com/questdb/ql/impl/sort/RecordComparator"));
        int recordClassIndex = asm.poolClass(asm.poolUtf8("com/questdb/ql/Record"));
        // this is name re-use, it used on all static interfaces that compare values
        int compareNameIndex = asm.poolUtf8("compare");
        // our compare method signature
        int compareDescIndex = asm.poolUtf8("(Lcom/questdb/ql/Record;)I");
        poolFieldArtifacts(compareNameIndex, thisClassIndex, recordClassIndex, m, keyColumnIndices);
        // elements for setLeft() method
        int setLeftNameIndex = asm.poolUtf8("setLeft");
        int setLeftDescIndex = asm.poolUtf8("(Lcom/questdb/ql/Record;)V");
        //
        asm.finishPool();
        asm.defineClass(1, thisClassIndex);
        // interface count
        asm.putShort(1);
        asm.putShort(interfaceClassIndex);
        // field count
        asm.putShort(fieldNameIndices.size());
        for (int i = 0, n = fieldNameIndices.size(); i < n; i++) {
            asm.defineField(0x02, fieldNameIndices.getQuick(i), fieldTypeIndices.getQuick(i));
        }
        // method count
        asm.putShort(3);
        asm.defineDefaultConstructor();
        instrumentSetLeftMethod(setLeftNameIndex, setLeftDescIndex, keyColumnIndices);
        instrumentCompareMethod(stackMapTableIndex, compareNameIndex, compareDescIndex, keyColumnIndices);

        // class attribute count
        asm.putShort(0);

        try {
            return (RecordComparator) asm.loadClass(ComparatorCompiler.class).newInstance();
        } catch (Exception e) {
            throw new JournalRuntimeException("Cannot instantiate comparator: ", e);
        }
    }

    private void instrumentCompareMethod(int stackMapTableIndex, int nameIndex, int descIndex, IntList keyColumns) {
        branches.clear();
        int sz = keyColumns.size();
        asm.startMethod(0x01, nameIndex, descIndex, sz + 3, 3);

        int codeStart = asm.position();
        for (int i = 0; i < sz; i++) {
            if (i > 0) {
                asm.put(BytecodeAssembler.iload_2);
                // last one does not jump
                branches.add(asm.position());
                asm.put(BytecodeAssembler.ifne);
                asm.putShort(0);
            }
            asm.put(BytecodeAssembler.aload_0);
            asm.put(BytecodeAssembler.getfield);
            asm.putShort(fieldIndices.getQuick(i));
            asm.put(BytecodeAssembler.aload_1);
            int index = keyColumns.getQuick(i);
            asm.putConstant((index > 0 ? index : -index) - 1);
            asm.invokeInterface(fieldRecordAccessorIndicesA.getQuick(i), 1);
            asm.put(BytecodeAssembler.invokestatic);
            asm.putShort(comparatorAccessorIndices.getQuick(i));
            if (index < 0) {
                asm.put(BytecodeAssembler.ineg);
            }
            asm.put(BytecodeAssembler.istore_2);
        }
        int p = asm.position();
        asm.put(BytecodeAssembler.iload_2);
        asm.put(BytecodeAssembler.ireturn);


        // update ifne jumps to jump to "p" position
        for (int i = 0, n = branches.size(); i < n; i++) {
            int ifneOffset = branches.getQuick(i);
            asm.putShort(ifneOffset + 1, p - ifneOffset);
        }

        asm.endMethodCode();
        // exceptions
        asm.putShort(0);

        // we have to add stack map table as branch target
        // jvm requires it

        // attributes: 1 - StackMapTable
        asm.putShort(1);
        // verification to ensure that return type is int and there is correct
        // value present on stack
        asm.putStackMapAppendInt(stackMapTableIndex, p - codeStart);
        asm.endMethod();
    }

    /*
     * setLeft(Record)
     *
     * This code generates method setLeft(Record), which assigns selected fields from
     * the record to class fields. Generally this method looks like:
     * f1 = record.getInt(3);
     * f2 = record.getFlyweightStr(5);
     *
     * as you can see record fields are accessed by constants, to speed up the process.
     *
     * Class like that would translate to bytecode:
     *
     * aload_0
     * aload_1
     * invokeinterface index
     * putfield index
     * ...
     *
     * and so on for each field
     * bytecode finishes with
     *
     * return
     *
     * all this complicated dancing around is to have class names, method names, field names
     * method signatures in constant pool in bytecode.
     */
    private void instrumentSetLeftMethod(int nameIndex, int descIndex, IntList keyColumns) {
        asm.startMethod(0x01, nameIndex, descIndex, 3, 2);
        for (int i = 0, n = keyColumns.size(); i < n; i++) {
            asm.put(BytecodeAssembler.aload_0);
            asm.put(BytecodeAssembler.aload_1);
            int index = keyColumns.getQuick(i);
            // make sure column index is valid in case of "descending sort" flag
            asm.putConstant((index > 0 ? index : -index) - 1);
            asm.invokeInterface(fieldRecordAccessorIndicesB.getQuick(i), 1);
            asm.put(BytecodeAssembler.putfield);
            asm.putShort(fieldIndices.getQuick(i));
        }
        asm.put(BytecodeAssembler.return_);
        asm.endMethodCode();
        // exceptions
        asm.putShort(0);
        // attributes
        asm.putShort(0);
        asm.endMethod();
    }

    private void poolFieldArtifacts(int compareMethodIndex, int thisClassIndex, int recordClassIndex, RecordMetadata m, IntList keyColumnIndices) {
        typeMap.clear();
        fieldIndices.clear();
        fieldNameIndices.clear();
        fieldTypeIndices.clear();
        fieldRecordAccessorIndicesA.clear();
        fieldRecordAccessorIndicesB.clear();
        comparatorAccessorIndices.clear();
        methodMap.clear();

        // define names and types
        for (int i = 0, n = keyColumnIndices.size(); i < n; i++) {
            String fieldType;
            String getterNameA;
            String getterNameB = null;
            String comparatorClass;
            String comparatorDesc = null;
            int index = keyColumnIndices.getQuick(i);

            if (index < 0) {
                index = -index;
            }

            // decrement to get real column index
            index--;

            switch (m.getColumn(index).getType()) {
                case ColumnType.BOOLEAN:
                    fieldType = "Z";
                    getterNameA = "getBool";
                    comparatorClass = "java/lang/Boolean";
                    break;
                case ColumnType.BYTE:
                    fieldType = "B";
                    getterNameA = "get";
                    comparatorClass = "java/lang/Byte";
                    break;
                case ColumnType.DOUBLE:
                    fieldType = "D";
                    getterNameA = "getDouble";
                    comparatorClass = "com/questdb/misc/Numbers";
                    break;
                case ColumnType.FLOAT:
                    fieldType = "F";
                    getterNameA = "getFloat";
                    comparatorClass = "com/questdb/misc/Numbers";
                    break;
                case ColumnType.INT:
                    fieldType = "I";
                    getterNameA = "getInt";
                    comparatorClass = "java/lang/Integer";
                    break;
                case ColumnType.LONG:
                case ColumnType.DATE:
                    fieldType = "J";
                    getterNameA = "getLong";
                    comparatorClass = "java/lang/Long";
                    break;
                case ColumnType.SHORT:
                    fieldType = "S";
                    getterNameA = "getShort";
                    comparatorClass = "java/lang/Short";
                    break;
                case ColumnType.STRING:
                    getterNameA = "getFlyweightStr";
                    getterNameB = "getFlyweightStrB";
                    fieldType = "Ljava/lang/CharSequence;";
                    comparatorClass = "com/questdb/misc/Chars";
                    break;
                case ColumnType.SYMBOL:
                    getterNameA = "getSym";
                    fieldType = "Ljava/lang/String;";
                    comparatorClass = "com/questdb/misc/Chars";
                    comparatorDesc = "(Ljava/lang/CharSequence;Ljava/lang/CharSequence;)I";
                    break;
                default:
                    throw new JournalUnsupportedTypeException(ColumnType.nameOf(m.getColumn(index).getType()).toString());
            }

            int nameIndex;
            int typeIndex = typeMap.get(fieldType);
            if (typeIndex == -1) {
                typeMap.put(fieldType, typeIndex = asm.poolUtf8(fieldType));
            }
            fieldTypeIndices.add(typeIndex);
            fieldNameIndices.add(nameIndex = asm.poolUtf8().put('f').put(i).$());
            fieldIndices.add(asm.poolField(thisClassIndex, asm.poolNameAndType(nameIndex, typeIndex)));

            int methodIndex = methodMap.get(getterNameA);
            if (methodIndex == -1) {
                methodMap.put(getterNameA, methodIndex = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8(getterNameA), asm.poolUtf8("(I)" + fieldType))));
            }
            fieldRecordAccessorIndicesA.add(methodIndex);

            if (getterNameB != null) {
                methodIndex = methodMap.get(getterNameB);
                if (methodIndex == -1) {
                    methodMap.put(getterNameB, methodIndex = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8(getterNameB), asm.poolUtf8("(I)" + fieldType))));
                }
            }
            fieldRecordAccessorIndicesB.add(methodIndex);

            int comparatorIndex = comparatorMap.get(comparatorClass);
            if (comparatorIndex == -1) {
                int cc = asm.poolClass(asm.poolUtf8(comparatorClass));
                int nt = asm.poolNameAndType(compareMethodIndex, comparatorDesc == null ? asm.poolUtf8().put('(').put(fieldType).put(fieldType).put(")I").$() : asm.poolUtf8(comparatorDesc));
                comparatorIndex = asm.poolMethod(cc, nt);
            }
            comparatorAccessorIndices.add(comparatorIndex);
        }
    }
}
