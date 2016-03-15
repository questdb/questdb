/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.impl.sort;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.ex.JournalUnsupportedTypeException;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.BytecodeAssembler;
import com.nfsdb.std.CharSequenceIntHashMap;
import com.nfsdb.std.IntList;
import com.nfsdb.std.Transient;

public class ComparatorCompiler {
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final CharSequenceIntHashMap typeMap = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap methodMap = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap comparatorMap = new CharSequenceIntHashMap();
    private final IntList fieldIndices = new IntList();
    private final IntList fieldNameIndices = new IntList();
    private final IntList fieldTypeIndices = new IntList();
    private final IntList fieldRecordAccessorIndices = new IntList();
    private final IntList comparatorAccessorIndices = new IntList();
    private final IntList branches = new IntList();

    public RecordComparator compile(Class host, RecordMetadata m, @Transient IntList keyColumnIndices) {

        assert keyColumnIndices.size() < 1560;
        asm.clear();
        asm.setupPool();
        int stackMapTableIndex = asm.poolUtf8("StackMapTable");
        int thisClassIndex = asm.poolClass(asm.poolUtf8("nfsasm"));
        int interfaceClassIndex = asm.poolClass(asm.poolUtf8("com/nfsdb/ql/impl/sort/RecordComparator"));
        int recordClassIndex = asm.poolClass(asm.poolUtf8("com/nfsdb/ql/Record"));
        // this is name re-use, it used on all static interfaces that compare values
        int compareNameIndex = asm.poolUtf8("compare");
        // our compare method signature
        int compareDescIndex = asm.poolUtf8("(Lcom/nfsdb/ql/Record;)I");
        poolFieldArtifacts(compareNameIndex, thisClassIndex, recordClassIndex, m, keyColumnIndices);
        // elements for setLeft() method
        int setLeftNameIndex = asm.poolUtf8("setLeft");
        int setLeftDescIndex = asm.poolUtf8("(Lcom/nfsdb/ql/Record;)V");
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
            return (RecordComparator) asm.loadClass(host).newInstance();
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
            asm.putConstant(keyColumns.getQuick(i));
            asm.invokeInterface(fieldRecordAccessorIndices.getQuick(i));
            asm.put(BytecodeAssembler.invokestatic);
            asm.putShort(comparatorAccessorIndices.getQuick(i));
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
            asm.putConstant(keyColumns.getQuick(i));
            asm.invokeInterface(fieldRecordAccessorIndices.getQuick(i));
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
        fieldRecordAccessorIndices.clear();
        methodMap.clear();

        // define names and types
        for (int i = 0, n = keyColumnIndices.size(); i < n; i++) {
            String fieldType;
            String getterName;
            String comparatorClass;
            String comparatorDesc = null;
            switch (m.getColumn(keyColumnIndices.getQuick(i)).getType()) {
                case BOOLEAN:
                    fieldType = "Z";
                    getterName = "getBool";
                    comparatorClass = "java/lang/Boolean";
                    break;
                case BYTE:
                    fieldType = "B";
                    getterName = "get";
                    comparatorClass = "java/lang/Byte";
                    break;
                case DOUBLE:
                    fieldType = "D";
                    getterName = "getDouble";
                    comparatorClass = "java/lang/Double";
                    break;
                case FLOAT:
                    fieldType = "F";
                    getterName = "getFloat";
                    comparatorClass = "java/lang/Float";
                    break;
                case INT:
                    fieldType = "I";
                    getterName = "getInt";
                    comparatorClass = "java/lang/Integer";
                    break;
                case LONG:
                case DATE:
                    fieldType = "J";
                    getterName = "getLong";
                    comparatorClass = "java/lang/Long";
                    break;
                case SHORT:
                    fieldType = "S";
                    getterName = "getShort";
                    comparatorClass = "java/lang/Short";
                    break;
                case STRING:
                    getterName = "getFlyweightStr";
                    fieldType = "Ljava/lang/CharSequence;";
                    comparatorClass = "com/nfsdb/misc/Chars";
                    break;
                case SYMBOL:
                    getterName = "getSym";
                    fieldType = "Ljava/lang/String;";
                    comparatorClass = "com/nfsdb/misc/Chars";
                    comparatorDesc = "(Ljava/lang/CharSequence;Ljava/lang/CharSequence;)I";
                    break;
                default:
                    throw new JournalUnsupportedTypeException(m.getColumn(keyColumnIndices.getQuick(i)).getType());
            }

            int nameIndex;
            int typeIndex = typeMap.get(fieldType);
            if (typeIndex == -1) {
                typeMap.put(fieldType, typeIndex = asm.poolUtf8(fieldType));
            }
            fieldTypeIndices.add(typeIndex);
            fieldNameIndices.add(nameIndex = asm.poolUtf8().put('f').put(i).$());
            fieldIndices.add(asm.poolField(thisClassIndex, asm.poolNameAndType(nameIndex, typeIndex)));

            int methodIndex = methodMap.get(getterName);
            if (methodIndex == -1) {
                methodMap.put(getterName, methodIndex = asm.poolInterfaceMethod(recordClassIndex, asm.poolNameAndType(asm.poolUtf8(getterName), asm.poolUtf8("(I)" + fieldType))));
            }
            fieldRecordAccessorIndices.add(methodIndex);

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
