/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlParser;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.*;

public class RecordComparatorCompiler {
    private final BytecodeAssembler asm;
    private final IntList branches = new IntList();
    private final IntList comparatorAccessorIndices = new IntList();
    private final IntList fieldIndices = new IntList();
    private final IntList fieldNameIndices = new IntList();
    private final IntList fieldRecordAccessorIndicesA = new IntList();
    private final IntList fieldRecordAccessorIndicesB = new IntList();
    private final IntList fieldTypeIndices = new IntList();
    private final CharSequenceIntHashMap methodMap = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap typeMap = new CharSequenceIntHashMap();

    public RecordComparatorCompiler(BytecodeAssembler asm) {
        this.asm = asm;
    }

    /**
     * Generates byte code for record comparator. To avoid frequent calls to
     * record field getters comparator caches values of left argument.
     *
     * @param columnTypes      types of columns in the cursor. All but BINARY types are supported
     * @param keyColumnIndices indexes of columns in types object. Column indexes are 1-based.
     *                         Index sign indicates direction of sort: negative - descending,
     *                         positive - ascending.
     * @return RecordComparator instance.
     */
    public RecordComparator compile(ColumnTypes columnTypes, @Transient IntList keyColumnIndices) {

        assert keyColumnIndices.size() < SqlParser.MAX_ORDER_BY_COLUMNS;

        asm.init(RecordComparator.class);
        asm.setupPool();

        int stackMapTableIndex = asm.poolUtf8("StackMapTable");
        int thisClassIndex = asm.poolClass(asm.poolUtf8("io/questdb/griffin/engine/comparator"));
        int interfaceClassIndex = asm.poolClass(RecordComparator.class);
        int recordClassIndex = asm.poolClass(Record.class);
        // this is name re-use, it used on all static interfaces that compare values
        int compareNameIndex = asm.poolUtf8("compare");
        // our compare method signature
        int compareDescIndex = asm.poolUtf8("(Lio/questdb/cairo/sql/Record;)I");
        int getUuidLocationIndex = -1;
        if (poolFieldArtifacts(compareNameIndex, thisClassIndex, recordClassIndex, columnTypes, keyColumnIndices)) {
            getUuidLocationIndex = asm.poolInterfaceMethod(recordClassIndex, "getUuidLocation", "(I)J");
        }
        // elements for setLeft() method
        int setLeftNameIndex = asm.poolUtf8("setLeft");
        int setLeftDescIndex = asm.poolUtf8("(Lio/questdb/cairo/sql/Record;)V");

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(fieldNameIndices.size());
        for (int i = 0, n = fieldNameIndices.size(); i < n; i++) {
            asm.defineField(fieldNameIndices.getQuick(i), fieldTypeIndices.getQuick(i));
        }
        asm.methodCount(3);
        asm.defineDefaultConstructor();
        instrumentSetLeftMethod(setLeftNameIndex, setLeftDescIndex, getUuidLocationIndex, keyColumnIndices, columnTypes);
        instrumentCompareMethod(stackMapTableIndex, compareNameIndex, compareDescIndex, getUuidLocationIndex, keyColumnIndices, columnTypes);

        // class attribute count
        asm.putShort(0);
        return asm.newInstance();
    }

    private void instrumentCompareMethod(int stackMapTableIndex, int nameIndex, int descIndex, int getUuidLocationIndex, IntList keyColumns, ColumnTypes columnTypes) {
        // if UUID is used, we need to allocate extra long on stack and also one extra long local variable
        // long uses 2 slots on stack and also 2 slots in local variable table
        int uuidSpace = getUuidLocationIndex == -1 ? 0 : 2;
        branches.clear();
        int sz = keyColumns.size();
        int maxStack = sz + (fieldIndices.size() > sz ? 4 : 0) + 3 + uuidSpace;
        asm.startMethod(nameIndex, descIndex, maxStack, 3 + uuidSpace);

        int fieldIndex = 0;
        for (int i = 0; i < sz; i++) {
            if (i > 0) {
                asm.iload(2);
                // last one does not jump
                branches.add(asm.ifne());
            }
            asm.aload(0);
            asm.getfield(fieldIndices.getQuick(fieldIndex++));

            int index = keyColumns.getQuick(i);
            int columnIndex = (index > 0 ? index : -index) - 1;
            int columnType = columnTypes.getColumnType(columnIndex);

            int argCount = 1;
            if (columnType == ColumnType.UUID) {
                asm.aload(1);
                asm.iconst(columnIndex);
                asm.invokeInterface(getUuidLocationIndex, 1);
                asm.lstore(3);
                argCount = 3;
            }

            asm.aload(1);
            asm.iconst(columnIndex);
            if (columnType == ColumnType.UUID) {
                asm.lload(3);
            }
            asm.invokeInterface(fieldRecordAccessorIndicesA.getQuick(i), argCount);

            if (columnType == ColumnType.LONG128 || columnType == ColumnType.UUID) {
                asm.aload(0);
                asm.getfield(fieldIndices.getQuick(fieldIndex++));
                asm.aload(1);
                asm.iconst(columnIndex);
                if (columnType == ColumnType.UUID) {
                    asm.lload(3);
                }
                asm.invokeInterface(fieldRecordAccessorIndicesB.getQuick(i), argCount);
            }

            asm.invokeStatic(comparatorAccessorIndices.getQuick(i));
            if (index < 0) {
                asm.ineg();
            }
            asm.istore(2);
        }
        int p = asm.position();
        asm.iload(2);
        asm.ireturn();

        // update ifne jumps to jump to "p" position
        for (int i = 0, n = branches.size(); i < n; i++) {
            asm.setJmp(branches.getQuick(i), p);
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
        asm.startStackMapTables(stackMapTableIndex, 1);
        // frame type APPEND
        asm.append_frame(1, p - asm.getCodeStart());
        asm.putITEM_Integer();
        asm.endStackMapTables();
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
    private void instrumentSetLeftMethod(int nameIndex, int descIndex, int getUuidLocationIndex, IntList keyColumns, ColumnTypes columnTypes) {
        // if UUID is used, we need to allocate extra long on stack and also one extra long local variable
        // long uses 2 slots on stack and also 2 slots in local variable table
        int uuidSpace = getUuidLocationIndex == -1 ? 0 : 2;
        asm.startMethod(nameIndex, descIndex, 3 + uuidSpace, 2 + uuidSpace);
        int fieldIndex = 0;
        for (int i = 0, n = keyColumns.size(); i < n; i++) {
            int index = keyColumns.getQuick(i);
            // make sure column index is valid in case of "descending sort" flag
            int columnIndex = (index > 0 ? index : -index) - 1;
            int columnType = columnTypes.getColumnType(columnIndex);

            int argCount = 1;
            if (columnType == ColumnType.UUID) {
                assert getUuidLocationIndex != -1;
                asm.aload(1); // push Record to stack
                asm.iconst(columnIndex); // push column index to stack
                asm.invokeInterface(getUuidLocationIndex, 1); // call getUuidLocation
                asm.lstore(2); // store result in local variable 2
                argCount = 3;
            }

            asm.aload(0);
            asm.aload(1);
            asm.iconst(columnIndex);
            if (columnType == ColumnType.UUID) {
                asm.lload(2);
            }
            asm.invokeInterface(fieldRecordAccessorIndicesB.getQuick(i), argCount);
            asm.putfield(fieldIndices.getQuick(fieldIndex++));

            if (columnType == ColumnType.LONG128 || columnType == ColumnType.UUID) {
                asm.aload(0);
                asm.aload(1);
                asm.iconst(columnIndex);
                if (columnType == ColumnType.UUID) {
                    asm.lload(2);
                }
                asm.invokeInterface(fieldRecordAccessorIndicesA.getQuick(i), argCount);
                asm.putfield(fieldIndices.getQuick(fieldIndex++));
            }
        }
        asm.return_();
        asm.endMethodCode();
        // exceptions
        asm.putShort(0);
        // attributes
        asm.putShort(0);
        asm.endMethod();
    }

    /**
     * Pool methods that are used to compare records.
     *
     * @param compareMethodIndex
     * @param thisClassIndex
     * @param recordClassIndex
     * @param columnTypes
     * @param keyColumnIndices
     * @return true if at least one key is UUID
     */
    private boolean poolFieldArtifacts(
            int compareMethodIndex,
            int thisClassIndex,
            int recordClassIndex,
            ColumnTypes columnTypes,
            IntList keyColumnIndices
    ) {
        typeMap.clear();
        fieldIndices.clear();
        fieldNameIndices.clear();
        fieldTypeIndices.clear();
        fieldRecordAccessorIndicesA.clear();
        fieldRecordAccessorIndicesB.clear();
        comparatorAccessorIndices.clear();
        methodMap.clear();

        boolean hasUuid = false;
        // define names and types
        for (int i = 0, n = keyColumnIndices.size(); i < n; i++) {
            String fieldType;
            String getterNameA;
            String getterNameB = null;
            @SuppressWarnings("rawtypes") Class comparatorClass;
            String comparatorDesc = null;
            int index = keyColumnIndices.getQuick(i);

            if (index < 0) {
                index = -index;
            }


            // decrement to get real column index
            index--;

            int columnType = columnTypes.getColumnType(index);
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BOOLEAN:
                    fieldType = "Z";
                    getterNameA = "getBool";
                    comparatorClass = Boolean.class;
                    break;
                case ColumnType.BYTE:
                    fieldType = "B";
                    getterNameA = "getByte";
                    comparatorClass = Byte.class;
                    break;
                case ColumnType.DOUBLE:
                    fieldType = "D";
                    getterNameA = "getDouble";
                    comparatorClass = Numbers.class;
                    break;
                case ColumnType.FLOAT:
                    fieldType = "F";
                    getterNameA = "getFloat";
                    comparatorClass = Numbers.class;
                    break;
                case ColumnType.INT:
                    fieldType = "I";
                    getterNameA = "getInt";
                    comparatorClass = Integer.class;
                    break;
                case ColumnType.LONG:
                    fieldType = "J";
                    getterNameA = "getLong";
                    comparatorClass = Long.class;
                    break;
                case ColumnType.DATE:
                    fieldType = "J";
                    getterNameA = "getDate";
                    comparatorClass = Long.class;
                    break;
                case ColumnType.TIMESTAMP:
                    fieldType = "J";
                    getterNameA = "getTimestamp";
                    comparatorClass = Long.class;
                    break;
                case ColumnType.SHORT:
                    fieldType = "S";
                    getterNameA = "getShort";
                    comparatorClass = Short.class;
                    break;
                case ColumnType.CHAR:
                    fieldType = "C";
                    getterNameA = "getChar";
                    comparatorClass = Character.class;
                    break;
                case ColumnType.STRING:
                    getterNameA = "getStr";
                    getterNameB = "getStrB";
                    fieldType = "Ljava/lang/CharSequence;";
                    comparatorClass = Chars.class;
                    break;
                case ColumnType.LONG256:
                    getterNameA = "getLong256A";
                    getterNameB = "getLong256B";
                    fieldType = "Lio/questdb/std/Long256;";
                    comparatorClass = Long256Util.class;
                    break;
                case ColumnType.LONG128:
                    getterNameA = "getLong128Hi";
                    getterNameB = "getLong128Lo";
                    fieldType = "J";
                    comparatorDesc = "(JJJJ)I";
                    comparatorClass = Long128Util.class;
                    break;
                case ColumnType.UUID:
                    getterNameA = "getUuidHi";
                    getterNameB = "getUuidLo";
                    fieldType = "J";
                    comparatorDesc = "(JJJJ)I";
                    comparatorClass = Long128Util.class;
                    hasUuid = true;
                    break;
                default:
                    // SYMBOL
                    getterNameA = "getSym";
                    getterNameB = "getSymB";
                    fieldType = "Ljava/lang/CharSequence;";
                    comparatorClass = Chars.class;
                    comparatorDesc = "(Ljava/lang/CharSequence;Ljava/lang/CharSequence;)I";
                    break;
            }

            int keyIndex;
            int nameIndex;
            int typeIndex;

            keyIndex = typeMap.keyIndex(fieldType);
            if (keyIndex > -1) {
                typeMap.putAt(keyIndex, fieldType, typeIndex = asm.poolUtf8(fieldType));
            } else {
                typeIndex = typeMap.valueAt(keyIndex);
            }

            fieldTypeIndices.add(typeIndex);
            fieldNameIndices.add(nameIndex = asm.poolUtf8().put('f').put(i).$());
            fieldIndices.add(asm.poolField(thisClassIndex, asm.poolNameAndType(nameIndex, typeIndex)));

            int methodIndex;
            String getterType = fieldType;
            if (columnType == ColumnType.LONG128 || columnType == ColumnType.UUID) {
                // Special cases: Long128 & UUID are 2 longs to compare
                fieldTypeIndices.add(typeIndex);
                int nameIndex2 = asm.poolUtf8().put('f').put(i).put(i).$();
                fieldNameIndices.add(nameIndex2);
                int nameAndTypeIndex = asm.poolNameAndType(nameIndex2, typeIndex);
                fieldIndices.add(asm.poolField(thisClassIndex, nameAndTypeIndex));
            }
            BytecodeAssembler.Utf8Appender utf8 = asm.poolUtf8();
            if (columnType != ColumnType.UUID) {
                // Default case: getters receive a single parameter: (int column_index)
                utf8.put("(I)");
            } else {
                // Special case: UUID getters receive the extra parameter: (int column_index, long location) 
                utf8.put("(IJ)");
            }
            int getterSignatureIndex = utf8.put(getterType).$();
            int getterNameIndex = asm.poolUtf8(getterNameA);
            int getterNameAndTypeIndex = asm.poolNameAndType(getterNameIndex, getterSignatureIndex);

            methodMap.putIfAbsent(getterNameA, methodIndex = asm.poolInterfaceMethod(recordClassIndex, getterNameAndTypeIndex));
            fieldRecordAccessorIndicesA.add(methodIndex);

            if (getterNameB != null) {
                getterNameIndex = asm.poolUtf8(getterNameB);
                getterNameAndTypeIndex = asm.poolNameAndType(getterNameIndex, getterSignatureIndex);
                methodMap.putIfAbsent(getterNameB, methodIndex = asm.poolInterfaceMethod(recordClassIndex, getterNameAndTypeIndex));
            }

            fieldRecordAccessorIndicesB.add(methodIndex);
            comparatorAccessorIndices.add(
                    asm.poolMethod(asm.poolClass(comparatorClass),
                            asm.poolNameAndType(
                                    compareMethodIndex, comparatorDesc == null ?
                                            asm.poolUtf8().put('(').put(fieldType).put(fieldType).put(")I").$()
                                            :
                                            asm.poolUtf8(comparatorDesc))
                    ));
        }
        return hasUuid;
    }
}
