/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin.engine.orderby;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.SqlParser;
import com.questdb.std.*;

public class RecordComparatorCompiler {
    private final BytecodeAssembler asm;
    private final CharSequenceIntHashMap typeMap = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap methodMap = new CharSequenceIntHashMap();
    private final IntList fieldIndices = new IntList();
    private final IntList fieldNameIndices = new IntList();
    private final IntList fieldTypeIndices = new IntList();
    private final IntList fieldRecordAccessorIndicesA = new IntList();
    private final IntList fieldRecordAccessorIndicesB = new IntList();
    private final IntList comparatorAccessorIndices = new IntList();
    private final IntList branches = new IntList();

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
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(RecordComparator.class);
        int recordClassIndex = asm.poolClass(Record.class);
        // this is name re-use, it used on all static interfaces that compare values
        int compareNameIndex = asm.poolUtf8("compare");
        // our compare method signature
        int compareDescIndex = asm.poolUtf8("(Lcom/questdb/cairo/sql/Record;)I");
        poolFieldArtifacts(compareNameIndex, thisClassIndex, recordClassIndex, columnTypes, keyColumnIndices);
        // elements for setLeft() method
        int setLeftNameIndex = asm.poolUtf8("setLeft");
        int setLeftDescIndex = asm.poolUtf8("(Lcom/questdb/cairo/sql/Record;)V");
        //
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
        instrumentSetLeftMethod(setLeftNameIndex, setLeftDescIndex, keyColumnIndices);
        instrumentCompareMethod(stackMapTableIndex, compareNameIndex, compareDescIndex, keyColumnIndices);

        // class attribute count
        asm.putShort(0);
        return asm.newInstance();
    }

    private void instrumentCompareMethod(int stackMapTableIndex, int nameIndex, int descIndex, IntList keyColumns) {
        branches.clear();
        int sz = keyColumns.size();
        asm.startMethod(nameIndex, descIndex, sz + 3, 3);

        for (int i = 0; i < sz; i++) {
            if (i > 0) {
                asm.iload(2);
                // last one does not jump
                branches.add(asm.ifne());
            }
            asm.aload(0);
            asm.getfield(fieldIndices.getQuick(i));
            asm.aload(1);
            int index = keyColumns.getQuick(i);
            asm.iconst((index > 0 ? index : -index) - 1);
            asm.invokeInterface(fieldRecordAccessorIndicesA.getQuick(i), 1);
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
    private void instrumentSetLeftMethod(int nameIndex, int descIndex, IntList keyColumns) {
        asm.startMethod(nameIndex, descIndex, 3, 2);
        for (int i = 0, n = keyColumns.size(); i < n; i++) {
            asm.aload(0);
            asm.aload(1);
            int index = keyColumns.getQuick(i);
            // make sure column index is valid in case of "descending sort" flag
            asm.iconst((index > 0 ? index : -index) - 1);
            asm.invokeInterface(fieldRecordAccessorIndicesB.getQuick(i), 1);
            asm.putfield(fieldIndices.getQuick(i));
        }
        asm.return_();
        asm.endMethodCode();
        // exceptions
        asm.putShort(0);
        // attributes
        asm.putShort(0);
        asm.endMethod();
    }

    private void poolFieldArtifacts(
            int compareMethodIndex,
            int thisClassIndex,
            int recordClassIndex,
            ColumnTypes columnTypes,
            IntList keyColumnIndices) {
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
            Class comparatorClass;
            String comparatorDesc = null;
            int index = keyColumnIndices.getQuick(i);

            if (index < 0) {
                index = -index;
            }


            // decrement to get real column index
            index--;

            switch (columnTypes.getColumnType(index)) {
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
                default:
                    // SYMBOL
                    getterNameA = "getSym";
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
            methodMap.putIfAbsent(getterNameA, methodIndex = asm.poolInterfaceMethod(recordClassIndex, getterNameA, "(I)" + fieldType));
            fieldRecordAccessorIndicesA.add(methodIndex);

            if (getterNameB != null) {
                methodMap.putIfAbsent(getterNameB, methodIndex = asm.poolInterfaceMethod(recordClassIndex, getterNameB, "(I)" + fieldType));
            }

            fieldRecordAccessorIndicesB.add(methodIndex);
            comparatorAccessorIndices.add(
                    asm.poolMethod(asm.poolClass(comparatorClass),
                            asm.poolNameAndType(compareMethodIndex, comparatorDesc == null ? asm.poolUtf8().put('(').put(fieldType).put(fieldType).put(")I").$() : asm.poolUtf8(comparatorDesc))
                    ));
        }
    }
}
