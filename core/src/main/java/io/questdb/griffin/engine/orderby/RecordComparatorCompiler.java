/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlParser;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Long128;
import io.questdb.std.Long256Util;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Uuid;
import io.questdb.std.ex.BytecodeException;
import io.questdb.std.str.Utf8s;

public class RecordComparatorCompiler {
    private static final Log LOG = LogFactory.getLog(RecordComparatorCompiler.class);
    private final BytecodeAssembler asm;
    private final IntList branches = new IntList();
    private final IntList comparatorAccessorIndices = new IntList();
    private final IntList fieldIndices = new IntList();
    private final IntList fieldNameIndices = new IntList();
    // We store the pooled method indices for record accessor in 2 separate lists to separate
    // records that are stored (left) and those that are directly used in comparison (right).
    // This is mandatory for accessors that relies on objects to give a representation of their
    // values (for example, `getStrA` and `getStrB`).
    // As there can be multiple indices for a single methods (if we need to pull multiple
    // values for a single column), we store the number of accessors in a separate list and the indices in the
    // order they will be called.
    //
    // For example, in Long128 case, we will have:
    // accessorCount: [2]
    // indicesLeft: [x, y]
    // indicesRight: [x, y]
    // While for something like String, we will have:
    // accessorCount: [1]
    // indicesLeft: [x]
    // indicesRight: [y]
    private final IntList fieldRecordAccessorCount = new IntList(); // Stores the number of accessors for a given column.
    private final IntList fieldRecordAccessorIndicesLeft = new IntList();
    private final IntList fieldRecordAccessorIndicesRight = new IntList();
    private final IntList fieldTypeIndices = new IntList();
    private final CharSequenceIntHashMap methodMap = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap typeMap = new CharSequenceIntHashMap();
    private int maxColumnSize = 0; // Maximum used stack-size by a column.

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
     * @return generated class.
     */
    public Class<RecordComparator> compile(ColumnTypes columnTypes, @Transient IntList keyColumnIndices) throws SqlException {
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
        poolFieldArtifacts(compareNameIndex, thisClassIndex, recordClassIndex, columnTypes, keyColumnIndices);
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
        instrumentSetLeftMethod(setLeftNameIndex, setLeftDescIndex, keyColumnIndices);
        instrumentCompareMethod(stackMapTableIndex, compareNameIndex, compareDescIndex, keyColumnIndices);

        // class attribute count
        asm.putShort(0);

        return asm.loadClass();
    }

    /**
     * Generates byte code for record comparator and creates an instance. To avoid frequent calls to
     * record field getters comparator caches values of left argument.
     *
     * @param columnTypes      types of columns in the cursor. All but BINARY types are supported
     * @param keyColumnIndices indexes of columns in types object. Column indexes are 1-based.
     *                         Index sign indicates direction of sort: negative - descending,
     *                         positive - ascending.
     * @return RecordComparator instance.
     */
    public RecordComparator newInstance(ColumnTypes columnTypes, @Transient IntList keyColumnIndices) throws SqlException {
        final Class<RecordComparator> clazz = compile(columnTypes, keyColumnIndices);
        return newInstance(clazz);
    }

    /**
     * Creates a record comparator instance for the given class.
     */
    public RecordComparator newInstance(Class<RecordComparator> clazz) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            LOG.critical().$("could not create an instance of GroupByFunctionsUpdater, cause: ").$(e).$();
            throw BytecodeException.INSTANCE;
        }
    }

    private void instrumentCompareMethod(int stackMapTableIndex, int nameIndex, int descIndex, IntList keyColumns) {
        branches.clear();
        int sz = keyColumns.size();
        int maxStack = maxColumnSize * 2 + 3;
        asm.startMethod(nameIndex, descIndex, maxStack, 3);

        int fieldIndex = 0;
        int accessorIndex = 0;
        for (int i = 0; i < sz; i++) {
            if (i > 0) {
                asm.iload(2);
                // last one does not jump
                branches.add(asm.ifne());
            }

            int accessorCount = fieldRecordAccessorCount.getQuick(i);
            // Loads left side of comparator
            for (int j = 0; j < accessorCount; j++) {
                asm.aload(0);
                asm.getfield(fieldIndices.getQuick(fieldIndex++));
            }

            int index = keyColumns.getQuick(i);
            int columnIndex = (index > 0 ? index : -index) - 1;

            // Loads right side of comparator
            for (int j = 0; j < accessorCount; j++) {
                asm.aload(1);
                asm.iconst(columnIndex);
                asm.invokeInterface(fieldRecordAccessorIndicesRight.getQuick(accessorIndex++), 1);
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
    private void instrumentSetLeftMethod(int nameIndex, int descIndex, IntList keyColumns) {
        asm.startMethod(nameIndex, descIndex, 3, 2);
        int fieldIndex = 0;
        int accessorIndex = 0;
        for (int i = 0, n = keyColumns.size(); i < n; i++) {
            int index = keyColumns.getQuick(i);
            // make sure column index is valid in case of "descending sort" flag
            int columnIndex = (index > 0 ? index : -index) - 1;
            int accessorCount = fieldRecordAccessorCount.getQuick(i);
            for (int j = 0; j < accessorCount; j++) {
                // stack: []
                asm.aload(0);
                // stack: [RecordComparator]
                asm.aload(1);
                // stack: [RecordComparator, Record]
                asm.iconst(columnIndex);
                // stack: [RecordComparator, Record, columnIndex]
                asm.invokeInterface(fieldRecordAccessorIndicesLeft.getQuick(accessorIndex++), 1);
                // stack: [RecordComparator, field]
                asm.putfield(fieldIndices.getQuick(fieldIndex++));
                // stack: []
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

    private void poolFieldArtifacts(
            int compareMethodIndex,
            int thisClassIndex,
            int recordClassIndex,
            ColumnTypes columnTypes,
            IntList keyColumnIndices
    ) throws SqlException {
        typeMap.clear();
        fieldIndices.clear();
        fieldNameIndices.clear();
        fieldTypeIndices.clear();
        fieldRecordAccessorIndicesLeft.clear();
        fieldRecordAccessorIndicesRight.clear();
        fieldRecordAccessorCount.clear();
        comparatorAccessorIndices.clear();
        methodMap.clear();
        maxColumnSize = 0;

        // define names and types
        for (int i = 0, n = keyColumnIndices.size(); i < n; i++) {
            String fieldType;
            @SuppressWarnings("rawtypes") Class comparatorClass;
            String comparatorDesc = null;
            int index = keyColumnIndices.getQuick(i);

            if (index < 0) {
                index = -index;
            }

            // decrement to get real column index
            index--;

            int accessorCount = fieldRecordAccessorIndicesLeft.size();

            int columnType = columnTypes.getColumnType(index);
            int getterSigIndex;
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BOOLEAN:
                    fieldType = "Z";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)Z"), "getBool");
                    comparatorClass = Boolean.class;
                    break;
                case ColumnType.BYTE:
                    fieldType = "B";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)B"), "getByte");
                    comparatorClass = Byte.class;
                    break;
                case ColumnType.DOUBLE:
                    fieldType = "D";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)D"), "getDouble");
                    comparatorClass = Numbers.class;
                    break;
                case ColumnType.FLOAT:
                    fieldType = "F";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)F"), "getFloat");
                    comparatorClass = Numbers.class;
                    break;
                case ColumnType.GEOBYTE:
                    fieldType = "B";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)B"), "getGeoByte");
                    comparatorClass = Byte.class;
                    break;
                case ColumnType.GEOSHORT:
                    fieldType = "S";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)S"), "getGeoShort");
                    comparatorClass = Short.class;
                    break;
                case ColumnType.GEOINT:
                    fieldType = "I";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)I"), "getGeoInt");
                    comparatorClass = Integer.class;
                    break;
                case ColumnType.GEOLONG:
                    fieldType = "J";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)J"), "getGeoLong");
                    comparatorClass = Long.class;
                    break;
                case ColumnType.INT:
                    fieldType = "I";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)I"), "getInt");
                    comparatorClass = Integer.class;
                    break;
                case ColumnType.IPv4:
                    fieldType = "J";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)J"), "getLongIPv4");
                    comparatorClass = Long.class;
                    break;
                case ColumnType.LONG:
                    fieldType = "J";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)J"), "getLong");
                    comparatorClass = Long.class;
                    break;
                case ColumnType.DATE:
                    fieldType = "J";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)J"), "getDate");
                    comparatorClass = Long.class;
                    break;
                case ColumnType.TIMESTAMP:
                    fieldType = "J";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)J"), "getTimestamp");
                    comparatorClass = Long.class;
                    break;
                case ColumnType.SHORT:
                    fieldType = "S";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)S"), "getShort");
                    comparatorClass = Short.class;
                    break;
                case ColumnType.CHAR:
                    fieldType = "C";
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)C"), "getChar");
                    comparatorClass = Character.class;
                    break;
                case ColumnType.STRING:
                    getterSigIndex = asm.poolUtf8("(I)Ljava/lang/CharSequence;");
                    poolFieldRecordObjectAccessor(recordClassIndex, getterSigIndex, "getStr");
                    fieldType = "Ljava/lang/CharSequence;";
                    comparatorClass = Chars.class;
                    break;
                case ColumnType.LONG256:
                    getterSigIndex = asm.poolUtf8("(I)Lio/questdb/std/Long256;");
                    poolFieldRecordObjectAccessor(recordClassIndex, getterSigIndex, "getLong256");
                    fieldType = "Lio/questdb/std/Long256;";
                    comparatorClass = Long256Util.class;
                    break;
                case ColumnType.UUID:
                    getterSigIndex = asm.poolUtf8("(I)J");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getLong128Hi");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getLong128Lo");
                    fieldType = "J";
                    comparatorDesc = "(JJJJ)I";
                    comparatorClass = Uuid.class;
                    break;
                case ColumnType.LONG128:
                    getterSigIndex = asm.poolUtf8("(I)J");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getLong128Hi");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getLong128Lo");
                    fieldType = "J";
                    comparatorDesc = "(JJJJ)I";
                    comparatorClass = Long128.class;
                    break;
                case ColumnType.VARCHAR:
                    getterSigIndex = asm.poolUtf8("(I)Lio/questdb/std/str/Utf8Sequence;");
                    poolFieldRecordObjectAccessor(recordClassIndex, getterSigIndex, "getVarchar");
                    fieldType = "Lio/questdb/std/str/Utf8Sequence;";
                    comparatorClass = Utf8s.class;
                    break;
                case ColumnType.DECIMAL8:
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)B"), "getDecimal8");
                    fieldType = "B";
                    comparatorClass = Byte.class;
                    break;
                case ColumnType.DECIMAL16:
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)S"), "getDecimal16");
                    fieldType = "S";
                    comparatorClass = Short.class;
                    break;
                case ColumnType.DECIMAL32:
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)I"), "getDecimal32");
                    fieldType = "I";
                    comparatorClass = Integer.class;
                    break;
                case ColumnType.DECIMAL64:
                    poolFieldRecordAccessor(recordClassIndex, asm.poolUtf8("(I)J"), "getDecimal64");
                    fieldType = "J";
                    comparatorClass = Long.class;
                    break;
                case ColumnType.DECIMAL128:
                    getterSigIndex = asm.poolUtf8("(I)J");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getDecimal128Hi");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getDecimal128Lo");
                    fieldType = "J";
                    comparatorClass = Decimal128.class;
                    comparatorDesc = "(JJJJ)I";
                    break;
                case ColumnType.DECIMAL256:
                    getterSigIndex = asm.poolUtf8("(I)J");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getDecimal256HH");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getDecimal256HL");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getDecimal256LH");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getDecimal256LL");
                    fieldType = "J";
                    comparatorClass = Decimal256.class;
                    comparatorDesc = "(JJJJJJJJ)I";
                    break;
                case ColumnType.SYMBOL:
                    // SYMBOL
                    getterSigIndex = asm.poolUtf8("(I)Ljava/lang/CharSequence;");
                    poolFieldRecordObjectAccessor(recordClassIndex, getterSigIndex, "getSym");
                    fieldType = "Ljava/lang/CharSequence;";
                    comparatorClass = Chars.class;
                    comparatorDesc = "(Ljava/lang/CharSequence;Ljava/lang/CharSequence;)I";
                    break;
                default:
                    throw SqlException.$(0, "column type is not supported for order by: ").put(ColumnType.nameOf(columnType));
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

            accessorCount = fieldRecordAccessorIndicesLeft.size() - accessorCount;
            fieldRecordAccessorCount.add(accessorCount);

            // We assume that if the type needs multiple accessors to retrieve its values, all these accessor will return
            // the same type. For example, Long128 returns longs for getLong128Hi and getLong128Lo.
            for (int j = 0; j < accessorCount; j++) {
                fieldTypeIndices.add(typeIndex);
                fieldNameIndices.add(nameIndex = asm.poolUtf8().putAscii('f').put(i).putAscii('_').put(j).$());
                fieldIndices.add(asm.poolField(thisClassIndex, asm.poolNameAndType(nameIndex, typeIndex)));
            }

            // Longs counts for twice the size of other fields
            final int columnSize = accessorCount * (Chars.equals(fieldType, 'J') ? 2 : 1);
            maxColumnSize = Math.max(maxColumnSize, columnSize);

            comparatorAccessorIndices.add(
                    asm.poolMethod(asm.poolClass(comparatorClass),
                            asm.poolNameAndType(
                                    compareMethodIndex,
                                    comparatorDesc == null
                                            ? asm.poolUtf8().putAscii('(').put(fieldType).put(fieldType).putAscii(")I").$()
                                            : asm.poolUtf8(comparatorDesc))
                    ));
        }
    }

    private void poolFieldRecordAccessor(int recordClassIndex, int getterSigIndex, CharSequence getterName) {
        int getterNameIndex = asm.poolUtf8(getterName);
        int getterIndex = asm.poolNameAndType(getterNameIndex, getterSigIndex);
        int methodIndex = asm.poolInterfaceMethod(recordClassIndex, getterIndex);
        methodMap.putIfAbsent(getterName, methodIndex);
        fieldRecordAccessorIndicesLeft.add(methodIndex);
        fieldRecordAccessorIndicesRight.add(methodIndex);
    }

    private void poolFieldRecordObjectAccessor(int recordClassIndex, int getterSigIndex, CharSequence getterName) {
        int getterNameIndex = asm.poolUtf8().put(getterName).putAscii('A').$();
        int getterIndex = asm.poolNameAndType(getterNameIndex, getterSigIndex);
        int methodIndex = asm.poolInterfaceMethod(recordClassIndex, getterIndex);
        methodMap.putIfAbsent(getterName, methodIndex);
        fieldRecordAccessorIndicesLeft.add(methodIndex);

        getterNameIndex = asm.poolUtf8().put(getterName).putAscii('B').$();
        getterIndex = asm.poolNameAndType(getterNameIndex, getterSigIndex);
        methodIndex = asm.poolInterfaceMethod(recordClassIndex, getterIndex);
        methodMap.putIfAbsent(getterName, methodIndex);
        fieldRecordAccessorIndicesRight.add(methodIndex);
    }
}
