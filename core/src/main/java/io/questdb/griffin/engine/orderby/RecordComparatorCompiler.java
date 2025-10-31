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
import io.questdb.std.ByteList;
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
    private static final byte RECORD_TYPE_NORMAL = 0;
    private static final byte RECORD_TYPE_DECIMAL128 = RECORD_TYPE_NORMAL + 1;
    private static final byte RECORD_TYPE_DECIMAL256 = RECORD_TYPE_DECIMAL128 + 1;
    private final BytecodeAssembler asm;
    private final IntList branches = new IntList();
    private final IntList comparatorAccessorIndices = new IntList();
    private final IntList fieldIndices = new IntList();
    private final IntList fieldNameIndices = new IntList();
    // We store the pooled method indices for record accessor in 2 separate lists to separate
    // records that are stored (left) and those that are directly used in comparison (right).
    // This is mandatory for accessors that relies on objects to give a representation of their
    // values (for example, `getStrA` and `getStrB`).
    // As there can be multiple indices for a single method (if we need to pull multiple
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
    private final ByteList fieldRecordTypes = new ByteList();
    private final IntList fieldTypeIndices = new IntList();
    private final CharSequenceIntHashMap methodMap = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap typeMap = new CharSequenceIntHashMap();
    private int decimal128ClassIndex = -1;
    private int decimal128CtorIndex = -1;
    // Decimal128 and Decimal256 are retrieved by passing an instance of them to the Record.
    // To accommodate for this, we may need to add an instance of each to be used in the right path, while we
    // store the left variable as 2 or 4 longs.
    private int decimal128FieldIndex = -1;
    // getHigh, getLow
    private int decimal128GetterIndex = -1;
    // getDecimal128(...)
    private int decimal128RecordGetterIndex = -1;
    private int decimal256ClassIndex = -1;
    private int decimal256CtorIndex = -1;
    private int decimal256FieldIndex = -1;
    // getHh, ..., getLl
    private int decimal256GetterIndex = -1;
    // getDecimal256(...)
    private int decimal256RecordGetterIndex = -1;
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
        poolDecimals();
        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(fieldNameIndices.size());
        for (int i = 0, n = fieldNameIndices.size(); i < n; i++) {
            asm.defineField(fieldNameIndices.getQuick(i), fieldTypeIndices.getQuick(i));
        }
        asm.methodCount(3);
        compileConstructor();
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
            LOG.critical().$("could not create an instance of RecordComparator, cause: ").$(e).$();
            throw BytecodeException.INSTANCE;
        }
    }

    /**
     * void RecordComparator()
     * <p>
     * sets up decimal128/decimal256 instances if required, equivalent to:
     * void RecordComparator() {
     * decimal128 = new Decimal128();
     * decimal256 = new Decimal256();
     * }
     */
    private void compileConstructor() {
        // constructor method entry
        asm.startMethod(asm.getDefaultConstructorNameIndex(), asm.getDefaultConstructorDescIndex(), 3, 1);
        // code
        asm.aload(0);
        asm.invokespecial(asm.getObjectInitMethodIndex());

        // decimal128 = new Decimal128();
        if (decimal128FieldIndex != -1) {
            // stack: []
            asm.aload(0);
            // stack: [this]
            asm.new_(decimal128ClassIndex);
            // stack: [this, decimal128]
            asm.dup();
            // stack: [this, decimal128, decimal128]
            asm.invokespecial(decimal128CtorIndex);
            // stack: [this, decimal128]
            asm.putfield(decimal128FieldIndex);
            // stack: []
        }

        // decimal256 = new Decimal256();
        if (decimal256FieldIndex != -1) {
            // stack: []
            asm.aload(0);
            // stack: [this]
            asm.new_(decimal256ClassIndex);
            // stack: [this, decimal256]
            asm.dup();
            // stack: [this, decimal256, decimal256]
            asm.invokespecial(decimal256CtorIndex);
            // stack: [this, decimal256]
            asm.putfield(decimal256FieldIndex);
            // stack: []
        }

        asm.return_();
        asm.endMethodCode();
        // exceptions
        asm.putShort(0);
        // attribute count
        asm.putShort(0);
        asm.endMethod();
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

            int recordType = fieldRecordTypes.getQuick(i);
            // Big Decimals are a bit special, they need to send an object to be filled with the
            // value instead of returning the value directly.
            switch (recordType) {
                case RECORD_TYPE_DECIMAL128 -> {
                    // stack: [...fields]
                    asm.aload(1);
                    // stack: [...fields, record]
                    asm.iconst(columnIndex);
                    // stack: [...fields, record, columnIndex]
                    asm.aload(0);
                    // stack: [...fields, record, columnIndex, this]
                    asm.getfield(decimal128FieldIndex);
                    // stack: [...fields, record, columnIndex, decimal128]
                    asm.dup_x2();
                    // stack: [...fields, decimal128, record, columnIndex, decimal128]
                    asm.invokeInterface(decimal128RecordGetterIndex, 2);
                    // stack: [...fields, decimal128]
                }
                case RECORD_TYPE_DECIMAL256 -> {
                    // stack: [...fields]
                    asm.aload(1);
                    // stack: [...fields, record]
                    asm.iconst(columnIndex);
                    // stack: [...fields, record, columnIndex]
                    asm.aload(0);
                    // stack: [...fields, record, columnIndex, this]
                    asm.getfield(decimal256FieldIndex);
                    // stack: [...fields, record, columnIndex, decimal256]
                    asm.dup_x2();
                    // stack: [...fields, decimal256, record, columnIndex, decimal256]
                    asm.invokeInterface(decimal256RecordGetterIndex, 2);
                    // stack: [...fields, decimal256]
                }
                default -> {
                    // Loads right side of comparator
                    for (int j = 0; j < accessorCount; j++) {
                        asm.aload(1);
                        asm.iconst(columnIndex);
                        asm.invokeInterface(fieldRecordAccessorIndicesRight.getQuick(accessorIndex++), 1);
                    }
                }
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
            int recordType = fieldRecordTypes.getQuick(i);
            switch (recordType) {
                case RECORD_TYPE_DECIMAL128 -> {
                    fieldIndex = instrumentSetLeftMethodDecimal128(columnIndex, fieldIndex);
                }
                case RECORD_TYPE_DECIMAL256 -> {
                    fieldIndex = instrumentSetLeftMethodDecimal256(columnIndex, fieldIndex);
                }
                default -> {
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
     * Emit the bytecode to retrieve the value of a decimal128 from a record and set it in the reserved fields:
     * record.getDecimal128(colIndex, decimal128);
     * fX_0 = decimal128.getHigh();
     * fX_1 = decimal128.getLow();
     */
    private int instrumentSetLeftMethodDecimal128(int colIndex, int fieldIndex) {
        // stack: []
        {
            // record.getDecimal128(colIndex, decimal128);
            asm.aload(1);
            // stack: [Record]
            asm.iconst(colIndex);
            // stack: [Record, columnIndex]
            asm.aload(0);
            // stack: [Record, columnIndex, this]
            asm.getfield(decimal128FieldIndex);
            // stack: [Record, columnIndex, decimal128]
            asm.invokeInterface(decimal128RecordGetterIndex, 2);
            // stack: []
        }
        for (int i = 0; i < 2; i++) {
            // fx_i = decimal128.getX();
            asm.aload(0);
            // stack: [RecordComparator]
            asm.dup();
            // stack: [RecordComparator, RecordComparator]
            asm.getfield(decimal128FieldIndex);
            // stack: [RecordComparator, decimal128]
            asm.invokeVirtual(decimal128GetterIndex + i);
            // stack: [RecordComparator, field]
            asm.putfield(fieldIndices.getQuick(fieldIndex++));
            // stack: []
        }
        return fieldIndex;
    }

    /**
     * Emit the bytecode to retrieve the value of a decimal256 from a record and set it in the reserved fields:
     * record.getDecimal256(colIndex, decimal256);
     * fX_0 = decimal256.getHh();
     * fX_1 = decimal256.getHl();
     * fX_2 = decimal256.getLh();
     * fX_3 = decimal256.getLl();
     */
    private int instrumentSetLeftMethodDecimal256(int colIndex, int fieldIndex) {
        // stack: []
        {
            // record.getDecimal256(colIndex, decimal256);
            asm.aload(1);
            // stack: [Record]
            asm.iconst(colIndex);
            // stack: [Record, columnIndex]
            asm.aload(0);
            // stack: [Record, columnIndex, this]
            asm.getfield(decimal256FieldIndex);
            // stack: [Record, columnIndex, decimal256]
            asm.invokeInterface(decimal256RecordGetterIndex, 2);
            // stack: []
        }
        for (int i = 0; i < 4; i++) {
            // fx_i = decimal256.getXx();
            asm.aload(0);
            // stack: [RecordComparator]
            asm.dup();
            // stack: [RecordComparator, RecordComparator]
            asm.getfield(decimal256FieldIndex);
            // stack: [RecordComparator, decimal256]
            asm.invokeVirtual(decimal256GetterIndex + i);
            // stack: [RecordComparator, field]
            asm.putfield(fieldIndices.getQuick(fieldIndex++));
            // stack: []
        }
        return fieldIndex;
    }

    private void poolDecimals() {
        if (decimal128FieldIndex != -1) {
            decimal128ClassIndex = asm.poolClass(Decimal128.class);
            decimal128CtorIndex = asm.poolMethod(decimal128ClassIndex, asm.getDefaultConstructorSigIndex());
            int sigIndex = asm.poolUtf8("()J");
            int getHighIndex = asm.poolNameAndType(asm.poolUtf8("getHigh"), sigIndex);
            int getLowIndex = asm.poolNameAndType(asm.poolUtf8("getLow"), sigIndex);
            decimal128GetterIndex = asm.poolMethod(decimal128ClassIndex, getHighIndex);
            asm.poolMethod(decimal128ClassIndex, getLowIndex);

            decimal128RecordGetterIndex = asm.poolInterfaceMethod(asm.poolClass(Record.class), "getDecimal128", "(ILio/questdb/std/Decimal128;)V");
        }

        if (decimal256FieldIndex != -1) {
            decimal256ClassIndex = asm.poolClass(Decimal256.class);
            decimal256CtorIndex = asm.poolMethod(decimal256ClassIndex, asm.getDefaultConstructorSigIndex());
            int sigIndex = asm.poolUtf8("()J");
            int getHhIndex = asm.poolNameAndType(asm.poolUtf8("getHh"), sigIndex);
            int getHlIndex = asm.poolNameAndType(asm.poolUtf8("getHl"), sigIndex);
            int getLhIndex = asm.poolNameAndType(asm.poolUtf8("getLh"), sigIndex);
            int getLlIndex = asm.poolNameAndType(asm.poolUtf8("getLl"), sigIndex);
            decimal256GetterIndex = asm.poolMethod(decimal256ClassIndex, getHhIndex);
            asm.poolMethod(decimal256ClassIndex, getHlIndex);
            asm.poolMethod(decimal256ClassIndex, getLhIndex);
            asm.poolMethod(decimal256ClassIndex, getLlIndex);

            decimal256RecordGetterIndex = asm.poolInterfaceMethod(asm.poolClass(Record.class), "getDecimal256", "(ILio/questdb/std/Decimal256;)V");
        }
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
        decimal128FieldIndex = -1;
        decimal256FieldIndex = -1;
        fieldRecordTypes.clear();

        // define names and types
        for (int i = 0, n = keyColumnIndices.size(); i < n; i++) {
            CharSequence fieldType;
            @SuppressWarnings("rawtypes") Class comparatorClass;
            CharSequence comparatorDesc = null;
            int index = keyColumnIndices.getQuick(i);

            if (index < 0) {
                index = -index;
            }

            // decrement to get real column index
            index--;

            int fieldCount = 1;

            int columnType = columnTypes.getColumnType(index);
            int getterSigIndex;
            byte fieldRecordType = RECORD_TYPE_NORMAL;
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
                    fieldCount = 2;
                    comparatorDesc = "(JJJJ)I";
                    comparatorClass = Uuid.class;
                    break;
                case ColumnType.LONG128:
                    getterSigIndex = asm.poolUtf8("(I)J");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getLong128Hi");
                    poolFieldRecordAccessor(recordClassIndex, getterSigIndex, "getLong128Lo");
                    fieldType = "J";
                    fieldCount = 2;
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
                    fieldType = "J";
                    fieldCount = 2;
                    comparatorClass = Decimal128.class;
                    comparatorDesc = "(JJLio/questdb/std/Decimal128;)I";
                    fieldRecordType = RECORD_TYPE_DECIMAL128;
                    if (decimal128FieldIndex == -1) {
                        int typeIndex = registerType("Lio/questdb/std/Decimal128;");
                        int fieldNameIndex = asm.poolUtf8("decimal128");
                        fieldTypeIndices.add(typeIndex);
                        fieldNameIndices.add(fieldNameIndex);
                        decimal128FieldIndex = asm.poolField(thisClassIndex, asm.poolNameAndType(fieldNameIndex, typeIndex));
                    }
                    break;
                case ColumnType.DECIMAL256:
                    fieldType = "J";
                    fieldCount = 4;
                    comparatorClass = Decimal256.class;
                    comparatorDesc = "(JJJJLio/questdb/std/Decimal256;)I";
                    fieldRecordType = RECORD_TYPE_DECIMAL256;
                    if (decimal256FieldIndex == -1) {
                        int typeIndex = registerType("Lio/questdb/std/Decimal256;");
                        int fieldNameIndex = asm.poolUtf8("decimal256");
                        fieldTypeIndices.add(typeIndex);
                        fieldNameIndices.add(fieldNameIndex);
                        decimal256FieldIndex = asm.poolField(thisClassIndex, asm.poolNameAndType(fieldNameIndex, typeIndex));
                    }
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
            fieldRecordTypes.add(fieldRecordType);

            int nameIndex;
            int typeIndex = registerType(fieldType);

            fieldRecordAccessorCount.add(fieldCount);

            // We assume that if the type needs multiple accessors to retrieve its values, all these accessor will return
            // the same type. For example, Long128 returns longs for getLong128Hi and getLong128Lo.
            for (int j = 0; j < fieldCount; j++) {
                fieldTypeIndices.add(typeIndex);
                fieldNameIndices.add(nameIndex = asm.poolUtf8().putAscii('f').put(i).putAscii('_').put(j).$());
                fieldIndices.add(asm.poolField(thisClassIndex, asm.poolNameAndType(nameIndex, typeIndex)));
            }

            // Longs counts for twice the size of other fields
            final int columnSize = fieldCount * (Chars.equals(fieldType, 'J') ? 2 : 1);
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

    private int registerType(CharSequence typeName) {
        int typeIndex;
        int keyIndex = typeMap.keyIndex(typeName);
        if (keyIndex > -1) {
            typeMap.putAt(keyIndex, typeName, typeIndex = asm.poolUtf8(typeName));
        } else {
            typeIndex = typeMap.valueAt(keyIndex);
        }
        return typeIndex;
    }
}
