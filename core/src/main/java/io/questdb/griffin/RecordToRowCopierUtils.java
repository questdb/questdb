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

package io.questdb.griffin;

import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DoubleArrayParser;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public class RecordToRowCopierUtils {

    private RecordToRowCopierUtils() {
    }

    // Creates data type converter.
    // INT and LONG NaN values are cast to their representation rather than Double or Float NaN.
    public static RecordToRowCopier generateCopier(
            BytecodeAssembler asm,
            ColumnTypes from,
            RecordMetadata to,
            ColumnFilter toColumnFilter
    ) {
        int timestampIndex = to.getTimestampIndex();
        asm.init(RecordToRowCopier.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("io/questdb/griffin/rowcopier"));
        int interfaceClassIndex = asm.poolClass(RecordToRowCopier.class);

        // Character        Type        Interpretation
        // B                byte        signed byte
        // C                char        Unicode character code point in the Basic Multilingual Plane, encoded with UTF-16
        // D                double      double-precision floating-point value
        // F                float       single-precision floating-point value
        // I                int         32-bit integer
        // J                long        64-bit integer
        // L ClassName ;    reference   an instance of class ClassName
        // S                short       signed short
        // Z                boolean     true or false
        // [                reference   one array dimension

        int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        int rGetIPv4 = asm.poolInterfaceMethod(Record.class, "getIPv4", "(I)I");
        int rGetGeoInt = asm.poolInterfaceMethod(Record.class, "getGeoInt", "(I)I");
        int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        int rGetGeoLong = asm.poolInterfaceMethod(Record.class, "getGeoLong", "(I)J");
        int rGetLong256 = asm.poolInterfaceMethod(Record.class, "getLong256A", "(I)Lio/questdb/std/Long256;");
        int rGetLong128Lo = asm.poolInterfaceMethod(Record.class, "getLong128Lo", "(I)J");
        int rGetLong128Hi = asm.poolInterfaceMethod(Record.class, "getLong128Hi", "(I)J");

        int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        int rGetTimestamp = asm.poolInterfaceMethod(Record.class, "getTimestamp", "(I)J");
        //
        int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        int rGetGeoByte = asm.poolInterfaceMethod(Record.class, "getGeoByte", "(I)B");
        int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        int rGetGeoShort = asm.poolInterfaceMethod(Record.class, "getGeoShort", "(I)S");
        int rGetChar = asm.poolInterfaceMethod(Record.class, "getChar", "(I)C");
        int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        int rGetSym = asm.poolInterfaceMethod(Record.class, "getSymA", "(I)Ljava/lang/CharSequence;");
        int rGetStrA = asm.poolInterfaceMethod(Record.class, "getStrA", "(I)Ljava/lang/CharSequence;");
        int rGetBin = asm.poolInterfaceMethod(Record.class, "getBin", "(I)Lio/questdb/std/BinarySequence;");
        int rGetVarchar = asm.poolInterfaceMethod(Record.class, "getVarcharA", "(I)Lio/questdb/std/str/Utf8Sequence;");
        int rGetArray = asm.poolInterfaceMethod(Record.class, "getArray", "(II)Lio/questdb/cairo/arr/ArrayView;");
        //
        int wPutInt = asm.poolInterfaceMethod(TableWriter.Row.class, "putInt", "(II)V");
        int wPutIPv4 = asm.poolInterfaceMethod(TableWriter.Row.class, "putIPv4", "(II)V");
        int wPutLong = asm.poolInterfaceMethod(TableWriter.Row.class, "putLong", "(IJ)V");
        int wPutLong256 = asm.poolInterfaceMethod(TableWriter.Row.class, "putLong256", "(ILio/questdb/std/Long256;)V");
        int wPutLong256Utf8 = asm.poolInterfaceMethod(TableWriter.Row.class, "putLong256Utf8", "(ILio/questdb/std/str/DirectUtf8Sequence;)V");
        int wPutLong128 = asm.poolInterfaceMethod(TableWriter.Row.class, "putLong128", "(IJJ)V");
        int wPutUuidStr = asm.poolInterfaceMethod(TableWriter.Row.class, "putUuid", "(ILjava/lang/CharSequence;)V");
        int wPutUuidUtf8 = asm.poolInterfaceMethod(TableWriter.Row.class, "putUuidUtf8", "(ILio/questdb/std/str/Utf8Sequence;)V");
        int wPutDate = asm.poolInterfaceMethod(TableWriter.Row.class, "putDate", "(IJ)V");
        int wPutTimestamp = asm.poolInterfaceMethod(TableWriter.Row.class, "putTimestamp", "(IJ)V");
        //
        int wPutByte = asm.poolInterfaceMethod(TableWriter.Row.class, "putByte", "(IB)V");
        int wPutShort = asm.poolInterfaceMethod(TableWriter.Row.class, "putShort", "(IS)V");
        int wPutBool = asm.poolInterfaceMethod(TableWriter.Row.class, "putBool", "(IZ)V");
        int wPutFloat = asm.poolInterfaceMethod(TableWriter.Row.class, "putFloat", "(IF)V");
        int wPutDouble = asm.poolInterfaceMethod(TableWriter.Row.class, "putDouble", "(ID)V");
        int wPutSym = asm.poolInterfaceMethod(TableWriter.Row.class, "putSym", "(ILjava/lang/CharSequence;)V");
        int wPutSymChar = asm.poolInterfaceMethod(TableWriter.Row.class, "putSym", "(IC)V");
        int wPutStr = asm.poolInterfaceMethod(TableWriter.Row.class, "putStr", "(ILjava/lang/CharSequence;)V");
        int wPutGeoStr = asm.poolInterfaceMethod(TableWriter.Row.class, "putGeoStr", "(ILjava/lang/CharSequence;)V");
        int wPutGeoVarchar = asm.poolInterfaceMethod(TableWriter.Row.class, "putGeoVarchar", "(ILio/questdb/std/str/Utf8Sequence;)V");
        int wPutVarchar = asm.poolInterfaceMethod(TableWriter.Row.class, "putVarchar", "(ILio/questdb/std/str/Utf8Sequence;)V");
        int wPutArray = asm.poolInterfaceMethod(TableWriter.Row.class, "putArray", "(ILio/questdb/cairo/arr/ArrayView;)V");

        int implicitCastCharAsByte = asm.poolMethod(SqlUtil.class, "implicitCastCharAsByte", "(CI)B");
        int implicitCastCharAsGeoHash = asm.poolMethod(SqlUtil.class, "implicitCastCharAsGeoHash", "(CI)B");
        int implicitCastStrAsFloat = asm.poolMethod(SqlUtil.class, "implicitCastStrAsFloat", "(Ljava/lang/CharSequence;)F");
        int implicitCastStrAsDouble = asm.poolMethod(SqlUtil.class, "implicitCastStrAsDouble", "(Ljava/lang/CharSequence;)D");
        int implicitCastStrAsByte = asm.poolMethod(SqlUtil.class, "implicitCastStrAsByte", "(Ljava/lang/CharSequence;)B");
        int implicitCastStrAsShort = asm.poolMethod(SqlUtil.class, "implicitCastStrAsShort", "(Ljava/lang/CharSequence;)S");
        int implicitCastStrAsChar = asm.poolMethod(SqlUtil.class, "implicitCastStrAsChar", "(Ljava/lang/CharSequence;)C");
        int implicitCastStrAsInt = asm.poolMethod(SqlUtil.class, "implicitCastStrAsInt", "(Ljava/lang/CharSequence;)I");
        int implicitCastStrAsIPv4 = asm.poolMethod(SqlUtil.class, "implicitCastStrAsIPv4", "(Ljava/lang/CharSequence;)I");
        int implicitCastUtf8StrAsIPv4 = asm.poolMethod(SqlUtil.class, "implicitCastStrAsIPv4", "(Lio/questdb/std/str/Utf8Sequence;)I");
        int implicitCastStrAsLong = asm.poolMethod(SqlUtil.class, "implicitCastStrAsLong", "(Ljava/lang/CharSequence;)J");
        int implicitCastStrAsLong256 = asm.poolMethod(SqlUtil.class, "implicitCastStrAsLong256", "(Ljava/lang/CharSequence;)Lio/questdb/griffin/engine/functions/constants/Long256Constant;");
        int implicitCastStrAsDate = asm.poolMethod(SqlUtil.class, "implicitCastStrAsDate", "(Ljava/lang/CharSequence;)J");
        int implicitCastStrAsTimestamp = asm.poolInterfaceMethod(TimestampDriver.class, "implicitCast", "(Ljava/lang/CharSequence;)J");
        int implicitCastShortAsByte = asm.poolMethod(SqlUtil.class, "implicitCastShortAsByte", "(S)B");
        int implicitCastIntAsByte = asm.poolMethod(SqlUtil.class, "implicitCastIntAsByte", "(I)B");
        int implicitCastLongAsByte = asm.poolMethod(SqlUtil.class, "implicitCastLongAsByte", "(J)B");
        int implicitCastFloatAsByte = asm.poolMethod(SqlUtil.class, "implicitCastFloatAsByte", "(F)B");
        int implicitCastDoubleAsByte = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsByte", "(D)B");

        int implicitCastVarcharAsLong = asm.poolMethod(SqlUtil.class, "implicitCastVarcharAsLong", "(Lio/questdb/std/str/Utf8Sequence;)J");
        int implicitCastVarcharAsShort = asm.poolMethod(SqlUtil.class, "implicitCastVarcharAsShort", "(Lio/questdb/std/str/Utf8Sequence;)S");
        int implicitCastVarcharAsInt = asm.poolMethod(SqlUtil.class, "implicitCastVarcharAsInt", "(Lio/questdb/std/str/Utf8Sequence;)I");
        int implicitCastVarcharAsByte = asm.poolMethod(SqlUtil.class, "implicitCastVarcharAsByte", "(Lio/questdb/std/str/Utf8Sequence;)B");
        int implicitCastVarcharAsChar = asm.poolMethod(SqlUtil.class, "implicitCastVarcharAsChar", "(Lio/questdb/std/str/Utf8Sequence;)C");
        int implicitCastVarcharAsFloat = asm.poolMethod(SqlUtil.class, "implicitCastVarcharAsFloat", "(Lio/questdb/std/str/Utf8Sequence;)F");
        int implicitCastVarcharAsDouble = asm.poolMethod(SqlUtil.class, "implicitCastVarcharAsDouble", "(Lio/questdb/std/str/Utf8Sequence;)D");

        int implicitCastIntAsShort = asm.poolMethod(SqlUtil.class, "implicitCastIntAsShort", "(I)S");
        int implicitCastLongAsShort = asm.poolMethod(SqlUtil.class, "implicitCastLongAsShort", "(J)S");
        int implicitCastFloatAsShort = asm.poolMethod(SqlUtil.class, "implicitCastFloatAsShort", "(F)S");
        int implicitCastDoubleAsShort = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsShort", "(D)S");

        int implicitCastLongAsInt = asm.poolMethod(SqlUtil.class, "implicitCastLongAsInt", "(J)I");
        int implicitCastFloatAsInt = asm.poolMethod(SqlUtil.class, "implicitCastFloatAsInt", "(F)I");
        int implicitCastDoubleAsInt = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsInt", "(D)I");

        int implicitCastDateAsTimestamp = asm.poolInterfaceMethod(TimestampDriver.class, "fromDate", "(J)J");
        int implicitCastTimestampAsDate = asm.poolInterfaceMethod(TimestampDriver.class, "toDate", "(J)J");
        int implicitCastTimestampAsTimestamp = asm.poolInterfaceMethod(TimestampDriver.class, "from", "(JI)J");

        int implicitCastFloatAsLong = asm.poolMethod(SqlUtil.class, "implicitCastFloatAsLong", "(F)J");
        int implicitCastDoubleAsLong = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsLong", "(D)J");
        int implicitCastDoubleAsFloat = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsFloat", "(D)F");
        int wPutStrChar = asm.poolInterfaceMethod(TableWriter.Row.class, "putStr", "(IC)V");
        int wPutVarcharChar = asm.poolInterfaceMethod(TableWriter.Row.class, "putVarchar", "(IC)V");
        int wPutChar = asm.poolInterfaceMethod(TableWriter.Row.class, "putChar", "(IC)V");
        int wPutBin = asm.poolInterfaceMethod(TableWriter.Row.class, "putBin", "(ILio/questdb/std/BinarySequence;)V");
        int implicitCastGeoHashAsGeoHash = asm.poolMethod(SqlUtil.class, "implicitCastGeoHashAsGeoHash", "(JII)J");
        int transferUuidToStrCol = asm.poolMethod(RecordToRowCopierUtils.class, "transferUuidToStrCol", "(Lio/questdb/cairo/TableWriter$Row;IJJ)V");
        int transferUuidToVarcharCol = asm.poolMethod(RecordToRowCopierUtils.class, "transferUuidToVarcharCol", "(Lio/questdb/cairo/TableWriter$Row;IJJ)V");
        int transferVarcharToStrCol = asm.poolInterfaceMethod(TableWriter.Row.class, "putStrUtf8", "(ILio/questdb/std/str/DirectUtf8Sequence;)V");
        int transferVarcharToSymbolCol = asm.poolMethod(RecordToRowCopierUtils.class, "transferVarcharToSymbolCol", "(Lio/questdb/cairo/TableWriter$Row;ILio/questdb/std/str/Utf8Sequence;)V");
        int implicitCastVarcharAsTimestamp = asm.poolInterfaceMethod(TimestampDriver.class, "implicitCastVarchar", "(Lio/questdb/std/str/Utf8Sequence;)J");
        int transferVarcharToDateCol = asm.poolMethod(RecordToRowCopierUtils.class, "transferVarcharToDateCol", "(Lio/questdb/cairo/TableWriter$Row;ILio/questdb/std/str/Utf8Sequence;)V");
        int transferStrToVarcharCol = asm.poolMethod(RecordToRowCopierUtils.class, "transferStrToVarcharCol", "(Lio/questdb/cairo/TableWriter$Row;ILjava/lang/CharSequence;)V");
        int getTimestampDriverRef = asm.poolMethod(ColumnType.class, "getTimestampDriver", "(I)Lio/questdb/cairo/TimestampDriver;");
        int validateArrayDimensionsAndTransferColString = asm.poolMethod(RecordToRowCopierUtils.class, "validateArrayDimensionsAndTransferCol", "(Lio/questdb/cairo/TableWriter$Row;ILio/questdb/cairo/arr/DoubleArrayParser;Ljava/lang/CharSequence;I)V");
        int validateArrayDimensionsAndTransferColVarchar = asm.poolMethod(RecordToRowCopierUtils.class, "validateArrayDimensionsAndTransferCol", "(Lio/questdb/cairo/TableWriter$Row;ILio/questdb/cairo/arr/DoubleArrayParser;Lio/questdb/std/str/Utf8Sequence;I)V");

        // in case of Geo Hashes column type can overflow short and asm.iconst() will not provide
        // the correct value.
        int n = toColumnFilter.getColumnCount();

        // pool column type constants
        int toColumnType_0 = asm.getPoolCount();
        int fromColumnType_0 = toColumnType_0 + 1;
        for (int i = 0; i < n; i++) {
            asm.poolIntConst(
                    to.getColumnType(
                            toColumnFilter.getColumnIndexFactored(i))
            );
            asm.poolIntConst(from.getColumnType(i));
        }

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lio/questdb/cairo/sql/Record;Lio/questdb/cairo/TableWriter$Row;)V");

        // if we have to do implicit cast from STRING to ARRAYs then we need to create a parser
        // the parser is instantiated in a constructor and stored in a field.
        // if parser is not required then we do not define and field and use just default (empty) constructor
        int parserFieldIndex = 0;
        int parserDescIndex = 0;
        int constructorNameIndex = 0;
        int constructorDescIndex = 0;
        int doubleArrayParserClassIndex = 0;
        int doubleArrayParserCtorIndex = 0;
        int parserFieldRef = 0;
        int objectCtorIndex = 0;
        boolean needsArrayParser = isArrayParserRequired(from, to, toColumnFilter, n);
        if (needsArrayParser) {
            parserFieldIndex = asm.poolUtf8("parser");
            parserDescIndex = asm.poolUtf8("Lio/questdb/cairo/arr/DoubleArrayParser;");
            constructorNameIndex = asm.poolUtf8("<init>");
            constructorDescIndex = asm.poolUtf8("()V");
            doubleArrayParserClassIndex = asm.poolClass(DoubleArrayParser.class);
            doubleArrayParserCtorIndex = asm.poolMethod(doubleArrayParserClassIndex, "<init>", "()V");
            int parserFieldNameAndType = asm.poolNameAndType(parserFieldIndex, parserDescIndex);
            parserFieldRef = asm.poolField(thisClassIndex, parserFieldNameAndType);
            objectCtorIndex = asm.poolMethod(Object.class, "<init>", "()V");
        }

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);

        if (needsArrayParser) {
            asm.fieldCount(1);
            asm.defineField(parserFieldIndex, parserDescIndex);
        } else {
            asm.fieldCount(0);
        }

        asm.methodCount(2);

        if (needsArrayParser) {
            asm.startMethod(constructorNameIndex, constructorDescIndex, 3, 1);
            // call super()
            asm.aload(0);
            asm.invokespecial(objectCtorIndex);

            // initialize parser field
            asm.aload(0);
            asm.new_(doubleArrayParserClassIndex);
            asm.dup();
            asm.invokespecial(doubleArrayParserCtorIndex);
            asm.putfield(parserFieldRef);

            asm.return_();
            asm.endMethodCode();
            asm.putShort(0); // exceptions
            asm.putShort(0); // attributes
            asm.endMethod();
        } else {
            asm.defineDefaultConstructor();
        }

        asm.startMethod(copyNameIndex, copySigIndex, 15, 5);

        for (int i = 0; i < n; i++) {

            final int toColumnIndex = toColumnFilter.getColumnIndexFactored(i);
            // do not copy timestamp, it will be copied externally to this helper

            if (toColumnIndex == timestampIndex) {
                continue;
            }

            final int toColumnType = to.getColumnType(toColumnIndex);
            final int fromColumnType = from.getColumnType(i);
            int fromColumnTypeTag = ColumnType.tagOf(fromColumnType);
            final int toColumnTypeTag = ColumnType.tagOf(toColumnType);
            final int toColumnWriterIndex = to.getWriterIndex(toColumnIndex);

            int timestampTypeRef = 0;
            // determine the `TimestampDriver` during bytecode generation to avoid
            // calling `ColumnType.getTimestampDriver()` at runtime much times.
            if (toColumnTypeTag == ColumnType.DATE && fromColumnTypeTag == ColumnType.TIMESTAMP) { // Timestamp -> Date
                timestampTypeRef = fromColumnType_0 + 2 * i;
            } else if (toColumnTypeTag == ColumnType.TIMESTAMP && (fromColumnTypeTag == ColumnType.DATE || // Date -> Timestamp
                    fromColumnTypeTag == ColumnType.VARCHAR || fromColumnTypeTag == ColumnType.STRING || // Varchar -> Timestamp or String -> Timestamp
                    (fromColumnTypeTag == ColumnType.TIMESTAMP && fromColumnType != toColumnType))) { // Timestamp -> Timestamp
                timestampTypeRef = toColumnType_0 + 2 * i;
            }

            // todo: this branch is not great, but we need parser
            // inside the stack building block, not sure how to do it better
            if (toColumnTypeTag == ColumnType.ARRAY &&
                    (ColumnType.tagOf(fromColumnType) == ColumnType.STRING || ColumnType.tagOf(fromColumnType) == ColumnType.VARCHAR)
            ) {
                // Build stack with parser in the right position
                asm.aload(2);
                // Stack: [rowWriter]
                asm.iconst(toColumnWriterIndex);
                // Stack: [rowWriter, toColumnIndex]
                asm.aload(0);
                // Stack: [rowWriter, toColumnIndex, this]
                asm.getfield(parserFieldRef);
                // Stack: [rowWriter, toColumnIndex, parser]
                asm.aload(1);
                // Stack: [rowWriter, toColumnIndex, parser, record]
                asm.iconst(i);
                // Stack: [rowWriter, toColumnIndex, parser, record, fromColumnIndex]
            } else {
                // Original stack building
                asm.aload(2);
                // stack: [rowWriter]
                asm.iconst(toColumnWriterIndex);
                // stack: [rowWriter, toColumnIndex]

                if (timestampTypeRef != 0) {
                    asm.ldc(timestampTypeRef);
                    asm.invokeStatic(getTimestampDriverRef);
                    // stack: [rowWriter, toColumnIndex, timestampDriver]
                }

                asm.aload(1);
                // stack: [rowWriter, toColumnIndex, [timestampDriver], record]
                asm.iconst(i);
                // stack: [rowWriter, toColumnIndex, [timestampDriver], record, fromColumnIndex]
            }

            if (fromColumnTypeTag == ColumnType.NULL) {
                fromColumnTypeTag = toColumnTypeTag;
            }
            switch (fromColumnTypeTag) {
                case ColumnType.INT: // from
                    // stack: [rowWriter, toColumnIndex, record, fromColumnIndex]
                    asm.invokeInterface(rGetInt);
                    // stack: [rowWriter, toColumnIndex, int]
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastIntAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeStatic(implicitCastIntAsShort);
                            asm.invokeInterface(wPutShort, 2);
                            break;
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
                        case ColumnType.TIMESTAMP:
                            asm.i2l();
                            asm.invokeInterface(wPutTimestamp, 3);
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
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.IPv4: // from
                    assert toColumnTypeTag == ColumnType.IPv4;
                    asm.invokeInterface(rGetIPv4);
                    asm.invokeInterface(wPutIPv4, 2);
                    break;
                case ColumnType.LONG: // from
                    asm.invokeInterface(rGetLong);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastLongAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeStatic(implicitCastLongAsShort);
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.INT:
                            asm.invokeStatic(implicitCastLongAsInt);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeInterface(wPutTimestamp, 3);
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
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.DATE: // from
                    asm.invokeInterface(rGetDate);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastLongAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeStatic(implicitCastLongAsShort);
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.INT:
                            asm.invokeStatic(implicitCastLongAsInt);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeInterface(implicitCastDateAsTimestamp, 2);
                            asm.invokeInterface(wPutTimestamp, 3);
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
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.TIMESTAMP: // from
                    asm.invokeInterface(rGetTimestamp);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastLongAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeStatic(implicitCastLongAsShort);
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.INT:
                            asm.invokeStatic(implicitCastLongAsInt);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.FLOAT:
                            asm.l2f();
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.l2d();
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        case ColumnType.DATE:
                            asm.invokeInterface(implicitCastTimestampAsDate, 2);
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            if (fromColumnType != toColumnType && fromColumnType != ColumnType.NULL) {
                                asm.ldc(fromColumnType_0 + i * 2);
                                asm.invokeInterface(implicitCastTimestampAsTimestamp, 3);
                            }
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.BYTE: // from
                    asm.invokeInterface(rGetByte);
                    switch (toColumnTypeTag) {
                        case ColumnType.BOOLEAN:
                        case ColumnType.BYTE:
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
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
                        case ColumnType.TIMESTAMP:
                            asm.i2l();
                            asm.invokeInterface(wPutTimestamp, 3);
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
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.SHORT: // from
                    asm.invokeInterface(rGetShort);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastShortAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeInterface(wPutShort, 2);
                            break;
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
                        case ColumnType.TIMESTAMP:
                            asm.i2l();
                            asm.invokeInterface(wPutTimestamp, 3);
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
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.BOOLEAN: // from
                    assert toColumnType == ColumnType.BOOLEAN;
                    asm.invokeInterface(rGetBool);
                    asm.invokeInterface(wPutBool, 2);
                    break;
                case ColumnType.FLOAT: // from
                    asm.invokeInterface(rGetFloat);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastFloatAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeStatic(implicitCastFloatAsShort);
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.INT:
                            asm.invokeStatic(implicitCastFloatAsInt);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeStatic(implicitCastFloatAsLong);
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.invokeStatic(implicitCastFloatAsLong);
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeStatic(implicitCastFloatAsLong);
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.FLOAT:
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.f2d();
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.DOUBLE: // from
                    asm.invokeInterface(rGetDouble);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastDoubleAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeStatic(implicitCastDoubleAsShort);
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.INT:
                            asm.invokeStatic(implicitCastDoubleAsInt);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeStatic(implicitCastDoubleAsLong);
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.invokeStatic(implicitCastDoubleAsLong);
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeStatic(implicitCastDoubleAsLong);
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.FLOAT:
                            asm.invokeStatic(implicitCastDoubleAsFloat);
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.CHAR: // from
                    asm.invokeInterface(rGetChar);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.iconst(toColumnType);
                            asm.invokeStatic(implicitCastCharAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.iconst(toColumnType);
                            asm.invokeStatic(implicitCastCharAsByte);
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.CHAR:
                            asm.invokeInterface(wPutChar, 2);
                            break;
                        case ColumnType.INT:
                            asm.iconst(toColumnType);
                            asm.invokeStatic(implicitCastCharAsByte);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.iconst(toColumnType);
                            asm.invokeStatic(implicitCastCharAsByte);
                            asm.i2l();
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            asm.iconst(toColumnType);
                            asm.invokeStatic(implicitCastCharAsByte);
                            asm.i2l();
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.ldc(toColumnType_0 + i * 2);
                            asm.invokeStatic(implicitCastCharAsByte);
                            asm.i2l();
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.FLOAT:
                            asm.iconst(toColumnType);
                            asm.invokeStatic(implicitCastCharAsByte);
                            asm.i2f();
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.iconst(toColumnType);
                            asm.invokeStatic(implicitCastCharAsByte);
                            asm.i2d();
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        case ColumnType.STRING:
                            asm.invokeInterface(wPutStrChar, 2);
                            break;
                        case ColumnType.VARCHAR:
                            asm.invokeInterface(wPutVarcharChar, 2);
                            break;
                        case ColumnType.SYMBOL:
                            asm.invokeInterface(wPutSymChar, 2);
                            break;
                        case ColumnType.GEOBYTE:
                            asm.ldc(toColumnType_0 + i * 2);
                            asm.invokeStatic(implicitCastCharAsGeoHash);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.SYMBOL: // from
                    asm.invokeInterface(rGetSym);
                    switch (toColumnTypeTag) {
                        case ColumnType.SYMBOL:
                            asm.invokeInterface(wPutSym, 2);
                            break;
                        case ColumnType.STRING:
                            asm.invokeInterface(wPutStr, 2);
                            break;
                        case ColumnType.VARCHAR:
                            asm.invokeStatic(transferStrToVarcharCol);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.VARCHAR: // from
                    switch (toColumnTypeTag) {
                        case ColumnType.VARCHAR:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeInterface(wPutVarchar, 2);
                            break;
                        case ColumnType.ARRAY:
                            // Initial stack: [rowWriter, toColumnIndex, parser, record, fromColumnIndex]
                            asm.invokeInterface(rGetVarchar);
                            // Stack: [rowWriter, toColumnIndex, parser, varchar]
                            asm.iconst(toColumnType);
                            // Stack: [rowWriter, toColumnIndex, parser, varchar, toColumnType]
                            asm.invokeStatic(validateArrayDimensionsAndTransferColVarchar);
                            // Stack: []
                            break;
                        case ColumnType.STRING:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeInterface(transferVarcharToStrCol, 2);
                            break;
                        case ColumnType.IPv4:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(implicitCastUtf8StrAsIPv4);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(implicitCastVarcharAsLong);
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(implicitCastVarcharAsShort);
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.INT:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(implicitCastVarcharAsInt);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.BYTE:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(implicitCastVarcharAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.CHAR:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(implicitCastVarcharAsChar);
                            asm.invokeInterface(wPutChar, 2);
                            break;
                        case ColumnType.FLOAT:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(implicitCastVarcharAsFloat);
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(implicitCastVarcharAsDouble);
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        case ColumnType.UUID:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeInterface(wPutUuidUtf8, 2);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeInterface(implicitCastVarcharAsTimestamp, 1);
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.SYMBOL:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(transferVarcharToSymbolCol);
                            break;
                        case ColumnType.DATE:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeStatic(transferVarcharToDateCol);
                            break;
                        case ColumnType.GEOBYTE:
                        case ColumnType.GEOSHORT:
                        case ColumnType.GEOINT:
                        case ColumnType.GEOLONG:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeInterface(wPutGeoVarchar, 2);
                            break;
                        case ColumnType.LONG256:
                            asm.invokeInterface(rGetVarchar);
                            asm.invokeInterface(wPutLong256Utf8, 2);
                            break;
                        default:
                            assert false;
                    }
                    break;
                case ColumnType.STRING: // from
                    // This is generic code, and it acts on a record
                    // whereas Functions support string to primitive conversions, Record instances
                    // do not. This is because functions are aware of their return type but records
                    // would have to do expensive checks to decide which conversion would be required
                    switch (toColumnTypeTag) {
                        case ColumnType.ARRAY:
                            // Initial stack: [rowWriter, toColumnIndex, parser, record, fromColumnIndex]
                            asm.invokeInterface(rGetStrA);
                            // Stack: [rowWriter, toColumnIndex, parser, string]
                            asm.iconst(toColumnType);
                            // Stack: [rowWriter, toColumnIndex, parser, string, toColumnType]
                            asm.invokeStatic(validateArrayDimensionsAndTransferColString);
                            // Stack: []
                            break;

                        case ColumnType.BYTE:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsShort);
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.CHAR:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsChar);
                            asm.invokeInterface(wPutChar, 2);
                            break;
                        case ColumnType.INT:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsInt);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.IPv4:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsIPv4);
                            asm.invokeInterface(wPutIPv4, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsLong);
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.FLOAT:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsFloat);
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsDouble);
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        case ColumnType.SYMBOL:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeInterface(wPutSym, 2);
                            break;
                        case ColumnType.DATE:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsDate);
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeInterface(implicitCastStrAsTimestamp, 1);
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.GEOBYTE:
                        case ColumnType.GEOSHORT:
                        case ColumnType.GEOINT:
                        case ColumnType.GEOLONG:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeInterface(wPutGeoStr, 2);
                            break;
                        case ColumnType.STRING:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeInterface(wPutStr, 2);
                            break;
                        case ColumnType.VARCHAR:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(transferStrToVarcharCol);
                            break;
                        case ColumnType.UUID:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeInterface(wPutUuidStr, 2);
                            break;
                        case ColumnType.LONG256:
                            asm.invokeInterface(rGetStrA);
                            asm.invokeStatic(implicitCastStrAsLong256);
                            asm.invokeInterface(wPutLong256, 2);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.BINARY: // from
                    assert toColumnTypeTag == ColumnType.BINARY;
                    asm.invokeInterface(rGetBin);
                    asm.invokeInterface(wPutBin, 2);
                    break;
                case ColumnType.LONG256: // from
                    assert toColumnTypeTag == ColumnType.LONG256;
                    asm.invokeInterface(rGetLong256);
                    asm.invokeInterface(wPutLong256, 2);
                    break;
                case ColumnType.GEOBYTE: // from
                    asm.invokeInterface(rGetGeoByte, 1);
                    if (fromColumnType != toColumnType && (fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOBYTE)) {
                        // truncate within the same storage type
                        asm.i2l();
                        asm.ldc(fromColumnType_0 + i * 2);
                        // toColumnType
                        asm.ldc(toColumnType_0 + i * 2);
                        asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                        asm.l2i();
                        asm.i2b();
                    }
                    asm.invokeInterface(wPutByte, 2);
                    break;
                case ColumnType.GEOSHORT: // from
                    asm.invokeInterface(rGetGeoShort, 1);
                    if (ColumnType.tagOf(toColumnType) == ColumnType.GEOBYTE) {
                        asm.i2l();
                        asm.ldc(fromColumnType_0 + i * 2);
                        asm.ldc(toColumnType_0 + i * 2);
                        asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                        asm.l2i();
                        asm.i2b();
                        asm.invokeInterface(wPutByte, 2);
                    } else if (fromColumnType != toColumnType && fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOSHORT) {
                        asm.i2l();
                        asm.ldc(fromColumnType_0 + i * 2);
                        asm.ldc(toColumnType_0 + i * 2);
                        asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                        asm.l2i();
                        asm.i2s();
                        asm.invokeInterface(wPutShort, 2);
                    } else {
                        asm.invokeInterface(wPutShort, 2);
                    }
                    break;
                case ColumnType.GEOINT: // from
                    asm.invokeInterface(rGetGeoInt, 1);
                    switch (ColumnType.tagOf(toColumnType)) {
                        case ColumnType.GEOBYTE:
                            asm.i2l();
                            asm.ldc(fromColumnType_0 + i * 2);
                            asm.ldc(toColumnType_0 + i * 2);
                            asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                            asm.l2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.GEOSHORT:
                            asm.i2l();
                            asm.ldc(fromColumnType_0 + i * 2);
                            asm.ldc(toColumnType_0 + i * 2);
                            asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                            asm.l2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.GEOINT:
                            if (fromColumnType != toColumnType && fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOINT) {
                                asm.i2l();
                                asm.ldc(fromColumnType_0 + i * 2);
                                asm.ldc(toColumnType_0 + i * 2);
                                asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                                asm.l2i();
                            }
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.GEOLONG: // from
                    asm.invokeInterface(rGetGeoLong, 1);
                    switch (ColumnType.tagOf(toColumnType)) {
                        case ColumnType.GEOBYTE:
                            asm.ldc(fromColumnType_0 + i * 2);
                            asm.ldc(toColumnType_0 + i * 2);
                            asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                            asm.l2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.GEOSHORT:
                            asm.ldc(fromColumnType_0 + i * 2);
                            asm.ldc(toColumnType_0 + i * 2);
                            asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                            asm.l2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.GEOINT:
                            asm.ldc(fromColumnType_0 + i * 2);
                            asm.ldc(toColumnType_0 + i * 2);
                            asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                            asm.l2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.GEOLONG:
                            if (fromColumnType != toColumnType && fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOLONG) {
                                asm.ldc(fromColumnType_0 + i * 2);
                                asm.ldc(toColumnType_0 + i * 2);
                                asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                            }
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.LONG128: // from
                    // fall through
                case ColumnType.UUID: // from
                    switch (ColumnType.tagOf(toColumnType)) {
                        case ColumnType.LONG128:
                            // fall through
                        case ColumnType.UUID:
                            asm.invokeInterface(rGetLong128Lo, 1);
                            // Stack: [RowWriter, toColumnIndex, lo]
                            asm.aload(1);  // Push record to the stack again
                            // Stack: [RowWriter, toColumnIndex, lo, Record]
                            asm.iconst(i); // Push column index to a stack
                            // Stack: [RowWriter, toColumnIndex, lo, Record, columnIndex]
                            asm.invokeInterface(rGetLong128Hi, 1);
                            // Stack: [RowWriter, toColumnIndex, lo, hi]
                            asm.invokeInterface(wPutLong128, 5); // long argument is counted as 2 stack slots
                            // invokeInterface consumes the entire stack. Including the RowWriter as invoke interface receives "this" as the first argument
                            // The stack is now empty, and we are done with this column
                            break;
                        case ColumnType.STRING:
                            assert fromColumnType == ColumnType.UUID;
                            // this logic is very similar to the one for ColumnType.UUID above
                            // There is one major difference: `SqlUtil.implicitCastUuidAsStr()` returns `false` to indicate
                            // that the UUID value represents null. In this case we won't call the writer and let null value
                            // to be written by TableWriter/WalWriter NullSetters. However, generating branches via asm is
                            // complicated as JVM requires jump targets to have stack maps, etc. This would complicate things
                            // so we rely on an auxiliary method `transferUuidToStrCol()` to do branching job and javac generates
                            // the stack maps.
                            // Stack: [RowWriter, toColumnIndex, Record, columnIndex]
                            asm.invokeInterface(rGetLong128Lo, 1);
                            // Stack: [RowWriter, toColumnIndex, lo]
                            asm.aload(1);  // Push record to the stack.
                            // Stack: [RowWriter, toColumnIndex, lo, Record]
                            asm.iconst(i); // Push column index to a stack
                            // Stack: [RowWriter, toColumnIndex, lo, Record, columnIndex]
                            asm.invokeInterface(rGetLong128Hi, 1);
                            // Stack: [RowWriter, toColumnIndex, lo, hi]
                            asm.invokeStatic(transferUuidToStrCol);
                            break;
                        case ColumnType.VARCHAR:
                            asm.invokeInterface(rGetLong128Lo, 1);
                            asm.aload(1);  // Push record to the stack.
                            asm.iconst(i); // Push column index to a stack
                            asm.invokeInterface(rGetLong128Hi, 1);
                            asm.invokeStatic(transferUuidToVarcharCol);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.ARRAY:
                    // we are going to assume (and prior validation is required) that the array is of the same type
                    // as in dimensions and element type. The actual validation is not a responsibility of this code
                    // it has to be done upstream to this call
                    if (ColumnType.tagOf(toColumnType) == ColumnType.ARRAY) {
                        asm.ldc(fromColumnType_0 + i * 2);
                        asm.invokeInterface(rGetArray, 2);
                        asm.invokeInterface(wPutArray, 2);
                    } else {
                        assert false;
                    }
                    break;
                default:
                    // we don't need to do anything for null as null is already written by TableWriter/WalWriter NullSetters
                    // every non-null-type is an error
                    assert fromColumnType == ColumnType.NULL;
            }
        }

        asm.return_();
        asm.endMethodCode();

        // exceptions
        asm.putShort(0);

        // we have do not have to add a stack map table because there are no branches
        // attributes: 0 (void, no branches -> no stack verification)
        asm.putShort(0);

        asm.endMethod();

        // class attribute count
        asm.putShort(0);

        return asm.newInstance();
    }

    @SuppressWarnings("unused")
    // Called from dynamically generated bytecode
    public static void transferStrToVarcharCol(TableWriter.Row row, int col, CharSequence str) {
        if (str == null) {
            return;
        }
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        sink.put(str);
        row.putVarchar(col, sink);
    }

    @SuppressWarnings("unused")
    // Called from dynamically generated bytecode
    public static void transferUuidToStrCol(TableWriter.Row row, int col, long lo, long hi) {
        StringSink threadLocalBuilder = Misc.getThreadLocalSink();
        if (SqlUtil.implicitCastUuidAsStr(lo, hi, threadLocalBuilder)) {
            row.putStr(col, threadLocalBuilder);
        }
    }

    @SuppressWarnings("unused")
    // Called from dynamically generated bytecode
    public static void transferUuidToVarcharCol(TableWriter.Row row, int col, long lo, long hi) {
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        if (SqlUtil.implicitCastUuidAsStr(lo, hi, sink)) {
            row.putVarchar(col, sink);
        }
    }

    @SuppressWarnings("unused")
    // Called from dynamically generated bytecode
    public static void transferVarcharToDateCol(TableWriter.Row row, int col, Utf8Sequence seq) {
        if (seq == null) {
            return;
        }
        StringSink sink = Misc.getThreadLocalSink();
        sink.put(seq);
        long date = SqlUtil.implicitCastVarcharAsDate(sink);
        row.putDate(col, date);
    }

    @SuppressWarnings("unused")
    // Called from dynamically generated bytecode
    public static void transferVarcharToSymbolCol(TableWriter.Row row, int col, Utf8Sequence seq) {
        if (seq == null) {
            return;
        }
        StringSink threadLocalBuilder = Misc.getThreadLocalSink();
        threadLocalBuilder.put(seq);
        row.putSym(col, threadLocalBuilder);
    }

    @SuppressWarnings("unused")
    // Called from dynamically generated bytecode
    public static void validateArrayDimensionsAndTransferCol(TableWriter.Row row, int col, DoubleArrayParser parser, CharSequence str, int expectedType) {
        if (str == null) {
            return;
        }

        ArrayView view = SqlUtil.implicitCastStringAsDoubleArray(str, parser, expectedType);
        row.putArray(col, view);
    }

    // Called from dynamically generated bytecode
    @SuppressWarnings("unused")
    public static void validateArrayDimensionsAndTransferCol(TableWriter.Row row, int col, DoubleArrayParser parser, Utf8Sequence vch, int expectedType) {
        if (vch == null) {
            return;
        }

        ArrayView view = SqlUtil.implicitCastVarcharAsDoubleArray(vch, parser, expectedType);
        row.putArray(col, view);
    }

    private static boolean isArrayParserRequired(ColumnTypes from, RecordMetadata to, ColumnFilter toColumnFilter, int n) {
        for (int i = 0; i < n; i++) {
            int toColumnIndex = toColumnFilter.getColumnIndexFactored(i);
            int toColumnType = to.getColumnType(toColumnIndex);
            int fromColumnType = from.getColumnType(i);
            if (ColumnType.tagOf(toColumnType) == ColumnType.ARRAY &&
                    (ColumnType.tagOf(fromColumnType) == ColumnType.STRING || ColumnType.tagOf(fromColumnType) == ColumnType.VARCHAR)
            ) {
                return true;
            }
        }
        return false;
    }
}
