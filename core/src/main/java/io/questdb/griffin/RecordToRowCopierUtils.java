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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.BytecodeAssembler;

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

        int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        int rGetGeoInt = asm.poolInterfaceMethod(Record.class, "getGeoInt", "(I)I");
        int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        int rGetGeoLong = asm.poolInterfaceMethod(Record.class, "getGeoLong", "(I)J");
        int rGetLong256 = asm.poolInterfaceMethod(Record.class, "getLong256A", "(I)Lio/questdb/std/Long256;");
        int rGetLong128Hi = asm.poolInterfaceMethod(Record.class, "getLong128Hi", "(I)J");
        int rGetLong128Lo = asm.poolInterfaceMethod(Record.class, "getLong128Lo", "(I)J");
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
        int rGetSym = asm.poolInterfaceMethod(Record.class, "getSym", "(I)Ljava/lang/CharSequence;");
        int rGetStr = asm.poolInterfaceMethod(Record.class, "getStr", "(I)Ljava/lang/CharSequence;");
        int rGetBin = asm.poolInterfaceMethod(Record.class, "getBin", "(I)Lio/questdb/std/BinarySequence;");
        //
        int wPutInt = asm.poolInterfaceMethod(TableWriter.Row.class, "putInt", "(II)V");
        int wPutLong = asm.poolInterfaceMethod(TableWriter.Row.class, "putLong", "(IJ)V");
        int wPutLong256 = asm.poolInterfaceMethod(TableWriter.Row.class, "putLong256", "(ILio/questdb/std/Long256;)V");
        int wPutLong128 = asm.poolInterfaceMethod(TableWriter.Row.class, "putLong128LittleEndian", "(IJJ)V");
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
        int implicitCastCharAsByte = asm.poolMethod(SqlUtil.class, "implicitCastCharAsByte", "(CI)B");
        int implicitCastCharAsGeoHash = asm.poolMethod(SqlUtil.class, "implicitCastCharAsGeoHash", "(CI)B");
        int implicitCastStrAsFloat = asm.poolMethod(SqlUtil.class, "implicitCastStrAsFloat", "(Ljava/lang/CharSequence;)F");
        int implicitCastStrAsDouble = asm.poolMethod(SqlUtil.class, "implicitCastStrAsDouble", "(Ljava/lang/CharSequence;)D");
        int implicitCastStrAsByte = asm.poolMethod(SqlUtil.class, "implicitCastStrAsByte", "(Ljava/lang/CharSequence;)B");
        int implicitCastStrAsShort = asm.poolMethod(SqlUtil.class, "implicitCastStrAsShort", "(Ljava/lang/CharSequence;)S");
        int implicitCastStrAsChar = asm.poolMethod(SqlUtil.class, "implicitCastStrAsChar", "(Ljava/lang/CharSequence;)C");
        int implicitCastStrAsInt = asm.poolMethod(SqlUtil.class, "implicitCastStrAsInt", "(Ljava/lang/CharSequence;)I");
        int implicitCastStrAsLong = asm.poolMethod(SqlUtil.class, "implicitCastStrAsLong", "(Ljava/lang/CharSequence;)J");
        int implicitCastStrAsDate = asm.poolMethod(SqlUtil.class, "implicitCastStrAsDate", "(Ljava/lang/CharSequence;)J");
        int implicitCastStrAsTimestamp = asm.poolMethod(SqlUtil.class, "implicitCastStrAsTimestamp", "(Ljava/lang/CharSequence;)J");
        int implicitCastDateAsTimestamp = asm.poolMethod(SqlUtil.class, "dateToTimestamp", "(J)J");
        int implicitCastShortAsByte = asm.poolMethod(SqlUtil.class, "implicitCastShortAsByte", "(S)B");
        int implicitCastIntAsByte = asm.poolMethod(SqlUtil.class, "implicitCastIntAsByte", "(I)B");
        int implicitCastLongAsByte = asm.poolMethod(SqlUtil.class, "implicitCastLongAsByte", "(J)B");
        int implicitCastFloatAsByte = asm.poolMethod(SqlUtil.class, "implicitCastFloatAsByte", "(F)B");
        int implicitCastDoubleAsByte = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsByte", "(D)B");

        int implicitCastIntAsShort = asm.poolMethod(SqlUtil.class, "implicitCastIntAsShort", "(I)S");
        int implicitCastLongAsShort = asm.poolMethod(SqlUtil.class, "implicitCastLongAsShort", "(J)S");
        int implicitCastFloatAsShort = asm.poolMethod(SqlUtil.class, "implicitCastFloatAsShort", "(F)S");
        int implicitCastDoubleAsShort = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsShort", "(D)S");

        int implicitCastLongAsInt = asm.poolMethod(SqlUtil.class, "implicitCastLongAsInt", "(J)I");
        int implicitCastFloatAsInt = asm.poolMethod(SqlUtil.class, "implicitCastFloatAsInt", "(F)I");
        int implicitCastDoubleAsInt = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsInt", "(D)I");

        int implicitCastFloatAsLong = asm.poolMethod(SqlUtil.class, "implicitCastFloatAsLong", "(F)J");
        int implicitCastDoubleAsLong = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsLong", "(D)J");
        int implicitCastDoubleAsFloat = asm.poolMethod(SqlUtil.class, "implicitCastDoubleAsFloat", "(D)F");
        int wPutStrChar = asm.poolInterfaceMethod(TableWriter.Row.class, "putStr", "(IC)V");
        int wPutChar = asm.poolInterfaceMethod(TableWriter.Row.class, "putChar", "(IC)V");
        int wPutBin = asm.poolInterfaceMethod(TableWriter.Row.class, "putBin", "(ILio/questdb/std/BinarySequence;)V");
        int implicitCastGeoHashAsGeoHash = asm.poolMethod(SqlUtil.class, "implicitCastGeoHashAsGeoHash", "(JII)J");

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

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(0);
        asm.methodCount(2);
        asm.defineDefaultConstructor();

        asm.startMethod(copyNameIndex, copySigIndex, 15, 3);

        for (int i = 0; i < n; i++) {

            final int toColumnIndex = toColumnFilter.getColumnIndexFactored(i);
            // do not copy timestamp, it will be copied externally to this helper

            if (toColumnIndex == timestampIndex) {
                continue;
            }

            final int toColumnType = to.getColumnType(toColumnIndex);
            final int fromColumnType = from.getColumnType(i);
            final int toColumnTypeTag = ColumnType.tagOf(toColumnType);
            final int toColumnWriterIndex = to.getWriterIndex(toColumnIndex);

            asm.aload(2);
            asm.iconst(toColumnWriterIndex);
            asm.aload(1);
            asm.iconst(i);

            int fromColumnTypeTag = ColumnType.tagOf(fromColumnType);
            if (fromColumnTypeTag == ColumnType.NULL) {
                fromColumnTypeTag = toColumnTypeTag;
            }
            switch (fromColumnTypeTag) {
                case ColumnType.INT:
                    asm.invokeInterface(rGetInt);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastIntAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeStatic(implicitCastIntAsShort);
                            asm.invokeInterface(wPutShort, 2);
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
                            asm.invokeInterface(wPutInt, 2);
                            break;
                    }
                    break;
                case ColumnType.LONG:
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
                            asm.invokeInterface(wPutLong, 3);
                            break;
                    }
                    break;
                case ColumnType.DATE:
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
                        case ColumnType.TIMESTAMP:
                            asm.invokeStatic(implicitCastDateAsTimestamp);
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
                            asm.invokeInterface(wPutDate, 3);
                            break;
                    }
                    break;
                case ColumnType.TIMESTAMP:
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
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.BYTE:
                    asm.invokeInterface(rGetByte);
                    switch (toColumnTypeTag) {
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
                            asm.invokeInterface(wPutByte, 2);
                            break;
                    }
                    break;
                case ColumnType.SHORT:
                    asm.invokeInterface(rGetShort);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastShortAsByte);
                            asm.invokeInterface(wPutByte, 2);
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
                            asm.invokeInterface(wPutShort, 2);
                            break;
                    }
                    break;
                case ColumnType.BOOLEAN:
                    asm.invokeInterface(rGetBool);
                    asm.invokeInterface(wPutBool, 2);
                    break;
                case ColumnType.FLOAT:
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
                        default:
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                    }
                    break;
                case ColumnType.CHAR:
                    asm.invokeInterface(rGetChar);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.iconst(toColumnType);
                            asm.invokeStatic(implicitCastCharAsByte);
                            asm.i2b();
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
                            asm.iconst(toColumnType);
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
                case ColumnType.SYMBOL:
                    asm.invokeInterface(rGetSym);
                    if (toColumnTypeTag == ColumnType.STRING) {
                        asm.invokeInterface(wPutStr, 2);
                    } else {
                        asm.invokeInterface(wPutSym, 2);
                    }
                    break;
                case ColumnType.STRING:
                    // This is generic code, and it acts on a record
                    // whereas Functions support string to primitive conversions, Record instances
                    // do not. This is because functions are aware of their return type but records
                    // would have to do expensive checks to decide which conversion would be required
                    asm.invokeInterface(rGetStr);
                    switch (toColumnTypeTag) {
                        case ColumnType.BYTE:
                            asm.invokeStatic(implicitCastStrAsByte);
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.SHORT:
                            asm.invokeStatic(implicitCastStrAsShort);
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.CHAR:
                            asm.invokeStatic(implicitCastStrAsChar);
                            asm.invokeInterface(wPutChar, 2);
                            break;
                        case ColumnType.INT:
                            asm.invokeStatic(implicitCastStrAsInt);
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeStatic(implicitCastStrAsLong);
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.FLOAT:
                            asm.invokeStatic(implicitCastStrAsFloat);
                            asm.invokeInterface(wPutFloat, 2);
                            break;
                        case ColumnType.DOUBLE:
                            asm.invokeStatic(implicitCastStrAsDouble);
                            asm.invokeInterface(wPutDouble, 3);
                            break;
                        case ColumnType.SYMBOL:
                            asm.invokeInterface(wPutSym, 2);
                            break;
                        case ColumnType.DATE:
                            asm.invokeStatic(implicitCastStrAsDate);
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeStatic(implicitCastStrAsTimestamp);
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.GEOBYTE:
                        case ColumnType.GEOSHORT:
                        case ColumnType.GEOINT:
                        case ColumnType.GEOLONG:
                            asm.invokeInterface(wPutGeoStr, 2);
                            break;
                        case ColumnType.STRING:
                            asm.invokeInterface(wPutStr, 2);
                            break;
                        default:
                            assert false;
                            break;
                    }
                    break;
                case ColumnType.BINARY:
                    asm.invokeInterface(rGetBin);
                    asm.invokeInterface(wPutBin, 2);
                    break;
                case ColumnType.LONG256:
                    asm.invokeInterface(rGetLong256);
                    asm.invokeInterface(wPutLong256, 2);
                    break;
                case ColumnType.LONG128:
                    asm.invokeInterface(rGetLong128Hi);
                    asm.aload(1);
                    asm.iconst(i);
                    asm.invokeInterface(rGetLong128Lo);
                    asm.invokeInterface(wPutLong128, 5);
                    break;
                case ColumnType.GEOBYTE:
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
                case ColumnType.GEOSHORT:
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
                case ColumnType.GEOINT:
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
                        default:
                            if (fromColumnType != toColumnType && fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOINT) {
                                asm.i2l();
                                asm.ldc(fromColumnType_0 + i * 2);
                                asm.ldc(toColumnType_0 + i * 2);
                                asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                                asm.l2i();
                            }
                            asm.invokeInterface(wPutInt, 2);
                            break;
                    }
                    break;
                case ColumnType.GEOLONG:
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
                        default:
                            if (fromColumnType != toColumnType && fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOLONG) {
                                asm.ldc(fromColumnType_0 + i * 2);
                                asm.ldc(toColumnType_0 + i * 2);
                                asm.invokeStatic(implicitCastGeoHashAsGeoHash);
                            }
                            asm.invokeInterface(wPutLong, 3);
                            break;
                    }
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
