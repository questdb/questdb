/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cairo.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;

/**
 * Writes the body of an SQL transaction record, the part after its type byte: command type, SQL text,
 * random seeds and bind variables. {@link WalEventCursor} decodes it for replay.
 */
public final class WalSqlEncoder {
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final StringSink sink = new StringSink();

    public void encode(MemoryA mem, int cmdType, CharSequence sqlText, SqlExecutionContext sqlExecutionContext) {
        mem.putInt(cmdType); // byte would be enough probably
        mem.putStr(sqlText);
        final Rnd rnd = sqlExecutionContext.getRandom();
        mem.putLong(rnd.getSeed0());
        mem.putLong(rnd.getSeed1());
        final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
        appendBindVariableValuesByIndex(mem, bindVariableService);
        appendBindVariableValuesByName(mem, bindVariableService);
    }

    private void appendBindVariableValuesByIndex(MemoryA mem, BindVariableService bindVariableService) {
        final int count = bindVariableService != null ? bindVariableService.getIndexedVariableCount() : 0;
        mem.putInt(count);
        for (int i = 0; i < count; i++) {
            appendFunctionValue(mem, bindVariableService.getFunction(i));
        }
    }

    private void appendBindVariableValuesByName(MemoryA mem, BindVariableService bindVariableService) {
        final int count = bindVariableService != null ? bindVariableService.getNamedVariables().size() : 0;
        mem.putInt(count);

        if (count > 0) {
            final ReadOnlyObjList<CharSequence> namedVariables = bindVariableService.getNamedVariables();
            for (int i = 0; i < count; i++) {
                final CharSequence name = namedVariables.get(i);
                mem.putStr(name);
                sink.clear();
                sink.put(':').put(name);
                appendFunctionValue(mem, bindVariableService.getFunction(sink));
            }
        }
    }

    private void appendFunctionValue(MemoryA mem, Function function) {
        final int type = function.getType();
        mem.putInt(type);

        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
                mem.putBool(function.getBool(null));
                break;
            case ColumnType.BYTE:
                mem.putByte(function.getByte(null));
                break;
            case ColumnType.GEOBYTE:
                mem.putByte(function.getGeoByte(null));
                break;
            case ColumnType.SHORT:
                mem.putShort(function.getShort(null));
                break;
            case ColumnType.GEOSHORT:
                mem.putShort(function.getGeoShort(null));
                break;
            case ColumnType.CHAR:
                mem.putChar(function.getChar(null));
                break;
            case ColumnType.INT:
                mem.putInt(function.getInt(null));
                break;
            case ColumnType.IPv4:
                mem.putInt(function.getIPv4(null));
                break;
            case ColumnType.GEOINT:
                mem.putInt(function.getGeoInt(null));
                break;
            case ColumnType.FLOAT:
                mem.putFloat(function.getFloat(null));
                break;
            case ColumnType.LONG:
                mem.putLong(function.getLong(null));
                break;
            case ColumnType.GEOLONG:
                mem.putLong(function.getGeoLong(null));
                break;
            case ColumnType.DATE:
                mem.putLong(function.getDate(null));
                break;
            case ColumnType.TIMESTAMP:
                mem.putLong(function.getTimestamp(null));
                break;
            case ColumnType.DOUBLE:
                mem.putDouble(function.getDouble(null));
                break;
            case ColumnType.STRING:
                mem.putStr(function.getStrA(null));
                break;
            case ColumnType.VARCHAR:
                VarcharTypeDriver.appendPlainValue(mem, function.getVarcharA(null));
                break;
            case ColumnType.BINARY:
                mem.putBin(function.getBin(null));
                break;
            case ColumnType.UUID:
                mem.putLong128(function.getLong128Lo(null), function.getLong128Hi(null));
                break;
            case ColumnType.ARRAY:
                mem.putArray(function.getArray(null));
                break;
            case ColumnType.DECIMAL8:
                mem.putByte(function.getDecimal8(null));
                break;
            case ColumnType.DECIMAL16:
                mem.putShort(function.getDecimal16(null));
                break;
            case ColumnType.DECIMAL32:
                mem.putInt(function.getDecimal32(null));
                break;
            case ColumnType.DECIMAL64:
                mem.putLong(function.getDecimal64(null));
                break;
            case ColumnType.DECIMAL128: {
                function.getDecimal128(null, decimal128);
                mem.putDecimal128(decimal128.getHigh(), decimal128.getLow());
                break;
            }
            case ColumnType.DECIMAL256: {
                function.getDecimal256(null, decimal256);
                mem.putDecimal256(
                        decimal256.getHh(),
                        decimal256.getHl(),
                        decimal256.getLh(),
                        decimal256.getLl()
                );
                break;
            }
            default:
                throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(type));
        }
    }
}
