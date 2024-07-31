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

package io.questdb.cutlass.pgwire;

import io.questdb.TelemetryOrigin;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.pgwire.PGConnectionContext.*;
import static io.questdb.cutlass.pgwire.PGOids.*;

public class PGPipelineEntry implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(PGPipelineEntry.class);
    // stores result format codes (0=Text,1=Binary) from the latest bind message
    // we need it in case cursor gets invalidated and bind used non-default binary format for some column(s)
    // pg clients (like asyncpg) fail when format sent by server is not the same as requested in bind message
    private final IntList msgBindSelectFormatCodes = new IntList();
    private AlterOperation alterOp = null;
    private boolean empty;
    private boolean executedFromNew = false;
    // this is a "union", so should only be one, depending on SQL type
    // SELECT or EXPLAIN
    private RecordCursorFactory factory = null;
    private InsertOperation insertOp = null;
    private int msgBindParameterFormatCodeCount;
    private int msgBindParameterValueCount;
    // types are sent to us via "parse" message
    private IntList msgParseParameterTypes = new IntList();
    private boolean named = false;
    // list of pair: column types (with format flag stored in first bit) AND additional type flag
    private IntList pgResultSetColumnTypes;
    private boolean shouldCache = false;
    private long sqlRowCount = 0;
    private CharSequence sqlTag = null;
    private CharSequence sqlText = null;
    private boolean sqlTextHasSecret = false;
    private int sqlType = 0;
    private TypesAndInsert tai = null;
    private TypesAndSelect tas = null;
    private TypesAndUpdate tau = null;
    private UpdateOperation updateOp = null;
    private String updateOpSql = null;

    @Override
    public void close() {

    }

    public int getMsgParseParameterTypeCount() {
        return msgParseParameterTypes.size();
    }

    public CharSequence getSqlText() {
        return sqlText;
    }

    public boolean isEmpty() {
        return empty;
    }

    public boolean isNamed() {
        return named;
    }

    public boolean lookup(
            SimpleAssociativeCache<TypesAndInsert> taiCache,
            SimpleAssociativeCache<TypesAndUpdate> tauCache,
            AssociativeCache<TypesAndSelect> tasCache,
            BindVariableService bindVariableService
    ) throws SqlException {
        bindVariableService.clear();

        tai = taiCache.peek(sqlText);

        // not found or not insert, try select
        // poll this cache because it is shared, and we do not want
        // select factory to be used by another thread concurrently
        if (tai != null) {
            tai.defineBindVariables(bindVariableService);
            sqlTag = TAG_INSERT;
            return true;
        }

        tau = tauCache.poll(sqlText);

        if (tau != null) {
            tau.defineBindVariables(bindVariableService);
            sqlTag = TAG_UPDATE;
            return true;
        }

        tas = tasCache.poll(sqlText);

        if (tas != null) {
            // cache hit, define bind variables
            tas.defineBindVariables(bindVariableService);
            sqlTag = TAG_SELECT;
            return true;
        }

        return false;
    }

    public void msgBindCopyParameterFormatCodes(long lo, long msgLimit, short formatCodeCount) throws BadProtocolException {
        this.msgBindParameterFormatCodeCount = formatCodeCount;
        if (formatCodeCount > 0) {
            if (formatCodeCount == 1) {
                // same format applies to all parameters
                mergeParameterTypesAndFormatCodesOneForAll(lo, msgLimit);
            } else if (msgParseParameterTypes.size() > 0) {
                //client doesn't need to specify types in Parse message and can use those returned in ParameterDescription
                mergeParameterTypesAndFormatCodes(lo, msgLimit, formatCodeCount);
            }
        }
    }

    public void msgBindCopySelectFormatCodes(long lo, long msgLimit, short selectFormatCodeCount) throws BadProtocolException {
        // Select format codes are switches between binary and text representation of the
        // result set. They are only applicable to the result set and SQLs that compile into a factory.
        if (factory != null) {
            final int columnCount = factory.getMetadata().getColumnCount();
            msgBindSelectFormatCodes.clear();
            msgBindSelectFormatCodes.setPos(columnCount);
            if (selectFormatCodeCount > 0) {

                // apply format codes to the cursor column types
                // but check if there is message is consistent

                final long spaceNeeded = lo + selectFormatCodeCount * Short.BYTES;
                if (spaceNeeded <= msgLimit) {
                    msgBindSelectFormatCodes.setPos(columnCount);
                    if (selectFormatCodeCount == columnCount) {
                        // good to go
                        for (int i = 0; i < columnCount; i++) {
                            msgBindSelectFormatCodes.setQuick(i, getShortUnsafe(lo));
                            lo += Short.BYTES;
                        }
                    } else if (selectFormatCodeCount == 1) {
                        msgBindSelectFormatCodes.setAll(columnCount, getShortUnsafe(lo));
                    } else {
                        LOG.error().$("could not process column format codes [fmtCount=").$(selectFormatCodeCount).$(", columnCount=").$(columnCount).I$();
                        throw BadProtocolException.INSTANCE;
                    }
                } else {
                    LOG.error().$("could not process column format codes [bufSpaceNeeded=").$(spaceNeeded).$(", bufSpaceAvail=").$(msgLimit).I$();
                    throw BadProtocolException.INSTANCE;
                }
            } else if (selectFormatCodeCount == 0) {
                // if count == 0 then we've to use default and clear binary flags that might come from cached statements
                msgBindSelectFormatCodes.setAll(columnCount, 0);
            }
        }
    }

    public long msgBindDefineBindVariableTypes(
            long lo,
            long msgLimit,
            BindVariableService bindVariableService,
            CharacterStore characterStore,
            DirectUtf8String utf8String,
            ObjectPool<DirectBinarySequence> binarySequenceParamsPool
    ) throws SqlException, BadProtocolException {
        if (msgBindParameterValueCount > 0) {
            // client doesn't need to specify any type in Parse message and can just use types returned in ParameterDescription message
            if (this.getMsgParseParameterTypeCount() > 0) {
                return msgBindDefineBindVariablesFromValues(
                        lo,
                        msgLimit,
                        bindVariableService,
                        characterStore,
                        utf8String,
                        binarySequenceParamsPool
                );
            } else {
                return msgBindDefineBindVariablesAsStrings(
                        lo,
                        msgLimit,
                        bindVariableService,
                        characterStore,
                        utf8String
                );
            }
        }
        return lo;
    }

    public void msgBindSetParameterValueCount(short valueCount) throws BadProtocolException {
        this.msgBindParameterValueCount = valueCount;
        if (valueCount > 0) {
            if (valueCount < getMsgParseParameterTypeCount()) {
                LOG.error().$("parameter type count must be less or equals to number of parameters values").$();
                throw BadProtocolException.INSTANCE;
            }
            if (msgBindParameterFormatCodeCount > 1 && msgBindParameterFormatCodeCount != valueCount) {
                LOG.error().$("parameter format count and parameter value count must match").$();
                throw BadProtocolException.INSTANCE;
            }
        }
    }

    public void msgParseCopyParameterTypesFromMsg(long lo, short parameterTypeCount) {
        msgParseParameterTypes.setPos(parameterTypeCount);
        for (int i = 0; i < parameterTypeCount; i++) {
            msgParseParameterTypes.setQuick(i, Unsafe.getUnsafe().getInt(lo + i * 4L));
        }
    }

    public void msgParseCopyParameterTypesFromService(BindVariableService bindVariableService) {
        final int n = bindVariableService.getIndexedVariableCount();
        if (n > 0) {
            msgParseParameterTypes.setPos(n);
            for (int i = 0; i < n; i++) {
                final Function f = bindVariableService.getFunction(i);
                msgParseParameterTypes.setQuick(i, Numbers.bswap(PGOids.getTypeOid(
                        f != null ? f.getType() : ColumnType.UNDEFINED
                )));
            }
        }
    }

    public void of(CharSequence sqlText) {
        this.sqlText = sqlText;
        this.empty = sqlText == null || sqlText.length() == 0;
    }

    public void parseNew(CairoEngine engine, SqlExecutionContext executionContext) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CompiledQuery cq = compiler.compile(sqlText, executionContext);

            executionContext.storeTelemetry(cq.getType(), TelemetryOrigin.POSTGRES);

            this.sqlType = cq.getType();

            switch (cq.getType()) {
                case CompiledQuery.CREATE_TABLE_AS_SELECT:
                    sqlTag = TAG_CTAS;
                    sqlRowCount = cq.getAffectedRowsCount();
                    break;
                case CompiledQuery.EXPLAIN:
                    this.factory = cq.getRecordCursorFactory();
                    msgParseBuildResultSetColumnTypes();
                    sqlTag = TAG_EXPLAIN;
                case CompiledQuery.SELECT:
                    this.factory = cq.getRecordCursorFactory();
                    msgParseBuildResultSetColumnTypes();
                    sqlTag = TAG_SELECT;
                    break;
                case CompiledQuery.PSEUDO_SELECT:
                    this.factory = cq.getRecordCursorFactory();
                    msgParseBuildResultSetColumnTypes();
                    sqlTag = TAG_PSEUDO_SELECT;
                    break;
                case CompiledQuery.INSERT:
                    this.insertOp = cq.getInsertOperation();
                    sqlTag = TAG_INSERT;
                    break;
                case CompiledQuery.UPDATE:
                    this.updateOp = cq.getUpdateOperation();
                    this.updateOpSql = cq.getSqlStatement();
                    sqlTag = TAG_UPDATE;
                    break;
                case CompiledQuery.INSERT_AS_SELECT:
                    this.insertOp = cq.getInsertOperation();
                    sqlTag = TAG_INSERT_AS_SELECT;
                    break;
                case CompiledQuery.SET:
                    sqlTag = TAG_SET;
                    break;
                case CompiledQuery.DEALLOCATE:
                    sqlTag = TAG_DEALLOCATE;
                    break;
                case CompiledQuery.BEGIN:
                    sqlTag = TAG_BEGIN;
                    break;
                case CompiledQuery.COMMIT:
                    sqlTag = TAG_COMMIT;
                    break;
                case CompiledQuery.ROLLBACK:
                    sqlTag = TAG_ROLLBACK;
                    break;
                case CompiledQuery.ALTER_USER:
                    sqlTextHasSecret = executionContext.containsSecret();
                    sqlTag = TAG_ALTER_ROLE;
                    break;
                case CompiledQuery.CREATE_USER:
                    sqlTextHasSecret = executionContext.containsSecret();
                    sqlTag = TAG_CREATE_ROLE;
                    break;
                case CompiledQuery.ALTER:
                    // future-proofing ALTER execution
                    this.alterOp = cq.getAlterOperation();
                    // fall through
                default:
                    // DDL
                    sqlTag = TAG_OK;
                    break;
            }
        }
    }

    public void setBindVariableAsStr(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService,
            CharacterStore characterStore,
            DirectUtf8String utf8String
    ) throws BadProtocolException, SqlException {
        CharacterStoreEntry e = characterStore.newEntry();
        Function fn = bindVariableService.getFunction(variableIndex);
        // If the function type is VARCHAR, there's no need to convert to UTF-16
        if (fn != null && fn.getType() == ColumnType.VARCHAR) {
            final int sequenceType = Utf8s.getUtf8SequenceType(valueAddr, valueAddr + valueSize);
            boolean ascii;
            switch (sequenceType) {
                case 0:
                    // ascii sequence
                    ascii = true;
                    break;
                case 1:
                    // non-ASCII sequence
                    ascii = false;
                    break;
                default:
                    LOG.error().$("invalid varchar bind variable type [variableIndex=").$(variableIndex).I$();
                    throw BadProtocolException.INSTANCE;
            }
            bindVariableService.setVarchar(variableIndex, utf8String.of(valueAddr, valueAddr + valueSize, ascii));

        } else {
            if (Utf8s.utf8ToUtf16(valueAddr, valueAddr + valueSize, e)) {
                bindVariableService.setStr(variableIndex, characterStore.toImmutable());
            } else {
                LOG.error().$("invalid str bind variable type [variableIndex=").$(variableIndex).I$();
                throw BadProtocolException.INSTANCE;
            }
        }
    }

    public void setNamed(boolean named) {
        this.named = named;
    }

    private static void ensureValueLength(int variableIndex, int sizeRequired, int sizeActual) throws BadProtocolException {
        if (sizeRequired == sizeActual) {
            return;
        }
        LOG.error()
                .$("bad parameter value length [sizeRequired=").$(sizeRequired)
                .$(", sizeActual=").$(sizeActual)
                .$(", variableIndex=").$(variableIndex)
                .I$();
        throw BadProtocolException.INSTANCE;
    }

    private static void setBindVariableAsBin(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService,
            ObjectPool<DirectBinarySequence> binarySequenceParamsPool
    ) throws SqlException {
        bindVariableService.setBin(variableIndex, binarySequenceParamsPool.next().of(valueAddr, valueSize));
        freezeRecvBuffer = true;
    }

    private static void setBindVariableAsBoolean(
            int variableIndex,
            int valueSize,
            BindVariableService bindVariableService
    ) throws SqlException {
        if (valueSize != 4 && valueSize != 5) {
            throw SqlException.$(0, "bad value for BOOLEAN parameter [variableIndex=").put(variableIndex).put(", valueSize=").put(valueSize).put(']');
        }
        bindVariableService.setBoolean(variableIndex, valueSize == 4);
    }

    private static void setBindVariableAsChar(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService,
            CharacterStore characterStore
    ) throws BadProtocolException, SqlException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Utf8s.utf8ToUtf16(valueAddr, valueAddr + valueSize, e)) {
            bindVariableService.setChar(variableIndex, characterStore.toImmutable().charAt(0));
        } else {
            LOG.error().$("invalid char UTF8 bytes [variableIndex=").$(variableIndex).I$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private static void setBindVariableAsDate(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService,
            CharacterStore characterStore
    ) throws SqlException, BadProtocolException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Utf8s.utf8ToUtf16(valueAddr, valueAddr + valueSize, e)) {
            bindVariableService.define(variableIndex, ColumnType.DATE, 0);
            bindVariableService.setStr(variableIndex, characterStore.toImmutable());
        } else {
            LOG.error().$("invalid str UTF8 bytes [variableIndex=").$(variableIndex).I$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private static void setBindVariableAsDouble(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Double.BYTES, valueSize);
        bindVariableService.setDouble(variableIndex, Double.longBitsToDouble(getLongUnsafe(valueAddr)));
    }

    private static void setBindVariableAsFloat(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Float.BYTES, valueSize);
        bindVariableService.setFloat(variableIndex, Float.intBitsToFloat(getIntUnsafe(valueAddr)));
    }

    private static void setBindVariableAsInt(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Integer.BYTES, valueSize);
        bindVariableService.setInt(variableIndex, getIntUnsafe(valueAddr));
    }

    private static void setBindVariableAsLong(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Long.BYTES, valueSize);
        bindVariableService.setLong(variableIndex, getLongUnsafe(valueAddr));
    }

    private static void setBindVariableAsShort(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Short.BYTES, valueSize);
        bindVariableService.setShort(variableIndex, getShortUnsafe(valueAddr));
    }

    private static void setBindVariableAsTimestamp(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Long.BYTES, valueSize);
        bindVariableService.setTimestamp(variableIndex, getLongUnsafe(valueAddr) + Numbers.JULIAN_EPOCH_OFFSET_USEC);
    }

    private static void setUuidBindVariable(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Long128.BYTES, valueSize);
        long hi = getLongUnsafe(valueAddr);
        long lo = getLongUnsafe(valueAddr + Long.BYTES);
        bindVariableService.setUuid(variableIndex, lo, hi);
    }

    private void mergeParameterTypesAndFormatCodes(long lo, long msgLimit, short parameterFormatCount) throws BadProtocolException {
        if (lo + Short.BYTES * parameterFormatCount <= msgLimit) {
            LOG.debug().$("processing bind formats [count=").$(parameterFormatCount).I$();
            for (int i = 0; i < parameterFormatCount; i++) {
                final short code = getShortUnsafe(lo + i * Short.BYTES);
                msgParseParameterTypes.setQuick(i, toParamBinaryType(code, msgParseParameterTypes.getQuick(i)));
            }
        } else {
            LOG.error().$("invalid format code count [value=").$(parameterFormatCount).I$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private void mergeParameterTypesAndFormatCodesOneForAll(long lo, long msgLimit) throws BadProtocolException {
        short code = getShort(lo, msgLimit, "could not read parameter formats");
        for (int i = 0, n = msgParseParameterTypes.size(); i < n; i++) {
            msgParseParameterTypes.setQuick(i, toParamBinaryType(code, msgParseParameterTypes.getQuick(i)));
        }
    }

    private long msgBindDefineBindVariablesAsStrings(
            long lo,
            long msgLimit,
            BindVariableService bindVariableService,
            CharacterStore characterStore,
            DirectUtf8String utf8String
    ) throws BadProtocolException, SqlException {
        for (int j = 0; j < msgBindParameterValueCount; j++) {
            final int valueSize = getInt(lo, msgLimit, "malformed bind variable");
            lo += Integer.BYTES;

            if (valueSize != -1 && lo + valueSize <= msgLimit) {
                setBindVariableAsStr(j, lo, valueSize, bindVariableService, characterStore, utf8String);
                lo += valueSize;
            } else if (valueSize != -1) {
                LOG.error()
                        .$("value length is outside of buffer [parameterIndex=").$(j)
                        .$(", valueSize=").$(valueSize)
                        .$(", messageRemaining=").$(msgLimit - lo)
                        .I$();
                throw BadProtocolException.INSTANCE;
            }
        }
        return lo;
    }

    private long msgBindDefineBindVariablesFromValues(
            long lo,
            long msgLimit,
            BindVariableService bindVariableService,
            CharacterStore characterStore,
            DirectUtf8String utf8String,
            ObjectPool<DirectBinarySequence> binarySequenceParamsPool
    ) throws BadProtocolException, SqlException {
        for (int j = 0; j < msgBindParameterValueCount; j++) {
            final int valueSize = getInt(lo, msgLimit, "malformed bind variable");
            lo += Integer.BYTES;
            if (valueSize == -1) {
                // undefined function?
                switch (msgParseParameterTypes.getQuick(j)) {
                    case X_B_PG_INT4:
                        bindVariableService.define(j, ColumnType.INT, 0);
                        break;
                    case X_B_PG_INT8:
                        bindVariableService.define(j, ColumnType.LONG, 0);
                        break;
                    case X_B_PG_TIMESTAMP:
                        bindVariableService.define(j, ColumnType.TIMESTAMP, 0);
                        break;
                    case X_B_PG_INT2:
                        bindVariableService.define(j, ColumnType.SHORT, 0);
                        break;
                    case X_B_PG_FLOAT8:
                        bindVariableService.define(j, ColumnType.DOUBLE, 0);
                        break;
                    case X_B_PG_FLOAT4:
                        bindVariableService.define(j, ColumnType.FLOAT, 0);
                        break;
                    case X_B_PG_CHAR:
                        bindVariableService.define(j, ColumnType.CHAR, 0);
                        break;
                    case X_B_PG_DATE:
                        bindVariableService.define(j, ColumnType.DATE, 0);
                        break;
                    case X_B_PG_BOOL:
                        bindVariableService.define(j, ColumnType.BOOLEAN, 0);
                        break;
                    case X_B_PG_BYTEA:
                        bindVariableService.define(j, ColumnType.BINARY, 0);
                        break;
                    case X_B_PG_UUID:
                        bindVariableService.define(j, ColumnType.UUID, 0);
                        break;
                    default:
                        bindVariableService.define(j, ColumnType.STRING, 0);
                        break;
                }
            } else if (lo + valueSize <= msgLimit) {
                switch (msgParseParameterTypes.getQuick(j)) {
                    case X_B_PG_INT4:
                        setBindVariableAsInt(j, lo, valueSize, bindVariableService);
                        break;
                    case X_B_PG_INT8:
                        setBindVariableAsLong(j, lo, valueSize, bindVariableService);
                        break;
                    case X_B_PG_TIMESTAMP:
                        setBindVariableAsTimestamp(j, lo, valueSize, bindVariableService);
                        break;
                    case X_B_PG_INT2:
                        setBindVariableAsShort(j, lo, valueSize, bindVariableService);
                        break;
                    case X_B_PG_FLOAT8:
                        setBindVariableAsDouble(j, lo, valueSize, bindVariableService);
                        break;
                    case X_B_PG_FLOAT4:
                        setBindVariableAsFloat(j, lo, valueSize, bindVariableService);
                        break;
                    case X_B_PG_CHAR:
                        setBindVariableAsChar(j, lo, valueSize, bindVariableService, characterStore);
                        break;
                    case X_B_PG_DATE:
                        setBindVariableAsDate(j, lo, valueSize, bindVariableService, characterStore);
                        break;
                    case X_B_PG_BOOL:
                        setBindVariableAsBoolean(j, valueSize, bindVariableService);
                        break;
                    case X_B_PG_BYTEA:
                        setBindVariableAsBin(j, lo, valueSize, bindVariableService, binarySequenceParamsPool);
                        break;
                    case X_B_PG_UUID:
                        setUuidBindVariable(j, lo, valueSize, bindVariableService);
                        break;
                    default:
                        setBindVariableAsStr(j, lo, valueSize, bindVariableService, characterStore, utf8String);
                        break;
                }
                lo += valueSize;
            } else {
                LOG.error().$("value length is outside of buffer [parameterIndex=").$(j).$(", valueSize=").$(valueSize).$(", messageRemaining=").$(msgLimit - lo).I$();
                throw BadProtocolException.INSTANCE;
            }
        }
        return lo;
    }

    // todo: this is required for "describe" and "sync" - may be we're building this too soon?
    private void msgParseBuildResultSetColumnTypes() {
        final RecordMetadata m = factory.getMetadata();
        final int columnCount = m.getColumnCount();
        pgResultSetColumnTypes.setPos(2 * columnCount);
        for (int i = 0; i < columnCount; i++) {
            final int columnType = m.getColumnType(i);
            pgResultSetColumnTypes.setQuick(2 * i, columnType);
            // the extra values stored here are used to render geo-hashes as strings
            pgResultSetColumnTypes.setQuick(2 * i + 1, GeoHashes.getBitFlags(columnType));
        }
    }
}
