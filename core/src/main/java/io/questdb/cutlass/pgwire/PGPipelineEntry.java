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
import io.questdb.cairo.*;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.*;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.QueryPausedException;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.pgwire.PGConnectionContext.*;
import static io.questdb.cutlass.pgwire.PGOids.*;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_MILLI_TIME_Z_PRINT_FORMAT;

public class PGPipelineEntry implements QuietCloseable {
    private final CompiledQueryImpl compiledQuery;
    private final CairoEngine engine;
    private final StringSink errorMessageSink = new StringSink();
    // stores result format codes (0=Text,1=Binary) from the latest bind message
    // we need it in case cursor gets invalidated and bind used non-default binary format for some column(s)
    // pg clients (like asyncpg) fail when format sent by server is not the same as requested in bind message
    private final IntList msgBindSelectFormatCodes = new IntList();
    // types are sent to us via "parse" message
    private final IntList msgParseParameterTypes = new IntList();
    private final IntList outTypeDescriptionTypes = new IntList();
    // list of pair: column types (with format flag stored in first bit) AND additional type flag
    private final IntList pgResultSetColumnTypes = new IntList();
    private final ObjList<CharSequence> portalNames = new ObjList<>();
    private boolean cacheHit = false;    // extended protocol cursor resume callback
    private RecordCursor cursor;
    private boolean empty;
    private boolean error = false;
    private int errorMessagePosition;
    // this is a "union", so should only be one, depending on SQL type
    // SELECT or EXPLAIN
    private RecordCursorFactory factory = null;
    private InsertOperation insertOp = null;
    private int msgBindParameterFormatCodeCount;
    private int msgBindParameterValueCount;
    private boolean outResendCursorRecord = false;
    private PGPipelineEntry parentPreparedStatementPipelineEntry;
    private boolean portal = false;
    private String portalName;
    private boolean preparedStatement = false;
    private String preparedStatementName;
    // the name of the prepared statement as used by "deallocate" SQL
    // not to be confused with prepared statements that come on the
    // PostgresSQL wire.
    private CharSequence preparedStatementNameToDeallocate;
    private long sqlAffectedRowCount = 0;
    // The count of rows sent that have been sent to the client per fetch. Client can either
    // fetch all rows at once, or in batches. In case of full fetch, this is the
    // count of rows in the cursor. If client fetches in batches, this is the count
    // of rows we sent so far in the current batch.
    // It is important to know this is NOT the count to be sent, this is the count we HAVE sent.
    private long sqlReturnRowCount = 0;
    // The row count sent to us by the client. This is the size of the batch the client wants to
    // receive from us.
    // todo: rename to batch size perhaps or client fetch size
    private long sqlReturnRowCountLimit = 0;
    private long sqlReturnRowCountToBeSent = 0;
    private String sqlTag = null;
    private CharSequence sqlText = null;
    private boolean sqlTextHasSecret = false;
    private short sqlType = 0;
    private boolean stateBind;
    private boolean stateClosed;
    private int stateDesc;
    private boolean stateExec = false;
    // boolean state, bitset?
    private boolean stateParse;
    private boolean stateParseExecuted = false;
    private int stateSync = 0;
    private TypesAndInsert tai = null;
    private TypesAndSelect tas = null;

    public PGPipelineEntry(CairoEngine engine) {
        this.engine = engine;
        this.compiledQuery = new CompiledQueryImpl(engine);
    }

    public void bindPortalName(CharSequence portalName) {
        portalNames.add(portalName);
    }

    public void cacheIfPossible(AssociativeCache<TypesAndSelect> tasCache, SimpleAssociativeCache<TypesAndInsert> taiCache) {
        if (isPortal() || isPreparedStatement()) {
            // must not cache prepared statements etc; we must only cache abandoned pipeline entries (their contents)
            return;
        }

        if (tas != null) {
            tasCache.put(Chars.toString(sqlText), tas);
            tas = null;
            // close cursor in case it is open
            cursor = Misc.free(cursor);
            // make sure factory is not released when the pipeline entry is closed
            factory = null;
            return;
        }

        if (tai != null) {
            taiCache.put(Chars.toString(sqlText), tai);
            // make sure we don't close insert operation when the pipeline entry is closed
            insertOp = null;
        }
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        factory = Misc.free(factory);
        insertOp = Misc.free(insertOp);
    }

    // return transaction state
    public int execute(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            SimpleAssociativeCache<TypesAndInsert> taiCache,
            ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters,
            WriterSource writerSource,
            CharSequenceObjHashMap<PGPipelineEntry> namedStatements
    ) {
        // do not execute anything, that has been parse-executed
        if (stateParseExecuted) {
            stateParseExecuted = false;
            return transactionState;
        }

        try {
            switch (this.sqlType) {
                case CompiledQuery.EXPLAIN:
                case CompiledQuery.SELECT:
                case CompiledQuery.PSEUDO_SELECT:
                    executeSelect(sqlExecutionContext);
                    break;
                case CompiledQuery.INSERT:
                    executeInsert(sqlExecutionContext, transactionState, taiCache, pendingWriters, writerSource);
                    break;
                case CompiledQuery.ALTER:
                case CompiledQuery.ALTER_USER:
                case CompiledQuery.CREATE_USER:
                case CompiledQuery.UPDATE:
                    sqlExecutionContext.containsSecret(sqlTextHasSecret);
                    executeCompiledQuery(sqlExecutionContext, transactionState);
                    break;
                case CompiledQuery.DEALLOCATE:
                    // this is supposed to work instead of sending 'close' message via the
                    // network protocol. My latest understanding is that this is meant to close either
                    // prepared statement or portal, depending on the name provided. The difference perhaps would be
                    // in the way we have to reply back to the client. Reply format out of 'execute' message is
                    // different from that of 'close' message.

                    preparedStatementNameToDeallocate = Chars.toString(compiledQuery.getStatementName());
                    throw kaput().put("unsupported for now");
                case CompiledQuery.BEGIN:
                    return IN_TRANSACTION;
                case CompiledQuery.COMMIT:
                case CompiledQuery.ROLLBACK:
                    freePendingWriters(pendingWriters, this.sqlType == CompiledQuery.COMMIT);
                    return NO_TRANSACTION;
                default:
                    // execute DDL that has not been parse-executed
                    if (!empty) {
                        engine.ddl(sqlText, sqlExecutionContext);
                    }
                    break;
            }
        } catch (Throwable e) {
            if (e instanceof FlyweightMessageContainer) {
                getErrorMessageSink().put(((FlyweightMessageContainer) e).getFlyweightMessage());
            } else {
                String message = e.getMessage();
                if (message != null) {
                    getErrorMessageSink().put(message);
                } else {
                    getErrorMessageSink().put("Internal error. Assert?");
                }
            }
        }
        return transactionState;
    }

    public int getErrorMessagePosition() {
        return errorMessagePosition;
    }

    public StringSink getErrorMessageSink() {
        error = true;
        return errorMessageSink;
    }

    public int getInt(long address, long msgLimit, CharSequence errorMessage) throws BadProtocolException {
        if (address + Integer.BYTES <= msgLimit) {
            return getIntUnsafe(address);
        }
        throw kaput().put(errorMessage);
    }

    public PGPipelineEntry getParentPreparedStatementPipelineEntry() {
        return parentPreparedStatementPipelineEntry;
    }

    public String getPortalName() {
        return portalName;
    }

    public ObjList<CharSequence> getPortalNames() {
        return portalNames;
    }

    public String getPreparedStatementName() {
        return preparedStatementName;
    }

    public short getShort(long address, long msgLimit, CharSequence errorMessage) throws BadProtocolException {
        if (address + Short.BYTES <= msgLimit) {
            return getShortUnsafe(address);
        }
        throw kaput().put(errorMessage);
    }

    public CharSequence getSqlText() {
        return sqlText;
    }

    public boolean isError() {
        return error;
    }

    public boolean isFactory() {
        return factory != null;
    }

    public boolean isPortal() {
        return portal;
    }

    public boolean isPreparedStatement() {
        return preparedStatement;
    }

    public boolean isStateExec() {
        return stateExec;
    }

    public void msgBindCopyParameterFormatCodes(long lo, long msgLimit, short formatCodeCount) throws BadProtocolException {
        this.msgBindParameterFormatCodeCount = formatCodeCount;
        if (formatCodeCount > 0) {
            if (formatCodeCount == 1) {
                // same format applies to all parameters
                msgBindMergeParameterTypesAndFormatCodesOneForAll(lo, msgLimit);
            } else if (outTypeDescriptionTypes.size() > 0) {
                //client doesn't need to specify types in Parse message and can use those returned in ParameterDescription
                msgBindMergeParameterTypesAndFormatCodes(lo, msgLimit, formatCodeCount);
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
                        throw kaput().put("could not process column format codes [fmtCount=").put(selectFormatCodeCount)
                                .put(", columnCount=").put(columnCount)
                                .put(']');

                    }
                } else {
                    throw kaput()
                            .put("could not process column format codes [bufSpaceNeeded=").put(spaceNeeded)
                            .put(", bufSpaceAvail=").put(msgLimit)
                            .put(']');

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
    ) throws BadProtocolException {
        if (msgBindParameterValueCount > 0) {
            // client doesn't need to specify any type in Parse message and can just use types returned in ParameterDescription message
            if (this.outTypeDescriptionTypes.size() > 0) {
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
                throw kaput().put("parameter type count must be less or equals to number of parameters values");
            }
            if (msgBindParameterFormatCodeCount > 1 && msgBindParameterFormatCodeCount != valueCount) {
                throw kaput().put("parameter format count and parameter value count must match");
            }
        }
    }

    public void msgParseCopyParameterTypesFromMsg(long lo, short parameterTypeCount) {
        msgParseParameterTypes.setPos(parameterTypeCount);
        for (int i = 0; i < parameterTypeCount; i++) {
            msgParseParameterTypes.setQuick(i, Unsafe.getUnsafe().getInt(lo + i * 4L));
        }
    }

    public void ofInsert(CharSequence utf16SqlText, TypesAndInsert tai) {
        this.sqlText = utf16SqlText;
        this.insertOp = tai.getInsert();
        this.sqlTag = tai.getSqlTag();
        this.sqlType = tai.getSqlType();
        this.cacheHit = true;
        this.tai = tai;
        tai.copyOutTypeDescriptionTypesTo(outTypeDescriptionTypes);
    }

    public void ofSelect(CharSequence utf16SqlText, TypesAndSelect tas) {
        this.sqlText = utf16SqlText;
        this.factory = tas.getFactory();
        this.sqlTag = tas.getSqlTag();
        this.sqlType = tas.getSqlType();
        this.tas = tas;
        this.cacheHit = true;
        buildResultSetColumnTypes();
        tas.copyOutTypeDescriptionTypesTo(outTypeDescriptionTypes);
    }

    public void ofSimpleQuery(
            CharSequence sqlText,
            SqlExecutionContext sqlExecutionContext,
            CompiledQuery cq,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool
    ) throws BadProtocolException {
        // pipeline entries begin life as anonymous, typical pipeline length is 1-3 entries
        // we do not need to create new objects until we know we're caching the entry
        this.sqlText = sqlText;
        this.empty = sqlText == null || sqlText.length() == 0;
        cacheHit = false;

        // todo: this is a hack it does not belong here
        if (cq.getType() == CompiledQuery.SELECT || cq.getType() == CompiledQuery.EXPLAIN) {
            setStateDesc(2); // 2 = portal
        }
        if (!empty) {
            // try insert, peek because this is our private cache,
            // and we do not want to remove statement from it
            try {
                resolveSqlType(sqlExecutionContext, taiPool, cq);
                buildResultSetColumnTypes();
            } catch (Throwable e) {
                throw kaput().put(e);
            }
        }
    }

    public void parseNewSql(
            CharSequence sqlText,
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool
    ) throws BadProtocolException {
        // pipeline entries begin life as anonymous, typical pipeline length is 1-3 entries
        // we do not need to create new objects until we know we're caching the entry
        this.sqlText = sqlText;
        this.empty = sqlText == null || sqlText.length() == 0;
        cacheHit = true;
        if (!empty) {
            // try insert, peek because this is our private cache,
            // and we do not want to remove statement from it
            try {
                cacheHit = false;
                parseNew(
                        engine,
                        sqlExecutionContext,
                        taiPool
                );
                buildResultSetColumnTypes();
            } catch (Throwable e) {
                throw kaput().put(e);
            }
        }
    }

    public void setErrorMessagePosition(int errorMessagePosition) {
        this.errorMessagePosition = errorMessagePosition;
    }

    public void setParentPreparedStatement(PGPipelineEntry preparedStatementPipelineEntry) {
        this.parentPreparedStatementPipelineEntry = preparedStatementPipelineEntry;
    }

    public void setPortal(boolean portal, String portalName) {
        this.portal = portal;
        // because this is now a prepared statement, it means the entry is
        // cached. All flyweight objects referenced from cache have to be internalized
        this.sqlText = Chars.toString(this.sqlText);
        this.portalName = portalName;
    }

    public void setPreparedStatement(boolean preparedStatement, String preparedStatementName) {
        this.preparedStatement = preparedStatement;
        // because this is now a prepared statement, it means the entry is
        // cached. All flyweight objects referenced from cache have to be internalized
        this.sqlText = Chars.toString(this.sqlText);
        this.preparedStatementName = preparedStatementName;
    }

    public void setReturnRowCountLimit(int rowCountLimit) {
        this.sqlReturnRowCountLimit = rowCountLimit;
    }

    public void setStateBind(boolean stateBind) {
        this.stateBind = stateBind;
    }

    public void setStateClosed(boolean stateClosed) {
        this.stateClosed = stateClosed;
        this.portal = false;
        this.preparedStatement = false;
    }

    public void setStateDesc(int stateDesc) {
        this.stateDesc = stateDesc;
    }

    public void setStateExec(boolean stateExec) {
        this.stateExec = stateExec;
    }

    public void setStateParse(boolean stateParse) {
        this.stateParse = stateParse;
    }

    /**
     * This method writes the response to the provided sink. The response is typically
     * larger than the available buffer. For that reason this method also flushes the buffers. During the
     * buffer flush it is entirely possible that nothing is receiving our data on the other side of the
     * network. For that reason this method maintains state and is re-entrant. If it is to throw an exception
     * pertaining network difficulties, the calling party must fix those difficulties and call this method
     * again.
     *
     * @param sqlExecutionContext the execution context used to optionally execute SQL and send result set out.
     * @param transactionState    the state of the current transaction; determines if to use insert auto-commit or not
     * @param taiCache            "insert" SQL cache, used to optionally execute the insert (when 'E' message is omitted)
     * @param pendingWriters      per connection write cache to be used by "insert" SQL. This is also part of the optional "execute"
     * @param writerSource        global writer cache
     * @param namedStatements
     * @param utf8Sink
     * @return
     * @throws Exception
     */
    public int sync(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            SimpleAssociativeCache<TypesAndInsert> taiCache,
            ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters,
            WriterSource writerSource,
            CharSequenceObjHashMap<PGPipelineEntry> namedStatements,
            PGResponseSink utf8Sink
    ) throws QueryPausedException, BadProtocolException {
        if (isError()) {
            utf8Sink.resetToBookmark();
            // todo: we need to test scenario, when sync does not fit the buffer
            outError(utf8Sink, getErrorMessagePosition(), getErrorMessageSink());
            freePendingWriters(pendingWriters, false);
        } else {
            switch (stateSync) {
                case 0:
                    if (stateParse) {
                        outParseComplete(utf8Sink);
                    }
                    stateSync = 1;
                case 1:
                    if (stateBind) {
                        outBindComplete(utf8Sink);
                    }
                    stateSync = 2;
                case 2:
                    switch (stateDesc) {
                        case 3:
                            // named prepared statement
                            outParameterTypeDescription(utf8Sink);
                            // fall through
                        case 2:
                        case 1:
                            // portal
                            if (factory != null) {
                                outRowDescription(utf8Sink);
                            } else {
                                outNoData(utf8Sink);
                            }
                            break;
                    }
                    stateSync = 3;
                case 3:
                    if (stateClosed) {
                        outSimpleMsg(utf8Sink, MESSAGE_TYPE_CLOSE_COMPLETE);
                    }
                    stateSync = 4;
                case 4:
                case 5:
                    // state goes deeper
                    if (empty && !preparedStatement && !portal) {
                        // strangely, Java driver does not need the server to produce
                        // empty query if his query was "prepared"
                        outEmptyQuery(utf8Sink);
                        stateSync = 6;
                    } else {
//                    if (!stateExec) {
//                        execute(sqlExecutionContext, transactionState, taiCache, pendingWriters, writerSource, namedStatements);
//                        stateExec = true;
//                    }
                        if (stateExec) {
                            // the flow when the pipeline entry was executed
                            switch (sqlType) {
                                case CompiledQuery.EXPLAIN:
                                case CompiledQuery.SELECT:
                                case CompiledQuery.PSEUDO_SELECT:
                                    // This is a long response (data set) and because of
                                    // this we are entering the interruptible state machine here. In that,
                                    // this call may end up in an exception and the code will have to be re-entered
                                    // at some point. Our own completion callback will invoke the pipeline callback
                                    outCursor(sqlExecutionContext, utf8Sink);
                                    // the above method changes state
                                    break;
                                case CompiledQuery.INSERT_AS_SELECT:
                                case CompiledQuery.INSERT: {
                                    utf8Sink.bookmark();
                                    // todo: if we get sent a lot of inserts as the pipeline, we might run out of buffer
                                    //           sending the replies. We should handle this
                                    utf8Sink.put(MESSAGE_TYPE_COMMAND_COMPLETE);
                                    long addr = utf8Sink.skipInt();
                                    utf8Sink.put(sqlTag).putAscii(" 0 ").put(sqlAffectedRowCount).put((byte) 0);
                                    utf8Sink.putLen(addr);
                                    stateSync = 6;
                                    break;
                                }
                                case CompiledQuery.UPDATE: {
                                    outCommandComplete(utf8Sink, sqlAffectedRowCount);
                                    stateSync = 6;
                                    break;
                                }
                                default:
                                    // create table is just "OK"
                                    utf8Sink.put(MESSAGE_TYPE_COMMAND_COMPLETE);
                                    long addr = utf8Sink.skipInt();
                                    utf8Sink.put(sqlTag).put((byte) 0);
                                    utf8Sink.putLen(addr);
                                    stateSync = 6;
                                    break;
                            }
                        }
                    }
                    break;
                case 20:
                case 30:
                    // ignore these, they are set by outCursor() call and should be processed outside of this
                    // switch statement
                    break;
                default:
                    assert false;
            }

            // this is a separate switch because there is no way to-recheck the stateSync that
            // is set withing the top switch. These values are set by outCursor()

            switch (stateSync) {
                case 20:
                    cursor = Misc.free(cursor);
                    outCommandComplete(utf8Sink, sqlReturnRowCount);
                    break;
                case 30:
                    outPortalSuspended(utf8Sink);
                    break;
            }

            if (isError()) {
                utf8Sink.resetToBookmark();
                // todo: we need to test scenario, when sync does not fit the buffer
                outError(utf8Sink, 0, getErrorMessageSink());
            }
        }

        // after the pipeline entry is synchronized we should prepare it for the next
        // execution iteration, in case the entry is a prepared statement or a portal
        clearState();
        return transactionState;
    }

    private static void outBindComplete(PGResponseSink utf8Sink) {
        outSimpleMsg(utf8Sink, MESSAGE_TYPE_BIND_COMPLETE);
    }

    private static void outEmptyQuery(PGResponseSink utf8Sink) {
        outSimpleMsg(utf8Sink, MESSAGE_TYPE_EMPTY_QUERY);
    }

    private static void outNoData(PGResponseSink utf8Sink) {
        outSimpleMsg(utf8Sink, MESSAGE_TYPE_NO_DATA);
    }

    private static void outParseComplete(PGResponseSink utf8Sink) {
        outSimpleMsg(utf8Sink, MESSAGE_TYPE_PARSE_COMPLETE);
    }

    private static void outPortalSuspended(PGResponseSink utf8Sink) {
        outSimpleMsg(utf8Sink, MESSAGE_TYPE_PORTAL_SUSPENDED);
    }

    private static void outSimpleMsg(PGResponseSink utf8Sink, byte msgByte) {
        utf8Sink.bookmark();
        utf8Sink.put(msgByte);
        utf8Sink.putIntDirect(INT_BYTES_X);
        utf8Sink.bookmark();
    }

    private static void setBindVariableAsBin(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService,
            ObjectPool<DirectBinarySequence> binarySequenceParamsPool
    ) throws SqlException {
        // todo: copy bind variable into the arena
        bindVariableService.setBin(variableIndex, binarySequenceParamsPool.next().of(valueAddr, valueSize));
    }

    private static void setBindVariableAsBoolean(
            int variableIndex,
            int valueSize,
            BindVariableService bindVariableService
    ) throws SqlException {
        if (valueSize != 4 && valueSize != 5) {
            throw SqlException
                    .$(0, "bad value for BOOLEAN parameter [variableIndex=").put(variableIndex)
                    .put(", valueSize=").put(valueSize)
                    .put(']');
        }
        bindVariableService.setBoolean(variableIndex, valueSize == 4);
    }

    private void applyBindSelectFormatCodes() {
        for (int i = 0; i < msgBindSelectFormatCodes.size(); i++) {
            int newValue = toColumnBinaryType((short) msgBindSelectFormatCodes.get(i), toColumnType(pgResultSetColumnTypes.getQuick(2 * i)));
            pgResultSetColumnTypes.setQuick(2 * i, newValue);
        }
    }

    private void buildResultSetColumnTypes() {
        if (factory != null) {
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

    private void defineBindVariableType(BindVariableService bindVariableService, int j) throws SqlException {
        switch (msgParseParameterTypes.getQuick(j)) {
            case X_PG_INT4:
                bindVariableService.define(j, ColumnType.INT, 0);
                break;
            case X_PG_INT8:
                bindVariableService.define(j, ColumnType.LONG, 0);
                break;
            case X_PG_TIMESTAMP:
                bindVariableService.define(j, ColumnType.TIMESTAMP, 0);
                break;
            case X_PG_INT2:
                bindVariableService.define(j, ColumnType.SHORT, 0);
                break;
            case X_PG_FLOAT8:
                bindVariableService.define(j, ColumnType.DOUBLE, 0);
                break;
            case X_PG_FLOAT4:
                bindVariableService.define(j, ColumnType.FLOAT, 0);
                break;
            case X_PG_CHAR:
                bindVariableService.define(j, ColumnType.CHAR, 0);
                break;
            case X_PG_DATE:
                bindVariableService.define(j, ColumnType.DATE, 0);
                break;
            case X_PG_BOOL:
                bindVariableService.define(j, ColumnType.BOOLEAN, 0);
                break;
            case X_PG_BYTEA:
                bindVariableService.define(j, ColumnType.BINARY, 0);
                break;
            case X_PG_UUID:
                bindVariableService.define(j, ColumnType.UUID, 0);
                break;
            default:
                bindVariableService.define(j, ColumnType.STRING, 0);
                break;
        }
    }

    private void ensureValueLength(int variableIndex, int sizeRequired, int sizeActual) throws BadProtocolException {
        if (sizeRequired == sizeActual) {
            return;
        }
        throw kaput()
                .put("bad parameter value length [sizeRequired=").put(sizeRequired)
                .put(", sizeActual=").put(sizeActual)
                .put(", variableIndex=").put(variableIndex)
                .put(']');
    }

    private void executeCompiledQuery(SqlExecutionContext sqlExecutionContext, int transactionState) throws SqlException {
        if (transactionState != ERROR_TRANSACTION) {
            // execute against writer from the engine, synchronously (null sequence)
            try (OperationFuture fut = compiledQuery.execute(sqlExecutionContext, null, true)) {
                // this doesn't actually wait, because the call is synchronous
                fut.await();
                sqlAffectedRowCount = fut.getAffectedRowsCount();
            }
        }
    }

    private void executeInsert(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            // todo: WriterSource is the interface used exclusively in PG Wire. We should not need to pass
            //    around heaps of state in very long call stacks
            SimpleAssociativeCache<TypesAndInsert> taiCache,
            ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters,
            WriterSource writerSource
    ) throws SqlException {
        switch (transactionState) {
            case IN_TRANSACTION:
                final InsertMethod m = insertOp.createMethod(sqlExecutionContext, writerSource);
                try {
                    sqlAffectedRowCount = m.execute();
                    TableWriterAPI writer = m.popWriter();
                    pendingWriters.put(writer.getTableToken(), writer);
                    if (tai.hasBindVariables()) {
                        taiCache.put(sqlText, tai);
                    }
                } catch (Throwable e) {
                    Misc.free(m);
                    throw e;
                }
                break;
            case ERROR_TRANSACTION:
                // when transaction is in error state, skip execution
                break;
            default:
                // in any other case we will commit in place
                try (final InsertMethod m2 = insertOp.createMethod(sqlExecutionContext, writerSource)) {
                    sqlAffectedRowCount = m2.execute();
                    m2.commit();
                }
                if (tai.hasBindVariables()) {
                    taiCache.put(sqlText, tai);
                }
                break;
        }
    }

    private void executeSelect(SqlExecutionContext sqlExecutionContext) throws SqlException {
        // todo: we should reuse cursor for portal executions, but not others
        if (cursor == null) {
            sqlExecutionContext.getCircuitBreaker().resetTimer();
            sqlExecutionContext.setCacheHit(cacheHit);
            try {
                cursor = factory.getCursor(sqlExecutionContext);
                // move binary/text flags, which we received from the "bind" onto the result set column types
                applyBindSelectFormatCodes();
            } catch (Throwable e) {
                // un-cache the erroneous SQL
                tas = Misc.free(tas);
                factory = null;
                throw e;
            }
        }
    }

    private void freePendingWriters(ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters, boolean commit) {
        try {
            for (ObjObjHashMap.Entry<TableToken, TableWriterAPI> pendingWriter : pendingWriters) {
                final TableWriterAPI m = pendingWriter.value;
                if (commit) {
                    m.commit();
                } else {
                    m.rollback();
                }
                Misc.free(m);
            }
        } finally {
            pendingWriters.clear();
        }
    }

    private int getMsgParseParameterTypeCount() {
        return msgParseParameterTypes.size();
    }

    private BadProtocolException kaput() {
        return BadProtocolException.instance(this);
    }

    private long msgBindDefineBindVariablesAsStrings(
            long lo,
            long msgLimit,
            BindVariableService bindVariableService,
            CharacterStore characterStore,
            DirectUtf8String utf8String
    ) throws BadProtocolException {
        for (int j = 0; j < msgBindParameterValueCount; j++) {
            final int valueSize = getInt(lo, msgLimit, "malformed bind variable");
            lo += Integer.BYTES;

            if (valueSize != -1 && lo + valueSize <= msgLimit) {
                setBindVariableAsStr(j, lo, valueSize, bindVariableService, characterStore, utf8String);
                lo += valueSize;
            } else if (valueSize != -1) {
                throw kaput().put("value length is outside of buffer [parameterIndex=").put(j)
                        .put(", valueSize=").put(valueSize)
                        .put(", messageRemaining=").put(msgLimit - lo)
                        .put(']');
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
    ) throws BadProtocolException {
        try {
            for (int j = 0; j < outTypeDescriptionTypes.size(); j++) {
                final int valueSize = getInt(lo, msgLimit, "malformed bind variable");
                lo += Integer.BYTES;
                if (valueSize == -1) {
                    // undefined function?
                    defineBindVariableType(bindVariableService, j);
                } else if (lo + valueSize <= msgLimit) {
                    switch (outTypeDescriptionTypes.getQuick(j)) {
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
                    throw kaput()
                            .put("value length is outside of buffer [parameterIndex=").put(j)
                            .put(", valueSize=").put(valueSize)
                            .put(", messageRemaining=").put(msgLimit - lo)
                            .put(']');
                }
            }
        } catch (Throwable e) {
            throw kaput().put(e);
        }
        return lo;
    }

    private void msgBindMergeParameterTypesAndFormatCodes(long lo, long msgLimit, short parameterFormatCount) throws BadProtocolException {
        if (lo + Short.BYTES * parameterFormatCount <= msgLimit) {
            for (int i = 0; i < parameterFormatCount; i++) {
                final short code = getShortUnsafe(lo + i * Short.BYTES);
                outTypeDescriptionTypes.setQuick(i, toParamBinaryType(code, outTypeDescriptionTypes.getQuick(i)));
            }
        } else {
            getErrorMessageSink().put("invalid format code count [value=").put(parameterFormatCount).put(']');
            throw BadProtocolException.INSTANCE;
        }
    }

    private void msgBindMergeParameterTypesAndFormatCodesOneForAll(long lo, long msgLimit) throws BadProtocolException {
        short code = getShort(lo, msgLimit, "could not read parameter formats");
        for (int i = 0, n = outTypeDescriptionTypes.size(); i < n; i++) {
            outTypeDescriptionTypes.setQuick(i, toParamBinaryType(code, outTypeDescriptionTypes.getQuick(i)));
        }
    }

    private void msgParseCacheSelectSQL(SqlExecutionContext sqlExecutionContext, CompiledQuery cq, String sqlTag, boolean cache) {
        this.sqlTag = sqlTag;
        this.factory = cq.getRecordCursorFactory();
        if (cache) {
            tas = new TypesAndSelect(
                    this.factory,
                    sqlType,
                    sqlTag,
                    sqlExecutionContext.getBindVariableService(),
                    msgParseParameterTypes
            );
        }
        // copy actual bind variable types as supplied by the client + defined by the SQL
        // compiler
        msgParseCopyOutTypeDescriptionTypesFromService(sqlExecutionContext.getBindVariableService());
    }

    private void msgParseCopyOutTypeDescriptionTypesFromService(BindVariableService bindVariableService) {
        final int n = bindVariableService.getIndexedVariableCount();
        if (n > 0) {
            outTypeDescriptionTypes.setPos(n);
            for (int i = 0; i < n; i++) {
                final Function f = bindVariableService.getFunction(i);
                outTypeDescriptionTypes.setQuick(i, Numbers.bswap(PGOids.getTypeOid(
                        f != null ? f.getType() : ColumnType.UNDEFINED
                )));
            }
        }
    }

    private void msgParseDefineBindVariableTypes(BindVariableService bindVariableService) throws SqlException {
        for (int i = 0, n = msgParseParameterTypes.size(); i < n; i++) {
            defineBindVariableType(bindVariableService, i);
        }
    }

    private void outColBinBool(PGResponseSink utf8Sink, Record record, int columnIndex) {
        utf8Sink.putNetworkInt(Byte.BYTES);
        utf8Sink.put(record.getBool(columnIndex) ? (byte) 1 : (byte) 0);
    }

    private void outColBinByte(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final byte value = record.getByte(columnIndex);
        utf8Sink.putNetworkInt(Short.BYTES);
        utf8Sink.putNetworkShort(value);
    }

    private void outColBinDate(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final long longValue = record.getDate(columnIndex);
        if (longValue != Numbers.LONG_NULL) {
            utf8Sink.putNetworkInt(Long.BYTES);
            // PG epoch starts at 2000 rather than 1970
            utf8Sink.putNetworkLong(longValue * 1000 - Numbers.JULIAN_EPOCH_OFFSET_USEC);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColBinDouble(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final double value = record.getDouble(columnIndex);
        if (value == value) {
            utf8Sink.putNetworkInt(Double.BYTES);
            utf8Sink.putNetworkDouble(value);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColBinFloat(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final float value = record.getFloat(columnIndex);
        if (value == value) {
            utf8Sink.putNetworkInt(Float.BYTES);
            utf8Sink.putNetworkFloat(value);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColBinInt(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final int value = record.getInt(columnIndex);
        if (value != Numbers.INT_NULL) {
            utf8Sink.checkCapacity(8);
            utf8Sink.putIntUnsafe(0, INT_BYTES_X);
            utf8Sink.putIntUnsafe(4, Numbers.bswap(value));
            utf8Sink.bump(8);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColBinLong(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue != Numbers.LONG_NULL) {
            utf8Sink.putNetworkInt(Long.BYTES);
            utf8Sink.putNetworkLong(longValue);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColBinShort(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final short value = record.getShort(columnIndex);
        utf8Sink.putNetworkInt(Short.BYTES);
        utf8Sink.putNetworkShort(value);
    }

    private void outColBinTimestamp(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final long longValue = record.getTimestamp(columnIndex);
        if (longValue == Numbers.LONG_NULL) {
            utf8Sink.setNullValue();
        } else {
            utf8Sink.putNetworkInt(Long.BYTES);
            // PG epoch starts at 2000 rather than 1970
            utf8Sink.putNetworkLong(longValue - Numbers.JULIAN_EPOCH_OFFSET_USEC);
        }
    }

    private void outColBinUuid(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final long lo = record.getLong128Lo(columnIndex);
        final long hi = record.getLong128Hi(columnIndex);
        if (Uuid.isNull(lo, hi)) {
            utf8Sink.setNullValue();
        } else {
            utf8Sink.putNetworkInt(Long.BYTES * 2);
            utf8Sink.putNetworkLong(hi);
            utf8Sink.putNetworkLong(lo);
        }
    }

    private void outColBinary(PGResponseSink utf8Sink, Record record, int i) throws BadProtocolException {
        BinarySequence sequence = record.getBin(i);
        if (sequence == null) {
            utf8Sink.setNullValue();
        } else {
            // if length is above max we will error out the result set
            long blobSize = sequence.length();
            if (blobSize < utf8Sink.getMaxBlobSize()) {
                utf8Sink.put(sequence);
            } else {
                throw kaput()
                        .put("blob is too large [blobSize=").put(blobSize)
                        .put(", maxBlobSize=").put(utf8Sink.getMaxBlobSize())
                        .put(", columnIndex=").put(i)
                        .put(']');
            }
        }
    }

    private void outColChar(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final char charValue = record.getChar(columnIndex);
        if (charValue == 0) {
            utf8Sink.setNullValue();
        } else {
            long a = utf8Sink.skipInt();
            utf8Sink.put(charValue);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColLong256(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final Long256 long256Value = record.getLong256A(columnIndex);
        if (long256Value.getLong0() == Numbers.LONG_NULL && long256Value.getLong1() == Numbers.LONG_NULL && long256Value.getLong2() == Numbers.LONG_NULL && long256Value.getLong3() == Numbers.LONG_NULL) {
            utf8Sink.setNullValue();
        } else {
            final long a = utf8Sink.skipInt();
            Numbers.appendLong256(long256Value.getLong0(), long256Value.getLong1(), long256Value.getLong2(), long256Value.getLong3(), utf8Sink);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColString(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final CharSequence strValue = record.getStrA(columnIndex);
        if (strValue == null) {
            utf8Sink.setNullValue();
        } else {
            final long a = utf8Sink.skipInt();
            utf8Sink.put(strValue);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColSymbol(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final CharSequence strValue = record.getSymA(columnIndex);
        if (strValue == null) {
            utf8Sink.setNullValue();
        } else {
            final long a = utf8Sink.skipInt();
            utf8Sink.put(strValue);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColTxtBool(PGResponseSink utf8Sink, Record record, int columnIndex) {
        utf8Sink.putNetworkInt(Byte.BYTES);
        utf8Sink.put(record.getBool(columnIndex) ? 't' : 'f');
    }

    private void outColTxtByte(PGResponseSink utf8Sink, Record record, int columnIndex) {
        long a = utf8Sink.skipInt();
        utf8Sink.put((int) record.getByte(columnIndex));
        utf8Sink.putLenEx(a);
    }

    private void outColTxtDate(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final long longValue = record.getDate(columnIndex);
        if (longValue != Numbers.LONG_NULL) {
            final long a = utf8Sink.skipInt();
            PG_DATE_MILLI_TIME_Z_PRINT_FORMAT.format(longValue, DateFormatUtils.EN_LOCALE, null, utf8Sink);
            utf8Sink.putLenEx(a);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColTxtDouble(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final double doubleValue = record.getDouble(columnIndex);
        if (doubleValue == doubleValue) {
            final long a = utf8Sink.skipInt();
            utf8Sink.put(doubleValue);
            utf8Sink.putLenEx(a);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColTxtFloat(PGResponseSink responseUtf8Sink, Record record, int columnIndex) {
        final float floatValue = record.getFloat(columnIndex);
        if (floatValue == floatValue) {
            final long a = responseUtf8Sink.skipInt();
            responseUtf8Sink.put(floatValue, 3);
            responseUtf8Sink.putLenEx(a);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void outColTxtGeoByte(PGResponseSink utf8Sink, Record rec, int col, int bitFlags) {
        outColTxtGeoHash(utf8Sink, rec.getGeoByte(col), bitFlags);
    }

    private void outColTxtGeoHash(PGResponseSink utf8Sink, long value, int bitFlags) {
        if (value == GeoHashes.NULL) {
            utf8Sink.setNullValue();
        } else {
            final long a = utf8Sink.skipInt();
            if (bitFlags < 0) {
                GeoHashes.appendCharsUnsafe(value, -bitFlags, utf8Sink);
            } else {
                GeoHashes.appendBinaryStringUnsafe(value, bitFlags, utf8Sink);
            }
            utf8Sink.putLenEx(a);
        }
    }

    private void outColTxtGeoInt(PGResponseSink utf8Sink, Record rec, int col, int bitFlags) {
        outColTxtGeoHash(utf8Sink, rec.getGeoInt(col), bitFlags);
    }

    private void outColTxtGeoLong(PGResponseSink utf8Sink, Record rec, int col, int bitFlags) {
        outColTxtGeoHash(utf8Sink, rec.getGeoLong(col), bitFlags);
    }

    private void outColTxtGeoShort(PGResponseSink utf8Sink, Record rec, int col, int bitFlags) {
        outColTxtGeoHash(utf8Sink, rec.getGeoShort(col), bitFlags);
    }

    private void outColTxtIPv4(PGResponseSink utf8Sink, Record record, int columnIndex) {
        int value = record.getIPv4(columnIndex);
        if (value == Numbers.IPv4_NULL) {
            utf8Sink.setNullValue();
        } else {
            final long a = utf8Sink.skipInt();
            Numbers.intToIPv4Sink(utf8Sink, value);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColTxtInt(PGResponseSink utf8Sink, Record record, int i) {
        final int intValue = record.getInt(i);
        if (intValue != Numbers.INT_NULL) {
            final long a = utf8Sink.skipInt();
            utf8Sink.put(intValue);
            utf8Sink.putLenEx(a);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColTxtLong(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue != Numbers.LONG_NULL) {
            final long a = utf8Sink.skipInt();
            utf8Sink.put(longValue);
            utf8Sink.putLenEx(a);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColTxtShort(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final long a = utf8Sink.skipInt();
        utf8Sink.put(record.getShort(columnIndex));
        utf8Sink.putLenEx(a);
    }

    private void outColTxtTimestamp(PGResponseSink utf8Sink, Record record, int i) {
        long a;
        long longValue = record.getTimestamp(i);
        if (longValue == Numbers.LONG_NULL) {
            utf8Sink.setNullValue();
        } else {
            a = utf8Sink.skipInt();
            TimestampFormatUtils.PG_TIMESTAMP_FORMAT.format(longValue, DateFormatUtils.EN_LOCALE, null, utf8Sink);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColTxtUuid(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final long lo = record.getLong128Lo(columnIndex);
        final long hi = record.getLong128Hi(columnIndex);
        if (Uuid.isNull(lo, hi)) {
            utf8Sink.setNullValue();
        } else {
            final long a = utf8Sink.skipInt();
            Numbers.appendUuid(lo, hi, utf8Sink);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColVarchar(PGResponseSink responseUtf8Sink, Record record, int i) {
        final Utf8Sequence strValue = record.getVarcharA(i);
        if (strValue == null) {
            responseUtf8Sink.setNullValue();
        } else {
            responseUtf8Sink.putNetworkInt(strValue.size());
            responseUtf8Sink.put(strValue);
        }
    }

    private void outCommandComplete(PGResponseSink utf8Sink, long rowCount) {
        utf8Sink.bookmark();
        utf8Sink.put(MESSAGE_TYPE_COMMAND_COMPLETE);
        long addr = utf8Sink.skipInt();
        utf8Sink.put(sqlTag).putAscii(' ').put(rowCount).put((byte) 0);
        utf8Sink.putLen(addr);
    }

    private void outComputeCursorSize() {
        this.sqlReturnRowCount = 0;
        if (sqlReturnRowCountLimit > 0) {
            sqlReturnRowCountToBeSent = sqlReturnRowCountLimit;
        } else {
            this.sqlReturnRowCountToBeSent = Long.MAX_VALUE;
        }
    }

    private void outCursor(
            SqlExecutionContext sqlExecutionContext,
            PGResponseSink utf8Sink,
            Record record,
            int columnCount
    ) throws QueryPausedException {
        if (!sqlExecutionContext.getCircuitBreaker().isTimerSet()) {
            sqlExecutionContext.getCircuitBreaker().resetTimer();
        }

        try {
            if (outResendCursorRecord) {
                utf8Sink.resetToBookmark();
                outRecord(utf8Sink, record, columnCount);
            }

            while (sqlReturnRowCount < sqlReturnRowCountToBeSent && cursor.hasNext()) {
                outResendCursorRecord = true;
                outRecord(utf8Sink, record, columnCount);
            }
        } catch (DataUnavailableException e) {
            stateSync = 100; // query is paused
            utf8Sink.resetToBookmark();
            throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
        } catch (NoSpaceLeftInResponseBufferException e) {
            throw e;
        } catch (Throwable e) {
            utf8Sink.resetToBookmark();
            if (e instanceof FlyweightMessageContainer) {
                getErrorMessageSink().put(((FlyweightMessageContainer) e).getFlyweightMessage());
            } else {
                e.printStackTrace();
                String msg = e.getMessage();
                getErrorMessageSink().put(msg != null ? msg : "no message provided (internal error)");
            }
        }

        // the above loop may have exited due to the return row limit as prescribed by the portal
        // either way, the result set was sent out as intended. The difference is in what we
        // send as the suffix.

        if (sqlReturnRowCount < sqlReturnRowCountToBeSent || !isPortal()) {
            // the limit does not tell is if it came from "fetchSize" or "maxRows". The former is
            // used for portals and effectively prescribes us to keep the cursor open and primed to continue the
            // fetch. The latter is acting like SQL limit query. E.g. we return top X rows and dispose of the cursor.
            stateSync = 20;
        } else {
            // we sent as many rows as was requested, but we have more to send
            stateSync = 30;
        }
    }

    private void outCursor(SqlExecutionContext sqlExecutionContext, PGResponseSink utf8Sink)
            throws QueryPausedException {
        switch (stateSync) {
            case 4:
                outComputeCursorSize();
                stateSync = 5;
            case 5:
                utf8Sink.bookmark();
                outCursor(
                        sqlExecutionContext,
                        utf8Sink,
                        cursor.getRecord(),
                        factory.getMetadata().getColumnCount()
                );
                break;
            default:
                assert false;
        }
    }

    private void outError(PGResponseSink utf8Sink, int position, CharSequence message) {
        utf8Sink.put(MESSAGE_TYPE_ERROR_RESPONSE);
        long addr = utf8Sink.skipInt();
        utf8Sink.putAscii('C');
        utf8Sink.putZ("00000");
        utf8Sink.putAscii('M');
        utf8Sink.putZ(message);
        utf8Sink.putAscii('S');
        utf8Sink.putZ("ERROR");
        if (position > -1) {
            utf8Sink.putAscii('P').put(position + 1).put((byte) 0);
        }
        utf8Sink.put((byte) 0);
        utf8Sink.putLen(addr);
    }

    private void outParameterTypeDescription(PGResponseSink utf8Sink) {
        utf8Sink.put(MESSAGE_TYPE_PARAMETER_DESCRIPTION);
        final long offset = utf8Sink.skipInt();
        final int n = outTypeDescriptionTypes.size();
        utf8Sink.putNetworkShort((short) n);
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                utf8Sink.putIntDirect(toParamType(outTypeDescriptionTypes.getQuick(i)));
            }
        }
        utf8Sink.putLen(offset);
    }

    private void outRecord(PGResponseSink utf8Sink, Record record, int columnCount) throws BadProtocolException {
        utf8Sink.put(MESSAGE_TYPE_DATA_ROW); // data
        final long offset = utf8Sink.skipInt();
        utf8Sink.putNetworkShort((short) columnCount);
        for (int i = 0; i < columnCount; i++) {
            final int type = pgResultSetColumnTypes.getQuick(2 * i);
            final short columnBinaryFlag = getColumnBinaryFlag(type);
            final int typeTag = ColumnType.tagOf(type);

            final int tagWithFlag = toColumnBinaryType(columnBinaryFlag, typeTag);
            switch (tagWithFlag) {
                case BINARY_TYPE_INT:
                    outColBinInt(utf8Sink, record, i);
                    break;
                case ColumnType.INT:
                    outColTxtInt(utf8Sink, record, i);
                    break;
                case ColumnType.IPv4:
                    outColTxtIPv4(utf8Sink, record, i);
                    break;
                case ColumnType.VARCHAR:
                case BINARY_TYPE_VARCHAR:
                    outColVarchar(utf8Sink, record, i);
                    break;
                case ColumnType.STRING:
                case BINARY_TYPE_STRING:
                    outColString(utf8Sink, record, i);
                    break;
                case ColumnType.SYMBOL:
                case BINARY_TYPE_SYMBOL:
                    outColSymbol(utf8Sink, record, i);
                    break;
                case BINARY_TYPE_LONG:
                    outColBinLong(utf8Sink, record, i);
                    break;
                case ColumnType.LONG:
                    outColTxtLong(utf8Sink, record, i);
                    break;
                case ColumnType.SHORT:
                    outColTxtShort(utf8Sink, record, i);
                    break;
                case BINARY_TYPE_DOUBLE:
                    outColBinDouble(utf8Sink, record, i);
                    break;
                case ColumnType.DOUBLE:
                    outColTxtDouble(utf8Sink, record, i);
                    break;
                case BINARY_TYPE_FLOAT:
                    outColBinFloat(utf8Sink, record, i);
                    break;
                case BINARY_TYPE_SHORT:
                    outColBinShort(utf8Sink, record, i);
                    break;
                case BINARY_TYPE_DATE:
                    outColBinDate(utf8Sink, record, i);
                    break;
                case BINARY_TYPE_TIMESTAMP:
                    outColBinTimestamp(utf8Sink, record, i);
                    break;
                case BINARY_TYPE_BYTE:
                    outColBinByte(utf8Sink, record, i);
                    break;
                case BINARY_TYPE_UUID:
                    outColBinUuid(utf8Sink, record, i);
                    break;
                case ColumnType.FLOAT:
                    outColTxtFloat(utf8Sink, record, i);
                    break;
                case ColumnType.TIMESTAMP:
                    outColTxtTimestamp(utf8Sink, record, i);
                    break;
                case ColumnType.DATE:
                    outColTxtDate(utf8Sink, record, i);
                    break;
                case ColumnType.BOOLEAN:
                    outColTxtBool(utf8Sink, record, i);
                    break;
                case BINARY_TYPE_BOOLEAN:
                    outColBinBool(utf8Sink, record, i);
                    break;
                case ColumnType.BYTE:
                    outColTxtByte(utf8Sink, record, i);
                    break;
                case ColumnType.BINARY:
                case BINARY_TYPE_BINARY:
                    outColBinary(utf8Sink, record, i);
                    break;
                case ColumnType.CHAR:
                case BINARY_TYPE_CHAR:
                    outColChar(utf8Sink, record, i);
                    break;
                case ColumnType.LONG256:
                case BINARY_TYPE_LONG256:
                    outColLong256(utf8Sink, record, i);
                    break;
                case ColumnType.GEOBYTE:
                    outColTxtGeoByte(utf8Sink, record, i, pgResultSetColumnTypes.getQuick(2 * i + 1));
                    break;
                case ColumnType.GEOSHORT:
                    outColTxtGeoShort(utf8Sink, record, i, pgResultSetColumnTypes.getQuick(2 * i + 1));
                    break;
                case ColumnType.GEOINT:
                    outColTxtGeoInt(utf8Sink, record, i, pgResultSetColumnTypes.getQuick(2 * i + 1));
                    break;
                case ColumnType.GEOLONG:
                    outColTxtGeoLong(utf8Sink, record, i, pgResultSetColumnTypes.getQuick(2 * i + 1));
                    break;
                case ColumnType.NULL:
                    utf8Sink.setNullValue();
                    break;
                case ColumnType.UUID:
                    outColTxtUuid(utf8Sink, record, i);
                    break;
                default:
                    assert false;
            }
        }
        utf8Sink.putLen(offset);
        utf8Sink.bookmark();
        outResendCursorRecord = false;
        sqlReturnRowCount++;
    }

    private void outRowDescription(PGResponseSink utf8Sink) {
        //todo: wide table metadata can overflow the send buffer and corrupt communication
        final RecordMetadata metadata = factory.getMetadata();
        utf8Sink.put(MESSAGE_TYPE_ROW_DESCRIPTION);
        final long addr = utf8Sink.skipInt();
        final int n = pgResultSetColumnTypes.size() / 2;
        utf8Sink.putNetworkShort((short) n);
        for (int i = 0; i < n; i++) {
            final int typeFlag = pgResultSetColumnTypes.getQuick(2 * i);
            final int columnType = toColumnType(ColumnType.isNull(typeFlag) ? ColumnType.STRING : typeFlag);
            utf8Sink.putZ(metadata.getColumnName(i));
            utf8Sink.putIntDirect(0); //tableOid ?
            utf8Sink.putNetworkShort((short) (i + 1)); //column number, starting from 1
            utf8Sink.putNetworkInt(PGOids.getTypeOid(columnType)); // type
            if (ColumnType.tagOf(columnType) < ColumnType.STRING) {
                // type size
                // todo: cache small endian type sizes and do not check if type is valid - its coming from metadata, must be always valid
                utf8Sink.putNetworkShort((short) ColumnType.sizeOf(columnType));
            } else {
                // type size
                utf8Sink.putNetworkShort((short) -1);
            }

            // type modifier
            utf8Sink.putIntDirect(INT_NULL_X);
            // this is special behaviour for binary fields to prevent binary data being hex encoded on the wire
            // format code
            utf8Sink.putNetworkShort(ColumnType.isBinary(columnType) ? 1 : getColumnBinaryFlag(typeFlag)); // format code
        }
        utf8Sink.putLen(addr);
        utf8Sink.bookmark();
    }

    private void parseNew(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            // Define the provided PostgresSQL types on the BindVariableService. The compilation
            // below will use these types to build the plan, and it will also define any missing bind
            // variables.
            msgParseDefineBindVariableTypes(sqlExecutionContext.getBindVariableService());
            CompiledQuery cq = compiler.compile(sqlText, sqlExecutionContext);
            resolveSqlType(sqlExecutionContext, taiPool, cq);
        }
    }

    private void resolveSqlType(
            SqlExecutionContext sqlExecutionContext,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool,
            CompiledQuery cq
    ) {
        sqlExecutionContext.storeTelemetry(cq.getType(), TelemetryOrigin.POSTGRES);

        this.sqlType = cq.getType();
        switch (sqlType) {
            case CompiledQuery.CREATE_TABLE_AS_SELECT:
                sqlTag = TAG_OK;
                sqlAffectedRowCount = cq.getAffectedRowsCount();
                stateParseExecuted = true;
                break;
            case CompiledQuery.EXPLAIN:
                msgParseCacheSelectSQL(sqlExecutionContext, cq, TAG_EXPLAIN, true);
                break;
            case CompiledQuery.SELECT:
                msgParseCacheSelectSQL(sqlExecutionContext, cq, TAG_SELECT, true);
                break;
            case CompiledQuery.PSEUDO_SELECT:
                // the PSEUDO_SELECT comes from a "copy" SQL, which is why
                // we do not intend to cache it. The fact we don't have
                // TypesAndSelect instance here should be enough to tell the
                // system not to cache.
                msgParseCacheSelectSQL(sqlExecutionContext, cq, TAG_PSEUDO_SELECT, false);
                break;
            case CompiledQuery.INSERT:
                this.insertOp = cq.getInsertOperation();
                tai = taiPool.pop();
                sqlTag = TAG_INSERT;
                // todo: check why TypeAndSelect has separate method for copying types from BindVariableService
                tai.of(insertOp, sqlExecutionContext.getBindVariableService(), sqlType, sqlTag);
                msgParseCopyOutTypeDescriptionTypesFromService(sqlExecutionContext.getBindVariableService());
                break;
            case CompiledQuery.UPDATE:
                // copy contents of the mutable CompiledQuery into our cache
                compiledQuery.ofUpdate(cq.getUpdateOperation());
                compiledQuery.withSqlText(cq.getSqlText());
                msgParseCopyOutTypeDescriptionTypesFromService(sqlExecutionContext.getBindVariableService());
                sqlTag = TAG_UPDATE;
                break;
            case CompiledQuery.INSERT_AS_SELECT:
                stateParseExecuted = true;
                sqlTag = TAG_INSERT_AS_SELECT;
                break;
            case CompiledQuery.SET:
                sqlTag = TAG_SET;
                break;
            case CompiledQuery.DEALLOCATE:
                this.preparedStatementNameToDeallocate = cq.getStatementName();
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
                sqlTextHasSecret = sqlExecutionContext.containsSecret();
                sqlTag = TAG_ALTER_ROLE;
                break;
            case CompiledQuery.CREATE_USER:
                sqlTextHasSecret = sqlExecutionContext.containsSecret();
                sqlTag = TAG_CREATE_ROLE;
                break;
            case CompiledQuery.ALTER:
                // future-proofing ALTER execution
                compiledQuery.ofAlter(cq.getAlterOperation());
                compiledQuery.withSqlText(cq.getSqlText());
                // fall through
            default:
                // DDL
                sqlTag = TAG_OK;
                stateParseExecuted = true;
                break;
        }
    }

    private void setBindVariableAsChar(
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
            throw kaput().put("invalid char UTF8 bytes [variableIndex=").put(variableIndex).put(']');
        }
    }

    private void setBindVariableAsDate(
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
            throw kaput().put("invalid str UTF8 bytes [variableIndex=").put(variableIndex).put(']');
        }
    }

    private void setBindVariableAsDouble(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Double.BYTES, valueSize);
        bindVariableService.setDouble(variableIndex, Double.longBitsToDouble(getLongUnsafe(valueAddr)));
    }

    private void setBindVariableAsFloat(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Float.BYTES, valueSize);
        bindVariableService.setFloat(variableIndex, Float.intBitsToFloat(getIntUnsafe(valueAddr)));
    }

    private void setBindVariableAsInt(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Integer.BYTES, valueSize);
        bindVariableService.setInt(variableIndex, getIntUnsafe(valueAddr));
    }

    private void setBindVariableAsLong(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Long.BYTES, valueSize);
        bindVariableService.setLong(variableIndex, getLongUnsafe(valueAddr));
    }

    private void setBindVariableAsShort(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Short.BYTES, valueSize);
        bindVariableService.setShort(variableIndex, getShortUnsafe(valueAddr));
    }

    private void setBindVariableAsStr(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService,
            CharacterStore characterStore,
            DirectUtf8String utf8String
    ) throws BadProtocolException {
        CharacterStoreEntry e = characterStore.newEntry();
        Function fn = bindVariableService.getFunction(variableIndex);
        // If the function type is VARCHAR, there's no need to convert to UTF-16
        try {
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
                        throw kaput().put("invalid varchar bind variable type [variableIndex=").put(variableIndex).put(']');
                }
                // todo: copy value of bind variable into the arena so that the receive buffer can
                //     remain to be dynamic
                bindVariableService.setVarchar(variableIndex, utf8String.of(valueAddr, valueAddr + valueSize, ascii));
            } else {
                if (Utf8s.utf8ToUtf16(valueAddr, valueAddr + valueSize, e)) {
                    bindVariableService.setStr(variableIndex, characterStore.toImmutable());
                } else {
                    throw kaput().put("invalid str bind variable type [variableIndex=").put(variableIndex).put(']');
                }
            }
        } catch (Throwable ex) {
            throw kaput().put(ex);
        }
    }

    private void setBindVariableAsTimestamp(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws BadProtocolException, SqlException {
        ensureValueLength(variableIndex, Long.BYTES, valueSize);
        bindVariableService.setTimestamp(variableIndex, getLongUnsafe(valueAddr) + Numbers.JULIAN_EPOCH_OFFSET_USEC);
    }

    private void setUuidBindVariable(
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

    void clearState() {
        error = false;
        stateSync = 0;
        stateParse = false;
        stateBind = false;
        stateDesc = 0;
        stateExec = false;
        stateClosed = false;
    }

    void copyStateFrom(PGPipelineEntry that) {
        stateParse = that.stateParse;
        stateBind = that.stateBind;
        stateDesc = that.stateDesc;
        stateExec = that.stateExec;
        stateClosed = that.stateClosed;
    }

    // When we pick up SQL (insert or select) from cache we have to check that the SQL was compiled with
    // the same PostgresSQL parameter types that were supplied when SQL was cached. When the parameter types
    // are different we will have to recompile the SQL.
    //
    // In this method we only compare PG parameter types. For example, if client sent 0 parameters
    // to cache the SQL and 0 parameters to retrieve SQL from cache - this is a match.
    // It is irrelevant which types were defined by the SQL compiler. We are assuming that same SQL text will
    // produce the same parameter definitions for every compilation.
    boolean msgParseReconcileParameterTypes(short parameterTypeCount, TypeContainer typeContainer) {
        if (parameterTypeCount > 0) {
            // both BindVariableService and the "typeContainer" have parameter types
            // we have to allow the possibility that parameter types between the
            // cache and the "parse" message could be different. If they are,
            // we have to discard the cache and re-compile the SQL text
            IntList cachedTypes = typeContainer.getPgParameterTypes();
            int cachedTypeCount = cachedTypes.size();
            int clientTypeCount = msgParseParameterTypes.size();
            if (cachedTypeCount == clientTypeCount) {
                for (int i = 0; i < cachedTypeCount; i++) {
                    if (cachedTypes.getQuick(i) != msgParseParameterTypes.getQuick(i)) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
        return true;
    }
}
