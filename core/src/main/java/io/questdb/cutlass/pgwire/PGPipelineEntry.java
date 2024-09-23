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
    private final BitSet msgBindParameterFormatCodes = new BitSet();
    // stores result format codes (0=Text,1=Binary) from the latest bind message
    // we need it in case cursor gets invalidated and bind used non-default binary format for some column(s)
    // pg clients (like asyncpg) fail when format sent by server is not the same as requested in bind message
    private final BitSet msgBindSelectFormatCodes = new BitSet();
    // types are sent to us via "parse" message
    private final IntList msgParseParameterTypeOIDs = new IntList();
    private final IntList outParameterTypeDescriptionTypeOIDs = new IntList();
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
    private int msgBindParameterValueCount;
    private short msgBindSelectFormatCodeCount = 0;
    private boolean outResendCursorRecord = false;
    private long parameterValueArenaHi;
    private long parameterValueArenaLo;
    private long parameterValueArenaPtr = 0;
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
        if (parameterValueArenaPtr != 0) {
            Unsafe.free(parameterValueArenaPtr, parameterValueArenaHi - parameterValueArenaPtr, MemoryTag.NATIVE_PGW_CONN);
            parameterValueArenaPtr = 0;
        }
    }

    public void compileNewSQL(
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
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    // Define the provided PostgresSQL types on the BindVariableService. The compilation
                    // below will use these types to build the plan, and it will also define any missing bind
                    // variables.
                    msgParseDefineBindVariableTypes(sqlExecutionContext.getBindVariableService());
                    CompiledQuery cq = compiler.compile(this.sqlText, sqlExecutionContext);
                    // copy actual bind variable types as supplied by the client + defined by the SQL
                    // compiler
                    msgParseCopyOutTypeDescriptionTypeOIDs(sqlExecutionContext.getBindVariableService());
                    setupEntryAfterSQLCompilation(sqlExecutionContext, taiPool, cq);
                }
                copyPgResultSetColumnTypes();
            } catch (SqlException e) {
                if (e.getMessage().equals("[0] empty query")) {
                    this.empty = true;
                } else {
                    throw kaput().put((Throwable) e);
                }
            } catch (Throwable e) {
                throw kaput().put(e);
            }
        }
    }

    public void copyColumnTypesToParameterTypeOIDs(IntList bindVariableColumnTypes) {
        outParameterTypeDescriptionTypeOIDs.clear();
        for (int i = 0, n = bindVariableColumnTypes.size(); i < n; i++) {
            outParameterTypeDescriptionTypeOIDs.add(Numbers.bswap(PGOids.getTypeOid(bindVariableColumnTypes.getQuick(i))));
        }
    }

    // return transaction state
    public int execute(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            SimpleAssociativeCache<TypesAndInsert> taiCache,
            ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters,
            WriterSource writerSource,
            CharacterStore characterStore,
            DirectUtf8String utf8String,
            ObjectPool<DirectBinarySequence> binarySequenceParamsPool
    ) throws BadProtocolException {
        // do not execute anything, that has been parse-executed
        if (stateParseExecuted) {
            stateParseExecuted = false;
            return transactionState;
        }
        sqlExecutionContext.containsSecret(sqlTextHasSecret);
        try {
            switch (this.sqlType) {
                case CompiledQuery.EXPLAIN:
                case CompiledQuery.SELECT:
                case CompiledQuery.PSEUDO_SELECT:
                    executeSelect(sqlExecutionContext, characterStore, utf8String, binarySequenceParamsPool);
                    break;
                case CompiledQuery.INSERT:
                    executeInsert(
                            sqlExecutionContext,
                            transactionState,
                            taiCache,
                            pendingWriters,
                            writerSource,
                            characterStore,
                            utf8String,
                            binarySequenceParamsPool
                    );
                    break;
                case CompiledQuery.ALTER:
                case CompiledQuery.ALTER_USER:
                case CompiledQuery.CREATE_USER:
                case CompiledQuery.UPDATE:
                    executeCompiledQuery(sqlExecutionContext, transactionState, characterStore, utf8String, binarySequenceParamsPool);
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
        } catch (BadProtocolException e) {
            throw e;
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

    public void msgBindCopyParameterFormatCodes(
            long lo,
            long msgLimit,
            short parameterFormatCodeCount,
            short parameterValueCount
    ) throws BadProtocolException {
        this.msgBindParameterValueCount = parameterValueCount;

        // Format codes pertain the parameter values sent in the same "bind" message.
        // When parameterFormatCodeCount is 1, it means all values are sent either all text or all binary. Any other
        // value for the parameterFormatCodeCount assumes that format is defined per value (doh). When
        // we have more formats than values - we ignore extra formats quietly. On other hand,
        // when we receive fewer formats than values - we assume that remaining values are
        // send by the client as string.

        // this would set all codes to 0 (in the bitset)
        this.msgBindParameterFormatCodes.clear();
        if (parameterFormatCodeCount > 0) {
            if (parameterFormatCodeCount == 1) {
                // all are the same
                short code = getShort(lo, msgLimit, "could not read parameter formats");
                // all binary? when string (0) - leave the bitset unset
                if (code == 1) {
                    // set all bits, indicating binary
                    for (int i = 0; i < parameterValueCount; i++) {
                        this.msgBindParameterFormatCodes.set(i);
                    }
                }
            } else {
                // Process all formats provided by the client. Should the client provide fewer
                // formats than the value count, we will assume the rest is string.
                if (lo + Short.BYTES * parameterFormatCodeCount <= msgLimit) {
                    for (int i = 0; i < parameterFormatCodeCount; i++) {
                        if (getShortUnsafe(lo + i * Short.BYTES) == 1) {
                            this.msgBindParameterFormatCodes.set(i);
                        }
                    }
                } else {
                    throw kaput().put("invalid format code count [value=").put(parameterFormatCodeCount).put(']');
                }
            }
        }
    }

    public long msgBindCopyParameterValuesArea(long lo, long msgLimit) throws BadProtocolException {
        long valueAreaSize = msgBindComputeParameterValueAreaSize(lo, msgLimit);
        if (valueAreaSize > 0) {
            long sz = Numbers.ceilPow2(valueAreaSize);
            if (parameterValueArenaPtr == 0) {
                parameterValueArenaPtr = Unsafe.malloc(sz, MemoryTag.NATIVE_PGW_CONN);
                parameterValueArenaLo = parameterValueArenaPtr;
                parameterValueArenaHi = parameterValueArenaPtr + sz;
            } else if (parameterValueArenaHi - parameterValueArenaPtr < valueAreaSize) {
                parameterValueArenaPtr = Unsafe.realloc(parameterValueArenaPtr, parameterValueArenaHi - parameterValueArenaPtr, sz, MemoryTag.NATIVE_PGW_CONN);
                parameterValueArenaLo = parameterValueArenaPtr;
                parameterValueArenaHi = parameterValueArenaPtr + sz;
            }
            long len = Math.min(valueAreaSize, msgLimit);
            Vect.memcpy(parameterValueArenaLo, lo, len);
            if (len < valueAreaSize) {
                parameterValueArenaLo += len;
                // todo: create "receive" state machine in the context, so that client messages can be split
                //       across multiple recv buffers
                throw BadProtocolException.INSTANCE;
            } else {
                parameterValueArenaLo = parameterValueArenaPtr;
            }
        }
        return lo + valueAreaSize;
    }

    public void msgBindCopySelectFormatCodes(long lo, short selectFormatCodeCount) {
        // Select format codes are switches between binary and text representation of the
        // result set. They are only applicable to the result set and SQLs that compile into a factory.
        msgBindSelectFormatCodes.clear();
        msgBindSelectFormatCodeCount = selectFormatCodeCount;
        if (factory != null && selectFormatCodeCount > 0) {
            for (int i = 0; i < selectFormatCodeCount; i++) {
                if (getShortUnsafe(lo) == 1) {
                    msgBindSelectFormatCodes.set(i);
                }
                lo += Short.BYTES;
            }
        }
    }

    public void msgParseCopyParameterTypesFromMsg(long lo, short parameterTypeCount) {
        msgParseParameterTypeOIDs.setPos(parameterTypeCount);
        for (int i = 0; i < parameterTypeCount; i++) {
            msgParseParameterTypeOIDs.setQuick(i, Unsafe.getUnsafe().getInt(lo + i * 4L));
        }
    }

    public void ofInsert(CharSequence utf16SqlText, TypesAndInsert tai) {
        this.sqlText = utf16SqlText;
        this.insertOp = tai.getInsert();
        this.sqlTag = tai.getSqlTag();
        this.sqlType = tai.getSqlType();
        this.cacheHit = true;
        this.tai = tai;
        copyColumnTypesToParameterTypeOIDs(tai.getBindVariableColumnTypes());
    }

    public void ofSelect(CharSequence utf16SqlText, TypesAndSelect tas) {
        this.sqlText = utf16SqlText;
        this.factory = tas.getFactory();
        this.sqlTag = tas.getSqlTag();
        this.sqlType = tas.getSqlType();
        this.tas = tas;
        this.cacheHit = true;
        copyPgResultSetColumnTypes();
        copyColumnTypesToParameterTypeOIDs(tas.getBindVariableColumnTypes());
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
                setupEntryAfterSQLCompilation(sqlExecutionContext, taiPool, cq);
                copyPgResultSetColumnTypes();
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
                    if (!portal) {
                        // if this is not a named portal
                        // then we have to close the cursor even if we didn't fully exhaust it
                        cursor = Misc.free(cursor);
                    }
                    break;
            }

            if (stateClosed) {
                outSimpleMsg(utf8Sink, MESSAGE_TYPE_CLOSE_COMPLETE);
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

    // defines bind variable from statement description types we sent to client.
    // that is a combination of types we received in the PARSE message and types the compiler inferred
    // unknown types are defined as strings
    private void bindDefineBindVariableType(BindVariableService bindVariableService, int j) throws SqlException {
        switch (outParameterTypeDescriptionTypeOIDs.getQuick(j)) {
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

    private void copyParameterValuesToBindVariableService(SqlExecutionContext sqlExecutionContext, CharacterStore characterStore, DirectUtf8String utf8String, ObjectPool<DirectBinarySequence> binarySequenceParamsPool) throws BadProtocolException, SqlException {
        // Bind variables have to be configured for the cursor.
        // We have stored the following:
        // - outTypeDescriptionTypeOIDs - OIDS of the parameter types, these are all types present in the SQL
        // - parameter values - list of parameter values supplied by the client; this list may be
        //                      incomplete insofar as being shorter than the list of bind variables. The values
        //                      are read from the parameter value arena.
        // - parameter format codes - list of switches, prescribing how to read the parameter values. Again,
        //                      nothing is stopping the client from sending more or less codes than the
        //                      parameter values.

        // Considering all the above, we are performing a 3-way merge of the existing states.
        final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
        bindVariableService.clear();
        long lo = parameterValueArenaPtr;
        long msgLimit = parameterValueArenaHi;
        for (int i = 0, n = outParameterTypeDescriptionTypeOIDs.size(); i < n; i++) {
            if (i < msgBindParameterValueCount) {
                // read value from the arena
                final int valueSize = getInt(lo, msgLimit, "malformed bind variable");
                lo += Integer.BYTES;
                if (valueSize == -1) {
                    // value is not provided, assume NULL
                    bindDefineBindVariableType(bindVariableService, i);
                } else {
                    if (msgBindParameterFormatCodes.get(i)) {
                        // binary value or a string (binary string and text string is the same)
                        switch (outParameterTypeDescriptionTypeOIDs.getQuick(i)) {
                            case X_PG_INT4:
                                setBindVariableAsInt(i, lo, valueSize, bindVariableService);
                                break;
                            case X_PG_INT8:
                                setBindVariableAsLong(i, lo, valueSize, bindVariableService);
                                break;
                            case X_PG_TIMESTAMP:
                                setBindVariableAsTimestamp(i, lo, valueSize, bindVariableService);
                                break;
                            case X_PG_INT2:
                                setBindVariableAsShort(i, lo, valueSize, bindVariableService);
                                break;
                            case X_PG_FLOAT8:
                                setBindVariableAsDouble(i, lo, valueSize, bindVariableService);
                                break;
                            case X_PG_FLOAT4:
                                setBindVariableAsFloat(i, lo, valueSize, bindVariableService);
                                break;
                            case X_PG_CHAR:
                                setBindVariableAsChar(i, lo, valueSize, bindVariableService, characterStore);
                                break;
                            case X_PG_DATE:
                                setBindVariableAsDate(i, lo, valueSize, bindVariableService, characterStore);
                                break;
                            case X_PG_BOOL:
                                setBindVariableAsBoolean(i, valueSize, bindVariableService);
                                break;
                            case X_PG_BYTEA:
                                setBindVariableAsBin(i, lo, valueSize, bindVariableService, binarySequenceParamsPool);
                                break;
                            case X_PG_UUID:
                                setUuidBindVariable(i, lo, valueSize, bindVariableService);
                                break;
                            default:
                                // before we bind a string, we need to define the type of the variable
                                // so the binding process can cast the string as required
                                bindDefineBindVariableType(bindVariableService, i);
                                setBindVariableAsStr(i, lo, valueSize, bindVariableService, characterStore, utf8String);
                                break;
                        }
                    } else {
                        // read as a string
                        bindDefineBindVariableType(bindVariableService, i);
                        setBindVariableAsStr(i, lo, valueSize, bindVariableService, characterStore, utf8String);
                    }
                    lo += valueSize;
                }
            } else {
                // set NULL for the type
                // todo: test how this works with vararg function args.
                defineBindVariableType(sqlExecutionContext.getBindVariableService(), i);
            }
        }
    }

    private void copyPgResultSetColumnTypes() {
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

    // unknown types are not defined so the compiler can infer the best possible type
    private void defineBindVariableType(BindVariableService bindVariableService, int j) throws SqlException {
        switch (msgParseParameterTypeOIDs.getQuick(j)) {
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
            case 0:
                // unknown types, we are not defining them for now - this gives
                // the compiler a chance to infer the best possible type
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

    private void executeCompiledQuery(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            CharacterStore characterStore,
            DirectUtf8String utf8String,
            ObjectPool<DirectBinarySequence> binarySequenceParamsPool
    ) throws SqlException, BadProtocolException {
        if (transactionState != ERROR_TRANSACTION) {
            copyParameterValuesToBindVariableService(
                    sqlExecutionContext,
                    characterStore,
                    utf8String,
                    binarySequenceParamsPool
            );
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
            WriterSource writerSource,
            CharacterStore characterStore,
            DirectUtf8String utf8String,
            ObjectPool<DirectBinarySequence> binarySequenceParamsPool
    ) throws SqlException, BadProtocolException {
        switch (transactionState) {
            case IN_TRANSACTION:
                copyParameterValuesToBindVariableService(
                        sqlExecutionContext,
                        characterStore,
                        utf8String,
                        binarySequenceParamsPool
                );
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
                copyParameterValuesToBindVariableService(
                        sqlExecutionContext,
                        characterStore,
                        utf8String,
                        binarySequenceParamsPool
                );
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

    private void executeSelect(
            SqlExecutionContext sqlExecutionContext,
            CharacterStore characterStore,
            DirectUtf8String utf8String,
            ObjectPool<DirectBinarySequence> binarySequenceParamsPool
    ) throws SqlException, BadProtocolException {
        if (cursor == null) {
            sqlExecutionContext.getCircuitBreaker().resetTimer();
            sqlExecutionContext.setCacheHit(cacheHit);
            try {
                copyParameterValuesToBindVariableService(
                        sqlExecutionContext,
                        characterStore,
                        utf8String,
                        binarySequenceParamsPool
                );
                cursor = factory.getCursor(sqlExecutionContext);
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

    private short getPgResultSetColumnFormatCode(int columnIndex) {
        // binary is always sent as binary (e.g.) we never Base64 encode that
        if (pgResultSetColumnTypes.getQuick(columnIndex * 2) != ColumnType.BINARY) {
            return (msgBindSelectFormatCodeCount > 1 ? msgBindSelectFormatCodes.get(columnIndex) : msgBindSelectFormatCodes.get(0)) ? (short) 1 : 0;
        }
        return 1;
    }

    private BadProtocolException kaput() {
        return BadProtocolException.instance(this);
    }

    private long msgBindComputeParameterValueAreaSize(
            long lo,
            long msgLimit
    ) throws BadProtocolException {
        if (msgBindParameterValueCount > 0) {
            long l = lo;
            for (int j = 0; j < msgBindParameterValueCount; j++) {
                final int valueSize = getInt(lo, msgLimit, "malformed bind variable");
                lo += Integer.BYTES;
                if (valueSize > 0) {
                    lo += valueSize;
                }
            }
            return lo - l;
        }
        return 0;
    }

    private void msgParseCopyOutTypeDescriptionTypeOIDs(BindVariableService bindVariableService) {
        final int n = bindVariableService.getIndexedVariableCount();
        outParameterTypeDescriptionTypeOIDs.clear();
        if (n > 0) {
            outParameterTypeDescriptionTypeOIDs.setPos(n);
            for (int i = 0; i < n; i++) {
                final Function f = bindVariableService.getFunction(i);
                outParameterTypeDescriptionTypeOIDs.setQuick(i, Numbers.bswap(PGOids.getTypeOid(
                        f != null ? f.getType() : ColumnType.UNDEFINED
                )));
            }
        }
    }

    // defines bind variables we receive in the parse message.
    // this is used before parsing SQL text received in the PARSE message (or Q)
    // unknown types are not defined so the compiler can infer the best possible type
    private void msgParseDefineBindVariableTypes(BindVariableService bindVariableService) throws SqlException {
        bindVariableService.clear();
        for (int i = 0, n = msgParseParameterTypeOIDs.size(); i < n; i++) {
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
        if (Double.isNaN(value)) {
            utf8Sink.setNullValue();
        } else {
            utf8Sink.putNetworkInt(Double.BYTES);
            utf8Sink.putNetworkDouble(value);
        }
    }

    private void outColBinFloat(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final float value = record.getFloat(columnIndex);
        if (Float.isNaN(value)) {
            utf8Sink.setNullValue();
        } else {
            utf8Sink.putNetworkInt(Float.BYTES);
            utf8Sink.putNetworkFloat(value);
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
        if (Double.isNaN(doubleValue)) {
            utf8Sink.setNullValue();
        } else {
            final long a = utf8Sink.skipInt();
            utf8Sink.put(doubleValue);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColTxtFloat(PGResponseSink responseUtf8Sink, Record record, int columnIndex) {
        final float floatValue = record.getFloat(columnIndex);
        if (Float.isNaN(floatValue)) {
            responseUtf8Sink.setNullValue();
        } else {
            final long a = responseUtf8Sink.skipInt();
            responseUtf8Sink.put(floatValue, 3);
            responseUtf8Sink.putLenEx(a);
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

        if (sqlReturnRowCount < sqlReturnRowCountToBeSent) {
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
        final int n = outParameterTypeDescriptionTypeOIDs.size();
        utf8Sink.putNetworkShort((short) n);
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                utf8Sink.putIntDirect(toParamType(outParameterTypeDescriptionTypeOIDs.getQuick(i)));
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
            final int typeTag = ColumnType.tagOf(type);
            final short columnBinaryFlag = getPgResultSetColumnFormatCode(i);

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
            utf8Sink.putNetworkShort(getPgResultSetColumnFormatCode(i)); // format code
        }
        utf8Sink.putLen(addr);
        utf8Sink.bookmark();
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
                // varchar value is sourced from the send-receive buffer (which is volatile, e.g. will be wiped
                // without warning). It seems to be "ok" for all situations, of which there are only two:
                // 1. the target type is "varchar", in which case the source value is "sank" into the buffer of
                //    the bind variable
                // 2. the target is not a varchar, in which case varchar is parsed on-the-fly
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

    private void setupEntryAfterSQLCompilation(
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
                this.sqlTag = TAG_EXPLAIN;
                this.factory = cq.getRecordCursorFactory();
                tas = new TypesAndSelect(
                        this.factory,
                        sqlType,
                        TAG_EXPLAIN,
                        sqlExecutionContext.getBindVariableService(),
                        msgParseParameterTypeOIDs
                );
                break;
            case CompiledQuery.SELECT:
                this.sqlTag = TAG_SELECT;
                this.factory = cq.getRecordCursorFactory();
                tas = new TypesAndSelect(
                        factory,
                        sqlType,
                        sqlTag,
                        sqlExecutionContext.getBindVariableService(),
                        msgParseParameterTypeOIDs
                );
                break;
            case CompiledQuery.PSEUDO_SELECT:
                // the PSEUDO_SELECT comes from a "copy" SQL, which is why
                // we do not intend to cache it. The fact we don't have
                // TypesAndSelect instance here should be enough to tell the
                // system not to cache.
                this.sqlTag = TAG_PSEUDO_SELECT;
                this.factory = cq.getRecordCursorFactory();
                break;
            case CompiledQuery.INSERT:
                this.insertOp = cq.getInsertOperation();
                tai = taiPool.pop();
                sqlTag = TAG_INSERT;
                tai.of(
                        insertOp,
                        sqlType,
                        sqlTag,
                        sqlExecutionContext.getBindVariableService(),
                        msgParseParameterTypeOIDs
                );
                break;
            case CompiledQuery.UPDATE:
                // copy contents of the mutable CompiledQuery into our cache
                compiledQuery.ofUpdate(cq.getUpdateOperation());
                compiledQuery.withSqlText(cq.getSqlText());
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
                sqlTag = TAG_OK;
                break;
            // fall through
            default:
                // DDL
                sqlTag = TAG_OK;
                stateParseExecuted = true;
                break;
        }
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
            IntList cachedTypes = typeContainer.getPgParameterTypeOIDs();
            int cachedTypeCount = cachedTypes.size();
            int clientTypeCount = msgParseParameterTypeOIDs.size();
            if (cachedTypeCount == clientTypeCount) {
                for (int i = 0; i < cachedTypeCount; i++) {
                    if (cachedTypes.getQuick(i) != msgParseParameterTypeOIDs.getQuick(i)) {
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
