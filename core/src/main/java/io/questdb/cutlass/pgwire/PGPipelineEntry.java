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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.CompiledQueryImpl;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.QueryPausedException;
import io.questdb.std.AssociativeCache;
import io.questdb.std.BinarySequence;
import io.questdb.std.BitSet;
import io.questdb.std.Chars;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long128;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.ObjectPool;
import io.questdb.std.ObjectStackPool;
import io.questdb.std.QuietCloseable;
import io.questdb.std.SimpleAssociativeCache;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.Vect;
import io.questdb.std.WeakSelfReturningObjectPool;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

import static io.questdb.cutlass.pgwire.PGConnectionContext.*;
import static io.questdb.cutlass.pgwire.PGOids.*;
import static io.questdb.cutlass.pgwire.PGUtils.calculateColumnBinSize;
import static io.questdb.cutlass.pgwire.PGUtils.estimateColumnTxtSize;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_MILLI_TIME_Z_PRINT_FORMAT;

public class PGPipelineEntry implements QuietCloseable, Mutable {
    // SYNC_DESC_ constants describe the state of the "describe" message
    // they have no relation to the state of SYNC message processing as such
    public static final int SYNC_DESC_NONE = 0;
    public static final int SYNC_DESC_PARAMETER_DESCRIPTION = 2;
    public static final int SYNC_DESC_ROW_DESCRIPTION = 1;
    private static final int ERROR_TAIL_MAX_SIZE = 23;
    private static final Log LOG = LogFactory.getLog(PGPipelineEntry.class);
    // tableOid + column number + type + type size + type modifier + format code
    private static final int ROW_DESCRIPTION_COLUMN_RECORD_FIXED_SIZE = 3 * Short.BYTES + 3 * Integer.BYTES;
    private static final int SYNC_BIND = 1;
    private static final int SYNC_COMPUTE_CURSOR_SIZE = 3;
    private static final int SYNC_DATA = 4;
    private static final int SYNC_DATA_EXHAUSTED = 6;
    private static final int SYNC_DATA_SUSPENDED = 7;
    private static final int SYNC_DESCRIBE = 2;
    private static final int SYNC_DONE = 5;
    private static final int SYNC_PARSE = 0;
    private final ObjectPool<PGNonNullBinaryArrayView> arrayViewPool = new ObjectPool<>(PGNonNullBinaryArrayView::new, 1);
    private final CairoEngine engine;
    private final StringSink errorMessageSink = new StringSink();
    private final int maxRecompileAttempts;
    private final BitSet msgBindParameterFormatCodes = new BitSet();
    // stores result format codes (0=Text,1=Binary) from the latest bind message
    // we need it in case cursor gets invalidated and bind used non-default binary format for some column(s)
    // pg clients (like asyncpg) fail when format sent by server is not the same as requested in bind message
    private final BitSet msgBindSelectFormatCodes = new BitSet();
    // types are sent to us via "parse" message
    private final IntList msgParseParameterTypeOIDs;
    private final ObjList<Utf8String> namedPortals = new ObjList<>();
    // List with encoded bind variable types. It combines types client sent to us in PARSE message and types
    // inferred by the SQL compiler. Each entry uses lower 32 bits for QuestDB native type and upper 32 bits
    // contains PGWire OID type. If a client sent us a type, then the high 32 bits (=OID type) is set to the same value.
    // Q: Why do we have to store each variable type in both native and pgwire encoding?
    //    Cannot we just maintain a single list and convert when needed?
    // A: Some QuestDB natives do not have native equivalents. For example BYTE or GEOHASH
    // Q: Cannot we just maintain QuestDB native types and convert to PGWire OIDs when needed?
    // A: PGWire clients could have specified their own expectations about type in a PARSE message,
    //    and we have to respect this. Thus, if a PARSE message contains a type VARCHAR then
    //    we need to read it from wire as VARCHAR even we use e.g. INT internally. So we need both native and wire types.
    private final LongList outParameterTypeDescriptionTypes;
    private final ObjList<String> pgResultSetColumnNames;
    // list of pair: column types (with format flag stored in first bit) AND additional type flag
    private final IntList pgResultSetColumnTypes;
    private final Utf8StringSink utf8StringSink = new Utf8StringSink();
    boolean isCopy;
    private boolean cacheHit = false;    // extended protocol cursor resume callback
    private CompiledQueryImpl compiledQuery;
    private RecordCursor cursor;
    private boolean empty;
    private boolean error = false;
    private int errorMessagePosition;
    // this is a "union", so should only be one, depending on SQL type
    // SELECT or EXPLAIN
    private RecordCursorFactory factory = null;
    private int msgBindParameterValueCount;
    private short msgBindSelectFormatCodeCount = 0;
    private Utf8String namedPortal;
    private Utf8String namedStatement;
    private Operation operation = null;
    private int outResendArrayFlatIndex = 0;
    private int outResendColumnIndex = 0;
    private boolean outResendCursorRecord = false;
    private boolean outResendRecordHeader = true;
    private long parameterValueArenaHi;
    private long parameterValueArenaLo;
    private long parameterValueArenaPtr = 0;
    private PGPipelineEntry parentPreparedStatementPipelineEntry;
    private boolean portal = false;
    // the name of the prepared statement as used by "deallocate" SQL
    // not to be confused with prepared statements that come on the
    // PostgresSQL wire.
    private Utf8Sequence preparedStatementNameToDeallocate;
    private long sqlAffectedRowCount = 0;
    // The count of rows sent that have been sent to the client per fetch. Client can either
    // fetch all rows at once, or in batches. In case of full fetch, this is the
    // count of rows in the cursor. If client fetches in batches, this is the count
    // of rows we sent so far in the current batch.
    // It is important to know this is NOT the count to be sent, this is the count we HAVE sent.
    private long sqlReturnRowCount = 0;
    // The row count sent to us by the client. This is the size of the batch the client wants to
    // receive from us.
    private long sqlReturnRowCountLimit = 0;
    private long sqlReturnRowCountToBeSent = 0;
    private String sqlTag = null;
    private CharSequence sqlText = null;
    private boolean sqlTextHasSecret = false;
    private short sqlType = 0;
    private boolean stalePlanError = false;
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
    // IMPORTANT: if you add a new state, make sure to add it to the close() method too!
    // PGPipelineEntry instances are pooled and reused, so we need to make sure
    // that all state is cleared before returning the instance to the pool

    public PGPipelineEntry(CairoEngine engine) {
        this.isCopy = false;
        this.engine = engine;
        this.maxRecompileAttempts = engine.getConfiguration().getMaxSqlRecompileAttempts();
        this.msgParseParameterTypeOIDs = new IntList();
        this.outParameterTypeDescriptionTypes = new LongList();
        this.pgResultSetColumnTypes = new IntList();
        this.pgResultSetColumnNames = new ObjList<>();
    }

    public void bindPortalName(Utf8String portalName) {
        namedPortals.add(portalName);
    }

    public void cacheIfPossible(
            @NotNull AssociativeCache<TypesAndSelect> tasCache,
            @NotNull SimpleAssociativeCache<TypesAndInsert> taiCache
    ) {
        if (isPortal() || isPreparedStatement()) {
            // must not cache prepared statements etc.; we must only cache abandoned pipeline entries (their contents)
            return;
        }

        if (tas != null) {
            // close cursor in case it is open
            cursor = Misc.free(cursor);
            // make sure factory is not released when the pipeline entry is closed
            factory = null;
            // we don't have to use immutable string since ConcurrentAssociativeCache does it when needed
            tasCache.put(sqlText, tas);
            tas = null;
        } else if (tai != null) {
            taiCache.put(sqlText, tai);
            // make sure we don't close insert operation when the pipeline entry is closed
            tai = null;
        }
    }

    @Override
    public void clear() {
        // this is expected to be called from a pool only

        // Paranoia mode: Safety check before returning this entry from the object pool.
        // While entries should already be clean when returned to the pool,
        // this serves as a safeguard against potential bugs where an entry
        // wasn't properly cleaned up. If a dirty entry is detected (sqlType != NONE),
        // we clear it to prevent unexpected behaviour.
        if (sqlType != CompiledQuery.NONE) {
            LOG.error().$("Object Pool contains dirty PGPipeline entries. This is likely a bug, please report it to https://github.com/questdb/questdb/issues/new?template=bug_report.yaml").$();
            close();
        }
    }

    @Override
    public void close() {
        // Release resources before returning to the pool.
        // INVARIANT: After calling this method, the state of this object is indistinguishable
        // from the state of the object after it was created by the constructor.

        // For maintainability, we should clear all fields in the order they are declared in the class
        // this makes it easier to check if a particular field has been cleared or not.
        // One exception to this rule are fields which are guarded by !isCopy condition

        if (!isCopy) {
            tai = Misc.free(tai);
            operation = Misc.free(operation);
            if (compiledQuery != null) {
                Misc.free(compiledQuery.getUpdateOperation());
            }
        } else {
            // if we are a copy, we do not own operations -> we cannot close them
            // so we just null them out and let the original entry close them
            tai = null;
            operation = null;
        }

        errorMessageSink.clear();
        msgBindParameterFormatCodes.clear();
        msgBindSelectFormatCodes.clear();
        msgParseParameterTypeOIDs.clear();
        outParameterTypeDescriptionTypes.clear();
        pgResultSetColumnNames.clear();
        pgResultSetColumnTypes.clear();
        namedPortals.clear();
        isCopy = false;
        cacheHit = false;
        cursor = Misc.free(cursor);
        error = false;
        empty = false;
        errorMessagePosition = 0;
        factory = Misc.free(factory);
        msgBindParameterValueCount = 0;
        msgBindSelectFormatCodeCount = 0;
        outResendColumnIndex = 0;
        outResendCursorRecord = false;
        outResendRecordHeader = true;
        if (parameterValueArenaPtr != 0) {
            parameterValueArenaPtr = Unsafe.free(parameterValueArenaPtr, parameterValueArenaHi - parameterValueArenaPtr, MemoryTag.NATIVE_PGW_PIPELINE);
            // no need to set lo and hi to 0, as they are not used after the pointer is freed
        }
        parentPreparedStatementPipelineEntry = null;
        portal = false;
        namedPortal = null;
        namedStatement = null;
        preparedStatementNameToDeallocate = null;
        sqlAffectedRowCount = 0;
        sqlReturnRowCount = 0;
        sqlReturnRowCountLimit = 0;
        sqlReturnRowCountToBeSent = 0;
        sqlTag = null;
        sqlText = null;
        sqlTextHasSecret = false;
        sqlType = 0;
        stalePlanError = false;
        stateBind = false;
        stateClosed = false;
        stateDesc = SYNC_DESC_NONE;
        stateExec = false;
        stateParse = false;
        stateParseExecuted = false;
        stateSync = SYNC_PARSE;
        tas = null;
        arrayViewPool.clear();
        utf8StringSink.clear();
    }

    public void commit(ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters) throws PGMessageProcessingException {
        try {
            for (ObjObjHashMap.Entry<TableToken, TableWriterAPI> pendingWriter : pendingWriters) {
                final TableWriterAPI w = pendingWriter.value;
                if (w != null) {
                    w.commit();
                }
                // We rely on the fact that writer will roll back itself when it is returned to the pool.
                // The pool will also handle a case, when rollback fails. This will release the writer object
                // fully and force next writer to load its state from disk.
                pendingWriter.value = Misc.free(w);
            }
            pendingWriters.clear();
        } catch (Throwable th) {
            // free remaining writers
            rollback(pendingWriters);
            throw kaput().put(th);
        }
    }

    public void compileNewSQL(
            CharSequence sqlText,
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool,
            boolean recompile
    ) throws PGMessageProcessingException {
        // pipeline entries begin life as anonymous, typical pipeline length is 1-3 entries
        // we do not need to create new objects until we know we're caching the entry
        this.sqlText = sqlText;
        if (!recompile) {
            sqlExecutionContext.resetFlags();
        }
        this.empty = sqlText == null || sqlText.length() == 0;
        if (empty) {
            sqlExecutionContext.setCacheHit(cacheHit = true);
            return;
        }
        // try insert, peek because this is our private cache,
        // and we do not want to remove statement from it
        try {
            sqlExecutionContext.setCacheHit(cacheHit = false);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // When recompiling, we would already have bind variable values in the bind variable
                // service. This is because re-compilation is typically triggered from "sync" message.
                // Types and values would already be richly defined.
                if (!recompile) {
                    // Define the provided PostgresSQL types on the BindVariableService. The compilation
                    // below will use these types to build the plan, and it will also define any missing bind
                    // variables.
                    msgParseDefineBindVariableTypes(sqlExecutionContext.getBindVariableService());
                }
                CompiledQuery cq = compiler.compile(sqlText, sqlExecutionContext);
                // copy actual bind variable types as supplied by the client + defined by the SQL compiler
                msgParseCopyOutTypeDescriptionTypeOIDs(sqlExecutionContext.getBindVariableService());
                setupEntryAfterSQLCompilation(sqlExecutionContext, taiPool, cq);
            }
            validatePgResultSetColumnTypesAndNames();
        } catch (Throwable th) {
            if (th instanceof PGMessageProcessingException) {
                throw (PGMessageProcessingException) th;
            }
            throw kaput().put(th);
        }
    }

    public @NotNull PGPipelineEntry copyIfExecuted(ObjectStackPool<PGPipelineEntry> entryPool) {
        if (!stateExec) {
            return this;
        }

        PGPipelineEntry newEntry = entryPool.next();
        newEntry.copyOf(this);
        return newEntry;
    }

    public int getErrorMessagePosition() {
        return errorMessagePosition;
    }

    public StringSink getErrorMessageSink() {
        if (!error) {
            errorMessageSink.clear();
            error = true;
        }
        return errorMessageSink;
    }

    public int getInt(long address, long msgLimit, CharSequence errorMessage) throws PGMessageProcessingException {
        if (address + Integer.BYTES <= msgLimit) {
            return getIntUnsafe(address);
        }
        throw kaput().put(errorMessage);
    }

    public Utf8String getNamedPortal() {
        return namedPortal;
    }

    public ObjList<Utf8String> getNamedPortals() {
        return namedPortals;
    }

    public Utf8String getNamedStatement() {
        return namedStatement;
    }

    public PGPipelineEntry getParentPreparedStatementPipelineEntry() {
        return parentPreparedStatementPipelineEntry;
    }

    public short getShort(long address, long msgLimit, CharSequence errorMessage) throws PGMessageProcessingException {
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
        return namedStatement != null;
    }

    public boolean isStateClosed() {
        return stateClosed;
    }

    public boolean isStateExec() {
        return stateExec;
    }

    public void msgBindCopyParameterFormatCodes(
            long lo,
            long msgLimit,
            short parameterFormatCodeCount,
            short parameterValueCount
    ) throws PGMessageProcessingException {
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

    public long msgBindCopyParameterValuesArea(long lo, long msgLimit) throws PGMessageProcessingException {
        long valueAreaSize = msgBindComputeParameterValueAreaSize(lo, msgLimit);
        if (valueAreaSize > 0) {
            long sz = Numbers.ceilPow2(valueAreaSize);
            if (parameterValueArenaPtr == 0) {
                parameterValueArenaPtr = Unsafe.malloc(sz, MemoryTag.NATIVE_PGW_PIPELINE);
                parameterValueArenaLo = parameterValueArenaPtr;
                parameterValueArenaHi = parameterValueArenaPtr + sz;
            } else if (parameterValueArenaHi - parameterValueArenaPtr < valueAreaSize) {
                parameterValueArenaPtr = Unsafe.realloc(
                        parameterValueArenaPtr, parameterValueArenaHi - parameterValueArenaPtr,
                        sz, MemoryTag.NATIVE_PGW_PIPELINE
                );
                parameterValueArenaLo = parameterValueArenaPtr;
                parameterValueArenaHi = parameterValueArenaPtr + sz;
            }
            long len = Math.min(valueAreaSize, msgLimit);
            Vect.memcpy(parameterValueArenaLo, lo, len);
            if (len < valueAreaSize) {
                parameterValueArenaLo += len;
                // todo: create "receive" state machine in the context, so that client messages can be split
                //       across multiple recv buffers
                throw PGMessageProcessingException.INSTANCE;
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

    // return transaction state
    public int msgExecute(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool,
            ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters,
            WriterSource writerSource,
            @Transient CharacterStore bindVariableCharacterStore,
            @Transient DirectUtf8String directUtf8String,
            @Transient ObjectPool<DirectBinarySequence> binarySequenceParamsPool,
            @Transient SCSequence tempSequence,
            Consumer<? super Utf8Sequence> namedStatementDeallocator
    ) throws PGMessageProcessingException {
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
                case CompiledQuery.INSERT:
                case CompiledQuery.INSERT_AS_SELECT:
                case CompiledQuery.UPDATE:
                case CompiledQuery.ALTER:
                    copyParameterValuesToBindVariableService(
                            sqlExecutionContext,
                            bindVariableCharacterStore,
                            directUtf8String,
                            binarySequenceParamsPool
                    );
                    break;
                default:
                    break;
            }
            switch (this.sqlType) {
                case CompiledQuery.EXPLAIN:
                case CompiledQuery.SELECT:
                case CompiledQuery.PSEUDO_SELECT:
                    msgExecuteSelect(sqlExecutionContext, transactionState, pendingWriters, taiPool, maxRecompileAttempts);
                    break;
                case CompiledQuery.INSERT:
                case CompiledQuery.INSERT_AS_SELECT:
                    msgExecuteInsert(sqlExecutionContext, transactionState, pendingWriters, writerSource, taiPool);
                    break;
                case CompiledQuery.UPDATE:
                    msgExecuteUpdate(sqlExecutionContext, transactionState, pendingWriters, tempSequence, taiPool);
                    break;
                case CompiledQuery.ALTER:
                    msgExecuteDDL(sqlExecutionContext, transactionState, tempSequence, taiPool);
                    break;
                case CompiledQuery.DEALLOCATE:
                    // this is supposed to work instead of sending 'close' message via the
                    // network protocol. Reply format out of 'execute' message is
                    // different from that of 'close' message.
                    namedStatementDeallocator.accept(preparedStatementNameToDeallocate);
                    break;
                case CompiledQuery.BEGIN:
                    return IN_TRANSACTION;
                case CompiledQuery.COMMIT:
                    commit(pendingWriters);
                    return IMPLICIT_TRANSACTION;
                case CompiledQuery.ROLLBACK:
                    rollback(pendingWriters);
                    return IMPLICIT_TRANSACTION;
                case CompiledQuery.CREATE_TABLE_AS_SELECT:
                    engine.getMetrics().pgWireMetrics().markStart();
                    try (OperationFuture fut = operation.execute(sqlExecutionContext, tempSequence)) {
                        fut.await();
                        sqlAffectedRowCount = fut.getAffectedRowsCount();
                    } finally {
                        engine.getMetrics().pgWireMetrics().markComplete();
                    }
                    break;
                case CompiledQuery.CREATE_TABLE:
                    // fall-through
                case CompiledQuery.CREATE_MAT_VIEW:
                    // fall-through
                case CompiledQuery.DROP:
                    engine.getMetrics().pgWireMetrics().markStart();
                    try (OperationFuture fut = operation.execute(sqlExecutionContext, tempSequence)) {
                        fut.await();
                    } finally {
                        engine.getMetrics().pgWireMetrics().markComplete();
                    }
                    break;
                default:
                    // execute statements that either have not been parse-executed
                    // or we are re-executing it from a prepared statement
                    if (!empty) {
                        engine.getMetrics().pgWireMetrics().markStart();
                        try {
                            engine.execute(sqlText, sqlExecutionContext);
                        } finally {
                            engine.getMetrics().pgWireMetrics().markComplete();
                        }
                    }
                    break;
            }
        } catch (PGMessageProcessingException e) {
            throw e;
        } catch (Throwable th) {
            if (th instanceof FlyweightMessageContainer) {
                setErrorMessagePosition(((FlyweightMessageContainer) th).getPosition());
                final StringSink errorMsgSink = getErrorMessageSink();
                int errno;
                if (th instanceof CairoException && (errno = ((CairoException) th).getErrno()) != CairoException.NON_CRITICAL) {
                    errorMsgSink.put('[');
                    errorMsgSink.put(errno);
                    errorMsgSink.put("] ");
                }
                errorMsgSink.put(((FlyweightMessageContainer) th).getFlyweightMessage());
            } else {
                String msg = th.getMessage();
                if (msg != null) {
                    getErrorMessageSink().put(msg);
                } else {
                    getErrorMessageSink().putAscii("Internal error. Exception type: ").putAscii(th.getClass().getSimpleName());
                }
            }
            if (th instanceof AssertionError) {
                // assertion errors means either a questdb bug or data corruption ->
                // we want to see a full stack trace in server logs and log it as critical
                LOG.critical().$("error in pgwire execute, ex=").$(th).$();
            } else {
                LOG.error().$(getErrorMessageSink()).$();
            }
        } finally {
            // after execute is complete, bind variable values have been used and no longer needed in the cache
            bindVariableCharacterStore.clear();
        }
        return transactionState;
    }

    public void msgParseCopyParameterTypesFrom(PGPipelineEntry that) {
        msgParseParameterTypeOIDs.addAll(that.msgParseParameterTypeOIDs);
    }

    public void msgParseCopyParameterTypesFromMsg(long lo, short parameterTypeCount) {
        msgParseParameterTypeOIDs.setPos(parameterTypeCount);
        for (int i = 0; i < parameterTypeCount; i++) {
            msgParseParameterTypeOIDs.setQuick(i, Unsafe.getUnsafe().getInt(lo + i * 4L));
        }
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
     * @param pendingWriters      per connection write cache to be used by "insert" SQL. This is also part of the
     *                            optional "execute"
     * @param utf8Sink            the response buffer
     * @throws QueryPausedException                 exception is thrown by SQL fetch, which could be in the middle of the fetch.
     *                                              The exception indicates that SQL engine has to wait for the data to be retrieved
     *                                              from cold storage, and it might take a while. The convention is to enqueue the
     *                                              connection (fd) with the IODispatcher and deque this connection when data is ready.
     *                                              When connection is dequeued the sync process should be resumed. Which is done by
     *                                              calling this method again.
     * @throws NoSpaceLeftInResponseBufferException exception is thrown when sync runs out of space in the
     *                                              response buffer. When this happens the caller has to flush the buffer
     *                                              and call sync again, unless the flush was ineffective (had 0 bytes to flush).
     *                                              The latter means that the response buffer is too small for an atomic write
     *                                              and the protocol has to error out.
     */
    public void msgSync(
            SqlExecutionContext sqlExecutionContext,
            ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters,
            PGResponseSink utf8Sink
    ) throws QueryPausedException, NoSpaceLeftInResponseBufferException {
        if (isError()) {
            outError(utf8Sink, pendingWriters);
        } else {
            switch (stateSync) {
                case SYNC_PARSE:
                    if (stateParse) {
                        outParseComplete(utf8Sink);
                    }
                    stateSync = SYNC_BIND;
                case SYNC_BIND:
                    if (stateBind) {
                        outBindComplete(utf8Sink);
                    }
                    stateSync = SYNC_DESCRIBE;
                case SYNC_DESCRIBE:
                    switch (stateDesc) {
                        case SYNC_DESC_PARAMETER_DESCRIPTION:
                            // named prepared statement
                            outParameterTypeDescription(utf8Sink);
                            // row description can be sent in parts
                            // do not resend parameter description
                            stateDesc = SYNC_DESC_ROW_DESCRIPTION;
                            // fall through
                        case SYNC_DESC_ROW_DESCRIPTION:
                            // portal
                            if (factory != null) {
                                outRowDescription(utf8Sink);
                            } else {
                                outNoData(utf8Sink);
                            }
                            break;
                    }
                    stateSync = SYNC_COMPUTE_CURSOR_SIZE;
                case SYNC_COMPUTE_CURSOR_SIZE:
                case SYNC_DATA:
                    // state goes deeper
                    if (empty && !isPreparedStatement() && !portal) {
                        // strangely, Java driver does not need the server to produce
                        // empty query if his query was "prepared"
                        outEmptyQuery(utf8Sink);
                        stateSync = SYNC_DONE;
                    } else {
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
                                    utf8Sink.put(MESSAGE_TYPE_COMMAND_COMPLETE);
                                    long addr = utf8Sink.skipInt();
                                    utf8Sink.put(sqlTag).putAscii(" 0 ").put(sqlAffectedRowCount).put((byte) 0);
                                    utf8Sink.putLen(addr);
                                    stateSync = SYNC_DONE;
                                    break;
                                }
                                case CompiledQuery.UPDATE:
                                case CompiledQuery.CREATE_TABLE_AS_SELECT:
                                    outCommandComplete(utf8Sink, sqlAffectedRowCount);
                                    stateSync = SYNC_DONE;
                                    break;
                                default:
                                    // create table is just "OK"
                                    utf8Sink.put(MESSAGE_TYPE_COMMAND_COMPLETE);
                                    long addr = utf8Sink.skipInt();
                                    utf8Sink.put(sqlTag).put((byte) 0);
                                    utf8Sink.putLen(addr);
                                    stateSync = SYNC_DONE;
                                    break;
                            }
                        }
                    }
                case SYNC_DATA_EXHAUSTED:
                case SYNC_DATA_SUSPENDED:
                    // ignore these, they are set by outCursor() call and should be processed outside of this
                    // switch statement
                    break;
                default:
                    assert false;
            }

            // this is a separate switch because there is no way to-recheck the stateSync that
            // is set withing the top switch. These values are set by outCursor()

            switch (stateSync) {
                case SYNC_DATA_EXHAUSTED:
                    cursor = Misc.free(cursor);
                    outCommandComplete(utf8Sink, sqlReturnRowCount);
                    break;
                case SYNC_DATA_SUSPENDED:
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
                outError(utf8Sink, pendingWriters);
            }
        }

        // after the pipeline entry is synchronized we should prepare it for the next
        // execution iteration, in case the entry is a prepared statement or a portal
        clearState();
    }

    public void ofCachedInsert(CharSequence utf16SqlText, TypesAndInsert tai) {
        this.sqlText = utf16SqlText;
        this.sqlTag = tai.getSqlTag();
        this.sqlType = tai.getSqlType();
        this.cacheHit = true;
        this.tai = tai;
        this.outParameterTypeDescriptionTypes.clear();
        this.outParameterTypeDescriptionTypes.addAll(tai.getPgOutParameterTypes());
    }

    public void ofCachedSelect(CharSequence utf16SqlText, TypesAndSelect tas) {
        this.sqlText = utf16SqlText;
        this.factory = tas.getFactory();
        this.sqlTag = tas.getSqlTag();
        this.sqlType = tas.getSqlType();
        this.tas = tas;
        this.cacheHit = true;
        this.outParameterTypeDescriptionTypes.clear();
        this.outParameterTypeDescriptionTypes.addAll(tas.getOutPgParameterTypes());
    }

    public void ofEmpty(CharSequence utf16SqlText) {
        this.sqlText = utf16SqlText;
        this.empty = true;
    }

    public void ofSimpleCachedSelect(CharSequence sqlText, SqlExecutionContext sqlExecutionContext, TypesAndSelect tas) throws SqlException {
        setStateDesc(SYNC_DESC_ROW_DESCRIPTION); // send out the row description message
        this.empty = sqlText == null || sqlText.length() == 0;
        this.sqlText = sqlText;
        this.factory = tas.getFactory();
        this.sqlTag = tas.getSqlTag();
        this.sqlType = tas.getSqlType();
        this.tas = tas;
        this.cacheHit = true;
        this.outParameterTypeDescriptionTypes.clear();
        assert tas.getOutPgParameterTypes().size() == 0;

        // We cannot use regular msgExecuteSelect() since this method is called from a callback in SqlCompiler and
        // msgExecuteSelect() may try to recompile the query on its own when it gets TableReferenceOutOfDateException.
        // Calling a compiler while being called from a compiler is a bad idea.
        sqlExecutionContext.setCacheHit(cacheHit);
        sqlExecutionContext.getCircuitBreaker().resetTimer();
        cursor = factory.getCursor(sqlExecutionContext);
        copyPgResultSetColumnTypesAndNames();
        setStateExec(true);
    }

    public void ofSimpleQuery(
            CharSequence sqlText,
            SqlExecutionContext sqlExecutionContext,
            CompiledQuery cq,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool
    ) throws PGMessageProcessingException {
        // pipeline entries begin life as anonymous, typical pipeline length is 1-3 entries
        // we do not need to create new objects until we know we're caching the entry
        this.sqlText = sqlText;
        this.empty = sqlText == null || sqlText.length() == 0;
        cacheHit = false;

        if (!empty) {
            // try insert, peek because this is our private cache,
            // and we do not want to remove statement from it
            try {
                setupEntryAfterSQLCompilation(sqlExecutionContext, taiPool, cq);
                copyPgResultSetColumnTypesAndNames();
            } catch (Throwable e) {
                throw kaput().put(e);
            }
        }

        // these types must reply with row description message
        // when used via the simple query protocol
        if (cq.getType() == CompiledQuery.SELECT
                || cq.getType() == CompiledQuery.EXPLAIN
                || cq.getType() == CompiledQuery.PSEUDO_SELECT
        ) {
            setStateDesc(SYNC_DESC_ROW_DESCRIPTION);
        }
    }

    public void rollback(ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters) {
        try {
            for (ObjObjHashMap.Entry<TableToken, TableWriterAPI> pendingWriter : pendingWriters) {
                // We rely on the fact that writer will roll back itself when it is returned to the pool.
                // The pool will also handle a case, when rollback fails. This will release the writer object
                // fully and force next writer to load its state from disk.
                pendingWriter.value = Misc.free(pendingWriter.value);
            }
        } finally {
            pendingWriters.clear();
        }
    }

    public void setErrorMessagePosition(int errorMessagePosition) {
        this.errorMessagePosition = errorMessagePosition;
    }

    public void setNamedPortal(boolean portal, Utf8String namedPortal) {
        this.portal = portal;
        // because this is now a prepared statement, it means the entry is
        // cached. All flyweight objects referenced from cache have to be internalized
        this.sqlText = Chars.toString(this.sqlText);
        this.namedPortal = namedPortal;
    }

    public void setNamedStatement(Utf8String namedStatement) {
        // because this is now a prepared statement, it means the entry is
        // cached. All flyweight objects referenced from cache have to be internalized
        this.sqlText = Chars.toString(this.sqlText);
        this.namedStatement = namedStatement;
    }

    public void setParentPreparedStatement(PGPipelineEntry preparedStatementPipelineEntry) {
        this.parentPreparedStatementPipelineEntry = preparedStatementPipelineEntry;
    }

    public void setReturnRowCountLimit(int rowCountLimit) {
        this.sqlReturnRowCountLimit = rowCountLimit;
    }

    public void setStateBind(boolean stateBind) {
        this.stateBind = stateBind;
    }

    public void setStateClosed(boolean stateClosed, boolean isStatementClose) {
        this.stateClosed = stateClosed;
        this.namedPortal = null;
        this.portal = false;
        if (isStatementClose) {
            this.namedStatement = null;
        }
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

    private long calculateRecordTailSize(
            Record record,
            int columnCount,
            long maxBlobSize,
            long sendBufferSize
    ) throws PGMessageProcessingException {
        long recordSize = 0;
        for (int i = outResendColumnIndex; i < columnCount; i++) {
            final int columnType = pgResultSetColumnTypes.getQuick(2 * i);
            final int typeTag = ColumnType.tagOf(columnType);
            final short columnBinaryFlag = getPgResultSetColumnFormatCode(i, typeTag);
            // if column is not variable size and format code is text, we can't calculate size
            if (columnBinaryFlag == 0 && txtAndBinSizesCanBeDifferent(columnType)) {
                return -1;
            }
            // number of bits or chars for geohash
            final int geohashSize = Math.abs(pgResultSetColumnTypes.getQuick(2 * i + 1));
            final int columnValueSize = calculateColumnBinSize(
                    this,
                    record,
                    i,
                    columnType,
                    geohashSize,
                    maxBlobSize,
                    outResendArrayFlatIndex
            );

            if (columnValueSize < 0) {
                return -1; // unsupported type
            }

            if (columnValueSize >= sendBufferSize && !ColumnType.isArray(columnType)) {
                // doesn't fit into send buffer
                return -1;
            }

            recordSize += columnValueSize;
        }
        return recordSize;
    }

    private void copyOf(PGPipelineEntry blueprint) {
        this.msgParseParameterTypeOIDs.clear();
        this.msgParseParameterTypeOIDs.addAll(blueprint.msgParseParameterTypeOIDs);

        this.outParameterTypeDescriptionTypes.clear();
        this.outParameterTypeDescriptionTypes.addAll(blueprint.outParameterTypeDescriptionTypes);

        this.pgResultSetColumnTypes.clear();
        this.pgResultSetColumnTypes.addAll(blueprint.pgResultSetColumnTypes);
        this.pgResultSetColumnNames.clear();
        this.pgResultSetColumnNames.addAll(blueprint.pgResultSetColumnNames);

        this.compiledQuery = blueprint.compiledQuery;

        // copy only the fields set at the PARSE time
        this.isCopy = true;
        this.cacheHit = blueprint.cacheHit;
        this.empty = blueprint.empty;
        this.operation = blueprint.operation;
        this.parentPreparedStatementPipelineEntry = blueprint.parentPreparedStatementPipelineEntry;
        this.namedStatement = blueprint.namedStatement;
        this.sqlTag = blueprint.sqlTag;
        this.sqlText = blueprint.sqlText;
        this.sqlType = blueprint.sqlType;
        this.sqlTextHasSecret = blueprint.sqlTextHasSecret;
        this.tai = blueprint.tai;
        this.tas = blueprint.tas;
    }

    private void copyParameterValuesToBindVariableService(
            SqlExecutionContext sqlExecutionContext,
            CharacterStore characterStore,
            @Transient DirectUtf8String directUtf8String,
            @Transient ObjectPool<DirectBinarySequence> binarySequenceParamsPool
    ) throws PGMessageProcessingException, SqlException {
        // Bind variables have to be configured for the cursor.
        // We have stored the following:
        // - outTypeDescriptionTypeOIDs - OIDS of the parameter types, these are all types present in the SQL
        // - outTypeDescriptionType - QuestDB native types, as inferred by SQL compiler
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
        for (int i = 0, n = outParameterTypeDescriptionTypes.size(); i < n; i++) {
            // lower 32 bits = native type, higher 32 bits = pgwire type
            long encodedType = outParameterTypeDescriptionTypes.getQuick(i);

            // define binding variable. we have to use the exact type as was inferred by the compiler. we cannot use
            // pgwire equivalent. why? some QuestDB native types do not have exact equivalent in pgwire so we approximate
            // them by using the most similar/suitable type
            // example: there is no 8-bit unsigned integer in pgwire, but there is a 'byte' type in QuestDB.
            //          so we use int2 in pgwire for binding variables inferred as 'byte'. however, int2 is also
            //          used for 'short' binding variables. when we are defining a binding variable we use the
            //          exact type previously provided by sql compiler. if we drive everything by 'pgwire' types
            //          then pgwire int2 would define a 'short binding variable. however if the compiled plan
            //          expect 'byte' then it will call 'function.getByte()' and 'shortFunction.getByte()' throws
            //          unsupported operation exception.
            int nativeType = Numbers.decodeLowInt(encodedType);
            bindVariableService.define(i, nativeType, 0);

            if (i >= msgBindParameterValueCount) {
                // client did not set this binding variable, keep it unset (=null)
                continue;
            }

            // read value from the arena
            final int valueSize = getInt(lo, msgLimit, "malformed bind variable");
            lo += Integer.BYTES;
            if (valueSize == -1) {
                // value is not provided, assume NULL
                continue;
            }

            // value must remain within message limit. this will never happen with a well-functioning client, but
            // a non-compliant client could send us rubbish or a bad actor could try to crash the server.
            if (lo + valueSize > msgLimit) {
                throw kaput()
                        .put("bind variable value is beyond the message limit [variableIndex=").put(i)
                        .put(", valueSize=").put(valueSize).put(']');
            }

            // when type is unspecified, we are assuming that bind variable has not been used in the SQL
            // e.g. something like this "select * from tab where a = $1 and b = $5". E.g.  there is a gap
            // in the bind variable sequence. Because of the gap, our compiler could not define types - there is
            // no usage in SQL, bing variables are left out to be NULL.
            // Now the client is sending values in those bind variables - we can ignore them, provided variables
            // are unused.
            if (encodedType != PG_UNSPECIFIED) {
                // read the pgwire protocol types
                if (msgBindParameterFormatCodes.get(i)) {
                    // beware, pgwire type is encoded as big endian
                    // that's why we use X_PG_INT4 and not just PG_INT4
                    int pgWireType = Numbers.decodeHighInt(encodedType);
                    switch (pgWireType) {
                        case X_PG_INT4:
                            setBindVariableAsInt(i, lo, valueSize, bindVariableService);
                            break;
                        case X_PG_INT8:
                            setBindVariableAsLong(i, lo, valueSize, bindVariableService);
                            break;
                        case X_PG_TIMESTAMP:
                        case X_PG_TIMESTAMP_TZ:
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
                            setBindVariableAsDate(i, lo, valueSize, bindVariableService);
                            break;
                        case X_PG_BOOL:
                            setBindVariableAsBoolean(i, lo, valueSize, bindVariableService);
                            break;
                        case X_PG_BYTEA:
                            setBindVariableAsBin(i, lo, valueSize, bindVariableService, binarySequenceParamsPool);
                            break;
                        case X_PG_UUID:
                            setUuidBindVariable(i, lo, valueSize, bindVariableService);
                            break;
                        case X_PG_ARR_INT8:
                        case X_PG_ARR_FLOAT8:
                            setBindVariableAsArray(i, lo, valueSize, msgLimit, bindVariableService);
                            break;
                        default:
                            // before we bind a string, we need to define the type of the variable
                            // so the binding process can cast the string as required
                            setBindVariableAsStr(i, lo, valueSize, bindVariableService, characterStore, directUtf8String);
                            break;
                    }
                } else {
                    // read as a string
                    setBindVariableAsStr(i, lo, valueSize, bindVariableService, characterStore, directUtf8String);
                }
            }
            lo += valueSize;
        }
    }

    private void copyPgResultSetColumnTypesAndNames() {
        // typically, this method called right after sql text has been compiled for the first time.
        // but for example when a factory is obtained from a cache then we postpone calling this method as much as possible.

        // invariant: this method can be called only once per pipeline entry. if an entry already has column types and names
        // set then subsequent compilation should use validatePgResultSetColumnTypesAndNames()
        assert pgResultSetColumnTypes.size() == 0;
        assert pgResultSetColumnNames.size() == 0;

        if (factory == null) {
            return;
        }
        final RecordMetadata m = factory.getMetadata();
        final int columnCount = m.getColumnCount();

        pgResultSetColumnTypes.setPos(2 * columnCount);
        pgResultSetColumnNames.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            final int columnType = m.getColumnType(i);
            pgResultSetColumnTypes.setQuick(2 * i, columnType);
            // the extra values stored here are used to render geo-hashes as strings
            pgResultSetColumnTypes.setQuick(2 * i + 1, GeoHashes.getBitFlags(columnType));
            pgResultSetColumnNames.setQuick(i, m.getColumnName(i));
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
            case X_PG_TIMESTAMP_TZ:
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
            case X_PG_ARR_INT8:
            case X_PG_ARR_FLOAT8:
                bindVariableService.define(j, ColumnType.ARRAY, 0);
                break;
            case PG_UNSPECIFIED:
                // unknown types, we are not defining them for now - this gives
                // the compiler a chance to infer the best possible type
                break;
            default:
                bindVariableService.define(j, ColumnType.STRING, 0);
                break;
        }
    }

    private void ensureCompiledQuery() {
        if (compiledQuery == null) {
            compiledQuery = new CompiledQueryImpl(engine);
        }
    }

    private void ensureValueLength(int variableIndex, int sizeRequired, int sizeActual) throws PGMessageProcessingException {
        if (sizeRequired == sizeActual) {
            return;
        }
        throw kaput()
                .put("bad parameter value length [sizeRequired=").put(sizeRequired)
                .put(", sizeActual=").put(sizeActual)
                .put(", variableIndex=").put(variableIndex)
                .put(']');
    }

    // Used to estimate required column size (or full record size in case of text format)
    // to be reported to the user in the insufficient send buffer size case.
    private long estimateRecordSize(Record record, int columnCount) throws PGMessageProcessingException {
        long recordSize = 0;
        for (int i = 0; i < columnCount; i++) {
            final int columnType = pgResultSetColumnTypes.getQuick(2 * i);
            final int typeTag = ColumnType.tagOf(columnType);
            final short columnBinaryFlag = getPgResultSetColumnFormatCode(i, typeTag);

            // number of bits or chars for geohash
            final int geohashSize = Math.abs(pgResultSetColumnTypes.getQuick(2 * i + 1));

            final long columnValueSize;
            // if column is not variable size and format code is text, we can't calculate size
            if (columnBinaryFlag == 0 && txtAndBinSizesCanBeDifferent(columnType)) {
                columnValueSize = estimateColumnTxtSize(record, i, typeTag);
            } else {
                columnValueSize = calculateColumnBinSize(this, record, i, columnType, geohashSize, Long.MAX_VALUE, 0);
            }

            if (columnValueSize < 0) {
                return Long.MIN_VALUE; // unsupported type
            }

            recordSize += columnValueSize;
        }
        return recordSize;
    }

    private short getPgResultSetColumnFormatCode(int columnIndex) {
        final int columnType = pgResultSetColumnTypes.getQuick(columnIndex * 2);
        return getPgResultSetColumnFormatCode(columnIndex, columnType);
    }

    private short getPgResultSetColumnFormatCode(int columnIndex, int columnType) {
        // binary is always sent as binary (e.g.) we never Base64 encode that
        if (columnType != ColumnType.BINARY) {
            return (msgBindSelectFormatCodeCount > 1 ? msgBindSelectFormatCodes.get(columnIndex) : msgBindSelectFormatCodes.get(0)) ? (short) 1 : 0;
        }
        return 1;
    }

    private boolean isTextFormat() {
        return msgBindSelectFormatCodeCount == 0 || (msgBindSelectFormatCodeCount == 1 && !msgBindSelectFormatCodes.get(0));
    }

    private PGMessageProcessingException kaput() {
        return PGMessageProcessingException.instance(this);
    }

    private long msgBindComputeParameterValueAreaSize(long lo, long msgLimit) throws PGMessageProcessingException {
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

    private void msgExecuteDDL(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            SCSequence tempSequence,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool
    ) throws SqlException, PGMessageProcessingException {
        if (transactionState != ERROR_TRANSACTION) {
            engine.getMetrics().pgWireMetrics().markStart();
            // execute against writer from the engine, synchronously (null sequence)
            try {
                ensureCompiledQuery();
                for (int attempt = 1; ; attempt++) {
                    try (OperationFuture fut = compiledQuery.execute(sqlExecutionContext, tempSequence, false)) {
                        // this doesn't actually wait, because the call is synchronous
                        fut.await();
                        sqlAffectedRowCount = fut.getAffectedRowsCount();
                        break;
                    } catch (TableReferenceOutOfDateException e) {
                        Misc.free(compiledQuery.getUpdateOperation());
                        if (attempt == maxRecompileAttempts) {
                            throw e;
                        }
                        compileNewSQL(sqlText, engine, sqlExecutionContext, taiPool, true);
                    }
                }
            } finally {
                engine.getMetrics().pgWireMetrics().markComplete();
            }
        }
    }

    private void msgExecuteInsert(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters,
            // todo: WriterSource is the interface used exclusively in PG Wire. We should not need to pass
            //    around heaps of state in very long call stacks
            WriterSource writerSource,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool
    ) throws SqlException, PGMessageProcessingException {
        switch (transactionState) {
            case IMPLICIT_TRANSACTION:
                // fall through, there is no difference between implicit and explicit transaction at this stage
            case IN_TRANSACTION: {
                sqlExecutionContext.setCacheHit(cacheHit);
                engine.getMetrics().pgWireMetrics().markStart();
                try {
                    for (int attempt = 1; ; attempt++) {
                        final InsertOperation insertOp = tai.getInsert();
                        InsertMethod m;
                        try {
                            m = insertOp.createMethod(sqlExecutionContext, writerSource);
                            try {
                                sqlAffectedRowCount = m.execute(sqlExecutionContext);
                                TableWriterAPI writer = m.popWriter();
                                pendingWriters.put(writer.getTableToken(), writer);
                            } catch (Throwable th) {
                                TableWriterAPI w = m.popWriter();
                                if (w != null) {
                                    pendingWriters.remove(w.getTableToken());
                                }
                                Misc.free(w);
                                throw th;
                            }
                            break;
                        } catch (TableReferenceOutOfDateException e) {
                            tai = Misc.free(tai);
                            if (attempt == maxRecompileAttempts) {
                                throw e;
                            }
                            compileNewSQL(sqlText, engine, sqlExecutionContext, taiPool, true);
                        }
                    }
                } finally {
                    engine.getMetrics().pgWireMetrics().markComplete();
                }
            }
            break;
            case ERROR_TRANSACTION:
                // when transaction is in error state, skip execution
                break;
            default:
                assert false : "unknown transaction state: " + transactionState;
        }
    }

    private void msgExecuteSelect(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool,
            int maxRecompileAttempts
    ) throws SqlException, PGMessageProcessingException {
        if (cursor == null) {
            engine.getMetrics().pgWireMetrics().markStart();

            // commit implicitly if we are not in a transaction
            // this makes data inserted in the same pipeline visible to the select
            if (transactionState == IMPLICIT_TRANSACTION) {
                commit(pendingWriters);
            }

            sqlExecutionContext.getCircuitBreaker().resetTimer();
            sqlExecutionContext.setCacheHit(cacheHit);
            // if the current execution is in the execute stage of prepare-execute mode, we always set the `cacheHit` to true after the first execution.
            // (The execute stage always does not compile the query, while the first execution corresponds to the prepare stage's cacheHit flag.)
            if (isPreparedStatement()) {
                cacheHit = true;
            }

            try {
                for (int attempt = 1; ; attempt++) {
                    // check if factory is null, what might happen is that
                    // prepared statement (entry we held on to) failed to compile, factory is null
                    // The goal would be to just recompile from text.
                    if (factory != null) {
                        try {
                            cursor = factory.getCursor(sqlExecutionContext);
                            // when factory is not null, and we can obtain cursor without issues
                            // we would exit early
                            break;
                        } catch (TableReferenceOutOfDateException e) {
                            if (attempt == maxRecompileAttempts) {
                                throw e;
                            }
                        }
                        factory = Misc.free(factory);
                    }
                    compileNewSQL(sqlText, engine, sqlExecutionContext, taiPool, true);
                }
            } catch (Throwable e) {
                // un-cache the erroneous SQL
                tas = Misc.free(tas);
                factory = null;
                throw e;
            }
        }
    }

    private void msgExecuteUpdate(
            SqlExecutionContext sqlExecutionContext,
            int transactionState,
            ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters,
            SCSequence tempSequence,
            WeakSelfReturningObjectPool<TypesAndInsert> taiPool
    ) throws SqlException, PGMessageProcessingException {
        if (transactionState != ERROR_TRANSACTION) {
            engine.getMetrics().pgWireMetrics().markStart();
            // execute against writer from the engine, synchronously (null sequence)
            ensureCompiledQuery();
            try {
                for (int attempt = 1; ; attempt++) {
                    try {
                        UpdateOperation updateOperation = compiledQuery.getUpdateOperation();
                        TableToken tableToken = updateOperation.getTableToken();
                        final int index = pendingWriters.keyIndex(tableToken);
                        if (index < 0) {
                            updateOperation.withContext(sqlExecutionContext);
                            // cached writers to remain in the list until transaction end
                            @SuppressWarnings("resource")
                            TableWriterAPI tableWriterAPI = pendingWriters.valueAt(index);
                            // Update implicitly commits. WAL table cannot do 2 commits in 1 call and require commits to be made upfront.
                            tableWriterAPI.commit();
                            sqlAffectedRowCount = tableWriterAPI.apply(updateOperation);
                        } else {
                            try (OperationFuture fut = compiledQuery.execute(sqlExecutionContext, tempSequence, false)) {
                                fut.await();
                                sqlAffectedRowCount = fut.getAffectedRowsCount();
                            }
                        }
                        break;
                    } catch (TableReferenceOutOfDateException e) {
                        Misc.free(compiledQuery.getUpdateOperation());
                        if (attempt == maxRecompileAttempts) {
                            throw e;
                        }
                        compileNewSQL(sqlText, engine, sqlExecutionContext, taiPool, true);
                    }
                }
            } finally {
                engine.getMetrics().pgWireMetrics().markComplete();
            }
        }
    }

    private void msgParseCopyOutTypeDescriptionTypeOIDs(BindVariableService bindVariableService) {
        // Priority order for determining OID column types:
        // 1. Client-specified type in PARSE message (if provided and not PG_UNSPECIFIED nor X_PG_VOID)
        // 2. Compiler-inferred type (fallback)
        //
        // Important: We cannot exclusively rely on compiler-inferred types for OID because:
        // - Clients may specify their own type expectations
        // - Type mismatches between PARSE and subsequent DESCRIBE messages can cause client errors
        // - Some clients (e.g., PG JDBC) strictly validate type consistency between types in PARSE and DESCRIBE messages
        // In contract, QuestDB natives types are always stored as derived by compiler. Without considering

        final int n = bindVariableService.getIndexedVariableCount();
        outParameterTypeDescriptionTypes.setPos(n);
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                int oid = PG_UNSPECIFIED;

                // First we prioritize the types we received in the PARSE message
                if (msgParseParameterTypeOIDs.size() > i) {
                    oid = msgParseParameterTypeOIDs.getQuick(i);
                }

                // if there was no type in the PARSE message, we use the type inferred by the compiler
                // Q: why we cannot always use the types provided by a compiler?
                // A: the compiler might infer slightly different type than the client provided.
                //    if the client include types in a PARSE message and a subsequent DESCRIBE sends back different types
                //    the client will error out. e.g. PG JDBC is very strict about this.
                final Function f = bindVariableService.getFunction(i);
                int nativeType;
                if (f != null) {
                    nativeType = f.getType();
                    if (oid == PG_UNSPECIFIED || oid == X_PG_VOID) {
                        // oid is stored as Big Endian
                        // since that's what clients expects - pgwire is big endian
                        oid = Numbers.bswap(PGOids.getTypeOid(nativeType));
                    }
                } else {
                    nativeType = ColumnType.UNDEFINED;
                }
                outParameterTypeDescriptionTypes.setQuick(i, Numbers.encodeLowHighInts(nativeType, oid));
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

    private void outColBinArr(PGResponseSink utf8Sink, Record record, int columnIndex, int columnType) {
        ArrayView array = record.getArray(columnIndex, columnType);
        if (array.getDimCount() == 0) {
            utf8Sink.setNullValue();
            return;
        }
        short elemType = array.getElemType();
        if (outResendArrayFlatIndex == 0) {
            // Send the header. We must ensure at least one element follows the header, otherwise the
            // outResendArrayFlatIndex stays at 0 even though the header was already sent, which will cause
            // the header to be sent again.
            int nDims = array.getDimCount();
            int componentTypeOid = getTypeOid(elemType);
            int notNullCount = PGUtils.countNotNull(array, 0);

            // The size field indicates the size of what follows, excluding its own size,
            // that's why we subtract Integer.BYTES from it. The same method is used to calculate
            // the full size of the message, and in that case this field must be included.
            utf8Sink.putNetworkInt(PGUtils.calculateArrayColBinSize(array, notNullCount) - Integer.BYTES);
            utf8Sink.putNetworkInt(nDims);
            utf8Sink.putIntDirect(notNullCount < array.getCardinality() ? 1 : 0); // "has nulls" flag
            utf8Sink.putNetworkInt(componentTypeOid);
            for (int i = 0; i < nDims; i++) {
                utf8Sink.putNetworkInt(array.getDimLen(i)); // length of each dimension
                utf8Sink.putNetworkInt(1); // lower bound, always 1 in QuestDB
            }
        }
        try {
            if (array.isVanilla()) {
                int len = array.getFlatViewLength();
                // Note that we rely on a HotSpot optimization: Loop-invariant code motion.
                // It moves the switch outside the loop.
                for (int i = outResendArrayFlatIndex; i < len; i++) {
                    switch (elemType) {
                        case ColumnType.LONG:
                            utf8Sink.putNetworkInt(Long.BYTES);
                            utf8Sink.putNetworkLong(array.getLong(i));
                            break;
                        case ColumnType.DOUBLE:
                            double val = array.getDouble(i);
                            if (Numbers.isFinite(val)) {
                                utf8Sink.putNetworkInt(Double.BYTES);
                                utf8Sink.putNetworkDouble(val);
                            } else {
                                utf8Sink.setNullValue();
                            }
                            break;
                    }
                    utf8Sink.bookmark();
                    outResendArrayFlatIndex = i + 1;
                }
            } else {
                outColBinArrRecursive(utf8Sink, array, elemType, 0, 0, 0);
            }
            outResendArrayFlatIndex = 0;
        } catch (NoSpaceLeftInResponseBufferException e) {
            utf8Sink.resetToBookmark();
            throw e;
        }
    }

    private int outColBinArrRecursive(
            PGResponseSink utf8Sink, ArrayView array, short elemType, int dim, int flatIndex, int outFlatIndex
    ) {
        final int count = array.getDimLen(dim);
        final int stride = array.getStride(dim);
        if (dim < array.getDimCount() - 1) {
            for (int i = 0; i < count; i++) {
                outFlatIndex = outColBinArrRecursive(utf8Sink, array, elemType, dim + 1, flatIndex, outFlatIndex);
                flatIndex += stride;
            }
        } else {
            for (int i = 0; i < count; i++) {
                if (outFlatIndex == outResendArrayFlatIndex) {
                    switch (elemType) {
                        case ColumnType.LONG: {
                            long val = array.getLong(flatIndex);
                            if (val != Numbers.LONG_NULL) {
                                utf8Sink.putNetworkInt(Double.BYTES);
                                utf8Sink.putNetworkDouble(val);
                            } else {
                                utf8Sink.setNullValue();
                            }
                            break;
                        }
                        case ColumnType.DOUBLE: {
                            double val = array.getDouble(flatIndex);
                            if (Numbers.isFinite(val)) {
                                utf8Sink.putNetworkInt(Double.BYTES);
                                utf8Sink.putNetworkDouble(val);
                            } else {
                                utf8Sink.setNullValue();
                            }
                            break;
                        }
                    }
                    utf8Sink.bookmark();
                    outResendArrayFlatIndex++;
                }
                outFlatIndex++;
                flatIndex += stride;
            }
        }
        return outFlatIndex;
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
        if (Numbers.isNull(value)) {
            utf8Sink.setNullValue();
        } else {
            utf8Sink.putNetworkInt(Double.BYTES);
            utf8Sink.putNetworkDouble(value);
        }
    }

    private void outColBinFloat(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final float value = record.getFloat(columnIndex);
        if (Numbers.isNull(value)) {
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

    private void outColBinTimestamp(PGResponseSink utf8Sink, Record record, int columnIndex, int columnType) {
        final long longValue = record.getTimestamp(columnIndex);
        if (longValue == Numbers.LONG_NULL) {
            utf8Sink.setNullValue();
        } else {
            utf8Sink.putNetworkInt(Long.BYTES);
            // PG epoch starts at 2000 rather than 1970
            utf8Sink.putNetworkLong(ColumnType.getTimestampDriver(columnType).toMicros(longValue) - Numbers.JULIAN_EPOCH_OFFSET_USEC);
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

    private void outColBinary(PGResponseSink utf8Sink, Record record, int columnIndex) throws PGMessageProcessingException {
        BinarySequence sequence = record.getBin(columnIndex);
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
                        .put(", columnIndex=").put(columnIndex)
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

    private void outColInterval(PGResponseSink utf8Sink, Record record, int columnIndex, int intervalType) {
        final Interval interval = record.getInterval(columnIndex);
        if (Interval.NULL.equals(interval)) {
            utf8Sink.setNullValue();
        } else {
            long a = utf8Sink.skipInt();
            interval.toSink(utf8Sink, intervalType);
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

    private void outColTxtArr(PGResponseSink utf8Sink, Record record, int columnIndex, int columnType) {
        ArrayView arrayView = record.getArray(columnIndex, columnType);

        // zero dimension array indicates NULL
        if (arrayView.getDimCount() == 0) {
            utf8Sink.setNullValue();
            return;
        }
        long a = utf8Sink.skipInt();
        ArrayTypeDriver.arrayToPgWire(arrayView, utf8Sink);
        utf8Sink.putLenEx(a);
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
            PG_DATE_MILLI_TIME_Z_PRINT_FORMAT.format(longValue, EN_LOCALE, null, utf8Sink);
            utf8Sink.putLenEx(a);
        } else {
            utf8Sink.setNullValue();
        }
    }

    private void outColTxtDouble(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final double doubleValue = record.getDouble(columnIndex);
        if (Numbers.isNull(doubleValue)) {
            utf8Sink.setNullValue();
        } else {
            final long a = utf8Sink.skipInt();
            utf8Sink.put(doubleValue);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColTxtFloat(PGResponseSink responseUtf8Sink, Record record, int columnIndex) {
        final float floatValue = record.getFloat(columnIndex);
        if (Numbers.isNull(floatValue)) {
            responseUtf8Sink.setNullValue();
        } else {
            final long a = responseUtf8Sink.skipInt();
            responseUtf8Sink.put(floatValue);
            responseUtf8Sink.putLenEx(a);
        }
    }

    private void outColTxtGeoByte(PGResponseSink utf8Sink, Record rec, int columnIndex, int bitFlags) {
        outColTxtGeoHash(utf8Sink, rec.getGeoByte(columnIndex), bitFlags);
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

    private void outColTxtGeoInt(PGResponseSink utf8Sink, Record rec, int columnIndex, int bitFlags) {
        outColTxtGeoHash(utf8Sink, rec.getGeoInt(columnIndex), bitFlags);
    }

    private void outColTxtGeoLong(PGResponseSink utf8Sink, Record rec, int columnIndex, int bitFlags) {
        outColTxtGeoHash(utf8Sink, rec.getGeoLong(columnIndex), bitFlags);
    }

    private void outColTxtGeoShort(PGResponseSink utf8Sink, Record rec, int columnIndex, int bitFlags) {
        outColTxtGeoHash(utf8Sink, rec.getGeoShort(columnIndex), bitFlags);
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

    private void outColTxtInt(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final int intValue = record.getInt(columnIndex);
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

    private void outColTxtLong256(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final Long256 long256Value = record.getLong256A(columnIndex);
        if (long256Value.getLong0() == Numbers.LONG_NULL
                && long256Value.getLong1() == Numbers.LONG_NULL
                && long256Value.getLong2() == Numbers.LONG_NULL
                && long256Value.getLong3() == Numbers.LONG_NULL) {
            utf8Sink.setNullValue();
        } else {
            final long a = utf8Sink.skipInt();
            Numbers.appendLong256(long256Value, utf8Sink);
            utf8Sink.putLenEx(a);
        }
    }

    private void outColTxtShort(PGResponseSink utf8Sink, Record record, int columnIndex) {
        final long a = utf8Sink.skipInt();
        utf8Sink.put(record.getShort(columnIndex));
        utf8Sink.putLenEx(a);
    }

    private void outColTxtTimestamp(PGResponseSink utf8Sink, Record record, int columnIndex, int timestampType) {
        long offset;
        long timestamp = record.getTimestamp(columnIndex);
        if (timestamp == Numbers.LONG_NULL) {
            utf8Sink.setNullValue();
        } else {
            offset = utf8Sink.skipInt();
            ColumnType.getTimestampDriver(timestampType).appendToPGWireText(utf8Sink, timestamp);
            utf8Sink.putLenEx(offset);
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

    private void outCursor(SqlExecutionContext sqlExecutionContext, PGResponseSink utf8Sink)
            throws QueryPausedException {
        if (pgResultSetColumnTypes.size() == 0) {
            copyPgResultSetColumnTypesAndNames();
        }

        switch (stateSync) {
            case SYNC_COMPUTE_CURSOR_SIZE:
                outComputeCursorSize();
                stateSync = SYNC_DATA;
            case SYNC_DATA:
                utf8Sink.bookmark();
                outCursor(sqlExecutionContext, utf8Sink, factory.getMetadata().getColumnCount());
                break;
            default:
                assert false;
        }
    }

    private void outCursor(
            SqlExecutionContext sqlExecutionContext,
            PGResponseSink utf8Sink,
            int columnCount
    ) throws QueryPausedException {
        if (!sqlExecutionContext.getCircuitBreaker().isTimerSet()) {
            sqlExecutionContext.getCircuitBreaker().resetTimer();
        }

        long recordStartAddress = utf8Sink.getSendBufferPtr();
        try {
            final Record record = cursor.getRecord();
            if (outResendCursorRecord) {
                outRecord(utf8Sink, record, columnCount);
                recordStartAddress = utf8Sink.getSendBufferPtr();
            }

            while (sqlReturnRowCount < sqlReturnRowCountToBeSent && cursor.hasNext()) {
                outResendCursorRecord = true;
                outResendRecordHeader = true;
                outRecord(utf8Sink, record, columnCount);
                recordStartAddress = utf8Sink.getSendBufferPtr();
            }
        } catch (DataUnavailableException e) {
            utf8Sink.resetToBookmark();
            throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
        } catch (NoSpaceLeftInResponseBufferException e) {
            throw e;
        } catch (Throwable th) {
            LOG.debug().$("unexpected error in outCursor [ex=").$(th).I$();
            // We'll be sending an error to the client, so reset to the start of the last sent message.
            utf8Sink.resetToBookmark(recordStartAddress);
            if (th instanceof FlyweightMessageContainer) {
                final StringSink errorMsgSink = getErrorMessageSink();
                int errno;
                if (th instanceof CairoException && (errno = ((CairoException) th).getErrno()) != CairoException.NON_CRITICAL) {
                    errorMsgSink.put('[');
                    errorMsgSink.put(errno);
                    errorMsgSink.put("] ");
                }
                errorMsgSink.put(((FlyweightMessageContainer) th).getFlyweightMessage());
            } else {
                String msg = th.getMessage();
                if (msg != null) {
                    getErrorMessageSink().put(msg);
                } else {
                    getErrorMessageSink().putAscii("no message provided (internal error)");
                }
                if (th instanceof AssertionError) {
                    // assertion errors means either a questdb bug or data corruption ->
                    // we want to see a full stack trace in server logs and log it as critical
                    LOG.critical().$("error in pgwire execute, ex=").$(th).$();
                }
            }
        }

        // the above loop may have exited due to the return row limit as prescribed by the portal
        // either way, the result set was sent out as intended. The difference is in what we
        // send as the suffix.

        if (sqlReturnRowCount < sqlReturnRowCountToBeSent) {
            stateSync = SYNC_DATA_EXHAUSTED;
        } else {
            // we sent as many rows as was requested, but we have more to send
            stateSync = SYNC_DATA_SUSPENDED;
        }

        engine.getMetrics().pgWireMetrics().markComplete();
    }

    private void outError(PGResponseSink utf8Sink, ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters) {
        rollback(pendingWriters);
        utf8Sink.resetToBookmark();
        utf8Sink.bookmark();

        final boolean emptyBuffer = utf8Sink.getWrittenBytes() == 0;
        final int position = getErrorMessagePosition();
        utf8Sink.put(MESSAGE_TYPE_ERROR_RESPONSE);
        long addr = utf8Sink.skipInt();

        utf8Sink.putAscii('C'); // C = SQLSTATE
        if (stalePlanError) {
            // this is what PostgresSQL sends when recompiling a query produces a different ResultSet.
            // some clients act on it by restarting the query from the beginning.
            utf8Sink.putZ("0A000"); // SQLSTATE = feature_not_supported
            utf8Sink.putAscii('R'); // R = Routine: the name of the source-code routine reporting the error, we mimic PostgresSQL here
            utf8Sink.putZ("RevalidateCachedQuery"); // name of the routine
        } else {
            utf8Sink.putZ("00000"); // SQLSTATE = successful_completion (sic)
        }

        utf8Sink.putAscii('M');

        final StringSink errorSink = getErrorMessageSink();
        final int remainingBufferBytes = (int) (utf8Sink.getSendBufferSize() - utf8Sink.getWrittenBytes());
        if (emptyBuffer) {
            // We've started from an empty send buffer. If the error message doesn't fit,
            // we write a truncated version of the message.
            if (!utf8Sink.putWithLimit(errorSink, remainingBufferBytes - ERROR_TAIL_MAX_SIZE - 4)) {
                utf8Sink.put("..."); // the message got truncated
            }
            utf8Sink.put((byte) 0); // trailing zero byte
        } else {
            utf8Sink.putZ(errorSink);
        }
        // Note: don't forget to update ERROR_TAIL_MAX_SIZE when changing this code.
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
        final int n = outParameterTypeDescriptionTypes.size();
        utf8Sink.putNetworkShort((short) n);
        for (int i = 0; i < n; i++) {
            int pgType = Numbers.decodeHighInt(outParameterTypeDescriptionTypes.getQuick(i));
            utf8Sink.putIntDirect(pgType);
        }
        utf8Sink.putLen(offset);
    }

    private void outRecord(PGResponseSink utf8Sink, Record record, int columnCount) throws PGMessageProcessingException {
        long messageLengthAddress = 0;
        // message header can be sent alone if we run out of space on the first column
        if (outResendColumnIndex == 0 && outResendRecordHeader) {
            utf8Sink.put(MESSAGE_TYPE_DATA_ROW);
            messageLengthAddress = utf8Sink.skipInt();
            utf8Sink.putNetworkShort((short) columnCount);
            utf8Sink.bookmark();
        }
        final boolean isMsgLengthRequired = messageLengthAddress > 0;
        try {
            while (outResendColumnIndex < columnCount) {
                final int colIndex = outResendColumnIndex;
                final int columnType = pgResultSetColumnTypes.getQuick(2 * colIndex);
                final int columnTag = ColumnType.tagOf(columnType);
                final short columnBinaryFlag = getPgResultSetColumnFormatCode(colIndex, columnType);

                final int tagWithFlag = toColumnBinaryType(columnBinaryFlag, columnTag);
                switch (tagWithFlag) {
                    case BINARY_TYPE_INT:
                        outColBinInt(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.INT:
                        outColTxtInt(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.IPv4:
                        outColTxtIPv4(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.INTERVAL:
                    case BINARY_TYPE_INTERVAL:
                        outColInterval(utf8Sink, record, colIndex, columnType);
                        break;
                    case ColumnType.VARCHAR:
                    case BINARY_TYPE_VARCHAR:
                        outColVarchar(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.ARRAY_STRING:
                    case BINARY_TYPE_ARRAY_STRING:
                        // intentional fall-through
                        // ARRAY_STRING is not a first-class type. it's a hack to implement certain postgresql
                        // metadata functions, we send it as if it was a STRING
                    case ColumnType.STRING:
                    case BINARY_TYPE_STRING:
                        outColString(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.SYMBOL:
                    case BINARY_TYPE_SYMBOL:
                        outColSymbol(utf8Sink, record, colIndex);
                        break;
                    case BINARY_TYPE_LONG:
                        outColBinLong(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.LONG:
                        outColTxtLong(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.SHORT:
                        outColTxtShort(utf8Sink, record, colIndex);
                        break;
                    case BINARY_TYPE_DOUBLE:
                        outColBinDouble(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.DOUBLE:
                        outColTxtDouble(utf8Sink, record, colIndex);
                        break;
                    case BINARY_TYPE_FLOAT:
                        outColBinFloat(utf8Sink, record, colIndex);
                        break;
                    case BINARY_TYPE_SHORT:
                        outColBinShort(utf8Sink, record, colIndex);
                        break;
                    case BINARY_TYPE_DATE:
                        outColBinDate(utf8Sink, record, colIndex);
                        break;
                    case BINARY_TYPE_TIMESTAMP:
                        outColBinTimestamp(utf8Sink, record, colIndex, columnType);
                        break;
                    case BINARY_TYPE_BYTE:
                        outColBinByte(utf8Sink, record, colIndex);
                        break;
                    case BINARY_TYPE_UUID:
                        outColBinUuid(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.FLOAT:
                        outColTxtFloat(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.TIMESTAMP:
                        outColTxtTimestamp(utf8Sink, record, colIndex, columnType);
                        break;
                    case ColumnType.DATE:
                        outColTxtDate(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.BOOLEAN:
                        outColTxtBool(utf8Sink, record, colIndex);
                        break;
                    case BINARY_TYPE_BOOLEAN:
                        outColBinBool(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.BYTE:
                        outColTxtByte(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.BINARY:
                    case BINARY_TYPE_BINARY:
                        outColBinary(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.CHAR:
                    case BINARY_TYPE_CHAR:
                        outColChar(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.LONG256:
                    case BINARY_TYPE_LONG256:
                        outColTxtLong256(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.GEOBYTE:
                        outColTxtGeoByte(utf8Sink, record, colIndex, pgResultSetColumnTypes.getQuick(2 * colIndex + 1));
                        break;
                    case ColumnType.GEOSHORT:
                        outColTxtGeoShort(utf8Sink, record, colIndex, pgResultSetColumnTypes.getQuick(2 * colIndex + 1));
                        break;
                    case ColumnType.GEOINT:
                        outColTxtGeoInt(utf8Sink, record, colIndex, pgResultSetColumnTypes.getQuick(2 * colIndex + 1));
                        break;
                    case ColumnType.GEOLONG:
                        outColTxtGeoLong(utf8Sink, record, colIndex, pgResultSetColumnTypes.getQuick(2 * colIndex + 1));
                        break;
                    case ColumnType.NULL:
                        utf8Sink.setNullValue();
                        break;
                    case ColumnType.UUID:
                        outColTxtUuid(utf8Sink, record, colIndex);
                        break;
                    case ColumnType.ARRAY:
                        outColTxtArr(utf8Sink, record, colIndex, columnType);
                        break;
                    case BINARY_TYPE_ARRAY:
                        outColBinArr(utf8Sink, record, colIndex, columnType);
                        break;
                    default:
                        assert false;
                }
                outResendColumnIndex++;
                utf8Sink.bookmark();
            }
        } catch (NoSpaceLeftInResponseBufferException e) {
            if (isTextFormat()) {
                assert messageLengthAddress > 0;
                resetIncompleteRecord(utf8Sink, messageLengthAddress);
                if (utf8Sink.getWrittenBytes() == 0) {
                    // We had nothing but the record in the send buffer,
                    // so we can estimate the required size to be reported to the user.
                    final long estimatedSize = estimateRecordSize(record, columnCount);
                    e.setBytesRequired(estimatedSize);
                }
            } else {
                utf8Sink.resetToBookmark();
                if (isMsgLengthRequired) {
                    final long sizeInBuffer = utf8Sink.getSendBufferPtr() - messageLengthAddress;
                    assert sizeInBuffer > 0;
                    try {
                        final long recordTailSize = calculateRecordTailSize(
                                record,
                                columnCount,
                                utf8Sink.getMaxBlobSize(),
                                utf8Sink.getSendBufferSize()
                        );
                        long msgLength = sizeInBuffer + recordTailSize;
                        if (recordTailSize > 0 && msgLength <= Integer.MAX_VALUE) {
                            putInt(messageLengthAddress, (int) msgLength);
                            outResendRecordHeader = false;
                        } else {
                            resetIncompleteRecord(utf8Sink, messageLengthAddress);
                            if (utf8Sink.getWrittenBytes() == 0) {
                                // We had nothing but the record in the send buffer,
                                // so we can estimate the required size to be reported to the user.
                                e.setBytesRequired(estimateRecordSize(record, columnCount));
                            }
                        }
                    } catch (PGMessageProcessingException bpe) {
                        // we have binary data blob size > maxBlobSize
                        resetIncompleteRecord(utf8Sink, messageLengthAddress);
                        throw bpe;
                    }
                }
            }
            throw e;
        }

        // no overflow, the full record is in the buffer
        if (isMsgLengthRequired) {
            utf8Sink.putLen(messageLengthAddress);
        }
        utf8Sink.bookmark();
        outResendCursorRecord = false;
        outResendColumnIndex = 0;
        outResendRecordHeader = true;
        sqlReturnRowCount++;
    }

    private void outRowDescription(PGResponseSink utf8Sink) {
        if (pgResultSetColumnTypes.size() == 0) {
            copyPgResultSetColumnTypesAndNames();
        }

        final int n = pgResultSetColumnTypes.size() / 2;
        long messageLengthAddress = 0;
        if (outResendColumnIndex == 0 && outResendRecordHeader) {
            utf8Sink.put(MESSAGE_TYPE_ROW_DESCRIPTION);
            messageLengthAddress = utf8Sink.skipInt();
            utf8Sink.putNetworkShort((short) n);
            utf8Sink.bookmark();
        }
        final boolean isMsgLengthRequired = messageLengthAddress > 0;
        try {
            while (outResendColumnIndex < n) {
                final int i = outResendColumnIndex;
                final int typeFlag = pgResultSetColumnTypes.getQuick(2 * i);
                final int columnType = toColumnType(ColumnType.isNull(typeFlag) ? ColumnType.STRING : typeFlag);
                utf8Sink.putZ(pgResultSetColumnNames.get(i));
                utf8Sink.putIntDirect(0); // tableOid for each column is optional, so we always set it to zero
                utf8Sink.putNetworkShort((short) (i + 1)); //column number, starting from 1
                int pgType = getTypeOid(columnType);
                utf8Sink.putNetworkInt(pgType); // type

                // type size
                short xSize = X_PG_TYPE_TO_SIZE_MAP.get(pgType);
                utf8Sink.putDirectShort(xSize);

                int xTypeModifier = PGOids.getXAttTypMod(pgType);
                utf8Sink.putDirectInt(xTypeModifier);

                // this is special behaviour for binary fields to prevent binary data being hex encoded on the wire
                // format code
                utf8Sink.putNetworkShort(getPgResultSetColumnFormatCode(i)); // format code
                utf8Sink.bookmark();
                outResendColumnIndex++;
            }
        } catch (NoSpaceLeftInResponseBufferException e) {
            utf8Sink.resetToBookmark();
            if (isMsgLengthRequired) {
                final long sizeInBuffer = utf8Sink.getSendBufferPtr() - messageLengthAddress;
                assert sizeInBuffer > 0;
                // tableOid + column number + type + type size + type modifier + format code
                long tailSize = 0;
                for (int i = outResendColumnIndex; i < n; i++) {
                    final String columnName = pgResultSetColumnNames.get(i);
                    assert columnName != null && !columnName.isEmpty();
                    final int utf8Bytes = Utf8s.utf8Bytes(columnName);
                    tailSize += utf8Bytes + 1 + ROW_DESCRIPTION_COLUMN_RECORD_FIXED_SIZE;
                }
                assert tailSize > 0;
                final long messageSizeWithHeader = sizeInBuffer + tailSize;
                if (messageSizeWithHeader <= Integer.MAX_VALUE) {
                    putInt(messageLengthAddress, (int) messageSizeWithHeader);
                    outResendRecordHeader = false;
                } else {
                    resetIncompleteRecord(utf8Sink, messageLengthAddress);
                    if (utf8Sink.getWrittenBytes() == 0) {
                        e.setBytesRequired(messageSizeWithHeader + 1); // +1 for the message type
                    }
                }
            }
            throw e;
        }
        if (isMsgLengthRequired) {
            utf8Sink.putLen(messageLengthAddress);
        }
        utf8Sink.bookmark();
        outResendColumnIndex = 0;
        outResendRecordHeader = true;
    }

    private void resetIncompleteRecord(PGResponseSink utf8Sink, long messageLengthAddress) {
        outResendColumnIndex = 0;
        outResendRecordHeader = true;
        // reset to the message start
        utf8Sink.resetToBookmark(messageLengthAddress - Byte.BYTES);
    }

    private void setBindVariableAsArray(int i, long lo, int valueSize, long msgLimit, BindVariableService bindVariableService) throws SqlException, PGMessageProcessingException {
        int dimensions = getInt(lo, msgLimit, "malformed array dimensions");
        lo += Integer.BYTES;
        valueSize -= Integer.BYTES;
        if (dimensions == 0) {
            throw kaput().put("array dimensions cannot be zero");
        }
        if (dimensions > ColumnType.ARRAY_NDIMS_LIMIT) {
            throw kaput().put("array dimensions cannot be greater than maximum array dimensions [dimensions=").put(dimensions).put(", max=").put(ColumnType.ARRAY_NDIMS_LIMIT).put(']');
        }

        getInt(lo, msgLimit, "malformed array null flag");
        // hasNull flag is not a reliable indicator of a null element, since some clients
        // send it as 0 even if the array element is null. we need to manually check for null
        lo += Integer.BYTES;
        valueSize -= Integer.BYTES;

        int componentOid = getInt(lo, msgLimit, "malformed array component oid");
        lo += Integer.BYTES;
        valueSize -= Integer.BYTES;

        PGNonNullBinaryArrayView arrayView = arrayViewPool.next();
        for (int j = 0; j < dimensions; j++) {
            int dimensionSize = getInt(lo, msgLimit, "malformed array dimension size");
            arrayView.addDimLen(dimensionSize);
            lo += Integer.BYTES;
            valueSize -= Integer.BYTES;

            lo += Integer.BYTES; // skip lower bound, it's always 1
            valueSize -= Integer.BYTES;
        }
        arrayView.setPtrAndCalculateStrides(lo, lo + valueSize, componentOid, this);
        bindVariableService.setArray(i, arrayView);
    }

    private void setBindVariableAsBoolean(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws SqlException, PGMessageProcessingException {
        ensureValueLength(variableIndex, Byte.BYTES, valueSize);
        byte val = Unsafe.getUnsafe().getByte(valueAddr);
        bindVariableService.setBoolean(variableIndex, val == 1);
    }

    private void setBindVariableAsChar(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService,
            CharacterStore characterStore
    ) throws PGMessageProcessingException, SqlException {
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
            BindVariableService bindVariableService
    ) throws SqlException, PGMessageProcessingException {
        ensureValueLength(variableIndex, Integer.BYTES, valueSize);

        // represents the number of days since 2000-01-01
        int days = getIntUnsafe(valueAddr);
        long millis = Dates.addDays(Numbers.JULIAN_EPOCH_OFFSET_MILLIS, days);
        bindVariableService.setDate(variableIndex, millis);
    }

    private void setBindVariableAsDouble(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws PGMessageProcessingException, SqlException {
        ensureValueLength(variableIndex, Double.BYTES, valueSize);
        bindVariableService.setDouble(variableIndex, Double.longBitsToDouble(getLongUnsafe(valueAddr)));
    }

    private void setBindVariableAsFloat(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws PGMessageProcessingException, SqlException {
        ensureValueLength(variableIndex, Float.BYTES, valueSize);
        bindVariableService.setFloat(variableIndex, Float.intBitsToFloat(getIntUnsafe(valueAddr)));
    }

    private void setBindVariableAsInt(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws PGMessageProcessingException, SqlException {
        ensureValueLength(variableIndex, Integer.BYTES, valueSize);
        bindVariableService.setInt(variableIndex, getIntUnsafe(valueAddr));
    }

    private void setBindVariableAsLong(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws PGMessageProcessingException, SqlException {
        ensureValueLength(variableIndex, Long.BYTES, valueSize);
        bindVariableService.setLong(variableIndex, getLongUnsafe(valueAddr));
    }

    private void setBindVariableAsShort(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws PGMessageProcessingException, SqlException {
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
    ) throws PGMessageProcessingException {
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
                    throw kaput().put("invalid UTF8 encoding for string value [variableIndex=").put(variableIndex).put(']');
                }
            }
        } catch (PGMessageProcessingException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw kaput().put(ex);
        }
    }

    private void setBindVariableAsTimestamp(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws PGMessageProcessingException, SqlException {
        ensureValueLength(variableIndex, Long.BYTES, valueSize);
        bindVariableService.setTimestampWithType(variableIndex, ColumnType.TIMESTAMP_MICRO, getLongUnsafe(valueAddr) + Numbers.JULIAN_EPOCH_OFFSET_USEC);
    }

    private void setUuidBindVariable(
            int variableIndex,
            long valueAddr,
            int valueSize,
            BindVariableService bindVariableService
    ) throws PGMessageProcessingException, SqlException {
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
                // fall-through
            case CompiledQuery.DROP:
                // fall-through
            case CompiledQuery.CREATE_MAT_VIEW:
                // fall-through
            case CompiledQuery.CREATE_TABLE:
                operation = cq.getOperation();
                sqlTag = TAG_OK;
                break;
            case CompiledQuery.EXPLAIN:
                sqlTag = TAG_EXPLAIN;
                factory = cq.getRecordCursorFactory();
                tas = new TypesAndSelect(
                        this.factory,
                        sqlType,
                        TAG_EXPLAIN,
                        msgParseParameterTypeOIDs,
                        outParameterTypeDescriptionTypes
                );
                break;
            case CompiledQuery.SELECT:
                sqlTag = TAG_SELECT;
                factory = cq.getRecordCursorFactory();
                tas = new TypesAndSelect(
                        factory,
                        sqlType,
                        sqlTag,
                        msgParseParameterTypeOIDs,
                        outParameterTypeDescriptionTypes
                );
                break;
            case CompiledQuery.PSEUDO_SELECT:
                // the PSEUDO_SELECT comes from a "copy" SQL, which is why
                // we do not intend to cache it. The fact we don't have
                // TypesAndSelect instance here should be enough to tell the
                // system not to cache.
                sqlTag = TAG_PSEUDO_SELECT;
                factory = cq.getRecordCursorFactory();
                break;
            case CompiledQuery.INSERT:
            case CompiledQuery.INSERT_AS_SELECT:
                final InsertOperation insertOp = cq.popInsertOperation();
                tai = taiPool.pop();
                sqlTag = sqlType == CompiledQuery.INSERT ? TAG_INSERT : TAG_INSERT_AS_SELECT;
                tai.of(
                        insertOp,
                        sqlType,
                        sqlTag,
                        msgParseParameterTypeOIDs,
                        outParameterTypeDescriptionTypes
                );
                break;
            case CompiledQuery.UPDATE:
                // copy contents of the mutable CompiledQuery into our cache
                String sqlText = cq.getSqlText();
                UpdateOperation updateOperation = cq.getUpdateOperation();
                updateOperation.withSqlStatement(sqlText);
                ensureCompiledQuery();
                compiledQuery.ofUpdate(updateOperation);
                compiledQuery.withSqlText(sqlText);
                sqlTag = TAG_UPDATE;
                break;
            case CompiledQuery.SET:
                sqlTag = TAG_SET;
                break;
            case CompiledQuery.DEALLOCATE:
                utf8StringSink.clear();
                utf8StringSink.put(cq.getStatementName());
                this.preparedStatementNameToDeallocate = utf8StringSink;
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
                sqlTag = TAG_ALTER_ROLE;
                break;
            case CompiledQuery.CREATE_USER:
                sqlTag = TAG_CREATE_ROLE;
                break;
            case CompiledQuery.ALTER:
                // future-proofing ALTER execution
                ensureCompiledQuery();
                compiledQuery.ofAlter(AlterOperation.deepCloneOf(cq.getAlterOperation()));
                compiledQuery.withSqlText(cq.getSqlText());
                sqlTag = TAG_OK;
                break;
            default:
                // DDL
                sqlTag = TAG_OK;
                break;
        }
        sqlTextHasSecret = sqlExecutionContext.containsSecret();
        stateParseExecuted = cq.executedAtParseTime();
    }

    // Returns false if column size is known to be the same in both text and binary formats.
    // Note: certain column types, e.g. LONG256, don't have a matching column type in Postgres,
    //       so we always serialize them in text format, we return false for that and true for everything else
    private boolean txtAndBinSizesCanBeDifferent(int columnType) {
        final int typeTag = ColumnType.tagOf(columnType);
        return !ColumnType.isVarSize(typeTag)
                && !ColumnType.isGeoHash(columnType)
                && typeTag != ColumnType.BOOLEAN
                && typeTag != ColumnType.CHAR
                && typeTag != ColumnType.IPv4
                && typeTag != ColumnType.LONG256
                && typeTag != ColumnType.SYMBOL;
    }

    private void validatePgResultSetColumnTypesAndNames() throws PGMessageProcessingException {
        if (factory == null) {
            return;
        }
        final RecordMetadata currentMetadata = factory.getMetadata();
        final int currentColumnCount = currentMetadata.getColumnCount();

        int cachedColumnCount = pgResultSetColumnNames.size();
        if (cachedColumnCount == 0) {
            // this is the first time we are setting up the result set
            // we can just copy the column types and names from factory, no need to validate
            assert pgResultSetColumnTypes.size() == 0;
            copyPgResultSetColumnTypesAndNames();
            return;
        }

        // we have a result set already, we need to validate that the new result set matches the old one
        if (cachedColumnCount != currentColumnCount) {
            stalePlanError = true;
            error = true;
            throw kaput().put("cached plan must not change result type");
        }

        for (int i = 0; i < currentColumnCount; i++) {
            final int currentColumnType = currentMetadata.getColumnType(i);
            int currentPgColumnType = PGOids.getTypeOid(ColumnType.isNull(currentColumnType) ? ColumnType.STRING : currentColumnType);

            int cachedColumnType = pgResultSetColumnTypes.getQuick(2 * i);
            int cachedPgColumnType = PGOids.getTypeOid(ColumnType.isNull(cachedColumnType) ? ColumnType.STRING : cachedColumnType);

            if (currentPgColumnType != cachedPgColumnType) {
                stalePlanError = true;
                error = true;
                throw kaput().put("cached plan must not change result type");
            }

            String currentColumnName = currentMetadata.getColumnName(i);
            String cachedColumnName = pgResultSetColumnNames.getQuick(i);

            if (!Chars.equals(currentColumnName, cachedColumnName)) {
                stalePlanError = true;
                error = true;
                throw kaput().put("cached plan must not change result type");
            }

            // we still override the column type with the current one, because even if the column types have the same
            // pgwire representation, the questdb type might still be different, and they have to be fetched differently.
            // example: VARCHAR and SYMBOL. They are both represented as TEXT in pgwire, but they are fetched differently
            // from questdb record
            pgResultSetColumnTypes.setQuick(2 * i, currentColumnType);
            pgResultSetColumnTypes.setQuick(2 * i + 1, GeoHashes.getBitFlags(currentColumnType));
        }
    }

    void clearState() {
        error = false;
        stalePlanError = false;
        stateSync = SYNC_PARSE;
        stateParse = false;
        stateBind = false;
        stateDesc = SYNC_DESC_NONE;
        stateExec = false;
        stateClosed = false;
        arrayViewPool.clear();
    }

    void copyStateFrom(PGPipelineEntry that) {
        stateParse = that.stateParse;
        stateBind = that.stateBind;
        stateDesc = that.stateDesc;
        stateExec = that.stateExec;
        stateClosed = that.stateClosed;
    }

    boolean isDirty() {
        return error || stateSync != SYNC_PARSE || stateParse || stateBind || stateDesc != SYNC_DESC_NONE || stateExec || stateClosed;
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
        final IntList cachedTypes = typeContainer.getPgInParameterTypeOIDs();
        final int cachedTypeCount = cachedTypes.size();
        if (parameterTypeCount != cachedTypeCount) {
            return false;
        }

        // both BindVariableService and the "typeContainer" have parameter types
        // we have to allow the possibility that parameter types between the
        // cache and the "parse" message could be different. If they are,
        // we have to discard the cache and re-compile the SQL text
        for (int i = 0; i < cachedTypeCount; i++) {
            if (cachedTypes.getQuick(i) != msgParseParameterTypeOIDs.getQuick(i)) {
                return false;
            }
        }
        return true;
    }

    boolean msgParseReconcileParameterTypes(TypeContainer typeContainer) {
        assert msgParseParameterTypeOIDs.size() <= Short.MAX_VALUE;
        return msgParseReconcileParameterTypes((short) msgParseParameterTypeOIDs.size(), typeContainer);
    }
}
