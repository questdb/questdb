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

import io.questdb.MessageBus;
import io.questdb.PropServerConfiguration;
import io.questdb.TelemetryOrigin;
import io.questdb.TelemetrySystemEvent;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexBuilder;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.MapWriter;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableNameRegistry;
import io.questdb.cairo.TableNameRegistryStore;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.VacuumColumnVersions;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewGraph;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriterMetadata;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.engine.ops.CopyCancelFactory;
import io.questdb.griffin.engine.ops.CopyExportFactory;
import io.questdb.griffin.engine.ops.CopyImportFactory;
import io.questdb.griffin.engine.ops.CreateMatViewOperation;
import io.questdb.griffin.engine.ops.CreateMatViewOperationBuilder;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilder;
import io.questdb.griffin.engine.ops.DropAllOperation;
import io.questdb.griffin.engine.ops.GenericDropOperation;
import io.questdb.griffin.engine.ops.GenericDropOperationBuilder;
import io.questdb.griffin.engine.ops.InsertAsSelectOperationImpl;
import io.questdb.griffin.engine.ops.InsertOperationImpl;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExplainModel;
import io.questdb.griffin.model.ExportModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.InsertModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.RenameTableModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseAsciiCharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Transient;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.TableUtils.TABLE_KIND_REGULAR_TABLE;
import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.griffin.model.ExportModel.COPY_TYPE_FROM;
import static io.questdb.std.GenericLexer.unquote;

public class SqlCompilerImpl implements SqlCompiler, Closeable, SqlParserCallback {
    public static final String ALTER_TABLE_EXPECTED_TOKEN_DESCR =
            "'add', 'alter', 'attach', 'detach', 'drop', 'convert', 'resume', 'rename', 'set' or 'squash'";
    static final ObjList<String> sqlControlSymbols = new ObjList<>(8);
    // null object used to skip null checks in batch method
    private static final BatchCallback EMPTY_CALLBACK = new BatchCallback() {
        @Override
        public void postCompile(SqlCompiler compiler, CompiledQuery cq, CharSequence queryText) {
        }

        @Override
        public boolean preCompile(SqlCompiler compiler, CharSequence sqlText) {
            return true;
        }
    };
    private static final boolean[][] columnConversionSupport = new boolean[ColumnType.NULL][ColumnType.NULL];
    private static final Log LOG = LogFactory.getLog(SqlCompilerImpl.class);
    protected final AlterOperationBuilder alterOperationBuilder;
    protected final SqlCodeGenerator codeGenerator;
    protected final CompiledQueryImpl compiledQuery;
    protected final CairoConfiguration configuration;
    protected final CairoEngine engine;
    protected final LowerCaseAsciiCharSequenceObjHashMap<KeywordBasedExecutor> keywordBasedExecutors = new LowerCaseAsciiCharSequenceObjHashMap<>();
    protected final GenericLexer lexer;
    protected final SqlOptimiser optimiser;
    protected final Path path;
    protected final QueryRegistry queryRegistry;
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final DatabaseBackupAgent backupAgent;
    private final BlockFileWriter blockFileWriter;
    private final CharacterStore characterStore;
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final CharSequenceObjHashMap<String> dropAllTablesFailedTableNames = new CharSequenceObjHashMap<>();
    private final GenericDropOperationBuilder dropOperationBuilder;
    private final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
    private final FilesFacade ff;
    private final FunctionParser functionParser;
    private final ListColumnFilter listColumnFilter = new ListColumnFilter();
    private final int maxRecompileAttempts;
    private final MemoryMARW mem = Vm.getCMARWInstance();
    private final MessageBus messageBus;
    private final SqlParser parser;
    private final TimestampValueRecord partitionFunctionRec = new TimestampValueRecord();
    private final QueryBuilder queryBuilder;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final Path renamePath;
    private final ObjectPool<ExpressionNode> sqlNodePool;
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    private final ObjList<TableWriterAPI> tableWriters = new ObjList<>();
    private final VacuumColumnVersions vacuumColumnVersions;
    protected CharSequence sqlText;
    private boolean closed = false;
    // Helper var used to pass back count in cases it can't be done via method result.
    private long insertCount;
    //determines how compiler parses query text
    //true - compiler treats whole input as single query and doesn't stop on ';'. Default mode.
    //false - compiler treats input as list of statements and stops processing statement on ';'. Used in batch processing.
    private boolean isSingleQueryMode = true;

    public SqlCompilerImpl(CairoEngine engine) {
        try {
            this.path = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
            this.renamePath = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
            this.engine = engine;
            this.maxRecompileAttempts = engine.getConfiguration().getMaxSqlRecompileAttempts();
            this.queryBuilder = new QueryBuilder(this);
            this.configuration = engine.getConfiguration();
            this.ff = configuration.getFilesFacade();
            this.messageBus = engine.getMessageBus();
            this.sqlNodePool = new ObjectPool<>(ExpressionNode.FACTORY, configuration.getSqlExpressionPoolCapacity());
            this.queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, configuration.getSqlColumnPoolCapacity());
            this.queryModelPool = new ObjectPool<>(QueryModel.FACTORY, configuration.getSqlModelPoolCapacity());
            this.compiledQuery = new CompiledQueryImpl(engine);
            this.characterStore = new CharacterStore(
                    configuration.getSqlCharacterStoreCapacity(),
                    configuration.getSqlCharacterStoreSequencePoolCapacity()
            );

            this.lexer = new GenericLexer(configuration.getSqlLexerPoolCapacity());
            this.functionParser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            this.codeGenerator = new SqlCodeGenerator(configuration, functionParser, sqlNodePool);
            this.vacuumColumnVersions = new VacuumColumnVersions(engine);

            // we have cyclical dependency here
            functionParser.setSqlCodeGenerator(codeGenerator);

            this.backupAgent = new DatabaseBackupAgent();

            registerKeywordBasedExecutors();

            configureLexer(lexer);

            final PostOrderTreeTraversalAlgo postOrderTreeTraversalAlgo = new PostOrderTreeTraversalAlgo();

            optimiser = newSqlOptimiser(
                    configuration,
                    characterStore,
                    sqlNodePool,
                    queryColumnPool,
                    queryModelPool,
                    postOrderTreeTraversalAlgo,
                    functionParser,
                    path
            );

            parser = new SqlParser(
                    configuration,
                    characterStore,
                    sqlNodePool,
                    queryColumnPool,
                    queryModelPool,
                    postOrderTreeTraversalAlgo
            );

            alterOperationBuilder = new AlterOperationBuilder();
            dropOperationBuilder = new GenericDropOperationBuilder();
            queryRegistry = engine.getQueryRegistry();
            blockFileWriter = new BlockFileWriter(ff, configuration.getCommitMode());
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static long copyOrderedBatched(
            SqlExecutionContext context,
            TableWriterAPI writer,
            RecordMetadata metadata,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long o3MaxLag
    ) {
        return copyOrderedBatched(context, writer, metadata, cursor, copier, cursorTimestampIndex, batchSize, o3MaxLag, CopyDataProgressReporter.NOOP, -1);
    }

    public static long copyOrderedBatched(
            SqlExecutionContext context,
            TableWriterAPI writer,
            RecordMetadata metadata,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long o3MaxLag,
            CopyDataProgressReporter reporter,
            int reportFrequency
    ) {
        long rowCount;
        int timestampColumnType = metadata.getColumnType(cursorTimestampIndex);
        if (ColumnType.isSymbolOrString(timestampColumnType)) {
            rowCount = copyOrderedBatchedStrTimestamp(context, writer, cursor, copier, cursorTimestampIndex, batchSize, o3MaxLag, reporter);
        } else if (metadata.getColumnType(cursorTimestampIndex) == ColumnType.VARCHAR) {
            rowCount = copyOrderedBatchedVarcharTimestamp(context, writer, cursor, copier, cursorTimestampIndex, batchSize, o3MaxLag, reporter);
        } else {
            rowCount = copyOrderedBatched0(context, writer, cursor, copier, timestampColumnType, cursorTimestampIndex, batchSize, o3MaxLag, reporter, reportFrequency);
        }
        return rowCount;
    }

    public static long copyUnordered(
            SqlExecutionContext context,
            RecordCursor cursor,
            TableWriterAPI writer,
            RecordToRowCopier copier
    ) {
        return copyUnordered(context, cursor, writer, copier, CopyDataProgressReporter.NOOP, -1);
    }

    /**
     * Returns number of copied rows.
     */
    public static long copyUnordered(
            SqlExecutionContext context,
            RecordCursor cursor,
            TableWriterAPI writer,
            RecordToRowCopier copier,
            CopyDataProgressReporter reporter,
            int reportFrequency
    ) {
        long rowCount = 0;
        final Record record = cursor.getRecord();
        reporter.onProgress(CopyDataProgressReporter.Stage.Start, cursor.size());
        while (cursor.hasNext()) {
            context.getCircuitBreaker().statefulThrowExceptionIfTripped();
            TableWriter.Row row = writer.newRow();
            copier.copy(context, record, row);
            row.append();
            rowCount++;
            if (reportFrequency > 0 && rowCount % reportFrequency == 0) {
                reporter.onProgress(CopyDataProgressReporter.Stage.Inserting, rowCount);
            }
        }
        reporter.onProgress(CopyDataProgressReporter.Stage.Finish, rowCount);
        return rowCount;
    }

    // public for testing
    public static void expectKeyword(GenericLexer lexer, CharSequence keyword) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put('\'').put(keyword).put("' expected");
        }
        if (!Chars.equalsLowerCaseAscii(tok, keyword)) {
            throw SqlException.position(lexer.lastTokenPosition()).put('\'').put(keyword).put("' expected");
        }
    }

    @Override
    public void clear() {
        clearExceptSqlText();
        sqlText = null;
    }

    @Override
    public void close() {
        if (closed) {
            throw new IllegalStateException("close was already called");
        }
        closed = true;
        Misc.free(backupAgent);
        Misc.free(vacuumColumnVersions);
        Misc.free(path);
        Misc.free(renamePath);
        Misc.free(codeGenerator);
        Misc.free(mem);
        Misc.freeObjList(tableWriters);
        Misc.free(blockFileWriter);
    }

    @Override
    @NotNull
    public CompiledQuery compile(@NotNull CharSequence sqlText, @NotNull SqlExecutionContext executionContext) throws SqlException {
        clear();
        // these are quick executions that do not require building of a model
        lexer.of(sqlText);
        isSingleQueryMode = true;

        compileInner(executionContext, sqlText, true);
        return compiledQuery;
    }

    /**
     * Allows processing of batches of sql statements (sql scripts) separated by ';' .
     * Each query is processed in sequence and processing stops on first error and whole batch gets discarded.
     * Noteworthy difference between this and 'normal' query is that all empty queries get ignored, e.g.
     * <br>
     * select 1;<br>
     * ; ;/* comment \*\/;--comment\n; - these get ignored <br>
     * update a set b=c  ; <br>
     * <p>
     * Useful PG doc link :
     *
     * @param batchText        - block of queries to process
     * @param executionContext - SQL execution context
     * @param batchCallback    - callback to perform actions prior to or after batch part compilation, e.g. clear caches or execute command
     * @throws SqlException              - in case of syntax error
     * @throws PeerDisconnectedException - when peer is disconnected
     * @throws PeerIsSlowToReadException - when peer is too slow
     * @throws QueryPausedException      - when query is paused
     * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4">PostgreSQL documentation</a>
     */
    @Override
    public void compileBatch(
            @NotNull CharSequence batchText,
            @NotNull SqlExecutionContext executionContext,
            BatchCallback batchCallback
    ) throws Exception {
        clear();
        lexer.of(batchText);
        isSingleQueryMode = false;

        if (batchCallback == null) {
            batchCallback = EMPTY_CALLBACK;
        }

        int position;

        while (lexer.hasNext()) {
            // skip over empty statements that'd cause error in parser
            position = getNextValidTokenPosition();
            if (position == -1) {
                return;
            }

            boolean recompileStale = true;
            for (int retries = 0; recompileStale; retries++) {
                clear(); // we don't use normal compile here because we can't reset existing lexer

                // Fetch sqlText, this will move lexer pointer (state change).
                // We try to avoid logging the entire sql batch, in case batch contains secrets
                final CharSequence sqlText = batchText.subSequence(position, goToQueryEnd());

                if (batchCallback.preCompile(this, sqlText)) {
                    // ok, the callback wants us to compile this query, let's go!

                    // re-position lexer pointer to where sqlText just began
                    lexer.backTo(position, null);
                    compileInner(executionContext, sqlText, true);

                    // consume residual text, such as semicolon
                    goToQueryEnd();
                    // We've to move lexer because some query handlers don't consume all tokens (e.g. SET )
                    // some code in postCompile might need full text of current query
                    try {
                        batchCallback.postCompile(this, compiledQuery, sqlText);
                        recompileStale = false;
                    } catch (TableReferenceOutOfDateException e) {
                        if (retries == maxRecompileAttempts) {
                            throw e;
                        }
                        LOG.info().$safe(e.getFlyweightMessage()).$();
                        // will recompile
                        lexer.restart();
                    }
                } else {
                    recompileStale = false;
                }
            }
        }
    }

    @Override
    public void execute(final Operation op, SqlExecutionContext executionContext) throws SqlException {
        switch (op.getOperationCode()) {
            case OperationCodes.CREATE_TABLE:
                executeCreateTable((CreateTableOperation) op, executionContext);
                break;
            case OperationCodes.CREATE_MAT_VIEW:
                executeCreateMatView((CreateMatViewOperation) op, executionContext);
                break;
            case OperationCodes.DROP_TABLE:
                executeDropTable((GenericDropOperation) op, executionContext);
                break;
            case OperationCodes.DROP_MAT_VIEW:
                executeDropMatView((GenericDropOperation) op, executionContext);
                break;
            case OperationCodes.DROP_ALL:
                executeDropAllTables(executionContext);
                break;
            default:
                throw SqlException.position(0).put("Unsupported operation [code=").put(op.getOperationCode()).put(']');
        }
    }

    @Override
    public RecordCursorFactory generateSelectWithRetries(
            @Transient QueryModel initialQueryModel,
            @Nullable @Transient InsertModel insertModel,
            @Transient SqlExecutionContext executionContext,
            boolean generateProgressLogger
    ) throws SqlException {
        QueryModel queryModel = initialQueryModel;
        int remainingRetries = maxRecompileAttempts;
        for (; ; ) {
            try {
                return generateSelectOneShot(queryModel, executionContext, generateProgressLogger);
            } catch (TableReferenceOutOfDateException e) {
                if (--remainingRetries < 0) {
                    throw SqlException.position(0).put("too many ").put(e.getFlyweightMessage());
                }
                LOG.info().$("retrying plan [q=`").$(queryModel).$("`, fd=").$(executionContext.getRequestFd()).I$();
                clearExceptSqlText();
                lexer.restart();
                if (insertModel != null) {
                    queryModel = compileExecutionModel(executionContext).getQueryModel();
                    insertModel.setQueryModel(queryModel);
                } else {
                    queryModel = (QueryModel) compileExecutionModel(executionContext);
                }
            }
        }
    }

    @Override
    public BytecodeAssembler getAsm() {
        return asm;
    }

    @Override
    public CairoEngine getEngine() {
        return engine;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return functionParser.getFunctionFactoryCache();
    }

    @Override
    public QueryBuilder query() {
        queryBuilder.clear();
        return queryBuilder;
    }

    @TestOnly
    @Override
    public void setEnableJitNullChecks(boolean value) {
        codeGenerator.setEnableJitNullChecks(value);
    }

    @TestOnly
    @Override
    public void setFullFatJoins(boolean value) {
        codeGenerator.setFullFatJoins(value);
    }

    @TestOnly
    @Override
    public ExecutionModel testCompileModel(CharSequence sqlText, SqlExecutionContext executionContext) throws SqlException {
        clear();
        lexer.of(sqlText);
        return compileExecutionModel(executionContext);
    }

    @TestOnly
    @Override
    public ExpressionNode testParseExpression(CharSequence expression, QueryModel model) throws SqlException {
        clear();
        lexer.of(expression);
        return parser.expr(lexer, model, this);
    }

    // test only
    @TestOnly
    @Override
    public void testParseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException {
        clear();
        lexer.of(expression);
        parser.expr(lexer, listener, this);
    }

    private static void addSupportedConversion(short fromType, short... toTypes) {
        for (short toType : toTypes) {
            columnConversionSupport[fromType][toType] = true;
            // Make it symmetrical
            columnConversionSupport[toType][fromType] = true;
        }
    }

    private static void configureLexer(GenericLexer lexer) {
        for (int i = 0, k = sqlControlSymbols.size(); i < k; i++) {
            lexer.defineSymbol(sqlControlSymbols.getQuick(i));
        }
        // note: it's safe to take any registry (new or old) because we don't use precedence here
        OperatorRegistry registry = OperatorExpression.getRegistry();
        for (int i = 0, k = registry.operators.size(); i < k; i++) {
            OperatorExpression op = registry.operators.getQuick(i);
            if (op.symbol) {
                lexer.defineSymbol(op.operator.token);
            }
        }
    }

    // returns number of copied rows
    private static long copyOrderedBatched0(
            SqlExecutionContext context,
            TableWriterAPI writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int fromTimestampType,
            int cursorTimestampIndex,
            long batchSize,
            long o3MaxLag,
            CopyDataProgressReporter reporter,
            int reportFrequency
    ) {
        long commitTarget = batchSize;
        long rowCount = 0;
        final Record record = cursor.getRecord();
        reporter.onProgress(CopyDataProgressReporter.Stage.Start, cursor.size());
        CommonUtils.TimestampUnitConverter converter = ColumnType.getTimestampDriver(writer.getMetadata().getTimestampType()).getTimestampUnitConverter(fromTimestampType);
        if (converter == null) {
            while (cursor.hasNext()) {
                context.getCircuitBreaker().statefulThrowExceptionIfTripped();
                TableWriter.Row row = writer.newRow(record.getTimestamp(cursorTimestampIndex));
                copier.copy(context, record, row);
                row.append();
                if (++rowCount >= commitTarget) {
                    writer.ic(o3MaxLag);
                    commitTarget = rowCount + batchSize;
                }
                if (reportFrequency > 0 && rowCount % reportFrequency == 0) {
                    reporter.onProgress(CopyDataProgressReporter.Stage.Inserting, rowCount);
                }
            }
        } else {
            while (cursor.hasNext()) {
                context.getCircuitBreaker().statefulThrowExceptionIfTripped();
                TableWriter.Row row = writer.newRow(converter.convert(record.getTimestamp(cursorTimestampIndex)));
                copier.copy(context, record, row);
                row.append();
                if (++rowCount >= commitTarget) {
                    writer.ic(o3MaxLag);
                    commitTarget = rowCount + batchSize;
                }
                if (reportFrequency > 0 && rowCount % reportFrequency == 0) {
                    reporter.onProgress(CopyDataProgressReporter.Stage.Inserting, rowCount);
                }
            }
        }
        reporter.onProgress(CopyDataProgressReporter.Stage.Finish, rowCount);

        return rowCount;
    }

    // returns number of copied rows
    private static long copyOrderedBatchedStrTimestamp(
            SqlExecutionContext context,
            TableWriterAPI writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long o3MaxLag,
            CopyDataProgressReporter reporter
    ) {
        long commitTarget = batchSize;
        long rowCount = 0;
        final Record record = cursor.getRecord();
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(writer.getMetadata().getTimestampType());
        reporter.onProgress(CopyDataProgressReporter.Stage.Start, cursor.size());
        while (cursor.hasNext()) {
            context.getCircuitBreaker().statefulThrowExceptionIfTripped();
            // It's allowed to insert ISO formatted string to timestamp column
            TableWriter.Row row = writer.newRow(timestampDriver.implicitCast(record.getStrA(cursorTimestampIndex)));
            copier.copy(context, record, row);
            row.append();
            if (++rowCount >= commitTarget) {
                writer.ic(o3MaxLag);
                commitTarget = rowCount + batchSize;
            }
            if (rowCount % 50000 == 0) {
                reporter.onProgress(CopyDataProgressReporter.Stage.Inserting, rowCount);
            }
        }
        reporter.onProgress(CopyDataProgressReporter.Stage.Finish, rowCount);

        return rowCount;
    }

    // returns number of copied rows
    private static long copyOrderedBatchedVarcharTimestamp(
            SqlExecutionContext context,
            TableWriterAPI writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long o3MaxLag,
            CopyDataProgressReporter reporter
    ) {
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(writer.getMetadata().getTimestampType());
        long commitTarget = batchSize;
        long rowCount = 0;
        final Record record = cursor.getRecord();
        reporter.onProgress(CopyDataProgressReporter.Stage.Start, cursor.size());
        while (cursor.hasNext()) {
            context.getCircuitBreaker().statefulThrowExceptionIfTripped();
            // It's allowed to insert ISO formatted string to timestamp column
            TableWriter.Row row = writer.newRow(timestampDriver.implicitCastVarchar(record.getVarcharA(cursorTimestampIndex)));
            copier.copy(context, record, row);
            row.append();
            if (++rowCount >= commitTarget) {
                writer.ic(o3MaxLag);
                commitTarget = rowCount + batchSize;
            }
            if (rowCount % 50000 == 0) {
                reporter.onProgress(CopyDataProgressReporter.Stage.Inserting, rowCount);
            }
        }
        reporter.onProgress(CopyDataProgressReporter.Stage.Finish, rowCount);
        return rowCount;
    }

    private static int estimateIndexValueBlockSizeFromReader(CairoConfiguration configuration, SqlExecutionContext executionContext, TableToken matViewToken, int columnIndex) {
        final int indexValueBlockSize;
        try (TableReader reader = executionContext.getReader(matViewToken)) {
            int symbolCount = reader.getSymbolMapReader(columnIndex).getSymbolCount();
            if (reader.getPartitionCount() == 0 || symbolCount == 0) {
                // No data to estimate accurately, fall back to default.
                return Numbers.ceilPow2(configuration.getIndexValueBlockSize());
            }
            // we are looking to estimate how many rowids we will need to store for each
            // symbol per partition. To do that wee are assuming the following formula:
            // max(2, table_row_count / table_partition_count / symbol_count / 4)
            // here:
            // 2 being the minimum number of row IDs per symbol
            // div by 4 - we are looking to have 4 chained block for an average symbol to make sure we
            // do not create overly-sparse index for symbols with row count below average.
            long v = Math.max(2, reader.size() / reader.getPartitionCount() / symbolCount / 4);
            if ((int) v != v) {
                // overflow, assign sensible default (large)
                indexValueBlockSize = 50_000_000;
            } else {
                indexValueBlockSize = (int) v;
            }
        }
        return indexValueBlockSize;
    }

    private static boolean isIPv4UpdateCast(int from, int to) {
        return (from == ColumnType.STRING && to == ColumnType.IPv4)
                || (from == ColumnType.IPv4 && to == ColumnType.STRING)
                || (from == ColumnType.VARCHAR && to == ColumnType.IPv4)
                || (from == ColumnType.IPv4 && to == ColumnType.VARCHAR);
    }

    private static boolean isTimestampUpdateCast(int from, int to) {
        return ColumnType.isTimestamp(to) && ColumnType.isAssignableFrom(from, to);
    }

    private int addColumnWithType(
            AlterOperationBuilder addColumn,
            CharSequence columnName,
            int columnNamePosition
    ) throws SqlException {
        CharSequence tok;
        tok = expectToken(lexer, "column type");

        int columnType = SqlUtil.toPersistedType(tok, lexer.lastTokenPosition());
        int typePosition = lexer.lastTokenPosition();

        int dim = SqlUtil.parseArrayDimensionality(lexer, columnType, typePosition);
        if (dim > 0) {
            if (!ColumnType.isSupportedArrayElementType(columnType)) {
                throw SqlException.position(typePosition)
                        .put("unsupported array element type [type=")
                        .put(ColumnType.nameOf(columnType))
                        .put(']');
            }
            columnType = ColumnType.encodeArrayType(ColumnType.tagOf(columnType), dim);
        }

        if (columnType == ColumnType.DECIMAL) {
            columnType = SqlParser.parseDecimalColumnType(lexer);
        }

        tok = SqlUtil.fetchNext(lexer);

        // check for an unmatched bracket
        if (tok != null && Chars.equals(tok, ']')) {
            throw SqlException.position(typePosition).put(columnName).put(" has an unmatched `]` - were you trying to define an array?");
        } else {
            lexer.unparseLast();
        }

        if (columnType == ColumnType.GEOHASH) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || tok.charAt(0) != '(') {
                throw SqlException.position(lexer.getPosition()).put("missing GEOHASH precision");
            }

            tok = SqlUtil.fetchNext(lexer);
            if (tok != null && tok.charAt(0) != ')') {
                int geoHashBits = GeoHashUtil.parseGeoHashBits(lexer.lastTokenPosition(), 0, tok);
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || tok.charAt(0) != ')') {
                    if (tok != null) {
                        throw SqlException.position(lexer.lastTokenPosition())
                                .put("invalid GEOHASH type literal, expected ')'")
                                .put(" found='").put(tok.charAt(0)).put("'");
                    }
                    throw SqlException.position(lexer.getPosition())
                            .put("invalid GEOHASH type literal, expected ')'");
                }
                columnType = ColumnType.getGeoHashTypeWithBits(geoHashBits);
            } else {
                throw SqlException.position(lexer.lastTokenPosition())
                        .put("missing GEOHASH precision");
            }
        }

        tok = SqlUtil.fetchNext(lexer);
        final int indexValueBlockCapacity;
        final boolean cache;
        int symbolCapacity;
        final boolean indexed;

        if (
                ColumnType.isSymbol(columnType)
                        && tok != null
                        && !Chars.equals(tok, ',')
                        && !Chars.equals(tok, ';')
        ) {
            if (isCapacityKeyword(tok)) {
                tok = expectToken(lexer, "symbol capacity");

                final boolean negative;
                final int errorPos = lexer.lastTokenPosition();
                if (Chars.equals(tok, '-')) {
                    negative = true;
                    tok = expectToken(lexer, "symbol capacity");
                } else {
                    negative = false;
                }

                try {
                    symbolCapacity = Numbers.parseInt(tok);
                } catch (NumericException e) {
                    throw SqlException.$(lexer.lastTokenPosition(), "numeric capacity expected");
                }

                if (negative) {
                    symbolCapacity = -symbolCapacity;
                }

                TableUtils.validateSymbolCapacity(errorPos, symbolCapacity);

                tok = SqlUtil.fetchNext(lexer);
            } else {
                symbolCapacity = configuration.getDefaultSymbolCapacity();
            }

            if (Chars.equalsLowerCaseAsciiNc("cache", tok)) {
                cache = true;
                tok = SqlUtil.fetchNext(lexer);
            } else if (Chars.equalsLowerCaseAsciiNc("nocache", tok)) {
                cache = false;
                tok = SqlUtil.fetchNext(lexer);
            } else {
                cache = configuration.getDefaultSymbolCacheFlag();
            }

            TableUtils.validateSymbolCapacityCached(cache, symbolCapacity, lexer.lastTokenPosition());

            indexed = Chars.equalsLowerCaseAsciiNc("index", tok);
            if (indexed) {
                tok = SqlUtil.fetchNext(lexer);
            }

            if (Chars.equalsLowerCaseAsciiNc("capacity", tok)) {
                tok = expectToken(lexer, "symbol index capacity");

                try {
                    indexValueBlockCapacity = Numbers.parseInt(tok);
                } catch (NumericException e) {
                    throw SqlException.$(lexer.lastTokenPosition(), "numeric capacity expected");
                }
                SqlUtil.fetchNext(lexer);
            } else {
                indexValueBlockCapacity = configuration.getIndexValueBlockSize();
            }
        } else { // set defaults
            // ignore `NULL` and `NOT NULL`
            if (tok != null && isNotKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);
            }

            if (tok != null && isNullKeyword(tok)) {
                SqlUtil.fetchNext(lexer);
            }

            cache = configuration.getDefaultSymbolCacheFlag();
            indexValueBlockCapacity = configuration.getIndexValueBlockSize();
            symbolCapacity = configuration.getDefaultSymbolCapacity();
            indexed = false;
        }

        addColumn.addColumnToList(
                columnName,
                columnNamePosition,
                columnType,
                Numbers.ceilPow2(symbolCapacity),
                cache,
                indexed,
                Numbers.ceilPow2(indexValueBlockCapacity),
                false
        );
        lexer.unparseLast();
        return columnType;
    }

    private void alterTableAddColumn(
            SecurityContext securityContext,
            int tableNamePosition,
            TableToken tableToken,
            TableRecordMetadata tableMetadata
    ) throws SqlException {
        // add columns to table
        CharSequence tok = SqlUtil.fetchNext(lexer);

        // ignore `column`
        if (tok != null && !isColumnKeyword(tok)) {
            lexer.unparseLast();
        }

        AlterOperationBuilder addColumn = alterOperationBuilder.ofAddColumn(
                tableNamePosition,
                tableToken,
                tableMetadata.getTableId()
        );

        int semicolonPos = -1;
        do {
            tok = maybeExpectToken(lexer, "'column' or column name", semicolonPos < 0);

            if (semicolonPos >= 0) {
                if (tok != null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
                }
                break;
            }

            if (isIfKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && isNotKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok != null && isExistsKeyword(tok)) {
                        tok = SqlUtil.fetchNext(lexer); // captured column name
                        final int columnIndex = tableMetadata.getColumnIndexQuiet(tok);
                        if (columnIndex != -1) {
                            tok = expectToken(lexer, "column type");
                            final int columnType = ColumnType.typeOf(tok);
                            if (columnType == -1) {
                                throw SqlException.$(lexer.lastTokenPosition(), "unrecognized column type: ").put(tok);
                            }
                            final int existingType = tableMetadata.getColumnType(columnIndex);
                            if (existingType != columnType) {
                                throw SqlException
                                        .$(lexer.lastTokenPosition(), "column already exists with a different column type [current type=")
                                        .put(ColumnType.nameOf(existingType))
                                        .put(", requested type=")
                                        .put(ColumnType.nameOf(columnType))
                                        .put(']');
                            }
                            break;
                        }
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "unexpected token '").put(tok)
                                .put("' for if not exists");
                    }
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'not' expected");
                }
            } else {
                int index = tableMetadata.getColumnIndexQuiet(tok);
                if (index != -1) {
                    throw SqlException.$(lexer.lastTokenPosition(), "column '").put(tok).put("' already exists");
                }
            }

            CharSequence columnName = GenericLexer.immutableOf(unquote(tok));
            int columnNamePosition = lexer.lastTokenPosition();

            if (!TableUtils.isValidColumnName(columnName, configuration.getMaxFileNameLength())) {
                throw SqlException.$(lexer.lastTokenPosition(), " new column name contains invalid characters");
            }

            addColumnWithType(addColumn, columnName, columnNamePosition);
            tok = SqlUtil.fetchNext(lexer);

            if (tok == null || (!isSingleQueryMode && isSemicolon(tok))) {
                break;
            }

            semicolonPos = Chars.equals(tok, ';') ? lexer.lastTokenPosition() : -1;
            if (semicolonPos < 0 && !Chars.equals(tok, ',')) {
                addColumnSuffix(securityContext, tok, tableToken, alterOperationBuilder);
                compiledQuery.ofAlter(alterOperationBuilder.build());
                return;
            }
        } while (true);

        addColumnSuffix(securityContext, null, tableToken, alterOperationBuilder);
        compiledQuery.ofAlter(alterOperationBuilder.build());
        if (alterOperationBuilder.getExtraStrInfo().size() == 0) {
            // there is no column to add, set the done flag to avoid execution of the query
            compiledQuery.done();
        }
    }

    private void alterTableChangeColumnType(
            SecurityContext securityContext,
            int tableNamePosition,
            TableToken tableToken,
            int columnNamePosition,
            CharSequence columnName,
            TableRecordMetadata tableMetadata,
            int columnIndex
    ) throws SqlException {
        AlterOperationBuilder changeColumn = alterOperationBuilder.ofColumnChangeType(
                tableNamePosition,
                tableToken,
                tableMetadata.getTableId()
        );
        int existingColumnType = tableMetadata.getColumnType(columnIndex);
        int newColumnType = addColumnWithType(changeColumn, columnName, columnNamePosition);
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok != null && !isSemicolon(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [").put(tok).put("] while trying to change column type");
        }
        if (columnIndex == tableMetadata.getTimestampIndex()) {
            throw SqlException.$(lexer.lastTokenPosition(), "cannot change type of designated timestamp column");
        }
        if (newColumnType == existingColumnType) {
            throw SqlException.$(lexer.lastTokenPosition(), "column '").put(columnName)
                    .put("' type is already '").put(ColumnType.nameOf(existingColumnType)).put('\'');
        } else {
            if (!isCompatibleColumnTypeChange(existingColumnType, newColumnType)) {
                throw SqlException.$(lexer.lastTokenPosition(), "incompatible column type change [existing=")
                        .put(ColumnType.nameOf(existingColumnType)).put(", new=").put(ColumnType.nameOf(newColumnType)).put(']');
            }
        }
        securityContext.authorizeAlterTableAlterColumnType(tableToken, alterOperationBuilder.getExtraStrInfo());
        compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private void alterTableChangeSymbolCapacity(
            SecurityContext securityContext,
            int tableNamePosition,
            TableToken tableToken,
            int columnNamePosition,
            CharSequence columnName,
            TableRecordMetadata tableMetadata,
            int columnIndex
    ) throws SqlException {
        final AlterOperationBuilder changeColumn = alterOperationBuilder.ofSymbolCapacityChange(
                tableNamePosition,
                tableToken,
                tableMetadata.getTableId()
        );
        final int existingColumnType = tableMetadata.getColumnType(columnIndex);
        if (!ColumnType.isSymbol(existingColumnType)) {
            throw SqlException.walRecoverable(columnNamePosition).put("column '").put(columnName).put("' is not of symbol type");
        }
        expectKeyword(lexer, "capacity");

        CharSequence tok = expectToken(lexer, "numeric capacity");

        final boolean negative;
        final int errorPos = lexer.lastTokenPosition();
        if (Chars.equals(tok, '-')) {
            negative = true;
            tok = expectToken(lexer, "numeric capacity");
        } else {
            negative = false;
        }

        int symbolCapacity;
        try {
            symbolCapacity = Numbers.parseInt(tok);
        } catch (NumericException e) {
            throw SqlException.$(lexer.lastTokenPosition(), "numeric capacity expected");
        }

        if (negative) {
            symbolCapacity = -symbolCapacity;
        }

        TableUtils.validateSymbolCapacity(errorPos, symbolCapacity);

        changeColumn.addColumnToList(
                columnName,
                columnNamePosition,
                ColumnType.SYMBOL,
                Numbers.ceilPow2(symbolCapacity),
                configuration.getDefaultSymbolCacheFlag(), // ignored
                false, // ignored
                Numbers.ceilPow2(configuration.getIndexValueBlockSize()), // ignored
                false // ignored
        );

        tok = SqlUtil.fetchNext(lexer);
        if (tok != null && !isSemicolon(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [").put(tok).put("] while trying to change symbol capacity");
        }

        securityContext.authorizeAlterTableAlterColumnType(tableToken, alterOperationBuilder.getExtraStrInfo());
        compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private void alterTableColumnAddIndex(
            SecurityContext securityContext,
            int tableNamePosition,
            TableToken tableToken,
            int columnNamePosition,
            CharSequence columnName,
            TableRecordMetadata metadata,
            int indexValueBlockSize
    ) throws SqlException {
        final int columnIndex = metadata.getColumnIndexQuiet(columnName);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(columnNamePosition, columnName);
        }

        final int type = metadata.getColumnType(columnIndex);
        if (!ColumnType.isSymbol(type)) {
            throw SqlException.position(columnNamePosition).put("indexes are only supported for symbol type [column=").put(columnName).put(", type=").put(ColumnType.nameOf(type)).put(']');
        }

        if (indexValueBlockSize == -1) {
            indexValueBlockSize = configuration.getIndexValueBlockSize();
        }

        alterOperationBuilder.ofAddIndex(
                tableNamePosition,
                tableToken,
                metadata.getTableId(),
                columnName,
                Numbers.ceilPow2(indexValueBlockSize)
        );
        securityContext.authorizeAlterTableAddIndex(tableToken, alterOperationBuilder.getExtraStrInfo());
        compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private void alterTableColumnCacheFlag(
            SecurityContext securityContext,
            int tableNamePosition,
            TableToken tableToken,
            int columnNamePosition,
            CharSequence columnName,
            TableRecordMetadata metadata,
            boolean cache
    ) throws SqlException {
        int columnIndex = metadata.getColumnIndexQuiet(columnName);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(columnNamePosition, columnName);
        }

        final int type = metadata.getColumnType(columnIndex);
        if (!ColumnType.isSymbol(type)) {
            throw SqlException.position(columnNamePosition).put("cache is only supported for symbol type [column=").put(columnName).put(", type=").put(ColumnType.nameOf(type)).put(']');
        }

        if (cache) {
            alterOperationBuilder.ofCacheSymbol(tableNamePosition, tableToken, metadata.getTableId(), columnName);
        } else {
            alterOperationBuilder.ofRemoveCacheSymbol(tableNamePosition, tableToken, metadata.getTableId(), columnName);
        }

        securityContext.authorizeAlterTableAlterColumnCache(tableToken, alterOperationBuilder.getExtraStrInfo());
        compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private void alterTableColumnDropIndex(
            SecurityContext securityContext,
            int tableNamePosition,
            TableToken tableToken,
            int columnNamePosition,
            CharSequence columnName,
            TableRecordMetadata metadata
    ) throws SqlException {
        int columnIndex = metadata.getColumnIndexQuiet(columnName);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(columnNamePosition, columnName);
        }

        final int type = metadata.getColumnType(columnIndex);
        if (!ColumnType.isSymbol(type)) {
            throw SqlException.position(columnNamePosition).put("indexes are only supported for symbol type [column=").put(columnName).put(", type=").put(ColumnType.nameOf(type)).put(']');
        }

        alterOperationBuilder.ofDropIndex(tableNamePosition, tableToken, metadata.getTableId(), columnName, columnNamePosition);
        securityContext.authorizeAlterTableDropIndex(tableToken, alterOperationBuilder.getExtraStrInfo());
        compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private void alterTableDedupEnable(int tableNamePosition, TableToken tableToken, TableRecordMetadata tableMetadata, GenericLexer lexer) throws SqlException {
        if (!tableMetadata.isWalEnabled()) {
            throw SqlException.$(tableNamePosition, "deduplication is only supported for WAL tables");
        }
        AlterOperationBuilder setDedup = alterOperationBuilder.ofDedupEnable(
                tableNamePosition,
                tableToken
        );
        CharSequence tok = SqlUtil.fetchNext(lexer);

        boolean tsIncludedInDedupColumns = false;

        // ALTER TABLE abc DEDUP <ENABLE> UPSERT KEYS(a, b)
        // ENABLE word is not mandatory to be compatible v7.3
        // where it was omitted from the syntax
        if (tok != null && isEnableKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
        }

        if (tok == null || !isUpsertKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put("expected 'upsert'");
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !isKeysKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put("expected 'keys'");
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok != null && Chars.equals(tok, '(')) {
            tok = SqlUtil.fetchNext(lexer);

            int columnListPos = lexer.lastTokenPosition();

            while (tok != null && !Chars.equals(tok, ')')) {
                validateLiteral(lexer.lastTokenPosition(), tok);
                final CharSequence columnName = unquote(tok);

                int colIndex = tableMetadata.getColumnIndexQuiet(columnName);
                if (colIndex < 0) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("deduplicate key column not found [column=").put(columnName).put(']');
                }

                if (colIndex == tableMetadata.getTimestampIndex()) {
                    tsIncludedInDedupColumns = true;
                }
                setDedup.setDedupKeyFlag(tableMetadata.getWriterIndex(colIndex));

                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && Chars.equals(tok, ',')) {
                    tok = SqlUtil.fetchNext(lexer);
                }
            }

            if (tok == null || !Chars.equals(tok, ')')) {
                throw SqlException.position(lexer.getPosition()).put("')' expected");
            }

            if (!tsIncludedInDedupColumns) {
                throw SqlException.position(columnListPos).put("deduplicate key list must include dedicated timestamp column");
            }

        } else {
            throw SqlException.$(lexer.getPosition(), "deduplicate key column list expected");
        }
        compiledQuery.ofAlter(setDedup.build());
    }

    private void alterTableDropColumn(
            SecurityContext securityContext,
            int tableNamePosition,
            TableToken tableToken,
            TableRecordMetadata metadata
    ) throws SqlException {
        AlterOperationBuilder dropColumnStatement = alterOperationBuilder.ofDropColumn(tableNamePosition, tableToken, metadata.getTableId());
        int semicolonPos = -1;
        do {
            CharSequence tok = unquote(maybeExpectToken(lexer, "column name", semicolonPos < 0));
            if (semicolonPos >= 0) {
                if (tok != null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
                }
                break;
            }

            if (metadata.getColumnIndexQuiet(tok) == -1) {
                throw SqlException.invalidColumn(lexer.lastTokenPosition(), tok);
            }

            CharSequence columnName = tok;
            dropColumnStatement.ofDropColumn(columnName);
            tok = SqlUtil.fetchNext(lexer);

            if (tok == null || (!isSingleQueryMode && isSemicolon(tok))) {
                break;
            }

            semicolonPos = Chars.equals(tok, ';') ? lexer.lastTokenPosition() : -1;
            if (semicolonPos < 0 && !Chars.equals(tok, ',')) {
                unknownDropColumnSuffix(securityContext, tok, tableToken, dropColumnStatement);
                return;
            }
        } while (true);

        securityContext.authorizeAlterTableDropColumn(tableToken, dropColumnStatement.getExtraStrInfo());
        compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private void alterTableDropConvertDetachOrAttachPartition(
            TableRecordMetadata tableMetadata,
            TableToken tableToken,
            int action,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final int pos = lexer.lastTokenPosition();
        TableReader reader = null;
        if (!tableMetadata.isWalEnabled() || executionContext.isWalApplication()) {
            reader = executionContext.getReader(tableToken);
        }

        try {
            if (reader != null && !PartitionBy.isPartitioned(reader.getMetadata().getPartitionBy())) {
                throw SqlException.$(pos, "table is not partitioned");
            }

            final CharSequence tok = expectToken(lexer, "'list' or 'where'");
            if (isListKeyword(tok)) {
                alterTableDropConvertDetachOrAttachPartitionByList(tableMetadata, tableToken, reader, pos, action);
            } else if (isWhereKeyword(tok)) {
                AlterOperationBuilder alterOperationBuilder = switch (action) {
                    case PartitionAction.DROP ->
                            this.alterOperationBuilder.ofDropPartition(pos, tableToken, tableMetadata.getTableId());
                    case PartitionAction.DETACH ->
                            this.alterOperationBuilder.ofDetachPartition(pos, tableToken, tableMetadata.getTableId());
                    case PartitionAction.CONVERT_TO_PARQUET, PartitionAction.CONVERT_TO_NATIVE -> {
                        final boolean toParquet = action == PartitionAction.CONVERT_TO_PARQUET;
                        yield this.alterOperationBuilder.ofConvertPartition(pos, tableToken, tableMetadata.getTableId(), toParquet);
                    }
                    default ->
                            throw SqlException.$(pos, "WHERE clause can only be used with command DROP PARTITION, DETACH PARTITION or CONVERT PARTITION");
                };

                final int functionPosition = lexer.getPosition();
                ExpressionNode expr = parser.expr(lexer, (QueryModel) null, this);
                String designatedTimestampColumnName = null;
                int tsIndex = tableMetadata.getTimestampIndex();
                if (tsIndex >= 0) {
                    designatedTimestampColumnName = tableMetadata.getColumnName(tsIndex);
                }
                if (designatedTimestampColumnName != null) {
                    GenericRecordMetadata metadata = new GenericRecordMetadata();
                    metadata.add(new TableColumnMetadata(designatedTimestampColumnName, tableMetadata.getTimestampType(), null));
                    Function function = functionParser.parseFunction(expr, metadata, executionContext);
                    try {
                        if (function != null && ColumnType.isBoolean(function.getType())) {
                            function.init(null, executionContext);
                            if (reader != null) {
                                int affected = filterPartitions(function, functionPosition, reader, alterOperationBuilder);
                                if (affected == 0) {
                                    throw CairoException.partitionManipulationRecoverable().position(functionPosition)
                                            .put("no partitions matched WHERE clause");
                                }
                            }
                            compiledQuery.ofAlter(this.alterOperationBuilder.build());
                        } else {
                            throw SqlException.$(lexer.lastTokenPosition(), "boolean expression expected");
                        }
                    } finally {
                        Misc.free(function);
                    }
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "this table does not have a designated timestamp column");
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'list' or 'where' expected");
            }
        } finally {
            Misc.free(reader);
        }
    }

    private void alterTableDropConvertDetachOrAttachPartitionByList(
            TableRecordMetadata tableMetadata,
            TableToken tableToken,
            @Nullable TableReader reader,
            int pos,
            int action
    ) throws SqlException {
        final AlterOperationBuilder alterOperationBuilder;
        switch (action) {
            case PartitionAction.CONVERT_TO_PARQUET:
            case PartitionAction.CONVERT_TO_NATIVE:
                final boolean toParquet = action == PartitionAction.CONVERT_TO_PARQUET;
                alterOperationBuilder = this.alterOperationBuilder.ofConvertPartition(pos, tableToken, tableMetadata.getTableId(), toParquet);
                break;
            case PartitionAction.DROP:
                alterOperationBuilder = this.alterOperationBuilder.ofDropPartition(pos, tableToken, tableMetadata.getTableId());
                break;
            case PartitionAction.FORCE_DROP:
                alterOperationBuilder = this.alterOperationBuilder.ofForceDropPartition(pos, tableToken, tableMetadata.getTableId());
                break;
            case PartitionAction.DETACH:
                alterOperationBuilder = this.alterOperationBuilder.ofDetachPartition(pos, tableToken, tableMetadata.getTableId());
                break;
            case PartitionAction.ATTACH:
                // attach
                alterOperationBuilder = this.alterOperationBuilder.ofAttachPartition(pos, tableToken, tableMetadata.getTableId());
                break;
            default:
                alterOperationBuilder = null;
                assert false;
        }

        int semicolonPos = -1;
        do {
            CharSequence tok = maybeExpectToken(lexer, "partition name", semicolonPos < 0);
            if (semicolonPos >= 0) {
                if (tok != null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
                }
                break;
            }
            if (Chars.equals(tok, ',') || Chars.equals(tok, ';')) {
                throw SqlException.$(lexer.lastTokenPosition(), "partition name missing");
            }
            final CharSequence partitionName = unquote(tok); // potentially a full timestamp, or part of it
            final int lastPosition = lexer.lastTokenPosition();

            // reader == null means it's compilation for WAL table
            // before applying to WAL writer
            final int timestampType;
            final int partitionBy;
            if (reader != null) {
                partitionBy = reader.getPartitionedBy();
                timestampType = reader.getMetadata().getTimestampType();
            } else {
                try (TableMetadata meta = engine.getTableMetadata(tableToken)) {
                    partitionBy = meta.getPartitionBy();
                    timestampType = meta.getTimestampType();
                }
            }
            try {
                // When force drop partitions, allow to specify partitions with the full format and split part timestamp
                // like 2022-02-26T155900-000001
                // Otherwise ignore the split time part.
                int hi = action == PartitionAction.FORCE_DROP ? partitionName.length() : -1;
                long timestamp = PartitionBy.parsePartitionDirName(partitionName, timestampType, partitionBy, 0, hi);
                alterOperationBuilder.addPartitionToList(timestamp, lastPosition);
            } catch (CairoException e) {
                throw SqlException.$(lexer.lastTokenPosition(), e.getFlyweightMessage());
            }

            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || (!isSingleQueryMode && isSemicolon(tok))) {
                break;
            }

            semicolonPos = Chars.equals(tok, ';') ? lexer.lastTokenPosition() : -1;
            if (semicolonPos < 0 && !Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
            }
        } while (true);

        compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private void alterTableRenameColumn(
            SecurityContext securityContext,
            int tableNamePosition,
            TableToken tableToken,
            TableRecordMetadata metadata
    ) throws SqlException {
        AlterOperationBuilder renameColumnStatement = alterOperationBuilder.ofRenameColumn(tableNamePosition, tableToken, metadata.getTableId());
        int hadSemicolonPos = -1;

        do {
            CharSequence tok = unquote(maybeExpectToken(lexer, "current column name", hadSemicolonPos < 0));
            if (hadSemicolonPos >= 0) {
                if (tok != null) {
                    throw SqlException.$(hadSemicolonPos, "',' expected");
                }
                break;
            }
            int columnIndex = metadata.getColumnIndexQuiet(tok);
            if (columnIndex == -1) {
                throw SqlException.invalidColumn(lexer.lastTokenPosition(), tok);
            }
            CharSequence existingName = GenericLexer.immutableOf(tok);

            tok = expectToken(lexer, "'to' expected");
            if (!isToKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'to' expected'");
            }

            tok = unquote(expectToken(lexer, "new column name"));
            if (Chars.equals(existingName, tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "new column name is identical to existing name");
            }

            if (metadata.getColumnIndexQuiet(tok) > -1) {
                throw SqlException.$(lexer.lastTokenPosition(), " column already exists");
            }

            if (!TableUtils.isValidColumnName(tok, configuration.getMaxFileNameLength())) {
                throw SqlException.$(lexer.lastTokenPosition(), " new column name contains invalid characters");
            }

            CharSequence newName = GenericLexer.immutableOf(tok);
            renameColumnStatement.ofRenameColumn(existingName, newName);

            tok = SqlUtil.fetchNext(lexer);

            if (tok == null || (!isSingleQueryMode && isSemicolon(tok))) {
                break;
            }

            hadSemicolonPos = Chars.equals(tok, ';') ? lexer.lastTokenPosition() : -1;
            if (hadSemicolonPos < 0 && !Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
            }
        } while (true);

        securityContext.authorizeAlterTableRenameColumn(tableToken, alterOperationBuilder.getExtraStrInfo());
        compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private void alterTableResume(int tableNamePosition, TableToken tableToken, long resumeFromTxn, SqlExecutionContext executionContext) {
        try {
            engine.getTableSequencerAPI().resumeTable(tableToken, resumeFromTxn);
            executionContext.storeTelemetry(TelemetrySystemEvent.WAL_APPLY_RESUME, TelemetryOrigin.WAL_APPLY);
            compiledQuery.ofTableResume();
        } catch (CairoException ex) {
            LOG.critical().$("table resume failed [table=").$(tableToken)
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            ex.position(tableNamePosition);
            throw ex;
        }
    }

    private void alterTableSetParam(
            CharSequence paramName, CharSequence value, int paramNamePosition,
            TableToken tableToken, int tableNamePosition, int tableId
    ) throws SqlException {
        if (isMaxUncommittedRowsKeyword(paramName)) {
            int maxUncommittedRows;
            try {
                maxUncommittedRows = Numbers.parseInt(value);
            } catch (NumericException e) {
                throw SqlException.$(paramNamePosition, "invalid value [value=").put(value)
                        .put(",parameter=").put(paramName)
                        .put(']');
            }
            if (maxUncommittedRows < 0) {
                throw SqlException.$(paramNamePosition, "maxUncommittedRows must be non negative");
            }
            compiledQuery.ofAlter(alterOperationBuilder.ofSetParamUncommittedRows(
                    tableNamePosition, tableToken, tableId, maxUncommittedRows).build());
        } else if (isO3MaxLagKeyword(paramName)) {
            long o3MaxLag = SqlUtil.expectMicros(value, paramNamePosition);
            if (o3MaxLag < 0) {
                throw SqlException.$(paramNamePosition, "o3MaxLag must be non negative");
            }
            compiledQuery.ofAlter(alterOperationBuilder.ofSetO3MaxLag(tableNamePosition, tableToken, tableId, o3MaxLag).build());
        } else {
            throw SqlException.$(paramNamePosition, "unknown parameter '").put(paramName).put('\'');
        }
    }

    private void alterTableSetType(
            SqlExecutionContext executionContext,
            int pos,
            TableToken tableToken,
            byte walFlag
    ) throws SqlException {
        executionContext.getSecurityContext().authorizeAlterTableSetType(tableToken);
        try {
            try (TableReader reader = engine.getReader(tableToken)) {
                if (reader != null && !PartitionBy.isPartitioned(reader.getMetadata().getPartitionBy())) {
                    throw SqlException.$(pos, "Cannot convert non-partitioned table");
                }
            }

            path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            TableUtils.createConvertFile(ff, path, walFlag);
            compiledQuery.ofTableSetType();
        } catch (CairoException e) {
            throw SqlException.position(pos)
                    .put(e.getFlyweightMessage())
                    .put("[errno=").put(e.getErrno()).put(']');
        }
    }

    private void alterTableSuspend(int tableNamePosition, TableToken tableToken, ErrorTag errorTag, String errorMessage, SqlExecutionContext executionContext) {
        try {
            engine.getTableSequencerAPI().suspendTable(tableToken, errorTag, errorMessage);
            executionContext.storeTelemetry(TelemetrySystemEvent.WAL_APPLY_SUSPEND, TelemetryOrigin.WAL_APPLY);
            compiledQuery.ofTableSuspend();
        } catch (CairoException ex) {
            LOG.critical().$("table suspend failed [table=").$(tableToken)
                    .$(", error=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            ex.position(tableNamePosition);
            throw ex;
        }
    }

    private CharSequence authorizeInsertForCopy(SecurityContext securityContext, ExportModel model) {
        final CharSequence tableName = unquote(model.getTableName());
        final TableToken tt = engine.getTableTokenIfExists(tableName);
        if (tt != null) {
            // for existing table user have to have INSERT permission
            // if the table is to be created, later we will check for CREATE TABLE permission instead
            securityContext.authorizeInsert(tt);
        }
        return tableName;
    }

    private void authorizeSelectForCopy(SecurityContext securityContext, ExportModel model) {
        final CharSequence tableName = unquote(model.getTableName());
        final TableToken tt = engine.verifyTableName(tableName);
        if (tt != null) {
            securityContext.authorizeSelectOnAnyColumn(tt);
        }
    }

    private void checkMatViewModification(ExecutionModel executionModel) throws SqlException {
        final CharSequence name = executionModel.getTableName();
        final TableToken tableToken = engine.getTableTokenIfExists(name);
        if (tableToken != null && tableToken.isMatView()) {
            throw SqlException.position(executionModel.getTableNameExpr().position).put("cannot modify materialized view [view=").put(name).put(']');
        }
    }

    private void checkMatViewModification(TableToken tableToken) throws SqlException {
        if (tableToken != null && tableToken.isMatView()) {
            throw SqlException.position(lexer.lastTokenPosition()).put("cannot modify materialized view [view=").put(tableToken.getTableName()).put(']');
        }
    }

    private void clearExceptSqlText() {
        sqlNodePool.clear();
        characterStore.clear();
        queryColumnPool.clear();
        queryModelPool.clear();
        optimiser.clear();
        parser.clear();
        backupAgent.clear();
        alterOperationBuilder.clear();
        functionParser.clear();
        compiledQuery.clear();
        columnNames.clear();
    }

    private void compileAlter(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || (!isTableKeyword(tok) && !isMaterializedKeyword(tok))) {
            compileAlterExt(executionContext, tok);
            return;
        }
        if (isTableKeyword(tok)) {
            compileAlterTable(executionContext);
        } else {
            compileAlterMatView(executionContext);
        }
    }

    private void compileAlterMatView(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = expectToken(lexer, "'view'");
        if (!isViewKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'view' expected'");
        }

        final int matViewNamePosition = lexer.getPosition();
        tok = expectToken(lexer, "materialized view name");
        assertNameIsQuotedOrNotAKeyword(tok, matViewNamePosition);
        final TableToken matViewToken = tableExistsOrFail(matViewNamePosition, unquote(tok), executionContext);
        if (!matViewToken.isMatView()) {
            throw SqlException.$(lexer.lastTokenPosition(), "materialized view name expected");
        }
        final SecurityContext securityContext = executionContext.getSecurityContext();
        final MatViewDefinition viewDefinition = engine.getMatViewGraph().getViewDefinition(matViewToken);
        if (viewDefinition == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "materialized view does not exist");
        }

        try (TableRecordMetadata tableMetadata = engine.getTableMetadata(matViewToken)) {
            tok = expectToken(lexer, "'alter' or 'resume' or 'suspend'");
            if (isAlterKeyword(tok)) {
                expectKeyword(lexer, "column");
                final int columnNamePosition = lexer.getPosition();
                tok = expectToken(lexer, "column name");
                final CharSequence columnName = GenericLexer.immutableOf(tok);
                final int columnIndex = tableMetadata.getColumnIndexQuiet(columnName);
                if (columnIndex == -1) {
                    throw SqlException.walRecoverable(columnNamePosition).put("column '").put(columnName)
                            .put("' does not exist in materialized view '").put(matViewToken.getTableName()).put('\'');
                }

                tok = expectToken(lexer, "'symbol capacity', 'add index' or 'drop index'");
                if (SqlKeywords.isSymbolKeyword(tok)) {
                    alterTableChangeSymbolCapacity(
                            securityContext,
                            matViewNamePosition,
                            matViewToken,
                            columnNamePosition,
                            columnName,
                            tableMetadata,
                            columnIndex
                    );
                } else if (SqlKeywords.isAddKeyword(tok)) {
                    expectKeyword(lexer, "index");

                    if (tableMetadata.isColumnIndexed(columnIndex)) {
                        throw SqlException.walRecoverable(columnNamePosition).put("column '").put(columnName)
                                .put("' already indexed");
                    }
                    int columnType = tableMetadata.getColumnType(columnIndex);
                    if (columnType != ColumnType.SYMBOL) {
                        throw SqlException.walRecoverable(columnNamePosition).put("column '").put(columnName)
                                .put("' is of type '").put(ColumnType.nameOf(columnType)).put("'. Index supports column type 'SYMBOL' only.");
                    }

                    final int indexValueBlockSize;
                    final boolean sizeInferred;

                    tok = SqlUtil.fetchNext(lexer);
                    if (tok == null) {
                        indexValueBlockSize = estimateIndexValueBlockSizeFromReader(configuration, executionContext, matViewToken, columnIndex);
                        sizeInferred = true;
                    } else {
                        if (!SqlKeywords.isCapacityKeyword(tok)) {
                            throw SqlException.$(lexer.lastTokenPosition(), "'capacity' keyword expected");
                        }
                        tok = expectToken(lexer, "index capacity value");
                        try {
                            indexValueBlockSize = Numbers.parseInt(tok);
                            sizeInferred = false;
                        } catch (NumericException e) {
                            throw SqlException.$(lexer.lastTokenPosition(), "index capacity value must be numeric");
                        }
                    }

                    LOG.info().$("adding mat view index [viewName=").$(matViewToken)
                            .$(", column=").$safe(columnName)
                            .$(", indexValueBlockSize=").$(indexValueBlockSize)
                            .$(", sizeInferred=").$(sizeInferred)
                            .I$();

                    alterTableColumnAddIndex(
                            securityContext,
                            matViewNamePosition,
                            matViewToken,
                            columnNamePosition,
                            columnName,
                            tableMetadata,
                            indexValueBlockSize
                    );
                } else if (SqlKeywords.isDropKeyword(tok)) {
                    expectKeyword(lexer, "index");

                    if (!tableMetadata.isColumnIndexed(columnIndex)) {
                        throw SqlException.walRecoverable(columnNamePosition).put("column '").put(columnName)
                                .put("' is not indexed");
                    }

                    alterTableColumnDropIndex(
                            securityContext,
                            matViewNamePosition,
                            matViewToken,
                            columnNamePosition,
                            columnName,
                            tableMetadata
                    );

                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'symbol capacity', 'add index' or 'drop index' expected");
                }
            } else if (isSetKeyword(tok)) {
                tok = expectToken(lexer, "'ttl' or 'refresh'");
                if (isTtlKeyword(tok)) {
                    final int ttlValuePos = lexer.getPosition();
                    final int ttlHoursOrMonths = SqlParser.parseTtlHoursOrMonths(lexer);
                    try (TableMetadata matViewMeta = engine.getTableMetadata(matViewToken)) {
                        PartitionBy.validateTtlGranularity(matViewMeta.getPartitionBy(), ttlHoursOrMonths, ttlValuePos);
                    }
                    final AlterOperationBuilder setTtl = alterOperationBuilder.ofSetTtl(
                            matViewNamePosition,
                            matViewToken,
                            tableMetadata.getTableId(),
                            ttlHoursOrMonths
                    );
                    compiledQuery.ofAlter(setTtl.build());
                } else if (isRefreshKeyword(tok)) {
                    tok = expectToken(lexer, "'immediate' or 'manual' or 'period' or 'every' or 'limit'");
                    if (isLimitKeyword(tok)) {
                        // SET REFRESH LIMIT
                        final int limitHoursOrMonths = SqlParser.parseTtlHoursOrMonths(lexer);
                        final AlterOperationBuilder setRefreshLimit = alterOperationBuilder.ofSetMatViewRefreshLimit(
                                matViewNamePosition,
                                matViewToken,
                                tableMetadata.getTableId(),
                                limitHoursOrMonths
                        );
                        compiledQuery.ofAlter(setRefreshLimit.build());
                    } else if (isImmediateKeyword(tok) || isManualKeyword(tok) || isEveryKeyword(tok) || isPeriodKeyword(tok)) {
                        // SET REFRESH [IMMEDIATE | MANUAL | EVERY <interval>] ...
                        int refreshType = MatViewDefinition.REFRESH_TYPE_IMMEDIATE;
                        int every = 0;
                        char everyUnit = 0;
                        long startUs = Numbers.LONG_NULL;
                        String tz = null;
                        TimeZoneRules tzRulesMicros = null;
                        int length = 0;
                        char lengthUnit = 0;
                        int delay = 0;
                        char delayUnit = 0;

                        if (isImmediateKeyword(tok)) {
                            tok = SqlUtil.fetchNext(lexer);
                        } else if (isManualKeyword(tok)) {
                            refreshType = MatViewDefinition.REFRESH_TYPE_MANUAL;
                            tok = SqlUtil.fetchNext(lexer);
                        } else if (isEveryKeyword(tok)) {
                            tok = expectToken(lexer, "interval");
                            every = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                            everyUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                            SqlParser.validateMatViewEveryUnit(everyUnit, lexer.lastTokenPosition());
                            refreshType = MatViewDefinition.REFRESH_TYPE_TIMER;
                            tok = SqlUtil.fetchNext(lexer);
                        }

                        if (tok != null && isPeriodKeyword(tok)) {
                            // REFRESH ... PERIOD(LENGTH <interval> [TIME ZONE '<timezone>'] [DELAY <interval>])
                            expectKeyword(lexer, "(");
                            expectKeyword(lexer, "length");
                            tok = expectToken(lexer, "LENGTH interval");
                            length = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                            lengthUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                            SqlParser.validateMatViewLength(length, lengthUnit, lexer.lastTokenPosition());
                            final TimestampSampler periodSamplerMicros = TimestampSamplerFactory.getInstance(
                                    MicrosTimestampDriver.INSTANCE,
                                    length,
                                    lengthUnit,
                                    lexer.lastTokenPosition()
                            );
                            tok = expectToken(lexer, "'time zone' or 'delay' or ')'");

                            if (isTimeKeyword(tok)) {
                                expectKeyword(lexer, "zone");
                                tok = expectToken(lexer, "TIME ZONE name");
                                if (Chars.equals(tok, ')') || isDelayKeyword(tok)) {
                                    throw SqlException.position(lexer.lastTokenPosition()).put("TIME ZONE name expected");
                                }
                                tz = unquote(tok).toString();
                                try {
                                    tzRulesMicros = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, tz);
                                } catch (CairoException e) {
                                    throw SqlException.position(lexer.lastTokenPosition()).put(e.getFlyweightMessage());
                                }
                                tok = expectToken(lexer, "'delay' or ')'");
                            }

                            if (isDelayKeyword(tok)) {
                                tok = expectToken(lexer, "DELAY interval");
                                delay = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                                delayUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                                SqlParser.validateMatViewDelay(length, lengthUnit, delay, delayUnit, lexer.lastTokenPosition());
                                tok = expectToken(lexer, "')'");
                            }

                            if (!Chars.equals(tok, ')')) {
                                throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
                            }

                            // Period timer start is at the boundary of the current period.
                            final long nowMicros = configuration.getMicrosecondClock().getTicks();
                            final long nowLocalMicros = tzRulesMicros != null ? nowMicros + tzRulesMicros.getOffset(nowMicros) : nowMicros;
                            startUs = periodSamplerMicros.round(nowLocalMicros);
                            tok = SqlUtil.fetchNext(lexer);
                        } else if (refreshType == MatViewDefinition.REFRESH_TYPE_TIMER) {
                            if (tok != null && isStartKeyword(tok)) {
                                // REFRESH EVERY <interval> [START '<datetime>' [TIME ZONE '<timezone>']]
                                tok = expectToken(lexer, "START timestamp");
                                try {
                                    startUs = MicrosTimestampDriver.INSTANCE.parseFloorLiteral(unquote(tok));
                                } catch (NumericException e) {
                                    throw SqlException.$(lexer.lastTokenPosition(), "invalid START timestamp value");
                                }
                                tok = maybeExpectToken(lexer, "'time zone'", false);

                                if (isTimeKeyword(tok)) {
                                    expectKeyword(lexer, "zone");
                                    tok = expectToken(lexer, "TIME ZONE name");
                                    tz = unquote(tok).toString();
                                    // validate time zone
                                    try {
                                        MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, tz);
                                    } catch (CairoException e) {
                                        throw SqlException.position(lexer.lastTokenPosition()).put(e.getFlyweightMessage());
                                    }
                                    tok = SqlUtil.fetchNext(lexer);
                                }
                            } else {
                                // REFRESH EVERY <interval> AS
                                // Don't forget to set timer params.
                                startUs = configuration.getMicrosecondClock().getTicks();
                            }
                        }

                        if (tok != null && !isSemicolon(tok)) {
                            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
                        }

                        final AlterOperationBuilder setTimer = alterOperationBuilder.ofSetMatViewRefresh(
                                matViewNamePosition,
                                matViewToken,
                                tableMetadata.getTableId(),
                                refreshType,
                                every,
                                everyUnit,
                                startUs,
                                tz,
                                length,
                                lengthUnit,
                                delay,
                                delayUnit
                        );
                        compiledQuery.ofAlter(setTimer.build());
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'immediate' or 'manual' or 'period' or 'every' or 'limit' expected");
                    }
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'ttl' or 'refresh' or 'start' expected");
                }
            } else if (isResumeKeyword(tok)) {
                parseResumeWal(matViewToken, matViewNamePosition, executionContext);
            } else if (isSuspendKeyword(tok)) {
                parseSuspendWal(matViewToken, matViewNamePosition, executionContext);
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'alter' or 'resume' or 'suspend' expected");
            }
        } catch (CairoException e) {
            LOG.info().$("could not alter materialized view [view=").$(matViewToken.getTableName())
                    .$(", msg=").$safe(e.getFlyweightMessage())
                    .$(", errno=").$(e.getErrno())
                    .I$();
            if (e.getPosition() == 0) {
                e.position(lexer.lastTokenPosition());
            }
            throw e;
        }
    }

    private void compileAlterTable(SqlExecutionContext executionContext) throws SqlException {
        final int tableNamePosition = lexer.getPosition();
        CharSequence tok = expectToken(lexer, "table name");
        assertNameIsQuotedOrNotAKeyword(tok, tableNamePosition);
        final TableToken tableToken = tableExistsOrFail(tableNamePosition, unquote(tok), executionContext);
        checkMatViewModification(tableToken);
        final SecurityContext securityContext = executionContext.getSecurityContext();

        try (TableRecordMetadata tableMetadata = executionContext.getMetadataForWrite(tableToken)) {
            tok = expectToken(lexer, ALTER_TABLE_EXPECTED_TOKEN_DESCR);

            if (isAddKeyword(tok)) {
                securityContext.authorizeAlterTableAddColumn(tableToken);
                alterTableAddColumn(executionContext.getSecurityContext(), tableNamePosition, tableToken, tableMetadata);
            } else if (isConvertKeyword(tok)) {
                tok = expectToken(lexer, "'partition'");
                if (!isPartitionKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                }
                tok = expectToken(lexer, "'to'");
                if (!isToKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'to' expected");
                }
                tok = expectToken(lexer, "'parquet' or 'native'");
                final int action;
                if (isParquetKeyword(tok)) {
                    action = PartitionAction.CONVERT_TO_PARQUET;
                } else if (isNativeKeyword(tok)) {
                    action = PartitionAction.CONVERT_TO_NATIVE;
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'parquet' or 'native' expected");
                }
                alterTableDropConvertDetachOrAttachPartition(tableMetadata, tableToken, action, executionContext);
            } else if (isDropKeyword(tok)) {
                tok = expectToken(lexer, "'column' or 'partition'");
                if (isColumnKeyword(tok)) {
                    alterTableDropColumn(executionContext.getSecurityContext(), tableNamePosition, tableToken, tableMetadata);
                } else if (isPartitionKeyword(tok)) {
                    securityContext.authorizeAlterTableDropPartition(tableToken);
                    alterTableDropConvertDetachOrAttachPartition(tableMetadata, tableToken, PartitionAction.DROP, executionContext);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'column' or 'partition' expected");
                }
            } else if (isRenameKeyword(tok)) {
                tok = expectToken(lexer, "'column'");
                if (isColumnKeyword(tok)) {
                    alterTableRenameColumn(securityContext, tableNamePosition, tableToken, tableMetadata);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'column' expected");
                }
            } else if (isAttachKeyword(tok)) {
                tok = expectToken(lexer, "'partition'");
                if (isPartitionKeyword(tok)) {
                    securityContext.authorizeAlterTableAttachPartition(tableToken);
                    alterTableDropConvertDetachOrAttachPartition(tableMetadata, tableToken, PartitionAction.ATTACH, executionContext);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                }
            } else if (isDetachKeyword(tok)) {
                tok = expectToken(lexer, "'partition'");
                if (isPartitionKeyword(tok)) {
                    securityContext.authorizeAlterTableDetachPartition(tableToken);
                    alterTableDropConvertDetachOrAttachPartition(tableMetadata, tableToken, PartitionAction.DETACH, executionContext);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                }
            } else if (isForceKeyword(tok)) {
                tok = expectToken(lexer, "'drop'");
                if (isDropKeyword(tok)) {
                    tok = expectToken(lexer, "'partition'");
                    if (isPartitionKeyword(tok)) {
                        tok = expectToken(lexer, "'list'");
                        if (isListKeyword(tok)) {
                            securityContext.authorizeAlterTableDropPartition(tableToken);
                            alterTableDropConvertDetachOrAttachPartitionByList(tableMetadata, tableToken, null, lexer.lastTokenPosition(), PartitionAction.FORCE_DROP);
                        } else {
                            throw SqlException.$(lexer.lastTokenPosition(), "'list' expected");
                        }
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                    }
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'drop' expected");
                }
            } else if (isAlterKeyword(tok)) {
                tok = expectToken(lexer, "'column'");
                if (isColumnKeyword(tok)) {
                    final int columnNamePosition = lexer.getPosition();
                    tok = expectToken(lexer, "column name");
                    final CharSequence columnName = GenericLexer.immutableOf(tok);
                    final int columnIndex = tableMetadata.getColumnIndexQuiet(columnName);
                    if (columnIndex == -1) {
                        throw SqlException.walRecoverable(columnNamePosition).put("column '").put(columnName)
                                .put("' does not exist in table '").put(tableToken.getTableName()).put('\'');
                    }

                    tok = expectToken(lexer, "'add index' or 'drop index' or 'type' or 'cache' or 'nocache' or 'symbol'");
                    if (isAddKeyword(tok)) {
                        expectKeyword(lexer, "index");
                        tok = SqlUtil.fetchNext(lexer);
                        int indexValueCapacity = -1;

                        if (tok != null && (!isSemicolon(tok))) {
                            if (!isCapacityKeyword(tok)) {
                                throw SqlException.$(lexer.lastTokenPosition(), "'capacity' expected");
                            } else {
                                tok = expectToken(lexer, "capacity value");
                                try {
                                    indexValueCapacity = Numbers.parseInt(tok);
                                    if (indexValueCapacity <= 0) {
                                        throw SqlException.$(lexer.lastTokenPosition(), "positive integer literal expected as index capacity");
                                    }
                                } catch (NumericException e) {
                                    throw SqlException.$(lexer.lastTokenPosition(), "positive integer literal expected as index capacity");
                                }
                            }
                        }

                        alterTableColumnAddIndex(
                                securityContext,
                                tableNamePosition,
                                tableToken,
                                columnNamePosition,
                                columnName,
                                tableMetadata,
                                indexValueCapacity
                        );
                    } else if (isDropKeyword(tok)) {
                        // alter table <table name> alter column drop index
                        expectKeyword(lexer, "index");
                        tok = SqlUtil.fetchNext(lexer);
                        if (tok != null && !isSemicolon(tok)) {
                            throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [").put(tok).put("] while trying to drop index");
                        }
                        alterTableColumnDropIndex(
                                securityContext,
                                tableNamePosition,
                                tableToken,
                                columnNamePosition,
                                columnName,
                                tableMetadata
                        );
                    } else if (isCacheKeyword(tok)) {
                        alterTableColumnCacheFlag(
                                securityContext,
                                tableNamePosition,
                                tableToken,
                                columnNamePosition,
                                columnName,
                                tableMetadata,
                                true
                        );
                    } else if (isNoCacheKeyword(tok)) {
                        alterTableColumnCacheFlag(
                                securityContext,
                                tableNamePosition,
                                tableToken,
                                columnNamePosition,
                                columnName,
                                tableMetadata,
                                false
                        );
                    } else if (isTypeKeyword(tok)) {
                        alterTableChangeColumnType(
                                securityContext,
                                tableNamePosition,
                                tableToken,
                                columnNamePosition,
                                columnName,
                                tableMetadata,
                                columnIndex
                        );
                    } else if (isSymbolKeyword(tok)) {
                        alterTableChangeSymbolCapacity(
                                securityContext,
                                tableNamePosition,
                                tableToken,
                                columnNamePosition,
                                columnName,
                                tableMetadata,
                                columnIndex
                        );
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'add', 'drop', 'symbol', 'cache' or 'nocache' expected").put(" found '").put(tok).put('\'');
                    }
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'column' or 'partition' expected");
                }
            } else if (isSetKeyword(tok)) {
                tok = expectToken(lexer, "'param', 'ttl' or 'type'");
                if (isParamKeyword(tok)) {
                    final int paramNamePosition = lexer.getPosition();
                    tok = expectToken(lexer, "param name");
                    final CharSequence paramName = GenericLexer.immutableOf(tok);
                    tok = expectToken(lexer, "'='");
                    if (tok.length() == 1 && tok.charAt(0) == '=') {
                        CharSequence value = GenericLexer.immutableOf(SqlUtil.fetchNext(lexer));
                        alterTableSetParam(paramName, value, paramNamePosition, tableToken, tableNamePosition, tableMetadata.getTableId());
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'=' expected");
                    }
                } else if (isTtlKeyword(tok)) {
                    final int ttlValuePos = lexer.getPosition();
                    final int ttlHoursOrMonths = SqlParser.parseTtlHoursOrMonths(lexer);
                    try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                        CairoTable table = metadataRO.getTable(tableToken);
                        assert table != null : "CairoTable == null after we already checked it exists";
                        PartitionBy.validateTtlGranularity(table.getPartitionBy(), ttlHoursOrMonths, ttlValuePos);
                    }
                    final AlterOperationBuilder setTtl = alterOperationBuilder.ofSetTtl(
                            tableNamePosition,
                            tableToken,
                            tableMetadata.getTableId(),
                            ttlHoursOrMonths
                    );
                    compiledQuery.ofAlter(setTtl.build());
                } else if (isTypeKeyword(tok)) {
                    tok = expectToken(lexer, "'bypass' or 'wal'");
                    if (isBypassKeyword(tok)) {
                        tok = expectToken(lexer, "'wal'");
                        if (isWalKeyword(tok)) {
                            alterTableSetType(executionContext, tableNamePosition, tableToken, (byte) 0);
                        } else {
                            throw SqlException.$(lexer.lastTokenPosition(), "'wal' expected");
                        }
                    } else if (isWalKeyword(tok)) {
                        alterTableSetType(executionContext, tableNamePosition, tableToken, (byte) 1);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'bypass' or 'wal' expected");
                    }
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'param' or 'type' expected");
                }
            } else if (isResumeKeyword(tok)) {
                parseResumeWal(tableToken, tableNamePosition, executionContext);
            } else if (isSuspendKeyword(tok)) {
                parseSuspendWal(tableToken, tableNamePosition, executionContext);
            } else if (isSquashKeyword(tok)) {
                securityContext.authorizeAlterTableDropPartition(tableToken);
                tok = expectToken(lexer, "'partitions'");
                if (isPartitionsKeyword(tok)) {
                    compiledQuery.ofAlter(alterOperationBuilder.ofSquashPartitions(tableNamePosition, tableToken).build());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'partitions' expected");
                }
            } else if (isDedupKeyword(tok) || isDeduplicateKeyword(tok)) {
                tok = expectToken(lexer, "'dedup columns'");

                if (isDisableKeyword(tok)) {
                    executionContext.getSecurityContext().authorizeAlterTableDedupDisable(tableToken);
                    final AlterOperationBuilder setDedup = alterOperationBuilder.ofDedupDisable(
                            tableNamePosition,
                            tableToken
                    );
                    compiledQuery.ofAlter(setDedup.build());
                } else {
                    lexer.unparseLast();
                    executionContext.getSecurityContext().authorizeAlterTableDedupEnable(tableToken);
                    alterTableDedupEnable(tableNamePosition, tableToken, tableMetadata, lexer);
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), ALTER_TABLE_EXPECTED_TOKEN_DESCR).put(" expected");
            }
        } catch (CairoException e) {
            LOG.info().$("could not alter table [table=").$(tableToken)
                    .$(", msg=").$safe(e.getFlyweightMessage())
                    .$(", errno=").$(e.getErrno())
                    .I$();
            if (e.getPosition() == 0) {
                e.position(lexer.lastTokenPosition());
            }
            throw e;
        }
    }

    private void compileBegin(SqlExecutionContext executionContext, @Transient CharSequence sqlText) {
        compiledQuery.ofBegin();
    }

    private void compileCancel(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !isQueryKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'QUERY' expected'");
        }

        tok = SqlUtil.fetchNext(lexer);
        int position = lexer.lastTokenPosition();
        try {
            long queryId = Numbers.parseLong(tok);
            tok = SqlUtil.fetchNext(lexer);
            if (tok != null && !isSemicolon(tok)) {
                throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
            }
            try {
                if (!executionContext.getCairoEngine().getQueryRegistry().cancel(queryId, executionContext)) {
                    throw SqlException.$(position, "query to cancel not found in registry [id=").put(queryId).put(']');
                }
            } catch (CairoException e) {
                throw SqlException.$(position, e.getFlyweightMessage());
            }
            compiledQuery.ofCancelQuery();
        } catch (NumericException e) {
            throw SqlException.$(position, "non-negative integer literal expected as query id");
        }
    }

    private void compileCheckpoint(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        executionContext.getSecurityContext().authorizeDatabaseSnapshot();
        executionContext.getCircuitBreaker().resetTimer();
        CharSequence tok = expectToken(lexer, "'create' or 'release'");

        if (isCreateKeyword(tok)) {
            engine.checkpointCreate(executionContext);
            compiledQuery.ofCheckpointCreate();
        } else if (Chars.equalsLowerCaseAscii(tok, "release")) {
            engine.checkpointRelease();
            compiledQuery.ofCheckpointRelease();
        } else {
            throw SqlException.position(lexer.lastTokenPosition()).put("'create' or 'release' expected");
        }
    }

    private void compileCommit(SqlExecutionContext executionContext, @Transient CharSequence sqlText) {
        compiledQuery.ofCommit();
    }

    private RecordCursorFactory compileCopyCancel(SqlExecutionContext executionContext, ExportModel model) throws SqlException {
        assert model.isCancel();

        long cancelCopyID;
        String cancelCopyIDStr = Chars.toString(unquote(model.getTableName()));
        try {
            cancelCopyID = Numbers.parseHexLong(cancelCopyIDStr);
        } catch (NumericException e) {
            throw SqlException.$(0, "copy cancel ID format is invalid: '").put(cancelCopyIDStr).put('\'');
        }

        RecordCursorFactory _import = null, _export = null;

        if (configuration.getSqlCopyInputRoot() != null) {
            try {
                _import = query()
                        .$("select * from '")
                        .$(engine.getConfiguration().getSystemTableNamePrefix())
                        .$("text_import_log' where id = '")
                        .$(cancelCopyIDStr)
                        .$("' limit -1")
                        .compile(executionContext).getRecordCursorFactory();
            } catch (SqlException e) {
                if (!e.isTableDoesNotExist()) {
                    throw e;
                }
            }
        }

        if (configuration.getSqlCopyExportRoot() != null && !configuration.isReadOnlyInstance()) {
            try {
                _export = query()
                        .$("select * from '")
                        .$(engine.getConfiguration().getSystemTableNamePrefix())
                        .$("copy_export_log' where id = '")
                        .$(cancelCopyIDStr)
                        .$("' limit -1")
                        .compile(executionContext).getRecordCursorFactory();
            } catch (SqlException e) {
                if (!e.isTableDoesNotExist()) {
                    throw e;
                }
            }
        }

        return new CopyCancelFactory(
                engine.getCopyImportContext(),
                engine.getCopyExportContext(),
                cancelCopyID,
                cancelCopyIDStr,
                _import,
                _export
        );
    }

    private RecordCursorFactory compileCopyFrom(SecurityContext securityContext, ExportModel model) throws SqlException {
        assert !model.isCancel();
        final CharSequence tableName = authorizeInsertForCopy(securityContext, model);

        if (model.getTimestampColumnName() == null
                && ((model.getPartitionBy() != -1 && model.getPartitionBy() != PartitionBy.NONE))) {
            throw SqlException.$(-1, "invalid option used for import without a designated timestamp (format or partition by)");
        }
        if (model.getDelimiter() < 0) {
            model.setDelimiter((byte) ',');
        }

        final ExpressionNode fileNameNode = model.getFileName();
        final CharSequence fileName = fileNameNode != null ? GenericLexer.assertNoDots(unquote(fileNameNode.token), fileNameNode.position) : null;
        assert fileName != null;

        return new CopyImportFactory(
                messageBus,
                engine.getCopyImportContext(),
                Chars.toString(tableName),
                Chars.toString(fileName),
                model
        );
    }


    private RecordCursorFactory compileCopyTo(SecurityContext securityContext, ExportModel model, CharSequence sqlText) throws SqlException {
        assert !model.isCancel();

        if (engine.getConfiguration().isReadOnlyInstance()) {
            throw SqlException.$(0, "COPY TO is not supported on read-only instance");
        }

        if (Chars.isBlank(engine.getConfiguration().getSqlCopyExportRoot())) {
            throw SqlException.$(0, "COPY TO is disabled ['cairo.sql.copy.export.root' is not set?]");
        }

        if (model.getTableName() != null) {
            authorizeSelectForCopy(securityContext, model);
        }

        if (!model.isParquetFormat()) {
            throw SqlException.$(0, "export format must be specified, supported formats: 'parquet'");
        }
        model.validCompressOptions();
        return new CopyExportFactory(
                engine,
                model,
                securityContext,
                sqlText
        );
    }

    private void compileDeallocate(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        CharSequence statementName = unquote(expectToken(lexer, "statement name"));
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
        }
        compiledQuery.ofDeallocate(statementName);
    }

    private void compileDrop(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        // drop does not have passwords
        QueryProgress.logStart(-1, sqlText, executionContext, false);
        // the selected method depends on the second token, we have already seen DROP
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok != null) {
            // DROP TABLE [ IF EXISTS ] name [;]
            if (isTableKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "expected ").put("IF EXISTS table-name");
                }
                boolean hasIfExists = false;
                int tableNamePosition = lexer.lastTokenPosition();
                if (isIfKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok == null || !isExistsKeyword(tok)) {
                        throw SqlException.$(lexer.lastTokenPosition(), "expected ").put("EXISTS table-name");
                    }
                    hasIfExists = true;
                    tableNamePosition = lexer.getPosition();
                } else {
                    lexer.unparseLast(); // tok has table name
                }

                tok = expectToken(lexer, "table name");
                assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());

                final CharSequence tableName = unquote(tok);
                // define operation to make sure we generate correct errors in case
                // of syntax check failure.
                final TableToken tableToken = executionContext.getTableTokenIfExists(tableName);
                checkMatViewModification(tableToken);
                dropOperationBuilder.clear();
                dropOperationBuilder.setOperationCode(OperationCodes.DROP_TABLE);
                dropOperationBuilder.setEntityName(tableName);
                dropOperationBuilder.setEntityNamePosition(tableNamePosition);
                dropOperationBuilder.setIfExists(hasIfExists);
                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && !Chars.equals(tok, ';')) {
                    compileDropExt(executionContext, dropOperationBuilder, tok, lexer.lastTokenPosition());
                }
                dropOperationBuilder.setSqlText(sqlText);
                compiledQuery.ofDrop(dropOperationBuilder.build());
                // DROP MATERIALIZED VIEW [ IF EXISTS ] name [;]
            } else if (isMaterializedKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || !isViewKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "expected VIEW");
                }
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "expected ").put("IF EXISTS mat-view-name");
                }
                boolean hasIfExists = false;
                int tableNamePosition = lexer.lastTokenPosition();
                if (isIfKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok == null || !isExistsKeyword(tok)) {
                        throw SqlException.$(lexer.lastTokenPosition(), "expected ").put("EXISTS mat-view-name");
                    }
                    hasIfExists = true;
                    tableNamePosition = lexer.getPosition();
                } else {
                    lexer.unparseLast(); // tok has table name
                }

                tok = expectToken(lexer, "view name");
                assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());

                final CharSequence matViewName = unquote(tok);
                // define operation to make sure we generate correct errors in case
                // of syntax check failure.
                final TableToken tableToken = executionContext.getTableTokenIfExists(matViewName);
                if (tableToken != null && !tableToken.isMatView()) {
                    throw SqlException.$(lexer.lastTokenPosition(), "materialized view name expected, got table name");
                }
                dropOperationBuilder.clear();
                dropOperationBuilder.setOperationCode(OperationCodes.DROP_MAT_VIEW);
                dropOperationBuilder.setEntityName(matViewName);
                dropOperationBuilder.setEntityNamePosition(tableNamePosition);
                dropOperationBuilder.setIfExists(hasIfExists);
                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && !Chars.equals(tok, ';')) {
                    compileDropExt(executionContext, dropOperationBuilder, tok, lexer.lastTokenPosition());
                }
                dropOperationBuilder.setSqlText(sqlText);
                compiledQuery.ofDrop(dropOperationBuilder.build());
            } else if (isAllKeyword(tok)) {
                // DROP ALL[;] or legacy DROP ALL TABLES [;]
                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && isTablesKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                }
                if (tok != null && !Chars.equals(tok, ';')) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("';' or 'tables' expected");
                }
                compiledQuery.ofDrop(DropAllOperation.INSTANCE);
            } else {
                compileDropOther(executionContext, tok, lexer.lastTokenPosition());
            }
        } else {
            compileDropReportExpected(lexer.getPosition());
        }
    }

    private ExecutionModel compileExecutionModel(SqlExecutionContext executionContext) throws SqlException {
        final ExecutionModel model = parser.parse(lexer, executionContext, this);
        if (model.getModelType() != ExecutionModel.EXPLAIN) {
            return compileExecutionModel0(executionContext, model);
        } else {
            final ExplainModel explainModel = (ExplainModel) model;
            final ExecutionModel innerModel = compileExplainExecutionModel0(executionContext, explainModel.getInnerExecutionModel());
            explainModel.setModel(innerModel);
            return explainModel;
        }
    }

    private ExecutionModel compileExecutionModel0(SqlExecutionContext executionContext, ExecutionModel model) throws SqlException {
        switch (model.getModelType()) {
            case ExecutionModel.QUERY:
                return optimiser.optimise((QueryModel) model, executionContext, this);
            case ExecutionModel.INSERT: {
                final InsertModel insertModel = (InsertModel) model;
                if (insertModel.getQueryModel() != null) {
                    validateAndOptimiseInsertAsSelect(executionContext, insertModel);
                } else {
                    lightlyValidateInsertModel(insertModel);
                }
                final TableToken tableToken = engine.getTableTokenIfExists(insertModel.getTableName());
                executionContext.getSecurityContext().authorizeInsert(tableToken);
                return insertModel;
            }
            case ExecutionModel.UPDATE:
                final QueryModel queryModel = (QueryModel) model;
                TableToken tableToken = executionContext.getTableToken(queryModel.getTableName());
                try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(tableToken)) {
                    optimiser.optimiseUpdate(queryModel, executionContext, metadata, this);
                    return model;
                }
            default:
                return model;
        }
    }

    private ExecutionModel compileExplainExecutionModel0(SqlExecutionContext executionContext, ExecutionModel model) throws SqlException {
        // CREATE TABLE AS SELECT and CREATE MATERIALIZED VIEW have an unoptimized SELECT model after the parsing.
        // We optimize and validate the model during the execution, but in case of EXPLAIN the model is
        // directly compiled into a factory, so we need to optimize it before proceeding.
        switch (model.getModelType()) {
            case ExecutionModel.CREATE_TABLE:
                executionContext.getSecurityContext().authorizeTableCreate();
                final CreateTableOperationBuilder createTableBuilder = (CreateTableOperationBuilder) model;
                if (createTableBuilder.getQueryModel() != null) {
                    final QueryModel selectModel = optimiser.optimise(createTableBuilder.getQueryModel(), executionContext, this);
                    createTableBuilder.setSelectModel(selectModel);
                }
                return model;
            case ExecutionModel.CREATE_MAT_VIEW:
                executionContext.getSecurityContext().authorizeMatViewCreate();
                final CreateMatViewOperationBuilder createMatViewBuilder = (CreateMatViewOperationBuilder) model;
                if (createMatViewBuilder.getQueryModel() != null) {
                    final QueryModel selectModel = optimiser.optimise(createMatViewBuilder.getQueryModel(), executionContext, this);
                    createMatViewBuilder.setSelectModel(selectModel);
                }
                return model;
        }
        return compileExecutionModel0(executionContext, model);
    }

    private void compileInner(
            @NotNull SqlExecutionContext executionContext,
            CharSequence sqlText,
            boolean generateProgressLogger
    ) throws SqlException {
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        if (!circuitBreaker.isTimerSet()) {
            circuitBreaker.resetTimer();
        }
        final CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            compiledQuery.ofEmpty();
            return;
        }

        final KeywordBasedExecutor executor = keywordBasedExecutors.get(tok);
        final long beginNanos = configuration.getNanosecondClock().getTicks();

        if (executor != null) {
            // an executor can return compiled query with NONE type as a fallback to execution model
            try {
                // we cannot log start of the executor SQL because we do not know if it
                // contains secrets or not.

                executor.execute(executionContext, sqlText);
                // executor might decide that SQL contains secret, otherwise we're logging it
                this.sqlText = executionContext.containsSecret() ? "** redacted for privacy **" : sqlText;
                QueryProgress.logEnd(-1, this.sqlText, executionContext, beginNanos);
            } catch (Throwable th) {
                // Executor is all-in-one, it parses SQL text and executes it right away. The convention is
                // that before parsing secrets the executor will notify the execution context. In that, even if
                // executor fails, the secret SQL text must not be logged
                this.sqlText = executionContext.containsSecret() ? "** redacted for privacy** " : sqlText;
                QueryProgress.logError(th, -1, this.sqlText, executionContext, beginNanos);
                throw th;
            }
        } else {
            this.sqlText = sqlText;
        }
        // executor is allowed to give up on the execution and fall back to standard behaviour
        if (executor == null || compiledQuery.getType() == CompiledQuery.NONE) {
            compileUsingModel(executionContext, beginNanos, generateProgressLogger);
        }
        final short type = compiledQuery.getType();
        if ((type == CompiledQuery.ALTER || type == CompiledQuery.UPDATE) && !executionContext.isWalApplication()) {
            compiledQuery.withSqlText(Chars.toString(sqlText));
        }
        compiledQuery.withContext(executionContext);
    }

    private InsertOperation compileInsert(InsertModel insertModel, SqlExecutionContext executionContext) throws SqlException {
        // todo: consider moving this method to InsertModel
        final ExpressionNode tableNameExpr = insertModel.getTableNameExpr();
        InsertOperationImpl insertOperation = null;
        Function timestampFunction = null;
        ObjList<Function> valueFunctions = null;
        TableToken token = tableExistsOrFail(tableNameExpr.position, tableNameExpr.token, executionContext);

        try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(token)) {
            final long metadataVersion = metadata.getMetadataVersion();
            insertOperation = new InsertOperationImpl(engine, metadata.getTableToken(), metadataVersion);
            final int metadataTimestampIndex = metadata.getTimestampIndex();
            final ObjList<CharSequence> columnNameList = insertModel.getColumnNameList();
            final int columnSetSize = columnNameList.size();
            int designatedTimestampPosition = 0;
            for (int tupleIndex = 0, n = insertModel.getRowTupleCount(); tupleIndex < n; tupleIndex++) {
                timestampFunction = null;
                listColumnFilter.clear();
                if (columnSetSize > 0) {
                    valueFunctions = new ObjList<>(columnSetSize);
                    for (int i = 0; i < columnSetSize; i++) {
                        int metadataColumnIndex = metadata.getColumnIndexQuiet(columnNameList.getQuick(i));
                        if (metadataColumnIndex > -1) {
                            final ExpressionNode node = insertModel.getRowTupleValues(tupleIndex).getQuick(i);
                            final Function function = functionParser.parseFunction(
                                    node,
                                    EmptyRecordMetadata.INSTANCE,
                                    executionContext
                            );

                            insertValidateFunctionAndAddToList(
                                    insertModel,
                                    tupleIndex,
                                    valueFunctions,
                                    metadata,
                                    metadataTimestampIndex,
                                    i,
                                    metadataColumnIndex,
                                    function,
                                    node.position,
                                    executionContext.getBindVariableService()
                            );

                            if (metadataTimestampIndex == metadataColumnIndex) {
                                timestampFunction = function;
                                designatedTimestampPosition = node.position;
                            }
                        } else {
                            throw SqlException.invalidColumn(insertModel.getColumnPosition(i), columnNameList.getQuick(i));
                        }
                    }
                } else {
                    final int columnCount = metadata.getColumnCount();
                    final ObjList<ExpressionNode> values = insertModel.getRowTupleValues(tupleIndex);
                    final int valueCount = values.size();
                    if (columnCount != valueCount) {
                        throw SqlException.$(
                                        insertModel.getEndOfRowTupleValuesPosition(tupleIndex),
                                        "row value count does not match column count [expected="
                                ).put(columnCount).put(", actual=").put(values.size())
                                .put(", tuple=").put(tupleIndex + 1).put(']');
                    }
                    valueFunctions = new ObjList<>(columnCount);

                    for (int i = 0; i < columnCount; i++) {
                        final ExpressionNode node = values.getQuick(i);

                        Function function = functionParser.parseFunction(node, EmptyRecordMetadata.INSTANCE, executionContext);
                        insertValidateFunctionAndAddToList(
                                insertModel,
                                tupleIndex,
                                valueFunctions,
                                metadata,
                                metadataTimestampIndex,
                                i,
                                i,
                                function,
                                node.position,
                                executionContext.getBindVariableService()
                        );

                        if (metadataTimestampIndex == i) {
                            timestampFunction = function;
                            designatedTimestampPosition = node.position;
                        }
                    }
                }

                // validate timestamp
                if (metadataTimestampIndex > -1) {
                    if (timestampFunction == null) {
                        throw SqlException.$(0, "insert statement must populate timestamp");
                    } else if (ColumnType.isNull(timestampFunction.getType()) || timestampFunction.isNullConstant()) {
                        throw SqlException.$(designatedTimestampPosition, "designated timestamp column cannot be NULL");
                    }
                }

                VirtualRecord record = new VirtualRecord(valueFunctions);
                RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(asm, record, metadata, listColumnFilter);
                insertOperation.addInsertRow(new InsertRowImpl(
                                record,
                                copier,
                                timestampFunction,
                                designatedTimestampPosition,
                                tupleIndex,
                                metadata.getTimestampType()
                        )
                );
            }
            return insertOperation;
        } catch (Throwable th) {
            Misc.free(insertOperation);
            Misc.free(timestampFunction);
            Misc.freeObjList(valueFunctions);
            throw th;
        }
    }

    private InsertOperation compileInsertAsSelect(ExecutionModel executionModel, SqlExecutionContext executionContext) throws SqlException {
        final InsertModel model = (InsertModel) executionModel;
        final ExpressionNode tableNameExpr = model.getTableNameExpr();
        TableToken tableToken = tableExistsOrFail(tableNameExpr.position, tableNameExpr.token, executionContext);
        RecordCursorFactory factory = null;

        try (TableRecordMetadata writerMetadata = executionContext.getMetadataForWrite(tableToken)) {
            final long metadataVersion = writerMetadata.getMetadataVersion();
            factory = generateSelectWithRetries(model.getQueryModel(), model, executionContext, true);
            final RecordMetadata cursorMetadata = factory.getMetadata();
            // Convert sparse writer metadata into dense
            final int writerTimestampIndex = writerMetadata.getTimestampIndex();
            final int cursorTimestampIndex = cursorMetadata.getTimestampIndex();
            final int cursorColumnCount = cursorMetadata.getColumnCount();

            final RecordToRowCopier copier;
            final ObjList<CharSequence> columnNameList = model.getColumnNameList();
            final int columnSetSize = columnNameList.size();
            int timestampIndexFound = -1;
            if (columnSetSize > 0) {
                // validate type cast
                // clear list column filter to re-populate it again
                listColumnFilter.clear();

                for (int i = 0; i < columnSetSize; i++) {
                    CharSequence columnName = columnNameList.get(i);
                    int index = writerMetadata.getColumnIndexQuiet(columnName);
                    if (index == -1) {
                        throw SqlException.invalidColumn(model.getColumnPosition(i), columnName);
                    }

                    int fromType = cursorMetadata.getColumnType(i);
                    int toType = writerMetadata.getColumnType(index);
                    if (ColumnType.isAssignableFrom(fromType, toType)) {
                        listColumnFilter.add(index + 1);
                    } else {
                        throw SqlException.inconvertibleTypes(
                                model.getColumnPosition(i),
                                fromType,
                                cursorMetadata.getColumnName(i),
                                toType,
                                writerMetadata.getColumnName(i)
                        );
                    }

                    if (index == writerTimestampIndex) {
                        timestampIndexFound = i;
                        if (!ColumnType.isTimestamp(fromType) && fromType != ColumnType.STRING) {
                            throw SqlException.$(tableNameExpr.position, "expected timestamp column but type is ").put(ColumnType.nameOf(fromType));
                        }
                    }
                }

                // fail when target table requires chronological data and cursor cannot provide it
                if (timestampIndexFound < 0 && writerTimestampIndex >= 0) {
                    throw SqlException.$(tableNameExpr.position, "select clause must provide timestamp column");
                }

                copier = RecordToRowCopierUtils.generateCopier(asm, cursorMetadata, writerMetadata, listColumnFilter);
            } else {
                // fail when target table requires chronological data and cursor cannot provide it
                if (writerTimestampIndex > -1 && cursorTimestampIndex == -1) {
                    if (cursorColumnCount <= writerTimestampIndex) {
                        throw SqlException.$(tableNameExpr.position, "select clause must provide timestamp column");
                    } else {
                        int columnType = ColumnType.tagOf(cursorMetadata.getColumnType(writerTimestampIndex));
                        if (columnType != ColumnType.TIMESTAMP && columnType != ColumnType.STRING && columnType != ColumnType.VARCHAR && columnType != ColumnType.NULL) {
                            throw SqlException.$(tableNameExpr.position, "expected timestamp column but type is ").put(ColumnType.nameOf(columnType));
                        }
                    }
                }

                if (writerTimestampIndex > -1 && cursorTimestampIndex > -1 && writerTimestampIndex != cursorTimestampIndex) {
                    throw SqlException
                            .$(tableNameExpr.position, "designated timestamp of existing table (").put(writerTimestampIndex)
                            .put(") does not match designated timestamp in select query (")
                            .put(cursorTimestampIndex)
                            .put(')');
                }
                timestampIndexFound = writerTimestampIndex;

                final int n = writerMetadata.getColumnCount();
                if (n > cursorMetadata.getColumnCount()) {
                    throw SqlException.$(model.getSelectKeywordPosition(), "not enough columns selected");
                }

                for (int i = 0; i < n; i++) {
                    int fromType = cursorMetadata.getColumnType(i);
                    int toType = writerMetadata.getColumnType(i);
                    if (ColumnType.isAssignableFrom(fromType, toType)) {
                        continue;
                    }

                    // We are going on a limp here. There is nowhere to position this error in our model.
                    // We will try to position on column (i) inside cursor's query model. Assumption is that
                    // it will always have a column, e.g. has been processed by optimiser
                    assert i < model.getQueryModel().getBottomUpColumns().size();
                    throw SqlException.inconvertibleTypes(
                            model.getQueryModel().getBottomUpColumns().getQuick(i).getAst().position,
                            fromType,
                            cursorMetadata.getColumnName(i),
                            toType,
                            writerMetadata.getColumnName(i)
                    );
                }

                entityColumnFilter.of(writerMetadata.getColumnCount());

                copier = RecordToRowCopierUtils.generateCopier(
                        asm,
                        cursorMetadata,
                        writerMetadata,
                        entityColumnFilter
                );
            }

            return new InsertAsSelectOperationImpl(engine, tableToken, factory, copier,
                    metadataVersion, timestampIndexFound, model.getBatchSize(), model.getO3MaxLag());
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        }
    }

    private void compileLegacyCheckpoint(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        executionContext.getSecurityContext().authorizeDatabaseSnapshot();
        CharSequence tok = expectToken(lexer, "'prepare' or 'complete'");

        if (Chars.equalsLowerCaseAscii(tok, "prepare")) {
            engine.snapshotCreate(executionContext);
            compiledQuery.ofCheckpointCreate();
        } else if (Chars.equalsLowerCaseAscii(tok, "complete")) {
            engine.checkpointRelease();
            compiledQuery.ofCheckpointRelease();
        } else {
            throw SqlException.position(lexer.lastTokenPosition()).put("'prepare' or 'complete' expected");
        }
    }

    private void compileMatViewQuery(
            @Transient @NotNull SqlExecutionContext executionContext,
            @NotNull CreateMatViewOperation createMatViewOp
    ) throws SqlException {
        final CreateTableOperation createTableOp = createMatViewOp.getCreateTableOperation();
        lexer.of(createTableOp.getSelectText());
        clear();

        SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        if (!circuitBreaker.isTimerSet()) {
            circuitBreaker.resetTimer();
        }
        final CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "SELECT query expected");
        }
        lexer.unparseLast();

        sqlText = createTableOp.getSelectText();
        compiledQuery.withContext(executionContext);

        final int startPos = lexer.getPosition();
        final long beginNanos = configuration.getNanosecondClock().getTicks();

        final int selectTextPosition = createTableOp.getSelectTextPosition();
        try {
            final QueryModel queryModel;
            try {
                final ExecutionModel executionModel = parser.parse(lexer, executionContext, this);
                if (executionModel.getModelType() != ExecutionModel.QUERY) {
                    throw SqlException.$(startPos, "SELECT query expected");
                }
                queryModel = optimiser.optimise((QueryModel) executionModel, executionContext, this);
            } catch (SqlException e) {
                e.setPosition(e.getPosition() + selectTextPosition);
                throw e;
            }
            createMatViewOp.validateAndUpdateMetadataFromModel(executionContext, optimiser.getFunctionFactoryCache(), queryModel);

            final boolean ogAllowNonDeterministic = executionContext.allowNonDeterministicFunctions();
            executionContext.setAllowNonDeterministicFunction(false);
            try {
                compiledQuery.ofSelect(generateSelectWithRetries(queryModel, null, executionContext, false));
            } catch (SqlException e) {
                e.setPosition(e.getPosition() + selectTextPosition);
                throw e;
            } finally {
                executionContext.setAllowNonDeterministicFunction(ogAllowNonDeterministic);
            }
        } catch (Throwable th) {
            QueryProgress.logError(th, -1, sqlText, executionContext, beginNanos);
            throw th;
        }
    }

    private void compileRefresh(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        CharSequence tok = expectToken(lexer, "'materialized'");
        if (!isMaterializedKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'materialized' expected'");
        }

        tok = expectToken(lexer, "'view'");
        if (!isViewKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'view' expected'");
        }

        tok = expectToken(lexer, "materialized view name");
        if (isSemicolon(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "materialized view name expected");
        }
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());

        final CharSequence matViewName = unquote(tok);
        final TableToken matViewToken = executionContext.getTableTokenIfExists(matViewName);
        if (matViewToken == null) {
            throw SqlException.matViewDoesNotExist(lexer.lastTokenPosition(), matViewName);
        }
        if (!matViewToken.isMatView()) {
            throw SqlException.$(lexer.lastTokenPosition(), "materialized view name expected, got table name");
        }

        final MatViewDefinition matViewDefinition = engine.getMatViewGraph().getViewDefinition(matViewToken);
        if (matViewDefinition == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "materialized view does not exist or is not ready for refresh");
        }
        final TimestampDriver driver = matViewDefinition.getBaseTableTimestampDriver();

        tok = expectToken(lexer, "'full' or 'incremental' or 'range'");
        final boolean fullRefresh = isFullKeyword(tok);
        long from = Numbers.LONG_NULL;
        long to = Numbers.LONG_NULL;
        if (isRangeKeyword(tok)) {
            expectKeyword(lexer, "from");
            tok = expectToken(lexer, "FROM timestamp");
            try {
                from = driver.parseFloorLiteral(unquote(tok));
            } catch (NumericException e) {
                throw SqlException.$(lexer.lastTokenPosition(), "invalid FROM timestamp value");
            }
            expectKeyword(lexer, "to");
            tok = expectToken(lexer, "TO timestamp");
            try {
                to = driver.parseFloorLiteral(unquote(tok));
            } catch (NumericException e) {
                throw SqlException.$(lexer.lastTokenPosition(), "invalid TO timestamp value");
            }
            if (from > to) {
                throw SqlException.$(lexer.lastTokenPosition(), "TO timestamp must not be earlier than FROM timestamp");
            }
        } else if (!fullRefresh && !isIncrementalKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'full' or 'incremental' or 'range' expected");
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok != null && !isSemicolon(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [").put(tok).put("] while trying to refresh materialized view");
        }

        executionContext.getSecurityContext().authorizeMatViewRefresh(matViewToken);

        final MatViewStateStore matViewStateStore = engine.getMatViewStateStore();
        if (fullRefresh) {
            matViewStateStore.enqueueFullRefresh(matViewToken);
        } else if (from != Numbers.LONG_NULL) {
            matViewStateStore.enqueueRangeRefresh(matViewToken, from, to);
        } else {
            matViewStateStore.enqueueIncrementalRefresh(matViewToken);
        }
        compiledQuery.ofRefreshMatView();
    }

    private void compileReindex(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !isTableKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "TABLE expected");
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || Chars.equals(tok, ',')) {
            throw SqlException.$(lexer.getPosition(), "table name expected");
        }

        if (Chars.isQuoted(tok)) {
            tok = unquote(tok);
        }
        final TableToken tableToken = tableExistsOrFail(lexer.lastTokenPosition(), tok, executionContext);

        checkMatViewModification(tableToken);

        try (IndexBuilder indexBuilder = new IndexBuilder(configuration)) {
            indexBuilder.of(path.of(configuration.getDbRoot()).concat(tableToken.getDirName()));

            tok = SqlUtil.fetchNext(lexer);
            CharSequence columnName = null;

            if (tok != null && isColumnKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);
                if (Chars.isQuoted(tok)) {
                    tok = unquote(tok);
                }
                if (tok == null || TableUtils.isValidColumnName(tok, configuration.getMaxFileNameLength())) {
                    columnName = GenericLexer.immutableOf(tok);
                    tok = SqlUtil.fetchNext(lexer);
                }
            }

            CharSequence partition = null;
            if (tok != null && isPartitionKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);

                if (Chars.isQuoted(tok)) {
                    tok = unquote(tok);
                }
                partition = tok;
                tok = SqlUtil.fetchNext(lexer);
            }

            if (tok == null || !isLockKeyword(tok)) {
                throw SqlException.$(lexer.getPosition(), "LOCK EXCLUSIVE expected");
            }

            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || !isExclusiveKeyword(tok)) {
                throw SqlException.$(lexer.getPosition(), "LOCK EXCLUSIVE expected");
            }

            tok = SqlUtil.fetchNext(lexer);
            if (tok != null && !isSemicolon(tok)) {
                throw SqlException.$(lexer.getPosition(), "EOF expected");
            }

            columnNames.clear();
            if (columnName != null) {
                columnNames.add(columnName);
            }
            executionContext.getSecurityContext().authorizeTableReindex(tableToken, columnNames);
            indexBuilder.reindex(partition, columnName);
        }
        compiledQuery.ofRepair();
    }

    private void compileRollback(SqlExecutionContext executionContext, @Transient CharSequence sqlText) {
        compiledQuery.ofRollback();
    }

    private void compileSet(SqlExecutionContext executionContext, @Transient CharSequence sqlText) {
        compiledQuery.ofSet();
    }

    private void compileTruncate(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);

        if (tok == null) {
            throw SqlException.$(lexer.getPosition(), "TABLE expected");
        }

        if (!isTableKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "TABLE expected");
        }

        tok = SqlUtil.fetchNext(lexer);
        boolean hasIfExists = false;
        if (tok != null && isIfKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || !isExistsKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "expected ").put("EXISTS table-name");
            }
            hasIfExists = true;
            tok = SqlUtil.fetchNext(lexer);
        }

        if (tok != null && isOnlyKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
        }

        if (tok != null && isWithKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "table name expected");
        }

        tableWriters.clear();
        try {
            do {
                if (tok == null || Chars.equals(tok, ',')) {
                    throw SqlException.$(lexer.getPosition(), "table name expected");
                }

                if (Chars.isQuoted(tok)) {
                    tok = unquote(tok);
                }

                final TableToken tableToken = executionContext.getTableTokenIfExists(tok);
                if (tableToken == null && !hasIfExists) {
                    throw SqlException.$(lexer.lastTokenPosition(), "table does not exist [table=").put(tok).put(']');
                }
                if (tableToken != null) {
                    checkMatViewModification(tableToken);
                    executionContext.getSecurityContext().authorizeTableTruncate(tableToken);
                    try {
                        tableWriters.add(engine.getTableWriterAPI(tableToken, "truncateTables"));
                    } catch (CairoException e) {
                        LOG.info().$("table busy [table=").$(tok).$(", e=").$((Throwable) e).I$();
                        throw SqlException.$(lexer.lastTokenPosition(), "table '").put(tok).put("' could not be truncated: ").put(e);
                    }
                }

                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || Chars.equals(tok, ';') || isKeepKeyword(tok)) {
                    break;
                }
                if (!Chars.equalsNc(tok, ',')) {
                    throw SqlException.$(lexer.getPosition(), "',' or 'keep' expected");
                }

                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && isKeepKeyword(tok)) {
                    throw SqlException.$(lexer.getPosition(), "table name expected");
                }
            } while (true);

            boolean keepSymbolTables = false;
            if (tok != null && isKeepKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || !isSymbolKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "SYMBOL expected");
                }
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || !isMapsKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "MAPS expected");
                }
                keepSymbolTables = true;
                tok = SqlUtil.fetchNext(lexer);
            }

            if (tok != null && !Chars.equals(tok, ';')) {
                throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
            }

            final long queryId = queryRegistry.register(sqlText, executionContext);
            try {
                for (int i = 0, n = tableWriters.size(); i < n; i++) {
                    final TableWriterAPI writer = tableWriters.getQuick(i);
                    try {
                        if (writer.getMetadata().isWalEnabled()) {
                            writer.truncateSoft();
                        } else {
                            TableToken tableToken = writer.getTableToken();
                            if (engine.lockReaders(tableToken)) {
                                try {
                                    if (keepSymbolTables) {
                                        writer.truncateSoft();
                                    } else {
                                        writer.truncate();
                                    }
                                } finally {
                                    engine.unlockReaders(tableToken);
                                }
                            } else {
                                throw SqlException.$(0, "there is an active query against '").put(tableToken.getTableName()).put("'. Try again.");
                            }
                        }
                    } catch (CairoException | CairoError e) {
                        LOG.error().$("could not truncate [table=").$(writer.getTableToken()).$(", e=").$((Sinkable) e).I$();
                        throw e;
                    }
                }
            } finally {
                queryRegistry.unregister(queryId, executionContext);
            }
        } finally {
            Misc.freeObjListAndClear(tableWriters);
        }
        compiledQuery.ofTruncate();
    }

    private void compileUsingModel(SqlExecutionContext executionContext, long beginNanos, boolean generateProgressLogger) throws SqlException {
        // This method will not populate sql cache directly; factories are assumed to be non-reentrant, and once
        // factory is out of this method, the caller assumes full ownership over it. However, the caller may
        // choose to return the factory back to this or any other instance of compiler for safekeeping

        // lexer would have parsed first token to determine direction of execution flow
        lexer.unparseLast();
        codeGenerator.clear();
        long sqlId = -1;

        ExecutionModel executionModel = null;
        try {
            executionModel = compileExecutionModel(executionContext);
            switch (executionModel.getModelType()) {
                case ExecutionModel.QUERY:
                    compiledQuery.ofSelect(
                            generateSelectWithRetries(
                                    (QueryModel) executionModel,
                                    null,
                                    executionContext,
                                    generateProgressLogger
                            )
                    );
                    break;
                case ExecutionModel.CREATE_TABLE:
                    compiledQuery.ofCreateTable(((CreateTableOperationBuilder) executionModel)
                            .build(this, executionContext, sqlText));
                    break;
                case ExecutionModel.CREATE_MAT_VIEW:
                    compiledQuery.ofCreateMatView(((CreateMatViewOperationBuilder) executionModel)
                            .build(this, executionContext, sqlText));
                    break;
                case ExecutionModel.COPY:
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    if (executionModel.getTableName() != null) {
                        assert executionModel.getModelType() == ExecutionModel.COPY;
                        if (((ExportModel) executionModel).getType() == COPY_TYPE_FROM) {
                            checkMatViewModification(executionModel);
                        }
                    }
                    copy(executionContext, (ExportModel) executionModel);
                    QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos);
                    break;
                case ExecutionModel.RENAME_TABLE:
                    sqlId = queryRegistry.register(sqlText, executionContext);
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    checkMatViewModification(executionModel);
                    final RenameTableModel rtm = (RenameTableModel) executionModel;
                    engine.rename(
                            executionContext.getSecurityContext(),
                            path,
                            mem,
                            unquote(rtm.getFrom().token),
                            renamePath,
                            unquote(rtm.getTo().token)
                    );
                    QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos);
                    compiledQuery.ofRenameTable();
                    break;
                case ExecutionModel.UPDATE:
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    checkMatViewModification(executionModel);
                    final QueryModel updateQueryModel = (QueryModel) executionModel;
                    TableToken tableToken = executionContext.getTableToken(updateQueryModel.getTableName());
                    try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(tableToken)) {
                        compiledQuery.ofUpdate(generateUpdate(updateQueryModel, executionContext, metadata));
                    }
                    QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos);
                    // update is delayed until operation execution (for non-wal tables) or pushed to wal job completely
                    break;
                case ExecutionModel.EXPLAIN:
                    sqlId = queryRegistry.register(sqlText, executionContext);
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    compiledQuery.ofExplain(generateExplain((ExplainModel) executionModel, executionContext));
                    QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos);
                    break;
                default:
                    checkMatViewModification(executionModel);
                    final InsertModel insertModel = (InsertModel) executionModel;
                    // we use SQL Compiler state (reusing objects) to generate InsertOperation
                    if (insertModel.getQueryModel() != null) {
                        // InsertSelect progress will be recorded during the execute phase, to accurately reflect its real select progress.
                        compiledQuery.ofInsert(compileInsertAsSelect(insertModel, executionContext), true);
                    } else {
                        QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                        compiledQuery.ofInsert(compileInsert(insertModel, executionContext), false);
                        QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos);
                    }
                    break;
            }

            short type = compiledQuery.getType();
            if (type == CompiledQuery.EXPLAIN
                    || type == CompiledQuery.RENAME_TABLE  // non-wal rename table is complete at this point
            ) {
                queryRegistry.unregister(sqlId, executionContext);
            }
        } catch (Throwable th) {
            if (executionModel != null) {
                freeTableNameFunctions(executionModel.getQueryModel());
            }
            // unregister query on error
            queryRegistry.unregister(sqlId, executionContext);

            QueryProgress.logError(th, sqlId, sqlText, executionContext, beginNanos);
            throw th;
        }
    }

    private void compileVacuum(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
        CharSequence tok = expectToken(lexer, "'table'");
        // It used to be VACUUM PARTITIONS but become VACUUM TABLE
        boolean partitionsKeyword = isPartitionsKeyword(tok);
        if (partitionsKeyword || isTableKeyword(tok)) {
            tok = expectToken(lexer, "table name");
            assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            CharSequence tableName = GenericLexer.assertNoDotsAndSlashes(unquote(tok), lexer.lastTokenPosition());
            int tableNamePos = lexer.lastTokenPosition();
            CharSequence eol = SqlUtil.fetchNext(lexer);
            if (eol == null || Chars.equals(eol, ';')) {
                final TableToken tableToken = tableExistsOrFail(lexer.lastTokenPosition(), tableName, executionContext);
                checkMatViewModification(tableToken);
                try (TableReader rdr = executionContext.getReader(tableToken)) {
                    int partitionBy = rdr.getMetadata().getPartitionBy();
                    if (PartitionBy.isPartitioned(partitionBy)) {
                        executionContext.getSecurityContext().authorizeTableVacuum(rdr.getTableToken());
                        if (!TableUtils.schedulePurgeO3Partitions(
                                messageBus,
                                rdr.getTableToken(),
                                rdr.getMetadata().getTimestampType(),
                                partitionBy
                        )) {
                            throw SqlException.$(
                                    tableNamePos,
                                    "cannot schedule vacuum action, queue is full, please retry " +
                                            "or increase Purge Discovery Queue Capacity"
                            );
                        }
                    } else if (partitionsKeyword) {
                        throw SqlException.$(lexer.lastTokenPosition(), "table '").put(tableName).put("' is not partitioned");
                    }
                    vacuumColumnVersions.run(rdr);
                    compiledQuery.ofVacuum();
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "end of line or ';' expected");
            }
        } else {
            throw SqlException.$(lexer.lastTokenPosition(), "'partitions' expected");
        }
    }

    private void copy(SqlExecutionContext executionContext, ExportModel exportModel) throws SqlException {
        if (!exportModel.isCancel() && Chars.equalsLowerCaseAscii(exportModel.getFileName().token, "stdin")) {
            // no-op implementation
            authorizeInsertForCopy(executionContext.getSecurityContext(), exportModel);
            compiledQuery.ofCopyRemote();
        } else {
            final RecordCursorFactory copyFactory;
            if (exportModel.isCancel()) {
                copyFactory = compileCopyCancel(executionContext, exportModel);
            } else {
                if (exportModel.getType() == COPY_TYPE_FROM) {
                    copyFactory = compileCopyFrom(executionContext.getSecurityContext(), exportModel);
                } else {
                    copyFactory = compileCopyTo(executionContext.getSecurityContext(), exportModel, sqlText);
                }
            }
            compiledQuery.ofPseudoSelect(copyFactory);
        }
    }

    // returns number of copied rows
    private long copyTableData(
            SqlExecutionContext context,
            RecordCursor cursor,
            RecordMetadata metadata,
            TableWriterAPI writer,
            RecordMetadata writerMetadata,
            RecordToRowCopier recordToRowCopier,
            long batchSize,
            long o3MaxLag,
            @NotNull CopyDataProgressReporter reporter,
            int reportFrequency
    ) {
        int timestampIndex = writerMetadata.getTimestampIndex();
        long rowCount;
        if (timestampIndex == -1) {
            rowCount = copyUnordered(context, cursor, writer, recordToRowCopier, reporter, reportFrequency);
        } else if (batchSize != -1) {
            rowCount = copyOrderedBatched(context, writer, metadata, cursor, recordToRowCopier, timestampIndex, batchSize, o3MaxLag, reporter, configuration.getParquetExportCopyReportFrequencyLines());
        } else {
            rowCount = copyOrderedBatched(context, writer, metadata, cursor, recordToRowCopier, timestampIndex, Long.MAX_VALUE, o3MaxLag, reporter, configuration.getParquetExportCopyReportFrequencyLines());
        }
        writer.commit();
        return rowCount;
    }

    /**
     * Sets insertCount to number of copied rows.
     */
    private void copyTableDataAndUnlock(
            SqlExecutionContext context,
            TableToken tableToken,
            boolean isWalEnabled,
            RecordCursor cursor,
            RecordMetadata cursorMetadata,
            long batchSize,
            long o3MaxLag,
            CopyDataProgressReporter reporter
    ) {
        TableWriterAPI writerAPI = null;
        TableWriter writer = null;

        try {
            if (!isWalEnabled) {
                writerAPI = writer = new TableWriter(
                        engine.getConfiguration(),
                        tableToken,
                        engine.getMessageBus(),
                        null,
                        false,
                        DefaultLifecycleManager.INSTANCE,
                        engine.getConfiguration().getDbRoot(),
                        engine.getDdlListener(tableToken),
                        engine.getCheckpointStatus(),
                        engine
                );
            } else {
                writerAPI = engine.getTableWriterAPI(tableToken, "create as select");
            }

            RecordMetadata writerMetadata = writerAPI.getMetadata();
            entityColumnFilter.of(writerMetadata.getColumnCount());
            this.insertCount = copyTableData(
                    context,
                    cursor,
                    cursorMetadata,
                    writerAPI,
                    writerMetadata,
                    RecordToRowCopierUtils.generateCopier(
                            asm,
                            cursorMetadata,
                            writerMetadata,
                            entityColumnFilter
                    ),
                    batchSize,
                    o3MaxLag,
                    reporter,
                    configuration.getParquetExportCopyReportFrequencyLines()
            );
        } catch (CairoException e) {
            // Close writer, the table will be removed
            writerAPI = Misc.free(writerAPI);
            writer = null;
            throw e;
        } finally {
            if (isWalEnabled) {
                Misc.free(writerAPI);
            } else {
                engine.unlock(context.getSecurityContext(), tableToken, writer, false);
            }
        }
    }

    private void executeCreateMatView(CreateMatViewOperation createMatViewOp, SqlExecutionContext executionContext) throws SqlException {
        if (createMatViewOp.getRefreshType() != MatViewDefinition.REFRESH_TYPE_IMMEDIATE
                && createMatViewOp.getRefreshType() != MatViewDefinition.REFRESH_TYPE_TIMER
                && createMatViewOp.getRefreshType() != MatViewDefinition.REFRESH_TYPE_MANUAL) {
            throw SqlException.$(createMatViewOp.getTableNamePosition(), "unexpected refresh type: ").put(createMatViewOp.getRefreshType());
        }

        final long sqlId = queryRegistry.register(createMatViewOp.getSqlText(), executionContext);
        final long beginNanos = configuration.getNanosecondClock().getTicks();
        QueryProgress.logStart(sqlId, createMatViewOp.getSqlText(), executionContext, false);
        try {
            final int status = executionContext.getTableStatus(path, createMatViewOp.getTableName());
            if (status == TableUtils.TABLE_EXISTS) {
                final TableToken tt = executionContext.getTableTokenIfExists(createMatViewOp.getTableName());
                if (tt != null && !tt.isMatView()) {
                    throw SqlException.$(createMatViewOp.getTableNamePosition(), "table with the requested name already exists");
                }
                if (createMatViewOp.ignoreIfExists()) {
                    createMatViewOp.updateOperationFutureTableToken(tt);
                } else {
                    throw SqlException.$(createMatViewOp.getTableNamePosition(), "materialized view already exists");
                }
            } else {
                CharSequence volumeAlias = createMatViewOp.getVolumeAlias();
                if (volumeAlias != null) {
                    CharSequence volumePath = configuration.getVolumeDefinitions().resolveAlias(volumeAlias);
                    if (volumePath != null) {
                        if (!ff.isDirOrSoftLinkDir(path.of(volumePath).$())) {
                            throw CairoException.critical(0).position(createMatViewOp.getVolumePosition())
                                    .put("not a valid path for volume [alias=")
                                    .put(volumeAlias).put(", path=").put(path).put(']');
                        }
                    } else {
                        throw SqlException.position(createMatViewOp.getVolumePosition()).put("volume alias is not allowed [alias=")
                                .put(volumeAlias).put(']');
                    }
                }

                final MatViewDefinition matViewDefinition;
                final TableToken matViewToken;

                final CreateTableOperation createTableOp = createMatViewOp.getCreateTableOperation();
                if (createTableOp.getSelectText() != null) {
                    RecordCursorFactory newFactory = null;
                    RecordCursor newCursor;
                    for (int retryCount = 0; ; retryCount++) {
                        try {
                            compileMatViewQuery(executionContext, createMatViewOp);
                            Misc.free(newFactory);
                            newFactory = compiledQuery.getRecordCursorFactory();
                            newCursor = newFactory.getCursor(executionContext);
                            break;
                        } catch (TableReferenceOutOfDateException e) {
                            if (retryCount == maxRecompileAttempts) {
                                Misc.free(newFactory);
                                throw SqlException.$(0, e.getFlyweightMessage());
                            }
                            LOG.info().$("retrying plan [q=`").$(createTableOp.getSelectText()).$("`]").$();
                        } catch (Throwable th) {
                            Misc.free(newFactory);
                            throw th;
                        }
                    }

                    try {
                        final RecordMetadata metadata = newFactory.getMetadata();
                        try (TableReader baseReader = engine.getReader(createMatViewOp.getBaseTableName())) {
                            createMatViewOp.validateAndUpdateMetadataFromSelect(metadata, baseReader.getMetadata(), newFactory.getScanDirection());
                        }

                        matViewDefinition = engine.createMatView(
                                executionContext.getSecurityContext(),
                                mem,
                                blockFileWriter,
                                path,
                                createMatViewOp.ignoreIfExists(),
                                createMatViewOp,
                                !createMatViewOp.isWalEnabled(),
                                volumeAlias != null
                        );
                        matViewToken = matViewDefinition.getMatViewToken();
                    } finally {
                        Misc.free(newCursor);
                        Misc.free(newFactory);
                    }

                    createMatViewOp.updateOperationFutureTableToken(matViewToken);
                } else {
                    throw SqlException.$(createTableOp.getTableNamePosition(), "materialized view requires a SELECT statement");
                }
            }
            QueryProgress.logEnd(sqlId, createMatViewOp.getSqlText(), executionContext, beginNanos);
        } catch (Throwable th) {
            if (th instanceof CairoException) {
                ((CairoException) th).position(createMatViewOp.getTableNamePosition());
            }
            QueryProgress.logError(th, sqlId, createMatViewOp.getSqlText(), executionContext, beginNanos);
            throw th;
        } finally {
            queryRegistry.unregister(sqlId, executionContext);
        }
    }

    private void executeCreateTable(CreateTableOperation createTableOp, SqlExecutionContext executionContext) throws SqlException {
        boolean needRegister = createTableOp.needRegister();
        long sqlId = 0;
        long beginNanos = 0;
        if (needRegister) {
            sqlId = queryRegistry.register(createTableOp.getSqlText(), executionContext);
            beginNanos = configuration.getNanosecondClock().getTicks();
            QueryProgress.logStart(sqlId, createTableOp.getSqlText(), executionContext, false);
            executionContext.setUseSimpleCircuitBreaker(true);
        }

        try {
            // Fast path for CREATE TABLE IF NOT EXISTS in scenario when the table already exists
            final int status = executionContext.getTableStatus(path, createTableOp.getTableName());
            if (status == TableUtils.TABLE_EXISTS) {
                final TableToken tt = executionContext.getTableTokenIfExists(createTableOp.getTableName());
                if (tt != null && tt.isMatView()) {
                    throw SqlException.$(createTableOp.getTableNamePosition(), "materialized view with the requested name already exists");
                }
                if (createTableOp.ignoreIfExists()) {
                    createTableOp.updateOperationFutureTableToken(tt);
                } else {
                    throw SqlException.$(createTableOp.getTableNamePosition(), "table already exists");
                }
            } else {
                // create table (...) ... in volume volumeAlias;
                CharSequence volumeAlias = createTableOp.getVolumeAlias();
                if (volumeAlias != null) {
                    CharSequence volumePath = configuration.getVolumeDefinitions().resolveAlias(volumeAlias);
                    if (volumePath != null) {
                        if (!ff.isDirOrSoftLinkDir(path.of(volumePath).$())) {
                            throw CairoException.critical(0).position(createTableOp.getVolumePosition())
                                    .put("not a valid path for volume [alias=")
                                    .put(volumeAlias).put(", path=").put(path).put(']');
                        }
                    } else {
                        throw SqlException.position(createTableOp.getVolumePosition()).put("volume alias is not allowed [alias=")
                                .put(volumeAlias).put(']');
                    }
                }

                final TableToken tableToken;
                if (createTableOp.getSelectText() != null) {
                    this.insertCount = -1;
                    final int position = createTableOp.getTableNamePosition();
                    RecordCursorFactory newFactory = null;
                    RecordCursor newCursor;
                    for (int retryCount = 0; ; retryCount++) {
                        try {
                            lexer.of(createTableOp.getSelectText());
                            clearExceptSqlText();
                            compileInner(executionContext, createTableOp.getSelectText(), false);
                            Misc.free(newFactory);
                            newFactory = compiledQuery.getRecordCursorFactory();
                            newCursor = newFactory.getCursor(executionContext);
                            break;
                        } catch (TableReferenceOutOfDateException e) {
                            if (retryCount == maxRecompileAttempts) {
                                Misc.free(newFactory);
                                throw SqlException.$(createTableOp.getSelectTextPosition(), e.getFlyweightMessage());
                            }
                            LOG.info().$("retrying plan [q=`").$(createTableOp.getSelectText()).$("`]").$();
                        } catch (SqlException e) {
                            e.setPosition(e.getPosition() + createTableOp.getSelectTextPosition());
                            Misc.free(newFactory);
                            throw e;
                        } catch (Throwable th) {
                            Misc.free(newFactory);
                            throw th;
                        }
                    }

                    try (
                            RecordCursorFactory factory = newFactory;
                            RecordCursor cursor = newCursor
                    ) {
                        final RecordMetadata metadata = factory.getMetadata();
                        createTableOp.validateAndUpdateMetadataFromSelect(metadata, factory.getScanDirection());
                        boolean keepLock = !createTableOp.isWalEnabled();

                        // todo: test create table if exists with select
                        tableToken = engine.createTable(
                                executionContext.getSecurityContext(),
                                mem,
                                path,
                                createTableOp.ignoreIfExists(),
                                createTableOp,
                                keepLock,
                                volumeAlias != null,
                                createTableOp.getTableKind()
                        );

                        try {
                            copyTableDataAndUnlock(
                                    executionContext,
                                    tableToken,
                                    createTableOp.isWalEnabled(),
                                    cursor,
                                    metadata,
                                    createTableOp.getBatchSize(),
                                    createTableOp.getBatchO3MaxLag(),
                                    createTableOp.getCopyDataProgressReporter()
                            );
                        } catch (CairoException e) {
                            e.position(position);
                            LogRecord record = LOG.error()
                                    .$("could not create table as select [message=").$safe(e.getFlyweightMessage());
                            if (!e.isCancellation()) {
                                record.$(", errno=").$(e.getErrno());
                            }
                            record.I$();
                            engine.dropTableOrMatView(path, tableToken);
                            engine.unlockTableName(tableToken);
                            throw e;
                        }
                    }
                    createTableOp.updateOperationFutureTableToken(tableToken);
                    createTableOp.updateOperationFutureAffectedRowsCount(insertCount);
                } else {
                    try {
                        if (createTableOp.getLikeTableName() != null) {
                            TableToken likeTableToken = executionContext.getTableTokenIfExists(createTableOp.getLikeTableName());
                            if (likeTableToken == null) {
                                throw SqlException
                                        .$(createTableOp.getLikeTableNamePosition(), "table does not exist [table=")
                                        .put(createTableOp.getLikeTableName()).put(']');
                            }

                            try (TableMetadata likeTableMetadata = executionContext.getCairoEngine().getTableMetadata(likeTableToken)) {
                                createTableOp.updateFromLikeTableMetadata(likeTableMetadata);
                                tableToken = engine.createTable(
                                        executionContext.getSecurityContext(),
                                        mem,
                                        path,
                                        createTableOp.ignoreIfExists(),
                                        createTableOp,
                                        false,
                                        volumeAlias != null,
                                        TABLE_KIND_REGULAR_TABLE
                                );
                            }
                        } else {
                            tableToken = engine.createTable(
                                    executionContext.getSecurityContext(),
                                    mem,
                                    path,
                                    createTableOp.ignoreIfExists(),
                                    createTableOp,
                                    false,
                                    volumeAlias != null,
                                    TABLE_KIND_REGULAR_TABLE
                            );
                        }
                        createTableOp.updateOperationFutureTableToken(tableToken);
                    } catch (EntryUnavailableException e) {
                        throw SqlException.$(createTableOp.getTableNamePosition(), "table already exists");
                    } catch (CairoException e) {
                        if (e.isAuthorizationError() || e.isCancellation()) {
                            // No point printing stack trace for authorization or cancellation errors
                            LOG.error().$("could not create table [error=").$safe(e.getFlyweightMessage()).I$();
                        } else {
                            LOG.error().$("could not create table [error=").$((Throwable) e).I$();
                        }
                        if (e.isInterruption()) {
                            throw e;
                        }
                        throw SqlException.$(createTableOp.getTableNamePosition(), "Could not create table, ")
                                .put(e.getFlyweightMessage());
                    }
                }
            }
            if (needRegister) {
                QueryProgress.logEnd(sqlId, createTableOp.getSqlText(), executionContext, beginNanos);
            }
        } catch (Throwable e) {
            if (e instanceof CairoException) {
                ((CairoException) e).position(createTableOp.getTableNamePosition());
            }
            if (needRegister) {
                QueryProgress.logError(e, sqlId, createTableOp.getSqlText(), executionContext, beginNanos);
            }
            throw e;
        } finally {
            if (needRegister) {
                executionContext.setUseSimpleCircuitBreaker(false);
                queryRegistry.unregister(sqlId, executionContext);
            }
        }
    }

    private void executeDropAllTables(SqlExecutionContext executionContext) {
        // collect table names
        dropAllTablesFailedTableNames.clear();
        tableTokenBucket.clear();
        engine.getTableTokens(tableTokenBucket, false);
        final SecurityContext securityContext = executionContext.getSecurityContext();
        TableToken tableToken;
        for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
            tableToken = tableTokenBucket.get(i);
            if (!tableToken.isSystem()) {
                if (tableToken.isMatView()) {
                    securityContext.authorizeMatViewDrop(tableToken);
                } else {
                    securityContext.authorizeTableDrop(tableToken);
                }
                try {
                    engine.dropTableOrMatView(path, tableToken);
                } catch (CairoException report) {
                    // it will fail when there are readers/writers and lock cannot be acquired
                    dropAllTablesFailedTableNames.put(tableToken.getTableName(), report.getMessage());
                }
            }
        }
        if (dropAllTablesFailedTableNames.size() > 0) {
            final CairoException ex = CairoException.nonCritical().put("failed to drop tables and materialized views [");
            CharSequence tableName;
            String reason;
            final ObjList<CharSequence> keys = dropAllTablesFailedTableNames.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                tableName = keys.get(i);
                reason = dropAllTablesFailedTableNames.get(tableName);
                ex.put('\'').put(tableName).put("': ").put(reason);
                if (i + 1 < n) {
                    ex.put(", ");
                }
            }
            throw ex.put(']');
        }
    }

    private void executeDropMatView(
            GenericDropOperation op,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final TableToken tableToken = sqlExecutionContext.getTableTokenIfExists(op.getEntityName());
        if (tableToken == null || TableNameRegistry.isLocked(tableToken)) {
            if (op.ifExists()) {
                return;
            }
            throw SqlException.matViewDoesNotExist(op.getEntityNamePosition(), op.getEntityName());
        }
        if (!tableToken.isMatView()) {
            throw SqlException.$(op.getEntityNamePosition(), "materialized view name expected, got table name");
        }
        sqlExecutionContext.getSecurityContext().authorizeMatViewDrop(tableToken);

        final String sqlText = op.getSqlText();
        final long queryId = queryRegistry.register(sqlText, sqlExecutionContext);
        try {
            engine.dropTableOrMatView(path, tableToken);
        } catch (CairoException ex) {
            if ((ex.isTableDropped() || ex.isTableDoesNotExist()) && op.ifExists()) {
                // all good, mat view dropped already
                return;
            } else if (!op.ifExists() && ex.isTableDropped()) {
                // Concurrently dropped, this should report mat view does not exist
                throw CairoException.matViewDoesNotExist(op.getEntityName());
            }
            throw ex;
        } finally {
            queryRegistry.unregister(queryId, sqlExecutionContext);
        }
    }

    private void executeDropTable(
            GenericDropOperation op,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final TableToken tableToken = sqlExecutionContext.getTableTokenIfExists(op.getEntityName());
        if (tableToken == null || TableNameRegistry.isLocked(tableToken)) {
            if (op.ifExists()) {
                return;
            }
            throw SqlException.tableDoesNotExist(op.getEntityNamePosition(), op.getEntityName());
        }
        if (tableToken.isMatView()) {
            throw SqlException.$(op.getEntityNamePosition(), "table name expected, got materialized view name: ").put(op.getEntityName());
        }
        sqlExecutionContext.getSecurityContext().authorizeTableDrop(tableToken);

        final String sqlText = op.getSqlText();
        final long queryId = queryRegistry.register(sqlText, sqlExecutionContext);
        try {
            engine.dropTableOrMatView(path, tableToken);
        } catch (CairoException ex) {
            if ((ex.isTableDropped() || ex.isTableDoesNotExist()) && op.ifExists()) {
                // all good, table dropped already
                return;
            } else if (!op.ifExists() && ex.isTableDropped()) {
                // Concurrently dropped, this should report table does not exist
                throw CairoException.tableDoesNotExist(op.getEntityName());
            }
            throw ex;
        } finally {
            queryRegistry.unregister(queryId, sqlExecutionContext);
        }
    }

    private int filterApply(
            Function filter,
            int functionPosition,
            AlterOperationBuilder changePartitionStatement,
            long timestamp
    ) {
        partitionFunctionRec.setTimestamp(timestamp);
        if (filter.getBool(partitionFunctionRec)) {
            changePartitionStatement.addPartitionToList(timestamp, functionPosition);
            return 1;
        }
        return 0;
    }

    private int filterPartitions(
            Function filter,
            int filterPosition,
            TableReader reader,
            AlterOperationBuilder changePartitionStatement
    ) {
        int affectedPartitions = 0;
        // Iterate partitions in descending order, so if folders are missing on disk,
        // removePartition does not fail to determine the next minTimestamp
        final int partitionCount = reader.getPartitionCount();
        if (partitionCount > 0) { // table may be empty
            // perform the action on the first and last partition in the end, those are more expensive than others
            long firstPartition = reader.getTxFile().getPartitionFloor(reader.getPartitionTimestampByIndex(0));
            long lastPartition = reader.getTxFile().getPartitionFloor(reader.getPartitionTimestampByIndex(partitionCount - 1));

            long lastLogicalPartition = Long.MIN_VALUE;
            for (int partitionIndex = 1; partitionIndex < partitionCount - 1; partitionIndex++) {
                long physicalTimestamp = reader.getPartitionTimestampByIndex(partitionIndex);
                long logicalTimestamp = reader.getTxFile().getPartitionFloor(physicalTimestamp);
                if (logicalTimestamp != lastLogicalPartition && logicalTimestamp != firstPartition && logicalTimestamp != lastPartition) {
                    if (filterApply(filter, filterPosition, changePartitionStatement, logicalTimestamp) > 0) {
                        affectedPartitions++;
                        lastLogicalPartition = logicalTimestamp;
                    }
                }
            }

            // perform the action on the first and last partition, dropping them have to read min/max timestamp of the next first/last partition
            affectedPartitions += filterApply(filter, filterPosition, changePartitionStatement, firstPartition);
            if (firstPartition != lastPartition) {
                affectedPartitions += filterApply(filter, filterPosition, changePartitionStatement, lastPartition);
            }
        }
        return affectedPartitions;
    }

    private void freeTableNameFunctions(QueryModel queryModel) {
        if (queryModel == null) {
            return;
        }

        do {
            final ObjList<QueryModel> joinModels = queryModel.getJoinModels();
            if (joinModels.size() > 1) {
                for (int i = 1, n = joinModels.size(); i < n; i++) {
                    freeTableNameFunctions(joinModels.getQuick(i));
                }
            }

            Misc.free(queryModel.getTableNameFunction());
            queryModel.setTableNameFunction(null);
        } while ((queryModel = queryModel.getNestedModel()) != null);
    }

    private RecordCursorFactory generateExplain(ExplainModel model, SqlExecutionContext executionContext) throws SqlException {
        if (model.getInnerExecutionModel().getModelType() == ExecutionModel.UPDATE) {
            QueryModel updateQueryModel = model.getInnerExecutionModel().getQueryModel();
            final QueryModel selectQueryModel = updateQueryModel.getNestedModel();
            final RecordCursorFactory recordCursorFactory = generateUpdateFactory(
                    updateQueryModel.getUpdateTableToken(),
                    selectQueryModel,
                    updateQueryModel,
                    executionContext
            );
            return codeGenerator.generateExplain(updateQueryModel, recordCursorFactory, model.getFormat());
        } else {
            return codeGenerator.generateExplain(model, executionContext);
        }
    }

    private UpdateOperation generateUpdate(QueryModel updateQueryModel, SqlExecutionContext executionContext, TableRecordMetadata metadata) throws SqlException {
        TableToken updateTableToken = updateQueryModel.getUpdateTableToken();
        final QueryModel selectQueryModel = updateQueryModel.getNestedModel();

        // Update QueryModel structure is
        // QueryModel with SET column expressions
        // |-- QueryModel of select-virtual or select-choose of data selected for update
        final RecordCursorFactory recordCursorFactory = generateUpdateFactory(
                updateTableToken,
                selectQueryModel,
                updateQueryModel,
                executionContext
        );

        if (!metadata.isWalEnabled() || executionContext.isWalApplication()) {
            return new UpdateOperation(
                    updateTableToken,
                    selectQueryModel.getTableId(),
                    selectQueryModel.getMetadataVersion(),
                    lexer.getPosition(),
                    recordCursorFactory
            );
        } else {
            recordCursorFactory.close();

            if (selectQueryModel.containsJoin()) {
                throw SqlException.position(0).put("UPDATE statements with join are not supported yet for WAL tables");
            }

            return new UpdateOperation(
                    updateTableToken,
                    metadata.getTableId(),
                    metadata.getMetadataVersion(),
                    lexer.getPosition()
            );
        }
    }

    private RecordCursorFactory generateUpdateFactory(
            TableToken tableToken,
            @Transient QueryModel selectQueryModel,
            @Transient QueryModel updateQueryModel,
            @Transient SqlExecutionContext executionContext
    ) throws SqlException {
        final IntList tableColumnTypes = selectQueryModel.getUpdateTableColumnTypes();
        final ObjList<CharSequence> tableColumnNames = selectQueryModel.getUpdateTableColumnNames();

        RecordCursorFactory updateToDataCursorFactory = generateSelectOneShot(selectQueryModel, executionContext, false);
        try {
            if (!updateToDataCursorFactory.supportsUpdateRowId(tableToken)) {
                // in theory this should never happen because all valid UPDATE statements should result in
                // a query plan with real row ids but better to check to prevent data corruption
                throw SqlException.$(updateQueryModel.getModelPosition(), "Unsupported SQL complexity for the UPDATE statement");
            }

            // Check that updateDataFactoryMetadata match types of table to be updated exactly
            final RecordMetadata updateDataFactoryMetadata = updateToDataCursorFactory.getMetadata();
            for (int i = 0, n = updateDataFactoryMetadata.getColumnCount(); i < n; i++) {
                int virtualColumnType = updateDataFactoryMetadata.getColumnType(i);
                CharSequence updateColumnName = updateDataFactoryMetadata.getColumnName(i);
                int tableColumnIndex = tableColumnNames.indexOf(updateColumnName);
                int tableColumnType = tableColumnTypes.get(tableColumnIndex);

                if (virtualColumnType != tableColumnType && !isIPv4UpdateCast(virtualColumnType, tableColumnType) && !isTimestampUpdateCast(virtualColumnType, tableColumnType)) {
                    if (!ColumnType.isSymbolOrString(tableColumnType) || !ColumnType.isAssignableFrom(virtualColumnType, ColumnType.STRING)) {
                        if (tableColumnType != ColumnType.VARCHAR || !ColumnType.isAssignableFrom(virtualColumnType, ColumnType.VARCHAR)) {
                            // get column position
                            ExpressionNode setRhs = updateQueryModel.getNestedModel().getColumns().getQuick(i).getAst();
                            throw SqlException.inconvertibleTypes(setRhs.position, virtualColumnType, "", tableColumnType, updateColumnName);
                        }
                    }
                }
            }
            return updateToDataCursorFactory;
        } catch (Throwable th) {
            updateToDataCursorFactory.close();
            throw th;
        }
    }

    private int getNextValidTokenPosition() throws SqlException {
        while (lexer.hasNext()) {
            CharSequence token = SqlUtil.fetchNext(lexer);
            if (token == null) {
                return -1;
            } else if (!isSemicolon(token)) {
                lexer.unparseLast();
                return lexer.lastTokenPosition();
            }
        }

        return -1;
    }

    private int goToQueryEnd() throws SqlException {
        CharSequence token;
        lexer.unparseLast();
        while (lexer.hasNext()) {
            token = SqlUtil.fetchNext(lexer);
            if (token == null || isSemicolon(token)) {
                break;
            }
        }

        return lexer.getPosition();
    }

    private void insertValidateFunctionAndAddToList(
            InsertModel model,
            int tupleIndex,
            ObjList<Function> valueFunctions,
            RecordMetadata metadata,
            int metadataTimestampIndex,
            int insertColumnIndex,
            int metadataColumnIndex,
            Function function,
            int functionPosition,
            BindVariableService bindVariableService
    ) throws SqlException {
        final int columnType = metadata.getColumnType(metadataColumnIndex);
        if (ColumnType.isUndefined(function.getType())) {
            function.assignType(columnType, bindVariableService);
        }

        if (ColumnType.isAssignableFrom(function.getType(), columnType)) {
            if (metadataColumnIndex == metadataTimestampIndex) {
                return;
            }

            valueFunctions.add(function);
            listColumnFilter.add(metadataColumnIndex + 1);
            return;
        }

        Function implicitCast = functionParser.createImplicitCast(functionPosition, function, columnType);
        if (implicitCast != null) {
            valueFunctions.add(implicitCast);
            listColumnFilter.add(metadataColumnIndex + 1);
            return;
        }

        throw SqlException.inconvertibleTypes(
                functionPosition,
                function.getType(),
                model.getRowTupleValues(tupleIndex).getQuick(insertColumnIndex).token,
                metadata.getColumnType(metadataColumnIndex),
                metadata.getColumnName(metadataColumnIndex)
        );
    }

    private boolean isCompatibleColumnTypeChange(int from, int to) {
        return columnConversionSupport[ColumnType.tagOf(from)][ColumnType.tagOf(to)];
    }

    private void lightlyValidateInsertModel(InsertModel model) throws SqlException {
        ExpressionNode tableNameExpr = model.getTableNameExpr();
        if (tableNameExpr.type != ExpressionNode.LITERAL) {
            throw SqlException.$(tableNameExpr.position, "literal expected");
        }

        int columnNameListSize = model.getColumnNameList().size();

        if (columnNameListSize > 0) {
            for (int i = 0, n = model.getRowTupleCount(); i < n; i++) {
                if (columnNameListSize != model.getRowTupleValues(i).size()) {
                    throw SqlException.$(
                                    model.getEndOfRowTupleValuesPosition(i),
                                    "row value count does not match column count [expected="
                            ).put(columnNameListSize)
                            .put(", actual=").put(model.getRowTupleValues(i).size())
                            .put(", tuple=").put(i + 1)
                            .put(']');
                }
            }
        }
    }

    private void parseResumeWal(TableToken tableToken, int tableNamePosition, SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = expectToken(lexer, "'wal'");
        if (!isWalKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'wal' expected");
        }
        if (!engine.isWalTable(tableToken)) {
            throw SqlException.$(lexer.lastTokenPosition(), tableToken.getTableName()).put(" is not a WAL table.");
        }

        tok = SqlUtil.fetchNext(lexer); // optional FROM part
        long fromTxn = -1;
        if (tok != null && !Chars.equals(tok, ';')) {
            if (isFromKeyword(tok)) {
                tok = expectToken(lexer, "'transaction' or 'txn'");
                if (!(isTransactionKeyword(tok) || isTxnKeyword(tok))) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'transaction' or 'txn' expected");
                }
                CharSequence txnValue = expectToken(lexer, "transaction value");
                try {
                    fromTxn = Numbers.parseLong(txnValue);
                } catch (NumericException e) {
                    throw SqlException.$(lexer.lastTokenPosition(), "invalid value [value=").put(txnValue).put(']');
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'from' expected");
            }
        }
        executionContext.getSecurityContext().authorizeResumeWal(tableToken);
        alterTableResume(tableNamePosition, tableToken, fromTxn, executionContext);
    }

    private void parseSuspendWal(TableToken tableToken, int tableNamePosition, SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = expectToken(lexer, "'wal'");
        if (!isWalKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'wal' expected");
        }
        if (!engine.isWalTable(tableToken)) {
            throw SqlException.$(lexer.lastTokenPosition(), tableToken.getTableName()).put(" is not a WAL table.");
        }
        if (!configuration.isDevModeEnabled()) {
            throw SqlException.$(0, "Cannot suspend table, database is not in dev mode");
        }

        ErrorTag errorTag = ErrorTag.NONE;
        String errorMessage = "";

        tok = SqlUtil.fetchNext(lexer); // optional WITH part
        if (tok != null && !Chars.equals(tok, ';')) {
            if (isWithKeyword(tok)) {
                tok = expectToken(lexer, "error code/tag");
                final CharSequence errorCodeOrTagValue = unquote(tok).toString();
                try {
                    final int errorCode = Numbers.parseInt(errorCodeOrTagValue);
                    errorTag = ErrorTag.resolveTag(errorCode);
                } catch (NumericException e) {
                    try {
                        errorTag = ErrorTag.resolveTag(errorCodeOrTagValue);
                    } catch (CairoException cairoException) {
                        throw SqlException.$(lexer.lastTokenPosition(), "invalid value [value=").put(errorCodeOrTagValue).put(']');
                    }
                }
                final CharSequence comma = expectToken(lexer, "','");
                if (!Chars.equals(comma, ',')) {
                    throw SqlException.position(lexer.getPosition()).put("',' expected");
                }
                tok = expectToken(lexer, "error message");
                errorMessage = unquote(tok).toString();
                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && !Chars.equals(tok, ';')) {
                    throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [token=").put(tok).put(']');
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'with' expected");
            }
        }
        alterTableSuspend(tableNamePosition, tableToken, errorTag, errorMessage, executionContext);
    }

    private TableToken tableExistsOrFail(int position, CharSequence tableName, SqlExecutionContext executionContext) throws SqlException {
        if (executionContext.getTableStatus(path, tableName) != TableUtils.TABLE_EXISTS) {
            throw SqlException.tableDoesNotExist(position, tableName);
        }
        TableToken token = executionContext.getTableTokenIfExists(tableName);
        if (token == null) {
            throw SqlException.tableDoesNotExist(position, tableName);
        }
        return token;
    }

    private void validateAndOptimiseInsertAsSelect(SqlExecutionContext executionContext, InsertModel model) throws SqlException {
        final QueryModel queryModel = optimiser.optimise(model.getQueryModel(), executionContext, this);
        int columnNameListSize = model.getColumnNameList().size();
        if (columnNameListSize > 0 && queryModel.getBottomUpColumns().size() != columnNameListSize) {
            throw SqlException.$(model.getTableNameExpr().position, "column count mismatch");
        }
        model.setQueryModel(queryModel);
    }

    protected static CharSequence expectToken(GenericLexer lexer, CharSequence expected) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);

        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put(expected).put(" expected");
        }

        if (Chars.equals(tok, ';')) {
            throw SqlException.position(lexer.lastTokenPosition()).put(expected).put(" expected");
        }
        return tok;
    }

    protected static CharSequence maybeExpectToken(GenericLexer lexer, CharSequence expected, boolean expect) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (expect) {
            if (tok == null) {
                throw SqlException.position(lexer.getPosition()).put(expected).put(" expected");
            }

            if (Chars.equals(tok, ';')) {
                throw SqlException.position(lexer.lastTokenPosition()).put(expected).put(" expected");
            }
        }
        return tok;
    }

    protected void addColumnSuffix(
            @Transient SecurityContext securityContext,
            @Nullable CharSequence tok,
            TableToken tableToken,
            AlterOperationBuilder alterOperationBuilder
    ) throws SqlException {
        if (tok != null) {
            throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
        }
    }

    protected void compileAlterExt(SqlExecutionContext executionContext, CharSequence tok) throws SqlException {
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put("'table' or 'materialized' expected");
        }
        throw SqlException.position(lexer.lastTokenPosition()).put("'table' or 'materialized' expected");
    }

    protected void compileDropExt(
            @NotNull SqlExecutionContext executionContext,
            @NotNull GenericDropOperationBuilder opBuilder,
            @NotNull CharSequence tok,
            int position
    ) throws SqlException {
        throw SqlException.$(position, "unexpected token [").put(tok).put(']');
    }

    protected void compileDropOther(
            @NotNull SqlExecutionContext executionContext,
            @NotNull CharSequence tok,
            int position
    ) throws SqlException {
        throw SqlException.position(position).put("'table' or 'materialized view' or 'all' expected");
    }

    protected void compileDropReportExpected(int position) throws SqlException {
        throw SqlException.position(position).put("'table' or 'materialized view' or 'all' expected");
    }

    protected RecordCursorFactory generateSelectOneShot(
            QueryModel selectQueryModel,
            SqlExecutionContext executionContext,
            boolean generateProgressLogger
    ) throws SqlException {
        RecordCursorFactory factory = codeGenerator.generate(selectQueryModel, executionContext);
        if (generateProgressLogger) {
            return new QueryProgress(queryRegistry, sqlText, factory);
        } else {
            return factory;
        }
    }

    @NotNull
    protected SqlOptimiser newSqlOptimiser(
            CairoConfiguration configuration,
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> sqlNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo postOrderTreeTraversalAlgo,
            FunctionParser functionParser,
            Path path
    ) {
        return new SqlOptimiser(
                configuration,
                characterStore,
                sqlNodePool,
                queryColumnPool,
                queryModelPool,
                postOrderTreeTraversalAlgo,
                functionParser,
                path
        );
    }

    protected void registerKeywordBasedExecutors() {
        // For each 'this::method' reference java compiles a class
        // We need to minimize repetition of this syntax as each site generates garbage
        final KeywordBasedExecutor compileSet = this::compileSet;

        keywordBasedExecutors.put("truncate", this::compileTruncate);
        keywordBasedExecutors.put("alter", this::compileAlter);
        keywordBasedExecutors.put("reindex", this::compileReindex);
        keywordBasedExecutors.put("set", compileSet);
        keywordBasedExecutors.put("begin", this::compileBegin);
        keywordBasedExecutors.put("commit", this::compileCommit);
        keywordBasedExecutors.put("rollback", this::compileRollback);
        keywordBasedExecutors.put("discard", compileSet);
        keywordBasedExecutors.put("close", compileSet); // no-op
        keywordBasedExecutors.put("unlisten", compileSet);  // no-op
        keywordBasedExecutors.put("reset", compileSet);  // no-op
        keywordBasedExecutors.put("drop", this::compileDrop);
        keywordBasedExecutors.put("backup", backupAgent::sqlBackup);
        keywordBasedExecutors.put("vacuum", this::compileVacuum);
        keywordBasedExecutors.put("checkpoint", this::compileCheckpoint);
        keywordBasedExecutors.put("snapshot", this::compileLegacyCheckpoint);
        keywordBasedExecutors.put("deallocate", this::compileDeallocate);
        keywordBasedExecutors.put("cancel", this::compileCancel);
        keywordBasedExecutors.put("refresh", this::compileRefresh);
    }

    protected void unknownDropColumnSuffix(
            @Transient SecurityContext securityContext,
            CharSequence tok,
            TableToken tableToken,
            AlterOperationBuilder dropColumnStatement
    ) throws SqlException {
        throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
    }

    @FunctionalInterface
    public interface KeywordBasedExecutor {
        void execute(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException;
    }

    public final static class PartitionAction {
        public static final int ATTACH = 2;
        public static final int CONVERT_TO_NATIVE = 5;
        public static final int CONVERT_TO_PARQUET = 4;
        public static final int DETACH = 3;
        public static final int DROP = 1;
        public static final int FORCE_DROP = 6;
    }

    private static class TimestampValueRecord implements Record {
        private long value;

        @Override
        public long getTimestamp(int col) {
            return value;
        }

        public void setTimestamp(long value) {
            this.value = value;
        }
    }

    private class DatabaseBackupAgent implements Closeable {
        private final Path auxPath;
        private final Path dstPath;
        private final StringSink sink = new StringSink();
        private final Path srcPath;
        private final Utf8SequenceObjHashMap<RecordToRowCopier> tableBackupRowCopiedCache = new Utf8SequenceObjHashMap<>();
        private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
        private final ObjHashSet<TableToken> tableTokens = new ObjHashSet<>();
        private transient String cachedBackupTmpRoot;
        private transient int dstCurrDirLen;
        private transient int dstPathRoot;

        public DatabaseBackupAgent() {
            try {
                auxPath = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
                dstPath = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
                srcPath = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        public void clear() {
            auxPath.trimTo(0);
            srcPath.trimTo(0);
            dstPath.trimTo(0);
            cachedBackupTmpRoot = null;
            dstPathRoot = 0;
            dstCurrDirLen = 0;
            tableBackupRowCopiedCache.clear();
            tableTokens.clear();
        }

        @Override
        public void close() {
            tableBackupRowCopiedCache.clear();
            Misc.free(auxPath);
            Misc.free(srcPath);
            Misc.free(dstPath);
        }

        private void backupTable(@NotNull final TableToken tableToken) throws SqlException {
            LOG.info().$("starting backup of ").$(tableToken).$();

            // the table is copied to a TMP folder and then this folder is moved to the final destination (dstPath)
            if (cachedBackupTmpRoot == null) {
                if (configuration.getBackupRoot() == null) {
                    throw CairoException.nonCritical()
                            .put("backup is disabled, server.conf property 'cairo.sql.backup.root' is not set");
                }
                auxPath.of(configuration.getBackupRoot()).concat(configuration.getBackupTempDirName()).slash$();
                cachedBackupTmpRoot = Utf8s.toString(auxPath); // absolute path to the TMP folder
            }

            final String tableName = tableToken.getTableName();
            auxPath.of(cachedBackupTmpRoot).concat(tableToken).slash();
            int tableRootLen = auxPath.size();
            try {
                try (TableReader reader = engine.getReader(tableToken)) { // acquire reader lock
                    if (ff.exists(auxPath.$())) {
                        throw CairoException.nonCritical()
                                .put("backup dir already exists [path=").put(auxPath)
                                .put(", table=").put(tableName)
                                .put(']');
                    }

                    // clone metadata

                    // create TMP folder
                    if (ff.mkdirs(auxPath, configuration.getBackupMkDirMode()) != 0) {
                        throw CairoException.critical(ff.errno()).put("could not create [dir=").put(auxPath).put(']');
                    }

                    // backup table metadata files to TMP folder
                    try {
                        TableReaderMetadata metadata = reader.getMetadata();

                        // _meta
                        mem.smallFile(ff, auxPath.trimTo(tableRootLen).concat(TableUtils.META_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                        metadata.dumpTo(mem);

                        // create symbol maps
                        auxPath.trimTo(tableRootLen).$();
                        int symbolMapCount = 0;
                        for (int i = 0, sz = metadata.getColumnCount(); i < sz; i++) {
                            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                                SymbolMapReader mapReader = reader.getSymbolMapReader(i);
                                MapWriter.createSymbolMapFiles(
                                        ff,
                                        mem,
                                        auxPath,
                                        metadata.getColumnName(i),
                                        COLUMN_NAME_TXN_NONE,
                                        mapReader.getSymbolCapacity(),
                                        mapReader.isCached()
                                );
                                symbolMapCount++;
                            }
                        }

                        // _txn
                        mem.smallFile(ff, auxPath.trimTo(tableRootLen).concat(TableUtils.TXN_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                        TableUtils.createTxn(mem, symbolMapCount, 0L, 0L, TableUtils.INITIAL_TXN, 0L, metadata.getMetadataVersion(), 0L, 0L);

                        // _cv
                        mem.smallFile(ff, auxPath.trimTo(tableRootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                        TableUtils.createColumnVersionFile(mem);

                        if (tableToken.isWal()) {
                            // _name
                            mem.smallFile(ff, auxPath.trimTo(tableRootLen).concat(TableUtils.TABLE_NAME_FILE).$(), MemoryTag.MMAP_DEFAULT);
                            TableUtils.createTableNameFile(mem, tableToken.getTableName());

                            // initialise txn_seq folder
                            auxPath.trimTo(tableRootLen).concat(WalUtils.SEQ_DIR).slash$();
                            if (ff.mkdirs(auxPath, configuration.getBackupMkDirMode()) != 0) {
                                throw CairoException.critical(ff.errno()).put("Cannot create [path=").put(auxPath).put(']');
                            }
                            int len = auxPath.size();
                            // _wal_index.d
                            mem.smallFile(ff, auxPath.concat(WalUtils.WAL_INDEX_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                            mem.putLong(0L);
                            mem.close(true, Vm.TRUNCATE_TO_POINTER);
                            // _txnlog
                            WalUtils.createTxnLogFile(
                                    ff,
                                    mem,
                                    auxPath.trimTo(len),
                                    configuration.getMicrosecondClock().getTicks(),
                                    configuration.getDefaultSeqPartTxnCount(),
                                    configuration.getMkDirMode()
                            );
                            // _txnlog.meta.i
                            mem.smallFile(ff, auxPath.trimTo(len).concat(WalUtils.TXNLOG_FILE_NAME_META_INX).$(), MemoryTag.MMAP_DEFAULT);
                            mem.putLong(0L);
                            mem.close(true, Vm.TRUNCATE_TO_POINTER);
                            // _txnlog.meta.d
                            mem.smallFile(ff, auxPath.trimTo(len).concat(WalUtils.TXNLOG_FILE_NAME_META_VAR).$(), MemoryTag.MMAP_DEFAULT);
                            mem.close(true, Vm.TRUNCATE_TO_POINTER);
                            // _meta
                            mem.smallFile(ff, auxPath.trimTo(len).concat(TableUtils.META_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                            WalWriterMetadata.syncToMetaFile(
                                    mem,
                                    metadata.getMetadataVersion(),
                                    metadata.getColumnCount(),
                                    metadata.getTimestampIndex(),
                                    metadata.getTableId(),
                                    false,
                                    metadata
                            );
                            mem.close(true, Vm.TRUNCATE_TO_POINTER);
                        }

                        if (tableToken.isMatView()) {
                            try (
                                    BlockFileReader matViewFileReader = new BlockFileReader(configuration);
                                    BlockFileWriter matViewFileWriter = new BlockFileWriter(ff, configuration.getCommitMode())
                            ) {
                                MatViewGraph graph = engine.getMatViewGraph();
                                final MatViewDefinition matViewDefinition = graph.getViewDefinition(tableToken);
                                if (matViewDefinition != null) {
                                    // srcPath is unused at this point, so we can overwrite it
                                    final boolean isMatViewStateExists = TableUtils.isMatViewStateFileExists(configuration, srcPath, tableToken.getDirName());
                                    if (isMatViewStateExists) {
                                        matViewFileReader.of(srcPath.of(configuration.getDbRoot()).concat(tableToken.getDirName()).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                                        MatViewStateReader matViewStateReader = new MatViewStateReader().of(matViewFileReader, tableToken);
                                        matViewFileWriter.of(auxPath.trimTo(tableRootLen).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                                        MatViewState.append(matViewStateReader, matViewFileWriter);
                                        matViewFileWriter.of(auxPath.trimTo(tableRootLen).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());
                                        MatViewDefinition.append(matViewDefinition, matViewFileWriter);
                                    } else {
                                        LOG.info().$("materialized view state for backup not found [view=").$(tableToken).I$();
                                    }
                                } else {
                                    LOG.info().$("materialized view definition for backup not found [view=").$(tableToken).I$();
                                }
                            }
                        }
                    } finally {
                        mem.close();
                    }

                    // copy the data
                    try (TableWriter backupWriter = engine.getBackupWriter(tableToken, cachedBackupTmpRoot)) {
                        RecordMetadata writerMetadata = backupWriter.getMetadata();
                        srcPath.of(tableName).slash().put(reader.getMetadataVersion()).$();
                        RecordToRowCopier recordToRowCopier = tableBackupRowCopiedCache.get(srcPath);
                        if (recordToRowCopier == null) {
                            entityColumnFilter.of(writerMetadata.getColumnCount());
                            recordToRowCopier = RecordToRowCopierUtils.generateCopier(
                                    asm,
                                    reader.getMetadata(),
                                    writerMetadata,
                                    entityColumnFilter
                            );
                            tableBackupRowCopiedCache.put(srcPath, recordToRowCopier);
                        }

                        sink.clear();
                        sink.put('\'').put(tableName).put('\'');

                        try (SqlExecutionContext allowAllContext = new SqlExecutionContextImpl(engine, 1)
                                .with(AllowAllSecurityContext.INSTANCE)
                                .with(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER)
                        ) {
                            while (true) {
                                try (
                                        RecordCursorFactory factory = engine.select(sink, allowAllContext);
                                        RecordCursor cursor = factory.getCursor(allowAllContext)
                                ) {
                                    // statement/query timeout value is most likely too small for backup operation
                                    copyTableData(
                                            allowAllContext,
                                            cursor,
                                            factory.getMetadata(),
                                            backupWriter,
                                            writerMetadata,
                                            recordToRowCopier,
                                            configuration.getCreateTableModelBatchSize(),
                                            configuration.getO3MaxLag(),
                                            CopyDataProgressReporter.NOOP,
                                            configuration.getParquetExportCopyReportFrequencyLines()
                                    );
                                    break;
                                } catch (TableReferenceOutOfDateException ex) {
                                    // Sometimes table can be out of data when a DDL is committed concurrently, we need to retry
                                    LOG.info().$("retrying backup due to concurrent metadata update [table=").$(tableToken)
                                            .$(", ex=").$(ex.getFlyweightMessage())
                                            .I$();
                                }
                            }
                        }

                        backupWriter.commit();
                    }
                } // release reader lock
                int renameRootLen = dstPath.size();
                try {
                    dstPath.trimTo(renameRootLen).concat(tableToken);
                    TableUtils.renameOrFail(ff, auxPath.trimTo(tableRootLen).$(), dstPath.$());
                    LOG.info().$("backup complete [table=").$(tableToken).$(", to=").$(dstPath).I$();
                } finally {
                    dstPath.trimTo(renameRootLen).$();
                }
            } catch (CairoException e) {
                LOG.info()
                        .$("could not backup [table=").$(tableToken)
                        .$(", msg=").$safe(e.getFlyweightMessage())
                        .$(", errno=").$(e.getErrno())
                        .I$();
                auxPath.of(cachedBackupTmpRoot).concat(tableToken).slash$();
                if (!ff.rmdir(auxPath)) {
                    LOG.error().$("could not delete directory [path=").$(auxPath).$(", errno=").$(ff.errno()).I$();
                }
                throw e;
            }
        }

        private void mkBackupDstDir(CharSequence dir, String errorMessage) {
            dstPath.trimTo(dstPathRoot).concat(dir).slash$();
            dstCurrDirLen = dstPath.size();
            if (ff.mkdirs(dstPath, configuration.getBackupMkDirMode()) != 0) {
                throw CairoException.critical(ff.errno()).put(errorMessage).put(dstPath).put(']');
            }
        }

        private void mkBackupDstRoot() {
            DateFormat format = configuration.getBackupDirTimestampFormat();
            long epochUs = configuration.getMicrosecondClock().getTicks();
            dstPath.of(configuration.getBackupRoot()).slash();
            int plen = dstPath.size();
            int n = 0;
            // There is a race here, two threads could try and create the same dstPath,
            // only one will succeed the other will throw a CairoException. It could be serialised
            do {
                dstPath.trimTo(plen);
                format.format(epochUs, configuration.getDefaultDateLocale(), null, dstPath);
                if (n > 0) {
                    dstPath.put('.').put(n);
                }
                dstPath.slash();
                n++;
            } while (ff.exists(dstPath.$()));
            if (ff.mkdirs(dstPath, configuration.getBackupMkDirMode()) != 0) {
                // the winner will succeed the looser thread will get this exception
                throw CairoException.critical(ff.errno()).put("could not create backup [dir=").put(dstPath).put(']');
            }
            dstPathRoot = dstPath.size();
        }

        private void sqlBackup(SqlExecutionContext executionContext, @Transient CharSequence sqlText) throws SqlException {
            if (configuration.getBackupRoot() == null) {
                throw CairoException.nonCritical().put("backup is disabled, server.conf property 'cairo.sql.backup.root' is not set");
            }
            CharSequence tok = SqlUtil.fetchNext(lexer);
            if (tok != null) {
                if (isTableKeyword(tok)) {
                    sqlTableBackup(executionContext);
                    return;
                }
                if (isDatabaseKeyword(tok)) {
                    sqlDatabaseBackup(executionContext);
                    return;
                }
                if (isMaterializedKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok != null && isViewKeyword(tok)) {
                        sqlTableBackup(executionContext);
                        return;
                    }
                }
            }
            throw SqlException.position(lexer.lastTokenPosition()).put("expected 'table', 'materialized view' or 'database'");
        }

        private void sqlDatabaseBackup(SqlExecutionContext executionContext) throws SqlException {
            mkBackupDstRoot();
            mkBackupDstDir(configuration.getDbDirectory(), "could not create backup [db dir=");

            // backup tables
            engine.getTableTokens(tableTokenBucket, false);
            executionContext.getSecurityContext().authorizeTableBackup(tableTokens);
            for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
                backupTable(tableTokenBucket.get(i));
            }

            srcPath.of(configuration.getDbRoot()).$();
            int srcLen = srcPath.size();

            // backup table registry file (tables.d.<last>)
            // Note: this is unsafe way to back up table name registry,
            //       but since we're going to deprecate BACKUP, that's ok
            int version = TableNameRegistryStore.findLastTablesFileVersion(ff, srcPath, sink);
            srcPath.trimTo(srcLen).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii('.').put(version);
            dstPath.trimTo(dstCurrDirLen).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii(".0"); // reset to 0
            LOG.info().$("backup copying file [from=").$(srcPath).$(", to=").$(dstPath).I$();
            if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
                throw CairoException.critical(ff.errno())
                        .put("cannot backup table registry file [from=").put(srcPath)
                        .put(", to=").put(dstPath)
                        .put(']');
            }

            // backup table index file (_tab_index.d)
            srcPath.trimTo(srcLen).concat(TableUtils.TAB_INDEX_FILE_NAME);
            dstPath.trimTo(dstCurrDirLen).concat(TableUtils.TAB_INDEX_FILE_NAME);
            LOG.info().$("backup copying file [from=").$(srcPath).$(", to=").$(dstPath).I$();
            if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
                throw CairoException.critical(ff.errno())
                        .put("cannot backup table index file [from=").put(srcPath)
                        .put(", to=").put(dstPath)
                        .put(']');
            }

            // backup conf directory
            mkBackupDstDir(PropServerConfiguration.CONFIG_DIRECTORY, "could not create backup [conf dir=");
            ff.copyRecursive(srcPath.of(configuration.getConfRoot()), auxPath.of(dstPath), configuration.getMkDirMode());
            compiledQuery.ofBackupTable();
        }

        private void sqlTableBackup(SqlExecutionContext executionContext) throws SqlException {
            mkBackupDstRoot();
            mkBackupDstDir(configuration.getDbDirectory(), "could not create backup [db dir=");
            try {
                tableTokens.clear();
                while (true) {
                    CharSequence tok = SqlUtil.fetchNext(lexer);
                    if (tok == null) {
                        throw SqlException.position(lexer.getPosition()).put("expected a table name");
                    }
                    final CharSequence tableName = GenericLexer.assertNoDotsAndSlashes(unquote(tok), lexer.lastTokenPosition());
                    TableToken tableToken = tableExistsOrFail(lexer.lastTokenPosition(), tableName, executionContext);
                    tableTokens.add(tableToken);
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok == null || Chars.equals(tok, ';')) {
                        break;
                    }
                    if (!Chars.equals(tok, ',')) {
                        throw SqlException.position(lexer.lastTokenPosition()).put("expected ','");
                    }
                }

                executionContext.getSecurityContext().authorizeTableBackup(tableTokens);

                for (int i = 0, n = tableTokens.size(); i < n; i++) {
                    backupTable(tableTokens.get(i));
                }
                compiledQuery.ofBackupTable();
            } finally {
                tableTokens.clear();
            }
        }
    }

    static {
        sqlControlSymbols.add("(");
        sqlControlSymbols.add(";");
        sqlControlSymbols.add(")");
        sqlControlSymbols.add(",");
        sqlControlSymbols.add("/*");
        sqlControlSymbols.add("/*+");
        sqlControlSymbols.add("*/");
        sqlControlSymbols.add("--");
        sqlControlSymbols.add("[");
        sqlControlSymbols.add("]");

        short[] numericTypes = {ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.TIMESTAMP, ColumnType.BOOLEAN, ColumnType.DATE, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL};
        addSupportedConversion(ColumnType.BYTE, numericTypes);
        addSupportedConversion(ColumnType.SHORT, numericTypes);
        addSupportedConversion(ColumnType.INT, numericTypes);
        addSupportedConversion(ColumnType.LONG, numericTypes);
        addSupportedConversion(ColumnType.FLOAT, numericTypes);
        addSupportedConversion(ColumnType.DOUBLE, numericTypes);
        addSupportedConversion(ColumnType.TIMESTAMP, numericTypes);
        addSupportedConversion(ColumnType.BOOLEAN, numericTypes);
        addSupportedConversion(ColumnType.DATE, numericTypes);

        //region Decimals
        for (short i = ColumnType.DECIMAL8; i <= ColumnType.DECIMAL256; i++) {
            for (short j = ColumnType.DECIMAL8; j <= ColumnType.DECIMAL256; j++) {
                addSupportedConversion(i, j);
            }
            // We only allow types that are accurately representable to be converted directly, others should use explicit
            // casting.
            addSupportedConversion(ColumnType.BYTE, i);
            addSupportedConversion(ColumnType.SHORT, i);
            addSupportedConversion(ColumnType.INT, i);
            addSupportedConversion(ColumnType.LONG, i);
        }
        //endregion

        // Other exotics <-> strings
        addSupportedConversion(ColumnType.IPv4, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL);
        addSupportedConversion(ColumnType.UUID, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL);
        addSupportedConversion(ColumnType.CHAR, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL);

        // Strings <-> Strings
        addSupportedConversion(ColumnType.SYMBOL, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL);
        addSupportedConversion(ColumnType.STRING, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL);
        addSupportedConversion(ColumnType.VARCHAR, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL);
    }
}
