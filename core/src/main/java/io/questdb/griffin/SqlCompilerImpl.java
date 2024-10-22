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
import io.questdb.cairo.AlterTableUtils;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexBuilder;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.MapWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableNameRegistryStore;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.VacuumColumnVersions;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
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
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.engine.ops.CopyCancelFactory;
import io.questdb.griffin.engine.ops.CopyFactory;
import io.questdb.griffin.engine.ops.InsertOperationImpl;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.model.ColumnCastModel;
import io.questdb.griffin.model.CopyModel;
import io.questdb.griffin.model.CreateTableModel;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExplainModel;
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
import io.questdb.std.IntIntHashMap;
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
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.griffin.SqlKeywords.*;

public class SqlCompilerImpl implements SqlCompiler, Closeable, SqlParserCallback {
    static final ObjList<String> sqlControlSymbols = new ObjList<>(8);
    // null object used to skip null checks in batch method
    private static final BatchCallback EMPTY_CALLBACK = new BatchCallback() {
        @Override
        public void postCompile(SqlCompiler compiler, CompiledQuery cq, CharSequence queryText) {
        }

        @Override
        public void preCompile(SqlCompiler compiler) {
        }
    };
    private static final IntList castGroups = new IntList();
    private static final boolean[][] columnConversionSupport = new boolean[ColumnType.NULL][ColumnType.NULL];
    @SuppressWarnings("FieldMayBeFinal")
    private static Log LOG = LogFactory.getLog(SqlCompilerImpl.class);
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
    private final CharacterStore characterStore;
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final DropStatementCompiler dropStmtCompiler = new DropStatementCompiler();
    private final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
    private final FilesFacade ff;
    private final FunctionParser functionParser;
    private final ListColumnFilter listColumnFilter = new ListColumnFilter();
    private final int maxRecompileAttempts;
    private final MemoryMARW mem = Vm.getMARWInstance();
    private final MessageBus messageBus;
    private final SqlParser parser;
    private final TimestampValueRecord partitionFunctionRec = new TimestampValueRecord();
    private final QueryBuilder queryBuilder;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final Path renamePath;
    private final ObjectPool<ExpressionNode> sqlNodePool;
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final ObjList<TableWriterAPI> tableWriters = new ObjList<>();
    private final IntIntHashMap typeCast = new IntIntHashMap();
    private final VacuumColumnVersions vacuumColumnVersions;
    protected CharSequence sqlText;
    private final ExecutableMethod insertAsSelectMethod = this::insertAsSelect;
    private boolean closed = false;
    // Helper var used to pass back count in cases it can't be done via method result.
    private long insertCount;
    private final ExecutableMethod createTableMethod = this::createTable;
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
            this.codeGenerator = new SqlCodeGenerator(engine, configuration, functionParser, sqlNodePool);
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
                    optimiser,
                    characterStore,
                    sqlNodePool,
                    queryColumnPool,
                    queryModelPool,
                    postOrderTreeTraversalAlgo
            );

            alterOperationBuilder = new AlterOperationBuilder();
            queryRegistry = engine.getQueryRegistry();
        } catch (Throwable th) {
            close();
            throw th;
        }
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

    public void clear() {
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
        sqlText = null;
        columnNames.clear();
    }

    @Override
    public void close() {
        if (closed) {
            throw new IllegalStateException("close was already called");
        }
        closed = true;
        Misc.free(backupAgent);
        Misc.free(dropStmtCompiler);
        Misc.free(vacuumColumnVersions);
        Misc.free(path);
        Misc.free(renamePath);
        Misc.free(codeGenerator);
        Misc.free(mem);
        Misc.freeObjList(tableWriters);
    }

    @NotNull
    public CompiledQuery compile(@NotNull CharSequence query, @Transient @NotNull SqlExecutionContext executionContext) throws SqlException {
        clear();
        // these are quick executions that do not require building of a model
        lexer.of(query);
        isSingleQueryMode = true;

        compileInner(executionContext, query);
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
     * @param query            - block of queries to process
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
            @NotNull CharSequence query,
            @NotNull SqlExecutionContext executionContext,
            BatchCallback batchCallback
    ) throws Exception {
        clear();
        lexer.of(query);
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
                batchCallback.preCompile(this);
                clear(); // we don't use normal compile here because we can't reset existing lexer

                // Fetch sqlText, this will move lexer pointer (state change).
                // We try to avoid logging the entire sql batch, in case batch contains secrets
                final CharSequence sqlText = query.subSequence(position, goToQueryEnd());
                // re-position lexer pointer to where sqlText just began
                lexer.backTo(position, null);
                compileInner(executionContext, sqlText);
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
                    LOG.info().$(e.getFlyweightMessage()).$();
                    // will recompile
                    lexer.restart();
                }
            }
        }
    }

    public CairoEngine getEngine() {
        return engine;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return functionParser.getFunctionFactoryCache();
    }

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
    public ExecutionModel testCompileModel(CharSequence query, SqlExecutionContext executionContext) throws SqlException {
        clear();
        lexer.of(query);
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

    private static boolean isCompatibleCase(int from, int to) {
        if (isIPv4Cast(from, to)) {
            return true;
        }
        return castGroups.getQuick(ColumnType.tagOf(from)) == castGroups.getQuick(ColumnType.tagOf(to));
    }

    private static boolean isIPv4Cast(int from, int to) {
        return (from == ColumnType.STRING && to == ColumnType.IPv4) || (from == ColumnType.VARCHAR && to == ColumnType.IPv4);
    }

    private static boolean isIPv4UpdateCast(int from, int to) {
        return (from == ColumnType.STRING && to == ColumnType.IPv4)
                || (from == ColumnType.IPv4 && to == ColumnType.STRING)
                || (from == ColumnType.VARCHAR && to == ColumnType.IPv4)
                || (from == ColumnType.IPv4 && to == ColumnType.VARCHAR);
    }

    private int addColumnWithType(AlterOperationBuilder addColumn, CharSequence columnName, int columnNamePosition) throws SqlException {
        CharSequence tok;
        tok = expectToken(lexer, "column type");

        int columnType = SqlUtil.toPersistedTypeTag(tok, lexer.lastTokenPosition());

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
            if (tok != null && SqlKeywords.isNotKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);
            }

            if (tok != null && SqlKeywords.isNullKeyword(tok)) {
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

    private void alterTable(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !SqlKeywords.isTableKeyword(tok)) {
            unknownAlterStatement(executionContext, tok);
            return;
        }
        final int tableNamePosition = lexer.getPosition();
        tok = expectToken(lexer, "table name");
        SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, tableNamePosition);
        final TableToken tableToken = tableExistsOrFail(tableNamePosition, GenericLexer.unquote(tok), executionContext);
        final SecurityContext securityContext = executionContext.getSecurityContext();

        try (TableRecordMetadata tableMetadata = executionContext.getMetadataForWrite(tableToken)) {
            tok = expectToken(lexer, AlterTableUtils.ALTER_TABLE_EXPECTED_TOKEN_DESCR);

            if (SqlKeywords.isAddKeyword(tok)) {
                securityContext.authorizeAlterTableAddColumn(tableToken);
                alterTableAddColumn(executionContext.getSecurityContext(), tableNamePosition, tableToken, tableMetadata);
            } else if (SqlKeywords.isConvertKeyword(tok)) {
                tok = expectToken(lexer, "'partition'");
                if (!SqlKeywords.isPartitionKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                }
                tok = expectToken(lexer, "'to'");
                if (!SqlKeywords.isToKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'to' expected");
                }
                tok = expectToken(lexer, "'parquet'");
                if (!SqlKeywords.isParquetKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'parquet' expected");
                }
                alterTableDropConvertDetachOrAttachPartition(tableMetadata, tableToken, PartitionAction.CONVERT, executionContext);
            } else if (SqlKeywords.isDropKeyword(tok)) {
                tok = expectToken(lexer, "'column' or 'partition'");
                if (SqlKeywords.isColumnKeyword(tok)) {
                    alterTableDropColumn(executionContext.getSecurityContext(), tableNamePosition, tableToken, tableMetadata);
                } else if (SqlKeywords.isPartitionKeyword(tok)) {
                    securityContext.authorizeAlterTableDropPartition(tableToken);
                    alterTableDropConvertDetachOrAttachPartition(tableMetadata, tableToken, PartitionAction.DROP, executionContext);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'column' or 'partition' expected");
                }
            } else if (SqlKeywords.isRenameKeyword(tok)) {
                tok = expectToken(lexer, "'column'");
                if (SqlKeywords.isColumnKeyword(tok)) {
                    alterTableRenameColumn(securityContext, tableNamePosition, tableToken, tableMetadata);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'column' expected");
                }
            } else if (SqlKeywords.isAttachKeyword(tok)) {
                tok = expectToken(lexer, "'partition'");
                if (SqlKeywords.isPartitionKeyword(tok)) {
                    securityContext.authorizeAlterTableAttachPartition(tableToken);
                    alterTableDropConvertDetachOrAttachPartition(tableMetadata, tableToken, PartitionAction.ATTACH, executionContext);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                }
            } else if (SqlKeywords.isDetachKeyword(tok)) {
                tok = expectToken(lexer, "'partition'");
                if (SqlKeywords.isPartitionKeyword(tok)) {
                    securityContext.authorizeAlterTableDetachPartition(tableToken);
                    alterTableDropConvertDetachOrAttachPartition(tableMetadata, tableToken, PartitionAction.DETACH, executionContext);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                }
            } else if (SqlKeywords.isAlterKeyword(tok)) {
                tok = expectToken(lexer, "'column'");
                if (SqlKeywords.isColumnKeyword(tok)) {
                    final int columnNamePosition = lexer.getPosition();
                    tok = expectToken(lexer, "column name");
                    final CharSequence columnName = GenericLexer.immutableOf(tok);
                    final int columnIndex = tableMetadata.getColumnIndexQuiet(columnName);
                    if (columnIndex == -1) {
                        throw SqlException.$(columnNamePosition, "column '").put(columnName)
                                .put("' does not exists in table '").put(tableToken.getTableName()).put('\'');
                    }

                    tok = expectToken(lexer, "'add index' or 'drop index' or 'type' or 'cache' or 'nocache'");
                    if (SqlKeywords.isAddKeyword(tok)) {
                        expectKeyword(lexer, "index");
                        tok = SqlUtil.fetchNext(lexer);
                        int indexValueCapacity = -1;

                        if (tok != null && (!isSemicolon(tok))) {
                            if (!SqlKeywords.isCapacityKeyword(tok)) {
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
                    } else if (SqlKeywords.isDropKeyword(tok)) {
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
                    } else if (SqlKeywords.isCacheKeyword(tok)) {
                        alterTableColumnCacheFlag(
                                securityContext,
                                tableNamePosition,
                                tableToken,
                                columnNamePosition,
                                columnName,
                                tableMetadata,
                                true
                        );
                    } else if (SqlKeywords.isNoCacheKeyword(tok)) {
                        alterTableColumnCacheFlag(
                                securityContext,
                                tableNamePosition,
                                tableToken,
                                columnNamePosition,
                                columnName,
                                tableMetadata,
                                false
                        );
                    } else if (SqlKeywords.isTypeKeyword(tok)) {
                        alterTableChangeColumnType(
                                securityContext,
                                tableNamePosition,
                                tableToken,
                                columnNamePosition,
                                columnName,
                                tableMetadata,
                                columnIndex,
                                executionContext
                        );
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'add', 'drop', 'cache' or 'nocache' expected").put(" found '").put(tok).put('\'');
                    }
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'column' or 'partition' expected");
                }
            } else if (SqlKeywords.isSetKeyword(tok)) {
                tok = expectToken(lexer, "'param' or 'type'");
                if (SqlKeywords.isParamKeyword(tok)) {
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
                } else if (SqlKeywords.isTypeKeyword(tok)) {
                    tok = expectToken(lexer, "'bypass' or 'wal'");
                    if (SqlKeywords.isBypassKeyword(tok)) {
                        tok = expectToken(lexer, "'wal'");
                        if (SqlKeywords.isWalKeyword(tok)) {
                            alterTableSetType(executionContext, tableNamePosition, tableToken, (byte) 0);
                        } else {
                            throw SqlException.$(lexer.lastTokenPosition(), "'wal' expected");
                        }
                    } else if (SqlKeywords.isWalKeyword(tok)) {
                        alterTableSetType(executionContext, tableNamePosition, tableToken, (byte) 1);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'bypass' or 'wal' expected");
                    }
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'param' or 'type' expected");
                }
            } else if (SqlKeywords.isResumeKeyword(tok)) {
                tok = expectToken(lexer, "'wal'");
                if (!SqlKeywords.isWalKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'wal' expected");
                }

                tok = SqlUtil.fetchNext(lexer); // optional FROM part
                long fromTxn = -1;
                if (tok != null && !Chars.equals(tok, ';')) {
                    if (SqlKeywords.isFromKeyword(tok)) {
                        tok = expectToken(lexer, "'transaction' or 'txn'");
                        if (!(SqlKeywords.isTransactionKeyword(tok) || SqlKeywords.isTxnKeyword(tok))) {
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
                if (!engine.isWalTable(tableToken)) {
                    throw SqlException.$(lexer.lastTokenPosition(), tableToken.getTableName()).put(" is not a WAL table.");
                }
                securityContext.authorizeResumeWal(tableToken);
                alterTableResume(tableNamePosition, tableToken, fromTxn, executionContext);
            } else if (SqlKeywords.isSuspendKeyword(tok)) {
                tok = expectToken(lexer, "'wal'");
                if (!SqlKeywords.isWalKeyword(tok)) {
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
                    if (SqlKeywords.isWithKeyword(tok)) {
                        tok = expectToken(lexer, "error code/tag");
                        final CharSequence errorCodeOrTagValue = GenericLexer.unquote(tok).toString();
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
                        errorMessage = GenericLexer.unquote(tok).toString();
                        tok = SqlUtil.fetchNext(lexer);
                        if (tok != null && !Chars.equals(tok, ';')) {
                            throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [token=").put(tok).put(']');
                        }
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'with' expected");
                    }
                }
                alterTableSuspend(tableNamePosition, tableToken, errorTag, errorMessage, executionContext);
            } else if (SqlKeywords.isSquashKeyword(tok)) {
                securityContext.authorizeAlterTableDropPartition(tableToken);
                tok = expectToken(lexer, "'partitions'");
                if (SqlKeywords.isPartitionsKeyword(tok)) {
                    compiledQuery.ofAlter(alterOperationBuilder.ofSquashPartitions(tableNamePosition, tableToken).build());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'partitions' expected");
                }
            } else if (SqlKeywords.isDedupKeyword(tok) || SqlKeywords.isDeduplicateKeyword(tok)) {
                tok = expectToken(lexer, "'dedup columns'");

                if (SqlKeywords.isDisableKeyword(tok)) {
                    executionContext.getSecurityContext().authorizeAlterTableDedupDisable(tableToken);
                    AlterOperationBuilder setDedup = alterOperationBuilder.ofDedupDisable(
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
                throw SqlException.$(lexer.lastTokenPosition(), AlterTableUtils.ALTER_TABLE_EXPECTED_TOKEN_DESCR).put(" expected");
            }
        } catch (CairoException e) {
            LOG.info().$("could not alter table [table=").$(tableToken.getTableName())
                    .$(", errno=").$(e.getErrno())
                    .$(", ex=").$(e.getFlyweightMessage()).$();
            if (e.getPosition() == 0) {
                e.position(lexer.lastTokenPosition());
            }
            throw e;
        }
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
        if (tok != null && !SqlKeywords.isColumnKeyword(tok)) {
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

            int index = tableMetadata.getColumnIndexQuiet(tok);
            if (index != -1) {
                throw SqlException.$(lexer.lastTokenPosition(), "column '").put(tok).put("' already exists");
            }

            CharSequence columnName = GenericLexer.immutableOf(GenericLexer.unquote(tok));
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
    }

    private void alterTableChangeColumnType(
            SecurityContext securityContext,
            int tableNamePosition,
            TableToken tableToken,
            int columnNamePosition,
            CharSequence columnName,
            TableRecordMetadata tableMetadata,
            int columnIndex,
            SqlExecutionContext executionContext
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
        }
        if (!isCompatibleColumnTypeChange(existingColumnType, newColumnType)) {
            throw SqlException.$(lexer.lastTokenPosition(), "incompatible column type change [existing=")
                    .put(ColumnType.nameOf(existingColumnType)).put(", new=").put(ColumnType.nameOf(newColumnType)).put(']');
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
                final CharSequence columnName = GenericLexer.unquote(tok);

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
            CharSequence tok = GenericLexer.unquote(maybeExpectToken(lexer, "column name", semicolonPos < 0));
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
            if (SqlKeywords.isListKeyword(tok)) {
                alterTableDropConvertDetachOrAttachPartitionByList(tableMetadata, tableToken, reader, pos, action);
            } else if (SqlKeywords.isWhereKeyword(tok)) {
                AlterOperationBuilder alterOperationBuilder;
                switch (action) {
                    case PartitionAction.DROP:
                        alterOperationBuilder = this.alterOperationBuilder.ofDropPartition(pos, tableToken, tableMetadata.getTableId());
                        break;
                    case PartitionAction.DETACH:
                        alterOperationBuilder = this.alterOperationBuilder.ofDetachPartition(pos, tableToken, tableMetadata.getTableId());
                        break;
                    case PartitionAction.CONVERT:
                        alterOperationBuilder = this.alterOperationBuilder.ofConvertPartition(pos, tableToken, tableMetadata.getTableId());
                        break;
                    default:
                        throw SqlException.$(pos, "WHERE clause can only be used with command DROP PARTITION, DETACH PARTITION or CONVERT PARTITION");
                }

                final int functionPosition = lexer.getPosition();
                ExpressionNode expr = parser.expr(lexer, (QueryModel) null, this);
                String designatedTimestampColumnName = null;
                int tsIndex = tableMetadata.getTimestampIndex();
                if (tsIndex >= 0) {
                    designatedTimestampColumnName = tableMetadata.getColumnName(tsIndex);
                }
                if (designatedTimestampColumnName != null) {
                    GenericRecordMetadata metadata = new GenericRecordMetadata();
                    metadata.add(new TableColumnMetadata(designatedTimestampColumnName, ColumnType.TIMESTAMP, null));
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
            case PartitionAction.CONVERT:
                alterOperationBuilder = this.alterOperationBuilder.ofConvertPartition(pos, tableToken, tableMetadata.getTableId());
                break;
            case PartitionAction.DROP:
                alterOperationBuilder = this.alterOperationBuilder.ofDropPartition(pos, tableToken, tableMetadata.getTableId());
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
            final CharSequence partitionName = GenericLexer.unquote(tok); // potentially a full timestamp, or part of it
            final int lastPosition = lexer.lastTokenPosition();

            // reader == null means it's compilation for WAL table
            // before applying to WAL writer
            final int partitionBy;
            if (reader != null) {
                partitionBy = reader.getPartitionedBy();
            } else {
                try (TableMetadata meta = engine.getTableMetadata(tableToken)) {
                    partitionBy = meta.getPartitionBy();
                }
            }
            try {
                long timestamp = PartitionBy.parsePartitionDirName(partitionName, partitionBy, 0, -1);
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
            CharSequence tok = GenericLexer.unquote(maybeExpectToken(lexer, "current column name", hadSemicolonPos < 0));
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
            if (!SqlKeywords.isToKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'to' expected'");
            }

            tok = GenericLexer.unquote(expectToken(lexer, "new column name"));
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
                    .$(", error=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            ex.position(tableNamePosition);
            throw ex;
        }
    }

    private void alterTableSetParam(CharSequence paramName, CharSequence value, int paramNamePosition, TableToken tableToken, int tableNamePosition, int tableId) throws SqlException {
        if (isMaxUncommittedRowsKeyword(paramName)) {
            int maxUncommittedRows;
            try {
                maxUncommittedRows = Numbers.parseInt(value);
            } catch (NumericException e) {
                throw SqlException.$(paramNamePosition, "invalid value [value=").put(value).put(",parameter=").put(paramName).put(']');
            }
            if (maxUncommittedRows < 0) {
                throw SqlException.$(paramNamePosition, "maxUncommittedRows must be non negative");
            }
            compiledQuery.ofAlter(alterOperationBuilder.ofSetParamUncommittedRows(tableNamePosition, tableToken, tableId, maxUncommittedRows).build());
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

            path.of(configuration.getRoot()).concat(tableToken.getDirName());
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
                    .$(", error=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            ex.position(tableNamePosition);
            throw ex;
        }
    }

    private CharSequence authorizeInsertForCopy(SecurityContext securityContext, CopyModel model) {
        final CharSequence tableName = GenericLexer.unquote(model.getTarget().token);
        final TableToken tt = engine.getTableTokenIfExists(tableName);
        if (tt != null) {
            // for existing table user have to have INSERT permission
            // if the table is to be created, later we will check for CREATE TABLE permission instead
            securityContext.authorizeInsert(tt);
        }
        return tableName;
    }

    private void cancelQuery(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !SqlKeywords.isQueryKeyword(tok)) {
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

    private void compileBegin(SqlExecutionContext executionContext) {
        compiledQuery.ofBegin();
    }

    private void compileCheckpoint(SqlExecutionContext executionContext) throws SqlException {
        executionContext.getSecurityContext().authorizeDatabaseSnapshot();
        CharSequence tok = expectToken(lexer, "'create' or 'release'");

        if (SqlKeywords.isCreateKeyword(tok)) {
            engine.checkpointCreate(executionContext);
            compiledQuery.ofCheckpointCreate();
        } else if (Chars.equalsLowerCaseAscii(tok, "release")) {
            engine.checkpointRelease();
            compiledQuery.ofCheckpointRelease();
        } else {
            throw SqlException.position(lexer.lastTokenPosition()).put("'create' or 'release' expected");
        }
    }

    private void compileCommit(SqlExecutionContext executionContext) {
        compiledQuery.ofCommit();
    }

    private RecordCursorFactory compileCopy(SecurityContext securityContext, CopyModel model) throws SqlException {
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
        final CharSequence fileName = fileNameNode != null ? GenericLexer.assertNoDots(GenericLexer.unquote(fileNameNode.token), fileNameNode.position) : null;
        assert fileName != null;

        return new CopyFactory(
                messageBus,
                engine.getCopyContext(),
                Chars.toString(tableName),
                Chars.toString(fileName),
                model
        );
    }

    private RecordCursorFactory compileCopyCancel(SqlExecutionContext executionContext, CopyModel model) throws SqlException {
        assert model.isCancel();

        long cancelCopyID;
        String cancelCopyIDStr = Chars.toString(GenericLexer.unquote(model.getTarget().token));
        try {
            cancelCopyID = Numbers.parseHexLong(cancelCopyIDStr);
        } catch (NumericException e) {
            throw SqlException.$(0, "copy cancel ID format is invalid: '").put(cancelCopyIDStr).put('\'');
        }
        return new CopyCancelFactory(
                engine.getCopyContext(),
                cancelCopyID,
                cancelCopyIDStr,
                query()
                        .$("select * from '")
                        .$(engine.getConfiguration().getSystemTableNamePrefix())
                        .$("text_import_log' where id = '")
                        .$(cancelCopyIDStr)
                        .$("' limit -1")
                        .compile(executionContext).getRecordCursorFactory()
        );
    }

    private void compileDeallocate(SqlExecutionContext executionContext) throws SqlException {
        CharSequence statementName = GenericLexer.unquote(expectToken(lexer, "statement name"));
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
        }
        compiledQuery.ofDeallocate(statementName);
    }

    private ExecutionModel compileExecutionModel(SqlExecutionContext executionContext) throws SqlException {
        ExecutionModel model = parser.parse(lexer, executionContext, this);

        if (ExecutionModel.EXPLAIN != model.getModelType()) {
            return compileExecutionModel0(executionContext, model);
        } else {
            ExplainModel explainModel = (ExplainModel) model;
            explainModel.setModel(compileExecutionModel0(executionContext, explainModel.getInnerExecutionModel()));
            return explainModel;
        }
    }

    private ExecutionModel compileExecutionModel0(SqlExecutionContext executionContext, ExecutionModel model) throws SqlException {
        switch (model.getModelType()) {
            case ExecutionModel.QUERY:
                return optimiser.optimise((QueryModel) model, executionContext, this);
            case ExecutionModel.INSERT: {
                InsertModel insertModel = (InsertModel) model;
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

    private void compileInner(@Transient @NotNull SqlExecutionContext executionContext, CharSequence sqlText) throws SqlException {
        SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
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
                executor.execute(executionContext);
                // executor might decide that SQL contains secret, otherwise we're logging it
                this.sqlText = executionContext.containsSecret() ? "** redacted for privacy ** " : sqlText;
            } catch (Throwable e) {
                // Executor is all-in-one, it parses SQL text and executes it right away. The convention is
                // that before parsing secrets the executor will notify the execution context. In that, even if
                // executor fails, the secret SQL text must not be logged
                this.sqlText = executionContext.containsSecret() ? "** redacted for privacy** " : sqlText;
                QueryProgress.logError(
                        e,
                        -1,
                        this.sqlText,
                        executionContext,
                        beginNanos,
                        false
                );
                throw e;
            }
        } else {
            this.sqlText = sqlText;
        }
        // executor is allowed to give up on the execution and fall back to standard behaviour
        if (executor == null || compiledQuery.getType() == CompiledQuery.NONE) {
            compileUsingModel(executionContext, beginNanos);
        } else {
            QueryProgress.logEnd(-1, this.sqlText, executionContext, beginNanos, false);
        }
        final short type = compiledQuery.getType();
        if ((type == CompiledQuery.ALTER || type == CompiledQuery.UPDATE) && !executionContext.isWalApplication()) {
            compiledQuery.withSqlStatement(Chars.toString(sqlText));
        }
        compiledQuery.withContext(executionContext);
    }

    private void compileLegacyCheckpoint(SqlExecutionContext executionContext) throws SqlException {
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

    private void compileRollback(SqlExecutionContext executionContext) {
        compiledQuery.ofRollback();
    }

    private void compileSet(SqlExecutionContext executionContext) {
        compiledQuery.ofSet();
    }

    private void compileUsingModel(SqlExecutionContext executionContext, long beginNanos) throws SqlException {
        // This method will not populate sql cache directly;
        // factories are assumed to be non-reentrant and once
        // factory is out of this method the caller assumes
        // full ownership over it. In that however caller may
        // choose to return factory back to this or any other
        // instance of compiler for safekeeping
        // lexer would have parsed first token to determine direction of execution flow
        lexer.unparseLast();
        codeGenerator.clear();
        long sqlId = -1;

        ExecutionModel executionModel = null;
        try {
            executionModel = compileExecutionModel(executionContext);
            switch (executionModel.getModelType()) {
                case ExecutionModel.QUERY:
                    QueryModel queryModel = (QueryModel) executionModel;
                    RecordCursorFactory factory = generateWithRetries(queryModel, executionContext);
                    compiledQuery.of(factory);
                    break;
                case ExecutionModel.CREATE_TABLE:
                    sqlId = queryRegistry.register(sqlText, executionContext);
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    createTableWithRetries(executionModel, executionContext);
                    QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos, false);
                    break;
                case ExecutionModel.COPY:
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    copy(executionContext, (CopyModel) executionModel);
                    QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos, false);
                    break;
                case ExecutionModel.RENAME_TABLE:
                    sqlId = queryRegistry.register(sqlText, executionContext);
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    final RenameTableModel rtm = (RenameTableModel) executionModel;
                    engine.rename(executionContext.getSecurityContext(), path, mem, GenericLexer.unquote(rtm.getFrom().token), renamePath, GenericLexer.unquote(rtm.getTo().token));
                    compiledQuery.ofRenameTable();
                    break;
                case ExecutionModel.UPDATE:
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    final QueryModel updateQueryModel = (QueryModel) executionModel;
                    TableToken tableToken = executionContext.getTableToken(updateQueryModel.getTableName());
                    try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(tableToken)) {
                        final UpdateOperation updateOperation = generateUpdate(updateQueryModel, executionContext, metadata);
                        compiledQuery.ofUpdate(updateOperation);
                    }
                    QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos, false);
                    // update is delayed until operation execution (for non-wal tables) or pushed to wal job completely
                    break;
                case ExecutionModel.EXPLAIN:
                    sqlId = queryRegistry.register(sqlText, executionContext);
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    compiledQuery.ofExplain(generateExplain((ExplainModel) executionModel, executionContext));
                    QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos, false);
                    break;
                default:
                    final InsertModel insertModel = (InsertModel) executionModel;
                    QueryProgress.logStart(sqlId, sqlText, executionContext, false);
                    if (insertModel.getQueryModel() != null) {
                        sqlId = queryRegistry.register(sqlText, executionContext);
                        executeWithRetries(
                                insertAsSelectMethod,
                                executionModel,
                                configuration.getCreateAsSelectRetryCount(),
                                executionContext
                        );
                    } else {
                        insert(executionModel, executionContext);
                        compiledQuery.getInsertOperation().setInsertSql(sqlText);
                    }
                    QueryProgress.logEnd(sqlId, sqlText, executionContext, beginNanos, false);
                    break;
            }

            short type = compiledQuery.getType();
            if (
                    type == CompiledQuery.INSERT_AS_SELECT // insert as select is immediate, simple insert is not!
                            || type == CompiledQuery.EXPLAIN
                            || type == CompiledQuery.RENAME_TABLE  // non-wal rename table is complete at this point
                            || type == CompiledQuery.CREATE_TABLE || type == CompiledQuery.CREATE_TABLE_AS_SELECT // create table is complete at this point
            ) {
                queryRegistry.unregister(sqlId, executionContext);
            }
        } catch (Throwable e) {
            if (executionModel != null) {
                freeTableNameFunctions(executionModel.getQueryModel());
            }
            // unregister query on error
            queryRegistry.unregister(sqlId, executionContext);

            QueryProgress.logError(
                    e,
                    sqlId,
                    sqlText,
                    executionContext,
                    beginNanos,
                    false
            );
            throw e;
        }
    }

    private void copy(SqlExecutionContext executionContext, CopyModel copyModel) throws SqlException {
        if (!copyModel.isCancel() && Chars.equalsLowerCaseAscii(copyModel.getFileName().token, "stdin")) {
            // no-op implementation
            authorizeInsertForCopy(executionContext.getSecurityContext(), copyModel);
            compiledQuery.ofCopyRemote();
        } else {
            final RecordCursorFactory copyFactory;
            if (copyModel.isCancel()) {
                copyFactory = compileCopyCancel(executionContext, copyModel);
            } else {
                copyFactory = compileCopy(executionContext.getSecurityContext(), copyModel);
            }
            compiledQuery.ofPseudoSelect(copyFactory);
        }
    }

    private long copyOrdered(
            TableWriterAPI writer,
            RecordMetadata metadata,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        long rowCount;

        if (ColumnType.isSymbolOrString(metadata.getColumnType(cursorTimestampIndex))) {
            rowCount = copyOrderedStrTimestamp(writer, cursor, copier, cursorTimestampIndex, circuitBreaker);
        } else {
            rowCount = copyOrdered0(writer, cursor, copier, cursorTimestampIndex, circuitBreaker);
        }
        writer.commit();

        return rowCount;
    }

    private long copyOrdered0(
            TableWriterAPI writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            TableWriter.Row row = writer.newRow(record.getTimestamp(cursorTimestampIndex));
            copier.copy(record, row);
            row.append();
            rowCount++;
        }

        return rowCount;
    }

    private long copyOrderedBatched(
            TableWriterAPI writer,
            RecordMetadata metadata,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long o3MaxLag,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        long rowCount;
        if (ColumnType.isSymbolOrString(metadata.getColumnType(cursorTimestampIndex))) {
            rowCount = copyOrderedBatchedStrTimestamp(writer, cursor, copier, cursorTimestampIndex, batchSize, o3MaxLag, circuitBreaker);
        } else {
            rowCount = copyOrderedBatched0(writer, cursor, copier, cursorTimestampIndex, batchSize, o3MaxLag, circuitBreaker);
        }
        writer.commit();

        return rowCount;
    }

    // returns number of copied rows
    private long copyOrderedBatched0(
            TableWriterAPI writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long o3MaxLag,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        long deadline = batchSize;
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            TableWriter.Row row = writer.newRow(record.getTimestamp(cursorTimestampIndex));
            copier.copy(record, row);
            row.append();
            if (++rowCount >= deadline) {
                writer.ic(o3MaxLag);
                deadline = rowCount + batchSize;
            }
        }
        return rowCount;
    }

    // returns number of copied rows
    private long copyOrderedBatchedStrTimestamp(
            TableWriterAPI writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long o3MaxLag,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        long deadline = batchSize;
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            CharSequence str = record.getStrA(cursorTimestampIndex);
            // It's allowed to insert ISO formatted string to timestamp column
            TableWriter.Row row = writer.newRow(SqlUtil.parseFloorPartialTimestamp(str, -1, ColumnType.STRING, ColumnType.TIMESTAMP));
            copier.copy(record, row);
            row.append();
            if (++rowCount >= deadline) {
                writer.ic(o3MaxLag);
                deadline = rowCount + batchSize;
            }
        }

        return rowCount;
    }

    //returns number of copied rows
    private long copyOrderedStrTimestamp(
            TableWriterAPI writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final CharSequence str = record.getStrA(cursorTimestampIndex);
            // It's allowed to insert ISO formatted string to timestamp column
            TableWriter.Row row = writer.newRow(SqlUtil.implicitCastStrAsTimestamp(str));
            copier.copy(record, row);
            row.append();
            rowCount++;
        }

        return rowCount;
    }

    // returns number of copied rows
    private long copyTableData(
            RecordCursor cursor,
            RecordMetadata metadata,
            TableWriterAPI writer,
            RecordMetadata writerMetadata,
            RecordToRowCopier recordToRowCopier,
            long batchSize,
            long o3MaxLag,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        int timestampIndex = writerMetadata.getTimestampIndex();
        if (timestampIndex == -1) {
            return copyUnordered(cursor, writer, recordToRowCopier, circuitBreaker);
        } else if (batchSize != -1) {
            return copyOrderedBatched(writer, metadata, cursor, recordToRowCopier, timestampIndex, batchSize, o3MaxLag, circuitBreaker);
        } else {
            return copyOrdered(writer, metadata, cursor, recordToRowCopier, timestampIndex, circuitBreaker);
        }
    }

    /**
     * Sets insertCount to number of copied rows.
     */
    private void copyTableDataAndUnlock(
            SecurityContext securityContext,
            TableToken tableToken,
            boolean isWalEnabled,
            RecordCursor cursor,
            RecordMetadata cursorMetadata,
            long batchSize,
            long o3MaxLag,
            SqlExecutionCircuitBreaker circuitBreaker
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
                        engine.getConfiguration().getRoot(),
                        engine.getDdlListener(tableToken),
                        engine.getCheckpointStatus(),
                        engine.getMetrics(),
                        engine
                );
            } else {
                writerAPI = engine.getTableWriterAPI(tableToken, "create as select");
            }

            RecordMetadata writerMetadata = writerAPI.getMetadata();
            entityColumnFilter.of(writerMetadata.getColumnCount());
            this.insertCount = copyTableData(
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
                    circuitBreaker
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
                engine.unlock(securityContext, tableToken, writer, false);
            }
        }
    }

    private void copyTableReaderMetadataToCreateTableModel(SqlExecutionContext executionContext, CreateTableModel model) throws SqlException {
        ExpressionNode likeTableName = model.getLikeTableName();
        CharSequence likeTableNameToken = likeTableName.token;
        TableToken tableToken = executionContext.getTableTokenIfExists(likeTableNameToken);
        if (tableToken == null) {
            throw SqlException.$(likeTableName.position, "table does not exist [table=").put(likeTableNameToken).put(']');
        }
        try (TableReader rdr = executionContext.getReader(tableToken)) {
            model.setO3MaxLag(rdr.getO3MaxLag());
            model.setMaxUncommittedRows(rdr.getMaxUncommittedRows());
            TableReaderMetadata rdrMetadata = rdr.getMetadata();
            for (int i = 0; i < rdrMetadata.getColumnCount(); i++) {
                int columnType = rdrMetadata.getColumnType(i);
                boolean isSymbol = ColumnType.isSymbol(columnType);
                int symbolCapacity = isSymbol ? rdr.getSymbolMapReader(i).getSymbolCapacity() : configuration.getDefaultSymbolCapacity();
                model.addColumn(rdrMetadata.getColumnName(i), columnType, symbolCapacity);
                if (isSymbol) {
                    model.cached(rdr.getSymbolMapReader(i).isCached());
                }
                model.setIndexFlags(rdrMetadata.isColumnIndexed(i), rdrMetadata.getIndexValueBlockCapacity(i));
                if (rdrMetadata.isDedupKey(i)) {
                    model.setDedupKeyFlag(i);
                }
            }
            model.setPartitionBy(SqlUtil.nextLiteral(sqlNodePool, PartitionBy.toString(rdr.getPartitionedBy()), 0));
            if (rdrMetadata.getTimestampIndex() != -1) {
                model.setTimestamp(SqlUtil.nextLiteral(sqlNodePool, rdrMetadata.getColumnName(rdrMetadata.getTimestampIndex()), 0));
            }
            model.setWalEnabled(configuration.isWalSupported() && rdrMetadata.isWalEnabled());
        }
        model.setLikeTableName(null); // resetting like table name as the metadata is copied already at this point.
    }

    /**
     * Returns number of copied rows.
     */
    private long copyUnordered(RecordCursor cursor, TableWriterAPI writer, RecordToRowCopier copier, SqlExecutionCircuitBreaker circuitBreaker) {
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            TableWriter.Row row = writer.newRow();
            copier.copy(record, row);
            row.append();
            rowCount++;
        }
        writer.commit();

        return rowCount;
    }

    private void createTable(final ExecutionModel model, SqlExecutionContext executionContext) throws SqlException {
        final CreateTableModel createTableModel = (CreateTableModel) model;
        final ExpressionNode name = createTableModel.getName();

        // Fast path for CREATE TABLE IF NOT EXISTS in scenario when the table already exists
        final int status = executionContext.getTableStatus(path, name.token);
        if (createTableModel.isIgnoreIfExists() && status == TableUtils.TABLE_EXISTS) {
            compiledQuery.ofCreateTable(executionContext.getTableTokenIfExists(name.token));
        } else if (status == TableUtils.TABLE_EXISTS) {
            throw SqlException.$(name.position, "table already exists");
        } else {
            // create table (...) ... in volume volumeAlias;
            CharSequence volumeAlias = createTableModel.getVolumeAlias();
            if (volumeAlias != null) {
                CharSequence volumePath = configuration.getVolumeDefinitions().resolveAlias(volumeAlias);
                if (volumePath != null) {
                    if (!ff.isDirOrSoftLinkDir(path.of(volumePath).$())) {
                        throw CairoException.critical(0).put("not a valid path for volume [alias=").put(volumeAlias).put(", path=").put(path).put(']');
                    }
                } else {
                    throw SqlException.position(0).put("volume alias is not allowed [alias=").put(volumeAlias).put(']');
                }
            }

            final TableToken tableToken;
            this.insertCount = -1;
            if (createTableModel.getQueryModel() == null) {
                try {
                    if (createTableModel.getLikeTableName() != null) {
                        copyTableReaderMetadataToCreateTableModel(executionContext, createTableModel);
                    }
                    if (volumeAlias == null) {
                        tableToken = engine.createTable(
                                executionContext.getSecurityContext(),
                                mem,
                                path,
                                createTableModel.isIgnoreIfExists(),
                                createTableModel,
                                false
                        );
                    } else {
                        tableToken = engine.createTableInVolume(
                                executionContext.getSecurityContext(),
                                mem,
                                path,
                                createTableModel.isIgnoreIfExists(),
                                createTableModel,
                                false
                        );
                    }
                } catch (EntryUnavailableException e) {
                    throw SqlException.$(name.position, "table already exists");
                } catch (CairoException e) {
                    if (e.isAuthorizationError() || e.isCancellation()) {
                        // No point printing stack trace for authorization or cancellation errors
                        LOG.error().$("could not create table [error=").$(e.getFlyweightMessage()).I$();
                    } else {
                        LOG.error().$("could not create table [error=").$((Throwable) e).I$();
                    }
                    if (e.isInterruption()) {
                        throw e;
                    }
                    throw SqlException.$(name.position, "Could not create table, ").put(e.getFlyweightMessage());
                }
            } else {
                tableToken = createTableFromCursorExecutor(createTableModel, executionContext, volumeAlias, name.position);
            }

            if (createTableModel.getQueryModel() == null) {
                compiledQuery.ofCreateTable(tableToken);
            } else {
                compiledQuery.ofCreateTableAsSelect(tableToken, insertCount);
            }
        }
    }

    private TableToken createTableFromCursorExecutor(
            CreateTableModel model,
            SqlExecutionContext executionContext,
            CharSequence volumeAlias,
            int position
    ) throws SqlException {
        executionContext.setUseSimpleCircuitBreaker(true);
        try (
                final RecordCursorFactory factory = generate(model.getQueryModel(), executionContext);
                final RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            typeCast.clear();
            final RecordMetadata metadata = factory.getMetadata();
            validateTableModelAndCreateTypeCast(model, metadata, typeCast);
            boolean keepLock = !model.isWalEnabled();

            final TableToken tableToken;

            if (volumeAlias == null) {
                tableToken = engine.createTable(
                        executionContext.getSecurityContext(),
                        mem,
                        path,
                        false,
                        tableStructureAdapter.of(model, metadata, typeCast),
                        keepLock
                );
            } else {
                tableToken = engine.createTableInVolume(
                        executionContext.getSecurityContext(),
                        mem,
                        path,
                        false,
                        tableStructureAdapter.of(model, metadata, typeCast),
                        keepLock
                );
            }

            SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
            try {
                copyTableDataAndUnlock(
                        executionContext.getSecurityContext(),
                        tableToken,
                        model.isWalEnabled(),
                        cursor,
                        metadata,
                        model.getBatchSize(),
                        model.getBatchO3MaxLag(),
                        circuitBreaker
                );
            } catch (CairoException e) {
                e.position(position);
                LogRecord record = LOG.error()
                        .$("could not create table as select [model=`").$(model)
                        .$("`, message=[")
                        .$(e.getFlyweightMessage());
                if (!e.isCancellation()) {
                    record.$(", errno=").$(e.getErrno());
                } else {
                    record.$(']'); // we are closing bracket for the underlying message
                }
                record.I$();
                engine.drop(path, tableToken);
                engine.unlockTableName(tableToken);
                throw e;
            }
            return tableToken;
        } finally {
            executionContext.setUseSimpleCircuitBreaker(false);
        }
    }

    /**
     * Creates new table.
     * <p>
     * Table name must not exist. Existence check relies on directory existence followed by attempt to clarify what
     * that directory is. Sometimes it can be just empty directory, which prevents new table from being created.
     * <p>
     * Table name can be utf8 encoded but must not contain '.' (dot). Dot is used to separate table and field name,
     * where table is uses as an alias.
     * <p>
     * Creating table from column definition looks like:
     * <code>
     * create table x (column_name column_type, ...) [timestamp(column_name)] [partition by ...]
     * </code>
     * For non-partitioned table partition by value would be NONE. For any other type of partition timestamp
     * has to be defined as reference to TIMESTAMP (type) column.
     *
     * @param executionModel   created from parsed sql.
     * @param executionContext provides access to bind variables and authorization module
     * @throws SqlException contains text of error and error position in SQL text.
     */
    private void createTableWithRetries(
            ExecutionModel executionModel,
            SqlExecutionContext executionContext
    ) throws SqlException {
        executeWithRetries(createTableMethod, executionModel, configuration.getCreateAsSelectRetryCount(), executionContext);
    }

    private void executeWithRetries(
            ExecutableMethod method,
            ExecutionModel executionModel,
            int retries,
            SqlExecutionContext executionContext
    ) throws SqlException {
        int attemptsLeft = retries;
        do {
            try {
                method.execute(executionModel, executionContext);
                return;
            } catch (TableReferenceOutOfDateException e) {
                attemptsLeft--;
                clear();
                lexer.restart();
                executionModel = compileExecutionModel(executionContext);
            }
        } while (attemptsLeft > 0);

        throw SqlException.position(0).put("underlying cursor is extremely volatile");
    }

    private int filterPartitions(
            Function function,
            int functionPosition,
            TableReader reader,
            AlterOperationBuilder changePartitionStatement
    ) {
        int affectedPartitions = 0;
        // Iterate partitions in descending order so if folders are missing on disk
        // removePartition does not fail to determine next minTimestamp
        final int partitionCount = reader.getPartitionCount();
        if (partitionCount > 0) { // table may be empty
            for (int i = partitionCount - 2; i > -1; i--) {
                long partitionTimestamp = reader.getPartitionTimestampByIndex(i);
                partitionFunctionRec.setTimestamp(partitionTimestamp);
                if (function.getBool(partitionFunctionRec)) {
                    changePartitionStatement.addPartitionToList(partitionTimestamp, functionPosition);
                    affectedPartitions++;
                }
            }

            // do action on last partition at the end, it's more expensive than others
            long partitionTimestamp = reader.getPartitionTimestampByIndex(partitionCount - 1);
            partitionFunctionRec.setTimestamp(partitionTimestamp);
            if (function.getBool(partitionFunctionRec)) {
                changePartitionStatement.addPartitionToList(partitionTimestamp, functionPosition);
                affectedPartitions++;
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
            final RecordCursorFactory recordCursorFactory = prepareForUpdate(
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
        final RecordCursorFactory recordCursorFactory = prepareForUpdate(
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

    private void insert(ExecutionModel executionModel, SqlExecutionContext executionContext) throws SqlException {
        final InsertModel model = (InsertModel) executionModel;
        final ExpressionNode tableNameExpr = model.getTableNameExpr();
        ObjList<Function> valueFunctions = null;
        TableToken token = tableExistsOrFail(tableNameExpr.position, tableNameExpr.token, executionContext);

        try (TableMetadata metadata = executionContext.getMetadataForWrite(token)) {
            final long metadataVersion = metadata.getMetadataVersion();
            final InsertOperationImpl insertOperation = new InsertOperationImpl(engine, metadata.getTableToken(), metadataVersion);
            final int metadataTimestampIndex = metadata.getTimestampIndex();
            final ObjList<CharSequence> columnNameList = model.getColumnNameList();
            final int columnSetSize = columnNameList.size();
            for (int tupleIndex = 0, n = model.getRowTupleCount(); tupleIndex < n; tupleIndex++) {
                Function timestampFunction = null;
                listColumnFilter.clear();
                if (columnSetSize > 0) {
                    valueFunctions = new ObjList<>(columnSetSize);
                    for (int i = 0; i < columnSetSize; i++) {
                        int metadataColumnIndex = metadata.getColumnIndexQuiet(columnNameList.getQuick(i));
                        if (metadataColumnIndex > -1) {
                            final ExpressionNode node = model.getRowTupleValues(tupleIndex).getQuick(i);
                            final Function function = functionParser.parseFunction(
                                    node,
                                    EmptyRecordMetadata.INSTANCE,
                                    executionContext
                            );

                            insertValidateFunctionAndAddToList(
                                    model,
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
                            }

                        } else {
                            throw SqlException.invalidColumn(model.getColumnPosition(i), columnNameList.getQuick(i));
                        }
                    }
                } else {
                    final int columnCount = metadata.getColumnCount();
                    final ObjList<ExpressionNode> values = model.getRowTupleValues(tupleIndex);
                    final int valueCount = values.size();
                    if (columnCount != valueCount) {
                        throw SqlException.$(
                                        model.getEndOfRowTupleValuesPosition(tupleIndex),
                                        "row value count does not match column count [expected="
                                ).put(columnCount).put(", actual=").put(values.size())
                                .put(", tuple=").put(tupleIndex + 1).put(']');
                    }
                    valueFunctions = new ObjList<>(columnCount);

                    for (int i = 0; i < columnCount; i++) {
                        final ExpressionNode node = values.getQuick(i);

                        Function function = functionParser.parseFunction(node, EmptyRecordMetadata.INSTANCE, executionContext);
                        insertValidateFunctionAndAddToList(
                                model,
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
                        }
                    }
                }

                // validate timestamp
                if (metadataTimestampIndex > -1) {
                    if (timestampFunction == null) {
                        throw SqlException.$(0, "insert statement must populate timestamp");
                    } else if (ColumnType.isNull(timestampFunction.getType()) || timestampFunction.isNullConstant()) {
                        throw SqlException.$(0, "designated timestamp column cannot be NULL");
                    }
                }


                VirtualRecord record = new VirtualRecord(valueFunctions);
                RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(asm, record, metadata, listColumnFilter);
                insertOperation.addInsertRow(new InsertRowImpl(record, copier, timestampFunction, tupleIndex));
            }

            insertOperation.setColumnNames(columnNameList);
            compiledQuery.ofInsert(insertOperation);
        } catch (SqlException e) {
            Misc.freeObjList(valueFunctions);
            throw e;
        }
    }

    private void insertAsSelect(ExecutionModel executionModel, SqlExecutionContext executionContext) throws SqlException {
        final InsertModel model = (InsertModel) executionModel;
        final ExpressionNode tableNameExpr = model.getTableNameExpr();

        TableToken tableToken = tableExistsOrFail(tableNameExpr.position, tableNameExpr.token, executionContext);
        long insertCount;

        executionContext.setUseSimpleCircuitBreaker(true);
        try (
                TableWriterAPI writer = engine.getTableWriterAPI(tableToken, "insertAsSelect");
                RecordCursorFactory factory = generate(model.getQueryModel(), executionContext);
                TableMetadata writerMetadata = executionContext.getMetadataForWrite(tableToken)
        ) {
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
                        if (fromType != ColumnType.TIMESTAMP && fromType != ColumnType.STRING) {
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

            SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();

            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                try {
                    if (writerTimestampIndex == -1) {
                        insertCount = copyUnordered(cursor, writer, copier, circuitBreaker);
                    } else {
                        if (model.getBatchSize() != -1) {
                            insertCount = copyOrderedBatched(
                                    writer,
                                    factory.getMetadata(),
                                    cursor,
                                    copier,
                                    timestampIndexFound,
                                    model.getBatchSize(),
                                    model.getO3MaxLag(),
                                    circuitBreaker
                            );
                        } else {
                            insertCount = copyOrdered(writer, factory.getMetadata(), cursor, copier, timestampIndexFound, circuitBreaker);
                        }
                    }
                } catch (Throwable e) {
                    // rollback data when system error occurs
                    writer.rollback();
                    throw e;
                }
            }
        } finally {
            executionContext.setUseSimpleCircuitBreaker(false);
        }

        compiledQuery.ofInsertAsSelect(insertCount);
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
        if (function.isUndefined()) {
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

    private RecordCursorFactory prepareForUpdate(
            TableToken tableToken,
            @Transient QueryModel selectQueryModel,
            @Transient QueryModel updateQueryModel,
            @Transient SqlExecutionContext executionContext
    ) throws SqlException {
        final IntList tableColumnTypes = selectQueryModel.getUpdateTableColumnTypes();
        final ObjList<CharSequence> tableColumnNames = selectQueryModel.getUpdateTableColumnNames();

        RecordCursorFactory updateToDataCursorFactory = generateFactory(selectQueryModel, executionContext, false);
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

                if (virtualColumnType != tableColumnType && !isIPv4UpdateCast(virtualColumnType, tableColumnType)) {
                    if (!ColumnType.isSymbolOrString(tableColumnType) || !ColumnType.isAssignableFrom(virtualColumnType, ColumnType.STRING)) {
                        // get column position
                        ExpressionNode setRhs = updateQueryModel.getNestedModel().getColumns().getQuick(i).getAst();
                        throw SqlException.inconvertibleTypes(setRhs.position, virtualColumnType, "", tableColumnType, updateColumnName);
                    }
                }
            }
            return updateToDataCursorFactory;
        } catch (Throwable th) {
            updateToDataCursorFactory.close();
            throw th;
        }
    }

    private void reindexTable(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !isTableKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "TABLE expected");
        }

        tok = SqlUtil.fetchNext(lexer);

        if (tok == null || Chars.equals(tok, ',')) {
            throw SqlException.$(lexer.getPosition(), "table name expected");
        }

        if (Chars.isQuoted(tok)) {
            tok = GenericLexer.unquote(tok);
        }
        TableToken tableToken = tableExistsOrFail(lexer.lastTokenPosition(), tok, executionContext);
        try (IndexBuilder indexBuilder = new IndexBuilder(configuration)) {
            indexBuilder.of(path.of(configuration.getRoot()).concat(tableToken.getDirName()));

            tok = SqlUtil.fetchNext(lexer);
            CharSequence columnName = null;

            if (tok != null && SqlKeywords.isColumnKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);
                if (Chars.isQuoted(tok)) {
                    tok = GenericLexer.unquote(tok);
                }
                if (tok == null || TableUtils.isValidColumnName(tok, configuration.getMaxFileNameLength())) {
                    columnName = GenericLexer.immutableOf(tok);
                    tok = SqlUtil.fetchNext(lexer);
                }
            }

            CharSequence partition = null;
            if (tok != null && SqlKeywords.isPartitionKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);

                if (Chars.isQuoted(tok)) {
                    tok = GenericLexer.unquote(tok);
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

    private TableToken tableExistsOrFail(int position, CharSequence tableName, SqlExecutionContext executionContext) throws SqlException {
        if (executionContext.getTableStatus(path, tableName) != TableUtils.TABLE_EXISTS) {
            throw SqlException.tableDoesNotExist(position, tableName);
        }
        return executionContext.getTableTokenIfExists(tableName);
    }

    private void truncateTables(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);

        if (tok == null) {
            throw SqlException.$(lexer.getPosition(), "TABLE expected");
        }

        if (!isTableKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "TABLE expected");
        }

        tok = SqlUtil.fetchNext(lexer);
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
                    tok = GenericLexer.unquote(tok);
                }
                TableToken tableToken = tableExistsOrFail(lexer.lastTokenPosition(), tok, executionContext);
                executionContext.getSecurityContext().authorizeTableTruncate(tableToken);
                try {
                    tableWriters.add(engine.getTableWriterAPI(tableToken, "truncateTables"));
                } catch (CairoException e) {
                    LOG.info().$("table busy [table=").$(tok).$(", e=").$((Throwable) e).I$();
                    throw SqlException.$(lexer.lastTokenPosition(), "table '").put(tok).put("' could not be truncated: ").put(e);
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
                    LOG.error().$("could not truncate [table=").$(writer.getTableToken()).$(", e=").$((Sinkable) e).$(']').$();
                    throw e;
                }
            }
        } finally {
            for (int i = 0, n = tableWriters.size(); i < n; i++) {
                tableWriters.getQuick(i).close();
            }
            tableWriters.clear();
        }
        compiledQuery.ofTruncate();
    }

    private void vacuum(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = expectToken(lexer, "'table'");
        // It used to be VACUUM PARTITIONS but become VACUUM TABLE
        boolean partitionsKeyword = isPartitionsKeyword(tok);
        if (partitionsKeyword || isTableKeyword(tok)) {
            CharSequence tableName = expectToken(lexer, "table name");
            SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            tableName = GenericLexer.assertNoDotsAndSlashes(GenericLexer.unquote(tableName), lexer.lastTokenPosition());
            int tableNamePos = lexer.lastTokenPosition();
            CharSequence eol = SqlUtil.fetchNext(lexer);
            if (eol == null || Chars.equals(eol, ';')) {
                TableToken tableToken = tableExistsOrFail(lexer.lastTokenPosition(), tableName, executionContext);
                try (TableReader rdr = executionContext.getReader(tableToken)) {
                    int partitionBy = rdr.getMetadata().getPartitionBy();
                    if (PartitionBy.isPartitioned(partitionBy)) {
                        executionContext.getSecurityContext().authorizeTableVacuum(rdr.getTableToken());
                        if (!TableUtils.schedulePurgeO3Partitions(messageBus, rdr.getTableToken(), partitionBy)) {
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

    private void validateAndOptimiseInsertAsSelect(SqlExecutionContext executionContext, InsertModel model) throws SqlException {
        final QueryModel queryModel = optimiser.optimise(model.getQueryModel(), executionContext, this);
        int columnNameListSize = model.getColumnNameList().size();
        if (columnNameListSize > 0 && queryModel.getBottomUpColumns().size() != columnNameListSize) {
            throw SqlException.$(model.getTableNameExpr().position, "column count mismatch");
        }
        model.setQueryModel(queryModel);
    }

    private void validateTableModelAndCreateTypeCast(
            @Transient CreateTableModel model,
            @Transient RecordMetadata metadata,
            @Transient IntIntHashMap typeCast
    ) throws SqlException {
        CharSequenceObjHashMap<ColumnCastModel> castModels = model.getColumnCastModels();
        ObjList<CharSequence> castColumnNames = castModels.keys();

        for (int i = 0, n = castColumnNames.size(); i < n; i++) {
            CharSequence columnName = castColumnNames.getQuick(i);
            int index = metadata.getColumnIndexQuiet(columnName);
            ColumnCastModel ccm = castModels.get(columnName);
            // the only reason why columns cannot be found at this stage is
            // concurrent table modification of table structure
            if (index == -1) {
                // Cast isn't going to go away when we reparse SQL. We must make this
                // permanent error
                throw SqlException.invalidColumn(ccm.getColumnNamePos(), columnName);
            }
            int from = metadata.getColumnType(index);
            int to = ccm.getColumnType();
            if (isCompatibleCase(from, to)) {
                int modelColumnIndex = model.getColumnIndex(columnName);
                if (!ColumnType.isSymbol(to) && model.isIndexed(modelColumnIndex)) {
                    throw SqlException.$(ccm.getColumnTypePos(), "indexes are supported only for SYMBOL columns: ").put(columnName);
                }
                typeCast.put(index, to);
            } else {
                throw SqlException.unsupportedCast(ccm.getColumnTypePos(), columnName, from, to);
            }
        }

        // validate that all indexes are specified only on columns with symbol type
        for (int i = 0, n = model.getColumnCount(); i < n; i++) {
            CharSequence columnName = model.getColumnName(i);
            ColumnCastModel ccm = castModels.get(columnName);
            if (ccm != null) {
                // We already checked this column when validating casts.
                continue;
            }
            int index = metadata.getColumnIndexQuiet(columnName);
            assert index > -1 : "wtf? " + columnName;
            if (!ColumnType.isSymbol(metadata.getColumnType(index)) && model.isIndexed(i)) {
                throw SqlException.$(0, "indexes are supported only for SYMBOL columns: ").put(columnName);
            }

            if (ColumnType.isNull(metadata.getColumnType(index))) {
                throw SqlException.$(0, "cannot create NULL-type column, please use type cast, e.g. ").put(columnName).put("::").put("type");
            }
        }

        // validate type of timestamp column
        // no need to worry that column will not resolve
        ExpressionNode timestamp = model.getTimestamp();
        if (timestamp != null && metadata.getColumnType(timestamp.token) != ColumnType.TIMESTAMP) {
            throw SqlException.position(timestamp.position).put("TIMESTAMP column expected [actual=").put(ColumnType.nameOf(metadata.getColumnType(timestamp.token))).put(']');
        }

        if (PartitionBy.isPartitioned(model.getPartitionBy()) && model.getTimestampIndex() == -1 && metadata.getTimestampIndex() == -1) {
            throw SqlException.position(0).put("timestamp is not defined");
        }
    }

    protected static CharSequence expectToken(GenericLexer lexer, CharSequence expected) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put(expected).put(" expected");
        }
        return tok;
    }

    protected static CharSequence maybeExpectToken(GenericLexer lexer, CharSequence expected, boolean expect) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (expect && tok == null) {
            throw SqlException.position(lexer.getPosition()).put(expected).put(" expected");
        }
        return tok;
    }

    protected void addColumnSuffix(
            @Transient SecurityContext securityContext,
            @Nullable CharSequence tok,
            TableToken tableToken,
            AlterOperationBuilder addColumnStatement
    ) throws SqlException {
        if (tok != null) {
            throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
        }
    }

    // returns true if it dropped the table, false otherwise (or throws exception)
    protected boolean dropTable(
            SqlExecutionContext executionContext,
            CharSequence tableName,
            int tableNamePosition,
            boolean hasIfExists
    ) throws SqlException {
        final TableToken tableToken = executionContext.getTableTokenIfExists(tableName);
        if (tableToken == null) {
            if (hasIfExists) {
                compiledQuery.ofDrop();
                return false;
            }
            throw SqlException.tableDoesNotExist(tableNamePosition, tableName);
        }
        executionContext.getSecurityContext().authorizeTableDrop(tableToken);
        engine.drop(path, tableToken);
        compiledQuery.ofDrop();
        return true;
    }

    RecordCursorFactory generate(
            @Transient QueryModel queryModel,
            @Transient SqlExecutionContext executionContext
    ) throws SqlException {
        return generateFactory(queryModel, executionContext, false);
    }

    protected RecordCursorFactory generateFactory(
            QueryModel selectQueryModel,
            SqlExecutionContext executionContext,
            boolean isSelect
    ) throws SqlException {
        RecordCursorFactory factory = codeGenerator.generate(selectQueryModel, executionContext);
        if (isSelect) {
            return new QueryProgress(queryRegistry, sqlText, factory);
        } else {
            return factory;
        }
    }

    RecordCursorFactory generateWithRetries(@Transient QueryModel initialQueryModel, @Transient SqlExecutionContext executionContext) throws SqlException {
        QueryModel queryModel = initialQueryModel;
        int remainingRetries = maxRecompileAttempts;
        for (; ; ) {
            try {
                return generateFactory(queryModel, executionContext, true);
            } catch (TableReferenceOutOfDateException e) {
                if (--remainingRetries < 0) {
                    throw SqlException.$(0, e.getFlyweightMessage());
                }
                LOG.info().$("retrying plan [q=`").$(queryModel).$("`, fd=").$(executionContext.getRequestFd()).$(']').$();
                clear();
                lexer.restart();
                queryModel = (QueryModel) compileExecutionModel(executionContext);
            }
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
        final KeywordBasedExecutor compileBegin = this::compileBegin;
        final KeywordBasedExecutor compileCommit = this::compileCommit;
        final KeywordBasedExecutor compileRollback = this::compileRollback;
        final KeywordBasedExecutor truncateTables = this::truncateTables;
        final KeywordBasedExecutor alterTable = this::alterTable;
        final KeywordBasedExecutor reindexTable = this::reindexTable;
        final KeywordBasedExecutor dropStatement = dropStmtCompiler::executorSelector;
        final KeywordBasedExecutor sqlBackup = backupAgent::sqlBackup;
        final KeywordBasedExecutor vacuumTable = this::vacuum;
        final KeywordBasedExecutor compileCheckpoint = this::compileCheckpoint;
        final KeywordBasedExecutor compileLegacyCheckpoint = this::compileLegacyCheckpoint;
        final KeywordBasedExecutor compileDeallocate = this::compileDeallocate;
        final KeywordBasedExecutor cancelQuery = this::cancelQuery;

        keywordBasedExecutors.put("truncate", truncateTables);
        keywordBasedExecutors.put("alter", alterTable);
        keywordBasedExecutors.put("reindex", reindexTable);
        keywordBasedExecutors.put("set", compileSet);
        keywordBasedExecutors.put("begin", compileBegin);
        keywordBasedExecutors.put("commit", compileCommit);
        keywordBasedExecutors.put("rollback", compileRollback);
        keywordBasedExecutors.put("discard", compileSet);
        keywordBasedExecutors.put("close", compileSet); // no-op
        keywordBasedExecutors.put("unlisten", compileSet); // no-op
        keywordBasedExecutors.put("reset", compileSet); // no-op
        keywordBasedExecutors.put("drop", dropStatement);
        keywordBasedExecutors.put("backup", sqlBackup);
        keywordBasedExecutors.put("vacuum", vacuumTable);
        keywordBasedExecutors.put("checkpoint", compileCheckpoint);
        keywordBasedExecutors.put("snapshot", compileLegacyCheckpoint);
        keywordBasedExecutors.put("deallocate", compileDeallocate);
        keywordBasedExecutors.put("cancel", cancelQuery);
    }

    protected void unknownAlterStatement(SqlExecutionContext executionContext, CharSequence tok) throws SqlException {
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put("'table' expected");
        }
        throw SqlException.position(lexer.lastTokenPosition()).put("'table' expected");
    }

    protected void unknownDropColumnSuffix(
            @Transient SecurityContext securityContext,
            CharSequence tok,
            TableToken tableToken,
            AlterOperationBuilder dropColumnStatement
    ) throws SqlException {
        throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
    }

    protected void unknownDropStatement(SqlExecutionContext executionContext, CharSequence tok) throws SqlException {
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put("'table' or 'all tables' expected");
        }
        throw SqlException.position(lexer.lastTokenPosition()).put("'table' or 'all tables' expected");
    }

    protected void unknownDropTableSuffix(
            SqlExecutionContext executionContext,
            CharSequence tok,
            CharSequence tableName,
            int tableNamePosition,
            boolean hasIfExists
    ) throws SqlException {
        throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
    }

    @FunctionalInterface
    private interface ExecutableMethod {
        void execute(ExecutionModel model, SqlExecutionContext sqlExecutionContext) throws SqlException;
    }

    @FunctionalInterface
    public interface KeywordBasedExecutor {
        void execute(SqlExecutionContext executionContext) throws SqlException;
    }

    public final static class PartitionAction {
        public static final int ATTACH = 2;
        public static final int CONVERT = 4;
        public static final int DETACH = 3;
        public static final int DROP = 1;
    }

    private static class TableStructureAdapter implements TableStructure {
        private RecordMetadata metadata;
        private CreateTableModel model;
        private int timestampIndex;
        private IntIntHashMap typeCast;

        @Override
        public int getColumnCount() {
            return model.getColumnCount();
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            return model.getColumnName(columnIndex);
        }

        @Override
        public int getColumnType(int columnIndex) {
            int castIndex = typeCast.keyIndex(columnIndex);
            if (castIndex < 0) {
                return typeCast.valueAt(castIndex);
            }
            return metadata.getColumnType(columnIndex);
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return model.getIndexBlockCapacity(columnIndex);
        }

        @Override
        public int getMaxUncommittedRows() {
            return model.getMaxUncommittedRows();
        }

        @Override
        public long getO3MaxLag() {
            return model.getO3MaxLag();
        }

        @Override
        public int getPartitionBy() {
            return model.getPartitionBy();
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            final ColumnCastModel ccm = model.getColumnCastModels().get(metadata.getColumnName(columnIndex));
            if (ccm != null) {
                return ccm.getSymbolCacheFlag();
            }
            return model.getSymbolCacheFlag(columnIndex);
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            final ColumnCastModel ccm = model.getColumnCastModels().get(metadata.getColumnName(columnIndex));
            if (ccm != null) {
                return ccm.getSymbolCapacity();
            } else {
                return model.getSymbolCapacity(columnIndex);
            }
        }

        @Override
        public CharSequence getTableName() {
            return model.getTableName();
        }

        @Override
        public int getTimestampIndex() {
            return timestampIndex;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return model.isDedupKey(columnIndex);
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return model.isIndexed(columnIndex);
        }

        @Override
        public boolean isSequential(int columnIndex) {
            return model.isSequential(columnIndex);
        }

        @Override
        public boolean isWalEnabled() {
            return model.isWalEnabled();
        }

        TableStructureAdapter of(CreateTableModel model, RecordMetadata metadata, IntIntHashMap typeCast) {
            if (model.getTimestampIndex() != -1) {
                timestampIndex = model.getTimestampIndex();
            } else {
                timestampIndex = metadata.getTimestampIndex();
            }
            this.model = model;
            this.metadata = metadata;
            this.typeCast = typeCast;
            return this;
        }
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

        private void backupTable(@NotNull TableToken tableToken, SqlExecutionContext executionContext) throws SqlException {
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

            String tableName = tableToken.getTableName();
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
                    } finally {
                        mem.close();
                    }

                    // copy the data
                    try (TableWriter backupWriter = engine.getBackupWriter(tableToken, cachedBackupTmpRoot)) {
                        RecordMetadata writerMetadata = backupWriter.getMetadata();
                        srcPath.of(tableName).slash().put(reader.getMetadataVersion()).$();
                        RecordToRowCopier recordToRowCopier = tableBackupRowCopiedCache.get(srcPath);
                        if (null == recordToRowCopier) {
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
                        try (
                                SqlExecutionContext allowAllContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
                                RecordCursorFactory factory = engine.select(sink, allowAllContext);
                                RecordCursor cursor = factory.getCursor(allowAllContext)
                        ) {
                            // statement/query timeout value is most likely too small for backup operation
                            copyTableData(
                                    cursor,
                                    factory.getMetadata(),
                                    backupWriter,
                                    writerMetadata,
                                    recordToRowCopier,
                                    configuration.getCreateTableModelBatchSize(),
                                    configuration.getO3MaxLag(),
                                    SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
                            );
                        }
                        backupWriter.commit();
                    }
                } // release reader lock
                int renameRootLen = dstPath.size();
                try {
                    dstPath.trimTo(renameRootLen).concat(tableToken);
                    TableUtils.renameOrFail(ff, auxPath.trimTo(tableRootLen).$(), dstPath.$());
                    LOG.info().$("backup complete [table=").utf8(tableName).$(", to=").$(dstPath).I$();
                } finally {
                    dstPath.trimTo(renameRootLen).$();
                }
            } catch (CairoException e) {
                LOG.info()
                        .$("could not backup [table=").utf8(tableName)
                        .$(", ex=").$(e.getFlyweightMessage())
                        .$(", errno=").$(e.getErrno())
                        .$(']').$();
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
            long epochMicros = configuration.getMicrosecondClock().getTicks();
            dstPath.of(configuration.getBackupRoot()).slash();
            int plen = dstPath.size();
            int n = 0;
            // There is a race here, two threads could try and create the same dstPath,
            // only one will succeed the other will throw a CairoException. It could be serialised
            do {
                dstPath.trimTo(plen);
                format.format(epochMicros, configuration.getDefaultDateLocale(), null, dstPath);
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

        private void sqlBackup(SqlExecutionContext executionContext) throws SqlException {
            if (null == configuration.getBackupRoot()) {
                throw CairoException.nonCritical().put("backup is disabled, server.conf property 'cairo.sql.backup.root' is not set");
            }
            CharSequence tok = SqlUtil.fetchNext(lexer);
            if (null != tok) {
                if (isTableKeyword(tok)) {
                    sqlTableBackup(executionContext);
                    return;
                }
                if (isDatabaseKeyword(tok)) {
                    sqlDatabaseBackup(executionContext);
                    return;
                }
            }
            throw SqlException.position(lexer.lastTokenPosition()).put("expected 'table' or 'database'");
        }

        private void sqlDatabaseBackup(SqlExecutionContext executionContext) throws SqlException {
            mkBackupDstRoot();
            mkBackupDstDir(configuration.getDbDirectory(), "could not create backup [db dir=");

            // backup tables
            engine.getTableTokens(tableTokenBucket, false);
            executionContext.getSecurityContext().authorizeTableBackup(tableTokens);
            for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
                backupTable(tableTokenBucket.get(i), executionContext);
            }

            srcPath.of(configuration.getRoot()).$();
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
                    if (null == tok) {
                        throw SqlException.position(lexer.getPosition()).put("expected a table name");
                    }
                    final CharSequence tableName = GenericLexer.assertNoDotsAndSlashes(GenericLexer.unquote(tok), lexer.lastTokenPosition());
                    TableToken tableToken = tableExistsOrFail(lexer.lastTokenPosition(), tableName, executionContext);
                    tableTokens.add(tableToken);
                    tok = SqlUtil.fetchNext(lexer);
                    if (null == tok || Chars.equals(tok, ';')) {
                        break;
                    }
                    if (!Chars.equals(tok, ',')) {
                        throw SqlException.position(lexer.lastTokenPosition()).put("expected ','");
                    }
                }

                executionContext.getSecurityContext().authorizeTableBackup(tableTokens);

                for (int i = 0, n = tableTokens.size(); i < n; i++) {
                    backupTable(tableTokens.get(i), executionContext);
                }
                compiledQuery.ofBackupTable();
            } finally {
                tableTokens.clear();
            }
        }
    }

    private class DropStatementCompiler implements Closeable {
        private final CharSequenceObjHashMap<String> dropTablesFailedList = new CharSequenceObjHashMap<>();
        private final ObjHashSet<TableToken> dropTablesList = new ObjHashSet<>();

        @Override
        public void close() {
            dropTablesList.clear();
            dropTablesFailedList.clear();
        }

        private void dropAllTables(SqlExecutionContext executionContext) {
            // collect table names
            dropTablesFailedList.clear();
            dropTablesList.clear();
            engine.getTableTokens(dropTablesList, false);
            SecurityContext securityContext = executionContext.getSecurityContext();
            TableToken tableToken;
            for (int i = 0, n = dropTablesList.size(); i < n; i++) {
                tableToken = dropTablesList.get(i);
                if (!tableToken.isSystem()) {
                    securityContext.authorizeTableDrop(tableToken);
                    try {
                        engine.drop(path, tableToken);
                    } catch (CairoException report) {
                        // it will fail when there are readers/writers and lock cannot be acquired
                        dropTablesFailedList.put(tableToken.getTableName(), report.getMessage());
                    }
                }
            }
            if (dropTablesFailedList.size() > 0) {
                CairoException ex = CairoException.nonCritical().put("failed to drop tables [");
                CharSequence tableName;
                String reason;
                ObjList<CharSequence> keys = dropTablesFailedList.keys();
                for (int i = 0, n = keys.size(); i < n; i++) {
                    tableName = keys.get(i);
                    reason = dropTablesFailedList.get(tableName);
                    ex.put('\'').put(tableName).put("': ").put(reason);
                    if (i + 1 < n) {
                        ex.put(", ");
                    }
                }
                throw ex.put(']');
            }
            compiledQuery.ofDrop();
        }

        private void executorSelector(SqlExecutionContext executionContext) throws SqlException {
            // the selected method depends on the second token, we have already seen DROP
            CharSequence tok = SqlUtil.fetchNext(lexer);
            if (tok != null) {
                // DROP TABLE [ IF EXISTS ] name [;]
                if (SqlKeywords.isTableKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok == null) {
                        throw parseErrorExpected("IF EXISTS table-name");
                    }
                    boolean hasIfExists = false;
                    if (SqlKeywords.isIfKeyword(tok)) {
                        tok = SqlUtil.fetchNext(lexer);
                        if (tok == null || !SqlKeywords.isExistsKeyword(tok)) {
                            throw parseErrorExpected("EXISTS table-name");
                        }
                        hasIfExists = true;
                    } else {
                        lexer.unparseLast(); // tok has table name
                    }
                    tok = expectToken(lexer, "table-name");

                    SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());

                    final CharSequence tableName = GenericLexer.unquote(tok);
                    final int tableNamePosition = lexer.lastTokenPosition();
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok == null || Chars.equals(tok, ';')) {
                        dropTable(executionContext, tableName, tableNamePosition, hasIfExists);
                    } else {
                        unknownDropTableSuffix(executionContext, tok, tableName, tableNamePosition, hasIfExists);
                    }
                    return;
                } else if (SqlKeywords.isAllKeyword(tok)) {
                    // DROP ALL TABLES [;]
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok != null && SqlKeywords.isTablesKeyword(tok)) {
                        tok = SqlUtil.fetchNext(lexer);
                        if (tok == null || Chars.equals(tok, ';')) {
                            dropAllTables(executionContext);
                            return;
                        }
                        throw parseErrorExpected("[;]");
                    } else {
                        throw SqlException.position(lexer.lastTokenPosition()).put("'tables' expected");
                    }
                }
            }
            unknownDropStatement(executionContext, tok);
        }

        private SqlException parseErrorExpected(CharSequence expected) {
            return SqlException.$(lexer.lastTokenPosition(), "expected ").put(expected);
        }
    }

    static {
        castGroups.extendAndSet(ColumnType.BOOLEAN, 2);
        castGroups.extendAndSet(ColumnType.BYTE, 1);
        castGroups.extendAndSet(ColumnType.SHORT, 1);
        castGroups.extendAndSet(ColumnType.CHAR, 1);
        castGroups.extendAndSet(ColumnType.INT, 1);
        castGroups.extendAndSet(ColumnType.LONG, 1);
        castGroups.extendAndSet(ColumnType.FLOAT, 1);
        castGroups.extendAndSet(ColumnType.DOUBLE, 1);
        castGroups.extendAndSet(ColumnType.DATE, 1);
        castGroups.extendAndSet(ColumnType.TIMESTAMP, 1);
        castGroups.extendAndSet(ColumnType.STRING, 3);
        castGroups.extendAndSet(ColumnType.VARCHAR, 3);
        castGroups.extendAndSet(ColumnType.SYMBOL, 3);
        castGroups.extendAndSet(ColumnType.BINARY, 4);

        sqlControlSymbols.add("(");
        sqlControlSymbols.add(";");
        sqlControlSymbols.add(")");
        sqlControlSymbols.add(",");
        sqlControlSymbols.add("/*");
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
