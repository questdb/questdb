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

import io.questdb.MessageBus;
import io.questdb.PropServerConfiguration;
import io.questdb.cairo.*;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.text.*;
import io.questdb.griffin.engine.functions.catalogue.*;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.engine.ops.CopyFactory;
import io.questdb.griffin.engine.ops.InsertOperationImpl;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.engine.table.ShowColumnsRecordCursorFactory;
import io.questdb.griffin.engine.table.TableListRecordCursorFactory;
import io.questdb.griffin.model.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.ServiceLoader;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.griffin.SqlKeywords.*;

public class SqlCompiler implements Closeable {
    public static final ObjList<String> sqlControlSymbols = new ObjList<>(8);
    private final static Log LOG = LogFactory.getLog(SqlCompiler.class);
    private static final IntList castGroups = new IntList();
    //null object used to skip null checks in batch method
    private static final BatchCallback EMPTY_CALLBACK = new BatchCallback() {
        @Override
        public void postCompile(SqlCompiler compiler, CompiledQuery cq, CharSequence queryText) {
        }

        @Override
        public void preCompile(SqlCompiler compiler) {
        }
    };
    protected final CairoEngine engine;
    private final GenericLexer lexer;
    private final Path path = new Path();
    private final CharSequenceObjHashMap<KeywordBasedExecutor> keywordBasedExecutors = new CharSequenceObjHashMap<>();
    private final CompiledQueryImpl compiledQuery;
    private final AlterOperationBuilder alterOperationBuilder;
    private final SqlOptimiser optimiser;
    private final SqlParser parser;
    private final ObjectPool<ExpressionNode> sqlNodePool;
    private final CharacterStore characterStore;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final Path renamePath = new Path();
    private final DatabaseBackupAgent backupAgent;
    private final DatabaseSnapshotAgent snapshotAgent;
    private final MemoryMARW mem = Vm.getMARWInstance();
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final MessageBus messageBus;
    private final ListColumnFilter listColumnFilter = new ListColumnFilter();
    private final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
    private final IntIntHashMap typeCast = new IntIntHashMap();
    private final ObjList<TableWriter> tableWriters = new ObjList<>();
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final FunctionParser functionParser;
    private final ExecutableMethod insertAsSelectMethod = this::insertAsSelect;
    private final TextLoader textLoader;
    private final FilesFacade ff;
    private final TimestampValueRecord partitionFunctionRec = new TimestampValueRecord();
    private final IndexBuilder rebuildIndex = new IndexBuilder();
    private final VacuumColumnVersions vacuumColumnVersions;
    //determines how compiler parses query text
    //true - compiler treats whole input as single query and doesn't stop on ';'. Default mode.
    //false - compiler treats input as list of statements and stops processing statement on ';'. Used in batch processing.
    private boolean isSingleQueryMode = true;
    // Helper var used to pass back count in cases it can't be done via method result.
    private long insertCount;
    private final ExecutableMethod createTableMethod = this::createTable;

    // Exposed for embedded API users.
    public SqlCompiler(CairoEngine engine) {
        this(engine, null, null);
    }

    public SqlCompiler(CairoEngine engine, @Nullable FunctionFactoryCache functionFactoryCache, @Nullable DatabaseSnapshotAgent snapshotAgent) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
        this.messageBus = engine.getMessageBus();
        this.sqlNodePool = new ObjectPool<>(ExpressionNode.FACTORY, configuration.getSqlExpressionPoolCapacity());
        this.queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, configuration.getSqlColumnPoolCapacity());
        this.queryModelPool = new ObjectPool<>(QueryModel.FACTORY, configuration.getSqlModelPoolCapacity());
        this.compiledQuery = new CompiledQueryImpl(engine);
        this.characterStore = new CharacterStore(
                configuration.getSqlCharacterStoreCapacity(),
                configuration.getSqlCharacterStoreSequencePoolCapacity());
        this.lexer = new GenericLexer(configuration.getSqlLexerPoolCapacity());
        this.functionParser = new FunctionParser(
                configuration,
                functionFactoryCache != null
                        ? functionFactoryCache
                        : new FunctionFactoryCache(engine.getConfiguration(), ServiceLoader.load(
                        FunctionFactory.class, FunctionFactory.class.getClassLoader()))
        );
        this.codeGenerator = new SqlCodeGenerator(engine, configuration, functionParser, sqlNodePool);
        this.vacuumColumnVersions = new VacuumColumnVersions(engine);

        // we have cyclical dependency here
        functionParser.setSqlCodeGenerator(codeGenerator);

        this.backupAgent = new DatabaseBackupAgent();
        this.snapshotAgent = snapshotAgent;

        // For each 'this::method' reference java compiles a class
        // We need to minimize repetition of this syntax as each site generates garbage
        final KeywordBasedExecutor compileSet = this::compileSet;
        final KeywordBasedExecutor compileBegin = this::compileBegin;
        final KeywordBasedExecutor compileCommit = this::compileCommit;
        final KeywordBasedExecutor compileRollback = this::compileRollback;
        final KeywordBasedExecutor truncateTables = this::truncateTables;
        final KeywordBasedExecutor alterTable = this::alterTable;
        final KeywordBasedExecutor repairTables = this::repairTables;
        final KeywordBasedExecutor reindexTable = this::reindexTable;
        final KeywordBasedExecutor dropTable = this::dropTable;
        final KeywordBasedExecutor sqlBackup = backupAgent::sqlBackup;
        final KeywordBasedExecutor sqlShow = this::sqlShow;
        final KeywordBasedExecutor vacuumTable = this::vacuum;
        final KeywordBasedExecutor snapshotDatabase = this::snapshotDatabase;
        final KeywordBasedExecutor compileDeallocate = this::compileDeallocate;

        keywordBasedExecutors.put("truncate", truncateTables);
        keywordBasedExecutors.put("TRUNCATE", truncateTables);
        keywordBasedExecutors.put("alter", alterTable);
        keywordBasedExecutors.put("ALTER", alterTable);
        keywordBasedExecutors.put("repair", repairTables);
        keywordBasedExecutors.put("REPAIR", repairTables);
        keywordBasedExecutors.put("reindex", reindexTable);
        keywordBasedExecutors.put("REINDEX", reindexTable);
        keywordBasedExecutors.put("set", compileSet);
        keywordBasedExecutors.put("SET", compileSet);
        keywordBasedExecutors.put("begin", compileBegin);
        keywordBasedExecutors.put("BEGIN", compileBegin);
        keywordBasedExecutors.put("commit", compileCommit);
        keywordBasedExecutors.put("COMMIT", compileCommit);
        keywordBasedExecutors.put("rollback", compileRollback);
        keywordBasedExecutors.put("ROLLBACK", compileRollback);
        keywordBasedExecutors.put("discard", compileSet);
        keywordBasedExecutors.put("DISCARD", compileSet);
        keywordBasedExecutors.put("close", compileSet); //no-op
        keywordBasedExecutors.put("CLOSE", compileSet);  //no-op
        keywordBasedExecutors.put("unlisten", compileSet);  //no-op
        keywordBasedExecutors.put("UNLISTEN", compileSet);  //no-op
        keywordBasedExecutors.put("reset", compileSet);  //no-op
        keywordBasedExecutors.put("RESET", compileSet);  //no-op
        keywordBasedExecutors.put("drop", dropTable);
        keywordBasedExecutors.put("DROP", dropTable);
        keywordBasedExecutors.put("backup", sqlBackup);
        keywordBasedExecutors.put("BACKUP", sqlBackup);
        keywordBasedExecutors.put("show", sqlShow);
        keywordBasedExecutors.put("SHOW", sqlShow);
        keywordBasedExecutors.put("vacuum", vacuumTable);
        keywordBasedExecutors.put("VACUUM", vacuumTable);
        keywordBasedExecutors.put("snapshot", snapshotDatabase);
        keywordBasedExecutors.put("SNAPSHOT", snapshotDatabase);
        keywordBasedExecutors.put("deallocate", compileDeallocate);
        keywordBasedExecutors.put("DEALLOCATE", compileDeallocate);

        configureLexer(lexer);

        final PostOrderTreeTraversalAlgo postOrderTreeTraversalAlgo = new PostOrderTreeTraversalAlgo();
        optimiser = new SqlOptimiser(
                configuration,
                engine,
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
        this.textLoader = new TextLoader(engine);
        alterOperationBuilder = new AlterOperationBuilder();
    }

    public static void configureLexer(GenericLexer lexer) {
        for (int i = 0, k = sqlControlSymbols.size(); i < k; i++) {
            lexer.defineSymbol(sqlControlSymbols.getQuick(i));
        }
        for (int i = 0, k = OperatorExpression.operators.size(); i < k; i++) {
            OperatorExpression op = OperatorExpression.operators.getQuick(i);
            if (op.symbol) {
                lexer.defineSymbol(op.token);
            }
        }
    }

    @Override
    public void close() {
        backupAgent.close();
        vacuumColumnVersions.close();
        Misc.free(path);
        Misc.free(renamePath);
        Misc.free(textLoader);
        Misc.free(rebuildIndex);
        Misc.free(codeGenerator);
        Misc.free(mem);
        Misc.freeObjList(tableWriters);
    }

    @NotNull
    public CompiledQuery compile(@NotNull CharSequence query, @NotNull SqlExecutionContext executionContext) throws SqlException {
        clear();
        // these are quick executions that do not require building of a model
        lexer.of(query);
        isSingleQueryMode = true;

        compileInner(executionContext);
        compiledQuery.withContext(executionContext);
        return compiledQuery;
    }

    /*
     * Allows processing of batches of sql statements (sql scripts) separated by ';' .
     * Each query is processed in sequence and processing stops on first error and whole batch gets discarded .
     * Noteworthy difference between this and 'normal' query is that all empty queries get ignored, e.g.
     * <br>
     * select 1;<br>
     * ; ;/* comment \*\/;--comment\n; - these get ignored <br>
     * update a set b=c  ; <br>
     * <p>
     * Useful PG doc link :
     *
     * @param query            - block of queries to process
     * @param batchCallback    - callback to perform actions prior to or after batch part compilation, e.g. clear caches or execute command
     * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4">PostgreSQL documentation</a>
     */
    public void compileBatch(
            @NotNull CharSequence query,
            @NotNull SqlExecutionContext executionContext,
            BatchCallback batchCallback
    ) throws SqlException, PeerIsSlowToReadException, PeerDisconnectedException {

        LOG.info().$("batch [text=").$(query).I$();

        clear();
        lexer.of(query);
        isSingleQueryMode = false;

        if (batchCallback == null) {
            batchCallback = EMPTY_CALLBACK;
        }

        int position;

        while (lexer.hasNext()) {
            //skip over empty statements that'd cause error in parser
            position = getNextValidTokenPosition();
            if (position == -1) {
                return;
            }

            boolean recompileStale = true;
            for (int retries = 0; recompileStale; retries++) {
                try {
                    batchCallback.preCompile(this);
                    clear();//we don't use normal compile here because we can't reset existing lexer
                    CompiledQuery current = compileInner(executionContext);
                    //We've to move lexer because some query handlers don't consume all tokens (e.g. SET )
                    //some code in postCompile might need full text of current query
                    CharSequence currentQuery = query.subSequence(position, goToQueryEnd());
                    batchCallback.postCompile(this, current, currentQuery);
                    recompileStale = false;
                } catch (ReaderOutOfDateException e) {
                    if (retries == ReaderOutOfDateException.MAX_RETRY_ATTEMPS) {
                        throw e;
                    }
                    LOG.info().$(e.getFlyweightMessage()).$();
                    // will recompile
                    lexer.restart();
                }
            }
        }
    }

    public void filterPartitions(
            Function function,
            TableReader reader,
            AlterOperationBuilder changePartitionStatement
    ) {
        // Iterate partitions in descending order so if folders are missing on disk
        // removePartition does not fail to determine next minTimestamp
        final int partitionCount = reader.getPartitionCount();
        if (partitionCount > 0) { // table may be empty
            for (int i = partitionCount - 2; i > -1; i--) {
                long partitionTimestamp = reader.getPartitionTimestampByIndex(i);
                partitionFunctionRec.setTimestamp(partitionTimestamp);
                if (function.getBool(partitionFunctionRec)) {
                    changePartitionStatement.ofPartition(partitionTimestamp);
                }
            }

            // remove last partition
            long partitionTimestamp = reader.getPartitionTimestampByIndex(partitionCount - 1);
            partitionFunctionRec.setTimestamp(partitionTimestamp);
            if (function.getBool(partitionFunctionRec)) {
                changePartitionStatement.ofPartition(partitionTimestamp);
            }
        }
    }

    public CairoEngine getEngine() {
        return engine;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return functionParser.getFunctionFactoryCache();
    }

    private static boolean isCompatibleCase(int from, int to) {
        return castGroups.getQuick(ColumnType.tagOf(from)) == castGroups.getQuick(ColumnType.tagOf(to));
    }

    private static void expectKeyword(GenericLexer lexer, CharSequence keyword) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);

        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put('\'').put(keyword).put("' expected");
        }

        if (!Chars.equalsLowerCaseAscii(tok, keyword)) {
            throw SqlException.position(lexer.lastTokenPosition()).put('\'').put(keyword).put("' expected");
        }
    }

    private static CharSequence expectToken(GenericLexer lexer, CharSequence expected) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);

        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put(expected).put(" expected");
        }

        return tok;
    }

    private static CharSequence maybeExpectToken(GenericLexer lexer, CharSequence expected, boolean expect) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);

        if (expect && tok == null) {
            throw SqlException.position(lexer.getPosition()).put(expected).put(" expected");
        }

        return tok;
    }

    private CompiledQuery alterSystemLockWriter(SqlExecutionContext executionContext) throws SqlException {
        final int tableNamePosition = lexer.getPosition();
        CharSequence tok = GenericLexer.unquote(expectToken(lexer, "table name"));
        tableExistsOrFail(tableNamePosition, tok, executionContext);
        try {
            CharSequence lockedReason = engine.lockWriter(executionContext.getCairoSecurityContext(), tok, "alterSystem");
            if (lockedReason != WriterPool.OWNERSHIP_REASON_NONE) {
                throw SqlException.$(tableNamePosition, "could not lock, busy [table=`").put(tok).put(", lockedReason=").put(lockedReason).put("`]");
            }
            return compiledQuery.ofLock();
        } catch (CairoException e) {
            throw SqlException.position(tableNamePosition)
                    .put(e.getFlyweightMessage())
                    .put("[errno=").put(e.getErrno()).put(']');
        }
    }

    private CompiledQuery alterSystemUnlockWriter(SqlExecutionContext executionContext) throws SqlException {
        final int tableNamePosition = lexer.getPosition();
        CharSequence tok = GenericLexer.unquote(expectToken(lexer, "table name"));
        tableExistsOrFail(tableNamePosition, tok, executionContext);
        try {
            engine.unlockWriter(executionContext.getCairoSecurityContext(), tok);
            return compiledQuery.ofUnlock();
        } catch (CairoException e) {
            throw SqlException.position(tableNamePosition)
                    .put(e.getFlyweightMessage())
                    .put("[errno=").put(e.getErrno()).put(']');
        }
    }

    private CompiledQuery alterTable(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok;
        tok = expectToken(lexer, "'table' or 'system'");

        if (SqlKeywords.isTableKeyword(tok)) {
            final int tableNamePosition = lexer.getPosition();
            tok = GenericLexer.unquote(expectToken(lexer, "table name"));
            tableExistsOrFail(tableNamePosition, tok, executionContext);

            CharSequence name = GenericLexer.immutableOf(tok);
            try (TableReader reader = engine.getReaderForStatement(executionContext, name, "alter table")) {
                String tableName = reader.getTableName();
                TableReaderMetadata tableMetadata = reader.getMetadata();
                tok = expectToken(lexer, "'add', 'alter' or 'drop'");

                if (SqlKeywords.isAddKeyword(tok)) {
                    return alterTableAddColumn(tableNamePosition, tableName, tableMetadata);
                } else if (SqlKeywords.isDropKeyword(tok)) {
                    tok = expectToken(lexer, "'column' or 'partition'");
                    if (SqlKeywords.isColumnKeyword(tok)) {
                        return alterTableDropColumn(tableNamePosition, tableName, tableMetadata);
                    } else if (SqlKeywords.isPartitionKeyword(tok)) {
                        return alterTableDropDetachOrAttachPartition(reader, PartitionAction.DROP, executionContext);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'column' or 'partition' expected");
                    }
                } else if (SqlKeywords.isAttachKeyword(tok)) {
                    tok = expectToken(lexer, "'partition'");
                    if (SqlKeywords.isPartitionKeyword(tok)) {
                        return alterTableDropDetachOrAttachPartition(reader, PartitionAction.ATTACH, executionContext);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                    }
                } else if (SqlKeywords.isDetachKeyword(tok)) {
                    tok = expectToken(lexer, "'partition'");
                    if (SqlKeywords.isPartitionKeyword(tok)) {
                        return alterTableDropDetachOrAttachPartition(reader, PartitionAction.DETACH, executionContext);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'partition' expected");
                    }
                } else if (SqlKeywords.isRenameKeyword(tok)) {
                    tok = expectToken(lexer, "'column'");
                    if (SqlKeywords.isColumnKeyword(tok)) {
                        return alterTableRenameColumn(tableNamePosition, tableName, tableMetadata);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'column' expected");
                    }
                } else if (SqlKeywords.isAlterKeyword(tok)) {
                    tok = expectToken(lexer, "'column'");
                    if (SqlKeywords.isColumnKeyword(tok)) {
                        final int columnNameNamePosition = lexer.getPosition();
                        tok = expectToken(lexer, "column name");
                        final CharSequence columnName = GenericLexer.immutableOf(tok);
                        tok = expectToken(lexer, "'add index' or 'drop index' or 'cache' or 'nocache'");
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

                            return alterTableColumnAddIndex(tableNamePosition, tableName, columnNameNamePosition, columnName, tableMetadata, indexValueCapacity);
                        } else if (SqlKeywords.isDropKeyword(tok)) {
                            // alter table <table name> alter column drop index
                            expectKeyword(lexer, "index");
                            tok = SqlUtil.fetchNext(lexer);
                            if (tok != null && !isSemicolon(tok)) {
                                throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [").put(tok).put("] while trying to drop index");
                            }
                            return alterTableColumnDropIndex(tableNamePosition, tableName, columnNameNamePosition, columnName, tableMetadata);
                        } else if (SqlKeywords.isCacheKeyword(tok)) {
                            return alterTableColumnCacheFlag(tableNamePosition, tableName, columnName, reader, true);
                        } else if (SqlKeywords.isNoCacheKeyword(tok)) {
                            return alterTableColumnCacheFlag(tableNamePosition, tableName, columnName, reader, false);
                        } else {
                            throw SqlException.$(lexer.lastTokenPosition(), "'add', 'drop', 'cache' or 'nocache' expected").put(" found '").put(tok).put('\'');
                        }
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'column' or 'partition' expected");
                    }
                } else if (SqlKeywords.isSetKeyword(tok)) {
                    tok = expectToken(lexer, "'param'");
                    if (SqlKeywords.isParamKeyword(tok)) {
                        final int paramNameNamePosition = lexer.getPosition();
                        tok = expectToken(lexer, "param name");
                        final CharSequence paramName = GenericLexer.immutableOf(tok);
                        tok = expectToken(lexer, "'='");
                        if (tok.length() == 1 && tok.charAt(0) == '=') {
                            CharSequence value = GenericLexer.immutableOf(SqlUtil.fetchNext(lexer));
                            return alterTableSetParam(paramName, value, paramNameNamePosition, tableName, tableMetadata.getId());
                        } else {
                            throw SqlException.$(lexer.lastTokenPosition(), "'=' expected");
                        }
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'param' expected");
                    }
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'add', 'drop', 'attach', 'detach', 'set' or 'rename' expected");
                }
            } catch (CairoException e) {
                LOG.info().$("could not alter table [table=").$(name).$(", ex=").$((Throwable) e).$();
                throw SqlException.$(lexer.lastTokenPosition(), "table '").put(name).put("' could not be altered: ").put(e);
            }
        } else if (SqlKeywords.isSystemKeyword(tok)) {
            tok = expectToken(lexer, "'lock' or 'unlock'");

            if (SqlKeywords.isLockKeyword(tok)) {
                tok = expectToken(lexer, "'writer'");

                if (SqlKeywords.isWriterKeyword(tok)) {
                    return alterSystemLockWriter(executionContext);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'writer' expected");
                }
            } else if (SqlKeywords.isUnlockKeyword(tok)) {
                tok = expectToken(lexer, "'writer'");

                if (SqlKeywords.isWriterKeyword(tok)) {
                    return alterSystemUnlockWriter(executionContext);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'writer' expected");
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'lock' or 'unlock' expected");
            }
        } else {
            throw SqlException.$(lexer.lastTokenPosition(), "'table' or 'system' expected");
        }
    }

    private CompiledQuery alterTableAddColumn(int tableNamePosition, String tableName, TableReaderMetadata tableMetadata) throws SqlException {
        // add columns to table
        CharSequence tok = SqlUtil.fetchNext(lexer);
        //ignoring `column`
        if (tok != null && !SqlKeywords.isColumnKeyword(tok)) {
            lexer.unparseLast();
        }

        AlterOperationBuilder addColumn = alterOperationBuilder.ofAddColumn(
                tableNamePosition,
                tableName,
                tableMetadata.getId());

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

            if (!TableUtils.isValidColumnName(columnName, configuration.getMaxFileNameLength())) {
                throw SqlException.$(lexer.lastTokenPosition(), " new column name contains invalid characters");
            }

            tok = expectToken(lexer, "column type");

            int type = ColumnType.tagOf(tok);
            if (type == -1) {
                throw SqlException.$(lexer.lastTokenPosition(), "invalid type");
            }

            if (type == ColumnType.GEOHASH) {
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || tok.charAt(0) != '(') {
                    throw SqlException.position(lexer.getPosition()).put("missing GEOHASH precision");
                }

                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && tok.charAt(0) != ')') {
                    int geosizeBits = GeoHashUtil.parseGeoHashBits(lexer.lastTokenPosition(), 0, tok);
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
                    type = ColumnType.getGeoHashTypeWithBits(geosizeBits);
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

            if (ColumnType.isSymbol(type) && tok != null &&
                    !Chars.equals(tok, ',') && !Chars.equals(tok, ';')) {

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


                if (Chars.equalsLowerCaseAsciiNc(tok, "cache")) {
                    cache = true;
                    tok = SqlUtil.fetchNext(lexer);
                } else if (Chars.equalsLowerCaseAsciiNc(tok, "nocache")) {
                    cache = false;
                    tok = SqlUtil.fetchNext(lexer);
                } else {
                    cache = configuration.getDefaultSymbolCacheFlag();
                }

                TableUtils.validateSymbolCapacityCached(cache, symbolCapacity, lexer.lastTokenPosition());

                indexed = Chars.equalsLowerCaseAsciiNc(tok, "index");
                if (indexed) {
                    tok = SqlUtil.fetchNext(lexer);
                }

                if (Chars.equalsLowerCaseAsciiNc(tok, "capacity")) {
                    tok = expectToken(lexer, "symbol index capacity");

                    try {
                        indexValueBlockCapacity = Numbers.parseInt(tok);
                    } catch (NumericException e) {
                        throw SqlException.$(lexer.lastTokenPosition(), "numeric capacity expected");
                    }
                    tok = SqlUtil.fetchNext(lexer);
                } else {
                    indexValueBlockCapacity = configuration.getIndexValueBlockSize();
                }
            } else { //set defaults

                //ignoring `NULL` and `NOT NULL`
                if (tok != null && SqlKeywords.isNotKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                }

                if (tok != null && SqlKeywords.isNullKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                }

                cache = configuration.getDefaultSymbolCacheFlag();
                indexValueBlockCapacity = configuration.getIndexValueBlockSize();
                symbolCapacity = configuration.getDefaultSymbolCapacity();
                indexed = false;
            }

            addColumn.ofAddColumn(
                    columnName,
                    type,
                    Numbers.ceilPow2(symbolCapacity),
                    cache,
                    indexed,
                    Numbers.ceilPow2(indexValueBlockCapacity)
            );

            if (tok == null || (!isSingleQueryMode && isSemicolon(tok))) {
                break;
            }

            semicolonPos = Chars.equals(tok, ';') ? lexer.lastTokenPosition() : -1;
            if (semicolonPos < 0 && !Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
            }

        } while (true);
        return compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private CompiledQuery alterTableColumnAddIndex(
            int tableNamePosition,
            String tableName,
            int columnNamePosition,
            CharSequence columnName,
            TableReaderMetadata metadata,
            int indexValueBlockSize
    ) throws SqlException {

        if (metadata.getColumnIndexQuiet(columnName) == -1) {
            throw SqlException.invalidColumn(columnNamePosition, columnName);
        }
        if (indexValueBlockSize == -1) {
            indexValueBlockSize = configuration.getIndexValueBlockSize();
        }
        return compiledQuery.ofAlter(
                alterOperationBuilder
                        .ofAddIndex(tableNamePosition, tableName, metadata.getId(), columnName, Numbers.ceilPow2(indexValueBlockSize))
                        .build()
        );
    }

    private CompiledQuery alterTableColumnCacheFlag(
            int tableNamePosition,
            String tableName,
            CharSequence columnName,
            TableReader reader,
            boolean cache
    ) throws SqlException {
        TableReaderMetadata metadata = reader.getMetadata();
        int columnIndex = metadata.getColumnIndexQuiet(columnName);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(lexer.lastTokenPosition(), columnName);
        }

        if (!ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
            throw SqlException.$(lexer.lastTokenPosition(), "Invalid column type - Column should be of type symbol");
        }

        return cache ? compiledQuery.ofAlter(
                alterOperationBuilder.ofCacheSymbol(tableNamePosition, tableName, metadata.getId(), columnName).build()
        )
                : compiledQuery.ofAlter(
                alterOperationBuilder.ofRemoveCacheSymbol(tableNamePosition, tableName, metadata.getId(), columnName).build()
        );
    }

    private CompiledQuery alterTableColumnDropIndex(
            int tableNamePosition,
            String tableName,
            int columnNamePosition,
            CharSequence columnName,
            TableReaderMetadata metadata
    ) throws SqlException {
        if (metadata.getColumnIndexQuiet(columnName) == -1) {
            throw SqlException.invalidColumn(columnNamePosition, columnName);
        }
        return compiledQuery.ofAlter(
                alterOperationBuilder
                        .ofDropIndex(tableNamePosition, tableName, metadata.getId(), columnName)
                        .build()
        );
    }

    private CompiledQuery alterTableDropColumn(int tableNamePosition, String tableName, TableReaderMetadata metadata) throws SqlException {
        AlterOperationBuilder dropColumnStatement = alterOperationBuilder.ofDropColumn(tableNamePosition, tableName, metadata.getId());
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
                throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
            }
        } while (true);

        return compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private CompiledQuery alterTableDropDetachOrAttachPartition(
            TableReader reader,
            int action,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final int pos = lexer.lastTokenPosition();
        TableReaderMetadata readerMetadata = reader.getMetadata();
        if (readerMetadata.getPartitionBy() == PartitionBy.NONE) {
            throw SqlException.$(pos, "table is not partitioned");
        }

        String tableName = reader.getTableName();
        final CharSequence tok = expectToken(lexer, "'list' or 'where'");
        if (SqlKeywords.isListKeyword(tok)) {
            return alterTableDropDetachOrAttachPartitionByList(reader, pos, action);
        } else if (SqlKeywords.isWhereKeyword(tok)) {
            AlterOperationBuilder alterPartitionStatement;
            switch (action) {
                case PartitionAction.DROP:
                    alterPartitionStatement = alterOperationBuilder.ofDropPartition(pos, tableName, reader.getMetadata().getId());
                    break;
                case PartitionAction.DETACH:
                    alterPartitionStatement = alterOperationBuilder.ofDetachPartition(pos, tableName, reader.getMetadata().getId());
                    break;
                default:
                    throw SqlException.$(pos, "WHERE clause can only be used with command DROP PARTITION, or DETACH PARTITION");
            }
            ExpressionNode expr = parser.expr(lexer, (QueryModel) null);
            String designatedTimestampColumnName = null;
            int tsIndex = readerMetadata.getTimestampIndex();
            if (tsIndex >= 0) {
                designatedTimestampColumnName = readerMetadata.getColumnName(tsIndex);
            }
            if (designatedTimestampColumnName != null) {
                GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata(designatedTimestampColumnName, 0, ColumnType.TIMESTAMP, null));
                Function function = functionParser.parseFunction(expr, metadata, executionContext);
                if (function != null && ColumnType.isBoolean(function.getType())) {
                    function.init(null, executionContext);
                    filterPartitions(function, reader, alterPartitionStatement);
                    return compiledQuery.ofAlter(alterOperationBuilder.build());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "boolean expression expected");
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "this table does not have a designated timestamp column");
            }
        } else {
            throw SqlException.$(lexer.lastTokenPosition(), "'list' or 'where' expected");
        }
    }

    private CompiledQuery alterTableDropDetachOrAttachPartitionByList(TableReader reader, int pos, int action) throws SqlException {
        String tableName = reader.getTableName();
        AlterOperationBuilder partitions;
        switch (action) {
            case PartitionAction.DROP:
                partitions = alterOperationBuilder.ofDropPartition(pos, tableName, reader.getMetadata().getId());
                break;
            case PartitionAction.DETACH:
                partitions = alterOperationBuilder.ofDetachPartition(pos, tableName, reader.getMetadata().getId());
                break;
            default:
                // attach
                partitions = alterOperationBuilder.ofAttachPartition(pos, tableName, reader.getMetadata().getId());
        }
        assert action == PartitionAction.DROP || action == PartitionAction.ATTACH || action == PartitionAction.DETACH;
        int semicolonPos = -1;
        do {
            CharSequence tok = maybeExpectToken(lexer, "partition name", semicolonPos < 0);
            if (semicolonPos >= 0) {
                if (tok != null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
                }
                break;
            }
            if (Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "partition name missing");
            }
            final CharSequence unquoted = GenericLexer.unquote(tok);

            final long timestamp;
            try {
                timestamp = PartitionBy.parsePartitionDirName(unquoted, reader.getPartitionedBy());
            } catch (CairoException e) {
                throw SqlException.$(lexer.lastTokenPosition(), e.getFlyweightMessage())
                        .put("[errno=").put(e.getErrno()).put(']');
            }

            partitions.ofPartition(timestamp);
            tok = SqlUtil.fetchNext(lexer);

            if (tok == null || (!isSingleQueryMode && isSemicolon(tok))) {
                break;
            }

            semicolonPos = Chars.equals(tok, ';') ? lexer.lastTokenPosition() : -1;
            if (semicolonPos < 0 && !Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
            }
        } while (true);

        return compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private CompiledQuery alterTableRenameColumn(int tableNamePosition, String tableName, TableReaderMetadata metadata) throws SqlException {
        AlterOperationBuilder renameColumnStatement = alterOperationBuilder.ofRenameColumn(tableNamePosition, tableName, metadata.getId());
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
        return compiledQuery.ofAlter(alterOperationBuilder.build());
    }

    private CompiledQuery alterTableSetParam(CharSequence paramName, CharSequence value, int paramNameNamePosition, String tableName, int tableId) throws SqlException {
        if (isMaxUncommittedRowsKeyword(paramName)) {
            int maxUncommittedRows;
            try {
                maxUncommittedRows = Numbers.parseInt(value);
            } catch (NumericException e) {
                throw SqlException.$(paramNameNamePosition, "invalid value [value=").put(value).put(",parameter=").put(paramName).put(']');
            }
            if (maxUncommittedRows < 0) {
                throw SqlException.$(paramNameNamePosition, "maxUncommittedRows must be non negative");
            }
            return compiledQuery.ofAlter(alterOperationBuilder.ofSetParamUncommittedRows(tableName, tableId, maxUncommittedRows).build());
        } else if (isCommitLagKeyword(paramName)) {
            long commitLag = SqlUtil.expectMicros(value, paramNameNamePosition);
            if (commitLag < 0) {
                throw SqlException.$(paramNameNamePosition, "commitLag must be non negative");
            }
            return compiledQuery.ofAlter(alterOperationBuilder.ofSetParamCommitLag(tableName, tableId, commitLag).build());
        } else {
            throw SqlException.$(paramNameNamePosition, "unknown parameter '").put(paramName).put('\'');
        }
    }

    private void cancelTextImport(CopyModel model) throws SqlException {
        assert model.isCancel();

        final TextImportExecutionContext textImportExecutionContext = engine.getTextImportExecutionContext();
        final AtomicBooleanCircuitBreaker circuitBreaker = textImportExecutionContext.getCircuitBreaker();

        long inProgressImportId = textImportExecutionContext.getActiveImportId();
        // The cancellation is based on the best effort, so we don't worry about potential races with imports.
        if (inProgressImportId == TextImportExecutionContext.INACTIVE) {
            throw SqlException.$(0, "No active import to cancel.");
        }
        long importId;
        try {
            CharSequence idString = model.getTarget().token;
            int start = 0;
            int end = idString.length();
            if (Chars.isQuoted(idString)) {
                start = 1;
                end--;
            }
            importId = Numbers.parseHexLong(idString, start, end);
        } catch (NumericException e) {
            throw SqlException.$(0, "Provided id has invalid format.");
        }
        if (inProgressImportId == importId) {
            circuitBreaker.cancel();
        } else {
            throw SqlException.$(0, "Active import has different id.");
        }
    }

    private void clear() {
        sqlNodePool.clear();
        characterStore.clear();
        queryColumnPool.clear();
        queryModelPool.clear();
        optimiser.clear();
        parser.clear();
        backupAgent.clear();
        alterOperationBuilder.clear();
        backupAgent.clear();
        functionParser.clear();
    }

    private CompiledQuery compileBegin(SqlExecutionContext executionContext) {
        return compiledQuery.ofBegin();
    }

    private CompiledQuery compileCommit(SqlExecutionContext executionContext) {
        return compiledQuery.ofCommit();
    }

    private ExecutionModel compileExecutionModel(SqlExecutionContext executionContext) throws SqlException {
        ExecutionModel model = parser.parse(lexer, executionContext);
        switch (model.getModelType()) {
            case ExecutionModel.QUERY:
                return optimiser.optimise((QueryModel) model, executionContext);
            case ExecutionModel.INSERT:
                InsertModel insertModel = (InsertModel) model;
                if (insertModel.getQueryModel() != null) {
                    return validateAndOptimiseInsertAsSelect(insertModel, executionContext);
                } else {
                    return lightlyValidateInsertModel(insertModel);
                }
            case ExecutionModel.UPDATE:
                optimiser.optimiseUpdate((QueryModel) model, executionContext);
                return model;
            default:
                return model;
        }
    }

    private CompiledQuery compileInner(@NotNull SqlExecutionContext executionContext) throws SqlException {
        SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        if (!circuitBreaker.isTimerSet()) {
            circuitBreaker.resetTimer();
        }

        final CharSequence tok = SqlUtil.fetchNext(lexer);

        if (tok == null) {
            throw SqlException.$(0, "empty query");
        }

        // Save execution context in resulting Compiled Query
        // it may be used for Alter Table statement execution
        compiledQuery.withContext(executionContext);
        final KeywordBasedExecutor executor = keywordBasedExecutors.get(tok);
        if (executor == null) {
            return compileUsingModel(executionContext);
        }
        return executor.execute(executionContext);
    }

    private CompiledQuery compileRollback(SqlExecutionContext executionContext) {
        return compiledQuery.ofRollback();
    }

    private CompiledQuery compileSet(SqlExecutionContext executionContext) {
        return compiledQuery.ofSet();
    }

    private CopyFactory compileTextImport(CopyModel model) throws SqlException {
        assert !model.isCancel();

        final CharSequence tableName = GenericLexer.unquote(model.getTarget().token);
        final ExpressionNode fileNameNode = model.getFileName();
        final CharSequence fileName = fileNameNode != null ? GenericLexer.assertNoDots(GenericLexer.unquote(fileNameNode.token), fileNameNode.position) : null;
        assert fileName != null;

        return new CopyFactory(
                messageBus,
                engine.getTextImportExecutionContext(),
                Chars.toString(tableName),
                Chars.toString(fileName),
                model
        );
    }

    private CompiledQuery compileDeallocate(SqlExecutionContext executionContext) throws SqlException {
        CharSequence statementName = GenericLexer.unquote(expectToken(lexer, "statement name"));
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [").put(tok).put("]");
        }
        return compiledQuery.ofDeallocate(statementName);
    }

    @NotNull
    private CompiledQuery compileUsingModel(SqlExecutionContext executionContext) throws SqlException {
        // This method will not populate sql cache directly;
        // factories are assumed to be non-reentrant and once
        // factory is out of this method the caller assumes
        // full ownership over it. In that however caller may
        // choose to return factory back to this or any other
        // instance of compiler for safekeeping

        // lexer would have parsed first token to determine direction of execution flow
        lexer.unparseLast();
        codeGenerator.clear();

        ExecutionModel executionModel = compileExecutionModel(executionContext);
        switch (executionModel.getModelType()) {
            case ExecutionModel.QUERY:
                LOG.info().$("plan [q=`").$((QueryModel) executionModel).$("`, fd=").$(executionContext.getRequestFd()).$(']').$();
                return compiledQuery.of(generate((QueryModel) executionModel, executionContext));
            case ExecutionModel.CREATE_TABLE:
                return createTableWithRetries(executionModel, executionContext);
            case ExecutionModel.COPY:
                return executeCopy(executionContext, (CopyModel) executionModel);
            case ExecutionModel.RENAME_TABLE:
                final RenameTableModel rtm = (RenameTableModel) executionModel;
                engine.rename(executionContext.getCairoSecurityContext(), path, GenericLexer.unquote(rtm.getFrom().token), renamePath, GenericLexer.unquote(rtm.getTo().token));
                return compiledQuery.ofRenameTable();
            case ExecutionModel.UPDATE:
                final QueryModel updateQueryModel = (QueryModel) executionModel;
                UpdateOperation updateStatement = generateUpdate(updateQueryModel, executionContext);
                return compiledQuery.ofUpdate(updateStatement);
            default:
                InsertModel insertModel = (InsertModel) executionModel;
                if (insertModel.getQueryModel() != null) {
                    return executeWithRetries(
                            insertAsSelectMethod,
                            executionModel,
                            configuration.getCreateAsSelectRetryCount(),
                            executionContext
                    );
                } else {
                    return insert(executionModel, executionContext);
                }
        }
    }

    private long copyOrdered(
            TableWriter writer,
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

    private long copyOrdered0(TableWriter writer,
                              RecordCursor cursor,
                              RecordToRowCopier copier,
                              int cursorTimestampIndex,
                              SqlExecutionCircuitBreaker circuitBreaker) {
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
            TableWriter writer,
            RecordMetadata metadata,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long commitLag,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        long rowCount;
        if (ColumnType.isSymbolOrString(metadata.getColumnType(cursorTimestampIndex))) {
            rowCount = copyOrderedBatchedStrTimestamp(writer, cursor, copier, cursorTimestampIndex, batchSize, commitLag, circuitBreaker);
        } else {
            rowCount = copyOrderedBatched0(writer, cursor, copier, cursorTimestampIndex, batchSize, commitLag, circuitBreaker);
        }
        writer.commit();

        return rowCount;
    }

    //returns number of copied rows
    private long copyOrderedBatched0(
            TableWriter writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long commitLag,
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
            if (++rowCount > deadline) {
                writer.commitWithLag(commitLag);
                deadline = rowCount + batchSize;
            }
        }

        return rowCount;
    }

    //returns number of copied rows
    private long copyOrderedBatchedStrTimestamp(
            TableWriter writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long commitLag,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        long deadline = batchSize;
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            CharSequence str = record.getStr(cursorTimestampIndex);
            // It's allowed to insert ISO formatted string to timestamp column
            TableWriter.Row row = writer.newRow(SqlUtil.parseFloorPartialTimestamp(str, -1, ColumnType.TIMESTAMP));
            copier.copy(record, row);
            row.append();
            if (++rowCount > deadline) {
                writer.commitWithLag(commitLag);
                deadline = rowCount + batchSize;
            }
        }

        return rowCount;
    }

    //returns number of copied rows
    private long copyOrderedStrTimestamp(
            TableWriter writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final CharSequence str = record.getStr(cursorTimestampIndex);
            // It's allowed to insert ISO formatted string to timestamp column
            TableWriter.Row row = writer.newRow(SqlUtil.implicitCastStrAsTimestamp(str));
            copier.copy(record, row);
            row.append();
            rowCount++;
        }

        return rowCount;
    }

    /**
     * Sets insertCount to number of copied rows.
     */
    private TableWriter copyTableData(
            CharSequence tableName,
            RecordCursor cursor,
            RecordMetadata cursorMetadata,
            SqlExecutionCircuitBreaker circuitBreaker
    ) throws SqlException {
        TableWriter writer = new TableWriter(
                configuration,
                tableName,
                messageBus,
                null,
                false,
                DefaultLifecycleManager.INSTANCE,
                configuration.getRoot(),
                engine.getMetrics());
        try {
            RecordMetadata writerMetadata = writer.getMetadata();
            entityColumnFilter.of(writerMetadata.getColumnCount());
            this.insertCount = copyTableData(
                    cursor,
                    cursorMetadata,
                    writer,
                    writerMetadata,
                    RecordToRowCopierUtils.generateCopier(
                            asm,
                            cursorMetadata,
                            writerMetadata,
                            entityColumnFilter
                    ),
                    circuitBreaker
            );
            return writer;
        } catch (Throwable e) {
            writer.close();
            throw e;
        }
    }

    /*
     * Returns number of copied rows.
     */
    private long copyTableData(
            RecordCursor cursor,
            RecordMetadata metadata,
            TableWriter writer,
            RecordMetadata
                    writerMetadata,
            RecordToRowCopier recordToRowCopier,
            SqlExecutionCircuitBreaker circuitBreaker
    ) throws SqlException {
        int timestampIndex = writerMetadata.getTimestampIndex();
        if (timestampIndex == -1) {
            return copyUnordered(cursor, writer, recordToRowCopier, circuitBreaker);
        } else {
            return copyOrdered(writer, metadata, cursor, recordToRowCopier, timestampIndex, circuitBreaker);
        }
    }

    /**
     * Returns number of copied rows.
     */
    private long copyUnordered(RecordCursor cursor, TableWriter writer, RecordToRowCopier copier, SqlExecutionCircuitBreaker circuitBreaker) {
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

    private CompiledQuery createTable(final ExecutionModel model, SqlExecutionContext executionContext) throws
            SqlException {
        final CreateTableModel createTableModel = (CreateTableModel) model;
        final ExpressionNode name = createTableModel.getName();

        // Fast path for CREATE TABLE IF NOT EXISTS in scenario when the table already exists
        if (createTableModel.isIgnoreIfExists()
                &&
                engine.getStatus(executionContext.getCairoSecurityContext(), path,
                        name.token, 0, name.token.length()) != TableUtils.TABLE_DOES_NOT_EXIST) {
            return compiledQuery.ofCreateTable();
        }

        this.insertCount = -1;

        // Slow path with lock attempt
        CharSequence lockedReason = engine.lock(executionContext.getCairoSecurityContext(), name.token, "createTable");
        if (null == lockedReason) {
            TableWriter writer = null;
            boolean newTable = false;
            try {
                if (engine.getStatus(executionContext.getCairoSecurityContext(), path,
                        name.token, 0, name.token.length()) != TableUtils.TABLE_DOES_NOT_EXIST) {
                    if (createTableModel.isIgnoreIfExists()) {
                        return compiledQuery.ofCreateTable();
                    }
                    throw SqlException.$(name.position, "table already exists");
                }
                try {
                    if (createTableModel.getQueryModel() == null) {
                        if (createTableModel.getLikeTableName() != null) {
                            copyTableReaderMetadataToCreateTableModel(executionContext, createTableModel);
                        }
                        engine.createTableUnsafe(executionContext.getCairoSecurityContext(), mem, path, createTableModel);
                        newTable = true;
                    } else {
                        writer = createTableFromCursor(createTableModel, executionContext);
                    }
                } catch (CairoException e) {
                    LOG.error().$("could not create table [error=").$((Throwable) e).$(']').$();
                    if (e.isInterruption()) {
                        throw e;
                    }

                    throw SqlException.$(name.position, "Could not create table. See log for details.");
                }
            } finally {
                engine.unlock(executionContext.getCairoSecurityContext(), name.token, writer, newTable);
            }
        } else {
            throw SqlException.$(name.position, "cannot acquire table lock [lockedReason=").put(lockedReason).put(']');
        }

        if (createTableModel.getQueryModel() == null) {
            return compiledQuery.ofCreateTable();
        } else {
            return compiledQuery.ofCreateTableAsSelect(insertCount);
        }
    }

    private void copyTableReaderMetadataToCreateTableModel(SqlExecutionContext executionContext, CreateTableModel model) throws SqlException {
        ExpressionNode likeTableName = model.getLikeTableName();
        CharSequence likeTableNameToken = likeTableName.token;
        tableExistsOrFail(likeTableName.position, likeTableNameToken, executionContext);
        try (TableReader rdr = engine.getReader(executionContext.getCairoSecurityContext(), likeTableNameToken)) {
            model.setCommitLag(rdr.getCommitLag());
            model.setMaxUncommittedRows(rdr.getMaxUncommittedRows());
            TableReaderMetadata rdrMetadata = rdr.getMetadata();
            for (int i = 0; i < rdrMetadata.getColumnCount(); i++) {
                int columnType = rdrMetadata.getColumnType(i);
                boolean isSymbol = ColumnType.isSymbol(columnType);
                int symbolCapacity = isSymbol ? rdr.getSymbolMapReader(i).getSymbolCapacity() : configuration.getDefaultSymbolCapacity();
                model.addColumn(rdrMetadata.getColumnName(i), columnType, symbolCapacity, rdrMetadata.getColumnHash(i));
                if (isSymbol) {
                    model.cached(rdr.getSymbolMapReader(i).isCached());
                }
                model.setIndexFlags(rdrMetadata.isColumnIndexed(i), rdrMetadata.getIndexValueBlockCapacity(i));
            }
            model.setPartitionBy(SqlUtil.nextLiteral(sqlNodePool, PartitionBy.toString(rdr.getPartitionedBy()), 0));
            if (rdrMetadata.getTimestampIndex() != -1) {
                model.setTimestamp(SqlUtil.nextLiteral(sqlNodePool, rdrMetadata.getColumnName(rdrMetadata.getTimestampIndex()), 0));
            }
            model.setWalEnabled(rdrMetadata.isWalEnabled());
        }
        model.setLikeTableName(null); // resetting like table name as the metadata is copied already at this point.
    }

    private TableWriter createTableFromCursor(CreateTableModel model, SqlExecutionContext executionContext) throws
            SqlException {
        try (
                final RecordCursorFactory factory = generate(model.getQueryModel(), executionContext);
                final RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            typeCast.clear();
            final RecordMetadata metadata = factory.getMetadata();
            validateTableModelAndCreateTypeCast(model, metadata, typeCast);
            engine.createTableUnsafe(
                    executionContext.getCairoSecurityContext(),
                    mem,
                    path,
                    tableStructureAdapter.of(model, metadata, typeCast)
            );

            SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();

            try {
                return copyTableData(model.getName().token, cursor, metadata, circuitBreaker);
            } catch (CairoException e) {
                LOG.error().$(e.getFlyweightMessage()).$(" [errno=").$(e.getErrno()).$(']').$();
                if (removeTableDirectory(model)) {
                    throw e;
                }
                throw SqlException.$(0, "Concurrent modification could not be handled. Failed to clean up. See log for more details.");
            }
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
    private CompiledQuery createTableWithRetries(
            ExecutionModel executionModel,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return executeWithRetries(createTableMethod, executionModel, configuration.getCreateAsSelectRetryCount(), executionContext);
    }

    private CompiledQuery dropTable(SqlExecutionContext executionContext) throws SqlException {
        // expected syntax: DROP TABLE [ IF EXISTS ] name [;]
        expectKeyword(lexer, "table");
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "expected [if exists] table-name");
        }
        boolean hasIfExists = false;
        if (SqlKeywords.isIfKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || !SqlKeywords.isExistsKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "expected exists");
            }
            hasIfExists = true;
        } else {
            lexer.unparseLast(); // tok has table name
        }
        final int tableNamePosition = lexer.getPosition();
        CharSequence tableName = GenericLexer.unquote(expectToken(lexer, "table name"));
        tok = SqlUtil.fetchNext(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [").put(tok).put("]");
        }
        if (TableUtils.TABLE_DOES_NOT_EXIST == engine.getStatus(executionContext.getCairoSecurityContext(), path, tableName)) {
            if (hasIfExists) {
                return compiledQuery.ofDrop();
            }
            throw SqlException.$(tableNamePosition, "table does not exist [table=").put(tableName).put(']');
        }
        engine.remove(executionContext.getCairoSecurityContext(), path, tableName);
        return compiledQuery.ofDrop();
    }

    @NotNull
    private CompiledQuery executeCopy(SqlExecutionContext executionContext, CopyModel executionModel) throws SqlException {
        executionContext.getCairoSecurityContext().checkWritePermission();
        if (!executionModel.isCancel() && Chars.equalsLowerCaseAscii(executionModel.getFileName().token, "stdin")) {
            // no-op implementation
            setupTextLoaderFromModel(executionModel);
            return compiledQuery.ofCopyRemote(textLoader);
        }
        RecordCursorFactory copyFactory = executeCopy0(executionModel);
        return compiledQuery.ofCopyLocal(copyFactory);
    }

    @Nullable
    private RecordCursorFactory executeCopy0(CopyModel model) throws SqlException {
        try {
            if (model.isCancel()) {
                cancelTextImport(model);
                return null;
            } else {
                if (model.getTimestampColumnName() == null &&
                        ((model.getPartitionBy() != -1 && model.getPartitionBy() != PartitionBy.NONE))) {
                    throw SqlException.$(-1, "invalid option used for import without a designated timestamp (format or partition by)");
                }
                if (model.getTimestampFormat() == null) {
                    model.setTimestampFormat("yyyy-MM-ddTHH:mm:ss.SSSUUUZ");
                }
                if (model.getDelimiter() < 0) {
                    model.setDelimiter((byte) ',');
                }
                return compileTextImport(model);
            }
        } catch (TextImportException | TextException e) {
            LOG.error().$((Throwable) e).$();
            throw SqlException.$(0, e.getMessage());
        }
    }

    private CompiledQuery executeWithRetries(
            ExecutableMethod method,
            ExecutionModel executionModel,
            int retries,
            SqlExecutionContext executionContext
    ) throws SqlException {
        int attemptsLeft = retries;
        do {
            try {
                return method.execute(executionModel, executionContext);
            } catch (ReaderOutOfDateException e) {
                attemptsLeft--;
                clear();
                lexer.restart();
                executionModel = compileExecutionModel(executionContext);
            }
        } while (attemptsLeft > 0);

        throw SqlException.position(0).put("underlying cursor is extremely volatile");
    }

    RecordCursorFactory generate(QueryModel queryModel, SqlExecutionContext executionContext) throws SqlException {
        return codeGenerator.generate(queryModel, executionContext);
    }

    UpdateOperation generateUpdate(QueryModel updateQueryModel, SqlExecutionContext executionContext) throws SqlException {
        // Update QueryModel structure is
        // QueryModel with SET column expressions
        // |-- QueryModel of select-virtual or select-choose of data selected for update
        final QueryModel selectQueryModel = updateQueryModel.getNestedModel();
        final RecordCursorFactory recordCursorFactory = prepareForUpdate(
                updateQueryModel.getUpdateTableName(),
                selectQueryModel,
                updateQueryModel,
                executionContext
        );

        return new UpdateOperation(
                updateQueryModel.getUpdateTableName(),
                selectQueryModel.getTableId(),
                selectQueryModel.getTableVersion(),
                lexer.getPosition(),
                recordCursorFactory
        );
    }

    private int getNextValidTokenPosition() {
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

    private int goToQueryEnd() {
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

    private CompiledQuery insert(ExecutionModel executionModel, SqlExecutionContext executionContext) throws SqlException {
        final InsertModel model = (InsertModel) executionModel;
        final ExpressionNode name = model.getTableName();
        tableExistsOrFail(name.position, name.token, executionContext);

        ObjList<Function> valueFunctions = null;
        try (TableReader reader = engine.getReader(
                executionContext.getCairoSecurityContext(),
                name.token,
                TableUtils.ANY_TABLE_ID,
                TableUtils.ANY_TABLE_VERSION
        )) {
            final long structureVersion = reader.getVersion();
            final RecordMetadata metadata = reader.getMetadata();
            final InsertOperationImpl insertOperation = new InsertOperationImpl(engine, reader.getTableName(), structureVersion);
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
                                    GenericRecordMetadata.EMPTY,
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
                                        "row value count does not match column count [expected=").put(columnCount).put(", actual=").put(values.size())
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
                if (metadataTimestampIndex > -1 && (timestampFunction == null || ColumnType.isNull(timestampFunction.getType()))) {
                    throw SqlException.$(0, "insert statement must populate timestamp");
                }

                VirtualRecord record = new VirtualRecord(valueFunctions);
                RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(asm, record, metadata, listColumnFilter);
                insertOperation.addInsertRow(new InsertRowImpl(record, copier, timestampFunction, tupleIndex));
            }
            return compiledQuery.ofInsert(insertOperation);
        } catch (SqlException e) {
            Misc.freeObjList(valueFunctions);
            throw e;
        }
    }

    private CompiledQuery insertAsSelect(ExecutionModel executionModel, SqlExecutionContext executionContext) throws SqlException {
        final InsertModel model = (InsertModel) executionModel;
        final ExpressionNode name = model.getTableName();
        tableExistsOrFail(name.position, name.token, executionContext);
        long insertCount;

        try (
                TableWriter writer = engine.getWriter(executionContext.getCairoSecurityContext(), name.token, "insertAsSelect");
                RecordCursorFactory factory = generate(model.getQueryModel(), executionContext)
        ) {
            final RecordMetadata cursorMetadata = factory.getMetadata();
            // Convert sparse writer metadata into dense
            final BaseRecordMetadata writerMetadata = writer.getMetadata().copyDense();
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
                            throw SqlException.$(name.position, "expected timestamp column but type is ").put(ColumnType.nameOf(fromType));
                        }
                    }
                }

                // fail when target table requires chronological data and cursor cannot provide it
                if (timestampIndexFound < 0 && writerTimestampIndex >= 0) {
                    throw SqlException.$(name.position, "select clause must provide timestamp column");
                }

                copier = RecordToRowCopierUtils.generateCopier(asm, cursorMetadata, writerMetadata, listColumnFilter);
            } else {
                // fail when target table requires chronological data and cursor cannot provide it
                if (writerTimestampIndex > -1 && cursorTimestampIndex == -1) {
                    if (cursorColumnCount <= writerTimestampIndex) {
                        throw SqlException.$(name.position, "select clause must provide timestamp column");
                    } else {
                        int columnType = ColumnType.tagOf(cursorMetadata.getColumnType(writerTimestampIndex));
                        if (columnType != ColumnType.TIMESTAMP && columnType != ColumnType.STRING && columnType != ColumnType.NULL) {
                            throw SqlException.$(name.position, "expected timestamp column but type is ").put(ColumnType.nameOf(columnType));
                        }
                    }
                }

                if (writerTimestampIndex > -1 && cursorTimestampIndex > -1 && writerTimestampIndex != cursorTimestampIndex) {
                    throw SqlException
                            .$(name.position, "designated timestamp of existing table (").put(writerTimestampIndex)
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
                                    writerTimestampIndex,
                                    model.getBatchSize(),
                                    model.getCommitLag(),
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
        }
        return compiledQuery.ofInsertAsSelect(insertCount);
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

    private ExecutionModel lightlyValidateInsertModel(InsertModel model) throws SqlException {
        ExpressionNode tableName = model.getTableName();
        if (tableName.type != ExpressionNode.LITERAL) {
            throw SqlException.$(tableName.position, "literal expected");
        }

        int columnNameListSize = model.getColumnNameList().size();

        for (int i = 0, n = model.getRowTupleCount(); i < n; i++) {
            if (columnNameListSize > 0 && columnNameListSize != model.getRowTupleValues(i).size()) {
                throw SqlException.$(
                                model.getEndOfRowTupleValuesPosition(i),
                                "row value count does not match column count [expected=").put(columnNameListSize)
                        .put(", actual=").put(model.getRowTupleValues(i).size())
                        .put(", tuple=").put(i + 1)
                        .put(']');
            }
        }

        return model;
    }

    private RecordCursorFactory prepareForUpdate(
            String tableName,
            QueryModel selectQueryModel,
            QueryModel updateQueryModel,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final IntList tableColumnTypes = selectQueryModel.getUpdateTableColumnTypes();
        final ObjList<CharSequence> tableColumnNames = selectQueryModel.getUpdateTableColumnNames();

        RecordCursorFactory updateToDataCursorFactory = codeGenerator.generate(selectQueryModel, executionContext);
        try {
            if (!updateToDataCursorFactory.supportsUpdateRowId(tableName)) {
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

                if (virtualColumnType != tableColumnType) {
                    if (!ColumnType.isSymbol(tableColumnType) || virtualColumnType != ColumnType.STRING) {
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

    private CompiledQuery reindexTable(SqlExecutionContext executionContext) throws SqlException {
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
        tableExistsOrFail(lexer.lastTokenPosition(), tok, executionContext);
        CharSequence tableName = tok;
        rebuildIndex.of(path.of(configuration.getRoot()).concat(tableName), configuration);

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

        rebuildIndex.reindex(partition, columnName);
        return compiledQuery.ofRepair();
    }

    private boolean removeTableDirectory(CreateTableModel model) {
        int errno;
        if ((errno = engine.removeDirectory(path, model.getName().token)) == 0) {
            return true;
        }
        LOG.error()
                .$("could not clean up after create table failure [path=").$(path)
                .$(", errno=").$(errno)
                .$(']').$();
        return false;
    }

    private CompiledQuery repairTables(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !isTableKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'table' expected");
        }

        do {
            tok = SqlUtil.fetchNext(lexer);

            if (tok == null || Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.getPosition(), "table name expected");
            }

            if (Chars.isQuoted(tok)) {
                tok = GenericLexer.unquote(tok);
            }
            tableExistsOrFail(lexer.lastTokenPosition(), tok, executionContext);
            tok = SqlUtil.fetchNext(lexer);

        } while (tok != null && Chars.equals(tok, ','));
        return compiledQuery.ofRepair();
    }

    // used in tests
    void setEnableJitNullChecks(boolean value) {
        codeGenerator.setEnableJitNullChecks(value);
    }

    void setFullFatJoins(boolean value) {
        codeGenerator.setFullFatJoins(value);
    }

    private void setupTextLoaderFromModel(CopyModel model) {
        textLoader.clear();
        textLoader.setState(TextLoader.ANALYZE_STRUCTURE);
        // todo: configure the following
        //   - what happens when data row errors out, max errors may be?
        //   - we should be able to skip X rows from top, dodgy headers etc.

        textLoader.configureDestination(model.getTarget().token, false, false,
                model.getAtomicity() != -1 ? model.getAtomicity() : Atomicity.SKIP_ROW,
                model.getPartitionBy() < 0 ? PartitionBy.NONE : model.getPartitionBy(),
                model.getTimestampColumnName(), model.getTimestampFormat());
    }

    private CompiledQuery snapshotDatabase(SqlExecutionContext executionContext) throws SqlException {
        executionContext.getCairoSecurityContext().checkWritePermission();
        CharSequence tok = expectToken(lexer, "'prepare' or 'complete'");

        if (Chars.equalsLowerCaseAscii(tok, "prepare")) {
            if (snapshotAgent == null) {
                throw SqlException.position(lexer.lastTokenPosition()).put("Snapshot agent is not configured. Try using different embedded API");
            }
            snapshotAgent.prepareSnapshot(executionContext);
            return compiledQuery.ofSnapshotPrepare();
        }

        if (Chars.equalsLowerCaseAscii(tok, "complete")) {
            if (snapshotAgent == null) {
                throw SqlException.position(lexer.lastTokenPosition()).put("Snapshot agent is not configured. Try using different embedded API");
            }
            snapshotAgent.completeSnapshot();
            return compiledQuery.ofSnapshotComplete();
        }

        throw SqlException.position(lexer.lastTokenPosition()).put("'prepare' or 'complete' expected");
    }

    private CompiledQuery sqlShow(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (null != tok) {
            if (isTablesKeyword(tok)) {
                return compiledQuery.of(new TableListRecordCursorFactory(configuration.getFilesFacade(), configuration.getRoot()));
            }
            if (isColumnsKeyword(tok)) {
                return sqlShowColumns(executionContext);
            }

            if (isTransactionKeyword(tok)) {
                return sqlShowTransaction();
            }

            if (isTransactionIsolation(tok)) {
                return compiledQuery.of(new ShowTransactionIsolationLevelCursorFactory());
            }

            if (isMaxIdentifierLength(tok)) {
                return compiledQuery.of(new ShowMaxIdentifierLengthCursorFactory());
            }

            if (isStandardConformingStrings(tok)) {
                return compiledQuery.of(new ShowStandardConformingStringsCursorFactory());
            }

            if (isSearchPath(tok)) {
                return compiledQuery.of(new ShowSearchPathCursorFactory());
            }

            if (isDateStyleKeyword(tok)) {
                return compiledQuery.of(new ShowDateStyleCursorFactory());
            }

            if (SqlKeywords.isTimeKeyword(tok)) {
                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && SqlKeywords.isZoneKeyword(tok)) {
                    return compiledQuery.of(new ShowTimeZoneFactory());
                }
            }
        }

        throw SqlException.position(lexer.lastTokenPosition()).put("expected 'tables', 'columns' or 'time zone'");
    }

    private CompiledQuery sqlShowColumns(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);
        if (null == tok || !isFromKeyword(tok)) {
            throw SqlException.position(lexer.getPosition()).put("expected 'from'");
        }
        tok = SqlUtil.fetchNext(lexer);
        if (null == tok) {
            throw SqlException.position(lexer.getPosition()).put("expected a table name");
        }
        final CharSequence tableName = GenericLexer.assertNoDotsAndSlashes(GenericLexer.unquote(tok), lexer.lastTokenPosition());
        int status = engine.getStatus(executionContext.getCairoSecurityContext(), path, tableName, 0, tableName.length());
        if (status != TableUtils.TABLE_EXISTS) {
            throw SqlException.$(lexer.lastTokenPosition(), "table does not exist [table=").put(tableName).put(']');
        }
        return compiledQuery.of(new ShowColumnsRecordCursorFactory(tableName));
    }

    private CompiledQuery sqlShowTransaction() throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok != null && isIsolationKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok != null && isLevelKeyword(tok)) {
                return compiledQuery.of(new ShowTransactionIsolationLevelCursorFactory());
            }
            throw SqlException.position(tok != null ? lexer.lastTokenPosition() : lexer.getPosition()).put("expected 'level'");
        }
        throw SqlException.position(tok != null ? lexer.lastTokenPosition() : lexer.getPosition()).put("expected 'isolation'");
    }

    private void tableExistsOrFail(int position, CharSequence tableName, SqlExecutionContext executionContext) throws SqlException {
        if (engine.getStatus(executionContext.getCairoSecurityContext(), path, tableName) == TableUtils.TABLE_DOES_NOT_EXIST) {
            throw SqlException.$(position, "table does not exist [table=").put(tableName).put(']');
        }
    }

    @TestOnly
    ExecutionModel testCompileModel(CharSequence query, SqlExecutionContext executionContext) throws SqlException {
        clear();
        lexer.of(query);
        return compileExecutionModel(executionContext);
    }

    // this exposed for testing only
    @TestOnly
    ExpressionNode testParseExpression(CharSequence expression, QueryModel model) throws SqlException {
        clear();
        lexer.of(expression);
        return parser.expr(lexer, model);
    }

    // test only
    @TestOnly
    void testParseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException {
        clear();
        lexer.of(expression);
        parser.expr(lexer, listener);
    }

    private CompiledQuery truncateTables(SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);

        if (tok == null) {
            throw SqlException.$(lexer.getPosition(), "'table' expected");
        }

        if (!isTableKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'table' expected");
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok != null && isOnlyKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
        }

        tableWriters.clear();
        try {
            try {
                do {
                    if (tok == null || Chars.equals(tok, ',')) {
                        throw SqlException.$(lexer.getPosition(), "table name expected");
                    }

                    if (Chars.isQuoted(tok)) {
                        tok = GenericLexer.unquote(tok);
                    }
                    tableExistsOrFail(lexer.lastTokenPosition(), tok, executionContext);

                    try {
                        tableWriters.add(engine.getWriter(executionContext.getCairoSecurityContext(), tok, "truncateTables"));
                    } catch (CairoException e) {
                        LOG.info().$("table busy [table=").$(tok).$(", e=").$((Throwable) e).$(']').$();
                        throw SqlException.$(lexer.lastTokenPosition(), "table '").put(tok).put("' could not be truncated: ").put(e);
                    }
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok == null || Chars.equals(tok, ';')) {
                        break;
                    }
                    if (Chars.equalsNc(tok, ',')) {
                        tok = SqlUtil.fetchNext(lexer);
                    }

                } while (true);
            } catch (SqlException e) {
                for (int i = 0, n = tableWriters.size(); i < n; i++) {
                    tableWriters.getQuick(i).close();
                }
                throw e;
            }

            for (int i = 0, n = tableWriters.size(); i < n; i++) {
                try (TableWriter writer = tableWriters.getQuick(i)) {
                    try {
                        if (engine.lockReaders(writer.getTableName())) {
                            try {
                                writer.truncate();
                            } finally {
                                engine.unlockReaders(writer.getTableName());
                            }
                        } else {
                            throw SqlException.$(0, "there is an active query against '").put(writer.getTableName()).put("'. Try again.");
                        }
                    } catch (CairoException | CairoError e) {
                        LOG.error().$("could not truncate [table=").$(writer.getTableName()).$(", e=").$((Sinkable) e).$(']').$();
                        throw e;
                    }
                }
            }
        } finally {
            tableWriters.clear();
        }
        return compiledQuery.ofTruncate();
    }

    private CompiledQuery vacuum(SqlExecutionContext executionContext) throws SqlException {
        executionContext.getCairoSecurityContext().checkWritePermission();
        CharSequence tok = expectToken(lexer, "'table'");
        // It used to be VACUUM PARTITIONS but become VACUUM TABLE
        boolean partitionsKeyword = isPartitionsKeyword(tok);
        if (partitionsKeyword || isTableKeyword(tok)) {
            CharSequence tableName = expectToken(lexer, "table name");
            tableName = GenericLexer.assertNoDotsAndSlashes(GenericLexer.unquote(tableName), lexer.lastTokenPosition());
            int tableNamePos = lexer.lastTokenPosition();
            CharSequence eol = SqlUtil.fetchNext(lexer);
            if (eol == null || Chars.equals(eol, ';')) {
                executionContext.getCairoSecurityContext().checkWritePermission();
                tableExistsOrFail(lexer.lastTokenPosition(), tableName, executionContext);
                try (TableReader rdr = engine.getReader(executionContext.getCairoSecurityContext(), tableName)) {
                    int partitionBy = rdr.getMetadata().getPartitionBy();
                    if (PartitionBy.isPartitioned(partitionBy)) {
                        if (!TableUtils.schedulePurgeO3Partitions(messageBus, rdr.getTableName(), partitionBy)) {
                            throw SqlException.$(
                                    tableNamePos,
                                    "cannot schedule vacuum action, queue is full, please retry " +
                                            "or increase Purge Discovery Queue Capacity"
                            );
                        }
                    } else if (partitionsKeyword) {
                        throw SqlException.$(lexer.lastTokenPosition(), "table '").put(tableName).put("' is not partitioned");
                    }
                    vacuumColumnVersions.run(executionContext, rdr);
                    return compiledQuery.ofVacuum();
                }
            }
            throw SqlException.$(lexer.lastTokenPosition(), "end of line or ';' expected");
        }
        throw SqlException.$(lexer.lastTokenPosition(), "'partitions' expected");
    }

    private InsertModel validateAndOptimiseInsertAsSelect(
            InsertModel model,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final QueryModel queryModel = optimiser.optimise(model.getQueryModel(), executionContext);
        int columnNameListSize = model.getColumnNameList().size();
        if (columnNameListSize > 0 && queryModel.getBottomUpColumns().size() != columnNameListSize) {
            throw SqlException.$(model.getTableName().position, "column count mismatch");
        }
        model.setQueryModel(queryModel);
        return model;
    }

    private void validateTableModelAndCreateTypeCast(
            CreateTableModel model,
            RecordMetadata metadata,
            @Transient IntIntHashMap typeCast) throws SqlException {
        CharSequenceObjHashMap<ColumnCastModel> castModels = model.getColumnCastModels();
        ObjList<CharSequence> castColumnNames = castModels.keys();

        for (int i = 0, n = castColumnNames.size(); i < n; i++) {
            CharSequence columnName = castColumnNames.getQuick(i);
            int index = metadata.getColumnIndexQuiet(columnName);
            // the only reason why columns cannot be found at this stage is
            // concurrent table modification of table structure
            if (index == -1) {
                // Cast isn't going to go away when we re-parse SQL. We must make this
                // permanent error
                throw SqlException.invalidColumn(castModels.get(columnName).getColumnNamePos(), columnName);
            }
            ColumnCastModel ccm = castModels.get(columnName);
            int from = metadata.getColumnType(index);
            int to = ccm.getColumnType();
            if (isCompatibleCase(from, to)) {
                typeCast.put(index, to);
            } else {
                throw SqlException.unsupportedCast(ccm.getColumnTypePos(), columnName, from, to);
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

    @FunctionalInterface
    protected interface KeywordBasedExecutor {
        CompiledQuery execute(SqlExecutionContext executionContext) throws SqlException;
    }

    @FunctionalInterface
    private interface ExecutableMethod {
        CompiledQuery execute(ExecutionModel model, SqlExecutionContext sqlExecutionContext) throws SqlException;
    }

    public final static class PartitionAction {
        public static final int DROP = 1;
        public static final int ATTACH = 2;
        public static final int DETACH = 3;
    }

    private static class TableStructureAdapter implements TableStructure {
        private CreateTableModel model;
        private RecordMetadata metadata;
        private IntIntHashMap typeCast;
        private int timestampIndex;

        @Override
        public int getColumnCount() {
            return model.getColumnCount();
        }

        @Override
        public long getColumnHash(int columnIndex) {
            return metadata.getColumnHash(columnIndex);
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
        public long getCommitLag() {
            return model.getCommitLag();
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
        public boolean isWallEnabled() {
            return model.isWallEnabled();
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return model.isIndexed(columnIndex);
        }

        @Override
        public boolean isSequential(int columnIndex) {
            return model.isSequential(columnIndex);
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
        protected final Path srcPath = new Path();
        private final CharSequenceObjHashMap<RecordToRowCopier> tableBackupRowCopiedCache = new CharSequenceObjHashMap<>();
        private final ObjHashSet<CharSequence> tableNames = new ObjHashSet<>();
        private final Path dstPath = new Path();
        private final StringSink fileNameSink = new StringSink();
        private transient String cachedTmpBackupRoot;
        private transient int changeDirPrefixLen;
        private transient int currDirPrefixLen;
        private final FindVisitor confFilesBackupOnFind = (file, type) -> {
            if (type == Files.DT_FILE) {
                srcPath.of(configuration.getConfRoot()).concat(file).$();
                dstPath.trimTo(currDirPrefixLen).concat(file).$();
                LOG.info().$("backup copying config file [from=").$(srcPath).$(",to=").$(dstPath).I$();
                if (ff.copy(srcPath, dstPath) < 0) {
                    throw CairoException.critical(ff.errno()).put("cannot backup conf file [to=").put(dstPath).put(']');
                }
            }
        };
        private transient SqlExecutionContext currentExecutionContext;
        private final FindVisitor sqlDatabaseBackupOnFind = (pUtf8NameZ, type) -> {
            if (Files.isDir(pUtf8NameZ, type, fileNameSink)) {
                try {
                    backupTable(fileNameSink, currentExecutionContext);
                } catch (CairoException e) {
                    LOG.error()
                            .$("could not backup [path=").$(fileNameSink)
                            .$(", e=").$(e.getFlyweightMessage())
                            .$(", errno=").$(e.getErrno())
                            .$(']').$();
                } catch (SqlException e) {
                    LOG.error()
                            .$("could not backup [path=").$(fileNameSink)
                            .$(", e=").$(e.getFlyweightMessage())
                            .$(']').$();
                }
            }
        };

        public void clear() {
            srcPath.trimTo(0);
            dstPath.trimTo(0);
            cachedTmpBackupRoot = null;
            changeDirPrefixLen = 0;
            currDirPrefixLen = 0;
            tableBackupRowCopiedCache.clear();
            tableNames.clear();
        }

        @Override
        public void close() {
            assert null == currentExecutionContext;
            assert tableNames.isEmpty();
            tableBackupRowCopiedCache.clear();
            Misc.free(srcPath);
            Misc.free(dstPath);
        }

        private void backupTabIndexFile() {
            srcPath.of(configuration.getRoot()).concat(TableUtils.TAB_INDEX_FILE_NAME).$();
            dstPath.trimTo(currDirPrefixLen).concat(TableUtils.TAB_INDEX_FILE_NAME).$();
            LOG.info().$("backup copying file [from=").$(srcPath).$(",to=").$(dstPath).I$();
            if (ff.copy(srcPath, dstPath) < 0) {
                throw CairoException.critical(ff.errno()).put("cannot backup tab index file [to=").put(dstPath).put(']');
            }
        }

        private void backupTable(@NotNull CharSequence tableName, @NotNull SqlExecutionContext executionContext) throws SqlException {
            LOG.info().$("Starting backup of ").$(tableName).$();
            if (null == cachedTmpBackupRoot) {
                if (null == configuration.getBackupRoot()) {
                    throw CairoException.nonCritical().put("Backup is disabled, no backup root directory is configured in the server configuration ['cairo.sql.backup.root' property]");
                }
                srcPath.of(configuration.getBackupRoot()).concat(configuration.getBackupTempDirName()).slash$();
                cachedTmpBackupRoot = Chars.toString(srcPath);
            }

            int renameRootLen = dstPath.length();
            try {
                CairoSecurityContext securityContext = executionContext.getCairoSecurityContext();
                try (TableReader reader = engine.getReader(securityContext, tableName)) {
                    cloneMetaData(tableName, cachedTmpBackupRoot, configuration.getBackupMkDirMode(), reader);
                    try (TableWriter backupWriter = engine.getBackupWriter(securityContext, tableName, cachedTmpBackupRoot)) {
                        RecordMetadata writerMetadata = backupWriter.getMetadata();
                        srcPath.of(tableName).slash().put(reader.getVersion()).$();
                        RecordToRowCopier recordToRowCopier = tableBackupRowCopiedCache.get(srcPath);
                        if (null == recordToRowCopier) {
                            entityColumnFilter.of(writerMetadata.getColumnCount());
                            recordToRowCopier = RecordToRowCopierUtils.generateCopier(
                                    asm,
                                    reader.getMetadata(),
                                    writerMetadata,
                                    entityColumnFilter
                            );
                            tableBackupRowCopiedCache.put(srcPath.toString(), recordToRowCopier);
                        }

                        RecordCursor cursor = reader.getCursor();
                        //statement/query timeout value  is most likely too small for backup operation
                        copyTableData(cursor, reader.getMetadata(), backupWriter, writerMetadata, recordToRowCopier, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                        backupWriter.commit();
                    }
                }

                srcPath.of(configuration.getBackupRoot()).concat(configuration.getBackupTempDirName()).concat(tableName).$();
                try {
                    dstPath.trimTo(renameRootLen).concat(tableName).$();
                    TableUtils.renameOrFail(ff, srcPath, dstPath);
                    LOG.info().$("backup complete [table=").$(tableName).$(", to=").$(dstPath).$(']').$();
                } finally {
                    dstPath.trimTo(renameRootLen).$();
                }
            } catch (CairoException | SqlException e) {
                LOG.info()
                        .$("could not backup [table=").$(tableName)
                        .$(", ex=").$(e.getFlyweightMessage())
                        .$(", errno=").$((e instanceof CairoException) ? ((CairoException) e).getErrno() : 0)
                        .$(']').$();
                srcPath.of(cachedTmpBackupRoot).concat(tableName).slash$();
                int errno;
                if ((errno = ff.rmdir(srcPath)) != 0) {
                    LOG.error().$("could not delete directory [path=").$(srcPath).$(", errno=").$(errno).$(']').$();
                }
                throw e;
            }
        }

        private void cdConfRenamePath() {
            mkdir(PropServerConfiguration.CONFIG_DIRECTORY, "could not create backup [conf dir=");
        }

        private void cdDbRenamePath() {
            mkdir(configuration.getDbDirectory(), "could not create backup [db dir=");
        }

        private void cloneMetaData(CharSequence tableName, CharSequence backupRoot, int mkDirMode, TableReader reader) {
            srcPath.of(backupRoot).concat(tableName).slash$();

            if (ff.exists(srcPath)) {
                throw CairoException.nonCritical().put("Backup dir for table \"").put(tableName).put("\" already exists [dir=").put(srcPath).put(']');
            }

            if (ff.mkdirs(srcPath, mkDirMode) != 0) {
                throw CairoException.critical(ff.errno()).put("Could not create [dir=").put(srcPath).put(']');
            }

            int rootLen = srcPath.length();

            TableReaderMetadata sourceMetaData = reader.getMetadata();
            try {
                mem.smallFile(ff, srcPath.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                sourceMetaData.dumpTo(mem);

                // create symbol maps
                srcPath.trimTo(rootLen).$();
                int symbolMapCount = 0;
                for (int i = 0, sz = sourceMetaData.getColumnCount(); i < sz; i++) {
                    if (ColumnType.isSymbol(sourceMetaData.getColumnType(i))) {
                        SymbolMapReader mapReader = reader.getSymbolMapReader(i);
                        MapWriter.createSymbolMapFiles(ff, mem, srcPath, sourceMetaData.getColumnName(i), COLUMN_NAME_TXN_NONE, mapReader.getSymbolCapacity(), mapReader.isCached());
                        symbolMapCount++;
                    }
                }
                mem.smallFile(ff, srcPath.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                TableUtils.createTxn(mem, symbolMapCount, 0L, TableUtils.INITIAL_TXN, 0L, sourceMetaData.getStructureVersion(), 0L, 0L);

                mem.smallFile(ff, srcPath.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                TableUtils.createColumnVersionFile(mem);
                srcPath.trimTo(rootLen).concat(TableUtils.TXN_SCOREBOARD_FILE_NAME).$();
            } finally {
                mem.close();
            }
        }

        private void mkdir(CharSequence dir, String errorMessage) {
            dstPath.trimTo(changeDirPrefixLen).concat(dir).slash$();
            currDirPrefixLen = dstPath.length();
            if (ff.mkdirs(dstPath, configuration.getBackupMkDirMode()) != 0) {
                throw CairoException.critical(ff.errno()).put(errorMessage).put(dstPath).put(']');
            }
        }

        private void setupBackupRenamePath() {
            DateFormat format = configuration.getBackupDirTimestampFormat();
            long epochMicros = configuration.getMicrosecondClock().getTicks();
            int n = 0;
            // There is a race here, two threads could try and create the same backupRenamePath,
            // only one will succeed the other will throw a CairoException. Maybe it should be serialised
            dstPath.of(configuration.getBackupRoot()).slash();
            int plen = dstPath.length();
            do {
                dstPath.trimTo(plen);
                format.format(epochMicros, configuration.getDefaultDateLocale(), null, dstPath);
                if (n > 0) {
                    dstPath.put('.').put(n);
                }
                dstPath.slash$();
                n++;
            } while (ff.exists(dstPath));

            if (ff.mkdirs(dstPath, configuration.getBackupMkDirMode()) != 0) {
                throw CairoException.critical(ff.errno()).put("could not create backup [dir=").put(dstPath).put(']');
            }
            changeDirPrefixLen = dstPath.length();
        }

        private CompiledQuery sqlBackup(SqlExecutionContext executionContext) throws SqlException {
            executionContext.getCairoSecurityContext().checkWritePermission();
            if (null == configuration.getBackupRoot()) {
                throw CairoException.nonCritical().put("Backup is disabled, no backup root directory is configured in the server configuration ['cairo.sql.backup.root' property]");
            }
            final CharSequence tok = SqlUtil.fetchNext(lexer);
            if (null != tok) {
                if (isTableKeyword(tok)) {
                    return sqlTableBackup(executionContext);
                }
                if (isDatabaseKeyword(tok)) {
                    return sqlDatabaseBackup(executionContext);
                }
            }
            throw SqlException.position(lexer.lastTokenPosition()).put("expected 'table' or 'database'");
        }

        private CompiledQuery sqlDatabaseBackup(SqlExecutionContext executionContext) {
            currentExecutionContext = executionContext;
            try {
                setupBackupRenamePath();
                cdDbRenamePath();
                ff.iterateDir(srcPath.of(configuration.getRoot()).$(), sqlDatabaseBackupOnFind);
                backupTabIndexFile();
                cdConfRenamePath();
                ff.iterateDir(srcPath.of(configuration.getConfRoot()).$(), confFilesBackupOnFind);
                return compiledQuery.ofBackupTable();
            } finally {
                currentExecutionContext = null;
            }
        }

        private CompiledQuery sqlTableBackup(SqlExecutionContext executionContext) throws SqlException {
            setupBackupRenamePath();
            cdDbRenamePath();

            try {
                tableNames.clear();
                while (true) {
                    CharSequence tok = SqlUtil.fetchNext(lexer);
                    if (null == tok) {
                        throw SqlException.position(lexer.getPosition()).put("expected a table name");
                    }
                    final CharSequence tableName = GenericLexer.assertNoDotsAndSlashes(GenericLexer.unquote(tok), lexer.lastTokenPosition());
                    int status = engine.getStatus(executionContext.getCairoSecurityContext(), srcPath, tableName, 0, tableName.length());
                    if (status != TableUtils.TABLE_EXISTS) {
                        throw SqlException.$(lexer.lastTokenPosition(), "table does not exist [table=").put(tableName).put(']');
                    }
                    tableNames.add(tableName);

                    tok = SqlUtil.fetchNext(lexer);
                    if (null == tok || Chars.equals(tok, ';')) {
                        break;
                    }
                    if (!Chars.equals(tok, ',')) {
                        throw SqlException.position(lexer.lastTokenPosition()).put("expected ','");
                    }
                }

                for (int n = 0; n < tableNames.size(); n++) {
                    backupTable(tableNames.get(n), executionContext);
                }

                return compiledQuery.ofBackupTable();
            } finally {
                tableNames.clear();
            }
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
    }
}
