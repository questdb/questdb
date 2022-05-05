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
import io.questdb.cutlass.text.Atomicity;
import io.questdb.cutlass.text.TextException;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.griffin.engine.functions.cast.CastCharToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.*;
import io.questdb.griffin.engine.table.ShowColumnsRecordCursorFactory;
import io.questdb.griffin.engine.table.TableListRecordCursorFactory;
import io.questdb.griffin.model.*;
import io.questdb.griffin.update.UpdateStatement;
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

import java.io.Closeable;
import java.util.ServiceLoader;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.griffin.SqlKeywords.*;


public class SqlCompiler implements Closeable {
    public static final ObjList<String> sqlControlSymbols = new ObjList<>(8);
    private final static Log LOG = LogFactory.getLog(SqlCompiler.class);
    private static final IntList castGroups = new IntList();
    private static final CastCharToStrFunctionFactory CHAR_TO_STR_FUNCTION_FACTORY = new CastCharToStrFunctionFactory();
    //null object used to skip null checks in batch method
    private static final BatchCallback EMPTY_CALLBACK = new BatchCallback() {
        @Override
        public void postCompile(SqlCompiler compiler, CompiledQuery cq, CharSequence queryText) {
        }

        @Override
        public void preCompile(SqlCompiler compiler) {
        }
    };
    protected final GenericLexer lexer;
    protected final Path path = new Path();
    protected final CairoEngine engine;
    protected final CharSequenceObjHashMap<KeywordBasedExecutor> keywordBasedExecutors = new CharSequenceObjHashMap<>();
    protected final CompiledQueryImpl compiledQuery;
    protected final AlterStatementBuilder alterQueryBuilder = new AlterStatementBuilder();
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
        final KeywordBasedExecutor dropTable = this::dropTable;
        final KeywordBasedExecutor sqlBackup = backupAgent::sqlBackup;
        final KeywordBasedExecutor sqlShow = this::sqlShow;
        final KeywordBasedExecutor vacuumTable = this::vacuum;
        final KeywordBasedExecutor snapshotDatabase = this::snapshotDatabase;

        keywordBasedExecutors.put("truncate", truncateTables);
        keywordBasedExecutors.put("TRUNCATE", truncateTables);
        keywordBasedExecutors.put("alter", alterTable);
        keywordBasedExecutors.put("ALTER", alterTable);
        keywordBasedExecutors.put("repair", repairTables);
        keywordBasedExecutors.put("REPAIR", repairTables);
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
    }

    // Creates data type converter.
    // INT and LONG NaN values are cast to their representation rather than Double or Float NaN.
    public static RecordToRowCopier assembleRecordToRowCopier(BytecodeAssembler asm, ColumnTypes from, RecordMetadata to, ColumnFilter toColumnFilter) {
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
        int wPutTimestampStr = asm.poolInterfaceMethod(TableWriter.Row.class, "putTimestamp", "(ILjava/lang/CharSequence;)V");
        int wPutStrChar = asm.poolInterfaceMethod(TableWriter.Row.class, "putStr", "(IC)V");
        int wPutChar = asm.poolInterfaceMethod(TableWriter.Row.class, "putChar", "(IC)V");
        int wPutBin = asm.poolInterfaceMethod(TableWriter.Row.class, "putBin", "(ILio/questdb/std/BinarySequence;)V");
        int truncateGeoHashTypes = asm.poolMethod(ColumnType.class, "truncateGeoHashTypes", "(JII)J");
        int encodeCharAsGeoByte = asm.poolMethod(GeoHashes.class, "encodeChar", "(C)B");

        int checkDoubleBounds = asm.poolMethod(RecordToRowCopierUtils.class, "checkDoubleBounds", "(DDDIII)V");
        int checkLongBounds = asm.poolMethod(RecordToRowCopierUtils.class, "checkLongBounds", "(JJJIII)V");

        int maxDoubleFloat = asm.poolDoubleConst(Float.MAX_VALUE);
        int minDoubleFloat = asm.poolDoubleConst(-Float.MAX_VALUE);
        int maxDoubleLong = asm.poolDoubleConst(Long.MAX_VALUE);
        int minDoubleLong = asm.poolDoubleConst(Long.MIN_VALUE);
        int maxDoubleInt = asm.poolDoubleConst(Integer.MAX_VALUE);
        int minDoubleInt = asm.poolDoubleConst(Integer.MIN_VALUE);
        int maxDoubleShort = asm.poolDoubleConst(Short.MAX_VALUE);
        int minDoubleShort = asm.poolDoubleConst(Short.MIN_VALUE);
        int maxDoubleByte = asm.poolDoubleConst(Byte.MAX_VALUE);
        int minDoubleByte = asm.poolDoubleConst(Byte.MIN_VALUE);

        int maxLongInt = asm.poolLongConst(Integer.MAX_VALUE);
        int minLongInt = asm.poolLongConst(Integer.MIN_VALUE);
        int maxLongShort = asm.poolLongConst(Short.MAX_VALUE);
        int minLongShort = asm.poolLongConst(Short.MIN_VALUE);
        int maxLongByte = asm.poolLongConst(Byte.MAX_VALUE);
        int minLongByte = asm.poolLongConst(Byte.MIN_VALUE);

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

        int n = toColumnFilter.getColumnCount();
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
                        case ColumnType.SHORT:
                            addCheckIntBoundsCall(asm, checkLongBounds, minLongShort, maxLongShort, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            addCheckIntBoundsCall(asm, checkLongBounds, minLongByte, maxLongByte, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
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
                        case ColumnType.INT:
                            addCheckLongBoundsCall(asm, checkLongBounds, minLongInt, maxLongInt, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.l2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.DATE:
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.SHORT:
                            addCheckLongBoundsCall(asm, checkLongBounds, minLongShort, maxLongShort, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.l2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            addCheckLongBoundsCall(asm, checkLongBounds, minLongByte, maxLongByte, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.l2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
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
                        case ColumnType.INT:
                            asm.l2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.SHORT:
                            asm.l2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            asm.l2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
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
                        case ColumnType.INT:
                            asm.l2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.SHORT:
                            asm.l2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            asm.l2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
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
                        default:
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                    }
                    break;
                case ColumnType.BYTE:
                    asm.invokeInterface(rGetByte);
                    switch (toColumnTypeTag) {
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
                        case ColumnType.SHORT:
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
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
                        case ColumnType.BYTE:
                            addCheckIntBoundsCall(asm, checkLongBounds, minLongByte, maxLongByte, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
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
                        case ColumnType.INT:
                            addCheckFloatBoundsCall(asm, checkDoubleBounds, minDoubleInt, maxDoubleInt, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.f2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            addCheckFloatBoundsCall(asm, checkDoubleBounds, minDoubleLong, maxDoubleLong, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.f2l();
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            addCheckFloatBoundsCall(asm, checkDoubleBounds, minDoubleLong, maxDoubleLong, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.f2l();
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            addCheckFloatBoundsCall(asm, checkDoubleBounds, minDoubleLong, maxDoubleLong, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.f2l();
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.SHORT:
                            addCheckFloatBoundsCall(asm, checkDoubleBounds, minDoubleShort, maxDoubleShort, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.f2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            addCheckFloatBoundsCall(asm, checkDoubleBounds, minDoubleByte, maxDoubleByte, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.f2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
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
                        case ColumnType.INT:
                            addCheckDoubleBoundsCall(asm, checkDoubleBounds, minDoubleInt, maxDoubleInt, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.d2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        case ColumnType.LONG:
                            addCheckDoubleBoundsCall(asm, checkDoubleBounds, minDoubleLong, maxDoubleLong, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.d2l();
                            asm.invokeInterface(wPutLong, 3);
                            break;
                        case ColumnType.DATE:
                            addCheckDoubleBoundsCall(asm, checkDoubleBounds, minDoubleLong, maxDoubleLong, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.d2l();
                            asm.invokeInterface(wPutDate, 3);
                            break;
                        case ColumnType.TIMESTAMP:
                            addCheckDoubleBoundsCall(asm, checkDoubleBounds, minDoubleLong, maxDoubleLong, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.d2l();
                            asm.invokeInterface(wPutTimestamp, 3);
                            break;
                        case ColumnType.SHORT:
                            addCheckDoubleBoundsCall(asm, checkDoubleBounds, minDoubleShort, maxDoubleShort, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.d2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.BYTE:
                            addCheckDoubleBoundsCall(asm, checkDoubleBounds, minDoubleByte, maxDoubleByte, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.d2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.FLOAT:
                            addCheckDoubleBoundsCall(asm, checkDoubleBounds, minDoubleFloat, maxDoubleFloat, fromColumnType, toColumnTypeTag, toColumnWriterIndex);
                            asm.d2f();
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
                        case ColumnType.STRING:
                            asm.invokeInterface(wPutStrChar, 2);
                            break;
                        case ColumnType.SYMBOL:
                            asm.invokeInterface(wPutSymChar, 2);
                            break;
                        case ColumnType.GEOBYTE:
                            asm.invokeStatic(encodeCharAsGeoByte);
                            if (ColumnType.getGeoHashBits(toColumnType) < 5) {
                                asm.i2l();
                                asm.iconst(ColumnType.getGeoHashTypeWithBits(5));
                                asm.iconst(toColumnType);
                                asm.invokeStatic(truncateGeoHashTypes);
                                asm.l2i();
                                asm.i2b();
                            }
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        default:
                            asm.invokeInterface(wPutChar, 2);
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
                    asm.invokeInterface(rGetStr);
                    switch (toColumnTypeTag) {
                        case ColumnType.SYMBOL:
                            asm.invokeInterface(wPutSym, 2);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeInterface(wPutTimestampStr, 2);
                            break;
                        case ColumnType.GEOBYTE:
                        case ColumnType.GEOSHORT:
                        case ColumnType.GEOINT:
                        case ColumnType.GEOLONG:
                            asm.invokeInterface(wPutGeoStr, 2);
                            break;
                        default:
                            asm.invokeInterface(wPutStr, 2);
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
                case ColumnType.GEOBYTE:
                    asm.invokeInterface(rGetGeoByte, 1);
                    if (fromColumnType != toColumnType && (fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOBYTE)) {
                        // truncate within the same storage type
                        asm.i2l();
                        asm.iconst(fromColumnType);
                        asm.iconst(toColumnType);
                        asm.invokeStatic(truncateGeoHashTypes);
                        asm.l2i();
                        asm.i2b();
                    }
                    asm.invokeInterface(wPutByte, 2);
                    break;
                case ColumnType.GEOSHORT:
                    asm.invokeInterface(rGetGeoShort, 1);
                    if (ColumnType.tagOf(toColumnType) == ColumnType.GEOBYTE) {
                        asm.i2l();
                        asm.iconst(fromColumnType);
                        asm.iconst(toColumnType);
                        asm.invokeStatic(truncateGeoHashTypes);
                        asm.l2i();
                        asm.i2b();
                        asm.invokeInterface(wPutByte, 2);
                    } else if (fromColumnType != toColumnType && fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOSHORT) {
                        asm.i2l();
                        asm.iconst(fromColumnType);
                        asm.iconst(toColumnType);
                        asm.invokeStatic(truncateGeoHashTypes);
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
                            asm.iconst(fromColumnType);
                            asm.iconst(toColumnType);
                            asm.invokeStatic(truncateGeoHashTypes);
                            asm.l2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.GEOSHORT:
                            asm.i2l();
                            asm.iconst(fromColumnType);
                            asm.iconst(toColumnType);
                            asm.invokeStatic(truncateGeoHashTypes);
                            asm.l2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        default:
                            if (fromColumnType != toColumnType && fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOINT) {
                                asm.i2l();
                                asm.iconst(fromColumnType);
                                asm.iconst(toColumnType);
                                asm.invokeStatic(truncateGeoHashTypes);
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
                            asm.iconst(fromColumnType);
                            asm.iconst(toColumnType);
                            asm.invokeStatic(truncateGeoHashTypes);
                            asm.l2i();
                            asm.i2b();
                            asm.invokeInterface(wPutByte, 2);
                            break;
                        case ColumnType.GEOSHORT:
                            asm.iconst(fromColumnType);
                            asm.iconst(toColumnType);
                            asm.invokeStatic(truncateGeoHashTypes);
                            asm.l2i();
                            asm.i2s();
                            asm.invokeInterface(wPutShort, 2);
                            break;
                        case ColumnType.GEOINT:
                            asm.iconst(fromColumnType);
                            asm.iconst(toColumnType);
                            asm.invokeStatic(truncateGeoHashTypes);
                            asm.l2i();
                            asm.invokeInterface(wPutInt, 2);
                            break;
                        default:
                            if (fromColumnType != toColumnType && fromColumnType != ColumnType.NULL && fromColumnType != ColumnType.GEOLONG) {
                                asm.iconst(fromColumnType);
                                asm.iconst(toColumnType);
                                asm.invokeStatic(truncateGeoHashTypes);
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

    public static boolean builtInFunctionCast(int toType, int fromType) {
        // This method returns true when a cast is not needed from type to type
        // because of the way typed functions are implemented.
        // For example IntFunction has getDouble() method implemented and does not need
        // additional wrap function to CAST to double.
        // This is usually case for widening conversions.
        return (fromType >= ColumnType.BYTE
                && toType >= ColumnType.BYTE
                && toType <= ColumnType.DOUBLE
                && fromType < toType)
                || fromType == ColumnType.NULL
                // char can be short and short can be char for symmetry
                || (fromType == ColumnType.CHAR && toType == ColumnType.SHORT)
                || (fromType == ColumnType.TIMESTAMP && toType == ColumnType.LONG);
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

    public static boolean isAssignableFrom(int to, int from) {
        final int toTag = ColumnType.tagOf(to);
        final int fromTag = ColumnType.tagOf(from);
        return (toTag == fromTag && (ColumnType.getGeoHashBits(to) <= ColumnType.getGeoHashBits(from)
                || ColumnType.getGeoHashBits(from) == 0) /* to account for typed NULL assignment */)
                // widening conversions,
                || builtInFunctionCast(to, from)
                //narrowing conversions
                || (fromTag == ColumnType.DOUBLE && (toTag == ColumnType.FLOAT || (toTag >= ColumnType.BYTE && toTag <= ColumnType.LONG)))
                || (fromTag == ColumnType.FLOAT && toTag >= ColumnType.BYTE && toTag <= ColumnType.LONG)
                || (fromTag == ColumnType.LONG && toTag >= ColumnType.BYTE && toTag <= ColumnType.INT)
                || (fromTag == ColumnType.INT && toTag >= ColumnType.BYTE && toTag <= ColumnType.SHORT)
                || (fromTag == ColumnType.SHORT && toTag == ColumnType.BYTE)
                //end of narrowing conversions
                || (fromTag == ColumnType.STRING && toTag == ColumnType.GEOBYTE)
                || (fromTag == ColumnType.CHAR && toTag == ColumnType.GEOBYTE && ColumnType.getGeoHashBits(to) < 6)
                || (fromTag == ColumnType.STRING && toTag == ColumnType.GEOSHORT)
                || (fromTag == ColumnType.STRING && toTag == ColumnType.GEOINT)
                || (fromTag == ColumnType.STRING && toTag == ColumnType.GEOLONG)
                || (fromTag == ColumnType.GEOLONG && toTag == ColumnType.GEOINT)
                || (fromTag == ColumnType.GEOLONG && toTag == ColumnType.GEOSHORT)
                || (fromTag == ColumnType.GEOLONG && toTag == ColumnType.GEOBYTE)
                || (fromTag == ColumnType.GEOINT && toTag == ColumnType.GEOSHORT)
                || (fromTag == ColumnType.GEOINT && toTag == ColumnType.GEOBYTE)
                || (fromTag == ColumnType.GEOSHORT && toTag == ColumnType.GEOBYTE)
                || (fromTag == ColumnType.STRING && toTag == ColumnType.SYMBOL)
                || (fromTag == ColumnType.SYMBOL && toTag == ColumnType.STRING)
                || (fromTag == ColumnType.CHAR && toTag == ColumnType.SYMBOL)
                || (fromTag == ColumnType.CHAR && toTag == ColumnType.STRING)
                || (fromTag == ColumnType.STRING && toTag == ColumnType.TIMESTAMP)
                || (fromTag == ColumnType.SYMBOL && toTag == ColumnType.TIMESTAMP);
    }

    @Override
    public void close() {
        backupAgent.close();
        codeGenerator.close();
        Misc.free(path);
        Misc.free(renamePath);
        Misc.free(textLoader);
    }

    @NotNull
    public CompiledQuery compile(@NotNull CharSequence query, @NotNull SqlExecutionContext executionContext) throws SqlException {
        CompiledQuery result = compile0(query, executionContext);
        if (result.getType() != CompiledQuery.UPDATE || configuration.enableDevelopmentUpdates()) {
            return result;
        }
        throw SqlException.$(0, "UPDATE statement is not supported yet");
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

            batchCallback.preCompile(this);
            clear();//we don't use normal compile here because we can't reset existing lexer
            CompiledQuery current = compileInner(executionContext);
            //We've to move lexer because some query handlers don't consume all tokens (e.g. SET )
            //some code in postCompile might need full text of current query
            CharSequence currentQuery = query.subSequence(position, goToQueryEnd());
            batchCallback.postCompile(this, current, currentQuery);
        }
    }

    public void filterPartitions(
            Function function,
            TableReader reader,
            AlterStatementBuilder changePartitionStatement
    ) {
        // Iterate partitions in descending order so if folders are missing on disk
        // removePartition does not fail to determine next minTimestamp
        // Last partition cannot be dropped, exclude it from the list
        // TODO: allow to drop last partition
        for (int i = reader.getPartitionCount() - 2; i > -1; i--) {
            long partitionTimestamp = reader.getPartitionTimestampByIndex(i);
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

    private static void addCheckDoubleBoundsCall(BytecodeAssembler asm, int checkDoubleBounds, int min, int max, int fromColumnType, int toColumnType, int toColumnIndex) {
        asm.dup2();

        invokeCheckMethod(asm, checkDoubleBounds, min, max, fromColumnType, toColumnType, toColumnIndex);
    }

    private static void addCheckFloatBoundsCall(BytecodeAssembler asm, int checkDoubleBounds, int min, int max, int fromColumnType, int toColumnType, int toColumnIndex) {
        asm.dup();
        asm.f2d();

        invokeCheckMethod(asm, checkDoubleBounds, min, max, fromColumnType, toColumnType, toColumnIndex);
    }

    private static void addCheckLongBoundsCall(BytecodeAssembler asm, int checkLongBounds, int min, int max, int fromColumnType, int toColumnType, int toColumnIndex) {
        asm.dup2();

        invokeCheckMethod(asm, checkLongBounds, min, max, fromColumnType, toColumnType, toColumnIndex);
    }

    private static void addCheckIntBoundsCall(BytecodeAssembler asm, int checkLongBounds, int min, int max, int fromColumnType, int toColumnType, int toColumnIndex) {
        asm.dup();
        asm.i2l();

        invokeCheckMethod(asm, checkLongBounds, min, max, fromColumnType, toColumnType, toColumnIndex);
    }

    private static void invokeCheckMethod(BytecodeAssembler asm, int checkBounds, int min, int max, int fromColumnType, int toColumnType, int toColumnIndex) {
        asm.ldc2_w(min);
        asm.ldc2_w(max);
        asm.iconst(fromColumnType);
        asm.iconst(toColumnType);
        asm.iconst(toColumnIndex);
        asm.invokeStatic(checkBounds);
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

    private static UpdateStatement generateUpdateStatement(
            @Transient QueryModel updateQueryModel,
            @Transient IntList tableColumnTypes,
            @Transient ObjList<CharSequence> tableColumnNames,
            int tableId,
            long tableVersion,
            RecordCursorFactory updateToCursorFactory
    ) throws SqlException {
        try {
            String tableName = updateQueryModel.getUpdateTableName();
            if (!updateToCursorFactory.supportsUpdateRowId(tableName)) {
                throw SqlException.$(updateQueryModel.getModelPosition(), "Only simple UPDATE statements without joins are supported");
            }

            // Check that updateDataFactoryMetadata match types of table to be updated exactly
            RecordMetadata updateDataFactoryMetadata = updateToCursorFactory.getMetadata();

            for (int i = 0, n = updateDataFactoryMetadata.getColumnCount(); i < n; i++) {
                int virtualColumnType = updateDataFactoryMetadata.getColumnType(i);
                CharSequence updateColumnName = updateDataFactoryMetadata.getColumnName(i);
                int tableColumnIndex = tableColumnNames.indexOf(updateColumnName);
                int tableColumnType = tableColumnTypes.get(tableColumnIndex);

                if (virtualColumnType != tableColumnType) {
                    // get column position
                    ExpressionNode setRhs = updateQueryModel.getNestedModel().getColumns().getQuick(i).getAst();
                    int position = setRhs.position;
                    throw SqlException.inconvertibleTypes(position, virtualColumnType, "", tableColumnType, updateColumnName);
                }
            }

            return new UpdateStatement(
                    tableName,
                    tableId,
                    tableVersion,
                    updateToCursorFactory
            );
        } catch (Throwable e) {
            Misc.free(updateToCursorFactory);
            throw e;
        }
    }

    private CompiledQuery alterSystemLockWriter(SqlExecutionContext executionContext) throws SqlException {
        final int tableNamePosition = lexer.getPosition();
        CharSequence tok = GenericLexer.unquote(expectToken(lexer, "table name"));
        tableExistsOrFail(tableNamePosition, tok, executionContext);
        try {
            CharSequence lockedReason = engine.lockWriter(tok, "alterSystem");
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
            engine.unlockWriter(tok);
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
                        return alterTableDropOrAttachPartition(reader, PartitionAction.DROP, executionContext);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'column' or 'partition' expected");
                    }
                } else if (SqlKeywords.isAttachKeyword(tok)) {
                    tok = expectToken(lexer, "'partition'");
                    if (SqlKeywords.isPartitionKeyword(tok)) {
                        return alterTableDropOrAttachPartition(reader, PartitionAction.ATTACH, executionContext);
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
                        tok = expectToken(lexer, "'add index' or 'cache' or 'nocache'");
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
                        } else {
                            if (SqlKeywords.isCacheKeyword(tok)) {
                                return alterTableColumnCacheFlag(tableNamePosition, tableName, columnName, reader, true);
                            } else if (SqlKeywords.isNoCacheKeyword(tok)) {
                                return alterTableColumnCacheFlag(tableNamePosition, tableName, columnName, reader, false);
                            } else {
                                throw SqlException.$(lexer.lastTokenPosition(), "'cache' or 'nocache' expected");
                            }
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
                    throw SqlException.$(lexer.lastTokenPosition(), "'add', 'drop', 'attach', 'set' or 'rename' expected");
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

        AlterStatementBuilder addColumn = alterQueryBuilder.ofAddColumn(
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

            if (!TableUtils.isValidColumnName(columnName)) {
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
        return compiledQuery.ofAlter(alterQueryBuilder.build());
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
                alterQueryBuilder
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
                alterQueryBuilder.ofCacheSymbol(tableNamePosition, tableName, metadata.getId(), columnName).build()
        )
                : compiledQuery.ofAlter(
                alterQueryBuilder.ofRemoveCacheSymbol(tableNamePosition, tableName, metadata.getId(), columnName).build()
        );
    }

    private CompiledQuery alterTableDropColumn(int tableNamePosition, String tableName, TableReaderMetadata metadata) throws SqlException {
        AlterStatementBuilder dropColumnStatement = alterQueryBuilder.ofDropColumn(tableNamePosition, tableName, metadata.getId());
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

        return compiledQuery.ofAlter(alterQueryBuilder.build());
    }

    private CompiledQuery alterTableDropOrAttachPartition(TableReader reader, int action, SqlExecutionContext executionContext)
            throws SqlException {
        final int pos = lexer.lastTokenPosition();
        TableReaderMetadata readerMetadata = reader.getMetadata();
        if (readerMetadata.getPartitionBy() == PartitionBy.NONE) {
            throw SqlException.$(pos, "table is not partitioned");
        }

        String tableName = reader.getTableName();
        final CharSequence tok = expectToken(lexer, "'list' or 'where'");
        if (SqlKeywords.isListKeyword(tok)) {
            return alterTableDropOrAttachPartitionByList(reader, pos, action);
        } else if (SqlKeywords.isWhereKeyword(tok)) {
            if (action != PartitionAction.DROP) {
                throw SqlException.$(pos, "WHERE clause can only be used with DROP PARTITION command");
            }
            AlterStatementBuilder alterPartitionStatement = alterQueryBuilder.ofDropPartition(pos, tableName, reader.getMetadata().getId());
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
                    return compiledQuery.ofAlter(alterQueryBuilder.build());
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

    private CompiledQuery alterTableDropOrAttachPartitionByList(TableReader reader, int pos, int action) throws SqlException {
        String tableName = reader.getTableName();
        AlterStatementBuilder partitions;
        if (action == PartitionAction.DROP) {
            partitions = alterQueryBuilder.ofDropPartition(pos, tableName, reader.getMetadata().getId());
        } else {
            partitions = alterQueryBuilder.ofAttachPartition(pos, tableName, reader.getMetadata().getId());
        }
        assert action == PartitionAction.DROP || action == PartitionAction.ATTACH;
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

        return compiledQuery.ofAlter(alterQueryBuilder.build());
    }

    private CompiledQuery alterTableRenameColumn(int tableNamePosition, String tableName, TableReaderMetadata metadata) throws SqlException {
        AlterStatementBuilder renameColumnStatement = alterQueryBuilder.ofRenameColumn(tableNamePosition, tableName, metadata.getId());
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

            if (!TableUtils.isValidColumnName(tok)) {
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
        return compiledQuery.ofAlter(alterQueryBuilder.build());
    }

    private CompiledQuery alterTableSetParam(CharSequence paramName, CharSequence value, int paramNameNamePosition, String tableName, int tableId) throws SqlException {
        if (isMaxUncommittedRowsParam(paramName)) {
            int maxUncommittedRows;
            try {
                maxUncommittedRows = Numbers.parseInt(value);
            } catch (NumericException e) {
                throw SqlException.$(paramNameNamePosition, "invalid value [value=").put(value).put(",parameter=").put(paramName).put(']');
            }
            if (maxUncommittedRows < 0) {
                throw SqlException.$(paramNameNamePosition, "maxUncommittedRows must be non negative");
            }
            return compiledQuery.ofAlter(alterQueryBuilder.ofSetParamUncommittedRows(tableName, tableId, maxUncommittedRows).build());
        } else if (isCommitLag(paramName)) {
            long commitLag = SqlUtil.expectMicros(value, paramNameNamePosition);
            if (commitLag < 0) {
                throw SqlException.$(paramNameNamePosition, "commitLag must be non negative");
            }
            return compiledQuery.ofAlter(alterQueryBuilder.ofSetParamCommitLag(tableName, tableId, commitLag).build());
        } else {
            throw SqlException.$(paramNameNamePosition, "unknown parameter '").put(paramName).put('\'');
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
        alterQueryBuilder.clear();
        backupAgent.clear();
        functionParser.clear();
    }

    @NotNull
    private CompiledQuery compile0(@NotNull CharSequence query, @NotNull SqlExecutionContext executionContext) throws SqlException {
        clear();
        // these are quick executions that do not require building of a model
        lexer.of(query);
        isSingleQueryMode = true;

        return compileInner(executionContext);
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
            default:
                return model;
        }
    }

    private CompiledQuery compileInner(@NotNull SqlExecutionContext executionContext) throws SqlException {
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
                UpdateStatement updateStatement = generateUpdate(updateQueryModel, executionContext);
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

    private long copyOrdered(TableWriter writer, RecordMetadata metadata, RecordCursor cursor, RecordToRowCopier copier, int cursorTimestampIndex) {
        long rowCount;

        if (ColumnType.isSymbolOrString(metadata.getColumnType(cursorTimestampIndex))) {
            rowCount = copyOrderedStrTimestamp(writer, cursor, copier, cursorTimestampIndex);
        } else {
            rowCount = copyOrdered0(writer, cursor, copier, cursorTimestampIndex);
        }
        writer.commit();

        return rowCount;
    }

    private long copyOrdered0(TableWriter writer, RecordCursor cursor, RecordToRowCopier copier, int cursorTimestampIndex) {
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
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
            long commitLag
    ) {
        long rowCount;
        if (ColumnType.isSymbolOrString(metadata.getColumnType(cursorTimestampIndex))) {
            rowCount = copyOrderedBatchedStrTimestamp(writer, cursor, copier, cursorTimestampIndex, batchSize, commitLag);
        } else {
            rowCount = copyOrderedBatched0(writer, cursor, copier, cursorTimestampIndex, batchSize, commitLag);
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
            long commitLag
    ) {
        long deadline = batchSize;
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
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
            long commitLag
    ) {
        long deadline = batchSize;
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            CharSequence str = record.getStr(cursorTimestampIndex);
            try {
                // It's allowed to insert ISO formatted string to timestamp column
                TableWriter.Row row = writer.newRow(IntervalUtils.parseFloorPartialDate(str));
                copier.copy(record, row);
                row.append();
                if (++rowCount > deadline) {
                    writer.commitWithLag(commitLag);
                    deadline = rowCount + batchSize;
                }
            } catch (NumericException numericException) {
                throw CairoException.instance(0).put("Invalid timestamp: ").put(str);
            }
        }

        return rowCount;
    }

    //returns number of copied rows
    private long copyOrderedStrTimestamp(TableWriter writer, RecordCursor cursor, RecordToRowCopier copier, int cursorTimestampIndex) {
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            final CharSequence str = record.getStr(cursorTimestampIndex);
            try {
                // It's allowed to insert ISO formatted string to timestamp column
                TableWriter.Row row = writer.newRow(IntervalUtils.parseFloorPartialDate(str));
                copier.copy(record, row);
                row.append();
                rowCount++;
            } catch (NumericException numericException) {
                throw CairoException.instance(0).put("Invalid timestamp: ").put(str);
            }
        }

        return rowCount;
    }

    private void copyTable(SqlExecutionContext executionContext, CopyModel model) throws SqlException {
        try {
            int len = configuration.getSqlCopyBufferSize();
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                final CharSequence name = GenericLexer.assertNoDots(GenericLexer.unquote(model.getFileName().token), model.getFileName().position);
                path.of(configuration.getInputRoot()).concat(name).$();
                long fd = ff.openRO(path);
                if (fd == -1) {
                    throw SqlException.$(model.getFileName().position, "could not open file [errno=").put(Os.errno()).put(", path=").put(path).put(']');
                }
                try {
                    long fileLen = ff.length(fd);
                    long n = ff.read(fd, buf, len, 0);
                    if (n > 0) {
                        textLoader.setForceHeaders(model.isHeader());
                        textLoader.setSkipRowsWithExtraValues(false);
                        textLoader.parse(buf, buf + n, executionContext.getCairoSecurityContext());
                        textLoader.setState(TextLoader.LOAD_DATA);
                        int read;
                        while (n < fileLen) {
                            read = (int) ff.read(fd, buf, len, n);
                            if (read < 1) {
                                throw SqlException.$(model.getFileName().position, "could not read file [errno=").put(ff.errno()).put(']');
                            }
                            textLoader.parse(buf, buf + read, executionContext.getCairoSecurityContext());
                            n += read;
                        }
                        textLoader.wrapUp();
                    }
                } finally {
                    ff.close(fd);
                }
            } finally {
                textLoader.clear();
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        } catch (TextException e) {
            // we do not expect JSON exception here
        } finally {
            LOG.info().$("copied").$();
        }
    }

    //sets insertCount to number of copied rows
    private TableWriter copyTableData(CharSequence tableName, RecordCursor cursor, RecordMetadata cursorMetadata) {
        TableWriter writer = new TableWriter(configuration, tableName, messageBus, false, DefaultLifecycleManager.INSTANCE, engine.getMetrics());
        try {
            RecordMetadata writerMetadata = writer.getMetadata();
            entityColumnFilter.of(writerMetadata.getColumnCount());
            RecordToRowCopier recordToRowCopier = assembleRecordToRowCopier(asm, cursorMetadata, writerMetadata, entityColumnFilter);
            this.insertCount = copyTableData(cursor, cursorMetadata, writer, writerMetadata, recordToRowCopier);
            return writer;
        } catch (Throwable e) {
            writer.close();
            throw e;
        }
    }

    /* returns number of copied rows*/
    private long copyTableData(RecordCursor cursor, RecordMetadata metadata, TableWriter writer, RecordMetadata writerMetadata, RecordToRowCopier recordToRowCopier) {
        int timestampIndex = writerMetadata.getTimestampIndex();
        if (timestampIndex == -1) {
            return copyUnordered(cursor, writer, recordToRowCopier);
        } else {
            return copyOrdered(writer, metadata, cursor, recordToRowCopier, timestampIndex);
        }
    }

    //returns number of copied rows
    private long copyUnordered(RecordCursor cursor, TableWriter writer, RecordToRowCopier copier) {
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            TableWriter.Row row = writer.newRow();
            copier.copy(record, row);
            row.append();
            rowCount++;
        }
        writer.commit();

        return rowCount;
    }

    private CompiledQuery createTable(final ExecutionModel model, SqlExecutionContext executionContext) throws SqlException {
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
                        engine.createTableUnsafe(executionContext.getCairoSecurityContext(), mem, path, createTableModel);
                        newTable = true;
                    } else {
                        writer = createTableFromCursor(createTableModel, executionContext);
                    }
                } catch (CairoException e) {
                    LOG.error().$("could not create table [error=").$((Throwable) e).$(']').$();
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

    private TableWriter createTableFromCursor(CreateTableModel model, SqlExecutionContext executionContext) throws SqlException {
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

            try {
                return copyTableData(model.getName().token, cursor, metadata);
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
            throw SqlException
                    .$(tableNamePosition, "table '")
                    .put(tableName)
                    .put("' does not exist");
        }
        engine.remove(executionContext.getCairoSecurityContext(), path, tableName);
        return compiledQuery.ofDrop();
    }

    @NotNull
    private CompiledQuery executeCopy(SqlExecutionContext executionContext, CopyModel executionModel) throws SqlException {
        setupTextLoaderFromModel(executionModel);
        if (Chars.equalsLowerCaseAscii(executionModel.getFileName().token, "stdin")) {
            return compiledQuery.ofCopyRemote(textLoader);
        }
        copyTable(executionContext, executionModel);
        return compiledQuery.ofCopyLocal();
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

    UpdateStatement generateUpdate(QueryModel updateQueryModel, SqlExecutionContext executionContext) throws SqlException {
        // Update QueryModel structure is
        // QueryModel with SET column expressions
        // |-- QueryModel of select-virtual or select-choose of data selected for update
        final QueryModel selectQueryModel = updateQueryModel.getNestedModel();

        // First generate plan for nested SELECT QueryModel
        final RecordCursorFactory updateToCursorFactory = codeGenerator.generate(selectQueryModel, executionContext);

        // And then generate plan for UPDATE top level QueryModel
        final IntList tableColumnTypes = selectQueryModel.getUpdateTableColumnTypes();
        final ObjList<CharSequence> tableColumnNames = selectQueryModel.getUpdateTableColumnNames();
        final int tableId = selectQueryModel.getTableId();
        final long tableVersion = selectQueryModel.getTableVersion();
        return generateUpdateStatement(
                updateQueryModel,
                tableColumnTypes,
                tableColumnNames,
                tableId,
                tableVersion,
                updateToCursorFactory
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
            final InsertStatementImpl insertStatement = new InsertStatementImpl(engine, reader.getTableName(), structureVersion);
            final int writerTimestampIndex = metadata.getTimestampIndex();
            final CharSequenceHashSet columnSet = model.getColumnSet();
            final int columnSetSize = columnSet.size();
            for (int t = 0, n = model.getRowTupleCount(); t < n; t++) {
                Function timestampFunction = null;
                listColumnFilter.clear();
                if (columnSetSize > 0) {
                    valueFunctions = new ObjList<>(columnSetSize);
                    for (int i = 0; i < columnSetSize; i++) {
                        int index = metadata.getColumnIndexQuiet(columnSet.get(i));
                        if (index > -1) {
                            final ExpressionNode node = model.getRowTupleValues(t).getQuick(i);

                            Function function = functionParser.parseFunction(
                                    node,
                                    GenericRecordMetadata.EMPTY,
                                    executionContext
                            );

                            function = validateAndConsume(
                                    model,
                                    t,
                                    valueFunctions,
                                    metadata,
                                    writerTimestampIndex,
                                    i,
                                    index,
                                    function,
                                    node.position,
                                    executionContext.getBindVariableService()
                            );

                            if (writerTimestampIndex == index) {
                                timestampFunction = function;
                            }

                        } else {
                            throw SqlException.invalidColumn(model.getColumnPosition(i), columnSet.get(i));
                        }
                    }
                } else {
                    final int columnCount = metadata.getColumnCount();
                    final ObjList<ExpressionNode> values = model.getRowTupleValues(t);
                    final int valueCount = values.size();
                    if (columnCount != valueCount) {
                        throw SqlException.$(
                                        model.getEndOfRowTupleValuesPosition(t),
                                        "row value count does not match column count [expected=").put(columnCount).put(", actual=").put(values.size())
                                .put(", tuple=").put(t + 1).put(']');
                    }
                    valueFunctions = new ObjList<>(columnCount);

                    for (int i = 0; i < columnCount; i++) {
                        final ExpressionNode node = values.getQuick(i);

                        Function function = functionParser.parseFunction(node, EmptyRecordMetadata.INSTANCE, executionContext);
                        validateAndConsume(
                                model,
                                t,
                                valueFunctions,
                                metadata,
                                writerTimestampIndex,
                                i,
                                i,
                                function,
                                node.position,
                                executionContext.getBindVariableService()
                        );

                        if (writerTimestampIndex == i) {
                            timestampFunction = function;
                        }
                    }
                }

                // validate timestamp
                if (writerTimestampIndex > -1 && (timestampFunction == null || ColumnType.isNull(timestampFunction.getType()))) {
                    throw SqlException.$(0, "insert statement must populate timestamp");
                }

                VirtualRecord record = new VirtualRecord(valueFunctions);
                RecordToRowCopier copier = assembleRecordToRowCopier(asm, record, metadata, listColumnFilter);
                insertStatement.addInsertRow(new InsertRowImpl(record, copier, timestampFunction));
            }
            return compiledQuery.ofInsert(insertStatement);
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

        try (TableWriter writer = engine.getWriter(executionContext.getCairoSecurityContext(), name.token, "insertAsSelect");
             RecordCursorFactory factory = generate(model.getQueryModel(), executionContext)) {

            final RecordMetadata cursorMetadata = factory.getMetadata();
            // Convert sparse writer metadata into dense
            final BaseRecordMetadata writerMetadata = writer.getMetadata().copyDense();
            final int writerTimestampIndex = writerMetadata.getTimestampIndex();
            final int cursorTimestampIndex = cursorMetadata.getTimestampIndex();
            final int cursorColumnCount = cursorMetadata.getColumnCount();

            final RecordToRowCopier copier;
            CharSequenceHashSet columnSet = model.getColumnSet();
            final int columnSetSize = columnSet.size();
            int timestampIndexFound = -1;
            if (columnSetSize > 0) {
                // validate type cast

                // clear list column filter to re-populate it again
                listColumnFilter.clear();

                for (int i = 0; i < columnSetSize; i++) {
                    CharSequence columnName = columnSet.get(i);
                    int index = writerMetadata.getColumnIndexQuiet(columnName);
                    if (index == -1) {
                        throw SqlException.invalidColumn(model.getColumnPosition(i), columnName);
                    }

                    int fromType = cursorMetadata.getColumnType(i);
                    int toType = writerMetadata.getColumnType(index);
                    if (isAssignableFrom(toType, fromType)) {
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

                copier = assembleRecordToRowCopier(asm, cursorMetadata, writerMetadata, listColumnFilter);
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
                    throw SqlException.$(name.position, "designated timestamp of existing table (").put(writerTimestampIndex).put(") does not match designated timestamp in select query (").put(cursorTimestampIndex).put(')');
                }
                timestampIndexFound = writerTimestampIndex;

                final int n = writerMetadata.getColumnCount();
                if (n > cursorMetadata.getColumnCount()) {
                    throw SqlException.$(model.getSelectKeywordPosition(), "not enough columns selected");
                }

                for (int i = 0; i < n; i++) {
                    int fromType = cursorMetadata.getColumnType(i);
                    int toType = writerMetadata.getColumnType(i);
                    if (isAssignableFrom(toType, fromType)) {
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

                copier = assembleRecordToRowCopier(asm, cursorMetadata, writerMetadata, entityColumnFilter);
            }

            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                try {
                    if (writerTimestampIndex == -1) {
                        insertCount = copyUnordered(cursor, writer, copier);
                    } else {
                        if (model.getBatchSize() != -1) {
                            insertCount = copyOrderedBatched(
                                    writer,
                                    factory.getMetadata(),
                                    cursor,
                                    copier,
                                    writerTimestampIndex,
                                    model.getBatchSize(),
                                    model.getCommitLag()
                            );
                        } else {
                            insertCount = copyOrdered(writer, factory.getMetadata(), cursor, copier, timestampIndexFound);
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

    private ExecutionModel lightlyValidateInsertModel(InsertModel model) throws SqlException {
        ExpressionNode tableName = model.getTableName();
        if (tableName.type != ExpressionNode.LITERAL) {
            throw SqlException.$(tableName.position, "literal expected");
        }

        int columnSetSize = model.getColumnSet().size();

        for (int i = 0, n = model.getRowTupleCount(); i < n; i++) {
            if (columnSetSize > 0 && columnSetSize != model.getRowTupleValues(i).size()) {
                throw SqlException.$(
                                model.getEndOfRowTupleValuesPosition(i),
                                "row value count does not match column count [expected=").put(columnSetSize).put(", actual=").put(model.getRowTupleValues(i).size())
                        .put(", tuple=").put(i + 1).put(']');
            }
        }

        return model;
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
        textLoader.configureDestination(model.getTableName().token, false, false, Atomicity.SKIP_ROW, PartitionBy.NONE, null);
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

            if (isTransactionIsolationKeyword(tok)) {
                return compiledQuery.of(new ShowTransactionIsolationLevelCursorFactory());
            }

            if (isMaxIdentifierLengthKeyword(tok)) {
                return compiledQuery.of(new ShowMaxIdentifierLengthCursorFactory());
            }

            if (isStandardConformingStringsKeyword(tok)) {
                return compiledQuery.of(new ShowStandardConformingStringsCursorFactory());
            }

            if (isSearchPath(tok)) {
                return compiledQuery.of(new ShowSearchPathCursorFactory());
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
            throw SqlException.position(lexer.lastTokenPosition()).put('\'').put(tableName).put("' is not a valid table");
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
            throw SqlException.$(position, "table '").put(tableName).put("' does not exist");
        }
    }

    ExecutionModel testCompileModel(CharSequence query, SqlExecutionContext executionContext) throws SqlException {
        clear();
        lexer.of(query);
        return compileExecutionModel(executionContext);
    }

    // this exposed for testing only
    ExpressionNode testParseExpression(CharSequence expression, QueryModel model) throws SqlException {
        clear();
        lexer.of(expression);
        return parser.expr(lexer, model);
    }

    // test only
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
                        LOG.error().$("could truncate [table=").$(writer.getTableName()).$(", e=").$((Sinkable) e).$(']').$();
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
        CharSequence tok = expectToken(lexer, "'partitions'");
        if (isPartitionsKeyword(tok)) {
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
                        return compiledQuery.ofVacuum();
                    }
                    throw SqlException.$(lexer.lastTokenPosition(), "table '").put(tableName).put("' is not partitioned");
                }
            }
            throw SqlException.$(lexer.lastTokenPosition(), "end of line or ';' expected");
        }
        throw SqlException.$(lexer.lastTokenPosition(), "'partitions' expected");
    }

    private Function validateAndConsume(
            InsertModel model,
            int tupleIndex,
            ObjList<Function> valueFunctions,
            RecordMetadata metadata,
            int writerTimestampIndex,
            int bottomUpColumnIndex,
            int metadataColumnIndex,
            Function function,
            int functionPosition,
            BindVariableService bindVariableService
    ) throws SqlException {

        final int columnType = metadata.getColumnType(metadataColumnIndex);
        if (function.isUndefined()) {
            function.assignType(columnType, bindVariableService);
        }

        if (isAssignableFrom(columnType, function.getType())) {
            if (metadataColumnIndex == writerTimestampIndex) {
                return function;
            }
            if (ColumnType.isGeoHash(columnType)) {
                switch (ColumnType.tagOf(function.getType())) {
                    case ColumnType.GEOBYTE:
                    case ColumnType.GEOSHORT:
                    case ColumnType.GEOINT:
                    case ColumnType.GEOLONG:
                        break;
                    case ColumnType.CHAR:
                        function = CHAR_TO_STR_FUNCTION_FACTORY.newInstance(function);
                        // fall through to STRING
                    default:
                        function = CastStrToGeoHashFunctionFactory.newInstance(functionPosition, columnType, function);
                        break;
                }
            }
            valueFunctions.add(function);
            listColumnFilter.add(metadataColumnIndex + 1);
            return function;
        }

        throw SqlException.inconvertibleTypes(
                functionPosition,
                function.getType(),
                model.getRowTupleValues(tupleIndex).getQuick(bottomUpColumnIndex).token,
                metadata.getColumnType(metadataColumnIndex),
                metadata.getColumnName(metadataColumnIndex)
        );
    }

    private InsertModel validateAndOptimiseInsertAsSelect(
            InsertModel model,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final QueryModel queryModel = optimiser.optimise(model.getQueryModel(), executionContext);
        int targetColumnCount = model.getColumnSet().size();
        if (targetColumnCount > 0 && queryModel.getBottomUpColumns().size() != targetColumnCount) {
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
                throw SqlException.$(ccm.getColumnTypePos(),
                        "unsupported cast [from=").put(ColumnType.nameOf(from)).put(",to=").put(ColumnType.nameOf(to)).put(']');
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

    public interface RecordToRowCopier {
        void copy(Record record, TableWriter.Row row);
    }

    public static class RecordToRowCopierUtils {
        private RecordToRowCopierUtils() {
        }

        //used by copier
        @SuppressWarnings("unused")
        static void checkDoubleBounds(double value, double min, double max, int fromType, int toType, int toColumnIndex) throws SqlException {
            if (value < min || value > max) {
                throw SqlException.inconvertibleValue(toColumnIndex, value, fromType, toType);
            }
        }

        //used by copier
        @SuppressWarnings("unused")
        static void checkLongBounds(long value, long min, long max, int fromType, int toType, int toColumnIndex) throws SqlException {
            if (value < min || value > max) {
                throw SqlException.inconvertibleValue(toColumnIndex, value, fromType, toType);
            }
        }
    }

    public final static class PartitionAction {
        public static final int DROP = 1;
        public static final int ATTACH = 2;
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
        public long getColumnHash(int columnIndex) {
            return metadata.getColumnHash(columnIndex);
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return model.getIndexBlockCapacity(columnIndex);
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
        public int getMaxUncommittedRows() {
            return model.getMaxUncommittedRows();
        }

        @Override
        public long getCommitLag() {
            return model.getCommitLag();
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
                    throw CairoException.instance(ff.errno()).put("cannot backup conf file [to=").put(dstPath).put(']');
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
                throw CairoException.instance(ff.errno()).put("cannot backup tab index file [to=").put(dstPath).put(']');
            }
        }

        private void backupTable(@NotNull CharSequence tableName, @NotNull SqlExecutionContext executionContext) {
            LOG.info().$("Starting backup of ").$(tableName).$();
            if (null == cachedTmpBackupRoot) {
                if (null == configuration.getBackupRoot()) {
                    throw CairoException.instance(0).put("Backup is disabled, no backup root directory is configured in the server configuration ['cairo.sql.backup.root' property]");
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
                            recordToRowCopier = assembleRecordToRowCopier(asm, reader.getMetadata(), writerMetadata, entityColumnFilter);
                            tableBackupRowCopiedCache.put(srcPath.toString(), recordToRowCopier);
                        }

                        RecordCursor cursor = reader.getCursor();
                        copyTableData(cursor, reader.getMetadata(), backupWriter, writerMetadata, recordToRowCopier);
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
            } catch (CairoException ex) {
                LOG.info()
                        .$("could not backup [table=").$(tableName)
                        .$(", ex=").$(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .$(']').$();
                srcPath.of(cachedTmpBackupRoot).concat(tableName).slash$();
                int errno;
                if ((errno = ff.rmdir(srcPath)) != 0) {
                    LOG.error().$("could not delete directory [path=").$(srcPath).$(", errno=").$(errno).$(']').$();
                }
                throw ex;
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
                throw CairoException.instance(0).put("Backup dir for table \"").put(tableName).put("\" already exists [dir=").put(srcPath).put(']');
            }

            if (ff.mkdirs(srcPath, mkDirMode) != 0) {
                throw CairoException.instance(ff.errno()).put("Could not create [dir=").put(srcPath).put(']');
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
                throw CairoException.instance(ff.errno()).put(errorMessage).put(dstPath).put(']');
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
                throw CairoException.instance(ff.errno()).put("could not create backup [dir=").put(dstPath).put(']');
            }
            changeDirPrefixLen = dstPath.length();
        }

        private CompiledQuery sqlBackup(SqlExecutionContext executionContext) throws SqlException {
            executionContext.getCairoSecurityContext().checkWritePermission();
            if (null == configuration.getBackupRoot()) {
                throw CairoException.instance(0).put("Backup is disabled, no backup root directory is configured in the server configuration ['cairo.sql.backup.root' property]");
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
                        throw SqlException.position(lexer.lastTokenPosition()).put('\'').put(tableName).put("' is not  a valid table");
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
