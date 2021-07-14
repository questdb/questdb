/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.AppendOnlyVirtualMemory;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.cutlass.text.TextException;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.griffin.engine.functions.catalogue.ShowSearchPathCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowStandardConformingStringsCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowTimeZoneFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowTransactionIsolationLevelCursorFactory;
import io.questdb.griffin.engine.table.ShowColumnsRecordCursorFactory;
import io.questdb.griffin.engine.table.TableListRecordCursorFactory;
import io.questdb.griffin.model.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.ServiceLoader;

import static io.questdb.griffin.SqlKeywords.*;


public class SqlCompiler implements Closeable {
    public static final ObjList<String> sqlControlSymbols = new ObjList<>(8);
    private final static Log LOG = LogFactory.getLog(SqlCompiler.class);
    private static final IntList castGroups = new IntList();
    protected final GenericLexer lexer;
    protected final Path path = new Path();
    protected final CairoEngine engine;
    protected final CharSequenceObjHashMap<KeywordBasedExecutor> keywordBasedExecutors = new CharSequenceObjHashMap<>();
    protected final CompiledQueryImpl compiledQuery = new CompiledQueryImpl();
    private final SqlOptimiser optimiser;
    private final SqlParser parser;
    private final ObjectPool<ExpressionNode> sqlNodePool;
    private final CharacterStore characterStore;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final Path renamePath = new Path();
    private final AppendOnlyVirtualMemory mem = new AppendOnlyVirtualMemory();
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final MessageBus messageBus;
    private final ListColumnFilter listColumnFilter = new ListColumnFilter();
    private final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
    private final IntIntHashMap typeCast = new IntIntHashMap();
    private final ObjList<TableWriter> tableWriters = new ObjList<>();
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final FunctionParser functionParser;
    private final ExecutableMethod insertAsSelectMethod = this::insertAsSelect;
    private final ExecutableMethod createTableMethod = this::createTable;
    private final TextLoader textLoader;
    private final FilesFacade ff;
    private final ObjHashSet<CharSequence> tableNames = new ObjHashSet<>();
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final CharSequenceObjHashMap<RecordToRowCopier> tableBackupRowCopieCache = new CharSequenceObjHashMap<>();
    private transient SqlExecutionContext currentExecutionContext;
    private transient String cachedTmpBackupRoot;
    private final FindVisitor sqlDatabaseBackupOnFind = (file, type) -> {
        nativeLPSZ.of(file);
        if (type == Files.DT_DIR && nativeLPSZ.charAt(0) != '.') {
            try {
                backupTable(nativeLPSZ, currentExecutionContext);
            } catch (CairoException ex) {
                LOG.error()
                        .$("could not backup [path=").$(nativeLPSZ)
                        .$(", ex=").$(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .$(']').$();
            }
        }
    };

    public SqlCompiler(CairoEngine engine) {
        this(engine, engine.getMessageBus(), null);
    }

    public SqlCompiler(CairoEngine engine, @Nullable MessageBus messageBus, @Nullable FunctionFactoryCache functionFactoryCache) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
        this.messageBus = messageBus;
        this.sqlNodePool = new ObjectPool<>(ExpressionNode.FACTORY, configuration.getSqlExpressionPoolCapacity());
        this.queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, configuration.getSqlColumnPoolCapacity());
        this.queryModelPool = new ObjectPool<>(QueryModel.FACTORY, configuration.getSqlModelPoolCapacity());
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
        this.codeGenerator = new SqlCodeGenerator(engine, configuration, functionParser);

        // we have cyclical dependency here
        functionParser.setSqlCodeGenerator(codeGenerator);

        // For each 'this::method' reference java compiles a class
        // We need to minimize repetition of this syntax as each site generates garbage
        final KeywordBasedExecutor compileSet = this::compileSet;
        final KeywordBasedExecutor truncateTables = this::truncateTables;
        final KeywordBasedExecutor alterTable = this::alterTable;
        final KeywordBasedExecutor repairTables = this::repairTables;
        final KeywordBasedExecutor dropTable = this::dropTable;
        final KeywordBasedExecutor sqlBackup = this::sqlBackup;
        final KeywordBasedExecutor sqlShow = this::sqlShow;

        keywordBasedExecutors.put("truncate", truncateTables);
        keywordBasedExecutors.put("TRUNCATE", truncateTables);
        keywordBasedExecutors.put("alter", alterTable);
        keywordBasedExecutors.put("ALTER", alterTable);
        keywordBasedExecutors.put("repair", repairTables);
        keywordBasedExecutors.put("REPAIR", repairTables);
        keywordBasedExecutors.put("set", compileSet);
        keywordBasedExecutors.put("SET", compileSet);
        keywordBasedExecutors.put("begin", compileSet);
        keywordBasedExecutors.put("BEGIN", compileSet);
        keywordBasedExecutors.put("commit", compileSet);
        keywordBasedExecutors.put("COMMIT", compileSet);
        keywordBasedExecutors.put("rollback", compileSet);
        keywordBasedExecutors.put("ROLLBACK", compileSet);
        keywordBasedExecutors.put("discard", compileSet);
        keywordBasedExecutors.put("DISCARD", compileSet);
        keywordBasedExecutors.put("drop", dropTable);
        keywordBasedExecutors.put("DROP", dropTable);
        keywordBasedExecutors.put("backup", sqlBackup);
        keywordBasedExecutors.put("BACKUP", sqlBackup);
        keywordBasedExecutors.put("show", sqlShow);
        keywordBasedExecutors.put("SHOW", sqlShow);

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
        int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        int rGetLong256 = asm.poolInterfaceMethod(Record.class, "getLong256A", "(I)Lio/questdb/std/Long256;");
        int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        int rGetTimestamp = asm.poolInterfaceMethod(Record.class, "getTimestamp", "(I)J");
        //
        int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        int rGetChar = asm.poolInterfaceMethod(Record.class, "getChar", "(I)C");
        int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        int rGetSym = asm.poolInterfaceMethod(Record.class, "getSym", "(I)Ljava/lang/CharSequence;");
        int rGetStr = asm.poolInterfaceMethod(Record.class, "getStr", "(I)Ljava/lang/CharSequence;");
        int rGetBin = asm.poolInterfaceMethod(Record.class, "getBin", "(I)Lio/questdb/std/BinarySequence;");
        //
        int wPutInt = asm.poolMethod(TableWriter.Row.class, "putInt", "(II)V");
        int wPutLong = asm.poolMethod(TableWriter.Row.class, "putLong", "(IJ)V");
        int wPutLong256 = asm.poolMethod(TableWriter.Row.class, "putLong256", "(ILio/questdb/std/Long256;)V");
        int wPutDate = asm.poolMethod(TableWriter.Row.class, "putDate", "(IJ)V");
        int wPutTimestamp = asm.poolMethod(TableWriter.Row.class, "putTimestamp", "(IJ)V");
        //
        int wPutByte = asm.poolMethod(TableWriter.Row.class, "putByte", "(IB)V");
        int wPutShort = asm.poolMethod(TableWriter.Row.class, "putShort", "(IS)V");
        int wPutBool = asm.poolMethod(TableWriter.Row.class, "putBool", "(IZ)V");
        int wPutFloat = asm.poolMethod(TableWriter.Row.class, "putFloat", "(IF)V");
        int wPutDouble = asm.poolMethod(TableWriter.Row.class, "putDouble", "(ID)V");
        int wPutSym = asm.poolMethod(TableWriter.Row.class, "putSym", "(ILjava/lang/CharSequence;)V");
        int wPutSymChar = asm.poolMethod(TableWriter.Row.class, "putSym", "(IC)V");
        int wPutStr = asm.poolMethod(TableWriter.Row.class, "putStr", "(ILjava/lang/CharSequence;)V");
        int wPutTimestampStr = asm.poolMethod(TableWriter.Row.class, "putTimestamp", "(ILjava/lang/CharSequence;)V");
        int wPutStrChar = asm.poolMethod(TableWriter.Row.class, "putStr", "(IC)V");
        int wPutChar = asm.poolMethod(TableWriter.Row.class, "putChar", "(IC)V");
        int wPutBin = asm.poolMethod(TableWriter.Row.class, "putBin", "(ILio/questdb/std/BinarySequence;)V");

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lio/questdb/cairo/sql/Record;Lio/questdb/cairo/TableWriter$Row;)V");

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(0);
        asm.methodCount(2);
        asm.defineDefaultConstructor();

        asm.startMethod(copyNameIndex, copySigIndex, 4, 3);

        int n = toColumnFilter.getColumnCount();
        for (int i = 0; i < n; i++) {

            final int toColumnIndex = toColumnFilter.getColumnIndexFactored(i);
            // do not copy timestamp, it will be copied externally to this helper

            if (toColumnIndex == timestampIndex) {
                continue;
            }

            asm.aload(2);
            asm.iconst(toColumnIndex);
            asm.aload(1);
            asm.iconst(i);

            final int toColumnType = to.getColumnType(toColumnIndex);
            int fromColumnType = from.getColumnType(i);
            if (fromColumnType == ColumnType.NULL) {
                fromColumnType = toColumnType;
            }
            switch (fromColumnType) {
                case ColumnType.INT:
                    asm.invokeInterface(rGetInt, 1);
                    switch (toColumnType) {
                        case ColumnType.LONG:
                            asm.i2l();
                            asm.invokeVirtual(wPutLong);
                            break;
                        case ColumnType.DATE:
                            asm.i2l();
                            asm.invokeVirtual(wPutDate);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.i2l();
                            asm.invokeVirtual(wPutTimestamp);
                            break;
                        case ColumnType.SHORT:
                            asm.i2s();
                            asm.invokeVirtual(wPutShort);
                            break;
                        case ColumnType.BYTE:
                            asm.i2b();
                            asm.invokeVirtual(wPutByte);
                            break;
                        case ColumnType.FLOAT:
                            asm.i2f();
                            asm.invokeVirtual(wPutFloat);
                            break;
                        case ColumnType.DOUBLE:
                            asm.i2d();
                            asm.invokeVirtual(wPutDouble);
                            break;
                        default:
                            asm.invokeVirtual(wPutInt);
                            break;
                    }
                    break;
                case ColumnType.LONG:
                    asm.invokeInterface(rGetLong, 1);
                    switch (toColumnType) {
                        case ColumnType.INT:
                            asm.l2i();
                            asm.invokeVirtual(wPutInt);
                            break;
                        case ColumnType.DATE:
                            asm.invokeVirtual(wPutDate);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeVirtual(wPutTimestamp);
                            break;
                        case ColumnType.SHORT:
                            asm.l2i();
                            asm.i2s();
                            asm.invokeVirtual(wPutShort);
                            break;
                        case ColumnType.BYTE:
                            asm.l2i();
                            asm.i2b();
                            asm.invokeVirtual(wPutByte);
                            break;
                        case ColumnType.FLOAT:
                            asm.l2f();
                            asm.invokeVirtual(wPutFloat);
                            break;
                        case ColumnType.DOUBLE:
                            asm.l2d();
                            asm.invokeVirtual(wPutDouble);
                            break;
                        default:
                            asm.invokeVirtual(wPutLong);
                            break;
                    }
                    break;
                case ColumnType.DATE:
                    asm.invokeInterface(rGetDate, 1);
                    switch (toColumnType) {
                        case ColumnType.INT:
                            asm.l2i();
                            asm.invokeVirtual(wPutInt);
                            break;
                        case ColumnType.LONG:
                            asm.invokeVirtual(wPutLong);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeVirtual(wPutTimestamp);
                            break;
                        case ColumnType.SHORT:
                            asm.l2i();
                            asm.i2s();
                            asm.invokeVirtual(wPutShort);
                            break;
                        case ColumnType.BYTE:
                            asm.l2i();
                            asm.i2b();
                            asm.invokeVirtual(wPutByte);
                            break;
                        case ColumnType.FLOAT:
                            asm.l2f();
                            asm.invokeVirtual(wPutFloat);
                            break;
                        case ColumnType.DOUBLE:
                            asm.l2d();
                            asm.invokeVirtual(wPutDouble);
                            break;
                        default:
                            asm.invokeVirtual(wPutDate);
                            break;
                    }
                    break;
                case ColumnType.TIMESTAMP:
                    asm.invokeInterface(rGetTimestamp, 1);
                    switch (toColumnType) {
                        case ColumnType.INT:
                            asm.l2i();
                            asm.invokeVirtual(wPutInt);
                            break;
                        case ColumnType.LONG:
                            asm.invokeVirtual(wPutLong);
                            break;
                        case ColumnType.SHORT:
                            asm.l2i();
                            asm.i2s();
                            asm.invokeVirtual(wPutShort);
                            break;
                        case ColumnType.BYTE:
                            asm.l2i();
                            asm.i2b();
                            asm.invokeVirtual(wPutByte);
                            break;
                        case ColumnType.FLOAT:
                            asm.l2f();
                            asm.invokeVirtual(wPutFloat);
                            break;
                        case ColumnType.DOUBLE:
                            asm.l2d();
                            asm.invokeVirtual(wPutDouble);
                            break;
                        case ColumnType.DATE:
                            asm.invokeVirtual(wPutDate);
                            break;
                        default:
                            asm.invokeVirtual(wPutTimestamp);
                            break;
                    }
                    break;
                case ColumnType.BYTE:
                    asm.invokeInterface(rGetByte, 1);
                    switch (toColumnType) {
                        case ColumnType.INT:
                            asm.invokeVirtual(wPutInt);
                            break;
                        case ColumnType.LONG:
                            asm.i2l();
                            asm.invokeVirtual(wPutLong);
                            break;
                        case ColumnType.DATE:
                            asm.i2l();
                            asm.invokeVirtual(wPutDate);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.i2l();
                            asm.invokeVirtual(wPutTimestamp);
                            break;
                        case ColumnType.SHORT:
                            asm.i2s();
                            asm.invokeVirtual(wPutShort);
                            break;
                        case ColumnType.FLOAT:
                            asm.i2f();
                            asm.invokeVirtual(wPutFloat);
                            break;
                        case ColumnType.DOUBLE:
                            asm.i2d();
                            asm.invokeVirtual(wPutDouble);
                            break;
                        default:
                            asm.invokeVirtual(wPutByte);
                            break;
                    }
                    break;
                case ColumnType.SHORT:
                    asm.invokeInterface(rGetShort, 1);
                    switch (toColumnType) {
                        case ColumnType.INT:
                            asm.invokeVirtual(wPutInt);
                            break;
                        case ColumnType.LONG:
                            asm.i2l();
                            asm.invokeVirtual(wPutLong);
                            break;
                        case ColumnType.DATE:
                            asm.i2l();
                            asm.invokeVirtual(wPutDate);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.i2l();
                            asm.invokeVirtual(wPutTimestamp);
                            break;
                        case ColumnType.BYTE:
                            asm.i2b();
                            asm.invokeVirtual(wPutByte);
                            break;
                        case ColumnType.FLOAT:
                            asm.i2f();
                            asm.invokeVirtual(wPutFloat);
                            break;
                        case ColumnType.DOUBLE:
                            asm.i2d();
                            asm.invokeVirtual(wPutDouble);
                            break;
                        default:
                            asm.invokeVirtual(wPutShort);
                            break;
                    }
                    break;
                case ColumnType.BOOLEAN:
                    asm.invokeInterface(rGetBool, 1);
                    asm.invokeVirtual(wPutBool);
                    break;
                case ColumnType.FLOAT:
                    asm.invokeInterface(rGetFloat, 1);
                    switch (toColumnType) {
                        case ColumnType.INT:
                            asm.f2i();
                            asm.invokeVirtual(wPutInt);
                            break;
                        case ColumnType.LONG:
                            asm.f2l();
                            asm.invokeVirtual(wPutLong);
                            break;
                        case ColumnType.DATE:
                            asm.f2l();
                            asm.invokeVirtual(wPutDate);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.f2l();
                            asm.invokeVirtual(wPutTimestamp);
                            break;
                        case ColumnType.SHORT:
                            asm.f2i();
                            asm.i2s();
                            asm.invokeVirtual(wPutShort);
                            break;
                        case ColumnType.BYTE:
                            asm.f2i();
                            asm.i2b();
                            asm.invokeVirtual(wPutByte);
                            break;
                        case ColumnType.DOUBLE:
                            asm.f2d();
                            asm.invokeVirtual(wPutDouble);
                            break;
                        default:
                            asm.invokeVirtual(wPutFloat);
                            break;
                    }
                    break;
                case ColumnType.DOUBLE:
                    asm.invokeInterface(rGetDouble, 1);
                    switch (toColumnType) {
                        case ColumnType.INT:
                            asm.d2i();
                            asm.invokeVirtual(wPutInt);
                            break;
                        case ColumnType.LONG:
                            asm.d2l();
                            asm.invokeVirtual(wPutLong);
                            break;
                        case ColumnType.DATE:
                            asm.d2l();
                            asm.invokeVirtual(wPutDate);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.d2l();
                            asm.invokeVirtual(wPutTimestamp);
                            break;
                        case ColumnType.SHORT:
                            asm.d2i();
                            asm.i2s();
                            asm.invokeVirtual(wPutShort);
                            break;
                        case ColumnType.BYTE:
                            asm.d2i();
                            asm.i2b();
                            asm.invokeVirtual(wPutByte);
                            break;
                        case ColumnType.FLOAT:
                            asm.d2f();
                            asm.invokeVirtual(wPutFloat);
                            break;
                        default:
                            asm.invokeVirtual(wPutDouble);
                            break;
                    }
                    break;
                case ColumnType.CHAR:
                    asm.invokeInterface(rGetChar, 1);
                    switch (toColumnType) {
                        case ColumnType.STRING:
                            asm.invokeVirtual(wPutStrChar);
                            break;
                        case ColumnType.SYMBOL:
                            asm.invokeVirtual(wPutSymChar);
                            break;
                        default:
                            asm.invokeVirtual(wPutChar);
                            break;
                    }
                    break;
                case ColumnType.SYMBOL:
                    asm.invokeInterface(rGetSym, 1);
                    if (toColumnType == ColumnType.STRING) {
                        asm.invokeVirtual(wPutStr);
                    } else {
                        asm.invokeVirtual(wPutSym);
                    }
                    break;
                case ColumnType.STRING:
                    asm.invokeInterface(rGetStr, 1);
                    switch (toColumnType) {
                        case ColumnType.SYMBOL:
                            asm.invokeVirtual(wPutSym);
                            break;
                        case ColumnType.TIMESTAMP:
                            asm.invokeVirtual(wPutTimestampStr);
                            break;
                        default:
                            asm.invokeVirtual(wPutStr);
                            break;
                    }
                    break;
                case ColumnType.BINARY:
                    asm.invokeInterface(rGetBin, 1);
                    asm.invokeVirtual(wPutBin);
                    break;
                case ColumnType.LONG256:
                    asm.invokeInterface(rGetLong256, 1);
                    asm.invokeVirtual(wPutLong256);
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
        return to == from
                || from == ColumnType.NULL
                || (from >= ColumnType.BYTE
                && to >= ColumnType.BYTE
                && to <= ColumnType.DOUBLE
                && from < to)
                || (from == ColumnType.STRING && to == ColumnType.SYMBOL)
                || (from == ColumnType.SYMBOL && to == ColumnType.STRING)
                || (from == ColumnType.CHAR && to == ColumnType.SYMBOL)
                || (from == ColumnType.CHAR && to == ColumnType.STRING)
                || (from == ColumnType.STRING && to == ColumnType.TIMESTAMP)
                || (from == ColumnType.SYMBOL && to == ColumnType.TIMESTAMP);
    }

    @Override
    public void close() {
        assert null == currentExecutionContext;
        assert tableNames.isEmpty();
        Misc.free(path);
        Misc.free(renamePath);
        Misc.free(textLoader);
    }

    @NotNull
    public CompiledQuery compile(@NotNull CharSequence query, @NotNull SqlExecutionContext executionContext) throws SqlException {
        clear();
        //
        // these are quick executions that do not require building of a model
        //
        lexer.of(query);

        final CharSequence tok = SqlUtil.fetchNext(lexer);

        if (tok == null) {
            throw SqlException.$(0, "empty query");
        }

        final KeywordBasedExecutor executor = keywordBasedExecutors.get(tok);
        if (executor == null) {
            return compileUsingModel(executionContext);
        }
        return executor.execute(executionContext);
    }

    public CairoEngine getEngine() {
        return engine;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return functionParser.getFunctionFactoryCache();
    }

    private static boolean isCompatibleCase(int from, int to) {
        return castGroups.getQuick(from) == castGroups.getQuick(to);
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

    private void alterSystemLockWriter(SqlExecutionContext executionContext) throws SqlException {
        final int tableNamePosition = lexer.getPosition();
        CharSequence tok = GenericLexer.unquote(expectToken(lexer, "table name"));
        tableExistsOrFail(tableNamePosition, tok, executionContext);
        try {
            CharSequence lockedReason = engine.lockWriter(tok, "alterSystem");
            if (null != lockedReason) {
                throw SqlException.$(tableNamePosition, "could not lock, busy [table=`").put(tok).put(", lockedReason=").put(lockedReason).put("`]");
            }
        } catch (CairoException e) {
            throw SqlException.position(tableNamePosition)
                    .put(e.getFlyweightMessage())
                    .put("[errno=").put(e.getErrno()).put(']');
        }
    }

    private void alterSystemUnlockWriter(SqlExecutionContext executionContext) throws SqlException {
        final int tableNamePosition = lexer.getPosition();
        CharSequence tok = GenericLexer.unquote(expectToken(lexer, "table name"));
        tableExistsOrFail(tableNamePosition, tok, executionContext);
        try {
            engine.unlockWriter(tok);
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

            CharSequence tableName = GenericLexer.immutableOf(tok);
            try (TableWriter writer = engine.getWriter(executionContext.getCairoSecurityContext(), tableName, "alterTable")) {

                tok = expectToken(lexer, "'add', 'alter' or 'drop'");

                if (SqlKeywords.isAddKeyword(tok)) {
                    alterTableAddColumn(tableNamePosition, writer);
                } else if (SqlKeywords.isDropKeyword(tok)) {
                    tok = expectToken(lexer, "'column' or 'partition'");
                    if (SqlKeywords.isColumnKeyword(tok)) {
                        alterTableDropColumn(tableNamePosition, writer);
                    } else if (SqlKeywords.isPartitionKeyword(tok)) {
                        alterTableDropOrAttachPartition(writer, PartitionAction.DROP, executionContext);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'column' or 'partition' expected");
                    }
                } else if (SqlKeywords.isAttachKeyword(tok)) {
                    tok = expectToken(lexer, "'partition'");
                    if (SqlKeywords.isPartitionKeyword(tok)) {
                        alterTableDropOrAttachPartition(writer, PartitionAction.ATTACH, executionContext);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "'column' or 'partition' expected");
                    }
                } else if (SqlKeywords.isRenameKeyword(tok)) {
                    tok = expectToken(lexer, "'column'");
                    if (SqlKeywords.isColumnKeyword(tok)) {
                        alterTableRenameColumn(tableNamePosition, writer);
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
                            alterTableColumnAddIndex(tableNamePosition, columnNameNamePosition, columnName, writer);
                        } else {
                            if (SqlKeywords.isCacheKeyword(tok)) {
                                alterTableColumnCacheFlag(tableNamePosition, columnName, writer, true);
                            } else if (SqlKeywords.isNoCacheKeyword(tok)) {
                                alterTableColumnCacheFlag(tableNamePosition, columnName, writer, false);
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
                            alterTableSetParam(paramName, value, paramNameNamePosition, writer);
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
                LOG.info().$("could not alter table [table=").$(tableName).$(", ex=").$((Sinkable) e).$();
                throw SqlException.$(tableNamePosition, "table '").put(tableName).put("' could not be altered: ").put(e);
            }
        } else if (SqlKeywords.isSystemKeyword(tok)) {
            tok = expectToken(lexer, "'lock' or 'unlock'");

            if (SqlKeywords.isLockKeyword(tok)) {
                tok = expectToken(lexer, "'writer'");

                if (SqlKeywords.isWriterKeyword(tok)) {
                    alterSystemLockWriter(executionContext);
                }
            } else if (SqlKeywords.isUnlockKeyword(tok)) {
                tok = expectToken(lexer, "'writer'");

                if (SqlKeywords.isWriterKeyword(tok)) {
                    alterSystemUnlockWriter(executionContext);
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'lock' or 'unlock' expected");
            }
        } else {
            throw SqlException.$(lexer.lastTokenPosition(), "'table' or 'system' expected");
        }
        return compiledQuery.ofAlter();
    }

    private void alterTableSetParam(CharSequence paramName, CharSequence value, int paramNameNamePosition, TableWriter writer) throws SqlException {
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
            writer.setMetaMaxUncommittedRows(maxUncommittedRows);
        } else if (isCommitLag(paramName)) {
            long commitLag = SqlUtil.expectMicros(value, paramNameNamePosition);
            if (commitLag < 0) {
                throw SqlException.$(paramNameNamePosition, "commitLag must be non negative");
            }
            writer.setMetaCommitLag(commitLag);
        } else {
            throw SqlException.$(paramNameNamePosition, "unknown parameter '").put(paramName).put('\'');
        }
    }

    private void alterTableAddColumn(int tableNamePosition, TableWriter writer) throws SqlException {
        // add columns to table

        CharSequence tok = SqlUtil.fetchNext(lexer);
        //ignoring `column`
        if (tok != null && !SqlKeywords.isColumnKeyword(tok)) {
            lexer.unparse();
        }

        do {
            tok = expectToken(lexer, "'column' or column name");

            int index = writer.getMetadata().getColumnIndexQuiet(tok);
            if (index != -1) {
                throw SqlException.$(lexer.lastTokenPosition(), "column '").put(tok).put("' already exists");
            }

            CharSequence columnName = GenericLexer.immutableOf(GenericLexer.unquote(tok));

            if (!TableUtils.isValidColumnName(columnName)) {
                throw SqlException.$(lexer.lastTokenPosition(), " new column name contains invalid characters");
            }

            tok = expectToken(lexer, "column type");

            int type = ColumnType.columnTypeOf(tok);
            if (type == -1) {
                throw SqlException.$(lexer.lastTokenPosition(), "invalid type");
            }

            tok = SqlUtil.fetchNext(lexer);

            final int indexValueBlockCapacity;
            final boolean cache;
            int symbolCapacity;
            final boolean indexed;

            if (type == ColumnType.SYMBOL && tok != null && !Chars.equals(tok, ',')) {

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
            } else {

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

            try {
                writer.addColumn(
                        columnName,
                        type,
                        Numbers.ceilPow2(symbolCapacity),
                        cache, indexed,
                        Numbers.ceilPow2(indexValueBlockCapacity),
                        false
                );
            } catch (CairoException e) {
                LOG.error().$("Cannot add column '").$(writer.getTableName()).$('.').$(columnName).$("'. Exception: ").$((Sinkable) e).$();
                throw SqlException.$(tableNamePosition, "could add column [error=").put(e.getFlyweightMessage())
                        .put(", errno=").put(e.getErrno())
                        .put(']');
            }

            if (tok == null) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
            }

        } while (true);
    }

    private void alterTableColumnAddIndex(int tableNamePosition, int columnNamePosition, CharSequence columnName, TableWriter w) throws SqlException {
        try {
            if (w.getMetadata().getColumnIndexQuiet(columnName) == -1) {
                throw SqlException.invalidColumn(columnNamePosition, columnName);
            }
            w.addIndex(columnName, configuration.getIndexValueBlockSize());
        } catch (CairoException e) {
            throw SqlException.position(tableNamePosition).put(e.getFlyweightMessage())
                    .put("[errno=").put(e.getErrno()).put(']');
        }
    }

    private void alterTableColumnCacheFlag(int tableNamePosition, CharSequence columnName, TableWriter writer, boolean cache) throws SqlException {
        try {
            RecordMetadata metadata = writer.getMetadata();

            int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex == -1) {
                throw SqlException.invalidColumn(lexer.lastTokenPosition(), columnName);
            }

            if (metadata.getColumnType(columnIndex) != ColumnType.SYMBOL) {
                throw SqlException.$(lexer.lastTokenPosition(), "Invalid column type - Column should be of type symbol");
            }

            writer.changeCacheFlag(columnIndex, cache);
        } catch (CairoException e) {
            throw SqlException.position(tableNamePosition).put(e.getFlyweightMessage())
                    .put("[errno=").put(e.getErrno()).put(']');
        }
    }

    private void alterTableDropColumn(int tableNamePosition, TableWriter writer) throws SqlException {
        RecordMetadata metadata = writer.getMetadata();

        do {
            CharSequence tok = GenericLexer.unquote(expectToken(lexer, "column name"));

            if (metadata.getColumnIndexQuiet(tok) == -1) {
                throw SqlException.invalidColumn(lexer.lastTokenPosition(), tok);
            }

            try {
                writer.removeColumn(tok);
            } catch (CairoException e) {
                LOG.error().$("cannot drop column '").$(writer.getTableName()).$('.').$(tok).$("'. Exception: ").$((Sinkable) e).$();
                throw SqlException.$(tableNamePosition, "cannot drop column. Try again later [errno=").put(e.getErrno()).put(']');
            }

            tok = SqlUtil.fetchNext(lexer);

            if (tok == null) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
            }
        } while (true);
    }

    private void alterTableDropOrAttachPartition(TableWriter writer, int action, SqlExecutionContext executionContext) throws SqlException {
        final int pos = lexer.lastTokenPosition();
        final CharSequence tok = expectToken(lexer, "'list' or 'where'");
        if (SqlKeywords.isListKeyword(tok)) {
            alterTableDropOrAttachPartitionByList(writer, action);
        } else if (SqlKeywords.isWhereKeyword(tok)) {
            ExpressionNode expr = parser.expr(lexer, (QueryModel) null);
            String designatedTimestampColumnName = writer.getDesignatedTimestampColumnName();
            if (designatedTimestampColumnName != null) {
                GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata(designatedTimestampColumnName, ColumnType.TIMESTAMP, null));
                Function function = functionParser.parseFunction(expr, metadata, currentExecutionContext);
                if (function != null && function.getType() == ColumnType.BOOLEAN) {
                    function.init(null, executionContext);
                    writer.removePartition(function, pos);
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

    private void alterTableDropOrAttachPartitionByList(TableWriter writer, int action) throws SqlException {
        do {
            CharSequence tok = expectToken(lexer, "partition name");
            if (Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "partition name missing");
            }
            final CharSequence unquoted = GenericLexer.unquote(tok);

            final long timestamp;
            try {
                timestamp = writer.partitionNameToTimestamp(unquoted);
            } catch (CairoException e) {
                throw SqlException.$(lexer.lastTokenPosition(), e.getFlyweightMessage())
                        .put("[errno=").put(e.getErrno()).put(']');
            }

            switch (action) {
                case PartitionAction.DROP:
                    if (!writer.removePartition(timestamp)) {
                        throw SqlException.$(lexer.lastTokenPosition(), "could not remove partition '").put(unquoted).put('\'');
                    }
                    break;
                case PartitionAction.ATTACH:
                    int statusCode = writer.attachPartition(timestamp);
                    switch (statusCode) {
                        case StatusCode.OK:
                            break;
                        case StatusCode.CANNOT_ATTACH_MISSING_PARTITION:
                            throw SqlException.$(lexer.lastTokenPosition(), "attach partition failed, folder '").put(unquoted).put("' does not exist");
                        case StatusCode.TABLE_HAS_SYMBOLS:
                            throw SqlException.$(lexer.lastTokenPosition(), "attaching partitions to tables with symbol columns not supported");
                        case StatusCode.PARTITION_EMPTY:
                            throw SqlException.$(lexer.lastTokenPosition(), "failed to attach partition '").put(unquoted).put("', data does not correspond to the partition folder or partition is empty");
                        case StatusCode.PARTITION_ALREADY_ATTACHED:
                            throw SqlException.$(lexer.lastTokenPosition(), "failed to attach partition '").put(unquoted).put("', partition already attached to the table");
                        default:
                            throw SqlException.$(lexer.lastTokenPosition(), "attach partition '").put(unquoted).put("', failed with error ").put(statusCode);
                    }
                    break;
                default:
                    throw SqlException.$(lexer.lastTokenPosition(), "unsupported partition action");
            }

            tok = SqlUtil.fetchNext(lexer);

            if (tok == null || Chars.equals(tok, ';')) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
            }
        } while (true);
    }

    private void alterTableRenameColumn(int tableNamePosition, TableWriter writer) throws SqlException {
        RecordMetadata metadata = writer.getMetadata();

        do {
            CharSequence tok = GenericLexer.unquote(expectToken(lexer, "current column name"));
            if (metadata.getColumnIndexQuiet(tok) == -1) {
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
            try {
                writer.renameColumn(existingName, newName);
            } catch (CairoException e) {
                LOG.error().$("cannot rename column '").$(writer.getTableName()).$('.').$(tok).$("'. Exception: ").$((Sinkable) e).$();
                throw SqlException.$(tableNamePosition, "cannot rename column. Try again later [errno=").put(e.getErrno()).put(']');
            }

            tok = SqlUtil.fetchNext(lexer);

            if (tok == null) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
            }
        } while (true);
    }

    private void backupTable(@NotNull CharSequence tableName, @NotNull SqlExecutionContext executionContext) {
        LOG.info().$("Starting backup of ").$(tableName).$();
        if (null == cachedTmpBackupRoot) {
            if (null == configuration.getBackupRoot()) {
                throw CairoException.instance(0).put("Backup is disabled, no backup root directory is configured in the server configuration ['cairo.sql.backup.root' property]");
            }
            path.of(configuration.getBackupRoot()).concat(configuration.getBackupTempDirName()).slash$();
            cachedTmpBackupRoot = Chars.toString(path);
        }

        int renameRootLen = renamePath.length();
        try {
            CairoSecurityContext securityContext = executionContext.getCairoSecurityContext();
            try (TableReader reader = engine.getReader(securityContext, tableName)) {
                cloneMetaData(tableName, cachedTmpBackupRoot, configuration.getBackupMkDirMode(), reader);
                try (TableWriter backupWriter = engine.getBackupWriter(securityContext, tableName, cachedTmpBackupRoot)) {
                    RecordMetadata writerMetadata = backupWriter.getMetadata();
                    path.of(tableName).slash().put(reader.getVersion()).$();
                    RecordToRowCopier recordToRowCopier = tableBackupRowCopieCache.get(path);
                    if (null == recordToRowCopier) {
                        entityColumnFilter.of(writerMetadata.getColumnCount());
                        recordToRowCopier = assembleRecordToRowCopier(asm, reader.getMetadata(), writerMetadata, entityColumnFilter);
                        tableBackupRowCopieCache.put(path.toString(), recordToRowCopier);
                    }

                    RecordCursor cursor = reader.getCursor();
                    copyTableData(cursor, reader.getMetadata(), backupWriter, writerMetadata, recordToRowCopier);
                    backupWriter.commit();
                }
            }

            path.of(configuration.getBackupRoot()).concat(configuration.getBackupTempDirName()).concat(tableName).$();
            try {
                renamePath.trimTo(renameRootLen).concat(tableName).$();
                TableUtils.renameOrFail(ff, path, renamePath);
                LOG.info().$("backup complete [table=").$(tableName).$(", to=").$(renamePath).$(']').$();
            } finally {
                renamePath.trimTo(renameRootLen).$();
            }
        } catch (CairoException ex) {
            LOG.info()
                    .$("could not backup [table=").$(tableName)
                    .$(", ex=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .$(']').$();
            path.of(cachedTmpBackupRoot).concat(tableName).slash$();
            int errno;
            if ((errno = ff.rmdir(path)) != 0) {
                LOG.error().$("could not delete directory [path=").$(path).$(", errno=").$(errno).$(']').$();
            }
            throw ex;
        }
    }

    private void clear() {
        sqlNodePool.clear();
        characterStore.clear();
        queryColumnPool.clear();
        queryModelPool.clear();
        optimiser.clear();
        parser.clear();
    }

    private void cloneMetaData(CharSequence tableName, CharSequence backupRoot, int mkDirMode, TableReader reader) {
        path.of(backupRoot).concat(tableName).slash$();

        if (ff.exists(path)) {
            throw CairoException.instance(0).put("Backup dir for table \"").put(tableName).put("\" already exists [dir=").put(path).put(']');
        }

        if (ff.mkdirs(path, mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("Could not create [dir=").put(path).put(']');
        }

        int rootLen = path.length();

        TableReaderMetadata sourceMetaData = reader.getMetadata();
        try {
            mem.of(ff, path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$(), ff.getPageSize());
            sourceMetaData.cloneTo(mem);

            // create symbol maps
            path.trimTo(rootLen).$();
            int symbolMapCount = 0;
            for (int i = 0, sz = sourceMetaData.getColumnCount(); i < sz; i++) {
                if (sourceMetaData.getColumnType(i) == ColumnType.SYMBOL) {
                    SymbolMapReader mapReader = reader.getSymbolMapReader(i);
                    SymbolMapWriter.createSymbolMapFiles(ff, mem, path, sourceMetaData.getColumnName(i), mapReader.getSymbolCapacity(), mapReader.isCached());
                    symbolMapCount++;
                }
            }
            mem.of(ff, path.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME).$(), ff.getPageSize());
            TableUtils.resetTxn(mem, symbolMapCount, 0L, TableUtils.INITIAL_TXN, 0L);
            path.trimTo(rootLen).concat(TableUtils.TXN_SCOREBOARD_FILE_NAME).$();
        } finally {
            mem.close();
        }
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
            default:
                return model;
        }
    }

    private CompiledQuery compileSet(SqlExecutionContext executionContext) {
        return compiledQuery.ofSet();
    }

    @NotNull
    private CompiledQuery compileUsingModel(SqlExecutionContext executionContext) throws SqlException {
        // This method will not populate sql cache directly;
        // factories are assumed to be non reentrant and once
        // factory is out of this method the caller assumes
        // full ownership over it. In that however caller may
        // chose to return factory back to this or any other
        // instance of compiler for safekeeping

        // lexer would have parsed first token to determine direction of execution flow
        lexer.unparse();
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
            default:
                InsertModel insertModel = (InsertModel) executionModel;
                if (insertModel.getQueryModel() != null) {
                    return executeWithRetries(
                            insertAsSelectMethod,
                            executionModel,
                            configuration.getCreateAsSelectRetryCount(),
                            executionContext
                    );
                }
                return insert(executionModel, executionContext);
        }
    }

    private void copyOrdered(TableWriter writer, RecordMetadata metadata, RecordCursor cursor, RecordToRowCopier copier, int cursorTimestampIndex) {
        int timestampType = metadata.getColumnType(cursorTimestampIndex);
        if (timestampType == ColumnType.STRING || timestampType == ColumnType.SYMBOL) {
            copyOrderedStrTimestamp(writer, cursor, copier, cursorTimestampIndex);
        } else {
            copyOrdered0(writer, cursor, copier, cursorTimestampIndex);
        }
        writer.commit();
    }

    private void copyOrdered0(TableWriter writer, RecordCursor cursor, RecordToRowCopier copier, int cursorTimestampIndex) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            TableWriter.Row row = writer.newRow(record.getTimestamp(cursorTimestampIndex));
            copier.copy(record, row);
            row.append();
        }
    }

    private void copyOrderedBatched(
            TableWriter writer,
            RecordMetadata metadata,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long commitLag
    ) {
        int timestampType = metadata.getColumnType(cursorTimestampIndex);
        if (timestampType == ColumnType.STRING || timestampType == ColumnType.SYMBOL) {
            copyOrderedBatchedStrTimestamp(writer, cursor, copier, cursorTimestampIndex, batchSize, commitLag);
        } else {
            copyOrderedBatched0(writer, cursor, copier, cursorTimestampIndex, batchSize, commitLag);
        }
        writer.commit();
    }

    private void copyOrderedBatched0(
            TableWriter writer,
            RecordCursor cursor,
            RecordToRowCopier copier,
            int cursorTimestampIndex,
            long batchSize,
            long commmitLag
    ) {
        long deadline = batchSize;
        long rowCount = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            TableWriter.Row row = writer.newRow(record.getTimestamp(cursorTimestampIndex));
            copier.copy(record, row);
            row.append();
            if (++rowCount > deadline) {
                writer.commitWithLag(commmitLag);
                deadline = rowCount + batchSize;
            }
        }
    }

    private void copyOrderedBatchedStrTimestamp(
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
    }

    private void copyOrderedStrTimestamp(TableWriter writer, RecordCursor cursor, RecordToRowCopier copier, int cursorTimestampIndex) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            final CharSequence str = record.getStr(cursorTimestampIndex);
            try {
                // It's allowed to insert ISO formatted string to timestamp column
                TableWriter.Row row = writer.newRow(IntervalUtils.parseFloorPartialDate(str));
                copier.copy(record, row);
                row.append();
            } catch (NumericException numericException) {
                throw CairoException.instance(0).put("Invalid timestamp: ").put(str);
            }
        }
    }

    private void copyTable(SqlExecutionContext executionContext, CopyModel model) throws SqlException {
        try {
            int len = configuration.getSqlCopyBufferSize();
            long buf = Unsafe.malloc(len);
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
                Unsafe.free(buf, len);
            }
        } catch (TextException e) {
            // we do not expect JSON exception here
        } finally {
            LOG.info().$("copied").$();
        }
    }

    private TableWriter copyTableData(CharSequence tableName, RecordCursor cursor, RecordMetadata cursorMetadata) {
        TableWriter writer = new TableWriter(configuration, tableName, messageBus, false, DefaultLifecycleManager.INSTANCE);
        try {
            RecordMetadata writerMetadata = writer.getMetadata();
            entityColumnFilter.of(writerMetadata.getColumnCount());
            RecordToRowCopier recordToRowCopier = assembleRecordToRowCopier(asm, cursorMetadata, writerMetadata, entityColumnFilter);
            copyTableData(cursor, cursorMetadata, writer, writerMetadata, recordToRowCopier);
            return writer;
        } catch (Throwable e) {
            writer.close();
            throw e;
        }
    }

    private void copyTableData(RecordCursor cursor, RecordMetadata metadata, TableWriter writer, RecordMetadata writerMetadata, RecordToRowCopier recordToRowCopier) {
        int timestampIndex = writerMetadata.getTimestampIndex();
        if (timestampIndex == -1) {
            copyUnordered(cursor, writer, recordToRowCopier);
        } else {
            copyOrdered(writer, metadata, cursor, recordToRowCopier, timestampIndex);
        }
    }

    private void copyUnordered(RecordCursor cursor, TableWriter writer, RecordToRowCopier copier) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            TableWriter.Row row = writer.newRow();
            copier.copy(record, row);
            row.append();
        }
        writer.commit();
    }

    private CompiledQuery createTable(final ExecutionModel model, SqlExecutionContext executionContext) throws SqlException {
        final CreateTableModel createTableModel = (CreateTableModel) model;
        final ExpressionNode name = createTableModel.getName();

        CharSequence lockedReason = engine.lock(executionContext.getCairoSecurityContext(), name.token, "createTable");
        if (null == lockedReason) {
            TableWriter writer = null;
            boolean newTable = false;
            try {
                if (engine.getStatus(
                        executionContext.getCairoSecurityContext(),
                        path,
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
                    LOG.error().$("could not create table [error=").$((Sinkable) e).$(']').$();
                    throw SqlException.$(name.position, "Could not create table. See log for details.");
                }
            } finally {
                engine.unlock(executionContext.getCairoSecurityContext(), name.token, writer, newTable);
            }
        } else {
            throw SqlException.$(name.position, "cannot acquire table lock [lockedReason=").put(lockedReason).put(']');
        }

        return compiledQuery.ofCreateTable();
    }

    private TableWriter createTableFromCursor(CreateTableModel model, SqlExecutionContext executionContext) throws SqlException {
        try (final RecordCursorFactory factory = generate(model.getQueryModel(), executionContext);
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
        expectKeyword(lexer, "table");
        final int tableNamePosition = lexer.getPosition();

        CharSequence tableName = GenericLexer.unquote(expectToken(lexer, "table name"));
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.$(lexer.lastTokenPosition(), "unexpected token");
        }
        tableExistsOrFail(tableNamePosition, tableName, executionContext);
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

    private CompiledQuery insert(ExecutionModel executionModel, SqlExecutionContext executionContext) throws SqlException {
        final InsertModel model = (InsertModel) executionModel;
        final ExpressionNode name = model.getTableName();
        tableExistsOrFail(name.position, name.token, executionContext);

        ObjList<Function> valueFunctions = null;
        try (TableReader reader = engine.getReader(executionContext.getCairoSecurityContext(), name.token, TableUtils.ANY_TABLE_VERSION)) {
            final long structureVersion = reader.getVersion();
            final RecordMetadata metadata = reader.getMetadata();
            final int writerTimestampIndex = metadata.getTimestampIndex();
            final CharSequenceHashSet columnSet = model.getColumnSet();
            final int columnSetSize = columnSet.size();
            Function timestampFunction = null;
            listColumnFilter.clear();
            if (columnSetSize > 0) {
                listColumnFilter.clear();
                valueFunctions = new ObjList<>(columnSetSize);
                for (int i = 0; i < columnSetSize; i++) {
                    int index = metadata.getColumnIndexQuiet(columnSet.get(i));
                    if (index > -1) {
                        final ExpressionNode node = model.getColumnValues().getQuick(i);

                        final Function function = functionParser.parseFunction(
                                node,
                                GenericRecordMetadata.EMPTY,
                                executionContext
                        );

                        validateAndConsume(
                                model,
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
                final ObjList<ExpressionNode> values = model.getColumnValues();
                final int valueCount = values.size();
                if (columnCount != valueCount) {
                    throw SqlException.$(model.getEndOfValuesPosition(), "not enough values [expected=").put(columnCount).put(", actual=").put(values.size()).put(']');
                }
                valueFunctions = new ObjList<>(columnCount);

                for (int i = 0; i < columnCount; i++) {
                    final ExpressionNode node = values.getQuick(i);

                    Function function = functionParser.parseFunction(node, EmptyRecordMetadata.INSTANCE, executionContext);
                    validateAndConsume(
                            model,
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
            if (writerTimestampIndex > -1 && (timestampFunction == null || timestampFunction.getType() == ColumnType.NULL)) {
                throw SqlException.$(0, "insert statement must populate timestamp");
            }

            VirtualRecord record = new VirtualRecord(valueFunctions);
            RecordToRowCopier copier = assembleRecordToRowCopier(asm, record, metadata, listColumnFilter);
            return compiledQuery.ofInsert(new InsertStatementImpl(engine, Chars.toString(name.token), record, copier, timestampFunction, structureVersion));
        } catch (SqlException e) {
            Misc.freeObjList(valueFunctions);
            throw e;
        }
    }

    private CompiledQuery insertAsSelect(ExecutionModel executionModel, SqlExecutionContext executionContext) throws SqlException {
        final InsertModel model = (InsertModel) executionModel;
        final ExpressionNode name = model.getTableName();
        tableExistsOrFail(name.position, name.token, executionContext);

        try (TableWriter writer = engine.getWriter(executionContext.getCairoSecurityContext(), name.token, "insertAsSelect");
             RecordCursorFactory factory = generate(model.getQueryModel(), executionContext)) {

            final RecordMetadata cursorMetadata = factory.getMetadata();
            final RecordMetadata writerMetadata = writer.getMetadata();
            final int writerTimestampIndex = writerMetadata.getTimestampIndex();
            final int cursorTimestampIndex = cursorMetadata.getTimestampIndex();
            final int cursorColumnCount = cursorMetadata.getColumnCount();

            // fail when target table requires chronological data and cursor cannot provide it
            if (writerTimestampIndex > -1 && cursorTimestampIndex == -1) {
                if (cursorColumnCount <= writerTimestampIndex) {
                    throw SqlException.$(name.position, "select clause must provide timestamp column");
                } else {
                    int columnType = cursorMetadata.getColumnType(writerTimestampIndex);
                    if (columnType != ColumnType.TIMESTAMP && columnType != ColumnType.STRING && columnType != ColumnType.NULL) {
                        throw SqlException.$(name.position, "expected timestamp column but type is ").put(ColumnType.nameOf(columnType));
                    }
                }
            }

            if (writerTimestampIndex > -1 && cursorTimestampIndex > -1 && writerTimestampIndex != cursorTimestampIndex) {
                throw SqlException.$(name.position, "nominated column of existing table (").put(writerTimestampIndex).put(") does not match nominated column in select query (").put(cursorTimestampIndex).put(')');
            }

            final RecordToRowCopier copier;
            CharSequenceHashSet columnSet = model.getColumnSet();
            final int columnSetSize = columnSet.size();
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
                }

                copier = assembleRecordToRowCopier(asm, cursorMetadata, writerMetadata, listColumnFilter);
            } else {

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
                        copyUnordered(cursor, writer, copier);
                    } else {
                        if (model.getBatchSize() != -1) {
                            copyOrderedBatched(
                                    writer,
                                    factory.getMetadata(),
                                    cursor,
                                    copier,
                                    writerTimestampIndex,
                                    model.getBatchSize(),
                                    model.getCommitLag()
                            );
                        } else {
                            copyOrdered(writer, factory.getMetadata(), cursor, copier, writerTimestampIndex);
                        }
                    }
                } catch (Throwable e) {
                    // rollback data when system error occurs
                    writer.rollback();
                    throw e;
                }
            }
        }
        return compiledQuery.ofInsertAsSelect();
    }

    private ExecutionModel lightlyValidateInsertModel(InsertModel model) throws SqlException {
        ExpressionNode tableName = model.getTableName();
        if (tableName.type != ExpressionNode.LITERAL) {
            throw SqlException.$(tableName.position, "literal expected");
        }

        int columnSetSize = model.getColumnSet().size();
        if (columnSetSize > 0 && columnSetSize != model.getColumnValues().size()) {
            throw SqlException.$(model.getColumnPosition(0), "value count does not match column count");
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

            try {
                //opening the writer will attempt to fix/repair the table. The writer is now opened inside migrateNullFlag()
                engine.migrateNullFlag(executionContext.getCairoSecurityContext(), tok);
            } catch (CairoException e) {
                LOG.info().$("table busy [table=").$(tok).$(", e=").$((Sinkable) e).$(']').$();
                throw SqlException.$(lexer.lastTokenPosition(), "table '").put(tok).put("' is busy");
            }
            tok = SqlUtil.fetchNext(lexer);

        } while (tok != null && Chars.equals(tok, ','));
        return compiledQuery.ofRepair();
    }

    void setFullSatJoins(boolean value) {
        codeGenerator.setFullFatJoins(value);
    }

    private void setupBackupRenamePath() {
        DateFormat format = configuration.getBackupDirTimestampFormat();
        long epochMicros = configuration.getMicrosecondClock().getTicks();
        int n = 0;
        // There is a race here, two threads could try and create the same renamePath, only one will succeed the other will throw
        // a CairoException. Maybe it should be serialised
        renamePath.of(configuration.getBackupRoot()).slash();
        int plen = renamePath.length();
        do {
            renamePath.trimTo(plen);
            format.format(epochMicros, configuration.getDefaultDateLocale(), null, renamePath);
            if (n > 0) {
                renamePath.put('.').put(n);
            }
            renamePath.slash$();
            n++;
        } while (ff.exists(renamePath));
        if (ff.mkdirs(renamePath, configuration.getBackupMkDirMode()) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create [dir=").put(renamePath).put(']');
        }
    }

    private void setupTextLoaderFromModel(CopyModel model) {
        textLoader.clear();
        textLoader.setState(TextLoader.ANALYZE_STRUCTURE);
        // todo: configure the following
        //   - what happens when data row errors out, max errors may be?
        //   - we should be able to skip X rows from top, dodgy headers etc.
        textLoader.configureDestination(model.getTableName().token, false, false, Atomicity.SKIP_ROW, PartitionBy.NONE, null);
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
            ff.iterateDir(path.of(configuration.getRoot()).$(), sqlDatabaseBackupOnFind);
            return compiledQuery.ofBackupTable();
        } finally {
            currentExecutionContext = null;
        }
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

    private CompiledQuery sqlTableBackup(SqlExecutionContext executionContext) throws SqlException {
        setupBackupRenamePath();

        try {
            tableNames.clear();
            while (true) {
                CharSequence tok = SqlUtil.fetchNext(lexer);
                if (null == tok) {
                    throw SqlException.position(lexer.getPosition()).put("expected a table name");
                }
                final CharSequence tableName = GenericLexer.assertNoDotsAndSlashes(GenericLexer.unquote(tok), lexer.lastTokenPosition());
                int status = engine.getStatus(executionContext.getCairoSecurityContext(), path, tableName, 0, tableName.length());
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
                        LOG.info().$("table busy [table=").$(tok).$(", e=").$((Sinkable) e).$(']').$();
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

    private void validateAndConsume(
            InsertModel model,
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
                return;
            }
            valueFunctions.add(function);
            listColumnFilter.add(metadataColumnIndex + 1);
            return;
        }

        throw SqlException.inconvertibleTypes(
                functionPosition,
                function.getType(),
                model.getColumnValues().getQuick(bottomUpColumnIndex).token,
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

        if (model.getPartitionBy() != PartitionBy.NONE && model.getTimestampIndex() == -1 && metadata.getTimestampIndex() == -1) {
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
