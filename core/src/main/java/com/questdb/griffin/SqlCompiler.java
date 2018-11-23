/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.model.*;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.ServiceLoader;

import static com.questdb.cairo.TableUtils.META_FILE_NAME;
import static com.questdb.cairo.TableUtils.TXN_FILE_NAME;


public class SqlCompiler implements Closeable {
    public static final ObjList<String> sqlControlSymbols = new ObjList<>(8);
    private final static Log LOG = LogFactory.getLog(SqlCompiler.class);
    private static final IntList castGroups = new IntList();
    private final SqlOptimiser optimiser;
    private final SqlParser parser;
    private final ObjectPool<ExpressionNode> sqlNodePool;
    private final CharacterStore characterStore;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final GenericLexer lexer;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final Path path = new Path();
    private final AppendMemory mem = new AppendMemory();
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final CairoWorkScheduler workScheduler;
    private final CairoEngine engine;
    private final ListColumnFilter listColumnFilter = new ListColumnFilter();
    private final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
    private final IntIntHashMap typeCast = new IntIntHashMap();
    private final ExecutableMethod insertAsSelectMethod = this::insertAsSelect;
    private final ExecutableMethod createTableMethod = this::createTable;
    private final SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl();
    private final AssociativeCache<RecordCursorFactory> sqlCache;
    private final ObjList<TableWriter> tableWriters = new ObjList<>();

    public SqlCompiler(CairoEngine engine, CairoConfiguration configuration) {
        this(engine, configuration, null);
    }

    public SqlCompiler(CairoEngine engine, CairoConfiguration configuration, @Nullable CairoWorkScheduler workScheduler) {
        this.engine = engine;
        this.workScheduler = workScheduler;
        this.sqlNodePool = new ObjectPool<>(ExpressionNode.FACTORY, configuration.getSqlExpressionPoolCapacity());
        this.queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, configuration.getSqlColumnPoolCapacity());
        this.queryModelPool = new ObjectPool<>(QueryModel.FACTORY, configuration.getSqlModelPoolCapacity());
        this.characterStore = new CharacterStore(
                configuration.getSqlCharacterStoreCapacity(),
                configuration.getSqlCharacterStorePoolCapacity()
        );
        this.lexer = new GenericLexer(configuration.getSqlLexerPoolCapacity());
        final FunctionParser functionParser = new FunctionParser(configuration, ServiceLoader.load(FunctionFactory.class));
        this.codeGenerator = new SqlCodeGenerator(engine, configuration, functionParser);
        this.configuration = configuration;

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
                functionParser
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

        this.sqlCache = new AssociativeCache<>(
                configuration.getSqlCacheBlockSize(),
                configuration.getSqlCacheBlockCount()
        );
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

    public void cache(CharSequence query, RecordCursorFactory factory) {
        sqlCache.put(query, factory);
    }

    @Override
    public void close() {
        Misc.free(path);
        Misc.free(sqlCache);
    }

    public RecordCursorFactory compile(CharSequence query, BindVariableService bindVariableService) throws SqlException {

        // these are quick executions that do not require building of a model
        //
        lexer.of(query);
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (Chars.equals(tok, "truncate")) {
            truncateTables(lexer);
            return null;
        }

        if (Chars.equals(tok, "alter")) {
            alterTable(lexer);
            return null;
        }

        // short circuit to cache if there is anything there
        RecordCursorFactory result = sqlCache.poll(query);
        if (result != null) {
            return result;
        }

        return compileUsingModel(query, bindVariableService);
    }

    // Creates data type converter.
    // INT and LONG NaN values are cast to their representation rather than Double or Float NaN.
    private static RecordToRowCopier assembleRecordToRowCopier(BytecodeAssembler asm, RecordMetadata from, RecordMetadata to, ColumnFilter toColumnFilter) {
        int timestampIndex = to.getTimestampIndex();
        asm.init(RecordToRowCopier.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(RecordToRowCopier.class);

        int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        int rGetTimestamp = asm.poolInterfaceMethod(Record.class, "getTimestamp", "(I)J");
        //
        int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        int rGetSym = asm.poolInterfaceMethod(Record.class, "getSym", "(I)Ljava/lang/CharSequence;");
        int rGetStr = asm.poolInterfaceMethod(Record.class, "getStr", "(I)Ljava/lang/CharSequence;");
        int rGetBin = asm.poolInterfaceMethod(Record.class, "getBin", "(I)Lcom/questdb/std/BinarySequence;");
        //
        int wPutInt = asm.poolMethod(TableWriter.Row.class, "putInt", "(II)V");
        int wPutLong = asm.poolMethod(TableWriter.Row.class, "putLong", "(IJ)V");
        int wPutDate = asm.poolMethod(TableWriter.Row.class, "putDate", "(IJ)V");
        int wPutTimestamp = asm.poolMethod(TableWriter.Row.class, "putTimestamp", "(IJ)V");
        //
        int wPutByte = asm.poolMethod(TableWriter.Row.class, "putByte", "(IB)V");
        int wPutShort = asm.poolMethod(TableWriter.Row.class, "putShort", "(IS)V");
        int wPutBool = asm.poolMethod(TableWriter.Row.class, "putBool", "(IZ)V");
        int wPutFloat = asm.poolMethod(TableWriter.Row.class, "putFloat", "(IF)V");
        int wPutDouble = asm.poolMethod(TableWriter.Row.class, "putDouble", "(ID)V");
        int wPutSym = asm.poolMethod(TableWriter.Row.class, "putSym", "(ILjava/lang/CharSequence;)V");
        int wPutStr = asm.poolMethod(TableWriter.Row.class, "putStr", "(ILjava/lang/CharSequence;)V");
        int wPutBin = asm.poolMethod(TableWriter.Row.class, "putBin", "(ILcom/questdb/std/BinarySequence;)V");

        int copyNameIndex = asm.poolUtf8("copy");
        int copySigIndex = asm.poolUtf8("(Lcom/questdb/cairo/sql/Record;Lcom/questdb/cairo/TableWriter$Row;)V");

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

            final int toColumnIndex = toColumnFilter.getColumnIndex(i);
            // do not copy timestamp, it will be copied externally to this helper

            if (toColumnIndex == timestampIndex) {
                continue;
            }

            asm.aload(2);
            asm.iconst(toColumnIndex);
            asm.aload(1);
            asm.iconst(i);


            switch (from.getColumnType(i)) {
                case ColumnType.INT:
                    asm.invokeInterface(rGetInt, 1);
                    switch (to.getColumnType(toColumnIndex)) {
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
                    switch (to.getColumnType(toColumnIndex)) {
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
                    switch (to.getColumnType(toColumnIndex)) {
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
                    switch (to.getColumnType(toColumnIndex)) {
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
                    switch (to.getColumnType(toColumnIndex)) {
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
                    switch (to.getColumnType(toColumnIndex)) {
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
                    switch (to.getColumnType(toColumnIndex)) {
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
                    switch (to.getColumnType(toColumnIndex)) {
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
                case ColumnType.SYMBOL:
                    asm.invokeInterface(rGetSym, 1);
                    switch (to.getColumnType(toColumnIndex)) {
                        case ColumnType.STRING:
                            asm.invokeVirtual(wPutStr);
                            break;
                        default:
                            asm.invokeVirtual(wPutSym);
                            break;
                    }
                    break;
                case ColumnType.STRING:
                    asm.invokeInterface(rGetStr, 1);
                    switch (to.getColumnType(toColumnIndex)) {
                        case ColumnType.SYMBOL:
                            asm.invokeVirtual(wPutSym);
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

    private static boolean isCompatibleCase(int from, int to) {
        return castGroups.getQuick(from) == castGroups.getQuick(to);
    }

    private static boolean isAssignableFrom(int to, int from) {
        return to == from
                || (
                from >= ColumnType.BYTE
                        && from <= ColumnType.DOUBLE
                        && to >= ColumnType.BYTE
                        && to <= ColumnType.DOUBLE
                        && from < to)
                || (from == ColumnType.STRING && to == ColumnType.SYMBOL)
                || (from == ColumnType.SYMBOL && to == ColumnType.STRING);
    }

    private void alterTable(GenericLexer lexer) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);
        if (!Chars.equals("table", tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'table' expected");
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.$(lexer.getPosition(), "table name expected");
        }

        tableExistsOrFail(lexer.lastTokenPosition(), tok);

        try (TableWriter writer = engine.getWriter(tok)) {

            tok = SqlUtil.fetchNext(lexer);
            if (tok == null) {
                throw SqlException.$(lexer.getPosition(), "'add' or 'drop' expected");
            }

            if (Chars.equals("add", tok)) {
                // add columns to table
                tok = SqlUtil.fetchNext(lexer);

                if (tok == null) {
                    throw SqlException.$(lexer.getPosition(), "'column' expected");
                }

                if (!Chars.equals(tok, "column")) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'column' expected");
                }

                do {
                    int tableNamePosition = lexer.getPosition();

                    tok = SqlUtil.fetchNext(lexer);
                    if (tok == null) {
                        throw SqlException.$(lexer.getPosition(), "column name expected");
                    }

                    int index = writer.getMetadata().getColumnIndexQuiet(tok);
                    if (index != -1) {
                        throw SqlException.$(lexer.lastTokenPosition(), "column '").put(tok).put("' already exists");
                    }

                    CharSequence columnName = GenericLexer.immutableOf(tok);

                    tok = SqlUtil.fetchNext(lexer);

                    if (tok == null) {
                        throw SqlException.$(lexer.getPosition(), "column type expected");
                    }

                    int type = ColumnType.columnTypeOf(tok);
                    if (type == -1) {
                        throw SqlException.$(lexer.lastTokenPosition(), "invalid type");
                    }

                    tok = SqlUtil.fetchNext(lexer);

                    if (type == ColumnType.SYMBOL && tok != null && !Chars.equals(tok, ',')) {

                        final int capacity;
                        if (Chars.equals(tok, "capacity")) {
                            tok = SqlUtil.fetchNext(lexer);

                            if (tok == null) {
                                throw SqlException.$(lexer.getPosition(), "symbol capacity expected");
                            }
                            try {
                                capacity = Numbers.parseInt(tok);
                            } catch (NumericException e) {
                                throw SqlException.$(lexer.lastTokenPosition(), "numeric capacity expected");
                            }

                            tok = SqlUtil.fetchNext(lexer);
                        } else {
                            capacity = configuration.getDefaultSymbolCapacity();
                        }

                        final boolean cache;
                        if (Chars.equalsNc("cache", tok)) {
                            cache = true;
                            tok = SqlUtil.fetchNext(lexer);
                        } else if (Chars.equalsNc("nocache", tok)) {
                            cache = false;
                            tok = SqlUtil.fetchNext(lexer);
                        } else {
                            cache = configuration.getDefaultSymbolCacheFlag();
                        }

                        final boolean indexed = Chars.equalsNc("index", tok);
                        if (indexed) {
                            tok = SqlUtil.fetchNext(lexer);
                        }

                        final int indexValueBlockCapacity;
                        if (Chars.equalsNc("capacity", tok)) {
                            tok = SqlUtil.fetchNext(lexer);
                            if (tok == null) {
                                throw SqlException.$(lexer.getPosition(), "symbol capacity expected");
                            }

                            try {
                                indexValueBlockCapacity = Numbers.parseInt(tok);
                            } catch (NumericException e) {
                                throw SqlException.$(lexer.lastTokenPosition(), "numeric capacity expected");
                            }
                            tok = SqlUtil.fetchNext(lexer);
                        } else {
                            indexValueBlockCapacity = configuration.getIndexValueBlockSize();
                        }

                        writer.addColumn(columnName, type, Numbers.ceilPow2(capacity), cache, indexed, Numbers.ceilPow2(indexValueBlockCapacity));
                    } else {

                        try {
                            writer.addColumn(columnName, type);
                        } catch (CairoException e) {
                            throw SqlException.$(tableNamePosition, "Cannot add column. Try again later.");
                        }
                    }

                    if (tok == null) {
                        break;
                    }

                    if (!Chars.equals(tok, ',')) {
                        throw SqlException.$(lexer.lastTokenPosition(), "',' expected");
                    }

                } while (true);
            }
        } catch (CairoException e) {
            LOG.info().$("failed to lock table for alter: ").$((Sinkable) e).$();
            throw SqlException.$(lexer.lastTokenPosition(), "table '").put(tok).put("' is busy");
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

    private ExecutionModel compileExecutionModel(GenericLexer lexer, SqlExecutionContext executionContext) throws SqlException {
        ExecutionModel model = parser.parse(lexer, executionContext);
        switch (model.getModelType()) {
            case ExecutionModel.QUERY:
                return optimiser.optimise((QueryModel) model, executionContext);
            case ExecutionModel.INSERT_AS_SELECT:
                return validateAndOptimiseInsertAsSelect((InsertAsSelectModel) model, executionContext);
            default:
                return model;
        }
    }

    ExecutionModel compileExecutionModel(CharSequence query, SqlExecutionContext executionContext) throws SqlException {
        clear();
        lexer.of(query);
        return compileExecutionModel(lexer, executionContext);
    }

    @Nullable
    private RecordCursorFactory compileUsingModel(CharSequence query, BindVariableService bindVariableService) throws SqlException {
        // This method will not populate sql cache directly;
        // factories are assumed to be non reentrant and once
        // factory is out of this method the caller assumes
        // full ownership over it. In that however caller may
        // chose to return factory back to this or any other
        // instance of compiler for safekeeping

        executionContext.with(bindVariableService);

        ExecutionModel executionModel = compileExecutionModel(query, executionContext);
        switch (executionModel.getModelType()) {
            case ExecutionModel.QUERY:
                return generate((QueryModel) executionModel, executionContext);
            case ExecutionModel.CREATE_TABLE:
                createTableWithRetries(query, executionModel);
                break;
            case ExecutionModel.INSERT_AS_SELECT:
                executeWithRetries(
                        query,
                        insertAsSelectMethod,
                        executionModel,
                        configuration.getCreateAsSelectRetryCount());
                break;
            default:
                break;
        }
        return null;
    }

    private void copyOrdered(TableWriter writer, RecordCursor cursor, RecordToRowCopier copier, int cursorTimestampIndex) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            TableWriter.Row row = writer.newRow(record.getTimestamp(cursorTimestampIndex));
            copier.copy(record, row);
            row.append();
        }
        writer.commit();
    }

    private TableWriter copyTableData(CharSequence tableName, RecordCursor cursor, RecordMetadata cursorMetadata) {
        TableWriter writer = new TableWriter(configuration, tableName, workScheduler, false, DefaultLifecycleManager.INSTANCE);
        try {
            RecordMetadata writerMetadata = writer.getMetadata();
            entityColumnFilter.of(writerMetadata.getColumnCount());
            RecordToRowCopier recordToRowCopier = assembleRecordToRowCopier(asm, cursorMetadata, writerMetadata, entityColumnFilter);

            int timestampIndex = writerMetadata.getTimestampIndex();
            if (timestampIndex == -1) {
                copyUnordered(cursor, writer, recordToRowCopier);
            } else {
                copyOrdered(writer, cursor, recordToRowCopier, timestampIndex);
            }
            return writer;
        } catch (CairoException e) {
            writer.close();
            throw e;
        }
    }

    private void copyUnordered(RecordCursor cursor, TableWriter writer, RecordToRowCopier ccopier) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            TableWriter.Row row = writer.newRow(0);
            ccopier.copy(record, row);
            row.append();
        }
        writer.commit();
    }

    private void createEmptyTable(CreateTableModel model) {
        final FilesFacade ff = configuration.getFilesFacade();
        path.of(configuration.getRoot()).concat(model.getName().token);
        final int rootLen = path.length();

        try (AppendMemory mem = this.mem) {

            mem.of(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());

            int count = model.getColumnCount();
            mem.putInt(count);
            final ExpressionNode partitionBy = model.getPartitionBy();
            if (partitionBy == null) {
                mem.putInt(PartitionBy.NONE);
            } else {
                mem.putInt(PartitionBy.fromString(partitionBy.token));
            }

            final ExpressionNode timestamp = model.getTimestamp();
            if (timestamp == null) {
                mem.putInt(-1);
            } else {
                mem.putInt(model.getColumnIndex(timestamp.token));
            }
            mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < count; i++) {
                mem.putByte((byte) model.getColumnType(i));
                mem.putBool(model.getIndexedFlag(i));
                mem.putInt(model.getIndexBlockCapacity(i));
                mem.skip(10); // reserved
            }
            for (int i = 0; i < count; i++) {
                mem.putStr(model.getColumnName(i));
            }

            // create symbol maps
            int symbolMapCount = 0;
            for (int i = 0; i < count; i++) {
                if (model.getColumnType(i) == ColumnType.SYMBOL) {
                    SymbolMapWriter.createSymbolMapFiles(
                            ff,
                            mem,
                            path.trimTo(rootLen),
                            model.getColumnName(i),
                            model.getSymbolCapacity(i),
                            model.getSymbolCacheFlag(i)
                    );
                    symbolMapCount++;
                }
            }
            mem.of(ff, path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), ff.getPageSize());
            TableUtils.resetTxn(mem, symbolMapCount);
        }
    }

    private void createTable(final ExecutionModel model, SqlExecutionContext executionContext) throws SqlException {
        final CreateTableModel createTableModel = (CreateTableModel) model;
        final FilesFacade ff = configuration.getFilesFacade();
        final ExpressionNode name = createTableModel.getName();

        if (TableUtils.exists(ff, path, configuration.getRoot(), name.token) != TableUtils.TABLE_DOES_NOT_EXIST) {
            throw SqlException.$(name.position, "table already exists");
        }

        if (engine.lock(name.token)) {

            TableWriter writer = null;

            try {
                if (ff.mkdir(path.chopZ().put(Files.SEPARATOR).$(), configuration.getMkDirMode()) != 0) {
                    LOG.error().$("table already exists [path=").utf8(path).$(", errno=").$(ff.errno()).$(']').$();
                    throw SqlException.$(name.position, "Cannot create table. See log for details.");
                }

                try {
                    if (createTableModel.getQueryModel() == null) {
                        createEmptyTable(createTableModel);
                    } else {
                        writer = createTableFromCursor(createTableModel, executionContext);
                    }
                } catch (SqlException e) {
                    removeTableDirectory(createTableModel);
                    throw e;
                } catch (ReaderOutOfDateException e) {
                    if (removeTableDirectory(createTableModel)) {
                        throw e;
                    }
                    throw SqlException.$(0, "Concurrent modification cannot be handled. Failed to clean up. See log for more details.");
                }
            } finally {
                engine.unlock(name.token, writer);
            }
        } else {
            throw SqlException.$(name.position, "cannot acquire table lock");
        }
    }

    private TableWriter createTableFromCursor(CreateTableModel model, SqlExecutionContext executionContext) throws SqlException {
        try (final RecordCursorFactory factory = generate(model.getQueryModel(), executionContext);
             final RecordCursor cursor = factory.getCursor(executionContext.getBindVariableService())) {
            typeCast.clear();
            final RecordMetadata metadata = factory.getMetadata();
            validateTableModelAndCreateTypeCast(model, metadata, typeCast);
            createTableMetaFile(model, metadata, typeCast);
            return copyTableData(model.getName().token, cursor, metadata);
        }
    }

    private void createTableMetaFile(
            CreateTableModel model,
            RecordMetadata metadata,
            @Transient IntIntHashMap typeCast) {
        final FilesFacade ff = configuration.getFilesFacade();
        path.of(configuration.getRoot()).concat(model.getName().token);
        final int rootLen = path.length();

        try (AppendMemory mem = this.mem) {

            mem.of(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());

            int count = model.getColumnCount();
            mem.putInt(count);
            final ExpressionNode partitionBy = model.getPartitionBy();
            if (partitionBy == null) {
                mem.putInt(PartitionBy.NONE);
            } else {
                mem.putInt(PartitionBy.fromString(partitionBy.token));
            }

            ExpressionNode timestamp = model.getTimestamp();
            if (timestamp == null) {
                mem.putInt(-1);
            } else {
                mem.putInt(model.getColumnIndex(timestamp.token));
            }
            mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < count; i++) {
                // use type cast when available
                int castIndex = typeCast.keyIndex(i);
                if (castIndex < 0) {
                    mem.putByte((byte) typeCast.valueAt(castIndex));
                } else {
                    mem.putByte((byte) metadata.getColumnType(i));
                }
                mem.putBool(model.getIndexedFlag(i));
                mem.putInt(model.getIndexBlockCapacity(i));
                mem.skip(10); // reserved
            }

            for (int i = 0; i < count; i++) {
                mem.putStr(model.getColumnName(i));
            }

            // create symbol maps
            int symbolMapCount = 0;
            for (int i = 0; i < count; i++) {

                int columnType;
                int castIndex = typeCast.keyIndex(i);
                if (castIndex < 0) {
                    columnType = typeCast.valueAt(castIndex);
                } else {
                    columnType = metadata.getColumnType(i);
                }

                if (columnType == ColumnType.SYMBOL) {
                    int symbolCapacity = model.getSymbolCapacity(i);
                    if (symbolCapacity == -1) {
                        symbolCapacity = configuration.getDefaultSymbolCapacity();
                    }
                    SymbolMapWriter.createSymbolMapFiles(
                            ff,
                            mem,
                            path.trimTo(rootLen),
                            model.getColumnName(i),
                            symbolCapacity,
                            model.getSymbolCacheFlag(i)
                    );
                    symbolMapCount++;
                }
            }
            mem.of(ff, path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), ff.getPageSize());
            TableUtils.resetTxn(mem, symbolMapCount);
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
     * @param query          SQL text. When concurrent modification is detected SQL text is re-parsed and re-executed
     * @param executionModel created from parsed sql.
     * @throws SqlException contains text of error and error position in SQL text.
     */
    private void createTableWithRetries(CharSequence query, ExecutionModel executionModel) throws SqlException {
        executeWithRetries(query, createTableMethod, executionModel, configuration.getCreateAsSelectRetryCount());
    }

    private void executeWithRetries(
            CharSequence query,
            ExecutableMethod method,
            ExecutionModel executionModel,
            int retries) throws SqlException {
        int attemptsLeft = retries;
        do {
            try {
                method.execute(executionModel, executionContext);
                break;
            } catch (ReaderOutOfDateException e) {
                attemptsLeft--;
                executionModel = compileExecutionModel(query, executionContext);
            }
        } while (attemptsLeft > 0);

        if (attemptsLeft < 1) {
            throw SqlException.position(0).put("underlying cursor is extremely volatile");
        }
    }

    RecordCursorFactory generate(QueryModel queryModel, SqlExecutionContext executionContext) throws SqlException {
        return codeGenerator.generate(queryModel, executionContext);
    }

    SqlCodeGenerator getCodeGenerator() {
        return codeGenerator;
    }

    private void insertAsSelect(ExecutionModel executionModel, SqlExecutionContext executionContext) throws SqlException {
        final InsertAsSelectModel model = (InsertAsSelectModel) executionModel;
        final ExpressionNode name = model.getTableName();
        tableExistsOrFail(name.position, name.token);

        try (TableWriter writer = engine.getWriter(name.token);
             RecordCursorFactory factory = generate(model.getQueryModel(), executionContext)) {

            final RecordMetadata cursorMetadata = factory.getMetadata();
            final RecordMetadata writerMetadata = writer.getMetadata();
            final int writerTimestampIndex = writerMetadata.getTimestampIndex();
            final int cursorTimestampIndex = cursorMetadata.getTimestampIndex();

            // fail when target table requires chronological data and cursor cannot provide it
            if (writerTimestampIndex > -1 && cursorTimestampIndex == -1) {
                throw SqlException.$(name.position, "select clause must provide timestamp column");
            }

            final RecordToRowCopier copier;

            boolean noTimestampColumn = true;

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

                    if (index == writerTimestampIndex) {
                        noTimestampColumn = false;
                    }

                    int fromType = cursorMetadata.getColumnType(i);
                    int toType = writerMetadata.getColumnType(index);
                    if (isAssignableFrom(toType, fromType)) {
                        listColumnFilter.add(index);
                    } else {
                        throw SqlException.$(model.getColumnPosition(i), "inconvertible types: ").put(ColumnType.nameOf(fromType)).put(" -> ").put(ColumnType.nameOf(toType));
                    }
                }

                // fail when target table requires chronological data and timestamp column
                // is not in the list of columns to be updated
                if (writerTimestampIndex > -1 && noTimestampColumn) {
                    throw SqlException.$(model.getColumnPosition(0), "column list must include timestamp");
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
                    assert i < model.getQueryModel().getColumns().size();
                    throw SqlException.$(model.getQueryModel().getColumns().getQuick(i).getAst().position, "inconvertible types: ").put(ColumnType.nameOf(fromType)).put(" -> ").put(ColumnType.nameOf(toType));
                }

                entityColumnFilter.of(writerMetadata.getColumnCount());

                copier = assembleRecordToRowCopier(asm, cursorMetadata, writerMetadata, entityColumnFilter);
            }

            try (RecordCursor cursor = factory.getCursor(executionContext.getBindVariableService())) {
                try {
                    if (writerTimestampIndex == -1) {
                        copyUnordered(cursor, writer, copier);
                    } else {
                        copyOrdered(writer, cursor, copier, cursorTimestampIndex);
                    }
                } catch (CairoException e) {
                    // rollback data when system error occurs
                    writer.rollback();
                    throw e;
                }
            }
        }
    }

    // this exposed for testing only
    ExpressionNode parseExpression(CharSequence expression, QueryModel model) throws SqlException {
        clear();
        lexer.of(expression);
        return parser.expr(lexer, model);
    }

    // test only
    void parseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException {
        clear();
        lexer.of(expression);
        parser.expr(lexer, listener);
    }

    private boolean removeTableDirectory(CreateTableModel model) {
        path.of(configuration.getRoot()).concat(model.getName().token);
        final FilesFacade ff = configuration.getFilesFacade();
        if (ff.rmdir(path.put(Files.SEPARATOR).$())) {
            return true;
        }
        LOG.error().$("failed to clean up after create table failure [path=").$(path).$(", errno=").$(ff.errno()).$(']').$();
        return false;
    }

    void setFullSatJoins(boolean value) {
        codeGenerator.setFullFatJoins(value);
    }

    private void tableExistsOrFail(int position, CharSequence tableName) throws SqlException {
        if (TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), tableName) == TableUtils.TABLE_DOES_NOT_EXIST) {
            throw SqlException.$(position, "table '").put(tableName).put("' does not exist");
        }
    }

    private void truncateTables(GenericLexer lexer) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);
        if (!Chars.equals("table", tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'table' expected");
        }

        tableWriters.clear();
        try {
            try {
                while (true) {
                    tok = SqlUtil.fetchNext(lexer);

                    if (tok == null || Chars.equals(tok, ',')) {
                        throw SqlException.$(lexer.getPosition(), "table name expected");
                    }

                    tableExistsOrFail(lexer.lastTokenPosition(), tok);

                    try {
                        tableWriters.add(engine.getWriter(tok));
                    } catch (CairoException e) {
                        LOG.info().$("failed to lock table for truncate: ").$((Sinkable) e).$();
                        throw SqlException.$(lexer.lastTokenPosition(), "table '").put(tok).put("' is busy");
                    }
                    tok = SqlUtil.fetchNext(lexer);

                    if (tok != null && Chars.equals(tok, ',')) {
                        continue;
                    }
                    break;
                }
            } catch (SqlException e) {
                for (int i = 0, n = tableWriters.size(); i < n; i++) {
                    tableWriters.getQuick(i).close();
                }
                throw e;
            }

            for (int i = 0, n = tableWriters.size(); i < n; i++) {
                try (TableWriter writer = tableWriters.getQuick(i)) {
                    writer.truncate();
                }
            }
        } finally {
            tableWriters.clear();
        }
    }

    private InsertAsSelectModel validateAndOptimiseInsertAsSelect(InsertAsSelectModel model, SqlExecutionContext executionContext) throws SqlException {
        final QueryModel queryModel = optimiser.optimise(model.getQueryModel(), executionContext);
        int targetColumnCount = model.getColumnSet().size();
        if (targetColumnCount > 0 && queryModel.getColumns().size() != targetColumnCount) {
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
    }

    @FunctionalInterface
    private interface ExecutableMethod {
        void execute(ExecutionModel model, SqlExecutionContext sqlExecutionContext) throws SqlException;
    }

    public interface RecordToRowCopier {
        void copy(Record record, TableWriter.Row row);
    }

    private class SqlExecutionContextImpl implements SqlExecutionContext {
        private BindVariableService bindVariableService;

        @Override
        public BindVariableService getBindVariableService() {
            return bindVariableService;
        }

        @Override
        public SqlCodeGenerator getCodeGenerator() {
            return codeGenerator;
        }

        void with(BindVariableService bindVariableService) {
            this.bindVariableService = bindVariableService;
        }
    }

    static {
        castGroups.extendAndSet(ColumnType.BOOLEAN, 2);
        castGroups.extendAndSet(ColumnType.BYTE, 1);
        castGroups.extendAndSet(ColumnType.SHORT, 1);
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
        sqlControlSymbols.add(")");
        sqlControlSymbols.add(",");
        sqlControlSymbols.add("/*");
        sqlControlSymbols.add("*/");
        sqlControlSymbols.add("--");
    }
}
