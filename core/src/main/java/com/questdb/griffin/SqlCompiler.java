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

import com.questdb.cairo.AppendMemory;
import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.SymbolMapWriter;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.common.RecordMetadata;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.model.*;
import com.questdb.std.*;
import com.questdb.std.str.Path;

import java.util.ServiceLoader;

import static com.questdb.cairo.TableUtils.META_FILE_NAME;
import static com.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class SqlCompiler {
    private final SqlOptimiser optimiser;
    private final SqlParser parser;
    private final ObjectPool<SqlNode> sqlNodePool;
    private final CharacterStore characterStore;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final GenericLexer lexer;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final Path path = new Path();
    private final AppendMemory mem = new AppendMemory();

    public SqlCompiler(CairoEngine engine, CairoConfiguration configuration) {
        //todo: apply configuration to all storage parameters
        this.sqlNodePool = new ObjectPool<>(SqlNode.FACTORY, 128);
        this.queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 64);
        this.queryModelPool = new ObjectPool<>(QueryModel.FACTORY, 16);
        this.characterStore = new CharacterStore();
        this.lexer = new GenericLexer();
        final FunctionParser functionParser = new FunctionParser(configuration, ServiceLoader.load(FunctionFactory.class));
        this.codeGenerator = new SqlCodeGenerator(engine, functionParser);
        this.configuration = configuration;

        configureLexer(lexer);

        final PostOrderTreeTraversalAlgo postOrderTreeTraversalAlgo = new PostOrderTreeTraversalAlgo();
        optimiser = new SqlOptimiser(
                engine,
                characterStore, sqlNodePool,
                queryColumnPool, queryModelPool, postOrderTreeTraversalAlgo, functionParser
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
    }

    public static void configureLexer(GenericLexer lexer) {
        lexer.defineSymbol("(");
        lexer.defineSymbol(")");
        lexer.defineSymbol(",");
        lexer.defineSymbol("/*");
        lexer.defineSymbol("*/");
        lexer.defineSymbol("--");
        for (int i = 0, k = OperatorExpression.operators.size(); i < k; i++) {
            OperatorExpression op = OperatorExpression.operators.getQuick(i);
            if (op.symbol) {
                lexer.defineSymbol(op.token);
            }
        }
    }

    public RecordCursorFactory compile(CharSequence query, BindVariableService bindVariableService) throws SqlException {
        ExecutionModel executionModel = compileExecutionModel(query, bindVariableService);
        if (executionModel.getModelType() == ExecutionModel.QUERY) {
            return generate((QueryModel) executionModel, bindVariableService);
        }
        throw new IllegalArgumentException();
    }

    public void execute(CharSequence query, BindVariableService bindVariableService) throws SqlException {
        ExecutionModel executionModel = compileExecutionModel(query, bindVariableService);
        switch (executionModel.getModelType()) {
            case ExecutionModel.QUERY:
                break;
            case ExecutionModel.CREATE_TABLE:
                createTable((CreateTableModel) executionModel, bindVariableService);
                break;
        }
    }

    private void checkTableNameAvailable(CreateTableModel model) throws SqlException {
        final FilesFacade ff = configuration.getFilesFacade();

        if (TableUtils.exists(ff, path, configuration.getRoot(), model.getName().token) != TableUtils.TABLE_DOES_NOT_EXIST) {
            throw SqlException.position(model.getName().position).put("table already exists [table=").put(model.getName().token).put(']');
        }

        path.of(configuration.getRoot()).concat(model.getName().token);

        if (ff.mkdirs(path.put(Files.SEPARATOR).$(), configuration.getMkDirMode()) == -1) {
            throw SqlException.position(model.getName().position).put("cannot create directory [path=").put(path).put(", errno=").put(ff.errno()).put(']');
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

    ExecutionModel compileExecutionModel(GenericLexer lexer, BindVariableService bindVariableService) throws SqlException {
        ExecutionModel model = parser.parse(lexer, bindVariableService);
        if (model.getModelType() == ExecutionModel.QUERY) {
            return optimiser.optimise((QueryModel) model, bindVariableService);
        }
        return model;
    }

    ExecutionModel compileExecutionModel(CharSequence query, BindVariableService bindVariableService) throws SqlException {
        clear();
        lexer.of(query);
        return compileExecutionModel(lexer, bindVariableService);
    }

    private void copyDataFromCursor(CreateTableModel model, BindVariableService bindVariableService) throws SqlException {
        RecordCursor cursor = generate(model.getQueryModel(), bindVariableService).getCursor();
        RecordMetadata metadata = cursor.getMetadata();
        IntIntHashMap typeCast = new IntIntHashMap();
        CharSequenceObjHashMap<ColumnCastModel> castModels = model.getColumnCastModels();
        ObjList<CharSequence> castColumnNames = castModels.keys();

        for (int i = 0, n = castColumnNames.size(); i < n; i++) {
            CharSequence columnName = castColumnNames.getQuick(i);
            int index = metadata.getColumnIndexQuiet(columnName);
            // the only reason why columns cannot be found at this stage is
            // concurrent table modification of table structure
            if (index == -1) {
                throw ConcurrentModificationException.INSTANCE;
            }
            typeCast.put(index, castModels.get(columnName).getColumnType());
        }

        // check that column count matches
        int columnCount = model.getColumnCount();
        if (columnCount != metadata.getColumnCount()) {
            throw ConcurrentModificationException.INSTANCE;
        }

        // check that names match
        for (int i = 0; i < columnCount; i++) {
            CharSequence modelColumnName = model.getColumnName(i);
            CharSequence metaColumnName = metadata.getColumnName(i);
            if (!Chars.equals(modelColumnName, metaColumnName)) {
                throw ConcurrentModificationException.INSTANCE;
            }
        }

        // validate type of timestamp column
        SqlNode timestamp = model.getTimestamp();
        if (timestamp != null && metadata.getColumn(timestamp.token).getType() != ColumnType.TIMESTAMP) {
            throw SqlException.$(timestamp.position, "TIMESTAMP reference expected");
        }

        final FilesFacade ff = configuration.getFilesFacade();
        path.of(configuration.getRoot()).concat(model.getName().token);
        final int rootLen = path.length();

        try (AppendMemory mem = this.mem) {

            mem.of(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());

            int count = model.getColumnCount();
            mem.putInt(count);
            mem.putInt(PartitionBy.fromString(model.getPartitionBy().token));
            mem.putInt(model.getColumnIndex(model.getTimestamp().token));
            mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < count; i++) {
                // use type cast when available
                int castIndex = typeCast.keyIndex(i);
                if (castIndex < 0) {
                    mem.putByte((byte) typeCast.valueAt(castIndex));
                } else {
                    mem.putByte((byte) metadata.getColumnQuick(i).getType());
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
                    columnType = metadata.getColumnQuick(i).getType();
                }

                if (columnType == ColumnType.SYMBOL) {
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

    //todo: creating table requires lock to guard against bad concurrency
    private void createEmptyTable(CreateTableModel model) {
        final FilesFacade ff = configuration.getFilesFacade();
        path.of(configuration.getRoot()).concat(model.getName().token);
        final int rootLen = path.length();

        try (AppendMemory mem = this.mem) {

            mem.of(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());

            int count = model.getColumnCount();
            mem.putInt(count);
            mem.putInt(PartitionBy.fromString(model.getPartitionBy().token));
            mem.putInt(model.getColumnIndex(model.getTimestamp().token));
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

    private void createTable(CreateTableModel model, BindVariableService bindVariableService) throws SqlException {
        checkTableNameAvailable(model);
        if (model.getQueryModel() == null) {
            createEmptyTable(model);
        } else {
            copyDataFromCursor(model, bindVariableService);
        }
    }

    RecordCursorFactory generate(QueryModel queryModel, BindVariableService bindVariableService) throws SqlException {
        return codeGenerator.generate(queryModel, bindVariableService);
    }

    // this exposed for testing only
    SqlNode parseExpression(CharSequence expression) throws SqlException {
        clear();
        lexer.of(expression);
        return parser.expr(lexer);
    }

    // test only
    void parseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException {
        clear();
        lexer.of(expression);
        parser.expr(lexer, listener);
    }
}
