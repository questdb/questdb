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

package com.questdb.griffin.compiler;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.common.PostOrderTreeTraversalAlgo;
import com.questdb.griffin.lexer.ParserException;
import com.questdb.griffin.lexer.SqlLexerOptimiser;
import com.questdb.griffin.lexer.model.ParsedModel;
import com.questdb.griffin.lexer.model.QueryModel;

public class QueryCompiler {
    private final SqlLexerOptimiser queryParser;
    private final QueryFilterAnalyser filterAnalyser = new QueryFilterAnalyser();
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final VirtualColumnBuilder virtualColumnBuilder;

    public QueryCompiler(CairoEngine engine, CairoConfiguration configuration) {
        this.queryParser = new SqlLexerOptimiser(engine, configuration);
        this.virtualColumnBuilder = new VirtualColumnBuilder(traversalAlgo, configuration);
    }

    public RecordCursorFactory compileQuery(CharSequence query) throws ParserException {
        return compile(queryParser.parse(query));
    }

    private void clearState() {
        // todo: clear
    }

    private RecordCursorFactory compile(ParsedModel model) {
        if (model.getModelType() == ParsedModel.QUERY) {
            clearState();
            final QueryModel qm = (QueryModel) model;
//            qm.setParameterMap(EMPTY_PARAMS);
//            RecordCursorFactory factory = compile(qm);
//            rs.setParameterMap(EMPTY_PARAMS);
            return null;
        }
        throw new IllegalArgumentException("QueryModel expected");
    }
/*

    @SuppressWarnings("ConstantConditions")
    private RecordCursorFactory compileTable(QueryModel model, CairoEngine engine) throws ParserException {

        applyLimit(model);

        ExprNode latestBy = model.getLatestBy();

        ExprNode whereClause = model.getWhereClause();

        try (TableReader reader = engine.getReader(model.getTableName().token)) {
            if (whereClause != null) {

                final RecordMetadata metadata = reader.getMetadata();
                final int timestampIndex;

                ExprNode timestamp = model.getTimestamp();
                if (timestamp != null) {
                    timestampIndex = metadata.getColumnIndex(timestamp.token);
                } else {
                    timestampIndex = -1;
                }

                final IntrinsicModel intrinsicModel = filterAnalyser.extract(model, whereClause, reader.getMetadata(), latestBy != null ? latestBy.token : null, timestampIndex);

                if (intrinsicModel.intrinsicValue == IntrinsicValue.FALSE) {
                    // todo: return empty factory
                    return null;
                }

                VirtualColumn filter;

                CharSequenceObjHashMap<Parameter> parameterMap = new CharSequenceObjHashMap<>();
                if (intrinsicModel.filter != null) {
                    filter = virtualColumnBuilder.buildFrom(intrinsicModel.filter, metadata, parameterMap);
                } else {
                    filter = null;
                }

                // validate filter
                if (filter != null) {
                    if (filter.getType() != ColumnType.BOOLEAN) {
                        throw ParserException.$(filter.getPosition(), "Boolean expression expected");
                    }

                    if (filter.isConstant()) {
                        // can pass null to constant function
                        if (filter.getBool(null)) {
                            // filter is constant "true", do not evaluate for every row
                            filter = null;
                        } else {
                            //todo: return factory, which would create empty record cursor for the given table
                            return null;
                        }
                    }
                }

                DataFrameCursorFactory dfcFactory;
                if (intrinsicModel.intervals != null) {
                    dfcFactory = new IntervalFrameCursorFactory(engine, model.getTableName().token.toString(), intrinsicModel.intervals);
                } else {
                    dfcFactory = new FullTableFrameCursorFactory(engine, model.getTableName().token.toString());
                }


            }
        }

        RecordMetadata metadata = reader.getMetadata();


        PartitionSource ps = new JournalPartitionSource(journalMetadata, true);
        RowSource rs = null;

        String latestByCol = null;
        RecordColumnMetadata latestByMetadata = null;
        ExprNode latestByNode = null;

        if (model.getLatestBy() != null) {
            latestByNode = model.getLatestBy();
            if (latestByNode.type != ExprNode.LITERAL) {
                throw QueryError.$(latestByNode.position, "Column name expected");
            }

            latestByCol = model.translateAlias(latestByNode.token).toString();

            int colIndex = journalMetadata.getColumnIndexQuiet(latestByCol);
            if (colIndex == -1) {
                throw QueryError.invalidColumn(latestByNode.position, latestByNode.token);
            }

            latestByMetadata = journalMetadata.getColumnQuick(colIndex);

            int type = latestByMetadata.getType();
            if (type != ColumnType.SYMBOL && type != ColumnType.STRING && type != ColumnType.INT && type != ColumnType.LONG) {
                throw QueryError.position(latestByNode.position).$("Expected symbol, string, int or long column, found: ").$(ColumnType.nameOf(type)).$();
            }

            if (!latestByMetadata.isIndexed()) {
                throw QueryError.position(latestByNode.position).$("Column is not indexed").$();
            }
        }

        ExprNode where = model.getWhereClause();
        if (where != null) {
            IntrinsicModel im = queryFilterAnalyser.extract(
                    model,
                    where,
                    journalMetadata,
                    latestByCol,
                    getTimestampIndexQuiet(model.getTimestamp(), journalMetadata)
            );

            VirtualColumn filter = im.filter != null ? virtualColumnBuilder.createVirtualColumn(model, im.filter, journalMetadata) : null;

            if (filter != null) {
                if (filter.getType() != ColumnType.BOOLEAN) {
                    throw QueryError.$(im.filter.position, "Boolean expression expected");
                }

                if (filter.isConstant()) {
                    if (filter.getBool(null)) {
                        // constant TRUE, no filtering needed
                        filter = null;
                    } else {
                        im.intrinsicValue = IntrinsicValue.FALSE;
                    }
                }
            }

            if (im.intrinsicValue == IntrinsicValue.FALSE) {
                ps = new NoOpJournalPartitionSource(journalMetadata);
            } else {

                if (im.intervals != null) {
                    ps = new MultiIntervalPartitionSource(ps, im.intervals);
                }

                if (latestByCol == null) {
                    if (im.keyColumn != null) {
                        switch (journalMetadata.getColumn(im.keyColumn).getType()) {
                            case ColumnType.SYMBOL:
                                rs = buildRowSourceForSym(im);
                                break;
                            case ColumnType.STRING:
                                rs = buildRowSourceForStr(im);
                                break;
                            case ColumnType.INT:
                                rs = buildRowSourceForInt(im);
                                break;
                            case ColumnType.LONG:
                                rs = buildRowSourceForLong(im);
                                break;
                            default:
                                break;
                        }
                    }

                    if (filter != null) {
                        rs = new FilteredRowSource(rs == null ? new AllRowSource() : rs, filter);
                    }
                } else {
                    if (im.keyColumn != null && im.keyValuesIsLambda) {
                        int lambdaColIndex;
                        RecordSource lambda = compileSourceInternal(factory, im.keyValues.get(0));
                        RecordMetadata m = lambda.getMetadata();

                        switch (m.getColumnCount()) {
                            case 0:
                                Misc.free(lambda);
                                throw QueryError.$(im.keyValuePositions.getQuick(0), "Query must select at least one column");
                            case 1:
                                lambdaColIndex = 0;
                                break;
                            default:
                                lambdaColIndex = m.getColumnIndexQuiet(latestByCol);
                                if (lambdaColIndex == -1) {
                                    Misc.free(lambda);
                                    throw QueryError.$(im.keyValuePositions.getQuick(0), "Ambiguous column names in lambda query. Specify select clause");
                                }
                                break;
                        }

                        int lambdaColType = m.getColumnQuick(lambdaColIndex).getType();
                        mutableSig.setParamCount(2).setName("").paramType(0, latestByMetadata.getType(), true).paramType(1, lambdaColType, false);
                        LatestByLambdaRowSourceFactory fact = LAMBDA_ROW_SOURCE_FACTORIES.get(mutableSig);
                        if (fact != null) {
                            rs = fact.newInstance(latestByCol, lambda, lambdaColIndex, filter);
                        } else {
                            Misc.free(lambda);
                            throw QueryError.$(im.keyValuePositions.getQuick(0), "Mismatched types");
                        }
                    } else {
                        switch (latestByMetadata.getType()) {
                            case ColumnType.SYMBOL:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexSymListHeadRowSource(latestByCol, new CharSequenceHashSet(im.keyValues), filter);
                                } else {
                                    rs = new KvIndexSymAllHeadRowSource(latestByCol, filter);
                                }
                                break;
                            case ColumnType.STRING:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexStrListHeadRowSource(latestByCol, new CharSequenceHashSet(im.keyValues), filter);
                                } else {
                                    Misc.free(rs);
                                    throw QueryError.$(latestByNode.position, "Filter on string column expected");
                                }
                                break;
                            case ColumnType.LONG:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexLongListHeadRowSource(latestByCol, toLongHashSet(im), filter);
                                } else {
                                    Misc.free(rs);
                                    throw QueryError.$(latestByNode.position, "Filter on long column expected");
                                }
                                break;
                            case ColumnType.INT:
                                if (im.keyColumn != null) {
                                    rs = new KvIndexIntListHeadRowSource(latestByCol, toIntHashSet(im), filter);
                                } else {
                                    Misc.free(rs);
                                    throw QueryError.$(latestByNode.position, "Filter on int column expected");
                                }
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
        } else if (latestByCol != null) {
            switch (latestByMetadata.getType()) {
                case ColumnType.SYMBOL:
                    rs = new KvIndexSymAllHeadRowSource(latestByCol, null);
                    break;
                default:
                    Misc.free(rs);
                    throw QueryError.$(latestByNode.position, "Only SYM columns can be used here without filter");
            }
        }

        // check for case of simple "select count() from tab"
        if (rs == null && model.getColumns().size() == 1) {
            QueryColumn qc = model.getColumns().getQuick(0);
            if ("count".equals(qc.getAst().token) && qc.getAst().paramCount == 0) {
                // remove order clause
                model.getOrderBy().clear();
                // remove columns so that there is no wrapping of result source
                model.getColumns().clear();
                return new CountRecordSource(qc.getAlias() == null ? "count" : qc.getAlias(), ps);
            }
        }

        RecordSource recordSource = new JournalRecordSource(ps, rs == null ? new AllRowSource() : rs);
        if (com.questdb.parser.sql.model.QueryModel.hasMarker(model.getJournalName().token)) {
            return new NoRowIdRecordSource().of(recordSource);
        }
        return recordSource;
    }

*/
}
