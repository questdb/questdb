/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

package io.questdb.test.sql;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;

/**
 * Generates random SQL SELECT statements for testing purposes.
 * <p>
 * Takes a CairoEngine instance and table names as input, and generates
 * random SELECT queries with various SQL features including:
 * <ul>
 *   <li>Column selection (random subset or all columns)</li>
 *   <li>WHERE clauses with various comparison operators</li>
 *   <li>JOINs (for multiple tables)</li>
 *   <li>GROUP BY with aggregate functions</li>
 *   <li>SAMPLE BY for time-series aggregation (requires designated timestamp)</li>
 *   <li>ORDER BY clauses</li>
 *   <li>LIMIT clauses</li>
 * </ul>
 */
public class RandomSelectGenerator {
    private final IntList currentOrderableColumns = new IntList(); // 1-based positions of non-BINARY, non-BOOLEAN columns
    private final CairoEngine engine;
    private final SqlExecutionContext executionContext;
    private final Rnd rnd;
    private final ObjList<TableInfo> tables = new ObjList<>();
    private double aggregationProbability = 0.5;
    private int currentGroupByColumnIndex = -1;
    // Temporary state during query generation
    private TableInfo currentGroupByTable;
    private boolean currentlyUsingSampleBy = false;
    private double generateLiteralProbability = 0.1;
    private double joinProbability = 0.3;
    // SAMPLE BY probability: orderByProbability * limitProbability
    private double limitProbability = 0.6;
    private double orderByProbability = 0.4;
    // SAMPLE BY probability: aggregationProbability * sampleByProbability
    // GROUP BY probability: aggregationProbability * (1 - sampleByProbability)
    // todo: fix sample by generation
    //  have to guarantee asc ordering on timestamp column!
    private double sampleByProbability = 0.0;
    private double whereClauseProbability = 0.6;

    /**
     * Creates a new random SELECT generator.
     *
     * @param engine     the CairoEngine instance to query table metadata
     * @param tableNames the table names to use in generated queries
     * @param rnd        random number generator for reproducibility
     */
    public RandomSelectGenerator(CairoEngine engine, Rnd rnd, String... tableNames) {
        this.engine = engine;
        this.rnd = rnd;

        executionContext = new SqlExecutionContextImpl(engine, 0).with(AllowAllSecurityContext.INSTANCE);

        for (String tableName : tableNames) {
            registerTable(tableName);
        }
    }

    /**
     * Generates a random SQL SELECT statement.
     *
     * @return a valid SQL SELECT statement
     */
    public String generate() throws SqlException {
        // Reset temporary state
        currentGroupByTable = null;
        currentGroupByColumnIndex = -1;
        currentOrderableColumns.clear();
        currentlyUsingSampleBy = false;

        // Initialize metadata cache for this generation - fetch once, use locally, then let GC clean up
        // This avoids fetching metadata multiple times and prevents holding references at instance level
        CharSequenceObjHashMap<TableMetadata> metadataCache = new CharSequenceObjHashMap<>();
        for (int i = 0, n = tables.size(); i < n; i++) {
            TableInfo table = tables.get(i);
            metadataCache.put(table.tableName(), engine.getTableMetadata(table.tableToken()));
        }

        try {
            StringSink sql = new StringSink();
            sql.put("SELECT ");

            // Decide if we're doing a multi-table query (JOIN)
            boolean isMultiTable = tables.size() > 1 && rnd.nextDouble() < joinProbability;

            // Decide if we're doing aggregation (GROUP BY or SAMPLE BY)
            boolean useAggregation = rnd.nextDouble() < aggregationProbability;
            boolean useSampleBy = false;

            // If using aggregation, decide between GROUP BY and SAMPLE BY
            if (useAggregation) {
                // SAMPLE BY requires a designated timestamp column, so check if table has one
                useSampleBy = !isMultiTable && rnd.nextDouble() < sampleByProbability && hasTimestampColumn(metadataCache);
                currentlyUsingSampleBy = useSampleBy;
            }

            boolean useGroupBy = useAggregation && !useSampleBy;

            // Select which tables to use
            ObjList<TableInfo> selectedTables = selectTables(isMultiTable);

            // Generate SELECT clause
            generateSelectClause(sql, selectedTables, useAggregation, metadataCache);

            // Generate FROM clause
            sql.put(" FROM ");
            generateFromClause(sql, selectedTables, isMultiTable, metadataCache);

            // Generate WHERE clause
            if (!useAggregation && rnd.nextDouble() < whereClauseProbability) {
                sql.put(" WHERE ");
                generateWhereClause(sql, selectedTables, metadataCache);
            }

            // Generate SAMPLE BY clause (mutually exclusive with GROUP BY)
            if (useSampleBy) {
                sql.put(" ");
                generateSampleByClause(sql);
            }
            // Generate GROUP BY clause
            else if (useGroupBy) {
                sql.put(" ");
                generateGroupByClause(sql, selectedTables, metadataCache);
            }

            // Generate ORDER BY clause
            if (rnd.nextDouble() < orderByProbability) {
                // LIMIT is only deterministic when we have an orderable GROUP BY key (if grouping)
                // or any orderable column (when not grouping)
                boolean hasOrderableGroupKey = useGroupBy && currentOrderableColumns.contains(1);
                boolean limitEligible = (!useGroupBy && currentOrderableColumns.size() > 0) || hasOrderableGroupKey;
                boolean willUseLimit = limitEligible && rnd.nextDouble() < limitProbability;

                if (willUseLimit) {
                    if (currentlyUsingSampleBy) {
                        // SAMPLE BY requires ascending timestamp ordering; enforce ascending on all orderable columns
                        sql.put(" ORDER BY ");
                        for (int i = 0, n = currentOrderableColumns.size(); i < n; i++) {
                            if (i > 0) {
                                sql.put(", ");
                            }
                            sql.put(currentOrderableColumns.get(i));
                        }
                        sql.put(" LIMIT ");
                        sql.put(1 + rnd.nextInt(100));
                        return sql.toString();
                    }
                    // When using LIMIT, ORDER BY all orderable columns positionally to ensure deterministic results
                    // (ordering by a single low-cardinality column can cause non-determinism when LIMIT cuts rows)
                    sql.put(" ORDER BY ");
                    for (int i = 0, n = currentOrderableColumns.size(); i < n; i++) {
                        if (i > 0) {
                            sql.put(", ");
                        }
                        sql.put(currentOrderableColumns.get(i));
                        if (rnd.nextBoolean()) {
                            sql.put(" DESC");
                        }
                    }
                    sql.put(" LIMIT ");
                    sql.put(1 + rnd.nextInt(100));
                } else {
                    // No LIMIT - use regular ORDER BY on a single column
                    sql.put(" ");
                    generateOrderByClause(sql, selectedTables, useGroupBy, metadataCache);
                }
            }

            return sql.toString();
        } finally {
            for (int i = 0, n = tables.size(); i < n; i++) {
                TableInfo table = tables.get(i);
                metadataCache.get(table.tableName()).close();
            }

            // Clear cache to release metadata references
            metadataCache.clear();
        }
    }

    public void registerTable(String tableName) {
        TableToken tableToken = engine.verifyTableName(tableName);
        String alias = (tableToken.isView() ? "v" : "t") + tables.size();
        tables.add(new TableInfo(tableName, tableToken, alias));
    }

    public RandomSelectGenerator setAggregationProbability(double probability) {
        aggregationProbability = probability;
        return this;
    }

    public RandomSelectGenerator setGenerateLiteralProbability(double probability) {
        generateLiteralProbability = probability;
        return this;
    }

    public RandomSelectGenerator setJoinProbability(double probability) {
        joinProbability = probability;
        return this;
    }

    public RandomSelectGenerator setLimitProbability(double probability) {
        limitProbability = probability;
        return this;
    }

    public RandomSelectGenerator setOrderByProbability(double probability) {
        orderByProbability = probability;
        return this;
    }

    public RandomSelectGenerator setSampleByProbability(double probability) {
        sampleByProbability = probability;
        return this;
    }

    public RandomSelectGenerator setWhereClauseProbability(double probability) {
        whereClauseProbability = probability;
        return this;
    }

    private boolean areTypesCompatibleForJoin(int type1, int type2) {
        // QuestDB requires exact type matches for JOINs
        // We only allow exact matches to avoid type mismatch errors
        return type1 == type2;
    }

    private void generateAggregateSelectClause(StringSink sql, ObjList<TableInfo> selectedTables, CharSequenceObjHashMap<TableMetadata> metadataCache) {
        TableInfo table = selectedTables.get(rnd.nextInt(selectedTables.size()));
        RecordMetadata metadata = metadataCache.get(table.tableName());
        for (int i = 0, n = selectedTables.size(); i < n && metadata.getTimestampIndex() < 0; i++) {
            table = selectedTables.get(i);
            metadata = metadataCache.get(table.tableName());
        }

        int columnCount = metadata.getColumnCount();
        if (columnCount == 0) {
            sql.put("count(*)");
            currentGroupByTable = null;
            currentGroupByColumnIndex = -1;
            currentOrderableColumns.add(1); // count(*) is orderable
            return;
        }

        int firstColIdx;

        // For SAMPLE BY, use the designated timestamp column
        if (currentlyUsingSampleBy) {
            firstColIdx = metadata.getTimestampIndex();
            if (firstColIdx < 0) {
                // Fallback if no timestamp (shouldn't happen as we check hasTimestampColumn)
                firstColIdx = 0;
            }
        } else {
            // For GROUP BY, pick a random column
            firstColIdx = rnd.nextInt(columnCount);
        }

        // Store for use in GROUP BY clause
        currentGroupByTable = table;
        currentGroupByColumnIndex = firstColIdx;

        // Add the first column (timestamp for SAMPLE BY, random for GROUP BY) to SELECT
        if (selectedTables.size() > 1) {
            sql.put(table.alias).put(".");
        }
        sql.put(metadata.getColumnName(firstColIdx));

        // Pick a random numeric column for aggregation
        IntList numericColumns = new IntList();
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            if (isNumericType(type)) {
                numericColumns.add(i);
            }
        }

        // Add an aggregate function
        sql.put(", ");
        if (numericColumns.size() == 0) {
            sql.put("count(*)");
        } else {
            // Pick random aggregate function
            String[] aggregates = {"sum", "avg", "min", "max", "count"};
            String aggregate = aggregates[rnd.nextInt(aggregates.length)];

            int colIdx = numericColumns.get(rnd.nextInt(numericColumns.size()));
            sql.put(aggregate).put("(");
            if (selectedTables.size() > 1) {
                sql.put(table.alias).put(".");
            }
            sql.put(metadata.getColumnName(colIdx));
            sql.put(")");
        }
        // Track orderable columns (non-BINARY, non-BOOLEAN - these have low cardinality causing non-determinism with LIMIT)
        int firstColType = metadata.getColumnType(firstColIdx);
        if (firstColType != ColumnType.BINARY && firstColType != ColumnType.BOOLEAN) {
            currentOrderableColumns.add(1); // GROUP BY column
        }
        currentOrderableColumns.add(2); // Aggregate result is always orderable (numeric)
    }

    private void generateComparisonOperator(StringSink sql, int columnType) {
        if (columnType == ColumnType.STRING || columnType == ColumnType.VARCHAR || columnType == ColumnType.SYMBOL) {
            String[] operators = {" = ", " != ", " LIKE "};
            sql.put(operators[rnd.nextInt(operators.length)]);
        } else if (columnType == ColumnType.BOOLEAN) {
            sql.put(rnd.nextBoolean() ? " = " : " != ");
        } else {
            String[] operators = {" = ", " != ", " > ", " >= ", " < ", " <= "};
            sql.put(operators[rnd.nextInt(operators.length)]);
        }
    }

    private void generateFromClause(StringSink sql, ObjList<TableInfo> selectedTables, boolean isMultiTable, CharSequenceObjHashMap<TableMetadata> metadataCache) {
        if (!isMultiTable || selectedTables.size() == 1) {
            // Single table
            TableInfo table = selectedTables.get(0);
            sql.put(table.tableName);
            if (selectedTables.size() > 1) {
                sql.put(" ").put(table.alias);
            }
        } else {
            // JOIN
            TableInfo table1 = selectedTables.get(0);
            TableInfo table2 = selectedTables.get(1);

            sql.put(table1.tableName).put(" ").put(table1.alias);

            // Pick random JOIN type
            String[] joinTypes = {"INNER JOIN", "LEFT JOIN", "JOIN"};
            String joinType = joinTypes[rnd.nextInt(joinTypes.length)];

            sql.put(" ").put(joinType).put(" ");
            sql.put(table2.tableName).put(" ").put(table2.alias);

            // Generate JOIN condition
            sql.put(" ON ");
            generateJoinCondition(sql, table1, table2, metadataCache);
        }
    }

    private void generateGroupByClause(StringSink sql, ObjList<TableInfo> selectedTables, CharSequenceObjHashMap<TableMetadata> metadataCache) {
        // Use the column that was selected in the aggregate SELECT clause
        if (currentGroupByTable == null || currentGroupByColumnIndex < 0) {
            // Fallback: pick a random table and column
            TableInfo table = selectedTables.get(rnd.nextInt(selectedTables.size()));
            RecordMetadata metadata = metadataCache.get(table.tableName());
            int columnCount = metadata.getColumnCount();
            if (columnCount == 0) {
                return;
            }
            currentGroupByTable = table;
            currentGroupByColumnIndex = rnd.nextInt(columnCount);
        }

        sql.put("GROUP BY ");
        if (selectedTables.size() > 1) {
            sql.put(currentGroupByTable.alias).put(".");
        }
        sql.put(metadataCache.get(currentGroupByTable.tableName()).getColumnName(currentGroupByColumnIndex));
    }

    private void generateJoinCondition(StringSink sql, TableInfo table1, TableInfo table2, CharSequenceObjHashMap<TableMetadata> metadataCache) {
        // Try to find matching column types between tables
        RecordMetadata meta1 = metadataCache.get(table1.tableName());
        RecordMetadata meta2 = metadataCache.get(table2.tableName());

        ObjList<int[]> matchingColumns = new ObjList<>();
        for (int i = 0; i < meta1.getColumnCount(); i++) {
            int type1 = meta1.getColumnType(i);
            if (type1 == ColumnType.BOOLEAN) {
                continue;
            }
            for (int j = 0; j < meta2.getColumnCount(); j++) {
                int type2 = meta2.getColumnType(j);
                if (areTypesCompatibleForJoin(type1, type2)) {
                    matchingColumns.add(new int[]{i, j});
                }
            }
        }

        if (matchingColumns.size() == 0) {
            // Fallback: use 1=1
            sql.put("1=1");
        } else {
            // Pick a random matching column pair
            int[] pair = matchingColumns.get(rnd.nextInt(matchingColumns.size()));
            sql.put(table1.alias).put(".").put(meta1.getColumnName(pair[0]));
            sql.put(" = ");
            sql.put(table2.alias).put(".").put(meta2.getColumnName(pair[1]));
        }
    }

    private void generateLiteralValue(StringSink sql, int columnType) {
        switch (columnType) {
            case ColumnType.BOOLEAN:
                sql.put(rnd.nextBoolean());
                break;
            case ColumnType.BYTE:
                sql.put(rnd.nextByte());
                break;
            case ColumnType.SHORT:
                sql.put(rnd.nextShort());
                break;
            case ColumnType.INT:
                // Generate reasonable int values to avoid overflow issues
                sql.put(rnd.nextInt() % 1000000);
                break;
            case ColumnType.LONG:
                // Generate reasonable long values
                sql.put(Math.abs(rnd.nextLong() % 1000000000L));
                break;
            case ColumnType.FLOAT:
                // Generate safe float values (avoid NaN, Infinity)
                float floatVal = rnd.nextFloat() * 1000.0f;
                if (Float.isNaN(floatVal) || Float.isInfinite(floatVal)) {
                    floatVal = 1.0f;
                }
                sql.put(floatVal);
                break;
            case ColumnType.DOUBLE:
                // Generate safe double values (avoid NaN, Infinity)
                double doubleVal = rnd.nextDouble() * 1000.0;
                if (Double.isNaN(doubleVal) || Double.isInfinite(doubleVal)) {
                    doubleVal = 1.0;
                }
                sql.put(doubleVal);
                break;
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
            case ColumnType.SYMBOL:
                sql.put("'");
                // Generate a simple random string
                int len = 1 + rnd.nextInt(10);
                for (int i = 0; i < len; i++) {
                    sql.put((char) ('a' + rnd.nextInt(26)));
                }
                sql.put("'");
                break;
            case ColumnType.TIMESTAMP:
            case ColumnType.DATE:
                // Generate a timestamp in microseconds (year 2024)
                long timestamp = 1704067200000000L + (Math.abs(rnd.nextLong()) % 31536000000000L);
                sql.put(timestamp);
                break;
            case ColumnType.CHAR:
                sql.put("'").put((char) ('a' + rnd.nextInt(26))).put("'");
                break;
            default:
                // For unsupported types, use NULL
                sql.put("NULL");
                break;
        }
    }

    private int generateOrderByClause(StringSink sql, ObjList<TableInfo> selectedTables, boolean hasGroupBy, CharSequenceObjHashMap<TableMetadata> metadataCache) {
        // SAMPLE BY requires ASC order on timestamp, so skip ORDER BY entirely
        // (default order is ASC which is what SAMPLE BY needs)
        if (currentlyUsingSampleBy) {
            // assumption: SAMPLE BY results are always ordered by timestamp ASC -> we can treat it as ORDER BY
            return ColumnType.TIMESTAMP;
        }

        if (hasGroupBy) {
            // When using GROUP BY, ORDER BY must reference columns in GROUP BY or aggregates
            // Use the GROUP BY column if available
            if (currentGroupByTable != null && currentGroupByColumnIndex >= 0) {
                RecordMetadata metadata = metadataCache.get(currentGroupByTable.tableName());
                int colType = metadata.getColumnType(currentGroupByColumnIndex);
                if (colType == ColumnType.BINARY) {
                    // ORDER BY with BINARY column is not supported, we skip
                    return Numbers.INT_NULL;
                }

                sql.put("ORDER BY ");
                if (selectedTables.size() > 1) {
                    sql.put(currentGroupByTable.alias).put(".");
                }
                sql.put(metadataCache.get(currentGroupByTable.tableName()).getColumnName(currentGroupByColumnIndex));

                // Random ASC/DESC
                if (rnd.nextBoolean()) {
                    sql.put(" DESC");
                }
                return colType;
            }
            // Otherwise skip ORDER BY for GROUP BY queries
            return Numbers.INT_NULL;
        } else {
            // Regular ORDER BY for non-aggregate queries
            TableInfo table = selectedTables.get(rnd.nextInt(selectedTables.size()));
            RecordMetadata metadata = metadataCache.get(table.tableName());

            int columnCount = metadata.getColumnCount();
            if (columnCount == 0) {
                return Numbers.INT_NULL;
            }

            // Pick a random column to order by
            int colIdx = rnd.nextInt(columnCount);

            int colType = metadata.getColumnType(colIdx);
            if (colType == ColumnType.BINARY) {
                // ORDER BY with BINARY column is not supported, we skip
                return Numbers.INT_NULL;
            }

            sql.put("ORDER BY ");
            if (selectedTables.size() > 1) {
                sql.put(table.alias).put(".");
            }
            sql.put(metadata.getColumnName(colIdx));

            // Random ASC/DESC
            if (rnd.nextBoolean()) {
                sql.put(" DESC");
            }
            return colType;
        }
    }

    private void generateSampleByClause(StringSink sql) {
        // SAMPLE BY requires time intervals
        String[] intervals = {"1s", "10s", "30s", "1m", "5m", "10m", "30m", "1h", "1d"};
        String interval = intervals[rnd.nextInt(intervals.length)];

        sql.put("SAMPLE BY ").put(interval);

        // Optionally add ALIGN TO clause
        if (rnd.nextBoolean()) {
            String[] alignOptions = {"CALENDAR", "FIRST OBSERVATION"};
            sql.put(" ALIGN TO ").put(alignOptions[rnd.nextInt(alignOptions.length)]);
        }
    }

    private void generateSelectClause(StringSink sql, ObjList<TableInfo> selectedTables, boolean useAggregation, CharSequenceObjHashMap<TableMetadata> metadataCache) {
        if (useAggregation) {
            generateAggregateSelectClause(sql, selectedTables, metadataCache);
        } else {
            generateSimpleSelectClause(sql, selectedTables, metadataCache);
        }
    }

    private void generateSimpleSelectClause(StringSink sql, ObjList<TableInfo> selectedTables, CharSequenceObjHashMap<TableMetadata> metadataCache) {
        // Pick a random table
        TableInfo table = selectedTables.get(rnd.nextInt(selectedTables.size()));
        RecordMetadata metadata = metadataCache.get(table.tableName());

        // Decide how many columns to select
        int columnCount = metadata.getColumnCount();
        if (columnCount == 0) {
            sql.put("*");
            return;
        }

        int numColumnsToSelect = 1 + rnd.nextInt(Math.min(5, columnCount));

        // Generate random column indices (ensure we get unique columns)
        IntList selectedColumns = new IntList();
        int attempts = 0;
        while (selectedColumns.size() < numColumnsToSelect && attempts < columnCount * 3) {
            int colIdx = rnd.nextInt(columnCount);
            if (!selectedColumns.contains(colIdx)) {
                selectedColumns.add(colIdx);
            }
            attempts++;
        }

        // Fallback: if we still have no columns, select first column
        if (selectedColumns.size() == 0) {
            selectedColumns.add(0);
        }

        // Build SELECT list
        boolean first = true;
        for (int i = 0, n = selectedColumns.size(); i < n; i++) {
            int colIdx = selectedColumns.get(i);
            if (!first) {
                sql.put(", ");
            }
            first = false;

            if (selectedTables.size() > 1) {
                sql.put(table.alias).put(".");
            }
            sql.put(metadata.getColumnName(colIdx));

            // Track orderable columns (non-BINARY, non-BOOLEAN) - position is 1-based
            // BINARY can't be ordered, BOOLEAN has low cardinality causing non-determinism with LIMIT
            int colType = metadata.getColumnType(colIdx);
            if (colType != ColumnType.BINARY && colType != ColumnType.BOOLEAN) {
                currentOrderableColumns.add(i + 1);
            }
        }
    }

    private void generateWhereClause(StringSink sql, ObjList<TableInfo> selectedTables, CharSequenceObjHashMap<TableMetadata> metadataCache) throws SqlException {
        TableInfo table = selectedTables.get(rnd.nextInt(selectedTables.size()));
        RecordMetadata metadata = metadataCache.get(table.tableName());

        IntList usableColumns = new IntList();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            int columnType = metadata.getColumnType(i);
            if (columnType != ColumnType.BINARY && columnType != ColumnType.LONG128) {
                usableColumns.add(i);
            }
        }
        if (usableColumns.size() == 0) {
            sql.put("1=1");
            return;
        }

        int numConditions = 1 + rnd.nextInt(Math.min(3, usableColumns.size()));
        for (int i = 0; i < numConditions; i++) {
            if (i > 0) {
                sql.put(rnd.nextBoolean() ? " AND " : " OR ");
            }

            int colIdx = usableColumns.get(rnd.nextInt(usableColumns.size()));
            int columnType = metadata.getColumnType(colIdx);

            if (selectedTables.size() > 1) {
                sql.put(table.alias).put(".");
            }

            String columnName = metadata.getColumnName(colIdx);
            sql.put(columnName);

            generateComparisonOperator(sql, columnType);

            if (rnd.nextDouble() < generateLiteralProbability) {
                generateLiteralValue(sql, columnType);
            } else {
                pickExistingValue(sql, table, columnName, columnType);
            }
        }
    }

    private boolean hasTimestampColumn(CharSequenceObjHashMap<TableMetadata> metadataCache) {
        // Check if any table has a designated timestamp column
        for (int i = 0, n = tables.size(); i < n; i++) {
            TableInfo table = tables.get(i);
            RecordMetadata metadata = metadataCache.get(table.tableName());
            if (metadata.getTimestampIndex() >= 0) {
                return true;
            }
        }
        return false;
    }

    private boolean isNumericType(int columnType) {
        return columnType == ColumnType.BYTE
                || columnType == ColumnType.SHORT
                || columnType == ColumnType.INT
                || columnType == ColumnType.LONG
                || columnType == ColumnType.FLOAT
                || columnType == ColumnType.DOUBLE;
    }

    private void pickExistingValue(StringSink sql, TableInfo table, String columnName, int columnType) throws SqlException {
        final String distinctSql = "select distinct " + columnName + " from " + table.tableName();
        try (
                RecordCursorFactory factory = engine.select(distinctSql, executionContext);
                RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            int index = rnd.nextInt(5);
            int i = -1;
            while (cursor.hasNext() && i < index) {
                i++;
            }
            if (i < 0) {
                // empty table, fallback to generate a new value
                generateLiteralValue(sql, columnType);
                return;
            }

            Record record = cursor.getRecord();
            switch (columnType) {
                case ColumnType.BOOLEAN:
                    sql.put(record.getBool(0));
                    break;
                case ColumnType.BYTE:
                    sql.put(record.getByte(0));
                    break;
                case ColumnType.SHORT:
                    sql.put(record.getShort(0));
                    break;
                case ColumnType.INT:
                    sql.put(record.getInt(0));
                    break;
                case ColumnType.LONG:
                    sql.put(record.getLong(0));
                    break;
                case ColumnType.FLOAT:
                    sql.put(record.getFloat(0));
                    break;
                case ColumnType.DOUBLE:
                    sql.put(record.getDouble(0));
                    break;
                case ColumnType.STRING:
                    sql.put("'");
                    sql.put(record.getStrA(0));
                    sql.put("'");
                    break;
                case ColumnType.VARCHAR:
                    sql.put("'");
                    sql.put(record.getVarcharA(0));
                    sql.put("'");
                    break;
                case ColumnType.SYMBOL:
                    sql.put("'");
                    sql.put(record.getSymA(0));
                    sql.put("'");
                    break;
                case ColumnType.TIMESTAMP:
                    sql.put(record.getTimestamp(0));
                    break;
                case ColumnType.DATE:
                    sql.put(record.getDate(0));
                    break;
                case ColumnType.CHAR:
                    sql.put("'");
                    sql.put(record.getChar(0));
                    sql.put("'");
                    break;
                default:
                    // For unsupported types, use NULL
                    sql.put("NULL");
                    break;
            }
        }
    }

    private ObjList<TableInfo> selectTables(boolean isMultiTable) {
        if (!isMultiTable || tables.size() == 1) {
            // Single table - pick one randomly
            TableInfo table = tables.get(rnd.nextInt(tables.size()));
            ObjList<TableInfo> selected = new ObjList<>(1);
            selected.add(table);
            return selected;
        } else {
            // Multi-table - pick 2 tables for simplicity
            int count = Math.min(2, tables.size());
            ObjList<TableInfo> selected = new ObjList<>(count);

            // Always include the first table
            selected.add(tables.get(0));

            // Pick another random table (different from first)
            int secondIdx = 1 + rnd.nextInt(tables.size() - 1);
            selected.add(tables.get(secondIdx));

            return selected;
        }
    }

    /**
     * Holds metadata about a table for query generation.
     */
    private record TableInfo(String tableName, TableToken tableToken, String alias) {

        @Override
        public String toString() {
            return "TableInfo[" +
                    "tableName=" + tableName + ", " +
                    "tableToken=" + tableToken + ", " +
                    "alias=" + alias + ']';
        }
    }
}
