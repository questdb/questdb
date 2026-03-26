/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cairo.pt;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;

import java.io.Closeable;

public class PayloadTransformStore implements Closeable {
    public static final char STATUS_ACTIVE = 'A';
    public static final char STATUS_REMOVED = 'R';
    private static final Log LOG = LogFactory.getLog(PayloadTransformStore.class);
    // Column indices for the system table (used for writes)
    private static final int COL_DLQ_PARTITION_BY = 5;
    private static final int COL_DLQ_TABLE = 4;
    private static final int COL_DLQ_TTL_UNIT = 7;
    private static final int COL_DLQ_TTL_VALUE = 6;
    private static final int COL_NAME = 1;
    private static final int COL_SELECT_SQL = 3;
    private static final int COL_STATUS = 8;
    private static final int COL_TARGET_TABLE = 2;
    // Query-result column indices for lookupTransform SELECT
    private static final int QRY_DLQ_PARTITION_BY = 4;
    private static final int QRY_DLQ_TABLE = 3;
    private static final int QRY_DLQ_TTL_UNIT = 6;
    private static final int QRY_DLQ_TTL_VALUE = 5;
    private static final int QRY_NAME = 0;
    private static final int QRY_SELECT_SQL = 2;
    private static final int QRY_STATUS = 7;
    private static final int QRY_TARGET_TABLE = 1;
    private static final String TABLE_NAME_SUFFIX = "payload_transforms";
    // Thread-safe cache: transform name -> definition.
    // Null values are not stored; a missing key means cache miss.
    private final ConcurrentHashMap<PayloadTransformDefinition> cache = new ConcurrentHashMap<>();
    private final CairoEngine engine;
    private final String tableName;
    private volatile boolean isInitialized;

    public PayloadTransformStore(CairoEngine engine) {
        this.engine = engine;
        this.tableName = engine.getConfiguration().getSystemTableNamePrefix() + TABLE_NAME_SUFFIX;
    }

    public void clear() {
        cache.clear();
        isInitialized = false;
    }

    @Override
    public void close() {
        clear();
    }

    public void createTransform(
            SqlExecutionContext executionContext,
            CharSequence name,
            CharSequence targetTable,
            CharSequence selectSql,
            CharSequence dlqTable,
            int dlqPartitionBy,
            long dlqTtlValue,
            CharSequence dlqTtlUnit
    ) throws SqlException {
        ensureInitialized(executionContext);
        final TableToken tableToken = engine.verifyTableName(tableName);
        final long timestamp = engine.getConfiguration().getMicrosecondClock().getTicks();
        try (TableWriterAPI writer = engine.getTableWriterAPI(tableToken, "payload_transform_create")) {
            TableWriter.Row row = writer.newRow(timestamp);
            row.putSym(COL_NAME, name);
            row.putStr(COL_TARGET_TABLE, targetTable);
            row.putStr(COL_SELECT_SQL, selectSql);
            if (dlqTable != null) {
                row.putStr(COL_DLQ_TABLE, dlqTable);
            }
            row.putInt(COL_DLQ_PARTITION_BY, dlqPartitionBy);
            row.putLong(COL_DLQ_TTL_VALUE, dlqTtlValue);
            if (dlqTtlUnit != null) {
                row.putStr(COL_DLQ_TTL_UNIT, dlqTtlUnit);
            }
            row.putSym(COL_STATUS, String.valueOf(STATUS_ACTIVE));
            row.append();
            writer.commit();
        }
        // Eagerly populate cache with the new definition
        final PayloadTransformDefinition def = new PayloadTransformDefinition();
        def.setName(Chars.toString(name));
        def.setTargetTable(Chars.toString(targetTable));
        def.setSelectSql(Chars.toString(selectSql));
        def.setDlqTable(dlqTable != null ? Chars.toString(dlqTable) : null);
        def.setDlqPartitionBy(dlqPartitionBy);
        def.setDlqTtlValue(dlqTtlValue);
        def.setDlqTtlUnit(dlqTtlUnit != null ? Chars.toString(dlqTtlUnit) : null);
        cache.put(Chars.toString(name), def);
    }

    public void dropTransform(SqlExecutionContext executionContext, CharSequence name) throws SqlException {
        ensureInitialized(executionContext);
        final TableToken tableToken = engine.verifyTableName(tableName);
        final long timestamp = engine.getConfiguration().getMicrosecondClock().getTicks();
        // Soft-delete: write a new row with status 'R'
        try (TableWriterAPI writer = engine.getTableWriterAPI(tableToken, "payload_transform_drop")) {
            TableWriter.Row row = writer.newRow(timestamp);
            row.putSym(COL_NAME, name);
            row.putSym(COL_STATUS, String.valueOf(STATUS_REMOVED));
            row.append();
            writer.commit();
        }
        cache.remove(Chars.toString(name));
    }

    public String getTableName() {
        return tableName;
    }

    public void init(SqlExecutionContext executionContext) throws SqlException {
        if (isInitialized) {
            return;
        }
        engine.execute(
                "CREATE TABLE IF NOT EXISTS \"" + tableName + "\" (" +
                        "ts TIMESTAMP, " +
                        "name SYMBOL, " +
                        "target_table STRING, " +
                        "select_sql STRING, " +
                        "dlq_table STRING, " +
                        "dlq_partition_by INT, " +
                        "dlq_ttl_value LONG, " +
                        "dlq_ttl_unit STRING, " +
                        "status SYMBOL" +
                        ") TIMESTAMP(ts) PARTITION BY MONTH WAL",
                executionContext
        );
        isInitialized = true;
        LOG.info().$("payload transform system table initialized [table=").$(tableName).$(']').$();
    }

    public boolean isTransformExists(SqlExecutionContext executionContext, CharSequence name) throws SqlException {
        ensureInitialized(executionContext);
        final String escapedName = Chars.toString(name).replace("'", "''");
        final String sql = "SELECT status FROM \"" + tableName + "\" WHERE name = '" + escapedName + "' LATEST ON ts PARTITION BY name";
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.query().$(sql).compile(executionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            if (cursor.hasNext()) {
                CharSequence status = cursor.getRecord().getSymA(0);
                return status != null && status.charAt(0) == STATUS_ACTIVE;
            }
            return false;
        }
    }

    public PayloadTransformDefinition lookupTransform(
            SqlExecutionContext executionContext,
            CharSequence name,
            PayloadTransformDefinition def
    ) throws SqlException {
        // Check cache first - zero-allocation lookup
        final String nameStr = Chars.toString(name);
        final PayloadTransformDefinition cached = cache.get(nameStr);
        if (cached != null) {
            def.copyFrom(cached);
            return def;
        }

        // Cache miss - query system table
        ensureInitialized(executionContext);
        final String escapedName = nameStr.replace("'", "''");
        final String sql = "SELECT name, target_table, select_sql, dlq_table, dlq_partition_by, dlq_ttl_value, dlq_ttl_unit, status "
                + "FROM \"" + tableName + "\" WHERE name = '" + escapedName + "' LATEST ON ts PARTITION BY name";
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.query().$(sql).compile(executionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            if (cursor.hasNext()) {
                Record record = cursor.getRecord();
                CharSequence status = record.getSymA(QRY_STATUS);
                if (status == null || status.charAt(0) != STATUS_ACTIVE) {
                    return null;
                }
                def.setName(Chars.toString(record.getSymA(QRY_NAME)));
                CharSequence targetTable = record.getStrA(QRY_TARGET_TABLE);
                CharSequence selectSql = record.getStrA(QRY_SELECT_SQL);
                if (targetTable == null || selectSql == null) {
                    LOG.error().$("corrupt transform definition, missing target_table or select_sql [name=").$(name).$(']').$();
                    return null;
                }
                def.setTargetTable(Chars.toString(targetTable));
                def.setSelectSql(Chars.toString(selectSql));
                CharSequence dlqTable = record.getStrA(QRY_DLQ_TABLE);
                def.setDlqTable(dlqTable != null ? Chars.toString(dlqTable) : null);
                def.setDlqPartitionBy(record.getInt(QRY_DLQ_PARTITION_BY));
                def.setDlqTtlValue(record.getLong(QRY_DLQ_TTL_VALUE));
                CharSequence dlqTtlUnit = record.getStrA(QRY_DLQ_TTL_UNIT);
                def.setDlqTtlUnit(dlqTtlUnit != null ? Chars.toString(dlqTtlUnit) : null);

                // Populate cache for subsequent lookups
                final PayloadTransformDefinition cacheEntry = new PayloadTransformDefinition();
                cacheEntry.copyFrom(def);
                cache.put(nameStr, cacheEntry);
                return def;
            }
        }
        return null;
    }

    private void ensureInitialized(SqlExecutionContext executionContext) throws SqlException {
        if (isInitialized && engine.getTableTokenIfExists(tableName) != null) {
            return;
        }
        synchronized (this) {
            if (isInitialized && engine.getTableTokenIfExists(tableName) != null) {
                return;
            }
            isInitialized = false;
            cache.clear();
            init(executionContext);
        }
    }

}
