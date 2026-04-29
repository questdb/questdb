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
import java.util.concurrent.atomic.AtomicLong;

// Concurrent DDL note: CREATE, DROP, and OR REPLACE are separate read-check-write
// operations with no name-level serialization. If two sessions operate on the same
// transform name concurrently, the outcome is last-writer-wins based on how
// LATEST ON resolves timestamp ties. nextTimestamp() ensures OR REPLACE (drop + create)
// produces distinct timestamps within the same process, but does not prevent races
// across concurrent sessions.
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
    private static final String STATUS_ACTIVE_STR = String.valueOf(STATUS_ACTIVE);
    private static final String STATUS_REMOVED_STR = String.valueOf(STATUS_REMOVED);
    private static final String TABLE_NAME_SUFFIX = "payload_transforms";
    // Thread-safe cache: transform name -> definition.
    // Null values are not stored; a missing key means cache miss.
    private final ConcurrentHashMap<PayloadTransformDefinition> cache = new ConcurrentHashMap<>();
    private final CairoEngine engine;
    private final AtomicLong lastWriteTimestamp = new AtomicLong(Long.MIN_VALUE);
    private final String tableName;
    private volatile boolean isInitialized;

    public PayloadTransformStore(CairoEngine engine) {
        this.engine = engine;
        this.tableName = engine.getConfiguration().getSystemTableNamePrefix() + TABLE_NAME_SUFFIX;
    }

    public void clear() {
        isInitialized = false;
        cache.clear();
        lastWriteTimestamp.set(Long.MIN_VALUE);
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
        final long timestamp = nextTimestamp();
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
            row.putSym(COL_STATUS, STATUS_ACTIVE_STR);
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
        final long timestamp = nextTimestamp();
        // Soft-delete: write a new row with status 'R'
        try (TableWriterAPI writer = engine.getTableWriterAPI(tableToken, "payload_transform_drop")) {
            TableWriter.Row row = writer.newRow(timestamp);
            row.putSym(COL_NAME, name);
            row.putSym(COL_STATUS, STATUS_REMOVED_STR);
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
        // Seed lastWriteTimestamp from the high-water mark of the persisted system
        // table. Without this, after a process restart (or after the in-memory state
        // is cleared) nextTimestamp() falls back to the wall clock; if the wall clock
        // ever moves backwards (NTP correction, clock skew, manual change) a fresh
        // CREATE/DROP row could land with a ts smaller than the latest persisted row
        // and be silently shadowed by LATEST ON ts PARTITION BY name.
        restoreLastWriteTimestamp(executionContext);
        isInitialized = true;
        LOG.info().$("payload transform system table initialized [table=").$(tableName).$(']').$();
    }

    public boolean hasTransform(SqlExecutionContext executionContext, CharSequence name) throws SqlException {
        if (engine.getTableTokenIfExists(tableName) == null) {
            return false;
        }
        // Single-quote escaping is safe here: name is a SYMBOL column and the query
        // uses QuestDB's own SQL parser, which does not support multi-statement or
        // escape sequences beyond ''. Parameterized queries are not available for
        // internal system-table lookups.
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
        // Check cache first - zero-allocation CharSequence lookup
        final PayloadTransformDefinition cached = cache.get(name);
        if (cached != null) {
            def.copyFrom(cached);
            return def;
        }

        // Cache miss - query system table only if it exists
        if (engine.getTableTokenIfExists(tableName) == null) {
            return null;
        }
        final String nameStr = Chars.toString(name);
        // Single-quote escaping is safe here: name is a SYMBOL column and the query
        // uses QuestDB's own SQL parser, which does not support multi-statement or
        // escape sequences beyond ''. Parameterized queries are not available for
        // internal system-table lookups.
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
                cache.putIfAbsent(nameStr, cacheEntry);
                return def;
            }
        }
        return null;
    }

    private long nextTimestamp() {
        final long now = engine.getConfiguration().getMicrosecondClock().getTicks();
        long last;
        long next;
        do {
            last = lastWriteTimestamp.get();
            next = Math.max(now, last + 1);
        } while (!lastWriteTimestamp.compareAndSet(last, next));
        return next;
    }

    /**
     * Read the high-water-mark ts from the persisted system table and seed
     * {@link #lastWriteTimestamp}. Called from {@link #init} so that the
     * monotonic timestamp generator survives process restarts and clock skew.
     * Failure to read is logged and ignored: an unreadable system table at
     * startup must not block the rest of the engine from coming up.
     */
    private void restoreLastWriteTimestamp(SqlExecutionContext executionContext) {
        final String sql = "SELECT max(ts) FROM \"" + tableName + "\"";
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.query().$(sql).compile(executionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            if (cursor.hasNext()) {
                final long maxTs = cursor.getRecord().getTimestamp(0);
                if (maxTs != Long.MIN_VALUE) {
                    // Use updateAndGet rather than set so we never move the watermark
                    // backwards if another writer raced ahead between the SELECT and here.
                    lastWriteTimestamp.updateAndGet(current -> Math.max(current, maxTs));
                    LOG.info().$("restored payload transform timestamp watermark [ts=").$(maxTs).$(']').$();
                }
            }
        } catch (Throwable e) {
            // Defensive: any failure here (SQL compile error, IO error, broken
            // table state) must not block engine startup. The store will fall
            // back to the wall clock for the very first nextTimestamp() call,
            // which is a regression to pre-fix behaviour but never worse.
            LOG.error().$("failed to restore payload transform timestamp watermark [msg=")
                    .$safe(e.getMessage()).$(']').$();
        }
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
