package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

/** Adds dedup/key/schema history and detached row copies to an insert-only transaction stream. */
public class FuzzDedupTransactionGenerator {
    /**
     * Returns an independently owned stream. The caller must close both streams.
     * Samples actual source-table rows before either replay, including nulls and variable data.
     * Every data transaction mixes new rows with exact copies; rollback flags are preserved.
     * Barriers preserve commit order even when multiple WAL writers replay the stream.
     */
    public static ObjList<FuzzTransaction> augment(
            CairoEngine engine,
            SqlExecutionContext context,
            String sourceTable,
            ObjList<FuzzTransaction> input,
            IntList enabledKeys,
            Rnd rnd,
            int toggleCount,
            int copiesPerTransaction,
            int structureVersionOffset
    ) throws SqlException {
        return augment(engine, context, sourceTable, input, enabledKeys, rnd, toggleCount,
                copiesPerTransaction, structureVersionOffset, null);
    }

    /**
     * With a schema column, four key changes also drop and re-add that column, using
     * a random new type. The stream then has six metadata changes and ends with dedup off.
     * Existing source rows are copied by physical writer identity, never by reused names.
     */
    public static ObjList<FuzzTransaction> augment(
            CairoEngine engine,
            SqlExecutionContext context,
            String sourceTable,
            ObjList<FuzzTransaction> input,
            IntList enabledKeys,
            Rnd rnd,
            int toggleCount,
            int copiesPerTransaction,
            int structureVersionOffset,
            String schemaColumn
    ) throws SqlException {
        if (schemaColumn != null && toggleCount != 4) {
            throw new IllegalArgumentException("schema history requires four key changes");
        }
        if (toggleCount < 1 || toggleCount >= input.size() || copiesPerTransaction < 1) {
            throw new IllegalArgumentException("need data between dedup changes and at least one copied row per transaction");
        }
        int structureVersion = input.getQuick(0).structureVersion;
        for (int i = 0; i < input.size(); i++) {
            FuzzTransaction transaction = input.getQuick(i);
            if (transaction.structureVersion != structureVersion || transaction.reopenTable
                    || transaction.hasReplaceRange() || transaction.getNoCommitIntervals() != null) {
                throw new IllegalArgumentException("dedup augmentation requires fixed-schema insert transactions");
            }
            for (int j = 0; j < transaction.operationList.size(); j++) {
                if (transaction.operationList.getQuick(j).getClass() != FuzzInsertOperation.class) {
                    throw new IllegalArgumentException("dedup augmentation requires ordinary inserts");
                }
            }
        }
        boolean isEnabled;
        int timestamp;
        int schemaIndex = -1;
        int nextWriterIndex;
        final IntList candidates = new IntList();
        final IntList writerIndexes = new IntList();
        StringBuilder sql = new StringBuilder("SELECT ");
        try (var reader = engine.getReader(sourceTable)) {
            final var metadata = reader.getMetadata();
            final int timestampIndex = metadata.getTimestampIndex();
            if (timestampIndex < 0 || !enabledKeys.contains(timestampIndex)) {
                throw new IllegalArgumentException("dedup keys must include the designated timestamp");
            }
            timestamp = metadata.getWriterIndex(timestampIndex);
            nextWriterIndex = metadata.getWriterColumnCount();
            if (schemaColumn != null) {
                schemaIndex = metadata.getColumnIndexQuiet(schemaColumn);
                if (schemaIndex < 0 || schemaIndex == timestampIndex) {
                    throw new IllegalArgumentException("schema column must exist and must not be the timestamp");
                }
                schemaIndex = metadata.getWriterIndex(schemaIndex);
            }
            for (int i = 0; i < enabledKeys.size(); i++) {
                int key = enabledKeys.getQuick(i);
                if (key < 0 || key >= metadata.getColumnCount() || metadata.getColumnType(key) <= 0) {
                    throw new IllegalArgumentException("invalid dedup column index");
                }
                final int writerIndex = metadata.getWriterIndex(key);
                if (writerIndex != schemaIndex) {
                    candidates.add(writerIndex);
                }
            }
            isEnabled = metadata.isDedupKey(timestampIndex);
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                if (metadata.getColumnType(i) <= 0) {
                    continue;
                }
                if (writerIndexes.size() > 0) {
                    sql.append(',');
                }
                writerIndexes.add(metadata.getWriterIndex(i));
                String name = quote(metadata.getColumnName(i));
                sql.append(name);
                // Detach symbol values from the source table's dictionaries.
                if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                    sql.append("::STRING AS ").append(name);
                }
            }
        }
        sql.append(" FROM ").append(quote(sourceTable));
        ObjList<FuzzCopyRowOperation> samples = new ObjList<>();
        ObjList<FuzzTransaction> output = new ObjList<>();
        try {
            try (var compiler = engine.getSqlCompiler();
                 var factory = compiler.compile(sql, context).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(context)) {
                long size = cursor.size();
                if (size < 0) {
                    size = 0;
                    while (cursor.hasNext()) {
                        size++;
                    }
                    cursor.toTop();
                }
                if (size == 0) {
                    throw new IllegalArgumentException("duplicate source table is empty");
                }
                LongList selected = new LongList();
                for (int i = 0, n = (int) Math.min(size, copiesPerTransaction * 2L); i < n; i++) {
                    selected.add(rnd.nextLong(size));
                }
                selected.sort();
                long row = 0;
                int sample = 0;
                while (sample < selected.size() && cursor.hasNext()) {
                    while (sample < selected.size() && selected.getQuick(sample) == row) {
                        samples.add(new FuzzCopyRowOperation(engine, factory.getMetadata(), cursor.getRecord(), writerIndexes));
                        sample++;
                    }
                    row++;
                }
                if (samples.size() != selected.size()) {
                    throw new IllegalStateException("source table changed while sampling duplicates");
                }
            }
            LongList positions = new LongList();
            for (int i = 1; i < input.size(); i++) {
                positions.add(i);
            }
            for (int i = 0; i < toggleCount; i++) {
                int picked = i + rnd.nextInt(positions.size() - i);
                long value = positions.getQuick(i);
                positions.setQuick(i, positions.getQuick(picked));
                positions.setQuick(picked, value);
            }
            positions.setPos(toggleCount);
            positions.sort();
            structureVersion += structureVersionOffset;
            int toggle = 0;
            for (int i = 0; i < input.size(); i++) {
                if (toggle < toggleCount && positions.getQuick(toggle) == i) {
                    final IntList keys;
                    if (schemaColumn == null) {
                        isEnabled = !isEnabled;
                        keys = isEnabled ? candidates : new IntList();
                    } else {
                        if (toggle == 1) {
                            transaction(output, structureVersion++).operationList.add(new FuzzDropColumnOperation(rnd, schemaColumn));
                            schemaIndex = -1;
                        } else if (toggle == 2) {
                            final int type = switch (rnd.nextInt(4)) {
                                case 0 -> ColumnType.LONG;
                                case 1 -> ColumnType.STRING;
                                case 2 -> ColumnType.SYMBOL;
                                default -> ColumnType.VARCHAR;
                            };
                            transaction(output, structureVersion++).operationList.add(
                                    new FuzzAddColumnOperation(schemaColumn, type, false, 256, true));
                            schemaIndex = nextWriterIndex++;
                        }
                        keys = toggle == 3 ? new IntList() : randomKeys(rnd, candidates, timestamp, schemaIndex);
                    }
                    transaction(output, structureVersion++).operationList.add(new FuzzDedupOperation(keys));
                    toggle++;
                }
                FuzzTransaction original = input.getQuick(i);
                FuzzTransaction transaction = transaction(output, structureVersion);
                transaction.rollback = original.rollback;
                for (int j = 0; j < original.operationList.size(); j++) {
                    transaction.operationList.add(new FuzzInsertOperation((FuzzInsertOperation) original.operationList.getQuick(j)));
                }
                for (int j = 0; j < copiesPerTransaction; j++) {
                    var copy = samples.getQuick(rnd.nextInt(samples.size())).copy(engine);
                    transaction.operationList.insert(rnd.nextInt(transaction.operationList.size() + 1), 1, copy);
                }
            }
            return output;
        } catch (Throwable error) {
            Misc.freeObjListAndClear(output);
            throw error;
        } finally {
            Misc.freeObjListAndClear(samples);
        }
    }

    private static String quote(CharSequence name) {
        return "\"" + name.toString().replace("\"", "\"\"") + "\"";
    }

    private static IntList randomKeys(Rnd rnd, IntList candidates, int timestamp, int schemaIndex) {
        final IntList keys = new IntList();
        keys.add(timestamp);
        for (int i = 0; i < candidates.size(); i++) {
            final int key = candidates.getQuick(i);
            if (key != timestamp && rnd.nextBoolean()) {
                keys.add(key);
            }
        }
        if (schemaIndex >= 0) {
            keys.add(schemaIndex);
        }
        return keys;
    }

    private static FuzzTransaction transaction(ObjList<FuzzTransaction> output, int structureVersion) {
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.structureVersion = structureVersion;
        transaction.waitBarrierVersion = output.size();
        transaction.waitAllDone = true;
        output.add(transaction);
        return transaction;
    }
}
