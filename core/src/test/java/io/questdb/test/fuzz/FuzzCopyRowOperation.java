package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;

/** An owned, replayable row snapshot. Symbols are captured as strings, never table-local IDs. */
public class FuzzCopyRowOperation implements FuzzTransactionOperation {
    private final RecordChain chain;
    private final GenericRecordMetadata metadata;
    private final long timestamp;
    private final IntList writerIndexes = new IntList();
    private RecordToRowCopier copier;
    private long copierVersion = -1;

    FuzzCopyRowOperation(CairoEngine engine, RecordMetadata metadata, Record record, IntList writerIndexes) {
        this.metadata = GenericRecordMetadata.copyOf(metadata);
        this.writerIndexes.addAll(writerIndexes);
        this.timestamp = record.getTimestamp(metadata.getTimestampIndex());
        EntityColumnFilter columns = new EntityColumnFilter();
        columns.of(metadata.getColumnCount());
        chain = new RecordChain(metadata, RecordSinkFactory.getInstance(
                engine.getConfiguration(), new BytecodeAssembler(), metadata, columns), 4096, Integer.MAX_VALUE);
        try {
            chain.put(record, -1);
        } catch (Throwable error) {
            chain.close();
            throw error;
        }
    }

    @Override
    public synchronized boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI writer, int virtualTimestampIndex, LongList excludedTsIntervals) {
        if (virtualTimestampIndex != -1) {
            throw new IllegalArgumentException("row snapshots require a designated timestamp");
        }
        if (excludedTsIntervals != null && IntervalUtils.isInIntervals(excludedTsIntervals, timestamp)) {
            return false;
        }
        final var target = writer.getMetadata();
        if (copier == null || copierVersion != target.getMetadataVersion()) {
            final ListColumnFilter columns = new ListColumnFilter();
            for (int i = 0; i < writerIndexes.size(); i++) {
                final int writerIndex = writerIndexes.getQuick(i);
                // The copier skips the designated timestamp. Map dropped source columns
                // there, leaving newly added columns NULL, even when they reuse a name.
                final int destination = writerIndex < target.getColumnCount() && target.getColumnType(writerIndex) > 0
                        ? writerIndex : target.getTimestampIndex();
                columns.add(destination + 1);
            }
            copier = RecordToRowCopierUtils.generateCopier(new BytecodeAssembler(), metadata,
                    target, columns, engine.getConfiguration());
            copierVersion = target.getMetadataVersion();
        }
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
            TableWriter.Row row = writer.newRow(timestamp);
            try {
                copier.copy(context, chain.getRecordAt(0), row);
                row.append();
            } catch (Throwable error) {
                row.cancel();
                throw error;
            }
        }
        return false;
    }

    @Override
    public void close() {
        chain.close();
    }

    public long getTimestamp() {
        return timestamp;
    }

    FuzzCopyRowOperation copy(CairoEngine engine) {
        return new FuzzCopyRowOperation(engine, metadata, chain.getRecordAt(0), writerIndexes);
    }
}
