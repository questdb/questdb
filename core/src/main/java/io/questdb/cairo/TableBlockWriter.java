package io.questdb.cairo;

import static io.questdb.cairo.TableUtils.iFile;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.microtime.Timestamps;
import io.questdb.std.str.Path;

public class TableBlockWriter implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableBlockWriter.class);
    private static final Timestamps.TimestampFloorMethod NO_PARTITIONING_FLOOR = (ts) -> {
        return 0;
    };

    private TableWriter writer;
    private final CharSequence root;
    private final FilesFacade ff;
    private final int mkDirMode;
    private final Path path = new Path();
    private final RingQueue<TableBlockWriterTaskHolder> queue;
    private final Sequence pubSeq;
    private final LongList columnRowsAdded = new LongList();

    private int rootLen;
    private RecordMetadata metadata;
    private int columnCount;
    private int partitionBy;
    private Timestamps.TimestampFloorMethod timestampFloorMethod;
    private int timestampColumnIndex;
    private long firstTimestamp;
    private long lastTimestamp;
    private long nRowsAdded;
    private int firstColumnPow2Size;

    private final LongObjHashMap<PartitionBlockWriter> partitionBlockWriterByTimestamp = new LongObjHashMap<>();
    private final ObjList<PartitionBlockWriter> partitionBlockWriters = new ObjList<>();
    private int nextPartitionBlockWriterIndex;
    private final ObjList<TableBlockWriterTask> concurrentTasks = new ObjList<>();
    private int nEnqueuedConcurrentTasks;
    private final AtomicInteger nCompletedConcurrentTasks = new AtomicInteger();;

    TableBlockWriter(CairoConfiguration configuration, MessageBus messageBus) {
        root = configuration.getRoot();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        queue = messageBus.getTableBlockWriterQueue();
        pubSeq = messageBus.getTableBlockWriterPubSequence();
    }

    public void appendBlock(long partitionTimestamp, int columnIndex, long blockLength, long sourceAddress) {
        if (columnIndex == 0) {
            nRowsAdded += blockLength >> firstColumnPow2Size;
        }
        if (columnIndex == timestampColumnIndex) {
            long firstBlockTimetamp = Unsafe.getUnsafe().getLong(sourceAddress);
            if (firstBlockTimetamp < firstTimestamp) {
                firstTimestamp = firstBlockTimetamp;
            }
            long addr = sourceAddress + blockLength - Long.BYTES;
            long lastBlockTimestamp = Unsafe.getUnsafe().getLong(addr);
            if (lastBlockTimestamp > lastTimestamp) {
                lastTimestamp = lastBlockTimestamp;
            }
        }
        PartitionBlockWriter partWriter = getPartitionBlockWriter(partitionTimestamp);
        if (sourceAddress != 0) {
            TableBlockWriterTask task = getConcurrentTask();
            task.assignAppendBlock(partWriter, columnIndex, blockLength, sourceAddress);
            enqueueConcurrentTask(task);
        } else {
            partWriter.setColumnTop(columnIndex, blockLength);
        }
    }

    private TableBlockWriterTask getConcurrentTask() {
        if (concurrentTasks.size() <= nEnqueuedConcurrentTasks) {
            concurrentTasks.extendAndSet(nEnqueuedConcurrentTasks, new TableBlockWriterTask());
        }
        return concurrentTasks.getQuick(nEnqueuedConcurrentTasks);
    }

    private void enqueueConcurrentTask(TableBlockWriterTask task) {
        assert concurrentTasks.getQuick(nEnqueuedConcurrentTasks) == task;
        assert !task.ready.get();
        task.ready.set(true);
        nEnqueuedConcurrentTasks++;

        long seq = pubSeq.next();
        if (seq < 0) {
            task.run();
            return;
        }
        try {
            queue.get(seq).task = task;
        } finally {
            pubSeq.done(seq);
        }
    }

    private void completePendingConcurrentTasks(boolean cancel) {
        if (nCompletedConcurrentTasks.get() < nEnqueuedConcurrentTasks) {
            for (int n = 0; n < nEnqueuedConcurrentTasks; n++) {
                TableBlockWriterTask task = concurrentTasks.getQuick(n);
                if (cancel) {
                    task.cancel();
                } else {
                    task.run();
                }
            }
        }

        while (nCompletedConcurrentTasks.get() < nEnqueuedConcurrentTasks) {
            LockSupport.parkNanos(0);
        }
        nEnqueuedConcurrentTasks = 0;
        nCompletedConcurrentTasks.set(0);
    }

    public void appendSymbolCharsBlock(int columnIndex, long blockLength, long sourceAddress) {
        writer.getSymbolMapWriter(columnIndex).appendSymbolCharsBlock(blockLength, sourceAddress);
    }

    public void commit() {
        LOG.info().$("committing block write of ").$(nRowsAdded).$(" rows to ").$(path).$(" [firstTimestamp=")
                .$ts(firstTimestamp).$(", lastTimestamp=").$ts(lastTimestamp).$(']').$();
        PartitionBlockWriter partWriter = getPartitionBlockWriter(firstTimestamp);
        // Need to complete all data tasks before we can start index tasks
        completePendingConcurrentTasks(false);
        partWriter.startCommitAppendedBlock(firstTimestamp, lastTimestamp, nRowsAdded);
        completePendingConcurrentTasks(false);
        writer.commitBlock(firstTimestamp, lastTimestamp, nRowsAdded);
        partWriter.clear();
        firstTimestamp = Long.MAX_VALUE;
        lastTimestamp = Long.MIN_VALUE;
        this.nRowsAdded = 0;
        LOG.info().$("commited new block [table=").$(writer.getName()).$(']').$();
    }

    public void cancel() {
        completePendingConcurrentTasks(true);
        writer.cancelRow();
        LOG.info().$("cancelled new block [table=").$(writer.getName()).$(']').$();
    }

    void open(TableWriter writer) {
        clear();
        this.writer = writer;
        metadata = writer.getMetadata();
        path.of(root).concat(writer.getName());
        rootLen = path.length();
        columnCount = metadata.getColumnCount();
        partitionBy = writer.getPartitionBy();
        columnRowsAdded.ensureCapacity(columnCount);
        timestampColumnIndex = metadata.getTimestampIndex();
        firstTimestamp = timestampColumnIndex >= 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        lastTimestamp = timestampColumnIndex >= 0 ? Long.MIN_VALUE : 0;
        nRowsAdded = 0;
        firstColumnPow2Size = ColumnType.pow2SizeOf(metadata.getColumnType(0));
        nEnqueuedConcurrentTasks = 0;
        nCompletedConcurrentTasks.set(0);
        switch (partitionBy) {
            case PartitionBy.DAY:
                timestampFloorMethod = Timestamps.FLOOR_DD;
                break;
            case PartitionBy.MONTH:
                timestampFloorMethod = Timestamps.FLOOR_MM;
                break;
            case PartitionBy.YEAR:
                timestampFloorMethod = Timestamps.FLOOR_YYYY;
                break;
            default:
                timestampFloorMethod = NO_PARTITIONING_FLOOR;
                break;
        }
        LOG.info().$("started new block [table=").$(writer.getName()).$(']').$();
    }

    void clear() {
        if (nCompletedConcurrentTasks.get() < nEnqueuedConcurrentTasks) {
            LOG.error().$("new block should have been either committed or cancelled [table=").$(writer.getName()).$(']').$();
            completePendingConcurrentTasks(true);
        }
        metadata = null;
        writer = null;
        for (int i = 0; i < nextPartitionBlockWriterIndex; i++) {
            partitionBlockWriters.getQuick(i).clear();
        }
        nextPartitionBlockWriterIndex = 0;
        partitionBlockWriterByTimestamp.clear();
    }

    @Override
    public void close() {
        clear();
        for (int i = 0, sz = partitionBlockWriters.size(); i < sz; i++) {
            partitionBlockWriters.getQuick(i).close();
        }
        partitionBlockWriters.clear();
        path.close();
    }

    private PartitionBlockWriter getPartitionBlockWriter(long timestamp) {
        long timestampLo = timestampFloorMethod.floor(timestamp);
        PartitionBlockWriter partWriter = partitionBlockWriterByTimestamp.get(timestampLo);
        if (null == partWriter) {
            assert nextPartitionBlockWriterIndex <= partitionBlockWriters.size();
            if (nextPartitionBlockWriterIndex == partitionBlockWriters.size()) {
                partWriter = new PartitionBlockWriter();
                partitionBlockWriters.extendAndSet(nextPartitionBlockWriterIndex, partWriter);
            } else {
                partWriter = partitionBlockWriters.getQuick(nextPartitionBlockWriterIndex);
            }
            nextPartitionBlockWriterIndex++;
            partitionBlockWriterByTimestamp.put(timestampLo, partWriter);
            partWriter.of(timestampLo);
        }

        partWriter.open();
        return partWriter;
    }

    private class PartitionBlockWriter {
        private final ObjList<AppendMemory> columns = new ObjList<>();
        private final LongList columnTops = new LongList();
        private long timestampLo;
        private boolean opened;

        private void of(long timestampLo) {
            this.timestampLo = timestampLo;
            opened = false;
            int columnsSize = columns.size();
            columnTops.ensureCapacity(columnCount);
            int requiredColumnsSize = columnCount << 1;
            while (columnsSize < requiredColumnsSize) {
                int columnIndex = columnsSize >> 1;
                columns.extendAndSet(columnsSize++, new AppendMemory());
                switch (metadata.getColumnType(columnIndex)) {
                    case ColumnType.STRING:
                    case ColumnType.BINARY:
                        columns.extendAndSet(columnsSize, new AppendMemory());
                        break;
                    default:
                        columns.extendAndSet(columnsSize, null);
                }
                columnsSize++;
            }
        }

        private void open() {
            if (!opened) {
                try {
                    TableUtils.setPathForPartition(path, partitionBy, timestampLo);
                    int plen = path.length();
                    if (ff.mkdirs(path.put(Files.SEPARATOR).$(), mkDirMode) != 0) {
                        throw CairoException.instance(ff.errno()).put("Cannot create directory: ").put(path);
                    }

                    assert columnCount > 0;
                    columnTops.setAll(columnCount, -1);
                    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                        final CharSequence name = metadata.getColumnName(columnIndex);
                        int i = columnIndex * 2;
                        AppendMemory mem = columns.getQuick(i);
                        mem.of(ff, TableUtils.dFile(path.trimTo(plen), name), ff.getMapPageSize());
                        mem.jumpTo(writer.getPrimaryAppendOffset(timestampLo, columnIndex));

                        AppendMemory imem = columns.getQuick(++i);
                        if (imem != null) {
                            imem.of(ff, iFile(path.trimTo(plen), name), ff.getMapPageSize());
                            imem.jumpTo(writer.getSecondaryAppendOffset(timestampLo, columnIndex));
                        }

                    }

                    opened = true;
                    LOG.info().$("opened partition to '").$(path).$('\'').$();
                } finally {
                    path.trimTo(rootLen);
                }
            }
        }

        private void appendBlock(int columnIndex, long blockLength, long sourceAddress) {
            AppendMemory mem = columns.getQuick(columnIndex * 2);
            long appendOffset = mem.getAppendOffset();
            try {
                mem.putBlockOfBytes(sourceAddress, blockLength);
            } finally {
                mem.jumpTo(appendOffset);
            }
        }

        private void setColumnTop(int columnIndex, long columnTop) {
            columnTops.set(columnIndex, columnTop);
        }

        private void startCommitAppendedBlock(long firstTimestamp, long lastTimestamp, long nRowsAdded) {
            writer.startAppendedBlock(firstTimestamp, lastTimestamp, nRowsAdded, columnTops, columnRowsAdded);

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                int columnType = metadata.getColumnType(columnIndex);
                long colNRowsAdded = columnRowsAdded.getQuick(columnIndex);

                // Add binary and string indexes
                switch (columnType) {

                    case ColumnType.STRING: {
                        TableBlockWriterTask task = getConcurrentTask();
                        task.assignUpdateStringIndex(this, columnIndex, colNRowsAdded);
                        enqueueConcurrentTask(task);
                        break;
                    }

                    case ColumnType.BINARY: {
                        TableBlockWriterTask task = getConcurrentTask();
                        task.assignUpdateBinaryIndex(this, columnIndex, colNRowsAdded);
                        enqueueConcurrentTask(task);
                        break;
                    }

                    case ColumnType.SYMBOL: {
                        TableBlockWriterTask task = getConcurrentTask();
                        task.assignUpdateSymbolCache(this, columnIndex, colNRowsAdded);
                        enqueueConcurrentTask(task);
                        break;
                    }

                    default:
                }
            }
        }

        private void updateSymbolCache(int columnIndex, long colNRowsAdded) {
            int i = columnIndex << 1;
            AppendMemory mem = columns.getQuick(i++);
            int nSymbols = Vect.maxInt(mem.addressOf(mem.getAppendOffset()), colNRowsAdded);
            nSymbols++;
            SymbolMapWriter symWriter = writer.getSymbolMapWriter(columnIndex);
            if (nSymbols > symWriter.getSymbolCount()) {
                symWriter.commitAppendedBlock(nSymbols - symWriter.getSymbolCount());
            }
        }

        private void updateBinaryIndex(int columnIndex, long colNRowsAdded) {
            int i = columnIndex << 1;
            AppendMemory mem = columns.getQuick(i++);
            AppendMemory imem = columns.getQuick(i);

            long offset = mem.getAppendOffset();
            for (int row = 0; row < colNRowsAdded; row++) {
                imem.putLong(offset);
                mem.jumpTo(offset);
                long binLen = mem.getBinLen(offset);
                if (binLen == TableUtils.NULL_LEN) {
                    offset += Long.BYTES;
                } else {
                    offset += Long.BYTES + binLen;
                }
            }
        }

        private void updateStringIndex(int columnIndex, long colNRowsAdded) {
            int i = columnIndex << 1;
            AppendMemory mem = columns.getQuick(i++);
            AppendMemory imem = columns.getQuick(i);

            long offset = mem.getAppendOffset();
            for (int row = 0; row < colNRowsAdded; row++) {
                imem.putLong(offset);
                mem.jumpTo(offset);
                int strLen = mem.getStrLen(offset);
                if (strLen == TableUtils.NULL_LEN) {
                    offset += VirtualMemory.STRING_LENGTH_BYTES;
                } else {
                    offset += VirtualMemory.STRING_LENGTH_BYTES + 2 * strLen;
                }
            }
        }

        private void clear() {
            for (int i = 0, sz = columns.size(); i < sz; i++) {
                AppendMemory mem = columns.getQuick(i);
                if (null != mem) {
                    mem.close(false);
                }
            }
            opened = false;
        }

        private void close() {
            columns.clear();
            timestampLo = 0;
            opened = false;
        }
    }

    public static class TableBlockWriterTaskHolder {
        private TableBlockWriterTask task;
    }

    private enum TaskType {
        AppendBlock, GenerateStringIndex, GenerateBinaryIndex, UpdateSymbolCache
    };

    private class TableBlockWriterTask {
        private TaskType taskType;
        private final AtomicBoolean ready = new AtomicBoolean(false);
        private PartitionBlockWriter blockWriter;
        private int columnIndex;
        private long blockLength;
        private long sourceAddress;
        private long colNRowsAdded;

        private boolean run() {
            if (ready.compareAndSet(true, false)) {
                try {
                    switch (taskType) {
                        case AppendBlock:
                            blockWriter.appendBlock(columnIndex, blockLength, sourceAddress);
                            return true;

                        case GenerateStringIndex:
                            blockWriter.updateStringIndex(columnIndex, colNRowsAdded);
                            return true;

                        case GenerateBinaryIndex:
                            blockWriter.updateBinaryIndex(columnIndex, colNRowsAdded);
                            return true;

                        case UpdateSymbolCache:
                            blockWriter.updateSymbolCache(columnIndex, colNRowsAdded);
                            return true;
                    }
                } finally {
                    nCompletedConcurrentTasks.incrementAndGet();
                }
            }

            return false;
        }

        private void cancel() {
            if (ready.compareAndSet(true, false)) {
                nCompletedConcurrentTasks.incrementAndGet();
            }
        }

        private void assignAppendBlock(
                PartitionBlockWriter partWriter, int columnIndex, long blockLength,
                long sourceAddress
        ) {
            taskType = TaskType.AppendBlock;
            this.blockWriter = partWriter;
            this.columnIndex = columnIndex;
            this.blockLength = blockLength;
            this.sourceAddress = sourceAddress;
        }

        private void assignUpdateStringIndex(PartitionBlockWriter partWriter, int columnIndex, long colNRowsAdded) {
            taskType = TaskType.GenerateStringIndex;
            this.blockWriter = partWriter;
            this.columnIndex = columnIndex;
            this.colNRowsAdded = colNRowsAdded;
        }

        private void assignUpdateBinaryIndex(PartitionBlockWriter partWriter, int columnIndex, long colNRowsAdded) {
            taskType = TaskType.GenerateBinaryIndex;
            this.blockWriter = partWriter;
            this.columnIndex = columnIndex;
            this.colNRowsAdded = colNRowsAdded;
        }

        private void assignUpdateSymbolCache(PartitionBlockWriter partWriter, int columnIndex, long colNRowsAdded) {
            taskType = TaskType.UpdateSymbolCache;
            this.blockWriter = partWriter;
            this.columnIndex = columnIndex;
            this.colNRowsAdded = colNRowsAdded;
        }
    }

    public static class TableBlockWriterJob implements Job {
        private final RingQueue<TableBlockWriterTaskHolder> queue;
        private final Sequence subSeq;

        public TableBlockWriterJob(MessageBus messageBus) {
            this.queue = messageBus.getTableBlockWriterQueue();
            this.subSeq = messageBus.getTableBlockWriterSubSequence();
        }

        @Override
        public boolean run(int workerId) {
            boolean useful = false;
            while (true) {
                long cursor = subSeq.next();
                if (cursor == -1) {
                    return useful;
                }

                if (cursor != -2) {
                    try {
                        final TableBlockWriterTaskHolder holder = queue.get(cursor);
                        useful |= holder.task.run();
                        holder.task = null;
                    } finally {
                        subSeq.done(cursor);
                    }
                }
            }
        }
    }
}
