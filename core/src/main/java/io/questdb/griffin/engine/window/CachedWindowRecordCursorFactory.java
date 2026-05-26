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

package io.questdb.griffin.engine.window;


import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.orderby.SortKeyType;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CachedWindowRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<WindowFunction> allFunctions;
    private final RecordCursorFactory base;
    private final GenericRecordMetadata chainMetadata;
    private final CachedWindowRecordCursor cursor;
    private final ObjList<ObjList<WindowFunction>> ordered2PassFunctions;
    private final ObjList<ObjList<WindowFunction>> orderedFunctions;
    private final int orderedGroupCount;
    private final ObjList<IntList> sortKeys;
    private final ObjList<WindowFunction> unordered2PassFunctions;
    @Nullable
    private final ObjList<WindowFunction> unorderedFunctions;
    private boolean closed = false;

    public CachedWindowRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordSink recordSink,
            GenericRecordMetadata metadata,
            @Transient ColumnTypes chainTypes,
            ObjList<RecordComparator> comparators,
            ObjList<ObjList<WindowFunction>> orderedFunctions,
            @Nullable ObjList<WindowFunction> unorderedFunctions,
            @NotNull IntList columnIndexes,
            @NotNull final ObjList<IntList> sortKeys,
            @NotNull GenericRecordMetadata chainMetadata
    ) {
        super(metadata);
        try {
            this.base = base;
            this.orderedGroupCount = comparators.size();
            assert orderedGroupCount == orderedFunctions.size();
            this.orderedFunctions = orderedFunctions;
            RecordArray recordChain = new RecordArray(
                    chainTypes,
                    recordSink,
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages()
            );
            this.sortKeys = sortKeys;
            this.chainMetadata = chainMetadata;

            ObjList<OrderedGroupSort> orderedGroups = new ObjList<>(orderedGroupCount);
            try {
                for (int i = 0; i < orderedGroupCount; i++) {
                    final IntList groupKeys = sortKeys.getQuick(i);
                    if (comparators.getQuiet(i) == null) {
                        orderedGroups.add(new EncodedOrderedGroupSort(configuration, chainMetadata, groupKeys));
                    } else {
                        orderedGroups.add(new TreeOrderedGroupSort(
                                configuration,
                                comparators.getQuick(i),
                                SortKeyEncoder.createRankMaps(chainMetadata, groupKeys)
                        ));
                    }
                }
            } catch (Throwable t) {
                Misc.freeObjList(orderedGroups);
                Misc.free(recordChain);
                throw t;
            }

            this.cursor = new CachedWindowRecordCursor(columnIndexes, recordChain, orderedGroups);
            this.allFunctions = new ObjList<>();

            ObjList<ObjList<WindowFunction>> orderedTmp = null;
            for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
                ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                allFunctions.addAll(functions);

                ObjList<WindowFunction> twoPassFunctions = null;
                for (int j = 0, k = functions.size(); j < k; j++) {
                    WindowFunction function = functions.getQuick(j);
                    if (function.getPassCount() > WindowFunction.ONE_PASS) {
                        if (twoPassFunctions == null) {
                            twoPassFunctions = new ObjList<>();
                        }
                        twoPassFunctions.add(function);
                    }
                }
                if (twoPassFunctions != null) {
                    if (orderedTmp == null) {
                        orderedTmp = new ObjList<>();
                    }

                    orderedTmp.extendAndSet(i, twoPassFunctions);
                }
            }

            ordered2PassFunctions = orderedTmp;

            ObjList<WindowFunction> unorderedTmp = null;
            if (unorderedFunctions != null) {
                allFunctions.addAll(unorderedFunctions);

                for (int i = 0, n = unorderedFunctions.size(); i < n; i++) {
                    WindowFunction function = unorderedFunctions.getQuick(i);
                    if (function.getPassCount() > WindowFunction.ONE_PASS) {
                        if (unorderedTmp == null) {
                            unorderedTmp = new ObjList<>();
                        }
                        unorderedTmp.add(function);
                    }
                }
            }
            this.unordered2PassFunctions = unorderedTmp;

            this.unorderedFunctions = unorderedFunctions;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return chainMetadata.getColumnName(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        cursor.of(baseCursor, executionContext);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("CachedWindow");

        boolean oldVal = sink.getUseBaseMetadata();
        try {
            if (orderedFunctions.size() > 0) {
                sink.attr("orderedFunctions");
                sink.val("[");

                sink.useBaseMetadata(true);

                for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
                    if (i > 0) {
                        sink.val(',');
                    }
                    sink.val('[');

                    addSortKeys(sink, sortKeys.getQuick(i));

                    sink.val("] => [");
                    ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                    for (int j = 0, k = functions.size(); j < k; j++) {
                        if (j > 0) {
                            sink.val(',');
                        }
                        sink.val(functions.getQuick(j));
                    }

                    sink.val("]");
                }
                sink.val(']');
            }

            sink.optAttr("unorderedFunctions", unorderedFunctions, true);
        } finally {
            sink.useBaseMetadata(oldVal);
        }

        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private void addSortKeys(PlanSink sink, IntList list) {
        for (int i = 0, n = list.size(); i < n; i++) {
            int colIdx = list.get(i);
            int col = (colIdx > 0 ? colIdx : -colIdx) - 1;
            if (i > 0) {
                sink.val(", ");
            }
            sink.val(chainMetadata.getColumnName(col));
            if (colIdx < 0) {
                sink.val(" ").val("desc");
            }
        }
    }

    private void resetFunctions() {
        for (int i = 0, n = allFunctions.size(); i < n; i++) {
            allFunctions.getQuick(i).reset();
        }
    }

    @Override
    protected void _close() {
        if (closed) {
            return;
        }
        closed = true;
        Misc.free(base);
        Misc.free(cursor);
        Misc.freeObjList(allFunctions);
    }

    private interface OrderedGroupSort extends QuietCloseable, Reopenable {
        void finishPut(SqlExecutionCircuitBreaker circuitBreaker);

        boolean hasNext();

        long next();

        void of(RecordCursor cursor);

        void put(Record chainRecord, long chainOffset);

        void toTop();
    }

    private static class EncodedOrderedGroupSort implements OrderedGroupSort {
        private static final long MAX_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 3;
        private final SortKeyEncoder encoder;
        private final DirectLongList entryMem;
        private final long maxEntryMemBytes;
        private final long parallelThreshold;
        private long count;
        private long currentAddr;
        private long endAddr;
        private int entrySize;
        private boolean isOpen;
        private SortKeyType keyType;
        private int longsPerEntry;
        private long maxEntries;
        private int rowIdOffset;
        private long startAddr;

        EncodedOrderedGroupSort(
                CairoConfiguration configuration,
                GenericRecordMetadata chainMetadata,
                IntList sortColumnFilter
        ) {
            this.encoder = new SortKeyEncoder(chainMetadata, sortColumnFilter);
            // Reuse SqlWindowTreeKeyPageSize as the initial buffer size so the
            // encoded path obeys the same per-group memory knob users tune for
            // the tree path. Config is in bytes; DirectLongList takes longs.
            this.entryMem = new DirectLongList(
                    Math.max(configuration.getSqlWindowTreeKeyPageSize() / Long.BYTES, 1),
                    MemoryTag.NATIVE_DEFAULT,
                    true
            );
            this.maxEntryMemBytes = Math.min(
                    configuration.getSqlSortKeyPageSize() * (long) configuration.getSqlSortKeyMaxPages()
                            + configuration.getSqlSortLightValuePageSize() * (long) configuration.getSqlSortLightValueMaxPages(),
                    MAX_HEAP_SIZE_LIMIT
            );
            this.parallelThreshold = configuration.getSqlSortEncodedParallelThreshold();
            this.isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                Misc.free(encoder);
                Misc.free(entryMem);
            }
        }

        @Override
        public void finishPut(SqlExecutionCircuitBreaker circuitBreaker) {
            if (count > 1) {
                Vect.sortEncodedEntries(
                        entryMem.getAddress(),
                        count,
                        keyType.keyLength() / Long.BYTES,
                        parallelThreshold
                );
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            }
            startAddr = entryMem.getAddress() + rowIdOffset;
            toTop();
        }

        @Override
        public boolean hasNext() {
            return currentAddr < endAddr;
        }

        @Override
        public long next() {
            long rowId = Unsafe.getLong(currentAddr);
            currentAddr += entrySize;
            return rowId;
        }

        @Override
        public void of(RecordCursor cursor) {
            keyType = encoder.init(cursor);
            assert keyType != SortKeyType.UNSUPPORTED;
            entrySize = keyType.entrySize();
            rowIdOffset = keyType.rowIdOffset();
            longsPerEntry = entrySize / Long.BYTES;
            maxEntries = maxEntryMemBytes / entrySize;
            count = 0;
            entryMem.clear();
            startAddr = entryMem.getAddress() + rowIdOffset;
            currentAddr = startAddr;
            endAddr = startAddr;
        }

        @Override
        public void put(Record chainRecord, long chainOffset) {
            if (count >= maxEntries) {
                throw LimitOverflowException.instance().put("limit of ").put(maxEntryMemBytes)
                        .put(" memory exceeded in window encoded sort");
            }
            entryMem.ensureCapacity(longsPerEntry);
            long entryAddr = entryMem.getAppendAddress();
            encoder.encode(chainRecord, entryAddr, chainOffset);
            entryMem.skip(longsPerEntry);
            count++;
        }

        @Override
        public void reopen() {
            if (!isOpen) {
                isOpen = true;
                entryMem.reopen();
            }
        }

        @Override
        public void toTop() {
            currentAddr = startAddr;
            endAddr = startAddr + count * entrySize;
        }
    }

    private static class TreeOrderedGroupSort implements OrderedGroupSort {
        private final RecordComparator comparator;
        private final ObjList<DirectIntList> rankMaps;
        private final LongTreeChain tree;
        private Record chainRightRecord;
        private LongTreeChain.TreeCursor cursor;
        private RecordCursor sourceCursor;

        TreeOrderedGroupSort(
                CairoConfiguration configuration,
                RecordComparator comparator,
                ObjList<DirectIntList> rankMaps
        ) {
            this.comparator = comparator;
            this.rankMaps = rankMaps;
            this.tree = new LongTreeChain(
                    configuration.getSqlWindowTreeKeyPageSize(),
                    configuration.getSqlWindowTreeKeyMaxPages(),
                    configuration.getSqlWindowRowIdPageSize(),
                    configuration.getSqlWindowRowIdMaxPages()
            );
        }

        @Override
        public void close() {
            Misc.free(tree);
            if (rankMaps != null) {
                Misc.freeObjListAndKeepObjects(rankMaps);
            }
            cursor = null;
            chainRightRecord = null;
            sourceCursor = null;
        }

        @Override
        public void finishPut(SqlExecutionCircuitBreaker circuitBreaker) {
        }

        @Override
        public boolean hasNext() {
            return cursor != null && cursor.hasNext();
        }

        @Override
        public long next() {
            return cursor.next();
        }

        @Override
        public void of(RecordCursor cursor) {
            this.sourceCursor = cursor;
            this.chainRightRecord = cursor.getRecordB();
            SortKeyEncoder.buildRankMaps(cursor, rankMaps, comparator);
        }

        @Override
        public void put(Record chainRecord, long chainOffset) {
            tree.put(chainRecord, sourceCursor, chainRightRecord, comparator);
        }

        @Override
        public void reopen() {
            tree.reopen();
        }

        @Override
        public void toTop() {
            cursor = tree.getCursor();
        }
    }

    class CachedWindowRecordCursor implements RecordCursor {
        private final IntList columnIndexes;
        private final ObjList<OrderedGroupSort> orderedGroups;
        private final RecordArray recordChain;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;
        private boolean isRecordChainBuilt;
        private long recordChainOffset;

        public CachedWindowRecordCursor(IntList columnIndexes, RecordArray recordChain, ObjList<OrderedGroupSort> orderedGroups) {
            this.columnIndexes = columnIndexes;
            this.recordChain = recordChain;
            this.recordChain.setSymbolTableResolver(this);
            this.isOpen = true;
            this.orderedGroups = orderedGroups;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
            if (!isRecordChainBuilt) {
                buildRecordChain();
            }
            isRecordChainBuilt = true;
            recordChain.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                Misc.free(baseCursor);
                Misc.free(recordChain);
                for (int i = 0, n = orderedGroups.size(); i < n; i++) {
                    Misc.free(orderedGroups.getQuick(i));
                }
                resetFunctions();
                isOpen = false;
            }
        }

        @Override
        public Record getRecord() {
            return recordChain.getRecord();
        }

        @Override
        public Record getRecordB() {
            return recordChain.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public boolean hasNext() {
            if (!isRecordChainBuilt) {
                buildRecordChain();
            }
            isRecordChainBuilt = true;
            return recordChain.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public long preComputedStateSize() {
            return recordChain.size();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordChain.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return isRecordChainBuilt ? recordChain.size() : -1;
        }

        @Override
        public void toTop() {
            recordChain.toTop();
        }

        private void buildRecordChain() {
            final Record record = baseCursor.getRecord();
            final Record chainRecord = recordChain.getRecord();
            if (orderedGroupCount > 0) {
                while (baseCursor.hasNext()) {
                    recordChainOffset = recordChain.put(record);
                    recordChain.recordAt(chainRecord, recordChainOffset);
                    for (int i = 0; i < orderedGroupCount; i++) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        orderedGroups.getQuick(i).put(chainRecord, recordChainOffset);
                    }
                }
                for (int i = 0; i < orderedGroupCount; i++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    orderedGroups.getQuick(i).finishPut(circuitBreaker);
                }
            } else {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    recordChainOffset = recordChain.put(record);
                }
            }

            long offset;
            if (orderedGroupCount > 0) {
                for (int i = 0; i < orderedGroupCount; i++) {
                    final OrderedGroupSort group = orderedGroups.getQuick(i);
                    final ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                    final int functionCount = functions.size();
                    group.toTop();
                    while (group.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        offset = group.next();
                        recordChain.recordAt(chainRecord, offset);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass1(chainRecord, offset, recordChain);
                        }
                    }
                }
            }

            if (unorderedFunctions != null) {
                for (int j = 0, n = unorderedFunctions.size(); j < n; j++) {
                    final WindowFunction f = unorderedFunctions.getQuick(j);
                    if (f.getPass1ScanDirection() == WindowFunction.Pass1ScanDirection.FORWARD) {
                        recordChain.toTop();
                        while (recordChain.hasNext()) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                            f.pass1(chainRecord, chainRecord.getRowId(), recordChain);
                        }
                    } else {
                        recordChain.toBottom();
                        while (recordChain.hasPrev()) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                            f.pass1(chainRecord, chainRecord.getRowId(), recordChain);
                        }
                    }
                }
            }

            if (ordered2PassFunctions != null) {
                for (int i = 0, n = ordered2PassFunctions.size(); i < n; i++) {
                    final ObjList<WindowFunction> functions = ordered2PassFunctions.getQuick(i);
                    if (functions == null) {
                        continue;
                    }
                    for (int j = 0, k = functions.size(); j < k; j++) {
                        functions.getQuick(j).preparePass2();
                    }
                }
            }
            if (unordered2PassFunctions != null) {
                for (int j = 0, n = unordered2PassFunctions.size(); j < n; j++) {
                    unordered2PassFunctions.getQuick(j).preparePass2();
                }
            }

            if (ordered2PassFunctions != null) {
                for (int i = 0, n = ordered2PassFunctions.size(); i < n; i++) {
                    final ObjList<WindowFunction> functions = ordered2PassFunctions.getQuick(i);
                    if (functions == null) {
                        continue;
                    }
                    final OrderedGroupSort group = orderedGroups.getQuick(i);
                    final int functionCount = functions.size();
                    group.toTop();
                    while (group.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        offset = group.next();
                        recordChain.recordAt(chainRecord, offset);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass2(chainRecord, offset, recordChain);
                        }
                    }
                }
            }

            if (unordered2PassFunctions != null) {
                for (int j = 0, n = unordered2PassFunctions.size(); j < n; j++) {
                    final WindowFunction f = unordered2PassFunctions.getQuick(j);
                    recordChain.toTop();
                    while (recordChain.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        f.pass2(chainRecord, chainRecord.getRowId(), recordChain);
                    }
                }
            }

            recordChain.toTop();
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            isRecordChainBuilt = false;
            recordChainOffset = -1;
            circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                isOpen = true;
                recordChain.setSymbolTableResolver(this);
                reopenOrderedGroups();
                reopen(allFunctions);
            }
            Function.init(allFunctions, this, executionContext, null);
            for (int i = 0; i < orderedGroupCount; i++) {
                orderedGroups.getQuick(i).of(this);
            }
        }

        private void reopen(ObjList<?> list) {
            for (int i = 0, n = list.size(); i < n; i++) {
                if (list.getQuick(i) instanceof Reopenable) {
                    ((Reopenable) list.getQuick(i)).reopen();
                }
            }
        }

        private void reopenOrderedGroups() {
            for (int i = 0; i < orderedGroupCount; i++) {
                orderedGroups.getQuick(i).reopen();
            }
        }
    }
}
