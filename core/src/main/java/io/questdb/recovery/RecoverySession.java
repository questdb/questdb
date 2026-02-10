/*******************************************************************************
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

package io.questdb.recovery;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Interactive recovery REPL. Runs a read-eval-print loop that accepts commands:
 * {@code tables}, {@code cd}, {@code ls}, {@code show}, {@code print},
 * {@code pwd}, {@code help}, {@code truncate}, and {@code check columns}.
 *
 * <p>Command dispatch is table-driven: {@link #buildCommandMap()} registers all
 * commands as static methods implementing {@link RecoveryCommand}. Each command
 * receives a {@link CommandContext} that bundles navigation state and services.
 *
 * <p>Use the {@link #create(CharSequence, FilesFacade, ConsoleRenderer)} factory
 * to wire all readers and services with default bounds.
 */
public class RecoverySession {
    private final CommandContext commandCtx;
    private final CharSequenceObjHashMap<RecoveryCommand> commands;
    private final NavigationContext nav;
    private final ConsoleRenderer renderer;

    public RecoverySession(
            CharSequence dbRoot,
            BoundedColumnVersionReader columnVersionReader,
            BoundedMetaReader metaReader,
            BoundedRegistryReader registryReader,
            BoundedSeqTxnLogReader seqTxnLogReader,
            BoundedTxnReader txnReader,
            BoundedWalEventReader walEventReader,
            ColumnCheckService columnCheckService,
            ColumnValueReader columnValueReader,
            FilesFacade ff,
            PartitionScanService partitionScanService,
            TableDiscoveryService tableDiscoveryService,
            WalDiscoveryService walDiscoveryService,
            ConsoleRenderer renderer
    ) {
        this.renderer = renderer;
        this.nav = new NavigationContext(
                dbRoot,
                columnVersionReader,
                metaReader,
                partitionScanService,
                registryReader,
                seqTxnLogReader,
                tableDiscoveryService,
                txnReader,
                walDiscoveryService
        );
        this.commandCtx = new CommandContext(
                nav,
                txnReader,
                walEventReader,
                columnCheckService,
                columnValueReader,
                ff,
                partitionScanService,
                renderer,
                walDiscoveryService
        );
        this.commands = buildCommandMap();
    }

    public static RecoverySession create(CharSequence dbRoot, FilesFacade ff, ConsoleRenderer renderer) {
        return new RecoverySession(
                dbRoot,
                new BoundedColumnVersionReader(ff),
                new BoundedMetaReader(ff),
                new BoundedRegistryReader(ff),
                new BoundedSeqTxnLogReader(ff),
                new BoundedTxnReader(ff),
                new BoundedWalEventReader(ff),
                new ColumnCheckService(ff),
                new ColumnValueReader(ff),
                ff,
                new PartitionScanService(ff),
                new TableDiscoveryService(ff),
                new WalDiscoveryService(ff),
                renderer
        );
    }

    public int run(BufferedReader in, PrintStream out, PrintStream err) throws IOException {
        renderer.printBanner(nav.getDbRoot(), out);

        String line;
        while (true) {
            out.print(nav.buildPrompt());
            out.flush();
            line = in.readLine();
            if (line == null) {
                return 0;
            }

            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            if ("quit".equalsIgnoreCase(line) || "exit".equalsIgnoreCase(line)) {
                return 0;
            }

            try {
                ParsedCommand parsed = parseCommand(line);
                if (parsed == null) {
                    err.println("Unknown command: " + line);
                    renderer.printHelp(out);
                } else {
                    parsed.command.execute(parsed.arg, commandCtx, out, err);
                }
            } catch (Throwable th) {
                err.println("command failed: " + th.getMessage());
            }
        }
    }

    private CharSequenceObjHashMap<RecoveryCommand> buildCommandMap() {
        CharSequenceObjHashMap<RecoveryCommand> map = new CharSequenceObjHashMap<>();
        map.put("cd", RecoverySession::cmdCd);
        map.put("check columns", RecoverySession::cmdCheckColumns);
        map.put("help", RecoverySession::cmdHelp);
        map.put("ls", RecoverySession::cmdLs);
        map.put("print", RecoverySession::cmdPrint);
        map.put("pwd", RecoverySession::cmdPwd);
        map.put("show", RecoverySession::cmdShow);
        map.put("show timeline", RecoverySession::cmdShowTimeline);
        map.put("tables", RecoverySession::cmdTables);
        map.put("truncate", RecoverySession::cmdTruncate);
        map.put("wal", RecoverySession::cmdWal);
        map.put("wal status", RecoverySession::cmdWalStatus);
        return map;
    }

    private ParsedCommand parseCommand(String line) {
        String lower = line.toLowerCase();

        // exact matches first
        if (lower.equals("check columns")) {
            return new ParsedCommand(commands.get("check columns"), "");
        }
        if (lower.equals("show timeline")) {
            return new ParsedCommand(commands.get("show timeline"), "");
        }
        if (lower.equals("wal status")) {
            return new ParsedCommand(commands.get("wal status"), "");
        }

        // extract verb (first word)
        int spaceIdx = line.indexOf(' ');
        String verb = spaceIdx < 0 ? line : line.substring(0, spaceIdx);
        String arg = spaceIdx < 0 ? "" : line.substring(spaceIdx + 1).trim();

        RecoveryCommand cmd = commands.get(verb.toLowerCase());
        return cmd != null ? new ParsedCommand(cmd, arg) : null;
    }



    private static void cmdCd(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        if (arg.isEmpty() || "/".equals(arg)) {
            nav.cdRoot();
        } else if ("..".equals(arg)) {
            nav.cdUp();
        } else {
            nav.cd(arg, err);
        }
    }

    private static void cmdCheckColumns(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        if (nav.isInWalMode()) {
            // check columns operates on table-level metadata, not WAL structures
            checkColumnsForTable(nav.getCurrentTable(), ctx, out, err);
        } else if (nav.isAtRoot()) {
            checkColumnsFromRoot(ctx, out, err);
        } else if (nav.isAtTable()) {
            checkColumnsForTable(nav.getCurrentTable(), ctx, out, err);
        } else {
            checkColumnsForPartition(ctx, out, err);
        }
    }

    private static void cmdHelp(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        ctx.getRenderer().printHelp(out);
    }

    private static void cmdLs(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        ConsoleRenderer renderer = ctx.getRenderer();

        // WAL navigation levels
        if (nav.isInWalMode()) {
            WalNavigationContext walNav = nav.getWalNav();
            switch (walNav.getLevel()) {
                case WAL_ROOT -> {
                    ensureSeqTxnLogState(nav);
                    enrichRecordsFromEvents(ctx, nav);
                    renderer.printWalDirectories(
                            walNav.getCachedWalScanState(),
                            nav.getCachedSeqTxnLogState(),
                            nav.getCachedTxnState(),
                            nav.getCachedMetaState(),
                            out
                    );
                }
                case WAL_DIR -> {
                    WalDirEntry walEntry = walNav.findWalDirEntry(walNav.getCurrentWalId());
                    if (walEntry != null) {
                        ensureSeqTxnLogState(nav);
                        enrichRecordsFromEvents(ctx, nav);
                        renderer.printWalSegments(
                                walEntry.segments(),
                                nav.getCachedSeqTxnLogState(),
                                nav.getCachedTxnState(),
                                nav.getCachedMetaState(),
                                walNav.getCurrentWalId(),
                                out
                        );
                    } else {
                        err.println("WAL directory not found");
                    }
                }
                case WAL_SEGMENT -> {
                    ensureWalEventState(walNav, ctx, nav);
                    renderer.printWalEvents(
                            walNav.getCachedWalEventState(),
                            nav.getCachedSeqTxnLogState(),
                            nav.getCachedTxnState(),
                            nav.getCachedMetaState(),
                            walNav.getCurrentWalId(),
                            walNav.getCurrentSegmentId(),
                            out
                    );
                }
            }
            return;
        }

        if (nav.isAtRoot()) {
            nav.discoverTables();
            RegistryState registryState = nav.readRegistryState();
            nav.getTableDiscoveryService().crossReferenceRegistry(nav.getLastDiscoveredTables(), registryState);
            renderer.printTables(nav.getLastDiscoveredTables(), registryState, out);
        } else if (nav.isAtTable()) {
            renderer.printPartitionScan(nav.getCachedPartitionScan(), nav.getCachedTxnState(), nav.getCachedMetaState(), out);
        } else if (nav.isAtPartition()) {
            MetaState cachedMetaState = nav.getCachedMetaState();
            if (cachedMetaState == null) {
                err.println("no meta state cached for current table");
                return;
            }
            long[] columnTops = nav.computeAllColumnTops();
            renderer.printColumns(cachedMetaState, columnTops, out);
        } else {
            showColumn(ctx, out, err);
        }
    }

    private static void cmdPrint(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        if (!nav.isAtColumn()) {
            err.println("print is only valid at column level");
            return;
        }

        if (!nav.getCachedColumnInPartition()) {
            err.println("column not in this partition (added later)");
            return;
        }

        long rowNo;
        try {
            rowNo = Numbers.parseLong(arg);
        } catch (NumericException e) {
            err.println("invalid row number: " + arg);
            return;
        }

        long cachedColumnTop = nav.getCachedColumnTop();
        long cachedEffectiveRows = nav.getCachedEffectiveRows();
        long partitionRowCount = cachedEffectiveRows + cachedColumnTop;
        if (rowNo < 0 || rowNo >= partitionRowCount) {
            err.println("row out of range [0, " + partitionRowCount + ")");
            return;
        }

        if (rowNo < cachedColumnTop) {
            out.println("[" + rowNo + "] = null (column top)");
            return;
        }

        MetaColumnState col = nav.getCachedMetaState().getColumns().getQuick(nav.getCurrentColumnIndex());
        PartitionScanEntry entry = nav.getCachedPartitionScan().getQuick(nav.getCurrentPartitionIndex());
        long fileRowNo = rowNo - cachedColumnTop;

        try (Path path = new Path()) {
            path.of(nav.getDbRoot()).concat(nav.getCurrentTable().getDirName()).$();
            String value = ctx.getColumnValueReader().readValue(
                    path.toString(),
                    entry.dirName(),
                    col.name(),
                    nav.getCachedColumnNameTxn(),
                    col.type(),
                    fileRowNo,
                    cachedEffectiveRows
            );
            out.println("[" + rowNo + "] = " + value);
        }
    }

    private static void cmdPwd(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        ctx.getNav().pwd(out);
    }

    private static void cmdShow(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();

        // WAL mode: show sequencer txnlog
        if (nav.isInWalMode()) {
            WalNavigationContext walNav = nav.getWalNav();
            switch (walNav.getLevel()) {
                case WAL_ROOT -> {
                    ensureSeqTxnLogState(nav);
                    enrichRecordsFromEvents(ctx, nav);
                    if (arg.isEmpty()) {
                        ctx.getRenderer().printSeqTxnLog(
                                nav.getCachedSeqTxnLogState(),
                                nav.getCachedTxnState(),
                                out,
                                100
                        );
                    } else {
                        showSeqTxnDetail(arg, ctx, nav, out, err);
                    }
                }
                case WAL_DIR -> {
                    WalDirEntry walEntry = walNav.findWalDirEntry(walNav.getCurrentWalId());
                    if (walEntry != null) {
                        ctx.getRenderer().printWalDirDetail(walEntry, nav.getCachedSeqTxnLogState(), out);
                    } else {
                        err.println("WAL directory not found");
                    }
                }
                case WAL_SEGMENT -> {
                    ensureWalEventState(walNav, ctx, nav);
                    if (arg.isEmpty()) {
                        ctx.getRenderer().printWalSegmentDetail(
                                walNav.getCachedWalEventState(),
                                nav.getCachedSeqTxnLogState(),
                                walNav.getCurrentWalId(),
                                walNav.getCurrentSegmentId(),
                                out
                        );
                    } else {
                        showWalEvent(arg, walNav, ctx, nav, out, err);
                    }
                }
            }
            return;
        }

        if (arg.isEmpty()) {
            if (nav.isAtColumn()) {
                showColumn(ctx, out, err);
            } else if (nav.getCurrentTable() != null) {
                TxnState state = nav.getCachedTxnState() != null
                        ? nav.getCachedTxnState()
                        : nav.getTxnReader().readForTable(nav.getDbRoot(), nav.getCurrentTable());
                ctx.getRenderer().printShow(nav.getCurrentTable(), state, out);
            } else {
                err.println("show requires a table name or index (or cd into a table first)");
            }
            return;
        }

        if (nav.getLastDiscoveredTables().size() == 0) {
            nav.discoverTables();
        }

        DiscoveredTable table = nav.findTable(arg);
        if (table == null) {
            err.println("table not found: " + arg);
            return;
        }

        TxnState state = nav.getTxnReader().readForTable(nav.getDbRoot(), table);
        ctx.getRenderer().printShow(table, state, out);
    }

    private static void cmdShowTimeline(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        if (!nav.isInWalMode()) {
            err.println("show timeline is only valid in WAL mode (cd into a table, then cd wal)");
            return;
        }
        WalNavigationContext walNav = nav.getWalNav();
        if (walNav.getLevel() != WalLevel.WAL_ROOT) {
            err.println("show timeline is only valid at WAL root level (cd .. to go up)");
            return;
        }
        ensureSeqTxnLogState(nav);
        enrichRecordsFromEvents(ctx, nav);
        ctx.getRenderer().printTimeline(
                nav.getCachedSeqTxnLogState(),
                nav.getCachedTxnState(),
                nav.getCachedMetaState(),
                out
        );
    }

    private static void cmdTruncate(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        FilesFacade ff = ctx.getFf();

        if (!nav.isAtPartition()) {
            err.println("truncate is only valid at partition level (cd into a partition first)");
            return;
        }

        long newRowCount;
        try {
            newRowCount = Numbers.parseLong(arg);
        } catch (NumericException e) {
            err.println("invalid row count: " + arg);
            return;
        }

        if (newRowCount <= 0) {
            err.println("row count must be > 0");
            return;
        }

        TxnState cachedTxnState = nav.getCachedTxnState();
        MetaState cachedMetaState = nav.getCachedMetaState();
        ColumnVersionState cachedCvState = nav.getCachedCvState();
        if (cachedTxnState == null || cachedMetaState == null) {
            err.println("no txn/meta state cached for current table");
            return;
        }

        PartitionScanEntry entry = nav.getCachedPartitionScan().getQuick(nav.getCurrentPartitionIndex());
        TxnPartitionState txnPart = entry.txnPartition();
        if (txnPart == null) {
            err.println("partition has no row count (ORPHAN)");
            return;
        }

        if (txnPart.parquetFormat()) {
            err.println("cannot truncate a parquet partition");
            return;
        }

        // Use the resolved row count from the scan entry, which handles the last-partition
        // convention: the _txn file stores the last partition's row count in the header's
        // transientRowCount field, not in the partition entry itself.
        long currentRowCount = entry.rowCount();
        if (newRowCount >= currentRowCount) {
            err.println("new row count (" + newRowCount + ") must be less than current (" + currentRowCount + ")");
            return;
        }

        ObjList<TxnPartitionState> oldPartitions = cachedTxnState.getPartitions();
        int partitionCount = oldPartitions.size();
        int targetTxnIndex = txnPart.index();
        boolean isLastPartition = (targetTxnIndex == partitionCount - 1);

        // build modified state
        // baseVersion must equal txn — they are checked by TxReader.unsafeLoadAll()
        TxnState modified = new TxnState();
        long newTxn = cachedTxnState.getTxn() + 1;
        modified.setBaseVersion(newTxn);
        modified.setTxn(newTxn);
        modified.setMinTimestamp(cachedTxnState.getMinTimestamp());
        modified.setMaxTimestamp(cachedTxnState.getMaxTimestamp());
        modified.setStructureVersion(cachedTxnState.getStructureVersion());
        modified.setDataVersion(cachedTxnState.getDataVersion() + 1);
        modified.setPartitionTableVersion(cachedTxnState.getPartitionTableVersion() + 1);
        modified.setColumnVersion(cachedTxnState.getColumnVersion());
        modified.setTruncateVersion(cachedTxnState.getTruncateVersion() + 1);
        modified.setSeqTxn(cachedTxnState.getSeqTxn());
        modified.setLagTxnCount(cachedTxnState.getLagTxnCount());
        modified.setLagRowCount(cachedTxnState.getLagRowCount());
        modified.setLagMinTimestamp(cachedTxnState.getLagMinTimestamp());
        modified.setLagMaxTimestamp(cachedTxnState.getLagMaxTimestamp());
        modified.setMapWriterCount(cachedTxnState.getMapWriterCount());

        // copy symbols
        for (int i = 0, n = cachedTxnState.getSymbols().size(); i < n; i++) {
            modified.getSymbols().add(cachedTxnState.getSymbols().getQuick(i));
        }

        // rebuild partitions with modified row count
        for (int i = 0; i < partitionCount; i++) {
            TxnPartitionState p = oldPartitions.getQuick(i);
            if (i == targetTxnIndex) {
                modified.getPartitions().add(new TxnPartitionState(
                        i,
                        p.timestampLo(),
                        newRowCount,
                        p.nameTxn(),
                        p.parquetFileSize(),
                        p.parquetFormat(),
                        p.readOnly(),
                        p.squashCount()
                ));
            } else {
                modified.getPartitions().add(p);
            }
        }

        // recompute fixedRowCount and transientRowCount
        // For non-last partitions, TxnPartitionState.getRowCount() is the actual value.
        // For the last partition, the _txn file stores 0 in the entry; the real count is
        // in the header's transientRowCount. We use the entry value for non-last, and
        // either newRowCount (if we're truncating the last) or the original header
        // transientRowCount (if we're truncating a non-last partition).
        long fixedRowCount = 0;
        for (int i = 0; i < partitionCount - 1; i++) {
            fixedRowCount += modified.getPartitions().getQuick(i).rowCount();
        }
        modified.setFixedRowCount(fixedRowCount);
        if (isLastPartition) {
            modified.setTransientRowCount(newRowCount);
        } else {
            modified.setTransientRowCount(cachedTxnState.getTransientRowCount());
        }

        // update maxTimestamp if truncating the last partition
        long oldMaxTimestamp = cachedTxnState.getMaxTimestamp();
        long newMaxTimestamp = oldMaxTimestamp;
        if (isLastPartition) {
            int tsIndex = cachedMetaState.getTimestampIndex();
            if (tsIndex < 0) {
                err.println("no timestamp column found in metadata");
                return;
            }

            long tsColumnTop = 0;
            if (cachedCvState != null && !NavigationContext.hasCvIssues(cachedCvState)) {
                tsColumnTop = cachedCvState.getColumnTop(txnPart.timestampLo(), tsIndex);
            }

            if (tsColumnTop >= newRowCount) {
                err.println("timestamp column top (" + tsColumnTop + ") >= new row count (" + newRowCount + "); cannot determine max timestamp");
                return;
            }

            long fileRowIndex = newRowCount - 1 - tsColumnTop;
            MetaColumnState tsCol = cachedMetaState.getColumns().getQuick(tsIndex);
            long tsNameTxn = -1;
            if (cachedCvState != null && !NavigationContext.hasCvIssues(cachedCvState)) {
                tsNameTxn = cachedCvState.getColumnNameTxn(txnPart.timestampLo(), tsIndex);
            }

            try (Path path = new Path()) {
                path.of(nav.getDbRoot()).concat(nav.getCurrentTable().getDirName())
                        .slash().concat(entry.dirName()).slash();
                TableUtils.dFile(path, tsCol.name(), tsNameTxn);

                long fd = ff.openRO(path.$());
                if (fd < 0) {
                    err.println("cannot open timestamp file: " + path);
                    return;
                }
                try {
                    long scratch = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long readOffset = fileRowIndex * Long.BYTES;
                        long bytesRead = ff.read(fd, scratch, Long.BYTES, readOffset);
                        if (bytesRead != Long.BYTES) {
                            err.println("cannot read timestamp at row " + fileRowIndex);
                            return;
                        }
                        newMaxTimestamp = Unsafe.getUnsafe().getLong(scratch);
                    } finally {
                        Unsafe.free(scratch, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(fd);
                }
            }
            modified.setMaxTimestamp(newMaxTimestamp);
        }

        // recompute lag checksum
        modified.setLagChecksum(TableUtils.calculateTxnLagChecksum(
                modified.getTxn(),
                modified.getSeqTxn(),
                modified.getLagRowCount(),
                modified.getLagMinTimestamp(),
                modified.getLagMaxTimestamp(),
                modified.getLagTxnCount()
        ));

        // backup original _txn
        try (Path txnPath = new Path(); Path bakPath = new Path()) {
            txnPath.of(nav.getDbRoot()).concat(nav.getCurrentTable().getDirName())
                    .concat(TableUtils.TXN_FILE_NAME).$();

            bakPath.of(txnPath).put(".bak").$();
            int bakNum = 0;
            while (ff.exists(bakPath.$())) {
                bakNum++;
                bakPath.of(txnPath).put(".bak.").put(bakNum).$();
            }

            int copyResult = ff.copy(txnPath.$(), bakPath.$());
            if (copyResult < 0) {
                err.println("failed to create backup: " + bakPath);
                return;
            }
            out.println("backup: " + bakPath);

            // print before/after summary
            out.println("partition: " + entry.partitionName());
            out.println("  rows: " + currentRowCount + " -> " + newRowCount);
            if (newMaxTimestamp != oldMaxTimestamp) {
                out.println("  maxTimestamp: " + ConsoleRenderer.formatTimestamp(oldMaxTimestamp)
                        + " -> " + ConsoleRenderer.formatTimestamp(newMaxTimestamp));
            }
            out.println("  fixedRowCount: " + modified.getFixedRowCount());
            out.println("  transientRowCount: " + modified.getTransientRowCount());

            // write
            TxnSerializer.write(modified, txnPath.$(), ff);

            // validate with BoundedTxnReader
            TxnState readBack = ctx.getBoundedTxnReader().read(txnPath.$());
            boolean valid = true;
            if (readBack.getPartitions().size() != partitionCount) {
                err.println("WARNING: partition count mismatch after write: expected "
                        + partitionCount + ", got " + readBack.getPartitions().size());
                valid = false;
            }
            if (readBack.getPartitions().size() > targetTxnIndex) {
                long readBackRowCount = readBack.getPartitions().getQuick(targetTxnIndex).rowCount();
                if (readBackRowCount != newRowCount) {
                    err.println("WARNING: row count mismatch after write: expected "
                            + newRowCount + ", got " + readBackRowCount);
                    valid = false;
                }
            }
            if (readBack.getFixedRowCount() != modified.getFixedRowCount()) {
                err.println("WARNING: fixedRowCount mismatch: expected "
                        + modified.getFixedRowCount() + ", got " + readBack.getFixedRowCount());
                valid = false;
            }
            if (readBack.getTransientRowCount() != modified.getTransientRowCount()) {
                err.println("WARNING: transientRowCount mismatch: expected "
                        + modified.getTransientRowCount() + ", got " + readBack.getTransientRowCount());
                valid = false;
            }
            if (readBack.getMaxTimestamp() != modified.getMaxTimestamp()) {
                err.println("WARNING: maxTimestamp mismatch: expected "
                        + modified.getMaxTimestamp() + ", got " + readBack.getMaxTimestamp());
                valid = false;
            }

            // validate with production TxReader
            try (TxReader txReader = new TxReader(ff)) {
                txReader.ofRO(txnPath.$(), cachedMetaState.getTimestampColumnType(), cachedMetaState.getPartitionBy());
                if (!txReader.unsafeLoadAll()) {
                    err.println("WARNING: production TxReader failed to load the written _txn file");
                    valid = false;
                } else {
                    if (txReader.getPartitionCount() != partitionCount) {
                        err.println("WARNING: TxReader partition count mismatch: expected "
                                + partitionCount + ", got " + txReader.getPartitionCount());
                        valid = false;
                    }
                    if (txReader.getFixedRowCount() != modified.getFixedRowCount()) {
                        err.println("WARNING: TxReader fixedRowCount mismatch: expected "
                                + modified.getFixedRowCount() + ", got " + txReader.getFixedRowCount());
                        valid = false;
                    }
                    if (txReader.getTransientRowCount() != modified.getTransientRowCount()) {
                        err.println("WARNING: TxReader transientRowCount mismatch: expected "
                                + modified.getTransientRowCount() + ", got " + txReader.getTransientRowCount());
                        valid = false;
                    }
                    if (txReader.getMaxTimestamp() != modified.getMaxTimestamp()) {
                        err.println("WARNING: TxReader maxTimestamp mismatch: expected "
                                + modified.getMaxTimestamp() + ", got " + txReader.getMaxTimestamp());
                        valid = false;
                    }
                }
            }

            if (valid) {
                out.println("OK");
            }

            // invalidate caches and re-navigate
            String tableName = nav.getCurrentTable().getTableName();
            nav.cdRoot();
            nav.cd(tableName, err);
        }
    }

    private static void cmdWal(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        if ("status".equalsIgnoreCase(arg)) {
            cmdWalStatus("", ctx, out, err);
        } else {
            err.println("unknown wal subcommand: " + arg);
            err.println("usage: wal status");
        }
    }

    private static void cmdWalStatus(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        if (nav.isAtRoot()) {
            err.println("wal status requires a table (cd into a table first)");
            return;
        }

        DiscoveredTable table = nav.getCurrentTable();
        if (table.isWalEnabledKnown() && !table.isWalEnabled()) {
            out.println("walEnabled: false");
            out.println("this table does not use WAL");
            return;
        }

        SeqTxnLogState seqState = nav.readSeqTxnLogState();
        enrichRecordsFromEvents(ctx, nav);
        TxnState txnState = nav.getCachedTxnState();
        MetaState metaState = nav.getCachedMetaState();
        ctx.getRenderer().printWalStatus(seqState, txnState, metaState, out);
    }

    private static void cmdTables(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        nav.discoverTables();
        RegistryState registryState = nav.readRegistryState();
        nav.getTableDiscoveryService().crossReferenceRegistry(nav.getLastDiscoveredTables(), registryState);
        ctx.getRenderer().printTables(nav.getLastDiscoveredTables(), registryState, out);
    }

    private static void ensureSeqTxnLogState(NavigationContext nav) {
        if (nav.getCachedSeqTxnLogState() == null) {
            nav.readSeqTxnLogState();
        }
    }

    private static void ensureWalEventState(WalNavigationContext walNav, CommandContext ctx, NavigationContext nav) {
        if (walNav.getCachedWalEventState() == null) {
            walNav.setCachedWalEventState(
                    ctx.getBoundedWalEventReader().readForSegment(
                            nav.getDbRoot(),
                            nav.getCurrentTable().getDirName(),
                            walNav.getCurrentWalId(),
                            walNav.getCurrentSegmentId()
                    )
            );
        }
    }

    /**
     * Enriches V1 seqTxn records with row counts and timestamps from WAL event
     * files. Groups records by (walId, segmentId) so each segment's event file
     * is read at most once. Records that already have data (V2) are skipped.
     * Missing or unreadable event files are silently ignored (fields stay UNSET).
     */
    private static void enrichRecordsFromEvents(CommandContext ctx, NavigationContext nav) {
        SeqTxnLogState seqState = nav.getCachedSeqTxnLogState();
        if (seqState == null) {
            return;
        }
        ObjList<SeqTxnRecord> records = seqState.getRecords();
        if (records.size() == 0) {
            return;
        }

        // check if enrichment is needed: skip if first data record already has row count
        boolean needsEnrichment = false;
        for (int i = 0, n = records.size(); i < n; i++) {
            SeqTxnRecord rec = records.getQuick(i);
            if (!rec.isDdlChange() && !rec.isTableDrop()) {
                needsEnrichment = rec.getRowCount() == TxnState.UNSET_LONG;
                break;
            }
        }
        if (!needsEnrichment) {
            return;
        }

        // group records needing enrichment by (walId, segmentId)
        // key = walId << 32 | segmentId
        LongObjHashMap<ObjList<SeqTxnRecord>> bySegment = new LongObjHashMap<>();
        for (int i = 0, n = records.size(); i < n; i++) {
            SeqTxnRecord rec = records.getQuick(i);
            if (rec.isDdlChange() || rec.isTableDrop()) {
                continue;
            }
            if (rec.getRowCount() != TxnState.UNSET_LONG) {
                continue;
            }
            long key = ((long) rec.getWalId() << 32) | (rec.getSegmentId() & 0xFFFFFFFFL);
            int idx = bySegment.keyIndex(key);
            ObjList<SeqTxnRecord> list;
            if (idx >= 0) {
                list = new ObjList<>();
                bySegment.putAt(idx, key, list);
            } else {
                list = bySegment.valueAt(idx);
            }
            list.add(rec);
        }

        BoundedWalEventReader eventReader = ctx.getBoundedWalEventReader();
        CharSequence dbRoot = nav.getDbRoot();
        CharSequence tableDirName = nav.getCurrentTable().getDirName();

        bySegment.forEach((key, recsForSegment) -> {
            int walId = (int) (key >> 32);
            int segmentId = (int) key;

            WalEventState eventState;
            try {
                eventState = eventReader.readForSegment(dbRoot, tableDirName, walId, segmentId);
            } catch (Exception e) {
                return; // event file missing or unreadable
            }
            if (eventState == null || eventState.getEvents().size() == 0) {
                return;
            }

            ObjList<WalEventEntry> events = eventState.getEvents();
            for (int i = 0, n = recsForSegment.size(); i < n; i++) {
                SeqTxnRecord rec = recsForSegment.getQuick(i);
                // find matching event by segmentTxn
                for (int j = 0, m = events.size(); j < m; j++) {
                    WalEventEntry evt = events.getQuick(j);
                    if (evt.getTxn() == rec.getSegmentTxn() && WalTxnType.isDataType(evt.getType())) {
                        long rows = evt.getEndRowID() - evt.getStartRowID();
                        rec.enrichFromEvent(
                                rows >= 0 ? rows : TxnState.UNSET_LONG,
                                evt.getMinTimestamp(),
                                evt.getMaxTimestamp()
                        );
                        break;
                    }
                }
            }
        });
    }

    private static void showWalEvent(
            String arg, WalNavigationContext walNav, CommandContext ctx,
            NavigationContext nav, PrintStream out, PrintStream err
    ) {
        long txn;
        try {
            txn = Numbers.parseLong(arg);
        } catch (NumericException e) {
            err.println("invalid event txn: " + arg);
            return;
        }

        WalEventState eventState = walNav.getCachedWalEventState();
        if (eventState == null) {
            err.println("no event state available");
            return;
        }

        ObjList<WalEventEntry> events = eventState.getEvents();
        for (int i = 0, n = events.size(); i < n; i++) {
            WalEventEntry entry = events.getQuick(i);
            if (entry.getTxn() == txn) {
                ctx.getRenderer().printWalEventDetail(
                        entry,
                        nav.getCachedSeqTxnLogState(),
                        nav.getCachedMetaState(),
                        walNav.getCurrentWalId(),
                        walNav.getCurrentSegmentId(),
                        out
                );
                return;
            }
        }

        err.println("event not found: txn " + txn);
    }

    private static void showSeqTxnDetail(
            String arg, CommandContext ctx, NavigationContext nav,
            PrintStream out, PrintStream err
    ) {
        long seqTxn;
        try {
            seqTxn = Numbers.parseLong(arg);
        } catch (NumericException e) {
            err.println("invalid seqTxn: " + arg);
            return;
        }

        ensureSeqTxnLogState(nav);
        SeqTxnLogState seqState = nav.getCachedSeqTxnLogState();
        if (seqState == null) {
            err.println("no sequencer txnlog state available");
            return;
        }

        SeqTxnRecord found = null;
        ObjList<SeqTxnRecord> records = seqState.getRecords();
        for (int i = 0, n = records.size(); i < n; i++) {
            SeqTxnRecord rec = records.getQuick(i);
            if (rec.getTxn() == seqTxn) {
                found = rec;
                break;
            }
        }

        if (found == null) {
            err.println("seqTxn not found: " + seqTxn);
            return;
        }

        out.println("seqTxn: " + found.getTxn());
        out.println("structureVersion: " + found.getStructureVersion());
        out.println("commitTimestamp: " + ConsoleRenderer.formatTimestamp(found.getCommitTimestamp()));

        if (found.isDdlChange()) {
            out.println("type: DDL");
            return;
        }
        if (found.isTableDrop()) {
            out.println("type: DROP TABLE");
            return;
        }

        out.println("walId: " + found.getWalId());
        out.println("segmentId: " + found.getSegmentId());
        out.println("segmentTxn: " + found.getSegmentTxn());
        if (found.getRowCount() != TxnState.UNSET_LONG) {
            out.println("rows: " + found.getRowCount());
        }

        // resolve to WAL event detail
        out.println("--- event detail ---");
        WalEventState eventState;
        try {
            eventState = ctx.getBoundedWalEventReader().readForSegment(
                    nav.getDbRoot(),
                    nav.getCurrentTable().getDirName(),
                    found.getWalId(),
                    found.getSegmentId()
            );
        } catch (Exception e) {
            err.println("could not read events for wal" + found.getWalId() + "/" + found.getSegmentId() + ": " + e.getMessage());
            return;
        }

        if (eventState == null || eventState.getEvents().size() == 0) {
            err.println("no events found in wal" + found.getWalId() + "/" + found.getSegmentId());
            return;
        }

        ObjList<WalEventEntry> events = eventState.getEvents();
        for (int i = 0, n = events.size(); i < n; i++) {
            WalEventEntry entry = events.getQuick(i);
            if (entry.getTxn() == found.getSegmentTxn()) {
                ctx.getRenderer().printWalEventDetail(
                        entry,
                        seqState,
                        nav.getCachedMetaState(),
                        found.getWalId(),
                        found.getSegmentId(),
                        out
                );
                return;
            }
        }

        err.println("event not found: segmentTxn " + found.getSegmentTxn() + " in wal" + found.getWalId() + "/" + found.getSegmentId());
    }



    private static void checkAllPartitions(
            String tableName,
            String tableDir,
            ObjList<PartitionScanEntry> partitionScan,
            MetaState metaState,
            ColumnVersionState cvState,
            CommandContext ctx,
            PrintStream out
    ) {
        ConsoleRenderer renderer = ctx.getRenderer();
        ColumnCheckService columnCheckService = ctx.getColumnCheckService();
        int checked = 0;
        int errors = 0;
        int warnings = 0;
        int skipped = 0;

        for (int i = 0, n = partitionScan.size(); i < n; i++) {
            PartitionScanEntry entry = partitionScan.getQuick(i);

            if (entry.status() == PartitionScanStatus.MISSING) {
                renderer.printCheckSkipped(entry.partitionName(), "MISSING", out);
                skipped++;
                continue;
            }

            TxnPartitionState txnPart = entry.txnPartition();
            if (txnPart == null) {
                renderer.printCheckSkipped(entry.partitionName(), "ORPHAN (no row count)", out);
                skipped++;
                continue;
            }

            if (txnPart.parquetFormat()) {
                renderer.printCheckSkipped(entry.partitionName(), "parquet", out);
                skipped++;
                continue;
            }

            long rowCount = entry.rowCount();
            ColumnCheckResult result = columnCheckService.checkPartition(
                    tableDir,
                    entry.dirName(),
                    txnPart.timestampLo(),
                    rowCount,
                    metaState,
                    cvState
            );
            renderer.printCheckResult(result, tableName, rowCount, out);
            checked++;

            for (int j = 0, m = result.entries().size(); j < m; j++) {
                ColumnCheckEntry checkEntry = result.entries().getQuick(j);
                switch (checkEntry.status()) {
                    case ERROR -> errors++;
                    case WARNING -> warnings++;
                }
            }
        }

        renderer.printCheckSummary(checked, errors, warnings, skipped, out);
    }

    private static void checkColumnsForPartition(CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        MetaState cachedMetaState = nav.getCachedMetaState();
        ObjList<PartitionScanEntry> cachedPartitionScan = nav.getCachedPartitionScan();
        int currentPartitionIndex = nav.getCurrentPartitionIndex();

        if (cachedMetaState == null || cachedMetaState.getColumns().size() == 0) {
            err.println("no meta state available for current table");
            return;
        }
        if (cachedPartitionScan == null || currentPartitionIndex >= cachedPartitionScan.size()) {
            err.println("no partition scan available");
            return;
        }

        PartitionScanEntry entry = cachedPartitionScan.getQuick(currentPartitionIndex);
        TxnPartitionState txnPart = entry.txnPartition();
        if (txnPart == null) {
            err.println("cannot check: partition has no row count (ORPHAN)");
            return;
        }
        if (txnPart.parquetFormat()) {
            ctx.getRenderer().printCheckSkipped(entry.partitionName(), "parquet", out);
            return;
        }

        long rowCount = entry.rowCount();

        try (Path path = new Path()) {
            path.of(nav.getDbRoot()).concat(nav.getCurrentTable().getDirName()).$();
            ColumnVersionState cvState = NavigationContext.hasCvIssues(nav.getCachedCvState()) ? null : nav.getCachedCvState();
            ColumnCheckResult result = ctx.getColumnCheckService().checkPartition(
                    path.toString(),
                    entry.dirName(),
                    txnPart.timestampLo(),
                    rowCount,
                    cachedMetaState,
                    cvState
            );
            ctx.getRenderer().printCheckResult(result, nav.getCurrentTable().getTableName(), rowCount, out);

            int errors = 0, warnings = 0;
            for (int j = 0, m = result.entries().size(); j < m; j++) {
                ColumnCheckEntry checkEntry = result.entries().getQuick(j);
                switch (checkEntry.status()) {
                    case ERROR -> errors++;
                    case WARNING -> warnings++;
                }
            }
            ctx.getRenderer().printCheckSummary(1, errors, warnings, 0, out);
        }
    }

    private static void checkColumnsForTable(DiscoveredTable table, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        MetaState metaState = nav.getCachedMetaState() != null
                ? nav.getCachedMetaState()
                : nav.getMetaReader().readForTable(nav.getDbRoot(), table);
        if (metaState.getColumns().size() == 0) {
            err.println("no columns found for table: " + table.getTableName());
            return;
        }

        TxnState txnState = nav.getCachedTxnState() != null
                ? nav.getCachedTxnState()
                : nav.getTxnReader().readForTable(nav.getDbRoot(), table);
        ColumnVersionState cvState = nav.getCachedCvState() != null
                ? nav.getCachedCvState()
                : nav.getColumnVersionReader().readForTable(nav.getDbRoot(), table);
        ObjList<PartitionScanEntry> partitionScan = nav.getCachedPartitionScan();
        if (partitionScan == null) {
            try (Path path = new Path()) {
                path.of(nav.getDbRoot()).concat(table.getDirName()).$();
                partitionScan = ctx.getPartitionScanService().scan(path.toString(), txnState, metaState);
            }
        }

        try (Path path = new Path()) {
            path.of(nav.getDbRoot()).concat(table.getDirName()).$();
            checkAllPartitions(
                    table.getTableName(),
                    path.toString(),
                    partitionScan,
                    metaState,
                    NavigationContext.hasCvIssues(cvState) ? null : cvState,
                    ctx,
                    out
            );
        }
    }

    private static void checkColumnsFromRoot(CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        ObjList<DiscoveredTable> tables = nav.getLastDiscoveredTables();
        if (tables.size() == 0) {
            nav.discoverTables();
            tables = nav.getLastDiscoveredTables();
        }

        for (int i = 0, n = tables.size(); i < n; i++) {
            DiscoveredTable table = tables.getQuick(i);
            out.println();
            out.println(ctx.getColor().bold("=== " + table.getTableName() + " ==="));
            checkColumnsForTable(table, ctx, out, err);
        }
    }

    private static void showColumn(CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        FilesFacade ff = ctx.getFf();
        MetaState cachedMetaState = nav.getCachedMetaState();
        int currentColumnIndex = nav.getCurrentColumnIndex();

        if (cachedMetaState == null || currentColumnIndex < 0) {
            err.println("no column selected");
            return;
        }

        MetaColumnState col = cachedMetaState.getColumns().getQuick(currentColumnIndex);
        long cachedColumnNameTxn = nav.getCachedColumnNameTxn();
        long cachedEffectiveRows = nav.getCachedEffectiveRows();

        if (!nav.getCachedColumnInPartition()) {
            ctx.getRenderer().printColumnDetail(col, currentColumnIndex,
                    0, cachedColumnNameTxn, 0,
                    -1, -1, -1, -1, false, out);
            return;
        }

        PartitionScanEntry entry = nav.getCachedPartitionScan().getQuick(nav.getCurrentPartitionIndex());
        int colType = col.type();

        long expectedDataSize;
        long expectedAuxSize = -1;
        long actualDataSize;
        long actualAuxSize = -1;

        try (Path path = new Path()) {
            path.of(nav.getDbRoot()).concat(nav.getCurrentTable().getDirName()).slash().concat(entry.dirName()).slash();
            int pathLen = path.size();

            if (ColumnType.isVarSize(colType)) {
                expectedAuxSize = cachedEffectiveRows >= 0 ? ColumnType.getDriver(colType).getAuxVectorSize(cachedEffectiveRows) : -1;
                // .d file
                path.trimTo(pathLen);
                TableUtils.dFile(path, col.name(), cachedColumnNameTxn);
                actualDataSize = ff.length(path.$());
                if (actualDataSize < 0) {
                    actualDataSize = -1;
                }
                // .i file
                path.trimTo(pathLen);
                TableUtils.iFile(path, col.name(), cachedColumnNameTxn);
                actualAuxSize = ff.length(path.$());
                if (actualAuxSize < 0) {
                    actualAuxSize = -1;
                }
                expectedDataSize = -1;
                if (cachedEffectiveRows == 0) {
                    expectedDataSize = 0;
                } else if (actualAuxSize >= 0) {
                    long auxFd = ff.openRO(path.$());
                    if (auxFd >= 0) {
                        try {
                            expectedDataSize = ColumnType.getDriver(colType).getDataVectorSizeAtFromFd(ff, auxFd, cachedEffectiveRows - 1);
                        } catch (Exception ignore) {
                            // expectedDataSize is already -1 from assignment above
                        } finally {
                            ff.close(auxFd);
                        }
                    }
                }
            } else {
                int typeSize = ColumnType.sizeOf(colType);
                expectedDataSize = cachedEffectiveRows * typeSize;
                path.trimTo(pathLen);
                TableUtils.dFile(path, col.name(), cachedColumnNameTxn);
                actualDataSize = ff.length(path.$());
                if (actualDataSize < 0) {
                    actualDataSize = -1;
                }
            }
        }

        ctx.getRenderer().printColumnDetail(
                col, currentColumnIndex,
                nav.getCachedColumnTop(), cachedColumnNameTxn, cachedEffectiveRows,
                expectedDataSize, actualDataSize,
                expectedAuxSize, actualAuxSize, true, out
        );
    }

    private static class ParsedCommand {
        final String arg;
        final RecoveryCommand command;

        ParsedCommand(RecoveryCommand command, String arg) {
            this.arg = arg;
            this.command = command;
        }
    }
}
