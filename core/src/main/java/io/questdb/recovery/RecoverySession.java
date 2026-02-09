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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

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
    private final Map<String, RecoveryCommand> commands;
    private final NavigationContext nav;
    private final ConsoleRenderer renderer;

    public RecoverySession(
            CharSequence dbRoot,
            BoundedColumnVersionReader columnVersionReader,
            BoundedMetaReader metaReader,
            BoundedRegistryReader registryReader,
            BoundedTxnReader txnReader,
            ColumnCheckService columnCheckService,
            ColumnValueReader columnValueReader,
            FilesFacade ff,
            PartitionScanService partitionScanService,
            TableDiscoveryService tableDiscoveryService,
            ConsoleRenderer renderer
    ) {
        this.renderer = renderer;
        this.nav = new NavigationContext(
                dbRoot,
                columnVersionReader,
                metaReader,
                partitionScanService,
                registryReader,
                tableDiscoveryService,
                txnReader
        );
        this.commandCtx = new CommandContext(
                nav,
                txnReader,
                columnCheckService,
                columnValueReader,
                ff,
                partitionScanService,
                renderer
        );
        this.commands = buildCommandMap();
    }

    public static RecoverySession create(CharSequence dbRoot, FilesFacade ff, ConsoleRenderer renderer) {
        return new RecoverySession(
                dbRoot,
                new BoundedColumnVersionReader(ff),
                new BoundedMetaReader(ff),
                new BoundedRegistryReader(ff),
                new BoundedTxnReader(ff),
                new ColumnCheckService(ff),
                new ColumnValueReader(ff),
                ff,
                new PartitionScanService(ff),
                new TableDiscoveryService(ff),
                renderer
        );
    }

    public int run(BufferedReader in, PrintStream out, PrintStream err) throws IOException {
        out.println("QuestDB offline recovery mode");
        out.println("dbRoot=" + nav.getDbRoot());
        renderer.printHelp(out);

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

    private Map<String, RecoveryCommand> buildCommandMap() {
        Map<String, RecoveryCommand> map = new HashMap<>();
        map.put("cd", RecoverySession::cmdCd);
        map.put("check columns", RecoverySession::cmdCheckColumns);
        map.put("help", RecoverySession::cmdHelp);
        map.put("ls", RecoverySession::cmdLs);
        map.put("print", RecoverySession::cmdPrint);
        map.put("pwd", RecoverySession::cmdPwd);
        map.put("show", RecoverySession::cmdShow);
        map.put("tables", RecoverySession::cmdTables);
        map.put("truncate", RecoverySession::cmdTruncate);
        return map;
    }

    private ParsedCommand parseCommand(String line) {
        String lower = line.toLowerCase();

        // exact matches first
        if (lower.equals("check columns")) {
            return new ParsedCommand(commands.get("check columns"), "");
        }

        // extract verb (first word)
        int spaceIdx = line.indexOf(' ');
        String verb = spaceIdx < 0 ? line : line.substring(0, spaceIdx);
        String arg = spaceIdx < 0 ? "" : line.substring(spaceIdx + 1).trim();

        RecoveryCommand cmd = commands.get(verb.toLowerCase());
        return cmd != null ? new ParsedCommand(cmd, arg) : null;
    }

    // -- command implementations -----------------------------------------------

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
        if (nav.isAtRoot()) {
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
                    entry.getDirName(),
                    col.getName(),
                    nav.getCachedColumnNameTxn(),
                    col.getType(),
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
        TxnPartitionState txnPart = entry.getTxnPartition();
        if (txnPart == null) {
            err.println("partition has no row count (ORPHAN)");
            return;
        }

        if (txnPart.isParquetFormat()) {
            err.println("cannot truncate a parquet partition");
            return;
        }

        // Use the resolved row count from the scan entry, which handles the last-partition
        // convention: the _txn file stores the last partition's row count in the header's
        // transientRowCount field, not in the partition entry itself.
        long currentRowCount = entry.getRowCount();
        if (newRowCount >= currentRowCount) {
            err.println("new row count (" + newRowCount + ") must be less than current (" + currentRowCount + ")");
            return;
        }

        ObjList<TxnPartitionState> oldPartitions = cachedTxnState.getPartitions();
        int partitionCount = oldPartitions.size();
        int targetTxnIndex = txnPart.getIndex();
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
                        p.getTimestampLo(),
                        newRowCount,
                        p.getNameTxn(),
                        p.getParquetFileSize(),
                        p.isParquetFormat(),
                        p.isReadOnly(),
                        p.getSquashCount()
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
            fixedRowCount += modified.getPartitions().getQuick(i).getRowCount();
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
                tsColumnTop = cachedCvState.getColumnTop(txnPart.getTimestampLo(), tsIndex);
            }

            if (tsColumnTop >= newRowCount) {
                err.println("timestamp column top (" + tsColumnTop + ") >= new row count (" + newRowCount + "); cannot determine max timestamp");
                return;
            }

            long fileRowIndex = newRowCount - 1 - tsColumnTop;
            MetaColumnState tsCol = cachedMetaState.getColumns().getQuick(tsIndex);
            long tsNameTxn = -1;
            if (cachedCvState != null && !NavigationContext.hasCvIssues(cachedCvState)) {
                tsNameTxn = cachedCvState.getColumnNameTxn(txnPart.getTimestampLo(), tsIndex);
            }

            try (Path path = new Path()) {
                path.of(nav.getDbRoot()).concat(nav.getCurrentTable().getDirName())
                        .slash().concat(entry.getDirName()).slash();
                TableUtils.dFile(path, tsCol.getName(), tsNameTxn);

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

            String bakSuffix = ".bak";
            bakPath.of(txnPath).put(bakSuffix).$();
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
            out.println("partition: " + entry.getPartitionName());
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
                long readBackRowCount = readBack.getPartitions().getQuick(targetTxnIndex).getRowCount();
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

    private static void cmdTables(String arg, CommandContext ctx, PrintStream out, PrintStream err) {
        NavigationContext nav = ctx.getNav();
        nav.discoverTables();
        RegistryState registryState = nav.readRegistryState();
        nav.getTableDiscoveryService().crossReferenceRegistry(nav.getLastDiscoveredTables(), registryState);
        ctx.getRenderer().printTables(nav.getLastDiscoveredTables(), registryState, out);
    }

    // -- helpers ---------------------------------------------------------------

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

            if (entry.getStatus() == PartitionScanStatus.MISSING) {
                renderer.printCheckSkipped(entry.getPartitionName(), "MISSING", out);
                skipped++;
                continue;
            }

            TxnPartitionState txnPart = entry.getTxnPartition();
            if (txnPart == null) {
                renderer.printCheckSkipped(entry.getPartitionName(), "ORPHAN (no row count)", out);
                skipped++;
                continue;
            }

            if (txnPart.isParquetFormat()) {
                renderer.printCheckSkipped(entry.getPartitionName(), "parquet", out);
                skipped++;
                continue;
            }

            long rowCount = entry.getRowCount();
            ColumnCheckResult result = columnCheckService.checkPartition(
                    tableDir,
                    entry.getDirName(),
                    txnPart.getTimestampLo(),
                    rowCount,
                    metaState,
                    cvState
            );
            renderer.printCheckResult(result, tableName, rowCount, out);
            checked++;

            for (int j = 0, m = result.getEntries().size(); j < m; j++) {
                ColumnCheckEntry checkEntry = result.getEntries().getQuick(j);
                switch (checkEntry.getStatus()) {
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
        TxnPartitionState txnPart = entry.getTxnPartition();
        if (txnPart == null) {
            err.println("cannot check: partition has no row count (ORPHAN)");
            return;
        }
        if (txnPart.isParquetFormat()) {
            ctx.getRenderer().printCheckSkipped(entry.getPartitionName(), "parquet", out);
            return;
        }

        long rowCount = entry.getRowCount();

        try (Path path = new Path()) {
            path.of(nav.getDbRoot()).concat(nav.getCurrentTable().getDirName()).$();
            ColumnVersionState cvState = NavigationContext.hasCvIssues(nav.getCachedCvState()) ? null : nav.getCachedCvState();
            ColumnCheckResult result = ctx.getColumnCheckService().checkPartition(
                    path.toString(),
                    entry.getDirName(),
                    txnPart.getTimestampLo(),
                    rowCount,
                    cachedMetaState,
                    cvState
            );
            ctx.getRenderer().printCheckResult(result, nav.getCurrentTable().getTableName(), rowCount, out);

            int errors = 0, warnings = 0;
            for (int j = 0, m = result.getEntries().size(); j < m; j++) {
                ColumnCheckEntry checkEntry = result.getEntries().getQuick(j);
                switch (checkEntry.getStatus()) {
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
        int colType = col.getType();

        long expectedDataSize;
        long expectedAuxSize = -1;
        long actualDataSize;
        long actualAuxSize = -1;

        try (Path path = new Path()) {
            path.of(nav.getDbRoot()).concat(nav.getCurrentTable().getDirName()).slash().concat(entry.getDirName()).slash();
            int pathLen = path.size();

            if (ColumnType.isVarSize(colType)) {
                expectedAuxSize = cachedEffectiveRows >= 0 ? ColumnType.getDriver(colType).getAuxVectorSize(cachedEffectiveRows) : -1;
                // .d file
                path.trimTo(pathLen);
                TableUtils.dFile(path, col.getName(), cachedColumnNameTxn);
                actualDataSize = ff.length(path.$());
                if (actualDataSize < 0) {
                    actualDataSize = -1;
                }
                // .i file
                path.trimTo(pathLen);
                TableUtils.iFile(path, col.getName(), cachedColumnNameTxn);
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
                TableUtils.dFile(path, col.getName(), cachedColumnNameTxn);
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
