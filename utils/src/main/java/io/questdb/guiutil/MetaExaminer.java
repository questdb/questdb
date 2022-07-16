/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.guiutil;

import io.questdb.cairo.*;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import javax.swing.*;
import javax.swing.tree.TreePath;

import java.awt.*;
import java.io.File;

public class MetaExaminer {

    private static final DateFormat DATE_FORMATTER = new TimestampFormatCompiler().compile(
            "yyyy-MM-ddTHH:mm:ss.SSSz"
    );

    static {
        // preload, which compiles the pattern and is costly, penalising startup time
        DATE_FORMATTER.format(0, null, "Z", new StringSink());
    }

    private final CairoConfiguration configuration = new DefaultCairoConfiguration("");
    private int dbRootLen;
    private final Path selectedPath = new Path();
    private final Path auxPath = new Path();
    private final TableReaderMetadata metaReader = new TableReaderMetadata(FilesFacadeImpl.INSTANCE);
    private final TxReader txReader = new TxReader(FilesFacadeImpl.INSTANCE);
    private final ColumnVersionReader cvReader = new ColumnVersionReader();
    private final StringSink messageBuilder = new StringSink();
    private final JFrame frame;
    private final FolderTreePanel folderTreeView;
    private final ConsolePanel console;


    public MetaExaminer() {
        frame = new JFrame() {
            @Override
            public void dispose() {
                super.dispose();
                onExit();
                System.exit(0);
            }
        };
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.setType(Window.Type.NORMAL);
        frame.setLayout(new BorderLayout());
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        int width = (int) (screenSize.getWidth() * 0.9);
        int height = (int) (screenSize.getHeight() * 0.9);
        int x = (int) (screenSize.getWidth() - width) / 2;
        int y = (int) (screenSize.getHeight() - height) / 2;
        frame.setSize(width, height);
        frame.setLocation(x, y);

        console = new ConsolePanel();
        frame.add(BorderLayout.CENTER, console);

        folderTreeView = new FolderTreePanel(this::onDbRootSet, this::onSelectedFile);
        folderTreeView.setPreferredSize(new Dimension(frame.getWidth() / 4, 0));
        frame.add(BorderLayout.WEST, folderTreeView);

        frame.setVisible(true);
    }

    public void setDbRoot(File dbRoot) {
        if (!dbRoot.exists() || !dbRoot.isDirectory()) {
            console.display("Folder does not exist: " + dbRoot);
        }
        folderTreeView.setDbRoot(dbRoot); // receives callback onDbRootSet
    }

    private void onExit() {
        Misc.free(metaReader);
        Misc.free(txReader);
        Misc.free(cvReader);
        Misc.free(selectedPath);
        Misc.free(auxPath);
    }

    private void onDbRootSet(File dbRoot) {
        String absolutePath = dbRoot.getAbsolutePath();
        selectedPath.trimTo(0).put(absolutePath).put(File.separatorChar);
        dbRootLen = selectedPath.length();
    }

    private void onSelectedFile(TreePath treePath) {
        Object[] nodes = treePath.getPath();
        String fileName = FolderTreePanel.extractItemName(
                nodes[nodes.length - 1].toString()
        );
        boolean isFolder = fileName.endsWith(File.separator);
        selectedPath.trimTo(dbRootLen); // first node
        for (int i = 1; i < nodes.length - 1; i++) {
            selectedPath.put(nodes[i].toString());
        }
        selectedPath.put(fileName).$(); // last node
        frame.setTitle("Current path: " + selectedPath);
        if (isFolder) {
            console.display("");
        } else {
            try {
                if (fileName.contains(TableUtils.COLUMN_VERSION_FILE_NAME)) {
                    displayCVFileContent();
                } else if (fileName.contains(TableUtils.META_FILE_NAME)) {
                    displayMetaFileContent();
                } else if (fileName.contains(TableUtils.TXN_SCOREBOARD_FILE_NAME)) {
                    console.display("No reader available.");
                } else if (fileName.contains(TableUtils.TXN_FILE_NAME)) {
                    displayTxnFileContent();
                } else if (fileName.contains(".c")) {
                    displayStaticSymbolMapFileContent();
                } else {
                    console.display("No reader available.");
                }
            } catch (Throwable t) {
                messageBuilder.clear();
                messageBuilder
                        .put("Failed to open file: ")
                        .put(t.getMessage())
                        .put(System.lineSeparator());
                console.display(messageBuilder.toString());
            }
        }
    }

    private void displayMetaFileContent() {
        metaReader.deferredInit(selectedPath, ColumnType.VERSION);
        int columnCount = metaReader.getColumnCount();
        messageBuilder.clear();
        mbAppendLn("tableId: ", metaReader.getId());
        mbAppendLn("version: ", metaReader.getVersion());
        mbAppendLn("structureVersion: ", metaReader.getStructureVersion());
        mbAppendLn("timestampIndex: ", metaReader.getTimestampIndex());
        mbAppendLn("partitionBy: ", metaReader.getPartitionBy());
        mbAppendLn("commitLag: ", metaReader.getCommitLag());
        mbAppendLn("maxUncommittedRows: ", metaReader.getMaxUncommittedRows());
        mbAppendLn();
        mbAppendLn("columnCount: ", columnCount);
        for (int i = 0; i < columnCount; i++) {
            int columnType = metaReader.getColumnType(i);
            mbAppendColumnLn(
                    i,
                    metaReader.getColumnName(i),
                    metaReader.getColumnHash(i),
                    columnType,
                    columnType > 0 && metaReader.isColumnIndexed(i),
                    columnType > 0 ? metaReader.getIndexValueBlockCapacity(i) : 0
            );
        }
        console.display(messageBuilder.toString());
    }

    private void displayTxnFileContent() {
        // load meta
        auxPath.of(selectedPath);
        selectInParentFolder(auxPath, TableUtils.META_FILE_NAME);
        if (!configuration.getFilesFacade().exists(auxPath.$())) {
            console.display("Could not find required file: " + auxPath);
            return;
        }
        metaReader.deferredInit(auxPath, ColumnType.VERSION);

        txReader.ofRO(selectedPath, metaReader.getPartitionBy());
        txReader.unsafeLoadAll();
        messageBuilder.clear();
        int symbolColumnCount = txReader.getSymbolColumnCount();
        mbAppendLn("txn: ", txReader.getTxn());
        mbAppendLn("version: ", txReader.getVersion());
        mbAppendLn("columnVersion: ", txReader.getColumnVersion());
        mbAppendLn("dataVersion: ", txReader.getDataVersion());
        mbAppendLn("structureVersion: ", txReader.getStructureVersion());
        mbAppendLn("truncateVersion: ", txReader.getTruncateVersion());
        mbAppendLn("partitionTableVersion: ", txReader.getPartitionTableVersion());
        mbAppendLn();
        mbAppendLn("rowCount: ", txReader.getRowCount());
        mbAppendLn("fixedRowCount: ", txReader.getFixedRowCount());
        mbAppendLn("transientRowCount: ", txReader.getTransientRowCount());
        mbAppendLn("minTimestamp: ", txReader.getMinTimestamp());
        mbAppendLn("maxTimestamp: ", txReader.getMaxTimestamp());
        mbAppendLn("recordSize: ", txReader.getRecordSize());
        mbAppendLn();
        mbAppendLn("symbolColumnCount: ", symbolColumnCount);
        for (int i = 0; i < symbolColumnCount; i++) {
            mbAppendLn(" - symbol " + i + " -> value count: ", txReader.getSymbolValueCount(i));
        }
        int partitionCount = txReader.getPartitionCount();
        mbAppendLn();
        mbAppendLn("partitionCount: ", partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            mbAppendPartitionLn(
                    i,
                    txReader.getPartitionTimestamp(i),
                    txReader.getPartitionNameTxn(i),
                    txReader.getPartitionSize(i),
                    txReader.getPartitionColumnVersion(i),
                    txReader.getSymbolValueCount(i)
            );
        }
        console.display(messageBuilder.toString());
    }

    private void displayCVFileContent() {
        cvReader.ofRO(FilesFacadeImpl.INSTANCE, selectedPath);
        cvReader.readSafe(MicrosecondClockImpl.INSTANCE, Long.MAX_VALUE);
        LongList cvEntries = cvReader.getCachedList();
        int limit = cvEntries.size();
        messageBuilder.clear();
        mbAppendLn("version: ", cvReader.getVersion());
        mbAppendLn("entryCount: ", limit / 4);
        for (int i = 0; i < limit; i += 4) {
            long partitionTimestamp = cvEntries.getQuick(i);
            mbAppendLn("  + entry ", i / 4);
            mbAppendTimestampLn(partitionTimestamp);
            mbAppendLn("     - columnIndex: ", cvEntries.getQuick(i + 1));
            mbAppendLn("     - columnNameTxn: ", cvEntries.getQuick(i + 2));
            mbAppendLn("     - columnTop: ", cvEntries.getQuick(i + 3));
            mbAppendLn();
        }
        console.display(messageBuilder.toString());
    }

    private void displayStaticSymbolMapFileContent() {
        // load meta
        auxPath.of(selectedPath);
        selectInParentFolder(auxPath, TableUtils.META_FILE_NAME);
        if (!configuration.getFilesFacade().exists(auxPath.$())) {
            console.display("Could not find required file: " + auxPath);
            return;
        }
        metaReader.deferredInit(auxPath, ColumnType.VERSION);

        // load txn
        auxPath.of(selectedPath);
        selectInParentFolder(auxPath, TableUtils.TXN_FILE_NAME);
        if (!configuration.getFilesFacade().exists(auxPath.$())) {
            console.display("Could not find required file: " + auxPath);
            return;
        }
        txReader.ofRO(auxPath, metaReader.getPartitionBy());
        txReader.unsafeLoadAll();

        // columnName and columnNameTxn (selected path may contain suffix .txn)
        auxPath.of(selectedPath);
        int len = auxPath.length();
        int nameStart = len - 1;
        while (auxPath.charAt(nameStart) != File.separatorChar) {
            nameStart--;
        }
        int dotIdx = ++nameStart;
        while (dotIdx < len && auxPath.charAt(dotIdx) != '.') {
            dotIdx++;
        }
        int dotIdx2 = dotIdx + 1;
        while (dotIdx2 < len && auxPath.charAt(dotIdx2) != '.') {
            dotIdx2++;
        }
        String tmp = auxPath.toString();
        String columnName = tmp.substring(nameStart, dotIdx);
        long columnNameTxn = dotIdx2 == len ? -1L : Long.parseLong(tmp.substring(dotIdx2 + 1, len));

        int colIdx = metaReader.getColumnIndex(columnName);
        int symbolCount = txReader.unsafeReadSymbolCount(colIdx);

        // this also opens the .o (offset) file, which contains symbolCapacity, isCached, containsNull
        // as well as the .k and .v (index key/value) files, which index the static table in this case
        selectInParentFolder(auxPath, null);
        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(
                configuration,
                auxPath,
                columnName,
                columnNameTxn,
                symbolCount
        );

        messageBuilder.clear();
        mbAppendLn("symbolCapacity: ", reader.getSymbolCapacity());
        mbAppendLn("containsNullValue: ", reader.containsNullValue());
        mbAppendLn("isCached: ", reader.isCached());
        mbAppendLn("isDeleted: ", reader.isDeleted());
        mbAppendLn();
        mbAppendLn("symbolCount: ", symbolCount);
        for (int i = 0; i < symbolCount; i++) {
            mbAppendIndexedSymbolLn(i, reader.valueOf(i));
        }
        console.display(messageBuilder.toString());
    }

    private static void selectInParentFolder(Path p, String fileName) {
        int idx = p.length() - 1;
        while (p.charAt(idx) != File.separatorChar) {
            idx--;
        }
        p.trimTo(idx + 1);
        if (fileName != null) {
            p.concat(fileName);
        }
    }

    private void mbAppendTimestampLn(long timestamp) {
        messageBuilder.put("     - partitionTimestamp: ").put(timestamp).put(" (");
        DATE_FORMATTER.format(timestamp, null, "Z", messageBuilder);
        messageBuilder.put(')').put(System.lineSeparator());
    }

    private void mbAppendLn(String name, int value) {
        messageBuilder.put(name).put(value).put(System.lineSeparator());
    }

    private void mbAppendLn(String name, long value) {
        messageBuilder.put(name).put(value).put(System.lineSeparator());
    }

    private void mbAppendLn(String name, boolean value) {
        messageBuilder.put(name).put(value).put(System.lineSeparator());
    }

    private void mbAppendIndexedSymbolLn(int index, CharSequence value) {
        messageBuilder.put("  - ").put(index).put(": ").put(value).put(System.lineSeparator());
    }

    private void mbAppendColumnLn(
            int columnIndex,
            CharSequence columnName,
            long columnHash,
            int columnType,
            boolean columnIsIndexed,
            int columnIndexBlockCapacity
    ) {
        messageBuilder.put(" - column ").put(columnIndex)
                .put(" -> name: ").put(columnName)
                .put(", hash: ").put(columnHash)
                .put(", type: ").put(ColumnType.nameOf(columnType))
                .put(", indexed: ").put(columnIsIndexed)
                .put(", indexBlockCapacity: ").put(columnIndexBlockCapacity)
                .put(System.lineSeparator());
    }

    private void mbAppendPartitionLn(
            int partitionIndex,
            long partitionTimestamp,
            long partitionNameTxn,
            long partitionSize,
            long partitionColumnVersion,
            long partitionSymbolValueCount
    ) {
        messageBuilder.put(" - partition ").put(partitionIndex)
                .put(" -> timestamp: ").put(partitionTimestamp)
                .put(", txn: ").put(partitionNameTxn)
                .put(", size: ").put(partitionSize)
                .put(", column version: ").put(partitionColumnVersion)
                .put(", symbol value count: ").put(partitionSymbolValueCount)
                .put(System.lineSeparator());
    }

    private void mbAppendLn() {
        messageBuilder.put(System.lineSeparator());
    }

    public static void main(String[] args) {
        EventQueue.invokeLater(() -> {
            MetaExaminer examiner = new MetaExaminer();
            if (args.length == 1) {
                examiner.setDbRoot(new File(args[0]));
            }
        });
    }
}
