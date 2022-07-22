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
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;

import javax.swing.*;
import javax.swing.tree.TreePath;

import java.awt.*;
import java.io.File;
import java.util.function.Consumer;

public class MetaExaminer {

    private final CairoConfiguration configuration = new DefaultCairoConfiguration("");
    private final FilesFacade ff = configuration.getFilesFacade();
    private final TableReaderMetadata metaReader = new TableReaderMetadata(FilesFacadeImpl.INSTANCE);
    private final TxReader txReader = new TxReader(FilesFacadeImpl.INSTANCE);
    private final ColumnVersionReader cvReader = new ColumnVersionReader();
    private int rootLen;
    private String potentialPartitionFolderName;
    private final Path selectedPath = new Path();
    private final Path auxPath = new Path();
    private final JFrame frame;
    private final FolderTreePanel treeView;
    private final ConsolePanel console;
    private final MessageSink ms = new MessageSink();


    public MetaExaminer() {
        frame = createFrame(this::onExit);
        console = new ConsolePanel();
        treeView = new FolderTreePanel(this::onRootSet, this::onSelectedFile);
        treeView.setPreferredSize(new Dimension(frame.getWidth() / 4, 0));
        frame.add(BorderLayout.CENTER, console);
        frame.add(BorderLayout.WEST, treeView);
        frame.setVisible(true);
    }

    public void setRoot(File root) {
        if (!root.exists() || !root.isDirectory()) {
            console.display("Folder does not exist: " + root);
        }
        treeView.setRoot(root); // receives callback onRootSet
    }

    private void onRootSet(File root) {
        String absolutePath = root.getAbsolutePath();
        selectedPath.trimTo(0).put(absolutePath).put(Files.SEPARATOR);
        rootLen = selectedPath.length();
    }

    private void onExit() {
        Misc.free(metaReader);
        Misc.free(txReader);
        Misc.free(cvReader);
        Misc.free(selectedPath);
        Misc.free(auxPath);
    }

    private void onSelectedFile(TreePath treePath) {
        Object[] nodes = treePath.getPath();
        String fileName = FolderTreePanel.extractItemName(
                nodes[nodes.length - 1].toString()
        );
        if (fileName.endsWith(File.separator)) {
            // do nothing for folders
            console.display("");
            return;
        }

        // build the selected path
        potentialPartitionFolderName = null;
        selectedPath.trimTo(rootLen); // first node
        for (int i = 1, limit = nodes.length - 1; i < limit; i++) {
            String pathElement = nodes[i].toString();
            selectedPath.put(pathElement);
            if (i == limit - 1) {
                int dotIdx = PathUtils.findNextDotIdx(pathElement, 0);
                int end = dotIdx == -1 ? pathElement.length() - 1 : dotIdx;
                potentialPartitionFolderName = pathElement.substring(0, end);
            }
        }
        selectedPath.put(fileName).$(); // last node
        frame.setTitle("Current path: " + selectedPath);
        try {
            if (fileName.contains(TableUtils.META_FILE_NAME)) {
                displayMetaFileContent();
            } else if (fileName.contains(TableUtils.TXN_SCOREBOARD_FILE_NAME)) {
                // no clear interface
                console.display("No reader available.");
            } else if (fileName.contains(TableUtils.TXN_FILE_NAME)) {
                displayTxnFileContent();
            } else if (fileName.contains(TableUtils.COLUMN_VERSION_FILE_NAME)) {
                displayCVFileContent();
            } else if (fileName.contains(".c")) {
                displayCOFileContent();
            } else if (fileName.contains(".o")) {
                selectedPath.trimTo(selectedPath.length() - fileName.length());
                selectedPath.concat(fileName.replace(".o", ".c")).$();
                displayCOFileContent();
            } else if (fileName.contains(".k")) {
                displayKVFileContent();
            } else if (fileName.contains(".v")) {
                selectedPath.trimTo(selectedPath.length() - fileName.length());
                selectedPath.concat(fileName.replace(".v", ".k")).$();
                displayKVFileContent();
            } else {
                console.display("No reader available.");
            }
        } catch (Throwable t) {
            ms.failedToOpenFile(selectedPath, t);
            console.display(ms.toString());
        }
    }

    private void displayMetaFileContent() {
        metaReader.deferredInit(selectedPath, ColumnType.VERSION);
        ms.clear();
        ms.addLn("tableId: ", metaReader.getId());
        ms.addLn("version: ", metaReader.getVersion());
        ms.addLn("structureVersion: ", metaReader.getStructureVersion());
        ms.addLn("timestampIndex: ", metaReader.getTimestampIndex());
        ms.addLn("partitionBy: ", PartitionBy.toString(metaReader.getPartitionBy()));
        ms.addLn("maxUncommittedRows: ", metaReader.getMaxUncommittedRows());
        ms.addTimeLn("commitLag: ", metaReader.getCommitLag());
        ms.addLn();
        int columnCount = metaReader.getColumnCount();
        ms.addLn("columnCount: ", columnCount);
        for (int i = 0; i < columnCount; i++) {
            int columnType = metaReader.getColumnType(i);
            ms.addColumnLn(
                    i,
                    metaReader.getColumnName(i),
                    metaReader.getColumnHash(i),
                    columnType,
                    columnType > 0 && metaReader.isColumnIndexed(i),
                    columnType > 0 ? metaReader.getIndexValueBlockCapacity(i) : 0,
                    true
            );
        }
        console.display(ms.toString());
    }

    private void displayCVFileContent() {
        cvReader.ofRO(FilesFacadeImpl.INSTANCE, selectedPath);
        cvReader.readSafe(MicrosecondClockImpl.INSTANCE, Long.MAX_VALUE);
        LongList cvEntries = cvReader.getCachedList();
        int limit = cvEntries.size();
        ms.clear();
        ms.addLn("version: ", cvReader.getVersion());
        ms.addLn("entryCount: ", limit / 4);
        for (int i = 0; i < limit; i += 4) {
            long partitionTimestamp = cvEntries.getQuick(i);
            ms.addLn("  + entry ", i / 4);
            ms.addTimestampLn("     - partitionTimestamp: ", partitionTimestamp);
            ms.addLn("     - columnIndex: ", cvEntries.getQuick(i + 1));
            ms.addLn("     - columnNameTxn: ", cvEntries.getQuick(i + 2));
            ms.addLn("     - columnTop: ", cvEntries.getQuick(i + 3));
            ms.addLn();
        }
        console.display(ms.toString());
    }

    private void displayTxnFileContent() {
        if (openRequiredMetaFile(1)) {
            // load txn
            txReader.ofRO(selectedPath, metaReader.getPartitionBy());
            txReader.unsafeLoadAll();
            ms.clear();
            int symbolColumnCount = txReader.getSymbolColumnCount();
            ms.addLn("txn: ", txReader.getTxn());
            ms.addLn("version: ", txReader.getVersion());
            ms.addLn("columnVersion: ", txReader.getColumnVersion());
            ms.addLn("dataVersion: ", txReader.getDataVersion());
            ms.addLn("structureVersion: ", txReader.getStructureVersion());
            ms.addLn("truncateVersion: ", txReader.getTruncateVersion());
            ms.addLn("partitionTableVersion: ", txReader.getPartitionTableVersion());
            ms.addLn();
            ms.addLn("rowCount: ", txReader.getRowCount());
            ms.addLn("fixedRowCount: ", txReader.getFixedRowCount());
            ms.addLn("transientRowCount: ", txReader.getTransientRowCount());
            ms.addTimestampLn("minTimestamp: ", txReader.getMinTimestamp());
            ms.addTimestampLn("maxTimestamp: ", txReader.getMaxTimestamp());
            ms.addLn("recordSize: ", txReader.getRecordSize());
            ms.addLn();
            ms.addLn("symbolColumnCount: ", symbolColumnCount);
            for (int i = 0; i < symbolColumnCount; i++) {
                ms.addLn(" - column " + i + " value count: ", txReader.getSymbolValueCount(i));
            }
            int partitionCount = txReader.getPartitionCount();
            ms.addLn();
            ms.addLn("partitionCount: ", partitionCount);
            for (int i = 0; i < partitionCount; i++) {
                ms.addPartitionLn(
                        i,
                        txReader.getPartitionTimestamp(i),
                        txReader.getPartitionNameTxn(i),
                        txReader.getPartitionSize(i),
                        txReader.getPartitionColumnVersion(i),
                        txReader.getSymbolValueCount(i)
                );
            }
            console.display(ms.toString());
        }
    }

    private void displayCOFileContent() {
        int metaLevelUp = insidePartitionFolder() ? 2 : 1;
        if (openRequiredMetaFile(metaLevelUp) && openRequiredTxnFile(metaLevelUp)) {
            auxPath.of(selectedPath);
            PathUtils.ColumnNameTxn cnTxn = PathUtils.columnNameTxnOf(auxPath);
            int colIdx = metaReader.getColumnIndex(cnTxn.columnName);
            int symbolCount = txReader.unsafeReadSymbolCount(colIdx);

            // this also opens the .o (offset) file, which contains symbolCapacity, isCached, containsNull
            // as well as the .k and .v (index key/value) files, which index the static table in this case
            PathUtils.selectFileInFolder(auxPath, 1, null);
            SymbolMapReaderImpl symReader = new SymbolMapReaderImpl(
                    configuration,
                    auxPath,
                    cnTxn.columnName,
                    cnTxn.columnNameTxn,
                    symbolCount
            );
            ms.clear();
            ms.addColumnLn(
                    colIdx,
                    metaReader.getColumnName(colIdx),
                    metaReader.getColumnHash(colIdx),
                    metaReader.getColumnType(colIdx),
                    metaReader.isColumnIndexed(colIdx),
                    metaReader.getIndexValueBlockCapacity(colIdx),
                    false
            );
            ms.addLn();
            ms.addLn("symbolCapacity: ", symReader.getSymbolCapacity());
            ms.addLn("isCached: ", symReader.isCached());
            ms.addLn("isDeleted: ", symReader.isDeleted());
            ms.addLn("containsNullValue: ", symReader.containsNullValue());
            ms.addLn("symbolCount: ", symbolCount);
            for (int i = 0; i < symbolCount; i++) {
                ms.addIndexedSymbolLn(i, symReader.valueOf(i), true);
            }
            console.display(ms.toString());
        }
    }

    private void displayKVFileContent() {
        int metaLevelUp = insidePartitionFolder() ? 2 : 1;
        if (openRequiredMetaFile(metaLevelUp) && openRequiredCvFile(metaLevelUp) && openRequiredTxnFile(metaLevelUp)) {
            auxPath.of(selectedPath);
            PathUtils.ColumnNameTxn cnTxn = PathUtils.columnNameTxnOf(auxPath);
            int colIdx = metaReader.getColumnIndex(cnTxn.columnName);
            int symbolCount = txReader.unsafeReadSymbolCount(colIdx);

            // this also opens the .o (offset) file, which contains symbolCapacity, isCached, containsNull
            // as well as the .k and .v (index key/value) files, which index the static table in this case
            PathUtils.selectFileInFolder(auxPath, metaLevelUp, null);
            SymbolMapReaderImpl symReader = new SymbolMapReaderImpl(
                    configuration,
                    auxPath,
                    cnTxn.columnName,
                    cnTxn.columnNameTxn,
                    symbolCount
            );

            long partitionTimestamp;
            try {
                partitionTimestamp = PartitionBy.parsePartitionDirName(potentialPartitionFolderName, metaReader.getPartitionBy());
            } catch (Throwable t) {
                partitionTimestamp = -1L;
            }
            int writerIdx = metaReader.getWriterIndex(colIdx);
            int versionRecordIdx = cvReader.getRecordIndex(partitionTimestamp, writerIdx);
            long columnTop = versionRecordIdx > -1L ? cvReader.getColumnTopByIndex(versionRecordIdx) : 0L;

            auxPath.of(selectedPath);
            PathUtils.selectFileInFolder(auxPath, 1, null);
            try (BitmapIndexFwdReader indexReader = new BitmapIndexFwdReader(
                    configuration,
                    auxPath,
                    cnTxn.columnName,
                    cnTxn.columnNameTxn,
                    columnTop,
                    -1L
            )) {
                ms.clear();
                if (symReader.containsNullValue()) {
                    RowCursor cursor = indexReader.getCursor(false, 0, 0, Long.MAX_VALUE);
                    if (cursor.hasNext()) {
                        ms.addLn("*: ", "");
                        ms.addLn(" - offset: ", cursor.next());
                        while (cursor.hasNext()) {
                            ms.addLn(" - offset: ", cursor.next());
                        }
                    }
                }
                for (int symbolKey = 0; symbolKey < symbolCount; symbolKey++) {
                    CharSequence symbol = symReader.valueOf(symbolKey);
                    RowCursor cursor = indexReader.getCursor(false, TableUtils.toIndexKey(symbolKey), 0, Long.MAX_VALUE);
                    if (cursor.hasNext()) {
                        ms.addIndexedSymbolLn(symbolKey, symbol, false);
                        ms.addLn(" - offset: ", cursor.next());
                        while (cursor.hasNext()) {
                            ms.addLn(" - offset: ", cursor.next());
                        }
                    }
                }
                console.display(ms.toString());
            }
        }
    }

    private boolean openRequiredMetaFile(int levelUpCount) {
        return onRequiredFile(levelUpCount, TableUtils.META_FILE_NAME, p -> {
            metaReader.deferredInit(p, ColumnType.VERSION);
        });
    }

    private boolean openRequiredTxnFile(int levelUpCount) {
        return onRequiredFile(levelUpCount, TableUtils.TXN_FILE_NAME, p -> {
            txReader.ofRO(p, metaReader.getPartitionBy());
            txReader.unsafeLoadAll();
        });
    }

    private boolean openRequiredCvFile(int levelUpCount) {
        return onRequiredFile(levelUpCount, TableUtils.COLUMN_VERSION_FILE_NAME, p -> {
            cvReader.ofRO(FilesFacadeImpl.INSTANCE, p);
            cvReader.readSafe(MicrosecondClockImpl.INSTANCE, Long.MAX_VALUE);
        });
    }

    private boolean onRequiredFile(int levelUpCount, String fileName, Consumer<Path> action) {
        auxPath.of(selectedPath);
        PathUtils.selectFileInFolder(auxPath, levelUpCount, fileName);
        if (!ff.exists(auxPath.$())) {
            console.display("Could not find required file: " + auxPath);
            return false;
        }
        action.accept(auxPath);
        return true;
    }

    private boolean insidePartitionFolder() {
        if (potentialPartitionFolderName != null) {
            int len = potentialPartitionFolderName.length();
            int i = 0;
            for (; i < len; i++) {
                char c = potentialPartitionFolderName.charAt(i);
                if (!(Character.isDigit(c) || c == '-' || c == 'T')) {
                    break;
                }
            }
            return i == len;
        }
        return false;
    }

    private static JFrame createFrame(Runnable onExit) {
        JFrame frame = new JFrame() {
            @Override
            public void dispose() {
                super.dispose();
                onExit.run();
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
        return frame;
    }

    public static void main(String[] args) {
        EventQueue.invokeLater(() -> {
            MetaExaminer examiner = new MetaExaminer();
            if (args.length == 1) {
                examiner.setRoot(new File(args[0]));
            }
        });
    }
}
