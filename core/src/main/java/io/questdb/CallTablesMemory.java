/*******************************************************************************
 *  Custom data snapshot scheduler
 ******************************************************************************/

package io.questdb;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.DirectString;
import io.questdb.tasks.O3OpenColumnTask;


public class CallTablesMemory extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(CallTablesMemory.class);
    public static List<Object[]> columnDataList = new ArrayList<>();

    private static int txnCount = 0; //starts from database startup
    public static List<Object[]> recoveredTuples = new ArrayList<>(); // Stores recovered tuples if recovery() is called. Size should be 2 if IS is used, 1 if COW is used
    private static Deque<String> snapshotVersionCounter = new ArrayDeque<>();

    /*
    * Choosing between Incremental Snapshot and Copy on Write:
    * - If Copy on Write is used, set boolean COW = true
    * - If Incremental Snapshot is used, set COW = false
    */
    private static boolean COW = false;

    
    private static int lastRecordedIndex = 0; // utility for incremental snapshot

    public CallTablesMemory(CairoEngine engine) throws SqlException{
        try{
            ObjHashSet<TableToken> tables = new ObjHashSet<>();
            engine.getTableTokens(tables, false);

            for (int t = 0, n = tables.size(); t < n; t++) {
                TableToken tableToken = tables.get(t);
                
                // Skipping system-related tables for the snapshot
                if (tableToken.getTableName().startsWith("sys.") || tableToken.getTableName().startsWith("telemetry")) {
                    LOG.info().$("[EDIT] Skipping system table: ").$(tableToken.getTableName()).$();
                    continue;
                }

                LOG.info().$("[EDIT] Reading [table=").$(tableToken).I$();
                TableReader reader = null;

                // Since at initialization there's no other worker scanning the table
                // no need to check whether the table is available for the reader or not
                reader = engine.getReaderWithRepair(tableToken);
                int partitionCount = reader.getPartitionCount();

                for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                    long rowCount = reader.openPartition(partitionIndex);
                    if (rowCount > 1){
                        for (int columnIndex = 0; columnIndex < reader.getColumnCount(); columnIndex++) {
                            MemoryCR column = null;
                            int absoluteIndex = reader.getPrimaryColumnIndex(reader.getColumnBase(partitionIndex), columnIndex);
                            column = reader.getColumn(absoluteIndex);
                            Object[] values = readEntireColumn(column, reader.getMetadata().getColumnType(columnIndex), rowCount);

                            
                            if (values.length == 0) {
                                LOG.info().$("Skipping empty or unsupported column at index ").$(columnIndex).$();
                                continue;
                            }

                            columnDataList.add(values);
                            // Timestamp as converted into microseconds since 1970-01-01T00:00:00 UTC
                            LOG.info().$("[EDIT] [First value of column=").$(values[0]).I$();
                            Misc.free(column);
                        }
                    }
                    
                }

                /*
                for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                    //LOG.info().$("TESTING").$(partitionCount).I$();
                    long rowCount = reader.openPartition(partitionIndex);
                    if (rowCount > 1){
                        for (int columnIndex = 0; columnIndex < reader.getColumnCount(); columnIndex++) {
                            int absoluteIndex = reader.getPrimaryColumnIndex(reader.getColumnBase(partitionIndex), columnIndex);
                            //MemoryR columnData = reader.getColumn(absoluteIndex);
                            LOG.info().$("[EDIT]").$(reader.getColumn(absoluteIndex));
                        }
                    }
                }
                */
                Misc.free(reader);
            }

            runSerially();
            
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    
    private Object[] readEntireColumn(MemoryCR columnData, int columnType, long rowCount) {
        Object[] values = new Object[(int) rowCount];
        DirectString tempStr = new DirectString(); // Temporary storage for strings
    
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            long offset = rowIndex * ColumnType.sizeOf(columnType); // Calculate the memory offset
    
            if (columnType == ColumnType.BINARY) {
                LOG.info().$("Skipping binary column: ");
                continue;
            }

            switch (columnType) {
                case ColumnType.INT:
                    values[rowIndex] = columnData.getInt(offset);
                    break;
                case ColumnType.LONG:
                    values[rowIndex] = columnData.getLong(offset);
                    break;
                case ColumnType.DOUBLE:
                    values[rowIndex] = columnData.getDouble(offset);
                    break;
                case ColumnType.FLOAT:
                    values[rowIndex] = columnData.getFloat(offset);
                    break;
                case ColumnType.STRING:
                    columnData.getStr(offset, tempStr); // Retrieve the string
                    values[rowIndex] = tempStr.toString(); // Convert DirectString to regular String
                    break;
                case ColumnType.TIMESTAMP:
                    long timestampMicros = columnData.getLong(offset); // Read TIMESTAMP as long
                    values[rowIndex] = timestampMicros; // Store raw timestamp (or format if needed)
                    break;
                default:
                    LOG.info().$("Unsupported column type: ").$(columnType).$();
                    return new Object[0];
            }
        }
        return values;
    }
    

    @Override
    public void close() {
        //this.halt();
        //LOG.info().$("Background worker stopped").$();
    }

    @Override
    public boolean runSerially() {
        /* Implementation of Snapshot strategies can go in here */
        
        scheduledSnapshotCreator(COW);


        if (false) {
            /*
            * Add appropriate flags if doing recovery
            */
            recovery();
        } 
        return false;
    }

    private void scheduledSnapshotCreator(boolean cow) {
        /*
        * Creates a snaphot ater every STEPS trasactions have occured since the last snapshot
        * To change the number of transactions after which to take a snapshot, change the STEPS variable
        */
        int STEPS = 100;

        O3OpenColumnTask oThree = new O3OpenColumnTask(); 
        int currTxnCount = Math.toIntExact(oThree.getTxn());
        
        if (currTxnCount >= txnCount + STEPS) {
            if (cow) {
                copyOnWrite();

            } else {
                
                int currRecordedIndex = incrementalSnapshot(lastRecordedIndex);
                lastRecordedIndex = currRecordedIndex;
            }
            
            txnCount = currTxnCount;
        }

    }

    private void copyOnWrite() {
        /*
        * Flow:
        * (1) Previous snapshots are discarded
        * (2) All elements in columnDataList is serialized to binary and saved to disk.
        */

        String latestSnapshotFile = writeToDisk(columnDataList);

        while (!snapshotVersionCounter.isEmpty()) {
            disposeSnapshot(snapshotVersionCounter.remove());
        }

        snapshotVersionCounter.add(latestSnapshotFile);
    }

    private int incrementalSnapshot(int lastRecordedIndex) {
        /*
        * Flow: 
        * (1) Creates a sublist of columnDataList from the lastRecordedIndex to the end of 
        * (2) Saves everything to disk from versionTwoTuples as a binary file with extension .d (as is common in QuestDB)
        * (3) Updates the global variable snapshotVersionCounter so that at a time we only save two snapshots: the ultimate and penultimate
        * (4) Cleans out previous files from disk
        */

        List<Object[]> versionTwoTuples = subListCreator(columnDataList, lastRecordedIndex);
        String latestSnapshotFile = writeToDisk(versionTwoTuples);

        int newRecordedIndex = columnDataList.size() - 1;

        snapshotVersionCounter.add(latestSnapshotFile);
        LOG.info().$("Snapshot successfully saved");

        while (snapshotVersionCounter.size() > 2) {

            disposeSnapshot(snapshotVersionCounter.remove());
        }

        return newRecordedIndex;
        
    }

    private void disposeSnapshot(String garbage) {
        /*
        * Removes file from memory
        */

        try {
            Files.deleteIfExists(Paths.get(garbage));
            LOG.info().$("Removed previous versions");
        } catch (NoSuchFileException e) {
            LOG.info().$("Nonexistent file at ").$(garbage).$();
        } catch (DirectoryNotEmptyException e) {
            LOG.info().$("Directory is not empty");
        } catch (IOException e) {
            LOG.info().$("Invalid permissions to access").$(garbage).$();
        }
    }

    private List<Object[]> subListCreator(List<Object[]> originalList, int lastRecordedIndex) {
        /*
        * Creates a sublist of columnDataList
        */
        if (lastRecordedIndex > 0) {
            return originalList.subList(lastRecordedIndex, originalList.size());
        } else {
            return originalList;
        }
    }

    private String writeToDisk(List<Object[]> versionTwoTuples) {
        /*
        * Custom writer which serializes versionTwoTuples into a file with .d extension
        */

       String filePath = "data/snapshots/snapshotAt__" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss")) + ".d";
       

       try {
            BinarySerializer.serializeToBinary(versionTwoTuples, filePath);
        } catch (IOException e) {
            e.printStackTrace();  // or handle the error appropriately
        }


        return filePath;
    }

    private List<Object[]> recoverFromDisk(String filePath) {
        /*
        * Utility function for recovery()
        */
        try {
            return BinarySerializer.deserializeFromBinary(filePath);
        } catch (IOException e) {
        }
        return new ArrayList<>();
    }

    private void recovery() {
        /*
        Add some flags if we're dong recovery
        */
        String temp = snapshotVersionCounter.removeLast();
        recoveredTuples = recoverFromDisk(temp);
        snapshotVersionCounter.add(temp);

    }


}
