/*******************************************************************************
 *  Custom in-memory data loader
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

public class CallTablesMemory extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(CallTablesMemory.class);
    public static Map<TableTokenTimestampKey, Object> updatedTuples = new HashMap<>();
    private static final int SnapAtStart = 1;
    
    private static int txnCount = 0; //starts from database startup
    public static Map<TableToken, List<Object>> recoveredTuples; // Stores recovered tuples if recovery() is called. Size should be 2 if IS is used, 1 if COW is used
    private static Deque<String> snapshotVersionCounter = new ArrayDeque<>();

    /*
    * Choosing between Incremental Snapshot and Copy on Write:
    * - If Copy on Write is used, set boolean COW = true
    * - If Incremental Snapshot is used, set COW = false
    */
    private static boolean COW = false;
    private static int lastRecordedIndex = 0; // utility for incremental snapshot
    private static Iterator<?> tupleIterator;

    public CallTablesMemory(CairoEngine engine) throws SqlException{
        try{
            if (SnapAtStart != 0){
                ObjHashSet<TableToken> tables = new ObjHashSet<>();
                engine.getTableTokens(tables, false);

                for (int t = 0, n = tables.size(); t < n; t++) {
                    TableToken tableToken = tables.get(t);

                    // Skipping system-related tables for the snapshot
                    if (tableToken.getTableName().startsWith("sys.") || tableToken.getTableName().startsWith("telemetry")) {
                        LOG.info().$("[EDIT] Skipping system table: ").$(tableToken.getTableName()).$();
                        continue;
                    }

                    TableReader reader = null;

                    // Since at initialization there's no other worker scanning the table
                    // no need to check whether the table is available for the reader or not
                    reader = engine.getReaderWithRepair(tableToken);
                    int partitionCount = reader.getPartitionCount();

                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                        long rowCount = reader.openPartition(partitionIndex);
                        if (rowCount > 1){
                            Object[] keys = getColumnValues(reader, partitionIndex, 0, rowCount);

                            for (int columnIndex = 1; columnIndex < reader.getColumnCount(); columnIndex++) {   
                                Object[] values = getColumnValues(reader, partitionIndex, 0, rowCount);

                                if (values.length == 0) {
                                    LOG.info().$("[EDIT] Skipping empty or unsupported column at index ").$(columnIndex).$();
                                    continue;
                                }

                                if (keys.length != values.length) {
                                    throw new IllegalStateException("[EDIT] Mismatched lengths between keys and values. Keys length: "
                                            + keys.length + ", Values length: " + values.length);
                                }
                            
                                for (int i = 0; i < keys.length; i++) {
                                    Long timestamp = (Long) keys[i];
                                    Object value = values[i];  
                                    TableTokenTimestampKey key = new TableTokenTimestampKey(tableToken, timestamp);
                                    updatedTuples.put(key, value);
                                } 

                                // Timestamp as converted into microseconds since 1970-01-01T00:00:00 UTC
                                LOG.info().$("[EDIT] [First value of column=").$(values[0]).I$();
                                //Misc.free(column); [Note] Cleaning column up not necessary and will mess up the existing pointers
                            }
                        }
                    }
                    Misc.free(reader);
                    LOG.info().$("[EDIT] Token Name: ").$(tableToken.getTableName()).$();
                    //updatedTuples.put(tableToken, new ArrayList<>());
                }

            }
            LOG.info().$("[EDIT] Size of the global hashmap ").$(updatedTuples.size()).$();
        } 
        catch (Throwable th) {
            close();
            throw th;
        }
    }

    private Object[] getColumnValues(TableReader reader, int partitionIndex, int columnIndex, long rowCount) {
        MemoryCR column = null;
        int absoluteIndex = reader.getPrimaryColumnIndex(reader.getColumnBase(partitionIndex), columnIndex);
        column = reader.getColumn(absoluteIndex);
        Object[] values = readEntireColumn(column, reader.getMetadata().getColumnType(columnIndex), rowCount);
        return values;
    }

    private Object[] readEntireColumn(MemoryCR columnData, int columnType, long rowCount) {
        Object[] values = new Object[(int) rowCount];
        DirectString tempStr = new DirectString(); // Temporary storage for strings
    
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            long offset = rowIndex * ColumnType.sizeOf(columnType); // Calculate the memory offset
    
            if (columnType == ColumnType.BINARY) {
                LOG.info().$("[EDIT] Skipping binary column: ");
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
                    columnData.getStr(offset, tempStr); 
                    values[rowIndex] = tempStr.toString(); // Convert DirectString to regular String
                    break;
                case ColumnType.TIMESTAMP:
                    long timestampMicros = columnData.getLong(offset); // Read TIMESTAMP as long
                    values[rowIndex] = timestampMicros; // Store raw timestamp (or format if needed)
                    break;
                default:
                    LOG.info().$("[EDIT] Unsupported column type: ").$(columnType).$();
                    return new Object[0];
            }
        }
        return values;
    }
    
    @Override
    public void close() {
        LOG.info().$("[EDIT] Background worker stopped").$();
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
        int STEPS = 1;

        Runnable snapshotRunnable = new Runnable() {
            public void run() {
                if (cow) {
                    copyOnWrite();

                } else {
                    incrementalSnapshot();
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(snapshotRunnable, 0, STEPS, TimeUnit.SECONDS);

    }

    private void copyOnWrite() {
        /*
        * Flow:
        * (1) Previous snapshots are discarded
        * (2) All elements in columnDataList is serialized to binary and saved to disk.
        */

        String latestSnapshotFile = writeToDisk(updatedTuples);

        while (!snapshotVersionCounter.isEmpty()) {
            disposeSnapshot(snapshotVersionCounter.remove());
        }

        snapshotVersionCounter.add(latestSnapshotFile);
    }

    private int incrementalSnapshot() {
        /*
        * Flow: 
        * (1) Creates a sublist of columnDataList from the lastRecordedIndex to the end of 
        * (2) Saves everything to disk from versionTwoTuples as a binary file with extension .d (as is common in QuestDB)
        * (3) Updates the global variable snapshotVersionCounter so that at a time we only save two snapshots: the ultimate and penultimate
        * (4) Cleans out previous files from disk
        */

        Map<TableToken, List<Object>> versionTwoTuples = subsetCreator();
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

    private Map<TableToken, List<Object>> subsetCreator() {
        /*
        * Creates a sublist of updatedTuples
        */
       if (tupleIterator == null || !tupleIterator.hasNext()) {
            tupleIterator = updatedTuples.entrySet().iterator();
        }
        
        // Create a new HashMap to store remaining entries
        Map<TableToken, List<Object>> remainingTuples = new HashMap<>();
        
        // Add all remaining elements from the current iterator position
        while (tupleIterator.hasNext()) {
            Entry<TableToken, List<Object>> entry = 
                (Entry<TableToken, List<Object>>) tupleIterator.next();
            remainingTuples.put(entry.getKey(), entry.getValue());
        }
        
        return remainingTuples;
        
    }

    private String writeToDisk(Map<TableToken, List<Object>> versionTwoTuples) {
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

    private Map<TableToken, List<Object>> recoverFromDisk(String filePath) {
        /*
        * Utility function for recovery()
        */
        try {
            return BinarySerializer.deserializeFromBinary(filePath);
        } catch (IOException e) {
        }
        return new HashMap<>();
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
