/*******************************************************************************
 *  Custom data snapshot scheduler
 ******************************************************************************/

package io.questdb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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

public class CallTablesMemory extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(CallTablesMemory.class);
    public static List<Object[]> columnDataList = new ArrayList<>();

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
        return false;
    }

    private int copyOnUpdateSnapshot(int lastDumpedIndex) {
        List<Object[]> versionTwoTuples = columnDataList.subList(lastDumpedIndex + 1, columnDataList.size());

        String latestSnapshot = writeToDisk(versionTwoTuples);

        

        return columnDataList.size();
    }

    private String writeToDisk(List<Object[]> versionTwoTuples) {
        /*
        * The TableWriter is specirfically designed for tokens, not tuples. 
        */

       String snapshotDir = "data/snapshots/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss"));
       new File(snapshotDir).mkdirs();
       String filePath = snapshotDir + "/output.d";
       

       try {
            BinarySerializer.serializeToBinary(versionTwoTuples, "data/output.d");
        } catch (IOException e) {
            e.printStackTrace();  // or handle the error appropriately
        }


        return filePath;
    }

    private List<Object[]> recoverFromDisk(String filePath) {
        try {
            return BinarySerializer.deserializeFromBinary(filePath);
        } catch (IOException e) {
        }
        return new ArrayList<>();
    }
}
