/*******************************************************************************
 *  Custom in-memory data loader
 ******************************************************************************/

package io.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.str.DirectString;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Misc;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CallTablesMemory extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(CallTablesMemory.class);
    public static Map<TableTokenTimestampKey, Object> updatedTuples = new HashMap<>();
    private static final int SnapAtStart = 0;

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
        return false;
    }
}
