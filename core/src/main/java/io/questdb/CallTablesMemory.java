/*******************************************************************************
 *  Custom data snapshot scheduler
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
    public static Map<TableToken, List<Object>> updatedTuples = new HashMap<>();;

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

                LOG.info().$("[EDIT] Token Name: ").$(tableToken.getTableName()).$();
                updatedTuples.put(tableToken, new ArrayList<>());
            }
            LOG.info().$("[EDIT] Size of the global hashmap ").$(updatedTuples.size()).$();
        } 
        catch (Throwable th) {
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
        //Misc.free(updatedTuples);
        //Misc.free(columnDataList_2);
        //Misc.free(tables2idx);
        LOG.info().$("[EDIT] Background worker stopped").$();
    }

    @Override
    public boolean runSerially() {
        /* Implementation of Snapshot strategies can go in here */
        
        return false;
    }
}
