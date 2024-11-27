/*******************************************************************************
 *  Custom data snapshot scheduler
 ******************************************************************************/

package io.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.str.DirectString;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Misc;

import java.io.Closeable;
import java.util.Timer;

public class CallTablesMemory extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(CallTablesMemory.class);

    public CallTablesMemory(CairoEngine engine) throws SqlException{
        try{
            ObjHashSet<TableToken> tables = new ObjHashSet<>();
            engine.getTableTokens(tables, false);

            for (int t = 0, n = tables.size(); t < n; t++) {
                TableToken tableToken = tables.get(t);
                LOG.info().$("[EDIT] Reading [table=").$(tableToken).I$();
                TableReader reader = null;
                // Since at initialization there's no other worker scanning the table
                reader = engine.getReaderWithRepair(tableToken);
                int partitionCount = reader.getPartitionCount();

                for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                    //LOG.info().$("TESTING").$(partitionCount).I$();
                    long rowCount = reader.openPartition(partitionIndex);
                    if (rowCount > 1){
                        for (int columnIndex = 0; columnIndex < reader.getColumnCount(); columnIndex++) {
                            int absoluteIndex = reader.getPrimaryColumnIndex(reader.getColumnBase(partitionIndex), columnIndex);
                            LOG.info().$("[EDIT]").$(reader.getColumn(absoluteIndex)).I$();
                        }
                    }
                }

                Misc.free(reader);



                }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /*
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
                default:
                    LOG.info().$("Unsupported column type: ").$(columnType).$();
                    return new Object[0]; // Return an empty array for unsupported types
            }
        }
        return values;
    }
    */

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
}
