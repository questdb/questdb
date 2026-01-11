/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.http.ilpv4;

/**
 * Represents a decoded table block from an ILP v4 message.
 * <p>
 * Provides columnar access to the decoded data for efficient WAL writing.
 */
public class IlpV4DecodedTableBlock {

    private final String tableName;
    private final int rowCount;
    private final IlpV4DecodedColumn[] columns;
    private final IlpV4ColumnDef[] schema;

    public IlpV4DecodedTableBlock(String tableName, int rowCount, IlpV4ColumnDef[] schema) {
        this.tableName = tableName;
        this.rowCount = rowCount;
        this.schema = schema;
        this.columns = new IlpV4DecodedColumn[schema.length];

        // Initialize columns from schema
        for (int i = 0; i < schema.length; i++) {
            IlpV4ColumnDef def = schema[i];
            columns[i] = new IlpV4DecodedColumn(
                    def.getName(),
                    def.getTypeCode(),
                    def.isNullable(),
                    rowCount
            );
        }
    }

    public String getTableName() {
        return tableName;
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getColumnCount() {
        return columns.length;
    }

    /**
     * Gets a column by index.
     *
     * @param index column index (0-based)
     * @return the decoded column
     */
    public IlpV4DecodedColumn getColumn(int index) {
        return columns[index];
    }

    /**
     * Gets a column by name.
     *
     * @param name column name
     * @return the decoded column, or null if not found
     */
    public IlpV4DecodedColumn getColumn(String name) {
        for (IlpV4DecodedColumn col : columns) {
            if (col.getName().equals(name)) {
                return col;
            }
        }
        return null;
    }

    /**
     * Gets the column index by name.
     *
     * @param name column name
     * @return column index, or -1 if not found
     */
    public int getColumnIndex(String name) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getName().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Gets the schema definition.
     *
     * @return array of column definitions
     */
    public IlpV4ColumnDef[] getSchema() {
        return schema;
    }

    /**
     * Creates a row iterator for sequential access.
     *
     * @return row iterator
     */
    public RowIterator rows() {
        return new RowIterator();
    }

    /**
     * Creates a column iterator for columnar access.
     *
     * @return column iterator
     */
    public ColumnIterator columns() {
        return new ColumnIterator();
    }

    /**
     * Iterator for row-by-row access.
     */
    public class RowIterator {
        private int currentRow = -1;

        public boolean hasNext() {
            return currentRow + 1 < rowCount;
        }

        public int next() {
            return ++currentRow;
        }

        public int getCurrentRow() {
            return currentRow;
        }

        public void reset() {
            currentRow = -1;
        }

        // Convenience methods for current row
        public boolean isNull(int columnIndex) {
            return columns[columnIndex].isNull(currentRow);
        }

        public byte getByte(int columnIndex) {
            return columns[columnIndex].getByte(currentRow);
        }

        public short getShort(int columnIndex) {
            return columns[columnIndex].getShort(currentRow);
        }

        public int getInt(int columnIndex) {
            return columns[columnIndex].getInt(currentRow);
        }

        public long getLong(int columnIndex) {
            return columns[columnIndex].getLong(currentRow);
        }

        public float getFloat(int columnIndex) {
            return columns[columnIndex].getFloat(currentRow);
        }

        public double getDouble(int columnIndex) {
            return columns[columnIndex].getDouble(currentRow);
        }

        public boolean getBoolean(int columnIndex) {
            return columns[columnIndex].getBoolean(currentRow);
        }

        public String getString(int columnIndex) {
            return columns[columnIndex].getString(currentRow);
        }

        public String getSymbol(int columnIndex) {
            return columns[columnIndex].getSymbol(currentRow);
        }

        public long getTimestamp(int columnIndex) {
            return columns[columnIndex].getTimestamp(currentRow);
        }

        public long getDate(int columnIndex) {
            return columns[columnIndex].getDate(currentRow);
        }

        public long getUuidHi(int columnIndex) {
            return columns[columnIndex].getUuidHi(currentRow);
        }

        public long getUuidLo(int columnIndex) {
            return columns[columnIndex].getUuidLo(currentRow);
        }

        public long getGeoHash(int columnIndex) {
            return columns[columnIndex].getGeoHash(currentRow);
        }
    }

    /**
     * Iterator for column-by-column access.
     */
    public class ColumnIterator {
        private int currentColumn = -1;

        public boolean hasNext() {
            return currentColumn + 1 < columns.length;
        }

        public IlpV4DecodedColumn next() {
            return columns[++currentColumn];
        }

        public int getCurrentColumnIndex() {
            return currentColumn;
        }

        public void reset() {
            currentColumn = -1;
        }
    }
}
