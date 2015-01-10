/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.factory.configuration;

import com.nfsdb.JournalKey;
import com.nfsdb.PartitionType;
import com.nfsdb.column.HugeBuffer;
import com.nfsdb.exceptions.JournalRuntimeException;
import org.jetbrains.annotations.NotNull;

public interface JournalMetadata<T> {

    JournalKey<T> deriveKey();

    /**
     * Number of columns in Journal.
     *
     * @return column count
     */
    int getColumnCount();

    /**
     * Lookup column metadata by name. This method is slower than
     * simple array de-reference of index lookup. Column names are case-sensitive.
     * <p/>
     * This method cannot return null. An exception is thrown if column name is invalid.
     *
     * @param name of column
     * @return column metadata
     */
    @NotNull
    ColumnMetadata getColumnMetadata(CharSequence name);

    /**
     * Lookup column metadata by index. This method does unchecked exception and
     * will throw ArrayIndexOutOfBoundsException.
     * <p/>
     * To obtain column index and validate column name {@see #getColumnIndex}
     *
     * @param columnIndex index of column
     * @return column metadata
     */
    ColumnMetadata getColumnMetadata(int columnIndex);

    /**
     * Lookup column index and validate column name. Column names are case-sensitive and {#JournalRuntimeException} is
     * thrown if column name is invalid.
     *
     * @param columnName the name
     * @return 0-based column index
     */
    int getColumnIndex(CharSequence columnName);

    ColumnMetadata getTimestampMetadata();

    int getTimestampIndex();

    String getLocation();

    PartitionType getPartitionType();

    Object newObject() throws JournalRuntimeException;

    Class<T> getModelClass();

    long getOpenFileTTL();

    int getLag();

    int getRecordHint();

    int getTxCountHint();

    String getKey();

    String getKeyQuiet();

    String getId();

    void write(HugeBuffer buf);

    boolean isPartialMapped();
}