/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.factory.configuration;

import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.NullsAdaptor;
import com.nfsdb.journal.factory.NullsAdaptorFactory;

import java.io.File;

public interface JournalMetadata<T> {

    NullsAdaptor<T> getNullsAdaptor();

    NullsAdaptorFactory<T> getNullsAdaptorFactory();

    ColumnMetadata getColumnMetadata(String name);

    ColumnMetadata getColumnMetadata(int columnIndex);

    int getColumnIndex(String columnName);

    String getLocation();

    PartitionType getPartitionType();

    int getColumnCount();

    ColumnMetadata getTimestampColumnMetadata();

    int getTimestampColumnIndex();

    Object newObject() throws JournalRuntimeException;

    Class<T> getModelClass();

    File getColumnIndexBase(File partitionDir, int columnIndex);

    long getOpenFileTTL();

    int getLag();

    int getRecordHint();

    int getTxCountHint();

    String getKey();

    String getKeyQuiet();

    String getId();
}
