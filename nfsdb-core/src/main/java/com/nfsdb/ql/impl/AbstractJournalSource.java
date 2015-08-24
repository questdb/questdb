/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql.impl;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;

abstract class AbstractJournalSource<T extends Record> extends AbstractImmutableIterator<T> implements RecordMetadata {
    private final JournalMetadata metadata;

    AbstractJournalSource(JournalMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return metadata.getColumn(index);
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return metadata.getColumn(name);
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        return metadata.getColumnIndex(name);
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return metadata.getColumnQuick(index);
    }

    @Override
    public RecordColumnMetadata getTimestampMetadata() {
        return metadata.getTimestampMetadata();
    }

    @Override
    public boolean invalidColumn(CharSequence name) {
        return metadata.invalidColumn(name);
    }
}
