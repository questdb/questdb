/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.wal.SequencerMetadata;
import io.questdb.std.*;

public class PooledMetadataFactory implements MetadataFactory {
    private final FilesFacade ff;
    private final CharSequence dbRoot;
    private final CairoConfiguration configuration;
    private final WeakClosableObjectPool<ReusableTableReaderMetadata> readerMetadataPool;
    private final WeakClosableObjectPool<ReusableSequencerMetadata> sequencerMetadataPool;
    private boolean isClosed = false;

    public PooledMetadataFactory(CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.dbRoot = configuration.getRoot();
        int poolCapacity = configuration.getMetadataPoolCapacity();
        this.configuration = configuration;
        this.readerMetadataPool = new WeakClosableObjectPool<>(ReusableTableReaderMetadata::new, poolCapacity, poolCapacity > 1);
        this.sequencerMetadataPool = new WeakClosableObjectPool<>(ReusableSequencerMetadata::new, poolCapacity, poolCapacity > 1);
    }

    @Override
    public void close() {
        isClosed = true;
        Misc.free(readerMetadataPool);
        Misc.free(sequencerMetadataPool);
    }

    @Override
    public TableRecordMetadata openTableReaderMetadata(String tableName) {
        TableReaderMetadata tableReaderMetadata = readerMetadataPool.pop();
        try {
            tableReaderMetadata.readSafe(dbRoot, tableName, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
            return tableReaderMetadata;
        } catch (CairoException e) {
            Misc.free(tableReaderMetadata);
            throw e;
        }
    }

    @Override
    public TableRecordMetadata openTableReaderMetadata(TableReader tableReader) {
        TableReaderMetadata tableReaderMetadata = readerMetadataPool.pop();
        tableReaderMetadata.copy(tableReader.getMetadata());
        return tableReaderMetadata;
    }

    @Override
    public SequencerMetadata getSequencerMetadata() {
        return sequencerMetadataPool.pop();
    }

    private class ReusableTableReaderMetadata extends TableReaderMetadata {
        boolean closing = false;

        ReusableTableReaderMetadata() {
            super(ff, "<temp>");
        }

        @Override
        public void close() {
            if (!isClosed && !closing) {
                super.clear();
                closing = true;
                readerMetadataPool.push(this);
                closing = false;
            } else {
                super.close();
            }
        }
    }

    private class ReusableSequencerMetadata extends SequencerMetadata {
        boolean closing = false;

        ReusableSequencerMetadata() {
            super(ff);
        }

        @Override
        public void close() {
            if (!isClosed && !closing) {
                super.clear(Vm.TRUNCATE_TO_PAGE);
                closing = true;
                sequencerMetadataPool.push(this);
                closing = false;
            } else {
                super.close();
            }
        }
    }
}
