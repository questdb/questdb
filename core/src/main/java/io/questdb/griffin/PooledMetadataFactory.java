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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.MetadataFactory;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.Sequencer;
import io.questdb.cairo.wal.SequencerMetadata;
import io.questdb.std.*;

public class PooledMetadataFactory implements MetadataFactory, QuietClosable {
    private final FilesFacade ff;
    private final CharSequence dbRoot;
    private final CairoConfiguration configuration;
    private final WeakClosableObjectPool<ReusableTableReaderMetadata> readerMetadataPool;
    private final WeakClosableObjectPool<ReusableSequencerMetadata> sequencerMetadataPool;
    private final CairoEngine engine;
    CharSequenceObjHashMap<String> tableNamePool = new CharSequenceObjHashMap<>();
    private boolean isClosed = false;

    public PooledMetadataFactory(CairoEngine engine) {
        CairoConfiguration configuration = engine.getConfiguration();
        this.engine = engine;
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
    public TableRecordMetadata openSequencerMetadata(Sequencer sequencer) {
        SequencerMetadata sequencerMetadata = sequencerMetadataPool.pop();
        sequencer.copyMetadataTo(sequencerMetadata);
        return sequencerMetadata;
    }

    @Override
    public TableRecordMetadata openTableReaderMetadata(CharSequence tableName) {
        TableReaderMetadata tableReaderMetadata = readerMetadataPool.pop();
        String tableNameStr = resolveString(tableNamePool, tableName);
        CharSequence systemTableName = engine.getSystemTableName(tableName);
        tableReaderMetadata.readSafe(dbRoot, tableNameStr, systemTableName, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
        return tableReaderMetadata;
    }

    @Override
    public SequencerMetadata getSequencerMetadata() {
        return sequencerMetadataPool.pop();
    }

    private String resolveString(CharSequenceObjHashMap<String> tableNamePool, CharSequence tableName) {
        String tableNameStr = tableNamePool.get(tableName);
        if (tableNameStr != null) {
            return tableNameStr;
        }
        tableNameStr = Chars.toString(tableName);
        tableNamePool.put(tableNameStr, tableNameStr);
        return tableNameStr;
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
            super(ff, SequencerMetadata.READ_WRITE);
        }

        @Override
        public void close() {
            if (!isClosed && !closing) {
                super.clear();
                closing = true;
                sequencerMetadataPool.push(this);
                closing = false;
            } else {
                super.close();
            }
        }
    }
}
