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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.*;
import io.questdb.cairo.vm.MemoryFCRImpl;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.MetadataChangeSPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.tasks.TableWriterTask;

public class AlterOperation extends AbstractOperation implements Mutable {
    public final static short ADD_COLUMN = 1;
    public final static short ADD_INDEX = 4;
    public final static short ADD_SYMBOL_CACHE = 6;
    public final static short ATTACH_PARTITION = 3;
    public final static String CMD_NAME = "ALTER TABLE";
    public final static short DETACH_PARTITION = 12;
    public final static short DO_NOTHING = 0;
    public final static short DROP_COLUMN = 8;
    public final static short DROP_INDEX = 5;
    public final static short DROP_PARTITION = 2;
    public final static short REMOVE_SYMBOL_CACHE = 7;
    public final static short RENAME_COLUMN = 9;
    public final static short SET_PARAM_COMMIT_LAG = 11;
    public final static short SET_PARAM_MAX_UNCOMMITTED_ROWS = 10;
    private final static Log LOG = LogFactory.getLog(AlterOperation.class);
    private final DirectCharSequenceList directCharList = new DirectCharSequenceList();
    // This is only used to serialize partition name in form 2020-02-12 or 2020-02 or 2020
    // to exception message using TableUtils.setSinkForPartition.
    private final LongList longList;
    private final ObjCharSequenceList objCharList;
    private CharSequenceList charSequenceList;
    private short command;
    private MemoryFCRImpl deserializeMem;

    public AlterOperation() {
        this(new LongList(), new ObjList<>());
    }

    public AlterOperation(LongList longList, ObjList<CharSequence> charSequenceObjList) {
        this.longList = longList;
        this.objCharList = new ObjCharSequenceList(charSequenceObjList);
        this.command = DO_NOTHING;
    }

    @Override
    // todo: supply bitset to indicate which ops are supported and which arent
    //     "structural changes" doesn't cover is as "add column" is supported
    public long apply(MetadataChangeSPI tableWriter, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
        try {
            switch (command) {
                case ADD_COLUMN:
                    applyAddColumn(tableWriter);
                    break;
                case DROP_COLUMN:
                    if (!contextAllowsAnyStructureChanges) {
                        throw AlterTableContextException.INSTANCE;
                    }
                    applyDropColumn(tableWriter);
                    break;
                case RENAME_COLUMN:
                    if (!contextAllowsAnyStructureChanges) {
                        throw AlterTableContextException.INSTANCE;
                    }
                    applyRenameColumn(tableWriter);
                    break;
                case DROP_PARTITION:
                    applyDropPartition(tableWriter);
                    break;
                case DETACH_PARTITION:
                    applyDetachPartition(tableWriter);
                    break;
                case ATTACH_PARTITION:
                    applyAttachPartition(tableWriter);
                    break;
                case ADD_INDEX:
                    applyAddIndex(tableWriter);
                    break;
                case DROP_INDEX:
                    applyDropIndex(tableWriter);
                    break;
                case ADD_SYMBOL_CACHE:
                    applySetSymbolCache(tableWriter, true);
                    break;
                case REMOVE_SYMBOL_CACHE:
                    applySetSymbolCache(tableWriter, false);
                    break;
                case SET_PARAM_MAX_UNCOMMITTED_ROWS:
                    applyParamUncommittedRows(tableWriter);
                    break;
                case SET_PARAM_COMMIT_LAG:
                    applyParamO3MaxLag(tableWriter);
                    break;
                default:
                    LOG.error()
                            .$("invalid alter table command [code=").$(command)
                            .$(" ,table=").$(tableWriter.getTableToken())
                            .I$();
                    throw CairoException.critical(0).put("invalid alter table command [code=").put(command).put(']');
            }
        } catch (EntryUnavailableException ex) {
            throw ex;
        } catch (CairoException e) {
            LOG.critical().$("could not alter table [table=").$(tableWriter.getTableToken())
                    .$(", command=").$(command)
                    .$(", errno=").$(e.getErrno())
                    .$(", message=`").$(e.getFlyweightMessage()).$('`')
                    .I$();

            throw e;
        }
        return 0;
    }

    @Override
    public void clear() {
        command = DO_NOTHING;
        objCharList.clear();
        directCharList.clear();
        charSequenceList = objCharList;
        longList.clear();
        clearCommandCorrelationId();
    }

    @Override
    public AlterOperation deserialize(TableWriterTask event) {
        clear();

        tableToken = event.getTableToken();
        long readPtr = event.getData();
        long length = event.getDataSize();
        final long hi = readPtr + length;

        // This is not hot path, do safe deserialization
        if (readPtr + 10 >= hi) {
            throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [1]");
        }

        if (deserializeMem == null) {
            deserializeMem = new MemoryFCRImpl();
        }
        deserializeMem.of(readPtr, length);
        deserializeBody(deserializeMem, 0L, length);
        return this;
    }

    public void deserializeBody(MemoryCR buffer, long offsetLo, long offsetHi) {
        clear();
        long readPtr = offsetLo;
        if (readPtr + 10 > offsetHi) {
            throw CairoException
                    .critical(0)
                    .put("cannot read alter statement serialized, data is too short to read 10 bytes header [offset=")
                    .put(offsetLo)
                    .put(", size=")
                    .put(offsetHi - offsetLo)
                    .put(']');
        }
        command = buffer.getShort(readPtr);
        readPtr += 2;
        tableNamePosition = buffer.getInt(readPtr);
        readPtr += 4;
        int longSize = buffer.getInt(readPtr);
        readPtr += 4;
        if (longSize < 0 || readPtr + longSize * 8L >= offsetHi) {
            throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [2]");
        }
        longList.clear();
        for (int i = 0; i < longSize; i++) {
            longList.add(buffer.getLong(readPtr));
            readPtr += 8;
        }
        directCharList.of(buffer, readPtr, offsetHi);
        charSequenceList = directCharList;
    }

    @Override
    public boolean isStructureChange() {
        switch (command) {
            case ADD_COLUMN:
            case RENAME_COLUMN:
            case DROP_COLUMN:
                return true;
            default:
                return false;
        }
    }

    public AlterOperation of(
            short command,
            TableToken tableToken,
            int tableId,
            int tableNamePosition
    ) {
        init(TableWriterTask.CMD_ALTER_TABLE, CMD_NAME, tableToken, tableId, -1, tableNamePosition);
        this.command = command;
        this.charSequenceList = this.objCharList;
        return this;
    }

    @Override
    public void serialize(TableWriterTask event) {
        super.serialize(event);
        event.putShort(command);
        event.putInt(tableNamePosition);
        event.putInt(longList.size());
        for (int i = 0, n = longList.size(); i < n; i++) {
            event.putLong(longList.getQuick(i));
        }

        event.putInt(objCharList.size());
        for (int i = 0, n = objCharList.size(); i < n; i++) {
            event.putStr(objCharList.getStrA(i));
        }
    }

    public void serializeBody(MemoryA sink) {
        sink.putShort(command);
        sink.putInt(tableNamePosition);
        sink.putInt(longList.size());
        for (int i = 0, n = longList.size(); i < n; i++) {
            sink.putLong(longList.getQuick(i));
        }

        sink.putInt(objCharList.size());
        for (int i = 0, n = objCharList.size(); i < n; i++) {
            sink.putStr(objCharList.getStrA(i));
        }
    }

    @Override
    public void startAsync() {
    }

    private void applyAddColumn(MetadataChangeSPI tableWriter) {
        int lParam = 0;
        for (int i = 0, n = charSequenceList.size(); i < n; i++) {
            CharSequence columnName = charSequenceList.getStrA(i);
            int type = (int) longList.get(lParam++);
            int symbolCapacity = (int) longList.get(lParam++);
            boolean symbolCacheFlag = longList.get(lParam++) > 0;
            boolean isIndexed = longList.get(lParam++) > 0;
            int indexValueBlockCapacity = (int) longList.get(lParam++);
            int columnNamePosition = (int) longList.get(lParam++);
            try {
                tableWriter.addColumn(
                        columnName,
                        type,
                        symbolCapacity,
                        symbolCacheFlag,
                        isIndexed,
                        indexValueBlockCapacity,
                        false
                );
            } catch (CairoException e) {
                e.position(columnNamePosition);
                throw e;
            }
        }
    }

    private void applyAddIndex(MetadataChangeSPI tableWriter) {
        final CharSequence columnName = charSequenceList.getStrA(0);
        try {
            tableWriter.addIndex(columnName, (int) longList.get(0));
        } catch (CairoException e) {
            // augment exception with table position
            e.position(tableNamePosition);
            throw e;
        }
    }

    private void applyAttachPartition(MetadataChangeSPI tableWriter) {
        for (int i = 0, n = longList.size() / 2; i < n; i++) {
            final long partitionTimestamp = longList.getQuick(i * 2);
            AttachDetachStatus attachDetachStatus = tableWriter.attachPartition(partitionTimestamp);
            if (AttachDetachStatus.OK != attachDetachStatus) {
                throw CairoException.critical(CairoException.METADATA_VALIDATION).put("could not attach partition [table=").put(tableToken != null ? tableToken.getTableName() : "<null>")
                        .put(", detachStatus=").put(attachDetachStatus.name())
                        .put(", partitionTimestamp=").ts(partitionTimestamp)
                        .put(", partitionBy=").put(PartitionBy.toString(tableWriter.getPartitionBy()))
                        .put(']')
                        .position((int) longList.getQuick(i * 2 + 1));
            }
        }
    }

    private void applyDetachPartition(MetadataChangeSPI tableWriter) {
        for (int i = 0, n = longList.size() / 2; i < n; i++) {
            final long partitionTimestamp = longList.getQuick(i * 2);
            AttachDetachStatus attachDetachStatus = tableWriter.detachPartition(partitionTimestamp);
            if (AttachDetachStatus.OK != attachDetachStatus) {
                throw CairoException.critical(CairoException.METADATA_VALIDATION).put("could not detach partition [table=").put(tableToken != null ? tableToken.getTableName() : "<null>")
                        .put(", detachStatus=").put(attachDetachStatus.name())
                        .put(", partitionTimestamp=").ts(partitionTimestamp)
                        .put(", partitionBy=").put(PartitionBy.toString(tableWriter.getPartitionBy()))
                        .put(']')
                        .position((int) longList.getQuick(i * 2 + 1));
            }
        }
    }

    private void applyDropColumn(MetadataChangeSPI writer) {
        for (int i = 0, n = charSequenceList.size(); i < n; i++) {
            writer.removeColumn(charSequenceList.getStrA(i));
        }
    }

    private void applyDropIndex(MetadataChangeSPI tableWriter) {
        final CharSequence columnName = charSequenceList.getStrA(0);
        final int columnNamePosition = (int) longList.get(0);
        try {
            tableWriter.dropIndex(columnName);
        } catch (CairoException e) {
            e.position(columnNamePosition);
            throw e;
        }
    }

    private void applyDropPartition(MetadataChangeSPI tableWriter) {
        // long list is a set of two longs per partition - (timestamp, partitionNamePosition)
        for (int i = 0, n = longList.size() / 2; i < n; i++) {
            long partitionTimestamp = longList.getQuick(i * 2);
            if (!tableWriter.removePartition(partitionTimestamp)) {
                throw CairoException.nonCritical()
                        .put("could not remove partition [table=").put(tableToken != null ? tableToken.getTableName() : "<null>")
                        .put(", partitionTimestamp=").ts(partitionTimestamp)
                        .put(", partitionBy=").put(PartitionBy.toString(tableWriter.getPartitionBy()))
                        .put(']')
                        .position((int) longList.getQuick(i * 2 + 1));
            }
        }
    }

    private void applyParamO3MaxLag(MetadataChangeSPI tableWriter) {
        long o3MaxLag = longList.get(0);
        try {
            tableWriter.setMetaO3MaxLag(o3MaxLag);
        } catch (CairoException e) {
            LOG.error().$("could not change o3MaxLag [table=").utf8(tableToken != null ? tableToken.getTableName() : "<null>")
                    .$(", errno=").$(e.getErrno())
                    .$(", error=").$(e.getFlyweightMessage())
                    .I$();
            throw e;
        }
    }

    private void applyParamUncommittedRows(MetadataChangeSPI tableWriter) {
        int maxUncommittedRows = (int) longList.get(0);
        try {
            tableWriter.setMetaMaxUncommittedRows(maxUncommittedRows);
        } catch (CairoException e) {
            e.position(tableNamePosition);
            throw e;
        }
    }

    private void applyRenameColumn(MetadataChangeSPI writer) {
        // To avoid storing 2 var len fields, store only new name as CharSequence
        // and index of existing column.
        int i = 0, n = charSequenceList.size();
        while (i < n) {
            CharSequence columnName = charSequenceList.getStrA(i++);
            CharSequence newName = charSequenceList.getStrB(i++);
            writer.renameColumn(columnName, newName);
        }
    }

    private void applySetSymbolCache(MetadataChangeSPI tableWriter, boolean isCacheOn) {
        CharSequence columnName = charSequenceList.getStrA(0);
        tableWriter.changeCacheFlag(
                tableWriter.getMetadata().getColumnIndex(columnName),
                isCacheOn
        );
    }

    interface CharSequenceList extends Mutable {
        CharSequence getStrA(int i);

        CharSequence getStrB(int i);

        int size();
    }

    private static class DirectCharSequenceList implements CharSequenceList {
        private final LongList offsets = new LongList();
        private final DirectCharSequence strA = new DirectCharSequence();
        private final DirectCharSequence strB = new DirectCharSequence();

        @Override
        public void clear() {
            offsets.clear();
        }

        public CharSequence getStrA(int i) {
            long lo = offsets.get(i * 2);
            long hi = offsets.get(i * 2 + 1);
            strA.of(lo, hi);
            return strA;
        }

        @Override
        public CharSequence getStrB(int i) {
            long lo = offsets.get(i * 2);
            long hi = offsets.get(i * 2 + 1);
            strB.of(lo, hi);
            return strB;
        }

        public void of(MemoryCR buffer, long lo, long hi) {
            if (lo + Integer.BYTES > hi) {
                throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [11]");
            }
            int size = buffer.getInt(lo);
            lo += 4;
            for (int i = 0; i < size; i++) {
                if (lo + Integer.BYTES > hi) {
                    throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [12]");
                }
                int stringSize = 2 * buffer.getInt(lo);
                lo += 4;
                if (lo + stringSize > hi) {
                    throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [13]");
                }
                long address = buffer.addressOf(lo);
                offsets.add(address, address + stringSize);
                lo += stringSize;
            }
        }

        @Override
        public int size() {
            return offsets.size() / 2;
        }
    }

    private static class ObjCharSequenceList implements CharSequenceList {
        private final ObjList<CharSequence> strings;

        public ObjCharSequenceList(ObjList<CharSequence> strings) {
            this.strings = strings;
        }

        @Override
        public void clear() {
            strings.clear();
        }

        public CharSequence getStrA(int i) {
            return strings.get(i);
        }

        @Override
        public CharSequence getStrB(int i) {
            return strings.get(i);
        }

        @Override
        public int size() {
            return strings.size();
        }
    }
}
