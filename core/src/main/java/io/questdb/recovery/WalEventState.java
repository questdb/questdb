/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.recovery;

import io.questdb.std.ObjList;

/**
 * Mutable state populated by {@link BoundedWalEventReader} after parsing
 * a WAL segment's {@code _event} and {@code _event.i} files. Contains the
 * event list, header fields, and any issues encountered during reading.
 */
public class WalEventState {
    private final ObjList<WalEventEntry> events = new ObjList<>();
    private long eventFileSize = -1;
    private long eventIndexFileSize = -1;
    private String eventIndexPath;
    private String eventPath;
    private int formatVersion = TxnState.UNSET_INT;
    private final ObjList<ReadIssue> issues = new ObjList<>();
    private int maxTxn = TxnState.UNSET_INT;

    public long getEventFileSize() {
        return eventFileSize;
    }

    public long getEventIndexFileSize() {
        return eventIndexFileSize;
    }

    public String getEventIndexPath() {
        return eventIndexPath;
    }

    public String getEventPath() {
        return eventPath;
    }

    public ObjList<WalEventEntry> getEvents() {
        return events;
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public ObjList<ReadIssue> getIssues() {
        return issues;
    }

    public int getMaxTxn() {
        return maxTxn;
    }

    void setEventFileSize(long eventFileSize) {
        this.eventFileSize = eventFileSize;
    }

    void setEventIndexFileSize(long eventIndexFileSize) {
        this.eventIndexFileSize = eventIndexFileSize;
    }

    void setEventIndexPath(String eventIndexPath) {
        this.eventIndexPath = eventIndexPath;
    }

    void setEventPath(String eventPath) {
        this.eventPath = eventPath;
    }

    void setFormatVersion(int formatVersion) {
        this.formatVersion = formatVersion;
    }

    void setMaxTxn(int maxTxn) {
        this.maxTxn = maxTxn;
    }
}
