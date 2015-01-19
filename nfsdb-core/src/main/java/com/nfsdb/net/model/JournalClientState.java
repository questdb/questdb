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

package com.nfsdb.net.model;

import com.nfsdb.collections.DirectIntList;

public class JournalClientState {
    private final DirectIntList symbolTabKeys = new DirectIntList();
    private int journalIndex;
    private long maxRowID;
    private long lagSize;
    private boolean clientStateInvalid = true;
    private boolean waitingOnEvents = false;
    private long clientStateSyncTime = 0;

    private String lagPartitionName;

    public int getJournalIndex() {
        return journalIndex;
    }

    public void setJournalIndex(int journalIndex) {
        this.journalIndex = journalIndex;
    }

    public long getMaxRowID() {
        return maxRowID;
    }

    public void setMaxRowID(long maxRowID) {
        this.maxRowID = maxRowID;
    }

    public long getLagSize() {
        return lagSize;
    }

    public void setLagSize(long lagSize) {
        this.lagSize = lagSize;
    }

    public void addSymbolTabKey(int key) {
        symbolTabKeys.add(key);
    }

    public DirectIntList getSymbolTabKeys() {
        return symbolTabKeys;
    }

    public boolean isClientStateInvalid() {
        return !clientStateInvalid;
    }

    public void invalidateClientState() {
        this.clientStateInvalid = true;
    }

    public boolean isWaitingOnEvents() {
        return waitingOnEvents;
    }

    public void setWaitingOnEvents(boolean waitingOnEvents) {
        this.waitingOnEvents = waitingOnEvents;
    }

    @Override
    public String toString() {
        return "JournalClientState{" +
                "journalIndex=" + journalIndex +
                ", maxRowID=" + maxRowID +
                ", lagSize=" + lagSize +
                ", symbolTabKeys=" + symbolTabKeys +
                ", clientStateInvalid=" + clientStateInvalid +
                ", waitingOnEvents=" + waitingOnEvents +
                ", clientStateSyncTime=" + clientStateSyncTime +
                ", lagPartitionName='" + lagPartitionName + '\'' +
                '}';
    }

    public void deepCopy(JournalClientState request) {
        request.setJournalIndex(this.getJournalIndex());
        request.setMaxRowID(this.getMaxRowID());
        request.setLagPartitionName(this.getLagPartitionName());
        request.setLagSize(this.getLagSize());
        request.reset();
        for (int i = 0, sz = symbolTabKeys.size(); i < sz; i++) {
            request.addSymbolTabKey(symbolTabKeys.get(i));
        }
    }

    public void reset() {
        symbolTabKeys.reset();
    }

    public void setSymbolTableKey(int columnIndex, int key) {
        while (symbolTabKeys.size() <= columnIndex) {
            symbolTabKeys.add(-1);
        }
        symbolTabKeys.set(columnIndex, key);
    }

    public String getLagPartitionName() {
        return lagPartitionName;
    }

    public void setLagPartitionName(String lagPartitionName) {
        this.lagPartitionName = lagPartitionName;
    }

    public long getClientStateSyncTime() {
        return clientStateSyncTime;
    }

    public void setClientStateSyncTime(long clientStateSyncTime) {
        this.clientStateSyncTime = clientStateSyncTime;
    }
}
