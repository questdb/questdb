/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.net.ha.model;

public class JournalClientState {
    private int journalIndex;
    private boolean clientStateInvalid = true;
    private boolean waitingOnEvents = false;
    private long clientStateSyncTime = 0;
    private long txn;
    private long txPin;

    public void deepCopy(JournalClientState request) {
        request.setJournalIndex(this.getJournalIndex());
        request.setTxn(this.getTxn());
        request.setTxPin(this.getTxPin());
    }

    public long getClientStateSyncTime() {
        return clientStateSyncTime;
    }

    public void setClientStateSyncTime(long clientStateSyncTime) {
        this.clientStateSyncTime = clientStateSyncTime;
    }

    public int getJournalIndex() {
        return journalIndex;
    }

    public void setJournalIndex(int journalIndex) {
        this.journalIndex = journalIndex;
    }

    public long getTxPin() {
        return txPin;
    }

    public void setTxPin(long txPin) {
        this.txPin = txPin;
    }

    public long getTxn() {
        return txn;
    }

    public void setTxn(long txn) {
        this.txn = txn;
    }

    public void invalidateClientState() {
        this.clientStateInvalid = true;
    }

    public boolean isClientStateInvalid() {
        return !clientStateInvalid;
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
                ", clientStateInvalid=" + clientStateInvalid +
                ", waitingOnEvents=" + waitingOnEvents +
                ", clientStateSyncTime=" + clientStateSyncTime +
                ", txn=" + txn +
                ", txPin=" + txPin +
                '}';
    }
}
