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

package com.nfsdb.ha.model;

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
