/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
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
