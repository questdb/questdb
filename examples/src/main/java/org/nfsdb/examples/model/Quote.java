/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2016 Appsicle
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

package org.nfsdb.examples.model;

@SuppressWarnings("unused")
public class Quote {
    private long timestamp;
    private String sym;
    private double bid;
    private double ask;
    private int bidSize;
    private int askSize;
    private String mode;
    private String ex;

    public void clear() {
        this.timestamp = 0;
        this.sym = null;
        this.bid = 0;
        this.ask = 0;
        this.bidSize = 0;
        this.askSize = 0;
        this.mode = null;
        this.ex = null;
    }

    public double getAsk() {
        return ask;
    }

    public void setAsk(double ask) {
        this.ask = ask;
    }

    public int getAskSize() {
        return askSize;
    }

    public void setAskSize(int askSize) {
        this.askSize = askSize;
    }

    public double getBid() {
        return bid;
    }

    public void setBid(double bid) {
        this.bid = bid;
    }

    public int getBidSize() {
        return bidSize;
    }

    public void setBidSize(int bidSize) {
        this.bidSize = bidSize;
    }

    public String getEx() {
        return ex;
    }

    public void setEx(String ex) {
        this.ex = ex;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getSym() {
        return sym;
    }

    public void setSym(String sym) {
        this.sym = sym;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (sym != null ? sym.hashCode() : 0);
        temp = Double.doubleToLongBits(bid);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(ask);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + bidSize;
        result = 31 * result + askSize;
        result = 31 * result + (mode != null ? mode.hashCode() : 0);
        return 31 * result + (ex != null ? ex.hashCode() : 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Quote)) return false;

        Quote q = (Quote) o;

        return Double.compare(q.ask, ask) == 0
                && askSize == q.askSize
                && Double.compare(q.bid, bid) == 0
                && bidSize == q.bidSize
                && timestamp == q.timestamp
                && !(ex != null ? !ex.equals(q.ex) : q.ex != null)
                && !(mode != null ? !mode.equals(q.mode) : q.mode != null)
                && !(sym != null ? !sym.equals(q.sym) : q.sym != null);

    }

    @Override
    public String toString() {
        return "Quote{" +
                "timestamp=" + timestamp +
                ", sym='" + sym + '\'' +
                ", bid=" + bid +
                ", ask=" + ask +
                ", bidSize=" + bidSize +
                ", askSize=" + askSize +
                ", mode='" + mode + '\'' +
                ", ex='" + ex + '\'' +
                '}';
    }
}
