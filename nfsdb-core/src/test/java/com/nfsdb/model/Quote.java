/*
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
 */

package com.nfsdb.model;

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

    public Quote setAsk(double ask) {
        this.ask = ask;
        return this;
    }

    public int getAskSize() {
        return askSize;
    }

    public Quote setAskSize(int askSize) {
        this.askSize = askSize;
        return this;
    }

    public double getBid() {
        return bid;
    }

    public Quote setBid(double bid) {
        this.bid = bid;
        return this;
    }

    public int getBidSize() {
        return bidSize;
    }

    public Quote setBidSize(int bidSize) {
        this.bidSize = bidSize;
        return this;
    }

    public String getEx() {
        return ex;
    }

    public Quote setEx(String ex) {
        this.ex = ex;
        return this;
    }

    public String getMode() {
        return mode;
    }

    public Quote setMode(String mode) {
        this.mode = mode;
        return this;
    }

    public String getSym() {
        return sym;
    }

    public Quote setSym(String sym) {
        this.sym = sym;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Quote setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
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
        result = 31 * result + (ex != null ? ex.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Quote)) return false;

        Quote quote = (Quote) o;

        return Double.compare(quote.ask, ask) == 0
                && askSize == quote.askSize
                && Double.compare(quote.bid, bid) == 0
                && bidSize == quote.bidSize
                && timestamp == quote.timestamp
                && !(ex != null ? !ex.equals(quote.ex) : quote.ex != null)
                && !(mode != null ? !mode.equals(quote.mode) : quote.mode != null)
                && !(sym != null ? !sym.equals(quote.sym) : quote.sym != null);

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
