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
 *
 ******************************************************************************/

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
