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

package com.questdb.model;

public class TestEntity {
    private long timestamp;
    private String bStr;
    private double aDouble;
    private int anInt;
    private String sym;
    private String dStr;
    private String dwStr;

    public double getADouble() {
        return aDouble;
    }

    public TestEntity setADouble(double aDouble) {
        this.aDouble = aDouble;
        return this;
    }

    public int getAnInt() {
        return anInt;
    }

    public TestEntity setAnInt(int anInt) {
        this.anInt = anInt;
        return this;
    }

    public String getBStr() {
        return bStr;
    }

    public TestEntity setBStr(String bStr) {
        this.bStr = bStr;
        return this;
    }

    public String getDStr() {
        return dStr;
    }

    public TestEntity setDStr(String dStr) {
        this.dStr = dStr;
        return this;
    }

    public String getDwStr() {
        return dwStr;
    }

    public TestEntity setDwStr(String dwStr) {
        this.dwStr = dwStr;
        return this;
    }

    public String getSym() {
        return sym;
    }

    public TestEntity setSym(String sym) {
        this.sym = sym;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TestEntity setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }
}
