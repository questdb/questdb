/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.model;

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
