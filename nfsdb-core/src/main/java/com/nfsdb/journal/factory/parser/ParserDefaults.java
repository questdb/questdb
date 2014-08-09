/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.factory.parser;

import com.nfsdb.journal.factory.NullsAdaptorFactory;

public class ParserDefaults {
    private int globalRecordHint;
    private int recordHint;
    private int openPartitionTTL;
    private int lagHours;
    private int stringAvgSize;

    private NullsAdaptorFactory nullsAdaptorFactory;

    public int getRecordHint() {
        return recordHint;
    }

    public void setRecordHint(int recordHint) {
        this.recordHint = recordHint;
    }

    public int getGlobalRecordHint() {
        return globalRecordHint;
    }

    public void setGlobalRecordHint(int globalRecordHint) {
        this.globalRecordHint = globalRecordHint;
    }

    public int getOpenPartitionTTL() {
        return openPartitionTTL;
    }

    public void setOpenPartitionTTL(int openPartitionTTL) {
        this.openPartitionTTL = openPartitionTTL;
    }

    public int getLagHours() {
        return lagHours;
    }

    public void setLagHours(int lagHours) {
        this.lagHours = lagHours;
    }

    public int getStringAvgSize() {
        return stringAvgSize;
    }

    public void setStringAvgSize(int stringAvgSize) {
        this.stringAvgSize = stringAvgSize;
    }

    public NullsAdaptorFactory getNullsAdaptorFactory() {
        return nullsAdaptorFactory;
    }

    public void setNullsAdaptorFactory(NullsAdaptorFactory nullsAdaptorFactory) {
        this.nullsAdaptorFactory = nullsAdaptorFactory;
    }
}
