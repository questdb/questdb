/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.std.Rosti;
import io.questdb.std.Vect;

import static io.questdb.griffin.SqlCodeGenerator.GKK_MICRO_HOUR_INT;
import static io.questdb.griffin.SqlCodeGenerator.GKK_NANO_HOUR_INT;

public class CountIntVectorAggregateFunction extends AbstractCountVectorAggregateFunction {

    public CountIntVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        super(columnIndex);
        if (keyKind == GKK_MICRO_HOUR_INT) {
            distinctFunc = Rosti::keyedMicroHourDistinct;
            keyValueFunc = Rosti::keyedMicroHourCountInt;
        } else if (keyKind == GKK_NANO_HOUR_INT) {
            distinctFunc = Rosti::keyedNanoHourDistinct;
            keyValueFunc = Rosti::keyedNanoHourCountInt;
        } else {
            distinctFunc = Rosti::keyedIntDistinct;
            keyValueFunc = Rosti::keyedIntCountInt;
        }
    }

    @Override
    public void aggregate(long address, long frameRowCount, int workerId) {
        if (address != 0) {
            final long value = Vect.countInt(address, frameRowCount);
            count.add(value);
            aggCount.increment();
        }
    }

    @Override
    public boolean aggregate(long pRosti, long keyAddress, long valueAddress, long frameRowCount) {
        if (valueAddress == 0) {
            return distinctFunc.run(pRosti, keyAddress, frameRowCount);
        } else {
            return keyValueFunc.run(pRosti, keyAddress, valueAddress, frameRowCount, valueOffset);
        }
    }
}
