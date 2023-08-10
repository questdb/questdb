/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.std;

import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import org.junit.Test;

public class HdrHistogramTest {
    @Test
    public void initialiseHist() {

        Histogram hist = new Histogram(1000, 2000, 3);

        for(int i = 1000; i < 2000; i++) {
            hist.recordSingleValue(i);
            //System.out.println(hist.getCountAtIndex(i));
        }

        for(int i = 1; i < 4; i++) {
            System.out.println(hist.getCountAtIndex(i));
        }

//        for(int i = 0; i < 20; i++) {
//            hist.recordSingleValue(2);
//        }

        //hist.recordSingleValue(506);

//        for(int i = 0; i < 2000; i++) {
//            //System.out.println(hist.getMaxValue());
//        }
//
//        System.out.println("Total Count: " + hist.getTotalCount());
//
        System.out.println("99th Percentile: " + hist.getValueAtPercentile(99));
    }
}
