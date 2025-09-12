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

package org.questdb;

import io.questdb.client.Sender;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

import java.time.temporal.ChronoUnit;

public class LineTCPSenderMain {
    /**
     * Creates a set to be used for generating fixed numbers from 0 to valueCount with variable, non-uniform
     * probability. The lower numbers have the highest probability.
     *
     * @param valueCount         number of values to generate probabilities for
     * @param diminishingScale   the scale at which probability should be diminishing
     * @param initialProbability the initial probability, the one assigned to the first value
     * @return an of ints. To generate number with calculated probabilities use value=set[rnd.nextInt(valueCount)]
     */
    public static int[] computeDiminishingFrequencyDistribution(int valueCount, double diminishingScale, double initialProbability) {

        assert diminishingScale < 1 && diminishingScale > 0;

        double lastProbability = initialProbability / diminishingScale;
        double[] probabilities = new double[valueCount];
        for (int i = 0; i < valueCount; i++) {
            final double prob = lastProbability * diminishingScale;
            probabilities[i] = prob;
            lastProbability = prob;
        }


        // find out scale of last probability
        double minProb = probabilities[valueCount - 1];
        int distScale = 1;
        while (((int) minProb / 100) == 0) {
            minProb *= 10;
            distScale *= 10;
        }

        // compute distribution set size
        int ccyDistSize = 0;
        for (int i = 0; i < valueCount; i++) {
            ccyDistSize += (int) (distScale * probabilities[i] / 100);
        }

        int[] ccyDist = new int[ccyDistSize];

        int x = 0;
        for (int i = 0; i < valueCount; i++) {
            final int len = (int) (distScale * probabilities[i] / 100);
            assert len > 0;
            int n = Math.min(x + len, ccyDistSize);
            for (; x < n; x++) {
                ccyDist[x] = i;
            }
        }
        return ccyDist;
    }

    public static String[] createValues(Rnd rnd, int count, int len) {
        String[] ccy = new String[count];
        for (int i = 0; i < count; i++) {
            ccy[i] = rnd.nextString(len);
        }
        return ccy;
    }

    public static void main(String[] args) {
        Rnd rnd = new Rnd();
        String[] ccy = createValues(rnd, 30, 6);
        int[] ccyDist = computeDiminishingFrequencyDistribution(ccy.length, 0.8, 10.0);
        String[] venue = createValues(rnd, 10, 8);
        int[] venueDist = computeDiminishingFrequencyDistribution(venue.length, 0.7, 20.0);
        String[] pool = createValues(rnd, 6, 3);
        int[] poolDist = computeDiminishingFrequencyDistribution(pool.length, 0.9, 30.0);


        int n = 1;
        final SOCountDownLatch haltLatch = new SOCountDownLatch(n);
        for (int i = 0; i < n; i++) {
            new Thread(() -> doSend(haltLatch, ccy, ccyDist, venue, venueDist, pool, poolDist)).start();
        }
        haltLatch.await();
    }

    private static void doSend(
            SOCountDownLatch haltLatch,
            String[] ccy,
            int[] ccyDist,
            String[] venue,
            int[] venueDist,
            String[] pool,
            int[] poolDist
    ) {
        final long count = 30_000_000;
        final Rnd rnd = new Rnd();
        long start = System.nanoTime();
        Clock clock = new MicrosecondClockImpl();
        String tab = "quotes";
        try (Sender sender = Sender.builder(Sender.Transport.TCP)
                .address("wet-crimson-879-30b0c6db.ilp.c7at.questdb.com:32495")
                .enableTls()
                .enableAuth("admin").authToken("eRNONc_PZfJTwVuFoOr_YZJRfVnyfCRYZvJ9asABFzs")
                .build()) {
            for (int i = 0; i < count; i++) {
                sender.table(tab);
                sender
                        .symbol("ccy", ccy[ccyDist[rnd.nextInt(ccyDist.length)]])
                        .symbol("venue", venue[venueDist[rnd.nextInt(venueDist.length)]])
                        .symbol("pool", pool[poolDist[rnd.nextInt(poolDist.length)]])
                        .doubleColumn("qty", rnd.nextDouble())
                        .doubleColumn("bid", rnd.nextDouble())
                        .doubleColumn("ask", rnd.nextDouble());
                sender.at(clock.getTicks(), ChronoUnit.MICROS);
            }
        } finally {
            System.out.println("time: " + (System.nanoTime() - start));
            haltLatch.countDown();
        }
    }
}
