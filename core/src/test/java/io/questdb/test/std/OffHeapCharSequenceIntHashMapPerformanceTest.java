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

package io.questdb.test.std;

import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.OffHeapCharSequenceIntHashMap;
import io.questdb.std.Rnd;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OffHeapCharSequenceIntHashMapPerformanceTest {

    @Test
    public void testMemoryUsagePattern() {
        System.out.println("\nMemory Usage Pattern Test");
        System.out.println("=========================");

        int numMaps = 50;
        int symbolsPerMap = 1000;

        System.out.printf("Creating %d maps with %d symbols each\n", numMaps, symbolsPerMap);

        // Generate test data once
        List<String> symbols = generateSymbols(symbolsPerMap);

        // Test heap maps
        System.gc();
        long heapStartTime = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        long heapStartMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        List<CharSequenceIntHashMap> heapMaps = new ArrayList<>();
        for (int i = 0; i < numMaps; i++) {
            CharSequenceIntHashMap map = new CharSequenceIntHashMap();
            for (int j = 0; j < symbols.size(); j++) {
                map.put(symbols.get(j), j);
            }
            heapMaps.add(map);
        }

        long heapEndTime = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        long heapEndMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.printf("Heap maps - Time: %d ms, Memory: %,d bytes\n",
                heapEndTime - heapStartTime, heapEndMem - heapStartMem);

        // Clear heap maps
        heapMaps.clear();
        System.gc();

        // Test off-heap maps
        long offHeapStartTime = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        long offHeapStartMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        List<OffHeapCharSequenceIntHashMap> offHeapMaps = new ArrayList<>();
        for (int i = 0; i < numMaps; i++) {
            OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap();
            for (int j = 0; j < symbols.size(); j++) {
                map.put(symbols.get(j), j);
            }
            offHeapMaps.add(map);
        }

        long offHeapEndTime = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        long offHeapEndMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.printf("Off-heap maps - Time: %d ms, Memory: %,d bytes\n",
                offHeapEndTime - offHeapStartTime, offHeapEndMem - offHeapStartMem);

        // Clean up off-heap maps
        for (OffHeapCharSequenceIntHashMap map : offHeapMaps) {
            map.close();
        }

        long heapMemory = heapEndMem - heapStartMem;
        long offHeapMemory = offHeapEndMem - offHeapStartMem;

        if (heapMemory > offHeapMemory) {
            double memSaving = ((double) (heapMemory - offHeapMemory) / heapMemory) * 100;
            System.out.printf("Off-heap saves %.1f%% heap memory\n", memSaving);
        }
    }

    @Test
    public void testWalWriterUsagePatternPerformance() {
        System.out.println("WAL Writer Usage Pattern Performance Test");
        System.out.println("==========================================");

        int[] mapSizes = {100, 1000, 5000};
        int rounds = 100;

        for (int mapSize : mapSizes) {
            System.out.printf("\nTesting with %d symbols, %d rounds\n", mapSize, rounds);

            // Generate test data
            List<String> symbols = generateSymbols(mapSize);

            // Test heap-based map
            long heapTime = testHeapMapPerformance(symbols, rounds);

            // Test off-heap map
            long offHeapTime = testOffHeapMapPerformance(symbols, rounds);

            System.out.printf("Heap map time:     %,d ms\n", heapTime);
            System.out.printf("Off-heap map time: %,d ms\n", offHeapTime);

            if (heapTime > offHeapTime) {
                double improvement = ((double) (heapTime - offHeapTime) / heapTime) * 100;
                System.out.printf("Off-heap is %.1f%% faster\n", improvement);
            } else {
                double slower = ((double) (offHeapTime - heapTime) / heapTime) * 100;
                System.out.printf("Off-heap is %.1f%% slower\n", slower);
            }
        }
    }

    private List<String> generateSymbols(int count) {
        List<String> symbols = new ArrayList<>(count);
        Rnd rnd = new Rnd(); // Random number generator

        for (int i = 0; i < count; i++) {
            // Generate realistic symbol names
            symbols.add("symbol_" + i + "_" + rnd.nextString(8));
        }

        return symbols;
    }

    private long testHeapMapPerformance(List<String> symbols, int rounds) {
        long startTime = System.currentTimeMillis();

        CharSequenceIntHashMap map = new CharSequenceIntHashMap();

        // Simulate WAL writer pattern: clear and repopulate repeatedly
        for (int round = 0; round < rounds; round++) {
            map.clear();
            for (int i = 0; i < symbols.size(); i++) {
                map.put(symbols.get(i), i);
            }
        }

        return System.currentTimeMillis() - startTime;
    }

    private long testOffHeapMapPerformance(List<String> symbols, int rounds) {
        long startTime = System.currentTimeMillis();

        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            // Simulate WAL writer pattern: clear and repopulate repeatedly
            for (int round = 0; round < rounds; round++) {
                map.clear();
                for (int i = 0; i < symbols.size(); i++) {
                    map.put(symbols.get(i), i);
                }
            }
        }

        return System.currentTimeMillis() - startTime;
    }
}