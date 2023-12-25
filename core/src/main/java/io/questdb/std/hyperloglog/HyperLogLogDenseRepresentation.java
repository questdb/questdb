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

package io.questdb.std.hyperloglog;

import java.util.Arrays;

import static io.questdb.std.hyperloglog.BiasCorrectionData.BIAS_DATA;
import static io.questdb.std.hyperloglog.BiasCorrectionData.RAW_ESTIMATE_DATA;
import static io.questdb.std.hyperloglog.BiasCorrectionData.THRESHOLD_DATA;

public class HyperLogLogDenseRepresentation implements HyperLogLogRepresentation {
    private static final int KNN_K = 6;
    static final int MIN_PRECISION = 4;
    static final int MAX_PRECISION = 18;
    private static final double[] RECIPROCALS_OF_POWER_OF_2 = new double[Long.SIZE - MIN_PRECISION + 2];

    private final int precision;
    private final byte[] registers;
    private final double alphaMM;
    private final int biasCorrectionThreshold;
    private final int biasCorrectionDataIndex;
    private final long leadingZerosMask;

    public HyperLogLogDenseRepresentation(int precision) {
        int registerCount = 1 << precision;
        this.precision = precision;
        this.biasCorrectionThreshold = 5 * registerCount;
        this.biasCorrectionDataIndex = precision - MIN_PRECISION;
        this.leadingZerosMask = 1L << (precision - 1);
        this.registers = new byte[registerCount];
        switch (registerCount) {
            case 16:
                alphaMM = 0.673 * registerCount * registerCount;
                break;
            case 32:
                alphaMM = 0.697 * registerCount * registerCount;
                break;
            case 64:
                alphaMM = 0.709 * registerCount * registerCount;
                break;
            default:
                alphaMM = (0.7213 / (1 + 1.079 / registerCount)) * registerCount * registerCount;
        }
    }

    @Override
    public void add(long hash) {
        int registerIdx = computeRegisterIndex(hash);
        byte leadingZeros = computeNumberOfLeadingZeros(hash);
        add(registerIdx, leadingZeros);
    }

    @Override
    public long computeCardinality() {
        double sum = 0;
        int emptyRegisterCount = 0;
        for (byte registerValue : registers) {
            sum += RECIPROCALS_OF_POWER_OF_2[registerValue];
            if (registerValue == 0) {
                emptyRegisterCount++;
            }
        }
        if (emptyRegisterCount > 0) {
            double h = linearCounting(registers.length, emptyRegisterCount);
            if (h < THRESHOLD_DATA[biasCorrectionDataIndex]) {
                return Math.round(h);
            }
        }
        double rawEstimate = alphaMM * (1 / sum);
        double correctedEstimate = rawEstimate;
        if (rawEstimate <= biasCorrectionThreshold) {
            correctedEstimate = rawEstimate - estimateBias(rawEstimate);
        }
        return Math.round(correctedEstimate);
    }

    private static double linearCounting(int total, int empty) {
        return total * Math.log(total / (double) empty);
    }

    // visible for testing
    public double estimateBias(double estimate) {
        // Here, we perform k-nearest neighbor interpolation.
        // Since the rawEstimateVector array is sorted in non-decreasing order,
        // we can find the nearest element using binary search and then locate
        // the remaining elements by moving left and right.
        double[] rawEstimateVector = RAW_ESTIMATE_DATA[biasCorrectionDataIndex];
        int nearest = Arrays.binarySearch(rawEstimateVector, estimate);
        nearest = nearest >= 0 ? nearest : Math.max(-(nearest + 1) - 1, 0);
        int left = nearest;
        int right = nearest + 1;
        for (int i = 0, n = KNN_K - 1; i < n; i++) {
            if (left - 1 < 0) {
                right++;
            } else if (right == rawEstimateVector.length) {
                left--;
            } else {
                double leftDistance = estimate - rawEstimateVector[left - 1];
                double rightDistance = rawEstimateVector[right] - estimate;
                if (leftDistance < rightDistance) {
                    left--;
                } else {
                    right++;
                }
            }
        }

        double[] biasVector = BIAS_DATA[biasCorrectionDataIndex];
        double biasTotal = 0.0;
        for (int i = left; i < right; i++) {
            biasTotal += biasVector[i];
        }
        return biasTotal / KNN_K;
    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public HyperLogLogRepresentation convertToDense() {
        return this;
    }

    @Override
    public void clear() {
        Arrays.fill(registers, (byte) 0);
    }

    private int computeRegisterIndex(long hash) {
        return (int) (hash >>> (Long.SIZE - precision));
    }

    private byte computeNumberOfLeadingZeros(long hash) {
        return (byte) (Long.numberOfLeadingZeros((hash << precision) | leadingZerosMask) + 1);
    }

    void add(int position, byte value) {
        byte curVal = registers[position];
        if (curVal < value) {
            registers[position] = value;
        }
    }

    static {
        for (int i = 0; i < RECIPROCALS_OF_POWER_OF_2.length; i++) {
            RECIPROCALS_OF_POWER_OF_2[i] = Math.pow(2, -i);
        }
    }
}
