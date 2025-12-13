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

package io.questdb.test.fuzz.sql;

/**
 * Configuration for SQL fuzz generation.
 * Controls complexity, probabilities, and mode weights for the fuzzer.
 * <p>
 * Use the builder pattern for fluent configuration:
 * <pre>
 * GeneratorConfig config = GeneratorConfig.builder()
 *     .maxDepth(3)
 *     .joinProb(0.7)
 *     .build();
 * </pre>
 */
public final class GeneratorConfig {

    // --- Depth control ---
    private final int maxDepth;
    private final int maxExpressionDepth;

    // --- Cardinality limits ---
    private final int maxCteCount;
    private final int maxJoins;
    private final int maxColumns;
    private final int maxGroupByColumns;
    private final int maxOrderByColumns;
    private final int maxUnionCount;
    private final int maxFunctionArgs;

    // --- Standard SQL feature probabilities (0.0 - 1.0) ---
    private final double cteProb;
    private final double subqueryProb;
    private final double joinProb;
    private final double whereProb;
    private final double groupByProb;
    private final double havingProb;
    private final double orderByProb;
    private final double limitProb;
    private final double distinctProb;
    private final double unionProb;

    // --- Expression probabilities ---
    private final double functionCallProb;
    private final double caseWhenProb;
    private final double castProb;
    private final double subqueryExprProb;
    private final double betweenProb;
    private final double inListProb;
    private final double likeProb;
    private final double isNullProb;

    // --- QuestDB-specific feature probabilities ---
    private final double sampleByProb;
    private final double latestOnProb;
    private final double asofJoinProb;
    private final double ltJoinProb;
    private final double spliceJoinProb;
    private final double windowFunctionProb;
    private final double timestampWithTimezoneProb;

    // --- Generation mode weights ---
    private final double validModeWeight;
    private final double corruptModeWeight;
    private final double garbageModeWeight;

    private GeneratorConfig(Builder builder) {
        this.maxDepth = builder.maxDepth;
        this.maxExpressionDepth = builder.maxExpressionDepth;
        this.maxCteCount = builder.maxCteCount;
        this.maxJoins = builder.maxJoins;
        this.maxColumns = builder.maxColumns;
        this.maxGroupByColumns = builder.maxGroupByColumns;
        this.maxOrderByColumns = builder.maxOrderByColumns;
        this.maxUnionCount = builder.maxUnionCount;
        this.maxFunctionArgs = builder.maxFunctionArgs;
        this.cteProb = builder.cteProb;
        this.subqueryProb = builder.subqueryProb;
        this.joinProb = builder.joinProb;
        this.whereProb = builder.whereProb;
        this.groupByProb = builder.groupByProb;
        this.havingProb = builder.havingProb;
        this.orderByProb = builder.orderByProb;
        this.limitProb = builder.limitProb;
        this.distinctProb = builder.distinctProb;
        this.unionProb = builder.unionProb;
        this.functionCallProb = builder.functionCallProb;
        this.caseWhenProb = builder.caseWhenProb;
        this.castProb = builder.castProb;
        this.subqueryExprProb = builder.subqueryExprProb;
        this.betweenProb = builder.betweenProb;
        this.inListProb = builder.inListProb;
        this.likeProb = builder.likeProb;
        this.isNullProb = builder.isNullProb;
        this.sampleByProb = builder.sampleByProb;
        this.latestOnProb = builder.latestOnProb;
        this.asofJoinProb = builder.asofJoinProb;
        this.ltJoinProb = builder.ltJoinProb;
        this.spliceJoinProb = builder.spliceJoinProb;
        this.windowFunctionProb = builder.windowFunctionProb;
        this.timestampWithTimezoneProb = builder.timestampWithTimezoneProb;
        this.validModeWeight = builder.validModeWeight;
        this.corruptModeWeight = builder.corruptModeWeight;
        this.garbageModeWeight = builder.garbageModeWeight;
    }

    /**
     * Returns a new builder with default values.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a configuration with reasonable default values.
     */
    public static GeneratorConfig defaults() {
        return builder().build();
    }

    /**
     * Returns a configuration focused on QuestDB-specific features with higher probabilities.
     */
    public static GeneratorConfig questdbFocused() {
        return builder()
                .asofJoinProb(0.25)
                .ltJoinProb(0.20)
                .spliceJoinProb(0.15)
                .sampleByProb(0.30)
                .latestOnProb(0.20)
                .windowFunctionProb(0.25)
                .build();
    }

    /**
     * Returns a configuration for simple queries (low complexity).
     */
    public static GeneratorConfig simple() {
        return builder()
                .maxDepth(2)
                .maxExpressionDepth(2)
                .maxCteCount(1)
                .maxJoins(1)
                .maxColumns(3)
                .cteProb(0.1)
                .subqueryProb(0.1)
                .joinProb(0.2)
                .build();
    }

    // --- Getters ---

    public int maxDepth() {
        return maxDepth;
    }

    public int maxExpressionDepth() {
        return maxExpressionDepth;
    }

    public int maxCteCount() {
        return maxCteCount;
    }

    public int maxJoins() {
        return maxJoins;
    }

    public int maxColumns() {
        return maxColumns;
    }

    public int maxGroupByColumns() {
        return maxGroupByColumns;
    }

    public int maxOrderByColumns() {
        return maxOrderByColumns;
    }

    public int maxUnionCount() {
        return maxUnionCount;
    }

    public int maxFunctionArgs() {
        return maxFunctionArgs;
    }

    public double cteProb() {
        return cteProb;
    }

    public double subqueryProb() {
        return subqueryProb;
    }

    public double joinProb() {
        return joinProb;
    }

    public double whereProb() {
        return whereProb;
    }

    public double groupByProb() {
        return groupByProb;
    }

    public double havingProb() {
        return havingProb;
    }

    public double orderByProb() {
        return orderByProb;
    }

    public double limitProb() {
        return limitProb;
    }

    public double distinctProb() {
        return distinctProb;
    }

    public double unionProb() {
        return unionProb;
    }

    public double functionCallProb() {
        return functionCallProb;
    }

    public double caseWhenProb() {
        return caseWhenProb;
    }

    public double castProb() {
        return castProb;
    }

    public double subqueryExprProb() {
        return subqueryExprProb;
    }

    public double betweenProb() {
        return betweenProb;
    }

    public double inListProb() {
        return inListProb;
    }

    public double likeProb() {
        return likeProb;
    }

    public double isNullProb() {
        return isNullProb;
    }

    public double sampleByProb() {
        return sampleByProb;
    }

    public double latestOnProb() {
        return latestOnProb;
    }

    public double asofJoinProb() {
        return asofJoinProb;
    }

    public double ltJoinProb() {
        return ltJoinProb;
    }

    public double spliceJoinProb() {
        return spliceJoinProb;
    }

    /**
     * Returns the combined probability of using a QuestDB time-series join (ASOF, LT, or SPLICE).
     * When generating a join, this probability determines whether to use a QuestDB-specific
     * join type instead of a standard SQL join.
     */
    public double questdbJoinProb() {
        // Combine the individual QuestDB join probabilities
        // Use the sum capped at 1.0, or take max if they overlap
        return Math.min(1.0, asofJoinProb + ltJoinProb + spliceJoinProb);
    }

    public double windowFunctionProb() {
        return windowFunctionProb;
    }

    public double timestampWithTimezoneProb() {
        return timestampWithTimezoneProb;
    }

    public double validModeWeight() {
        return validModeWeight;
    }

    public double corruptModeWeight() {
        return corruptModeWeight;
    }

    public double garbageModeWeight() {
        return garbageModeWeight;
    }

    /**
     * Returns the total weight of all generation modes.
     */
    public double totalModeWeight() {
        return validModeWeight + corruptModeWeight + garbageModeWeight;
    }

    /**
     * Builder for GeneratorConfig.
     */
    public static final class Builder {
        // --- Depth control defaults ---
        private int maxDepth = 5;
        private int maxExpressionDepth = 4;

        // --- Cardinality limit defaults ---
        private int maxCteCount = 3;
        private int maxJoins = 4;
        private int maxColumns = 10;
        private int maxGroupByColumns = 5;
        private int maxOrderByColumns = 5;
        private int maxUnionCount = 3;
        private int maxFunctionArgs = 5;

        // --- Standard SQL probability defaults ---
        private double cteProb = 0.3;
        private double subqueryProb = 0.3;
        private double joinProb = 0.5;
        private double whereProb = 0.6;
        private double groupByProb = 0.3;
        private double havingProb = 0.2;
        private double orderByProb = 0.4;
        private double limitProb = 0.3;
        private double distinctProb = 0.1;
        private double unionProb = 0.1;

        // --- Expression probability defaults ---
        private double functionCallProb = 0.3;
        private double caseWhenProb = 0.1;
        private double castProb = 0.1;
        private double subqueryExprProb = 0.05;
        private double betweenProb = 0.1;
        private double inListProb = 0.1;
        private double likeProb = 0.1;
        private double isNullProb = 0.1;

        // --- QuestDB-specific probability defaults ---
        private double sampleByProb = 0.2;
        private double latestOnProb = 0.1;
        private double asofJoinProb = 0.15;
        private double ltJoinProb = 0.1;
        private double spliceJoinProb = 0.05;
        private double windowFunctionProb = 0.15;
        private double timestampWithTimezoneProb = 0.1;

        // --- Mode weight defaults ---
        private double validModeWeight = 0.6;
        private double corruptModeWeight = 0.3;
        private double garbageModeWeight = 0.1;

        private Builder() {
        }

        public GeneratorConfig build() {
            validate();
            return new GeneratorConfig(this);
        }

        private void validate() {
            if (maxDepth < 1) {
                throw new IllegalArgumentException("maxDepth must be >= 1");
            }
            if (maxExpressionDepth < 1) {
                throw new IllegalArgumentException("maxExpressionDepth must be >= 1");
            }
            if (maxCteCount < 0) {
                throw new IllegalArgumentException("maxCteCount must be >= 0");
            }
            if (maxJoins < 0) {
                throw new IllegalArgumentException("maxJoins must be >= 0");
            }
            if (maxColumns < 1) {
                throw new IllegalArgumentException("maxColumns must be >= 1");
            }
            double totalWeight = validModeWeight + corruptModeWeight + garbageModeWeight;
            if (totalWeight <= 0) {
                throw new IllegalArgumentException("Total mode weight must be > 0");
            }
        }

        // --- Depth setters ---

        public Builder maxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder maxExpressionDepth(int maxExpressionDepth) {
            this.maxExpressionDepth = maxExpressionDepth;
            return this;
        }

        // --- Cardinality setters ---

        public Builder maxCteCount(int maxCteCount) {
            this.maxCteCount = maxCteCount;
            return this;
        }

        public Builder maxJoins(int maxJoins) {
            this.maxJoins = maxJoins;
            return this;
        }

        public Builder maxColumns(int maxColumns) {
            this.maxColumns = maxColumns;
            return this;
        }

        public Builder maxGroupByColumns(int maxGroupByColumns) {
            this.maxGroupByColumns = maxGroupByColumns;
            return this;
        }

        public Builder maxOrderByColumns(int maxOrderByColumns) {
            this.maxOrderByColumns = maxOrderByColumns;
            return this;
        }

        public Builder maxUnionCount(int maxUnionCount) {
            this.maxUnionCount = maxUnionCount;
            return this;
        }

        public Builder maxFunctionArgs(int maxFunctionArgs) {
            this.maxFunctionArgs = maxFunctionArgs;
            return this;
        }

        // --- Standard SQL probability setters ---

        public Builder cteProb(double cteProb) {
            this.cteProb = cteProb;
            return this;
        }

        public Builder subqueryProb(double subqueryProb) {
            this.subqueryProb = subqueryProb;
            return this;
        }

        public Builder joinProb(double joinProb) {
            this.joinProb = joinProb;
            return this;
        }

        public Builder whereProb(double whereProb) {
            this.whereProb = whereProb;
            return this;
        }

        public Builder groupByProb(double groupByProb) {
            this.groupByProb = groupByProb;
            return this;
        }

        public Builder havingProb(double havingProb) {
            this.havingProb = havingProb;
            return this;
        }

        public Builder orderByProb(double orderByProb) {
            this.orderByProb = orderByProb;
            return this;
        }

        public Builder limitProb(double limitProb) {
            this.limitProb = limitProb;
            return this;
        }

        public Builder distinctProb(double distinctProb) {
            this.distinctProb = distinctProb;
            return this;
        }

        public Builder unionProb(double unionProb) {
            this.unionProb = unionProb;
            return this;
        }

        // --- Expression probability setters ---

        public Builder functionCallProb(double functionCallProb) {
            this.functionCallProb = functionCallProb;
            return this;
        }

        public Builder caseWhenProb(double caseWhenProb) {
            this.caseWhenProb = caseWhenProb;
            return this;
        }

        public Builder castProb(double castProb) {
            this.castProb = castProb;
            return this;
        }

        public Builder subqueryExprProb(double subqueryExprProb) {
            this.subqueryExprProb = subqueryExprProb;
            return this;
        }

        public Builder betweenProb(double betweenProb) {
            this.betweenProb = betweenProb;
            return this;
        }

        public Builder inListProb(double inListProb) {
            this.inListProb = inListProb;
            return this;
        }

        public Builder likeProb(double likeProb) {
            this.likeProb = likeProb;
            return this;
        }

        public Builder isNullProb(double isNullProb) {
            this.isNullProb = isNullProb;
            return this;
        }

        // --- QuestDB-specific probability setters ---

        public Builder sampleByProb(double sampleByProb) {
            this.sampleByProb = sampleByProb;
            return this;
        }

        public Builder latestOnProb(double latestOnProb) {
            this.latestOnProb = latestOnProb;
            return this;
        }

        public Builder asofJoinProb(double asofJoinProb) {
            this.asofJoinProb = asofJoinProb;
            return this;
        }

        public Builder ltJoinProb(double ltJoinProb) {
            this.ltJoinProb = ltJoinProb;
            return this;
        }

        public Builder spliceJoinProb(double spliceJoinProb) {
            this.spliceJoinProb = spliceJoinProb;
            return this;
        }

        public Builder windowFunctionProb(double windowFunctionProb) {
            this.windowFunctionProb = windowFunctionProb;
            return this;
        }

        public Builder timestampWithTimezoneProb(double timestampWithTimezoneProb) {
            this.timestampWithTimezoneProb = timestampWithTimezoneProb;
            return this;
        }

        // --- Mode weight setters ---

        public Builder validModeWeight(double validModeWeight) {
            this.validModeWeight = validModeWeight;
            return this;
        }

        public Builder corruptModeWeight(double corruptModeWeight) {
            this.corruptModeWeight = corruptModeWeight;
            return this;
        }

        public Builder garbageModeWeight(double garbageModeWeight) {
            this.garbageModeWeight = garbageModeWeight;
            return this;
        }

        /**
         * Sets mode weights to generate only valid queries (no corruption or garbage).
         */
        public Builder validOnly() {
            this.validModeWeight = 1.0;
            this.corruptModeWeight = 0.0;
            this.garbageModeWeight = 0.0;
            return this;
        }

        /**
         * Sets mode weights to generate only corrupt queries.
         */
        public Builder corruptOnly() {
            this.validModeWeight = 0.0;
            this.corruptModeWeight = 1.0;
            this.garbageModeWeight = 0.0;
            return this;
        }

        /**
         * Sets mode weights to generate only garbage queries.
         */
        public Builder garbageOnly() {
            this.validModeWeight = 0.0;
            this.corruptModeWeight = 0.0;
            this.garbageModeWeight = 1.0;
            return this;
        }
    }
}
