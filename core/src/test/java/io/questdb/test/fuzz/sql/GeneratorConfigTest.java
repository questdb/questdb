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

import org.junit.Assert;
import org.junit.Test;

public class GeneratorConfigTest {

    @Test
    public void testDefaults() {
        GeneratorConfig config = GeneratorConfig.defaults();

        // Depth defaults
        Assert.assertEquals(5, config.maxDepth());
        Assert.assertEquals(4, config.maxExpressionDepth());

        // Cardinality defaults
        Assert.assertEquals(3, config.maxCteCount());
        Assert.assertEquals(4, config.maxJoins());
        Assert.assertEquals(10, config.maxColumns());

        // Mode weight defaults
        Assert.assertEquals(0.6, config.validModeWeight(), 0.001);
        Assert.assertEquals(0.3, config.corruptModeWeight(), 0.001);
        Assert.assertEquals(0.1, config.garbageModeWeight(), 0.001);
        Assert.assertEquals(1.0, config.totalModeWeight(), 0.001);
    }

    @Test
    public void testBuilder() {
        GeneratorConfig config = GeneratorConfig.builder()
                .maxDepth(3)
                .maxExpressionDepth(2)
                .maxCteCount(5)
                .maxJoins(2)
                .joinProb(0.7)
                .whereProb(0.8)
                .sampleByProb(0.5)
                .validModeWeight(0.5)
                .corruptModeWeight(0.4)
                .garbageModeWeight(0.1)
                .build();

        Assert.assertEquals(3, config.maxDepth());
        Assert.assertEquals(2, config.maxExpressionDepth());
        Assert.assertEquals(5, config.maxCteCount());
        Assert.assertEquals(2, config.maxJoins());
        Assert.assertEquals(0.7, config.joinProb(), 0.001);
        Assert.assertEquals(0.8, config.whereProb(), 0.001);
        Assert.assertEquals(0.5, config.sampleByProb(), 0.001);
        Assert.assertEquals(0.5, config.validModeWeight(), 0.001);
        Assert.assertEquals(0.4, config.corruptModeWeight(), 0.001);
        Assert.assertEquals(0.1, config.garbageModeWeight(), 0.001);
    }

    @Test
    public void testQuestdbFocused() {
        GeneratorConfig config = GeneratorConfig.questdbFocused();

        // Should have higher QuestDB-specific probabilities
        Assert.assertEquals(0.25, config.asofJoinProb(), 0.001);
        Assert.assertEquals(0.20, config.ltJoinProb(), 0.001);
        Assert.assertEquals(0.15, config.spliceJoinProb(), 0.001);
        Assert.assertEquals(0.30, config.sampleByProb(), 0.001);
        Assert.assertEquals(0.20, config.latestOnProb(), 0.001);
        Assert.assertEquals(0.25, config.windowFunctionProb(), 0.001);
    }

    @Test
    public void testSimple() {
        GeneratorConfig config = GeneratorConfig.simple();

        Assert.assertEquals(2, config.maxDepth());
        Assert.assertEquals(2, config.maxExpressionDepth());
        Assert.assertEquals(1, config.maxCteCount());
        Assert.assertEquals(1, config.maxJoins());
        Assert.assertEquals(3, config.maxColumns());
        Assert.assertEquals(0.1, config.cteProb(), 0.001);
        Assert.assertEquals(0.1, config.subqueryProb(), 0.001);
        Assert.assertEquals(0.2, config.joinProb(), 0.001);
    }

    @Test
    public void testValidOnly() {
        GeneratorConfig config = GeneratorConfig.builder()
                .validOnly()
                .build();

        Assert.assertEquals(1.0, config.validModeWeight(), 0.001);
        Assert.assertEquals(0.0, config.corruptModeWeight(), 0.001);
        Assert.assertEquals(0.0, config.garbageModeWeight(), 0.001);
    }

    @Test
    public void testCorruptOnly() {
        GeneratorConfig config = GeneratorConfig.builder()
                .corruptOnly()
                .build();

        Assert.assertEquals(0.0, config.validModeWeight(), 0.001);
        Assert.assertEquals(1.0, config.corruptModeWeight(), 0.001);
        Assert.assertEquals(0.0, config.garbageModeWeight(), 0.001);
    }

    @Test
    public void testGarbageOnly() {
        GeneratorConfig config = GeneratorConfig.builder()
                .garbageOnly()
                .build();

        Assert.assertEquals(0.0, config.validModeWeight(), 0.001);
        Assert.assertEquals(0.0, config.corruptModeWeight(), 0.001);
        Assert.assertEquals(1.0, config.garbageModeWeight(), 0.001);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMaxDepth() {
        GeneratorConfig.builder()
                .maxDepth(0)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMaxExpressionDepth() {
        GeneratorConfig.builder()
                .maxExpressionDepth(0)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMaxColumns() {
        GeneratorConfig.builder()
                .maxColumns(0)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTotalModeWeight() {
        GeneratorConfig.builder()
                .validModeWeight(0)
                .corruptModeWeight(0)
                .garbageModeWeight(0)
                .build();
    }

    @Test
    public void testAllProbabilityGetters() {
        GeneratorConfig config = GeneratorConfig.defaults();

        // Standard SQL
        Assert.assertTrue(config.cteProb() >= 0 && config.cteProb() <= 1);
        Assert.assertTrue(config.subqueryProb() >= 0 && config.subqueryProb() <= 1);
        Assert.assertTrue(config.joinProb() >= 0 && config.joinProb() <= 1);
        Assert.assertTrue(config.whereProb() >= 0 && config.whereProb() <= 1);
        Assert.assertTrue(config.groupByProb() >= 0 && config.groupByProb() <= 1);
        Assert.assertTrue(config.havingProb() >= 0 && config.havingProb() <= 1);
        Assert.assertTrue(config.orderByProb() >= 0 && config.orderByProb() <= 1);
        Assert.assertTrue(config.limitProb() >= 0 && config.limitProb() <= 1);
        Assert.assertTrue(config.distinctProb() >= 0 && config.distinctProb() <= 1);
        Assert.assertTrue(config.unionProb() >= 0 && config.unionProb() <= 1);

        // Expression
        Assert.assertTrue(config.functionCallProb() >= 0 && config.functionCallProb() <= 1);
        Assert.assertTrue(config.caseWhenProb() >= 0 && config.caseWhenProb() <= 1);
        Assert.assertTrue(config.castProb() >= 0 && config.castProb() <= 1);
        Assert.assertTrue(config.subqueryExprProb() >= 0 && config.subqueryExprProb() <= 1);
        Assert.assertTrue(config.betweenProb() >= 0 && config.betweenProb() <= 1);
        Assert.assertTrue(config.inListProb() >= 0 && config.inListProb() <= 1);
        Assert.assertTrue(config.likeProb() >= 0 && config.likeProb() <= 1);
        Assert.assertTrue(config.isNullProb() >= 0 && config.isNullProb() <= 1);

        // QuestDB-specific
        Assert.assertTrue(config.sampleByProb() >= 0 && config.sampleByProb() <= 1);
        Assert.assertTrue(config.latestOnProb() >= 0 && config.latestOnProb() <= 1);
        Assert.assertTrue(config.asofJoinProb() >= 0 && config.asofJoinProb() <= 1);
        Assert.assertTrue(config.ltJoinProb() >= 0 && config.ltJoinProb() <= 1);
        Assert.assertTrue(config.spliceJoinProb() >= 0 && config.spliceJoinProb() <= 1);
        Assert.assertTrue(config.windowFunctionProb() >= 0 && config.windowFunctionProb() <= 1);
        Assert.assertTrue(config.timestampWithTimezoneProb() >= 0 && config.timestampWithTimezoneProb() <= 1);
    }

    @Test
    public void testAllCardinalityGetters() {
        GeneratorConfig config = GeneratorConfig.defaults();

        Assert.assertTrue(config.maxGroupByColumns() > 0);
        Assert.assertTrue(config.maxOrderByColumns() > 0);
        Assert.assertTrue(config.maxUnionCount() > 0);
        Assert.assertTrue(config.maxFunctionArgs() > 0);
    }
}
