/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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


package io.questdb.griffin.plan.logical;

import io.questdb.std.IntList;
import io.questdb.std.ObjectFactory;

/**
 * A further consumer of a decorrelation domain the same LATERAL step built once: a leaf that reads the rows of the
 * producer domain instead of a copy of it. It defines its own output columns, one for each outer column, each paired
 * with the producer's column for the same outer column, see {@link #getProducerColumnIds()}. Tree walks stop at it, so
 * no pass reaches the producer through it.
 */
public final class DomainReadPlan extends LogicalPlan {
    public static final ObjectFactory<DomainReadPlan> FACTORY = DomainReadPlan::new;
    private final IntList producerColumnIds = new IntList();
    private Algorithm algorithm;
    private AggregatePlan producer;

    @Override
    public void clear() {
        super.clear();
        producerColumnIds.clear();
        algorithm = null;
        producer = null;
    }

    /**
     * How the generator reads the producer's rows, which operator planning records; null before planning.
     */
    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public AggregatePlan getProducer() {
        return producer;
    }

    /**
     * The output column of the producer each output column of the read reads, in output order.
     */
    public IntList getProducerColumnIds() {
        return producerColumnIds;
    }

    @Override
    public LogicalPlan inputAt(int index) {
        throw new IndexOutOfBoundsException("domain read has no input: " + index);
    }

    @Override
    public int inputCount() {
        return 0;
    }

    public DomainReadPlan of(AggregatePlan producer, int position) {
        this.producer = producer;
        setPosition(position);
        return this;
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        throw new IndexOutOfBoundsException("domain read has no input: " + index);
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    /**
     * Reads the producer of a copied read from the copy of the producer.
     */
    public void setProducer(AggregatePlan producer) {
        this.producer = producer;
    }

    /**
     * How the generator reads the rows of the producer: through a shared cursor of the factory it builds for the
     * producer once, which serves shared cursors, or from a factory it builds for the producer's plan again.
     */
    public enum Algorithm {
        REGENERATED, SHARED
    }
}
