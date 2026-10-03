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

package io.questdb.griffin.engine.functions.window;

public class CovarPopDoubleWindowFunctionFactory extends AbstractBivariateStatWindowFunctionFactory {

    public static double value(double sum, double delta) {
        return AbstractBivariateStatWindowFunctionFactory.value(sum, delta);
    }

    public static double accumulateProduct(double sum, double x, double y) {
        return AbstractBivariateStatWindowFunctionFactory.accumulateProduct(sum, x, y);
    }

    public static double advanceMean(double mean, double next, long count) {
        return AbstractBivariateStatWindowFunctionFactory.advanceMean(mean, next, count);
    }

    public static double advanceComoment(double comoment, double x, double meanX, double y, double oldMeanY) {
        return AbstractBivariateStatWindowFunctionFactory.advanceComoment(comoment, x, meanX, y, oldMeanY);
    }

    @Override
    public String getSignature() {
        return "covar_pop(DD)";
    }

    @Override
    protected boolean isCorrelation() {
        return false;
    }

    @Override
    protected boolean isSample() {
        return false;
    }

    @Override
    protected String name() {
        return "covar_pop";
    }
}
