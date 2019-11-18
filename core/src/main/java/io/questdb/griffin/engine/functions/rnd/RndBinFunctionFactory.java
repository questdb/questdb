/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.BinFunction;
import io.questdb.griffin.engine.functions.StatelessFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndBinFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_bin()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new FixLenFunction(position, configuration);
    }

    private static final class FixLenFunction extends BinFunction implements StatelessFunction {
        private final Sequence sequence = new Sequence();

        public FixLenFunction(int position, CairoConfiguration configuration) {
            super(position);
            this.sequence.rnd = SharedRandom.getRandom(configuration);
            this.sequence.len = 32;
        }

        @Override
        public BinarySequence getBin(Record rec) {
            return sequence;
        }

        @Override
        public long getBinLen(Record rec) {
            return sequence.len;
        }
    }

    private static class Sequence implements BinarySequence {
        private Rnd rnd;
        private long len;

        @Override
        public byte byteAt(long index) {
            return rnd.nextByte();
        }

        @Override
        public long length() {
            return len;
        }
    }
}
