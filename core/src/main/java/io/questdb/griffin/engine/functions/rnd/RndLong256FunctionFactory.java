/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.StatelessFunction;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class RndLong256FunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_long256()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new RndFunction(position, configuration);
    }

    private static class RndFunction extends Long256Function implements StatelessFunction {

        private final Rnd rnd;
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();

        public RndFunction(int position, CairoConfiguration configuration) {
            super(position);
            this.rnd = SharedRandom.getRandom(configuration);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            return rndLong(long256A);
        }

        @NotNull
        private Long256 rndLong(Long256Impl long256) {
            long256.setLong0(rnd.nextLong());
            long256.setLong1(rnd.nextLong());
            long256.setLong2(rnd.nextLong());
            long256.setLong3(rnd.nextLong());
            return long256;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            return rndLong(long256B);
        }

        @Override
        public void getLong256(Record rec, CharSink sink) {
            sink.put("0x");
            Numbers.appendHex(sink, rnd.nextLong());
            Numbers.appendHex(sink, rnd.nextLong());
            Numbers.appendHex(sink, rnd.nextLong());
            Numbers.appendHex(sink, rnd.nextLong());
        }
    }
}
