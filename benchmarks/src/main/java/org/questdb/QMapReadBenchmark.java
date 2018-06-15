/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package org.questdb;

import com.questdb.cairo.map.QMap;
import com.questdb.cairo.map.SingleColumnType;
import com.questdb.common.ColumnType;
import com.questdb.ql.map.ColumnTypeResolver;
import com.questdb.ql.map.DirectMap;
import com.questdb.ql.map.DirectMapValues;
import com.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class QMapReadBenchmark {

    private static final int N = 5000000;
    private static final double loadFactor = 0.9;
    private static final int M = 25;
    private static final Rnd rnd = new Rnd();
    private static QMap qmap = new QMap(1024 * 1024, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), N, loadFactor);
    private static DirectMap map = new DirectMap(1024 * 1024, new ColumnTypeResolver() {
        @Override
        public int count() {
            return 1;
        }

        @Override
        public int getColumnType(int index) {
            return ColumnType.STRING;
        }
    },
            new ColumnTypeResolver() {
                @Override
                public int count() {
                    return 1;
                }

                @Override
                public int getColumnType(int index) {
                    return ColumnType.LONG;
                }
            });

    static {
        for (int i = 0; i < N; i++) {
            QMap.Key key = qmap.withKey();
            key.putStr(rnd.nextChars(M));
            QMap.Value value = key.createValue();
            value.putLong(0, i);
        }

        for (int i = 0; i < N; i++) {
            DirectMap.KeyWriter kw = map.keyWriter();
            kw.putStr(rnd.nextChars(M));
            DirectMapValues values = map.getOrCreateValues();
            values.putLong(0, 20);
        }

    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QMapReadBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        System.out.print(" [q=" + qmap.size() + ", l=" + map.size() + ", clash=" + qmap.getCountClashes() + ", chains=" + qmap.getCountChains() + ", recurs=" + qmap.getCountRecursions() + "] ");
//        rnd.reset();
    }

    @Benchmark
    public CharSequence baseline() {
        return rnd.nextChars(M);
    }

    @Benchmark
    public QMap.Value testAdd() {
        QMap.Key key = qmap.withKey();
        key.putStr(rnd.nextChars(M));
        return key.findValue();
    }

    @Benchmark
    public DirectMapValues testAddLegacy() {
        DirectMap.KeyWriter kw = map.keyWriter();
        kw.putStr(rnd.nextChars(M));
        return map.getValues();
    }
}
