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

open module io.questdb {
    requires transitive jdk.unsupported;
    requires static org.jetbrains.annotations;
    requires static java.management;
    requires jdk.management;
    requires java.desktop;
    requires java.sql;

    uses io.questdb.griffin.FunctionFactory;
    exports io.questdb;
    exports io.questdb.cairo;
    exports io.questdb.cairo.vm;
    exports io.questdb.cairo.map;
    exports io.questdb.cairo.sql;
    exports io.questdb.cairo.pool;
    exports io.questdb.cairo.pool.ex;
    exports io.questdb.cairo.security;

    exports io.questdb.cutlass;
    exports io.questdb.cutlass.http;
    exports io.questdb.cutlass.http.processors;
    exports io.questdb.cutlass.http.ex;
    exports io.questdb.cutlass.json;
    exports io.questdb.cutlass.line;
    exports io.questdb.cutlass.line.udp;
    exports io.questdb.cutlass.line.tcp;
    exports io.questdb.cutlass.line.http;
    exports io.questdb.cutlass.pgwire;
    exports io.questdb.cutlass.text;
    exports io.questdb.cutlass.text.types;

    exports io.questdb.griffin;
    exports io.questdb.griffin.engine;
    exports io.questdb.griffin.model;
    exports io.questdb.griffin.engine.functions;
    exports io.questdb.griffin.engine.functions.rnd;
    exports io.questdb.griffin.engine.functions.bind;
    exports io.questdb.griffin.engine.functions.bool;
    exports io.questdb.griffin.engine.functions.cast;
    exports io.questdb.griffin.engine.functions.catalogue;
    exports io.questdb.griffin.engine.functions.columns;
    exports io.questdb.griffin.engine.functions.conditional;
    exports io.questdb.griffin.engine.functions.constants;
    exports io.questdb.griffin.engine.functions.date;
    exports io.questdb.griffin.engine.functions.eq;
    exports io.questdb.griffin.engine.functions.groupby;
    exports io.questdb.griffin.engine.functions.lt;
    exports io.questdb.griffin.engine.functions.math;
    exports io.questdb.griffin.engine.functions.regex;
    exports io.questdb.griffin.engine.functions.str;
    exports io.questdb.griffin.engine.functions.test;
    exports io.questdb.griffin.engine.functions.geohash;
    exports io.questdb.griffin.engine.functions.bin;
    exports io.questdb.griffin.engine.functions.lock;
    exports io.questdb.griffin.engine.functions.window;
    exports io.questdb.griffin.engine.functions.table;
    exports io.questdb.griffin.engine.groupby;
    exports io.questdb.griffin.engine.groupby.vect;
    exports io.questdb.griffin.engine.orderby;
    exports io.questdb.griffin.engine.window;
    exports io.questdb.griffin.engine.table;
    exports io.questdb.jit;
    exports io.questdb.std;
    exports io.questdb.std.datetime;
    exports io.questdb.std.datetime.microtime;
    exports io.questdb.std.datetime.millitime;
    exports io.questdb.std.datetime.nanotime;
    exports io.questdb.std.str;
    exports io.questdb.std.ex;
    exports io.questdb.std.fastdouble;
    exports io.questdb.network;
    exports io.questdb.log;
    exports io.questdb.mp;
    exports io.questdb.tasks;
    exports io.questdb.metrics;
    exports io.questdb.cairo.vm.api;
    exports io.questdb.cairo.mig;
    exports io.questdb.griffin.engine.join;
    exports io.questdb.griffin.engine.ops;
    exports io.questdb.cairo.sql.async;
    exports io.questdb.cutlass.http.client;
    exports io.questdb.griffin.engine.functions.long128;
    exports io.questdb.cairo.wal;
    exports io.questdb.cairo.wal.seq;
    exports io.questdb.cutlass.auth;
    exports io.questdb.cutlass.line.tcp.auth;
    exports io.questdb.cairo.frm;
    exports io.questdb.cairo.frm.file;
    exports io.questdb.std.histogram.org.HdrHistogram;
    exports io.questdb.client;
    exports io.questdb.std.bytes;
    exports io.questdb.std.histogram.org.HdrHistogram.packedarray;
    exports io.questdb.client.impl;
    exports io.questdb.griffin.engine.groupby.hyperloglog;
    exports io.questdb.griffin.engine.functions.finance;
    exports io.questdb.std.json;
    exports io.questdb.griffin.engine.functions.json;
    exports io.questdb.std.filewatch;
    exports io.questdb.griffin.engine.table.parquet;
    exports io.questdb.cairo.mv;
    exports io.questdb.cairo.file;
    exports io.questdb.cairo.arr;
    exports io.questdb.griffin.engine.functions.array;
    exports io.questdb.cutlass.line.array;
    exports io.questdb.preferences;
    exports io.questdb.griffin.engine.functions.memoization;
}
