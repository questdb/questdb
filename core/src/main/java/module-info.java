open module io.questdb {
    requires transitive jdk.unsupported;
    requires java.base;
    requires static org.jetbrains.annotations;
    requires static java.sql;

    uses io.questdb.griffin.FunctionFactory;

    exports io.questdb;
    exports io.questdb.cairo;
    exports io.questdb.cairo.map;
    exports io.questdb.cairo.sql;
    exports io.questdb.cairo.pool;
    exports io.questdb.cairo.pool.ex;
    exports io.questdb.cairo.security;

    exports io.questdb.cutlass.http;
    exports io.questdb.cutlass.http.processors;
    exports io.questdb.cutlass.json;
    exports io.questdb.cutlass.line;
    exports io.questdb.cutlass.line.udp;
    exports io.questdb.cutlass.pgwire;
    exports io.questdb.cutlass.text;
    exports io.questdb.cutlass.text.types;

    exports io.questdb.griffin;
    exports io.questdb.griffin.engine;
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
    exports io.questdb.griffin.engine.functions.gt;
    exports io.questdb.griffin.engine.functions.lt;
    exports io.questdb.griffin.engine.functions.math;
    exports io.questdb.griffin.engine.functions.regex;
    exports io.questdb.griffin.engine.functions.str;
    exports io.questdb.griffin.engine.groupby;
    exports io.questdb.griffin.engine.groupby.vect;

    exports io.questdb.std;
    exports io.questdb.std.str;
    exports io.questdb.network;
    exports io.questdb.log;
    exports io.questdb.mp;
}
