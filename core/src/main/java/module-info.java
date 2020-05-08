open module io.questdb {
    requires transitive jdk.unsupported;
    requires java.base;
    requires static org.jetbrains.annotations;
    requires static java.sql;

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

    exports io.questdb.std;
    exports io.questdb.std.str;
    exports io.questdb.network;
    exports io.questdb.log;
    exports io.questdb.mp;
}
