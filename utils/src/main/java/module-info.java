open module io.questdb.cliutil {
    uses io.questdb.griffin.FunctionFactory;
    requires transitive io.questdb;
    requires com.google.gson;
    requires java.sql;
    exports io.questdb.cliutil;
}