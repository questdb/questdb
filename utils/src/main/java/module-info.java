@SuppressWarnings("Java9RedundantRequiresStatement")
open module io.questdb.cliutil {
    uses io.questdb.griffin.FunctionFactory;
    requires transitive io.questdb;
    requires static questdb.client;
    requires com.google.gson;
    requires java.sql;
    exports io.questdb.cliutil;
}
