module io.questdb.benchmarks {
    requires jdk.unsupported;
    requires java.base;
    requires io.questdb;
    requires jmh.core;
    requires org.apache.logging.log4j;
    requires simpleclient;
    requires simpleclient.common;

    exports org.questdb.jmh_generated;
}
