module io.questdb.benchmarks {
    requires jdk.unsupported;
    requires java.base;
    requires io.questdb;
    requires jmh.core;
    requires org.apache.logging.log4j;

    exports org.questdb.generated;
}