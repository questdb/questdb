namespace java com.nfsdb.journal.thrift.model

struct Quote {
     1: required i64 timestamp;
     2: required string sym;
     3: required double bid;
     4: required double ask;
     5: required i32 bidSize;
     6: required i32 askSize;
     7: required string mode;
     8: required string ex;
}
