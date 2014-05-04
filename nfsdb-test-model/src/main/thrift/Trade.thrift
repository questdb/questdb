namespace java com.nfsdb.journal.test.model

struct Trade {
     1: required i64 timestamp;
     2: required string sym;
     3: required double price;
     4: required i32 size;
     5: required i32 stop;
     6: required string cond;
     7: required string ex;
}
