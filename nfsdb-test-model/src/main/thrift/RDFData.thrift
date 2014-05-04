namespace java com.nfsdb.journal.test.model

struct RDFData {
     1: required string subj;
     2: required string subjType;
     3: required string predicate;
     4: required string obj;
     5: required string objType;
     6: required i64 timestamp;
     7: optional bool deleted;
}