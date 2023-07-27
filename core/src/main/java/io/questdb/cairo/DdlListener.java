package io.questdb.cairo;

// TODO: use this for table/column rename and table/column drop too
public interface DdlListener {
    void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName);
    void onTableCreated(SecurityContext securityContext, TableToken tableToken);
}
