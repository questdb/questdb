package io.questdb.cairo;

// TODO: use this for table/column rename
public interface DdlListener {

    void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName);

    void onTableCreated(SecurityContext securityContext, TableToken tableToken);
}
