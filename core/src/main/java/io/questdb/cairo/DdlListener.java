package io.questdb.cairo;

public interface DdlListener {

    void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName);

    void onColumnRemoved(SecurityContext securityContext, TableToken tableToken, CharSequence columnName, int columnIdx);

    void onColumnRenamed(SecurityContext securityContext, TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName);

    void onTableCreated(SecurityContext securityContext, TableToken tableToken);

    void onTableRenamed(SecurityContext securityContext, TableToken oldTableToken, TableToken newTableToken);
}
