package io.questdb.cairo;

public class DdlListenerImpl implements DdlListener {
    public static final DdlListener INSTANCE = new DdlListenerImpl();

    @Override
    public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
    }

    @Override
    public void onColumnRenamed(SecurityContext securityContext, TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {

    }

    @Override
    public void onTableCreated(SecurityContext securityContext, TableToken tableToken) {
    }

    @Override
    public void onTableRenamed(SecurityContext securityContext, TableToken oldTableToken, TableToken newTableToken) {

    }
}
