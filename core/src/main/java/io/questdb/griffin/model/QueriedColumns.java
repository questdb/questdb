package io.questdb.griffin.model;

import io.questdb.cairo.TableToken;
import io.questdb.std.*;

public class QueriedColumns extends AbstractSelfReturningObject<QueriedColumns> implements Mutable {
    private final ObjList<CharSequence> columnNamesList = new ObjList<>();
    private final LowerCaseCharSequenceHashSet columnNamesSet = new LowerCaseCharSequenceHashSet();
    private TableToken tableToken;

    public QueriedColumns(WeakSelfReturningObjectPool<QueriedColumns> parentPool) {
        super(parentPool);
    }

    public QueriedColumns deepCopy() {
        // we are creating columns with null pool because this is a deep copy that is not meant to be
        // "closed". If something attempts to close this instance they will get NPE.
        QueriedColumns queriedColumns = new QueriedColumns(null);
        queriedColumns.columnNamesList.addAll(this.columnNamesList);
        queriedColumns.tableToken = this.tableToken;
        return queriedColumns;
    }

    public void merge(ObjList<CharSequence> columnNames) {
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            final CharSequence columnName = columnNames.getQuick(i);
            final int idx = this.columnNamesSet.keyIndex(columnName);
            if (idx > -1) {
                this.columnNamesSet.addAt(idx, columnName);
                this.columnNamesList.add(columnName);
            }
        }
    }

    @Override
    public void clear() {
        tableToken = null;
        columnNamesSet.clear();
        columnNamesList.clear();
    }

    @Override
    public void close() {
        clear();
        super.close();
    }

    public ObjList<CharSequence> getColumnNames() {
        return columnNamesList;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public QueriedColumns of(TableToken token) {
        this.tableToken = token;
        return this;
    }

    public QueriedColumns of(TableToken tableToken, ObjList<CharSequence> columns) {
        this.tableToken = tableToken;
        merge(columns);
        return this;
    }
}
