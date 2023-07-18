package io.questdb.griffin.model;

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.std.*;

public class QueriedTables implements Mutable {
    private final LongIntHashMap tableIdMap = new LongIntHashMap();
    private final ObjList<QueriedColumns> queriedColumnList = new ObjList<>();
    private final WeakSelfReturningObjectPool<QueriedColumns> queriedColumnPool;
    // for UPDATE sql updates columns are assigned from pool as needed
    private QueriedColumns updatedColumns;

    public QueriedTables() {
        this.queriedColumnPool = new WeakSelfReturningObjectPool<>(QueriedColumns::new, 16);
    }

    public void mergePermissions(TableToken tableToken, ObjList<CharSequence> columnNames) {
        final QueriedColumns queriedColumns;
        final int index = tableIdMap.keyIndex(tableToken.getTableId());
        if (index < 0) {
            //
            queriedColumns = queriedColumnList.getQuick(tableIdMap.valueAt(index));
        } else {
            queriedColumns = queriedColumnPool.pop().of(tableToken);
            int listIndex = queriedColumnList.size();
            queriedColumnList.add(queriedColumns);
            tableIdMap.putAt(index, tableToken.getTableId(), listIndex);
        }
        queriedColumns.merge(columnNames);
    }

    public void revalidate(SecurityContext securityContext) {
        for (int i = 0, n = queriedColumnList.size(); i < n; i++) {
            final QueriedColumns qc = queriedColumnList.getQuick(i);
            securityContext.authorizeSelect(qc.getTableToken(), qc.getColumnNames());
        }

        if (updatedColumns != null) {
            securityContext.authorizeTableUpdate(updatedColumns.getTableToken(), updatedColumns.getColumnNames());
        }
    }

    @Override
    public void clear() {
        tableIdMap.clear();
        // these are pooled objects (QueriedColumns), we have to return them back
        Misc.freeObjListAndClear(queriedColumnList);
        updatedColumns = Misc.free(updatedColumns);
    }

    public QueriedTables deepCopy() {
        QueriedTables tables = new QueriedTables();
        tables.tableIdMap.putAll(this.tableIdMap);
        for (int i = 0, n = queriedColumnList.size(); i < n; i++) {
            tables.queriedColumnList.add(this.queriedColumnList.getQuick(i).deepCopy());
        }
        if (this.updatedColumns != null) {
            tables.updatedColumns = this.updatedColumns.deepCopy();
        }
        return tables;
    }

    public void mergeUpdatedColumns(TableToken tableToken, ObjList<CharSequence> columnNames) {
        if (updatedColumns == null) {
            updatedColumns = queriedColumnPool.pop().of(tableToken);
        }
        this.updatedColumns.merge(columnNames);
    }
}
