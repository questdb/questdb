/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.model;

import com.questdb.factory.configuration.JournalStructure;
import com.questdb.ql.RecordSource;
import com.questdb.std.Mutable;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectFactory;

public class CreateJournalModel implements Mutable, ParsedModel {
    public static final ObjectFactory<CreateJournalModel> FACTORY = new ObjectFactory<CreateJournalModel>() {
        @Override
        public CreateJournalModel newInstance() {
            return new CreateJournalModel();
        }
    };

    private String name;
    private QueryModel queryModel;
    private ExprNode timestamp;
    private ExprNode partitionBy;
    private ExprNode recordHint;
    private JournalStructure struct;
    private ObjList<ColumnIndexModel> columnIndexModels = new ObjList<>();
    private RecordSource recordSource;

    private CreateJournalModel() {
    }

    public void addColumnIndexModel(ColumnIndexModel model) {
        columnIndexModels.add(model);
    }

    @Override
    public void clear() {
        columnIndexModels.clear();
        queryModel = null;
        timestamp = null;
        partitionBy = null;
        recordHint = null;
        struct = null;
        recordSource = null;
        name = null;
    }

    public ObjList<ColumnIndexModel> getColumnIndexModels() {
        return columnIndexModels;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExprNode getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(ExprNode partitionBy) {
        this.partitionBy = partitionBy;
    }

    public QueryModel getQueryModel() {
        return queryModel;
    }

    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }

    public ExprNode getRecordHint() {
        return recordHint;
    }

    public void setRecordHint(ExprNode recordHint) {
        this.recordHint = recordHint;
    }

    public RecordSource getRecordSource() {
        return recordSource;
    }

    public void setRecordSource(RecordSource recordSource) {
        this.recordSource = recordSource;
    }

    public JournalStructure getStruct() {
        return struct;
    }

    public void setStruct(JournalStructure struct) {
        this.struct = struct;
    }

    public ExprNode getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(ExprNode timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean isQuery() {
        return false;
    }
}
