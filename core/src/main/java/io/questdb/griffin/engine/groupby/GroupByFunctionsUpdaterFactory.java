/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.groupby;

import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.ObjList;

public class GroupByFunctionsUpdaterFactory {

    private GroupByFunctionsUpdaterFactory() {
    }

    /**
     * Creates a GroupByFunctionUpdater instance capturing the provided group by functions.
     * The functions are unrolled in the updateExisting method to avoid megamorphic calls
     * hurting JIT compiler efficiency.
     */
    public static GroupByFunctionsUpdater getInstance(
            BytecodeAssembler asm,
            ObjList<GroupByFunction> groupByFunctions
    ) {
        asm.init(GroupByFunctionsUpdater.class);
        asm.setupPool();
        final int thisClassIndex = asm.poolClass(asm.poolUtf8("io/questdb/griffin/engine/groupby/GroupByFunctionsUpdaterAsm"));
        final int superclassIndex = asm.poolClass(AbstractGroupByFunctionsUpdater.class);

        final int superIndex = asm.poolMethod(superclassIndex, "<init>", "()V");
        final int typeIndex = asm.poolUtf8("Lio/questdb/std/ObjList;");
        final int nameIndex = asm.poolUtf8().put("groupByFunctions").$();
        final int functionsFieldIndex = asm.poolField(thisClassIndex, asm.poolNameAndType(nameIndex, typeIndex));
        final int getQuickIndex = asm.poolMethod(ObjList.class, "getQuick", "(I)Ljava/lang/Object;");

        final int computeFirstIndex = asm.poolInterfaceMethod(GroupByFunction.class, "computeFirst", "(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/sql/Record;)V");
        final int computeNextIndex = asm.poolInterfaceMethod(GroupByFunction.class, "computeNext", "(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/sql/Record;)V");
        final int setEmptyIndex = asm.poolInterfaceMethod(GroupByFunction.class, "setEmpty", "(Lio/questdb/cairo/map/MapValue;)V");

        final int updateNewIndex = asm.poolUtf8("updateNew");
        final int updateNewSigIndex = asm.poolUtf8("(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/sql/Record;)V");
        final int updateExistingIndex = asm.poolUtf8("updateExisting");
        final int updateExistingSigIndex = asm.poolUtf8("(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/sql/Record;)V");
        final int updateEmptyIndex = asm.poolUtf8("updateEmpty");
        final int updateEmptySigIndex = asm.poolUtf8("(Lio/questdb/cairo/map/MapValue;)V");

        asm.finishPool();

        asm.defineClass(thisClassIndex, superclassIndex);
        asm.interfaceCount(0);
        asm.fieldCount(0);
        asm.methodCount(4);
        asm.defineDefaultConstructor(superIndex);

        generateUpdateNew(asm, groupByFunctions, functionsFieldIndex, getQuickIndex, computeFirstIndex, updateNewIndex, updateNewSigIndex);
        generateUpdateExisting(asm, groupByFunctions, functionsFieldIndex, getQuickIndex, computeNextIndex, updateExistingIndex, updateExistingSigIndex);
        generateUpdateEmpty(asm, groupByFunctions, functionsFieldIndex, getQuickIndex, setEmptyIndex, updateEmptyIndex, updateEmptySigIndex);

        // class attribute count
        asm.putShort(0);

        GroupByFunctionsUpdater updater = asm.newInstance();
        ((AbstractGroupByFunctionsUpdater) updater).groupByFunctions = groupByFunctions;
        return updater;
    }

    /**
     * Here we simply unroll the function list and call computeNext on each of them.
     * The end bytecode equals to the following:
     * <code>
     *   groupByFunctions.getQuick(0).computeNext(value, record);
     *   groupByFunctions.getQuick(1).computeNext(value, record);
     *   // ...
     *   groupByFunctions.getQuick(n).computeNext(value, record);
     * </code>
     * <p>
     * Other generated methods look similar.
     */
    private static void generateUpdateNew(
            BytecodeAssembler asm,
            ObjList<GroupByFunction> groupByFunctions,
            int functionsFieldIndex,
            int getQuickIndex,
            int computeFirstIndex,
            int updateNameIndex,
            int updateSigIndex
    ) {
        asm.startMethod(updateNameIndex, updateSigIndex, 3, 3);
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            asm.aload(0);
            asm.getfield(functionsFieldIndex);
            asm.iconst(i);
            asm.invokeVirtual(getQuickIndex);
            asm.aload(1); // map value
            asm.aload(2); // record
            asm.invokeInterface(computeFirstIndex, 2);
        }
        asm.return_();
        asm.endMethodCode();
        // exceptions
        asm.putShort(0);
        // attributes
        asm.putShort(0);
        asm.endMethod();
    }

    private static void generateUpdateExisting(
            BytecodeAssembler asm,
            ObjList<GroupByFunction> groupByFunctions,
            int functionsFieldIndex,
            int getQuickIndex,
            int computeNextIndex,
            int updateNameIndex,
            int updateSigIndex
    ) {
        asm.startMethod(updateNameIndex, updateSigIndex, 3, 3);
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            asm.aload(0);
            asm.getfield(functionsFieldIndex);
            asm.iconst(i);
            asm.invokeVirtual(getQuickIndex);
            asm.aload(1); // map value
            asm.aload(2); // record
            asm.invokeInterface(computeNextIndex, 2);
        }
        asm.return_();
        asm.endMethodCode();
        // exceptions
        asm.putShort(0);
        // attributes
        asm.putShort(0);
        asm.endMethod();
    }

    private static void generateUpdateEmpty(
            BytecodeAssembler asm,
            ObjList<GroupByFunction> groupByFunctions,
            int functionsFieldIndex,
            int getQuickIndex,
            int setEmptyIndex,
            int updateNameIndex,
            int updateSigIndex
    ) {
        asm.startMethod(updateNameIndex, updateSigIndex, 3, 3);
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            asm.aload(0);
            asm.getfield(functionsFieldIndex);
            asm.iconst(i);
            asm.invokeVirtual(getQuickIndex);
            asm.aload(1); // map value
            asm.invokeInterface(setEmptyIndex, 1);
        }
        asm.return_();
        asm.endMethodCode();
        // exceptions
        asm.putShort(0);
        // attributes
        asm.putShort(0);
        asm.endMethod();
    }
}
