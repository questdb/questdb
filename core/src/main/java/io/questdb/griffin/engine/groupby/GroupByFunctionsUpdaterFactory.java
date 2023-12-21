/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import org.jetbrains.annotations.NotNull;

public class GroupByFunctionsUpdaterFactory {
    private static final int FIELD_POOL_OFFSET = 3;

    private GroupByFunctionsUpdaterFactory() {
    }

    /**
     * Creates a GroupByFunctionUpdater instance capturing the provided group by functions.
     * The generated class will have fields GroupByFunction f0, GroupByFunction f1, GroupByFunction f2 ... GroupByFunction fn
     * for each group by function from the provided list.
     * <p>
     * The generated class will have the following methods:
     * <ul>
     * <li>updateNew(MapValue value, Record record) - calls f0, f1, f2 ... fn.computeFirst(value, record) for each group by function</li>
     * <li>updateExisting(MapValue value, Record record) - calls f0, f1, f2 ... fn.computeNext(value, record) for each group by function</li>
     * <li>updateEmpty(MapValue value) - calls f0, f1, f2 ... fn.setEmpty(value) for each group by function</li>
     * <li>merge(MapValue destValue, MapValue srcValue) - calls fn.merge(destValue, srcValue) for each group by function</li>
     * <li>setFunctions(ObjList&lt;GroupByFunction&gt; groupByFunctions) - sets the group by functions to the fields. This method is called by the factory and should not be called by the caller.</li>
     * </ul>
     *
     * @param asm              BytecodeAssembler instance
     * @param groupByFunctions list of group by functions
     * @return GroupByFunctionUpdater instance
     */
    public static GroupByFunctionsUpdater getInstance(
            BytecodeAssembler asm,
            @NotNull ObjList<GroupByFunction> groupByFunctions
    ) {
        asm.init(GroupByFunctionsUpdater.class);
        asm.setupPool();
        final int thisClassIndex = asm.poolClass(asm.poolUtf8("io/questdb/griffin/engine/groupby/GroupByFunctionsUpdaterAsm"));
        final int superclassIndex = asm.poolClass(Object.class);
        int interfaceClassIndex = asm.poolClass(GroupByFunctionsUpdater.class);

        final int superIndex = asm.poolMethod(superclassIndex, "<init>", "()V");

        final int typeIndex = asm.poolUtf8("Lio/questdb/griffin/engine/functions/GroupByFunction;");
        final int functionSize = groupByFunctions.size();

        int firstFieldNameIndex = 0;
        int firstFieldIndex = 0;
        for (int i = 0; i < functionSize; i++) {
            // if you change pool calls then you will likely need to change the FIELD_POOL_OFFSET constant
            int fieldNameIndex = asm.poolUtf8().putAscii("f").put(i).$();
            int nameAndType = asm.poolNameAndType(fieldNameIndex, typeIndex);
            int fieldIndex = asm.poolField(thisClassIndex, nameAndType);
            if (i == 0) {
                firstFieldNameIndex = fieldNameIndex;
                firstFieldIndex = fieldIndex;
            }
        }

        final int computeFirstIndex = asm.poolInterfaceMethod(GroupByFunction.class, "computeFirst", "(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/sql/Record;)V");
        final int computeNextIndex = asm.poolInterfaceMethod(GroupByFunction.class, "computeNext", "(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/sql/Record;)V");
        final int setEmptyIndex = asm.poolInterfaceMethod(GroupByFunction.class, "setEmpty", "(Lio/questdb/cairo/map/MapValue;)V");
        final int mergeFunctionIndex = asm.poolInterfaceMethod(GroupByFunction.class, "merge", "(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/map/MapValue;)V");

        final int updateNewIndex = asm.poolUtf8("updateNew");
        final int updateNewSigIndex = asm.poolUtf8("(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/sql/Record;)V");
        final int updateExistingIndex = asm.poolUtf8("updateExisting");
        final int updateExistingSigIndex = asm.poolUtf8("(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/sql/Record;)V");
        final int updateEmptyIndex = asm.poolUtf8("updateEmpty");
        final int updateEmptySigIndex = asm.poolUtf8("(Lio/questdb/cairo/map/MapValue;)V");
        final int setFunctionsIndex = asm.poolUtf8("setFunctions");
        final int setFunctionsSigIndex = asm.poolUtf8("(Lio/questdb/std/ObjList;)V");
        final int mergeIndex = asm.poolUtf8("merge");
        final int mergeSigIndex = asm.poolUtf8("(Lio/questdb/cairo/map/MapValue;Lio/questdb/cairo/map/MapValue;)V");

        final int getIndex = asm.poolMethod(ObjList.class, "get", "(I)Ljava/lang/Object;");

        asm.finishPool();

        asm.defineClass(thisClassIndex, superclassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(functionSize);
        for (int i = 0; i < functionSize; i++) {
            asm.defineField(firstFieldNameIndex + (i * FIELD_POOL_OFFSET), typeIndex);
        }
        asm.methodCount(6);
        asm.defineDefaultConstructor(superIndex);

        generateUpdateNew(asm, functionSize, firstFieldIndex, computeFirstIndex, updateNewIndex, updateNewSigIndex);
        generateUpdateExisting(asm, functionSize, firstFieldIndex, computeNextIndex, updateExistingIndex, updateExistingSigIndex);
        generateUpdateEmpty(asm, functionSize, firstFieldIndex, setEmptyIndex, updateEmptyIndex, updateEmptySigIndex);
        generateSetFunctions(asm, functionSize, firstFieldIndex, setFunctionsIndex, setFunctionsSigIndex, getIndex);
        generateMerge(asm, functionSize, firstFieldIndex, mergeFunctionIndex, mergeIndex, mergeSigIndex);

        // class attribute count
        asm.putShort(0);

        GroupByFunctionsUpdater updater = asm.newInstance();

        updater.setFunctions(groupByFunctions);
        return updater;
    }

    private static void generateMerge(
            BytecodeAssembler asm,
            int fieldCount,
            int firstFieldIndex,
            int mergeFunctionIndex,
            int mergeIndex,
            int mergeSigIndex
    ) {
        asm.startMethod(mergeIndex, mergeSigIndex, 3, 3);
        for (int i = 0; i < fieldCount; i++) {
            asm.aload(0);
            asm.getfield(firstFieldIndex + (i * FIELD_POOL_OFFSET));
            asm.aload(1); // destValue
            asm.aload(2); // srcValue
            asm.invokeInterface(mergeFunctionIndex, 2);
        }
        asm.return_();
        asm.endMethodCode();
        // exceptions
        asm.putShort(0);
        // attributes
        asm.putShort(0);
        asm.endMethod();
    }

    private static void generateSetFunctions(BytecodeAssembler asm, int functionSize, int firstFieldIndex, int setFunctionsIndex, int setFunctionsSigIndex, int getIndex) {
        asm.startMethod(setFunctionsIndex, setFunctionsSigIndex, 3, 3);
        for (int i = 0; i < functionSize; i++) {
            asm.aload(0);
            asm.aload(1);
            asm.iconst(i);
            asm.invokeVirtual(getIndex);
            asm.putfield(firstFieldIndex + (i * FIELD_POOL_OFFSET));
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
            int fieldCount,
            int firstFieldIndex,
            int setEmptyIndex,
            int updateNameIndex,
            int updateSigIndex
    ) {
        asm.startMethod(updateNameIndex, updateSigIndex, 3, 3);
        for (int i = 0; i < fieldCount; i++) {
            asm.aload(0);
            asm.getfield(firstFieldIndex + (i * FIELD_POOL_OFFSET));
            asm.aload(1);
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

    private static void generateUpdateExisting(
            BytecodeAssembler asm,
            int fieldCount,
            int firstFieldIndex,
            int computeNextIndex,
            int updateNameIndex,
            int updateSigIndex
    ) {
        asm.startMethod(updateNameIndex, updateSigIndex, 3, 3);
        for (int i = 0; i < fieldCount; i++) {
            asm.aload(0);
            asm.getfield(firstFieldIndex + (i * FIELD_POOL_OFFSET));
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

    /**
     * Here we simply unroll the function list and call computeNext on each of them.
     * The end bytecode equals to the following:
     * <code>
     * f0.computeFist(value, record);
     * f1.computeFirst(value, record);
     * // ...
     * fn.computeFirst(value, record);
     * </code>
     * <p>
     * Other generated methods look similar.
     */
    private static void generateUpdateNew(
            BytecodeAssembler asm,
            int fieldCount,
            int firstFieldIndex,
            int computeFirstIndex,
            int updateNameIndex,
            int updateSigIndex
    ) {
        asm.startMethod(updateNameIndex, updateSigIndex, 3, 3);
        for (int i = 0; i < fieldCount; i++) {
            asm.aload(0);
            asm.getfield(firstFieldIndex + (i * FIELD_POOL_OFFSET));
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
}
