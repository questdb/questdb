/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.std.histogram.org.HdrHistogram.packedarray;

import io.questdb.cairo.CairoException;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A packed-value, sparse array context used for storing 64 bit signed values.
 * <p>
 * An array context is optimised for tracking sparsely set (as in mostly zeros) values that tend to not make
 * use pof the full 64 bit value range even when they are non-zero. The array context's internal representation
 * is such that the packed value at each virtual array index may be represented by 0-8 bytes of actual storage.
 * <p>
 * An array context encodes the packed values in 8 "set trees" with each set tree representing one byte of the
 * packed value at the virtual index in question. The {@link #getPackedIndex(int, int, boolean)} method is used
 * to look up the byte-index corresponding to the given (set tree) value byte of the given virtual index, and can
 * be used to add entries to represent that byte as needed. As a succesful {@link #getPackedIndex(int, int, boolean)}
 * may require a resizing of the array, it can throw a {@link ResizeException} to indicate that the requested
 * packed index cannot be found or added without a resize of the physical storage.
 */
abstract class AbstractPackedArrayContext implements Serializable {
    static final int MAX_SUPPORTED_PACKED_COUNTS_ARRAY_LENGTH = (Short.MAX_VALUE / 4);
    static final int MINIMUM_INITIAL_PACKED_ARRAY_CAPACITY = 16;
    private static final int LEAF_LEVEL_SHIFT = 3;
    private static final int NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS = 2;
    private static final int NON_LEAF_ENTRY_PREVIOUS_VERSION_OFFSET = 1;
    private static final int NON_LEAF_ENTRY_SLOT_INDICATORS_OFFSET = 0;
    private static final int NUMBER_OF_SETS = 8;
    private static final int PACKED_ARRAY_GROWTH_FRACTION_POW2 = 4;
    /**
     * The physical representation uses an insert-at-the-end mechanism for adding contents to the array. Any
     * insertion will occur at the very end of the array, and any expansion of an element will move it to the end,
     * leaving an empty slot behind.
     * <p>
     * Terminology:
     * <p>
     * long-word: a 64-bit-aligned 64 bit word
     * short-word: a 16-bit-aligned 16 bit word
     * byte: an 8-bit-aligned byte
     * <p>
     * long-index: an index of a 64-bit-aligned word within the overall array (i.e. in multiples of 8 bytes)
     * short-index: an index of a 16-bit aligned short within the overall array (i.e. in multiples of 2 bytes)
     * byte-index: an index of an 8-bit aligned byte within the overall array (i.e. in multiples of 1 byte)
     * <p>
     * The storage array stores long (64 bit) words. Lookups for the variuous sizes are done as such:
     * <p>
     * long getAtLongIndex(int longIndex) { return array[longIndex]; }
     * short getAtShortIndex(int shortIndex) { return (short)((array[shortIndex >> 2] >> (shortIndex & 0x3)) & 0xffff); }
     * byte getAtByteIndex(int byteIndex) { return (byte)((array[byteIndex >> 3] >> (byteIndex & 0x7)) & 0xff); }
     * <p>
     * [Therefore there is no dependence on byte endiannes of the underlying arhcitecture]
     * <p>
     * Structure:
     * <p>
     * The packed array captures values at virtual indexes in a collection of striped "set trees" (also called "sets"),
     * with each set tree representing one byte of the value at the virtual index in question. As such, there are 8
     * sets in the array, each corresponding to a byte in the overall value being stored. Set 0 contains the LSByte
     * of the value, and Set 7 contains the MSByte of the value.
     * <p>
     * The array contents is comprised of thre types of entries:
     * - The root indexes: A fixed size 8 short-words array of short indexes at the start of the array, containing
     * the short-index of the root entry of each of the 8 set trees.
     * <p>
     * - Non-Leaf Entires: Variable sized, 2-18 short-words entries representing non-leaf entries in a set tree.
     * Non-Leaf entries comprise of a 2 short-word header containing a packed slot indicators bitmask and the
     * (optional non-zero) index of previous version of the entry, followed by an array of 0-16 shortwords.
     * The short-word found at a given slot in this array holds an index to an entry in the next level of
     * the set tree.
     * <p>
     * - Leaf Entries: comprised of long-words. Each byte [0-7] in the longword holds an actual value. Specifically,
     * the byte-index of that LeafEntry byte in the array is the byte-index for the given set's byte value of a
     * virtual index.
     * <p>
     * If a given virtual index for a given set has no entry in a given set tree, the byte value for that set of
     * that virtual index interpreted as 0. If a given set tree does not have an entry for a given virtual index,
     * it is safe to assume that no higher significance set tree have one either.
     * *
     * Non-leaf entries structure and mutation protocols:
     * <p>
     * The structure of a Non-Leaf entry in the array can be roughly desctibed in terms of this C-stylre struct:
     * <p>
     * struct nonLeafEntry {
     * short packedSlotIndicators;
     * short previousVersionIndex;
     * short[] enrtrySlotsIndexes;
     * }
     * <p>
     * Non-leaf entries are 2-18 short-words in length, with the length determined by the number of bits set in
     * the packedSlotIndicators short-word in the entry. The packed slot indicators short-word is a bit mask which
     * represents the 16 possible next-level entries below the given entry, and has a bit set (to '1') for each slot
     * that is actually populated with a next level entry. Each of the short-words in the enrtrySlots is
     * associated with a specific active ('1') bit in the packedSlotIndicators short-word, and holds the index
     * to the next level's entry associated with ta given path in the tree. [Note: the values in enrtrySlotsIndexes[]
     * are short-indexes if the next level is not a leaf level, and long-indexes if the next level is
     * a leaf.]
     * <p>
     * Summary of Non-leaf entry use and replacement protocol:
     * <p>
     * - No value in any enrtrySlotsIndexes[] array is ever initialized to a zero value. Zero values in
     * enrtrySlotsIndexes[] can only appear through consolidation (see below). Once an enrtrySlotsIndexes[]
     * slot is observed to contain a zero, it cannot change to a non-zero value.
     * <p>
     * - Zero values encountered in enrtrySlotsIndexes[] arrays are never followed. If a zero value is found
     * when looking for the index to a lower level entry during a tree walk, the tree walking operation is
     * restarted from the root.
     * <p>
     * - A Non-Leaf entry with an active (non zero index) previous version is never followed or expanded.
     * Instead, any thread encountering a Non-leaf entry with an active previous version will consolidate
     * the previous version with the current one. the consolidation opeartion will clear (zero) the
     * previousVersionIndex, which will then allow the caller to continue with whatever use the thread was
     * attempting to make of the entry.
     * <p>
     * - Expansion of entries: Since entries hold only enough storage to represent currently populated paths
     * below them in the set tree, any addition of entries at a lower level requires the expansion of the entry
     * to make room for a larger enrtrySlotsIndexes array. Expansion allocates a new and larger entry structure,
     * and populates the newly inserted slot in it with an index to a newly allocated next-level entry. It then
     * links the newly expanded entry the previous entry structure via the previousVersionIndex field, and
     * publishes the newly expanded entry by [atomically] replacing the "pointer index" to the previous entry
     * (located at a higher level entry's slot, or in the root indexes) with a "pointer index" to the newly
     * expanded entry structure.  A failure to atomically publish a newly expanded entry (e.g. if the "pointer
     * index" being replaced holds a value other than that in our not-yet-published previousVersionIndex) will
     * restart the expansion operation from the beginning.
     * When first published, a newly-visible expanded entry is immediately "usable" because it has an active,
     * "not yet consolidated" previous version entry, and any user of the entry will first have to consolidate it.
     * The expansion will follow publication of the expanded entry with a consolidation of the previous entry
     * into the new one, clearing the previousVersionIndex field in the process, and enabling normal use of
     * the expanded entry.
     * <p>
     * - Concurrent consolidation: While expansion and consolidation are ongoing, other threads can be
     * concurrently walking the set trees. Per the protocol stated here, any tree walk encountering a Non-Leaf
     * entry with an active previous version will consolidate the entry before using it. Consolidation can
     * of a given entry can occur concurrently by an an expanding thread and by multiple walking threads.
     * <p>
     * - Consolidation of a a previous version entry into a current one is done by:
     * - For each non-zero index in the previous version enrty, copy that index to the new assocaited
     * entry slot in the entry, and CAS a zero in the old entry slot. If the CAS fails, repeat (including
     * the zero check).
     * - Once all entry slots in the previous version entry have been consolidated and zeroed, zero
     * the index to the previous version entry.
     */

    private static final int PACKED_ARRAY_GROWTH_INCREMENT = 16;
    private static final int SET_0_START_INDEX = 0;
    private final boolean isPacked;
    private int physicalLength;
    private int topLevelShift = Integer.MAX_VALUE; // Make it non-sensical until properly initialized.
    private int virtualLength = 0;

    AbstractPackedArrayContext(final int virtualLength, final int initialPhysicalLength) {
        physicalLength = Math.max(initialPhysicalLength, MINIMUM_INITIAL_PACKED_ARRAY_CAPACITY);
        isPacked = (physicalLength <= AbstractPackedArrayContext.MAX_SUPPORTED_PACKED_COUNTS_ARRAY_LENGTH);
        if (!isPacked) {
            physicalLength = virtualLength;
        }
    }

    @Override
    public String toString() {
        String output = "PackedArrayContext:\n";
        if (!isPacked()) {
            return output + "Context is unpacked:\n" + unpackedToString();
        }
        for (int setNumber = 0; setNumber < NUMBER_OF_SETS; setNumber++) {
            try {
                int entryPointerIndex = SET_0_START_INDEX + setNumber;
                int entryIndex = getIndexAtShortIndex(entryPointerIndex);
                output += String.format("Set %d: root = %d \n", setNumber, entryIndex);
                if (entryIndex == 0) continue;
                output += nonLeafEntryToString(entryIndex, getTopLevelShift(), 4);
            } catch (Exception ex) {
                output += String.format("Exception thrown in set %d\n", setNumber);
            }
        }
        output += recordedValuesToString();
        return output;
    }

    //
    //     ###    ########   ######  ######## ########     ###     ######  ########  ######
    //    ## ##   ##     ## ##    ##    ##    ##     ##   ## ##   ##    ##    ##    ##    ##
    //   ##   ##  ##     ## ##          ##    ##     ##  ##   ##  ##          ##    ##
    //  ##     ## ########   ######     ##    ########  ##     ## ##          ##     ######
    //  ######### ##     ##       ##    ##    ##   ##   ######### ##          ##          ##
    //  ##     ## ##     ## ##    ##    ##    ##    ##  ##     ## ##    ##    ##    ##    ##
    //  ##     ## ########   ######     ##    ##     ## ##     ##  ######     ##     ######
    //

    private boolean casIndexAtEntrySlot(final int entryIndex,
                                        final int slot,
                                        final short expectedIndexValue,
                                        final short newIndexValue) {
        return casAtShortIndex(entryIndex + NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS + slot,
                expectedIndexValue, newIndexValue);
    }

    private boolean casIndexAtEntrySlotIfNonZeroAndLessThan(final int entryIndex,
                                                            final int slot,
                                                            final short newIndexValue) {
        boolean success;
        do {
            short existingIndexValue = getIndexAtEntrySlot(entryIndex, slot);
            if (existingIndexValue == 0) return false;
            if (newIndexValue <= existingIndexValue) return false;
            success = casIndexAtEntrySlot(entryIndex, slot, existingIndexValue, newIndexValue);
        } while (!success);
        return true;
    }

    /**
     * Consolidate entry with previous entry verison if one exists
     *
     * @param entryIndex The shortIndex of the entry to be consolidated
     */
    private void consolidateEntry(final int entryIndex) {
        int previousVersionIndex = getPreviousVersionIndex(entryIndex);
        if (previousVersionIndex == 0) return;
        if (getPreviousVersionIndex(previousVersionIndex) != 0) {
            throw new IllegalStateException("Encountered Previous Version Entry that is not itself consolidated.");
        }

        int previousVersionPackedSlotsIndicators = getPackedSlotIndicators(previousVersionIndex);
        // Previous version exists, needs consolidation

        int packedSlotsIndicators = getPackedSlotIndicators(entryIndex);
        int insertedSlotMask = packedSlotsIndicators ^ previousVersionPackedSlotsIndicators; // the only bit that differs
        int slotsBelowBitNumber = packedSlotsIndicators & (insertedSlotMask - 1);
        int insertedSlotIndex = Integer.bitCount(slotsBelowBitNumber);
        int numberOfSlotsInEntry = Integer.bitCount(packedSlotsIndicators);

        // Copy the entry slots from previous version, skipping the newly inserted slot in the target:
        int sourceSlot = 0;
        for (int targetSlot = 0; targetSlot < numberOfSlotsInEntry; targetSlot++) {
            if (targetSlot != insertedSlotIndex) {
                boolean success = true;
                do {
                    short indexAtSlot = getIndexAtEntrySlot(previousVersionIndex, sourceSlot);
                    if (indexAtSlot != 0) {
                        // Copy observed index at slot to current entry
                        // (only copy value in if previous value is less than new one AND is non-zero)
                        casIndexAtEntrySlotIfNonZeroAndLessThan(entryIndex, targetSlot, indexAtSlot);

                        // CAS the previous verison slot to 0.
                        // (Succeeds only if the index in that slot has not changed. Retry if it did).
                        success = casIndexAtEntrySlot(previousVersionIndex, sourceSlot, indexAtSlot, (short) 0);
                    }
                }
                while (!success);
                sourceSlot++;
            }
        }

        setPreviousVersionIndex(entryIndex, (short) 0);
    }

    private long contextLocalGetValueAtIndex(final int virtualIndex) {
        long value = 0;
        for (int byteNum = 0; byteNum < NUMBER_OF_SETS; byteNum++) {
            int packedIndex = 0;
            long byteValueAtPackedIndex;
            do {
                try {
                    packedIndex = getPackedIndex(byteNum, virtualIndex, false);
                    if (packedIndex < 0) {
                        return value;
                    }
                    byteValueAtPackedIndex = (((long) getAtByteIndex(packedIndex)) & 0xff) << (byteNum << 3);
                } catch (ResizeException ex) {
                    throw new IllegalStateException("Should never encounter a resize excpetion without inserts");
                }
            } while (packedIndex == 0);

            value += byteValueAtPackedIndex;
        }
        return value;
    }

    private void copyEntriesAtLevelFromOther(final AbstractPackedArrayContext other,
                                             final int otherLevelEntryIndex,
                                             final int levelEntryIndexPointer,
                                             final int otherIndexShift) {
        boolean nextLevelIsLeaf = (otherIndexShift == LEAF_LEVEL_SHIFT);
        int packedSlotIndicators = other.getPackedSlotIndicators(otherLevelEntryIndex);
        int numberOfSlots = Integer.bitCount(packedSlotIndicators);
        int sizeOfEntry = NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS + numberOfSlots;

        // Allocate entry:
        int entryIndex = 0;
        do {
            try {
                entryIndex = newEntry(sizeOfEntry);
            } catch (ResizeException ex) {
                resizeArray(ex.getNewSize());
            }
        }
        while (entryIndex == 0);

        setAtShortIndex(levelEntryIndexPointer, (short) entryIndex);
        setAtShortIndex(entryIndex + NON_LEAF_ENTRY_SLOT_INDICATORS_OFFSET, (short) packedSlotIndicators);
        setAtShortIndex(entryIndex + NON_LEAF_ENTRY_PREVIOUS_VERSION_OFFSET, (short) 0);
        for (int i = 0; i < numberOfSlots; i++) {
            if (nextLevelIsLeaf) {
                // Make leaf in other:
                int leafEntryIndex = 0;
                do {
                    try {
                        leafEntryIndex = newLeafEntry();
                    } catch (ResizeException ex) {
                        resizeArray(ex.getNewSize());
                    }
                }
                while (leafEntryIndex == 0);
                setIndexAtEntrySlot(entryIndex, i, (short) leafEntryIndex);
                lazySetAtLongIndex(leafEntryIndex, 0);
            } else {
                int otherNextLevelEntryIndex = other.getIndexAtEntrySlot(otherLevelEntryIndex, i);
                copyEntriesAtLevelFromOther(other, otherNextLevelEntryIndex,
                        (entryIndex + NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS + i),
                        otherIndexShift - 4);
            }
        }
    }

    private void expandArrayIfNeeded(final int entryLengthInLongs) throws ResizeException {
        final int currentLength = length();
        if (length() < getPopulatedLongLength() + entryLengthInLongs) {
            int growthIncrement = Math.max(entryLengthInLongs, PACKED_ARRAY_GROWTH_INCREMENT);
            growthIncrement = Math.max(growthIncrement, getPopulatedLongLength() >> PACKED_ARRAY_GROWTH_FRACTION_POW2);
            throw new ResizeException(currentLength + growthIncrement);
        }
    }

    /**
     * Expand entry as indicated.
     *
     * @param existingEntryIndex the index of the entry
     * @param entryPointerIndex  index to the slot pointing to the entry (needs to be fixed up)
     * @param insertedSlotIndex  realtive [packed] index of slot being inserted into entry
     * @param insertedSlotMask   mask value fo slot being inserted
     * @param nextLevelIsLeaf    the level below this one is a leaf level
     * @return the updated index of the entry (-1 if epansion failed due to conflict)
     * @throws RetryException if expansion fails due to concurrent conflict, and caller should try again.
     */
    private int expandEntry(final int existingEntryIndex,
                            final int entryPointerIndex,
                            final int insertedSlotIndex,
                            final int insertedSlotMask,
                            final boolean nextLevelIsLeaf) throws RetryException, ResizeException {
        int packedSlotIndicators = ((int) getAtShortIndex(existingEntryIndex)) & 0xffff;
        packedSlotIndicators |= insertedSlotMask;
        int numberOfslotsInExpandedEntry = Integer.bitCount(packedSlotIndicators);
        if (insertedSlotIndex >= numberOfslotsInExpandedEntry) {
            throw new IllegalStateException("inserted slot index is out of range given provided masks");
        }
        int expandedEntryLength = numberOfslotsInExpandedEntry + NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS;

        // Create new next-level entry to refer to from slot at this level:
        int indexOfNewNextLevelEntry = 0;
        if (nextLevelIsLeaf) {
            indexOfNewNextLevelEntry = newLeafEntry(); // Establish long-index to new leaf entry
        } else {
            // TODO: Optimize this by creating the whole sub-tree here, rather than a step that will immediaterly expand
            // Create a new 1 word (empty, no slots set) entry for the next level:
            indexOfNewNextLevelEntry = newEntry(NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS); // Establish short-index to new leaf entry
            setPackedSlotIndicators(indexOfNewNextLevelEntry, (short) 0);
            setPreviousVersionIndex(indexOfNewNextLevelEntry, (short) 0);
        }
        short insertedSlotValue = (short) indexOfNewNextLevelEntry;

        int expandedEntryIndex = newEntry(expandedEntryLength);

        // populate the packed indicators word:
        setPackedSlotIndicators(expandedEntryIndex, (short) packedSlotIndicators);
        setPreviousVersionIndex(expandedEntryIndex, (short) existingEntryIndex);

        // Populate the inserted slot with the iundex of the new next level entry:
        setIndexAtEntrySlot(expandedEntryIndex, insertedSlotIndex, insertedSlotValue);

        // Copy of previous version entries is deferred to later consolidateEntry() call.

        // Set the pointer to the updated entry index. If CAS fails, discard by throwing retry expecption.
        boolean success = casAtShortIndex(entryPointerIndex, (short) existingEntryIndex, (short) expandedEntryIndex);
        if (!success) {
            throw new RetryException();
        }

        // Exanded entry is published, now consolidate it:

        consolidateEntry(expandedEntryIndex);

        return expandedEntryIndex;

    }

    private int findFirstPotentiallyPopulatedVirtualIndexStartingAt(final int startingVirtualIndex) {
        int nextVirtrualIndex = -1;
        // Look for a populated virtual index in set 0:
        boolean retry;
        do {
            retry = false;
            try {
                int entryIndex = getRootEntry(0);
                if (entryIndex == 0) return getVirtualLength(); // Nothing under the root
                nextVirtrualIndex =
                        seekToPopulatedVirtualIndexStartingAtLevel(startingVirtualIndex, entryIndex, getTopLevelShift());
            } catch (RetryException ex) {
                retry = true;
            }
        } while (retry);

        // Don't drill to value if out of range:
        if ((nextVirtrualIndex < 0) || (nextVirtrualIndex >= getVirtualLength())) {
            return getVirtualLength();
        }

        return nextVirtrualIndex;
    }

    private short getIndexAtEntrySlot(final int entryIndex, final int slot) {
        return getAtShortIndex(entryIndex + NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS + slot);
    }

    private int getPackedSlotIndicators(final int entryIndex) {
        return ((int) getAtShortIndex(entryIndex + NON_LEAF_ENTRY_SLOT_INDICATORS_OFFSET)) & 0xffff;
    }

    private short getPreviousVersionIndex(final int entryIndex) {
        return getAtShortIndex(entryIndex + NON_LEAF_ENTRY_PREVIOUS_VERSION_OFFSET);
    }

    private int getRootEntry(final int setNumber) {
        try {
            return getRootEntry(setNumber, false);
        } catch (RetryException | ResizeException ex) {
            throw new IllegalStateException("Should not Resize or Retry exceptions on real-only read: ", ex);
        }

    }

    private int getRootEntry(final int setNumber, boolean insertAsNeeded) throws RetryException, ResizeException {
        int entryPointerIndex = SET_0_START_INDEX + setNumber;
        int entryIndex = getIndexAtShortIndex(entryPointerIndex);

        if (entryIndex == 0) {
            if (!insertAsNeeded) {
                return 0; // Index does not currently exist in packed array;
            }

            entryIndex = newEntry(NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS);
            // Create a new empty (no slots set) entry for the next level:
            setPackedSlotIndicators(entryIndex, (short) 0);
            setPreviousVersionIndex(entryIndex, (short) 0);

            boolean success = casAtShortIndex(entryPointerIndex, (short) 0, (short) entryIndex);
            if (!success) {
                throw new RetryException();
            }
        }

        if (((getTopLevelShift() != LEAF_LEVEL_SHIFT)) && getPreviousVersionIndex(entryIndex) != 0) {
            consolidateEntry(entryIndex);
        }
        return entryIndex;
    }

    private String leafEntryToString(final int entryIndex, final int indentLevel) {
        String output = "";
        for (int i = 0; i < indentLevel; i++) {
            output += "  ";
        }
        try {
            output += "Leaf bytes : ";
            for (int i = 56; i >= 0; i -= 8) {
                output += String.format("0x%02x ", (getAtLongIndex(entryIndex) >>> i) & 0xff);

            }
            output += "\n";
        } catch (Exception ex) {
            output += String.format("Exception thrown at leafEnty at index %d\n", entryIndex);
        }
        return output;
    }

    private int newEntry(final int entryLengthInShorts) throws ResizeException {
        // Add entry at the end of the array:
        int newEntryIndex;
        boolean success;
        do {
            newEntryIndex = getPopulatedShortLength();
            expandArrayIfNeeded((entryLengthInShorts >> 2) + 1);
            success = casPopulatedShortLength(newEntryIndex, (newEntryIndex + entryLengthInShorts));
        } while (!success);

        for (int i = 0; i < entryLengthInShorts; i++) {
            setAtShortIndex(newEntryIndex + i, (short) -1); // Poison value -1. Must be overriden before reads
        }
        return newEntryIndex;
    }

    //
    //  ########  ########  #### ##     ## #### ######## #### ##     ## ########     #######  ########   ######
    //  ##     ## ##     ##  ##  ###   ###  ##     ##     ##  ##     ## ##          ##     ## ##     ## ##    ##
    //  ##     ## ##     ##  ##  #### ####  ##     ##     ##  ##     ## ##          ##     ## ##     ## ##
    //  ########  ########   ##  ## ### ##  ##     ##     ##  ##     ## ######      ##     ## ########   ######
    //  ##        ##   ##    ##  ##     ##  ##     ##     ##   ##   ##  ##          ##     ## ##              ##
    //  ##        ##    ##   ##  ##     ##  ##     ##     ##    ## ##   ##          ##     ## ##        ##    ##
    //  ##        ##     ## #### ##     ## ####    ##    ####    ###    ########     #######  ##         ######
    //

    private int newLeafEntry() throws ResizeException {
        // Add entry at the end of the array:
        int newEntryIndex;
        boolean success;
        do {
            newEntryIndex = getPopulatedLongLength();
            expandArrayIfNeeded(1);
            success = casPopulatedLongLength(newEntryIndex, (newEntryIndex + 1));
        } while (!success);

        lazySetAtLongIndex(newEntryIndex, 0);
        return newEntryIndex;
    }

    private String nonLeafEntryToString(final int entryIndex,
                                        final int indexShift,
                                        final int indentLevel) {
        String output = "";
        for (int i = 0; i < indentLevel; i++) {
            output += "  ";
        }
        try {
            final int packedSlotIndicators = getPackedSlotIndicators(entryIndex);
            output += String.format("slotIndiators: 0x%02x, prevVersionIndex: %3d: [ ",
                    packedSlotIndicators,
                    getPreviousVersionIndex(entryIndex));
            final int numberOfslotsInEntry = Integer.bitCount(packedSlotIndicators);
            for (int i = 0; i < numberOfslotsInEntry; i++) {
                output += String.format("%d", getIndexAtEntrySlot(entryIndex, i));
                if (i < numberOfslotsInEntry - 1) {
                    output += ", ";
                }
            }
            output += String.format(" ] (indexShift = %d)\n", indexShift);
            final boolean nextLevelIsLeaf = (indexShift == LEAF_LEVEL_SHIFT);
            for (int i = 0; i < numberOfslotsInEntry; i++) {
                final int nextLevelEntryIndex = getIndexAtEntrySlot(entryIndex, i);
                if (nextLevelIsLeaf) {
                    output += leafEntryToString(nextLevelEntryIndex, indentLevel + 4);
                } else {
                    output += nonLeafEntryToString(nextLevelEntryIndex,
                            indexShift - 4, indentLevel + 4);
                }
            }
        } catch (Exception ex) {
            output += String.format("Exception thrown at nonLeafEnty at index %d with indexShift %d\n",
                    entryIndex, indexShift);
        }
        return output;
    }

    private String recordedValuesToString() {
        String output = "";
        try {
            for (IterationValue v : nonZeroValues()) {
                output += String.format("[%d] : %d\n", v.getIndex(), v.getValue());
            }
            return output;
        } catch (Exception ex) {
            output += "!!! Exception thown in value iteration...\n";
        }
        return output;
    }

    private int seekToPopulatedVirtualIndexStartingAtLevel(final int startingVirtualIndex,
                                                           final int levelEntryIndex,
                                                           final int indexShift) throws RetryException {
        int virtualIndex = startingVirtualIndex;
        int firstVirtualIndexPastThisLevel = (((virtualIndex >>> indexShift) | 0xf) + 1) << indexShift;
        boolean nextLevelIsLeaf = (indexShift == LEAF_LEVEL_SHIFT);
        do {
            // Target is a packedSlotIndicators entry
            int packedSlotIndicators = getPackedSlotIndicators(levelEntryIndex);
            int startingSlotBitNumber = (virtualIndex >>> indexShift) & 0xf;
            int slotMask = 1 << startingSlotBitNumber;
            int slotsAtAndAboveBitNumber = packedSlotIndicators & ~(slotMask - 1);
            int nextActiveSlotBitNumber = Integer.numberOfTrailingZeros(slotsAtAndAboveBitNumber);


            if (nextActiveSlotBitNumber > 15) {
                // this level has no more set bits, pop back up a level.
                int indexShiftAbove = indexShift + 4;
                virtualIndex += 1 << indexShiftAbove;
                virtualIndex &= ~((1 << indexShiftAbove) - 1); // Start at the beginning of the next slot a level above.
                return -virtualIndex; // Negative value indicates a skip to a different index.
            }

            // Drill into bit.
            if (nextActiveSlotBitNumber != startingSlotBitNumber) {
                virtualIndex += (nextActiveSlotBitNumber - startingSlotBitNumber) << indexShift;
                virtualIndex &= ~((1 << indexShift) - 1); // Start at the beginning of the next slot of this level
            }

            if (nextLevelIsLeaf) {
                // There is recorded value here. No need to look.
                return virtualIndex;
            }

            // Next level is not a leaf. Drill into it:

            int nextSlotMask = 1 << nextActiveSlotBitNumber;
            int slotsBelowNextBitNumber = packedSlotIndicators & (nextSlotMask - 1);
            int nextSlotNumber = Integer.bitCount(slotsBelowNextBitNumber);

            if ((packedSlotIndicators & nextSlotMask) == 0) {
                throw new IllegalStateException("Unexpected 0 at slot index");
            }

            int entryPointerIndex = levelEntryIndex + NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS + nextSlotNumber;
            int nextLevelEntryIndex = getIndexAtShortIndex(entryPointerIndex);
            if (nextLevelEntryIndex == 0) {
                throw new RetryException();
            }
            if (getPreviousVersionIndex(nextLevelEntryIndex) != 0) {
                consolidateEntry(nextLevelEntryIndex);
            }

            virtualIndex = seekToPopulatedVirtualIndexStartingAtLevel(virtualIndex, nextLevelEntryIndex, indexShift - 4);
            if (virtualIndex < 0) {
                virtualIndex = -virtualIndex;
            } else {
                return virtualIndex;
            }
        } while (virtualIndex < firstVirtualIndexPastThisLevel);

        return virtualIndex;
    }

    private void setIndexAtEntrySlot(final int entryIndex, final int slot, final short newIndexValue) {
        setAtShortIndex(entryIndex + NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS + slot, newIndexValue);
    }

    private void setPackedSlotIndicators(final int entryIndex, final short newPackedSlotIndicators) {
        setAtShortIndex(entryIndex + NON_LEAF_ENTRY_SLOT_INDICATORS_OFFSET, newPackedSlotIndicators);
    }

    private void setPreviousVersionIndex(final int entryIndex, final short newPreviosVersionIndex) {
        setAtShortIndex(entryIndex + NON_LEAF_ENTRY_PREVIOUS_VERSION_OFFSET, newPreviosVersionIndex);
    }

    private void setTopLevelShift(final int topLevelShift) {
        this.topLevelShift = topLevelShift;
    }

    //
    //  ######## ##    ## ######## ########  ##    ##    ######## #### ######## ##       ########   ######
    //  ##       ###   ##    ##    ##     ##  ##  ##     ##        ##  ##       ##       ##     ## ##    ##
    //  ##       ####  ##    ##    ##     ##   ####      ##        ##  ##       ##       ##     ## ##
    //  ######   ## ## ##    ##    ########     ##       ######    ##  ######   ##       ##     ##  ######
    //  ##       ##  ####    ##    ##   ##      ##       ##        ##  ##       ##       ##     ##       ##
    //  ##       ##   ###    ##    ##    ##     ##       ##        ##  ##       ##       ##     ## ##    ##
    //  ######## ##    ##    ##    ##     ##    ##       ##       #### ######## ######## ########   ######
    //

    abstract long addAndGetAtUnpackedIndex(int index, long valueToAdd);

    /**
     * add a byte value to a current byte value in the array
     *
     * @param byteIndex  index of byte value to add to
     * @param valueToAdd byte value to add
     * @return the afterAddValue. ((afterAddValue & 0x100) != 0) indicates a carry.
     */
    long addAtByteIndex(final int byteIndex, final byte valueToAdd) {
        int longIndex = byteIndex >> 3;
        int byteShift = (byteIndex & 0x7) << 3;
        long byteMask = ((long) 0xff) << byteShift;
        boolean success;
        long newValue;
        do {
            long currentLongValue = getAtLongIndex(longIndex);
            long byteValueAsLong = (currentLongValue >> byteShift) & 0xff;
            newValue = byteValueAsLong + (((long) valueToAdd) & 0xff);
            long newByteValueAsLong = newValue & 0xff;
            long newLongValue = (currentLongValue & ~byteMask) | (newByteValueAsLong << byteShift);
            success = casAtLongIndex(longIndex, currentLongValue, newLongValue);
        }
        while (!success);
        return newValue;
    }

    abstract boolean casAtLongIndex(int longIndex, long expectedValue, long newValue);

    boolean casAtShortIndex(final int shortIndex, final short expectedValue, final short newValue) {
        int longIndex = shortIndex >> 2;
        int shortShift = (shortIndex & 0x3) << 4;
        long shortMask = ~(((long) 0xffff) << shortShift);
        long newShortValueAsLong = ((long) newValue) & 0xffff;
        long expectedShortValueAsLong = ((long) expectedValue) & 0xffff;
        boolean success;
        do {
            long currentLongValue = getAtLongIndex(longIndex);
            long currentShortValueAsLong = (currentLongValue >> shortShift) & 0xffff;
            if (currentShortValueAsLong != expectedShortValueAsLong) {
                return false;
            }
            long newLongValue = (currentLongValue & shortMask) | (newShortValueAsLong << shortShift);
            success = casAtLongIndex(longIndex, currentLongValue, newLongValue);
        }
        while (!success);
        return true;
    }

    abstract boolean casPopulatedLongLength(int expectedPopulatedShortLength, int newPopulatedShortLength);

    abstract boolean casPopulatedShortLength(int expectedPopulatedShortLength, int newPopulatedShortLength);

    abstract void clearContents();

    int determineTopLevelShiftForVirtualLength(final int virtualLength) {
        int sizeMagnitude = (int) Math.ceil(Math.log(virtualLength) / Math.log(2));
        int eightsSizeMagnitude = sizeMagnitude - 3;
        int multipleOfFourSizeMagnitude = (int) Math.ceil(eightsSizeMagnitude / 4.0) * 4;
        multipleOfFourSizeMagnitude = Math.max(multipleOfFourSizeMagnitude, 8);
        return (multipleOfFourSizeMagnitude - 4) + 3;
    }

    //
    //  ######## ##    ## ######## ########  ##    ##     #######  ########   ######
    //  ##       ###   ##    ##    ##     ##  ##  ##     ##     ## ##     ## ##    ##
    //  ##       ####  ##    ##    ##     ##   ####      ##     ## ##     ## ##
    //  ######   ## ## ##    ##    ########     ##       ##     ## ########   ######
    //  ##       ##  ####    ##    ##   ##      ##       ##     ## ##              ##
    //  ##       ##   ###    ##    ##    ##     ##       ##     ## ##        ##    ##
    //  ######## ##    ##    ##    ##     ##    ##        #######  ##         ######
    //

    byte getAtByteIndex(final int byteIndex) {
        return (byte) ((getAtLongIndex(byteIndex >> 3) >> ((byteIndex & 0x7) << 3)) & 0xff);
    }

    abstract long getAtLongIndex(int longIndex);

    short getAtShortIndex(final int shortIndex) {
        return (short) ((getAtLongIndex(shortIndex >> 2) >> ((shortIndex & 0x3) << 4)) & 0xffff);
    }

    abstract long getAtUnpackedIndex(int index);

    short getIndexAtShortIndex(final int shortIndex) {
        return (short) ((getAtLongIndex(shortIndex >> 2) >> ((shortIndex & 0x3) << 4)) & 0x7fff);
    }


    //
    //   ######   ######## ########    ##     ##    ###    ##             ## #### ##    ## ########  ######## ##     ##
    //  ##    ##  ##          ##       ##     ##   ## ##   ##            ##   ##  ###   ## ##     ## ##        ##   ##
    //  ##        ##          ##       ##     ##  ##   ##  ##           ##    ##  ####  ## ##     ## ##         ## ##
    //  ##   #### ######      ##       ##     ## ##     ## ##          ##     ##  ## ## ## ##     ## ######      ###
    //  ##    ##  ##          ##        ##   ##  ######### ##         ##      ##  ##  #### ##     ## ##         ## ##
    //  ##    ##  ##          ##         ## ##   ##     ## ##        ##       ##  ##   ### ##     ## ##        ##   ##
    //   ######   ########    ##          ###    ##     ## ######## ##       #### ##    ## ########  ######## ##     ##
    //

    /**
     * Get the byte-index (into the packed array) corresponding to a given (set tree) value byte of given virtual index.
     * Inserts new set tree nodes as needed if indicated.
     *
     * @param setNumber      The set tree number (0-7, 0 corresponding with the LSByte set tree)
     * @param virtualIndex   The virtual index into the PackedArray
     * @param insertAsNeeded If true, will insert new set tree nodes as needed if they do not already exist
     * @return the byte-index corresponding to the given (set tree) value byte of the given virtual index
     */
    int getPackedIndex(final int setNumber, final int virtualIndex, final boolean insertAsNeeded)
            throws ResizeException {
        int byteIndex = 0; // Must be overwritten to finish. Will retry until non-zero.
        do {
            try {
                assert (setNumber >= 0 && setNumber < NUMBER_OF_SETS);
                if (virtualIndex >= getVirtualLength()) {
                    throw CairoException.nonCritical().put("Attempting access at index ").put(virtualIndex)
                            .put(", beyond virtualLength ").put(getVirtualLength());
                }
                int entryPointerIndex = SET_0_START_INDEX + setNumber;
                int entryIndex = getRootEntry(setNumber, insertAsNeeded);
                if (entryIndex == 0) {
                    return -1; // Index does not currently exist in packed array;
                }

                // Work down the levels of non-leaf entries:
                for (int indexShift = getTopLevelShift(); indexShift >= LEAF_LEVEL_SHIFT; indexShift -= 4) {
                    boolean nextLevelIsLeaf = (indexShift == LEAF_LEVEL_SHIFT);
                    // Target is a packedSlotIndicators entry
                    int packedSlotIndicators = getPackedSlotIndicators(entryIndex);
                    int slotBitNumber = (virtualIndex >>> indexShift) & 0xf;
                    int slotMask = 1 << slotBitNumber;
                    int slotsBelowBitNumber = packedSlotIndicators & (slotMask - 1);
                    int slotNumber = Integer.bitCount(slotsBelowBitNumber);

                    if ((packedSlotIndicators & slotMask) == 0) {
                        // The entryIndex slot does not have the contents we want
                        if (!insertAsNeeded) {
                            return -1; // Index does not currently exist in packed array;
                        }

                        // Expand the entry, adding the index to new entry at the proper slot:
                        entryIndex = expandEntry(entryIndex, entryPointerIndex, slotNumber, slotMask, nextLevelIsLeaf);
                    }

                    // Next level's entry pointer index is in the appropriate slot in in the entries array in this entry:
                    entryPointerIndex = entryIndex + NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS + slotNumber;

                    entryIndex = getIndexAtShortIndex(entryPointerIndex);
                    if (entryIndex == 0) {
                        throw new RetryException();
                    }
                    if ((!nextLevelIsLeaf) && getPreviousVersionIndex(entryIndex) != 0) {
                        consolidateEntry(entryIndex);
                    }

                    // entryIndex is either holds the long-index of a leaf entry, or the shorty-index of the next
                    // level entry's packed slot indicators short-word.
                }

                // entryIndex is the long-index of a leaf entry that contains the value byte for the given set

                byteIndex = (entryIndex << 3) + (virtualIndex & 0x7); // Determine byte index offset within leaf entry

            } catch (RetryException ignored) {
                // Retry will happen automatically since byteIndex was not set to non-zero value;
            }
        }
        while (byteIndex == 0);

        return byteIndex;
    }

    int getPhysicalLength() {
        return physicalLength;
    }

    int getPopulatedByteLength() {
        return getPopulatedShortLength() << 1;
    }

    int getPopulatedLongLength() {
        return (getPopulatedShortLength() + 3) >> 2; // round up
    }

    //
    //  ##     ##         ########   #######  ########  ##     ## ##          ###    ######## ########
    //   ##   ##          ##     ## ##     ## ##     ## ##     ## ##         ## ##      ##    ##
    //    ## ##           ##     ## ##     ## ##     ## ##     ## ##        ##   ##     ##    ##
    //     ###    ####### ########  ##     ## ########  ##     ## ##       ##     ##    ##    ######
    //    ## ##           ##        ##     ## ##        ##     ## ##       #########    ##    ##
    //   ##   ##          ##        ##     ## ##        ##     ## ##       ##     ##    ##    ##
    //  ##     ##         ##         #######  ##         #######  ######## ##     ##    ##    ########
    //

    abstract int getPopulatedShortLength();

    int getTopLevelShift() {
        return topLevelShift;
    }

    //
    //  #### ######## ######## ########     ###    ######## ####  #######  ##    ##
    //   ##     ##    ##       ##     ##   ## ##      ##     ##  ##     ## ###   ##
    //   ##     ##    ##       ##     ##  ##   ##     ##     ##  ##     ## ####  ##
    //   ##     ##    ######   ########  ##     ##    ##     ##  ##     ## ## ## ##
    //   ##     ##    ##       ##   ##   #########    ##     ##  ##     ## ##  ####
    //   ##     ##    ##       ##    ##  ##     ##    ##     ##  ##     ## ##   ###
    //  ####    ##    ######## ##     ## ##     ##    ##    ####  #######  ##    ##
    //

    // Recorded Value iteration:

    int getVirtualLength() {
        return virtualLength;
    }

    abstract long incrementAndGetAtUnpackedIndex(int index);

    // Recorded values iteration:

    void init(final int virtualLength) {
        if (!isPacked()) {
            // Deal with non-packed context init:
            this.virtualLength = virtualLength;
            return;
        }
        // room for the 8 shorts root indexes:
        do {
        } while (!casPopulatedShortLength(getPopulatedShortLength(), SET_0_START_INDEX + 8));

        // Populate empty root entries, and point to them from the root indexes:
        for (int i = 0; i < NUMBER_OF_SETS; i++) {
            setAtShortIndex(SET_0_START_INDEX + i, (short) 0);
        }
        setVirtualLength(virtualLength);
    }

    boolean isPacked() {
        return isPacked;
    }

    abstract void lazySetAtLongIndex(int longIndex, long newValue);

    //
    //   ######  #### ######## ########      ####        ######  ##     ## #### ######## ########
    //  ##    ##  ##       ##  ##           ##  ##      ##    ## ##     ##  ##  ##          ##
    //  ##        ##      ##   ##            ####       ##       ##     ##  ##  ##          ##
    //   ######   ##     ##    ######       ####         ######  #########  ##  ######      ##
    //        ##  ##    ##     ##          ##  ## ##          ## ##     ##  ##  ##          ##
    //  ##    ##  ##   ##      ##          ##   ##      ##    ## ##     ##  ##  ##          ##
    //   ######  #### ######## ########     ####  ##     ######  ##     ## #### ##          ##
    //

    abstract void lazysetAtUnpackedIndex(int index, long newValue);

    abstract int length();

    /**
     * An Iterator over all non-Zero values in the array
     *
     * @return an Iterator over all non-Zero values in the array
     */
    Iterable<IterationValue> nonZeroValues() {
        return NonZeroValuesIterator::new;
    }

    void populateEquivalentEntriesWithZerosFromOther(final AbstractPackedArrayContext other) {
        if (getVirtualLength() < other.getVirtualLength()) {
            throw new IllegalStateException("Cannot populate array of smaller virtrual length");
        }
        for (int i = 0; i < NUMBER_OF_SETS; i++) {
            int otherEntryIndex = other.getAtShortIndex(SET_0_START_INDEX + i);
            if (otherEntryIndex == 0) continue; // No tree to duplicate
            int entryIndexPointer = SET_0_START_INDEX + i;
            for (i = getTopLevelShift(); i > other.getTopLevelShift(); i -= 4) {
                // for each inserted level:

                // Allocate entry in other:
                int sizeOfEntry = NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS + 1;
                int newEntryIndex = 0;
                do {
                    try {
                        newEntryIndex = newEntry(sizeOfEntry);
                    } catch (ResizeException ex) {
                        resizeArray(ex.getNewSize());
                    }
                }
                while (newEntryIndex == 0);

                // Link new level in.
                setAtShortIndex(entryIndexPointer, (short) newEntryIndex);
                // Populate new level entry, use pointer to slot 0 as place to populate under:
                setPackedSlotIndicators(newEntryIndex, (short) 0x1); // Slot 0 populated
                setPreviousVersionIndex(newEntryIndex, (short) 0); // No previous version
                entryIndexPointer = newEntryIndex + NON_LEAF_ENTRY_HEADER_SIZE_IN_SHORTS; // Where the slot 0 index goes.
            }
            copyEntriesAtLevelFromOther(other, otherEntryIndex,
                    entryIndexPointer, other.getTopLevelShift());
        }
    }

    abstract void resizeArray(int newLength);

    void setAtByteIndex(final int byteIndex, final byte value) {
        int longIndex = byteIndex >> 3;
        int byteShift = (byteIndex & 0x7) << 3;
        long byteMask = ((long) 0xff) << byteShift;
        long byteValueAsLong = ((long) value) & 0xff;
        setValuePart(longIndex, byteValueAsLong, byteMask, byteShift);
    }

    void setAtShortIndex(final int shortIndex, final short value) {
        int longIndex = shortIndex >> 2;
        int shortShift = (shortIndex & 0x3) << 4;
        long shortMask = ((long) 0xffff) << shortShift;
        long shortValueAsLong = ((long) value) & 0xffff;
        setValuePart(longIndex, shortValueAsLong, shortMask, shortShift);
    }

    abstract void setAtUnpackedIndex(int index, long newValue);

    void setValuePart(final int longIndex,
                      final long valuePartAsLong,
                      final long valuePartMask,
                      final int valuePartShift) {
        boolean success;
        do {
            long currentLongValue = getAtLongIndex(longIndex);
            long newLongValue = (currentLongValue & ~valuePartMask) | (valuePartAsLong << valuePartShift);
            success = casAtLongIndex(longIndex, currentLongValue, newLongValue);
        }
        while (!success);
    }

    //
    //   ########  #######           ######  ######## ########  #### ##    ##  ######
    //      ##    ##     ##         ##    ##    ##    ##     ##  ##  ###   ## ##    ##
    //      ##    ##     ##         ##          ##    ##     ##  ##  ####  ## ##
    //      ##    ##     ## #######  ######     ##    ########   ##  ## ## ## ##   ####
    //      ##    ##     ##               ##    ##    ##   ##    ##  ##  #### ##    ##
    //      ##    ##     ##         ##    ##    ##    ##    ##   ##  ##   ### ##    ##
    //      ##     #######           ######     ##    ##     ## #### ##    ##  ######
    //

    void setVirtualLength(final int virtualLength) {
        if (!isPacked()) {
            throw new IllegalStateException("Should never be adjusting the virtual size of a non-packed context");
        }
        int newTopLevelShift = determineTopLevelShiftForVirtualLength(virtualLength);
        setTopLevelShift(newTopLevelShift);
        this.virtualLength = virtualLength;
    }

    abstract String unpackedToString();

    private static class RetryException extends Exception {
    }

    class NonZeroValues implements Iterable<IterationValue> {
        public Iterator<IterationValue> iterator() {
            return new NonZeroValuesIterator();
        }
    }

    class NonZeroValuesIterator implements Iterator<IterationValue> {

        final IterationValue currentIterationValue = new IterationValue();
        long nextValue;
        int nextVirtrualIndex = 0;

        NonZeroValuesIterator() {
            findFirstNonZeroValueVirtualIndexStartingAt(0);
        }

        @Override
        public boolean hasNext() {
            return ((nextVirtrualIndex >= 0) &&
                    (nextVirtrualIndex < getVirtualLength()));
        }

        @Override
        public IterationValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            currentIterationValue.set(nextVirtrualIndex, nextValue);
            findFirstNonZeroValueVirtualIndexStartingAt(nextVirtrualIndex + 1);
            return currentIterationValue;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void findFirstNonZeroValueVirtualIndexStartingAt(final int startingVirtualIndex) {
            if (!isPacked()) {
                // Look for non-zero value in unpacked context:
                for (nextVirtrualIndex = startingVirtualIndex;
                     nextVirtrualIndex < getVirtualLength();
                     nextVirtrualIndex++) {
                    if ((nextValue = getAtUnpackedIndex(nextVirtrualIndex)) != 0) {
                        return;
                    }
                }
                return;
            }
            // Context is packed:
            nextVirtrualIndex = startingVirtualIndex;
            do {
                nextVirtrualIndex = findFirstPotentiallyPopulatedVirtualIndexStartingAt(nextVirtrualIndex);
                if (nextVirtrualIndex >= getVirtualLength()) break;
                if ((nextValue = contextLocalGetValueAtIndex(nextVirtrualIndex)) != 0) break;
                nextVirtrualIndex++;
            } while (true);
        }
    }
}
