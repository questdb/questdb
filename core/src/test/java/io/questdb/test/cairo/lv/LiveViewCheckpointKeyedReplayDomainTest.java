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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointKeyedReplay;
import io.questdb.cairo.lv.LiveViewSymbolIdRegistry;
import io.questdb.cairo.lv.LiveViewSymbolIdSource;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * {@link LiveViewCheckpointKeyedReplay#arm} against the two checkpoint key domains it must
 * tell apart: today's resolved-STRING one, which every existing view keys through and which
 * {@link LiveViewCheckpointKeyedReplayTest} already covers end to end, and the translated
 * SYMBOL-plus-dictionary one no live view can produce yet, because a term only keys through
 * it once a translator is bound into the classifier. These tests drive the second domain
 * directly against a real {@link LiveViewSymbolIdRegistry} - the same fixture shape
 * {@link LiveViewSymbolIdRegistryTest} uses - the way the earlier steps of this optimization
 * tested a mechanism nothing end to end could reach yet.
 */
public class LiveViewCheckpointKeyedReplayDomainTest extends AbstractCairoTest {

    private static final int BASE_TABLE_ID = 11;
    private static final int SCAN_COLUMN = 0;
    private static final ColumnTypes STRING_KEY_TYPES = new ArrayColumnTypes().add(ColumnType.STRING);
    private static final ColumnTypes SYMBOL_KEY_TYPES = new ArrayColumnTypes().add(ColumnType.SYMBOL);
    private static final int SLOT = 4;
    private static final int WRITER_COLUMN = 0;

    @Test
    public void testStringDomainStillNeedsNoDictionary() {
        // The resolved-STRING domain is what every view keys through today, and it must
        // keep working with no registry bound at all - arm() only reaches for one once the
        // checkpoint key type says SYMBOL.
        try (LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()) {
            final Dictionary base = new Dictionary("acct-1", "acct-2");
            final CharSequenceHashSet keys = new CharSequenceHashSet();
            keys.add("acct-1");
            Assert.assertTrue(replay.arm(SCAN_COLUMN, base, STRING_KEY_TYPES, null, -1, keys, false));
            Assert.assertTrue(replay.isArmed());
            Assert.assertTrue(replay.getOutputKeys().contains(stringKey("acct-1")));
        }
    }

    @Test
    public void testTranslatedDomainEncodesTheDictionaryIdRatherThanTheString() {
        try (
                LiveViewSymbolIdRegistry registry = registry();
                LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()
        ) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary base = new Dictionary("acct-1", "acct-2");
            registry.armFor(source(base));
            final int idOne = registry.translate(SLOT, 0);
            final int idTwo = registry.translate(SLOT, 1);
            Assert.assertNotEquals(idOne, idTwo);
            registry.disarm();

            final CharSequenceHashSet keys = new CharSequenceHashSet();
            keys.add("acct-1");
            keys.add("acct-2");
            Assert.assertTrue(replay.arm(SCAN_COLUMN, base, SYMBOL_KEY_TYPES, registry, SLOT, keys, false));
            Assert.assertTrue(replay.isArmed());
            Assert.assertEquals(2, replay.getBaseSymbolKeys().size());
            Assert.assertTrue(replay.getOutputKeys().contains(intKey(idOne)));
            Assert.assertTrue(replay.getOutputKeys().contains(intKey(idTwo)));
            // The resolved string never appears in the translated domain - the whole point
            // of translating is that a checkpoint root no longer carries it.
            Assert.assertFalse(replay.getOutputKeys().contains(stringKey("acct-1")));
        }
    }

    @Test
    public void testTranslatedDomainEncodesTheNullKeyAsValueIsNull() {
        try (
                LiveViewSymbolIdRegistry registry = registry();
                LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()
        ) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary base = new Dictionary("acct-1");
            // No non-null keys: the null key alone must still arm and resolve to
            // VALUE_IS_NULL without touching the dictionary at all.
            final CharSequenceHashSet keys = new CharSequenceHashSet();
            Assert.assertTrue(replay.arm(SCAN_COLUMN, base, SYMBOL_KEY_TYPES, registry, SLOT, keys, true));
            Assert.assertTrue(replay.getOutputKeys().contains(intKey(SymbolTable.VALUE_IS_NULL)));
            Assert.assertEquals(0, registry.getDictionarySize(SLOT));
        }
    }

    @Test
    public void testTranslatedDomainRefusesAKeyTheDictionaryNeverInterned() {
        // A repair must refuse a key its dictionary has never seen rather than mint an id
        // for it, because a minted id would key a repair through an entry no published map
        // or root could hold.
        try (
                LiveViewSymbolIdRegistry registry = registry();
                LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()
        ) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary base = new Dictionary("acct-1", "acct-2");
            registry.armFor(source(base));
            registry.translate(SLOT, 0);
            registry.disarm();
            final long dictionarySizeBefore = registry.getDictionarySize(SLOT);

            final CharSequenceHashSet keys = new CharSequenceHashSet();
            // acct-1 interned, acct-2 never resolved through the registry.
            keys.add("acct-1");
            keys.add("acct-2");
            Assert.assertFalse(replay.arm(SCAN_COLUMN, base, SYMBOL_KEY_TYPES, registry, SLOT, keys, false));
            Assert.assertFalse(replay.isArmed());
            Assert.assertEquals(0, replay.getOutputKeys().size());
            Assert.assertEquals(dictionarySizeBefore, registry.getDictionarySize(SLOT));
        }
    }

    @Test
    public void testTranslatedDomainRequiresABoundDictionary() {
        try (LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()) {
            final Dictionary base = new Dictionary("acct-1");
            final CharSequenceHashSet keys = new CharSequenceHashSet();
            keys.add("acct-1");
            // No registry at all.
            Assert.assertFalse(replay.arm(SCAN_COLUMN, base, SYMBOL_KEY_TYPES, null, SLOT, keys, false));
            Assert.assertFalse(replay.isArmed());

            try (LiveViewSymbolIdRegistry registry = registry()) {
                // A registry that exists but never bound this slot.
                Assert.assertFalse(replay.arm(SCAN_COLUMN, base, SYMBOL_KEY_TYPES, registry, SLOT, keys, false));
                Assert.assertFalse(replay.isArmed());
            }
        }
    }

    // One INT partition-key column, as LiveViewSnapshotKeyCodec writes a SYMBOL slot: a
    // plain little-endian four-byte int, id or VALUE_IS_NULL alike.
    private static byte[] intKey(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static LiveViewSymbolIdRegistry registry() {
        return new LiveViewSymbolIdRegistry(new TableToken("lv", "lv~1", null, 1, true, false, false));
    }

    private static LiveViewSymbolIdSource source(Dictionary dictionary) {
        return (registry, slot, scan, writer) -> registry.armStatic(slot, dictionary.size(), dictionary);
    }

    // One STRING partition-key column, as LiveViewSnapshotKeyCodec writes it: a four-byte
    // character count then two bytes per character.
    private static byte[] stringKey(String value) {
        final ByteBuffer key = ByteBuffer.allocate(Integer.BYTES + value.length() * Character.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        key.putInt(value.length());
        for (int i = 0; i < value.length(); i++) {
            key.putChar(value.charAt(i));
        }
        return key.array();
    }

    /**
     * One column's symbol space, in the shape both {@link LiveViewSymbolIdRegistry} and
     * {@link LiveViewCheckpointKeyedReplay#arm} consume it: an id-to-string resolver with a
     * count, standing in for a pinned reader's symbol map.
     */
    private static final class Dictionary implements StaticSymbolTable, SymbolTableSource {
        private final ObjList<String> values = new ObjList<>();

        Dictionary(String... values) {
            for (String value : values) {
                this.values.add(value);
            }
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return values.size();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return this;
        }

        @Override
        public int keyOf(CharSequence value) {
            final int index = values.indexOf(value);
            return index < 0 ? SymbolTable.VALUE_NOT_FOUND : index;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return this;
        }

        int size() {
            return values.size();
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key >= 0 && key < values.size() ? values.getQuick(key) : null;
        }
    }
}
