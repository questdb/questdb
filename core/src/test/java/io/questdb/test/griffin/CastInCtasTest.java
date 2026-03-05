package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests for CTAS (CREATE TABLE AS SELECT) explicit cast syntax.
 * Each test verifies that a specific from→to type cast is allowed
 * in the CTAS-specific cast clause: CREATE TABLE t AS (...), cast(col AS type).
 * The isCompatibleCast() gate in CreateTableOperationBuilderImpl allows
 * STRING/VARCHAR casts from LONG, CHAR, SYMBOL, and UUID source types.
 */
public class CastInCtasTest extends AbstractCairoTest {

    // --- CHAR → STRING / VARCHAR ---

    /**
     * Verifies CTAS cast-list support for CHAR to STRING.
     */
    @Test
    public void testCastCharToStringInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_char() x FROM long_sequence(5)), cast(x AS string)");
            assertSql("typeOf\nSTRING\n", "SELECT typeOf(x) FROM t LIMIT 1");
        });
    }

    /**
     * Verifies CTAS cast-list support for CHAR to VARCHAR.
     */
    @Test
    public void testCastCharToVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_char() x FROM long_sequence(5)), cast(x AS varchar)");
            assertSql("typeOf\nVARCHAR\n", "SELECT typeOf(x) FROM t LIMIT 1");
        });
    }

    // --- INT → LONG (existing within-group cast test) ---

    /**
     * Verifies CTAS cast-list support for INT to LONG.
     */
    @Test
    public void testCastIntToLongInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 1 x FROM long_sequence(5)), cast(x AS long)");
            assertSql("x\n1\n1\n1\n1\n1\n", "SELECT x FROM t");
        });
    }

    // --- LONG → VARCHAR (existing cross-group cast test) ---

    /**
     * Verifies CTAS cast-list support for LONG to VARCHAR.
     */
    @Test
    public void testCastLongToVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x FROM long_sequence(5)), cast(x AS varchar)");
            assertSql("typeOf\nVARCHAR\n", "SELECT typeOf(x) FROM t LIMIT 1");
        });
    }

    /**
     * Verifies CTAS cast-list support for LONG to STRING.
     */
    @Test
    public void testCastLongToStringInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x FROM long_sequence(5)), cast(x AS string)");
            assertSql("typeOf\nSTRING\n", "SELECT typeOf(x) FROM t LIMIT 1");
        });
    }

    // --- SYMBOL → STRING / VARCHAR ---

    /**
     * Verifies CTAS cast-list support for SYMBOL to STRING.
     */
    @Test
    public void testCastSymbolToStringInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_symbol('a', 'b', 'c') x FROM long_sequence(5)), cast(x AS string)");
            assertSql("typeOf\nSTRING\n", "SELECT typeOf(x) FROM t LIMIT 1");
        });
    }

    /**
     * Verifies CTAS cast-list support for SYMBOL to VARCHAR.
     */
    @Test
    public void testCastSymbolToVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_symbol('a', 'b', 'c') x FROM long_sequence(5)), cast(x AS varchar)");
            assertSql("typeOf\nVARCHAR\n", "SELECT typeOf(x) FROM t LIMIT 1");
        });
    }

    // --- UUID → STRING / VARCHAR ---

    /**
     * Verifies CTAS cast-list support for UUID to STRING.
     */
    @Test
    public void testCastUuidToStringInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_uuid4() x FROM long_sequence(5)), cast(x AS string)");
            assertSql("typeOf\nSTRING\n", "SELECT typeOf(x) FROM t LIMIT 1");
        });
    }

    /**
     * Verifies CTAS cast-list support for UUID to VARCHAR.
     */
    @Test
    public void testCastUuidToVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_uuid4() x FROM long_sequence(5)), cast(x AS varchar)");
            assertSql("typeOf\nVARCHAR\n", "SELECT typeOf(x) FROM t LIMIT 1");
        });
    }
}
