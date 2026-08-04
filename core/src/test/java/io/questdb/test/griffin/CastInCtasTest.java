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
            execute("CREATE TABLE t AS (SELECT rnd_char() x FROM long_sequence(5)), CAST(x AS string)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nSTRING\n");
        });
    }

    /**
     * Verifies CTAS cast-list support for CHAR to VARCHAR.
     */
    @Test
    public void testCastCharToVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_char() x FROM long_sequence(5)), CAST(x AS varchar)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nVARCHAR\n");
        });
    }

    // --- INT → LONG (existing within-group cast test) ---

    /**
     * Verifies CTAS cast-list support for INT to LONG.
     */
    @Test
    public void testCastIntToLongInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 1 x FROM long_sequence(5)), CAST(x AS long)");
            assertQuery("SELECT x FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1\n1\n1\n1\n1\n");

        });
    }

    // --- LONG → VARCHAR (existing cross-group cast test) ---

    /**
     * Verifies CTAS cast-list support for LONG to STRING.
     */
    @Test
    public void testCastLongToStringInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x FROM long_sequence(5)), CAST(x AS string)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nSTRING\n");
            assertQuery("SELECT x FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1\n2\n3\n4\n5\n");
        });
    }

    /**
     * Verifies CTAS cast-list support for LONG to SYMBOL remains unsupported.
     */
    @Test
    public void testCastLongToSymbolInCtasFails() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "CREATE TABLE t AS (SELECT x FROM long_sequence(5)), CAST(x AS symbol)",
                    57,
                    "unsupported cast [column=x, from=LONG, to=SYMBOL]"
            );
        });
    }

    /**
     * Verifies CTAS cast-list support for LONG to VARCHAR.
     */
    @Test
    public void testCastLongToVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x FROM long_sequence(5)), CAST(x AS varchar)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nVARCHAR\n");
            assertQuery("SELECT x FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1\n2\n3\n4\n5\n");
        });
    }

    // --- SYMBOL → STRING / VARCHAR ---

    /**
     * Verifies CTAS cast-list support for STRING to VARCHAR.
     */
    @Test
    public void testCastStringToVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 'a' x FROM long_sequence(5)), CAST(x AS varchar)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nVARCHAR\n");
        });
    }

    /**
     * Verifies CTAS cast-list support for SYMBOL to STRING.
     */
    @Test
    public void testCastSymbolToStringInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_symbol('a', 'b', 'c') x FROM long_sequence(5)), CAST(x AS string)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nSTRING\n");
        });
    }

    /**
     * Verifies CTAS cast-list support for SYMBOL to VARCHAR.
     */
    @Test
    public void testCastSymbolToVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_symbol('a', 'b', 'c') x FROM long_sequence(5)), CAST(x AS varchar)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nVARCHAR\n");
        });
    }

    // --- UUID → STRING / VARCHAR ---

    /**
     * Verifies CTAS cast-list support for UUID to STRING.
     */
    @Test
    public void testCastUuidToStringInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_uuid4() x FROM long_sequence(5)), CAST(x AS string)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nSTRING\n");
        });
    }

    /**
     * Verifies CTAS cast-list support for UUID to SYMBOL remains unsupported.
     */
    @Test
    public void testCastUuidToSymbolInCtasFails() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "CREATE TABLE t AS (SELECT rnd_uuid4() x FROM long_sequence(5)), CAST(x AS symbol)",
                    69,
                    "unsupported cast [column=x, from=UUID, to=SYMBOL]"
            );
        });
    }

    /**
     * Verifies CTAS cast-list support for UUID to VARCHAR.
     */
    @Test
    public void testCastUuidToVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_uuid4() x FROM long_sequence(5)), CAST(x AS varchar)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nVARCHAR\n");
        });
    }

    /**
     * Verifies CTAS cast-list support for VARCHAR to STRING.
     */
    @Test
    public void testCastVarcharToStringInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT CAST('a' AS varchar) x FROM long_sequence(5)), CAST(x AS string)");
            assertQuery("SELECT typeOf(x) FROM t LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("typeOf\nSTRING\n");
        });
    }
}
