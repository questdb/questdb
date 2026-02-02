const postgres = require('postgres');

/**
 * Node.js/postgres-specific tests that cannot be expressed in the generic YAML test format.
 * These tests use postgres.js-specific features (e.g., cursors, template literals) that
 * are not available in other drivers or cannot be triggered via the generic runner.
 */

async function runAll() {
    await checkCursorIterationDoesNotThrowSpuriousExecutionMessage();
}

// Regression test for cursor iteration with multiple batches.
// When iterating over a cursor, the first batch succeeds but subsequent batches
// throw a "spurious execution message" error.
async function checkCursorIterationDoesNotThrowSpuriousExecutionMessage() {
    const testName = 'checkCursorIterationDoesNotThrowSpuriousExecutionMessage';
    console.log(`Running test '${testName}'`);

    const port = process.env.PGPORT || 8812;
    const tableName = 'cursor_test_table';
    const items = ['i1', 'i2', 'i3'];

    const sql = postgres({
        host: 'localhost',
        port: port,
        database: 'qdb',
        username: 'admin',
        password: 'quest'
    });

    try {
        // Create and populate test table with columns matching the items array
        await sql.unsafe(`DROP TABLE IF EXISTS ${tableName}`);
        await sql.unsafe(`CREATE TABLE ${tableName} (timestamp TIMESTAMP, i1 LONG, i2 LONG, i3 LONG)`);

        // Insert enough rows to require multiple cursor batches
        for (let i = 0; i < 25; i++) {
            await sql.unsafe(
                `INSERT INTO ${tableName} VALUES (dateadd('s', ${i}, now()), ${i}, ${i * 10}, ${i * 100})`
            );
        }

        // Use cursor with batch size of 10 to force multiple fetches
        const cursor = sql`
            SELECT ${sql(items)}
            FROM ${sql(tableName)}
            ORDER BY timestamp;
        `.cursor(10);

        let batchCount = 0;
        for await (const rows of cursor) {
            batchCount++;
            console.log(`Batch ${batchCount}: ${rows.length} rows`);
        }

        if (batchCount === 0) {
            throw new Error('No batches returned from cursor');
        }

        console.log(`Test '${testName}' passed.`);
    } catch (error) {
        console.log(`Test '${testName}' failed: ${error.message}`);
        process.exit(1);
    } finally {
        // Cleanup
        try {
            await sql.unsafe(`DROP TABLE IF EXISTS ${tableName}`);
        } catch (e) {
            // Ignore cleanup errors
        }
        await sql.end();
    }
}

// Main execution
if (require.main === module) {
    runAll().catch(error => {
        console.error('Error:', error);
        process.exit(1);
    });
}

module.exports = { runAll };
