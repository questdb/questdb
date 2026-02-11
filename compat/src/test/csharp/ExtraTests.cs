using Npgsql;

namespace csharp;

/// <summary>
/// C#/Npgsql-specific tests that cannot be expressed in the generic YAML test format.
/// These tests use Npgsql-specific features (e.g., NpgsqlBatch for pipelining) that
/// are not available in other drivers or cannot be triggered via the generic runner.
/// </summary>
public static class ExtraTests
{
    public static async Task RunAll()
    {
        await CheckBindVarsInBatchedQueriesAreConsistent();
        await CheckNpgsqlMultiUrlWorks();
        await CheckPreparedBatchWithSameSqlReturnsCorrectResults();
    }

    // Regression test for https://github.com/questdb/questdb/issues/6123
    // When executing a batch of SELECT statements with different bound parameter values,
    // each query should return results for its own binding, not the last one.
    //
    // NpgsqlBatch sends multiple Bind/Execute messages before a single Sync (pipelining),
    // which triggers the bug where all queries share the same BindVariableService and
    // later bind messages overwrite earlier values. Without the fix, all results would
    // have BoundIndex equal to the last value (batchSize-1) instead of matching UnboundIndex.
    private static async Task CheckBindVarsInBatchedQueriesAreConsistent()
    {
        const string testName = "CheckBindVarsInBatchedQueriesAreConsistent";
        Console.WriteLine($"Running test '{testName}'");

        try
        {
            var port = int.Parse(Environment.GetEnvironmentVariable("PGPORT") ?? "8812");
            await using var dataSource = new NpgsqlDataSourceBuilder(
                $"Host=localhost;Port={port};Username=admin;Password=quest;Database=qdb;ServerCompatibilityMode=NoTypeLoading;"
            ).Build();

            const int batchSize = 5;
            await using var connection = await dataSource.OpenConnectionAsync();

            await using var batch = new NpgsqlBatch(connection);
            for (var i = 0; i < batchSize; i++)
            {
                var cmd = new NpgsqlBatchCommand($"SELECT {i} AS UnboundIndex, @index AS BoundIndex");
                cmd.Parameters.AddWithValue("index", i);
                batch.BatchCommands.Add(cmd);
            }

            await using var reader = await batch.ExecuteReaderAsync();

            var first = true;
            for (var i = 0; i < batch.BatchCommands.Count; i++)
            {
                if (!first)
                {
                    await reader.NextResultAsync();
                }
                first = false;

                if (!await reader.ReadAsync())
                {
                    throw new InvalidOperationException("No rows returned");
                }

                var unboundIndex = reader.GetInt32(0);
                var boundIndex = reader.GetInt32(1);
                if (unboundIndex != boundIndex)
                {
                    throw new InvalidOperationException(
                        $"Invalid index: {boundIndex}, expected: {unboundIndex}"
                    );
                }
            }

            Console.WriteLine($"Test '{testName}' passed.");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Test '{testName}' failed: {e.Message}");
            Environment.Exit(1);
        }
    }

    private static async Task CheckNpgsqlMultiUrlWorks()
    {
        const string testName = "CheckNpgsqlMultiUrlWorks";
        Console.WriteLine($"Running test '{testName}'");

        try
        {
            var port = int.Parse(Environment.GetEnvironmentVariable("PGPORT") ?? "8812");
            await using var dataSource = new NpgsqlDataSourceBuilder(
                $"Host=localhost,127.0.0.1;Port={port};Username=admin;Password=quest;Database=qdb;ServerCompatibilityMode=NoTypeLoading;"
            ).Build();

            await using var connection = await dataSource.OpenConnectionAsync();

            Console.WriteLine($"Test '{testName}' passed.");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Test '{testName}' failed: {e.Message}");
            Environment.Exit(1);
        }
    }

    // Regression test for https://github.com/questdb/questdb/issues/6703.
    private static async Task CheckPreparedBatchWithSameSqlReturnsCorrectResults()
    {
        const string testName = "CheckPreparedBatchWithSameSqlReturnsCorrectResults";
        Console.WriteLine($"Running test '{testName}'");

        try
        {
            var port = int.Parse(Environment.GetEnvironmentVariable("PGPORT") ?? "8812");
            await using var dataSource = new NpgsqlDataSourceBuilder(
                $"Host=localhost;Port={port};Username=admin;Password=quest;Database=qdb;ServerCompatibilityMode=NoTypeLoading;"
            ).Build();

            await using var connection = await dataSource.OpenConnectionAsync();
            await using var batch = new NpgsqlBatch(connection);

            var time1 = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            var time2 = new DateTime(2024, 6, 20, 14, 45, 0, DateTimeKind.Utc);

            var cmd1 = new NpgsqlBatchCommand("SELECT @time;");
            cmd1.Parameters.AddWithValue("time", time1);
            batch.BatchCommands.Add(cmd1);

            var cmd2 = new NpgsqlBatchCommand("SELECT @time;");
            cmd2.Parameters.AddWithValue("time", time2);
            batch.BatchCommands.Add(cmd2);

            await batch.PrepareAsync();

            await using var reader = await batch.ExecuteReaderAsync();

            if (!await reader.ReadAsync())
            {
                throw new InvalidOperationException("No rows returned for first query");
            }
            var result1 = reader.GetDateTime(0);
            if (result1 != time1)
            {
                throw new InvalidOperationException($"First query returned {result1}, expected {time1}");
            }

            await reader.NextResultAsync();
            if (!await reader.ReadAsync())
            {
                throw new InvalidOperationException("No rows returned for second query");
            }
            var result2 = reader.GetDateTime(0);
            if (result2 != time2)
            {
                throw new InvalidOperationException($"Second query returned {result2}, expected {time2}");
            }

            Console.WriteLine($"Test '{testName}' passed.");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Test '{testName}' failed: {e.Message}");
            Environment.Exit(1);
        }
    }
}