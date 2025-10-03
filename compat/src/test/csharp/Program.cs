using System.Globalization;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using InvalidCastException = System.InvalidCastException;

namespace csharp;

public class Program
{
    public static async Task Main(string[] args)
    {
        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
        if (args.Length != 1)
        {
            Console.WriteLine("Usage: dotnet run <test_file.yaml>");
            Environment.Exit(1);
        }

        var yamlFile = args[0];
        var runner = new TestRunner();
        await runner.RunTests(yamlFile);
    }
}

public class TestRunner
{
    private readonly IDeserializer _deserializer;

    public TestRunner()
    {
        _deserializer = new DeserializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .Build();
    }

    public async Task RunTests(string yamlFile)
    {
        var testData = LoadYaml(yamlFile);
        var globalVariables = testData.Variables ?? new Dictionary<string, object>();

        foreach (var test in testData.Tests)
        {
            var iterations = test.Iterations ?? 50;
            if (test.Exclude.Contains("csharp"))
            {
                Console.WriteLine($"Skipping test '{test.Name}' because it's excluded for C#.");
                continue;
            }
            for (var i = 0; i < iterations; i++)
            {
                Console.WriteLine($"Running test '{test.Name}' (iteration {i + 1})");
                await RunTest(test, globalVariables);
            }
        }
    }

    private TestData LoadYaml(string filePath)
    {
        var yaml = File.ReadAllText(filePath);
        return _deserializer.Deserialize<TestData>(yaml);
    }

    private async Task RunTest(Test test, Dictionary<string, object> globalVariables)
    {
        var variables = new Dictionary<string, object>(globalVariables);
        foreach (var (key, value) in test.Variables ?? new Dictionary<string, object>())
        {
            variables[key] = value;
        }

        var port = int.Parse(Environment.GetEnvironmentVariable("PGPORT") ?? "8812");
        var connectionString = $"Host=localhost;Port={port};Username=admin;Password=quest;Database=qdb;";

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        var testFailed = false;
        try
        {
            // Prepare phase
            await ExecuteSteps(test.Prepare ?? new List<Step>(), variables, connection);

            // Test steps
            await ExecuteSteps(test.Steps, variables, connection);

            Console.WriteLine($"Test '{test.Name}' passed.");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Test '{test.Name}' failed: {e.Message}");
            testFailed = true;
        }
        finally
        {
            try
            {
                // Teardown phase
                await ExecuteSteps(test.Teardown ?? new List<Step>(), variables, connection);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Teardown for test '{test.Name}' failed: {e.Message}");
            }

            if (testFailed)
            {
                Environment.Exit(1);
            }
        }
    }

    private async Task ExecuteSteps(List<Step> steps, Dictionary<string, object> variables, NpgsqlConnection connection)
    {
        foreach (var step in steps)
        {
            if (step.Loop != null)
            {
                await ExecuteLoop(step.Loop, variables, connection);
            }
            else
            {
                await ExecuteStep(step, variables, connection);
            }
        }
    }

    private async Task ExecuteLoop(LoopDefinition loop, Dictionary<string, object> variables,
        NpgsqlConnection connection)
    {
        var loopVariables = new Dictionary<string, object>(variables);

        IEnumerable<object> iterable;
        if (loop.Over != null)
        {
            iterable = loop.Over;
        }
        else if (loop.Range != null)
        {
            iterable = Enumerable.Range(loop.Range.Start, loop.Range.End - loop.Range.Start + 1).Cast<object>();
        }
        else
        {
            throw new ArgumentException("Loop must have 'over' or 'range' defined.");
        }

        foreach (var item in iterable)
        {
            loopVariables[loop.As] = item;
            await ExecuteSteps(loop.Steps, loopVariables, connection);
        }
    }

    private async Task ExecuteStep(Step step, Dictionary<string, object> variables, NpgsqlConnection connection)
    {
        var queryTemplate = step.Query;
        var parameters = step.Parameters ?? new List<Parameter>();
        var expect = step.Expect;

        var substitutedQuery = SubstituteVariables(queryTemplate, variables);
        var query = AdjustPlaceholderSyntax(substitutedQuery);

        var resolvedParameters = ResolveParameters(parameters, variables);
        var result = await ExecuteQuery(connection, query, step.Action, resolvedParameters);

        if (expect != null)
        {
            AssertResult(expect, result);
        }
    }

    private string SubstituteVariables(string template, Dictionary<string, object> variables)
    {
        if (string.IsNullOrEmpty(template)) return template;

        return variables.Aggregate(template, (current, variable) =>
            current.Replace($"${{{variable.Key}}}", variable.Value.ToString() ?? ""));
    }

    private string AdjustPlaceholderSyntax(string query)
    {
        return System.Text.RegularExpressions.Regex.Replace(query, @"\$\[(\d+)\]", "@p$1");
    }

    private async Task<object> ExecuteQuery(NpgsqlConnection connection, string query, Action queryType,
        List<NpgsqlParameter> parameters)
    {
        await using var cmd = new NpgsqlCommand(query, connection);
        cmd.Parameters.AddRange(parameters.ToArray());

        if (queryType == Action.Query)
        {
            var result = new List<OrderedDictionary<string, object>>();
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var row = new OrderedDictionary<string, object>();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    string fieldName = reader.GetName(i);
                    row[fieldName] = reader.GetValue(i);
                }

                result.Add(row);
            }

            return result;
        }

        var rowsAffected = await cmd.ExecuteNonQueryAsync();
        return new List<OrderedDictionary<string, object>>
        {
            new() { { "count", rowsAffected } }
        };
    }

    private List<NpgsqlParameter> ResolveParameters(List<Parameter> parameters, Dictionary<string, object> variables)
    {
        var result = new List<NpgsqlParameter>();
        for (var i = 0; i < parameters.Count; i++)
        {
            var param = parameters[i];
            var value = param.Value;

            if (value is string strValue)
            {
                value = SubstituteVariables(strValue, variables);
            }

            var npgsqlParameter = new NpgsqlParameter($"p{i + 1}", ConvertToNpgsqlType(param.Type))
            {
                Value = ConvertParameterValue(value, param.Type)
            };

            result.Add(npgsqlParameter);
        }

        return result;
    }

    private NpgsqlDbType ConvertToNpgsqlType(string type) => type.ToLower() switch
    {
        "int4" => NpgsqlDbType.Integer,
        "int8" => NpgsqlDbType.Bigint,
        "float4" => NpgsqlDbType.Real,
        "float8" => NpgsqlDbType.Double,
        "boolean" => NpgsqlDbType.Boolean,
        "varchar" => NpgsqlDbType.Varchar,
        "timestamp" => NpgsqlDbType.Timestamp,
        "date" => NpgsqlDbType.Date,
        "char" => NpgsqlDbType.Char,
        "array_float8" => NpgsqlDbType.Double | NpgsqlDbType.Array,
        "numeric" => NpgsqlDbType.Numeric,
        _ => throw new ArgumentException($"Unsupported type: {type}")
    };

    private object ConvertParameterValue(object value, string type)
    {
        return type.ToLower() switch
        {
            "int4" or "int8" => Convert.ToInt64(value, CultureInfo.InvariantCulture),
            "float4" or "float8" => Convert.ToDouble(value.ToString(), CultureInfo.InvariantCulture),
            "boolean" => Convert.ToBoolean(value),
            "varchar" => value.ToString()!,
            "date" => DateTime.Parse(value.ToString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            "char" => Convert.ToChar(value),
            "timestamp" => DateTime.Parse(value.ToString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            "array_float8" => ParseFloatArray(value.ToString()),
            "numeric" => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
            _ => value
        };
    }

    private double?[] ParseFloatArray(string? arrayString)
    {
        if (string.IsNullOrEmpty(arrayString))
            return Array.Empty<double?>();

        // Remove the curly braces and split by comma
        string trimmed = arrayString.Trim().TrimStart('{').TrimEnd('}');

        // Handle empty array case
        if (string.IsNullOrWhiteSpace(trimmed))
            return Array.Empty<double?>();

        // Split the string by commas and convert each element to nullable double
        return trimmed.Split(',')
            .Select(s =>
            {
                string value = s.Trim();
                if (string.Equals(value, "NULL", StringComparison.OrdinalIgnoreCase))
                    return (double?)null;  // Explicit cast to double?
                else
                    return Convert.ToDouble(value, CultureInfo.InvariantCulture);
            })
            .ToArray();
    }

    private void AssertResult(ExpectDefinition expect, object actual)
    {
        var actualConverted = ConvertQueryResult(actual);

        if (expect.Result != null)
        {
            if (expect.Result is List<object> expectedResult)
            {
                if (!DeepEquals(actualConverted, expectedResult))
                {
                    throw new AssertionException(
                        $"Expected result {System.Text.Json.JsonSerializer.Serialize(expectedResult)}, got {System.Text.Json.JsonSerializer.Serialize(actualConverted)}");
                }
            }
            else
            {
                // Single value comparison
                if (!actual.ToString()!.Equals(expect.Result.ToString()))
                {
                    throw new AssertionException($"Expected result '{expect.Result}', got '{actual}'");
                }
            }
        }
        else if (expect.ResultContains != null)
        {
            foreach (var expectedRow in expect.ResultContains)
            {
                var expectedRowList = expectedRow as List<object>;
                if (expectedRowList == null)
                {
                    throw new AssertionException($"Expected row {expectedRow} is not in the correct format");
                }

                if (!actualConverted.Any(row => DeepEquals(row, expectedRowList)))
                {
                    throw new AssertionException(
                        $"Expected row {System.Text.Json.JsonSerializer.Serialize(expectedRow)} not found in actual results {System.Text.Json.JsonSerializer.Serialize(actualConverted)}");
                }
            }
        }
    }

    private List<List<object>> ConvertQueryResult(object result)
    {
        if (result is not List<OrderedDictionary<string, object>> dictList)
            return new List<List<object>>();

        return dictList.Select(dict => dict.Values
                .Select(value => value switch
                {
                    DateTime dt => (object)dt.ToString("yyyy-MM-ddTHH:mm:ss.ffffffZ"),
                    // Convert all numeric types to string and cast back to object
                    long l => l.ToString(CultureInfo.InvariantCulture),
                    double d => d.ToString("0.0########", CultureInfo.InvariantCulture),
                    int i => i.ToString(CultureInfo.InvariantCulture),
                    short s => s.ToString(CultureInfo.InvariantCulture),
                    // Ugly hack to normalize the decimal before printing, so that 1.0 becomes 1.
                    // Source: https://stackoverflow.com/questions/4525854/remove-trailing-zeros-from-system-decimal
                    decimal dec => (dec / 1.000000000000000000000000000000000m).ToString(CultureInfo.InvariantCulture),
                    double[] doubleArray => ArrayToText(doubleArray),
                    double[,] array => ArrayToText(array),
                    _ => value.ToString() ?? ""
                })
                .ToList())
            .ToList();
    }


    // Process any array object into PostgreSQL text format
    private static string ArrayToText(object arrayValue)
    {
        if (arrayValue == null)
        {
            return "NULL";
        }

        var sb = new StringBuilder();
        var arr = (Array)arrayValue;
        ProcessDimension(arr, 0, new int[arr.Rank], sb);
        return sb.ToString();
    }

    // Recursive method to process array dimensions - closely matches your Java implementation
    private static void ProcessDimension(Array array, int dim, int[] indices, StringBuilder sb)
    {
        int count = array.GetLength(dim);
        bool atDeepestDim = (dim == array.Rank - 1);

        // Opening brace
        sb.Append('{');

        for (int i = 0; i < count; i++)
        {
            // Add comma between elements
            if (i > 0)
                sb.Append(',');

            indices[dim] = i;

            if (atDeepestDim)
            {
                // At leaf level, append the actual value
                object? value = array.GetValue(indices);

                if (value == null || value is DBNull)
                {
                    sb.Append("NULL");
                }
                else if (value is double d)
                {
                    // Ensure decimal point for integer values
                    if (Math.Abs(d % 1) < double.Epsilon)
                        sb.Append($"{d:0.0}");
                    else
                        sb.Append(d);
                }
                else
                {
                    sb.Append(value);
                }
            }
            else
            {
                // For nested dimensions, recurse
                ProcessDimension(array, dim + 1, indices, sb);
            }
        }

        // Closing brace
        sb.Append('}');
    }

    private bool DeepEquals(object obj1, object obj2)
    {
        if (obj1 == null && obj2 == null) return true;
        if (obj1 == null || obj2 == null) return false;

        var json1 = System.Text.Json.JsonSerializer.Serialize(obj1);
        var json2 = System.Text.Json.JsonSerializer.Serialize(obj2);
        return json1 == json2;
    }
}

public class TestData
{
    public Dictionary<string, object>? Variables { get; set; }
    public List<Test> Tests { get; set; } = new();
}

public class Test
{
    public string Name { get; set; } = "";
    public Dictionary<string, object>? Variables { get; set; }
    public int? Iterations { get; set; }
    public List<Step>? Prepare { get; set; }
    public List<Step> Steps { get; set; } = new();
    public List<Step>? Teardown { get; set; }
    public string Description { get; set; } = "";
    public List<string> Exclude { get; set; } = new();
}

public enum Action
{
    Query,
    Execute
}

public class Step
{
    public Action Action { get; set; } = Action.Execute;
    public string Query { get; set; } = "";
    public List<Parameter>? Parameters { get; set; }
    public ExpectDefinition? Expect { get; set; }
    public LoopDefinition? Loop { get; set; }
    public string Description { get; set; } = "";
}

public class Parameter
{
    public string Type { get; set; } = "";
    public object Value { get; set; } = null!;
}

public class ExpectDefinition
{
    public object? Result { get; set; }
    [YamlMember(Alias = "result_contains", ApplyNamingConventions = false)]
    public List<object>? ResultContains { get; set; }
}

public class LoopDefinition
{
    public string As { get; set; } = "";
    public List<object>? Over { get; set; }
    public RangeDefinition? Range { get; set; }
    public List<Step> Steps { get; set; } = new();
}

public class RangeDefinition
{
    public int Start { get; set; }
    public int End { get; set; }
}

public class AssertionException : Exception
{
    public AssertionException(string message) : base(message)
    {
    }
}