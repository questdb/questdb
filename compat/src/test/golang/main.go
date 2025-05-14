package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

func init() {
	// Force UTC timezone globally
	time.Local = time.UTC
}

type TestRunner struct{}

// Parameter represents a typed parameter for SQL queries
type Parameter struct {
	Type  string      `yaml:"type"`
	Value interface{} `yaml:"value"`
}

// ExpectClause defines what results to expect from a query
type ExpectClause struct {
	Result         interface{}     `yaml:"result"`
	ResultContains [][]interface{} `yaml:"result_contains"`
}

// Loop represents a loop construct in the test definition
type Loop struct {
	Over  []interface{} `yaml:"over"`
	Range struct {
		Start int `yaml:"start"`
		End   int `yaml:"end"`
	} `yaml:"range"`
	As    string `yaml:"as"`
	Steps []Step `yaml:"steps"`
}

// Step represents a test step with a query and expected result
type Step struct {
	Query      string       `yaml:"query"`
	Parameters []Parameter  `yaml:"parameters"`
	Expect     ExpectClause `yaml:"expect"`
	Loop       *Loop        `yaml:"loop"`
}

// Test represents a complete test case
type Test struct {
	Name       string                 `yaml:"name"`
	Variables  map[string]interface{} `yaml:"variables"`
	Prepare    []Step                 `yaml:"prepare"`
	Steps      []Step                 `yaml:"steps"`
	Teardown   []Step                 `yaml:"teardown"`
	Iterations int                    `yaml:"iterations"`
}

// TestConfig represents the complete test configuration
type TestConfig struct {
	Variables map[string]interface{} `yaml:"variables"`
	Tests     []Test                 `yaml:"tests"`
}

// substituteVariables replaces variable placeholders in a string
func (tr *TestRunner) substituteVariables(text string, variables map[string]interface{}) string {
	if text == "" {
		return ""
	}

	re := regexp.MustCompile(`\${(\w+)}`)
	return re.ReplaceAllStringFunc(text, func(match string) string {
		key := match[2 : len(match)-1]
		if val, ok := variables[key]; ok {
			return fmt.Sprintf("%v", val)
		}
		return match
	})
}

// adjustPlaceholderSyntax converts $[n] to $n for pgx
func (tr *TestRunner) adjustPlaceholderSyntax(query string) string {
	re := regexp.MustCompile(`\$\[(\d+)\]`)
	return re.ReplaceAllString(query, "$$$1")
}

// executeQuery executes a SQL query and returns the result
func (tr *TestRunner) executeQuery(ctx context.Context, conn *pgx.Conn, query string, parameters []interface{}) (interface{}, error) {
	rows, err := conn.Query(ctx, query, parameters...)
	if err != nil {
		return err.Error(), nil
	}
	defer rows.Close()

	var result [][]interface{}

	// Process query results
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err.Error(), nil
		}
		result = append(result, values)
	}

	if err = rows.Err(); err != nil {
		return err.Error(), nil
	}

	// For non-SELECT queries, return row count
	if len(result) == 0 {
		commandTag := rows.CommandTag()
		return [][]interface{}{{commandTag.RowsAffected()}}, nil
	}

	return result, nil
}

// resolveParameters converts and types parameters for query execution
func (tr *TestRunner) resolveParameters(typedParameters []Parameter, variables map[string]interface{}) ([]interface{}, error) {
	resolvedParameters := make([]interface{}, 0, len(typedParameters))

	for _, param := range typedParameters {
		paramType := strings.ToLower(param.Type)
		value := param.Value

		// Substitute variables in string parameters
		if strValue, ok := value.(string); ok {
			value = tr.substituteVariables(strValue, variables)
		}

		switch paramType {
		case "int4", "int8":
			switch v := value.(type) {
			case int:
				resolvedParameters = append(resolvedParameters, v)
			case float64:
				resolvedParameters = append(resolvedParameters, int(v))
			case string:
				intVal, err := strconv.Atoi(v)
				if err != nil {
					return nil, fmt.Errorf("invalid integer value: %v", v)
				}
				resolvedParameters = append(resolvedParameters, intVal)
			default:
				resolvedParameters = append(resolvedParameters, v)
			}
		case "float4", "float8":
			switch v := value.(type) {
			case float64:
				resolvedParameters = append(resolvedParameters, v)
			case string:
				floatVal, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid float value: %v", v)
				}
				resolvedParameters = append(resolvedParameters, floatVal)
			default:
				resolvedParameters = append(resolvedParameters, v)
			}
		case "boolean":
			switch v := value.(type) {
			case bool:
				resolvedParameters = append(resolvedParameters, v)
			case string:
				strVal := strings.ToLower(strings.TrimSpace(v))
				if strVal == "true" {
					resolvedParameters = append(resolvedParameters, true)
				} else if strVal == "false" {
					resolvedParameters = append(resolvedParameters, false)
				} else {
					return nil, fmt.Errorf("invalid boolean value: %v", v)
				}
			default:
				resolvedParameters = append(resolvedParameters, v)
			}
		case "varchar", "char":
			resolvedParameters = append(resolvedParameters, fmt.Sprintf("%v", value))
		case "timestamp", "date":
			resolvedParameters = append(resolvedParameters, value)
		default:
			resolvedParameters = append(resolvedParameters, value)
		}
	}

	return resolvedParameters, nil
}

// formatValue formats values for consistent comparison
func (tr *TestRunner) formatValue(value interface{}) interface{} {
	switch v := value.(type) {
	case time.Time:
		// Convert to ISO string with exactly 6 decimal places
		isoString := v.UTC().Format("2006-01-02T15:04:05.000000Z")
		return isoString
	case []byte:
		return string(v)
	}
	return value
}

// convertResult normalizes query results for consistent comparison
func (tr *TestRunner) convertResult(result interface{}) interface{} {
	if rows, ok := result.([][]interface{}); ok {
		normalizedRows := make([][]interface{}, len(rows))
		for i, row := range rows {
			normalizedRow := make([]interface{}, len(row))
			for j, val := range row {
				normalizedRow[j] = tr.formatValue(val)
			}
			normalizedRows[i] = normalizedRow
		}
		return normalizedRows
	}
	return result
}

// assertResult compares actual results against expected results
func (tr *TestRunner) assertResult(expect ExpectClause, actual interface{}) error {
	normalizedActual := tr.convertResult(actual)

	if expect.Result != nil {
		expectedResult := expect.Result

		if rows, ok := expectedResult.([][]interface{}); ok {
			actualStr, ok := normalizedActual.(string)
			if ok {
				return fmt.Errorf("expected result %v, got status '%s'", expectedResult, actualStr)
			}

			actualRows, ok := normalizedActual.([][]interface{})
			if !ok {
				return fmt.Errorf("expected result %v, got %v", expectedResult, normalizedActual)
			}

			// Compare result sets
			if len(rows) != len(actualRows) {
				return fmt.Errorf("expected %d rows, got %d rows", len(rows), len(actualRows))
			}

			for i := range rows {
				if len(rows[i]) != len(actualRows[i]) {
					return fmt.Errorf("row %d: expected %d columns, got %d columns",
						i, len(rows[i]), len(actualRows[i]))
				}

				for j := range rows[i] {
					if fmt.Sprintf("%v", rows[i][j]) != fmt.Sprintf("%v", actualRows[i][j]) {
						return fmt.Errorf("row %d, col %d: expected '%v', got '%v'",
							i, j, rows[i][j], actualRows[i][j])
					}
				}
			}
		} else {
			// Compare single values
			if fmt.Sprintf("%v", normalizedActual) != fmt.Sprintf("%v", expectedResult) {
				return fmt.Errorf("expected result '%v', got '%v'", expectedResult, normalizedActual)
			}
		}
	} else if len(expect.ResultContains) > 0 {
		// Check result contains expected rows
		actualStr, ok := normalizedActual.(string)
		if ok {
			return fmt.Errorf("expected result containing %v, got status '%s'",
				expect.ResultContains, actualStr)
		}

		actualRows, ok := normalizedActual.([][]interface{})
		if !ok {
			return fmt.Errorf("expected result containing %v, got %v",
				expect.ResultContains, normalizedActual)
		}

		for _, expectedRow := range expect.ResultContains {
			found := false
			for _, actualRow := range actualRows {
				if len(expectedRow) == len(actualRow) {
					match := true
					for i := range expectedRow {
						if fmt.Sprintf("%v", expectedRow[i]) != fmt.Sprintf("%v", actualRow[i]) {
							match = false
							break
						}
					}
					if match {
						found = true
						break
					}
				}
			}

			if !found {
				return fmt.Errorf("expected row %v not found in actual results", expectedRow)
			}
		}
	}

	return nil
}

// executeLoop executes a loop construct in the test
func (tr *TestRunner) executeLoop(ctx context.Context, loopDef *Loop, variables map[string]interface{}, conn *pgx.Conn) error {
	loopVarName := loopDef.As
	loopVariables := make(map[string]interface{})
	for k, v := range variables {
		loopVariables[k] = v
	}

	var iterable []interface{}

	if len(loopDef.Over) > 0 {
		iterable = loopDef.Over
	} else if loopDef.Range.End >= loopDef.Range.Start {
		// Create range slice
		for i := loopDef.Range.Start; i <= loopDef.Range.End; i++ {
			iterable = append(iterable, i)
		}
	} else {
		return fmt.Errorf("loop must have 'over' or 'range' defined")
	}

	for _, item := range iterable {
		loopVariables[loopVarName] = item
		if err := tr.executeSteps(ctx, loopDef.Steps, loopVariables, conn); err != nil {
			return err
		}
	}

	return nil
}

// executeStep executes a single test step
func (tr *TestRunner) executeStep(ctx context.Context, step Step, variables map[string]interface{}, conn *pgx.Conn) error {
	queryTemplate := step.Query
	parameters := step.Parameters
	expect := step.Expect

	queryWithVars := tr.substituteVariables(queryTemplate, variables)
	query := tr.adjustPlaceholderSyntax(queryWithVars)

	resolvedParameters, err := tr.resolveParameters(parameters, variables)
	if err != nil {
		return err
	}

	result, err := tr.executeQuery(ctx, conn, query, resolvedParameters)
	if err != nil {
		return err
	}

	// Check expectations if any are defined
	if expect.Result != nil || len(expect.ResultContains) > 0 {
		return tr.assertResult(expect, result)
	}

	return nil
}

// executeSteps executes a sequence of test steps
func (tr *TestRunner) executeSteps(ctx context.Context, steps []Step, variables map[string]interface{}, conn *pgx.Conn) error {
	for _, step := range steps {
		if step.Loop != nil {
			if err := tr.executeLoop(ctx, step.Loop, variables, conn); err != nil {
				return err
			}
		} else {
			if err := tr.executeStep(ctx, step, variables, conn); err != nil {
				return err
			}
		}
	}
	return nil
}

// runTest executes a complete test including preparation and teardown
func (tr *TestRunner) runTest(ctx context.Context, test Test, globalVariables map[string]interface{}, conn *pgx.Conn) error {
	// Merge global variables with test-specific variables
	variables := make(map[string]interface{})
	for k, v := range globalVariables {
		variables[k] = v
	}
	for k, v := range test.Variables {
		variables[k] = v
	}

	testFailed := false
	var testErr error

	// Prepare phase
	if err := tr.executeSteps(ctx, test.Prepare, variables, conn); err != nil {
		fmt.Printf("Test '%s' preparation failed: %v\n", test.Name, err)
		testFailed = true
		testErr = err
	}

	// Test steps (only if preparation succeeded)
	if !testFailed {
		if err := tr.executeSteps(ctx, test.Steps, variables, conn); err != nil {
			fmt.Printf("Test '%s' failed: %v\n", test.Name, err)
			testFailed = true
			testErr = err
		} else {
			fmt.Printf("Test '%s' passed.\n", test.Name)
		}
	}

	// Teardown phase (always attempt teardown)
	if err := tr.executeSteps(ctx, test.Teardown, variables, conn); err != nil {
		fmt.Printf("Teardown for test '%s' failed: %v\n", test.Name, err)
		if !testFailed {
			testFailed = true
			testErr = err
		}
	}

	if testFailed {
		return testErr
	}
	return nil
}

// main is the entry point for the test runner
func (tr *TestRunner) main(yamlFile string) error {
	// Read and parse YAML file
	yamlData, err := os.ReadFile(yamlFile)
	if err != nil {
		return fmt.Errorf("failed to read test file: %v", err)
	}

	var config TestConfig
	if err := yaml.Unmarshal(yamlData, &config); err != nil {
		return fmt.Errorf("failed to parse YAML: %v", err)
	}

	// Get port from environment or use default
	port := os.Getenv("PGPORT")
	if port == "" {
		port = "8812"
	}

	ctx := context.Background()

	// Run each test
	for _, test := range config.Tests {
		iterations := test.Iterations
		if iterations == 0 {
			iterations = 50 // Default iterations
		}

		for i := 0; i < iterations; i++ {
			fmt.Printf("Running test '%s' iteration %d...\n", test.Name, i+1)

			// Create a new connection for each test iteration
			connString := fmt.Sprintf("postgres://admin:quest@localhost:%s/qdb", port)
			conn, err := pgx.Connect(ctx, connString)
			if err != nil {
				return fmt.Errorf("unable to connect to database: %v", err)
			}

			err = tr.runTest(ctx, test, config.Variables, conn)
			conn.Close(ctx)

			if err != nil {
				return err // Exit on first test failure
			}
		}
	}

	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run runner.go <test_file.yaml>")
		os.Exit(1)
	}

	yamlFile := os.Args[1]
	runner := &TestRunner{}

	if err := runner.main(yamlFile); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
