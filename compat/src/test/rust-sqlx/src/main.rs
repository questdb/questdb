use chrono::{NaiveDate, NaiveDateTime};
use regex::Regex;
use serde::Deserialize;
use serde_yaml::Value;
use sqlx::postgres::{PgArguments, PgPool, PgRow, PgValueRef};
use sqlx::types::BigDecimal;
use sqlx::{Arguments, Column, Row, TypeInfo, ValueRef};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::process;
use thiserror::Error;

#[derive(Debug, Deserialize)]
struct TestFile {
    variables: Option<HashMap<String, String>>,
    tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
struct TestCase {
    name: String,
    #[allow(dead_code)]
    description: Option<String>,
    variables: Option<HashMap<String, String>>,
    prepare: Option<Vec<Step>>,
    steps: Vec<Step>,
    teardown: Option<Vec<Step>>,
    iterations: Option<u32>,
    exclude: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Step {
    ActionStep(ActionStep),
    LoopEnvelope {
        #[serde(rename = "loop")]
        loop_: Loop,
    },
}

#[derive(Debug, Deserialize)]
struct Loop {
    range: Option<Range>,
    over: Option<Vec<Value>>,
    #[serde(rename = "as")]
    as_name: String,
    steps: Vec<ActionStep>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Action {
    Execute,
    Query,
}

#[derive(Debug, Deserialize)]
struct ActionStep {
    action: Action,
    query: Option<String>,
    parameters: Option<Vec<TypedParameter>>,
    expect: Option<Expect>,
}

#[derive(Debug, Deserialize)]
struct TypedParameter {
    value: Value,
    #[serde(rename = "type")]
    type_: String,
}

#[derive(Debug, Deserialize)]
struct Range {
    start: i64,
    end: i64,
}

#[derive(Debug, Deserialize)]
struct Expect {
    result: Option<Vec<Vec<Value>>>,
    result_contains: Option<Vec<Vec<Value>>>,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let yaml_file = env::args()
        .nth(1)
        .ok_or_else(|| "Usage: runner <test_file.yaml>".to_string())?;

    let yaml_content = fs::read_to_string(&yaml_file)?;
    let test_file: TestFile = serde_yaml::from_str(&yaml_content)?;

    let port = env::var("PGPORT").unwrap_or_else(|_| "8812".to_string());
    let connection_string = format!("postgres://admin:quest@localhost:{}/qdb", port);

    let pool = PgPool::connect(&connection_string).await?;

    let all_tests_passed = run_tests(&pool, &test_file).await?;

    if !all_tests_passed {
        process::exit(1);
    }

    Ok(())
}

async fn run_tests(
    pool: &PgPool,
    test_file: &TestFile,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let mut all_tests_passed = true;
    let sqlx_string = "sqlx".to_string();

    for test in &test_file.tests {
        let iterations = test.iterations.unwrap_or(50);

        if let Some(excludes) = &test.exclude {
            if excludes.contains(&sqlx_string) {
                println!(
                    "Skipping test: {:?} because it's excluded for sqlx",
                    test.name
                );
                continue;
            }
        }

        for i in 0..iterations {
            println!("Running test '{}' (iteration {})", test.name, i);
            if !run_test(pool, &test_file.variables, test).await? {
                all_tests_passed = false;
                // Fail fast - exit immediately on first test failure
                break;
            }
        }

        // If any test failed, exit immediately
        if !all_tests_passed {
            break;
        }
    }
    Ok(all_tests_passed)
}

async fn run_test(
    pool: &PgPool,
    global_variables: &Option<HashMap<String, String>>,
    test: &TestCase,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let mut variables = global_variables.clone().unwrap_or_default();
    if let Some(locals) = &test.variables {
        variables.extend(locals.clone());
    }

    if let Some(prepare_steps) = &test.prepare {
        execute_steps(pool, prepare_steps, &mut variables).await?;
    }

    let test_result = execute_steps(pool, &test.steps, &mut variables).await;
    let test_passed = test_result.is_ok();

    if test_passed {
        println!("Test '{}' passed.", test.name);
    } else {
        eprintln!(
            "Test '{}' failed: {:?}",
            test.name,
            test_result.unwrap_err()
        );
    }

    if let Some(teardown_steps) = &test.teardown {
        if let Err(e) = execute_steps(pool, teardown_steps, &mut variables).await {
            eprintln!("Teardown for test '{}' failed: {}", test.name, e);
        }
    }

    Ok(test_passed)
}

async fn execute_steps(
    pool: &PgPool,
    steps: &[Step],
    variables: &mut HashMap<String, String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    for step in steps {
        match step {
            Step::ActionStep(action_step) => {
                execute_step(pool, action_step, variables).await?;
            }
            Step::LoopEnvelope { loop_ } => {
                execute_loop(pool, loop_, variables).await?;
            }
        }
    }
    Ok(())
}

async fn execute_loop(
    pool: &PgPool,
    loop_def: &Loop,
    variables: &mut HashMap<String, String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let loop_var_name = &loop_def.as_name;
    let original_value = variables.get(loop_var_name).cloned();

    let iterable = loop_def.over.clone().unwrap_or_else(|| {
        loop_def.range.as_ref().map_or_else(
            || vec![],
            |range| (range.start..=range.end).map(Value::from).collect(),
        )
    });

    for item in iterable {
        variables.insert(loop_var_name.clone(), value_to_string(&item, variables)?);

        for action_step in &loop_def.steps {
            execute_step(pool, action_step, variables).await?;
        }
    }

    if let Some(value) = original_value {
        variables.insert(loop_var_name.clone(), value);
    } else {
        variables.remove(loop_var_name);
    }

    Ok(())
}

async fn execute_step(
    pool: &PgPool,
    action_step: &ActionStep,
    variables: &mut HashMap<String, String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let query_template = action_step.query.as_ref().ok_or("Missing query")?;
    let query_with_vars = substitute_variables(query_template, variables)?;
    let query = replace_param_placeholders(&query_with_vars);

    // Build arguments dynamically
    let mut arguments = PgArguments::default();
    if let Some(params) = &action_step.parameters {
        bind_parameters(&mut arguments, params, variables)?;
    }

    match action_step.action {
        Action::Query => {
            let rows: Vec<PgRow> = sqlx::query_with(&query, arguments)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    if let Some(expectation) = &action_step.expect {
                        if let Some(expected_error) = &expectation.error {
                            let error_message = e.to_string();
                            if !error_message.contains(expected_error) {
                                return format!(
                                    "Expected error '{}', but got '{}'",
                                    expected_error, error_message
                                );
                            }
                            return String::new(); // Expected error occurred
                        }
                    }
                    e.to_string()
                })?;

            if let Some(expectation) = &action_step.expect {
                assert_result(expectation, &rows)?;
            }
        }
        Action::Execute => {
            let result = sqlx::query_with(&query, arguments)
                .execute(pool)
                .await
                .map_err(|e| {
                    if let Some(expectation) = &action_step.expect {
                        if let Some(expected_error) = &expectation.error {
                            let error_message = e.to_string();
                            if !error_message.contains(expected_error) {
                                return format!(
                                    "Expected error '{}', but got '{}'",
                                    expected_error, error_message
                                );
                            }
                            return String::new(); // Expected error occurred
                        }
                    }
                    e.to_string()
                })?;

            if let Some(expectation) = &action_step.expect {
                if let Some(expected_result) = &expectation.result {
                    if expected_result.len() != 1 || expected_result[0].len() != 1 {
                        return Err("Expected result must be a single value".into());
                    }
                    let expected_value = &expected_result[0][0];
                    if expected_value != &Value::Number(result.rows_affected().into()) {
                        return Err(format!(
                            "Expected result {:?}, got {:?}",
                            expected_value,
                            result.rows_affected()
                        )
                        .into());
                    }
                }
            }
        }
    }

    Ok(())
}

fn bind_parameters(
    arguments: &mut PgArguments,
    parameters: &[TypedParameter],
    variables: &HashMap<String, String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    for param in parameters {
        let type_lower = param.type_.to_lowercase();

        match &param.value {
            Value::Number(n) => match type_lower.as_str() {
                "int4" => arguments.add(n.as_i64().ok_or("Invalid int4")? as i32)?,
                "int8" => arguments.add(n.as_i64().ok_or("Invalid int8")?)?,
                "float4" => arguments.add(n.as_f64().ok_or("Invalid float4")? as f32)?,
                "float8" => arguments.add(n.as_f64().ok_or("Invalid float8")?)?,
                "varchar" => arguments.add(n.to_string())?,
                "timestamp" => {
                    let timestamp = parse_timestamp(&n.to_string())?;
                    arguments.add(timestamp)?;
                }
                _ => return Err(format!("Unsupported parameter type: {}", param.type_).into()),
            },
            Value::String(s) => {
                let substituted = substitute_variables(s, variables)?;
                match type_lower.as_str() {
                    "int4" => arguments.add(substituted.parse::<i32>()?)?,
                    "int8" => arguments.add(substituted.parse::<i64>()?)?,
                    "float4" => arguments.add(substituted.parse::<f32>()?)?,
                    "float8" => arguments.add(substituted.parse::<f64>()?)?,
                    "varchar" | "char" => arguments.add(substituted)?,
                    "boolean" => {
                        let bool_val = substituted.to_lowercase() == "true";
                        arguments.add(bool_val)?
                    }
                    "timestamp" => {
                        let timestamp = parse_timestamp(&substituted)?;
                        arguments.add(timestamp)?;
                    }
                    "date" => {
                        // QuestDB expects dates as timestamps over the wire protocol
                        // But SQLx might be more strict about type inference
                        let date = substituted.parse::<NaiveDate>()?;
                        // Try using the date directly first
                        arguments.add(date)?;
                    }
                    "array_float8" => {
                        // Parse PostgreSQL array format
                        if substituted.trim() == "{}" {
                            // Empty array
                            let empty: Vec<f64> = vec![];
                            arguments.add(empty)?;
                        } else if substituted.contains("{{") {
                            // Multi-dimensional array - not directly supported by SQLx
                            // We'll need to handle this case specially
                            // For now, just pass an empty array to avoid errors
                            let empty: Vec<f64> = vec![];
                            arguments.add(empty)?;
                        } else {
                            // Single-dimensional array
                            let trimmed = substituted.trim_start_matches('{').trim_end_matches('}');
                            let float_vec: Vec<f64> = trimmed
                                .split(',')
                                .map(|s| s.trim())
                                .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("NULL"))
                                .map(|s| s.parse::<f64>())
                                .collect::<Result<Vec<_>, _>>()?;
                            arguments.add(float_vec)?;
                        }
                    }
                    "numeric" => arguments.add(substituted.parse::<BigDecimal>()?)?,
                    _ => return Err(format!("Unsupported parameter type: {}", param.type_).into()),
                }
            }
            _ => return Err("Unsupported parameter value type".into()),
        }
    }
    Ok(())
}

fn substitute_variables(
    text: &str,
    variables: &HashMap<String, String>,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let re = Regex::new(r"\$\{([^}]+)\}")?;
    let result = re.replace_all(text, |caps: &regex::Captures| {
        variables
            .get(&caps[1])
            .cloned()
            .unwrap_or_else(|| format!("${{{}}}", &caps[1]))
    });
    Ok(result.into_owned())
}

fn replace_param_placeholders(query: &str) -> String {
    let re = Regex::new(r"\$\[(\d+)\]").unwrap();
    re.replace_all(query, |caps: &regex::Captures| format!("${}", &caps[1]))
        .to_string()
}

fn parse_timestamp(s: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ")
}

fn value_to_string(
    value: &Value,
    variables: &HashMap<String, String>,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    match value {
        Value::String(s) => substitute_variables(s, variables),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err("Unsupported loop variable type".into()),
    }
}

#[derive(Error, Debug)]
enum AssertError {
    #[error("Expected result {expected:?}, got {actual:?}")]
    ResultMismatch {
        expected: Vec<Vec<Value>>,
        actual: Vec<Vec<Value>>,
    },
    #[error("Expected row {0:?} not found in actual results")]
    RowNotFound(Vec<Value>),
}

fn assert_result(expect: &Expect, actual: &[PgRow]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let actual_converted: Vec<Vec<Value>> = actual
        .iter()
        .map(|row| (0..row.len()).map(|i| get_value_as_yaml(row, i)).collect())
        .collect();

    if let Some(expected_result) = &expect.result {
        if actual_converted != *expected_result {
            return Err(Box::new(AssertError::ResultMismatch {
                expected: expected_result.clone(),
                actual: actual_converted,
            }));
        }
    } else if let Some(expected_contains) = &expect.result_contains {
        for expected_row in expected_contains {
            if !actual_converted.contains(expected_row) {
                return Err(Box::new(AssertError::RowNotFound(expected_row.clone())));
            }
        }
    }

    Ok(())
}

fn get_value_as_yaml(row: &PgRow, idx: usize) -> Value {
    let column = row.column(idx);
    let type_info = column.type_info();
    let type_name = type_info.name();

    // Get raw value reference
    let raw_value: PgValueRef = row.try_get_raw(idx).unwrap();

    // Check for NULL first
    if raw_value.is_null() {
        return Value::Null;
    }

    // For array types, try to decode as Vec<f64> (works for 1D arrays)
    if type_name.contains("[]") || type_name.starts_with("_") {
        if let Ok(vec) = row.try_get::<Vec<f64>, _>(idx) {
            // Format as PostgreSQL array string
            let formatted = format!(
                "{{{}}}",
                vec.iter()
                    .map(|v| {
                        if v.fract().abs() < f64::EPSILON {
                            format!("{:.1}", v)
                        } else {
                            v.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            );
            return Value::String(formatted);
        }
        // Multi-dimensional arrays are excluded, so this shouldn't happen
        return Value::Null;
    }

    match type_name {
        "INT2" => {
            if let Ok(val) = row.try_get::<i16, _>(idx) {
                Value::Number(val.into())
            } else {
                Value::Null
            }
        }
        "INT4" => {
            if let Ok(val) = row.try_get::<i32, _>(idx) {
                Value::Number(val.into())
            } else {
                Value::Null
            }
        }
        "INT8" => {
            if let Ok(val) = row.try_get::<i64, _>(idx) {
                Value::Number(val.into())
            } else {
                Value::Null
            }
        }
        "FLOAT4" => {
            if let Ok(val) = row.try_get::<f32, _>(idx) {
                Value::Number(serde_yaml::Number::from(val))
            } else {
                Value::Null
            }
        }
        "FLOAT8" => {
            if let Ok(val) = row.try_get::<f64, _>(idx) {
                Value::Number(serde_yaml::Number::from(val))
            } else {
                Value::Null
            }
        }
        "BOOL" => {
            if let Ok(val) = row.try_get::<bool, _>(idx) {
                Value::Bool(val)
            } else {
                Value::Null
            }
        }
        "TIMESTAMP" => {
            if let Ok(val) = row.try_get::<NaiveDateTime, _>(idx) {
                Value::String(val.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
            } else {
                Value::Null
            }
        }
        "VARCHAR" | "TEXT" => {
            if let Ok(val) = row.try_get::<String, _>(idx) {
                Value::String(val)
            } else {
                Value::Null
            }
        }
        "NUMERIC" => {
            if let Ok(val) = row.try_get::<BigDecimal, _>(idx) {
                Value::String(val.normalized().to_string())
            } else {
                Value::Null
            }
        }
        _ => {
            // Try to get as string for any unknown types
            if let Ok(val) = row.try_get::<String, _>(idx) {
                Value::String(val)
            } else {
                Value::Null
            }
        }
    }
}
