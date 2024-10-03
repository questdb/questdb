use chrono::NaiveDateTime;
use regex::Regex;
use serde::Deserialize;
use serde_yaml::Value;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::process;
use thiserror::Error;
use tokio_postgres::{types::ToSql, Client, NoTls, Row};
use crate::TestError::AssertionError;

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
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Step {
    ActionStep(ActionStep),
    LoopEnvelope {
        #[serde(rename = "loop")]
        loop_: Loop
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
async fn main() -> TestResult<()> {
    let yaml_file = env::args().nth(1).ok_or_else(|| TestError::InputError("Usage: runner <test_file.yaml>".to_string()))?;
    let yaml_content = fs::read_to_string(&yaml_file).map_err(|e| TestError::InputError(e.to_string()))?;
    let test_file: TestFile = serde_yaml::from_str(&yaml_content).map_err(|e| TestError::InputError(e.to_string()))?;

    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=8812 user=admin password=quest dbname=qdb",
        NoTls,
    )
        .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let all_tests_passed = run_tests(&client, &test_file).await?;

    if !all_tests_passed {
        process::exit(1);
    }

    Ok(())
}

async fn run_tests(client: &Client, test_file: &TestFile) -> TestResult<bool> {
    let mut all_tests_passed = true;
    for test in &test_file.tests {
        if !run_test(client, &test_file.variables, test).await? {
            all_tests_passed = false;
        }
    }
    Ok(all_tests_passed)
}

async fn run_test(
    client: &Client,
    global_variables: &Option<HashMap<String, String>>,
    test: &TestCase,
) -> TestResult<bool> {
    let mut variables = global_variables.clone().unwrap_or_default();
    if let Some(locals) = &test.variables {
        variables.extend(locals.clone());
    }

    if let Some(prepare_steps) = &test.prepare {
        execute_steps(client, prepare_steps, &mut variables).await?;
    }

    let test_result = execute_steps(client, &test.steps, &mut variables).await;
    let test_passed = test_result.is_ok();

    if test_passed {
        println!("Test '{}' passed.", test.name);
    } else {
        eprintln!("Test '{}' failed: {:?}", test.name, test_result.unwrap_err());
    }

    if let Some(teardown_steps) = &test.teardown {
        if let Err(e) = execute_steps(client, teardown_steps, &mut variables).await {
            eprintln!("Teardown for test '{}' failed: {}", test.name, e);
        }
    }

    Ok(test_passed)
}

async fn execute_steps(
    client: &Client,
    steps: &[Step],
    variables: &mut HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for step in steps {
        match step {
            Step::ActionStep(action_step) => {
                execute_step(client, action_step, variables).await?;
            }
            Step::LoopEnvelope { loop_ } => {
                execute_loop(client, loop_, variables).await?;
            }
        }
    }
    Ok(())
}

async fn execute_loop(
    client: &Client,
    loop_def: &Loop,
    variables: &mut HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
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
            execute_step(client, action_step, variables).await?;
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
    client: &Client,
    action_step: &ActionStep,
    variables: &mut HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let query_template = action_step.query.as_ref().ok_or("Missing query")?;
    let query_with_vars = substitute_variables(query_template, variables)?;
    let query = replace_param_placeholders(&query_with_vars);

    let params: Vec<Box<dyn ToSql + Sync>> = action_step.parameters
        .as_ref()
        .map(|params| extract_parameters(params, variables))
        .transpose()?
        .unwrap_or_default();

    let stmt = client.prepare(&query).await?;
    let params_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(AsRef::as_ref).collect();

    match action_step.action {
        Action::Query => {
            let result = client.query(&stmt, &params_refs).await;
            handle_query_result(result, action_step)?;
        }
        Action::Execute => {
            let result = client.execute(&stmt, &params_refs).await;
            handle_execute_result(result, action_step)?;
        }
    }

    Ok(())
}

fn substitute_variables(
    text: &str,
    variables: &HashMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
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

fn extract_parameters(
    parameters: &[TypedParameter],
    variables: &HashMap<String, String>,
) -> Result<Vec<Box<dyn ToSql + Sync>>, Box<dyn std::error::Error>> {
    parameters.iter().map(|param| {
        let param_value: Box<dyn ToSql + Sync> = match &param.value {
            Value::Number(n) => match param.type_.as_str() {
                "int4" => Box::new(n.as_i64().ok_or("Invalid int4")? as i32),
                "int8" => Box::new(n.as_i64().ok_or("Invalid int8")?),
                "timestamp" => Box::new(parse_timestamp(&n.to_string())?),
                "float4" => Box::new(n.as_f64().ok_or("Invalid float4")? as f32),
                "float8" => Box::new(n.as_f64().ok_or("Invalid float8")?),
                "varchar" => Box::new(n.to_string()),
                _ => return Err("Unsupported parameter type".into()),
            },
            Value::String(s) => {
                let substituted = substitute_variables(s, variables)?;
                match param.type_.to_lowercase().as_str() {
                    "int4" => Box::new(substituted.parse::<i32>()?),
                    "int8" => Box::new(substituted.parse::<i64>()?),
                    "timestamp" => Box::new(parse_timestamp(&substituted)?),
                    "float4" => Box::new(substituted.parse::<f32>()?),
                    "float8" => Box::new(substituted.parse::<f64>()?),
                    "varchar" => Box::new(substituted),
                    _ => return Err("Unsupported parameter type".into()),
                }
            }
            _ => return Err("Unsupported parameter type".into()),
        };
        Ok(param_value)
    }).collect()
}

fn parse_timestamp(s: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ")
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

    #[error("Expected error message {0:?}, got {1:?}")]
    ErrorMsgMismatch(String, String),
}

fn assert_result(expect: &Expect, actual: &[Row]) -> Result<(), AssertError> {
    let actual_converted: Vec<Vec<Value>> = actual
        .iter()
        .map(|row| (0..row.len()).map(|i| get_value_as_yaml(row, i)).collect())
        .collect();

    if let Some(expected_result) = &expect.result {
        if actual_converted != *expected_result {
            return Err(AssertError::ResultMismatch {
                expected: expected_result.clone(),
                actual: actual_converted,
            });
        }
    } else if let Some(expected_contains) = &expect.result_contains {
        for expected_row in expected_contains {
            if !actual_converted.contains(expected_row) {
                return Err(AssertError::RowNotFound(expected_row.clone()));
            }
        }
    }

    Ok(())
}

fn get_value_as_yaml(row: &Row, idx: usize) -> Value {
    let column_type = row.columns()[idx].type_();

    match *column_type {
        tokio_postgres::types::Type::INT2 => Value::Number(row.get::<_, i16>(idx).into()),
        tokio_postgres::types::Type::INT4 => Value::Number(row.get::<_, i32>(idx).into()),
        tokio_postgres::types::Type::INT8 => Value::Number(row.get::<_, i64>(idx).into()),
        tokio_postgres::types::Type::FLOAT4 => Value::Number(serde_yaml::Number::from(row.get::<_, f32>(idx))),
        tokio_postgres::types::Type::FLOAT8 => Value::Number(serde_yaml::Number::from(row.get::<_, f64>(idx))),
        tokio_postgres::types::Type::BOOL => Value::Bool(row.get(idx)),
        tokio_postgres::types::Type::TIMESTAMP => {
            let val: NaiveDateTime = row.get(idx);
            Value::String(val.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        }
        tokio_postgres::types::Type::VARCHAR => {
            Value::String(row.get(idx))
        }
        _ => Value::String(row.get(idx)),
    }
}

fn value_to_string(
    value: &Value,
    variables: &HashMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
    match value {
        Value::String(s) => substitute_variables(s, variables),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err("Unsupported loop variable type".into()),
    }
}

fn handle_query_result(
    result: Result<Vec<Row>, tokio_postgres::Error>,
    action_step: &ActionStep,
) -> Result<(), TestError> {
    match result {
        Ok(rows) => {
            if let Some(expectation) = &action_step.expect {
                assert_result(expectation, &rows)?;
            }
        }
        Err(e) => {
            if let Some(expectation) = &action_step.expect {
                if let Some(expected_error) = &expectation.error {
                    let error_message = e.to_string();
                    if !error_message.contains(expected_error) {
                        let error = AssertError::ErrorMsgMismatch(expected_error.clone(), error_message);
                        return Err(AssertionError(error));
                    }
                } else {
                    return Err(e.into());
                }
            } else {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

fn handle_execute_result(
    result: Result<u64, tokio_postgres::Error>,
    action_step: &ActionStep,
) -> Result<(), Box<dyn std::error::Error>> {
    match result {
        Ok(rows_affected) => {
            if let Some(expectation) = &action_step.expect {
                if let Some(expected_result) = &expectation.result {
                    if expected_result.len() != 1 || expected_result[0].len() != 1 {
                        return Err("Expected result must be a single value".into());
                    }
                    let expected_value = &expected_result[0][0];
                    if expected_value != &Value::Number(rows_affected.into()) {
                        return Err(format!(
                            "Expected result {:?}, got {:?}",
                            expected_value, rows_affected
                        )
                            .into());
                    }
                }
            }
        }
        Err(e) => {
            if let Some(expectation) = &action_step.expect {
                if let Some(expected_error) = &expectation.error {
                    let error_message = e.to_string();
                    if !error_message.contains(expected_error) {
                        return Err(format!(
                            "Expected error '{}', but got '{}'",
                            expected_error, error_message
                        )
                            .into());
                    }
                } else {
                    return Err(e.into());
                }
            } else {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

#[derive(Error, Debug)]
enum TestError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] tokio_postgres::Error),
    #[error("Assertion error: {0}")]
    AssertionError(#[from] AssertError),
    #[error("Input error: {0}")]
    InputError(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
}

type TestResult<T> = Result<T, TestError>;

impl From<Box<dyn std::error::Error>> for TestError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        TestError::ExecutionError(error.to_string())
    }
}