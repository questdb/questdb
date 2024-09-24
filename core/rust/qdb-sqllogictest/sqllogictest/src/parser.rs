//! Sqllogictest parser.

use std::fmt;
use std::iter::Peekable;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use regex::Regex;

use crate::ColumnType;

const RESULTS_DELIMITER: &str = "----";

/// The location in source file.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Location {
    file: Arc<str>,
    line: u32,
    upper: Option<Arc<Location>>,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.line)?;
        if let Some(upper) = &self.upper {
            write!(f, "\nat {upper}")?;
        }
        Ok(())
    }
}

impl Location {
    /// File path.
    pub fn file(&self) -> &str {
        &self.file
    }

    /// Line number.
    pub fn line(&self) -> u32 {
        self.line
    }

    fn new(file: impl Into<Arc<str>>, line: u32) -> Self {
        Self {
            file: file.into(),
            line,
            upper: None,
        }
    }

    /// Returns the location of next line.
    #[must_use]
    fn next_line(mut self) -> Self {
        self.line += 1;
        self
    }

    /// Returns the location of next level file.
    fn include(&self, file: &str) -> Self {
        Self {
            file: file.into(),
            line: 0,
            upper: Some(Arc::new(self.clone())),
        }
    }
}

/// Expectation for a statement.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementExpect {
    /// Statement should succeed.
    Ok,
    /// Statement should succeed and affect the given number of rows.
    Count(u64),
    /// Statement should fail with the given error message.
    Error(ExpectedError),
}

/// Expectation for a query.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpect<T: ColumnType> {
    /// Query should succeed and return the given results.
    Results {
        types: Vec<T>,
        sort_mode: Option<SortMode>,
        label: Option<String>,
        results: Vec<String>,
    },
    /// Query should fail with the given error message.
    Error(ExpectedError),
}

impl<T: ColumnType> QueryExpect<T> {
    /// Creates a new [`QueryExpect`] with empty results.
    fn empty_results() -> Self {
        Self::Results {
            types: Vec::new(),
            sort_mode: None,
            label: None,
            results: Vec::new(),
        }
    }
}

/// A single directive in a sqllogictest file.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Record<T: ColumnType> {
    /// An include copies all records from another files.
    Include {
        loc: Location,
        /// A glob pattern
        filename: String,
    },
    /// A statement is an SQL command that is to be evaluated but from which we do not expect to
    /// get results (other than success or failure).
    Statement {
        loc: Location,
        conditions: Vec<Condition>,
        connection: Connection,
        /// The SQL command.
        sql: String,
        expected: StatementExpect,
    },
    /// A query is an SQL command from which we expect to receive results. The result set might be
    /// empty.
    Query {
        loc: Location,
        conditions: Vec<Condition>,
        connection: Connection,
        /// The SQL command.
        sql: String,
        expected: QueryExpect<T>,
    },
    /// A system command is an external command that is to be executed by the shell. Currently it
    /// must succeed and the output is ignored.
    #[non_exhaustive]
    System {
        loc: Location,
        conditions: Vec<Condition>,
        /// The external command.
        command: String,
        stdout: Option<String>,
    },
    /// A sleep period.
    Sleep {
        loc: Location,
        duration: Duration,
    },
    /// Subtest.
    Subtest {
        loc: Location,
        name: String,
    },
    /// A halt record merely causes sqllogictest to ignore the rest of the test script.
    /// For debugging use only.
    Halt {
        loc: Location,
    },
    /// Control statements.
    Control(Control),
    /// Set the maximum number of result values that will be accepted
    /// for a query.  If the number of result values exceeds this number,
    /// then an MD5 hash is computed of all values, and the resulting hash
    /// is the only result.
    ///
    /// If the threshold is 0, then hashing is never used.
    HashThreshold {
        loc: Location,
        threshold: u64,
    },
    /// Condition statements, including `onlyif` and `skipif`.
    Condition(Condition),
    /// Connection statements to specify the connection to use for the following statement.
    Connection(Connection),
    Comment(Vec<String>),
    Newline,
    /// Internally injected record which should not occur in the test file.
    Injected(Injected),
}

impl<T: ColumnType> Record<T> {
    /// Unparses the record to its string representation in the test file.
    ///
    /// # Panics
    /// If the record is an internally injected record which should not occur in the test file.
    pub fn unparse(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
        write!(w, "{self}")
    }
}

/// As is the standard for Display, does not print any trailing
/// newline except for records that always end with a blank line such
/// as Query and Statement.
impl<T: ColumnType> std::fmt::Display for Record<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Record::Include { loc: _, filename } => {
                write!(f, "include {filename}")
            }
            Record::Statement {
                loc: _,
                conditions: _,
                connection: _,
                sql,
                expected,
            } => {
                write!(f, "statement ")?;
                match expected {
                    StatementExpect::Ok => write!(f, "ok")?,
                    StatementExpect::Count(cnt) => write!(f, "count {cnt}")?,
                    StatementExpect::Error(err) => err.fmt_inline(f)?,
                }
                writeln!(f)?;
                // statement always end with a blank line
                writeln!(f, "{sql}")?;

                if let StatementExpect::Error(err) = expected {
                    err.fmt_multiline(f)?;
                }
                Ok(())
            }
            Record::Query {
                loc: _,
                conditions: _,
                connection: _,
                sql,
                expected,
            } => {
                write!(f, "query ")?;
                match expected {
                    QueryExpect::Results {
                        types,
                        sort_mode,
                        label,
                        ..
                    } => {
                        write!(f, "{}", types.iter().map(|c| c.to_char()).join(""))?;
                        if let Some(sort_mode) = sort_mode {
                            write!(f, " {}", sort_mode.as_str())?;
                        }
                        if let Some(label) = label {
                            write!(f, " {label}")?;
                        }
                    }
                    QueryExpect::Error(err) => err.fmt_inline(f)?,
                }
                writeln!(f)?;
                writeln!(f, "{sql}")?;

                match expected {
                    QueryExpect::Results { results, .. } => {
                        write!(f, "{}", RESULTS_DELIMITER)?;

                        for result in results {
                            write!(f, "\n{result}")?;
                        }
                        // query always ends with a blank line
                        writeln!(f)?
                    }
                    QueryExpect::Error(err) => err.fmt_multiline(f)?,
                }
                Ok(())
            }
            Record::System {
                loc: _,
                conditions: _,
                command,
                stdout,
            } => {
                writeln!(f, "system ok\n{command}")?;
                if let Some(stdout) = stdout {
                    writeln!(f, "----\n{}\n", stdout.trim())?;
                }
                Ok(())
            }
            Record::Sleep { loc: _, duration } => {
                write!(f, "sleep {}", humantime::format_duration(*duration))
            }
            Record::Subtest { loc: _, name } => {
                write!(f, "subtest {name}")
            }
            Record::Halt { loc: _ } => {
                write!(f, "halt")
            }
            Record::Control(c) => match c {
                Control::SortMode(m) => write!(f, "control sortmode {}", m.as_str()),
                Control::Substitution(s) => write!(f, "control substitution {}", s.as_str()),
                Control::IsoTimestamp(s) => write!(f, "control iso_timestamp {}", s.as_str()),
            },
            Record::Condition(cond) => match cond {
                Condition::OnlyIf { label } => write!(f, "onlyif {label}"),
                Condition::SkipIf { label } => write!(f, "skipif {label}"),
            },
            Record::Connection(conn) => {
                if let Connection::Named(conn) = conn {
                    write!(f, "connection {}", conn)?;
                }
                Ok(())
            }
            Record::HashThreshold { loc: _, threshold } => {
                write!(f, "hash-threshold {threshold}")
            }
            Record::Comment(comment) => {
                let mut iter = comment.iter();
                write!(f, "#{}", iter.next().unwrap().trim_end())?;
                for line in iter {
                    write!(f, "\n#{}", line.trim_end())?;
                }
                Ok(())
            }
            Record::Newline => Ok(()), // Display doesn't end with newline
            Record::Injected(p) => panic!("unexpected injected record: {p:?}"),
        }
    }
}

/// Expected error message after `error` or under `----`.
#[derive(Debug, Clone)]
pub enum ExpectedError {
    /// No expected error message.
    ///
    /// Any error message is considered as a match.
    Empty,
    /// An inline regular expression after `error`.
    ///
    /// The actual error message that matches the regex is considered as a match.
    Inline(Regex),
    /// A multiline error message under `----`, ends with 2 consecutive empty lines.
    ///
    /// The actual error message that's exactly the same as the expected one is considered as a
    /// match.
    Multiline(String),
}

impl ExpectedError {
    /// Parses an inline regex variant from tokens.
    fn parse_inline_tokens(tokens: &[&str]) -> Result<Self, ParseErrorKind> {
        Self::new_inline(tokens.join(" "))
    }

    /// Creates an inline expected error message from a regex string.
    ///
    /// If the regex is empty, it's considered as [`ExpectedError::Empty`].
    fn new_inline(regex: String) -> Result<Self, ParseErrorKind> {
        if regex.is_empty() {
            Ok(Self::Empty)
        } else {
            let regex =
                Regex::new(&regex).map_err(|_| ParseErrorKind::InvalidErrorMessage(regex))?;
            Ok(Self::Inline(regex))
        }
    }

    /// Returns whether it's an empty match.
    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    /// Unparses the expected message after `statement`.
    fn fmt_inline(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error")?;
        if let Self::Inline(regex) = self {
            write!(f, " {regex}")?;
        }
        Ok(())
    }

    /// Unparses the expected message with `----`, if it's multiline.
    fn fmt_multiline(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Self::Multiline(results) = self {
            writeln!(f, "{}", RESULTS_DELIMITER)?;
            writeln!(f, "{}", results.trim())?;
            writeln!(f)?; // another empty line to indicate the end of multiline message
        }
        Ok(())
    }

    /// Returns whether the given error message matches the expected one.
    pub fn is_match(&self, err: &str) -> bool {
        match self {
            Self::Empty => true,
            Self::Inline(regex) => regex.is_match(err),
            Self::Multiline(results) => results.trim() == err.trim(),
        }
    }

    /// Creates an expected error message from the actual error message. Used by the runner
    /// to update the test cases with `--override`.
    ///
    /// A reference might be provided to help decide whether to use inline or multiline.
    pub fn from_actual_error(reference: Option<&Self>, actual_err: &str) -> Self {
        let trimmed_err = actual_err.trim();
        let err_is_multiline = trimmed_err.lines().next_tuple::<(_, _)>().is_some();

        let multiline = match reference {
            Some(Self::Multiline(_)) => true, // always multiline if the ref is multiline
            _ => err_is_multiline,            // prefer inline as long as it fits
        };

        if multiline {
            // Even if the actual error is empty, we still use `Multiline` to indicate that
            // an exact empty error is expected, instead of any error by `Empty`.
            Self::Multiline(trimmed_err.to_string())
        } else {
            Self::new_inline(regex::escape(actual_err)).expect("escaped regex should be valid")
        }
    }
}

impl std::fmt::Display for ExpectedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpectedError::Empty => write!(f, "(any)"),
            ExpectedError::Inline(regex) => write!(f, "(regex) {}", regex),
            ExpectedError::Multiline(results) => write!(f, "(multiline) {}", results.trim()),
        }
    }
}

impl PartialEq for ExpectedError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Empty, Self::Empty) => true,
            (Self::Inline(l0), Self::Inline(r0)) => l0.as_str() == r0.as_str(),
            (Self::Multiline(l0), Self::Multiline(r0)) => l0 == r0,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[non_exhaustive]
pub enum Control {
    /// Control sort mode.
    SortMode(SortMode),
    /// Control whether or not to substitute variables in the SQL.
    Substitution(bool),
    IsoTimestamp(bool),
}

trait ControlItem: Sized {
    /// Try to parse from string.
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind>;

    /// Convert to string.
    fn as_str(&self) -> &'static str;
}

impl ControlItem for bool {
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind> {
        match s {
            "on" => Ok(true),
            "off" => Ok(false),
            _ => Err(ParseErrorKind::InvalidControl(s.to_string())),
        }
    }

    fn as_str(&self) -> &'static str {
        if *self {
            "on"
        } else {
            "off"
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Injected {
    /// Pseudo control command to indicate the begin of an include statement. Automatically
    /// injected by sqllogictest parser.
    BeginInclude(String),
    /// Pseudo control command to indicate the end of an include statement. Automatically injected
    /// by sqllogictest parser.
    EndInclude(String),
}

/// The condition to run a query.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Condition {
    /// The statement or query is evaluated only if the label is seen.
    OnlyIf { label: String },
    /// The statement or query is not evaluated if the label is seen.
    SkipIf { label: String },
}

impl Condition {
    /// Evaluate condition on given `label`, returns whether to skip this record.
    pub(crate) fn should_skip<'a>(&'a self, labels: impl IntoIterator<Item=&'a str>) -> bool {
        match self {
            Condition::OnlyIf { label } => !labels.into_iter().contains(&label.as_str()),
            Condition::SkipIf { label } => labels.into_iter().contains(&label.as_str()),
        }
    }
}

/// The connection to use for the following statement.
#[derive(Default, Debug, PartialEq, Eq, Hash, Clone)]
pub enum Connection {
    /// The default connection if not specified or if the name is "default".
    #[default]
    Default,
    /// A named connection.
    Named(String),
}

impl Connection {
    fn new(name: impl AsRef<str>) -> Self {
        match name.as_ref() {
            "default" => Self::Default,
            name => Self::Named(name.to_owned()),
        }
    }
}

/// Whether to apply sorting before checking the results of a query.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SortMode {
    /// The default option. The results appear in exactly the order in which they were received
    /// from the database engine.
    NoSort,
    /// Gathers all output from the database engine then sorts it by rows.
    RowSort,
    /// It works like rowsort except that it does not honor row groupings. Each individual result
    /// value is sorted on its own.
    ValueSort,
}


/// Whether to print timestamp in default SQLlogictest format or ISO microseconds.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TimestampFormat {
    /// The default option for compatibility with SQLite, DuckDB
    Default,
    Iso,
}

impl ControlItem for SortMode {
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind> {
        match s {
            "nosort" => Ok(Self::NoSort),
            "rowsort" => Ok(Self::RowSort),
            "valuesort" => Ok(Self::ValueSort),
            _ => Err(ParseErrorKind::InvalidSortMode(s.to_string())),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::NoSort => "nosort",
            Self::RowSort => "rowsort",
            Self::ValueSort => "valuesort",
        }
    }
}

/// The error type for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
#[error("parse error at {loc}: {kind}")]
pub struct ParseError {
    kind: ParseErrorKind,
    loc: Location,
}

impl ParseError {
    /// Returns the corresponding [`ParseErrorKind`] for this error.
    pub fn kind(&self) -> ParseErrorKind {
        self.kind.clone()
    }

    /// Returns the location from which the error originated.
    pub fn location(&self) -> Location {
        self.loc.clone()
    }
}

/// The error kind for parsing sqllogictest.
#[derive(thiserror::Error, Debug, Eq, PartialEq, Clone)]
#[non_exhaustive]
pub enum ParseErrorKind {
    #[error("unexpected token: {0:?}")]
    UnexpectedToken(String),
    #[error("unexpected EOF")]
    UnexpectedEOF,
    #[error("invalid sort mode: {0:?}")]
    InvalidSortMode(String),
    #[error("invalid line: {0:?}")]
    InvalidLine(String),
    #[error("invalid type character: {0:?} in type string")]
    InvalidType(char),
    #[error("invalid number: {0:?}")]
    InvalidNumber(String),
    #[error("invalid error message: {0:?}")]
    InvalidErrorMessage(String),
    #[error("duplicated error messages after error` and under `----`")]
    DuplicatedErrorMessage,
    #[error("statement should have no result, use `query` instead")]
    StatementHasResults,
    #[error("invalid duration: {0:?}")]
    InvalidDuration(String),
    #[error("invalid control: {0:?}")]
    InvalidControl(String),
    #[error("invalid include file pattern: {0}")]
    InvalidIncludeFile(String),
    #[error("no files found for include file pattern: {0:?}")]
    EmptyIncludeFile(String),
    #[error("no such file")]
    FileNotFound,
}

impl ParseErrorKind {
    fn at(self, loc: Location) -> ParseError {
        ParseError { kind: self, loc }
    }
}

/// Parse a sqllogictest script into a list of records.
pub fn parse<T: ColumnType>(script: &str) -> Result<Vec<Record<T>>, ParseError> {
    parse_inner(&Location::new("<unknown>", 0), script)
}

/// Parse a sqllogictest script into a list of records with a given script name.
pub fn parse_with_name<T: ColumnType>(
    script: &str,
    name: impl Into<Arc<str>>,
) -> Result<Vec<Record<T>>, ParseError> {
    parse_inner(&Location::new(name, 0), script)
}

#[allow(clippy::collapsible_match)]
fn parse_inner<T: ColumnType>(loc: &Location, script: &str) -> Result<Vec<Record<T>>, ParseError> {
    let mut lines = script.lines().enumerate().peekable();
    let mut records = vec![];
    let mut conditions = vec![];
    let mut connection = Connection::Default;
    let mut comments = vec![];

    while let Some((num, line)) = lines.next() {
        if let Some(text) = line.strip_prefix('#') {
            comments.push(text.to_string());
            if lines.peek().is_none() {
                // Special handling for the case where the last line is a comment.
                records.push(Record::Comment(comments));
                break;
            }
            continue;
        }
        if !comments.is_empty() {
            records.push(Record::Comment(comments));
            comments = vec![];
        }

        if line.is_empty() {
            records.push(Record::Newline);
            continue;
        }

        let mut loc = loc.clone();
        loc.line = num as u32 + 1;

        let tokens: Vec<&str> = line.split_whitespace().collect();
        match tokens.as_slice() {
            [] => continue,
            ["include", included] => records.push(Record::Include {
                loc,
                filename: included.to_string(),
            }),
            ["halt"] => {
                records.push(Record::Halt { loc });
            }
            ["subtest", name] => {
                records.push(Record::Subtest {
                    loc,
                    name: name.to_string(),
                });
            }
            ["sleep", dur] => {
                records.push(Record::Sleep {
                    duration: humantime::parse_duration(dur).map_err(|_| {
                        ParseErrorKind::InvalidDuration(dur.to_string()).at(loc.clone())
                    })?,
                    loc,
                });
            }
            ["skipif", label] => {
                let cond = Condition::SkipIf {
                    label: label.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["onlyif", label] => {
                let cond = Condition::OnlyIf {
                    label: label.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["connection", name] => {
                let conn = Connection::new(name);
                connection = conn.clone();
                records.push(Record::Connection(conn));
            }
            ["statement", res @ ..] => {
                let mut expected = match res {
                    ["ok"] => StatementExpect::Ok,
                    ["error", tokens @ ..] => {
                        let error = ExpectedError::parse_inline_tokens(tokens)
                            .map_err(|e| e.at(loc.clone()))?;
                        StatementExpect::Error(error)
                    }
                    ["count", count_str] => {
                        let count = count_str.parse::<u64>().map_err(|_| {
                            ParseErrorKind::InvalidNumber((*count_str).into()).at(loc.clone())
                        })?;
                        StatementExpect::Count(count)
                    }
                    _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
                };
                let (sql, has_results) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;
                if has_results {
                    if let StatementExpect::Error(e) = &mut expected {
                        // If no inline error message is specified, it might be a multiline error.
                        if e.is_empty() {
                            *e = parse_multiline_error(&mut lines);
                        } else {
                            return Err(ParseErrorKind::DuplicatedErrorMessage.at(loc.clone()));
                        }
                    } else {
                        return Err(ParseErrorKind::StatementHasResults.at(loc.clone()));
                    }
                }
                records.push(Record::Statement {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    connection: std::mem::take(&mut connection),
                    sql,
                    expected,
                });
            }
            ["query", res @ ..] => {
                let mut expected = match res {
                    ["error", tokens @ ..] => {
                        let error = ExpectedError::parse_inline_tokens(tokens)
                            .map_err(|e| e.at(loc.clone()))?;
                        QueryExpect::Error(error)
                    }
                    [type_str, res @ ..] => {
                        let types = type_str
                            .chars()
                            .map(|ch| {
                                T::from_char(ch)
                                    .ok_or_else(|| ParseErrorKind::InvalidType(ch).at(loc.clone()))
                            })
                            .try_collect()?;
                        let sort_mode = res
                            .first()
                            .map(|&s| SortMode::try_from_str(s))
                            .transpose()
                            .map_err(|e| e.at(loc.clone()))?;
                        let label = res.get(1).map(|s| s.to_string());
                        QueryExpect::Results {
                            types,
                            sort_mode,
                            label,
                            results: Vec::new(),
                        }
                    }
                    [] => QueryExpect::empty_results(),
                };

                // The SQL for the query is found on second and subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let (sql, has_result) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;
                if has_result {
                    match &mut expected {
                        // Lines following the "----" are expected results of the query, one value
                        // per line.
                        QueryExpect::Results { results, .. } => {
                            for (_, line) in &mut lines {
                                if line.is_empty() {
                                    break;
                                }
                                results.push(line.to_string());
                            }
                        }
                        // If no inline error message is specified, it might be a multiline error.
                        QueryExpect::Error(e) => {
                            if e.is_empty() {
                                *e = parse_multiline_error(&mut lines);
                            } else {
                                return Err(ParseErrorKind::DuplicatedErrorMessage.at(loc.clone()));
                            }
                        }
                    }
                }
                records.push(Record::Query {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    connection: std::mem::take(&mut connection),
                    sql,
                    expected,
                });
            }
            ["system", "ok"] => {
                // TODO: we don't support asserting error message for system command
                // The command is found on second and subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let (command, has_result) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;
                let stdout = if has_result {
                    Some(parse_multiple_result(&mut lines))
                } else {
                    None
                };
                records.push(Record::System {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    command,
                    stdout,
                });
            }
            ["control", res @ ..] => match res {
                ["sortmode", sort_mode] => match SortMode::try_from_str(sort_mode) {
                    Ok(sort_mode) => records.push(Record::Control(Control::SortMode(sort_mode))),
                    Err(k) => return Err(k.at(loc)),
                },
                ["substitution", on_off] => match bool::try_from_str(on_off) {
                    Ok(on_off) => records.push(Record::Control(Control::Substitution(on_off))),
                    Err(k) => return Err(k.at(loc)),
                },
                ["iso_timestamp", on_off] => match bool::try_from_str(on_off) {
                    Ok(on_off) => records.push(Record::Control(Control::IsoTimestamp(on_off))),
                    Err(k) => return Err(k.at(loc)),
                },
                _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
            },
            ["hash-threshold", threshold] => {
                records.push(Record::HashThreshold {
                    loc: loc.clone(),
                    threshold: threshold.parse::<u64>().map_err(|_| {
                        ParseErrorKind::InvalidNumber((*threshold).into()).at(loc.clone())
                    })?,
                });
            }
            ["require", _name] => {
                comments.push(line.to_string());
            }
            _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
        }
    }
    Ok(records)
}

/// Parse a sqllogictest file. The included scripts are inserted after the `include` record.
pub fn parse_file<T: ColumnType>(filename: impl AsRef<Path>) -> Result<Vec<Record<T>>, ParseError> {
    let filename = filename.as_ref().to_str().unwrap();
    parse_file_inner(Location::new(filename, 0))
}

fn parse_file_inner<T: ColumnType>(loc: Location) -> Result<Vec<Record<T>>, ParseError> {
    let path = Path::new(loc.file());
    if !path.exists() {
        return Err(ParseErrorKind::FileNotFound.at(loc.clone()));
    }
    let script = std::fs::read_to_string(path).unwrap();
    let mut records = vec![];
    for rec in parse_inner(&loc, &script)? {
        records.push(rec.clone());

        if let Record::Include { filename, loc } = rec {
            let complete_filename = {
                let mut path_buf = path.to_path_buf();
                path_buf.pop();
                path_buf.push(filename.clone());
                path_buf.as_os_str().to_string_lossy().to_string()
            };

            let mut iter = glob::glob(&complete_filename)
                .map_err(|e| ParseErrorKind::InvalidIncludeFile(e.to_string()).at(loc.clone()))?
                .peekable();
            if iter.peek().is_none() {
                return Err(ParseErrorKind::EmptyIncludeFile(filename).at(loc.clone()));
            }
            for included_file in iter {
                let included_file = included_file.map_err(|e| {
                    ParseErrorKind::InvalidIncludeFile(e.to_string()).at(loc.clone())
                })?;
                let included_file = included_file.as_os_str().to_string_lossy().to_string();

                records.push(Record::Injected(Injected::BeginInclude(
                    included_file.clone(),
                )));
                records.extend(parse_file_inner(loc.include(&included_file))?);
                records.push(Record::Injected(Injected::EndInclude(included_file)));
            }
        }
    }
    Ok(records)
}

/// Parse one or more lines until empty line or a delimiter.
fn parse_lines<'a>(
    lines: &mut impl Iterator<Item=(usize, &'a str)>,
    loc: &Location,
    delimiter: Option<&str>,
) -> Result<(String, bool), ParseError> {
    let mut found_delimiter = false;
    let mut out = match lines.next() {
        Some((_, line)) => Ok(line.into()),
        None => Err(ParseErrorKind::UnexpectedEOF.at(loc.clone().next_line())),
    }?;

    for (_, line) in lines {
        if line.is_empty() {
            break;
        }
        if let Some(delimiter) = delimiter {
            if line == delimiter {
                found_delimiter = true;
                break;
            }
        }
        out += "\n";
        out += line;
    }

    Ok((out, found_delimiter))
}

/// Parse multiline output under `----`.
fn parse_multiple_result<'a>(
    lines: &mut Peekable<impl Iterator<Item=(usize, &'a str)>>,
) -> String {
    let mut results = String::new();

    while let Some((_, line)) = lines.next() {
        // empty line
        if line.is_empty() {
            lines.next();
            break;
        }
        results += line;
        results.push('\n');
    }

    results.trim().to_string()
}

/// Parse multiline error message under `----`.
fn parse_multiline_error<'a>(
    lines: &mut Peekable<impl Iterator<Item=(usize, &'a str)>>,
) -> ExpectedError {
    let lines = parse_multiple_result(lines);
    if lines.is_empty() {
        ExpectedError::Empty
    } else {
        ExpectedError::Multiline(lines)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::DefaultColumnType;

    #[test]
    fn test_trailing_comment() {
        let script = "\
# comment 1
#  comment 2
";
        let records = parse::<DefaultColumnType>(script).unwrap();
        assert_eq!(
            records,
            vec![Record::Comment(vec![
                " comment 1".to_string(),
                "  comment 2".to_string(),
            ]), ]
        );
    }

    #[test]
    fn test_include_glob() {
        let records =
            parse_file::<DefaultColumnType>("../tests/slt/include/include_1.slt").unwrap();
        assert_eq!(15, records.len());
    }

    #[test]
    fn test_basic() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/basic.slt")
    }

    #[test]
    fn test_condition() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/condition.slt")
    }

    #[test]
    fn test_file_level_sort_mode() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/file_level_sort_mode.slt")
    }

    #[test]
    fn test_rowsort() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/rowsort.slt")
    }

    #[test]
    fn test_substitution() {
        parse_roundtrip::<DefaultColumnType>("../tests/substitution/basic.slt")
    }

    #[test]
    fn test_test_dir_escape() {
        parse_roundtrip::<DefaultColumnType>("../tests/test_dir_escape/test_dir_escape.slt")
    }

    #[test]
    fn test_validator() {
        parse_roundtrip::<DefaultColumnType>("../tests/validator/validator.slt")
    }

    #[test]
    fn test_custom_type() {
        parse_roundtrip::<CustomColumnType>("../tests/custom_type/custom_type.slt")
    }

    #[test]
    fn test_system_command() {
        parse_roundtrip::<DefaultColumnType>("../tests/system_command/system_command.slt")
    }

    #[test]
    fn test_fail_unknown_type() {
        let script = "\
query IA
select * from unknown_type
----
";

        let error_kind = parse::<CustomColumnType>(script).unwrap_err().kind;

        assert_eq!(error_kind, ParseErrorKind::InvalidType('A'));
    }

    #[test]
    fn test_parse_no_types() {
        let script = "\
query
select * from foo;
----
";
        let records = parse::<DefaultColumnType>(script).unwrap();

        assert_eq!(
            records,
            vec![Record::Query {
                loc: Location::new("<unknown>", 1),
                conditions: vec![],
                connection: Connection::Default,
                sql: "select * from foo;".to_string(),
                expected: QueryExpect::empty_results(),
            }]
        );
    }

    /// Verifies Display impl is consistent with parsing by ensuring
    /// roundtrip parse(unparse(parse())) is consistent
    #[track_caller]
    fn parse_roundtrip<T: ColumnType>(filename: impl AsRef<Path>) {
        let filename = filename.as_ref();
        let records = parse_file::<T>(filename).expect("parsing to complete");

        let unparsed = records
            .iter()
            .map(|record| record.to_string())
            .collect::<Vec<_>>();

        let output_contents = unparsed.join("\n");

        // The original and parsed records should be logically equivalent
        let mut output_file = tempfile::NamedTempFile::new().expect("Error creating tempfile");
        output_file
            .write_all(output_contents.as_bytes())
            .expect("Unable to write file");
        output_file.flush().unwrap();

        let output_path = output_file.into_temp_path();
        let reparsed_records =
            parse_file(&output_path).expect("reparsing to complete successfully");

        let records = normalize_filename(records);
        let reparsed_records = normalize_filename(reparsed_records);

        pretty_assertions::assert_eq!(records, reparsed_records, "Mismatch in reparsed records");
    }

    /// Replaces the actual filename in all Records with
    /// "__FILENAME__" so different files with the same contents can
    /// compare equal
    fn normalize_filename<T: ColumnType>(records: Vec<Record<T>>) -> Vec<Record<T>> {
        records
            .into_iter()
            .map(|mut record| {
                match &mut record {
                    Record::Include { loc, .. } => normalize_loc(loc),
                    Record::Statement { loc, .. } => normalize_loc(loc),
                    Record::System { loc, .. } => normalize_loc(loc),
                    Record::Query { loc, .. } => normalize_loc(loc),
                    Record::Sleep { loc, .. } => normalize_loc(loc),
                    Record::Subtest { loc, .. } => normalize_loc(loc),
                    Record::Halt { loc, .. } => normalize_loc(loc),
                    Record::HashThreshold { loc, .. } => normalize_loc(loc),
                    // even though these variants don't include a
                    // location include them in this match statement
                    // so if new variants are added, this match
                    // statement must be too.
                    Record::Condition(_)
                    | Record::Connection(_)
                    | Record::Comment(_)
                    | Record::Control(_)
                    | Record::Newline
                    | Record::Injected(_) => {}
                };
                record
            })
            .collect()
    }

    // Normalize a location
    fn normalize_loc(loc: &mut Location) {
        loc.file = Arc::from("__FILENAME__");
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    pub enum CustomColumnType {
        Integer,
        Boolean,
    }

    impl ColumnType for CustomColumnType {
        fn from_char(value: char) -> Option<Self> {
            match value {
                'I' => Some(Self::Integer),
                'B' => Some(Self::Boolean),
                _ => None,
            }
        }

        fn to_char(&self) -> char {
            match self {
                Self::Integer => 'I',
                Self::Boolean => 'B',
            }
        }
    }
}
