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
use crate::allocator::{take_last_alloc_error, AllocFailure};
use qdb_core::cairo::CairoException;
use qdb_core::error::{CoreError, CoreErrorReason};
use std::alloc::AllocError;
use std::backtrace::{Backtrace, BacktraceStatus};
use std::collections::TryReserveError;
use std::fmt::{Debug, Display, Formatter, Write};
use std::sync::Arc;

/// Cause of a parquet error.
#[derive(Debug, Clone)]
pub enum ParquetErrorReason {
    OutOfMemory(Option<AllocFailure>),
    Parquet2(parquet2::error::Error),
    QdbMeta(Arc<serde_json::Error>),
    Layout,
    Unsupported,
    InvalidType,
    InvalidLayout,
    Utf8Decode(std::str::Utf8Error),
    Utf16Decode(std::char::DecodeUtf16Error),
    Io(Arc<std::io::Error>),

    #[cfg(test)]
    Arrow(Arc<arrow::error::ArrowError>),

    #[cfg(test)]
    ArrowParquet(Arc<parquet::errors::ParquetError>),
}

impl From<CoreErrorReason> for ParquetErrorReason {
    fn from(reason: CoreErrorReason) -> Self {
        match reason {
            CoreErrorReason::InvalidType => ParquetErrorReason::InvalidType,
            CoreErrorReason::InvalidLayout => ParquetErrorReason::InvalidLayout,
            CoreErrorReason::Io(err) => ParquetErrorReason::Io(err),
        }
    }
}

impl ParquetErrorReason {
    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParquetErrorReason::OutOfMemory(maybe_err) => maybe_err
                .as_ref()
                .map(|err| err as &(dyn std::error::Error + 'static)),
            ParquetErrorReason::Parquet2(err) => Some(err),
            ParquetErrorReason::QdbMeta(err) => Some(err.as_ref()),
            ParquetErrorReason::Utf8Decode(err) => Some(err),
            ParquetErrorReason::Utf16Decode(err) => Some(err),
            ParquetErrorReason::Io(err) => Some(err.as_ref()),
            #[cfg(test)]
            ParquetErrorReason::Arrow(err) => Some(err.as_ref()),
            #[cfg(test)]
            ParquetErrorReason::ArrowParquet(err) => Some(err.as_ref()),
            _ => None,
        }
    }

    #[track_caller]
    pub fn into_err(self) -> ParquetError {
        ParquetError::new(self)
    }
}

/// An error reading or writing parquet.
#[derive(Clone)]
pub struct ParquetError {
    /// What caused the error.
    reason: ParquetErrorReason,

    /// Initial message (if any) and
    /// stack of additional contextual information,
    /// printed in reverse order.
    context: Vec<String>,

    /// Root location of the error.
    backtrace: Arc<Backtrace>,
}

impl ParquetError {
    fn fmt_msg<W: Write>(&self, f: &mut W) -> std::fmt::Result {
        // Print the context first in reverse order.
        let source = self.reason.source();
        let last_index = self.context.len().saturating_sub(1);
        for (index, context) in self.context.iter().rev().enumerate() {
            if index == last_index {
                write!(f, "{context}")?;
            } else {
                write!(f, "{context}: ")?;
            }
        }

        // Then the source's cause, if there is one.
        if let Some(source) = source {
            if self.context.is_empty() {
                write!(f, "{source}")?;
            } else {
                write!(f, ": {source}")?;
            }
        }
        Ok(())
    }

    fn build_out_of_memory() -> ParquetError {
        let last_err = take_last_alloc_error();
        let no_last_err = last_err.is_none();
        let mut err = Self::new(ParquetErrorReason::OutOfMemory(last_err));
        if no_last_err {
            err.add_context("memory allocation failed");
        }
        err
    }

    pub fn into_cairo_exception(self) -> CairoException {
        CairoException::new(self.to_string())
            .out_of_memory(matches!(self.reason, ParquetErrorReason::OutOfMemory(_)))
            .backtrace(self.backtrace.clone())
    }
}

impl ParquetError {
    #[track_caller]
    pub fn new(reason: ParquetErrorReason) -> Self {
        Self {
            reason,
            context: Vec::new(),
            backtrace: Backtrace::capture().into(),
        }
    }

    #[track_caller]
    pub fn with_descr(reason: ParquetErrorReason, descr: impl Into<String>) -> Self {
        Self {
            reason,
            context: vec![descr.into()],
            backtrace: Backtrace::capture().into(),
        }
    }

    #[cfg(test)]
    pub fn reason(&self) -> &ParquetErrorReason {
        &self.reason
    }

    pub fn add_context(&mut self, context: impl Into<String>) {
        self.context.push(context.into());
    }
}

impl Debug for ParquetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ParquetError\n    Reason: {:?}", self.reason)?;
        writeln!(f, "    Context:")?;
        for line in self.context.iter().rev() {
            writeln!(f, "        {line}")?;
        }
        if self.backtrace.status() == BacktraceStatus::Captured {
            writeln!(f, "    Backtrace:\n{}", self.backtrace)?;
        }
        Ok(())
    }
}

impl Display for ParquetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_msg(f)?;
        Ok(())
    }
}

impl std::error::Error for ParquetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.reason.source()
    }
}

impl From<AllocError> for ParquetError {
    fn from(_: AllocError) -> Self {
        Self::build_out_of_memory()
    }
}

impl From<TryReserveError> for ParquetError {
    fn from(_: TryReserveError) -> Self {
        Self::build_out_of_memory()
    }
}

impl From<parquet2::error::Error> for ParquetError {
    fn from(source: parquet2::error::Error) -> Self {
        Self::new(ParquetErrorReason::Parquet2(source))
    }
}

impl From<std::io::Error> for ParquetError {
    fn from(e: std::io::Error) -> Self {
        Self::new(ParquetErrorReason::Io(Arc::new(e)))
    }
}

impl From<CoreError> for ParquetError {
    fn from(e: CoreError) -> Self {
        let (cause, context, backtrace) = e.into_tuple();
        let cause: ParquetErrorReason = cause.into();
        Self { reason: cause, context, backtrace }
    }
}

#[cfg(test)]
impl From<arrow::error::ArrowError> for ParquetError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::new(ParquetErrorReason::Arrow(Arc::new(e)))
    }
}

#[cfg(test)]
impl From<parquet::errors::ParquetError> for ParquetError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::new(ParquetErrorReason::ArrowParquet(Arc::new(e)))
    }
}

pub type ParquetResult<T> = Result<T, ParquetError>;

pub trait ParquetErrorExt<T> {
    fn context(self, context: &str) -> ParquetResult<T>;
    fn with_context<F>(self, context: F) -> ParquetResult<T>
    where
        F: FnOnce(&mut ParquetError) -> String;
}

impl<T, E> ParquetErrorExt<T> for Result<T, E>
where
    E: Into<ParquetError>,
{
    /// Add a layer of context to the error.
    /// The `context: &str` is copied into a `String` iff the error is an `Err`.
    /// Use the `with_context` method if you need to compute the context lazily.
    fn context(self, context: &str) -> ParquetResult<T> {
        match self {
            Ok(val) => Ok(val),
            Err(e) => {
                let mut err = e.into();
                err.add_context(context);
                Err(err)
            }
        }
    }

    /// Lazily add a layer of context to the error.
    fn with_context<F>(self, context: F) -> ParquetResult<T>
    where
        F: FnOnce(&mut ParquetError) -> String,
    {
        match self {
            Ok(val) => Ok(val),
            Err(e) => {
                let mut err = e.into();
                let context = context(&mut err);
                err.add_context(context);
                Err(err)
            }
        }
    }
}

macro_rules! fmt_err {
    ($cause:ident($inner:expr), $($arg:tt)*) => {
        crate::parquet::error::ParquetError::with_descr(
            crate::parquet::error::ParquetErrorReason::$cause($inner),
            format!($($arg)*))
    };
    ($cause:ident, $($arg:tt)*) => {
        crate::parquet::error::ParquetError::with_descr(
            crate::parquet::error::ParquetErrorReason::$cause,
            format!($($arg)*))
    };
}

pub(crate) use fmt_err;
