/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
use crate::cairo::CairoException;
use std::alloc::AllocError;
use std::backtrace::{Backtrace, BacktraceStatus};
use std::collections::TryReserveError;
use std::fmt::{Debug, Display, Formatter, Write};
use std::sync::Arc;

/// Cause of a parquet error.
#[derive(Debug, Clone)]
pub enum ParquetErrorCause {
    OutOfMemory(Option<AllocFailure>),
    Parquet2(parquet2::error::Error),
    QdbMeta(Arc<serde_json::Error>),
    Layout,
    Unsupported,
    Invalid,
    Utf8Decode(std::str::Utf8Error),
    Utf16Decode(std::char::DecodeUtf16Error),
    Io(Arc<std::io::Error>),

    #[cfg(test)]
    Arrow(Arc<arrow::error::ArrowError>),

    #[cfg(test)]
    ArrowParquet(Arc<parquet::errors::ParquetError>),
}

impl ParquetErrorCause {
    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParquetErrorCause::OutOfMemory(maybe_err) => maybe_err
                .as_ref()
                .map(|err| err as &(dyn std::error::Error + 'static)),
            ParquetErrorCause::Parquet2(err) => Some(err),
            ParquetErrorCause::QdbMeta(err) => Some(err.as_ref()),
            ParquetErrorCause::Utf8Decode(err) => Some(err),
            ParquetErrorCause::Utf16Decode(err) => Some(err),
            ParquetErrorCause::Io(err) => Some(err.as_ref()),
            #[cfg(test)]
            ParquetErrorCause::Arrow(err) => Some(err.as_ref()),
            #[cfg(test)]
            ParquetErrorCause::ArrowParquet(err) => Some(err.as_ref()),
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
    cause: ParquetErrorCause,

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
        let source = self.cause.source();
        let last_index = self.context.len().saturating_sub(1);
        for (index, context) in self.context.iter().rev().enumerate() {
            if index == last_index {
                write!(f, "{}", context)?;
            } else {
                write!(f, "{}: ", context)?;
            }
        }

        // Then the source's cause, if there is one.
        if let Some(source) = source {
            if self.context.is_empty() {
                write!(f, "{}", source)?;
            } else {
                write!(f, ": {}", source)?;
            }
        }
        Ok(())
    }

    fn fmt_msg_with_backtrace<W: Write>(&self, f: &mut W) -> std::fmt::Result {
        self.fmt_msg(f)?;
        if self.backtrace.status() == BacktraceStatus::Captured {
            write!(f, "\n{}", self.backtrace)?;
        }
        Ok(())
    }

    fn build_out_of_memory() -> ParquetError {
        let last_err = take_last_alloc_error();
        let no_last_err = last_err.is_none();
        let mut err = Self::new(ParquetErrorCause::OutOfMemory(last_err));
        if no_last_err {
            err.add_context("memory allocation failed");
        }
        err
    }

    pub fn into_cairo_exception(self) -> CairoException {
        CairoException::new(self.to_string())
            .out_of_memory(matches!(self.cause, ParquetErrorCause::OutOfMemory(_)))
            .backtrace(self.backtrace.clone())
    }
}

impl ParquetError {
    #[track_caller]
    pub fn new(cause: ParquetErrorCause) -> Self {
        Self {
            cause,
            context: Vec::new(),
            backtrace: Backtrace::capture().into(),
        }
    }

    #[track_caller]
    pub fn with_descr(cause: ParquetErrorCause, descr: impl Into<String>) -> Self {
        Self {
            cause,
            context: vec![descr.into()],
            backtrace: Backtrace::capture().into(),
        }
    }

    #[cfg(test)]
    pub fn get_cause(&self) -> &ParquetErrorCause {
        &self.cause
    }

    pub fn add_context(&mut self, context: impl Into<String>) {
        self.context.push(context.into());
    }
}

impl Debug for ParquetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ParquetError\n    Cause: {:?}", self.cause)?;
        writeln!(f, "    Context:")?;
        for line in self.context.iter().rev() {
            writeln!(f, "        {}", line)?;
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

pub struct ParquetErrorWithBacktraceDisplay<'a>(&'a ParquetError);

impl Display for ParquetErrorWithBacktraceDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt_msg_with_backtrace(f)?;
        Ok(())
    }
}

impl std::error::Error for ParquetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.source()
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
        Self::new(ParquetErrorCause::Parquet2(source))
    }
}

impl From<std::io::Error> for ParquetError {
    fn from(e: std::io::Error) -> Self {
        Self::new(ParquetErrorCause::Io(Arc::new(e)))
    }
}

#[cfg(test)]
impl From<arrow::error::ArrowError> for ParquetError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::new(ParquetErrorCause::Arrow(Arc::new(e)))
    }
}

#[cfg(test)]
impl From<parquet::errors::ParquetError> for ParquetError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::new(ParquetErrorCause::ArrowParquet(Arc::new(e)))
    }
}

pub type ParquetResult<T> = Result<T, ParquetError>;

pub trait ParquetErrorExt<T> {
    fn context(self, context: &str) -> Self;
    fn with_context<F>(self, context: F) -> Self
    where
        F: FnOnce(&mut ParquetError) -> String;
}

impl<T> ParquetErrorExt<T> for ParquetResult<T> {
    /// Add a layer of context to the error.
    /// The `context: &str` is copied into a `String` iff the error is an `Err`.
    /// Use the `with_context` method if you need to compute the context lazily.
    fn context(self, context: &str) -> Self {
        match self {
            Ok(val) => Ok(val),
            Err(mut err) => {
                err.add_context(context);
                Err(err)
            }
        }
    }

    /// Lazily add a layer of context to the error.
    fn with_context<F>(self, context: F) -> Self
    where
        F: FnOnce(&mut ParquetError) -> String,
    {
        match self {
            Ok(val) => Ok(val),
            Err(mut err) => {
                let context = context(&mut err);
                err.add_context(context);
                Err(err)
            }
        }
    }
}

macro_rules! fmt_err {
    ($cause: ident, $($arg:tt)*) => {
        ParquetError::with_descr(
            crate::parquet::error::ParquetErrorCause::$cause,
            format!($($arg)*))
    };
}

pub(crate) use fmt_err;
