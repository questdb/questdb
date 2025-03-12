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
use std::backtrace::{Backtrace, BacktraceStatus};
use std::fmt::{Debug, Display, Formatter, Write};
use std::sync::Arc;

/// Cause of an error.
#[derive(Debug, Clone)]
pub enum CoreErrorCause {
    InvalidColumnType,
    InvalidColumnData,
    Io(Arc<std::io::Error>),
}

impl CoreErrorCause {
    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidColumnType | Self::InvalidColumnData => None,
            Self::Io(err) => Some(err.as_ref()),
        }
    }

    #[track_caller]
    pub fn into_err(self) -> CoreError {
        CoreError::new(self)
    }
}

/// An error reading or writing parquet.
#[derive(Clone)]
pub struct CoreError {
    /// What caused the error.
    cause: CoreErrorCause,

    /// Initial message (if any) and
    /// stack of additional contextual information,
    /// printed in reverse order.
    context: Vec<String>,

    /// Root location of the error.
    backtrace: Arc<Backtrace>,
}

impl CoreError {
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

    pub fn into_tuple(self) -> (CoreErrorCause, Vec<String>, Arc<Backtrace>) {
        (self.cause, self.context, self.backtrace)
    }
}

impl CoreError {
    #[track_caller]
    pub fn new(cause: CoreErrorCause) -> Self {
        Self {
            cause,
            context: Vec::new(),
            backtrace: Backtrace::capture().into(),
        }
    }

    #[track_caller]
    pub fn with_descr(cause: CoreErrorCause, descr: impl Into<String>) -> Self {
        Self {
            cause,
            context: vec![descr.into()],
            backtrace: Backtrace::capture().into(),
        }
    }

    #[cfg(test)]
    pub fn get_cause(&self) -> &CoreErrorCause {
        &self.cause
    }

    pub fn add_context(&mut self, context: impl Into<String>) {
        self.context.push(context.into());
    }
}

impl Debug for CoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CoreError\n    Cause: {:?}", self.cause)?;
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

impl Display for CoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_msg(f)?;
        Ok(())
    }
}

pub struct CoreErrorWithBacktraceDisplay<'a>(&'a CoreError);

impl Display for CoreErrorWithBacktraceDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt_msg_with_backtrace(f)?;
        Ok(())
    }
}

impl std::error::Error for CoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.source()
    }
}

impl From<std::io::Error> for CoreError {
    fn from(e: std::io::Error) -> Self {
        Self::new(CoreErrorCause::Io(Arc::new(e)))
    }
}

pub type CoreResult<T> = Result<T, CoreError>;

pub trait CoreErrorExt<T> {
    fn context(self, context: &str) -> CoreResult<T>;
    fn with_context<F>(self, context: F) -> CoreResult<T>
    where
        F: FnOnce(&mut CoreError) -> String;
}

impl<T, E> CoreErrorExt<T> for Result<T, E>
where
    E: Into<CoreError>,
{
    /// Add a layer of context to the error.
    /// The `context: &str` is copied into a `String` iff the error is an `Err`.
    /// Use the `with_context` method if you need to compute the context lazily.
    fn context(self, context: &str) -> CoreResult<T> {
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
    fn with_context<F>(self, context: F) -> CoreResult<T>
    where
        F: FnOnce(&mut CoreError) -> String,
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
    ($cause: ident, $($arg:tt)*) => {
        crate::error::CoreError::with_descr(
            crate::error::CoreErrorCause::$cause,
            format!($($arg)*))
    };
}

pub(crate) use fmt_err;
