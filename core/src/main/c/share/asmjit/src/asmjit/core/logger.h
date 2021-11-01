// AsmJit - Machine code generation for C++
//
//  * Official AsmJit Home Page: https://asmjit.com
//  * Official Github Repository: https://github.com/asmjit/asmjit
//
// Copyright (c) 2008-2020 The AsmJit Authors
//
// This software is provided 'as-is', without any express or implied
// warranty. In no event will the authors be held liable for any damages
// arising from the use of this software.
//
// Permission is granted to anyone to use this software for any purpose,
// including commercial applications, and to alter it and redistribute it
// freely, subject to the following restrictions:
//
// 1. The origin of this software must not be misrepresented; you must not
//    claim that you wrote the original software. If you use this software
//    in a product, an acknowledgment in the product documentation would be
//    appreciated but is not required.
// 2. Altered source versions must be plainly marked as such, and must not be
//    misrepresented as being the original software.
// 3. This notice may not be removed or altered from any source distribution.

#ifndef ASMJIT_CORE_LOGGING_H_INCLUDED
#define ASMJIT_CORE_LOGGING_H_INCLUDED

#include "../core/inst.h"
#include "../core/string.h"
#include "../core/formatter.h"

#ifndef ASMJIT_NO_LOGGING

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_logging
//! \{

// ============================================================================
// [asmjit::Logger]
// ============================================================================

//! Logging interface.
//!
//! This class can be inherited and reimplemented to fit into your own logging
//! needs. When reimplementing a logger use \ref Logger::_log() method to log
//! customize the output.
//!
//! There are two `Logger` implementations offered by AsmJit:
//!   - \ref FileLogger - logs into a `FILE*`.
//!   - \ref StringLogger - concatenates all logs into a \ref String.
class ASMJIT_VIRTAPI Logger {
public:
  ASMJIT_BASE_CLASS(Logger)
  ASMJIT_NONCOPYABLE(Logger)

  //! Format options.
  FormatOptions _options;

  //! \name Construction & Destruction
  //! \{

  //! Creates a `Logger` instance.
  ASMJIT_API Logger() noexcept;
  //! Destroys the `Logger` instance.
  ASMJIT_API virtual ~Logger() noexcept;

  //! \}

  //! \name Format Options
  //! \{

  //! Returns \ref FormatOptions of this logger.
  inline FormatOptions& options() noexcept { return _options; }
  //! \overload
  inline const FormatOptions& options() const noexcept { return _options; }

  //! Returns formatting flags, see \ref FormatOptions::Flags.
  inline uint32_t flags() const noexcept { return _options.flags(); }
  //! Tests whether the logger has the given `flag` enabled.
  inline bool hasFlag(uint32_t flag) const noexcept { return _options.hasFlag(flag); }
  //! Sets formatting flags to `flags`, see \ref FormatOptions::Flags.
  inline void setFlags(uint32_t flags) noexcept { _options.setFlags(flags); }
  //! Enables the given formatting `flags`, see \ref FormatOptions::Flags.
  inline void addFlags(uint32_t flags) noexcept { _options.addFlags(flags); }
  //! Disables the given formatting `flags`, see \ref FormatOptions::Flags.
  inline void clearFlags(uint32_t flags) noexcept { _options.clearFlags(flags); }

  //! Returns indentation of `type`, see \ref FormatOptions::IndentationType.
  inline uint32_t indentation(uint32_t type) const noexcept { return _options.indentation(type); }
  //! Sets indentation of the given indentation `type` to `n` spaces, see \ref
  //! FormatOptions::IndentationType.
  inline void setIndentation(uint32_t type, uint32_t n) noexcept { _options.setIndentation(type, n); }
  //! Resets indentation of the given indentation `type` to 0 spaces.
  inline void resetIndentation(uint32_t type) noexcept { _options.resetIndentation(type); }

  //! \}

  //! \name Logging Interface
  //! \{

  //! Logs `str` - must be reimplemented.
  //!
  //! The function can accept either a null terminated string if `size` is
  //! `SIZE_MAX` or a non-null terminated string of the given `size`. The
  //! function cannot assume that the data is null terminated and must handle
  //! non-null terminated inputs.
  virtual Error _log(const char* data, size_t size) noexcept = 0;

  //! Logs string `str`, which is either null terminated or having size `size`.
  inline Error log(const char* data, size_t size = SIZE_MAX) noexcept { return _log(data, size); }
  //! Logs content of a string `str`.
  inline Error log(const String& str) noexcept { return _log(str.data(), str.size()); }

  //! Formats the message by using `snprintf()` and then passes the formatted
  //! string to \ref _log().
  ASMJIT_API Error logf(const char* fmt, ...) noexcept;

  //! Formats the message by using `vsnprintf()` and then passes the formatted
  //! string to \ref _log().
  ASMJIT_API Error logv(const char* fmt, va_list ap) noexcept;

  //! \}
};

// ============================================================================
// [asmjit::FileLogger]
// ============================================================================

//! Logger that can log to a `FILE*`.
class ASMJIT_VIRTAPI FileLogger : public Logger {
public:
  ASMJIT_NONCOPYABLE(FileLogger)

  FILE* _file;

  //! \name Construction & Destruction
  //! \{

  //! Creates a new `FileLogger` that logs to `FILE*`.
  ASMJIT_API FileLogger(FILE* file = nullptr) noexcept;
  //! Destroys the `FileLogger`.
  ASMJIT_API virtual ~FileLogger() noexcept;

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the logging output stream or null if the logger has no output
  //! stream.
  inline FILE* file() const noexcept { return _file; }

  //! Sets the logging output stream to `stream` or null.
  //!
  //! \note If the `file` is null the logging will be disabled. When a logger
  //! is attached to `CodeHolder` or any emitter the logging API will always
  //! be called regardless of the output file. This means that if you really
  //! want to disable logging at emitter level you must not attach a logger
  //! to it.
  inline void setFile(FILE* file) noexcept { _file = file; }

  //! \}

  ASMJIT_API Error _log(const char* data, size_t size = SIZE_MAX) noexcept override;
};

// ============================================================================
// [asmjit::StringLogger]
// ============================================================================

//! Logger that stores everything in an internal string buffer.
class ASMJIT_VIRTAPI StringLogger : public Logger {
public:
  ASMJIT_NONCOPYABLE(StringLogger)

  //! Logger data as string.
  String _content;

  //! \name Construction & Destruction
  //! \{

  //! Create new `StringLogger`.
  ASMJIT_API StringLogger() noexcept;
  //! Destroys the `StringLogger`.
  ASMJIT_API virtual ~StringLogger() noexcept;

  //! \}

  //! \name Logger Data Accessors
  //! \{

  //! Returns the content of the logger as \ref String.
  //!
  //! It can be moved, if desired.
  inline String& content() noexcept { return _content; }
  //! \overload
  inline const String& content() const noexcept { return _content; }

  //! Returns aggregated logger data as `char*` pointer.
  //!
  //! The pointer is owned by `StringLogger`, it can't be modified or freed.
  inline const char* data() const noexcept { return _content.data(); }
  //! Returns size of the data returned by `data()`.
  inline size_t dataSize() const noexcept { return _content.size(); }

  //! \}

  //! \name Logger Data Manipulation
  //! \{

  //! Clears the accumulated logger data.
  inline void clear() noexcept { _content.clear(); }

  //! \}

  ASMJIT_API Error _log(const char* data, size_t size = SIZE_MAX) noexcept override;
};

//! \}

ASMJIT_END_NAMESPACE

#endif

#endif // ASMJIT_CORE_LOGGER_H_INCLUDED
