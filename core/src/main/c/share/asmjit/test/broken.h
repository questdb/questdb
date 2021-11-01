// Broken - Lightweight unit testing for C++
//
// This is free and unencumbered software released into the public domain.
//
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
//
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
// For more information, please refer to <http://unlicense.org>

#ifndef BROKEN_H_INCLUDED
#define BROKEN_H_INCLUDED

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utility>

// Hide everything when using Doxygen. Ideally this can be protected by a macro,
// but there is not globally and widely used one across multiple projects.

//! \cond

// ============================================================================
// [Broken - API]
// ============================================================================

struct BrokenAPI {
  //! Entry point of a unit test defined by `UNIT` macro.
  typedef void (*Entry)(void);

  enum Flags : unsigned {
    kFlagFinished = 0x1
  };

  //! Test defined by `UNIT` macro.
  struct Unit {
    Entry entry;
    const char* name;
    int priority;
    unsigned flags;
    Unit* next;
  };

  //! Automatic unit registration by using static initialization.
  struct AutoUnit : Unit {
    inline AutoUnit(Entry entry_, const char* name_, int priority_ = 0, int dummy_ = 0) noexcept {
      // Not used, only to trick `UNIT()` macro.
      (void)dummy_;

      this->entry = entry_;
      this->name = name_;
      this->priority = priority_;
      this->flags = 0;
      this->next = nullptr;
      BrokenAPI::add(this);
    }
  };

  static bool hasArg(const char* name) noexcept;

  //! Register a new unit test (called automatically by `AutoUnit` and `UNIT`).
  static void add(Unit* unit) noexcept;

  //! Set output file to a `file`.
  static void setOutputFile(FILE* file) noexcept;

  //! Initialize `Broken` framework.
  //!
  //! Returns `true` if `run()` should be called.
  static int run(int argc, const char* argv[], Entry onBeforeRun = nullptr, Entry onAfterRun = nullptr);

  //! Log message, adds automatically new line if not present.
  static void info(const char* fmt, ...) noexcept;

  //! Called on `EXPECT()` failure.
  static void fail(const char* file, int line, const char* expression, const char* fmt, ...) noexcept;

  //! Used internally by `EXPECT` macro.
  template<typename T>
  static inline void expect(const char* file, int line, const char* expression, const T& result) noexcept {
    if (!result)
      fail(file, line, expression, nullptr);
  }

  //! Used internally by `EXPECT` macro.
  template<typename T, typename... Args>
  static inline void expect(const char* file, int line, const char* expression, const T& result, const char* fmt, Args&&... args) noexcept {
    if (!result)
      fail(file, line, expression, fmt, std::forward<Args>(args)...);
  }
};

// ============================================================================
// [Broken - Macros]
// ============================================================================

//! Internal macro used by `UNIT()`.
#define BROKEN_UNIT_INTERNAL(NAME, PRIORITY) \
  static void unit_##NAME##_entry(void); \
  static ::BrokenAPI::AutoUnit unit_##NAME##_autoinit(unit_##NAME##_entry, #NAME, PRIORITY); \
  static void unit_##NAME##_entry(void)

//! Stringifies the expression used by EXPECT().
#define BROKEN_STRINFIGY_EXPRESSION_INTERNAL(EXP, ...) #EXP

//! \def UNIT(NAME [, PRIORITY])
//!
//! Define a unit test with an optional priority.
//!
//! `NAME` can only contain ASCII characters, numbers and underscore. It has
//! the same rules as identifiers in C and C++.
//!
//! `PRIORITY` specifies the order in which unit tests are run. Lesses value
//! increases the priority. At the moment all units are first sorted by
//! priority and then by name - this makes the run always deterministic.
#define UNIT(NAME, ...) BROKEN_UNIT_INTERNAL(NAME, __VA_ARGS__ + 0)

//! #define INFO(FORMAT [, ...])
//!
//! Informative message printed to `stdout`.
#define INFO(...) ::BrokenAPI::info(__VA_ARGS__)

//! #define INFO(EXP [, FORMAT [, ...]])
//!
//! Expect `EXP` to be true or evaluates to true, fail otherwise.
#define EXPECT(...) ::BrokenAPI::expect(__FILE__, __LINE__, BROKEN_STRINFIGY_EXPRESSION_INTERNAL(__VA_ARGS__), __VA_ARGS__)

//! \endcond

#endif // BROKEN_H_INCLUDED
