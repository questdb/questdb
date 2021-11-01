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

#ifndef ASMJIT_CORE_ERRORHANDLER_H_INCLUDED
#define ASMJIT_CORE_ERRORHANDLER_H_INCLUDED

#include "../core/globals.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_error_handling
//! \{

// ============================================================================
// [Forward Declarations]
// ============================================================================

class BaseEmitter;

// ============================================================================
// [asmjit::ErrorHandler]
// ============================================================================

//! Error handler can be used to override the default behavior of error handling.
//!
//! It's available to all classes that inherit `BaseEmitter`. Override
//! \ref ErrorHandler::handleError() to implement your own error handler.
//!
//! The following use-cases are supported:
//!
//!   - Record the error and continue code generation. This is the simplest
//!     approach that can be used to at least log possible errors.
//!   - Throw an exception. AsmJit doesn't use exceptions and is completely
//!     exception-safe, but it's perfectly legal to throw an exception from
//!     the error handler.
//!   - Use plain old C's `setjmp()` and `longjmp()`. Asmjit always puts Assembler,
//!     Builder and Compiler to a consistent state before calling \ref handleError(),
//!     so `longjmp()` can be used without issues to cancel the code-generation if
//!     an error occurred. This method can be used if exception handling in your
//!     project is turned off and you still want some comfort. In most cases it
//!     should be safe as AsmJit uses \ref Zone memory and the ownership of memory
//!     it allocates always ends with the instance that allocated it. If using this
//!     approach please never jump outside the life-time of \ref CodeHolder and
//!     \ref BaseEmitter.
//!
//! \ref ErrorHandler can be attached to \ref CodeHolder or \ref BaseEmitter,
//! which has a priority. The example below uses error handler that just prints
//! the error, but lets AsmJit continue:
//!
//! ```
//! // Error Handling #1 - Logging and returing Error.
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! // Error handler that just prints the error and lets AsmJit ignore it.
//! class SimpleErrorHandler : public ErrorHandler {
//! public:
//!   Error err;
//!
//!   inline SimpleErrorHandler() : err(kErrorOk) {}
//!
//!   void handleError(Error err, const char* message, BaseEmitter* origin) override {
//!     this->err = err;
//!     fprintf(stderr, "ERROR: %s\n", message);
//!   }
//! };
//!
//! int main() {
//!   JitRuntime rt;
//!   SimpleErrorHandler eh;
//!
//!   CodeHolder code;
//!   code.init(rt.environment());
//!   code.setErrorHandler(&eh);
//!
//!   // Try to emit instruction that doesn't exist.
//!   x86::Assembler a(&code);
//!   a.emit(x86::Inst::kIdMov, x86::xmm0, x86::xmm1);
//!
//!   if (eh.err) {
//!     // Assembler failed!
//!     return 1;
//!   }
//!
//!   return 0;
//! }
//! ```
//!
//! If error happens during instruction emitting / encoding the assembler behaves
//! transactionally - the output buffer won't advance if encoding failed, thus
//! either a fully encoded instruction or nothing is emitted. The error handling
//! shown above is useful, but it's still not the best way of dealing with errors
//! in AsmJit. The following example shows how to use exception handling to handle
//! errors in a more C++ way:
//!
//! ```
//! // Error Handling #2 - Throwing an exception.
//! #include <asmjit/x86.h>
//! #include <exception>
//! #include <string>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! // Error handler that throws a user-defined `AsmJitException`.
//! class AsmJitException : public std::exception {
//! public:
//!   Error err;
//!   std::string message;
//!
//!   AsmJitException(Error err, const char* message) noexcept
//!     : err(err),
//!       message(message) {}
//!
//!   const char* what() const noexcept override { return message.c_str(); }
//! };
//!
//! class ThrowableErrorHandler : public ErrorHandler {
//! public:
//!   // Throw is possible, functions that use ErrorHandler are never 'noexcept'.
//!   void handleError(Error err, const char* message, BaseEmitter* origin) override {
//!     throw AsmJitException(err, message);
//!   }
//! };
//!
//! int main() {
//!   JitRuntime rt;
//!   ThrowableErrorHandler eh;
//!
//!   CodeHolder code;
//!   code.init(rt.environment());
//!   code.setErrorHandler(&eh);
//!
//!   x86::Assembler a(&code);
//!
//!   // Try to emit instruction that doesn't exist.
//!   try {
//!     a.emit(x86::Inst::kIdMov, x86::xmm0, x86::xmm1);
//!   }
//!   catch (const AsmJitException& ex) {
//!     printf("EXCEPTION THROWN: %s\n", ex.what());
//!     return 1;
//!   }
//!
//!   return 0;
//! }
//! ```
//!
//! If C++ exceptions are not what you like or your project turns off them
//! completely there is still a way of reducing the error handling to a minimum
//! by using a standard setjmp/longjmp approach. AsmJit is exception-safe and
//! cleans up everything before calling the ErrorHandler, so any approach is
//! safe. You can simply jump from the error handler without causing any
//! side-effects or memory leaks. The following example demonstrates how it
//! could be done:
//!
//! ```
//! // Error Handling #3 - Using setjmp/longjmp if exceptions are not allowed.
//! #include <asmjit/x86.h>
//! #include <setjmp.h>
//! #include <stdio.h>
//!
//! class LongJmpErrorHandler : public asmjit::ErrorHandler {
//! public:
//!   inline LongJmpErrorHandler() : err(asmjit::kErrorOk) {}
//!
//!   void handleError(asmjit::Error err, const char* message, asmjit::BaseEmitter* origin) override {
//!     this->err = err;
//!     longjmp(state, 1);
//!   }
//!
//!   jmp_buf state;
//!   asmjit::Error err;
//! };
//!
//! int main(int argc, char* argv[]) {
//!   using namespace asmjit;
//!
//!   JitRuntime rt;
//!   LongJmpErrorHandler eh;
//!
//!   CodeHolder code;
//!   code.init(rt.rt.environment());
//!   code.setErrorHandler(&eh);
//!
//!   x86::Assembler a(&code);
//!
//!   if (!setjmp(eh.state)) {
//!     // Try to emit instruction that doesn't exist.
//!     a.emit(x86::Inst::kIdMov, x86::xmm0, x86::xmm1);
//!   }
//!   else {
//!     Error err = eh.err;
//!     printf("ASMJIT ERROR: 0x%08X [%s]\n", err, DebugUtils::errorAsString(err));
//!   }
//!
//!   return 0;
//! }
//! ```
class ASMJIT_VIRTAPI ErrorHandler {
public:
  ASMJIT_BASE_CLASS(ErrorHandler)

  // --------------------------------------------------------------------------
  // [Construction / Destruction]
  // --------------------------------------------------------------------------

  //! Creates a new `ErrorHandler` instance.
  ASMJIT_API ErrorHandler() noexcept;
  //! Destroys the `ErrorHandler` instance.
  ASMJIT_API virtual ~ErrorHandler() noexcept;

  // --------------------------------------------------------------------------
  // [Handle Error]
  // --------------------------------------------------------------------------

  //! Error handler (must be reimplemented).
  //!
  //! Error handler is called after an error happened and before it's propagated
  //! to the caller. There are multiple ways how the error handler can be used:
  //!
  //! 1. User-based error handling without throwing exception or using C's
  //!    `longjmp()`. This is for users that don't use exceptions and want
  //!    customized error handling.
  //!
  //! 2. Throwing an exception. AsmJit doesn't use exceptions and is completely
  //!    exception-safe, but you can throw exception from your error handler if
  //!    this way is the preferred way of handling errors in your project.
  //!
  //! 3. Using plain old C's `setjmp()` and `longjmp()`. Asmjit always puts
  //!    `BaseEmitter` to a consistent state before calling `handleError()`
  //!    so `longjmp()` can be used without any issues to cancel the code
  //!    generation if an error occurred. There is no difference between
  //!    exceptions and `longjmp()` from AsmJit's perspective, however,
  //!    never jump outside of `CodeHolder` and `BaseEmitter` scope as you
  //!    would leak memory.
  virtual void handleError(Error err, const char* message, BaseEmitter* origin) = 0;
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_ERRORHANDLER_H_INCLUDED

