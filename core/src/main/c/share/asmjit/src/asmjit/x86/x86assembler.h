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

#ifndef ASMJIT_X86_X86ASSEMBLER_H_INCLUDED
#define ASMJIT_X86_X86ASSEMBLER_H_INCLUDED

#include "../core/assembler.h"
#include "../x86/x86emitter.h"
#include "../x86/x86operand.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \addtogroup asmjit_x86
//! \{

// ============================================================================
// [asmjit::Assembler]
// ============================================================================

//! X86/X64 assembler implementation.
//!
//! x86::Assembler is a code emitter that emits machine code directly into the
//! \ref CodeBuffer. The assembler is capable of targeting both 32-bit and 64-bit
//! instruction sets, the instruction set can be configured through \ref CodeHolder.
//!
//! ### Basics
//!
//! The following example shows a basic use of `x86::Assembler`, how to generate
//! a function that works in both 32-bit and 64-bit modes, and how to connect
//! \ref JitRuntime, \ref CodeHolder, and `x86::Assembler`.
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! // Signature of the generated function.
//! typedef int (*SumFunc)(const int* arr, size_t count);
//!
//! int main() {
//!   JitRuntime rt;                    // Create a runtime specialized for JIT.
//!   CodeHolder code;                  // Create a CodeHolder.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Assembler a(&code);          // Create and attach x86::Assembler to code.
//!
//!   // Decide between 32-bit CDECL, WIN64, and SysV64 calling conventions:
//!   //   32-BIT - passed all arguments by stack.
//!   //   WIN64  - passes first 4 arguments by RCX, RDX, R8, and R9.
//!   //   UNIX64 - passes first 6 arguments by RDI, RSI, RCX, RDX, R8, and R9.
//!   x86::Gp arr, cnt;
//!   x86::Gp sum = x86::eax;           // Use EAX as 'sum' as it's a return register.
//!
//!   if (ASMJIT_ARCH_BITS == 64) {
//!   #if defined(_WIN32)
//!     arr = x86::rcx;                 // First argument (array ptr).
//!     cnt = x86::rdx;                 // Second argument (number of elements)
//!   #else
//!     arr = x86::rdi;                 // First argument (array ptr).
//!     cnt = x86::rsi;                 // Second argument (number of elements)
//!   #endif
//!   }
//!   else {
//!     arr = x86::edx;                 // Use EDX to hold the array pointer.
//!     cnt = x86::ecx;                 // Use ECX to hold the counter.
//!     // Fetch first and second arguments from [ESP + 4] and [ESP + 8].
//!     a.mov(arr, x86::ptr(x86::esp, 4));
//!     a.mov(cnt, x86::ptr(x86::esp, 8));
//!   }
//!
//!   Label Loop = a.newLabel();        // To construct the loop, we need some labels.
//!   Label Exit = a.newLabel();
//!
//!   a.xor_(sum, sum);                 // Clear 'sum' register (shorter than 'mov').
//!   a.test(cnt, cnt);                 // Border case:
//!   a.jz(Exit);                       //   If 'cnt' is zero jump to 'Exit' now.
//!
//!   a.bind(Loop);                     // Start of a loop iteration.
//!   a.add(sum, x86::dword_ptr(arr));  // Add int at [arr] to 'sum'.
//!   a.add(arr, 4);                    // Increment 'arr' pointer.
//!   a.dec(cnt);                       // Decrease 'cnt'.
//!   a.jnz(Loop);                      // If not zero jump to 'Loop'.
//!
//!   a.bind(Exit);                     // Exit to handle the border case.
//!   a.ret();                          // Return from function ('sum' == 'eax').
//!   // ----> x86::Assembler is no longer needed from here and can be destroyed <----
//!
//!   SumFunc fn;
//!   Error err = rt.add(&fn, &code);   // Add the generated code to the runtime.
//!
//!   if (err) return 1;                // Handle a possible error returned by AsmJit.
//!   // ----> CodeHolder is no longer needed from here and can be destroyed <----
//!
//!   static const int array[6] = { 4, 8, 15, 16, 23, 42 };
//!
//!   int result = fn(array, 6);        // Execute the generated code.
//!   printf("%d\n", result);           // Print sum of array (108).
//!
//!   rt.release(fn);                   // Explicitly remove the function from the runtime
//!   return 0;                         // Everything successful...
//! }
//! ```
//!
//! The example should be self-explanatory. It shows how to work with labels,
//! how to use operands, and how to emit instructions that can use different
//! registers based on runtime selection. It implements 32-bit CDECL, WIN64,
//! and SysV64 caling conventions and will work on most X86/X64 environments.
//!
//! Although functions prologs / epilogs can be implemented manually, AsmJit
//! provides utilities that can be used to create function prologs and epilogs
//! automatically, see \ref asmjit_function for more details.
//!
//! ### Instruction Validation
//!
//! Assembler prefers speed over strictness by default. The implementation checks
//! the type of operands and fails if the signature of types is invalid, however,
//! it does only basic checks regarding registers and their groups used in
//! instructions. It's possible to pass operands that don't form any valid
//! signature to the implementation and succeed. This is usually not a problem
//! as Assembler provides typed API so operand types are normally checked by C++
//! compiler at compile time, however, Assembler is fully dynamic and its \ref
//! emit() function can be called with any instruction id, options, and operands.
//! Moreover, it's also possible to form instructions that will be accepted by
//! the typed API, for example by calling `mov(x86::eax, x86::al)` - the C++
//! compiler won't see a problem as both EAX and AL are \ref Gp registers.
//!
//! To help with common mistakes AsmJit allows to activate instruction validation.
//! This feature instruments the Assembler to call \ref InstAPI::validate() before
//! it attempts to encode any instruction.
//!
//! The example below illustrates how validation can be turned on:
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! int main(int argc, char* argv[]) {
//!   JitRuntime rt;                    // Create a runtime specialized for JIT.
//!   CodeHolder code;                  // Create a CodeHolder.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Assembler a(&code);          // Create and attach x86::Assembler to code.
//!
//!   // Enable strict validation.
//!   a.addValidationOptions(BaseEmitter::kValidationOptionAssembler);
//!
//!   // Try to encode invalid or ill-formed instructions.
//!   Error err;
//!
//!   // Invalid instruction.
//!   err = a.mov(x86::eax, x86::al);
//!   printf("Status: %s\n", DebugUtils::errorAsString(err));
//!
//!   // Invalid instruction.
//!   err = a.emit(x86::Inst::kIdMovss, x86::eax, x86::xmm0);
//!   printf("Status: %s\n", DebugUtils::errorAsString(err));
//!
//!   // Ambiguous operand size - the pointer requires size.
//!   err = a.inc(x86::ptr(x86::rax), 1);
//!   printf("Status: %s\n", DebugUtils::errorAsString(err));
//!
//!   return 0;
//! }
//! ```
//!
//! ### Native Registers
//!
//! All emitters provide functions to construct machine-size registers depending
//! on the target. This feature is for users that want to write code targeting
//! both 32-bit and 64-bit architectures at the same time. In AsmJit terminology
//! such registers have prefix `z`, so for example on X86 architecture the
//! following native registers are provided:
//!
//!   - `zax` - mapped to either `eax` or `rax`
//!   - `zbx` - mapped to either `ebx` or `rbx`
//!   - `zcx` - mapped to either `ecx` or `rcx`
//!   - `zdx` - mapped to either `edx` or `rdx`
//!   - `zsp` - mapped to either `esp` or `rsp`
//!   - `zbp` - mapped to either `ebp` or `rbp`
//!   - `zsi` - mapped to either `esi` or `rsi`
//!   - `zdi` - mapped to either `edi` or `rdi`
//!
//! They are accessible through \ref x86::Assembler, \ref x86::Builder, and
//! \ref x86::Compiler. The example below illustrates how to use this feature:
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! typedef int (*Func)(void);
//!
//! int main(int argc, char* argv[]) {
//!   JitRuntime rt;                    // Create a runtime specialized for JIT.
//!   CodeHolder code;                  // Create a CodeHolder.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Assembler a(&code);          // Create and attach x86::Assembler to code.
//!
//!   // Let's get these registers from x86::Assembler.
//!   x86::Gp zbp = a.zbp();
//!   x86::Gp zsp = a.zsp();
//!
//!   int stackSize = 32;
//!
//!   // Function prolog.
//!   a.push(zbp);
//!   a.mov(zbp, zsp);
//!   a.sub(zsp, stackSize);
//!
//!   // ... emit some code (this just sets return value to zero) ...
//!   a.xor_(x86::eax, x86::eax);
//!
//!   // Function epilog and return.
//!   a.mov(zsp, zbp);
//!   a.pop(zbp);
//!   a.ret();
//!
//!   // To make the example complete let's call it.
//!   Func fn;
//!   Error err = rt.add(&fn, &code);   // Add the generated code to the runtime.
//!   if (err) return 1;                // Handle a possible error returned by AsmJit.
//!
//!   int result = fn();                // Execute the generated code.
//!   printf("%d\n", result);           // Print the resulting "0".
//!
//!   rt.release(fn);                   // Remove the function from the runtime.
//!   return 0;
//! }
//! ```
//!
//! The example just returns `0`, but the function generated contains a standard
//! prolog and epilog sequence and the function itself reserves 32 bytes of local
//! stack. The advantage is clear - a single code-base can handle multiple targets
//! easily. If you want to create a register of native size dynamically by
//! specifying its id it's also possible:
//!
//! ```
//! void example(x86::Assembler& a) {
//!   x86::Gp zax = a.gpz(x86::Gp::kIdAx);
//!   x86::Gp zbx = a.gpz(x86::Gp::kIdBx);
//!   x86::Gp zcx = a.gpz(x86::Gp::kIdCx);
//!   x86::Gp zdx = a.gpz(x86::Gp::kIdDx);
//!
//!   // You can also change register's id easily.
//!   x86::Gp zsp = zax;
//!   zsp.setId(4); // or x86::Gp::kIdSp.
//! }
//! ```
//!
//! ### Data Embedding
//!
//! x86::Assembler extends the standard \ref BaseAssembler with X86/X64 specific
//! conventions that are often used by assemblers to embed data next to the code.
//! The following functions can be used to embed data:
//!
//!   - \ref x86::Assembler::db() - embeds byte (8 bits) (x86 naming).
//!   - \ref x86::Assembler::dw() - embeds word (16 bits) (x86 naming).
//!   - \ref x86::Assembler::dd() - embeds dword (32 bits) (x86 naming).
//!   - \ref x86::Assembler::dq() - embeds qword (64 bits) (x86 naming).
//!
//!   - \ref BaseAssembler::embedInt8() - embeds int8_t (portable naming).
//!   - \ref BaseAssembler::embedUInt8() - embeds uint8_t (portable naming).
//!   - \ref BaseAssembler::embedInt16() - embeds int16_t (portable naming).
//!   - \ref BaseAssembler::embedUInt16() - embeds uint16_t (portable naming).
//!   - \ref BaseAssembler::embedInt32() - embeds int32_t (portable naming).
//!   - \ref BaseAssembler::embedUInt32() - embeds uint32_t (portable naming).
//!   - \ref BaseAssembler::embedInt64() - embeds int64_t (portable naming).
//!   - \ref BaseAssembler::embedUInt64() - embeds uint64_t (portable naming).
//!   - \ref BaseAssembler::embedFloat() - embeds float (portable naming).
//!   - \ref BaseAssembler::embedDouble() - embeds double (portable naming).
//!
//! The following example illustrates how embed works:
//!
//! ```
//! #include <asmjit/x86.h>
//! using namespace asmjit;
//!
//! void embedData(x86::Assembler& a) {
//!   a.db(0xFF);         // Embeds 0xFF byte.
//!   a.dw(0xFF00);       // Embeds 0xFF00 word (little-endian).
//!   a.dd(0xFF000000);   // Embeds 0xFF000000 dword (little-endian).
//!   a.embedFloat(0.4f); // Embeds 0.4f (32-bit float, little-endian).
//! }
//! ```
//!
//! Sometimes it's required to read the data that is embedded after code, for
//! example. This can be done through \ref Label as shown below:
//!
//! ```
//! #include <asmjit/x86.h>
//! using namespace asmjit;
//!
//! void embedData(x86::Assembler& a, const Label& L_Data) {
//!   x86::Gp addr = a.zax();  // EAX or RAX.
//!   x86::Gp val = x86::edi;  // Where to store some value...
//!
//!   // Approach 1 - Load the address to register through LEA. This approach
//!   //              is flexible as the address can be then manipulated, for
//!   //              example if you have a data array, which would need index.
//!   a.lea(addr, L_Data);     // Loads the address of the label to EAX or RAX.
//!   a.mov(val, dword_ptr(addr));
//!
//!   // Approach 2 - Load the data directly by using L_Data in address. It's
//!   //              worth noting that this doesn't work with indexes in X64
//!   //              mode. It will use absolute address in 32-bit mode and
//!   //              relative address (RIP) in 64-bit mode.
//!   a.mov(val, dword_ptr(L_Data));
//! }
//! ```
//!
//! ### Label Embedding
//!
//! It's also possible to embed labels. In general AsmJit provides the following
//! options:
//!
//!   - \ref BaseEmitter::embedLabel() - Embeds absolute address of a label.
//!     This is target dependent and would embed either 32-bit or 64-bit data
//!     that embeds absolute label address. This kind of embedding cannot be
//!     used in a position independent code.
//!
//!   - \ref BaseEmitter::embedLabelDelta() - Embeds a difference between two
//!     labels. The size of the difference can be specified so it's possible to
//!     embed 8-bit, 16-bit, 32-bit, and 64-bit difference, which is sufficient
//!     for most purposes.
//!
//! The following example demonstrates how to embed labels and their differences:
//!
//! ```
//! #include <asmjit/x86.h>
//! using namespace asmjit;
//!
//! void embedLabel(x86::Assembler& a, const Label& L_Data) {
//!   // [1] Embed L_Data - the size of the data will be dependent on the target.
//!   a.embedLabel(L_Data);
//!
//!   // [2] Embed a 32-bit difference of two labels.
//!   Label L_Here = a.newLabel();
//!   a.bind(L_Here);
//!   // Embeds int32_t(L_Data - L_Here).
//!   a.embedLabelDelta(L_Data, L_Here, 4);
//! }
//! ```
//!
//! ### Using FuncFrame and FuncDetail with x86::Assembler
//!
//! The example below demonstrates how \ref FuncFrame and \ref FuncDetail can be
//! used together with \ref x86::Assembler to generate a function that will use
//! platform dependent calling conventions automatically depending on the target:
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! typedef void (*SumIntsFunc)(int* dst, const int* a, const int* b);
//!
//! int main(int argc, char* argv[]) {
//!   JitRuntime rt;                    // Create JIT Runtime.
//!   CodeHolder code;                  // Create a CodeHolder.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Assembler a(&code);          // Create and attach x86::Assembler to code.
//!
//!   // Decide which registers will be mapped to function arguments. Try changing
//!   // registers of dst, src_a, and src_b and see what happens in function's
//!   // prolog and epilog.
//!   x86::Gp dst   = a.zax();
//!   x86::Gp src_a = a.zcx();
//!   x86::Gp src_b = a.zdx();
//!
//!   X86::Xmm vec0 = x86::xmm0;
//!   X86::Xmm vec1 = x86::xmm1;
//!
//!   // Create/initialize FuncDetail and FuncFrame.
//!   FuncDetail func;
//!   func.init(FuncSignatureT<void, int*, const int*, const int*>(CallConv::kIdHost));
//!
//!   FuncFrame frame;
//!   frame.init(func);
//!
//!   // Make XMM0 and XMM1 dirty - kGroupVec describes XMM|YMM|ZMM registers.
//!   frame.setDirtyRegs(x86::Reg::kGroupVec, IntUtils::mask(0, 1));
//!
//!   // Alternatively, if you don't want to use register masks you can pass BaseReg
//!   // to addDirtyRegs(). The following code would add both xmm0 and xmm1.
//!   frame.addDirtyRegs(x86::xmm0, x86::xmm1);
//!
//!   FuncArgsAssignment args(&func);   // Create arguments assignment context.
//!   args.assignAll(dst, src_a, src_b);// Assign our registers to arguments.
//!   args.updateFrameInfo(frame);      // Reflect our args in FuncFrame.
//!   frame.finalize();                 // Finalize the FuncFrame (updates it).
//!
//!   a.emitProlog(frame);              // Emit function prolog.
//!   a.emitArgsAssignment(frame, args);// Assign arguments to registers.
//!   a.movdqu(vec0, x86::ptr(src_a));  // Load 4 ints from [src_a] to XMM0.
//!   a.movdqu(vec1, x86::ptr(src_b));  // Load 4 ints from [src_b] to XMM1.
//!   a.paddd(vec0, vec1);              // Add 4 ints in XMM1 to XMM0.
//!   a.movdqu(x86::ptr(dst), vec0);    // Store the result to [dst].
//!   a.emitEpilog(frame);              // Emit function epilog and return.
//!
//!   SumIntsFunc fn;
//!   Error err = rt.add(&fn, &code);   // Add the generated code to the runtime.
//!   if (err) return 1;                // Handle a possible error case.
//!
//!   // Execute the generated function.
//!   int inA[4] = { 4, 3, 2, 1 };
//!   int inB[4] = { 1, 5, 2, 8 };
//!   int out[4];
//!   fn(out, inA, inB);
//!
//!   // Prints {5 8 4 9}
//!   printf("{%d %d %d %d}\n", out[0], out[1], out[2], out[3]);
//!
//!   rt.release(fn);
//!   return 0;
//! }
//! ```
//!
//! ### Using x86::Assembler as Code-Patcher
//!
//! This is an advanced topic that is sometimes unavoidable. AsmJit by default
//! appends machine code it generates into a \ref CodeBuffer, however, it also
//! allows to set the offset in \ref CodeBuffer explicitly and to overwrite its
//! content. This technique is extremely dangerous as X86 instructions have
//! variable length (see below), so you should in general only patch code to
//! change instruction's immediate values or some other details not known the
//! at a time the instruction was emitted. A typical scenario that requires
//! code-patching is when you start emitting function and you don't know how
//! much stack you want to reserve for it.
//!
//! Before we go further it's important to introduce instruction options, because
//! they can help with code-patching (and not only patching, but that will be
//! explained in AVX-512 section):
//!
//!   - Many general-purpose instructions (especially arithmetic ones) on X86
//!     have multiple encodings - in AsmJit this is usually called 'short form'
//!     and 'long form'.
//!   - AsmJit always tries to use 'short form' as it makes the resulting
//!     machine-code smaller, which is always good - this decision is used
//!     by majority of assemblers out there.
//!   - AsmJit allows to override the default decision by using `short_()`
//!     and `long_()` instruction options to force short or long form,
//!     respectively. The most useful is `long_()` as it basically forces
//!     AsmJit to always emit the longest form. The `short_()` is not that
//!     useful as it's automatic (except jumps to non-bound labels). Note that
//!     the underscore after each function name avoids collision with built-in
//!     C++ types.
//!
//! To illustrate what short form and long form means in binary let's assume
//! we want to emit "add esp, 16" instruction, which has two possible binary
//! encodings:
//!
//!   - `83C410` - This is a short form aka `short add esp, 16` - You can see
//!     opcode byte (0x8C), MOD/RM byte (0xC4) and an 8-bit immediate value
//!     representing `16`.
//!   - `81C410000000` - This is a long form aka `long add esp, 16` - You can
//!     see a different opcode byte (0x81), the same Mod/RM byte (0xC4) and a
//!     32-bit immediate in little-endian representing `16`.
//!
//! It should be obvious that patching an existing instruction into an instruction
//! having a different size may create various problems. So it's recommended to be
//! careful and to only patch instructions into instructions having the same size.
//! The example below demonstrates how instruction options can be used to guarantee
//! the size of an instruction by forcing the assembler to use long-form encoding:
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! typedef int (*Func)(void);
//!
//! int main(int argc, char* argv[]) {
//!   JitRuntime rt;                    // Create a runtime specialized for JIT.
//!   CodeHolder code;                  // Create a CodeHolder.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Assembler a(&code);          // Create and attach x86::Assembler to code.
//!
//!   // Let's get these registers from x86::Assembler.
//!   x86::Gp zbp = a.zbp();
//!   x86::Gp zsp = a.zsp();
//!
//!   // Function prolog.
//!   a.push(zbp);
//!   a.mov(zbp, zsp);
//!
//!   // This is where we are gonna patch the code later, so let's get the offset
//!   // (the current location) from the beginning of the code-buffer.
//!   size_t patchOffset = a.offset();
//!   // Let's just emit 'sub zsp, 0' for now, but don't forget to use LONG form.
//!   a.long_().sub(zsp, 0);
//!
//!   // ... emit some code (this just sets return value to zero) ...
//!   a.xor_(x86::eax, x86::eax);
//!
//!   // Function epilog and return.
//!   a.mov(zsp, zbp);
//!   a.pop(zbp);
//!   a.ret();
//!
//!   // Now we know how much stack size we want to reserve. I have chosen 128
//!   // bytes on purpose as it's encodable only in long form that we have used.
//!
//!   int stackSize = 128;              // Number of bytes to reserve on the stack.
//!   a.setOffset(patchOffset);         // Move the current cursor to `patchOffset`.
//!   a.long_().sub(zsp, stackSize);    // Patch the code; don't forget to use LONG form.
//!
//!   // Now the code is ready to be called
//!   Func fn;
//!   Error err = rt.add(&fn, &code);   // Add the generated code to the runtime.
//!   if (err) return 1;                // Handle a possible error returned by AsmJit.
//!
//!   int result = fn();                // Execute the generated code.
//!   printf("%d\n", result);           // Print the resulting "0".
//!
//!   rt.release(fn);                   // Remove the function from the runtime.
//!   return 0;
//! }
//! ```
//!
//! If you run the example it will just work, because both instructions have
//! the same size. As an experiment you can try removing `long_()` form to
//! see what happens when wrong code is generated.
//!
//! ### Code Patching and REX Prefix
//!
//! In 64-bit mode there is one more thing to worry about when patching code:
//! REX prefix. It's a single byte prefix designed to address registers with
//! ids from 9 to 15 and to override the default width of operation from 32
//! to 64 bits. AsmJit, like other assemblers, only emits REX prefix when it's
//! necessary. If the patched code only changes the immediate value as shown
//! in the previous example then there is nothing to worry about as it doesn't
//! change the logic behind emitting REX prefix, however, if the patched code
//! changes register id or overrides the operation width then it's important
//! to take care of REX prefix as well.
//!
//! AsmJit contains another instruction option that controls (forces) REX
//! prefix - `rex()`. If you use it the instruction emitted will always use
//! REX prefix even when it's encodable without it. The following list contains
//! some instructions and their binary representations to illustrate when it's
//! emitted:
//!
//!   - `__83C410` - `add esp, 16`     - 32-bit operation in 64-bit mode doesn't require REX prefix.
//!   - `4083C410` - `rex add esp, 16` - 32-bit operation in 64-bit mode with forced REX prefix (0x40).
//!   - `4883C410` - `add rsp, 16`     - 64-bit operation in 64-bit mode requires REX prefix (0x48).
//!   - `4183C410` - `add r12d, 16`    - 32-bit operation in 64-bit mode using R12D requires REX prefix (0x41).
//!   - `4983C410` - `add r12, 16`     - 64-bit operation in 64-bit mode using R12 requires REX prefix (0x49).
//!
//! ### More Prefixes
//!
//! X86 architecture is known for its prefixes. AsmJit supports all prefixes
//! that can affect how the instruction is encoded:
//!
//! ```
//! #include <asmjit/x86.h>
//!
//! using namespace asmjit;
//!
//! void prefixesExample(x86::Assembler& a) {
//!   // Lock prefix for implementing atomics:
//!   //   lock add dword ptr [dst], 1
//!   a.lock().add(x86::dword_ptr(dst), 1);
//!
//!   // Similarly, XAcquire/XRelease prefixes are also available:
//!   //   xacquire add dword ptr [dst], 1
//!   a.xacquire().add(x86::dword_ptr(dst), 1);
//!
//!   // Rep prefix (see also repe/repz and repne/repnz):
//!   //   rep movs byte ptr [dst], byte ptr [src]
//!   a.rep().movs(x86::byte_ptr(dst), x86::byte_ptr(src));
//!
//!   // Forcing REX prefix in 64-bit mode.
//!   //   rex mov eax, 1
//!   a.rex().mov(x86::eax, 1);
//!
//!   // AVX instruction without forced prefix uses the shortest encoding:
//!   //   vaddpd xmm0, xmm1, xmm2 -> [C5|F1|58|C2]
//!   a.vaddpd(x86::xmm0, x86::xmm1, x86::xmm2);
//!
//!   // Forcing VEX3 prefix (AVX):
//!   //   vex3 vaddpd xmm0, xmm1, xmm2 -> [C4|E1|71|58|C2]
//!   a.vex3().vaddpd(x86::xmm0, x86::xmm1, x86::xmm2);
//!
//!   // Forcing EVEX prefix (AVX512):
//!   //   evex vaddpd xmm0, xmm1, xmm2 -> [62|F1|F5|08|58|C2]
//!   a.evex().vaddpd(x86::xmm0, x86::xmm1, x86::xmm2);
//!
//!   // Some instructions accept prefixes not originally intended to:
//!   //   rep ret
//!   a.rep().ret();
//! }
//! ```
//!
//! It's important to understand that prefixes are part of instruction options.
//! When a member function that involves adding a prefix is called the prefix
//! is combined with existing instruction options, which will affect the next
//! instruction generated.
//!
//! ### Generating AVX512 code.
//!
//! x86::Assembler can generate AVX512+ code including the use of opmask
//! registers. Opmask can be specified through \ref x86::Assembler::k()
//! function, which stores it as an extra register, which will be used
//! by the next instruction. AsmJit uses such concept for manipulating
//! instruction options as well.
//!
//! The following AVX512 features are supported:
//!
//!   - Opmask selector {k} and zeroing {z}.
//!   - Rounding modes {rn|rd|ru|rz} and suppress-all-exceptions {sae} option.
//!   - AVX512 broadcasts {1toN}.
//!
//! The following example demonstrates how AVX512 features can be used:
//!
//! ```
//! #include <asmjit/x86.h>
//!
//! using namespace asmjit;
//!
//! void generateAVX512Code(x86::Assembler& a) {
//!   using namespace x86;
//!
//!   // Opmask Selectors
//!   // ----------------
//!   //
//!   //   - Opmask / zeroing is part of the instruction options / extraReg.
//!   //   - k(reg) is like {kreg} in Intel syntax.
//!   //   - z() is like {z} in Intel syntax.
//!
//!   // vaddpd zmm {k1} {z}, zmm1, zmm2
//!   a.k(k1).z().vaddpd(zmm0, zmm1, zmm2);
//!
//!   // Memory Broadcasts
//!   // -----------------
//!   //
//!   //   - Broadcast data is part of memory operand.
//!   //   - Use x86::Mem::_1toN(), which returns a new x86::Mem operand.
//!
//!   // vaddpd zmm0 {k1} {z}, zmm1, [rcx] {1to8}
//!   a.k(k1).z().vaddpd(zmm0, zmm1, x86::mem(rcx)._1to8());
//!
//!   // Embedded Rounding & Suppress-All-Exceptoins
//!   // -------------------------------------------
//!   //
//!   //   - Rounding mode and {sae} are part of instruction options.
//!   //   - Use sae() to enable exception suppression.
//!   //   - Use rn_sae(), rd_sae(), ru_sae(), and rz_sae() - to enable rounding.
//!   //   - Embedded rounding implicitly sets {sae} as well, that's why the API
//!   //     also has sae() suffix, to make it clear.
//!
//!   // vcmppd k1, zmm1, zmm2, 0x00 {sae}
//!   a.sae().vcmppd(k1, zmm1, zmm2, 0);
//!
//!   // vaddpd zmm0, zmm1, zmm2 {rz}
//!   a.rz_sae().vaddpd(zmm0, zmm1, zmm2);
//! }
//! ```
class ASMJIT_VIRTAPI Assembler
  : public BaseAssembler,
    public EmitterImplicitT<Assembler> {
public:
  ASMJIT_NONCOPYABLE(Assembler)
  typedef BaseAssembler Base;

  //! \name Construction & Destruction
  //! \{

  ASMJIT_API explicit Assembler(CodeHolder* code = nullptr) noexcept;
  ASMJIT_API virtual ~Assembler() noexcept;

  //! \}

  //! \cond INTERNAL
  //! \name Internal
  //! \{

  // NOTE: x86::Assembler uses _privateData to store 'address-override' bit that
  // is used to decide whether to emit address-override (67H) prefix based on
  // the memory BASE+INDEX registers. It's either `kX86MemInfo_67H_X86` or
  // `kX86MemInfo_67H_X64`.
  inline uint32_t _addressOverrideMask() const noexcept { return _privateData; }
  inline void _setAddressOverrideMask(uint32_t m) noexcept { _privateData = m; }

  //! \}
  //! \endcond

  //! \name Emit
  //! \{

  ASMJIT_API Error _emit(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_* opExt) override;

  //! \}
  //! \endcond

  //! \name Align
  //! \{

  ASMJIT_API Error align(uint32_t alignMode, uint32_t alignment) override;

  //! \}

  //! \name Events
  //! \{

  ASMJIT_API Error onAttach(CodeHolder* code) noexcept override;
  ASMJIT_API Error onDetach(CodeHolder* code) noexcept override;

  //! \}
};

//! \}

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_X86_X86ASSEMBLER_H_INCLUDED
