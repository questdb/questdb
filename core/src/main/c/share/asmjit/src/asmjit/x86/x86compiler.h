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

#ifndef ASMJIT_X86_X86COMPILER_H_INCLUDED
#define ASMJIT_X86_X86COMPILER_H_INCLUDED

#include "../core/api-config.h"
#ifndef ASMJIT_NO_COMPILER

#include "../core/compiler.h"
#include "../core/datatypes.h"
#include "../core/type.h"
#include "../x86/x86emitter.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \addtogroup asmjit_x86
//! \{

// ============================================================================
// [asmjit::x86::Compiler]
// ============================================================================

//! X86/X64 compiler implementation.
//!
//! ### Compiler Basics
//!
//! The first \ref x86::Compiler example shows how to generate a function that
//! simply returns an integer value. It's an analogy to the first Assembler example:
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! // Signature of the generated function.
//! typedef int (*Func)(void);
//!
//! int main() {
//!   JitRuntime rt;                    // Runtime specialized for JIT code execution.
//!   CodeHolder code;                  // Holds code and relocation information.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Compiler cc(&code);          // Create and attach x86::Compiler to code.
//!
//!   cc.addFunc(FuncSignatureT<int>());// Begin a function of `int fn(void)` signature.
//!
//!   x86::Gp vReg = cc.newGpd();       // Create a 32-bit general purpose register.
//!   cc.mov(vReg, 1);                  // Move one to our virtual register `vReg`.
//!   cc.ret(vReg);                     // Return `vReg` from the function.
//!
//!   cc.endFunc();                     // End of the function body.
//!   cc.finalize();                    // Translate and assemble the whole 'cc' content.
//!   // ----> x86::Compiler is no longer needed from here and can be destroyed <----
//!
//!   Func fn;
//!   Error err = rt.add(&fn, &code);   // Add the generated code to the runtime.
//!   if (err) return 1;                // Handle a possible error returned by AsmJit.
//!   // ----> CodeHolder is no longer needed from here and can be destroyed <----
//!
//!   int result = fn();                // Execute the generated code.
//!   printf("%d\n", result);           // Print the resulting "1".
//!
//!   rt.release(fn);                   // Explicitly remove the function from the runtime.
//!   return 0;
//! }
//! ```
//!
//! The \ref BaseCompiler::addFunc() and \ref BaseCompiler::endFunc() functions
//! are used to define the function and its end. Both must be called per function,
//! but the body doesn't have to be generated in sequence. An example of generating
//! two functions will be shown later. The next example shows more complicated code
//! that contain a loop and generates a simple memory copy function that uses
//! `uint32_t` items:
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! // Signature of the generated function.
//! typedef void (*MemCpy32)(uint32_t* dst, const uint32_t* src, size_t count);
//!
//! int main() {
//!   JitRuntime rt;                    // Runtime specialized for JIT code execution.
//!   CodeHolder code;                  // Holds code and relocation information.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Compiler cc(&code);          // Create and attach x86::Compiler to code.
//!
//!   cc.addFunc(                       // Begin the function of the following signature:
//!     FuncSignatureT<void,            //   Return value - void      (no return value).
//!       uint32_t*,                    //   1st argument - uint32_t* (machine reg-size).
//!       const uint32_t*,              //   2nd argument - uint32_t* (machine reg-size).
//!       size_t>());                   //   3rd argument - size_t    (machine reg-size).
//!
//!   Label L_Loop = cc.newLabel();     // Start of the loop.
//!   Label L_Exit = cc.newLabel();     // Used to exit early.
//!
//!   x86::Gp dst = cc.newIntPtr("dst");// Create `dst` register (destination pointer).
//!   x86::Gp src = cc.newIntPtr("src");// Create `src` register (source pointer).
//!   x86::Gp i = cc.newUIntPtr("i");   // Create `i` register (loop counter).
//!
//!   cc.setArg(0, dst);                // Assign `dst` argument.
//!   cc.setArg(1, src);                // Assign `src` argument.
//!   cc.setArg(2, i);                  // Assign `i` argument.
//!
//!   cc.test(i, i);                    // Early exit if length is zero.
//!   cc.jz(L_Exit);
//!
//!   cc.bind(L_Loop);                  // Bind the beginning of the loop here.
//!
//!   x86::Gp tmp = cc.newInt32("tmp"); // Copy a single dword (4 bytes).
//!   cc.mov(tmp, x86::dword_ptr(src)); // Load DWORD from [src] address.
//!   cc.mov(x86::dword_ptr(dst), tmp); // Store DWORD to [dst] address.
//!
//!   cc.add(src, 4);                   // Increment `src`.
//!   cc.add(dst, 4);                   // Increment `dst`.
//!
//!   cc.dec(i);                        // Loop until `i` is non-zero.
//!   cc.jnz(L_Loop);
//!
//!   cc.bind(L_Exit);                  // Label used by early exit.
//!   cc.endFunc();                     // End of the function body.
//!
//!   cc.finalize();                    // Translate and assemble the whole 'cc' content.
//!   // ----> x86::Compiler is no longer needed from here and can be destroyed <----
//!
//!   // Add the generated code to the runtime.
//!   MemCpy32 memcpy32;
//!   Error err = rt.add(&memcpy32, &code);
//!
//!   // Handle a possible error returned by AsmJit.
//!   if (err)
//!     return 1;
//!   // ----> CodeHolder is no longer needed from here and can be destroyed <----
//!
//!   // Test the generated code.
//!   uint32_t input[6] = { 1, 2, 3, 5, 8, 13 };
//!   uint32_t output[6];
//!   memcpy32(output, input, 6);
//!
//!   for (uint32_t i = 0; i < 6; i++)
//!     printf("%d\n", output[i]);
//!
//!   rt.release(memcpy32);
//!   return 0;
//! }
//! ```
//!
//! ### AVX and AVX-512
//!
//! AVX and AVX-512 code generation must be explicitly enabled via \ref FuncFrame
//! to work properly. If it's not setup correctly then Prolog & Epilog would use
//! SSE instead of AVX instructions to work with SIMD registers. In addition, Compiler
//! requires explicitly enable AVX-512 via \ref FuncFrame in order to use all 32 SIMD
//! registers.
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! // Signature of the generated function.
//! typedef void (*Func)(void*);
//!
//! int main() {
//!   JitRuntime rt;                    // Runtime specialized for JIT code execution.
//!   CodeHolder code;                  // Holds code and relocation information.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Compiler cc(&code);          // Create and attach x86::Compiler to code.
//!
//!   cc.addFunc(FuncSignatureT<void, void*>());
//!
//!   // Use the following to enable AVX and/or AVX-512.
//!   cc.func()->frame().setAvxEnabled();
//!   cc.func()->frame().setAvx512Enabled();
//!
//!   // Do something with the input pointer.
//!   x86::Gp addr = cc.newIntPtr("addr");
//!   x86::Zmm vreg = cc.newZmm("vreg");
//!
//!   cc.setArg(0, addr);
//!
//!   cc.vmovdqu32(vreg, x86::ptr(addr));
//!   cc.vpaddq(vreg, vreg, vreg);
//!   cc.vmovdqu32(x86::ptr(addr), vreg);
//!
//!   cc.endFunc();                     // End of the function body.
//!   cc.finalize();                    // Translate and assemble the whole 'cc' content.
//!   // ----> x86::Compiler is no longer needed from here and can be destroyed <----
//!
//!   Func fn;
//!   Error err = rt.add(&fn, &code);   // Add the generated code to the runtime.
//!   if (err) return 1;                // Handle a possible error returned by AsmJit.
//!   // ----> CodeHolder is no longer needed from here and can be destroyed <----
//!
//!   // Execute the generated code and print some output.
//!   uint64_t data[] = { 1, 2, 3, 4, 5, 6, 7, 8 };
//!   fn(data);
//!   printf("%llu\n", (unsigned long long)data[0]);
//!
//!   rt.release(fn);                   // Explicitly remove the function from the runtime.
//!   return 0;
//! }
//! ```
//!
//! ### Recursive Functions
//!
//! It's possible to create more functions by using the same \ref x86::Compiler
//! instance and make links between them. In such case it's important to keep
//! the pointer to \ref FuncNode.
//!
//! The example below creates a simple Fibonacci function that calls itself recursively:
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! // Signature of the generated function.
//! typedef uint32_t (*Fibonacci)(uint32_t x);
//!
//! int main() {
//!   JitRuntime rt;                    // Runtime specialized for JIT code execution.
//!   CodeHolder code;                  // Holds code and relocation information.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Compiler cc(&code);          // Create and attach x86::Compiler to code.
//!
//!   FuncNode* func = cc.addFunc(      // Begin of the Fibonacci function, addFunc()
//!     FuncSignatureT<int, int>());    // Returns a pointer to the FuncNode node.
//!
//!   Label L_Exit = cc.newLabel()      // Exit label.
//!   x86::Gp x = cc.newU32();          // Function x argument.
//!   x86::Gp y = cc.newU32();          // Temporary.
//!
//!   cc.setArg(0, x);
//!
//!   cc.cmp(x, 3);                     // Return x if less than 3.
//!   cc.jb(L_Exit);
//!
//!   cc.mov(y, x);                     // Make copy of the original x.
//!   cc.dec(x);                        // Decrease x.
//!
//!   InvokeNode* invokeNode;           // Function invocation:
//!   cc.invoke(&invokeNode,            //   - InvokeNode (output).
//!     func->label(),                  //   - Function address or Label.
//!     FuncSignatureT<int, int>());    //   - Function signature.
//!
//!   invokeNode->setArg(0, x);         // Assign x as the first argument.
//!   invokeNode->setRet(0, x);         // Assign x as a return value as well.
//!
//!   cc.add(x, y);                     // Combine the return value with y.
//!
//!   cc.bind(L_Exit);
//!   cc.ret(x);                        // Return x.
//!   cc.endFunc();                     // End of the function body.
//!
//!   cc.finalize();                    // Translate and assemble the whole 'cc' content.
//!   // ----> x86::Compiler is no longer needed from here and can be destroyed <----
//!
//!   Fibonacci fib;
//!   Error err = rt.add(&fib, &code);  // Add the generated code to the runtime.
//!   if (err) return 1;                // Handle a possible error returned by AsmJit.
//!   // ----> CodeHolder is no longer needed from here and can be destroyed <----
//!
//!   // Test the generated code.
//!   printf("Fib(%u) -> %u\n", 8, fib(8));
//!
//!   rt.release(fib);
//!   return 0;
//! }
//! ```
//!
//! ### Stack Management
//!
//! Function's stack-frame is managed automatically, which is used by the register allocator to spill virtual registers. It also provides an interface to allocate user-defined block of the stack, which can be used as a temporary storage by the generated function. In the following example a stack of 256 bytes size is allocated, filled by bytes starting from 0 to 255 and then iterated again to sum all the values.
//!
//! ```
//! #include <asmjit/x86.h>
//! #include <stdio.h>
//!
//! using namespace asmjit;
//!
//! // Signature of the generated function.
//! typedef int (*Func)(void);
//!
//! int main() {
//!   JitRuntime rt;                    // Runtime specialized for JIT code execution.
//!   CodeHolder code;                  // Holds code and relocation information.
//!
//!   code.init(rt.environment());      // Initialize code to match the JIT environment.
//!   x86::Compiler cc(&code);          // Create and attach x86::Compiler to code.
//!
//!   cc.addFunc(FuncSignatureT<int>());// Create a function that returns int.
//!
//!   x86::Gp p = cc.newIntPtr("p");
//!   x86::Gp i = cc.newIntPtr("i");
//!
//!   // Allocate 256 bytes on the stack aligned to 4 bytes.
//!   x86::Mem stack = cc.newStack(256, 4);
//!
//!   x86::Mem stackIdx(stack);         // Copy of stack with i added.
//!   stackIdx.setIndex(i);             // stackIdx <- stack[i].
//!   stackIdx.setSize(1);              // stackIdx <- byte ptr stack[i].
//!
//!   // Load a stack address to `p`. This step is purely optional and shows
//!   // that `lea` is useful to load a memory operands address (even absolute)
//!   // to a general purpose register.
//!   cc.lea(p, stack);
//!
//!   // Clear i (xor is a C++ keyword, hence 'xor_' is used instead).
//!   cc.xor_(i, i);
//!
//!   Label L1 = cc.newLabel();
//!   Label L2 = cc.newLabel();
//!
//!   cc.bind(L1);                      // First loop, fill the stack.
//!   cc.mov(stackIdx, i.r8());         // stack[i] = uint8_t(i).
//!
//!   cc.inc(i);                        // i++;
//!   cc.cmp(i, 256);                   // if (i < 256)
//!   cc.jb(L1);                        //   goto L1;
//!
//!   // Second loop, sum all bytes stored in `stack`.
//!   x86::Gp sum = cc.newI32("sum");
//!   x86::Gp val = cc.newI32("val");
//!
//!   cc.xor_(i, i);
//!   cc.xor_(sum, sum);
//!
//!   cc.bind(L2);
//!
//!   cc.movzx(val, stackIdx);          // val = uint32_t(stack[i]);
//!   cc.add(sum, val);                 // sum += val;
//!
//!   cc.inc(i);                        // i++;
//!   cc.cmp(i, 256);                   // if (i < 256)
//!   cc.jb(L2);                        //   goto L2;
//!
//!   cc.ret(sum);                      // Return the `sum` of all values.
//!   cc.endFunc();                     // End of the function body.
//!
//!   cc.finalize();                    // Translate and assemble the whole 'cc' content.
//!   // ----> x86::Compiler is no longer needed from here and can be destroyed <----
//!
//!   Func func;
//!   Error err = rt.add(&func, &code); // Add the generated code to the runtime.
//!   if (err) return 1;                // Handle a possible error returned by AsmJit.
//!   // ----> CodeHolder is no longer needed from here and can be destroyed <----
//!
//!   printf("Func() -> %d\n", func()); // Test the generated code.
//!
//!   rt.release(func);
//!   return 0;
//! }
//! ```
//!
//! ### Constant Pool
//!
//! Compiler provides two constant pools for a general purpose code generation:
//!
//!   - Local constant pool - Part of \ref FuncNode, can be only used by a
//!     single function and added after the function epilog sequence (after
//!     `ret` instruction).
//!
//!   - Global constant pool - Part of \ref BaseCompiler, flushed at the end
//!     of the generated code by \ref BaseEmitter::finalize().
//!
//! The example below illustrates how a built-in constant pool can be used:
//!
//! ```
//! #include <asmjit/x86.h>
//!
//! using namespace asmjit;
//!
//! static void exampleUseOfConstPool(x86::Compiler& cc) {
//!   cc.addFunc(FuncSignatureT<int>());
//!
//!   x86::Gp v0 = cc.newGpd("v0");
//!   x86::Gp v1 = cc.newGpd("v1");
//!
//!   x86::Mem c0 = cc.newInt32Const(ConstPool::kScopeLocal, 200);
//!   x86::Mem c1 = cc.newInt32Const(ConstPool::kScopeLocal, 33);
//!
//!   cc.mov(v0, c0);
//!   cc.mov(v1, c1);
//!   cc.add(v0, v1);
//!
//!   cc.ret(v0);
//!   cc.endFunc();
//! }
//! ```
//!
//! ### Jump Tables
//!
//! x86::Compiler supports `jmp` instruction with reg/mem operand, which is a
//! commonly used pattern to implement indirect jumps within a function, for
//! example to implement `switch()` statement in a programming languages. By
//! default AsmJit assumes that every basic block can be a possible jump
//! target as it's unable to deduce targets from instruction's operands. This
//! is a very pessimistic default that should be avoided if possible as it's
//! costly and very unfriendly to liveness analysis and register allocation.
//!
//! Instead of relying on such pessimistic default behavior, let's use \ref
//! JumpAnnotation to annotate a jump where all targets are known:
//!
//! ```
//! #include <asmjit/x86.h>
//!
//! using namespace asmjit;
//!
//! static void exampleUseOfIndirectJump(x86::Compiler& cc) {
//!   cc.addFunc(FuncSignatureT<float, float, float, uint32_t>(CallConv::kIdHost));
//!
//!   // Function arguments
//!   x86::Xmm a = cc.newXmmSs("a");
//!   x86::Xmm b = cc.newXmmSs("b");
//!   x86::Gp op = cc.newUInt32("op");
//!
//!   x86::Gp target = cc.newIntPtr("target");
//!   x86::Gp offset = cc.newIntPtr("offset");
//!
//!   Label L_Table = cc.newLabel();
//!   Label L_Add = cc.newLabel();
//!   Label L_Sub = cc.newLabel();
//!   Label L_Mul = cc.newLabel();
//!   Label L_Div = cc.newLabel();
//!   Label L_End = cc.newLabel();
//!
//!   cc.setArg(0, a);
//!   cc.setArg(1, b);
//!   cc.setArg(2, op);
//!
//!   // Jump annotation is a building block that allows to annotate all
//!   // possible targets where `jmp()` can jump. It then drives the CFG
//!   // contruction and liveness analysis, which impacts register allocation.
//!   JumpAnnotation* annotation = cc.newJumpAnnotation();
//!   annotation->addLabel(L_Add);
//!   annotation->addLabel(L_Sub);
//!   annotation->addLabel(L_Mul);
//!   annotation->addLabel(L_Div);
//!
//!   // Most likely not the common indirect jump approach, but it
//!   // doesn't really matter how final address is calculated. The
//!   // most important path using JumpAnnotation with `jmp()`.
//!   cc.lea(offset, x86::ptr(L_Table));
//!   if (cc.is64Bit())
//!     cc.movsxd(target, x86::dword_ptr(offset, op.cloneAs(offset), 2));
//!   else
//!     cc.mov(target, x86::dword_ptr(offset, op.cloneAs(offset), 2));
//!   cc.add(target, offset);
//!   cc.jmp(target, annotation);
//!
//!   // Acts like a switch() statement in C.
//!   cc.bind(L_Add);
//!   cc.addss(a, b);
//!   cc.jmp(L_End);
//!
//!   cc.bind(L_Sub);
//!   cc.subss(a, b);
//!   cc.jmp(L_End);
//!
//!   cc.bind(L_Mul);
//!   cc.mulss(a, b);
//!   cc.jmp(L_End);
//!
//!   cc.bind(L_Div);
//!   cc.divss(a, b);
//!
//!   cc.bind(L_End);
//!   cc.ret(a);
//!
//!   cc.endFunc();
//!
//!   // Relative int32_t offsets of `L_XXX - L_Table`.
//!   cc.bind(L_Table);
//!   cc.embedLabelDelta(L_Add, L_Table, 4);
//!   cc.embedLabelDelta(L_Sub, L_Table, 4);
//!   cc.embedLabelDelta(L_Mul, L_Table, 4);
//!   cc.embedLabelDelta(L_Div, L_Table, 4);
//! }
//! ```
class ASMJIT_VIRTAPI Compiler
  : public BaseCompiler,
    public EmitterExplicitT<Compiler> {
public:
  ASMJIT_NONCOPYABLE(Compiler)
  typedef BaseCompiler Base;

  //! \name Construction & Destruction
  //! \{

  ASMJIT_API explicit Compiler(CodeHolder* code = nullptr) noexcept;
  ASMJIT_API virtual ~Compiler() noexcept;

  //! \}

  //! \name Virtual Registers
  //! \{

#ifndef ASMJIT_NO_LOGGING
# define ASMJIT_NEW_REG_FMT(OUT, PARAM, FORMAT, ARGS)                         \
    _newRegFmt(&OUT, PARAM, FORMAT, ARGS)
#else
# define ASMJIT_NEW_REG_FMT(OUT, PARAM, FORMAT, ARGS)                         \
    DebugUtils::unused(FORMAT);                                               \
    DebugUtils::unused(std::forward<Args>(args)...);                          \
    _newReg(&OUT, PARAM)
#endif

#define ASMJIT_NEW_REG_CUSTOM(FUNC, REG)                                      \
    inline REG FUNC(uint32_t typeId) {                                        \
      REG reg(Globals::NoInit);                                               \
      _newReg(&reg, typeId);                                                  \
      return reg;                                                             \
    }                                                                         \
                                                                              \
    template<typename... Args>                                                \
    inline REG FUNC(uint32_t typeId, const char* fmt, Args&&... args) {       \
      REG reg(Globals::NoInit);                                               \
      ASMJIT_NEW_REG_FMT(reg, typeId, fmt, std::forward<Args>(args)...);      \
      return reg;                                                             \
    }

#define ASMJIT_NEW_REG_TYPED(FUNC, REG, TYPE_ID)                              \
    inline REG FUNC() {                                                       \
      REG reg(Globals::NoInit);                                               \
      _newReg(&reg, TYPE_ID);                                                 \
      return reg;                                                             \
    }                                                                         \
                                                                              \
    template<typename... Args>                                                \
    inline REG FUNC(const char* fmt, Args&&... args) {                        \
      REG reg(Globals::NoInit);                                               \
      ASMJIT_NEW_REG_FMT(reg, TYPE_ID, fmt, std::forward<Args>(args)...);     \
      return reg;                                                             \
    }

  template<typename RegT>
  inline RegT newSimilarReg(const RegT& ref) {
    RegT reg(Globals::NoInit);
    _newReg(reg, ref);
    return reg;
  }

  template<typename RegT, typename... Args>
  inline RegT newSimilarReg(const RegT& ref, const char* fmt, Args&&... args) {
    RegT reg(Globals::NoInit);
    ASMJIT_NEW_REG_FMT(reg, ref, fmt, std::forward<Args>(args)...);
    return reg;
  }

  ASMJIT_NEW_REG_CUSTOM(newReg    , Reg )
  ASMJIT_NEW_REG_CUSTOM(newGp     , Gp  )
  ASMJIT_NEW_REG_CUSTOM(newVec    , Vec )
  ASMJIT_NEW_REG_CUSTOM(newK      , KReg)

  ASMJIT_NEW_REG_TYPED(newI8     , Gp  , Type::kIdI8     )
  ASMJIT_NEW_REG_TYPED(newU8     , Gp  , Type::kIdU8     )
  ASMJIT_NEW_REG_TYPED(newI16    , Gp  , Type::kIdI16    )
  ASMJIT_NEW_REG_TYPED(newU16    , Gp  , Type::kIdU16    )
  ASMJIT_NEW_REG_TYPED(newI32    , Gp  , Type::kIdI32    )
  ASMJIT_NEW_REG_TYPED(newU32    , Gp  , Type::kIdU32    )
  ASMJIT_NEW_REG_TYPED(newI64    , Gp  , Type::kIdI64    )
  ASMJIT_NEW_REG_TYPED(newU64    , Gp  , Type::kIdU64    )
  ASMJIT_NEW_REG_TYPED(newInt8   , Gp  , Type::kIdI8     )
  ASMJIT_NEW_REG_TYPED(newUInt8  , Gp  , Type::kIdU8     )
  ASMJIT_NEW_REG_TYPED(newInt16  , Gp  , Type::kIdI16    )
  ASMJIT_NEW_REG_TYPED(newUInt16 , Gp  , Type::kIdU16    )
  ASMJIT_NEW_REG_TYPED(newInt32  , Gp  , Type::kIdI32    )
  ASMJIT_NEW_REG_TYPED(newUInt32 , Gp  , Type::kIdU32    )
  ASMJIT_NEW_REG_TYPED(newInt64  , Gp  , Type::kIdI64    )
  ASMJIT_NEW_REG_TYPED(newUInt64 , Gp  , Type::kIdU64    )
  ASMJIT_NEW_REG_TYPED(newIntPtr , Gp  , Type::kIdIntPtr )
  ASMJIT_NEW_REG_TYPED(newUIntPtr, Gp  , Type::kIdUIntPtr)

  ASMJIT_NEW_REG_TYPED(newGpb    , Gp  , Type::kIdU8     )
  ASMJIT_NEW_REG_TYPED(newGpw    , Gp  , Type::kIdU16    )
  ASMJIT_NEW_REG_TYPED(newGpd    , Gp  , Type::kIdU32    )
  ASMJIT_NEW_REG_TYPED(newGpq    , Gp  , Type::kIdU64    )
  ASMJIT_NEW_REG_TYPED(newGpz    , Gp  , Type::kIdUIntPtr)
  ASMJIT_NEW_REG_TYPED(newXmm    , Xmm , Type::kIdI32x4  )
  ASMJIT_NEW_REG_TYPED(newXmmSs  , Xmm , Type::kIdF32x1  )
  ASMJIT_NEW_REG_TYPED(newXmmSd  , Xmm , Type::kIdF64x1  )
  ASMJIT_NEW_REG_TYPED(newXmmPs  , Xmm , Type::kIdF32x4  )
  ASMJIT_NEW_REG_TYPED(newXmmPd  , Xmm , Type::kIdF64x2  )
  ASMJIT_NEW_REG_TYPED(newYmm    , Ymm , Type::kIdI32x8  )
  ASMJIT_NEW_REG_TYPED(newYmmPs  , Ymm , Type::kIdF32x8  )
  ASMJIT_NEW_REG_TYPED(newYmmPd  , Ymm , Type::kIdF64x4  )
  ASMJIT_NEW_REG_TYPED(newZmm    , Zmm , Type::kIdI32x16 )
  ASMJIT_NEW_REG_TYPED(newZmmPs  , Zmm , Type::kIdF32x16 )
  ASMJIT_NEW_REG_TYPED(newZmmPd  , Zmm , Type::kIdF64x8  )
  ASMJIT_NEW_REG_TYPED(newMm     , Mm  , Type::kIdMmx64  )
  ASMJIT_NEW_REG_TYPED(newKb     , KReg, Type::kIdMask8  )
  ASMJIT_NEW_REG_TYPED(newKw     , KReg, Type::kIdMask16 )
  ASMJIT_NEW_REG_TYPED(newKd     , KReg, Type::kIdMask32 )
  ASMJIT_NEW_REG_TYPED(newKq     , KReg, Type::kIdMask64 )

#undef ASMJIT_NEW_REG_TYPED
#undef ASMJIT_NEW_REG_CUSTOM
#undef ASMJIT_NEW_REG_FMT

  //! \}

  //! \name Stack
  //! \{

  //! Creates a new memory chunk allocated on the current function's stack.
  inline Mem newStack(uint32_t size, uint32_t alignment, const char* name = nullptr) {
    Mem m(Globals::NoInit);
    _newStack(&m, size, alignment, name);
    return m;
  }

  //! \}

  //! \name Constants
  //! \{

  //! Put data to a constant-pool and get a memory reference to it.
  inline Mem newConst(uint32_t scope, const void* data, size_t size) {
    Mem m(Globals::NoInit);
    _newConst(&m, scope, data, size);
    return m;
  }

  //! Put a BYTE `val` to a constant-pool.
  inline Mem newByteConst(uint32_t scope, uint8_t val) noexcept { return newConst(scope, &val, 1); }
  //! Put a WORD `val` to a constant-pool.
  inline Mem newWordConst(uint32_t scope, uint16_t val) noexcept { return newConst(scope, &val, 2); }
  //! Put a DWORD `val` to a constant-pool.
  inline Mem newDWordConst(uint32_t scope, uint32_t val) noexcept { return newConst(scope, &val, 4); }
  //! Put a QWORD `val` to a constant-pool.
  inline Mem newQWordConst(uint32_t scope, uint64_t val) noexcept { return newConst(scope, &val, 8); }

  //! Put a WORD `val` to a constant-pool.
  inline Mem newInt16Const(uint32_t scope, int16_t val) noexcept { return newConst(scope, &val, 2); }
  //! Put a WORD `val` to a constant-pool.
  inline Mem newUInt16Const(uint32_t scope, uint16_t val) noexcept { return newConst(scope, &val, 2); }
  //! Put a DWORD `val` to a constant-pool.
  inline Mem newInt32Const(uint32_t scope, int32_t val) noexcept { return newConst(scope, &val, 4); }
  //! Put a DWORD `val` to a constant-pool.
  inline Mem newUInt32Const(uint32_t scope, uint32_t val) noexcept { return newConst(scope, &val, 4); }
  //! Put a QWORD `val` to a constant-pool.
  inline Mem newInt64Const(uint32_t scope, int64_t val) noexcept { return newConst(scope, &val, 8); }
  //! Put a QWORD `val` to a constant-pool.
  inline Mem newUInt64Const(uint32_t scope, uint64_t val) noexcept { return newConst(scope, &val, 8); }

  //! Put a SP-FP `val` to a constant-pool.
  inline Mem newFloatConst(uint32_t scope, float val) noexcept { return newConst(scope, &val, 4); }
  //! Put a DP-FP `val` to a constant-pool.
  inline Mem newDoubleConst(uint32_t scope, double val) noexcept { return newConst(scope, &val, 8); }

#ifndef ASMJIT_NO_DEPRECATED
  ASMJIT_DEPRECATED("newMmConst() uses a deprecated Data64, use newConst() with your own data instead")
  inline Mem newMmConst(uint32_t scope, const Data64& val) noexcept { return newConst(scope, &val, 8); }

  ASMJIT_DEPRECATED("newXmmConst() uses a deprecated Data128, use newConst() with your own data instead")
  inline Mem newXmmConst(uint32_t scope, const Data128& val) noexcept { return newConst(scope, &val, 16); }

  ASMJIT_DEPRECATED("newYmmConst() uses a deprecated Data256, use newConst() with your own data instead")
  inline Mem newYmmConst(uint32_t scope, const Data256& val) noexcept { return newConst(scope, &val, 32); }
#endif // !ASMJIT_NO_DEPRECATED

  //! \}

  //! \name Instruction Options
  //! \{

  //! Force the compiler to not follow the conditional or unconditional jump.
  inline Compiler& unfollow() noexcept { _instOptions |= Inst::kOptionUnfollow; return *this; }
  //! Tell the compiler that the destination variable will be overwritten.
  inline Compiler& overwrite() noexcept { _instOptions |= Inst::kOptionOverwrite; return *this; }

  //! \}

  //! \name Function Call & Ret Intrinsics
  //! \{

  //! Invoke a function call without `target` type enforcement.
  inline Error invoke_(InvokeNode** out, const Operand_& target, const FuncSignature& signature) {
    return _addInvokeNode(out, Inst::kIdCall, target, signature);
  }

  //! Invoke a function call of the given `target` and `signature` and store
  //! the added node to `out`.
  //!
  //! Creates a new \ref InvokeNode, initializes all the necessary members to
  //! match the given function `signature`, adds the node to the compiler, and
  //! stores its pointer to `out`. The operation is atomic, if anything fails
  //! nullptr is stored in `out` and error code is returned.
  inline Error invoke(InvokeNode** out, const Gp& target, const FuncSignature& signature) { return invoke_(out, target, signature); }
  //! \overload
  inline Error invoke(InvokeNode** out, const Mem& target, const FuncSignature& signature) { return invoke_(out, target, signature); }
  //! \overload
  inline Error invoke(InvokeNode** out, const Label& target, const FuncSignature& signature) { return invoke_(out, target, signature); }
  //! \overload
  inline Error invoke(InvokeNode** out, const Imm& target, const FuncSignature& signature) { return invoke_(out, target, signature); }
  //! \overload
  inline Error invoke(InvokeNode** out, uint64_t target, const FuncSignature& signature) { return invoke_(out, Imm(int64_t(target)), signature); }

#ifndef _DOXYGEN
  template<typename Target>
  ASMJIT_DEPRECATED("Use invoke() instead of call()")
  inline InvokeNode* call(const Target& target, const FuncSignature& signature) {
    InvokeNode* invokeNode;
    invoke(&invokeNode, target, signature);
    return invokeNode;
  }
#endif

  //! Return.
  inline FuncRetNode* ret() { return addRet(Operand(), Operand()); }
  //! \overload
  inline FuncRetNode* ret(const BaseReg& o0) { return addRet(o0, Operand()); }
  //! \overload
  inline FuncRetNode* ret(const BaseReg& o0, const BaseReg& o1) { return addRet(o0, o1); }

  //! \}

  //! \name Jump Tables Support
  //! \{

  using EmitterExplicitT<Compiler>::jmp;

  //! Adds a jump to the given `target` with the provided jump `annotation`.
  inline Error jmp(const BaseReg& target, JumpAnnotation* annotation) { return emitAnnotatedJump(Inst::kIdJmp, target, annotation); }
  //! \overload
  inline Error jmp(const BaseMem& target, JumpAnnotation* annotation) { return emitAnnotatedJump(Inst::kIdJmp, target, annotation); }

  //! \}

  //! \name Finalize
  //! \{

  ASMJIT_API Error finalize() override;

  //! \}

  //! \name Events
  //! \{

  ASMJIT_API Error onAttach(CodeHolder* code) noexcept override;

  //! \}
};

//! \}

ASMJIT_END_SUB_NAMESPACE

#endif // !ASMJIT_NO_COMPILER
#endif // ASMJIT_X86_X86COMPILER_H_INCLUDED
