AsmJit
------

AsmJit is a lightweight library for machine code generation written in C++ language.

  * [Official Home Page (asmjit.com)](https://asmjit.com)
  * [Official Repository (asmjit/asmjit)](https://github.com/asmjit/asmjit)
  * [Public Chat Channel](https://gitter.im/asmjit/asmjit)
  * [Zlib License](./LICENSE.md)

See [asmjit.com](https://asmjit.com) page for more details, examples, and documentation.

Documentation
-------------

  * [Documentation Index](https://asmjit.com/doc/index.html)
  * [Build Instructions](https://asmjit.com/doc/group__asmjit__build.html)

Breaking Changes
----------------

Breaking the API is sometimes inevitable, what to do?

  * See [Breaking Changes Guide](https://asmjit.com/doc/group__asmjit__breaking__changes.html), which is now part of AsmJit documentation.
  * See asmjit tests, they always compile and provide implementation of many use-cases:
    * [asmjit_test_emitters.cpp](./test/asmjit_test_emitters.cpp) - Tests that demonstrate the purpose of emitters.
    * [asmjit_test_assembler_x86.cpp](./test/asmjit_test_assembler_x86.cpp) - Tests targeting AsmJit's Assembler (x86/x64).
    * [asmjit_test_compiler_x86.cpp](./test/asmjit_test_compiler_x86.cpp) - Tests targeting AsmJit's Compiler (x86/x64).
    * [asmjit_test_instinfo.cpp](./test/asmjit_test_instinfo.cpp) - Tests that query instruction information.
    * [asmjit_test_x86_sections.cpp](./test/asmjit_test_x86_sections.cpp) - Multiple sections test.
  * Visit our [Official Chat](https://gitter.im/asmjit/asmjit) if you need a quick help.

Project Organization
--------------------

  * **`/`**        - Project root.
    * **src**      - Source code.
      * **asmjit** - Source code and headers (always point include path in here).
        * **core** - Core API, backend independent except relocations.
        * **arm**  - ARM specific API, used only by ARM and AArch64 backends.
        * **x86**  - X86 specific API, used only by X86 and X64 backends.
    * **test**     - Unit and integration tests (don't embed in your project).
    * **tools**    - Tools used for configuring, documenting, and generating files.

TODO
----

  * [ ] Core:
    * [ ] Add support for user external buffers in CodeBuffer / CodeHolder.
    * [ ] Register allocator doesn't understand register pairs, affected instructions:
      * [ ] v4fmaddps, v4fmaddss, v4fnmaddps, v4fnmaddss
      * [ ] vp4dpwssd, vp4dpwssds
      * [ ] vp2intersectd, vp2intersectq
  * [ ] Ports:
    * [ ] ARM/Thumb/AArch64 support.

Support
-------

  * AsmJit project has both community and commercial support, see [AsmJit's Support Page](https://asmjit.com/support.html)
  * You can help the development and maintenance through Petr Kobalicek's [GitHub sponsors Profile](https://github.com/sponsors/kobalicek)

Notable Donors List:

  * [ZehMatt](https://github.com/ZehMatt)


Authors & Maintainers
---------------------

  * Petr Kobalicek <kobalicek.petr@gmail.com>
