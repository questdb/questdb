#                       ASMLIB.MAKE                        2016-11-09 Agner Fog

# Makefile for ASMLIB function library, NASM version 
# See asmlib-instructions.doc for a description

# The following tools are required for building this library package:
# Microsoft nmake or other make utility
# Microsoft link
# NASM assembler nasm.exe
# Object file converter objconv.exe (www.agner.org/optimize)
# Winzip command line version (www.winzip.com) or other zip utility

# Note: position-independent versions removed because they only work with the YASM
# assembler which unfortunately is no longer maintained

# modify as necessary:
libpath64="C:\Program Files\Microsoft Visual Studio 9.0\VC\lib\amd64"

# Main target is zip file
# Using wzzip, which is the command line version of Winzip (www.winzip.com)
asmlib.zip: lib/libacof32.lib lib/libacof32o.lib lib/libacof64.lib lib/libacof64o.lib \
lib/libaomf32.lib lib/libaomf32o.lib \
lib/libaelf32.a lib/libaelf32o.a \
lib/libaelf64.a lib/libaelf64o.a \
lib/libamac32.a lib/libamac32o.a \
lib/libamac64.a lib/libamac64o.a \
lib/libad32.dll lib/libad32.lib lib/libad64.dll lib/libad64.lib \
asmlib.h asmlibran.h asmlib-instructions.pdf license.txt \
asmlibSrc.zip inteldispatchpatch.zip 
  wzzip $@ $?
  
# Remvoved: lib/libaelf32p.a lib/libaelf32op.a lib/libamac32p.a lib/libamac32op.a   
  
# Make zip archive of source code  
asmlibSrc.zip: makeasmlib.bat asmlib.make \
asm/instrset32.asm asm/instrset64.asm asm/procname32.asm asm/procname64.asm \
asm/rdtsc32.asm asm/rdtsc64.asm asm/round32.asm asm/round64.asm \
asm/libad32.asm asm/libad32.def asm/libad64.asm asm/libad64.def \
asm/memcpy32.asm asm/memmove32.asm asm/memcpy64.asm asm/memmove64.asm \
asm/memset32.asm asm/memset64.asm asm/memcmp32.asm asm/memcmp64.asm \
asm/strlen32.asm asm/strlen64.asm \
asm/strcpy32.asm asm/strcpy64.asm asm/strcat32.asm asm/strcat64.asm \
asm/strcmp32.asm asm/strcmp64.asm asm/stricmp32.asm asm/stricmp64.asm \
asm/strtouplow32.asm asm/strtouplow64.asm asm/strstr32.asm asm/strstr64.asm \
asm/substring32.asm asm/substring64.asm asm/strspn32.asm asm/strspn64.asm \
asm/strcountutf832.asm asm/strcountutf864.asm \
asm/strcountset32.asm asm/strcountset64.asm \
asm/divfixedi32.asm asm/divfixedi64.asm \
asm/divfixedv32.asm asm/divfixedv64.asm \
asm/popcount32.asm asm/popcount64.asm \
asm/cpuid32.asm asm/cpuid64.asm asm/cputype32.asm asm/cputype64.asm \
asm/physseed32.asm asm/physseed64.asm \
asm/mother32.asm asm/mother64.asm asm/mersenne32.asm asm/mersenne64.asm \
asm/randomah.asi asm/sfmt32.asm asm/sfmt64.asm \
asm/debugbreak32.asm asm/debugbreak64.asm \
asm/unalignedisfaster32.asm asm/unalignedisfaster64.asm \
asm/cachesize32.asm asm/cachesize64.asm \
asm/dispatchpatch32.asm asm/dispatchpatch64.asm \
testalib.cpp testrandom.cpp testmem.cpp
  wzzip $@ $?
  
# Make zip archive of inteldispatchpatch
inteldispatchpatch.zip: patch/dispatchpatch.txt \
patch/dispatchpatch32.obj patch/dispatchpatch32.o patch/dispatchpatch32.mac.o \
patch/dispatchpatch64.obj patch/dispatchpatch64.o patch/dispatchpatch64.mac.o \
patch/intel_cpu_feature_patch.c patch/intel_mkl_feature_patch.c
  wzzip $@ $?


# Build each library version:

# 32 bit Windows/COFF library
lib/libacof32.lib: obj/instrset32.obj32 obj/procname32.obj32 \
obj/cpuid32.obj32 obj/rdtsc32.obj32 obj/round32.obj32 \
obj/memcpy32.obj32 obj/memmove32.obj32 obj/memset32.obj32 obj/memcmp32.obj32 \
obj/strlen32.obj32 obj/strcpy32.obj32 obj/strcat32.obj32 \
obj/strstr32.obj32 obj/strcmp32.obj32 obj/stricmp32.obj32 \
obj/strtouplow32.obj32 obj/substring32.obj32 obj/strspn32.obj32 \
obj/strcountutf832.obj32 obj/strcountset32.obj32 \
obj/divfixedi32.obj32 obj/divfixedv32.obj32 obj/popcount32.obj32 \
obj/physseed32.obj32 obj/mother32.obj32 obj/mersenne32.obj32 \
obj/sfmt32.obj32 \
obj/cputype32.obj32 obj/debugbreak32.obj32 obj/unalignedisfaster32.obj32 \
obj/cachesize32.obj32
  objconv -fcof32 -wex -lib $@ $?

# 32 bit ELF library, position dependent
lib/libaelf32.a: obj/instrset32.o32 obj/procname32.o32 \
obj/cpuid32.o32 obj/rdtsc32.o32 obj/round32.o32 \
obj/memcpy32.o32 obj/memmove32.o32 obj/memset32.o32 obj/memcmp32.o32 \
obj/strlen32.o32 obj/strcpy32.o32 obj/strcat32.o32 \
obj/strstr32.o32 obj/strcmp32.o32 obj/stricmp32.o32 \
obj/strtouplow32.o32 obj/substring32.o32 obj/strspn32.o32 \
obj/strcountutf832.o32 obj/strcountset32.o32 \
obj/divfixedi32.o32 obj/divfixedv32.o32 obj/popcount32.o32 \
obj/physseed32.o32 obj/mother32.o32 obj/mersenne32.o32 \
obj/sfmt32.o32 \
obj/cputype32.o32 obj/debugbreak32.o32 obj/unalignedisfaster32.o32 \
obj/cachesize32.o32
  objconv -felf32 -nu -wex -lib $@ $?

# 32 bit ELF library, position independent (not included)
lib/libaelf32p.a: obj/instrset32.o32pic obj/procname32.o32pic \
obj/cpuid32.o32pic obj/rdtsc32.o32pic obj/round32.o32pic \
obj/memcpy32.o32pic obj/memmove32.o32pic obj/memset32.o32pic obj/memcmp32.o32pic \
obj/strlen32.o32pic obj/strcpy32.o32pic obj/strcat32.o32pic \
obj/strstr32.o32pic obj/strcmp32.o32pic obj/stricmp32.o32pic \
obj/strtouplow32.o32pic obj/substring32.o32pic obj/strspn32.o32pic \
obj/strcountutf832.o32pic obj/strcountset32.o32pic \
obj/divfixedi32.o32pic obj/divfixedv32.o32pic obj/popcount32.o32pic \
obj/physseed32.o32pic obj/mother32.o32pic obj/mersenne32.o32pic \
obj/sfmt32.o32pic \
obj/cputype32.o32pic obj/debugbreak32.o32 obj/unalignedisfaster32.o32 \
obj/cachesize32.o32pic
  objconv -felf32 -nu -wex -lib $@ $?

# 64 bit COFF library Windows
lib/libacof64.lib: obj/instrset64.obj64 obj/procname64.obj64 \
obj/cpuid64.obj64 obj/rdtsc64.obj64 obj/round64.obj64 \
obj/memcpy64.obj64 obj/memmove64.obj64 obj/memset64.obj64 obj/memcmp64.obj64 \
obj/strlen64.obj64 obj/strcpy64.obj64 obj/strcat64.obj64 \
obj/strstr64.obj64 obj/strcmp64.obj64 obj/stricmp64.obj64 \
obj/strtouplow64.obj64 obj/substring64.obj64 obj/strspn64.obj64 \
obj/strcountutf864.obj64 obj/strcountset64.obj64 \
obj/divfixedi64.obj64 obj/divfixedv64.obj64 obj/popcount64.obj64 \
obj/physseed64.obj64 obj/mother64.obj64 obj/mersenne64.obj64 \
obj/sfmt64.obj64 \
obj/cputype64.obj64 obj/debugbreak64.obj64 obj/unalignedisfaster64.obj64 \
obj/cachesize64.obj64
  objconv -fcof64 -wex -lib $@ $?

# 64 bit ELF library Unix
lib/libaelf64.a: obj/instrset64.o64 obj/procname64.o64 \
obj/cpuid64.o64 obj/rdtsc64.o64 obj/round64.o64 \
obj/memcpy64.o64 obj/memmove64.o64 obj/memset64.o64 obj/memcmp64.o64 \
obj/strlen64.o64 obj/strcpy64.o64 obj/strcat64.o64 \
obj/strstr64.o64 obj/strcmp64.o64 obj/stricmp64.o64 \
obj/strtouplow64.o64 obj/substring64.o64 obj/strspn64.o64 \
obj/strcountutf864.o64 obj/strcountset64.o64 \
obj/divfixedi64.o64 obj/divfixedv64.o64 obj/popcount64.o64 \
obj/physseed64.o64 obj/mother64.o64 obj/mersenne64.o64 \
obj/sfmt64.o64 \
obj/cputype64.o64 obj/debugbreak64.o64 obj/unalignedisfaster64.o64 \
obj/cachesize64.o64
  objconv -felf64 -nu -wex -wd1029 -lib $@ $?

# Convert these libraries to other versions:
  
# 32 bit COFF library, override version
lib/libacof32o.lib: lib/libacof32.lib
  objconv -fcof32 -np:?OVR_:_ -wex $** $@

# 32 bit OMF library
lib/libaomf32.lib: lib/libacof32.lib  
  objconv -fomf32 -nu -wex $** $@
  
# 32 bit OMF library, override version
lib/libaomf32o.lib: lib/libacof32o.lib  
  objconv -fomf32 -nu -wex $** $@
  
# 32 bit ELF library, override, position dependent
lib/libaelf32o.a: lib/libaelf32.a
  objconv -felf32 -np:?OVR_: -wex $** $@

# 32 bit ELF library, override, position independent
lib/libaelf32op.a: lib/libaelf32p.a
  objconv -felf32 -np:?OVR_: -wex $** $@

# 32 bit Mach-O library, position dependent
lib/libamac32.a: lib/libaelf32.a
  objconv -fmac32 -nu -wex -wd1050 $** $@
  
# 32 bit Mach-O library, position independent
lib/libamac32p.a: lib/libaelf32p.a
  objconv -fmac32 -nu -wex -wd1050 $** $@
  
# 32 bit Mach-O library, override
lib/libamac32o.a: lib/libaelf32o.a
  objconv -fmac32 -nu -wex -wd1050 $** $@
  
# 32 bit Mach-O library, override, position independent
lib/libamac32op.a: lib/libaelf32op.a
  objconv -fmac32 -nu -wex -wd1050 $** $@  

# Make 64 bit COFF library, override
lib/libacof64o.lib: lib/libacof64.lib
  objconv -fcof64 -np:?OVR_: -wex $** $@

# 64 bit ELF library, override
lib/libaelf64o.a: lib/libaelf64.a
  objconv -felf64 -np:?OVR_: -wex -wd1029 $** $@

# 64 bit Mach-O library
lib/libamac64.a: lib/libaelf64.a
  objconv -fmac64 -nu -wex $** $@
  
# 64 bit Mach-O library, override
lib/libamac64o.a: lib/libaelf64o.a
  objconv -fmac64 -nu -wex $** $@

# Convert 32 bit COFF library to DLL
lib/libad32.dll: lib/libacof32.lib obj/libad32.obj32 asm/libad32.def
  link /DLL /DEF:asm\libad32.def /SUBSYSTEM:WINDOWS /NODEFAULTLIB /ENTRY:DllEntry obj\libad32.obj32 lib/libacof32.lib
  move libad32.* lib\

# Convert 64 bit COFF library to DLL
lib/libad64.dll: lib/libacof64.lib obj/libad64.obj64 asm/libad64.def
  link /DLL /DEF:asm\libad64.def /SUBSYSTEM:WINDOWS /LIBPATH:$(libpath64) /ENTRY:DllEntry obj\libad64.obj64 lib/libacof64.lib
  move libad64.* lib\

  
# Object files for inteldispatchpatch.zip:

patch/dispatchpatch32.obj: obj/dispatchpatch32.obj32
  copy obj\dispatchpatch32.obj32 patch\dispatchpatch32.obj
# Note: copy must have '\', not '/'
  
patch/dispatchpatch32.o: obj/dispatchpatch32.o32pic
  copy obj\dispatchpatch32.o32pic patch\dispatchpatch32.o
  
patch/dispatchpatch32.mac.o: obj/dispatchpatch32.o32pic  
  objconv -fmac32 -nu -wex -wd1050 $** $@  

patch/dispatchpatch64.obj: obj/dispatchpatch64.obj64
  copy obj\dispatchpatch64.obj64 patch\dispatchpatch64.obj
  
patch/dispatchpatch64.o: obj/dispatchpatch64.o64
  copy obj\dispatchpatch64.o64 patch\dispatchpatch64.o
  
patch/dispatchpatch64.mac.o: obj/dispatchpatch64.o64
  objconv -fmac64 -nu -wex $** $@  


# Generic rules for assembling

# Generic rule for assembling 32-bit code for Windows (position dependent)
{asm\}.asm{obj\}.obj32:
  nasm -fwin32 -DWINDOWS -Worphan-labels -Werror -o $*.obj32 $<
# ML /c /Cx /W3 /coff /Fl /Fo$*.obj32

# Generic rule for assembling 32-bit for Unix, position-dependent
{asm\}.asm{obj\}.o32:
  nasm -felf32 -DUNIX -Worphan-labels -Werror -o $*.o32 $<
  objconv -felf32 -nu- -wd2005 $*.o32 $*.o32
  
# Generic rule for assembling 32-bit for Unix, position-independent
{asm\}.asm{obj\}.o32pic:
  yasm -felf32 -DUNIX -DPOSITIONINDEPENDENT -Worphan-labels -Werror -o $*.o32pic $<
  objconv -felf32 -nu- -wd2005 $*.o32pic $*.o32pic
  
# Note: re-insert -Werror when nasm bug fixed
# nasm -fwin64 -DWINDOWS -Worphan-labels -Werror -o $*.obj64 $<

# Generic rule for assembling 64-bit code for Windows
{asm\}.asm{obj\}.obj64:
  nasm -fwin64 -DWINDOWS -Worphan-labels -o $*.obj64 $<

# Generic rule for assembling 64-bit code for Linux, BSD, Mac
{asm\}.asm{obj\}.o64:
  nasm -felf64 -DUNIX -Worphan-labels -o $*.o64 $<
