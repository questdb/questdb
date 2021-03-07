;***********************  dispatchpatch64.asm  ********************************
; Author:           Agner Fog
; Date created:     2007-07-20
; Last modified:    2014-07-30
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 64 bit
;
; C++ prototype:
; extern "C" int  __intel_cpu_indicator = 0;
; extern "C" void __intel_cpu_indicator_init()
;
; Description:
; Example of how to replace Intel CPU dispatcher in order to improve 
; compatibility of Intel function libraries with non-Intel processors.
; Only works with static link libraries (*.lib, *.a), not dynamic libraries
; (*.dll, *.so). Linking in this as an object file will override the functions
; with the same name in the library.; 
; 
; Copyright (c) 2007-2014 GNU LGPL License v. 3.0 www.gnu.org/licenses/lgpl.html
;******************************************************************************

; extern InstructionSet
%ifdef __YASM_VER__
  %include "instrset64.asm"              ; include code for _InstructionSet function
%else
  %include "../instrset64.asm"          ; nasm looks in current path, not the path of the file
%endif

; InstructionSet function return value:
;  4 or above = SSE2 supported
;  5 or above = SSE3 supported
;  6 or above = Supplementary SSE3
;  8 or above = SSE4.1 supported
;  9 or above = POPCNT supported
; 10 or above = SSE4.2 supported
; 11 or above = AVX supported by processor and operating system
; 12 or above = PCLMUL and AES supported
; 13 or above = AVX2 supported
; 14 or above = FMA3, F16C, BMI1, BMI2, LZCNT
; 15 or above = AVX512F

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  Dispatcher for Intel standard libraries and SVML library,
;  old versions
;
;  __intel_cpu_indicator is for older versions of Intel compiler
;  version 14.0 uses __intel_cpu_features_init_x() instead
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

global __intel_cpu_indicator
global __intel_cpu_indicator_init


SECTION .data
intel_cpu_indicator@:                  ; local name
__intel_cpu_indicator: dd 0

; table of indicator values
itable  DD      1                      ; 0: generic version, 80386 instruction set
        DD      8, 8                   ; 1,   2: MMX
        DD      0x80                   ; 3:      SSE
        DD      0x200                  ; 4:      SSE2
        DD      0x800                  ; 5:      SSE3
        DD      0x1000,  0x1000        ; 6,   7: SSSE3
        DD      0x2000,  0x2000        ; 8,   9: SSE4.1
        DD      0x8000,  0x8000        ; 10, 11: SSE4.2 and popcnt
        DD      0x20000, 0x20000       ; 12, 13: AVX, pclmul, aes
        DD      0x400000               ; 14:     AVX2, F16C, BMI1, BMI2, LZCNT, FMA3
;        DD      0x800000               ; 15:     HLE, RTM
itablelen equ ($ - itable) / 4         ; length of table

SECTION .text

__intel_cpu_indicator_init:
        push    rax                    ; registers must be pushed
        push    rcx
        push    rdx
        push    r8
        push    r9
        push    r10
        push    r11
        push    rsi
        push    rdi
        call    InstructionSet
        cmp     eax, itablelen
        jb      L100
        mov     eax, itablelen - 1     ; limit to table length
L100:   lea     rdx, [rel itable]
        mov     eax, [rdx + 4*rax]
        mov     [rel intel_cpu_indicator@], eax             ; store in __intel_cpu_indicator
        pop     rdi
        pop     rsi
        pop     r11
        pop     r10
        pop     r9
        pop     r8
        pop     rdx
        pop     rcx
        pop     rax
        ret

;__intel_cpu_indicator_init ENDP


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;     Dispatcher for Math Kernel Library (MKL),
;     version 10.2 and higher
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

global mkl_serv_cpu_detect

SECTION .data
; table of indicator values
; Note: the table is different in 32 bit and 64 bit mode

mkltab  DD      0, 0, 0, 0             ; 0-3: generic version, 80386 instruction set
        DD      0                      ; 4:      SSE2
		DD      1                      ; 5:      SSE3
        DD      2                      ; 6:      SSSE3
        DD      2                      ; 7:      unused
        DD      2                      ; 8:      SSE4.1
        DD      2                      ; 9:      POPCNT
        DD      3                      ; 10:     SSE4.2
        DD      4                      ; 11:     AVX
        DD      4                      ; 12:     PCLMUL, AES
        DD      4                      ; 13:     AVX2
        DD      5                      ; 14:     FMA3, BMI1/2, LZCNT
;        DD      5                      ; 15:     AVX512F
mkltablen equ ($ - mkltab) / 4         ; length of table

SECTION .text

mkl_serv_cpu_detect:
        push    rcx                    ; Perhaps not needed
        push    rdx
        push    r8
        push    r9
%ifdef WINDOWS
        push    rsi
        push    rdi
%endif
        call    InstructionSet
        cmp     eax, mkltablen
        jb      M100
        mov     eax, mkltablen - 1     ; limit to table length
M100:   
        lea     rdx, [rel mkltab]
        mov     eax, [rdx + 4*rax]
%ifdef WINDOWS
        pop     rdi
        pop     rsi
%endif
        pop     r9
        pop     r8
        pop     rdx
        pop     rcx
        ret
; end mkl_serv_cpu_detect 
       


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;     Dispatcher for Vector Math Library (VML)
;     version 14.0 and higher
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

global mkl_vml_serv_cpu_detect

SECTION .data
; table of indicator values
; Note: the table is different in 32 bit and 64 bit mode

vmltab  DD      0, 0, 0, 0             ; 0-3: generic version, 80386 instruction set
        DD      1                      ; 4:      SSE2
		DD      2                      ; 5:      SSE3
        DD      3                      ; 6:      SSSE3
        DD      3                      ; 7:      unused
        DD      4                      ; 8:      SSE4.1
        DD      4                      ; 9:      POPCNT
        DD      5                      ; 10:     SSE4.2
        DD      6                      ; 11:     AVX
        DD      6                      ; 12:     PCLMUL, AES
        DD      6                      ; 13:     AVX2
        DD      7                      ; 14:     FMA3, BMI1/2, LZCNT
;        DD      7                      ; 15:     AVX512F
vmltablen equ ($ - vmltab) / 4         ; length of table

SECTION .text

mkl_vml_serv_cpu_detect:
        push    rcx                    ; Perhaps not needed
        push    rdx
        push    r8
        push    r9
%ifdef WINDOWS
        push    rsi
        push    rdi
%endif
        call    InstructionSet
        cmp     eax, vmltablen
        jb      V100
        mov     eax, vmltablen - 1     ; limit to table length
V100:   
        lea     rdx, [rel vmltab]
        mov     eax, [rdx + 4*rax]
%ifdef WINDOWS
        pop     rdi
        pop     rsi
%endif
        pop     r9
        pop     r8
        pop     rdx
        pop     rcx
        ret
; end mkl_vml_serv_cpu_detect        


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;     Dispatcher for __intel_cpu_feature_indicator 
;     version 13 and higher
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

%if 0   ; Don't include this!

; __intel_cpu_features_init and __intel_cpu_features_init_x are 
; identical, except that the former checks the CPU brand, the
; latter does not. Don't override this function. Instead, set
; the indicator variables to 0 to force a re-evaluation,
; and call __intel_cpu_features_init_x.
; If you do want to override these functions then you must
; save all registers.


global __intel_cpu_features_init
global __intel_cpu_features_init_x
global __intel_cpu_feature_indicator
global __intel_cpu_feature_indicator_x
global __intel_cpu_fms_indicator
global __intel_cpu_fms_indicator_x

SECTION .data
; table of indicator values

intel_cpu_feature_indicator@:
__intel_cpu_feature_indicator:
__intel_cpu_feature_indicator_x  DD 0, 0
intel_cpu_fms_indicator@:
__intel_cpu_fms_indicator:
__intel_cpu_fms_indicator_x:     DD 0, 0


feattab DD  1                ; 0 default
        DD  0BH              ; 1 MMX
        DD  0FH              ; 2 conditional move and FCOMI supported
        DD  3FH              ; 3 SSE
        DD  7FH              ; 4 SSE2
        DD  0FFH             ; 5 SSE3
        DD  1FFH, 1FFH       ; 6 Supplementary SSE3
        DD  3FFH             ; 8 SSE4.1
        DD  0BFFH            ; 9 POPCNT 
        DD  0FFFH            ; 10 SSE4.2 
        DD  10FFFH           ; 11 AVX 
        DD  16FFFH           ; 12 PCLMUL and AES 
        DD  816FFFH          ; 13 AVX2 
        DD  9DEFFFH          ; 14 FMA3, F16C, BMI1, BMI2, LZCNT
        DD  0FDEFFFH         ; 15 HLE, RTM 

feattablen equ ($ - feattab) / 4  ; length of table

SECTION .text

__intel_cpu_features_init:
__intel_cpu_features_init_x:
        push    rcx 
        push    rdx
        push    r8
        push    r9
%ifdef WINDOWS
        push    rsi
        push    rdi
%endif
        call    InstructionSet
        cmp     eax, feattablen
        jb      F100
        mov     eax, vmltablen - 1     ; limit to table length
F100:   
        lea     rdx, [rel feattab]
        mov     ebx, [rdx + 4*rax]     ; look up in table        
        push    rbx
        mov     eax, 1
        cpuid
        pop     rbx
        bt      ecx, 22                ; MOVBE
        jnc     F200
        or      ebx, 1000H
F200:   mov     [intel_cpu_feature_indicator@], rbx

        ; get family and model
        mov     edx, eax
        and     eax, 0FH               ; stepping bit 0-3
        mov     ecx, edx
        shr     ecx, 4
        and     ecx, 0FH               ; model
        mov     ebx, edx
        shr     ebx, 12
        and     ebx, 0F0H              ; x model
        or      ecx, ebx               ; full model
        mov     ah,  cl                ; model bit 8 - 15
        mov     ecx, edx
        shr     ecx, 8
        and     ecx, 0FH               ; family
        mov     ebx, edx
        shr     ebx, 20
        and     ebx, 0FFH              ; x family
        add     ecx, ebx               ; full family
        shl     ecx, 16
        or      eax, ecx               ; full family bit 16 - 23
        mov     [intel_cpu_fms_indicator@], eax
        
%ifdef WINDOWS
        pop     rdi
        pop     rsi
%endif
        pop     r9
        pop     r8
        pop     rdx
        pop     rcx
        ret
; end __intel_cpu_features_init        

%endif
