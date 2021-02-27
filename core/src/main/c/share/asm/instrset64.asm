;*************************  instrset64.asm  **********************************
; Author:           Agner Fog
; Date created:     2003-12-12
; Last modified:    2018-04-24
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 64 bit
;
; C++ prototype:
; extern "C" int InstructionSet (void);
;
; Description:
; This function returns an integer indicating which instruction set is
; supported by the microprocessor and operating system. A program can
; call this function to determine if a particular set of instructions can
; be used.
;
; The method used here for detecting whether XMM instructions are enabled by
; the operating system is different from the method recommended by Intel.
; The method used here has the advantage that it is independent of the 
; ability of the operating system to catch invalid opcode exceptions. For 
; further discussion of this method, see my manual "Optimizing subroutines
; in assembly language" (www.agner.org/optimize/).
; 
; Copyright (c) 2003-2018 GNU General Public License www.gnu.org/licenses
;******************************************************************************
;
; ********** InstructionSet function **********
; C++ prototype:
; extern "C" int InstructionSet (void);
;
; return value:
;  0 =  80386 instruction set only
;  1 or above = MMX instructions supported
;  2 or above = conditional move and FCOMI supported
;  3 or above = SSE (XMM) supported by processor and operating system
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
; 15 or above = AVX512F supported
; 16 or above = AVX512BW, AVX512DQ, AVX512VL supported
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

default rel

global InstructionSet
global IInstrSet


SECTION .data
align 16

IInstrSet@:                            ; local name to avoid problems in shared objects
IInstrSet:  dd      -1                 ; this global variable is valid after first call


SECTION .text  align=16

; ********** InstructionSet function **********
; C++ prototype:
; extern "C" int InstructionSet (void);


InstructionSet:
        ; Check if this function has been called before
        mov     eax, [IInstrSet@]
        test    eax, eax
        js      FirstTime              ; Negative means first time
        ; Early return. Has been called before
        ret                            ; Return value is in eax

FirstTime:
        push    rbx

        mov     eax, 1
        cpuid                          ; get features into edx and ecx
        
        mov     eax, 4                 ; at least SSE2 supported in 64 bit mode
        test    ecx, 1                 ; SSE3 support by microprocessor
        jz      ISEND
        inc     eax                    ; 5
        
        bt      ecx, 9                 ; Suppl-SSE3 support by microprocessor
        jnc     ISEND
        inc     eax                    ; 6
        
        bt      ecx, 19                ; SSE4.1 support by microprocessor
        jnc     ISEND
        mov     al, 8                  ; 8        
        
        bt      ecx, 23                ; POPCNT support by microprocessor
        jnc     ISEND
        inc     eax                    ; 9
        
        bt      ecx, 20                ; SSE4.2 support by microprocessor
        jnc     ISEND
        inc     eax                    ; 10

        ; check OS support for YMM registers (AVX)
        bt      ecx, 27                ; OSXSAVE: XGETBV supported
        jnc     ISEND
        push    rax
        push    rcx
        push    rdx
        xor     ecx, ecx
        xgetbv                         ; db 0FH, 01H, 0D0H         ; XGETBV
        and     eax, 6
        cmp     eax, 6                 ; AVX support by OS
        pop     rdx
        pop     rcx
        pop     rax
        jne     ISEND

        bt      ecx, 28                ; AVX support by microprocessor
        jnc     ISEND
        inc     eax                    ; 11
        
        bt      ecx, 1                 ; PCLMUL support
        jnc     ISEND
        bt      ecx, 25                ; AES support
        jnc     ISEND
        inc     eax                    ; 12
        
        push    rax
        push    rcx
        mov     eax, 7
        xor     ecx, ecx
        cpuid                          ; check for AVX2
        bt      ebx, 5
        pop     rcx
        pop     rax
        jnc     ISEND
        inc     eax                    ; 13
        
; 14 or above = FMA3, F16C, BMI1, BMI2, LZCNT
        bt      ecx, 12                ; FMA3
        jnc     ISEND
        bt      ecx, 29                ; F16C
        jnc     ISEND
        bt      ebx, 3                 ; BMI1
        jnc     ISEND
        bt      ebx, 8                 ; BMI2
        jnc     ISEND
        
        push    rax
        push    rbx
        push    rcx
        mov     eax, 80000001H
        cpuid
        bt      ecx, 5                 ; LZCNT
        pop     rcx
        pop     rbx
        pop     rax
        jnc     ISEND
        inc     eax                    ; 14

        bt      ebx, 16                ; AVX512f
        jnc     ISEND
        push    rax
        push    rcx
        push    rdx
        xor     ecx, ecx
        xgetbv  
        and     al, 0xE0
        cmp     al, 0xE0               ; AVX512 support by OS
        pop     rdx
        pop     rcx
        pop     rax
        jne     ISEND
        inc     eax                    ; 15

        bt      ebx, 17                ; AVX512DQ
        jnc     ISEND
        bt      ebx, 30                ; AVX512BW
        jnc     ISEND
        bt      ebx, 31                ; AVX512VL
        jnc     ISEND
        inc     eax                    ; 16
       
ISEND:  mov     [IInstrSet@], eax      ; save value in global variable

        pop     rbx
        ret                            ; return value is in eax

;InstructionSet ENDP
