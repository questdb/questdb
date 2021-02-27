;*************************  instrset32.asm  **********************************
; Author:           Agner Fog
; Date created:     2003-12-12
; Last modified:    2018-04-24
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 32 bit
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
; ability of the operating system to catch invalid opcode exceptions. The
; method used here has been thoroughly tested on many different versions of
; Intel and AMD microprocessors, and is believed to work reliably. For further
; discussion of this method, see my manual "Optimizing subroutines in assembly
; language" (www.agner.org/optimize/).
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

global _InstructionSet
global _IInstrSet


SECTION .data
align 16
_IInstrSet:
_IInstrSet@: dd    -1                  ; local name

SECTION .text  align=16

%IFDEF POSITIONINDEPENDENT

; Local function for reading instruction pointer into edi
GetThunkEDX:
        mov     edx, [esp]
        ret

%ENDIF  ; POSITIONINDEPENDENT


_InstructionSet:
        
%IFDEF POSITIONINDEPENDENT
        ; Position-independent code for ELF and Mach-O shared objects:
        call    GetThunkEDX
        add     edx, _IInstrSet@ - $
        mov     eax, [edx]
%ELSE
        mov     eax, [_IInstrSet@]
%ENDIF        
        ; Check if this function has been called before
        test    eax, eax
        js      FirstTime              ; Negative means first time
        ret                            ; Early return. Has been called before

FirstTime:                             ; Function has not been called before
        push    ebx

%IFNDEF POSITIONINDEPENDENT
        mov     edx, _IInstrSet@       ; make edx point to _IInstrSet
%ENDIF
        push    edx                    ; save address of _IInstrSet
        
        ; detect if CPUID instruction supported by microprocessor:
        pushfd
        pop     eax
        btc     eax, 21                ; check if CPUID bit can toggle
        push    eax
        popfd
        pushfd
        pop     ebx
        xor     ebx, eax
        xor     eax, eax               ; 0
        bt      ebx, 21
        jc      ISEND                  ; CPUID not supported
        
        cpuid                          ; get number of CPUID functions
        test    eax, eax
        jz      ISEND                  ; function 1 not supported
        mov     eax, 1
        cpuid                          ; get features
        xor     eax, eax               ; 0
        
        test    edx, 1                 ; floating point support
        jz      ISEND
        bt      edx, 23                ; MMX support        
        jnc     ISEND
        inc     eax                    ; 1
        
        bt      edx, 15                ; conditional move support
        jnc     ISEND
        inc     eax                    ; 2

        ; check OS support for XMM registers (SSE)
        bt      edx, 24                ; FXSAVE support by microprocessor
        jnc     ISEND
        push    ecx
        push    edx
        mov     ebx, esp               ; save stack pointer
        sub     esp, 200H              ; allocate space for FXSAVE
        and     esp, -10H              ; align by 16
TESTDATA EQU 0D95A34BEH                ; random test value
TESTPS   EQU 10CH                      ; position to write TESTDATA = upper part of XMM6 image
        fxsave  [esp]                  ; save FP/MMX and XMM registers
        mov     ecx,[esp+TESTPS]       ; read part of XMM6 register
        xor     DWORD [esp+TESTPS],TESTDATA  ; change value
        fxrstor [esp]                  ; load changed value into XMM6
        mov     [esp+TESTPS],ecx       ; restore old value in buffer
        fxsave  [esp]                  ; save again
        mov     edx,[esp+TESTPS]       ; read changed XMM6 register
        mov     [esp+TESTPS],ecx       ; restore old value
        fxrstor [esp]                  ; load old value into XMM6
        xor     ecx, edx               ; get difference between old and new value
        mov     esp, ebx               ; restore stack pointer
        cmp     ecx, TESTDATA          ; test if XMM6 was changed correctly
        pop     edx
        pop     ecx
        jne     ISEND
        
        bt      edx, 25                ; SSE support by microprocessor
        jnc     ISEND
        inc     eax                    ; 3
        
        bt      edx, 26                ; SSE2 support by microprocessor
        jnc     ISEND
        inc     eax                    ; 4
        
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
        pushad
        xor     ecx, ecx
        xgetbv                         ; db 0FH, 01H, 0D0H         ; XGETBV
        and     eax, 6
        cmp     eax, 6                 ; AVX support by OS
        popad
        jne     ISEND
        
        bt      ecx, 28                ; AVX support by microprocessor
        jnc     ISEND
        inc     eax                    ; 11
        
        bt      ecx, 1                 ; PCLMUL support
        jnc     ISEND
        bt      ecx, 25                ; AES support
        jnc     ISEND
        inc     eax                    ; 12
        
        push    eax
        push    ecx
        mov     eax, 7
        xor     ecx, ecx
        cpuid                          ; check for AVX2
        bt      ebx, 5
        pop     ecx
        pop     eax
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
                
        push    eax
        push    ebx
        push    ecx
        mov     eax, 80000001H
        cpuid
        bt      ecx, 5                 ; LZCNT
        pop     ecx
        pop     ebx
        pop     eax
        jnc     ISEND        
        inc     eax                    ; 14

        bt      ebx, 16                ; AVX512f
        jnc     ISEND
        pushad
        xor     ecx, ecx
        xgetbv  
        and     al, 0x60
        cmp     al, 0x60               ; AVX512 support by OS
        popad
        jne     ISEND

        inc     eax                    ; 15
        
        bt      ebx, 17                ; AVX512DQ
        jnc     ISEND
        bt      ebx, 30                ; AVX512BW
        jnc     ISEND
        bt      ebx, 31                ; AVX512VL
        jnc     ISEND
        inc     eax                    ; 16

ISEND:  pop     edx                    ; address of _IInstrSet
        mov     [edx], eax             ; save value in public variable _IInstrSet
        pop     ebx
        ret                            ; return value is in eax

;_InstructionSet ENDP
