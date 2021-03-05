;*************************  popcount32.asm  ************************************
; Author:           Agner Fog
; Date created:     2011-07-20
; Last modified:    2011-08-21

; Description:
; Population count function. Counts the number of 1-bits in a 32-bit integer
; unsigned int A_popcount (unsigned int x);
;
; Position-independent code is generated if POSITIONINDEPENDENT is defined.
;
; CPU dispatching included for 386 and SSE4.2 instruction sets.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************

global _A_popcount

; Direct entries to CPU-specific versions
global _popcountGeneric
global _popcountSSE42

; Imported from instrset32.asm:
extern _InstructionSet                 ; Instruction set for CPU dispatcher

section .text

;******************************************************************************
;                               popcount function
;******************************************************************************


_A_popcount: ; function dispatching

%IFNDEF POSITIONINDEPENDENT
        jmp     near [popcountDispatch] ; Go to appropriate version, depending on instruction set

%ELSE   ; Position-independent code

        call    get_thunk_edx          ; get reference point for position-independent code
RP1:                                   ; reference point edx = offset RP1

; Make the following instruction with address relative to RP1:
        jmp     near [edx+popcountDispatch-RP1]

%ENDIF

align 16
_popcountSSE42: ; SSE4.2 version
        popcnt  eax, dword [esp+4]
        ret


;******************************************************************************
;                               popcount function generic
;******************************************************************************

_popcountGeneric: ; Generic version
        mov     eax, [esp+4]           ; x
        mov     edx, eax
        shr     eax, 1
        and     eax, 55555555h         ; odd bits in eax, even bits in edx
        and     edx, 55555555h
        add     eax, edx
        mov     edx, eax
        shr     eax, 2
        and     eax, 33333333h
        and     edx, 33333333h
        add     eax, edx
        mov     edx, eax
        shr     eax, 4
        add     eax, edx
        and     eax, 0F0F0F0Fh
        mov     edx, eax
        shr     eax, 8
        add     eax, edx
        mov     edx, eax
        shr     eax, 16
        add     eax, edx
        and     eax, 03FH
        ret
;_popcountGeneric end

; ********************************************************************************

%IFDEF  POSITIONINDEPENDENT
get_thunk_edx: ; load caller address into edx for position-independent code
        mov edx, [esp]
        ret
%ENDIF

; ********************************************************************************
; CPU dispatching for popcount. This is executed only once
; ********************************************************************************

popcountCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version
        mov     ecx, _popcountGeneric
        cmp     eax, 9                ; check popcnt supported
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        mov     ecx, _popcountSSE42
Q100:   mov     [popcountDispatch], ecx
        ; Continue in appropriate version 
        jmp     ecx

%ELSE   ; Position-independent version
        ; get supported instruction set
        call    _InstructionSet
        call    get_thunk_edx
RP10:   ; reference point edx
        ; Point to generic version
        lea     ecx, [edx+_popcountGeneric-RP10]
        cmp     eax, 9                ; check popcnt supported
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     ecx, [edx+_popcountSSE42-RP10]
Q100:   mov     [edx+popcountDispatch-RP10], ecx
        ; Continue in appropriate version
        jmp     ecx
%ENDIF


SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
popcountDispatch  DD popcountCPUDispatch

%IFDEF POSITIONINDEPENDENT
; Fix potential problem in Mac linker
        DD      0, 0
%ENDIF
