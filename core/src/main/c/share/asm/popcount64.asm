;*************************  popcount64.asm  ************************************
; Author:           Agner Fog
; Date created:     2011-07-20
; Last modified:    2011-07-20

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
default rel

global A_popcount

; Direct entries to CPU-specific versions
global popcountGeneric
global popcountSSE42

; Imported from instrset32.asm:
extern InstructionSet                 ; Instruction set for CPU dispatcher

section .text

;******************************************************************************
;                               popcount function
;******************************************************************************


A_popcount: ; function dispatching
        jmp     near [popcountDispatch] ; Go to appropriate version, depending on instruction set

align 16
popcountSSE42: ; SSE4.2 version
%ifdef  WINDOWS
        popcnt  eax, ecx
%else
        popcnt  eax, edi
%endif        
        ret


;******************************************************************************
;                               popcount function generic
;******************************************************************************

popcountGeneric: ; Generic version
%ifdef  WINDOWS
        mov     eax, ecx
%else
        mov     eax, edi
%endif        
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
;popcountGeneric end

; ********************************************************************************
; CPU dispatching for popcount. This is executed only once
; ********************************************************************************

%ifdef  WINDOWS
%define par1      rcx                  ; parameter 1, pointer to haystack
%else
%define par1      rdi                  ; parameter 1, pointer to haystack
%endif

popcountCPUDispatch:
        ; get supported instruction set
        push    par1
        call    InstructionSet
        pop     par1
        ; Point to generic version of strstr
        lea     rdx, [popcountGeneric]
        cmp     eax, 9                ; check popcnt supported
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     rdx, [popcountSSE42]
Q100:   mov     [popcountDispatch], rdx
        ; Continue in appropriate version 
        jmp     rdx

SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
popcountDispatch  DQ popcountCPUDispatch
