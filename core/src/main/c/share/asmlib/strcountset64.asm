;*************************  strcountinset64.asm  *********************************
; Author:           Agner Fog
; Date created:     2011-07-20
; Last modified:    2016-11-08

; Description:
; size_t strCountInSet(const char * str, const char * set);
;
; Counts how many characters in str that belong to the set defined by set.
; Both strings are zero-terminated ASCII strings.
;
; Note that this functions may read up to 15 bytes beyond the end of the strings.
; This is rarely a problem but it can in principle generate a protection violation
; if a string is placed at the end of the data segment.
;
; Position-independent code is generated if POSITIONINDEPENDENT is defined.
;
; CPU dispatching included for 386 and SSE4.2 instruction sets.
;
; Copyright (c) 2011-2016 GNU General Public License www.gnu.org/licenses
;******************************************************************************
default rel

global strCountInSet

; Direct entries to CPU-specific versions
global strCountInSetGeneric
global strCountInSetSSE42

; Imported from instrset64.asm:
extern InstructionSet                 ; Instruction set for CPU dispatcher

section .text

;******************************************************************************
;                               strCountInSet function
;******************************************************************************
%ifdef  WINDOWS
%define par1    rcx
%define par2    rdx
%else
%define par1    rdi
%define par2    rsi
%endif


strCountInSet: ; function dispatching
        jmp     near [strCountInSetDispatch] ; Go to appropriate version, depending on instruction set


align 16
strCountInSetSSE42: ; SSE4.2 version
%ifdef  WINDOWS
        push    rsi
        push    rdi
        mov     rdi, rcx               ; str
        mov     rsi, rdx               ; set
%endif
        mov     r8,  rsi
        xor     eax, eax               ; match counter
str_next:
        movdqu  xmm2, [rdi]            ; str
        movdqu  xmm1, [rsi]            ; set
        pcmpistrm xmm1, xmm2, 00000000b; find in set, return bit mask in xmm0
        movd    ecx, xmm0
        jns     set_extends            ; the set is more than 16 bytes
        jz      str_finished
set_finished:
        popcnt  ecx, ecx
        add     rax, rcx        
        ; first 16 characters checked, continue with next 16 characters (a terminating zero would never match)
        add     rdi, 16                ; next 16 bytes of str
        jmp     str_next

set_and_str_finished:        
        or      ecx, edx               ; accumulate matches
str_finished:
        popcnt  ecx, ecx
        add     rax, rcx
%ifdef  WINDOWS
        pop     rdi
        pop     rsi
%endif
        ret

set_loop:
        or      ecx, edx               ; accumulate matches
set_extends:
        add     rsi, 16
        movdqu  xmm1, [rsi]            ; next part of set
        pcmpistrm xmm1, xmm2, 00000000b; find in set, return bit mask in xmm0
        movd    edx, xmm0
        jns     set_loop
        jz      set_and_str_finished
        mov     rsi, r8                ; restore set pointer
        or      ecx, edx               ; accumulate matches
        jmp     set_finished
        
;strCountInSetSSE42 end

;******************************************************************************
;                               strCountInSet function generic
;******************************************************************************

align 8
strCountInSetGeneric: ; Generic version
%ifdef  WINDOWS
        push    rsi
        push    rdi
        mov     rdi, rcx               ; str
        mov     rsi, rdx               ; set
%endif
        mov     r8,  rsi
        xor     eax, eax               ; match counter
str_next10:
        mov     cl, [rdi]              ; read one byte from str
        test    cl, cl
        jz      str_finished10         ; str finished
set_next10:
        mov     dl, [rsi]
        test    dl, dl
        jz      set_finished10
        inc     rsi                    ; next in set
        cmp     cl, dl
        jne     set_next10
        ; character match found, goto next character
        inc     rax                    ; count match

set_finished10: ; end of set, no match found
        mov     rsi, r8                ; restore set pointer
        inc     rdi
        jmp     str_next10             ; next in string

str_finished10: ; end of str, count is in eax
%ifdef  WINDOWS
        pop     rdi
        pop     rsi
%endif
        ret
;strCountInSetGeneric end



; ********************************************************************************
; CPU dispatching for strCountInSet. This is executed only once
; ********************************************************************************

strCountInSetCPUDispatch:
        ; get supported instruction set
        push    par1
        push    par2
        call    InstructionSet
        pop     par2
        pop     par1
        ; Point to generic version of strstr
        lea     r8, [strCountInSetGeneric]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     r8, [strCountInSetSSE42]
Q100:   mov     [strCountInSetDispatch], r8
        ; Continue in appropriate version 
        jmp     r8


SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
strCountInSetDispatch  DQ strCountInSetCPUDispatch

SECTION .bss
dq 0, 0
