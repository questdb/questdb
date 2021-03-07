;*************************  strspn64.asm  ************************************
; Author:           Agner Fog
; Date created:     2011-07-19
; Last modified:    2011-07-19

; Description:
; Faster version of the standard strspn and strcspn functions:
; size_t A_strspn (const char * str, const char * set);
; size_t A_strcspn(const char * str, const char * set);
;
; A_strspn finds the length of the initial portion of str which consists only of
; characters that are part of set. 
; A_strcspn finds the length of the initial portion of str which consists only of
; characters that are not part of set. 
;
; Note that these functions may read up to 15 bytes beyond the end of the strings.
; This is rarely a problem but it can in principle generate a protection violation
; if a string is placed at the end of the data segment.
;
; Overriding standard functions strspn and strcspn:
; Overriding is disabled because the functions may read beyond the end of a string, 
; while the standard strspn and strcspn functions are guaranteed to work in all cases.
;
; Position-independent code is generated if POSITIONINDEPENDENT is defined.
;
; CPU dispatching included for 386 and SSE4.2 instruction sets.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************
default rel

%define ALLOW_OVERRIDE 0               ; Set to one if override of standard function desired

global A_strspn
global A_strcspn

; Direct entries to CPU-specific versions
global strspnGeneric
global strcspnGeneric
global strspnSSE42
global strcspnSSE42

; Imported from instrset64.asm:
extern InstructionSet                 ; Instruction set for CPU dispatcher

section .text

;******************************************************************************
;                               strspn function
;******************************************************************************

%if ALLOW_OVERRIDE
global ?OVR_strspn
?OVR_strspn:
%endif

align 16
A_strspn: ; function dispatching
        jmp     near [strspnDispatch] ; Go to appropriate version, depending on instruction set

strspnSSE42: ; SSE4.2 version
%ifdef  WINDOWS
        push    rdi
        push    rsi
        mov     rdi, rcx               ; str
        mov     rsi, rdx               ; set
%endif
        mov     r8,  rsi
        xor     ecx, ecx               ; span counter
str_next:
        movdqu  xmm2, [rdi]            ; str
        movdqu  xmm1, [rsi]            ; set (the memory read port is likely to be vacant early, so no need to put this read outside the loop)
        pcmpistrm xmm1, xmm2, 00000000b; find in set, return bit mask in xmm0
        movd    eax, xmm0
        jns     set_extends
set_finished:
        cmp     ax, -1
        jne     str_finished
        ; first 16 characters matched, continue with next 16 characters (a terminating zero would never match)
        add     rdi, 16                ; next 16 bytes of str
        add     rcx, 16                ; count span
        jmp     str_next

str_finished:
        not     eax
        bsf     eax, eax
        add     rax, rcx
%ifdef  WINDOWS
        pop     rsi
        pop     rdi
%endif
        ret

set_loop:
        or      eax, edx               ; accumulate matches
set_extends: ; the set is more than 16 bytes
        add     rsi, 16
        movdqu  xmm1, [rsi]            ; next part of set
        pcmpistrm xmm1, xmm2, 00000000b; find in set, return bit mask in xmm0
        movd    edx, xmm0
        jns     set_loop
        mov     rsi, r8                ; restore set pointer
        or      eax, edx               ; accumulate matches
        jmp     set_finished


;******************************************************************************
;                       strcspn function
;******************************************************************************

%if ALLOW_OVERRIDE
global ?OVR_strcspn
?OVR_strcspn:
%endif

align 16
A_strcspn: ; function dispatching
        jmp     near [strcspnDispatch] ; Go to appropriate version, depending on instruction set

strcspnSSE42: ; SSE4.2 version
%ifdef  WINDOWS
        push    rdi
        push    rsi
        mov     rdi, rcx               ; str
        mov     rsi, rdx               ; set
%endif
        mov     r8,  rsi
        xor     ecx, ecx               ; span counter
str_next2:
        movdqu  xmm2, [rdi]            ; str
        movdqu  xmm1, [rsi]            ; set
        pcmpistrm xmm1, xmm2, 00110000b; find in set, invert valid bits, return bit mask in xmm0
        movd    eax, xmm0
        jns     set_extends2
set_finished2:
        cmp     ax, -1
        jne     str_finished2
        ; first 16 characters matched, continue with next 16 characters (a terminating zero would never match)
        add     rdi, 16                ; next 16 bytes of str
        add     rcx, 16                ; count span
        jmp     str_next2

str_finished2:
        not     eax
        bsf     eax, eax
        add     rax, rcx
%ifdef  WINDOWS
        pop     rsi
        pop     rdi
%endif
        ret

set_loop2:
        and     eax, edx               ; accumulate matches
set_extends2: ; the set is more than 16 bytes
        add     rsi, 16
        movdqu  xmm1, [rsi]            ; next part of set
        pcmpistrm xmm1, xmm2, 00110000b; find in set, invert valid bits, return bit mask in xmm0
        movd    edx, xmm0
        jns     set_loop2
        mov     rsi, r8                ; restore set pointer
        and     eax, edx               ; accumulate matches
        jmp     set_finished2


;******************************************************************************
;                               strspn function generic
;******************************************************************************

align 8
strspnGeneric: ; Generic version
%ifdef  WINDOWS
        push    rdi
        push    rsi
        mov     rdi, rcx               ; str
        mov     rsi, rdx               ; set
%endif
        mov     r8,  rsi
        mov     r9,  rdi

str_next10:
        mov     al, [rdi]              ; read one byte from str
        test    al, al
        jz      str_finished10         ; str finished
set_next10:
        mov     dl, [rsi]
        test    dl, dl
        jz      set_finished10
        inc     rsi
        cmp     al, dl
        jne     set_next10
        ; character match found, goto next character
        inc     rdi
        mov     rsi, r8                ; set pointer
        jmp     str_next10

str_finished10: ; end of str, all match
set_finished10: ; end of set, mismatch found
        sub     rdi, r9                ; calculate position
        mov     rax, rdi
%ifdef  WINDOWS
        pop     rsi
        pop     rdi
%endif
        ret
;_strspnGeneric end

align 8
strcspnGeneric: ; Generic version
%ifdef  WINDOWS
        push    rdi
        push    rsi
        mov     rdi, rcx               ; str
        mov     rsi, rdx               ; set
%endif
        mov     r8,  rsi
        mov     r9,  rdi
str_next20:
        mov     al, [rdi]              ; read one byte from str
        test    al, al
        jz      str_finished20         ; str finished
set_next20:
        mov     dl, [rsi]
        test    dl, dl
        jz      set_finished20
        inc     rsi
        cmp     al, dl
        jne     set_next20
        ; character match found, stop search
        jmp     str_finished20

set_finished20: ; end of set, mismatch found
        inc     rdi
        mov     rsi, r8                ; set pointer
        jmp     str_next20

str_finished20: ; end of str, all match
        sub     rdi, r9                ; calculate position
        mov     rax, rdi
%ifdef  WINDOWS
        pop     rsi
        pop     rdi
%endif
        ret
;_strcspnGeneric end


; ********************************************************************************
; CPU dispatching for strspn. This is executed only once
; ********************************************************************************

%ifdef  WINDOWS
%define par1    rcx
%define par2    rdx
%else   ; UNIX
%define par1    rdi
%define par2    rsi
%endif

strspnCPUDispatch:
        ; get supported instruction set
        push    par1
        push    par2
        call    InstructionSet
        pop     par2
        pop     par1
        ; Point to generic version of strstr
        lea     r8, [strspnGeneric]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     r8, [strspnSSE42]
Q100:   mov     [strspnDispatch], r8
        ; Continue in appropriate version 
        jmp     r8


strcspnCPUDispatch:
        ; get supported instruction set
        push    par1
        push    par2
        call    InstructionSet
        pop     par2
        pop     par1
        ; Point to generic version of strstr
        lea     r8, [strcspnGeneric]
        cmp     eax, 10                ; check SSE4.2
        jb      Q200
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     r8, [strcspnSSE42]
Q200:   mov     [strcspnDispatch], r8
        ; Continue in appropriate version 
        jmp     r8

SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
strspnDispatch  DQ strspnCPUDispatch
strcspnDispatch DQ strcspnCPUDispatch

SECTION .bss
dq 0, 0
