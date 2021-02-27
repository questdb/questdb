;*************************  strcountinset32.asm  *********************************
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

global _strCountInSet

; Direct entries to CPU-specific versions
global _strCountInSetGeneric
global _strCountInSetSSE42

; Imported from instrset32.asm:
extern _InstructionSet                 ; Instruction set for CPU dispatcher

section .text

;******************************************************************************
;                               strCountInSet function
;******************************************************************************

_strCountInSet: ; function dispatching

%IFNDEF POSITIONINDEPENDENT
        jmp     near [strCountInSetDispatch] ; Go to appropriate version, depending on instruction set

%ELSE   ; Position-independent code

        call    get_thunk_edx          ; get reference point for position-independent code
RP1:                                   ; reference point edx = offset RP1

; Make the following instruction with address relative to RP1:
        jmp     near [edx+strCountInSetDispatch-RP1]

%ENDIF


align 16
_strCountInSetSSE42: ; SSE4.2 version
        push    esi
        push    edi
        mov     esi, [esp+12]          ; str
        mov     edi, [esp+16]          ; set
        xor     eax, eax               ; match counter
str_next:
        movdqu  xmm2, [esi]            ; str
        movdqu  xmm1, [edi]            ; set
        pcmpistrm xmm1, xmm2, 00000000b; find in set, return bit mask in xmm0
        movd    ecx, xmm0
        jns     set_extends            ; the set is more than 16 bytes
        jz      str_finished
set_finished:
        popcnt  ecx, ecx
        add     eax, ecx        
        ; first 16 characters checked, continue with next 16 characters (a terminating zero would never match)
        add     esi, 16                ; next 16 bytes of str
        jmp     str_next

set_and_str_finished:        
        or      ecx, edx               ; accumulate matches
str_finished:
        popcnt  ecx, ecx
        add     eax, ecx        
        pop     edi
        pop     esi
        ret

set_loop:
        or      ecx, edx               ; accumulate matches
set_extends:
        add     edi, 16
        movdqu  xmm1, [edi]            ; next part of set
        pcmpistrm xmm1, xmm2, 00000000b; find in set, return bit mask in xmm0
        movd    edx, xmm0
        jns     set_loop
        jz      set_and_str_finished
        mov     edi, [esp+16]          ; restore set pointer
        or      ecx, edx               ; accumulate matches
        jmp     set_finished
        
;_strCountInSetSSE42 end

;******************************************************************************
;                               strCountInSet function generic
;******************************************************************************

align 8
_strCountInSetGeneric: ; Generic version
        push    esi
        push    edi
        mov     esi, [esp+12]          ; str pointer
        mov     edi, [esp+16]          ; set pointer
        xor     eax, eax               ; match counter
str_next10:
        mov     cl, [esi]              ; read one byte from str
        test    cl, cl
        jz      str_finished10         ; str finished
set_next10:
        mov     dl, [edi]
        test    dl, dl
        jz      set_finished10
        inc     edi                    ; next in set
        cmp     cl, dl
        jne     set_next10
        ; character match found, goto next character
        inc     eax                    ; count match

set_finished10: ; end of set, no match found
        mov     edi, [esp+16]          ; restore set pointer
        inc     esi
        jmp     str_next10             ; next in string

str_finished10: ; end of str, count is in eax
        pop     edi
        pop     esi
        ret
;_strCountInSetGeneric end


; ********************************************************************************

%IFDEF  POSITIONINDEPENDENT
get_thunk_edx: ; load caller address into edx for position-independent code
        mov edx, [esp]
        ret
%ENDIF

; ********************************************************************************
; CPU dispatching for strCountInSet. This is executed only once
; ********************************************************************************

strCountInSetCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version of strstr
        mov     ecx, _strCountInSetGeneric
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        mov     ecx, _strCountInSetSSE42
Q100:   mov     [strCountInSetDispatch], ecx
        ; Continue in appropriate version 
        jmp     ecx

%ELSE   ; Position-independent version
        ; get supported instruction set
        call    _InstructionSet
        call    get_thunk_edx
RP11:   ; reference point edx
        ; Point to generic version
        lea     ecx, [edx+_strCountInSetGeneric-RP11]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     ecx, [edx+_strCountInSetSSE42-RP11]
Q100:   mov     [edx+strCountInSetDispatch-RP11], ecx
        ; Continue in appropriate version
        jmp     ecx
%ENDIF


SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
strCountInSetDispatch  DD strCountInSetCPUDispatch

%IFDEF POSITIONINDEPENDENT
; Fix potential problem in Mac linker
        DD      0, 0
%ENDIF

SECTION .bss
resq 4
