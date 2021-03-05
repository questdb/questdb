;*************************  strspn32.asm  ************************************
; Author:           Agner Fog
; Date created:     2011-07-19
; Last modified:    2011-08-21

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
%define ALLOW_OVERRIDE 0               ; Set to one if override of standard function desired

global _A_strspn
global _A_strcspn

; Direct entries to CPU-specific versions
global _strspnGeneric
global _strcspnGeneric
global _strspnSSE42
global _strcspnSSE42

; Imported from instrset32.asm:
extern _InstructionSet                 ; Instruction set for CPU dispatcher

section .text

;******************************************************************************
;                               strspn function
;******************************************************************************

%if ALLOW_OVERRIDE
global ?OVR_strspn
?OVR_strspn:
%endif

_A_strspn: ; function dispatching

%IFNDEF POSITIONINDEPENDENT
        jmp     near [strspnDispatch] ; Go to appropriate version, depending on instruction set

%ELSE   ; Position-independent code

        call    get_thunk_edx          ; get reference point for position-independent code
RP1:                                   ; reference point edx = offset RP1

; Make the following instruction with address relative to RP1:
        jmp     near [edx+strspnDispatch-RP1]

%ENDIF

align 16
_strspnSSE42: ; SSE4.2 version
        push    esi
        push    edi
        mov     esi, [esp+12]          ; str
        mov     edi, [esp+16]          ; set
        xor     ecx, ecx               ; span counter
str_next:
        movdqu  xmm2, [esi]            ; str
        movdqu  xmm1, [edi]            ; set
        pcmpistrm xmm1, xmm2, 00000000b; find in set, return bit mask in xmm0
        movd    eax, xmm0
        jns     set_extends
set_finished:
        cmp     ax, -1
        jne     str_finished
        ; first 16 characters matched, continue with next 16 characters (a terminating zero would never match)
        add     esi, 16                ; next 16 bytes of str
        add     ecx, 16                ; count span
        jmp     str_next

str_finished:
        not     eax
        bsf     eax, eax
        add     eax, ecx
        pop     edi
        pop     esi
        ret

set_loop:
        or      eax, edx               ; accumulate matches
set_extends: ; the set is more than 16 bytes
        add     edi, 16
        movdqu  xmm1, [edi]            ; next part of set
        pcmpistrm xmm1, xmm2, 00000000b; find in set, return bit mask in xmm0
        movd    edx, xmm0
        jns     set_loop
        mov     edi, [esp+16]          ; restore set pointer
        or      eax, edx               ; accumulate matches
        jmp     set_finished


;******************************************************************************
;                       strcspn function
;******************************************************************************

%if ALLOW_OVERRIDE
global ?OVR_strcspn
?OVR_strcspn:
%endif

_A_strcspn: ; function dispatching

%IFNDEF POSITIONINDEPENDENT
        jmp     near [strcspnDispatch] ; Go to appropriate version, depending on instruction set

%ELSE   ; Position-independent code

        call    get_thunk_edx          ; get reference point for position-independent code
RP2:                                   ; reference point edx = offset RP2

; Make the following instruction with address relative to RP2:
        jmp     near [edx+strcspnDispatch-RP2]

%ENDIF

align 16
_strcspnSSE42: ; SSE4.2 version
        push    esi
        push    edi
        mov     esi, [esp+12]          ; str
        mov     edi, [esp+16]          ; set
        xor     ecx, ecx               ; span counter
str_next2:
        movdqu  xmm2, [esi]            ; str
        movdqu  xmm1, [edi]            ; set
        pcmpistrm xmm1, xmm2, 00110000b; find in set, invert valid bits, return bit mask in xmm0
        movd    eax, xmm0
        jns     set_extends2
set_finished2:
        cmp     ax, -1
        jne     str_finished2
        ; first 16 characters matched, continue with next 16 characters (a terminating zero would never match)
        add     esi, 16                ; next 16 bytes of str
        add     ecx, 16                ; count span
        jmp     str_next2

str_finished2:
        not     eax
        bsf     eax, eax
        add     eax, ecx
        pop     edi
        pop     esi
        ret

set_loop2:
        and     eax, edx               ; accumulate matches
set_extends2: ; the set is more than 16 bytes
        add     edi, 16
        movdqu  xmm1, [edi]            ; next part of set
        pcmpistrm xmm1, xmm2, 00110000b; find in set, invert valid bits, return bit mask in xmm0
        movd    edx, xmm0
        jns     set_loop2
        mov     edi, [esp+16]          ; restore set pointer
        and     eax, edx               ; accumulate matches
        jmp     set_finished2


;******************************************************************************
;                               strspn function generic
;******************************************************************************

align 8
_strspnGeneric: ; Generic version
        push    esi
        push    edi
        mov     esi, [esp+12]          ; str pointer
str_next10:
        mov     edi, [esp+16]          ; set pointer
        mov     al, [esi]              ; read one byte from str
        test    al, al
        jz      str_finished10         ; str finished
set_next10:
        mov     dl, [edi]
        test    dl, dl
        jz      set_finished10
        inc     edi
        cmp     al, dl
        jne     set_next10
        ; character match found, goto next character
        inc     esi
        jmp     str_next10

str_finished10: ; end of str, all match
set_finished10: ; end of set, mismatch found
        sub     esi, [esp+12]          ; calculate position
        mov     eax, esi
        pop     edi
        pop     esi
        ret
;_strspnGeneric end

align 8
_strcspnGeneric: ; Generic version
        push    esi
        push    edi
        mov     esi, [esp+12]          ; str pointer
str_next20:
        mov     edi, [esp+16]          ; set pointer
        mov     al, [esi]              ; read one byte from str
        test    al, al
        jz      str_finished20         ; str finished
set_next20:
        mov     dl, [edi]
        test    dl, dl
        jz      set_finished20
        inc     edi
        cmp     al, dl
        jne     set_next20
        ; character match found, stop search
        jmp     str_finished20

set_finished20: ; end of set, mismatch found
        inc     esi
        jmp     str_next20

str_finished20: ; end of str, all match
        sub     esi, [esp+12]          ; calculate position
        mov     eax, esi
        pop     edi
        pop     esi
        ret
;_strcspnGeneric end

; ********************************************************************************

%IFDEF  POSITIONINDEPENDENT
get_thunk_edx: ; load caller address into edx for position-independent code
        mov edx, [esp]
        ret
%ENDIF

; ********************************************************************************
; CPU dispatching for strspn. This is executed only once
; ********************************************************************************

strspnCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version of strstr
        mov     ecx, _strspnGeneric
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        mov     ecx, _strspnSSE42
Q100:   mov     [strspnDispatch], ecx
        ; Continue in appropriate version 
        jmp     ecx

%ELSE   ; Position-independent version
        ; get supported instruction set
        call    _InstructionSet
        call    get_thunk_edx
RP11:   ; reference point edx
        ; Point to generic version
        lea     ecx, [edx+_strspnGeneric-RP11]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     ecx, [edx+_strspnSSE42-RP11]
Q100:   mov     [edx+strspnDispatch-RP11], ecx
        ; Continue in appropriate version
        jmp     ecx
%ENDIF

strcspnCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version of strstr
        mov     ecx, _strcspnGeneric
        cmp     eax, 10                ; check SSE4.2
        jb      Q200
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        mov     ecx, _strcspnSSE42
Q200:   mov     [strcspnDispatch], ecx
        ; Continue in appropriate version 
        jmp     ecx

%ELSE   ; Position-independent version
        ; get supported instruction set
        call    _InstructionSet
        call    get_thunk_edx
RP12:   ; reference point edx
        ; Point to generic version
        lea     ecx, [edx+_strcspnGeneric-RP12]
        cmp     eax, 10                ; check SSE4.2
        jb      Q200
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     ecx, [edx+_strcspnSSE42-RP12]
Q200:   mov     [edx+strcspnDispatch-RP12], ecx
        ; Continue in appropriate version
        jmp     ecx
%ENDIF

SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
strspnDispatch  DD strspnCPUDispatch
strcspnDispatch DD strcspnCPUDispatch

%IFDEF POSITIONINDEPENDENT
; Fix problem in Mac linker
        DD      0,0,0,0
%ENDIF


SECTION .bss
; Append 16 bytes to end of last data section to allow reading past end of strings:
; (We might use names .bss$zzz etc. under Windows to make it is placed
; last, but the assembler gives sections with unknown names wrong attributes.
; Here, we are just relying on library data being placed after main data.
; This can be verified by making a link map file)
        resq  4
