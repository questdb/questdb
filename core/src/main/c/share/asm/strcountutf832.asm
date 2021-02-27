;*************************  strcountutf832.asm  ***********************************
; Author:           Agner Fog
; Date created:     2011-07-20
; Last modified:    2013-09-11

; Description:
; size_t strcount_UTF8(const char * str);
; Counts the number of characters in a UTF-8 encoded string.
;
; This functions does not check if the string contains valid UTF-8 code, it 
; simply counts all bytes except continuation bytes 10xxxxxxB.
;
; Note that this functions may read up to 15 bytes beyond the end of the string.
; This is rarely a problem but it can in principle generate a protection violation
; if a string is placed at the end of the data segment.
;
; Position-independent code is generated if POSITIONINDEPENDENT is defined.
;
; CPU dispatching included for 386 and SSE4.2 instruction sets.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************

global _strcount_UTF8

; Direct entries to CPU-specific versions
global _strcount_UTF8Generic
global _strcount_UTF8SSE42

; Imported from instrset32.asm:
extern _InstructionSet                 ; Instruction set for CPU dispatcher

section .data
align  16
byterange: times 8  DB 10000000b, 10111111b ; range for UTF-8 continuation bytes

section .text

;******************************************************************************
;                               strcount_UTF8 function
;******************************************************************************


_strcount_UTF8: ; function dispatching

%IFNDEF POSITIONINDEPENDENT
        jmp     near [strcount_UTF8Dispatch] ; Go to appropriate version, depending on instruction set

%ELSE   ; Position-independent code

        call    get_thunk_edx          ; get reference point for position-independent code
RP1:                                   ; reference point edx = offset RP1

; Make the following instruction with address relative to RP1:
        jmp     near [edx+strcount_UTF8Dispatch-RP1]

%ENDIF

;******************************************************************************
;                        strcount_UTF8 function SSE4.2 version
;******************************************************************************
align 16
_strcount_UTF8SSE42: ; SSE4.2 version
        mov     edx,  [esp+4]          ; str
        movdqa  xmm1, [byterange]      ; define range of continuation bytes to ignore
        xor     ecx, ecx               ; character counter
str_next:
        pcmpistrm xmm1, [edx], 00110100b; check range, invert valid bits, return bit mask in xmm0
        movd    eax, xmm0
        jz      str_finished           ; terminating zero found
        popcnt  eax, eax               ; count
        add     ecx, eax
        add     edx, 16
        jmp     str_next

str_finished:
        popcnt  eax, eax
        add     eax, ecx
        ret


;******************************************************************************
;                        strcount_UTF8 function generic
;******************************************************************************

align 8
_strcount_UTF8Generic:
        mov     edx,  [esp+4]          ; str
        xor     eax, eax               ; character counter
        xor     ecx, ecx               ; zero extend cl
str_next1:
        mov     cl, [edx]              ; one byte fron string
        test    cl, cl
        jz      str_finished1          ; terminating zero
        sub     cl, 10000000b          ; lower limit of continuation bytes
        cmp     cl, 00111111b          ; upper limit - lower limit
        seta    cl                     ; 1 if outside limit (unsigned compare includes negative values as above)
        add     eax, ecx
        inc     edx
        jmp     str_next1
        
str_finished1:
        ret
;_strcount_UTF8Generic end


; ********************************************************************************

%IFDEF  POSITIONINDEPENDENT
get_thunk_edx: ; load caller address into edx for position-independent code
        mov edx, [esp]
        ret
%ENDIF

; ********************************************************************************
; CPU dispatching for strcount_UTF8. This is executed only once
; ********************************************************************************

strcount_UTF8CPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version of strstr
        mov     ecx, _strcount_UTF8Generic
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        mov     ecx, _strcount_UTF8SSE42
Q100:   mov     [strcount_UTF8Dispatch], ecx
        ; Continue in appropriate version 
        jmp     ecx

%ELSE   ; Position-independent version
        ; get supported instruction set
        call    _InstructionSet
        call    get_thunk_edx
RP11:   ; reference point edx
        ; Point to generic version
        lea     ecx, [edx+_strcount_UTF8Generic-RP11]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     ecx, [edx+_strcount_UTF8SSE42-RP11]
Q100:   mov     [edx+strcount_UTF8Dispatch-RP11], ecx
        ; Continue in appropriate version
        jmp     ecx
%ENDIF

SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
strcount_UTF8Dispatch  DD strcount_UTF8CPUDispatch

%IFDEF POSITIONINDEPENDENT
; Fix potential problem in Mac linker
        DD      0, 0
%ENDIF

SECTION .bss
resq 4
