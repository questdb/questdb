;*************************  strcountutf864.asm  ***********************************
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
; CPU dispatching included for SSE2 and SSE4.2 instruction sets.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************
default rel

global strcount_UTF8

; Direct entries to CPU-specific versions
global strcount_UTF8Generic
global strcount_UTF8SSE42

; Imported from instrset64.asm:
extern InstructionSet                 ; Instruction set for CPU dispatcher

section .data
align  16
byterange: times 8  DB 10000000b, 10111111b ; range for UTF-8 continuation bytes

section .text

;******************************************************************************
;                               strcount_UTF8 function
;******************************************************************************


strcount_UTF8: ; function dispatching

        jmp     near [strcount_UTF8Dispatch] ; Go to appropriate version, depending on instruction set


;******************************************************************************
;                        strcount_UTF8 function SSE4.2 version
;******************************************************************************

%ifdef  WINDOWS
%define  par1  rcx
%else
%define  par1  rdi
%endif

align 16
strcount_UTF8SSE42: ; SSE4.2 version
        movdqa  xmm1, [byterange]      ; define range of continuation bytes to ignore
        xor     edx, edx               ; character counter
str_next:
        pcmpistrm xmm1, [par1], 00110100b; check range, invert valid bits, return bit mask in xmm0
        movd    eax, xmm0
        jz      str_finished           ; terminating zero found
        popcnt  eax, eax               ; count
        add     rdx, rax
        add     par1, 16
        jmp     str_next

str_finished:
        popcnt  eax, eax
        add     rax, rdx
        ret


;******************************************************************************
;                        strcount_UTF8 function generic
;******************************************************************************

align 8
strcount_UTF8Generic:
        xor     eax, eax               ; character counter
        xor     edx, edx               ; zero extend dl
str_next1:
        mov     dl, [par1]             ; one byte fron string
        test    dl, dl
        jz      str_finished1          ; terminating zero
        sub     dl, 10000000b          ; lower limit of continuation bytes
        cmp     dl, 00111111b          ; upper limit - lower limit
        seta    dl                     ; 1 if outside limit (unsigned compare includes negative values as above)
        add     rax, rdx
        inc     par1
        jmp     str_next1
        
str_finished1:
        ret
;_strcount_UTF8Generic end


; ********************************************************************************
; CPU dispatching for strcount_UTF8. This is executed only once
; ********************************************************************************

strcount_UTF8CPUDispatch:
        ; get supported instruction set
        push    par1
        call    InstructionSet
        pop     par1
        ; Point to generic version of strstr
        lea     rdx, [strcount_UTF8Generic]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     rdx, [strcount_UTF8SSE42]
Q100:   mov     [strcount_UTF8Dispatch], rdx
        ; Continue in appropriate version 
        jmp     rdx

SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
strcount_UTF8Dispatch  DQ strcount_UTF8CPUDispatch

SECTION .bss
dq 0, 0
