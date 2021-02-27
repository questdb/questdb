;*************************  strcmp64.asm  ************************************
; Author:           Agner Fog
; Date created:     2011-07-14
; Last modified:    2012-07-07

; Description:
; Faster version of the standard strcmp function:
; int A_strcmp(const char * s1, const char * s2);
; Tests if two strings are equal. The strings must be zero-terminated.
;
; Note that this function may read up to 15 bytes beyond the end of the strings.
; This is rarely a problem but it can in principle generate a protection violation
; if a string is placed at the end of the data segment.
;
; Overriding standard function strcmp:
; The alias ?OVR_strcmp is changed to _strcmp in the object file if
; it is desired to override the standard library function strcmp.
; Overriding is disabled because the function may read beyond the end of a 
; string, while the standard strcmp function is guaranteed to work in all cases.
;
; Position-independent code is generated if POSITIONINDEPENDENT is defined.
;
; CPU dispatching included for 386, and SSE4.2 instruction sets.
;
; Copyright (c) 2011 - 2012 GNU General Public License www.gnu.org/licenses
;******************************************************************************
default rel

%define ALLOW_OVERRIDE 0               ; Set to one if override of standard function desired

global A_strcmp             ; Function A_strcmp

; Direct entries to CPU-specific versions
global strcmpGeneric            ; Generic version for processors without SSE4.2
global strcmpSSE42          ; Version for processors with SSE4.2

; Imported from instrset32.asm:
extern InstructionSet                 ; Instruction set for CPU dispatcher

section .text

; strcmp function

%if ALLOW_OVERRIDE
global ?OVR_strcmp
?OVR_strcmp:
%endif

A_strcmp: ; function dispatching

        jmp     near [strcmpDispatch] ; Go to appropriate version, depending on instruction set

align 16
strcmpSSE42:
%ifdef  WINDOWS
        push    rdi
        mov     rdi, rcx
%define rs1     rdi                    ; pointer to string 1
%define rs2     rdx                    ; pointer to string 2
%define par1    rcx
%define par2    rdx
%else   ; UNIX
%define rs1     rdi
%define rs2     rsi
%define par1    rdi
%define par2    rsi
%endif

        mov     rax, -16               ; offset counter
compareloop:
        add     rax, 16                ; increment offset
        movdqu  xmm1, [rs1+rax]        ; read 16 bytes of string 1
        pcmpistri xmm1, [rs2+rax], 00011000B ; unsigned bytes, equal each, invert. returns index in ecx
        jnbe    compareloop            ; jump if not carry flag and not zero flag
        
        jnc     equal
notequal:
        ; strings are not equal
        add     rcx, rax               ; offset to first differing byte
        movzx   eax, byte [rs1+rcx]    ; compare first differing byte
        movzx   edx, byte [rs2+rcx]
		sub     rax, rdx
%ifdef  WINDOWS
        pop     rdi
%endif
		ret

equal:
        xor     eax, eax               ; strings are equal
%ifdef  WINDOWS
        pop     rdi
%endif
		ret

;strcmpSSE42: endp


align 16
strcmpGeneric:  ; generic version
; This is a very simple solution. There is not much gained by using SSE2 or anything complicated
%ifdef  WINDOWS
%define ss1     rcx                    ; pointer to string 1
%define ss2     rdx                    ; pointer to string 2
%else   ; UNIX
%define ss1     rdi
%define ss2     rsi
%endif

		
_compareloop:
        mov     al, [ss1]
        cmp     al, [ss2]
        jne     _notequal
        test    al, al
        jz      _equal
        inc     ss1
        inc     ss2
        jmp     _compareloop        
        
_equal: xor     eax, eax               ; strings are equal
		ret

_notequal:                             ; strings are not equal
        movzx   eax, byte [ss1]        ; compare first differing byte
        movzx   edx, byte [ss2]
		sub     eax, edx
		ret
		
;strcmpGeneric end


; CPU dispatching for strcmp. This is executed only once
strcmpCPUDispatch:
        ; get supported instruction set
        push    par1
        push    par2
        call    InstructionSet
        pop     par2
        pop     par1
        ; Point to generic version of strcmp
        lea     r9, [strcmpGeneric]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strcmp
        lea     r9, [strcmpSSE42]
Q100:   mov     [strcmpDispatch], r9
        ; Continue in appropriate version of strcmp
        jmp     r9

SECTION .data

; Pointer to appropriate version. Initially points to dispatcher
strcmpDispatch DQ strcmpCPUDispatch

; Append 16 bytes to end of last data section to allow reading past end of strings:
; (We might use names .bss$zzz etc. under Windows to make it is placed
; last, but the assembler gives sections with unknown names wrong attributes.
; Here, we are just relying on library data being placed after main data.
; This can be verified by making a link map file)
SECTION .bss
        dq      0, 0
