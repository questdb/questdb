;*************************  strcmp32.asm  ************************************
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
%define ALLOW_OVERRIDE 0               ; Set to one if override of standard function desired

global _A_strcmp             ; Function A_strcmp

; Direct entries to CPU-specific versions
global _strcmpGeneric            ; Generic version for processors without SSE4.2
global _strcmpSSE42          ; Version for processors with SSE4.2

; Imported from instrset32.asm:
extern _InstructionSet                 ; Instruction set for CPU dispatcher

section .text

; strcmp function

%if ALLOW_OVERRIDE
global ?OVR_strcmp
?OVR_strcmp:
%endif

_A_strcmp: ; function dispatching

%IFNDEF POSITIONINDEPENDENT
        jmp     dword [strcmpDispatch] ; Go to appropriate version, depending on instruction set

%ELSE   ; Position-independent code

        call    get_thunk_edx          ; get reference point for position-independent code
RP:                                    ; reference point edx = offset RP

; Make the following instruction with address relative to RP:
        jmp     near [edx+strcmpDispatch-RP]

%ENDIF

align 16
_strcmpSSE42:
		mov     eax, [esp+4]           ; string 1
		mov     edx, [esp+8]           ; string 2
		push    ebx
		mov     ebx, -16               ; offset counter

compareloop:
        add     ebx, 16                ; increment offset
        movdqu  xmm1, [eax+ebx]        ; read 16 bytes of string 1
        pcmpistri xmm1, [edx+ebx], 00011000B ; unsigned bytes, equal each, invert. returns index in ecx
        jnbe    compareloop            ; jump if not carry flag and not zero flag
        
        jnc     equal
notequal:                              ; strings are not equal
        ; strings are not equal
        add     ecx, ebx               ; offset to first differing byte
        movzx   eax, byte [eax+ecx]    ; compare bytes
        movzx   edx, byte [edx+ecx]
		sub     eax, edx
		pop     ebx
		ret
        
equal:
        xor     eax, eax               ; strings are equal
		pop     ebx
		ret

;_strcmpSSE42: endp


align 16
_strcmpGeneric:  ; generic version
; This is a very simple solution. There is not much gained by using SSE2 or anything complicated
		mov     ecx, [esp+4]          ; string 1
		mov     edx, [esp+8]          ; string 2
		
_compareloop:
        mov     al, [ecx]
        cmp     al, [edx]
        jne     _notequal
        test    al, al
        jz      _equal
        inc     ecx
        inc     edx
        jmp     _compareloop        
        
_equal: xor     eax, eax               ; strings are equal
		ret

_notequal:                             ; strings are not equal
        movzx   eax, byte [ecx]        ; compare first differing byte
        movzx   edx, byte [edx]
		sub     eax, edx
		ret
		
;_strcmpGeneric end


%IFDEF  POSITIONINDEPENDENT
get_thunk_edx: ; load caller address into edx for position-independent code
        mov edx, [esp]
        ret
%ENDIF

; CPU dispatching for strcmp. This is executed only once
strcmpCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version of strcmp
        mov     ecx, _strcmpGeneric
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strcmp
        mov     ecx, _strcmpSSE42
Q100:   mov     [strcmpDispatch], ecx
        ; Continue in appropriate version of strcmp
        jmp     ecx

%ELSE   ; Position-independent version
        ; get supported instruction set
        call    _InstructionSet
        call    get_thunk_edx
RP2:    ; reference point edx
        ; Point to generic version of strcmp
        lea     ecx, [edx+_strcmpGeneric-RP2]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strcmp
        lea     ecx, [edx+_strcmpSSE42-RP2]
Q100:   mov     [edx+strcmpDispatch-RP2], ecx
        ; Continue in appropriate version of strcmp
        jmp     ecx
%ENDIF

SECTION .data

; Pointer to appropriate version. Initially points to dispatcher
strcmpDispatch DD strcmpCPUDispatch

%IFDEF POSITIONINDEPENDENT
; Fix potential problem in Mac linker
        DD      0, 0
%ENDIF


; Append 16 bytes to end of last data section to allow reading past end of strings:
; (We might use names .bss$zzz etc. under Windows to make it is placed
; last, but the assembler gives sections with unknown names wrong attributes.
; Here, we are just relying on library data being placed after main data.
; This can be verified by making a link map file)
SECTION .bss
;        dq      0, 0
        resq  4
