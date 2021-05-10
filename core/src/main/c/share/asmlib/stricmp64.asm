;*************************  stricmpaz64.asm  **********************************
; Author:           Agner Fog
; Date created:     2008-12-05
; Last modified:    2011-07-01
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Description:
; Faster version of the standard stricmp or strcasecmp function:
; int A_stricmp(const char *string1, const char *string2);
; Compares two zero-terminated strings without case sensitivity.
; Does not recognize locale-specific characters. A-Z are changed
; to a-z before comparing, while other upper-case letters are not
; converted but considered unique.
;
; Optimization:
; SSE4.2 version not implemented because the gain is small.
;
; Copyright (c) 2008-2011 GNU General Public License www.gnu.org/licenses/gpl.html
;******************************************************************************

default rel

global A_stricmp                     ; Function A_stricmp

; ***************************************************************************
; Define registers used for function parameters, used in 64-bit mode only
; ***************************************************************************
 
%IFDEF WINDOWS
  %define par1   rcx                   ; first parameter
  %define par2   rdx                   ; second parameter
%ENDIF
  
%IFDEF UNIX
  %define par1   rdi                   ; first parameter
  %define par2   rsi                   ; second parameter
%ENDIF

SECTION .text  align=16

; extern "C" int A_stricmp(const char *string1, const char *string2);

A_stricmp:
        sub     par2, par1
        
L10:    mov     al,  [par1]            ; string1
        cmp     al,  [par1+par2]       ; string2
        jne     L20
        inc     par1
        test    al, al
        jnz     L10                    ; continue with next byte
        
        ; terminating zero found. Strings are equal
        xor     eax, eax
        ret        
        
L20:    ; bytes are different. check case
        xor     al, 20H                ; toggle case
        cmp     al, [par1+par2]
        jne     L30
        ; possibly differing only by case. Check if a-z
        or      al, 20H                ; upper case
        sub     al, 'a'
        cmp     al, 'z'-'a'
        ja      L30                    ; not a-z
        ; a-z and differing only by case
        inc     par1
        jmp     L10                    ; continue with next byte

L30:    ; bytes are different, even after changing case
        movzx   eax, byte [par1]       ; get original value again
        sub     eax, 'A'
        cmp     eax, 'Z' - 'A'
        ja      L40
        add     eax, 20H               ; A-Z, make lower case
L40:    movzx   edx, byte [par1+par2]
        sub     edx, 'A'
        cmp     edx, 'Z' - 'A'
        ja      L50
        add     edx, 20H                ; A-Z, make lower case
L50:    sub     eax, edx                ; subtract to get result
        ret

;A_stricmp END
