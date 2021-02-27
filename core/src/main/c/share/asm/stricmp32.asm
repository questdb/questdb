;*************************  stricmpaz32.asm  **********************************
; Author:           Agner Fog
; Date created:     2008-12-05
; Last modified:    2011-07-01
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Description:
; Faster version of the standard stricmp or strcasecmp function:
; int stricmp_az(const char *string1, const char *string2);
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

global _A_stricmp                     ; Function _A_stricmp

SECTION .text  align=16

; extern "C" int stricmp_az(const char *string1, const char *string2);

_A_stricmp:
        mov     ecx, [esp+4]           ; string1
        mov     edx, [esp+8]           ; string2
        sub     edx, ecx
        
L10:    mov     al,  [ecx]
        cmp     al,  [ecx+edx]
        jne     L20
        inc     ecx
        test    al, al
        jnz     L10                    ; continue with next byte
        
        ; terminating zero found. Strings are equal
        xor     eax, eax
        ret        
        
L20:    ; bytes are different. check case
        xor     al, 20H                ; toggle case
        cmp     al, [ecx+edx]
        jne     L30
        ; possibly differing only by case. Check if a-z
        or      al, 20H                ; upper case
        sub     al, 'a'
        cmp     al, 'z'-'a'
        ja      L30                    ; not a-z
        ; a-z and differing only by case
        inc     ecx
        jmp     L10                    ; continue with next byte

L30:    ; bytes are different, even after changing case
        movzx   eax, byte [ecx]        ; get original value again
        sub     eax, 'A'
        cmp     eax, 'Z' - 'A'
        ja      L40
        add     eax, 20H
L40:    movzx   edx, byte [ecx+edx]
        sub     edx, 'A'
        cmp     edx, 'Z' - 'A'
        ja      L50
        add     edx, 20H
L50:    sub     eax, edx                 ; subtract to get result
        ret

;_A_stricmp END
