;*************************  substring32.asm  **********************************
; Author:           Agner Fog
; Date created:     2011-07-18
; Last modified:    2011-07-18
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Description:
; Makes a substring of a zero-terminated ASCII string
;
; C++ prototype:
; extern "C"
; size_t A_substring(char * dest, const char * source, size_t pos, size_t len);
; Makes a substring from source, starting at position pos (zero-based) and length
; len and stores it in the array dest. It is the responsibility of the programmer
; that the size of the dest array is at least len + 1.
; The return value is the actual length of the substring. This may be less than 
; len if the length of source is less than pos + len.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses/gpl.html
;******************************************************************************

global _A_substring                     ; Function _A_substring

extern _A_strlen
extern _A_memcpy

SECTION .text

; extern "C"                 4                   8            12          16
; size_t A_substring(char * dest, const char * source, size_t pos, size_t len);

_A_substring:
        mov     ecx, [esp+8]           ; source
        push    ecx
        call    _A_strlen              ; eax = strlen(source)
        pop     ecx
        mov     edx, [esp+12]          ; pos
        sub     eax, edx               ; max length = strlen(source) - pos
        jbe     empty                  ; strlen(source) <= pos. Return empty string
        mov     ecx, [esp+16]          ; len
        cmp     eax, ecx
        cmova   eax, ecx               ; min(len, maxlen)
        add     edx, [esp+8]           ; source + pos
        mov     ecx, [esp+4]           ; dest
        push    eax                    ; length for memcpy
        push    edx                    ; source for memcpy
        push    ecx                    ; dest for memcpy
        call    _A_memcpy
        pop     ecx
        pop     edx
        pop     eax                    ; return final length
        mov     byte [ecx+eax], 0      ; terminating zero        
        ret
        
empty:  ; return empty string
        mov     ecx, [esp+4]           ; dest
        xor     eax, eax               ; return 0
        mov     byte [ecx], al
        ret
        
;_A_substring END
