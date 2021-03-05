;*************************  strcat32.asm  ************************************
; Author:           Agner Fog
; Date created:     2008-07-19
; Last modified:    2008-10-16
; Description:
; Faster version of the standard strcat function:
; char * strcat(char * dest, const char * src);
; Copies zero-terminated string from src to end of dest.
;
; Overriding standard function strcat:
; The alias ?OVR_strcat is changed to _strcat in the object file if
; it is desired to override the standard library function strcat.
;
; Optimization:
; Uses optimized functions A_strlen and A_memcpy.
;
; Copyright (c) 2009 GNU General Public License www.gnu.org/licenses
;******************************************************************************

global _A_strcat                  ; Function _A_strcat
global ?OVR_strcat                ; ?OVR removed if standard function strcat overridden

; Imported from strlen32.asm
extern _A_strlen

; Imported from memcpy32.asm
extern _A_memcpy


SECTION .text  align=16

; extern "C" char * A_strcat(char * dest, const char * src) {
;    memcpy(dest+strlen(dest), src, strlen(src)+1);
;    return dest
; }

; Function entry:
_A_strcat:
?OVR_strcat:

        mov     eax, [esp+8]           ; src
        push    eax
        call    _A_strlen              ; length of src
        inc     eax                    ; include terminating zero in length
        push    eax                    ; strlen(src)+1        
        mov     edx, [esp+4+8]         ; dest
        push    edx
        call    _A_strlen              ; length of dest
        pop     edx                    ; dest. Assume unchanged by _A_strlen
        add     edx, eax               ; dest+strlen(dest)
        mov     ecx, [esp+8+8]         ; src
                                       ; strlen(src)+1 is on stack
        push    ecx                    ; src
        push    edx                    ; dest+strlen(dest)
        call    _A_memcpy              ; copy
        add     esp, 16                ; clean up stack
        mov     eax, [esp+4]           ; return dest
        ret

;_A_strcat ENDP
