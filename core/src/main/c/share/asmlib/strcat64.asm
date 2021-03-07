;*************************  strcat64.asm  ************************************
; Author:           Agner Fog
; Date created:     2008-07-19
; Last modified:    2008-10-16
; Description:
; Faster version of the standard strcat function:
; char * strcat(char *dest, const char * src);
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

default rel

global A_strcat                  ; Function A_strcat
global ?OVR_strcat               ; ?OVR removed if standard function strcat overridden

; Imported from strlen64.asm
extern A_strlen

; Imported from memcpy64.asm
extern A_memcpy


SECTION .text  align=16

; extern "C" char * A_strcat(char * dest, const char * src) {
;    memcpy(dest+strlen(dest), src, strlen(src)+1);
;    return dest
; }

; Function entry:
A_strcat:
?OVR_strcat:

%IFDEF  WINDOWS
%define Rpar1   rcx                    ; function parameter 1
%define Rpar2   rdx                    ; function parameter 2
%define Rpar3   r8                     ; function parameter 3
%ENDIF
%IFDEF  UNIX
%define Rpar1   rdi                    ; function parameter 1
%define Rpar2   rsi                    ; function parameter 2
%define Rpar3   rdx                    ; function parameter 3
%ENDIF

        push    Rpar1                  ; dest
        push    Rpar2                  ; src
        call    A_strlen               ; length of dest
        push    rax                    ; strlen(dest)
        mov     Rpar1, [rsp+8]         ; src
        call    A_strlen               ; length of src
        pop     Rpar1                  ; strlen(dest)
        pop     Rpar2                  ; src
        add     Rpar1, [rsp]           ; dest + strlen(dest)
        lea     Rpar3, [rax+1]         ; strlen(src)+1
        call    A_memcpy               ; copy
        pop     rax                    ; return dest
        ret

;A_strcat ENDP
