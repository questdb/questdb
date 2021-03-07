; ----------------------------- LIBAD64.ASM ---------------------------
; DLL entry function for LIBAD64.DLL

default rel

global DllEntry

SECTION .text  align=16

DllEntry:
        mov     eax, 1
        ret
;DllMain endp
