;                   procname32.asm
;
; Author:           Agner Fog
; Date created:     2007
; Last modified:    2013-09-11
; Description:
; ProcessorName
; =============
; This function produces a zero-terminated ASCII string containing a name
; for the microprocessor in human-readable format.
; 
; Copyright (c) 2007-2016 GNU General Public License www.gnu.org/licenses
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

global _ProcessorName               


SECTION .data
align 16

NameBuffer times 50H db 0              ; Static buffer to contain name


SECTION .text  align=16

%IFDEF POSITIONINDEPENDENT
; Local function for reading instruction pointer into edi
GetThunkEDI:
        mov     edi, [esp]
        ret
%ENDIF  ; End IF POSITIONINDEPENDENT


; ********** ProcessorName function **********
; C++ prototype:
; extern "C" char * ProcessorName ();

; This function finds the name of the microprocessor. The name is returned
; in the parameter text, which must be a character array of at least 68 bytes.

_ProcessorName:
        push    ebx
        push    edi
        
; Make edi point to NameBuffer:
        
%IFDEF POSITIONINDEPENDENT
        ; Position-independent code. Get edi = eip for reference point
        call    GetThunkEDI
        add     edi, NameBuffer - $
%ELSE
        ; Normal code requiring base relocation:
        mov     edi, NameBuffer        
%ENDIF
        
; detect if CPUID instruction supported by microprocessor:
        pushfd
        pop     eax
        xor     eax, 1 << 21           ; Check if CPUID bit can toggle
        push    eax
        popfd
        pushfd
        pop     ebx
        xor     eax, ebx
        and     eax, 1 << 21
        jnz     NOID                   ; CPUID not supported
        xor     eax, eax
        cpuid                          ; Get number of CPUID functions
        test    eax, eax
        jnz     IDENTIFYABLE           ; Function 1 supported
        
NOID:
        ; processor has no CPUID
        mov     DWORD [edi], '8038'    ; Write text '80386 or 80486'
        mov     DWORD [edi+4], '6 or'
        mov     DWORD [edi+8], ' 804'
        mov     DWORD [edi+12], '86'   ; End with 0
        jmp     PNEND
        
IDENTIFYABLE:
        mov     eax, 80000000H
        cpuid
        cmp     eax, 80000004H         ; Text if extended vendor string available
        jb      no_ext_vendor_string

        ; Has extended vendor string
        mov     eax, 80000002H
        cpuid
        mov     [edi], eax             ; Store 16 bytes of extended vendor string
        mov     [edi+4], ebx
        mov     [edi+8], ecx
        mov     [edi+0CH], edx
        mov     eax, 80000003H
        cpuid
        mov     [edi+10H], eax         ; Next 16 bytes
        mov     [edi+14H], ebx
        mov     [edi+18H], ecx
        mov     [edi+1CH], edx
        mov     eax, 80000004H
        cpuid
        mov     [edi+20H], eax         ; Next 16 bytes
        mov     [edi+24H], ebx
        mov     [edi+28H], ecx
        mov     [edi+2CH], edx
        jmp     get_family_and_model
        
no_ext_vendor_string:
        ; No extended vendor string. Get short vendor string
        xor     eax, eax
        cpuid
        mov     [edi],ebx              ; Store short vendor string
        mov     [edi+4],edx
        mov     [edi+8],ecx
        mov     byte [edi+12],0        ; Terminate string
        
get_family_and_model:
        push    edi                    ; Save string address
        xor     eax, eax
        mov     ecx, 30H
        cld
        repne   scasb                  ; Find end of text
        dec     edi
        mov     dword [edi], ' Fam'    ; Append text " Family "
        mov     dword [edi+4], 'ily '
        add     edi, 8

        mov     eax, 1
        cpuid                          ; Get family and model
        mov     ebx, eax
        mov     ecx, eax
        shr     eax, 8
        and     eax, 0FH               ; Family
        shr     ecx, 20
        and     ecx, 0FFH              ; Extended family
        add     eax, ecx               ; Family + extended family
        call    WriteHex               ; Write as hexadecimal

        mov     dword [edi], 'H Mo'    ; Write text "H Model "
        mov     dword [edi+4], 'del '
        add     edi, 8
        
        mov     eax, ebx
        shr     eax, 4
        and     eax, 0FH               ; Model
        mov     ecx, ebx
        shr     ecx, 12
        and     ecx, 0F0H              ; Extended model
        or      eax, ecx               ; Model | extended model
        call    WriteHex               ; Write as hexadecimal

        mov     dword [edi], 'H'       ; Write text "H"
        pop     edi                    ; Restore string address
        
PNEND:  ; finished
        mov     eax, edi               ; Pointer to result
        pop     edi
        pop     ebx
        ret
;_ProcessorName ENDP

WriteHex:                              ; Local function: Write 2 hexadecimal digits
        ; Parameters: AL = number to write, EDI = text destination
        mov     ecx, eax
        shr     ecx, 4
        and     ecx, 0FH               ; most significant digit first
        cmp     ecx, 10
        jnb     W1
        ; 0 - 9
        add     ecx, '0'
        jmp     W2
W1:     ; A - F
        add     ecx, 'A' - 10
W2:     mov     [edi], cl              ; write digit
                
        mov     ecx, eax
        and     ecx, 0FH               ; next digit
        cmp     ecx, 10
        jnb     W3
        ; 0 - 9
        add     ecx, '0'
        jmp     W4
W3:     ; A - F
        add     ecx, 'A' - 10
W4:     mov     [edi+1], cl            ; write digit
        add     edi, 2                 ; advance string pointer
        ret
