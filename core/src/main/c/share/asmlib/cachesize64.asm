;*************************  cachesize64.asm  *************************************
; Author:           Agner Fog
; Date created:     2011-07-11
; Last modified:    2013-08-14
; Description:
; Determines the size of the data caches 
;
; extern "C" site_t DataCacheSize(int level);
; Input: 
; level: n = 1 - 4: level n data cache
;        0 = largest level data cache
; Return value: size in bytes of data cache
;
; The latest version of this file is available at:
; www.agner.org/optimize/asmexamples.zip
; Copyright (c) 2011-2013 GNU General Public License www.gnu.org/licenses
;******************************************************************************
%include "piccall.asi"

default rel

global DataCacheSize

; Imported from cputype64.asm
extern CpuType                         ; near. Determine CPU vendor

struc   data_layout
ok:     resd    2
level1: resq    1
level2: resq    1
level3: resq    1
level4: resq    1
descriptortable: resd 60
endstruc

struc   descriptor_record              ; record for table of cache descriptors
d_key:          resb 1                 ; key from cpuid instruction
d_level:        resb 1                 ; cache level
d_sizem:        resb 1                 ; size multiplier
d_2pow:         resb 1                 ; power of 2. size = d_sizem << d_2pow
endstruc

SECTION .data

dataref:                               ; reference point
ok_:       DD      0, 0                ; 1 when values are determined
level1_:   DQ      0                   ; level 1 data cache size
level2_:   DQ      0                   ; level 2 data cache size
level3_:   DQ      0                   ; level 3 data cache size
level4_:   DQ      0                   ; level 4 data cache size
numlevels  equ     4                   ; max level

; From "Intel Processor Identification and the CPUID Instruction, Application note 485
descriptortable_:                      ; table of Intel cache descriptors
db 0Ah, 1, 1, 13                       ; 8 kb L1 data cache
db 0Ch, 1, 1, 14                       ; 16 kb L1 data cache
db 0Dh, 1, 1, 14                       ; 16 kb L1 data cache
db 21h, 2, 1, 18                       ; 256 kb L2 data cache
db 22h, 3, 1, 19                       ; 512 kb L3 data cache
db 23h, 3, 1, 20                       ; 1 Mb L3 data cache
db 25h, 3, 1, 21                       ; 2 Mb L3 data cache
db 29h, 3, 1, 22                       ; 4 Mb L3 data cache
db 2Ch, 1, 1, 15                       ; 32 kb L1 data cache
db 39h, 2, 1, 17                       ; 128 kb L2 data cache
db 3Ah, 2, 3, 16                       ; 192 kb L2 data cache
db 3Bh, 2, 1, 17                       ; 128 kb L1 data cache
db 3Ch, 2, 1, 18                       ; 256 kb L1 data cache
db 3Dh, 2, 3, 17                       ; 384 kb L2 data cache
db 3Eh, 2, 1, 19                       ; 512 kb L2 data cache
db 41h, 2, 1, 17                       ; 128 kb L2 data cache
db 42h, 2, 1, 18                       ; 256 kb L2 data cache
db 43h, 2, 1, 19                       ; 512 kb L2 data cache
db 44h, 2, 1, 20                       ; 1 Mb L2 data cache
db 45h, 2, 1, 21                       ; 2 Mb L2 data cache
db 46h, 3, 1, 22                       ; 4 Mb L3 data cache
db 47h, 3, 1, 23                       ; 8 Mb L3 data cache
db 48h, 2, 3, 20                       ; 3 Mb L2 data cache
db 49h, 2, 1, 22                       ; 4 Mb L2 or 3 data cache
db 4Ah, 3, 3, 21                       ; 6 Mb L3 data cache
db 4Bh, 3, 1, 23                       ; 8 Mb L3 data cache
db 4Ch, 3, 3, 22                       ; 12 Mb L3 data cache
db 4Dh, 3, 1, 24                       ; 16 Mb L3 data cache
db 4Eh, 2, 3, 21                       ; 6 Mb L2 data cache
db 60h, 1, 1, 14                       ; 16 kb L1 data cache
db 66h, 1, 1, 13                       ; 8 kb L1 data cache
db 67h, 1, 1, 14                       ; 16 kb L1 data cache
db 68h, 1, 1, 15                       ; 32 kb L1 data cache
db 78h, 2, 1, 20                       ; 1 Mb L2 data cache
db 79h, 2, 1, 17                       ; 128 kb L2 data cache
db 7Ah, 2, 1, 18                       ; 256 kb L2 data cache
db 7Bh, 2, 1, 19                       ; 512 kb L2 data cache
db 7Ch, 2, 1, 20                       ; 1 Mb L2 data cache
db 7Dh, 2, 1, 21                       ; 2 Mb L2 data cache
db 7Fh, 2, 1, 19                       ; 512 kb L2 data cache
db 82h, 2, 1, 18                       ; 256 kb L2 data cache
db 83h, 2, 1, 19                       ; 512 kb L2 data cache
db 84h, 2, 1, 20                       ; 1 Mb L2 data cache
db 85h, 2, 1, 21                       ; 2 Mb L2 data cache
db 86h, 2, 1, 19                       ; 512 kb L2 data cache
db 87h, 2, 1, 20                       ; 1 Mb L2 data cache
db 0D0h, 3, 1, 19                      ; 512 kb L3 data cache
db 0D1h, 3, 1, 20                      ; 1 Mb L3 data cache
db 0D2h, 3, 1, 21                      ; 2 Mb L3 data cache
db 0D6h, 3, 1, 20                      ; 1 Mb L3 data cache
db 0D7h, 3, 1, 21                      ; 2 Mb L3 data cache
db 0D8h, 3, 1, 22                      ; 4 Mb L3 data cache
db 0DCh, 3, 3, 19                      ; 1.5 Mb L3 data cache
db 0DDh, 3, 3, 20                      ; 3 Mb L3 data cache
db 0DEh, 3, 3, 21                      ; 6 Mb L3 data cache
db 0E2h, 3, 1, 21                      ; 2 Mb L3 data cache
db 0E3h, 3, 1, 22                      ; 4 Mb L3 data cache
db 0E4h, 3, 1, 23                      ; 8 Mb L3 data cache
db 0EAh, 3, 3, 22                      ; 12 Mb L3 data cache
db 0EBh, 3, 9, 21                      ; 18 Mb L3 data cache
db 0ECh, 3, 3, 23                      ; 24 Mb L3 data cache
descriptortablelength equ ($ - descriptortable_) / descriptor_record_size


SECTION .text

; extern "C" site_t DataCacheSize(int level);

; Function entry:
DataCacheSize:
        push    rbx
        push    r14
%ifdef  WINDOWS
        push    rsi
        push    rdi
        mov     r14d, ecx              ; level
%else   ; UNIX
        mov     r14d, edi              ; level
%endif
        ; check if called before
        lea     r9, [dataref]
        cmp     dword [r9+ok], 1       ; ok
        je      D800
        
        ; find cpu vendor
        push    0
%ifdef  WINDOWS
        mov     rcx, rsp
        xor     edx, edx
        xor     r8d, r8d
%else   ; UNIX
        mov     rdi, rsp
        xor     esi, esi
        xor     edx, edx
%endif        
        callW   CpuType
        lea     r9, [dataref]
        pop     rax                    ; eax = vendor
        dec     eax
        jz      Intel
        dec     eax
        jz      AMD
        dec     eax
        jz      VIA
        ; unknown vendor, try all methods
        call    IntelNewMethod
        jnc     D800                   ; not carry = success
        call    AMDMethod
        jnc     D800                   ; not carry = success
        call    IntelOldMethod
        jmp     D800                   ; return whether success or not
        
Intel:  call    IntelNewMethod
        jnc     D800                   ; not carry = success
        call    IntelOldMethod
        jmp     D800                   ; return whether success or not

AMD:    ; AMD and VIA use same method
VIA:    call    AMDMethod
        
D800:   ; cache data known, get desired return value
        xor     eax, eax
        cmp     r14d, numlevels
        ja      D900
        cmp     r14d, 0
        je      D820
        ; level = 1 .. numlevels
        mov     rax, [r9 + r14*8]      ; size of selected cache
        jmp     D850
D820:   ; level = 0. Get size of largest level cache
        mov     rax, [r9 + level3]     ; level3
        test    rax, rax
        jnz     D850
        mov     rax, [r9 + level2]     ; level2
        test    rax, rax
        jnz     D850
        mov     eax, [r9 + level1]     ; level1
D850:   mov     dword [r9 + ok], 1     ; remember called, whether success or not
D900:   
%ifdef  WINDOWS
        pop     rdi
        pop     rsi
%endif
        pop     r14
        pop     rbx
        ret


; Determine cache sizes by CPUID function 4
; input: esi = pointer to dataref
; output: values returned in dataref + level1, level2, level3
; carry flag = 0 on succes
IntelNewMethod:
        xor     eax, eax
        cpuid                          ; get number of CPUID functions
        cmp     eax, 4
        jb      I900                   ; fail
        xor     esi, esi               ; loop counter
I100:   mov     eax, 4
        mov     ecx, esi
        cpuid                          ; get cache parameters
        mov     edx, eax
        and     edx, 11111b            ; cache type
        jz      I500                   ; no more caches
        cmp     edx, 2
        je      I200                   ; code cache, ignore
        inc     ecx                    ; sets
        mov     edx, ebx
        shr     edx, 22
        inc     edx                    ; ways
        imul    ecx, edx
        mov     edx, ebx
        shr     edx, 12
        and     edx, 1111111111b
        inc     edx                    ; partitions
        imul    ecx, edx
        and     ebx, 111111111111b        
        inc     ebx                    ; line size
        imul    rcx, rbx               ; calculated cache size (64 bit)
        shr     eax, 5
        and     eax, 111b              ; cache level
        cmp     eax, numlevels
        jna     I180
        mov     eax, numlevels         ; limit higher levels
I180:   mov     [r9+rax*8], rcx        ; store size of data cache level eax
I200:   inc     esi
        cmp     esi, 100h              ; avoid infinite loop
        jb      I100                   ; next cache
I500:   ; loop finished
        ; check if OK
        mov     eax, [r9+level1]       ; level1
        cmp     eax, 1024
I900:   ret                            ; carry flag set if fail

; Determine cache sizes by CPUID function 2
; input: esi = pointer to dataref
; output: values returned in dataref + level1, level2, level3
; carry flag = 0 on succes
IntelOldMethod:
        xor     eax, eax
        cpuid                          ; get number of CPUID functions
        cmp     eax, 2
        jb      J900                   ; fail
        mov     eax, 2
        xor     ecx, ecx
        cpuid                          ; get 16 descriptor bytes in eax, ebx, ecx, edx
        mov     al, 0                  ; al does not contain a descriptor
        sub     rsp, 16
        mov     [rsp],    eax          ; save all descriptors
        mov     [rsp+4],  ebx
        mov     [rsp+8],  ecx
        mov     [rsp+12], edx
        mov     edx, 15                ; loop counter
        ; loop to read 16 descriptor bytes
J100:   mov     al, byte [rsp+rdx]
        ; find in table
        mov     ebx, descriptortablelength-1  ; loop counter
        ; loop to search in descriptortable
J200:   cmp     al, [r9 + descriptortable + rbx*4 + d_key]
        jne     J300
        ; descriptor found
        movzx   eax, byte [r9 + descriptortable + rbx*4 + d_sizem]
        mov     cl,  [r9 + descriptortable + rbx*4 + d_2pow]
        shl     eax, cl                ; compute size
        movzx   ecx, byte [r9 + descriptortable + rbx*4 + d_level]
        ; check that level = 1-3
        cmp     ecx, 3
        ja      J300
        mov     [r9+rcx*8], rax        ; store size eax of data cache level ecx
J300:   dec     ebx
        jns     J200                   ; inner loop
        dec     edx
        jns     J100                   ; outer loop
        add     rsp, 16                ; remove from stack
        ; check if OK
        mov     eax, [r9 + level1]
        cmp     eax, 1024
J900:   ret                            ; carry flag set if fail


; Determine cache sizes by CPUID function 80000005H - 80000006H
; input: esi = pointer to dataref
; output: values returned in dataref
; carry flag = 0 on succes
AMDMethod:
        mov     eax, 80000000H
        cpuid                          ; get number of CPUID functions
        cmp     eax, 6
        jb      K900                   ; fail
        mov     eax, 80000005H
        cpuid                          ; get L1 cache size
        shr     ecx, 24                ; L1 data cache size in kbytes
        shl     ecx, 10                ; L1 data cache size in bytes
        mov     [r9 + level1], ecx     ; store L1 data cache size
        mov     eax, 80000006H
        cpuid                          ; get L2 and L3 cache sizes
        shr     ecx, 16                ; L2 data cache size in kbytes
        shl     ecx, 10                ; L2 data cache size in bytes
        mov     [r9 + level2], ecx     ; store L2 data cache size
        mov     ecx, edx
        shr     ecx, 18                ; L3 data cache size / 512 kbytes
        shl     rcx, 19                ; L3 data cache size in bytes
%if 0   ; AMD manual is unclear: 
        ; do we have to increase the value if the number of ways is not a power or 2?
        shr     edx, 12
        and     edx, 1111b             ; L3 associativity
        cmp     edx, 3
        jb      K100
        test    edx, 1
        jz      K100
        ; number of ways is not a power of 2, multiply by 1.5 ?
        mov     rax, rcx
        shr     rax, 1
        add     rcx, rax
%endif
K100:   mov     [r9 + level3], rcx     ; store L3 data cache size
        ; check if OK
        mov     eax, [r9 + level1]
        cmp     eax, 1024
K900:   ret                            ; carry flag set if fail
