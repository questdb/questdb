;*************************  cachesize32.asm  *************************************
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

global _DataCacheSize

; Imported from cputype32.asm
extern _CpuType                 ; near. Determine CPU vendor

; data are referenced as [esi+structuremember] rather than [esi+label-dataref] because
; of a bug in yasm v. 1.1.0.2352:

struc   data_layout
ok:     resd    1
level1: resd    1
level2: resd    1
level3: resd    1
level4: resd    1
descriptortable: resd 60
endstruc

struc   descriptor_record              ; record for table of cache descriptors
d_key:          resb 1                 ; key from cpuid instruction
d_level:        resb 1                 ; cache level
d_sizem:        resb 1                 ; size multiplier
d_2pow:         resb 1                 ; power of 2. size = d_sizem << d_2pow
endstruc

SECTION .data

dataref:                                ; reference point
ok_:     DD      0                      ; 1 when values are determined
level1_: DD      0                      ; level 1 data cache size
level2_: DD      0                      ; level 2 data cache size
level3_: DD      0                      ; level 3 data cache size
level4_: DD      0                      ; level 4 data cache size
numlevels   equ  4                      ; max level

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

; extern "C" site_t _DataCacheSize(int level);

; Function entry:
_DataCacheSize:
        push    ebx
        push    esi
        push    edi
        push    ebp
        mov     edi, [esp+20]       ; level
%IFDEF  POSITIONINDEPENDENT
        call    get_thunk_esi
        add     esi, dataref - $  ; point to dataref
%ELSE
        mov     esi, dataref      ; point to dataref
%ENDIF
        ; check if called before
        cmp     dword [esi + ok], 1
        je      D800
        
        ; find cpu vendor
        push    0
        mov     eax, esp
        push    0
        push    0
        push    eax
        call    _CpuType
        add     esp, 12
        pop     eax                    ; eax = vendor
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
        cmp     edi, numlevels
        ja      D900
        cmp     edi, 0
        je      D820
        ; level = 1 .. numlevels
        mov     eax, [esi + edi*4]     ; size of selected cache
        jmp     D850
D820:   ; level = 0. Get size of largest level cache
        mov     eax, [esi + level3]
        test    eax, eax
        jnz     D850
        mov     eax, [esi + level2]
        test    eax, eax
        jnz     D850
        mov     eax, [esi + level1]
D850:   mov     dword [esi + ok], 1  ; remember called, whether success or not
D900:   pop     ebp
        pop     edi
        pop     esi
        pop     ebx
        ret


%IFDEF  POSITIONINDEPENDENT
get_thunk_esi:
        mov     esi, [esp]
        ret
%ENDIF


; Determine cache sizes by CPUID function 4
; input: esi = pointer to dataref
; output: values returned in dataref + level1, level2, level3
; carry flag = 0 on succes
IntelNewMethod:
        xor     eax, eax
        cpuid                          ; get number of CPUID functions
        cmp     eax, 4
        jb      I900                   ; fail
        xor     ebp, ebp               ; loop counter
I100:   mov     eax, 4
        mov     ecx, ebp
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
        imul    ecx, ebx               ; calculated cache size
        shr     eax, 5
        and     eax, 111b              ; cache level
        cmp     eax, numlevels
        jna     I180
        mov     eax, numlevels         ; limit higher levels
I180:   mov     [esi+eax*4], ecx       ; store size of data cache level eax
I200:   inc     ebp
        cmp     ebp, 100h              ; avoid infinite loop
        jb      I100                   ; next cache
I500:   ; loop finished
        ; check if OK
        mov     eax, [esi+level1]
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
        push    eax                    ; save all descriptors
        push    ebx
        push    ecx
        push    edx                    ; now esp points to descriptors
        mov     edx, 15                ; loop counter
        ; loop to read 16 descriptor bytes
J100:   mov     al, byte [esp+edx]
        ; find in table
        mov     ebx, descriptortablelength-1  ; loop counter
        ; loop to search in descriptortable
J200:   cmp     al, [esi + descriptortable + ebx*descriptor_record_size + d_key]
        jne     J300
        ; descriptor found
;       YASM v. 1.1.0 fails if there are too many of (label-dataref): !
;       movzx   eax, byte [esi + ebx*4 + (descriptortable_-dataref) + d_sizem]
        movzx   eax, byte [esi + ebx*4 + descriptortable + d_sizem]
        mov     cl,  [esi + ebx*4 + descriptortable + d_2pow]
        shl     eax, cl                ; compute size
        movzx   ecx, byte [esi + descriptortable + ebx*4 + d_level]
        ; check that level = 1-3
        cmp     ecx, 3
        ja      J300
        mov     [esi+ecx*4], eax       ; store size eax of data cache level ecx
J300:   dec     ebx
        jns     J200                   ; inner loop
        dec     edx
        jns     J100                   ; outer loop
        add     esp, 16                ; remove from stack
        ; check if OK
        mov     eax, [esi+level1]
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
        mov     [esi+level1], ecx      ; store L1 data cache size
        mov     eax, 80000006H
        cpuid                          ; get L2 and L3 cache sizes
        shr     ecx, 16                ; L2 data cache size in kbytes
        shl     ecx, 10                ; L2 data cache size in bytes
        mov     [esi+level2], ecx      ; store L2 data cache size
        mov     ecx, edx
        shr     ecx, 18                ; L3 data cache size / 512 kbytes
        shl     ecx, 19                ; L3 data cache size in bytes
%if 0   ; AMD manual is unclear: 
        ; do we have to increase the value if the number of ways is not a power or 2?
        shr     edx, 12
        and     edx, 1111b             ; L3 associativity
        cmp     edx, 3
        jb      K100
        test    edx, 1
        jz      K100
        ; number of ways is not a power of 2, multiply by 1.5 ?
        mov     eax, ecx
        shr     eax, 1
        add     ecx, eax
%endif
K100:   mov     [esi+level3], ecx      ; store L3 data cache size
        ; check if OK
        mov     eax, [esi+level1]
        cmp     eax, 1024
K900:   ret                            ; carry flag set if fail
