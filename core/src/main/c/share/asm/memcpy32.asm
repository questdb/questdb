;*************************  memcpy32.asm  ************************************
; Author:           Agner Fog
; Date created:     2008-07-18
; Last modified:    2016-11-12

; Description:
; Faster version of the standard memcpy function:
; void * A_memcpy(void *dest, const void *src, size_t count);
; Copies 'count' bytes from 'src' to 'dest'
;
; Overriding standard function memcpy:
; The alias ?OVR_memcpy is changed to _memcpy in the object file if
; it is desired to override the standard library function memcpy.
;
; The function uses non-temporal writes to bypass the cache when the size is 
; bigger than half the size of the largest_level cache. This limit can be
; read with _GetMemcpyCacheLimit and changed with _SetMemcpyCacheLimit (in 
; memmove32.asm). C++ prototypes:
; extern "C" size_t GetMemcpyCacheLimit();  // in memcpy32.asm
; extern "C" void SetMemcpyCacheLimit();    // in memmove32.asm
; extern "C" void SetMemcpyCacheLimit1();   // used internally
;
; Position-independent code is generated if POSITIONINDEPENDENT is defined.
;
; CPU dispatching included for the following instruction sets:
; 386, SSE2, Suppl-SSE3, AVX, AVX512F, AVX512BW.
;
; Copyright (c) 2008-2016 GNU General Public License www.gnu.org/licenses
;******************************************************************************

global _A_memcpy                       ; Function A_memcpy
global ?OVR_memcpy                     ; ?OVR removed if standard function memcpy overridden

; Direct entries to CPU-specific versions
global _memcpy386                      ; Generic version for processors without SSE2
global _memcpySSE2                     ; Version for processors with SSE2
global _memcpySSSE3                    ; Version for processors with SSSE3
global _memcpyU                        ; Alternative version for processors with fast unaligned read
global _memcpyU256                     ; Version for processors with fast 256-bit read/write
global _memcpyAVX512F                  ; Version for processors with fast 512-bit read/write
global _memcpyAVX512BW                 ; Version for processors with fast 512-bit read/write

global _GetMemcpyCacheLimit            ; Get the size limit for bypassing cache when copying with memcpy and memmove
global _SetMemcpyCacheLimit1           ; Set the size limit for bypassing cache when copying with memcpy
global getDispatch


; Imported from instrset32.asm:
extern _InstructionSet                 ; Instruction set for CPU dispatcher

; Imported from unalignedisfaster32.asm:
extern _UnalignedIsFaster              ; Tells if unaligned read is faster than PALIGNR
extern _Store256BitIsFaster            ; Tells if a 256 bit store is faster than two 128 bit stores

; Imported from cachesize32.asm:
extern _DataCacheSize                  ; Gets size of data cache


; Define prolog for this function
%MACRO  PROLOGM  0
        push    esi
        push    edi
        mov     edi, [esp+12]          ; dest
        mov     esi, [esp+16]          ; src
        mov     ecx, [esp+20]          ; count
%IFDEF  POSITIONINDEPENDENT
        push    ebx
        mov     ebx, edx               ; pointer to reference point RP
%ENDIF
%ENDM


; Define return from this function
%MACRO  EPILOGM 0
%IFDEF  POSITIONINDEPENDENT
        pop     ebx
%ENDIF
        pop     edi
        pop     esi
        mov     eax, [esp+4]           ; Return value = dest
        ret
%ENDMACRO


SECTION .text  align=16


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;                          Common entry for dispatch
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; extern "C" void * A_memcpy(void * dest, const void * src, size_t count);
; Function entry:
_A_memcpy:
?OVR_memcpy:

%IFNDEF POSITIONINDEPENDENT
        jmp     dword [memcpyDispatch] ; Go to appropriate version, depending on instruction set
RP      equ     0                      ; RP = 0 if not position-independent

%ELSE   ; Position-independent code

        call    get_thunk_edx          ; get reference point for position-independent code
RP:                                    ; reference point edx = offset RP

; Make the following instruction with address relative to RP:
        jmp     dword [edx+memcpyDispatch-RP]

%ENDIF


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; AVX512BW Version for processors with fast unaligned read and fast 512 bits write
; No position-independent version
; Requires AVX512BW, BMI2
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; memcpyAVX512BW:
align 16
; Version for size <= 40H. Requires AVX512BW and BMI2
L000:   mov     eax, -1                ; if count = 1-31: |  if count = 32-63:
        bzhi    eax, eax, ecx          ; -----------------|-------------------
        kmovd   k1, eax                ;       count 1's  |  all 1's
        xor     eax, eax               ;                  |
        sub     ecx, 32                ;                  |
        cmovb   ecx, eax               ;               0  |  count-32
        dec     eax                    ;                  |
        bzhi    eax, eax, ecx          ;                  |
        kmovd   k2, eax                ;               0  |  count-32 1's
        kunpckdq k3, k2, k1            ; low 32 bits from k1, high 32 bits from k2. total = count 1's
        vmovdqu8 zmm0{k3}{z}, [esi]    ; move count bytes
        vmovdqu8 [edi]{k3}, zmm0
        vzeroupper
        EPILOGM

align 8
; Version for size = 40H - 80H
L010:   ; make two partially overlapping blocks
        vmovdqu64 zmm0, [esi]
        vmovdqu64 zmm1, [esi+ecx-40H]
        vmovdqu64 [edi], zmm0
        vmovdqu64 [edi+ecx-40H], zmm1
        vzeroupper
        EPILOGM
        
; Function entry
; edi = dest
; esi = src
; ecx = count

align 16
times 9 nop                            ; align L200
_memcpyAVX512BW:                       ; global label
memcpyAVX512BW@:                       ; local label
        PROLOGM
        cmp     ecx, 040H
        jbe     L000
        cmp     ecx, 080H
        jbe     L010

L100:   ; count > 80H                  ; Entry from memcpyAVX512F
        vmovdqu64 zmm1, [esi]          ; save first possibly unaligned block to after main loop
        vmovdqu64 zmm2, [esi+ecx-40H]  ; save last  possibly unaligned block to after main loop

        mov    eax, edi                ; save destination
        add    edi, ecx                ; end of destination
        and    edi, -40H               ; round down to align by 40H
        mov    edx, edi
        sub    edx, eax
        add    esi, edx                ; end of main blocks of source
        and    edx, -40H               ; size of aligned blocks to copy

        ; Check if count very big
        cmp     edx, [_CacheBypassLimit]
        ja      L500                             ; Use non-temporal store if count > CacheBypassLimit

        neg    edx                     ; negative index from end of aligned blocks        
L200:   ; main loop. Move 40H bytes at a time
        vmovdqu64 zmm0, [esi+edx]
        vmovdqa64 [edi+edx], zmm0
        add     edx, 40H
        jnz     L200

L210:   ; insert remaining bytes at beginning and end, possibly overlapping main blocks
        vmovdqu64 [eax], zmm1
        vmovdqu64 [eax+ecx-40H], zmm2
        vzeroupper
        EPILOGM

align 16
L500:   ; Move 40H bytes at a time, non-temporal
        neg     edx
L510:   vmovdqu64 zmm0, [esi+edx]
        vmovntdq [edi+edx], zmm0
        add     edx, 40H
        jnz     L510
        sfence
        jmp     L210


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; AVX512F Version for processors with fast unaligned read and fast 512 bits write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Function entry
align 16
_memcpyAVX512F:   ; global label
memcpyAVX512F@:   ; local label
        PROLOGM

; rdi = dest
; rsi = src
; rcx = count
        cmp     ecx, 080H
        ja      L100
        cmp     ecx, 040H
        jae     L010
        ; count < 40H
        jmp     A1000


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; AVX Version for processors with fast unaligned read and fast 256 bits write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

align 16
_memcpyU256:   ; global label
%IFDEF POSITIONINDEPENDENT
        call    get_thunk_edx
        add     edx, RP-$
%ENDIF
memcpyU256@:
        PROLOGM
        cmp     ecx, 40H
        jb      A1000                  ; Use simpler code if count < 64

        ; count >= 64
        ; Calculate size of first block up to first regular boundary of dest
        mov     edx, edi
        neg     edx
        and     edx, 1FH
        jz      B3100                    ; Skip if dest aligned by 32
        
        ; edx = size of first partial block, 1 - 31 bytes
        test    dl, 3
        jz      B3030
        test    dl, 1
        jz      B3020
        ; move 1 byte
        movzx   eax, byte [esi]
        mov     [edi], al
        inc     esi
        inc     edi
B3020:  test    dl, 2
        jz      B3030
        ; move 2 bytes
        movzx   eax, word [esi]
        mov     [edi], ax
        add     esi, 2
        add     edi, 2
B3030:  test    dl, 4
        jz      B3040
        ; move 4 bytes
        mov     eax, [esi]
        mov     [edi], eax
        add     esi, 4
        add     edi, 4
B3040:  test    dl, 8
        jz      B3050
        ; move 8 bytes
        movq    xmm0, qword [esi]
        movq    qword [edi], xmm0
        add     esi, 8
        add     edi, 8
B3050:  test    dl, 16
        jz      B3060        
        ; move 16 bytes
        movups  xmm0, [esi]
        movaps  [edi], xmm0
        add     esi, 16
        add     edi, 16
B3060:  sub     ecx, edx

B3100:  ; Now dest is aligned by 32. Any partial block has been moved        
        
        ; Set up for loop moving 32 bytes per iteration:
        mov     edx, ecx               ; Save count
        and     ecx, -20H              ; Round down to nearest multiple of 32
        add     esi, ecx               ; Point to the end
        add     edi, ecx               ; Point to the end
        sub     edx, ecx               ; Remaining data after loop
        
        ; Check if count very big
%IFNDEF POSITIONINDEPENDENT
        ; Check if count very big
        cmp     ecx, [_CacheBypassLimit]
%ELSE
        cmp     ecx, [ebx+_CacheBypassLimit-RP]
%ENDIF
        ja      I3100                   ; Use non-temporal store if count > CacheBypassLimit
        neg     ecx                    ; Negative index from the end      

H3100:  ; copy -ecx bytes in blocks of 32 bytes.

        ; Check for false memory dependence: The CPU may falsely assume
        ; a partial overlap between the written destination and the following
        ; read source if source is unaligned and
        ; (src-dest) modulo 4096 is close to 4096
        test    esi, 1FH
        jz      H3110                  ; aligned
        mov     eax, esi
        sub     eax, edi
        and     eax, 0FFFH             ; modulo 4096
        cmp     eax, 1000H - 200H
        ja      J3100
        
H3110:  ; main copy loop, 32 bytes at a time
        ; ecx has negative index from the end, counting up to zero
        vmovups ymm0, [esi+ecx]
        vmovaps [edi+ecx], ymm0
        add     ecx, 20H
        jnz     H3110
        vzeroupper                      ; end of AVX mode        
        
        ; Move the remaining edx bytes (0 - 31):
H3120:  add     esi, edx
        add     edi, edx
        neg     edx
        jz      H3500                   ; Skip if no more data
        ; move 16-8-4-2-1 bytes, aligned
        cmp     edx, -10H
        jg      H3200
        ; move 16 bytes
        movups  xmm0, [esi+edx]
        movaps  [edi+edx], xmm0
        add     edx, 10H
H3200:  cmp     edx, -8
        jg      H3210        
        ; move 8 bytes
        movq    xmm0, qword [esi+edx]
        movq    qword [edi+edx], xmm0
        add     edx, 8 
        jz      H3500                   ; Early skip if count divisible by 8       
H3210:  cmp     edx, -4
        jg      H3220        
        ; move 4 bytes
        mov     eax, [esi+edx]
        mov     [edi+edx], eax
        add     edx, 4        
H3220:  cmp     edx, -2
        jg      H3230        
        ; move 2 bytes
        movzx   eax, word [esi+edx]
        mov     [edi+edx], ax
        add     edx, 2
H3230:  cmp     edx, -1
        jg      H3500        
        ; move 1 byte
        movzx   eax, byte [esi+edx]
        mov     [edi+edx], al
H3500:  ; finished     
        EPILOGM
        
I3100:  ; non-temporal move
        neg     ecx                    ; Negative index from the end
align 16
I3110:  ; main copy loop, 32 bytes at a time
        ; ecx has negative index from the end, counting up to zero
        vmovups ymm0, [esi+ecx]
        vmovntps [edi+ecx], ymm0
        add     ecx, 20H
        jnz     I3110
        sfence
        vzeroupper                     ; end of AVX mode
        jmp     H3120                  ; Move the remaining edx bytes (0 - 31):

align 16
J3100:  ; There is a false memory dependence.
        ; check if src and dest overlap, if not then it is safe 
        ; to copy backwards to avoid false memory dependence
%if 1
        ; Use this version if you want consistent behavior in the case
        ; where dest > src and overlap. However, this case is undefined
        ; anyway because part of src is overwritten before copying     
        push    edx
        mov     eax, esi
        sub     eax, edi
        cdq
        xor     eax, edx
        sub     eax, edx   ; abs(src-dest)
        neg     ecx        ; size
        pop     edx        ; restore edx
        cmp     eax, ecx
        jnb     J3110
        neg     ecx        ; restore ecx
        jmp     H3110       ; overlap between src and dest. Can't copy backwards
%else
        ; save time by not checking the case that is undefined anyway         
        mov     eax, esi
        sub     eax, edi
        neg     ecx        ; size
        cmp     eax, ecx
        jnb     J3110       ; OK to copy backwards
        ; must copy forwards
        neg     ecx        ; restore ecx
        jmp     H3110       ; copy forwards
%endif
        
J3110:  ; copy backwards, ecx = size. esi, edi = end of src, dest        
        push    esi
        push    edi
        sub     esi, ecx
        sub     edi, ecx
J3120:  ; loop backwards
        vmovups ymm1, [esi+ecx-20H]
        vmovaps [edi+ecx-20H], ymm1
        sub     ecx, 20H
        jnz     J3120
        vzeroupper
        pop     edi
        pop     esi
        jmp     H3120

        ; count < 64. Move 32-16-8-4-2-1 bytes
        ; multiple CPU versions (SSSE3 and later)
A1000:  add     esi, ecx               ; end of src
        add     edi, ecx               ; end of dest
        neg     ecx                    ; negative index from the end
        cmp     ecx, -20H
        jg      A1100        
        ; move 32 bytes
        ; movdqu is faster than movq on all processors with SSSE3
        movups  xmm0, oword [esi+ecx]
        movups  xmm1, oword [esi+ecx+10H]
        movups  oword [edi+ecx], xmm0
        movups  oword [edi+ecx+10H], xmm1
        add     ecx, 20H
A1100:  cmp     ecx, -10H        
        jg      A1200
        ; move 16 bytes
        movups  xmm0, oword [esi+ecx]
        movups  oword [edi+ecx], xmm0
        add     ecx, 10H
A1200:  cmp     ecx, -8        
        jg      A1300
        ; move 8 bytes
        movq    xmm0, qword [esi+ecx]
        movq    qword [edi+ecx], xmm0
        add     ecx, 8
A1300:  cmp     ecx, -4        
        jg      A1400
        ; move 4 bytes
        mov     eax, [esi+ecx]
        mov     [edi+ecx], eax
        add     ecx, 4
        jz      A1900                 ; early out if count divisible by 4
A1400:  cmp     ecx, -2        
        jg      A1500
        ; move 2 bytes
        movzx   eax, word [esi+ecx]
        mov     [edi+ecx], ax
        add     ecx, 2
A1500:  cmp     ecx, -1
        jg      A1900        
        ; move 1 byte
        movzx   eax, byte [esi+ecx]
        mov     [edi+ecx], al
A1900:  ; finished
        EPILOGM


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;  Version for processors with fast unaligned read and fast 16 bytes write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
                
align 16
_memcpyU:   ; global label
%IFDEF POSITIONINDEPENDENT
        call    get_thunk_edx
        add     edx, RP-$
%ENDIF
memcpyU@:   ; local label
        PROLOGM
        cmp     ecx, 40H
        jb      A1000                  ; Use simpler code if count < 64

        ; count >= 64
        ; Calculate size of first block up to first regular boundary of dest
        mov     edx, edi
        neg     edx
        and     edx, 0FH
        jz      B2100                    ; Skip if dest aligned by 16
        
        ; edx = size of first partial block, 1 - 15 bytes
        test    dl, 3
        jz      B2030
        test    dl, 1
        jz      B2020
        ; move 1 byte
        movzx   eax, byte [esi]
        mov     [edi], al
        inc     esi
        inc     edi
B2020:  test    dl, 2
        jz      B2030
        ; move 2 bytes
        movzx   eax, word [esi]
        mov     [edi], ax
        add     esi, 2
        add     edi, 2
B2030:  test    dl, 4
        jz      B2040
        ; move 4 bytes
        mov     eax, [esi]
        mov     [edi], eax
        add     esi, 4
        add     edi, 4
B2040:  test    dl, 8
        jz      B2050
        ; move 8 bytes
        movq    xmm0, qword [esi]
        movq    qword [edi], xmm0
        add     esi, 8
        add     edi, 8
B2050:  sub     ecx, edx
B2100:  ; Now dest is aligned by 16. Any partial block has been moved        
        
        ; Set up for loop moving 32 bytes per iteration:
        mov     edx, ecx               ; Save count
        and     ecx, -20H              ; Round down to nearest multiple of 32
        add     esi, ecx               ; Point to the end
        add     edi, ecx               ; Point to the end
        sub     edx, ecx               ; Remaining data after loop
        
        ; Check if count very big
%IFNDEF POSITIONINDEPENDENT
        ; Check if count very big
        cmp     ecx, [_CacheBypassLimit]
%ELSE
        cmp     ecx, [ebx+_CacheBypassLimit-RP]
%ENDIF
        ja      I100                   ; Use non-temporal store if count > CacheBypassLimit
        neg     ecx                    ; Negative index from the end      

H100:   ; copy -ecx bytes in blocks of 32 bytes.

        ; Check for false memory dependence: The CPU may falsely assume
        ; a partial overlap between the written destination and the following
        ; read source if source is unaligned and
        ; (src-dest) modulo 4096 is close to 4096
        test    esi, 0FH
        jz      H110                   ; aligned
        mov     eax, esi
        sub     eax, edi
        and     eax, 0FFFH             ; modulo 4096
        cmp     eax, 1000H - 200H
        ja      J100
        
H110:   ; main copy loop, 32 bytes at a time
        ; ecx has negative index from the end, counting up to zero
        movups  xmm0, [esi+ecx]
        movups  xmm1, [esi+ecx+10H]
        movaps  [edi+ecx], xmm0
        movaps  [edi+ecx+10H], xmm1
        add     ecx, 20H
        jnz     H110
        
        ; Move the remaining edx bytes (0 - 31):
H120:   add     esi, edx
        add     edi, edx
        neg     edx
        jz      H500                   ; Skip if no more data
        ; move 16-8-4-2-1 bytes, aligned
        cmp     edx, -10H
        jg      H200
        ; move 16 bytes
        movups  xmm0, [esi+edx]
        movaps  [edi+edx], xmm0
        add     edx, 10H
H200:   cmp     edx, -8
        jg      H210        
        ; move 8 bytes
        movq    xmm0, qword [esi+edx]
        movq    qword [edi+edx], xmm0
        add     edx, 8 
        jz      H500                   ; Early skip if count divisible by 8       
H210:   cmp     edx, -4
        jg      H220        
        ; move 4 bytes
        mov     eax, [esi+edx]
        mov     [edi+edx], eax
        add     edx, 4        
H220:   cmp     edx, -2
        jg      H230        
        ; move 2 bytes
        movzx   eax, word [esi+edx]
        mov     [edi+edx], ax
        add     edx, 2
H230:   cmp     edx, -1
        jg      H500        
        ; move 1 byte
        movzx   eax, byte [esi+edx]
        mov     [edi+edx], al
H500:   ; finished     
        EPILOGM
        
I100:   ; non-temporal move
        neg     ecx                    ; Negative index from the end
align 16
I110:   ; main copy loop, 32 bytes at a time
        ; ecx has negative index from the end, counting up to zero
        movups  xmm0, [esi+ecx]
        movups  xmm1, [esi+ecx+10H]
        movntps [edi+ecx], xmm0
        movntps [edi+ecx+10H], xmm1
        add     ecx, 20H
        jnz     I110
        sfence
        jmp     H120                  ; Move the remaining edx bytes (0 - 31):

align 16
J100:   ; There is a false memory dependence.
        ; check if src and dest overlap, if not then it is safe 
        ; to copy backwards to avoid false memory dependence
%if 1
        ; Use this version if you want consistent behavior in the case
        ; where dest > src and overlap. However, this case is undefined
        ; anyway because part of src is overwritten before copying     
        push    edx
        mov     eax, esi
        sub     eax, edi
        cdq
        xor     eax, edx
        sub     eax, edx   ; abs(src-dest)
        neg     ecx        ; size
        pop     edx        ; restore rdx
        cmp     eax, ecx
        jnb     J110
        neg     ecx        ; restore rcx
        jmp     H110       ; overlap between src and dest. Can't copy backwards
%else
        ; save time by not checking the case that is undefined anyway         
        mov     eax, esi
        sub     eax, edi
        neg     ecx        ; size
        cmp     eax, ecx
        jnb     J110       ; OK to copy backwards
        ; must copy forwards
        neg     ecx        ; restore ecx
        jmp     H110       ; copy forwards
%endif
        
J110:   ; copy backwards, ecx = size. esi, edi = end of src, dest        
        push    esi
        push    edi
        sub     esi, ecx
        sub     edi, ecx
J120:   ; loop backwards
        movups  xmm1, [esi+ecx-20H]
        movups  xmm0, [esi+ecx-10H]
        movaps  [edi+ecx-20H], xmm1
        movaps  [edi+ecx-10H], xmm0
        sub     ecx, 20H
        jnz     J120
        pop     edi
        pop     esi
        jmp     H120
        
        
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;  Version for processors with SSSE3. Aligned read + shift + aligned write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
        
align 16
_memcpySSSE3:   ; global label
%IFDEF POSITIONINDEPENDENT
        call    get_thunk_edx
        add     edx, RP-$
%ENDIF
memcpySSSE3@:   ; local label
        PROLOGM
        cmp     ecx, 40H
        jb      A1000                  ; Use simpler code if count < 64        
        
        ; count >= 64
        ; This part will not always work if count < 64
        ; Calculate size of first block up to first regular boundary of dest
        mov     edx, edi
        neg     edx
        and     edx, 0FH
        jz      B1200                    ; Skip if dest aligned by 16
        
        ; edx = size of first partial block, 1 - 15 bytes
        test    dl, 3
        jz      B1120
        test    edx, 1
        jz      B1110
        ; move 1 byte
        movzx   eax, byte [esi]
        mov     [edi], al
        inc     esi
        inc     edi
B1110:  test    dl, 2
        jz      B1120
        ; move 2 bytes
        movzx   eax, word [esi]
        mov     [edi], ax
        add     esi, 2
        add     edi, 2
B1120:  test    dl, 4
        jz      B1130
        ; move 4 bytes
        mov     eax, [esi]
        mov     [edi], eax
        add     esi, 4
        add     edi, 4
B1130:  test    dl, 8
        jz      B1140
        ; move 8 bytes
        movq    xmm0, qword [esi]
        movq    qword [edi], xmm0
        add     esi, 8
        add     edi, 8
B1140:  sub     ecx, edx

B1200:   ; Now dest is aligned by 16. Any partial block has been moved        
        ; Find alignment of src modulo 16 at this point:
        mov     eax, esi
        and     eax, 0FH
        
        ; Set up for loop moving 32 bytes per iteration:
        mov     edx, ecx               ; Save count
        and     ecx, -20H              ; Round down to nearest multiple of 32
        add     esi, ecx               ; Point to the end
        add     edi, ecx               ; Point to the end
        sub     edx, ecx               ; Remaining data after loop
        sub     esi, eax               ; Nearest preceding aligned block of src

%IFNDEF POSITIONINDEPENDENT
        ; Check if count very big
        cmp     ecx, [_CacheBypassLimit]
        ja      B1400                   ; Use non-temporal store if count > _CacheBypassLimit
        neg     ecx                    ; Negative index from the end
        
        ; Dispatch to different codes depending on src alignment
        jmp     dword [AlignmentDispatchSSSE3+eax*4]

B1400:   neg     ecx
        ; Dispatch to different codes depending on src alignment
        jmp     dword [AlignmentDispatchNT+eax*4]

%ELSE   ; Position-independent code

        ; Check if count very big
        ; Make the following instruction with address relative to RP:
        cmp     ecx, [ebx-RP+_CacheBypassLimit]
        ja      B1400                   ; Use non-temporal store if count > _CacheBypassLimit
        neg     ecx                    ; Negative index from the end
        
        ; Dispatch to different codes depending on src alignment        

        ; AlignmentDispatch table contains addresses relative to RP
        ; Add table entry to ebx=RP to get jump address.

        ; Make the following instruction with address relative to RP:
        add     ebx, [ebx-RP+AlignmentDispatchSSSE3+eax*4]
        jmp     ebx
        
B1400:   neg     ecx

        ; Same with AlignmentDispatchNT:        
        add     ebx, [ebx-RP+AlignmentDispatchNT+eax*4]
        jmp     ebx        
%ENDIF

align   16
C100:   ; Code for aligned src. SSE2 and later instruction set
        ; The nice case, src and dest have same alignment.

        ; Loop. ecx has negative index from the end, counting up to zero
        movaps  xmm0, [esi+ecx]
        movaps  xmm1, [esi+ecx+10H]
        movaps  [edi+ecx], xmm0
        movaps  [edi+ecx+10H], xmm1
        add     ecx, 20H
        jnz     C100
        
        ; Move the remaining edx bytes (0 - 31):
        add     esi, edx
        add     edi, edx
        neg     edx
        jz      C500                   ; Skip if no more data
        ; move 16-8-4-2-1 bytes, aligned
        cmp     edx, -10H
        jg      C200
        ; move 16 bytes
        movaps  xmm0, [esi+edx]
        movaps  [edi+edx], xmm0
        add     edx, 10H
C200:   cmp     edx, -8
        jg      C210        
        ; move 8 bytes
        movq    xmm0, qword [esi+edx]
        movq    qword [edi+edx], xmm0
        add     edx, 8 
        jz      C500                   ; Early skip if count divisible by 8       
C210:   cmp     edx, -4
        jg      C220        
        ; move 4 bytes
        mov     eax, [esi+edx]
        mov     [edi+edx], eax
        add     edx, 4        
C220:   cmp     edx, -2
        jg      C230        
        ; move 2 bytes
        movzx   eax, word [esi+edx]
        mov     [edi+edx], ax
        add     edx, 2
C230:   cmp     edx, -1
        jg      C500        
        ; move 1 byte
        movzx   eax, byte [esi+edx]
        mov     [edi+edx], al
C500:   ; finished     
        EPILOGM
        
        
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;  Version for processors with SSE2. Aligned read + shift + aligned write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
        
align 16
_memcpySSE2:    ; global label
%IFDEF POSITIONINDEPENDENT
        call    get_thunk_edx
        add     edx, RP-$
%ENDIF
memcpySSE2@:    ; local label
        PROLOGM
        cmp     ecx, 40H
        jae     B100                   ; Use simpler code if count < 64
        
        ; count < 64. Move 32-16-8-4-2-1 bytes
        add     esi, ecx               ; end of src
        add     edi, ecx               ; end of dest
        neg     ecx                    ; negative index from the end
        cmp     ecx, -20H
        jg      A100        
        ; move 32 bytes
        ; movq is faster than movdqu on Intel Pentium M and Core 1
        ; movdqu is fast on Nehalem and later
        movq    xmm0, qword [esi+ecx]
        movq    xmm1, qword [esi+ecx+8]
        movq    xmm2, qword [esi+ecx+10H]
        movq    xmm3, qword [esi+ecx+18H]
        movq    qword [edi+ecx], xmm0
        movq    qword [edi+ecx+8], xmm1
        movq    qword [edi+ecx+10H], xmm2
        movq    qword [edi+ecx+18H], xmm3
        add     ecx, 20H
A100:   cmp     ecx, -10H        
        jg      A200
        ; move 16 bytes
        movq    xmm0, qword [esi+ecx]
        movq    xmm1, qword [esi+ecx+8]
        movq    qword [edi+ecx], xmm0
        movq    qword [edi+ecx+8], xmm1
        add     ecx, 10H
A200:   cmp     ecx, -8        
        jg      A300
        ; move 8 bytes
        movq    xmm0, qword [esi+ecx]
        movq    qword [edi+ecx], xmm0
        add     ecx, 8
A300:   cmp     ecx, -4        
        jg      A400
        ; move 4 bytes
        mov     eax, [esi+ecx]
        mov     [edi+ecx], eax
        add     ecx, 4
        jz      A900                     ; early out if count divisible by 4
A400:   cmp     ecx, -2        
        jg      A500
        ; move 2 bytes
        movzx   eax, word [esi+ecx]
        mov     [edi+ecx], ax
        add     ecx, 2
A500:   cmp     ecx, -1
        jg      A900        
        ; move 1 byte
        movzx   eax, byte [esi+ecx]
        mov     [edi+ecx], al
A900:   ; finished
        EPILOGM        
        
B100:   ; count >= 64
        ; This part will not always work if count < 64
        ; Calculate size of first block up to first regular boundary of dest
        mov     edx, edi
        neg     edx
        and     edx, 0FH
        jz      B200                    ; Skip if dest aligned by 16
        
        ; edx = size of first partial block, 1 - 15 bytes
        test    dl, 3
        jz      B120
        test    dl, 1
        jz      B110
        ; move 1 byte
        movzx   eax, byte [esi]
        mov     [edi], al
        inc     esi
        inc     edi
B110:   test    dl, 2
        jz      B120
        ; move 2 bytes
        movzx   eax, word [esi]
        mov     [edi], ax
        add     esi, 2
        add     edi, 2
B120:   test    dl, 4
        jz      B130
        ; move 4 bytes
        mov     eax, [esi]
        mov     [edi], eax
        add     esi, 4
        add     edi, 4
B130:   test    dl, 8
        jz      B140
        ; move 8 bytes
        movq    xmm0, qword [esi]
        movq    qword [edi], xmm0
        add     esi, 8
        add     edi, 8
B140:   sub     ecx, edx

B200:   ; Now dest is aligned by 16. Any partial block has been moved        
        ; Find alignment of src modulo 16 at this point:
        mov     eax, esi
        and     eax, 0FH
        
        ; Set up for loop moving 32 bytes per iteration:
        mov     edx, ecx               ; Save count
        and     ecx, -20H              ; Round down to nearest multiple of 32
        add     esi, ecx               ; Point to the end
        add     edi, ecx               ; Point to the end
        sub     edx, ecx               ; Remaining data after loop
        sub     esi, eax               ; Nearest preceding aligned block of src

%IFNDEF POSITIONINDEPENDENT
        ; Check if count very big
        cmp     ecx, [_CacheBypassLimit]
        ja      B400                   ; Use non-temporal store if count > _CacheBypassLimit
        neg     ecx                    ; Negative index from the end
        
        ; Dispatch to different codes depending on src alignment
        jmp     dword [AlignmentDispatchSSE2+eax*4]

B400:   neg     ecx
        ; Dispatch to different codes depending on src alignment
        jmp     dword [AlignmentDispatchNT+eax*4]

%ELSE   ; Position-independent code

        ; Check if count very big
        ; Make the following instruction with address relative to RP:
        cmp     ecx, [ebx-RP+_CacheBypassLimit]
        ja      B400                   ; Use non-temporal store if count > _CacheBypassLimit
        neg     ecx                    ; Negative index from the end
        
        ; Dispatch to different codes depending on src alignment        

        ; AlignmentDispatch tables contain addresses relative to RP
        ; Add table entry to ebx=RP to get jump address.

        ; Make the following instruction with address relative to RP:
        add     ebx, [ebx-RP+AlignmentDispatchSSE2+eax*4]
        jmp     ebx
        
B400:   neg     ecx

        ; Same with AlignmentDispatchNT:        
        add     ebx, [ebx-RP+AlignmentDispatchNT+eax*4]
        jmp     ebx        
%ENDIF


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;  Macros and alignment jump tables
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Macros for each src alignment, SSE2 instruction set:
; Make separate code for each alignment u because the shift instructions
; have the shift count as a constant:

%MACRO MOVE_UNALIGNED_SSE2  2
; Move ecx + edx bytes of data
; Source is misaligned. (src-dest) modulo 16 = %1
; %2 = 1 if non-temporal store desired
; eax = %1
; esi = src - %1 = nearest preceding 16-bytes boundary
; edi = dest (aligned)
; ecx = - (count rounded down to nearest divisible by 32)
; edx = remaining bytes to move after loop
        movdqa  xmm0, [esi+ecx]        ; Read from nearest preceding 16B boundary
%%L1:   ; Loop. ecx has negative index from the end, counting up to zero
        movdqa  xmm1, [esi+ecx+10H]    ; Read next two blocks aligned
        movdqa  xmm2, [esi+ecx+20H]
        movdqa  xmm3, xmm1             ; Copy because used twice
        psrldq  xmm0, %1               ; shift right
        pslldq  xmm1, 16-%1            ; shift left
        por     xmm0, xmm1             ; combine blocks
        %IF %2 == 0
        movdqa  [edi+ecx], xmm0        ; Save aligned
        %ELSE
        movntdq [edi+ecx], xmm0        ; non-temporal save
        %ENDIF
        movdqa  xmm0, xmm2             ; Save for next iteration
        psrldq  xmm3, %1               ; shift right
        pslldq  xmm2, 16-%1            ; shift left
        por     xmm3, xmm2             ; combine blocks
        %IF %2 == 0
        movdqa  [edi+ecx+10H], xmm3    ; Save aligned
        %ELSE
        movntdq [edi+ecx+10H], xmm3    ; non-temporal save
        %ENDIF
        add     ecx, 20H               ; Loop through negative values up to zero
        jnz     %%L1
        
        ; Set up for edx remaining bytes
        add     esi, edx
        add     edi, edx
        neg     edx
        cmp     edx, -10H
        jg      %%L2
        ; One more 16-bytes block to move
        movdqa  xmm1, [esi+edx+10H]
        psrldq  xmm0, %1               ; shift right
        pslldq  xmm1, 16-%1            ; shift left
        por     xmm0, xmm1             ; combine blocks
        %IF %2 == 0
        movdqa  [edi+edx], xmm0        ; Save aligned
        %ELSE
        movntdq [edi+edx], xmm0        ; non-temporal save
        %ENDIF        
        add     edx, 10H        
%%L2:   ; Get src pointer back to misaligned state
        add     esi, eax
        %IF %2 == 1
        sfence
        %ENDIF
        ; Move remaining 0 - 15 bytes, unaligned
        jmp     C200
%ENDMACRO

%MACRO  MOVE_UNALIGNED_SSE2_4  1
; Special case for u = 4
; %1 = 1 if non-temporal store desired
        movaps  xmm0, [esi+ecx]        ; Read from nearest preceding 16B boundary
%%L1:   ; Loop. ecx has negative index from the end, counting up to zero
        movaps  xmm1, [esi+ecx+10H]    ; Read next two blocks aligned
        movss   xmm0, xmm1             ; Moves 4 bytes, leaves remaining bytes unchanged
       ;pshufd  xmm0, xmm0, 00111001B
        shufps  xmm0, xmm0, 00111001B
        %IF %1 == 0
        movaps  [edi+ecx], xmm0        ; Save aligned
        %ELSE
        movntps [edi+ecx], xmm0        ; Non-temporal save
        %ENDIF
        movaps  xmm0, [esi+ecx+20H]
        movss   xmm1, xmm0
        shufps  xmm1, xmm1, 00111001B
        %IF %1 == 0
        movaps  [edi+ecx+10H], xmm1    ; Save aligned
        %ELSE
        movntps [edi+ecx+10H], xmm1    ; Non-temporal save
        %ENDIF
        add     ecx, 20H               ; Loop through negative values up to zero
        jnz     %%L1        
        ; Set up for edx remaining bytes
        add     esi, edx
        add     edi, edx
        neg     edx
        cmp     edx, -10H
        jg      %%L2
        ; One more 16-bytes block to move
        movaps  xmm1, [esi+edx+10H]    ; Read next two blocks aligned
        movss   xmm0, xmm1
        shufps  xmm0, xmm0, 00111001B
        %IF %1 == 0
        movaps  [edi+edx], xmm0        ; Save aligned
        %ELSE
        movntps [edi+edx], xmm0        ; Non-temporal save
        %ENDIF
        add     edx, 10H        
%%L2:   ; Get src pointer back to misaligned state
        add     esi, eax
        %IF %1 == 1
        sfence
        %ENDIF
        ; Move remaining 0 - 15 bytes, unaligned
        jmp     C200
%ENDMACRO

%MACRO  MOVE_UNALIGNED_SSE2_8  1
; Special case for u = 8
; %1 = 1 if non-temporal store desired
        movaps  xmm0, [esi+ecx]        ; Read from nearest preceding 16B boundary
%%L1:  ; Loop. ecx has negative index from the end, counting up to zero
        movaps  xmm1, [esi+ecx+10H]    ; Read next two blocks aligned
        movsd   xmm0, xmm1             ; Moves 8 bytes, leaves remaining bytes unchanged
        shufps  xmm0, xmm0, 01001110B  ; Rotate
        %IF %1 == 0
        movaps  [edi+ecx], xmm0        ; Save aligned
        %ELSE
        movntps [edi+ecx], xmm0        ; Non-temporal save
        %ENDIF
        movaps  xmm0, [esi+ecx+20H]
        movsd   xmm1, xmm0
        shufps  xmm1, xmm1, 01001110B
        %IF %1 == 0
        movaps  [edi+ecx+10H], xmm1    ; Save aligned
        %ELSE
        movntps [edi+ecx+10H], xmm1    ; Non-temporal save
        %ENDIF
        add     ecx, 20H               ; Loop through negative values up to zero
        jnz     %%L1        
        ; Set up for edx remaining bytes
        add     esi, edx
        add     edi, edx
        neg     edx
        cmp     edx, -10H
        jg      %%L2
        ; One more 16-bytes block to move
        movaps  xmm1, [esi+edx+10H]    ; Read next two blocks aligned
        movsd   xmm0, xmm1
        shufps  xmm0, xmm0, 01001110B
        %IF %1 == 0
        movaps  [edi+edx], xmm0        ; Save aligned
        %ELSE
        movntps [edi+edx], xmm0        ; Non-temporal save
        %ENDIF
        add     edx, 10H        
%%L2:   ; Get src pointer back to misaligned state
        add     esi, eax
        %IF %1 == 1
        sfence
        %ENDIF
        ; Move remaining 0 - 15 bytes, unaligned
        jmp     C200
%ENDMACRO

%MACRO  MOVE_UNALIGNED_SSE2_12  1
; %1 = 1 if non-temporal store desired
; Special case for u = 12
        movaps  xmm0, [esi+ecx]        ; Read from nearest preceding 16B boundary
        shufps  xmm0, xmm0, 10010011B
%%L1:   ; Loop. ecx has negative index from the end, counting up to zero
        movaps  xmm1, [esi+ecx+10H]    ; Read next two blocks aligned
        movaps  xmm2, [esi+ecx+20H]
        shufps  xmm1, xmm1, 10010011B
        shufps  xmm2, xmm2, 10010011B
        movaps  xmm3, xmm2
        movss   xmm2, xmm1             ; Moves 4 bytes, leaves remaining bytes unchanged
        movss   xmm1, xmm0             ; Moves 4 bytes, leaves remaining bytes unchanged       
        %IF %1 == 0
        movaps  [edi+ecx], xmm1        ; Save aligned
        movaps  [edi+ecx+10H], xmm2    ; Save aligned
        %ELSE
        movntps [edi+ecx], xmm1        ; Non-temporal save
        movntps [edi+ecx+10H], xmm2    ; Non-temporal save
        %ENDIF
        movaps  xmm0, xmm3             ; Save for next iteration        
        add     ecx, 20H               ; Loop through negative values up to zero
        jnz     %%L1        
        ; Set up for edx remaining bytes
        add     esi, edx
        add     edi, edx
        neg     edx
        cmp     edx, -10H
        jg      %%L2
        ; One more 16-bytes block to move
        movaps  xmm1, [esi+edx+10H]    ; Read next two blocks aligned
        shufps  xmm1, xmm1, 10010011B
        movss   xmm1, xmm0             ; Moves 4 bytes, leaves remaining bytes unchanged       
        %IF %1 == 0
        movaps  [edi+edx], xmm1        ; Save aligned
        %ELSE
        movntps [edi+edx], xmm1        ; Non-temporal save
        %ENDIF
        add     edx, 10H        
%%L2:   ; Get src pointer back to misaligned state
        add     esi, eax
        %IF %1 == 1
        sfence
        %ENDIF
        ; Move remaining 0 - 15 bytes, unaligned
        jmp     C200
%ENDMACRO

; Macros for each src alignment, Suppl.SSE3 instruction set:
; Make separate code for each alignment u because the palignr instruction
; has the shift count as a constant:

%MACRO  MOVE_UNALIGNED_SSSE3  1
; Move ecx + edx bytes of data
; Source is misaligned. (src-dest) modulo 16 = %1
; eax = %1
; esi = src - %1 = nearest preceding 16-bytes boundary
; edi = dest (aligned)
; ecx = - (count rounded down to nearest divisible by 32)
; edx = remaining bytes to move after loop
        movdqa  xmm0, [esi+ecx]        ; Read from nearest preceding 16B boundary
        
%%L1:   ; Loop. ecx has negative index from the end, counting up to zero
        movdqa  xmm2, [esi+ecx+10H]    ; Read next two blocks
        movdqa  xmm3, [esi+ecx+20H]
        movdqa  xmm1, xmm0             ; Save xmm0
        movdqa  xmm0, xmm3             ; Save for next iteration
        palignr xmm3, xmm2, %1         ; Combine parts into aligned block
        palignr xmm2, xmm1, %1         ; Combine parts into aligned block
        movdqa  [edi+ecx], xmm2        ; Save aligned
        movdqa  [edi+ecx+10H], xmm3    ; Save aligned
        add     ecx, 20H
        jnz     %%L1
        
        ; Set up for edx remaining bytes
        add     esi, edx
        add     edi, edx
        neg     edx
        cmp     edx, -10H
        jg      %%L2
        ; One more 16-bytes block to move
        movdqa  xmm2, [esi+edx+10H]
        palignr xmm2, xmm0, %1
        movdqa  [edi+edx], xmm2
        add     edx, 10H        
%%L2:   ; Get src pointer back to misaligned state
        add     esi, eax
        ; Move remaining 0 - 15 bytes
        jmp     C200
%ENDMACRO

; Make 15 instances of SSE2 macro for each value of the alignment u.
; These are pointed to by the jump table AlignmentDispatchSSE2 below

; (aligns are inserted manually to minimize the number of 16-bytes
; boundaries inside loops in the most common cases)

align   16
D104:   MOVE_UNALIGNED_SSE2_4    0
D108:   MOVE_UNALIGNED_SSE2_8    0
;align 8
D10C:   MOVE_UNALIGNED_SSE2_12   0
D101:   MOVE_UNALIGNED_SSE2 1,   0
D102:   MOVE_UNALIGNED_SSE2 2,   0
D103:   MOVE_UNALIGNED_SSE2 3,   0
D105:   MOVE_UNALIGNED_SSE2 5,   0
D106:   MOVE_UNALIGNED_SSE2 6,   0
D107:   MOVE_UNALIGNED_SSE2 7,   0
D109:   MOVE_UNALIGNED_SSE2 9,   0
D10A:   MOVE_UNALIGNED_SSE2 0AH, 0
D10B:   MOVE_UNALIGNED_SSE2 0BH, 0
D10D:   MOVE_UNALIGNED_SSE2 0DH, 0
D10E:   MOVE_UNALIGNED_SSE2 0EH, 0
D10F:   MOVE_UNALIGNED_SSE2 0FH, 0
        
; Make 15 instances of Suppl-SSE3 macro for each value of the alignment u.
; These are pointed to by the jump table AlignmentDispatchSupSSE3 below

align   16
;times 11 nop
E104:   MOVE_UNALIGNED_SSSE3 4
;times 5 nop
E108:   MOVE_UNALIGNED_SSSE3 8
;times 5 nop
E10C:   MOVE_UNALIGNED_SSSE3 0CH
;times 5 nop
E101:   MOVE_UNALIGNED_SSSE3 1
;times 5 nop
E102:   MOVE_UNALIGNED_SSSE3 2
;times 5 nop
E103:   MOVE_UNALIGNED_SSSE3 3
;times 5 nop
E105:   MOVE_UNALIGNED_SSSE3 5
;times 5 nop
E106:   MOVE_UNALIGNED_SSSE3 6
;times 5 nop
E107:   MOVE_UNALIGNED_SSSE3 7
;times 5 nop
E109:   MOVE_UNALIGNED_SSSE3 9
;times 5 nop
E10A:   MOVE_UNALIGNED_SSSE3 0AH
;times 5 nop
E10B:   MOVE_UNALIGNED_SSSE3 0BH
;times 5 nop
E10D:   MOVE_UNALIGNED_SSSE3 0DH
;times 5 nop
E10E:   MOVE_UNALIGNED_SSSE3 0EH
;times 5 nop
E10F:   MOVE_UNALIGNED_SSSE3 0FH

; Codes for non-temporal move. Aligned case first

align   8
F100:   ; Non-temporal move, src and dest have same alignment.
        ; Loop. ecx has negative index from the end, counting up to zero
        movaps  xmm0, [esi+ecx]        ; Read
        movaps  xmm1, [esi+ecx+10H]
        movntps [edi+ecx], xmm0        ; Write non-temporal (bypass cache)
        movntps [edi+ecx+10H], xmm1
        add     ecx, 20H
        jnz     F100                   ; Loop through negative ecx up to zero
                
        ; Move the remaining edx bytes (0 - 31):
        add     esi, edx
        add     edi, edx
        neg     edx
        jz      C500                   ; Skip if no more data
        ; Check if we can more one more 16-bytes block
        cmp     edx, -10H
        jg      C200
        ; move 16 bytes, aligned
        movaps  xmm0, [esi+edx]
        movntps [edi+edx], xmm0
        add     edx, 10H
        sfence
        ; move the remaining 0 - 15 bytes
        jmp     C200

; Make 15 instances of MOVE_UNALIGNED_SSE2 macro for each value of 
; the alignment u.
; These are pointed to by the jump table AlignmentDispatchNT below

;align 16
F104:   MOVE_UNALIGNED_SSE2_4    1
F108:   MOVE_UNALIGNED_SSE2_8    1
F10C:   MOVE_UNALIGNED_SSE2_12   1
F101:   MOVE_UNALIGNED_SSE2 1,   1
F102:   MOVE_UNALIGNED_SSE2 2,   1
F103:   MOVE_UNALIGNED_SSE2 3,   1
F105:   MOVE_UNALIGNED_SSE2 5,   1
F106:   MOVE_UNALIGNED_SSE2 6,   1
F107:   MOVE_UNALIGNED_SSE2 7,   1
F109:   MOVE_UNALIGNED_SSE2 9,   1
F10A:   MOVE_UNALIGNED_SSE2 0AH, 1
F10B:   MOVE_UNALIGNED_SSE2 0BH, 1
F10D:   MOVE_UNALIGNED_SSE2 0DH, 1
F10E:   MOVE_UNALIGNED_SSE2 0EH, 1
F10F:   MOVE_UNALIGNED_SSE2 0FH, 1

%IFDEF  POSITIONINDEPENDENT
get_thunk_edx: ; load caller address into edx for position-independent code
        mov edx, [esp]
        ret
%ENDIF


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;  Version for old processors without SSE2
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

align 8
; 80386 version used when SSE2 not supported:
_memcpy386:  ; global label
memcpy386@:  ; local label
        PROLOGM
; edi = dest
; esi = src
; ecx = count
        cld
        cmp     ecx, 8
        jb      G500
G100:   test    edi, 1
        jz      G200
        movsb
        dec     ecx
G200:   test    edi, 2
        jz      G300
        movsw
        sub     ecx, 2
G300:   ; edi is aligned now
        mov     edx, ecx
        shr     ecx, 2
        rep     movsd                  ; move 4 bytes at a time
        mov     ecx, edx
        and     ecx, 3
        rep     movsb                  ; move remaining 0-3 bytes
        EPILOGM
        
G500:   ; count < 8. Move one byte at a time
        rep     movsb                  ; move count bytes
        EPILOGM
        

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;    CPU dispatcher
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; CPU dispatching for memcpy. This is executed only once
memcpyCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        pushad
        ; set _CacheBypassLimit to half the size of the largest level cache
        call    GetMemcpyCacheLimit@
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version of memcpy
        mov     esi, memcpy386@
        cmp     eax, 4                 ; check SSE2
        jb      Q100
        ; SSE2 supported
        ; Point to SSE2 version of memcpy
        mov     esi, memcpySSE2@
        cmp     eax, 6                 ; check Suppl-SSE3
        jb      Q100
        ; Suppl-SSE3 supported
        ; Point to SSSE3 version of memcpy
        mov     esi, memcpySSSE3@
        call    _UnalignedIsFaster     ; Test if unaligned read is faster than aligned read and shift
        test    eax, eax
        jz      Q100
        ; Point to unaligned version of memcpy
        mov     esi, memcpyU@
        call    _Store256BitIsFaster   ; Test if 256-bit read/write is available and faster than 128-bit read/write
        test    eax, eax
        jz      Q100
        mov     esi, memcpyU256@
        call    _InstructionSet
        cmp     eax, 15
        jb      Q100
        mov     esi, memcpyAVX512F@
        cmp     eax, 16
        jb      Q100
        mov     esi, memcpyAVX512BW@

Q100:   
        mov     [memcpyDispatch], esi
        popad
        ; Continue in appropriate version of memcpy
        jmp     [memcpyDispatch]

%ELSE   ; Position-independent version
        pushad
        mov     ebx, edx               ; reference point
        ; set _CacheBypassLimit to half the size of the largest level cache
        call    GetMemcpyCacheLimit@
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version of memcpy
        lea     esi, [ebx+memcpy386@-RP]
        cmp     eax, 4                  ; check SSE2
        jb      Q100
        ; SSE2 supported
        ; Point to SSE2 version of memcpy
        lea     esi, [ebx+memcpySSE2@-RP]
        cmp     eax, 6                  ; check Suppl-SSE3
        jb      Q100
        ; Suppl-SSE3 supported
        ; Point to SSSE3 version of memcpy
        lea     esi, [ebx+memcpySSSE3@-RP]        
        call    _UnalignedIsFaster      ; Test if unaligned read is faster than aligned read and shift
        test    eax, eax
        jz      Q100
        ; Point to unaligned version of memcpy
        lea     esi, [ebx+memcpyU@-RP]
        call    _Store256BitIsFaster    ; Test if 256-bit read/write is available and faster than 128-bit read/write
        test    eax, eax
        jz      Q100
        lea     esi, [ebx+memcpyU256@-RP]

; memcpyAVX512F and memcpyAVX512BW are not available in position-independent versions
;       call    _InstructionSet
;       cmp     eax, 15
;       jb      Q100
;       lea     esi, [ebx+memcpyAVX512F@-RP]
;       cmp     eax, 16
;       jb      Q100
;       lea     esi, [ebx+memcpyAVX512BW@-RP]

Q100:   ; insert appropriate pointer
        mov     dword [ebx+memcpyDispatch-RP], esi
        popad
        ; Continue in appropriate version of memcpy
        jmp     [edx+memcpyDispatch-RP]        
%ENDIF

; extern "C" size_t GetMemcpyCacheLimit();
_GetMemcpyCacheLimit:
GetMemcpyCacheLimit@:  ; local label
        push    ebx
%ifdef  POSITIONINDEPENDENT
        call    get_thunk_edx
        lea     ebx, [edx + _CacheBypassLimit - $]
%else
        mov     ebx, _CacheBypassLimit
%endif
        mov     eax, [ebx]
        test    eax, eax
        jnz     U200
        ; Get half the size of the largest level cache
        push    0                      ; 0 means largest level cache
        call    _DataCacheSize         ; get cache size
        pop     ecx
        shr     eax, 1                 ; half the size
        jnz     U100
        mov     eax, 400000H           ; cannot determine cache size. use 4 Mbytes
U100:   mov     [ebx], eax
U200:   pop     ebx
        ret
        
; Called internally from _SetMemcpyCacheLimit defined in memmove32.asm
; Must return the value set
_SetMemcpyCacheLimit1:
        push    ebx
%ifdef  POSITIONINDEPENDENT
        call    get_thunk_edx
        lea     ebx, [edx + _CacheBypassLimit - $]
%else
        mov     ebx, _CacheBypassLimit
%endif
        mov     eax, [esp+8]
        test    eax, eax
        jnz     U400
        ; zero, means default
        mov     [ebx], eax
        call    GetMemcpyCacheLimit@
U400:   
        mov     [ebx], eax
        pop     ebx
        ret
        

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;                   getDispatch, for testing only
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
getDispatch:
mov eax,[memcpyDispatch]
ret


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;    data section. jump tables, dispatch function pointer, cache size
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Data segment must be included in function namespace
SECTION .data
align 16

; Jump tables for alignments 0 - 15:
; The CPU dispatcher replaces AlignmentDispatchSSE2 with 
; AlignmentDispatchSupSSE3 if Suppl-SSE3 is supported
; RP = reference point if position-independent code, otherwise RP = 0

; Code pointer for each alignment for SSE2 instruction set
AlignmentDispatchSSE2:
DD C100-RP, D101-RP, D102-RP, D103-RP, D104-RP, D105-RP, D106-RP, D107-RP
DD D108-RP, D109-RP, D10A-RP, D10B-RP, D10C-RP, D10D-RP, D10E-RP, D10F-RP

; Code pointer for each alignment for Suppl.SSE3 instruction set
AlignmentDispatchSSSE3:
DD C100-RP, E101-RP, E102-RP, E103-RP, E104-RP, E105-RP, E106-RP, E107-RP
DD E108-RP, E109-RP, E10A-RP, E10B-RP, E10C-RP, E10D-RP, E10E-RP, E10F-RP

; Code pointer for each alignment for non-temporal store
AlignmentDispatchNT:
DD F100-RP, F101-RP, F102-RP, F103-RP, F104-RP, F105-RP, F106-RP, F107-RP
DD F108-RP, F109-RP, F10A-RP, F10B-RP, F10C-RP, F10D-RP, F10E-RP, F10F-RP


; Pointer to appropriate version.
; This initially points to memcpyCPUDispatch. memcpyCPUDispatch will
; change this to the appropriate version of memcpy, so that
; memcpyCPUDispatch is only executed once:
memcpyDispatch: DD memcpyCPUDispatch

; Bypass cache by using non-temporal moves if count > _CacheBypassLimit
; The optimal value of _CacheBypassLimit is difficult to estimate, but
; a reasonable value is half the size of the largest cache:
_CacheBypassLimit: DD 0

%IFDEF POSITIONINDEPENDENT
; Fix potential problem in Mac linker
        DD      0, 0
%ENDIF
