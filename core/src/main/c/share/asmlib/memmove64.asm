;*************************  memmove64.asm  ***********************************
; Author:           Agner Fog
; Date created:     2008-07-18
; Last modified:    2016-11-16
; Description:
; Faster version of the standard memmove function:
; void * A_memmove(void *dest, const void *src, size_t count);
; Moves 'count' bytes from 'src' to 'dest'. src and dest may overlap.
;
; Overriding standard function memmove:
; The alias ?OVR_memmove is changed to _memmove in the object file if
; it is desired to override the standard library function memmove.
;
; CPU dispatching included for different CPUs
;
; Copyright (c) 2008-2016 GNU General Public License www.gnu.org/licenses
;******************************************************************************

default rel

global A_memmove                       ; Function A_memmove
global ?OVR_memmove                    ; ?OVR removed if standard function memmove overridden
global memmoveSSE2                     ; Version for processors with only SSE2
global memmoveSSSE3                    ; Version for processors with SSSE3
global memmoveU                        ; Version for processors with fast unaligned read
global memmoveU256                     ; Version for processors with fast 256-bit read/write
global memmoveAVX512F                  ; Version for processors with fast 512-bit read/write
global memmoveAVX512BW                 ; Version for processors with fast 512-bit read/write
global SetMemcpyCacheLimit             ; Change limit for bypassing cache

; Imported from memcpy64.asm:
extern A_memcpy                        ; function entry
extern memcpySSE2                      ; CPU specific function entry
extern memcpySSSE3                     ; CPU specific function entry
extern memcpyU                         ; CPU specific function entry
extern memcpyU256                      ; CPU specific function entry
extern memcpyAVX512F                   ; CPU specific function entry
extern memcpyAVX512BW                  ; CPU specific function entry

; Imported from instrset64.asm
extern InstructionSet                  ; Instruction set for CPU dispatcher

; Imported from unalignedisfaster64.asm:
extern UnalignedIsFaster               ; Tells if unaligned read is faster than PALIGNR
extern Store256BitIsFaster             ; Tells if a 256 bit store is faster than two 128 bit stores

; Imported from memcpy64.asm
extern GetMemcpyCacheLimit             ; Get the size limit for bypassing cache when copying with memcpy and memmove
extern SetMemcpyCacheLimit1            ; Set the size limit for bypassing cache when copying with memcpy


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;      Prolog macro. Determine if we should move forwards or backwards
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Define prolog for this function
; Parameter 1 is forward function label
%MACRO  PROLOGM  1
%IFDEF  WINDOWS
        ; Check if dest overlaps src
        mov     rax, rcx
        sub     rax, rdx
        cmp     rax, r8
        ; We can avoid testing for dest < src by using unsigned compare:
        ; (Assume that the memory block cannot span across address 0)
        ; Must move backwards if unsigned(dest-src) < count
        jae     %1                     ; Jump to memcpy if we can move forwards
        push    rsi
        push    rdi
        mov     rdi, rcx               ; dest
        mov     r9,  rcx               ; dest
        mov     rsi, rdx               ; src
        mov     rcx, r8                ; count
%ELSE   ; Unix
        ; Check if dest overlaps src
        mov     rax, rdi
        sub     rax, rsi
        cmp     rax, rdx
        ; Must move backwards if unsigned(dest-src) < count
        jae     %1                     ; Jump to memcpy if we can move forwards
        mov     rcx, rdx               ; count
        mov     r9,  rdi               ; dest
%ENDIF
%ENDM


; Define return from this function
%MACRO  EPILOGM  0
%IFDEF  WINDOWS
        pop     rdi
        pop     rsi
%ENDIF
        mov     rax, r9                ; Return value = dest
        ret
%ENDMACRO


SECTION .text  align=16

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;                          Common entry for dispatch
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; extern "C" void * A_memmove(void * dest, const void * src, size_t count);
; Function entry:
A_memmove:
?OVR_memmove:
        jmp     qword [memmoveDispatch] ; Go to appropriate version, depending on instruction set

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; AVX512BW Version for processors with fast unaligned read and fast 512 bit write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; rdi = destination
; rsi = source
; rcx = length

align 16 ; short versions
L000:   ; 41H - 80H bytes
        vmovdqu64 zmm16, [rsi+rcx-40H]
        vmovdqu64 [rdi+rcx-40H], zmm16
        sub     rcx, 40H

L010:   ; 0 - 40H bytes
        mov     rax, -1                ; generate masks
        bzhi    rax, rax, rcx
        kmovq   k1, rax
        vmovdqu8 zmm16{k1}{z}, [rsi]
        vmovdqu8 [rdi]{k1}, zmm16
        ; vzeroupper not needed when using zmm16-31?
        EPILOGM

%IFDEF  WINDOWS
times 10 nop                           ; align L200
%ELSE   ; Unix
times 4 nop                            ; align L200
%ENDIF

memmoveAVX512BW:                       ; Version for processors with fast 512-bit read/write
memmoveAVX512BW@:                      ; local label
        PROLOGM memcpyAVX512BW         ; jump to memcpyAVX512BW if copying forwards
        cmp     rcx, 40H
        jbe     L010                   ; Use simpler code if count <= 64
        cmp     rcx, 80H
        jbe     L000                   ; Use simpler code if count <= 128
        
L100:   ; count >= 80H                 ; Entry from memmoveAVX512F
        vmovdqu64 zmm17, [rsi]         ; save first possibly unaligned block to after main loop
        vmovdqu64 zmm18, [rsi+rcx-40H] ; save last  possibly unaligned block to after main loop

        ; Align destination by 40H
        mov     rdx, rdi
        add     rdi, 40H
        and     rdi, -40H              ; first 40H boundary of destination
        lea     rax, [rdx+rcx]         ; end of destination
        sub     rdx, rdi               ; -offset
        sub     rsi, rdx               ; add same offset to source
        add     rcx, rdx               ; size minus first unaligned part
        and     rcx, -40H              ; round down size to multiple of 40H
        
        ; Check if count very big
        cmp     rcx, [CacheBypassLimit]
        ja      L800                   ; Use non-temporal store if count > CacheBypassLimit

;align   16 
L200:   ; Main move loop
        sub      rcx, 40H
        vmovdqu64 zmm16, [rsi+rcx]
        vmovdqa64 [rdi+rcx], zmm16
        jnz      L200
        
L300:   ; insert remaining parts in beginning and end, possibly overwriting what has already been stored
        vmovdqu64 [rax-40H], zmm18
        vmovdqu64 [r9], zmm17
        ; vzeroupper not needed when using zmm16-31?
        EPILOGM

align 16
L800:  ; move loop, bypass cache
        sub      rcx, 40H
        vmovdqu64 zmm16, [rsi+rcx]
        vmovntdq [rdi+rcx], zmm16
        jnz      L800
        sfence
        jmp      L300

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; AVX512F Version for processors with fast unaligned read and fast 512 bit write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; rdi = destination
; rsi = source
; rcx = length

align 16
memmoveAVX512F:                        ; Version for processors with fast 512-bit read/write
memmoveAVX512F@:                       ; local label
        PROLOGM memcpyAVX512F          ; jump to memcpyAVX512F if copying forwards
        cmp     rcx, 80H
        jnb     L100                   ; use same code as memmoveAVX512BW if count >= 80H
        cmp     rcx, 40H
        jb      A1000                  ; use code below if count < 40H
        ; count = 40H - 80H. Make two partially overlapping 40H blocks
        vmovdqu64 zmm16, [rsi]
        vmovdqu64 zmm17, [rsi+rcx-40H]
        vmovdqu64 [rdi], zmm16
        vmovdqu64 [rdi+rcx-40H], zmm17
        ; vzeroupper not needed when using zmm16-31?
        EPILOGM


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; AVX Version for processors with fast unaligned read and fast 256 bit write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

align 16
memmoveU256:   ; Version for processors with fast 256-bit read/write
memmoveU256@:  ; local label
        PROLOGM memcpyU256
      
        cmp     rcx, 40H
        jb      A1000                    ; Use simpler code if count < 64
        
        ; count >= 64
        ; Note: this part will not always work if count < 64
        ; Calculate size of last block after last regular boundary of dest
        lea     edx, [rdi+rcx]          ; end of dext
        and     edx, 1FH
        jz      B4300                   ; Skip if end of dest aligned by 32
        
        ; edx = size of last partial block, 1 - 31 bytes
        test    dl, 3
        jz      B4210
        test    dl, 1
        jz      B4201      ; B4200 if we haven't tested edx,3
        ; move 1 byte
        dec     rcx
        movzx   eax, byte [rsi+rcx]
        mov     [rdi+rcx], al        
B4200:  test    dl, 2
        jz      B4210
B4201:  ; move 2 bytes
        sub     rcx, 2
        movzx   eax, word [rsi+rcx]
        mov     [rdi+rcx], ax        
B4210:  test    dl, 4
        jz      B4220
        ; move 4 bytes
        sub     rcx, 4
        mov     eax, [rsi+rcx]
        mov     [rdi+rcx], eax
B4220:  test    dl, 8
        jz      B4230
        ; move 8 bytes
        sub     rcx, 8
        mov     rax, [rsi+rcx]
        mov     [rdi+rcx], rax
B4230:  test    dl, 16
        jz      B4300
        ; move 16 bytes
        sub     rcx, 16
        movups  xmm0, [rsi+rcx]
        movaps  [rdi+rcx], xmm0
        
B4300:  ; Now end of dest is aligned by 32. Any partial block has been moved        
        mov     rdx, rcx
        and     ecx, 1FH              ; remaining size after 32 bytes blocks moved
        and     rdx, -20H             ; number of 32 bytes blocks
        jz      H4100
        add     rsi, rcx
        add     rdi, rcx
        
        ; Check if count very big
        cmp     rdx, [CacheBypassLimit]
        ja      H4800                   ; Use non-temporal store if count > _CacheBypassLimit

H4000:  ; 32 bytes move loop
        vmovups  ymm0, [rsi+rdx-20H]
        vmovaps  [rdi+rdx-20H], ymm0
        sub      rdx, 20H
        jnz      H4000
        vzeroupper
        
H4090:  sub      rsi, rcx
        sub      rdi, rcx

H4100:  ; remaining 0-31 bytes
        test    ecx, ecx
        jz      H4600        
        test    cl, 10H
        jz      H4200
        ; move 16 bytes
        sub     ecx, 10H
        movups  xmm0, [rsi+rcx]
        movaps  [rdi+rcx], xmm0
        jz      H4600                     ; early out if count divisible by 16
H4200:  test    cl, 8
        jz      H4300
        ; move 8 bytes
        sub     ecx, 8
        mov     rax, [rsi+rcx]
        mov     [rdi+rcx], rax
H4300:  test    cl, 4
        jz      H4400
        ; move 4 bytes
        sub     ecx, 4
        mov     eax, [rsi+rcx]
        mov     [rdi+rcx], eax
        jz      H4600                     ; early out if count divisible by 4
H4400:  test    cl, 2
        jz      H4500
        ; move 2 bytes
        sub     ecx, 2
        movzx   eax, word [rsi+rcx]
        mov     [rdi+rcx], ax
H4500:  test    cl, 1
        jz      H4600
        ; move 1 byte
        movzx   eax, byte [rsi]   ; rcx-1 = 0
        mov     [rdi], al
H4600:  ; finished
        EPILOGM

align 16
H4800:  ; 32 bytes move loop, bypass cache
        vmovups  ymm0, [rsi+rdx-20H]
        vmovntps [rdi+rdx-20H], ymm0
        sub      rdx, 20H
        jnz      H4800        
        vzeroupper
        sfence
        jmp      H4090
        
A1000:  ; count < 64. Move 32-16-8-4-2-1 bytes
        test    cl, 20H
        jz      A1100
        ; move 32 bytes
        ; movups is faster on processors with SSSE3
        sub     ecx, 20H
        movups     xmm0, [rsi+rcx+10H]
        movups     xmm1, [rsi+rcx]
        movups     [rdi+rcx+10H], xmm0
        movups     [rdi+rcx], xmm1
A1100:  test    cl, 10H
        jz      A1200
        ; move 16 bytes
        sub     ecx, 10H
        movups     xmm0, [rsi+rcx]
        movups     [rdi+rcx], xmm0
A1200:  test    cl, 8
        jz      A1300
        ; move 8 bytes
        sub     ecx, 8
        mov     rax, [rsi+rcx]
        mov     [rdi+rcx], rax
A1300:  test    cl, 4
        jz      A1400
        ; move 4 bytes
        sub     ecx, 4
        mov     eax, [rsi+rcx]
        mov     [rdi+rcx], eax
        jz      A1900                     ; early out if count divisible by 4
A1400:  test    cl, 2
        jz      A1500
        ; move 2 bytes
        sub     ecx, 2
        movzx   eax, word [rsi+rcx]
        mov     [rdi+rcx], ax
A1500:  test    cl, 1
        jz      A1900
        ; move 1 byte
        movzx   eax, byte [rsi]   ; rcx-1 = 0
        mov     [rdi], al
A1900:  ; finished
        EPILOGM
        

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;  Version for processors with fast unaligned read and fast 16 bytes write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
                
align 16
memmoveU:  ; Version for processors with fast unaligned read
memmoveU@: ; local label
        PROLOGM memcpyU
      
        cmp     rcx, 40H
        jb      A1000                    ; Use simpler code if count < 64
        
        ; count >= 64
        ; Note: this part will not always work if count < 64
        ; Calculate size of last block after last regular boundary of dest
        lea     edx, [rdi+rcx]          ; end of dext
        and     edx, 0FH
        jz      B3300                   ; Skip if end of dest aligned by 16
        
        ; edx = size of last partial block, 1 - 15 bytes
        test    dl, 3
        jz      B3210
        test    dl, 1
        jz      B3201      ; B3200 if we haven't tested edx,3
        ; move 1 byte
        dec     rcx
        mov     al, byte [rsi+rcx]
        mov     [rdi+rcx], al        
B3200:  test    dl, 2
        jz      B3210
B3201:  ; move 2 bytes
        sub     rcx, 2
        mov     ax, word [rsi+rcx]
        mov     [rdi+rcx], ax        
B3210:  test    dl, 4
        jz      B3220
        ; move 4 bytes
        sub     rcx, 4
        mov     eax, [rsi+rcx]
        mov     [rdi+rcx], eax
B3220:  test    dl, 8
        jz      B3300
        ; move 8 bytes
        sub     rcx, 8
        mov     rax, [rsi+rcx]
        mov     [rdi+rcx], rax        
        
B3300:  ; Now end of dest is aligned by 16. Any partial block has been moved        
        mov     rdx, rcx
        and     ecx, 1FH              ; remaining size after 32 bytes blocks moved
        and     rdx, -20H             ; number of 32 bytes blocks
        jz      H1100
        add     rsi, rcx
        add     rdi, rcx
        
        ; Check if count very big
        cmp     rdx, [CacheBypassLimit]
        ja      H1800                   ; Use non-temporal store if count > _CacheBypassLimit

        ;  align   16
H1000:  ; 32 bytes move loop
        movups   xmm1, [rsi+rdx-20H]
        movups   xmm0, [rsi+rdx-10H]
        movaps   [rdi+rdx-20H], xmm1
        movaps   [rdi+rdx-10H], xmm0
        sub      rdx, 20H
        jnz      H1000
        
H1090:  sub      rsi, rcx
        sub      rdi, rcx

H1100:  ; remaining 0-31 bytes
        test    ecx, ecx
        jz      H1600        
        test    cl, 10H
        jz      H1200
        ; move 16 bytes
        sub     ecx, 10H
        movups  xmm0, [rsi+rcx]
        movaps  [rdi+rcx], xmm0
        jz      H1600                     ; early out if count divisible by 16
H1200:  test    cl, 8
        jz      H1300
        ; move 8 bytes
        sub     ecx, 8
        mov     rax, [rsi+rcx]
        mov     [rdi+rcx], rax
H1300:  test    cl, 4
        jz      H1400
        ; move 4 bytes
        sub     ecx, 4
        mov     eax, [rsi+rcx]
        mov     [rdi+rcx], eax
        jz      H1600                     ; early out if count divisible by 4
H1400:  test    cl, 2
        jz      H1500
        ; move 2 bytes
        sub     ecx, 2
        movzx   eax, word [rsi+rcx]
        mov     [rdi+rcx], ax
H1500:  test    cl, 1
        jz      H1600
        ; move 1 byte
        movzx   eax, byte [rsi]   ; rcx-1 = 0
        mov     [rdi], al
H1600:  ; finished
        EPILOGM

align 16
H1800:  ; 32 bytes move loop, bypass cache
        movups   xmm1, [rsi+rdx-20H]
        movups   xmm0, [rsi+rdx-10H]
        movntps  [rdi+rdx-20H], xmm1
        movntps  [rdi+rdx-10H], xmm0
        sub      rdx, 20H
        jnz      H1800 
        sfence       
        jmp      H1090
        
        
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;  Version for processors with SSSE3. Aligned read + shift + aligned write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
        
align 16
memmoveSSSE3:   ; SSSE3 version begins here
memmoveSSSE3@:  ; local label
        PROLOGM memcpySSSE3

        ; Cannot use memcpy. Must move backwards because of overlap between src and dest
        cmp     rcx, 40H
        jb      A1000                    ; Use simpler code if count < 64
        ; count >= 64
        ; Note: this part will not always work if count < 64
        ; Calculate size of last block after last regular boundary of dest
        lea     edx, [rdi+rcx]         ; end of dext
        and     edx, 0FH
        jz      B1300                   ; Skip if end of dest aligned by 16
        
        ; edx = size of last partial block, 1 - 15 bytes
        test    dl, 3
        jz      B1210
        test    dl, 1
        jz      B1201      ; B1200 if we haven't tested edx,3
        ; move 1 byte
        dec     rcx
        movzx   eax, byte [rsi+rcx]
        mov     [rdi+rcx], al        
B1200:  test    dl, 2
        jz      B1210
B1201:  ; move 2 bytes
        sub     rcx, 2
        movzx   eax, word [rsi+rcx]
        mov     [rdi+rcx], ax        
B1210:  test    dl, 4
        jz      B1220
        ; move 4 bytes
        sub     rcx, 4
        mov     eax, [rsi+rcx]
        mov     [rdi+rcx], eax
B1220:  test    dl, 8
        jz      B1300
        ; move 8 bytes
        sub     rcx, 8
        mov     rax, [rsi+rcx]
        mov     [rdi+rcx], rax
              
B1300:  ; Now end of dest is aligned by 16. Any partial block has been moved        
        ; Find alignment of end of src modulo 16 at this point:
        lea     eax, [rsi+rcx]
        and     eax, 0FH
        
        ; Set up for loop moving 32 bytes per iteration:
        mov     edx, ecx               ; Save count
        and     rcx, -20H              ; Round down to nearest multiple of 32
        sub     edx, ecx               ; Remaining data after loop
        sub     rsi, rax               ; Nearest preceding aligned block of src
        ; Add the same to rsi and rdi as we have subtracted from rcx
        add     rsi, rdx
        add     rdi, rdx
        
        ; Check if count very big
        cmp     rcx, [CacheBypassLimit]
        ja      B1400                   ; Use non-temporal store if count > CacheBypassLimit
        
        ; Dispatch to different codes depending on src alignment
        lea     r8, [MAlignmentDispatchSSSE3]
        jmp     near [r8+rax*8]

B1400:  ; Dispatch to different codes depending on src alignment
        lea     r8, [MAlignmentDispatchNT]
        jmp     near [r8+rax*8]
        

align   16
C100:   ; Code for aligned src. SSE2 and later CPUs
        ; The nice case, src and dest have same alignment.

        ; Loop. rcx has positive index from the beginning, counting down to zero
        movaps  xmm0, [rsi+rcx-10H]
        movaps  xmm1, [rsi+rcx-20H]
        movaps  [rdi+rcx-10H], xmm0
        movaps  [rdi+rcx-20H], xmm1
        sub     rcx, 20H
        jnz     C100
        
        ; Move the remaining edx bytes (0 - 31):
        ; move 16-8-4-2-1 bytes, aligned
        test    edx, edx
        jz      C500                   ; Early out if no more data
        test    dl, 10H
        jz      C200
        ; move 16 bytes
        sub     rcx, 10H
        movaps  xmm0, [rsi+rcx]
        movaps  [rdi+rcx], xmm0
        
C200:   ; Other branches come in here, rcx may contain arbitrary offset
        test    edx, edx
        jz      C500                   ; Early out if no more data
        test    dl, 8
        jz      C210        
        ; move 8 bytes
        sub     rcx, 8 
        mov     rax, [rsi+rcx]
        mov     [rdi+rcx], rax
C210:   test    dl, 4
        jz      C220        
        ; move 4 bytes
        sub     rcx, 4        
        mov     eax, [rsi+rcx]
        mov     [rdi+rcx], eax
        jz      C500                   ; Early out if count divisible by 4
C220:   test    dl, 2
        jz      C230        
        ; move 2 bytes
        sub     rcx, 2
        movzx   eax, word [rsi+rcx]
        mov     [rdi+rcx], ax
C230:   test    dl, 1
        jz      C500        
        ; move 1 byte
        movzx   eax, byte [rsi+rcx-1]   ; rcx-1 is not always 0 here
        mov     [rdi+rcx-1], al
C500:   ; finished     
        EPILOGM
        

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;  Version for processors with SSE2. Aligned read + shift + aligned write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
        
memmoveSSE2:   ; SSE2 version begins here
memmoveSSE2@:  ; local label
        PROLOGM  memcpySSE2

        ; Cannot use memcpy. Must move backwards because of overlap between src and dest
        cmp     rcx, 40H
        jae     B0100                    ; Use simpler code if count < 64
        
        ; count < 64. Move 32-16-8-4-2-1 bytes
        test    cl, 20H
        jz      A100
        ; move 32 bytes
        ; mov is faster than movdqu on SSE2 processors,
        ; movdqu is faster on later processors
        sub     ecx, 20H
        mov     rax, [rsi+rcx+18H]
        mov     rdx, [rsi+rcx+10H]
        mov     [rdi+rcx+18H], rax
        mov     [rdi+rcx+10H], rdx
        mov     rax, [rsi+rcx+8]
        mov     rdx, [rsi+rcx]
        mov     [rdi+rcx+8], rax
        mov     [rdi+rcx], rdx
A100:   test    cl, 10H
        jz      A200
        ; move 16 bytes
        sub     ecx, 10H
        mov     rax, [rsi+rcx+8]
        mov     rdx, [rsi+rcx]
        mov     [rdi+rcx+8], rax
        mov     [rdi+rcx], rdx
A200:   test    cl, 8
        jz      A300
        ; move 8 bytes
        sub     ecx, 8
        mov     rax, [rsi+rcx]
        mov     [rdi+rcx], rax
A300:   test    cl, 4
        jz      A400
        ; move 4 bytes
        sub     ecx, 4
        mov     eax, [rsi+rcx]
        mov     [rdi+rcx], eax
        jz      A900                     ; early out if count divisible by 4
A400:   test    cl, 2
        jz      A500
        ; move 2 bytes
        sub     ecx, 2
        movzx   eax, word [rsi+rcx]
        mov     [rdi+rcx], ax
A500:   test    cl, 1
        jz      A900
        ; move 1 byte
        movzx   eax, byte [rsi]       ; rcx-1 = 0
        mov     [rdi], al
A900:   ; finished
        EPILOGM
        
B0100:  ; count >= 64
        ; Note: this part will not always work if count < 64
        ; Calculate size of last block after last regular boundary of dest
        lea     edx, [rdi+rcx]         ; end of dext
        and     edx, 0FH
        jz      B0300                   ; Skip if end of dest aligned by 16
        
        ; edx = size of last partial block, 1 - 15 bytes
        test    dl, 3
        jz      B0210
        test    dl, 1
        jz      B0201      ; B0200 if we haven't tested edx,3
        ; move 1 byte
        dec     rcx
        movzx   eax, byte [rsi+rcx]
        mov     [rdi+rcx], al        
B0200:  test    dl, 2
        jz      B0210
B0201:  ; move 2 bytes
        sub     rcx, 2
        movzx   eax, word [rsi+rcx]
        mov     [rdi+rcx], ax        
B0210:  test    dl, 4
        jz      B0220
        ; move 4 bytes
        sub     rcx, 4
        mov     eax, [rsi+rcx]
        mov     [rdi+rcx], eax
B0220:  test    dl, 8
        jz      B0300
        ; move 8 bytes
        sub     rcx, 8
        mov     rax, [rsi+rcx]
        mov     [rdi+rcx], rax
              
B0300:  ; Now end of dest is aligned by 16. Any partial block has been moved        
        ; Find alignment of end of src modulo 16 at this point:
        lea     eax, [rsi+rcx]
        and     eax, 0FH
        
        ; Set up for loop moving 32 bytes per iteration:
        mov     edx, ecx               ; Save count
        and     rcx, -20H              ; Round down to nearest multiple of 32
        sub     edx, ecx               ; Remaining data after loop
        sub     rsi, rax               ; Nearest preceding aligned block of src
        ; Add the same to rsi and rdi as we have subtracted from rcx
        add     rsi, rdx
        add     rdi, rdx
        
        ; Check if count very big
        cmp     rcx, [CacheBypassLimit]
        ja      B0400                   ; Use non-temporal store if count > CacheBypassLimit
        
        ; Dispatch to different codes depending on src alignment
        lea     r8, [MAlignmentDispatchSSE2]
        jmp     near [r8+rax*8]

B0400:   ; Dispatch to different codes depending on src alignment
        lea     r8, [MAlignmentDispatchNT]
        jmp     near [r8+rax*8]
        
        
        
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;  Macros and alignment jump tables
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Macros for each src alignment, SSE2 instruction set:
; Make separate code for each alignment u because the shift instructions
; have the shift count as a constant:

%MACRO MOVE_REVERSE_UNALIGNED_SSE2  2 ; u, nt
; Move rcx + rdx bytes of data
; Source is misaligned. (src-dest) modulo 16 = %1
; %2 = 1 if non-temporal store desired
; eax = %1
; rsi = src - %1 = nearest preceding 16-bytes boundary
; rdi = dest (aligned)
; rcx = count rounded down to nearest divisible by 32
; edx = remaining bytes to move after loop
        movdqa  xmm0, [rsi+rcx]        ; Read from nearest following 16B boundary        
%%L1:   ; Loop. rcx has positive index from the beginning, counting down to zero
        sub     rcx, 20H
        movdqa  xmm1, [rsi+rcx+10H]    ; Read next two blocks aligned
        movdqa  xmm2, [rsi+rcx]
        movdqa  xmm3, xmm1             ; Copy because used twice
        pslldq  xmm0, 16-%1            ; shift left
        psrldq  xmm1, %1               ; shift right
        por     xmm0, xmm1             ; combine blocks
        %IF %2 == 0
        movdqa  [rdi+rcx+10H], xmm0    ; Save aligned
        %ELSE
        movntdq [rdi+rcx+10H], xmm0    ; Save aligned
        %ENDIF
        movdqa  xmm0, xmm2             ; Save for next iteration
        pslldq  xmm3, 16-%1            ; shift left
        psrldq  xmm2, %1                ; shift right
        por     xmm3, xmm2             ; combine blocks
        %IF %2 == 0
        movdqa  [rdi+rcx], xmm3        ; Save aligned
        %ELSE
        movntdq [rdi+rcx], xmm3        ; Save aligned
        %ENDIF
        jnz     %%L1
                
        ; Move edx remaining bytes
        test    dl, 10H
        jz      %%L2
        ; One more 16-bytes block to move
        sub     rcx, 10H
        movdqa  xmm1, [rsi+rcx]
        pslldq  xmm0, 16-%1            ; shift left
        psrldq  xmm1, %1               ; shift right
        por     xmm0, xmm1             ; combine blocks
        %IF %2 == 0
        movdqa  [rdi+rcx], xmm0        ; Save aligned
        %ELSE
        movntdq [rdi+rcx], xmm0        ; Save aligned
        %ENDIF        
%%L2:   ; Get src pointer back to misaligned state
        add     rsi, rax
        %IF %2 == 1
        sfence
        %ENDIF
        ; Move remaining 0 - 15 bytes, unaligned
        jmp     C200
%ENDMACRO


%MACRO  MOVE_REVERSE_UNALIGNED_SSE2_4  1 ; nt
; Special case: u = 4
; %1 = 1 if non-temporal store desired
        movaps  xmm0, [rsi+rcx]        ; Read from nearest following 16B boundary
%%L1:   ; Loop. rcx has positive index from the beginning, counting down to zero
        sub     rcx, 20H
        movaps  xmm1, [rsi+rcx+10H]    ; Read next two blocks aligned
        movaps  xmm2, [rsi+rcx]
        movaps  xmm3, xmm0
        movaps  xmm0, xmm2        
        movss   xmm2, xmm1
        shufps  xmm2, xmm2, 00111001B  ; Rotate right
        movss   xmm1, xmm3
        shufps  xmm1, xmm1, 00111001B  ; Rotate right
        %IF %1 == 0
        movaps  [rdi+rcx+10H], xmm1    ; Save aligned
        movaps  [rdi+rcx], xmm2        ; Save aligned
        %ELSE
        movntps [rdi+rcx+10H], xmm1    ; Non-temporal save
        movntps [rdi+rcx], xmm2        ; Non-temporal save
        %ENDIF
        jnz     %%L1
                
        ; Move edx remaining bytes
        test    dl, 10H
        jz      %%L2
        ; One more 16-bytes block to move
        sub     rcx, 10H
        movaps  xmm1, [rsi+rcx]
        movss   xmm1, xmm0
        shufps  xmm1, xmm1, 00111001B  ; Rotate right
        %IF %1 == 0
        movaps  [rdi+rcx], xmm1        ; Save aligned
        %ELSE
        movntps [rdi+rcx], xmm1        ; Non-temporal save
        %ENDIF        
%%L2:     ; Get src pointer back to misaligned state
        add     rsi, rax
        %IF %1 == 1
        sfence
        %ENDIF
        ; Move remaining 0 - 15 bytes, unaligned
        jmp     C200
%ENDMACRO


%MACRO  MOVE_REVERSE_UNALIGNED_SSE2_8  1 ; nt
; Special case: u = 8
; %1 = 1 if non-temporal store desired
        movaps  xmm0, [rsi+rcx]        ; Read from nearest following 16B boundary
        shufps  xmm0, xmm0, 01001110B  ; Rotate
%%L1:   ; Loop. rcx has positive index from the beginning, counting down to zero
        sub     rcx, 20H
        movaps  xmm1, [rsi+rcx+10H]    ; Read next two blocks aligned
        shufps  xmm1, xmm1, 01001110B  ; Rotate
        movsd   xmm0, xmm1
        %IF %1 == 0
        movaps  [rdi+rcx+10H], xmm0    ; Save aligned
        %ELSE
        movntps [rdi+rcx+10H], xmm0    ; Non-temporal save
        %ENDIF
        movaps  xmm0, [rsi+rcx]
        shufps  xmm0, xmm0, 01001110B  ; Rotate
        movsd   xmm1, xmm0
        %IF %1 == 0
        movaps  [rdi+rcx], xmm1        ; Save aligned
        %ELSE
        movntps [rdi+rcx], xmm1        ; Non-temporal save
        %ENDIF
        jnz     %%L1
                
        ; Move edx remaining bytes
        test    dl, 10H
        jz      %%L2
        ; One more 16-bytes block to move
        sub     rcx, 10H
        movaps  xmm1, [rsi+rcx]
        shufps  xmm1, xmm1, 01001110B  ; Rotate 
        movsd   xmm0, xmm1
        %IF %1 == 0
        movaps  [rdi+rcx], xmm0        ; Save aligned
        %ELSE
        movntps [rdi+rcx], xmm0        ; Non-temporal save
        %ENDIF        
%%L2:   ; Get src pointer back to misaligned state
        add     rsi, rax
        %IF %1 == 1
        sfence
        %ENDIF
        ; Move remaining 0 - 15 bytes, unaligned
        jmp     C200
%ENDMACRO


%MACRO  MOVE_REVERSE_UNALIGNED_SSE2_12  1 ; nt
; Special case: u = 12
; %1 = 1 if non-temporal store desired
        movaps  xmm0, [rsi+rcx]        ; Read from nearest following 16B boundary
        shufps  xmm0, xmm0, 10010011B  ; Rotate right
%%L1:   ; Loop. rcx has positive index from the beginning, counting down to zero
        sub     rcx, 20H
        movaps  xmm1, [rsi+rcx+10H]    ; Read next two blocks aligned
        shufps  xmm1, xmm1, 10010011B  ; Rotate left
        movss   xmm0, xmm1
        %IF %1 == 0
        movaps  [rdi+rcx+10H], xmm0    ; Save aligned
        %ELSE
        movntps [rdi+rcx+10H], xmm0    ; Non-temporal save
        %ENDIF
        movaps  xmm0, [rsi+rcx]
        shufps  xmm0, xmm0, 10010011B  ; Rotate left
        movss   xmm1, xmm0
        %IF %1 == 0
        movaps  [rdi+rcx], xmm1        ; Save aligned
        %ELSE
        movntps [rdi+rcx], xmm1        ; Non-temporal save
        %ENDIF
        jnz     %%L1
                
        ; Move edx remaining bytes
        test    dl, 10H
        jz      %%L2
        ; One more 16-bytes block to move
        sub     rcx, 10H
        movaps  xmm1, [rsi+rcx]
        shufps  xmm1, xmm1, 10010011B  ; Rotate left
        movss   xmm0, xmm1
        %IF %1 == 0
        movaps  [rdi+rcx], xmm0        ; Save aligned
        %ELSE
        movntps [rdi+rcx], xmm0        ; Non-temporal save
        %ENDIF        
%%L2:   ; Get src pointer back to misaligned state
        add     rsi, rax
        %IF %1 == 1
        sfence
        %ENDIF
        ; Move remaining 0 - 15 bytes, unaligned
        jmp     C200
%ENDMACRO


; Macros for each src alignment, Suppl.SSE3 instruction set:
; Code for unaligned src, Suppl.SSE3 instruction set.
; Make separate code for each alignment u because the palignr instruction
; has the shift count as a constant:

%MACRO  MOVE_REVERSE_UNALIGNED_SSSE3  1; u
; Move rcx + rdx bytes of data
; Source is misaligned. (src-dest) modulo 16 = %1
; eax = %1
; rsi = src - %1 = nearest preceding 16-bytes boundary
; rdi = dest (aligned)
; rcx = - (count rounded down to nearest divisible by 32)
; edx = remaining bytes to move after loop
        movdqa  xmm0, [rsi+rcx]        ; Read from nearest following 16B boundary
        
%%L1:   ; Loop. rcx has positive index from the beginning, counting down to zero
        movdqa  xmm1, [rsi+rcx-10H]    ; Read next two blocks        
        palignr xmm0, xmm1, %1         ; Combine parts into aligned block
        movdqa  [rdi+rcx-10H], xmm0    ; Save aligned
        movdqa  xmm0, [rsi+rcx-20H]
        palignr xmm1, xmm0, %1         ; Combine parts into aligned block
        movdqa  [rdi+rcx-20H], xmm1    ; Save aligned
        sub     rcx, 20H
        jnz     %%L1
        
        ; Set up for edx remaining bytes
        test    dl, 10H
        jz      %%L2
        ; One more 16-bytes block to move
        sub     rcx, 10H
        movdqa  xmm1, [rsi+rcx]        ; Read next two blocks        
        palignr xmm0, xmm1, %1         ; Combine parts into aligned block
        movdqa  [rdi+rcx], xmm0        ; Save aligned
        
%%L2:   ; Get src pointer back to misaligned state
        add     rsi, rax
        ; Move remaining 0 - 15 bytes
        jmp     C200
%ENDMACRO


; Make 15 instances of SSE2 macro for each value of the alignment u.
; These are pointed to by the jump table MAlignmentDispatchSSE2 below
; (aligns and fillers are inserted manually to minimize the 
;  number of 16-bytes boundaries inside loops)

align   16
D104:   MOVE_REVERSE_UNALIGNED_SSE2_4    0
D108:   MOVE_REVERSE_UNALIGNED_SSE2_8    0
D10C:   MOVE_REVERSE_UNALIGNED_SSE2_12   0
D101:   MOVE_REVERSE_UNALIGNED_SSE2 1,   0
D102:   MOVE_REVERSE_UNALIGNED_SSE2 2,   0
D103:   MOVE_REVERSE_UNALIGNED_SSE2 3,   0
D105:   MOVE_REVERSE_UNALIGNED_SSE2 5,   0
D106:   MOVE_REVERSE_UNALIGNED_SSE2 6,   0
D107:   MOVE_REVERSE_UNALIGNED_SSE2 7,   0
D109:   MOVE_REVERSE_UNALIGNED_SSE2 9,   0
D10A:   MOVE_REVERSE_UNALIGNED_SSE2 0AH, 0
D10B:   MOVE_REVERSE_UNALIGNED_SSE2 0BH, 0
D10D:   MOVE_REVERSE_UNALIGNED_SSE2 0DH, 0
D10E:   MOVE_REVERSE_UNALIGNED_SSE2 0EH, 0
D10F:   MOVE_REVERSE_UNALIGNED_SSE2 0FH, 0

; Make 15 instances of Suppl-SSE3 macro for each value of the alignment u.
; These are pointed to by the jump table MAlignmentDispatchSupSSE3 below

align   16
E104:   MOVE_REVERSE_UNALIGNED_SSSE3 4
E108:   MOVE_REVERSE_UNALIGNED_SSSE3 8
E10C:   MOVE_REVERSE_UNALIGNED_SSSE3 0CH
E101:   MOVE_REVERSE_UNALIGNED_SSSE3 1
E102:   MOVE_REVERSE_UNALIGNED_SSSE3 2
E103:   MOVE_REVERSE_UNALIGNED_SSSE3 3
E105:   MOVE_REVERSE_UNALIGNED_SSSE3 5
E106:   MOVE_REVERSE_UNALIGNED_SSSE3 6
E107:   MOVE_REVERSE_UNALIGNED_SSSE3 7
E109:   MOVE_REVERSE_UNALIGNED_SSSE3 9
E10A:   MOVE_REVERSE_UNALIGNED_SSSE3 0AH
E10B:   MOVE_REVERSE_UNALIGNED_SSSE3 0BH
E10D:   MOVE_REVERSE_UNALIGNED_SSSE3 0DH
E10E:   MOVE_REVERSE_UNALIGNED_SSSE3 0EH
E10F:   MOVE_REVERSE_UNALIGNED_SSSE3 0FH
        
align   16
F100:   ; Non-temporal move, src and dest have same alignment.
        ; Loop. rcx has positive index from the beginning, counting down to zero
        sub     rcx, 20H
        movaps  xmm0, [rsi+rcx+10H]
        movaps  xmm1, [rsi+rcx]
        movntps [rdi+rcx+10H], xmm0
        movntps [rdi+rcx], xmm1
        jnz     F100
        
        ; Move the remaining edx bytes (0 - 31):
        ; move 16-8-4-2-1 bytes, aligned
        test    dl, 10H
        jz      C200
        ; move 16 bytes
        sub     rcx, 10H
        movaps  xmm0, [rsi+rcx]
        movntps  [rdi+rcx], xmm0
        sfence
        ; move the remaining 0 - 15 bytes
        jmp     C200

; Non-temporal move, src and dest have different alignment.
; Make 15 instances of SSE2 macro for each value of the alignment u.
; These are pointed to by the jump table MAlignmentDispatchNT below

align 16
F101:   MOVE_REVERSE_UNALIGNED_SSE2 1,   1
F102:   MOVE_REVERSE_UNALIGNED_SSE2 2,   1
F103:   MOVE_REVERSE_UNALIGNED_SSE2 3,   1
F104:   MOVE_REVERSE_UNALIGNED_SSE2_4    1
F105:   MOVE_REVERSE_UNALIGNED_SSE2 5,   1
F106:   MOVE_REVERSE_UNALIGNED_SSE2 6,   1
F107:   MOVE_REVERSE_UNALIGNED_SSE2 7,   1
F108:   MOVE_REVERSE_UNALIGNED_SSE2_8    1
F109:   MOVE_REVERSE_UNALIGNED_SSE2 9,   1
F10A:   MOVE_REVERSE_UNALIGNED_SSE2 0AH, 1
F10B:   MOVE_REVERSE_UNALIGNED_SSE2 0BH, 1
F10C:   MOVE_REVERSE_UNALIGNED_SSE2_12   1
F10D:   MOVE_REVERSE_UNALIGNED_SSE2 0DH, 1
F10E:   MOVE_REVERSE_UNALIGNED_SSE2 0EH, 1
F10F:   MOVE_REVERSE_UNALIGNED_SSE2 0FH, 1


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;    CPU dispatcher
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

memmoveCPUDispatch:   ; CPU dispatcher, check for Suppl-SSE3 instruction set
        ; This part is executed only once
        push    rbx
        push    rcx
        push    rdx
        push    rsi
        push    rdi
        push    r8        

        ; set CacheBypassLimit to half the size of the largest level cache
%ifdef  WINDOWS
        xor     ecx, ecx               ; 0 means default
%else
        xor     edi, edi
%endif
        call    SetMemcpyCacheLimit@
        mov     eax, 1
        cpuid                          ; Get feature flags
        lea     rbx, [memmoveSSE2@]
        bt      ecx, 9                 ; Test bit for SupplSSE3
        jnc     Q100
        lea     rbx, [memmoveSSSE3@]
        call    UnalignedIsFaster
        test    eax, eax
        jz      Q100
        lea     rbx, [memmoveU@]
        call    Store256BitIsFaster
        test    eax, eax
        jz      Q100
        lea     rbx, [memmoveU256@]
        call    InstructionSet
        cmp     eax, 15
        jb      Q100
        lea     rbx, [memmoveAVX512F@]
        cmp     eax, 16
        jb      Q100
        lea     rbx, [memmoveAVX512BW@]
        
Q100:   ; Insert appropriate pointer
        mov     [memmoveDispatch], rbx
        mov     rax, rbx
        pop     r8
        pop     rdi
        pop     rsi
        pop     rdx
        pop     rcx
        pop     rbx
        ; Jump according to the replaced function pointer
        jmp     rax
        
; Note: Must call SetMemcpyCacheLimit1 defined in memcpy64.asm
SetMemcpyCacheLimit:
SetMemcpyCacheLimit@:
        call    SetMemcpyCacheLimit1
        mov     [CacheBypassLimit], rax
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
; The CPU dispatcher replaces MAlignmentDispatch with 
; MAlignmentDispatchSSE2 or MAlignmentDispatchSupSSE3 if Suppl-SSE3 
; is supported.

; Code pointer for each alignment for SSE2 instruction set
MAlignmentDispatchSSE2:
DQ C100, D101, D102, D103, D104, D105, D106, D107
DQ D108, D109, D10A, D10B, D10C, D10D, D10E, D10F

; Code pointer for each alignment for Suppl-SSE3 instruction set
MAlignmentDispatchSSSE3:
DQ C100, E101, E102, E103, E104, E105, E106, E107
DQ E108, E109, E10A, E10B, E10C, E10D, E10E, E10F

; Code pointer for each alignment for non-temporal store
MAlignmentDispatchNT:
DQ F100, F101, F102, F103, F104, F105, F106, F107
DQ F108, F109, F10A, F10B, F10C, F10D, F10E, F10F

memmoveDispatch: DQ memmoveCPUDispatch

; Bypass cache by using non-temporal moves if count > _CacheBypassLimit
; The optimal value of CacheBypassLimit is difficult to estimate, but
; a reasonable value is half the size of the largest cache:
CacheBypassLimit: DQ 0
