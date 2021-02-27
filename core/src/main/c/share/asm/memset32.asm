;*************************  memset32.asm  *************************************
; Author:           Agner Fog
; Date created:     2008-07-19
; Last modified:    2016-11-06
; Description:
; Faster version of the standard memset function:
; void * A_memset(void * dest, int c, size_t count);
; Sets 'count' bytes from 'dest' to the 8-bit value 'c'
;
; Overriding standard function memset:
; The alias ?OVR_memset is changed to _memset in the object file if
; it is desired to override the standard library function memset.
;
; extern "C" size_t GetMemsetCacheLimit(); // Data blocks bigger than this will be stored uncached by memset
; extern "C" void   SetMemsetCacheLimit(); // Change limit in GetMemsetCacheLimit
;
; Position-independent code is generated if POSITIONINDEPENDENT is defined.
;
; Optimization:
; Uses vector registers to set many bytes at a time, aligned.
;
; The latest version of this file is available at:
; www.agner.org/optimize/asmexamples.zip
; Copyright (c) 2008-2016 GNU General Public License www.gnu.org/licenses
;******************************************************************************

global _A_memset             ; Function memset
global ?OVR_memset           ; ?OVR removed if standard function memset overridden
global _GetMemsetCacheLimit  ; Data blocks bigger than this will be stored uncached by memset
global _SetMemsetCacheLimit  ; Change limit in GetMemsetCacheLimit
; Direct entries to CPU-specific versions
global _memset386            ; version for old CPUs without SSE
global _memsetSSE2           ; SSE2 version
global _memsetAVX            ; version for CPUs with fast 256-bit store
global _memsetAVX512F        ; version for CPUs with fast 512-bit store
global _memsetAVX512BW       ; version for CPUs with fast 512-bit store


; Imported from cachesize32.asm:
extern _DataCacheSize                  ; Get size of data cache

; Imported from instrset32.asm
extern _InstructionSet                 ; Instruction set for CPU dispatcher

; Imported from unalignedisfaster32.asm:
extern _Store256BitIsFaster            ; Tells if a 256 bit store is faster than two 128 bit stores

; Define return from this function
%MACRO  RETURNM  0
%IFDEF  POSITIONINDEPENDENT
        pop     ebx
%ENDIF
        mov     eax, [esp+4]           ; return dest
        ret
%ENDMACRO


SECTION .text  align=16

; extern "C" void * memset(void * dest, int c, size_t count);
; Function entry:
_A_memset:
?OVR_memset:
%IFNDEF POSITIONINDEPENDENT
        jmp     dword [memsetDispatch] ; Go to appropriate version, depending on instruction set
RP      equ     0                      ; RP = 0 if not position-independent

%ELSE   ; Position-independent code
        push    ebx
        call    get_thunk_ebx          ; get reference point for position-independent code
RP:                                    ; reference point ebx = offset RP

; Make the following instruction with address relative to RP:
        jmp     dword [ebx+memsetDispatch-RP]

%ENDIF


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; AVX512BW Version for processors with fast 512 bits write
; Requires AVX512BW, BMI2
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

align 16

_memsetAVX512BW:                       ; AVX512BW version. Use zmm register
memsetAVX512BW@:                       ; local label
        mov     edx, [esp+4]           ; dest
        movzx   eax, byte [esp+8]      ; c
        imul    eax, 01010101H         ; Broadcast c into all bytes of eax
        mov     ecx, [esp+12]          ; count
        push    edi

        mov     edi, edx               ; save dest
        vpbroadcastd zmm0, eax         ; Broadcast further into 64 bytes

        cmp     ecx, 40H
        jbe     L520
        cmp     ecx, 80H
        jbe     L500                   ; Use simpler code if count <= 128

L050:   ; Common code for memsetAVX512BW and memsetAVX512F:

        ; count > 80H
        ; store first 40H bytes
        vmovdqu64 [edx], zmm0

        ; find first 40H boundary
        add     edx, 40H
        and     edx, -40H

        ; find last 40H boundary
        lea     eax, [edi + ecx]
        and     eax, -40H
        sub     edx, eax               ; negative count from last 40H boundary
        ; Check if count very big
        cmp     ecx, [_MemsetCacheLimit]        
        ja      L200                   ; Use non-temporal store if count > MemsetCacheLimit

L100:   ; main loop, aligned
        vmovdqa64 [eax+edx], zmm0
        add     edx, 40H
        jnz     L100

L110:   ; remaining 0 - 3FH bytes
        ; overlap previous write
        vmovdqu64 [edi + ecx - 40H], zmm0

        vzeroupper                     ; is this needed?
        mov     eax, edi               ; return dest
        pop     edi
        ret

align 16
L200:   ; loop with non-temporal stores
        vmovntdq [eax+edx], zmm0
        add     edx, 40H
        jnz     L200
        sfence
        jmp     L110

align 16 ; short versions, memsetAVX512BW only:

L500:   ; count = 41H - 80H
        vmovdqu64 [edx], zmm0
        add     edx, 40H
        sub     ecx, 40H

L520:   ; count = 00H - 40H
        or      eax, -1                ; if count = 1-31: |  if count = 32-63:
        bzhi    eax, eax, ecx          ; -----------------|--------------------
        kmovd   k1, eax                ;       count 1's  |  all 1's
        xor     eax, eax               ;                  |
        sub     ecx, 32                ;                  |
        cmovb   ecx, eax               ;               0  |  count-32
        dec     eax                    ;                  |
        bzhi    eax, eax, ecx          ;                  |
        kmovd   k2, eax                ;               0  |  count-32 1's
        kunpckdq k3, k2, k1            ; low 32 bits from k1, high 32 bits from k2. total = count 1's
        vmovdqu8 [edx]{k3}, zmm0
        vzeroupper
        mov     eax, edi               ; return dest
        pop     edi
        ret


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; AVX512F Version for processors with fast 512 bits write
; Requires AVX512F
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

align 16

_memsetAVX512F:                        ; AVX512BW version. Use zmm register
memsetAVX512F@:                        ; local label
        mov     edx, [esp+4]           ; dest
        movzx   eax, byte [esp+8]      ; c
        mov     ecx, [esp+12]          ; count
        imul    eax, 01010101H         ; Broadcast c into all bytes of eax
        cmp     ecx, 80H
        jbe     B010                   ; Use memsetAVX code if count <= 128
        push    edi
        mov     edi, edx               ; save dest
        vpbroadcastd zmm0, eax         ; Broadcast further into 64 bytes
        jmp     L050                   ; Use preceding code


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; AVX Version for processors with fast 256 bits write
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

_memsetAVX:  ; AVX version. Use ymm register
%IFDEF POSITIONINDEPENDENT
        push    ebx
        call    get_thunk_ebx          ; get reference point for position-independent code
        add     ebx, RP - $
memsetAVX@: ; local label
        mov     edx, [esp+4+4]           ; dest
        movzx   eax, byte [esp+4+8]      ; c
        mov     ecx, [esp+4+12]          ; count
%ELSE
memsetAVX@: ; local label
        mov     edx, [esp+4]           ; dest
        movzx   eax, byte [esp+8]      ; c
        mov     ecx, [esp+12]          ; count
%ENDIF        
        imul    eax, 01010101H         ; Broadcast c into all bytes of eax

B010:   ; entry from AVX512F version
        cmp     ecx, 16
        ja      B100
        
B050:   ; count <= 16, both SSE2 and AVX version
%IFNDEF POSITIONINDEPENDENT
        jmp     dword [MemsetJTab+ecx*4]
%ELSE
        jmp     dword [MemsetJTab-RP+ebx+ecx*4]
%ENDIF        
        
; Separate code for each count from 0 to 16:
M16:    mov     [edx+12], eax
M12:    mov     [edx+8],  eax
M08:    mov     [edx+4],  eax
M04:    mov     [edx],    eax
M00:    RETURNM

M15:    mov     [edx+11], eax
M11:    mov     [edx+7],  eax
M07:    mov     [edx+3],  eax
M03:    mov     [edx+1],  ax
M01:    mov     [edx],    al
        RETURNM
       
M14:    mov     [edx+10], eax
M10:    mov     [edx+6],  eax
M06:    mov     [edx+2],  eax
M02:    mov     [edx],    ax
        RETURNM

M13:    mov     [edx+9],  eax
M09:    mov     [edx+5],  eax
M05:    mov     [edx+1],  eax
        mov     [edx],    al
        RETURNM

align   16
B100:   ; count > 16.
        movd    xmm0, eax
        pshufd  xmm0, xmm0, 0          ; Broadcast c into all bytes of xmm0        
        lea     eax, [edx+ecx]         ; point to end
        
        cmp     ecx, 20H
        jbe     K600                   ; faster to use xmm registers if small

        ; Store the first possibly unaligned 16 bytes
        ; It is faster to always write 16 bytes, possibly overlapping
        ; with the subsequent regular part, than to make possibly mispredicted
        ; branches depending on the size of the first part.
        movups  oword [edx], xmm0
        
        ; store another 16 bytes, aligned        
        add     edx, 10H
        and     edx, -10H
        movaps  oword [edx], xmm0
        
        ; go to next 32 bytes boundary
        add     edx, 10H
        and     edx, -20H
        
        ; Check if count very big
%IFNDEF POSITIONINDEPENDENT
        cmp     ecx, [_MemsetCacheLimit]        
%ELSE   ; position-independent code
        cmp     ecx, [ebx+_MemsetCacheLimit-RP]
%ENDIF
        ja      K300                   ; Use non-temporal store if count > MemsetCacheLimit
        
        ; find last 32 bytes boundary
        mov     ecx, eax
        and     ecx, -20H
        
        ; - size of 32-bytes blocks
        sub     edx, ecx
        jnb     K200                   ; Jump if not negative
        
        ; extend value to 256 bits
        vinsertf128 ymm0,ymm0,xmm0,1
        
K100:   ; Loop through 32-bytes blocks
        ; ecx = end of 32-bytes blocks part
        ; edx = negative index from the end, counting up to zero
        vmovaps [ecx+edx], ymm0
        add     edx, 20H
        jnz     K100
        vzeroupper
        
K200:   ; the last part from ecx to eax is < 32 bytes. write last 32 bytes with overlap
        movups  [eax-20H], xmm0
        movups  [eax-10H], xmm0
        RETURNM
        
K300:   ; Use non-temporal moves, same code as above:

        ; find last 32 bytes boundary
        mov     ecx, eax
        and     ecx, -20H

        ; - size of 32-bytes blocks
        sub     edx, ecx
        jnb     K500                   ; Jump if not negative
        
        ; extend value to 256 bits
        vinsertf128 ymm0,ymm0,xmm0,1

align   16        
K400:   ; Loop through 32-bytes blocks
        ; ecx = end of 32-bytes blocks part
        ; edx = negative index from the end, counting up to zero
        vmovntps [ecx+edx], ymm0
        add     edx, 20H
        jnz     K400
        sfence
        vzeroupper
        
K500:   ; the last part from ecx to eax is < 32 bytes. write last 32 bytes with overlap
        movups  [eax-20H], xmm0
        movups  [eax-10H], xmm0
        RETURNM
        
K600:   ; 16 < count <= 32
        movups  [edx], xmm0
        movups  [eax-10H], xmm0
        RETURNM        

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;   SSE2 Version
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

align 16
_memsetSSE2:  ; SSE2 version. Use xmm register
%IFDEF POSITIONINDEPENDENT
        push    ebx
        call    get_thunk_ebx          ; get reference point for position-independent code
        add     ebx, RP - $
memsetSSE2@: ; local label
        mov     edx, [esp+4+4]           ; dest
        movzx   eax, byte [esp+4+8]      ; c
        mov     ecx, [esp+4+12]          ; count
%ELSE
memsetSSE2@: ; local label
        mov     edx, [esp+4]           ; dest
        movzx   eax, byte [esp+8]      ; c
        mov     ecx, [esp+12]          ; count
%ENDIF        
        imul    eax, 01010101H         ; Broadcast c into all bytes of eax
        cmp     ecx, 16
        jna     B050                   ; small counts: same as AVX version
        movd    xmm0, eax
        pshufd  xmm0, xmm0, 0          ; Broadcast c into all bytes of xmm0
        
        ; Store the first unaligned part.
        ; The size of this part is 1 - 16 bytes.
        ; It is faster to always write 16 bytes, possibly overlapping
        ; with the subsequent regular part, than to make possibly mispredicted
        ; branches depending on the size of the first part.
        movq    qword [edx],   xmm0
        movq    qword [edx+8], xmm0
        
        ; Check if count very big
%IFNDEF POSITIONINDEPENDENT
        cmp     ecx, [_MemsetCacheLimit]        
%ELSE   ; position-independent code
        cmp     ecx, [ebx+_MemsetCacheLimit-RP]
%ENDIF
        ja      M500                   ; Use non-temporal store if count > MemsetCacheLimit
        
        ; Point to end of regular part:
        ; Round down dest+count to nearest preceding 16-bytes boundary
        lea     ecx, [edx+ecx-1]
        and     ecx, -10H
        
        ; Point to start of regular part:
        ; Round up dest to next 16-bytes boundary
        add     edx, 10H
        and     edx, -10H
        
        ; -(size of regular part)
        sub     edx, ecx
        jnb     M300                   ; Jump if not negative

align 16        
M200:   ; Loop through regular part
        ; ecx = end of regular part
        ; edx = negative index from the end, counting up to zero
        movdqa  [ecx+edx], xmm0
        add     edx, 10H
        jnz     M200
        
M300:   ; Do the last irregular part
        ; The size of this part is 1 - 16 bytes.
        ; It is faster to always write 16 bytes, possibly overlapping
        ; with the preceding regular part, than to make possibly mispredicted
        ; branches depending on the size of the last part.
%IFDEF  POSITIONINDEPENDENT            ; (ebx is pushed)
        mov     eax, [esp+4+4]         ; dest
        mov     ecx, [esp+4+12]        ; count
%ELSE
        mov     eax, [esp+4]           ; dest
        mov     ecx, [esp+12]          ; count
%ENDIF
        movq    qword [eax+ecx-10H], xmm0
        movq    qword [eax+ecx-8], xmm0
        RETURNM
   
M500:   ; Use non-temporal moves, same code as above:
        ; End of regular part:
        ; Round down dest+count to nearest preceding 16-bytes boundary
        lea     ecx, [edx+ecx-1]
        and     ecx, -10H
        
        ; Start of regular part:
        ; Round up dest to next 16-bytes boundary
        add     edx, 10H
        and     edx, -10H
        
        ; -(size of regular part)
        sub     edx, ecx
        jnb     M700                   ; Jump if not negative

align 16        
M600:   ; Loop through regular part
        ; ecx = end of regular part
        ; edx = negative index from the end, counting up to zero
        movntdq [ecx+edx], xmm0
        add     edx, 10H
        jnz     M600
        sfence
        
M700:   ; Do the last irregular part (same as M300)
%IFDEF  POSITIONINDEPENDENT            ; (ebx is pushed)
        mov     eax, [esp+4+4]         ; dest
        mov     ecx, [esp+4+12]        ; count
%ELSE
        mov     eax, [esp+4]           ; dest
        mov     ecx, [esp+12]          ; count
%ENDIF
        movq    qword [eax+ecx-10H], xmm0
        movq    qword [eax+ecx-8], xmm0
        RETURNM
     
        
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;   80386 Version
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

_memset386:  ; 80386 version
%IFDEF POSITIONINDEPENDENT
        push    ebx
        call    get_thunk_ebx          ; get reference point for position-independent code
        add     ebx, RP - $
memset386@: ; local label
        mov     edx, [esp+4+4]           ; dest
        xor     eax, eax
        mov     al,  byte [esp+4+8]      ; c
        mov     ecx, [esp+4+12]          ; count
%ELSE
memset386@: ; local label
        mov     edx, [esp+4]           ; dest
        xor     eax, eax
        mov     al,  byte [esp+8]      ; c
        mov     ecx, [esp+12]          ; count
%ENDIF        
        imul    eax, 01010101H         ; Broadcast c into all bytes of eax
        push    edi
        mov     edi, edx
        cmp     ecx, 4
        jb      N400
N200:   test    edi, 3
        jz      N300
        ; unaligned
N210:   mov     [edi], al              ; store 1 byte until edi aligned
        inc     edi
        dec     ecx
        test    edi, 3
        jnz     N210
N300:   ; aligned
        mov     edx, ecx
        shr     ecx, 2
        cld
        rep     stosd                  ; store 4 bytes at a time
        mov     ecx, edx
        and     ecx, 3
N400:   rep     stosb                  ; store any remaining bytes
        pop     edi
        RETURNM
        
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; CPU dispatching for memset. This is executed only once
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
        
memsetCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        pushad
        call    GetMemsetCacheLimit@                    ; calculate cache limit
        call    _InstructionSet                         ; get supported instruction set
        mov     ebx, eax
        ; Point to generic version of memset
        mov     dword [memsetDispatch],  memset386@
        cmp     eax, 4                 ; check SSE2
        jb      Q100
        ; SSE2 supported
        ; Point to SSE2 version of memset
        mov     dword [memsetDispatch],  memsetSSE2@
        call    _Store256BitIsFaster                    ; check if 256-bit stores are available and faster
        test    eax, eax
        jz      Q100
        mov     dword [memsetDispatch],  memsetAVX@
        cmp     ebx, 15
        jb      Q100
        mov     dword [memsetDispatch],  memsetAVX512F@
        cmp     ebx, 16
        jb      Q100
        mov     dword [memsetDispatch],  memsetAVX512BW@
        
Q100:   popad
        ; Continue in appropriate version of memset
        jmp     dword [memsetDispatch]

%ELSE   ; Position-independent version
        pushad
        call    GetMemsetCacheLimit@
        call    _InstructionSet 
                
        ; Point to generic version of memset
        lea     esi, [ebx+memset386@-RP]
        cmp     eax, 4                 ; check SSE2
        jb      Q100
        ; SSE2 supported
        ; Point to SSE2 version of memset
        lea     esi, [ebx+memsetSSE2@-RP]
        call    _Store256BitIsFaster                    ; check if 256-bit stores are available and faster
        test    eax, eax
        jz      Q100
        lea     esi, [ebx+memsetAVX@-RP]
        ; memsetAVX512F and memsetAVX512BW have no position-independent version

Q100:   mov     [ebx+memsetDispatch-RP], esi
        popad
        ; Continue in appropriate version of memset
        jmp     [ebx+memsetDispatch-RP]        
        
get_thunk_ebx: ; load caller address into ebx for position-independent code
        mov     ebx, [esp]
        ret        
%ENDIF


; extern "C" size_t GetMemsetCacheLimit(); // Data blocks bigger than this will be stored uncached by memset
_GetMemsetCacheLimit:
GetMemsetCacheLimit@:  ; local label
        push    ebx
%ifdef  POSITIONINDEPENDENT
        call    get_thunk_ebx
        add     ebx, _MemsetCacheLimit - $
%else
        mov     ebx, _MemsetCacheLimit
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

; extern "C" void   SetMemsetCacheLimit(); // Change limit in GetMemsetCacheLimit
_SetMemsetCacheLimit:
        push    ebx
%ifdef  POSITIONINDEPENDENT
        call    get_thunk_ebx
        add     ebx, _MemsetCacheLimit - $
%else
        mov     ebx, _MemsetCacheLimit
%endif
        mov     eax, [esp+8]
        test    eax, eax
        jnz     U400
        ; zero, means default
        mov     [ebx], eax
        call    GetMemsetCacheLimit@
U400:   
        mov     [ebx], eax
        pop     ebx
        ret


SECTION .data
align 16

; Jump table for count from 0 to 16:
MemsetJTab DD M00, M01, M02, M03, M04, M05, M06, M07
           DD M08, M09, M10, M11, M12, M13, M14, M15, M16

; Pointer to appropriate version.
; This initially points to memsetCPUDispatch. memsetCPUDispatch will
; change this to the appropriate version of memset, so that
; memsetCPUDispatch is only executed once:
memsetDispatch DD memsetCPUDispatch

; Bypass cache by using non-temporal moves if count > MemsetCacheLimit
; The optimal value of MemsetCacheLimit is difficult to estimate, but
; a reasonable value is half the size of the largest cache
_MemsetCacheLimit: DD 0

%IFDEF POSITIONINDEPENDENT
; Fix potential problem in Mac linker
        DD      0, 0
%ENDIF
