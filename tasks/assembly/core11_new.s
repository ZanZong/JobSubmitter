.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
    movq $23000, %rcx
    s11:
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       divss  %xmm1, %xmm0
       nop
       nop
       nop
       loop s11
