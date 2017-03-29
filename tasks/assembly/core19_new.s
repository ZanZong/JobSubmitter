.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
     movq $171440, %rcx       
     s19:
        divss  value1, %xmm0
        divss  value1, %xmm0
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        loop s19
