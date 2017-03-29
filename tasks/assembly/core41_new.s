.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
      movq $342816, %rcx
      s41:
        addss  %xmm0, %xmm1
        nop
        nop
        nop
        nop
        nop
        nop
        nop
        nop 
        loop s41
