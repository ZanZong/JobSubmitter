.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
    movq $301251, %rcx
    s26:
       cld
       loop s26
