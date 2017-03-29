.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
    movq $400038, %rcx
    s2: 
       subl %ebx, %eax
       nop
       nop
       nop
       nop
       loop s2
