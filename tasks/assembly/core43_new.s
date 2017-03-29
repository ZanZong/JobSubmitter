.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
      movq $266905, %rcx
      s43:
        cld
        nop
        nop
        nop
        nop
	loop s43
