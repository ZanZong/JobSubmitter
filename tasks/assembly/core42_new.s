.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
      movq $400020, %rcx
      s42:
        mulss   %xmm0, %xmm1
        nop
        nop
        nop
        nop
	loop s42
