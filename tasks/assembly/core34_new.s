.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
      movq $481012, %rcx
      s34:
        mulss  value1, %xmm0
	loop s34
